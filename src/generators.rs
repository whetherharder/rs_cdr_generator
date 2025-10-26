// Event generation logic for CALL, SMS, and DATA events
use crate::config::Config;
use crate::identity::{build_contacts, build_subscribers, gen_imei, Subscriber};
use crate::subscriber_db::SubscriberDatabase;
use crate::timezone_utils::{to_epoch_ms, tz_from_name, tz_offset_minutes};
use crate::writer::{EventRow, EventWriter};
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Weekday};
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::{Distribution, LogNormal, Normal};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Calculate lognormal mu and sigma from quantiles
pub fn lognorm_params_from_quantiles(p50: f64, p90: f64) -> (f64, f64) {
    let mu = p50.max(1.0).ln();
    let sigma = (p90.max(1.0) / p50.max(1.0)).ln() / 1.2815515655446004;
    let sigma = sigma.max(0.2).min(2.0);
    (mu, sigma)
}

/// Sample call duration from lognormal distribution
pub fn sample_call_duration(rng: &mut StdRng, mu: f64, sigma: f64) -> i64 {
    let log_normal = LogNormal::new(mu, sigma).unwrap();
    log_normal.sample(rng).max(1.0) as i64
}

/// Sample from Poisson distribution
pub fn sample_poisson(mean: f64, rng: &mut StdRng) -> usize {
    if mean <= 0.0 {
        return 0;
    }
    if mean < 30.0 {
        let l = (-mean).exp();
        let mut k: usize = 0;
        let mut p = 1.0;
        while p > l {
            k += 1;
            p *= rng.gen::<f64>();
        }
        k.saturating_sub(1)
    } else {
        let normal = Normal::new(mean, mean.sqrt()).unwrap();
        normal.sample(rng).max(0.0) as usize
    }
}

/// Calculate activity multiplier based on time of day, season, and special days
pub fn diurnal_multiplier(dt: &DateTime<chrono_tz::Tz>, cfg: &Config, day_str: &str) -> f64 {
    let arr = if dt.weekday() == Weekday::Sat || dt.weekday() == Weekday::Sun {
        &cfg.diurnal_weekend
    } else {
        &cfg.diurnal_weekday
    };

    let base = arr[dt.hour() as usize];
    let seas = cfg.seasonality.get(&(dt.month() as usize)).unwrap_or(&1.0);
    let special = cfg.special_days.get(day_str).unwrap_or(&1.0);

    base * seas * special
}

/// Generate CALL events
pub struct CallGenerator {
    p_mo: f64,
    dispo_pop: Vec<String>,
    dispo_dist: WeightedIndex<f64>,
    mu: f64,
    sigma: f64,
}

impl CallGenerator {
    pub fn new(cfg: &Config) -> Self {
        let p_mo = cfg.mo_share_call;

        let dispo_pop: Vec<String> = cfg.call_dispositions.keys().cloned().collect();
        let dispo_wts: Vec<f64> = dispo_pop
            .iter()
            .map(|k| *cfg.call_dispositions.get(k).unwrap())
            .collect();

        let dispo_dist = WeightedIndex::new(&dispo_wts).unwrap();

        let (mu, sigma) = lognorm_params_from_quantiles(
            cfg.call_duration_quantiles.p50 as f64,
            cfg.call_duration_quantiles.p90 as f64,
        );

        CallGenerator {
            p_mo,
            dispo_pop,
            dispo_dist,
            mu,
            sigma,
        }
    }

    pub fn generate(
        &self,
        sub: &Subscriber,
        start_local: DateTime<chrono_tz::Tz>,
        other_msisdn: u64,
        tz_name: &'static str,
        cell_id: u32,
        rng: &mut StdRng,
    ) -> EventRow {
        let direction = if rng.gen::<f64>() < self.p_mo {
            "MO"
        } else {
            "MT"
        };

        let (msisdn_src, msisdn_dst) = if direction == "MO" {
            (sub.msisdn, other_msisdn)
        } else {
            (other_msisdn, sub.msisdn)
        };

        let dispo = &self.dispo_pop[self.dispo_dist.sample(rng)];

        let (dur_sec, cause) = match dispo.as_str() {
            "ANSWERED" => {
                let ring = rng.gen_range(2..=25);
                let dur = sample_call_duration(rng, self.mu, self.sigma);
                (ring + dur, "normalRelease")
            }
            "NO ANSWER" => {
                let dur = rng.gen_range(5..=30);
                (dur, "noAnswer")
            }
            "BUSY" => {
                let dur = rng.gen_range(2..=10);
                (dur, "busy")
            }
            _ => {
                // FAILED or CONGESTION
                let dur = rng.gen_range(1..=5);
                (dur, "failure")
            }
        };

        let end_local = start_local + Duration::seconds(dur_sec);

        EventRow {
            event_type: "CALL",
            msisdn_src,
            msisdn_dst,
            direction,
            start_ts_ms: to_epoch_ms(&start_local.with_timezone(&chrono::Utc)),
            end_ts_ms: to_epoch_ms(&end_local.with_timezone(&chrono::Utc)),
            tz_name,
            tz_offset_min: tz_offset_minutes(&start_local),
            duration_sec: dur_sec,
            mccmnc: sub.mccmnc,
            imsi: sub.imsi,
            imei: sub.imei,
            cell_id,
            record_type: "mscVoiceRecord",
            cause_for_record_closing: cause,
            sms_segments: 0,
            sms_status: "",
            data_bytes_in: 0,
            data_bytes_out: 0,
            data_duration_sec: 0,
            apn: "",
            rat: "",
        }
    }
}

/// Generate SMS events
pub struct SmsGenerator {
    p_mo: f64,
    status_dist: WeightedIndex<f64>,
    segments_dist: WeightedIndex<f64>,
}

impl SmsGenerator {
    pub fn new(cfg: &Config) -> Self {
        let status_weights = [0.1, 0.88, 0.02];
        let status_dist = WeightedIndex::new(&status_weights).unwrap();

        let segments_weights = [0.85, 0.13, 0.02];
        let segments_dist = WeightedIndex::new(&segments_weights).unwrap();

        SmsGenerator {
            p_mo: cfg.mo_share_sms,
            status_dist,
            segments_dist,
        }
    }

    pub fn generate(
        &self,
        sub: &Subscriber,
        start_local: DateTime<chrono_tz::Tz>,
        other_msisdn: u64,
        tz_name: &'static str,
        cell_id: u32,
        rng: &mut StdRng,
    ) -> EventRow {
        let direction = if rng.gen::<f64>() < self.p_mo {
            "MO"
        } else {
            "MT"
        };

        let (msisdn_src, msisdn_dst, record_type) = if direction == "MO" {
            (sub.msisdn, other_msisdn, "sgsnSMORecord")
        } else {
            (other_msisdn, sub.msisdn, "sgsnSMTRecord")
        };

        let dur = rng.gen_range(1..=5);
        let end_local = start_local + Duration::seconds(dur);

        let sms_status = match self.status_dist.sample(rng) {
            0 => "SENT",
            1 => "DELIVERED",
            _ => "FAILED",
        };

        let cause = if sms_status == "FAILED" {
            "deliveryFailure"
        } else {
            "deliverySuccess"
        };

        let sms_segments = match self.segments_dist.sample(rng) {
            0 => 1,
            1 => 2,
            _ => 3,
        };

        EventRow {
            event_type: "SMS",
            msisdn_src,
            msisdn_dst,
            direction,
            start_ts_ms: to_epoch_ms(&start_local.with_timezone(&chrono::Utc)),
            end_ts_ms: to_epoch_ms(&end_local.with_timezone(&chrono::Utc)),
            tz_name,
            tz_offset_min: tz_offset_minutes(&start_local),
            duration_sec: dur,
            mccmnc: sub.mccmnc,
            imsi: sub.imsi,
            imei: sub.imei,
            cell_id,
            record_type,
            cause_for_record_closing: cause,
            sms_segments,
            sms_status,
            data_bytes_in: 0,
            data_bytes_out: 0,
            data_duration_sec: 0,
            apn: "",
            rat: "",
        }
    }
}

/// Generate DATA session events
pub struct DataGenerator {
    cells_by_rat: HashMap<String, Vec<u32>>,
    cells_all: Vec<u32>,
    rat_dist: WeightedIndex<f64>,
    apn_dist: WeightedIndex<f64>,
}

impl DataGenerator {
    pub fn new(cells_by_rat: HashMap<String, Vec<u32>>, cells_all: Vec<u32>) -> Self {
        let rat_weights = [0.3, 0.5, 0.2];
        let rat_dist = WeightedIndex::new(&rat_weights).unwrap();

        let apn_weights = [0.8, 0.1, 0.1];
        let apn_dist = WeightedIndex::new(&apn_weights).unwrap();

        DataGenerator {
            cells_by_rat,
            cells_all,
            rat_dist,
            apn_dist,
        }
    }

    pub fn generate(
        &self,
        sub: &Subscriber,
        start_local: DateTime<chrono_tz::Tz>,
        tz_name: &'static str,
        rng: &mut StdRng,
    ) -> EventRow {
        let rat = match self.rat_dist.sample(rng) {
            0 => "WCDMA",
            1 => "LTE",
            _ => "NR",
        };

        let (down_mean, down_sd, up_ratio_min, up_ratio_max, dur_mean, dur_sd) = match rat {
            "LTE" => (4_000_000.0, 2_000_000.0, 0.1, 0.3, 300.0, 180.0),
            "NR" => (12_000_000.0, 8_000_000.0, 0.1, 0.35, 240.0, 180.0),
            _ => (1_000_000.0, 600_000.0, 0.08, 0.25, 420.0, 240.0),
        };

        let dur_normal = Normal::new(dur_mean, dur_sd).unwrap();
        let dur = (dur_normal.sample(rng) as f64).abs().max(5.0) as i64;
        let end_local = start_local + Duration::seconds(dur);

        let down_normal = Normal::new(down_mean, down_sd).unwrap();
        let down = (down_normal.sample(rng) as f64).abs().max(2_000.0) as u64;
        let up = (down as f64 * rng.gen_range(up_ratio_min..=up_ratio_max))
            .max(1_000.0) as u64;

        let apn = match self.apn_dist.sample(rng) {
            0 => "internet",
            1 => "ims",
            _ => "mms",
        };

        let candidates = self.cells_by_rat.get(rat).unwrap_or(&self.cells_all);
        let cell_id = if !candidates.is_empty() {
            candidates[rng.gen_range(0..candidates.len())]
        } else {
            rng.gen_range(10_000..100_000)
        };

        let record_types = ["sgsnPDPRecord", "pgwRecord"];
        let record_type = record_types[rng.gen_range(0..record_types.len())];

        EventRow {
            event_type: "DATA",
            msisdn_src: sub.msisdn,
            msisdn_dst: 0,
            direction: "MO",
            start_ts_ms: to_epoch_ms(&start_local.with_timezone(&chrono::Utc)),
            end_ts_ms: to_epoch_ms(&end_local.with_timezone(&chrono::Utc)),
            tz_name,
            tz_offset_min: tz_offset_minutes(&start_local),
            duration_sec: dur,
            mccmnc: sub.mccmnc,
            imsi: sub.imsi,
            imei: sub.imei,
            cell_id,
            record_type,
            cause_for_record_closing: "normalRelease",
            sms_segments: 0,
            sms_status: "",
            data_bytes_in: up,
            data_bytes_out: down,
            data_duration_sec: dur,
            apn,
            rat,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardStats {
    pub shard: usize,
    pub calls: usize,
    pub sms: usize,
    pub data: usize,
}

/// Worker process that generates events for a shard of users
pub fn worker_generate(
    day: DateTime<chrono_tz::Tz>,
    shard_id: usize,
    users_range: (usize, usize),
    cfg: &Config,
    out_dir: &Path,
    subscriber_db: Option<&SubscriberDatabase>,
) -> anyhow::Result<()> {
    let seed = (cfg.workers as u64).wrapping_mul(1000) + shard_id as u64;
    let mut rng = StdRng::seed_from_u64(seed);

    let tz = tz_from_name(&cfg.tz_name);
    // Convert to 'static str for zero-copy EventRow usage
    let tz_name: &'static str = Box::leak(cfg.tz_name.clone().into_boxed_str());

    // Build contacts & subscribers for this shard
    let (start_u, end_u) = users_range;
    let shard_pop = end_u - start_u;

    // Pre-allocate with exact capacity to avoid reallocations
    let contacts = build_contacts(shard_pop, 30, &mut rng);

    // Use subscriber database if provided, otherwise generate random subscribers
    let subs = if let Some(_db) = subscriber_db {
        // When using subscriber DB, we'll generate a placeholder list
        // The actual subscriber data will be fetched dynamically per event
        vec![Subscriber {
            msisdn: 0,
            imsi: 0,
            mccmnc: 0,
            imei: 0,
        }; shard_pop]
    } else {
        build_subscribers(shard_pop, &cfg.prefixes, &cfg.mccmnc_pool, &mut rng)
    };

    // Event counts per user
    let avg_calls = cfg.avg_calls_per_user;
    let avg_sms = cfg.avg_sms_per_user;
    let avg_data = cfg.avg_data_sessions_per_user;

    // Initialize generators
    let call_gen = CallGenerator::new(cfg);
    let sms_gen = SmsGenerator::new(cfg);
    let data_gen = DataGenerator::new(HashMap::new(), vec![]);

    let day_str = day.format("%Y-%m-%d").to_string();
    let mut writer = EventWriter::new(out_dir, &day_str, cfg.rotate_bytes, shard_id)?;

    let day_start_local = tz
        .with_ymd_and_hms(day.year(), day.month(), day.day(), 0, 0, 0)
        .unwrap();

    let mut stats = ShardStats {
        shard: shard_id,
        calls: 0,
        sms: 0,
        data: 0,
    };

    // Helper: sample time during the day with diurnal pattern
    let sample_time = |rng: &mut StdRng| -> DateTime<chrono_tz::Tz> {
        for _ in 0..10 {
            let offset_secs = rng.gen_range(0..86400);
            let t = day_start_local + Duration::seconds(offset_secs);
            if rng.gen::<f64>() < diurnal_multiplier(&t, cfg, &day_str) {
                return t;
            }
        }
        let offset_secs = rng.gen_range(0..86400);
        day_start_local + Duration::seconds(offset_secs)
    };

    // Build IMSI list for this shard if using subscriber DB
    let imsi_list: Vec<String> = if let Some(db) = subscriber_db {
        // Get unique IMSIs from database and distribute across shards
        let all_imsis: Vec<String> = db.get_all_unique_imsi();
        let total_imsis = all_imsis.len();

        if total_imsis == 0 {
            eprintln!("Warning: No IMSIs found in subscriber database");
            Vec::with_capacity(0)
        } else {
            // Distribute IMSIs across shards with pre-allocation
            let mut list = Vec::with_capacity(shard_pop);
            list.extend(
                all_imsis.into_iter()
                    .skip(start_u % total_imsis)
                    .take(shard_pop)
            );
            list
        }
    } else {
        Vec::with_capacity(0)
    };

    // Parse prefixes to u64 for numeric operations
    let numeric_prefixes: Vec<u64> = cfg.prefixes
        .iter()
        .map(|s| s.parse().unwrap_or(31612))
        .collect();

    for uidx in 0..shard_pop {
        // Get subscriber info - either from DB snapshot or generated subscriber
        let mut sub = if subscriber_db.is_some() {
            // TODO: Full subscriber database support with primitive types
            // For now, skip when using subscriber database
            continue;
        } else {
            let mut s = subs[uidx];
            // Occasional IMEI change (new device) - only for non-DB mode
            if rng.gen::<f64>() < cfg.imei_daily_change_prob {
                s.imei = gen_imei(&mut rng);
            }
            s
        };

        let c = &contacts[uidx % contacts.len()];
        let c_pool = &c.pool;
        let c_probs = &c.probs;

        // Pre-create contact distribution (CRITICAL OPTIMIZATION!)
        let contact_dist = if !c_pool.is_empty() {
            Some(WeightedIndex::new(c_probs).unwrap())
        } else {
            None
        };

        // Sample event counts for this user
        let n_calls = sample_poisson(avg_calls, &mut rng);
        let n_sms = sample_poisson(avg_sms, &mut rng);
        let n_data = sample_poisson(avg_data, &mut rng);

        // Generate CALL events
        for _ in 0..n_calls {
            let start_local = sample_time(&mut rng);

            // Pick counterpart MSISDN (u64)
            let other_msisdn: u64 = if let Some(ref dist) = contact_dist {
                let other_idx = c_pool[dist.sample(&mut rng)] % subs.len();
                subs[other_idx].msisdn
            } else {
                // Generate random MSISDN
                let prefix_idx = rng.gen_range(0..numeric_prefixes.len());
                let prefix = numeric_prefixes[prefix_idx];
                let subscriber_number = rng.gen_range(0..10_000_000u64);
                prefix * 10_000_000 + subscriber_number
            };

            let cell_id = rng.gen_range(10_000..100_000);
            let row = call_gen.generate(&sub, start_local, other_msisdn, tz_name, cell_id, &mut rng);
            writer.write_row(&row)?;
            stats.calls += 1;
        }

        // Generate SMS events
        for _ in 0..n_sms {
            let start_local = sample_time(&mut rng);

            // TODO: Support subscriber database updates for SMS
            if subscriber_db.is_some() {
                // Skip for now when using subscriber database
                continue;
            }

            // Pick counterpart MSISDN (u64)
            let other_msisdn: u64 = if let Some(ref dist) = contact_dist {
                let other_idx = c_pool[dist.sample(&mut rng)] % subs.len();
                subs[other_idx].msisdn
            } else {
                // Generate random MSISDN
                let prefix_idx = rng.gen_range(0..numeric_prefixes.len());
                let prefix = numeric_prefixes[prefix_idx];
                let subscriber_number = rng.gen_range(0..10_000_000u64);
                prefix * 10_000_000 + subscriber_number
            };

            let cell_id = rng.gen_range(10_000..100_000);
            let row = sms_gen.generate(&sub, start_local, other_msisdn, tz_name, cell_id, &mut rng);
            writer.write_row(&row)?;
            stats.sms += 1;
        }

        // Generate DATA sessions
        for _ in 0..n_data {
            let start_local = sample_time(&mut rng);

            // TODO: Support subscriber database updates for DATA
            if subscriber_db.is_some() {
                // Skip for now when using subscriber database
                continue;
            }

            let row = data_gen.generate(&sub, start_local, tz_name, &mut rng);
            writer.write_row(&row)?;
            stats.data += 1;
        }
    }

    writer.close()?;

    // Write stats
    let stat_path = out_dir
        .join(&day_str)
        .join(format!("stats_shard{:03}.json", shard_id));
    let stats_json = serde_json::to_string_pretty(&stats)?;
    std::fs::write(stat_path, stats_json)?;

    Ok(())
}
