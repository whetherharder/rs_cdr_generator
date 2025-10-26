// Event generation logic for CALL, SMS, and DATA events
use crate::async_writer::{EventBatch, WriterMessage};
use crate::config::Config;
use crate::event_pool::EventPool;
use crate::identity::{build_contacts, build_subscribers, gen_imei, Subscriber};
use crate::subscriber_db::SubscriberDatabase;
use crate::subscriber_db_redb::SubscriberDbRedb;
use crate::timezone_utils::{to_epoch_ms, tz_from_name, tz_offset_minutes};
use crate::writer::EventRow;
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Weekday};
use crossbeam_channel::Sender;
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
        event: &mut EventRow,
        sub: &Subscriber,
        start_local: DateTime<chrono_tz::Tz>,
        other_msisdn: u64,
        tz_name: &'static str,
        cell_id: u32,
        rng: &mut StdRng,
    ) {
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

        event.event_type = "CALL";
        event.msisdn_src = msisdn_src;
        event.msisdn_dst = msisdn_dst;
        event.direction = direction;
        event.start_ts_ms = to_epoch_ms(&start_local.with_timezone(&chrono::Utc));
        event.end_ts_ms = to_epoch_ms(&end_local.with_timezone(&chrono::Utc));
        event.tz_name = tz_name;
        event.tz_offset_min = tz_offset_minutes(&start_local);
        event.duration_sec = dur_sec;
        event.mccmnc = sub.mccmnc;
        event.imsi = sub.imsi;
        event.imei = sub.imei;
        event.cell_id = cell_id;
        event.record_type = "mscVoiceRecord";
        event.cause_for_record_closing = cause;
        // Leave other fields at default (reset by pool)
    }

    /// Generate call event with forced direction (for MO↔MT correlation)
    /// This allows explicit MO or MT record generation
    pub fn generate_forced_direction(
        &self,
        event: &mut EventRow,
        sub: &Subscriber,
        start_local: DateTime<chrono_tz::Tz>,
        other_msisdn: u64,
        tz_name: &'static str,
        cell_id: u32,
        rng: &mut StdRng,
        forced_direction: &'static str,  // "MO" or "MT"
    ) {
        let direction = forced_direction;

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

        event.event_type = "CALL";
        event.msisdn_src = msisdn_src;
        event.msisdn_dst = msisdn_dst;
        event.direction = direction;
        event.start_ts_ms = to_epoch_ms(&start_local.with_timezone(&chrono::Utc));
        event.end_ts_ms = to_epoch_ms(&end_local.with_timezone(&chrono::Utc));
        event.tz_name = tz_name;
        event.tz_offset_min = tz_offset_minutes(&start_local);
        event.duration_sec = dur_sec;
        event.mccmnc = sub.mccmnc;
        event.imsi = sub.imsi;
        event.imei = sub.imei;
        event.cell_id = cell_id;
        event.record_type = "mscVoiceRecord";
        event.cause_for_record_closing = cause;
        // Leave other fields at default (reset by pool)
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
        event: &mut EventRow,
        sub: &Subscriber,
        start_local: DateTime<chrono_tz::Tz>,
        other_msisdn: u64,
        tz_name: &'static str,
        cell_id: u32,
        rng: &mut StdRng,
    ) {
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

        event.event_type = "SMS";
        event.msisdn_src = msisdn_src;
        event.msisdn_dst = msisdn_dst;
        event.direction = direction;
        event.start_ts_ms = to_epoch_ms(&start_local.with_timezone(&chrono::Utc));
        event.end_ts_ms = to_epoch_ms(&end_local.with_timezone(&chrono::Utc));
        event.tz_name = tz_name;
        event.tz_offset_min = tz_offset_minutes(&start_local);
        event.duration_sec = dur;
        event.mccmnc = sub.mccmnc;
        event.imsi = sub.imsi;
        event.imei = sub.imei;
        event.cell_id = cell_id;
        event.record_type = record_type;
        event.cause_for_record_closing = cause;
        event.sms_segments = sms_segments;
        event.sms_status = sms_status;
        // Leave data fields at default (reset by pool)
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
        event: &mut EventRow,
        sub: &Subscriber,
        start_local: DateTime<chrono_tz::Tz>,
        tz_name: &'static str,
        rng: &mut StdRng,
    ) {
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

        event.event_type = "DATA";
        event.msisdn_src = sub.msisdn;
        event.msisdn_dst = 0;
        event.direction = "MO";
        event.start_ts_ms = to_epoch_ms(&start_local.with_timezone(&chrono::Utc));
        event.end_ts_ms = to_epoch_ms(&end_local.with_timezone(&chrono::Utc));
        event.tz_name = tz_name;
        event.tz_offset_min = tz_offset_minutes(&start_local);
        event.duration_sec = dur;
        event.mccmnc = sub.mccmnc;
        event.imsi = sub.imsi;
        event.imei = sub.imei;
        event.cell_id = cell_id;
        event.record_type = record_type;
        event.cause_for_record_closing = "normalRelease";
        event.data_bytes_in = up;
        event.data_bytes_out = down;
        event.data_duration_sec = dur;
        event.apn = apn;
        event.rat = rat;
        // Leave SMS fields at default (reset by pool)
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
    subscriber_db_path: Option<&Path>,
    writer_tx: Sender<WriterMessage>,
) -> anyhow::Result<()> {
    // If redb path is configured, use chunked processing for memory efficiency
    if let Some(ref redb_path) = cfg.subscriber_db_redb_path {
        return worker_generate_redb_chunked(
            day,
            shard_id,
            users_range,
            cfg,
            out_dir,
            redb_path,
            writer_tx,
        );
    }

    use chrono::Duration;

    let seed = (cfg.workers as u64).wrapping_mul(1000) + shard_id as u64;
    let mut rng = StdRng::seed_from_u64(seed);

    // Load and filter subscriber database for this worker's subscriber range
    let subscriber_db = if let Some(db_path) = subscriber_db_path {
        let (start_u, end_u) = users_range;

        // Load events from start of history to end of generation day
        let day_end_ts = (day + Duration::days(1)).timestamp_millis();

        // Memory-efficient loading: filter during read, not after
        let mut filtered_db = if db_path.extension().and_then(|s| s.to_str()) == Some("arrow") {
            SubscriberDatabase::load_from_arrow_with_msisdn_filter(
                db_path,
                0,
                day_end_ts,
                start_u,
                end_u,
                &cfg.prefixes,
            )?
        } else {
            // CSV fallback: still need to load all then filter
            let full_db = SubscriberDatabase::load_from_csv(db_path)?;
            full_db.filter_by_msisdn_range(start_u, end_u, &cfg.prefixes)
        };

        // Build snapshots for fast lookup
        filtered_db.build_snapshots();

        Some(filtered_db)
    } else {
        None
    };

    let tz = tz_from_name(&cfg.tz_name);
    // Convert to 'static str for zero-copy EventRow usage
    let tz_name: &'static str = Box::leak(cfg.tz_name.clone().into_boxed_str());

    // Build contacts & subscribers for this shard
    let (start_u, end_u) = users_range;
    let shard_pop = end_u - start_u;

    // Pre-allocate with exact capacity to avoid reallocations
    let contacts = build_contacts(shard_pop, 30, &mut rng);

    // Use subscriber database if provided, otherwise generate random subscribers
    let subs = if let Some(ref db) = subscriber_db {
        // Pre-allocate subscribers array
        let mut subscribers = vec![Subscriber {
            msisdn: 0,
            imsi: 0,
            mccmnc: 0,
            imei: 0,
        }; shard_pop];

        // Fill from database snapshots
        let day_start_ts = day.timestamp_millis();

        for uidx in 0..shard_pop {
            let sub_idx = start_u + uidx;

            // Generate MSISDN for this subscriber
            let prefix = &cfg.prefixes[sub_idx % cfg.prefixes.len()];
            let number = sub_idx % 10_000_000;
            let msisdn_str = format!("{}{:07}", prefix, number);

            // Get snapshot from database
            if let Some(snapshot) = db.get_snapshot_by_msisdn(&msisdn_str, day_start_ts) {
                subscribers[uidx] = Subscriber {
                    msisdn: snapshot.msisdn.parse::<u64>().unwrap_or(0),
                    imsi: snapshot.imsi.parse::<u64>().unwrap_or(0),
                    imei: snapshot.imei.parse::<u64>().unwrap_or(0),
                    mccmnc: snapshot.mccmnc.parse::<u32>().unwrap_or(0),
                };
            }
        }

        subscribers
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

    // Initialize event pool for zero-allocation event generation
    let mut event_pool = EventPool::new(cfg.event_pool_size);

    // Initialize batch for async writing
    let batch_capacity = cfg.batch_size_bytes / 230; // ~230 bytes per event
    let mut batch = EventBatch::new(batch_capacity);

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

    // Parse prefixes to u64 for numeric operations
    let numeric_prefixes: Vec<u64> = cfg.prefixes
        .iter()
        .map(|s| s.parse().unwrap_or(31612))
        .collect();

    for uidx in 0..shard_pop {
        // Get subscriber info from pre-loaded array
        let mut sub = subs[uidx];

        // Skip if subscriber has no data (msisdn == 0)
        if sub.msisdn == 0 {
            continue;
        }

        // Occasional IMEI change (new device) - only for non-DB mode
        if subscriber_db.is_none() && rng.gen::<f64>() < cfg.imei_daily_change_prob {
            sub.imei = gen_imei(&mut rng);
        }

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

            // Pick counterpart MSISDN (u64) and track if they're in our database
            let (other_msisdn, other_sub_opt): (u64, Option<&Subscriber>) = if let Some(ref dist) = contact_dist {
                let other_idx = c_pool[dist.sample(&mut rng)] % subs.len();
                let other_sub = &subs[other_idx];
                (other_sub.msisdn, Some(other_sub))
            } else {
                // Generate random MSISDN (not in our database)
                let prefix_idx = rng.gen_range(0..numeric_prefixes.len());
                let prefix = numeric_prefixes[prefix_idx];
                let subscriber_number = rng.gen_range(0..10_000_000u64);
                (prefix * 10_000_000 + subscriber_number, None)
            };

            let cell_id = rng.gen_range(10_000..100_000);

            // Generate MO (Mobile Originated) record for current subscriber
            let mo_event = event_pool.acquire();
            call_gen.generate_forced_direction(mo_event, &sub, start_local, other_msisdn, tz_name, cell_id, &mut rng, "MO");

            // Add MO record to batch
            batch.push(mo_event.clone());
            stats.calls += 1;

            // Send batch if full
            if batch.is_full(cfg.batch_size_bytes) {
                writer_tx.send(WriterMessage::Batch(batch))?;
                batch = EventBatch::new(batch_capacity);
            }

            // If other party is in our database, generate correlated MT (Mobile Terminated) record
            if let Some(other_sub) = other_sub_opt {
                // Skip if other subscriber has no data
                if other_sub.msisdn == 0 {
                    continue;
                }

                // Save call parameters from MO event for MT correlation (before borrowing event_pool again)
                let start_ts = mo_event.start_ts_ms;
                let end_ts = mo_event.end_ts_ms;
                let tz_offset = mo_event.tz_offset_min;
                let duration = mo_event.duration_sec;
                let cause = mo_event.cause_for_record_closing;

                // Generate MT record with same call parameters (time, duration, disposition)
                let mt_event = event_pool.acquire();

                // Copy call parameters from MO event for correlation
                mt_event.event_type = "CALL";
                mt_event.msisdn_src = other_msisdn;
                mt_event.msisdn_dst = sub.msisdn;
                mt_event.direction = "MT";
                mt_event.start_ts_ms = start_ts;
                mt_event.end_ts_ms = end_ts;
                mt_event.tz_name = tz_name;
                mt_event.tz_offset_min = tz_offset;
                mt_event.duration_sec = duration;
                mt_event.mccmnc = other_sub.mccmnc;
                mt_event.imsi = other_sub.imsi;
                mt_event.imei = other_sub.imei;
                mt_event.cell_id = cell_id;
                mt_event.record_type = "mscVoiceRecord";
                mt_event.cause_for_record_closing = cause;

                // Add MT record to batch
                batch.push(mt_event.clone());
                stats.calls += 1;

                // Send batch if full
                if batch.is_full(cfg.batch_size_bytes) {
                    writer_tx.send(WriterMessage::Batch(batch))?;
                    batch = EventBatch::new(batch_capacity);
                }
            }
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

            // Acquire event from pool and populate it
            let event = event_pool.acquire();
            sms_gen.generate(event, &sub, start_local, other_msisdn, tz_name, cell_id, &mut rng);

            // Add to batch (clone because batch needs ownership)
            batch.push(event.clone());
            stats.sms += 1;

            // Send batch if full
            if batch.is_full(cfg.batch_size_bytes) {
                writer_tx.send(WriterMessage::Batch(batch))?;
                batch = EventBatch::new(batch_capacity);
            }
        }

        // Generate DATA sessions
        for _ in 0..n_data {
            let start_local = sample_time(&mut rng);

            // TODO: Support subscriber database updates for DATA
            if subscriber_db.is_some() {
                // Skip for now when using subscriber database
                continue;
            }

            // Acquire event from pool and populate it
            let event = event_pool.acquire();
            data_gen.generate(event, &sub, start_local, tz_name, &mut rng);

            // Add to batch (clone because batch needs ownership)
            batch.push(event.clone());
            stats.data += 1;

            // Send batch if full
            if batch.is_full(cfg.batch_size_bytes) {
                writer_tx.send(WriterMessage::Batch(batch))?;
                batch = EventBatch::new(batch_capacity);
            }
        }
    }

    // Send remaining events in batch
    if !batch.is_empty() {
        writer_tx.send(WriterMessage::Batch(batch))?;
    }

    // No need to send Close here - main.rs will handle that after all workers complete

    // Write stats
    let stat_path = out_dir
        .join(&day_str)
        .join(format!("stats_shard{:03}.json", shard_id));
    let stats_json = serde_json::to_string_pretty(&stats)?;
    std::fs::write(stat_path, stats_json)?;

    Ok(())
}

/// Worker process with redb-based chunked processing for memory efficiency
/// This version loads subscribers in small chunks to minimize memory usage
fn worker_generate_redb_chunked(
    day: DateTime<chrono_tz::Tz>,
    shard_id: usize,
    users_range: (usize, usize),
    cfg: &Config,
    out_dir: &Path,
    redb_path: &Path,
    writer_tx: Sender<WriterMessage>,
) -> anyhow::Result<()> {
    use chrono::Duration;

    let seed = (cfg.workers as u64).wrapping_mul(1000) + shard_id as u64;
    let mut rng = StdRng::seed_from_u64(seed);

    // Open redb database
    let redb = SubscriberDbRedb::open(redb_path)?;

    let tz = tz_from_name(&cfg.tz_name);
    let tz_name: &'static str = Box::leak(cfg.tz_name.clone().into_boxed_str());

    // Initialize generators
    let call_gen = CallGenerator::new(cfg);
    let sms_gen = SmsGenerator::new(cfg);
    let data_gen = DataGenerator::new(HashMap::new(), vec![]);

    let day_str = day.format("%Y-%m-%d").to_string();

    // Initialize event pool
    let mut event_pool = EventPool::new(cfg.event_pool_size);

    // Initialize batch
    let batch_capacity = cfg.batch_size_bytes / 230;
    let mut batch = EventBatch::new(batch_capacity);

    let day_start_local = tz
        .with_ymd_and_hms(day.year(), day.month(), day.day(), 0, 0, 0)
        .unwrap();

    let day_start_ts = day.timestamp_millis();

    let mut stats = ShardStats {
        shard: shard_id,
        calls: 0,
        sms: 0,
        data: 0,
    };

    // Event counts per user
    let avg_calls = cfg.avg_calls_per_user;
    let avg_sms = cfg.avg_sms_per_user;
    let avg_data = cfg.avg_data_sessions_per_user;

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

    // Parse prefixes to u64 for numeric operations
    let numeric_prefixes: Vec<u64> = cfg.prefixes
        .iter()
        .map(|s| s.parse().unwrap_or(31612))
        .collect();

    // Calculate total subscriber range for this worker
    let (start_u, end_u) = users_range;
    let total_subs = end_u - start_u;

    // Calculate MSISDN range for this worker
    let start_msisdn_idx = start_u;
    let end_msisdn_idx = end_u;

    // Process subscribers in chunks
    let chunk_size = cfg.chunk_size;
    for chunk_start_idx in (0..total_subs).step_by(chunk_size) {
        let chunk_end_idx = (chunk_start_idx + chunk_size).min(total_subs);

        // Load chunk from redb
        // For each subscriber in chunk, generate MSISDN and lookup in redb
        let mut chunk_subs = Vec::with_capacity((chunk_end_idx - chunk_start_idx) as usize);

        for sub_idx in (start_msisdn_idx + chunk_start_idx)..(start_msisdn_idx + chunk_end_idx) {
            // Generate MSISDN for this subscriber
            let prefix = &cfg.prefixes[sub_idx % cfg.prefixes.len()];
            let number = sub_idx % 10_000_000;
            let msisdn = format!("{}{:07}", prefix, number).parse::<u64>().unwrap_or(0);

            // Look up subscriber in redb
            if let Some(snapshot) = redb.get_subscriber_at(msisdn, day_start_ts)? {
                chunk_subs.push(Subscriber {
                    msisdn: snapshot.msisdn,
                    imsi: snapshot.imsi,
                    imei: snapshot.imei,
                    mccmnc: snapshot.mccmnc,
                });
            }
        }

        // Generate events for this chunk
        for sub in &chunk_subs {
            if sub.msisdn == 0 {
                continue;
            }

            // Sample event counts for this user
            let n_calls = sample_poisson(avg_calls, &mut rng);
            let n_sms = sample_poisson(avg_sms, &mut rng);
            let n_data = sample_poisson(avg_data, &mut rng);

            // Generate CALL events
            for _ in 0..n_calls {
                let start_local = sample_time(&mut rng);

                // Generate random contact MSISDN (either from our database or external)
                let other_msisdn: u64 = if rng.gen::<f64>() < 0.7 {
                    // Generate from our subscriber range (may or may not be in DB)
                    let random_idx = rng.gen_range(start_msisdn_idx..end_msisdn_idx);
                    let prefix = &cfg.prefixes[random_idx % cfg.prefixes.len()];
                    let number = random_idx % 10_000_000;
                    format!("{}{:07}", prefix, number).parse().unwrap_or(0)
                } else {
                    // Generate external number
                    let prefix_idx = rng.gen_range(0..numeric_prefixes.len());
                    let prefix = numeric_prefixes[prefix_idx];
                    let subscriber_number = rng.gen_range(0..10_000_000u64);
                    prefix * 10_000_000 + subscriber_number
                };

                let cell_id = rng.gen_range(10_000..100_000);

                // Generate MO record
                let mo_event = event_pool.acquire();
                call_gen.generate_forced_direction(
                    mo_event,
                    sub,
                    start_local,
                    other_msisdn,
                    tz_name,
                    cell_id,
                    &mut rng,
                    "MO",
                );

                batch.push(mo_event.clone());
                stats.calls += 1;

                if batch.is_full(cfg.batch_size_bytes) {
                    writer_tx.send(WriterMessage::Batch(batch))?;
                    batch = EventBatch::new(batch_capacity);
                }

                // Check if other party is in database for MT generation
                if let Some(other_snapshot) = redb.get_subscriber_at(other_msisdn, day_start_ts)? {
                    if other_snapshot.msisdn == 0 {
                        continue;
                    }

                    // Save parameters for MT correlation
                    let start_ts = mo_event.start_ts_ms;
                    let end_ts = mo_event.end_ts_ms;
                    let tz_offset = mo_event.tz_offset_min;
                    let duration = mo_event.duration_sec;
                    let cause = mo_event.cause_for_record_closing;

                    // Generate correlated MT record
                    let mt_event = event_pool.acquire();
                    mt_event.event_type = "CALL";
                    mt_event.msisdn_src = other_msisdn;
                    mt_event.msisdn_dst = sub.msisdn;
                    mt_event.direction = "MT";
                    mt_event.start_ts_ms = start_ts;
                    mt_event.end_ts_ms = end_ts;
                    mt_event.tz_name = tz_name;
                    mt_event.tz_offset_min = tz_offset;
                    mt_event.duration_sec = duration;
                    mt_event.mccmnc = other_snapshot.mccmnc;
                    mt_event.imsi = other_snapshot.imsi;
                    mt_event.imei = other_snapshot.imei;
                    mt_event.cell_id = cell_id;
                    mt_event.record_type = "mscVoiceRecord";
                    mt_event.cause_for_record_closing = cause;

                    batch.push(mt_event.clone());
                    stats.calls += 1;

                    if batch.is_full(cfg.batch_size_bytes) {
                        writer_tx.send(WriterMessage::Batch(batch))?;
                        batch = EventBatch::new(batch_capacity);
                    }
                }
            }

            // Generate SMS events
            for _ in 0..n_sms {
                let start_local = sample_time(&mut rng);

                // Generate random contact MSISDN
                let other_msisdn: u64 = if rng.gen::<f64>() < 0.7 {
                    let random_idx = rng.gen_range(start_msisdn_idx..end_msisdn_idx);
                    let prefix = &cfg.prefixes[random_idx % cfg.prefixes.len()];
                    let number = random_idx % 10_000_000;
                    format!("{}{:07}", prefix, number).parse().unwrap_or(0)
                } else {
                    let prefix_idx = rng.gen_range(0..numeric_prefixes.len());
                    let prefix = numeric_prefixes[prefix_idx];
                    let subscriber_number = rng.gen_range(0..10_000_000u64);
                    prefix * 10_000_000 + subscriber_number
                };

                let cell_id = rng.gen_range(10_000..100_000);

                let event = event_pool.acquire();
                sms_gen.generate(event, sub, start_local, other_msisdn, tz_name, cell_id, &mut rng);

                batch.push(event.clone());
                stats.sms += 1;

                if batch.is_full(cfg.batch_size_bytes) {
                    writer_tx.send(WriterMessage::Batch(batch))?;
                    batch = EventBatch::new(batch_capacity);
                }
            }

            // Generate DATA events
            for _ in 0..n_data {
                let start_local = sample_time(&mut rng);

                let event = event_pool.acquire();
                data_gen.generate(event, sub, start_local, tz_name, &mut rng);

                batch.push(event.clone());
                stats.data += 1;

                if batch.is_full(cfg.batch_size_bytes) {
                    writer_tx.send(WriterMessage::Batch(batch))?;
                    batch = EventBatch::new(batch_capacity);
                }
            }
        }

        // Chunk is dropped here, memory released
    }

    // Send remaining batch
    if !batch.is_empty() {
        writer_tx.send(WriterMessage::Batch(batch))?;
    }

    // Write stats
    let stat_path = out_dir
        .join(&day_str)
        .join(format!("stats_shard{:03}.json", shard_id));
    let stats_json = serde_json::to_string_pretty(&stats)?;
    std::fs::write(stat_path, stats_json)?;

    Ok(())
}
