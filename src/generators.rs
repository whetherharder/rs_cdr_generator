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

/// Отдаёт serving_ne_id для АБОНЕНТА (не для соты и не для события).
///
/// В питоне serving_ne_id — поле модели абонента (assets/models.py:38-47),
/// присваивается один раз при генерации состава через tac_to_ne, и все
/// события абонента идут через его элемент. В rust нет топологии (TAC),
/// заводить её в эту порцию не должны — поэтому распределяем абонентов
/// по списку network_elements детерминированным хешем их MSISDN, а не
/// от соты события: тот же абонент при том же конфиге ВСЕГДА попадает
/// в один и тот же NE, независимо от того, в какой соте его застало
/// конкретное событие. Это и есть содержательный инвариант, который важен
/// для партиций (docs/field-mapping.md, «serving_ne_id»).
///
/// Сознательно не персистентное поле redb-базы абонентов (в отличие от
/// питона): функция чистая от msisdn, поэтому переигрывается одинаково
/// при каждом запуске без миграции схемы базы. Наблюдаемое поведение то
/// же самое — абонент стабильно закреплён за одним NE.
fn assign_ne_id(msisdn: u64, network_elements: &[String]) -> String {
    if network_elements.is_empty() {
        return "unknown-ne".to_string();
    }
    let idx = (msisdn as usize) % network_elements.len();
    network_elements[idx].clone()
}

/// event_type("CALL"/"SMS"/"DATA") + direction("MO"/"MT") + для DATA — сторона
/// пары (sgw/pgw) → record_type CDR_FIELDS питоновского эталона
/// (mo_call/mt_call/mo_sms/mt_sms/sgw_data/pgw_data). Rust не порождает
/// SGW+PGW пару на одну data-сессию (models/cdr.py, data.py:
/// _create_sgw_pgw_pair) — эмитим один pgw_data-эквивалент, это тоже
/// зафиксировано как упрощение этапа 1 в docs/field-mapping.md.
fn cdr_record_type(event_type: &str, direction: &str) -> &'static str {
    match (event_type, direction) {
        ("CALL", "MO") => "mo_call",
        ("CALL", _) => "mt_call",
        ("SMS", "MO") => "mo_sms",
        ("SMS", _) => "mt_sms",
        ("DATA", _) => "pgw_data",
        _ => "",
    }
}

/// ISO-8601 с миллисекундами и суффиксом Z — тот же формат, что питоновский
/// _fmt_dt в models/cdr.py (isoformat(timespec="milliseconds") + "Z").
fn fmt_ts_ms(epoch_ms: i64) -> String {
    use chrono::TimeZone;
    let dt = chrono::Utc.timestamp_millis_opt(epoch_ms).single().expect("epoch_ms всегда валиден");
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

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

/// Optimized sampler for event counts (Poisson approximation) (OPTIMIZATION #4)
pub struct EventCountSampler {
    pub mean: f64,
    pub normal_dist: Option<Normal<f64>>,  // For mean >= 30, use Normal approximation
    pub exp_lambda: f64,  // For mean < 30, use exact Poisson
}

impl EventCountSampler {
    pub fn new(mean: f64) -> Self {
        if mean >= 30.0 {
            EventCountSampler {
                mean,
                normal_dist: Some(Normal::new(mean, mean.sqrt()).unwrap()),
                exp_lambda: 0.0,
            }
        } else {
            EventCountSampler {
                mean,
                normal_dist: None,
                exp_lambda: (-mean).exp(),
            }
        }
    }

    pub fn sample(&self, rng: &mut StdRng) -> usize {
        if self.mean <= 0.0 {
            return 0;
        }
        if let Some(ref normal) = self.normal_dist {
            normal.sample(rng).max(0.0) as usize
        } else {
            // Exact Poisson for small mean
            let mut k: usize = 0;
            let mut p = 1.0;
            while p > self.exp_lambda {
                k += 1;
                p *= rng.gen::<f64>();
            }
            k.saturating_sub(1)
        }
    }
}

/// Sample from Poisson distribution (legacy function, kept for compatibility)
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
    duration_dist: LogNormal<f64>,  // Pre-computed distribution (OPTIMIZATION #4)
    network_elements: Vec<String>,
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

        // Pre-compute LogNormal distribution (OPTIMIZATION #4)
        let duration_dist = LogNormal::new(mu, sigma).unwrap();

        CallGenerator {
            p_mo,
            dispo_pop,
            dispo_dist,
            mu,
            sigma,
            duration_dist,
            network_elements: cfg.network_elements.clone(),
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
                // Use pre-computed distribution (OPTIMIZATION #4)
                let dur = self.duration_dist.sample(rng).max(1.0) as i64;
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
        let ne_id = assign_ne_id(sub.msisdn, &self.network_elements);

        event.record_type = cdr_record_type("CALL", direction).to_string();
        event.served_imsi = sub.imsi.to_string();
        event.served_msisdn = sub.msisdn.to_string();
        event.served_imei = sub.imei.to_string();
        event.calling_number = msisdn_src.to_string();
        event.called_number = msisdn_dst.to_string();
        event.event_timestamp = fmt_ts_ms(to_epoch_ms(&start_local.with_timezone(&chrono::Utc)));
        event.release_timestamp = fmt_ts_ms(to_epoch_ms(&end_local.with_timezone(&chrono::Utc)));
        event.duration_seconds = dur_sec.to_string();
        event.first_cell_id = cell_id.to_string();
        event.last_cell_id = cell_id.to_string();
        event.serving_ne_id = ne_id;
        // cause_for_termination: у питона это числовой код (voice.py normal_termination_causes),
        // в rust — только текстовая причина (cause_for_record_closing); числовой код не заводим,
        // чтобы не изобретать соответствие — оставляем пустым (docs/field-mapping.md).
        let _ = cause;
        // Остальные поля (answer_timestamp, apn, qci, rat_type, vendor_extensions,
        // sequence_number, consolidation_id, charging_id, record_opening/closure_time,
        // uplink/downlink_volume_bytes) — пустые (сброшены пулом), см. docs/field-mapping.md.
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
                // Use pre-computed distribution (OPTIMIZATION #4)
                let dur = self.duration_dist.sample(rng).max(1.0) as i64;
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
        let ne_id = assign_ne_id(sub.msisdn, &self.network_elements);

        event.record_type = cdr_record_type("CALL", direction).to_string();
        event.served_imsi = sub.imsi.to_string();
        event.served_msisdn = sub.msisdn.to_string();
        event.served_imei = sub.imei.to_string();
        event.calling_number = msisdn_src.to_string();
        event.called_number = msisdn_dst.to_string();
        event.event_timestamp = fmt_ts_ms(to_epoch_ms(&start_local.with_timezone(&chrono::Utc)));
        event.release_timestamp = fmt_ts_ms(to_epoch_ms(&end_local.with_timezone(&chrono::Utc)));
        event.duration_seconds = dur_sec.to_string();
        event.first_cell_id = cell_id.to_string();
        event.last_cell_id = cell_id.to_string();
        event.serving_ne_id = ne_id;
        let _ = cause; // числовой cause_for_termination не заводим — docs/field-mapping.md
    }
}

/// Generate SMS events
pub struct SmsGenerator {
    p_mo: f64,
    status_dist: WeightedIndex<f64>,
    segments_dist: WeightedIndex<f64>,
    network_elements: Vec<String>,
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
            network_elements: cfg.network_elements.clone(),
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

        let ne_id = assign_ne_id(sub.msisdn, &self.network_elements);
        let _ = record_type; // используем cdr_record_type ниже — своё имя записи rust не совпадает с питоном
        event.record_type = cdr_record_type("SMS", direction).to_string();
        event.served_imsi = sub.imsi.to_string();
        event.served_msisdn = sub.msisdn.to_string();
        event.served_imei = sub.imei.to_string();
        event.calling_number = msisdn_src.to_string();
        event.called_number = msisdn_dst.to_string();
        event.event_timestamp = fmt_ts_ms(to_epoch_ms(&start_local.with_timezone(&chrono::Utc)));
        event.duration_seconds = dur.to_string();
        event.first_cell_id = cell_id.to_string();
        event.last_cell_id = cell_id.to_string();
        event.serving_ne_id = ne_id;
        let _ = (cause, sms_segments, sms_status); // деталей SMS в CDR_FIELDS нет — docs/field-mapping.md
    }
}

/// Generate DATA session events
pub struct DataGenerator {
    cells_by_rat: HashMap<String, Vec<u32>>,
    cells_all: Vec<u32>,
    rat_dist: WeightedIndex<f64>,
    apn_dist: WeightedIndex<f64>,
    network_elements: Vec<String>,
}

impl DataGenerator {
    pub fn new(cells_by_rat: HashMap<String, Vec<u32>>, cells_all: Vec<u32>, network_elements: Vec<String>) -> Self {
        let rat_weights = [0.3, 0.5, 0.2];
        let rat_dist = WeightedIndex::new(&rat_weights).unwrap();

        let apn_weights = [0.8, 0.1, 0.1];
        let apn_dist = WeightedIndex::new(&apn_weights).unwrap();

        DataGenerator {
            cells_by_rat,
            cells_all,
            rat_dist,
            apn_dist,
            network_elements,
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

        let _ = tz_name;
        let ne_id = assign_ne_id(sub.msisdn, &self.network_elements);
        let event_ts = fmt_ts_ms(to_epoch_ms(&start_local.with_timezone(&chrono::Utc)));
        let closure_ts = fmt_ts_ms(to_epoch_ms(&end_local.with_timezone(&chrono::Utc)));

        // Rust не порождает пару SGW+PGW на сессию (в отличие от data.py,
        // _create_sgw_pgw_pair) — один record_type "pgw_data" на сессию,
        // объёмы полностью на этой стороне (docs/field-mapping.md).
        event.record_type = cdr_record_type("DATA", "MO").to_string();
        event.served_imsi = sub.imsi.to_string();
        event.served_msisdn = sub.msisdn.to_string();
        event.served_imei = sub.imei.to_string();
        event.calling_number = sub.msisdn.to_string();
        event.event_timestamp = event_ts.clone();
        event.duration_seconds = dur.to_string();
        event.first_cell_id = cell_id.to_string();
        event.last_cell_id = cell_id.to_string();
        event.serving_ne_id = ne_id;
        event.record_opening_time = event_ts;
        event.record_closure_time = closure_ts;
        // uplink = данные ОТ абонента (up), downlink = данные К абоненту (down) —
        // как volume_uplink/volume_downlink питоновского generators/data.py.
        event.uplink_volume_bytes = up.to_string();
        event.downlink_volume_bytes = down.to_string();
        event.apn = apn.to_string();
        event.rat_type = rat.to_string();
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
    redb: Option<&std::sync::Arc<SubscriberDbRedb>>,
    writer_tx: Sender<WriterMessage>,
) -> anyhow::Result<()> {
    // If redb database is provided, use chunked processing for memory efficiency
    if let Some(redb_arc) = redb {
        return worker_generate_redb_chunked(
            day,
            shard_id,
            users_range,
            cfg,
            out_dir,
            redb_arc.clone(),
            writer_tx,
        );
    }

    use chrono::Duration;

    let seed = (cfg.workers as u64).wrapping_mul(1000) + shard_id as u64;
    let mut rng = StdRng::seed_from_u64(seed);

    // Load and filter subscriber database for this worker's subscriber range (CSV format only)
    let subscriber_db = if let Some(db_path) = subscriber_db_path {
        let (start_u, end_u) = users_range;

        // CSV loading: load all then filter
        let full_db = SubscriberDatabase::load_from_csv(db_path)?;
        let mut filtered_db = full_db.filter_by_msisdn_range(start_u, end_u, &cfg.prefixes);

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

    // Pre-compute event count samplers (OPTIMIZATION #4)
    let calls_sampler = EventCountSampler::new(avg_calls);
    let sms_sampler = EventCountSampler::new(avg_sms);
    let data_sampler = EventCountSampler::new(avg_data);

    // Initialize generators
    let call_gen = CallGenerator::new(cfg);
    let sms_gen = SmsGenerator::new(cfg);
    let data_gen = DataGenerator::new(HashMap::new(), vec![], cfg.network_elements.clone());

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

        // Use pre-computed contact distribution (OPTIMIZATION #2)
        let contact_dist = c.dist.as_ref();

        // Sample event counts for this user (OPTIMIZATION #4)
        let n_calls = calls_sampler.sample(&mut rng);
        let n_sms = sms_sampler.sample(&mut rng);
        let n_data = data_sampler.sample(&mut rng);

        // Generate CALL events
        for _ in 0..n_calls {
            let start_local = sample_time(&mut rng);

            // Pick counterpart MSISDN (u64) and track if they're in our database
            let (other_msisdn, other_sub_opt): (u64, Option<&Subscriber>) = if let Some(dist) = contact_dist {
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
                let event_timestamp = mo_event.event_timestamp.clone();
                let release_timestamp = mo_event.release_timestamp.clone();
                let duration_seconds = mo_event.duration_seconds.clone();

                // Generate MT record with same call parameters (time, duration, disposition)
                let mt_event = event_pool.acquire();

                // Copy call parameters from MO event for correlation
                mt_event.record_type = cdr_record_type("CALL", "MT").to_string();
                mt_event.served_imsi = other_sub.imsi.to_string();
                mt_event.served_msisdn = other_sub.msisdn.to_string();
                mt_event.served_imei = other_sub.imei.to_string();
                mt_event.calling_number = other_msisdn.to_string();
                mt_event.called_number = sub.msisdn.to_string();
                mt_event.event_timestamp = event_timestamp;
                mt_event.release_timestamp = release_timestamp;
                mt_event.duration_seconds = duration_seconds;
                mt_event.first_cell_id = cell_id.to_string();
                mt_event.last_cell_id = cell_id.to_string();
                mt_event.serving_ne_id = assign_ne_id(other_sub.msisdn, &cfg.network_elements);

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
            let other_msisdn: u64 = if let Some(dist) = contact_dist {
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
    redb: std::sync::Arc<SubscriberDbRedb>,
    writer_tx: Sender<WriterMessage>,
) -> anyhow::Result<()> {
    use chrono::Duration;

    let seed = (cfg.workers as u64).wrapping_mul(1000) + shard_id as u64;
    let mut rng = StdRng::seed_from_u64(seed);

    let tz = tz_from_name(&cfg.tz_name);
    let tz_name: &'static str = Box::leak(cfg.tz_name.clone().into_boxed_str());

    // Initialize generators
    let call_gen = CallGenerator::new(cfg);
    let sms_gen = SmsGenerator::new(cfg);
    let data_gen = DataGenerator::new(HashMap::new(), vec![], cfg.network_elements.clone());

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

    // Pre-compute event count samplers (OPTIMIZATION #4)
    let calls_sampler = EventCountSampler::new(avg_calls);
    let sms_sampler = EventCountSampler::new(avg_sms);
    let data_sampler = EventCountSampler::new(avg_data);

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

        // Calculate MSISDN range for this chunk
        let chunk_start_sub = start_msisdn_idx + chunk_start_idx;
        let chunk_end_sub = start_msisdn_idx + chunk_end_idx;

        // Calculate min and max MSISDN for efficient range query
        let mut min_msisdn = u64::MAX;
        let mut max_msisdn = 0u64;

        for sub_idx in chunk_start_sub..chunk_end_sub {
            let prefix_idx = sub_idx % cfg.prefixes.len();
            let prefix = numeric_prefixes[prefix_idx];
            let number = (sub_idx % 10_000_000) as u64;
            let msisdn = prefix * 10_000_000 + number;
            min_msisdn = min_msisdn.min(msisdn);
            max_msisdn = max_msisdn.max(msisdn);
        }

        // Load chunk from redb in one transaction (OPTIMIZATION #1)
        let chunk_data = redb.load_chunk(min_msisdn, max_msisdn + 1)?;

        // Build HashMap for O(1) lookup (OPTIMIZATION #1)
        let snapshot_cache: HashMap<u64, Vec<crate::subscriber_db_redb::SubscriberSnapshotNumeric>> =
            chunk_data.into_iter().collect();

        // Build subscriber list for this chunk using cache
        let mut chunk_subs = Vec::with_capacity((chunk_end_idx - chunk_start_idx) as usize);

        for sub_idx in chunk_start_sub..chunk_end_sub {
            // Generate MSISDN using arithmetic (OPTIMIZATION #3 - partial)
            let prefix_idx = sub_idx % cfg.prefixes.len();
            let prefix = numeric_prefixes[prefix_idx];
            let number = (sub_idx % 10_000_000) as u64;
            let msisdn = prefix * 10_000_000 + number;

            // Look up subscriber in cache (OPTIMIZATION #1)
            if let Some(snapshots) = snapshot_cache.get(&msisdn) {
                if let Some(snapshot) = crate::subscriber_db_redb::SubscriberDbRedb::find_snapshot_at(snapshots, day_start_ts) {
                    chunk_subs.push(Subscriber {
                        msisdn: snapshot.msisdn,
                        imsi: snapshot.imsi,
                        imei: snapshot.imei,
                        mccmnc: snapshot.mccmnc,
                    });
                }
            }
        }

        // Generate events for this chunk
        for sub in &chunk_subs {
            if sub.msisdn == 0 {
                continue;
            }

            // Sample event counts for this user (OPTIMIZATION #4)
            let n_calls = calls_sampler.sample(&mut rng);
            let n_sms = sms_sampler.sample(&mut rng);
            let n_data = data_sampler.sample(&mut rng);

            // Generate CALL events
            for _ in 0..n_calls {
                let start_local = sample_time(&mut rng);

                // Generate random contact MSISDN using arithmetic (OPTIMIZATION #3)
                let other_msisdn: u64 = if rng.gen::<f64>() < 0.7 {
                    // Generate from our subscriber range (may or may not be in DB)
                    let random_idx = rng.gen_range(start_msisdn_idx..end_msisdn_idx);
                    let prefix_idx = random_idx % cfg.prefixes.len();
                    let prefix = numeric_prefixes[prefix_idx];
                    let number = (random_idx % 10_000_000) as u64;
                    prefix * 10_000_000 + number
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
                // First check cache, fallback to DB for out-of-chunk MSISDNs (OPTIMIZATION #1)
                let other_snapshot_opt = if let Some(snapshots) = snapshot_cache.get(&other_msisdn) {
                    crate::subscriber_db_redb::SubscriberDbRedb::find_snapshot_at(snapshots, day_start_ts).cloned()
                } else {
                    // Fallback: MSISDN is outside current chunk, use DB lookup
                    redb.get_subscriber_at(other_msisdn, day_start_ts)?
                };

                if let Some(ref other_snapshot) = other_snapshot_opt {
                    if other_snapshot.msisdn == 0 {
                        continue;
                    }

                    // Save parameters for MT correlation
                    let event_timestamp = mo_event.event_timestamp.clone();
                    let release_timestamp = mo_event.release_timestamp.clone();
                    let duration_seconds = mo_event.duration_seconds.clone();

                    // Generate correlated MT record
                    let mt_event = event_pool.acquire();
                    mt_event.record_type = cdr_record_type("CALL", "MT").to_string();
                    mt_event.served_imsi = other_snapshot.imsi.to_string();
                    mt_event.served_msisdn = other_snapshot.msisdn.to_string();
                    mt_event.served_imei = other_snapshot.imei.to_string();
                    mt_event.calling_number = other_msisdn.to_string();
                    mt_event.called_number = sub.msisdn.to_string();
                    mt_event.event_timestamp = event_timestamp;
                    mt_event.release_timestamp = release_timestamp;
                    mt_event.duration_seconds = duration_seconds;
                    mt_event.first_cell_id = cell_id.to_string();
                    mt_event.last_cell_id = cell_id.to_string();
                    mt_event.serving_ne_id = assign_ne_id(other_snapshot.msisdn, &cfg.network_elements);

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

                // Generate random contact MSISDN using arithmetic (OPTIMIZATION #3)
                let other_msisdn: u64 = if rng.gen::<f64>() < 0.7 {
                    let random_idx = rng.gen_range(start_msisdn_idx..end_msisdn_idx);
                    let prefix_idx = random_idx % cfg.prefixes.len();
                    let prefix = numeric_prefixes[prefix_idx];
                    let number = (random_idx % 10_000_000) as u64;
                    prefix * 10_000_000 + number
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
