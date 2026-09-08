// Event generation logic for CALL, SMS, and DATA events
use crate::async_writer::{EventBatch, WriterMessage};
use crate::config::Config;
use crate::contact_book::{ContactBook, CONTACT_THRESHOLD, EXTERNAL_CALL_RATIO};
// Счётчики тира выбора собеседника (внешний/книга/пусто-книга-fallback/
// случайный) — печатаются при `CB_DEBUG=1` (main.rs) для проверки, что
// реальные пропорции соответствуют CONTACT_THRESHOLD/EXTERNAL_CALL_RATIO
// на конкретном прогоне, а не только по формуле. Relaxed — счётчик
// диагностический, порядок операций между воркерами не важен.
pub static DBG_EXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static DBG_BOOK: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static DBG_FALLBACK: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static DBG_RAND: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
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
use std::sync::Arc;

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

/// event_type("CALL"/"SMS"/"DATA") + direction("MO"/"MT") → record_type
/// CDR_FIELDS питоновского эталона (mo_call/mt_call/mo_sms/mt_sms). DATA
/// парой (sgw_data/pgw_data) генерирует DataGenerator::generate_pair —
/// см. его докстринг, здесь для DATA не вызывается.
fn cdr_record_type(event_type: &str, direction: &str) -> &'static str {
    match (event_type, direction) {
        ("CALL", "MO") => "mo_call",
        ("CALL", _) => "mt_call",
        ("SMS", "MO") => "mo_sms",
        ("SMS", _) => "mt_sms",
        _ => "",
    }
}

/// 32-hex-символьный случайный идентификатор — как у питона
/// (`voice.py`/`sms.py`, `bytes(rng.integers(0, 256, size=16)).hex()`):
/// это не смоделированные данные, а просто случайная метка корреляции
/// MO/MT одной попытки вызова/SMS, поэтому воспроизвести её как случайную
/// строку — не значит «придумать значение» (docs/field-mapping.md).
fn gen_correlation_id(rng: &mut StdRng) -> String {
    let bytes: [u8; 16] = rng.gen();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
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
    /// This allows explicit MO or MT record generation.
    ///
    /// Возвращает `true`, если сэмплированная диспозиция — `ANSWERED`
    /// (вызов состоялся). Вызывающий код обязан использовать это значение,
    /// чтобы решить, порождать ли MT-запись: у питона несостоявшийся вызов
    /// (`success_rate`) даёт только MO-запись без пары (`voice.py`,
    /// `_generate_failed_call` возвращает список из одной записи) — иначе
    /// `success_rate` из конфига не влияет на соотношение mt_call/mo_call.
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
    ) -> bool {
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
        let is_answered = dispo.as_str() == "ANSWERED";

        event.record_type = cdr_record_type("CALL", direction).to_string();
        event.served_imsi = sub.imsi.to_string();
        event.served_msisdn = sub.msisdn.to_string();
        event.served_imei = sub.imei.to_string();
        event.calling_number = msisdn_src.to_string();
        event.called_number = msisdn_dst.to_string();
        event.event_timestamp = fmt_ts_ms(to_epoch_ms(&start_local.with_timezone(&chrono::Utc)));
        // answer_timestamp — по правилу питона (voice.py, _generate_successful_call):
        // фиксированная 1 секунда от начала события, только для ANSWERED
        // (у failed-звонка питон это поле вовсе не заполняет).
        event.answer_timestamp = if is_answered {
            fmt_ts_ms(to_epoch_ms(&(start_local + Duration::seconds(1)).with_timezone(&chrono::Utc)))
        } else {
            String::new()
        };
        event.release_timestamp = fmt_ts_ms(to_epoch_ms(&end_local.with_timezone(&chrono::Utc)));
        event.duration_seconds = dur_sec.to_string();
        event.first_cell_id = cell_id.to_string();
        event.last_cell_id = cell_id.to_string();
        event.serving_ne_id = ne_id;
        // consolidation_id — случайная метка корреляции MO/MT одной попытки
        // вызова, как у питона (voice.py, `_buf.get_uuid()`/`rng.integers`);
        // вызывающий код (worker_generate_shard) копирует то же значение
        // в парную MT-запись.
        event.consolidation_id = gen_correlation_id(rng);
        let _ = cause; // числовой cause_for_termination не заводим — docs/field-mapping.md
        is_answered
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
        // events.sms.delivery_success_rate из конфига — доля DELIVERED;
        // остаток делится между SENT/FAILED в прежней пропорции дефолта
        // (0.1:0.02 = 5:1), как и call_dispositions у голоса (config.rs).
        let status_weights = match cfg.sms_delivery_success_rate {
            Some(sr) => {
                let sr = sr.clamp(0.0, 1.0);
                let rest = 1.0 - sr;
                [rest * 5.0 / 6.0, sr, rest * 1.0 / 6.0]
            }
            None => [0.1, 0.88, 0.02],
        };
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

    /// Та же запись, что и `generate`, но направление задано вызывающим,
    /// а не разыграно по `p_mo` — нужна для пары MO(от sub)+коррелированный
    /// MT(собеседнику), как у `CallGenerator::generate_forced_direction`.
    /// Без неё SMS в `worker_generate_shard` эмитил РОВНО одну запись на
    /// событие (либо MO, либо MT по монетке `p_mo`), а не пару, как звонок
    /// (`mo_call`+коррелированный `mt_call`) — SMS давал в ~2 раза меньше
    /// записей на единицу lambda, чем голос, и доля SMS проседала (см.
    /// docs/contact-graph-and-memory-2026-09-08.md, задача 1).
    pub fn generate_forced_direction(
        &self,
        event: &mut EventRow,
        sub: &Subscriber,
        start_local: DateTime<chrono_tz::Tz>,
        other_msisdn: u64,
        tz_name: &'static str,
        cell_id: u32,
        rng: &mut StdRng,
        forced_direction: &'static str,
    ) {
        let direction = forced_direction;

        let (msisdn_src, msisdn_dst) = if direction == "MO" {
            (sub.msisdn, other_msisdn)
        } else {
            (other_msisdn, sub.msisdn)
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
        // consolidation_id — как у CallGenerator::generate_forced_direction:
        // случайная метка корреляции MO/MT одной SMS, тот же приём, что
        // и у питона (sms.py, `_buf.get_uuid()`).
        event.consolidation_id = gen_correlation_id(rng);
        let _ = (cause, sms_segments, sms_status, end_local, tz_name);
    }
}

/// Generate DATA session events
pub struct DataGenerator {
    cells_by_rat: HashMap<String, Vec<u32>>,
    cells_all: Vec<u32>,
    rat_dist: WeightedIndex<f64>,
    apn_dist: WeightedIndex<f64>,
    network_elements: Vec<String>,
    sgw_elements: Vec<String>,
    pgw_elements: Vec<String>,
    // events.data.volume_uplink/volume_downlink (mu, sigma, min_bytes) —
    // None означает «конфиг не задал events.data», тогда draw_session
    // берёт прежнюю зашитую по-RAT таблицу (Normal, 1-12 МБ), которая
    // расходилась с конфигом контура и объясняла −28.6% объёма выхода.
    volume_uplink: Option<(f64, f64, f64)>,
    volume_downlink: Option<(f64, f64, f64)>,
}

impl DataGenerator {
    pub fn new(cells_by_rat: HashMap<String, Vec<u32>>, cells_all: Vec<u32>, network_elements: Vec<String>) -> Self {
        Self::new_with_volume(cells_by_rat, cells_all, network_elements, None, None)
    }

    /// То же самое, но с объёмами data-сессии из `events.data.*`
    /// контурного конфига (contour.rs::DataEventsCfg) вместо зашитой
    /// таблицы. `None` в любом из аргументов — используется дефолт.
    pub fn new_with_volume(
        cells_by_rat: HashMap<String, Vec<u32>>,
        cells_all: Vec<u32>,
        network_elements: Vec<String>,
        volume_uplink: Option<(f64, f64, f64)>,
        volume_downlink: Option<(f64, f64, f64)>,
    ) -> Self {
        let rat_weights = [0.3, 0.5, 0.2];
        let rat_dist = WeightedIndex::new(&rat_weights).unwrap();

        let apn_weights = [0.8, 0.1, 0.1];
        let apn_dist = WeightedIndex::new(&apn_weights).unwrap();

        // Элементы-кандидаты для SGW- и PGW-стороны пары — отбираем по
        // подстроке id ("sgw"/"pgw"), как их называет demo-cdr.yaml
        // (network.elements[].id: "sgw-01", "pgw-01"). Полной TAC-based
        // топологии нет (см. assign_ne_id) — это тот же уровень упрощения,
        // что и раньше, только теперь СТОРОНЫ пары различаются по id, а не
        // совпадают всегда с одним и тем же элементом.
        let sgw_elements: Vec<String> = network_elements
            .iter()
            .filter(|id| id.to_lowercase().contains("sgw"))
            .cloned()
            .collect();
        let pgw_elements: Vec<String> = network_elements
            .iter()
            .filter(|id| id.to_lowercase().contains("pgw"))
            .cloned()
            .collect();

        DataGenerator {
            cells_by_rat,
            cells_all,
            rat_dist,
            apn_dist,
            network_elements,
            sgw_elements,
            pgw_elements,
            volume_uplink,
            volume_downlink,
        }
    }

    /// Разыгрывает объём/RAT/соту/длительность ОДИН раз на всю пару
    /// sgw_data+pgw_data — общий вход для `fill_side`, вызываемого дважды
    /// (по одной стороне за раз: `EventPool::acquire` отдаёт только одну
    /// живую `&mut EventRow` за раз, второй слот пула нельзя занять, пока
    /// первый ещё используется, поэтому пара не собирается одним вызовом
    /// с двумя `&mut` — розыгрыш вынесен в отдельный, "чистый от событий"
    /// шаг). Одна data-сессия → ДВЕ записи с ОДНИМ и тем же объёмом/RAT/
    /// длительностью/таймингом — `sgw_data` и `pgw_data`, как у питона
    /// (`generators/data.py`, `_create_sgw_pgw_pair`; demo-cdr.yaml прямо
    /// описывает это как «одна data-сессия даёт ДВЕ записи»). Раньше rust
    /// эмитил один `pgw_data`-эквивалент и `sgw_data` не порождался вовсе
    /// (docs/field-mapping.md, «Одна data-запись вместо пары» — снято этой
    /// правкой). Общий розыгрыш гарантирует точное равенство числа
    /// sgw_data и pgw_data по построению (эталон на боевом наборе:
    /// 7 632 306 = 7 632 306), а не совпадение «в среднем».
    pub fn draw_session(&self, start_local: DateTime<chrono_tz::Tz>, rng: &mut StdRng) -> DataSessionDraw {
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

        // Объём сессии — из events.data.volume_uplink/volume_downlink
        // конфига (lognormal, независимо по каждому направлению — как
        // у питона `generators/data.py`), если конфиг их задал; иначе
        // прежняя зашитая по-RAT таблица (Normal, down затем up как доля
        // down) — единственный путь без --config или со старым плоским.
        let (down, up) = match (self.volume_downlink, self.volume_uplink) {
            (Some((down_mu, down_sigma, down_min)), Some((up_mu, up_sigma, up_min))) => {
                let down_dist = LogNormal::new(down_mu, down_sigma).unwrap();
                let down = down_dist.sample(rng).max(down_min) as u64;
                let up_dist = LogNormal::new(up_mu, up_sigma).unwrap();
                let up = up_dist.sample(rng).max(up_min) as u64;
                (down, up)
            }
            _ => {
                let down_normal = Normal::new(down_mean, down_sd).unwrap();
                let down = (down_normal.sample(rng) as f64).abs().max(2_000.0) as u64;
                let up = (down as f64 * rng.gen_range(up_ratio_min..=up_ratio_max))
                    .max(1_000.0) as u64;
                (down, up)
            }
        };

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

        let event_ts = fmt_ts_ms(to_epoch_ms(&start_local.with_timezone(&chrono::Utc)));
        let closure_ts = fmt_ts_ms(to_epoch_ms(&end_local.with_timezone(&chrono::Utc)));

        // charging_id — как у питона (`generators/data.py`,
        // `rng.integers(1, 2**31)`): случайный ID сессии, общий у sgw_data
        // и pgw_data одной пары (там это буквально то же поле, которым
        // потребитель склеивает partial records одной сессии).
        let charging_id: u32 = rng.gen_range(1..=2_147_483_647u32);

        DataSessionDraw {
            rat: rat.to_string(),
            apn: apn.to_string(),
            cell_id,
            dur,
            up,
            down,
            event_ts,
            closure_ts,
            charging_id,
        }
    }

    /// Заполняет одну сторону пары (`sgw_data`/`pgw_data`) из общего
    /// розыгрыша `draw_session` — вызывать дважды, по одному разу на
    /// сторону, каждый раз с новым `EventRow`, полученным из пула.
    pub fn fill_side(
        &self,
        event: &mut EventRow,
        sub: &Subscriber,
        draw: &DataSessionDraw,
        record_type: &'static str,
        side: DataSide,
    ) {
        // Элементы нужного типа id ("sgw-01"/"pgw-01") — если в конфиге их
        // нет (id без подстроки "sgw"/"pgw"), падаем на общий список, как
        // раньше делал assign_ne_id: пара не теряет запись, только точность
        // выбора обслуживающего элемента.
        let side_elements = match side {
            DataSide::Sgw => &self.sgw_elements,
            DataSide::Pgw => &self.pgw_elements,
        };
        let ne_pool: &[String] = if side_elements.is_empty() {
            &self.network_elements
        } else {
            side_elements
        };
        let ne_id = assign_ne_id(sub.msisdn, ne_pool);

        event.record_type = record_type.to_string();
        event.served_imsi = sub.imsi.to_string();
        event.served_msisdn = sub.msisdn.to_string();
        event.served_imei = sub.imei.to_string();
        event.calling_number = sub.msisdn.to_string();
        event.event_timestamp = draw.event_ts.clone();
        event.duration_seconds = draw.dur.to_string();
        event.first_cell_id = draw.cell_id.to_string();
        event.last_cell_id = draw.cell_id.to_string();
        event.serving_ne_id = ne_id;
        event.record_opening_time = draw.event_ts.clone();
        event.record_closure_time = draw.closure_ts.clone();
        // uplink = данные ОТ абонента (up), downlink = данные К абоненту
        // (down) — как volume_uplink/volume_downlink питоновского
        // generators/data.py, общие для обеих сторон пары.
        event.uplink_volume_bytes = draw.up.to_string();
        event.downlink_volume_bytes = draw.down.to_string();
        event.apn = draw.apn.clone();
        event.rat_type = draw.rat.clone();
        event.charging_id = draw.charging_id.to_string();
    }
}

/// Общий розыгрыш параметров одной data-сессии — сторона пары не входит,
/// она различает только `record_type`/`serving_ne_id` (см. `fill_side`).
pub struct DataSessionDraw {
    rat: String,
    apn: String,
    cell_id: u32,
    dur: i64,
    up: u64,
    down: u64,
    event_ts: String,
    closure_ts: String,
    charging_id: u32,
}

#[derive(Clone, Copy)]
pub enum DataSide {
    Sgw,
    Pgw,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShardStats {
    pub shard: usize,
    pub calls: usize,
    pub sms: usize,
    pub data: usize,
}

/// Worker process for the CSV-путь (устаревший, не достижим через CLI —
/// generate-cdr всегда открывает redb-базу и работает через
/// `worker_generate_shard`, см. main.rs). Оставлен как есть, вне области
/// этапов 3–5: правки на реальную выборку абонентов и общий пул контактов
/// применены только к достижимому redb-пути.
pub fn worker_generate(
    day: DateTime<chrono_tz::Tz>,
    shard_id: usize,
    users_range: (usize, usize),
    cfg: &Config,
    out_dir: &Path,
    subscriber_db_path: Option<&Path>,
    writer_tx: Sender<WriterMessage>,
) -> anyhow::Result<()> {
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
    let date_compact = day.format("%Y%m%d").to_string();

    // Initialize event pool for zero-allocation event generation
    let mut event_pool = EventPool::new(cfg.event_pool_size);

    // Initialize batch for async writing
    let batch_capacity = cfg.batch_size_bytes / 230; // ~230 bytes per event
    let mut batch = EventBatch::new(batch_capacity, &date_compact);

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
            // Функция dead-code (worker_generate не вызывается из main.rs,
            // см. worker_generate_shard ниже) — success_rate/MT-гейт здесь
            // намеренно не применяем, чтобы не трогать неиспользуемый путь.
            let _ = call_gen.generate_forced_direction(mo_event, &sub, start_local, other_msisdn, tz_name, cell_id, &mut rng, "MO");

            // Add MO record to batch
            batch.push(mo_event.clone());
            stats.calls += 1;

            // Send batch if full
            if batch.is_full(cfg.batch_size_bytes) {
                writer_tx.send(WriterMessage::Batch(batch))?;
                batch = EventBatch::new(batch_capacity, &date_compact);
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
                    batch = EventBatch::new(batch_capacity, &date_compact);
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
                batch = EventBatch::new(batch_capacity, &date_compact);
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

            // sgw_data и pgw_data одной сессии — общий розыгрыш, две
            // заливки по одной живой &mut EventRow за раз (см. докстринг
            // DataGenerator::draw_session). Путь не достижим через CLI
            // (см. докстринг функции выше), правится только чтобы не
            // разойтись с сигнатурой draw_session/fill_side.
            let draw = data_gen.draw_session(start_local, &mut rng);
            let sgw_event = event_pool.acquire();
            data_gen.fill_side(sgw_event, &sub, &draw, "sgw_data", crate::generators::DataSide::Sgw);
            batch.push(sgw_event.clone());
            let pgw_event = event_pool.acquire();
            data_gen.fill_side(pgw_event, &sub, &draw, "pgw_data", crate::generators::DataSide::Pgw);
            batch.push(pgw_event.clone());
            stats.data += 2;

            // Send batch if full
            if batch.is_full(cfg.batch_size_bytes) {
                writer_tx.send(WriterMessage::Batch(batch))?;
                batch = EventBatch::new(batch_capacity, &date_compact);
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

/// Один рабочий элемент параллелизма этапа 4: (день, диапазон индексов
/// в ОБЩЕМ пуле абонентов `all_msisdns`). Ось параллелизма — даты (и внутри
/// дня — куски общего пула, чтобы rayon было чем занять воркеры даже при
/// одном-двух днях в прогоне), а не диапазон абонентов на воркер, как было
/// раньше: старая схема давала каждому воркеру СВОЙ диапазон абонентов на
/// весь прогон, из-за чего звонок собеседнику выбирался внутри того же
/// диапазона в 70% случаев (generators.rs, `other_msisdn`) — граф контактов
/// получался разреженным по границам диапазонов, а не по реальному кругу
/// общения (docs/field-mapping.md, задание этапа 4).
///
/// `all_msisdns` — реальные MSISDN абонентов из redb (этап 3: раньше здесь
/// вычисляли MSISDN арифметически из индекса и почти никогда не находили
/// абонента в базе), общие на весь прогон и видимые каждому воркеру целиком
/// — так собеседник звонка выбирается из ВСЕГО пула, а не из своего куска.
///
/// `writer_channels` — несколько writer-задач (этап 5): событие маршрутизируется
/// по индексу serving_ne_id обслуженного абонента, поэтому каждую пару
/// (ne_id, дата) пишет ровно одна задача независимо от того, сколько работ
/// (день, кусок пула) отправили в неё события.
/// `contact_book` — постоянная книга контактов (см. `crate::contact_book`),
/// построенная РАЗ на весь прогон в main.rs и общая для всех work item'ов,
/// как и `all_msisdns`: круг общения абонента не должен зависеть ни от дня,
/// ни от куска пула, который его в этот день обслуживает.
#[allow(clippy::too_many_arguments)]
pub fn worker_generate_shard(
    day: DateTime<chrono_tz::Tz>,
    day_idx: usize,
    chunk_idx: usize,
    idx_range: (usize, usize),
    all_msisdns: &Arc<Vec<u64>>,
    contact_book: &Arc<ContactBook>,
    cfg: &Config,
    out_dir: &Path,
    redb: &Arc<SubscriberDbRedb>,
    writer_channels: &[Sender<WriterMessage>],
    base_seed: u64,
) -> anyhow::Result<()> {
    use chrono::Duration;

    // Seed воркера привязан к дате: без этого (старая формула — только
    // workers*1000 + shard_id, без дня) все дни при переносе параллелизма
    // на даты получили бы одинаковый seed и стали бы похожи друг на друга
    // (задание этапа 4, «ДЕТЕРМИНИЗМ»). Формула: базовый seed прогона плюс
    // вклад дня (крупный множитель, чтобы дни не пересекались) плюс вклад
    // куска пула внутри дня (мелкий множитель — кусков на порядки меньше,
    // чем 1_000_000).
    let seed = base_seed
        .wrapping_add((day_idx as u64).wrapping_mul(1_000_000))
        .wrapping_add(chunk_idx as u64);
    let mut rng = StdRng::seed_from_u64(seed);

    let tz = tz_from_name(&cfg.tz_name);
    let tz_name: &'static str = Box::leak(cfg.tz_name.clone().into_boxed_str());

    // Initialize generators
    let call_gen = CallGenerator::new(cfg);
    let sms_gen = SmsGenerator::new(cfg);
    let data_gen = DataGenerator::new_with_volume(
        HashMap::new(),
        vec![],
        cfg.network_elements.clone(),
        cfg.data_volume_uplink,
        cfg.data_volume_downlink,
    );

    let day_str = day.format("%Y-%m-%d").to_string();
    let date_compact = day.format("%Y%m%d").to_string();

    // Initialize event pool
    let mut event_pool = EventPool::new(cfg.event_pool_size);

    // Этап 5: своя пачка на каждую writer-задачу, а не одна общая — событие
    // маршрутизируется по serving_ne_id обслуженного абонента (см. ниже,
    // route_writer_idx), и разным ne_id может достаться разная задача.
    let batch_capacity = cfg.batch_size_bytes / 230;
    let mut batches: Vec<EventBatch> = (0..writer_channels.len())
        .map(|_| EventBatch::new(batch_capacity, &date_compact))
        .collect();

    // Индекс сетевого элемента → индекс writer-задачи, ПО РЕАЛЬНОМУ
    // serving_ne_id события, а не пересчётом из msisdn.
    //
    // Раньше индекс писателя вычислялся заново как `msisdn % network_elements
    // .len()`, в предположении, что это всегда тот же индекс, что даёт
    // assign_ne_id. Для CALL/SMS так и есть — assign_ne_id берёт элемент из
    // ПОЛНОГО cfg.network_elements. Но для DATA (DataGenerator::fill_side)
    // ne_id берётся из ПОДСПИСКА — sgw_elements/pgw_elements (только id,
    // содержащие "sgw"/"pgw"), длина которого меньше полного списка (в
    // дефолтном конфиге — 1 против 5). `msisdn % 5`, применённый как индекс
    // подсписка длины 1, давал значения 0..4, размазанные `% writer_tasks`
    // по РАЗНЫМ writer-задачам — хотя реальный ne_id для всех этих событий
    // один и тот же ("sgw-01"/"pgw-01"). Несколько задач параллельно
    // открывали (`File::create`, усечение) и писали в один и тот же
    // gzip-файл (writer.rs, EventWriter::write_row) — gzip-поток портился
    // без единой строки в логе: `gzip -t` находит CRC/data-stream error
    // (воспроизведено на 2000 абонентов / 5 суток / 6 воркеров: 10 из 25
    // файлов sgw-01/pgw-01 побиты, msc/smsc целы — те же симптомы, что
    // и на боевом прогоне 12 000×240).
    //
    // Починка: маршрутизировать по САМОМУ ne_id (его позиции в полном
    // cfg.network_elements), а не пересчитывать индекс заново из msisdn.
    // Тогда неважно, из какого пула (полного или подсписка) ne_id выбран —
    // одна и та же строка ne_id всегда даёт один и тот же индекс задачи,
    // и пара (ne_id, дата) остаётся собственностью ровно одной задачи.
    let writer_tasks = writer_channels.len().max(1);
    let route_writer_idx = |ne_id: &str| -> usize {
        let ne_pos = cfg
            .network_elements
            .iter()
            .position(|id| id == ne_id)
            .unwrap_or(0);
        ne_pos % writer_tasks
    };
    macro_rules! flush_if_full {
        ($idx:expr) => {
            if batches[$idx].is_full(cfg.batch_size_bytes) {
                let full = std::mem::replace(&mut batches[$idx], EventBatch::new(batch_capacity, &date_compact));
                writer_channels[$idx].send(WriterMessage::Batch(full))?;
            }
        };
    }

    let day_start_local = tz
        .with_ymd_and_hms(day.year(), day.month(), day.day(), 0, 0, 0)
        .unwrap();

    let day_start_ts = day.timestamp_millis();

    let mut stats = ShardStats {
        shard: chunk_idx,
        calls: 0,
        sms: 0,
        data: 0,
    };

    // Интенсивности из контурного конфига (subscribers.profiles[].
    // daily_rates.*.params.lambda) — раньше разбирались в contour.rs, но
    // не долетали до generators.rs: генератор всегда работал на
    // avg_calls_per_user/avg_sms_per_user/avg_data_sessions_per_user
    // (дефолты Config::default либо старый плоский ключ), контурный YAML
    // на объём и структуру трафика не влиял. Теперь на каждого абонента
    // выбирается профиль (взвешенно по subscribers.profiles[].weight,
    // как у питона — сумма весов нормирована в apply_contour_config), и
    // число событий каждого типа сэмплируется от лямбды ЭТОГО профиля,
    // а не от общего среднего по популяции.
    //
    // Если контурных профилей нет (--config не задан или старый плоский
    // конфиг) — единственный "профиль" на avg_*_per_user, поведение как
    // до этой правки.
    struct ProfileSamplers {
        weight: f64,
        calls: EventCountSampler,
        sms: EventCountSampler,
        data: EventCountSampler,
    }
    let profile_samplers: Vec<ProfileSamplers> = if cfg.subscriber_profiles.is_empty() {
        vec![ProfileSamplers {
            weight: 1.0,
            calls: EventCountSampler::new(cfg.avg_calls_per_user),
            sms: EventCountSampler::new(cfg.avg_sms_per_user),
            data: EventCountSampler::new(cfg.avg_data_sessions_per_user),
        }]
    } else {
        cfg.subscriber_profiles
            .iter()
            .map(|p| ProfileSamplers {
                weight: p.weight,
                // Только mo_*_lambda: у питона (engine/runner.py, _ProfileCache)
                // mt_call/mt_sms lambda из конфига не читаются вовсе — MT-запись
                // не независимое событие, а корреляция ответа на чей-то MO (ниже
                // по циклу, generate correlated MT). Суммирование mo+mt здесь было
                // ошибкой: каждый абонент получал вдвое больше исходящих попыток,
                // чем задано лямбдой mo_call/mo_sms, и голос/SMS перелетали питона
                // (54.1%/15.2% при 39.5%/20.3% у питона, см. docs/contact-graph-
                // and-memory-2026-09-08.md).
                calls: EventCountSampler::new(p.mo_call_lambda),
                sms: EventCountSampler::new(p.mo_sms_lambda),
                data: EventCountSampler::new(p.data_lambda),
            })
            .collect()
    };
    // Веса нормированы в apply_contour_config и никогда не все нулевые
    // (там же — падение на weight_sum <= 0.0 не создаёт этой ветки), но
    // на пустом контурном пути (единственный профиль weight=1.0) и на
    // случай вырожденного конфига (все веса 0) подстрахуемся минимальным
    // положительным весом — иначе WeightedIndex::new паникует.
    let profile_dist = WeightedIndex::new(
        profile_samplers.iter().map(|p| p.weight.max(1e-9)),
    )
    .expect("веса профилей после нормировки не могут дать пустое/невалидное распределение");

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

    // Префиксы численно — для ветки «внешний номер» (не из базы). Если
    // конфиг задал subscribers.external_numbers.prefixes с весами —
    // используем их (WeightedIndex), иначе прежний равновероятный список
    // cfg.prefixes (было раньше единственным источником).
    let (numeric_prefixes, external_prefix_dist): (Vec<u64>, Option<WeightedIndex<f64>>) =
        if !cfg.external_number_prefixes.is_empty() {
            let prefixes: Vec<u64> = cfg.external_number_prefixes.iter().map(|(p, _)| *p).collect();
            let weights: Vec<f64> = cfg.external_number_prefixes.iter().map(|(_, w)| w.max(1e-9)).collect();
            (prefixes, WeightedIndex::new(&weights).ok())
        } else {
            let prefixes: Vec<u64> = cfg.prefixes.iter().map(|s| s.parse().unwrap_or(31612)).collect();
            (prefixes, None)
        };
    // Доля внешних звонков/SMS и порог "внешний+книга" — из
    // subscribers.contact_book конфига, если он задан, иначе прежние
    // константы contact_book.rs (EXTERNAL_CALL_RATIO=0.15/REPEAT_CALL_PROBABILITY,
    // которые раньше расходились с demo-cdr.yaml, где 0.12).
    let external_call_ratio = cfg.contact_book_external_call_ratio.unwrap_or(EXTERNAL_CALL_RATIO);
    let repeat_call_probability = cfg
        .contact_book_repeat_call_probability
        .unwrap_or(crate::contact_book::REPEAT_CALL_PROBABILITY);
    let contact_threshold = if cfg.contact_book_external_call_ratio.is_some() {
        external_call_ratio + (1.0 - external_call_ratio) * repeat_call_probability
    } else {
        CONTACT_THRESHOLD
    };
    let draw_external_msisdn = |rng: &mut StdRng| -> u64 {
        let prefix_idx = match &external_prefix_dist {
            Some(dist) => dist.sample(rng),
            None => rng.gen_range(0..numeric_prefixes.len()),
        };
        let prefix = numeric_prefixes[prefix_idx];
        let subscriber_number = rng.gen_range(0..10_000_000u64);
        prefix * 10_000_000 + subscriber_number
    };

    // Диапазон индексов в ОБЩЕМ пуле all_msisdns, обрабатываемый этим work
    // item'ом (этап 3+4: реальные ключи, а не арифметика по индексу; кусок
    // общего пула, а не персональный диапазон воркера).
    let (chunk_start_idx, chunk_end_idx) = idx_range;
    if chunk_start_idx >= chunk_end_idx {
        return Ok(());
    }
    let chunk_msisdns = &all_msisdns[chunk_start_idx..chunk_end_idx];

    // all_msisdns отсортирован по возрастанию (redb хранит ключи как
    // B-дерево) — min/max смежного среза совпадают с его границами,
    // load_chunk вернёт ровно эти записи и ни одной лишней.
    let min_msisdn = chunk_msisdns[0];
    let max_msisdn = chunk_msisdns[chunk_msisdns.len() - 1];

    // Load chunk from redb in one transaction (OPTIMIZATION #1)
    let chunk_data = redb.load_chunk(min_msisdn, max_msisdn + 1)?;

    // Build HashMap for O(1) lookup (OPTIMIZATION #1)
    let snapshot_cache: HashMap<u64, Vec<crate::subscriber_db_redb::SubscriberSnapshotNumeric>> =
        chunk_data.into_iter().collect();

    // Build subscriber list for this chunk using cache — по реальным
    // ключам, поэтому попадание в кеш теперь не случайность, а гарантия
    // (этап 3): каждый msisdn из chunk_msisdns реально есть в базе.
    let mut chunk_subs = Vec::with_capacity(chunk_msisdns.len());

    for &msisdn in chunk_msisdns {
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

        // Профиль абонента — взвешенный выбор (subscribers.profiles[].weight),
        // затем число событий каждого типа сэмплируется от лямбды ЭТОГО
        // профиля (см. комментарий у profile_samplers выше).
        let profile = &profile_samplers[profile_dist.sample(&mut rng)];
        let n_calls = profile.calls.sample(&mut rng);
        let n_sms = profile.sms.sample(&mut rng);
        let n_data = profile.data.sample(&mut rng);

        // Generate CALL events
        for _ in 0..n_calls {
            let start_local = sample_time(&mut rng);

            // Собеседник звонка — трёхуровневый выбор, как у питона
            // (`b_party.py`, docstring): внешний номер (EXTERNAL_CALL_RATIO),
            // затем постоянная книга контактов абонента (CONTACT_THRESHOLD),
            // иначе — случайный абонент из ОБЩЕГО пула all_msisdns (видимого
            // каждому work item целиком — иначе граф разрежен границами
            // шардов, см. докстринг функции выше). Книга не пуста лишь пока
            // у абонента есть круг общения — при пустой книге (degree=0
            // выпал у Zipf) откатываемся на случайного абонента, как и
            // питон делает при пустом contact_book.get() (`b_party.py:128`).
            let roll: f64 = rng.gen();
            let is_external = roll < external_call_ratio;
            let other_msisdn: u64 = if is_external {
                DBG_EXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                draw_external_msisdn(&mut rng)
            } else if roll < contact_threshold {
                let my_contacts = contact_book.contacts_of(sub.msisdn);
                if my_contacts.is_empty() {
                    DBG_FALLBACK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    all_msisdns[rng.gen_range(0..all_msisdns.len())]
                } else {
                    DBG_BOOK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    my_contacts[rng.gen_range(0..my_contacts.len())]
                }
            } else {
                DBG_RAND.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                all_msisdns[rng.gen_range(0..all_msisdns.len())]
            };

            let cell_id = rng.gen_range(10_000..100_000);

            // Generate MO record
            let mo_event = event_pool.acquire();
            // Возврат — сэмплированная диспозиция ANSWERED/не-ANSWERED
            // (success_rate из events.voice.success_rate, config.rs). MO
            // пишется всегда — это попытка вызова, видна коммутатору
            // вызывающего независимо от исхода (как у питона,
            // `voice.py::_generate_failed_call` тоже возвращает MO). MT
            // порождаем только при ANSWERED — иначе несостоявшийся вызов
            // всё равно давал бы пару MO/MT и success_rate не влиял бы
            // на соотношение mt_call/mo_call (было 0.989 вместо ~0.85).
            let is_answered = call_gen.generate_forced_direction(
                mo_event,
                sub,
                start_local,
                other_msisdn,
                tz_name,
                cell_id,
                &mut rng,
                "MO",
            );

            let mo_idx = route_writer_idx(&mo_event.serving_ne_id);
            batches[mo_idx].push(mo_event.clone());
            stats.calls += 1;
            flush_if_full!(mo_idx);

            if !is_answered {
                continue;
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
                let answer_timestamp = mo_event.answer_timestamp.clone();
                let release_timestamp = mo_event.release_timestamp.clone();
                let duration_seconds = mo_event.duration_seconds.clone();
                let consolidation_id = mo_event.consolidation_id.clone();

                // Generate correlated MT record
                let mt_event = event_pool.acquire();
                mt_event.record_type = cdr_record_type("CALL", "MT").to_string();
                mt_event.served_imsi = other_snapshot.imsi.to_string();
                mt_event.served_msisdn = other_snapshot.msisdn.to_string();
                mt_event.served_imei = other_snapshot.imei.to_string();
                mt_event.consolidation_id = consolidation_id;
                // Направление MT-записи — то же, что у MO (calling=инициатор
                // звонка, called=собеседник), а не развёрнутое: это одна и
                // та же попытка звонка, увиденная с двух коммутаторов, а не
                // "обратный звонок" (сверено с питоном, `voice.py`, mt-запись
                // сохраняет calling/called без перестановки). Прежняя
                // перестановка здесь была багом: она форсировала directed
                // (A→B) и (B→A) на КАЖДЫЙ звонок, поэтому граф контактов
                // выходил симметричным на 100% вместо ожидаемых ~1% у питона
                // (см. docs/contact-graph-and-memory-2026-09-08.md, задача 1).
                mt_event.calling_number = sub.msisdn.to_string();
                mt_event.called_number = other_msisdn.to_string();
                mt_event.event_timestamp = event_timestamp;
                mt_event.answer_timestamp = answer_timestamp;
                mt_event.release_timestamp = release_timestamp;
                mt_event.duration_seconds = duration_seconds;
                mt_event.first_cell_id = cell_id.to_string();
                mt_event.last_cell_id = cell_id.to_string();
                mt_event.serving_ne_id = assign_ne_id(other_snapshot.msisdn, &cfg.network_elements);

                let mt_idx = route_writer_idx(&mt_event.serving_ne_id);
                batches[mt_idx].push(mt_event.clone());
                stats.calls += 1;
                flush_if_full!(mt_idx);
            } else if is_external {
                // Внешний абонент не в нашей subscriber_db (redb) по
                // построению — прежде MT-запись здесь молча не создавалась
                // вовсе (see README, «нет mt_call для внешних адресатов»).
                // served_imsi/served_imei для внешнего абонента нам не
                // известны (это не наша сеть) — пусто, как остальные
                // немоделируемые поля (docs/field-mapping.md).
                let event_timestamp = mo_event.event_timestamp.clone();
                let answer_timestamp = mo_event.answer_timestamp.clone();
                let release_timestamp = mo_event.release_timestamp.clone();
                let duration_seconds = mo_event.duration_seconds.clone();
                let consolidation_id = mo_event.consolidation_id.clone();

                let mt_event = event_pool.acquire();
                mt_event.record_type = cdr_record_type("CALL", "MT").to_string();
                mt_event.served_imsi = String::new();
                mt_event.served_msisdn = other_msisdn.to_string();
                mt_event.served_imei = String::new();
                mt_event.consolidation_id = consolidation_id;
                mt_event.calling_number = sub.msisdn.to_string();
                mt_event.called_number = other_msisdn.to_string();
                mt_event.event_timestamp = event_timestamp;
                mt_event.answer_timestamp = answer_timestamp;
                mt_event.release_timestamp = release_timestamp;
                mt_event.duration_seconds = duration_seconds;
                mt_event.first_cell_id = cell_id.to_string();
                mt_event.last_cell_id = cell_id.to_string();
                // Обслуживающий элемент — свой (сеть sub'а видит только
                // свою сторону вызова на внешний номер), не внешний.
                mt_event.serving_ne_id = assign_ne_id(sub.msisdn, &cfg.network_elements);

                let mt_idx = route_writer_idx(&mt_event.serving_ne_id);
                batches[mt_idx].push(mt_event.clone());
                stats.calls += 1;
                flush_if_full!(mt_idx);
            }
        }

        // Generate SMS events
        for _ in 0..n_sms {
            let start_local = sample_time(&mut rng);

            // Тот же трёхуровневый выбор собеседника, что и у звонков выше
            // (книга контактов общая для всех типов событий абонента).
            let roll: f64 = rng.gen();
            let sms_is_external = roll < external_call_ratio;
            let other_msisdn: u64 = if sms_is_external {
                DBG_EXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                draw_external_msisdn(&mut rng)
            } else if roll < contact_threshold {
                let my_contacts = contact_book.contacts_of(sub.msisdn);
                if my_contacts.is_empty() {
                    DBG_FALLBACK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    all_msisdns[rng.gen_range(0..all_msisdns.len())]
                } else {
                    DBG_BOOK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    my_contacts[rng.gen_range(0..my_contacts.len())]
                }
            } else {
                DBG_RAND.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                all_msisdns[rng.gen_range(0..all_msisdns.len())]
            };

            let cell_id = rng.gen_range(10_000..100_000);

            // MO от sub + коррелированный MT собеседнику — та же схема, что
            // у CALL выше (`generate_forced_direction`, а не разыгранный
            // `p_mo`): n_sms — это отправленные sub'ом сообщения (лямбда
            // mo_sms профиля), MT — не независимое событие, а ответ сети
            // получателю, как у питона (generators/sms.py: MO+MT парой на
            // одно сообщение).
            let mo_event = event_pool.acquire();
            sms_gen.generate_forced_direction(
                mo_event, sub, start_local, other_msisdn, tz_name, cell_id, &mut rng, "MO",
            );
            let mo_idx = route_writer_idx(&mo_event.serving_ne_id);
            batches[mo_idx].push(mo_event.clone());
            stats.sms += 1;
            flush_if_full!(mo_idx);

            let other_snapshot_opt = if let Some(snapshots) = snapshot_cache.get(&other_msisdn) {
                crate::subscriber_db_redb::SubscriberDbRedb::find_snapshot_at(snapshots, day_start_ts).cloned()
            } else {
                redb.get_subscriber_at(other_msisdn, day_start_ts)?
            };

            if let Some(ref other_snapshot) = other_snapshot_opt {
                if other_snapshot.msisdn != 0 {
                    // served_* — получатель (other_snapshot), а не sub: MT-
                    // запись видна с коммутатора получателя, ровно как у
                    // коррелированного mt_call выше. calling/called не
                    // разворачиваются — та же ориентация, что у MO.
                    let event_timestamp = mo_event.event_timestamp.clone();
                    let duration_seconds = mo_event.duration_seconds.clone();
                    let consolidation_id = mo_event.consolidation_id.clone();
                    let mt_event = event_pool.acquire();
                    mt_event.record_type = cdr_record_type("SMS", "MT").to_string();
                    mt_event.served_imsi = other_snapshot.imsi.to_string();
                    mt_event.served_msisdn = other_snapshot.msisdn.to_string();
                    mt_event.served_imei = other_snapshot.imei.to_string();
                    mt_event.consolidation_id = consolidation_id;
                    mt_event.calling_number = sub.msisdn.to_string();
                    mt_event.called_number = other_msisdn.to_string();
                    mt_event.event_timestamp = event_timestamp;
                    mt_event.duration_seconds = duration_seconds;
                    mt_event.first_cell_id = cell_id.to_string();
                    mt_event.last_cell_id = cell_id.to_string();
                    mt_event.serving_ne_id = assign_ne_id(other_snapshot.msisdn, &cfg.network_elements);

                    let mt_idx = route_writer_idx(&mt_event.serving_ne_id);
                    batches[mt_idx].push(mt_event.clone());
                    stats.sms += 1;
                    flush_if_full!(mt_idx);
                }
            } else if sms_is_external {
                // Тот же пробел, что и у CALL выше: внешний адресат SMS не
                // в redb, MT молча не создавался — то же исправление.
                let event_timestamp = mo_event.event_timestamp.clone();
                let duration_seconds = mo_event.duration_seconds.clone();
                let consolidation_id = mo_event.consolidation_id.clone();
                let mt_event = event_pool.acquire();
                mt_event.record_type = cdr_record_type("SMS", "MT").to_string();
                mt_event.served_imsi = String::new();
                mt_event.served_msisdn = other_msisdn.to_string();
                mt_event.served_imei = String::new();
                mt_event.consolidation_id = consolidation_id;
                mt_event.calling_number = sub.msisdn.to_string();
                mt_event.called_number = other_msisdn.to_string();
                mt_event.event_timestamp = event_timestamp;
                mt_event.duration_seconds = duration_seconds;
                mt_event.first_cell_id = cell_id.to_string();
                mt_event.last_cell_id = cell_id.to_string();
                mt_event.serving_ne_id = assign_ne_id(sub.msisdn, &cfg.network_elements);

                let mt_idx = route_writer_idx(&mt_event.serving_ne_id);
                batches[mt_idx].push(mt_event.clone());
                stats.sms += 1;
                flush_if_full!(mt_idx);
            }
        }

        // Generate DATA events — sgw_data + pgw_data одной сессии, общий
        // розыгрыш (DataGenerator::draw_session), см. его докстринг: это
        // и даёт точное равенство числа sgw_data/pgw_data по построению.
        for _ in 0..n_data {
            let start_local = sample_time(&mut rng);
            let draw = data_gen.draw_session(start_local, &mut rng);

            let sgw_event = event_pool.acquire();
            data_gen.fill_side(sgw_event, sub, &draw, "sgw_data", DataSide::Sgw);
            let sgw_idx = route_writer_idx(&sgw_event.serving_ne_id);
            batches[sgw_idx].push(sgw_event.clone());
            stats.data += 1;
            flush_if_full!(sgw_idx);

            let pgw_event = event_pool.acquire();
            data_gen.fill_side(pgw_event, sub, &draw, "pgw_data", DataSide::Pgw);
            let pgw_idx = route_writer_idx(&pgw_event.serving_ne_id);
            batches[pgw_idx].push(pgw_event.clone());
            stats.data += 1;
            flush_if_full!(pgw_idx);
        }
    }

    // Send remaining batches
    for (idx, batch) in batches.into_iter().enumerate() {
        if !batch.is_empty() {
            writer_channels[idx].send(WriterMessage::Batch(batch))?;
        }
    }

    // Write stats — имя файла включает и день, и кусок пула: при
    // параллелизме по датам разные (day_idx, chunk_idx) пишут в один
    // day_dir одновременно, и общий счётчик shard_id больше не подходит.
    let stat_path = out_dir
        .join(&day_str)
        .join(format!("stats_shard_d{:03}_c{:05}.json", day_idx, chunk_idx));
    let stats_json = serde_json::to_string_pretty(&stats)?;
    std::fs::write(stat_path, stats_json)?;

    Ok(())
}
