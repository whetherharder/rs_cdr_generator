// Configuration management for CDR generator
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub const DEFAULT_TZ_NAME: &str = "Europe/Amsterdam";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    // Population
    pub subscribers: usize,
    pub cells: usize,
    pub prefixes: Vec<String>,
    pub mccmnc_pool: Vec<String>,

    // Geography
    pub center_lat: f64,
    pub center_lon: f64,
    pub radius_km: f64,

    // Event rates (per user per day)
    pub avg_calls_per_user: f64,
    pub avg_sms_per_user: f64,
    pub avg_data_sessions_per_user: f64,

    // MO/MT shares
    pub mo_share_call: f64,
    pub mo_share_sms: f64,

    // Device behavior
    pub imei_daily_change_prob: f64,

    // Call dispositions
    pub call_dispositions: HashMap<String, f64>,

    // Call duration (seconds)
    pub call_duration_quantiles: CallDurationQuantiles,

    // Interconnect traffic
    pub interconnect_share: f64,

    // Temporal patterns - hourly multipliers (24 values)
    pub diurnal_weekday: Vec<f64>,
    pub diurnal_weekend: Vec<f64>,

    // Seasonality (monthly multipliers, 1-12)
    pub seasonality: HashMap<usize, f64>,

    // Special days (YYYY-MM-DD -> multiplier)
    pub special_days: HashMap<String, f64>,

    // File rotation
    pub rotate_bytes: u64,

    // Timezone
    pub tz_name: String,

    // Multiprocessing
    pub workers: usize,

    // Performance optimization settings
    pub event_pool_size: usize,      // EventRow object pool size per worker
    pub batch_size_bytes: usize,     // Batch size for async writing (bytes)
    pub writer_tasks: usize,         // Number of async writer tasks (0 = auto)

    // Subscriber database
    pub subscriber_db_path: Option<PathBuf>,
    pub generate_subscriber_db: Option<PathBuf>,
    pub db_size: usize,
    pub db_history_days: usize,
    pub db_device_change_rate: f64,
    pub db_number_release_rate: f64,
    pub db_cooldown_days: usize,
    pub validate_db_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallDurationQuantiles {
    pub p50: u32,
    pub p90: u32,
    pub p99: u32,
}

impl Default for Config {
    fn default() -> Self {
        let mut call_dispositions = HashMap::new();
        call_dispositions.insert("ANSWERED".to_string(), 0.82);
        call_dispositions.insert("NO ANSWER".to_string(), 0.12);
        call_dispositions.insert("BUSY".to_string(), 0.04);
        call_dispositions.insert("FAILED".to_string(), 0.015);
        call_dispositions.insert("CONGESTION".to_string(), 0.005);

        let mut seasonality = HashMap::new();
        seasonality.insert(1, 0.95);
        seasonality.insert(2, 0.9);
        seasonality.insert(3, 1.0);
        seasonality.insert(4, 1.05);
        seasonality.insert(5, 1.1);
        seasonality.insert(6, 1.15);
        seasonality.insert(7, 1.2);
        seasonality.insert(8, 1.15);
        seasonality.insert(9, 1.05);
        seasonality.insert(10, 1.0);
        seasonality.insert(11, 0.95);
        seasonality.insert(12, 1.0);

        Config {
            subscribers: 100_000,
            cells: 2000,
            prefixes: vec![
                "31612".to_string(),
                "31613".to_string(),
                "31620".to_string(),
                "31621".to_string(),
            ],
            mccmnc_pool: vec![
                "20408".to_string(),
                "20416".to_string(),
                "20420".to_string(),
            ],
            center_lat: 52.37,
            center_lon: 4.895,
            radius_km: 50.0,
            avg_calls_per_user: 3.5,
            avg_sms_per_user: 5.2,
            avg_data_sessions_per_user: 12.0,
            mo_share_call: 0.5,
            mo_share_sms: 0.5,
            imei_daily_change_prob: 0.02,
            call_dispositions,
            call_duration_quantiles: CallDurationQuantiles {
                p50: 75,
                p90: 240,
                p99: 600,
            },
            interconnect_share: 0.15,
            diurnal_weekday: vec![
                0.3, 0.2, 0.15, 0.1, 0.1, 0.15,  // 00-05
                0.3, 0.6, 1.2, 1.4, 1.3, 1.2,     // 06-11
                1.1, 1.0, 1.1, 1.2, 1.3, 1.5,     // 12-17
                1.6, 1.4, 1.2, 1.0, 0.7, 0.5,     // 18-23
            ],
            diurnal_weekend: vec![
                0.2, 0.15, 0.1, 0.1, 0.1, 0.1,    // 00-05
                0.2, 0.3, 0.5, 0.8, 1.0, 1.2,     // 06-11
                1.3, 1.2, 1.1, 1.0, 1.1, 1.3,     // 12-17
                1.4, 1.3, 1.2, 1.0, 0.6, 0.4,     // 18-23
            ],
            seasonality,
            special_days: HashMap::new(),
            rotate_bytes: 100_000_000,
            tz_name: DEFAULT_TZ_NAME.to_string(),
            workers: 0,
            event_pool_size: 10_000,           // 10K EventRow objects per worker
            batch_size_bytes: 10_485_760,      // 10MB batch size
            writer_tasks: 0,                   // Auto-detect (workers / 2)
            subscriber_db_path: None,
            generate_subscriber_db: None,
            db_size: 10_000,
            db_history_days: 365,
            db_device_change_rate: 0.15,
            db_number_release_rate: 0.05,
            db_cooldown_days: 90,
            validate_db_only: false,
        }
    }
}

/// Parse comma-separated phone number prefixes
/// Validates that each prefix is 3-6 digits
pub fn parse_prefixes(prefixes_str: &str) -> anyhow::Result<Vec<String>> {
    if prefixes_str.is_empty() {
        return Ok(Config::default().prefixes);
    }

    let parts: Vec<String> = prefixes_str
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let mut result = Vec::new();
    for p in parts {
        if !p.chars().all(|c| c.is_ascii_digit()) || p.len() < 3 || p.len() > 6 {
            anyhow::bail!("Invalid prefix: {}. Must be 3-6 digits.", p);
        }
        result.push(p);
    }

    if result.is_empty() {
        Ok(Config::default().prefixes)
    } else {
        Ok(result)
    }
}

/// Load configuration from YAML file and merge with defaults
pub fn load_config(config_path: Option<&Path>) -> anyhow::Result<Config> {
    let mut config = Config::default();

    if let Some(path) = config_path {
        if path.exists() {
            let contents = std::fs::read_to_string(path)?;
            let user_config: serde_yaml::Value = serde_yaml::from_str(&contents)?;

            // Merge user config with defaults
            if let serde_yaml::Value::Mapping(map) = user_config {
                for (key, value) in map {
                    if let serde_yaml::Value::String(key_str) = key {
                        merge_config_value(&mut config, &key_str, value);
                    }
                }
            }
        }
    }

    Ok(config)
}

fn merge_config_value(config: &mut Config, key: &str, value: serde_yaml::Value) {
    match key {
        "subscribers" => {
            if let Some(v) = value.as_u64() {
                config.subscribers = v as usize;
            }
        }
        "cells" => {
            if let Some(v) = value.as_u64() {
                config.cells = v as usize;
            }
        }
        "prefixes" => {
            if let Some(arr) = value.as_sequence() {
                config.prefixes = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            }
        }
        "mccmnc_pool" => {
            if let Some(arr) = value.as_sequence() {
                config.mccmnc_pool = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            }
        }
        "center_lat" => {
            if let Some(v) = value.as_f64() {
                config.center_lat = v;
            }
        }
        "center_lon" => {
            if let Some(v) = value.as_f64() {
                config.center_lon = v;
            }
        }
        "radius_km" => {
            if let Some(v) = value.as_f64() {
                config.radius_km = v;
            }
        }
        "avg_calls_per_user" => {
            if let Some(v) = value.as_f64() {
                config.avg_calls_per_user = v;
            }
        }
        "avg_sms_per_user" => {
            if let Some(v) = value.as_f64() {
                config.avg_sms_per_user = v;
            }
        }
        "avg_data_sessions_per_user" => {
            if let Some(v) = value.as_f64() {
                config.avg_data_sessions_per_user = v;
            }
        }
        "mo_share_call" => {
            if let Some(v) = value.as_f64() {
                config.mo_share_call = v;
            }
        }
        "mo_share_sms" => {
            if let Some(v) = value.as_f64() {
                config.mo_share_sms = v;
            }
        }
        "tz_name" => {
            if let Some(v) = value.as_str() {
                config.tz_name = v.to_string();
            }
        }
        "workers" => {
            if let Some(v) = value.as_u64() {
                config.workers = v as usize;
            }
        }
        "event_pool_size" => {
            if let Some(v) = value.as_u64() {
                config.event_pool_size = v as usize;
            }
        }
        "batch_size_bytes" => {
            if let Some(v) = value.as_u64() {
                config.batch_size_bytes = v as usize;
            }
        }
        "writer_tasks" => {
            if let Some(v) = value.as_u64() {
                config.writer_tasks = v as usize;
            }
        }
        "rotate_bytes" => {
            if let Some(v) = value.as_u64() {
                config.rotate_bytes = v;
            }
        }
        "db_size" => {
            if let Some(v) = value.as_u64() {
                config.db_size = v as usize;
            }
        }
        "db_history_days" => {
            if let Some(v) = value.as_u64() {
                config.db_history_days = v as usize;
            }
        }
        "db_device_change_rate" => {
            if let Some(v) = value.as_f64() {
                config.db_device_change_rate = v;
            }
        }
        "db_number_release_rate" => {
            if let Some(v) = value.as_f64() {
                config.db_number_release_rate = v;
            }
        }
        "db_cooldown_days" => {
            if let Some(v) = value.as_u64() {
                config.db_cooldown_days = v as usize;
            }
        }
        _ => {}
    }
}
