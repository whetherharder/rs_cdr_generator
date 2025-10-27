// Unified CDR generator (calls + SMS + data) for large-scale synthetic telecom datasets.
//
// Version 5.2 - Restructured with subcommands and redb-only subscriber database
//
// Features:
// - One semicolon-delimited CSV for CALL/SMS/DATA with unified minimal spec
// - Timestamps in milliseconds since Unix epoch, with timezone info
// - File rotation at ~100 MB; per-day TAR.GZ bundling
// - Deterministic with --seed
// - Parallel processing with rayon
// - Persistent cells catalog reused across runs
// - Stable subscriber identity: MSISDN ↔ IMSI ↔ MCCMNC
// - redb-based subscriber database for efficient chunked processing

use chrono::{Datelike, Duration, TimeZone};
use clap::{Parser, Subcommand};
use crossbeam_channel::unbounded;
use rayon::prelude::*;
use rs_cdr_generator::async_writer::{writer_task, WriterMessage};
use rs_cdr_generator::cells::{ensure_cells_catalog, load_cells_catalog};
use rs_cdr_generator::config::{load_config, parse_prefixes, Config};
use rs_cdr_generator::generators::worker_generate;
use rs_cdr_generator::subscriber_db_generator::{generate_database_redb, GeneratorConfig};
use rs_cdr_generator::subscriber_db_redb::SubscriberDbRedb;
use rs_cdr_generator::timezone_utils::tz_from_name;
use rs_cdr_generator::utils::{bundle_day, create_daily_summary};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "rs_cdr_generator")]
#[command(about = "Unified CDR generator (CALL/SMS/DATA)", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Generate subscriber database in redb format
    GenerateSubscribers {
        /// Путь к выходному файлу базы данных (.redb)
        #[arg(short, long, default_value = "subscriber_db.redb")]
        output: PathBuf,

        /// Количество начальных абонентов
        #[arg(long, default_value = "100000")]
        size: usize,

        /// Период истории базы абонентов (дни)
        #[arg(long, default_value = "365")]
        history_days: usize,

        /// Вероятность смены устройства в год [0..1]
        #[arg(long, default_value = "0.15")]
        device_change_rate: f64,

        /// Вероятность освобождения номера в год [0..1]
        #[arg(long, default_value = "0.05")]
        number_release_rate: f64,

        /// Дни "остывания" номера перед переназначением
        #[arg(long, default_value = "90")]
        cooldown_days: usize,

        /// Префиксы без кода страны, через запятую
        #[arg(long)]
        prefixes: Option<String>,

        /// Seed для детерминизма
        #[arg(long, default_value = "42")]
        seed: u64,

        /// YAML конфиг (для prefixes и mccmnc_pool)
        #[arg(long)]
        config: Option<PathBuf>,
    },

    /// Generate CDR data from subscriber database
    GenerateCdr {
        /// Путь к базе данных подписчиков (redb)
        #[arg(long)]
        subscriber_db: PathBuf,

        /// Стартовая дата YYYY-MM-DD
        #[arg(long, default_value = "2025-01-01")]
        start: String,

        /// Сколько дней генерировать
        #[arg(long, default_value = "1")]
        days: usize,

        /// Каталог вывода
        #[arg(long, default_value = "out")]
        out: PathBuf,

        /// Seed для детерминизма
        #[arg(long, default_value = "42")]
        seed: u64,

        /// Префиксы без кода страны, через запятую
        #[arg(long)]
        prefixes: Option<String>,

        /// Предел размера файла (байт)
        #[arg(long)]
        rotate_bytes: Option<u64>,

        /// Число процессов (0 = auto-detect)
        #[arg(long)]
        workers: Option<usize>,

        /// YAML конфиг поверх дефолтов
        #[arg(long)]
        config: Option<PathBuf>,

        /// Таймзона для локального времени
        #[arg(long)]
        tz: Option<String>,

        /// Сколько сгенерировать вышек (cell_id)
        #[arg(long)]
        cells: Option<usize>,

        /// Центр (lat,lon) для генерации вышек
        #[arg(long)]
        cell_center: Option<String>,

        /// Радиус круга (км) для вышек
        #[arg(long)]
        cell_radius_km: Option<f64>,

        /// Вероятность MO для CALL [0..1]
        #[arg(long)]
        mo_share_call: Option<f64>,

        /// Вероятность MO для SMS [0..1]
        #[arg(long)]
        mo_share_sms: Option<f64>,

        /// Вероятность смены IMEI в день [0..1]
        #[arg(long)]
        imei_change_prob: Option<f64>,

        /// Удалять исходные файлы после архивации
        #[arg(long, default_value = "false")]
        cleanup_after_archive: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::GenerateSubscribers {
            output,
            size,
            history_days,
            device_change_rate,
            number_release_rate,
            cooldown_days,
            prefixes,
            seed,
            config,
        } => {
            handle_generate_subscribers(
                output,
                size,
                history_days,
                device_change_rate,
                number_release_rate,
                cooldown_days,
                prefixes,
                seed,
                config,
            )
        }
        Commands::GenerateCdr {
            subscriber_db,
            start,
            days,
            out,
            seed,
            prefixes,
            rotate_bytes,
            workers,
            config,
            tz,
            cells,
            cell_center,
            cell_radius_km,
            mo_share_call,
            mo_share_sms,
            imei_change_prob,
            cleanup_after_archive,
        } => {
            handle_generate_cdr(
                subscriber_db,
                start,
                days,
                out,
                seed,
                prefixes,
                rotate_bytes,
                workers,
                config,
                tz,
                cells,
                cell_center,
                cell_radius_km,
                mo_share_call,
                mo_share_sms,
                imei_change_prob,
                cleanup_after_archive,
            )
        }
    }
}

fn handle_generate_subscribers(
    output: PathBuf,
    size: usize,
    history_days: usize,
    device_change_rate: f64,
    number_release_rate: f64,
    cooldown_days: usize,
    prefixes: Option<String>,
    seed: u64,
    config_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    println!("=== Generating Subscriber Database ===\n");

    // Load config for prefixes and mccmnc_pool
    let cfg = if let Some(ref path) = config_path {
        load_config(Some(path))?
    } else {
        Config::default()
    };

    // Parse prefixes from CLI or use config
    let prefixes_list = if let Some(prefixes_str) = prefixes {
        parse_prefixes(&prefixes_str)?
    } else {
        cfg.prefixes.clone()
    };

    let gen_config = GeneratorConfig {
        initial_subscribers: size,
        history_days,
        device_change_rate: device_change_rate.max(0.0).min(1.0),
        number_release_rate: number_release_rate.max(0.0).min(1.0),
        cooldown_days,
        prefixes: prefixes_list,
        mccmnc_pool: cfg.mccmnc_pool.clone(),
        seed,
        start_timestamp_ms: 1704067200000, // 2024-01-01
    };

    generate_database_redb(&gen_config, &output)?;

    println!("\n=== Subscriber Database Generation Complete ===");
    println!("Database file: {:?}", output);

    Ok(())
}

fn handle_generate_cdr(
    subscriber_db: PathBuf,
    start: String,
    days: usize,
    out: PathBuf,
    seed: u64,
    prefixes: Option<String>,
    rotate_bytes: Option<u64>,
    workers: Option<usize>,
    config_path: Option<PathBuf>,
    tz: Option<String>,
    cells: Option<usize>,
    cell_center: Option<String>,
    cell_radius_km: Option<f64>,
    mo_share_call: Option<f64>,
    mo_share_sms: Option<f64>,
    imei_change_prob: Option<f64>,
    cleanup_after_archive: bool,
) -> anyhow::Result<()> {
    println!("=== Generating CDR Data ===\n");

    // Verify subscriber database exists
    if !subscriber_db.exists() {
        eprintln!("Error: Subscriber database not found: {:?}", subscriber_db);
        eprintln!("\nPlease generate a subscriber database first:");
        eprintln!("  rs_cdr_generator generate-subscribers --output subscriber_db.redb");
        std::process::exit(1);
    }

    // Load and merge configuration with CLI priority
    let mut cfg = if let Some(ref path) = config_path {
        load_config(Some(path))?
    } else {
        Config::default()
    };

    // Set subscriber database path
    cfg.subscriber_db_redb_path = Some(subscriber_db.clone());

    // CLI overrides YAML (only if explicitly provided)
    if let Some(prefixes_str) = prefixes {
        cfg.prefixes = parse_prefixes(&prefixes_str)?;
    }

    if let Some(rb) = rotate_bytes {
        cfg.rotate_bytes = rb;
    }

    if let Some(w) = workers {
        cfg.workers = if w == 0 {
            num_cpus::get()
        } else {
            w.max(1)
        };
    } else if cfg.workers == 0 {
        cfg.workers = num_cpus::get();
    }

    if let Some(tz_name) = tz {
        cfg.tz_name = tz_name;
    }

    if let Some(mo) = mo_share_call {
        cfg.mo_share_call = mo.max(0.0).min(1.0);
    }

    if let Some(mo) = mo_share_sms {
        cfg.mo_share_sms = mo.max(0.0).min(1.0);
    }

    if let Some(prob) = imei_change_prob {
        cfg.imei_daily_change_prob = prob.max(0.0).min(1.0);
    }

    // Parse cell center from CLI or use config values
    let (center_lat, center_lon) = if let Some(cell_center_str) = cell_center {
        let parts: Vec<&str> = cell_center_str.split(',').collect();
        if parts.len() == 2 {
            let lat = parts[0].trim().parse::<f64>().unwrap_or(cfg.center_lat);
            let lon = parts[1].trim().parse::<f64>().unwrap_or(cfg.center_lon);
            (lat, lon)
        } else {
            (cfg.center_lat, cfg.center_lon)
        }
    } else {
        (cfg.center_lat, cfg.center_lon)
    };

    let cell_radius = cell_radius_km.unwrap_or(cfg.radius_km);
    let num_cells = cells.unwrap_or(cfg.cells);

    // Ensure cells catalog
    let cells_path = ensure_cells_catalog(
        &out,
        num_cells,
        center_lat,
        center_lon,
        cell_radius,
        seed,
    )?;

    let (_cells_all, _cells_by_rat) = load_cells_catalog(&cells_path)?;

    let tz = tz_from_name(&cfg.tz_name);

    // Parse start date
    let start_date = chrono::NaiveDate::parse_from_str(&start, "%Y-%m-%d")?;

    // Open redb database (will be shared across all workers)
    println!("Loading subscriber database: {:?}", subscriber_db);
    let redb = SubscriberDbRedb::open(&subscriber_db)?;
    let subs = redb.count_msisdns()?;
    println!("Loaded {} subscribers from database\n", subs);

    let redb_arc = Arc::new(redb);

    // Generate data for each day
    for d in 0..days {
        let day_naive = start_date + Duration::days(d as i64);
        let day = tz
            .with_ymd_and_hms(
                day_naive.year(),
                day_naive.month(),
                day_naive.day(),
                0,
                0,
                0,
            )
            .unwrap();

        let day_str = day.format("%Y-%m-%d").to_string();
        let day_dir = out.join(&day_str);
        std::fs::create_dir_all(&day_dir)?;

        // Split users uniformly across workers
        let w = cfg.workers;
        let shard_size = subs / w;

        let mut ranges = Vec::new();
        let mut s = 0;
        for i in 0..w {
            let e = if i < w - 1 { s + shard_size } else { subs };
            ranges.push((s, e));
            s = e;
        }

        // Create Tokio runtime for async writers
        let rt = tokio::runtime::Runtime::new()?;

        // Determine number of writer tasks (default: workers / 2)
        let writer_tasks = if cfg.writer_tasks > 0 {
            cfg.writer_tasks
        } else {
            (w / 2).max(1)
        };

        // Create channels and spawn async writer tasks
        let mut writer_channels = Vec::new();
        let mut writer_handles = Vec::new();

        for shard_id in 0..writer_tasks {
            let (tx, rx) = unbounded();
            writer_channels.push(tx);

            let out_dir = out.clone();
            let day_str_clone = day_str.clone();
            let rotate_bytes = cfg.rotate_bytes;
            let compression_type = rs_cdr_generator::compression::CompressionType::from_str(&cfg.compression_type)
                .unwrap_or(rs_cdr_generator::compression::CompressionType::Gzip);

            let handle = rt.spawn(async move {
                writer_task(
                    rx,
                    out_dir,
                    day_str_clone,
                    shard_id,
                    rotate_bytes,
                    compression_type,
                )
                .await
            });

            writer_handles.push(handle);
        }

        // Run workers in parallel with writer channels
        ranges
            .par_iter()
            .enumerate()
            .try_for_each(|(i, &(lo, hi))| {
                // Map worker to writer shard (round-robin)
                let writer_idx = i % writer_tasks;
                let writer_tx = writer_channels[writer_idx].clone();

                worker_generate(day, i, (lo, hi), &cfg, &out, None, Some(&redb_arc), writer_tx)
            })?;

        // Send Close messages to all writers
        for tx in writer_channels {
            tx.send(WriterMessage::Close)?;
        }

        // Wait for all writer tasks to complete
        for handle in writer_handles {
            rt.block_on(handle)??;
        }

        // Create summary and bundle
        create_daily_summary(&out, &day)?;
        let compression_ext = rs_cdr_generator::compression::CompressionType::from_str(&cfg.compression_type)
            .unwrap_or(rs_cdr_generator::compression::CompressionType::Gzip)
            .extension();
        let tarfile_path = bundle_day(&out, &day, cleanup_after_archive, compression_ext)?;

        println!("Day {} done → {:?}", day_str, tarfile_path);
    }

    println!("\n=== CDR Generation Complete ===");

    Ok(())
}
