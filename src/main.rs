// Unified CDR generator (calls + SMS + data) for large-scale synthetic telecom datasets.
//
// Version 5.1 - Rust port with full behavioral compatibility.
//
// Features:
// - One semicolon-delimited CSV for CALL/SMS/DATA with unified minimal spec
// - Timestamps in milliseconds since Unix epoch, with timezone info
// - File rotation at ~100 MB; per-day TAR.GZ bundling
// - Deterministic with --seed
// - Parallel processing with rayon
// - Persistent cells catalog reused across runs
// - Stable subscriber identity: MSISDN ↔ IMSI ↔ MCCMNC

use chrono::{Datelike, Duration, TimeZone};
use clap::Parser;
use crossbeam_channel::unbounded;
use rayon::prelude::*;
use rs_cdr_generator::async_writer::{writer_task, WriterMessage};
use rs_cdr_generator::cells::{ensure_cells_catalog, load_cells_catalog};
use rs_cdr_generator::config::{load_config, parse_prefixes, Config};
use rs_cdr_generator::generators::worker_generate;
use rs_cdr_generator::subscriber_db::SubscriberDatabase;
use rs_cdr_generator::subscriber_db_generator::{export_to_csv, generate_database, GeneratorConfig};
use rs_cdr_generator::timezone_utils::tz_from_name;
use rs_cdr_generator::utils::{bundle_day, create_daily_summary};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "rs_cdr_generator")]
#[command(about = "Unified CDR generator (CALL/SMS/DATA)", long_about = None)]
struct Args {
    /// Количество абонентов
    #[arg(long, default_value = "100000")]
    subs: usize,

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

    // Subscriber database options
    /// Путь к CSV файлу с базой абонентов
    #[arg(long)]
    subscriber_db: Option<PathBuf>,

    /// Генерировать базу абонентов и сохранить в файл
    #[arg(long)]
    generate_db: Option<PathBuf>,

    /// Размер генерируемой базы абонентов
    #[arg(long)]
    db_size: Option<usize>,

    /// Период истории базы абонентов (дни)
    #[arg(long)]
    db_history_days: Option<usize>,

    /// Вероятность смены устройства в год [0..1]
    #[arg(long)]
    db_device_change_rate: Option<f64>,

    /// Вероятность освобождения номера в год [0..1]
    #[arg(long)]
    db_number_release_rate: Option<f64>,

    /// Дни "остывания" номера перед переназначением
    #[arg(long)]
    db_cooldown_days: Option<usize>,

    /// Только валидировать базу абонентов (не генерировать CDR)
    #[arg(long, default_value = "false")]
    validate_db: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load and merge configuration with CLI priority
    let mut cfg = if let Some(config_path) = &args.config {
        load_config(Some(config_path))?
    } else {
        Config::default()
    };

    // CLI overrides YAML (only if explicitly provided)
    if let Some(prefixes_str) = &args.prefixes {
        cfg.prefixes = parse_prefixes(prefixes_str)?;
    }

    if let Some(rb) = args.rotate_bytes {
        cfg.rotate_bytes = rb;
    }

    if let Some(w) = args.workers {
        cfg.workers = if w == 0 {
            num_cpus::get()
        } else {
            w.max(1)
        };
    } else if cfg.workers == 0 {
        cfg.workers = num_cpus::get();
    }

    if let Some(tz) = &args.tz {
        cfg.tz_name = tz.clone();
    }

    if let Some(mo) = args.mo_share_call {
        cfg.mo_share_call = mo.max(0.0).min(1.0);
    }

    if let Some(mo) = args.mo_share_sms {
        cfg.mo_share_sms = mo.max(0.0).min(1.0);
    }

    if let Some(prob) = args.imei_change_prob {
        cfg.imei_daily_change_prob = prob.max(0.0).min(1.0);
    }

    // Subscriber database options
    cfg.subscriber_db_path = args.subscriber_db.clone();
    cfg.generate_subscriber_db = args.generate_db.clone();
    cfg.validate_db_only = args.validate_db;

    if let Some(size) = args.db_size {
        cfg.db_size = size;
    }
    if let Some(days) = args.db_history_days {
        cfg.db_history_days = days;
    }
    if let Some(rate) = args.db_device_change_rate {
        cfg.db_device_change_rate = rate.max(0.0).min(1.0);
    }
    if let Some(rate) = args.db_number_release_rate {
        cfg.db_number_release_rate = rate.max(0.0).min(1.0);
    }
    if let Some(cooldown) = args.db_cooldown_days {
        cfg.db_cooldown_days = cooldown;
    }

    // Handle subscriber database generation if requested
    if let Some(ref gen_path) = cfg.generate_subscriber_db {
        println!("Generating subscriber database...");

        let gen_config = GeneratorConfig {
            initial_subscribers: cfg.db_size,
            history_days: cfg.db_history_days,
            device_change_rate: cfg.db_device_change_rate,
            number_release_rate: cfg.db_number_release_rate,
            cooldown_days: cfg.db_cooldown_days,
            prefixes: cfg.prefixes.clone(),
            mccmnc_pool: cfg.mccmnc_pool.clone(),
            seed: args.seed,
            start_timestamp_ms: 1704067200000, // 2024-01-01
        };

        let events = generate_database(&gen_config)?;
        export_to_csv(&events, gen_path)?;

        println!("Subscriber database generated successfully!");

        // If only generation was requested (no CDR generation), exit
        if cfg.subscriber_db_path.is_none() && !cfg.validate_db_only {
            return Ok(());
        }
    }

    // Handle subscriber database validation if requested
    if cfg.validate_db_only {
        if let Some(ref db_path) = cfg.subscriber_db_path {
            println!("Loading subscriber database from {:?}...", db_path);
            let db = SubscriberDatabase::load_from_csv(db_path)?;

            println!("Validating database...");
            db.validate()?;

            println!("✓ Database validation passed!");
            println!("  Events: {}", db.event_count());
            println!("  Unique IMSI: {}", db.unique_imsi_count());

            return Ok(());
        } else {
            eprintln!("Error: --validate-db requires --subscriber-db <path>");
            std::process::exit(1);
        }
    }

    // Load subscriber database if provided
    let subscriber_db = if let Some(ref db_path) = cfg.subscriber_db_path {
        println!("Loading subscriber database from {:?}...", db_path);
        let mut db = SubscriberDatabase::load_from_csv(db_path)?;

        println!("Validating database...");
        db.validate()?;

        println!("Building snapshots for fast lookup...");
        db.build_snapshots();

        println!("✓ Subscriber database loaded:");
        println!("  Events: {}", db.event_count());
        println!("  Snapshots: {}", db.snapshot_count());
        println!("  Unique IMSI: {}", db.unique_imsi_count());

        Some(db)
    } else {
        None
    };

    // Parse cell center from CLI or use config values
    let (center_lat, center_lon) = if let Some(ref cell_center_str) = args.cell_center {
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

    let cell_radius = args.cell_radius_km.unwrap_or(cfg.radius_km);
    let num_cells = args.cells.unwrap_or(cfg.cells);

    // Ensure cells catalog
    let cells_path = ensure_cells_catalog(
        &args.out,
        num_cells,
        center_lat,
        center_lon,
        cell_radius,
        args.seed,
    )?;

    let (_cells_all, _cells_by_rat) = load_cells_catalog(&cells_path)?;

    let tz = tz_from_name(&cfg.tz_name);

    // Parse start date
    let start_date = chrono::NaiveDate::parse_from_str(&args.start, "%Y-%m-%d")?;

    // Generate data for each day
    for d in 0..args.days {
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
        let day_dir = args.out.join(&day_str);
        std::fs::create_dir_all(&day_dir)?;

        // Split users uniformly across workers
        let w = cfg.workers;
        let subs = args.subs;
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

            let out_dir = args.out.clone();
            let day_str_clone = day_str.clone();

            let handle = rt.spawn(async move {
                writer_task(
                    rx,
                    out_dir,
                    day_str_clone,
                    shard_id,
                )
                .await
            });

            writer_handles.push(handle);
        }

        // Run workers in parallel with writer channels
        let sub_db_ref = subscriber_db.as_ref();
        ranges
            .par_iter()
            .enumerate()
            .try_for_each(|(i, &(lo, hi))| {
                // Map worker to writer shard (round-robin)
                let writer_idx = i % writer_tasks;
                let writer_tx = writer_channels[writer_idx].clone();

                worker_generate(day, i, (lo, hi), &cfg, &args.out, sub_db_ref, writer_tx)
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
        create_daily_summary(&args.out, &day)?;
        let tarfile_path = bundle_day(&args.out, &day, args.cleanup_after_archive)?;

        println!("Day {} done → {:?}", day_str, tarfile_path);
    }

    Ok(())
}
