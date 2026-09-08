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
use crossbeam_channel::bounded;
use rayon::prelude::*;
use rs_cdr_generator::async_writer::{writer_task, WriterMessage};
use rs_cdr_generator::cells::{ensure_cells_catalog, load_cells_catalog};
use rs_cdr_generator::config::{load_config, parse_prefixes, Config};
use rs_cdr_generator::generators::worker_generate_shard;
use rs_cdr_generator::subscriber_db_generator::{generate_database_redb, GeneratorConfig};
use rs_cdr_generator::subscriber_db_redb::SubscriberDbRedb;
use rs_cdr_generator::timezone_utils::tz_from_name;
use rs_cdr_generator::utils::create_daily_summary;
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

    // Контрольный вывод прочитанных значений вложенного конфига контура —
    // критерий готовности этапа 2 (docs/field-mapping.md): показать
    // разобранные значения выводом программы, а не рассуждением.
    if cfg.contour_seed.is_some() || cfg.contour_time_range_start.is_some() {
        println!("Конфиг контура прочитан:");
        println!("  meta.seed = {:?}", cfg.contour_seed);
        println!(
            "  meta.time_range = {:?} .. {:?}",
            cfg.contour_time_range_start, cfg.contour_time_range_end
        );
        println!("  subscribers.total_count = {}", cfg.subscribers);
        println!("  network.elements (id) = {:?}", cfg.network_elements);
        println!();
    }

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
    // Этап 3: реальные MSISDN абонентов, как они лежат в базе — не
    // арифметика по индексу (docs/field-mapping.md, «Найденный попутно
    // дефект»). Общий на весь прогон и на всех воркеров сразу (этап 4,
    // требование 2) — оборачиваем в Arc и раздаём каждому work item целиком,
    // а не своим куском: собеседник звонка выбирается из ВСЕГО пула.
    let all_msisdns = Arc::new(redb.list_all_msisdns()?);
    let subs = all_msisdns.len();
    println!("Loaded {} subscribers from database\n", subs);

    // Книга контактов строится РАЗ на весь прогон, здесь, а не внутри
    // worker_generate_shard — иначе она пересобиралась бы на каждый work
    // item (день × кусок пула) и круг общения абонента прыгал бы между
    // сутками (требование этапа: детерминизм от seed и состава абонентов,
    // независимость от периода дат и числа воркеров). Seed книги — свой,
    // не завязанный на day_idx/chunk_idx, которые определяют seed события.
    let book_seed = seed.wrapping_add(0x636f6e74616374); // "contact" в hex, чтобы не совпасть с seed событий
    let contact_book = Arc::new(rs_cdr_generator::contact_book::ContactBook::build(&all_msisdns, book_seed));
    println!("Contact book built for {} subscribers\n", subs);
    if std::env::var("CB_DEBUG").is_ok() {
        let lens: Vec<usize> = all_msisdns.iter().map(|m| contact_book.contacts_of(*m).len()).collect();
        let empty = lens.iter().filter(|&&l| l == 0).count();
        let mean = lens.iter().sum::<usize>() as f64 / lens.len().max(1) as f64;
        eprintln!("CB_DEBUG n={} empty={} mean_degree={:.2} sample_first={:?}", lens.len(), empty, mean, &all_msisdns[..3.min(all_msisdns.len())].iter().map(|m| contact_book.contacts_of(*m).to_vec()).collect::<Vec<_>>());
    }

    let redb_arc = Arc::new(redb);

    // Дни и день-каталоги готовим заранее — воркеры разных дней теперь
    // работают одновременно (этап 4), а не по очереди.
    let mut days_vec = Vec::with_capacity(days);
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
        std::fs::create_dir_all(out.join(&day_str))?;
        days_vec.push(day);
    }

    // Этап 4: ось параллелизма — даты (и внутри дня — куски общего пула
    // абонентов размером cfg.chunk_size, чтобы rayon было чем занять
    // воркеры даже при одном-двух днях в прогоне — «или по парам
    // элемент-сутки» из задания реализовано как «день × кусок пула»,
    // так же однозначно закрепляющее файл (ne_id, дата) за воркерами,
    // как и деление по элементам, но без завязки на их число).
    // Было: внешний цикл по дням последовательный, rayon резал только
    // абонентов внутри дня (main.rs, ранее ~строка 400).
    let chunk_size = cfg.chunk_size.max(1);
    let mut work_items: Vec<(usize, usize, (usize, usize))> = Vec::new();
    // Сколько кусков пула приходится на каждый день — нужно, чтобы понять,
    // когда день закрыт целиком (все его куски отправили свои пачки) и можно
    // закрыть его файлы у writer-задач, не дожидаясь конца всего прогона.
    let mut chunks_per_day: Vec<usize> = Vec::with_capacity(days);
    for (day_idx, _) in days_vec.iter().enumerate() {
        let mut chunk_idx = 0usize;
        let mut s = 0usize;
        while s < subs {
            let e = (s + chunk_size).min(subs);
            work_items.push((day_idx, chunk_idx, (s, e)));
            s = e;
            chunk_idx += 1;
        }
        if subs == 0 {
            // Пустая база — всё равно один пустой work item, чтобы день
            // получил свой (пустой) стат-файл и попал в дневную сводку.
            work_items.push((day_idx, 0, (0, 0)));
            chunk_idx = 1;
        }
        chunks_per_day.push(chunk_idx);
    }
    let date_compacts: Vec<String> = days_vec.iter().map(|d| d.format("%Y%m%d").to_string()).collect();
    // 🔴 Источник роста пика RSS с числом дней в прогоне (проверено:
    // --workers 2, 2 дня → 315 МБ, 8 дней → 919 МБ, 32 дня → 1236 МБ) — не
    // число одновременно работающих воркеров (оно уже ограничено пулом
    // rayon ниже), а то, что writer-задача, живущая весь прогон, держит
    // открытыми файлы ВСЕХ дней сразу. `day_remaining` считает, сколько
    // кусков дня ещё не отправили свои пачки; когда счётчик доходит до 0,
    // всем writer-задачам уходит CloseDate — они закрывают файлы этой даты
    // и больше не держат их открытыми до конца прогона.
    let day_remaining: Vec<std::sync::atomic::AtomicUsize> = chunks_per_day
        .iter()
        .map(|&n| std::sync::atomic::AtomicUsize::new(n))
        .collect();

    // Determine number of writer tasks (default: workers / 2)
    let w = cfg.workers;
    let writer_tasks = if cfg.writer_tasks > 0 {
        cfg.writer_tasks
    } else {
        (w / 2).max(1)
    };

    // Этап 5: writer-задачи живут ВЕСЬ прогон (не пересоздаются на каждый
    // день, как было раньше) — события разных дней и разных work item'ов
    // могут прийти в одну и ту же задачу, но каждую пару (ne_id, дата)
    // маршрутизирует к задаче ровно один и тот же индекс (generators.rs,
    // route_writer_idx), поэтому коллизии открытия файла нет и
    // writer_tasks > 1 больше не запрещён.
    let rt = tokio::runtime::Runtime::new()?;
    let mut writer_channels = Vec::new();
    let mut writer_handles = Vec::new();
    // Канал ОГРАНИЧЕН, а не unbounded: этап 4 запустил work item'ы ВСЕХ дней
    // сразу (rayon.par_iter по флоскому work_items), и без ограничения
    // производители обгоняют writer-задачи — бэклог пачек в канале растёт
    // пропорционально суммарному объёму сгенерированного, то есть пику RSS,
    // растущему с числом дней (проверено прогоном: 2 дня → 351 МБ,
    // 8 дней (4×) → 1671 МБ, почти линейно). Ограничение возвращает
    // потоковую запись: производитель блокируется на send(), когда писатель
    // отстаёт, и бэклог не может расти неограниченно.
    for shard_id in 0..writer_tasks {
        let (tx, rx) = bounded(64);
        writer_channels.push(tx);
        let out_dir = out.clone();
        let handle = rt.spawn(async move { writer_task(rx, out_dir, shard_id).await });
        writer_handles.push(handle);
    }

    // Run all (день, кусок пула) work items in parallel.
    //
    // 🔴 Пул rayon ограничен cfg.workers явно, а не глобальным дефолтом
    // (= число ядер). Раньше в один момент времени работало ровно `w` задач
    // (`ranges` — массив длины `w`, по одной на воркер, один день за раз);
    // при плоском списке work_items (день × кусок пула) число элементов
    // стало равно days × chunks_per_day, и глобальный пул rayon без
    // ограничения планирует СТОЛЬКО задач параллельно, сколько ядер —
    // это даёт пик RSS, растущий с числом дней (проверено прогоном: 2 дня →
    // 354 МБ, 8 дней (4×) → 1710 МБ, почти линейно), даже после того как
    // канал писателя стал ограниченным (bounded(64) выше не помог — узкое
    // место было не в канале, а в числе одновременно работающих
    // производителей). Явный лимит возвращает потоковую запись: параллельно
    // всегда не больше `w` кусков, независимо от того, сколько дней в прогоне.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.workers)
        .build()?;
    pool.install(|| {
        work_items.par_iter().try_for_each(|&(day_idx, chunk_idx, range)| {
            worker_generate_shard(
                days_vec[day_idx],
                day_idx,
                chunk_idx,
                range,
                &all_msisdns,
                &contact_book,
                &cfg,
                &out,
                &redb_arc,
                &writer_channels,
                seed,
            )?;

            // Этот work item отправил все свои пачки — если это был
            // последний непогашенный кусок дня, день закрыт целиком:
            // рассылаем CloseDate, чтобы writer-задачи освободили файлы
            // этой даты сразу, а не держали их до конца всего прогона.
            let remaining = day_remaining[day_idx]
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            if remaining == 1 {
                for tx in &writer_channels {
                    tx.send(WriterMessage::CloseDate(date_compacts[day_idx].clone()))?;
                }
            }
            Ok::<(), anyhow::Error>(())
        })
    })?;

    // Send Close messages to all writers
    for tx in writer_channels {
        tx.send(WriterMessage::Close)?;
    }

    // Wait for all writer tasks to complete
    for handle in writer_handles {
        rt.block_on(handle)??;
    }

    // Дневные сводки — все дни уже сгенерированы (стат-файлы лежат по
    // out/<day_str>/stats_shard_d*_c*.json), считаем сводку по каждому дню.
    for day in &days_vec {
        create_daily_summary(&out, day)?;
        println!(
            "Day {} done → {:?}",
            day.format("%Y-%m-%d"),
            out
        );
    }
    // Раньше здесь был bundle_day — склейка шард-файлов в один архив дня.
    // Больше не нужен: итоговые файлы уже в формате контура
    // (out/<ne_id>/CDR_{ne_id}_{date}.csv.gz), один на (ne_id, дата),
    // и являются готовой поставкой сами по себе — упаковывать их в
    // ещё один архив незачем и ломало бы ожидаемое имя файла.
    let _ = cleanup_after_archive;

    if std::env::var("CB_DEBUG").is_ok() {
        use std::sync::atomic::Ordering::Relaxed;
        eprintln!(
            "CB_DEBUG selection: ext={} book={} fallback={} rand={}",
            rs_cdr_generator::generators::DBG_EXT.load(Relaxed),
            rs_cdr_generator::generators::DBG_BOOK.load(Relaxed),
            rs_cdr_generator::generators::DBG_FALLBACK.load(Relaxed),
            rs_cdr_generator::generators::DBG_RAND.load(Relaxed),
        );
    }
    println!("\n=== CDR Generation Complete ===");

    Ok(())
}
