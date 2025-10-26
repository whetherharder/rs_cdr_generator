// Integration test for validating event generation counts
use chrono::TimeZone;
use rs_cdr_generator::cells::{ensure_cells_catalog, load_cells_catalog};
use rs_cdr_generator::config::{Config, parse_prefixes};
use rs_cdr_generator::generators::worker_generate;
use rs_cdr_generator::timezone_utils::tz_from_name;
use std::collections::{HashMap, HashSet};
use std::fs;
use tempfile::TempDir;

#[derive(Debug)]
struct EventCounts {
    total_calls: usize,
    total_sms: usize,
    total_data: usize,
    unique_src_msisdn_all: usize,
    unique_src_msisdn_data: usize,
    unique_src_msisdn_call_mo: usize,
    unique_src_msisdn_sms_mo: usize,
}


#[test]
fn test_event_generation_counts() -> anyhow::Result<()> {
    // Test parameters
    let num_subs = 1000;
    let num_workers = 4;
    let seed = 42u64;

    // Create temporary directory for output
    let temp_dir = TempDir::new()?;
    let out_dir = temp_dir.path().to_path_buf();

    // Setup configuration
    let mut cfg = Config::default();
    cfg.prefixes = parse_prefixes("31612,31613")?;
    cfg.mccmnc_pool = vec!["20408".to_string(), "20416".to_string()];
    cfg.avg_calls_per_user = 3.5;
    cfg.avg_sms_per_user = 5.2;
    cfg.avg_data_sessions_per_user = 12.0;
    cfg.workers = num_workers;
    cfg.rotate_bytes = 100_000_000; // 100MB - no rotation for this test

    // Ensure cells catalog
    let _cells_path = ensure_cells_catalog(
        &out_dir,
        2000,
        52.37,
        4.895,
        50.0,
        seed,
    )?;

    let (_cells_all, _cells_by_rat) = load_cells_catalog(&_cells_path)?;

    // Generate test date
    let tz = tz_from_name(&cfg.tz_name);
    let day = tz.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let day_str = day.format("%Y-%m-%d").to_string();
    let day_dir = out_dir.join(&day_str);
    fs::create_dir_all(&day_dir)?;

    // Calculate shard ranges
    let shard_size = num_subs / num_workers;
    let mut ranges = Vec::new();
    let mut s = 0;
    for i in 0..num_workers {
        let e = if i < num_workers - 1 {
            s + shard_size
        } else {
            num_subs
        };
        ranges.push((s, e));
        s = e;
    }

    // Generate events for each shard
    for (shard_id, &(lo, hi)) in ranges.iter().enumerate() {
        worker_generate(day, shard_id, (lo, hi), &cfg, &out_dir)?;
    }

    // Read and aggregate all CSV files
    let mut total_counts = EventCounts {
        total_calls: 0,
        total_sms: 0,
        total_data: 0,
        unique_src_msisdn_all: 0,
        unique_src_msisdn_data: 0,
        unique_src_msisdn_call_mo: 0,
        unique_src_msisdn_sms_mo: 0,
    };

    let mut all_src_msisdn = HashSet::new();
    let mut data_src_msisdn = HashSet::new();
    let mut call_mo_src_msisdn = HashSet::new();
    let mut sms_mo_src_msisdn = HashSet::new();

    // Find all CSV files
    for entry in fs::read_dir(&day_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            let content = fs::read_to_string(&path)?;
            let mut lines = content.lines();

            // Skip header
            lines.next();

            for line in lines {
                let fields: Vec<&str> = line.split(';').collect();
                if fields.len() < 4 {
                    continue;
                }

                let event_type = fields[0];
                let src_msisdn = fields[1];
                let direction = fields[3];

                all_src_msisdn.insert(src_msisdn.to_string());

                match event_type {
                    "CALL" => {
                        total_counts.total_calls += 1;
                        if direction == "MO" {
                            call_mo_src_msisdn.insert(src_msisdn.to_string());
                        }
                    }
                    "SMS" => {
                        total_counts.total_sms += 1;
                        if direction == "MO" {
                            sms_mo_src_msisdn.insert(src_msisdn.to_string());
                        }
                    }
                    "DATA" => {
                        total_counts.total_data += 1;
                        data_src_msisdn.insert(src_msisdn.to_string());
                    }
                    _ => {}
                }
            }
        }
    }

    total_counts.unique_src_msisdn_all = all_src_msisdn.len();
    total_counts.unique_src_msisdn_data = data_src_msisdn.len();
    total_counts.unique_src_msisdn_call_mo = call_mo_src_msisdn.len();
    total_counts.unique_src_msisdn_sms_mo = sms_mo_src_msisdn.len();

    // Print actual counts for debugging
    println!("\n=== Event Generation Test Results ===");
    println!("Subscribers: {}", num_subs);
    println!("Workers: {}", num_workers);
    println!("\nExpected averages per subscriber:");
    println!("  CALL events: {}", cfg.avg_calls_per_user);
    println!("  SMS events: {}", cfg.avg_sms_per_user);
    println!("  DATA events: {}", cfg.avg_data_sessions_per_user);
    println!("\nExpected totals (approximate):");
    println!("  CALL events: ~{}", (num_subs as f64 * cfg.avg_calls_per_user) as usize);
    println!("  SMS events: ~{}", (num_subs as f64 * cfg.avg_sms_per_user) as usize);
    println!("  DATA events: ~{}", (num_subs as f64 * cfg.avg_data_sessions_per_user) as usize);
    println!("\nActual results:");
    println!("  CALL events: {}", total_counts.total_calls);
    println!("  SMS events: {}", total_counts.total_sms);
    println!("  DATA events: {}", total_counts.total_data);
    println!("\nUnique subscribers (src_msisdn):");
    println!("  In DATA events: {}", total_counts.unique_src_msisdn_data);
    println!("  In MO CALL events: {}", total_counts.unique_src_msisdn_call_mo);
    println!("  In MO SMS events: {}", total_counts.unique_src_msisdn_sms_mo);
    println!("  Overall: {}", total_counts.unique_src_msisdn_all);

    // Validate event counts are within reasonable range (±20% due to Poisson distribution)
    let expected_calls = (num_subs as f64 * cfg.avg_calls_per_user) as usize;
    let expected_sms = (num_subs as f64 * cfg.avg_sms_per_user) as usize;
    let expected_data = (num_subs as f64 * cfg.avg_data_sessions_per_user) as usize;

    let tolerance = 0.20; // 20% tolerance for Poisson distribution

    // Check CALL events
    let call_lower = (expected_calls as f64 * (1.0 - tolerance)) as usize;
    let call_upper = (expected_calls as f64 * (1.0 + tolerance)) as usize;
    assert!(
        total_counts.total_calls >= call_lower && total_counts.total_calls <= call_upper,
        "CALL events {} not in expected range [{}, {}]",
        total_counts.total_calls,
        call_lower,
        call_upper
    );

    // Check SMS events
    let sms_lower = (expected_sms as f64 * (1.0 - tolerance)) as usize;
    let sms_upper = (expected_sms as f64 * (1.0 + tolerance)) as usize;
    assert!(
        total_counts.total_sms >= sms_lower && total_counts.total_sms <= sms_upper,
        "SMS events {} not in expected range [{}, {}]",
        total_counts.total_sms,
        sms_lower,
        sms_upper
    );

    // Check DATA events
    let data_lower = (expected_data as f64 * (1.0 - tolerance)) as usize;
    let data_upper = (expected_data as f64 * (1.0 + tolerance)) as usize;
    assert!(
        total_counts.total_data >= data_lower && total_counts.total_data <= data_upper,
        "DATA events {} not in expected range [{}, {}]",
        total_counts.total_data,
        data_lower,
        data_upper
    );

    // Check unique subscribers in DATA events (should be close to num_subs)
    // Due to Poisson distribution with mean=12, probability of 0 events is ~0.000006
    // So we expect at least 99% of subscribers to have DATA events
    let min_unique_data_subs = (num_subs as f64 * 0.99) as usize;
    assert!(
        total_counts.unique_src_msisdn_data >= min_unique_data_subs,
        "Unique DATA subscribers {} less than expected minimum {} (99% of {})",
        total_counts.unique_src_msisdn_data,
        min_unique_data_subs,
        num_subs
    );

    // Check that we don't have MORE unique subscribers than we generated
    // Note: total unique can be higher due to MT events with random contacts
    // But DATA events should only have our subscribers
    assert!(
        total_counts.unique_src_msisdn_data <= num_subs,
        "Unique DATA subscribers {} exceeds total subscribers {}",
        total_counts.unique_src_msisdn_data,
        num_subs
    );

    println!("\n✅ All validation checks passed!");

    Ok(())
}

#[test]
fn test_no_duplicate_subscribers_across_shards() -> anyhow::Result<()> {
    // Test that each subscriber appears in exactly one shard
    let num_subs = 500;
    let num_workers = 4;
    let seed = 123u64;

    let temp_dir = TempDir::new()?;
    let out_dir = temp_dir.path().to_path_buf();

    let mut cfg = Config::default();
    cfg.prefixes = parse_prefixes("31612")?;
    cfg.mccmnc_pool = vec!["20408".to_string()];
    cfg.avg_data_sessions_per_user = 5.0; // Lower to ensure most subs have events
    cfg.workers = num_workers;
    cfg.rotate_bytes = 100_000_000;

    let _cells_path = ensure_cells_catalog(&out_dir, 1000, 52.37, 4.895, 50.0, seed)?;
    let (_cells_all, _cells_by_rat) = load_cells_catalog(&_cells_path)?;

    let tz = tz_from_name(&cfg.tz_name);
    let day = tz.with_ymd_and_hms(2025, 2, 1, 0, 0, 0).unwrap();
    let day_str = day.format("%Y-%m-%d").to_string();
    let day_dir = out_dir.join(&day_str);
    fs::create_dir_all(&day_dir)?;

    let shard_size = num_subs / num_workers;
    let mut ranges = Vec::new();
    let mut s = 0;
    for i in 0..num_workers {
        let e = if i < num_workers - 1 {
            s + shard_size
        } else {
            num_subs
        };
        ranges.push((s, e));
        s = e;
    }

    for (shard_id, &(lo, hi)) in ranges.iter().enumerate() {
        worker_generate(day, shard_id, (lo, hi), &cfg, &out_dir)?;
    }

    // Collect DATA event subscribers per shard
    let mut shard_subscribers: HashMap<usize, HashSet<String>> = HashMap::new();

    for entry in fs::read_dir(&day_dir)? {
        let entry = entry?;
        let path = entry.path();
        let filename = path.file_name().unwrap().to_str().unwrap();

        // Extract shard_id from filename
        if filename.starts_with("cdr_") && filename.contains("_shard") {
            let parts: Vec<&str> = filename.split('_').collect();
            if parts.len() >= 4 {
                let shard_str = parts[3].replace("shard", "");
                if let Ok(shard_id) = shard_str.parse::<usize>() {
                    let content = fs::read_to_string(&path)?;
                    let mut subs = HashSet::new();

                    for line in content.lines().skip(1) {
                        let fields: Vec<&str> = line.split(';').collect();
                        if fields.len() >= 4 && fields[0] == "DATA" {
                            subs.insert(fields[1].to_string());
                        }
                    }

                    shard_subscribers.insert(shard_id, subs);
                }
            }
        }
    }

    println!("\n=== Shard Subscriber Distribution ===");
    for (shard_id, subs) in shard_subscribers.iter() {
        println!("Shard {}: {} unique DATA subscribers", shard_id, subs.len());
    }

    // Check for overlaps between shards
    let shard_ids: Vec<usize> = shard_subscribers.keys().copied().collect();
    for i in 0..shard_ids.len() {
        for j in (i + 1)..shard_ids.len() {
            let shard_i = shard_ids[i];
            let shard_j = shard_ids[j];

            let subs_i = &shard_subscribers[&shard_i];
            let subs_j = &shard_subscribers[&shard_j];

            let overlap: HashSet<_> = subs_i.intersection(subs_j).collect();

            assert!(
                overlap.is_empty(),
                "Found {} overlapping subscribers between shard {} and shard {}",
                overlap.len(),
                shard_i,
                shard_j
            );
        }
    }

    println!("✅ No subscriber overlaps between shards!");

    Ok(())
}
