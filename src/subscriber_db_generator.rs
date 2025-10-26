// Generator for synthetic subscriber database with realistic history
use crate::identity::gen_imei;
use crate::subscriber_db::{SubscriberEvent, SubscriberEventType};
use crate::subscriber_db_arrow::write_events_to_arrow;
use anyhow::Result;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::SeedableRng;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Configuration for subscriber database generation
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Number of initial subscribers
    pub initial_subscribers: usize,
    /// History period in days
    pub history_days: usize,
    /// Annual device change rate (0.0 - 1.0)
    pub device_change_rate: f64,
    /// Annual number release rate (0.0 - 1.0)
    pub number_release_rate: f64,
    /// Cooldown period in days before reassigning released numbers
    pub cooldown_days: usize,
    /// Phone number prefixes
    pub prefixes: Vec<String>,
    /// MCC+MNC pool
    pub mccmnc_pool: Vec<String>,
    /// Random seed
    pub seed: u64,
    /// Start timestamp (milliseconds)
    pub start_timestamp_ms: i64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        GeneratorConfig {
            initial_subscribers: 1000,
            history_days: 365,
            device_change_rate: 0.15,
            number_release_rate: 0.05,
            cooldown_days: 90,
            prefixes: vec!["31612".to_string(), "31613".to_string()],
            mccmnc_pool: vec!["20408".to_string(), "20416".to_string()],
            seed: 42,
            start_timestamp_ms: 1704067200000, // 2024-01-01
        }
    }
}

/// Subscriber state during generation
#[derive(Debug, Clone)]
struct ActiveSubscriber {
    imsi: String,
    msisdn: String,
    imei: u64,
    mccmnc: String,
    #[allow(dead_code)]
    activation_time: i64,
}

/// Released phone number in cooldown
#[derive(Debug, Clone)]
struct ReleasedNumber {
    msisdn: String,
    release_time: i64,
}

/// Generate subscriber database with realistic history
pub fn generate_database(config: &GeneratorConfig) -> Result<Vec<SubscriberEvent>> {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut events = Vec::new();
    let mut active_subscribers: HashMap<String, ActiveSubscriber> = HashMap::new();
    let mut released_numbers: Vec<ReleasedNumber> = Vec::new();
    let mut used_msisdns: HashSet<String> = HashSet::new();
    let mut imsi_counter = 0u64;

    let ms_per_day = 86400000i64;

    // Helper: generate unique MSISDN
    let gen_msisdn = |rng: &mut StdRng, used: &HashSet<String>, prefixes: &[String]| -> String {
        loop {
            let prefix = prefixes.choose(rng).unwrap();
            let number = rng.gen_range(0..10_000_000);
            let msisdn = format!("{}{:07}", prefix, number);
            if !used.contains(&msisdn) {
                return msisdn;
            }
        }
    };

    // Helper: generate unique IMSI
    let gen_imsi = |counter: &mut u64, mccmnc_pool: &[String]| -> String {
        let mccmnc = mccmnc_pool[(*counter as usize) % mccmnc_pool.len()].to_string();
        let msin = *counter % 10_000_000_000u64;
        *counter += 1;
        format!("{}{:010}", mccmnc, msin)
    };

    // Step 1: Create initial subscribers
    println!(
        "Generating {} initial subscribers...",
        config.initial_subscribers
    );
    for _ in 0..config.initial_subscribers {
        let imsi = gen_imsi(&mut imsi_counter, &config.mccmnc_pool);
        let msisdn = gen_msisdn(&mut rng, &used_msisdns, &config.prefixes);
        let imei = gen_imei(&mut rng);
        let mccmnc = config.mccmnc_pool.choose(&mut rng).unwrap().clone();

        used_msisdns.insert(msisdn.clone());

        events.push(SubscriberEvent {
            timestamp_ms: config.start_timestamp_ms,
            event_type: SubscriberEventType::NewSubscriber,
            imsi: imsi.clone(),
            msisdn: Some(msisdn.clone()),
            imei: Some(imei.to_string()),
            mccmnc: mccmnc.clone(),
        });

        active_subscribers.insert(
            imsi.clone(),
            ActiveSubscriber {
                imsi,
                msisdn,
                imei,
                mccmnc,
                activation_time: config.start_timestamp_ms,
            },
        );
    }

    // Step 2: Generate events over time
    println!("Generating historical events over {} days...", config.history_days);

    // Calculate daily event probabilities
    let device_change_daily_prob = 1.0 - (1.0 - config.device_change_rate).powf(1.0 / 365.0);
    let number_release_daily_prob = 1.0 - (1.0 - config.number_release_rate).powf(1.0 / 365.0);

    let cooldown_ms = config.cooldown_days as i64 * ms_per_day;

    for day in 1..config.history_days {
        let current_time = config.start_timestamp_ms + (day as i64 * ms_per_day);

        // Process device changes
        let subscribers: Vec<String> = active_subscribers.keys().cloned().collect();
        for imsi in &subscribers {
            if rng.gen::<f64>() < device_change_daily_prob {
                if let Some(sub) = active_subscribers.get_mut(imsi) {
                    let new_imei = gen_imei(&mut rng);
                    events.push(SubscriberEvent {
                        timestamp_ms: current_time,
                        event_type: SubscriberEventType::ChangeDevice,
                        imsi: sub.imsi.clone(),
                        msisdn: Some(sub.msisdn.clone()),
                        imei: Some(new_imei.to_string()),
                        mccmnc: sub.mccmnc.clone(),
                    });
                    sub.imei = new_imei;
                }
            }
        }

        // Process number releases
        let subscribers: Vec<String> = active_subscribers.keys().cloned().collect();
        for imsi in &subscribers {
            if rng.gen::<f64>() < number_release_daily_prob {
                if let Some(sub) = active_subscribers.remove(imsi) {
                    events.push(SubscriberEvent {
                        timestamp_ms: current_time,
                        event_type: SubscriberEventType::ReleaseNumber,
                        imsi: sub.imsi.clone(),
                        msisdn: Some(sub.msisdn.clone()),
                        imei: None,
                        mccmnc: sub.mccmnc.clone(),
                    });

                    released_numbers.push(ReleasedNumber {
                        msisdn: sub.msisdn,
                        release_time: current_time,
                    });
                }
            }
        }

        // Process number reassignments (after cooldown)
        let mut to_reassign = Vec::new();
        released_numbers.retain(|rel| {
            if current_time - rel.release_time >= cooldown_ms {
                to_reassign.push(rel.msisdn.clone());
                false
            } else {
                true
            }
        });

        for msisdn in to_reassign {
            // Assign to new subscriber
            let imsi = gen_imsi(&mut imsi_counter, &config.mccmnc_pool);
            let imei = gen_imei(&mut rng);
            let mccmnc = config.mccmnc_pool.choose(&mut rng).unwrap().clone();

            events.push(SubscriberEvent {
                timestamp_ms: current_time,
                event_type: SubscriberEventType::AssignNumber,
                imsi: imsi.clone(),
                msisdn: Some(msisdn.clone()),
                imei: Some(imei.to_string()),
                mccmnc: mccmnc.clone(),
            });

            active_subscribers.insert(
                imsi.clone(),
                ActiveSubscriber {
                    imsi,
                    msisdn,
                    imei,
                    mccmnc,
                    activation_time: current_time,
                },
            );
        }

        // Occasionally add completely new subscribers
        if rng.gen::<f64>() < 0.01 {
            // 1% chance per day
            let imsi = gen_imsi(&mut imsi_counter, &config.mccmnc_pool);
            let msisdn = gen_msisdn(&mut rng, &used_msisdns, &config.prefixes);
            let imei = gen_imei(&mut rng);
            let mccmnc = config.mccmnc_pool.choose(&mut rng).unwrap().clone();

            used_msisdns.insert(msisdn.clone());

            events.push(SubscriberEvent {
                timestamp_ms: current_time,
                event_type: SubscriberEventType::NewSubscriber,
                imsi: imsi.clone(),
                msisdn: Some(msisdn.clone()),
                imei: Some(imei.to_string()),
                mccmnc: mccmnc.clone(),
            });

            active_subscribers.insert(
                imsi.clone(),
                ActiveSubscriber {
                    imsi,
                    msisdn,
                    imei,
                    mccmnc,
                    activation_time: current_time,
                },
            );
        }
    }

    // Sort events by timestamp
    events.sort_by_key(|e| e.timestamp_ms);

    println!("Generated {} events", events.len());
    println!("Active subscribers: {}", active_subscribers.len());
    println!("Released numbers in cooldown: {}", released_numbers.len());

    Ok(events)
}

/// Export events to CSV file
pub fn export_to_csv<P: AsRef<Path>>(events: &[SubscriberEvent], path: P) -> Result<()> {
    let mut file = File::create(&path)?;

    // Write header
    writeln!(file, "timestamp_ms,event_type,imsi,msisdn,imei,mccmnc")?;

    // Write events
    for event in events {
        writeln!(
            file,
            "{},{},{},{},{},{}",
            event.timestamp_ms,
            event.event_type.to_str(),
            event.imsi,
            event.msisdn.as_deref().unwrap_or(""),
            event.imei.as_deref().unwrap_or(""),
            event.mccmnc
        )?;
    }

    println!("Exported {} events to {:?}", events.len(), path.as_ref());
    Ok(())
}

// ============================================================================
// Parallel Arrow-based generation with streaming and k-way merge
// ============================================================================

/// Generate deterministic IMSI based on subscriber index
fn gen_imsi_deterministic(sub_idx: usize, mccmnc_pool: &[String]) -> String {
    let mccmnc = &mccmnc_pool[sub_idx % mccmnc_pool.len()];
    let msin = sub_idx as u64 % 10_000_000_000u64;
    format!("{}{:010}", mccmnc, msin)
}

/// Generate deterministic MSISDN based on subscriber index
fn gen_msisdn_deterministic(sub_idx: usize, prefixes: &[String]) -> String {
    let prefix = &prefixes[sub_idx % prefixes.len()];
    let number = sub_idx % 10_000_000;
    format!("{}{:07}", prefix, number)
}

/// Event line for k-way merge with timestamp ordering
#[derive(Eq, PartialEq)]
struct EventLine {
    timestamp: i64,
    line: String,
    chunk_id: usize,
}

impl Ord for EventLine {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap (BinaryHeap is max-heap by default)
        other.timestamp.cmp(&self.timestamp)
            .then_with(|| self.chunk_id.cmp(&other.chunk_id))
    }
}

impl PartialOrd for EventLine {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Merge sorted CSV chunk files into single Arrow file using k-way merge
fn merge_chunks_to_arrow(chunk_files: &[PathBuf], output: &Path, batch_size: usize) -> Result<()> {
    use crate::subscriber_db_arrow::{subscriber_event_schema, events_to_record_batch};
    use arrow::ipc::writer::FileWriter;

    if chunk_files.is_empty() {
        return Ok(());
    }

    println!("Merging {} chunks into {:?}...", chunk_files.len(), output);

    // Open all chunk files
    let mut readers: Vec<BufReader<File>> = chunk_files
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<Result<Vec<_>, _>>()?;

    // Skip headers
    for reader in &mut readers {
        let mut header = String::new();
        reader.read_line(&mut header)?;
    }

    // Initialize heap with first line from each chunk
    let mut heap = BinaryHeap::new();
    let mut iterators: Vec<_> = readers.into_iter().enumerate().collect();

    for (chunk_id, reader) in &mut iterators {
        let mut line = String::new();
        if reader.read_line(&mut line)? > 0 {
            if let Some(ts) = parse_timestamp_from_csv(&line) {
                heap.push(EventLine {
                    timestamp: ts,
                    line: line.trim().to_string(),
                    chunk_id: *chunk_id,
                });
            }
        }
    }

    // Create Arrow writer
    let schema = subscriber_event_schema();
    let file = File::create(output)?;
    let mut writer = FileWriter::try_new(file, &schema)?;

    // Merge and write in batches
    let mut batch_events = Vec::with_capacity(batch_size);
    let mut merged_count = 0;

    while let Some(event_line) = heap.pop() {
        // Parse CSV line to SubscriberEvent
        if let Some(event) = parse_csv_line_to_event(&event_line.line) {
            batch_events.push(event);
            merged_count += 1;

            if merged_count % 1_000_000 == 0 {
                println!("Merged {} million events...", merged_count / 1_000_000);
            }

            // Write batch when full
            if batch_events.len() >= batch_size {
                let batch = events_to_record_batch(&batch_events)?;
                writer.write(&batch)?;
                batch_events.clear();
            }
        }

        // Read next line from the same chunk
        let chunk_id = event_line.chunk_id;
        let mut line = String::new();
        if iterators[chunk_id].1.read_line(&mut line)? > 0 {
            if let Some(ts) = parse_timestamp_from_csv(&line) {
                heap.push(EventLine {
                    timestamp: ts,
                    line: line.trim().to_string(),
                    chunk_id,
                });
            }
        }
    }

    // Write remaining events
    if !batch_events.is_empty() {
        let batch = events_to_record_batch(&batch_events)?;
        writer.write(&batch)?;
    }

    // Finish writing
    writer.finish()?;

    println!("Merge complete: {} total events written", merged_count);
    Ok(())
}

/// Parse timestamp from CSV line
fn parse_timestamp_from_csv(line: &str) -> Option<i64> {
    line.split(',').next()?.parse().ok()
}

/// Parse CSV line to SubscriberEvent
fn parse_csv_line_to_event(line: &str) -> Option<SubscriberEvent> {
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() != 6 {
        return None;
    }

    Some(SubscriberEvent {
        timestamp_ms: parts[0].parse().ok()?,
        event_type: SubscriberEventType::from_str(parts[1]).ok()?,
        imsi: parts[2].to_string(),
        msisdn: if parts[3].is_empty() { None } else { Some(parts[3].to_string()) },
        imei: if parts[4].is_empty() { None } else { Some(parts[4].to_string()) },
        mccmnc: parts[5].to_string(),
    })
}

/// Generate chunk with streaming CSV write
fn generate_chunk_streaming_csv(
    chunk_id: usize,
    start_sub_idx: usize,
    end_sub_idx: usize,
    config: &GeneratorConfig,
    output_path: &Path,
) -> Result<usize> {
    let mut file = BufWriter::with_capacity(1024 * 1024, File::create(output_path)?);
    let mut rng = StdRng::seed_from_u64(config.seed.wrapping_add(chunk_id as u64));

    // Write header
    writeln!(file, "timestamp_ms,event_type,imsi,msisdn,imei,mccmnc")?;

    let subscriber_count = end_sub_idx - start_sub_idx;
    let mut events_written = 0;

    // State tracking (only for this chunk)
    let mut active_subscribers: HashMap<String, ActiveSubscriber> = HashMap::new();
    let mut released_numbers: Vec<ReleasedNumber> = Vec::new();
    let mut used_msisdns: HashSet<String> = HashSet::new();

    let ms_per_day = 86400000i64;

    // Day 0: Generate initial subscribers for this chunk
    for local_idx in 0..subscriber_count {
        let global_idx = start_sub_idx + local_idx;
        let imsi = gen_imsi_deterministic(global_idx, &config.mccmnc_pool);
        let msisdn = gen_msisdn_deterministic(global_idx, &config.prefixes);
        let imei = gen_imei(&mut rng);
        let mccmnc = config.mccmnc_pool.choose(&mut rng).unwrap().clone();

        used_msisdns.insert(msisdn.clone());

        writeln!(
            file,
            "{},{},{},{},{},{}",
            config.start_timestamp_ms,
            SubscriberEventType::NewSubscriber.to_str(),
            imsi,
            msisdn,
            imei,
            mccmnc
        )?;
        events_written += 1;

        active_subscribers.insert(
            imsi.clone(),
            ActiveSubscriber {
                imsi,
                msisdn,
                imei,
                mccmnc,
                activation_time: config.start_timestamp_ms,
            },
        );
    }

    // Calculate daily probabilities
    let device_change_daily_prob = 1.0 - (1.0 - config.device_change_rate).powf(1.0 / 365.0);
    let number_release_daily_prob = 1.0 - (1.0 - config.number_release_rate).powf(1.0 / 365.0);
    let cooldown_ms = config.cooldown_days as i64 * ms_per_day;

    // Days 1..N: Generate historical events
    for day in 1..config.history_days {
        let current_time = config.start_timestamp_ms + (day as i64 * ms_per_day);

        // Process device changes
        let subscribers: Vec<String> = active_subscribers.keys().cloned().collect();
        for imsi in &subscribers {
            if rng.gen::<f64>() < device_change_daily_prob {
                if let Some(sub) = active_subscribers.get_mut(imsi) {
                    let new_imei = gen_imei(&mut rng);
                    writeln!(
                        file,
                        "{},{},{},{},{},{}",
                        current_time,
                        SubscriberEventType::ChangeDevice.to_str(),
                        sub.imsi,
                        sub.msisdn,
                        new_imei,
                        sub.mccmnc
                    )?;
                    events_written += 1;
                    sub.imei = new_imei;
                }
            }
        }

        // Process number releases
        let subscribers: Vec<String> = active_subscribers.keys().cloned().collect();
        for imsi in &subscribers {
            if rng.gen::<f64>() < number_release_daily_prob {
                if let Some(sub) = active_subscribers.remove(imsi) {
                    writeln!(
                        file,
                        "{},{},{},{},,{}",
                        current_time,
                        SubscriberEventType::ReleaseNumber.to_str(),
                        sub.imsi,
                        sub.msisdn,
                        sub.mccmnc
                    )?;
                    events_written += 1;

                    released_numbers.push(ReleasedNumber {
                        msisdn: sub.msisdn,
                        release_time: current_time,
                    });
                }
            }
        }

        // Process number reassignments
        let mut to_reassign = Vec::new();
        released_numbers.retain(|rel| {
            if current_time - rel.release_time >= cooldown_ms {
                to_reassign.push(rel.msisdn.clone());
                false
            } else {
                true
            }
        });

        for msisdn in to_reassign {
            let imsi = format!("{}{:010}",
                config.mccmnc_pool.choose(&mut rng).unwrap(),
                rng.gen_range(0..10_000_000_000u64)
            );
            let imei = gen_imei(&mut rng);
            let mccmnc = config.mccmnc_pool.choose(&mut rng).unwrap().clone();

            writeln!(
                file,
                "{},{},{},{},{},{}",
                current_time,
                SubscriberEventType::AssignNumber.to_str(),
                imsi,
                msisdn,
                imei,
                mccmnc
            )?;
            events_written += 1;

            active_subscribers.insert(
                imsi.clone(),
                ActiveSubscriber {
                    imsi,
                    msisdn,
                    imei,
                    mccmnc,
                    activation_time: current_time,
                },
            );
        }

        // Occasionally add new subscribers
        if rng.gen::<f64>() < 0.01 {
            let imsi = format!("{}{:010}",
                config.mccmnc_pool.choose(&mut rng).unwrap(),
                rng.gen_range(0..10_000_000_000u64)
            );
            let prefix = config.prefixes.choose(&mut rng).unwrap();
            let number = rng.gen_range(0..10_000_000);
            let msisdn = format!("{}{:07}", prefix, number);
            let imei = gen_imei(&mut rng);
            let mccmnc = config.mccmnc_pool.choose(&mut rng).unwrap().clone();

            used_msisdns.insert(msisdn.clone());

            writeln!(
                file,
                "{},{},{},{},{},{}",
                current_time,
                SubscriberEventType::NewSubscriber.to_str(),
                imsi,
                msisdn,
                imei,
                mccmnc
            )?;
            events_written += 1;

            active_subscribers.insert(
                imsi.clone(),
                ActiveSubscriber {
                    imsi,
                    msisdn,
                    imei,
                    mccmnc,
                    activation_time: current_time,
                },
            );
        }
    }

    file.flush()?;
    Ok(events_written)
}

/// Generate database in parallel with Arrow output
pub fn generate_database_parallel_arrow<P: AsRef<Path>>(
    config: &GeneratorConfig,
    output_path: P,
) -> Result<()> {
    // Memory optimization: limit chunk size
    let max_chunk_size = 2_000_000;
    let chunk_size = max_chunk_size.min(config.initial_subscribers);

    let total_chunks = (config.initial_subscribers + chunk_size - 1) / chunk_size;

    // Memory optimization: limit parallel workers
    let max_parallel_workers = (num_cpus::get() / 2).max(1).min(4);
    let workers = max_parallel_workers.min(total_chunks);

    println!("Generating subscriber database with {} parallel workers...", workers);
    println!("  Subscribers: {}", config.initial_subscribers);
    println!("  History days: {}", config.history_days);
    println!("  Chunk size: {} subscribers per chunk", chunk_size);
    println!("  Total chunks: {}", total_chunks);
    println!();

    let temp_dir = PathBuf::from("temp_chunks");
    std::fs::create_dir_all(&temp_dir)?;

    let mut all_temp_files = Vec::new();

    // Process chunks in batches to control memory
    for batch_start in (0..total_chunks).step_by(workers) {
        let batch_end = (batch_start + workers).min(total_chunks);
        println!("Processing batch {}-{} of {} chunks...", batch_start, batch_end - 1, total_chunks);

        let batch_files: Vec<PathBuf> = (batch_start..batch_end)
            .into_par_iter()
            .map(|chunk_id| {
                let start_sub_idx = chunk_id * chunk_size;
                let end_sub_idx = ((chunk_id + 1) * chunk_size).min(config.initial_subscribers);

                let chunk_path = temp_dir.join(format!("chunk_{:04}.csv", chunk_id));

                println!("Worker {:02}: generating subscribers {}-{} ({} total)...",
                    chunk_id, start_sub_idx, end_sub_idx, end_sub_idx - start_sub_idx);

                let events_count = generate_chunk_streaming_csv(
                    chunk_id,
                    start_sub_idx,
                    end_sub_idx,
                    config,
                    &chunk_path,
                )?;

                println!("Worker {:02}: completed, wrote {} events", chunk_id, events_count);
                Ok(chunk_path)
            })
            .collect::<Result<Vec<_>>>()?;

        all_temp_files.extend(batch_files);
        println!("Batch {}-{} completed\n", batch_start, batch_end - 1);
    }

    println!("All chunks completed. Generated {} chunk files.", all_temp_files.len());
    println!("Starting k-way merge to Arrow format...");

    // Merge all chunks into Arrow format
    merge_chunks_to_arrow(&all_temp_files, output_path.as_ref(), 100_000)?;

    // Cleanup temp files
    println!("Cleaning up temporary files...");
    for temp_file in &all_temp_files {
        let _ = std::fs::remove_file(temp_file);
    }
    let _ = std::fs::remove_dir(&temp_dir);

    println!("Database generation complete: {:?}", output_path.as_ref());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_generate_database() {
        let config = GeneratorConfig {
            initial_subscribers: 100,
            history_days: 30,
            device_change_rate: 0.15,
            number_release_rate: 0.05,
            cooldown_days: 7,
            prefixes: vec!["31612".to_string()],
            mccmnc_pool: vec!["20408".to_string()],
            seed: 42,
            start_timestamp_ms: 1704067200000,
        };

        let events = generate_database(&config).unwrap();
        assert!(!events.is_empty());
        assert!(events.len() >= config.initial_subscribers);

        // Check chronological order
        for i in 1..events.len() {
            assert!(events[i].timestamp_ms >= events[i - 1].timestamp_ms);
        }
    }

    #[test]
    fn test_export_csv() {
        let events = vec![
            SubscriberEvent {
                timestamp_ms: 1704067200000,
                event_type: SubscriberEventType::NewSubscriber,
                imsi: "204081234567890".to_string(),
                msisdn: Some("31612345678".to_string()),
                imei: Some("123456789012345".to_string()),
                mccmnc: "20408".to_string(),
            },
        ];

        let file = NamedTempFile::new().unwrap();
        export_to_csv(&events, file.path()).unwrap();

        // Verify file was created
        let metadata = std::fs::metadata(file.path()).unwrap();
        assert!(metadata.len() > 0);
    }
}
