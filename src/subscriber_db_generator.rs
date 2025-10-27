// Generator for synthetic subscriber database with realistic history
use crate::identity::gen_imei;
use crate::subscriber_db::{SubscriberEvent, SubscriberEventType};
use anyhow::Result;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::SeedableRng;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::Path;

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
// Direct redb generation without Arrow intermediate format
// ============================================================================

/// Generate subscriber database directly in redb format
/// This is more efficient than Arrow->redb conversion
pub fn generate_database_redb<P: AsRef<Path>>(
    config: &GeneratorConfig,
    output_path: P,
) -> Result<()> {
    use crate::subscriber_db::{SubscriberDatabase, SubscriberSnapshot};
    use crate::subscriber_db_redb::{SubscriberDbRedb, SubscriberSnapshotNumeric};
    use std::collections::HashMap;

    println!("Generating subscriber database directly to redb format...");
    println!("  Subscribers: {}", config.initial_subscribers);
    println!("  History days: {}", config.history_days);
    println!();

    // Generate events using existing logic
    let events = generate_database(config)?;

    // Build in-memory database and compute snapshots
    println!("Building snapshots from events...");
    let mut db = SubscriberDatabase::new();
    db.events = events;
    db.build_indices();
    db.build_snapshots();

    let snapshots = db.get_snapshots();
    println!("Total snapshots: {}", snapshots.len());

    // Group snapshots by MSISDN
    println!("Grouping snapshots by MSISDN...");
    let mut msisdn_snapshots: HashMap<u64, Vec<SubscriberSnapshotNumeric>> = HashMap::new();

    for snapshot in snapshots {
        let msisdn = snapshot.msisdn.parse::<u64>().unwrap_or(0);
        if msisdn == 0 {
            continue; // Skip invalid MSISDNs
        }

        let numeric_snapshot = SubscriberSnapshotNumeric::from(snapshot);
        msisdn_snapshots.entry(msisdn)
            .or_insert_with(Vec::new)
            .push(numeric_snapshot);
    }

    println!("Unique MSISDNs: {}", msisdn_snapshots.len());

    // Create redb database and insert snapshots
    println!("Creating redb database at {:?}...", output_path.as_ref());
    let redb = SubscriberDbRedb::new(output_path.as_ref())?;

    println!("Inserting snapshots into redb (batch mode)...");

    // Convert HashMap to Vec for batching
    let all_entries: Vec<(u64, Vec<SubscriberSnapshotNumeric>)> = msisdn_snapshots.into_iter().collect();
    let total = all_entries.len();

    // Insert in batches of 10,000 for optimal performance
    let batch_size = 10_000;
    let mut inserted = 0;

    for batch_start in (0..total).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(total);
        let batch = &all_entries[batch_start..batch_end];

        redb.insert_snapshots_batch(batch)?;
        inserted += batch.len();

        if inserted % 50_000 == 0 || inserted == total {
            println!("  Inserted {}/{} MSISDNs...", inserted, total);
        }
    }

    println!("\nGeneration complete!");
    println!("Database saved to: {:?}", output_path.as_ref());

    // Print statistics
    let stats = redb.stats()?;
    println!("\nDatabase statistics:");
    println!("  Total MSISDNs: {}", stats.total_msisdns);
    println!("  Total snapshots: {}", stats.total_snapshots);

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
