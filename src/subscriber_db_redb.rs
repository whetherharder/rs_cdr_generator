use anyhow::{Context, Result};
use bincode::{deserialize, serialize};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::subscriber_db::{SubscriberDatabase, SubscriberSnapshot};

/// Numeric version of SubscriberSnapshot for efficient storage and lookup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberSnapshotNumeric {
    pub imsi: u64,
    pub msisdn: u64,
    pub imei: u64,
    pub mccmnc: u32,
    pub valid_from: i64,
    pub valid_to: Option<i64>,
}

impl From<&SubscriberSnapshot> for SubscriberSnapshotNumeric {
    fn from(snapshot: &SubscriberSnapshot) -> Self {
        SubscriberSnapshotNumeric {
            imsi: snapshot.imsi.parse::<u64>().unwrap_or(0),
            msisdn: snapshot.msisdn.parse::<u64>().unwrap_or(0),
            imei: snapshot.imei.parse::<u64>().unwrap_or(0),
            mccmnc: snapshot.mccmnc.parse::<u32>().unwrap_or(0),
            valid_from: snapshot.valid_from,
            valid_to: snapshot.valid_to,
        }
    }
}

/// Table: MSISDN -> Vec<SubscriberSnapshot>
/// Stores all historical snapshots for each MSISDN
const SNAPSHOTS: TableDefinition<u64, &[u8]> = TableDefinition::new("snapshots");

/// Embedded redb-based subscriber database for chunked processing
///
/// Architecture:
/// - Key = MSISDN (u64)
/// - Value = Vec<SubscriberSnapshot> serialized with bincode
/// - Supports efficient range queries for chunk loading
/// - Supports O(1) lookup by MSISDN for MT generation
pub struct SubscriberDbRedb {
    db: Database,
}

impl SubscriberDbRedb {
    /// Create or open a redb database at the given path
    pub fn new(path: &Path) -> Result<Self> {
        let db = Database::create(path).context("Failed to create redb database")?;
        Ok(Self { db })
    }

    /// Open an existing redb database (read-only)
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::open(path).context("Failed to open redb database")?;
        Ok(Self { db })
    }

    /// Insert or update snapshots for a given MSISDN
    /// This appends new snapshots to the existing list
    pub fn insert_snapshots(&self, msisdn: u64, snapshots: &[SubscriberSnapshotNumeric]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(SNAPSHOTS)?;

            // Check if MSISDN already has snapshots
            let mut all_snapshots = if let Some(existing) = table.get(msisdn)? {
                let bytes = existing.value();
                deserialize::<Vec<SubscriberSnapshotNumeric>>(bytes)
                    .context("Failed to deserialize existing snapshots")?
            } else {
                Vec::new()
            };

            // Append new snapshots
            all_snapshots.extend_from_slice(snapshots);

            // Serialize and store
            let serialized = serialize(&all_snapshots)
                .context("Failed to serialize snapshots")?;
            table.insert(msisdn, serialized.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Batch insert snapshots for multiple MSISDNs in a single transaction (much faster)
    pub fn insert_snapshots_batch(&self, batch: &[(u64, Vec<SubscriberSnapshotNumeric>)]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(SNAPSHOTS)?;

            for (msisdn, snapshots) in batch {
                // Serialize and store (assuming no existing data for bulk import)
                let serialized = serialize(snapshots)
                    .context("Failed to serialize snapshots")?;
                table.insert(*msisdn, serialized.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Get the subscriber snapshot valid at the given timestamp
    /// Returns None if MSISDN not found or no valid snapshot at that time
    pub fn get_subscriber_at(&self, msisdn: u64, timestamp: i64) -> Result<Option<SubscriberSnapshotNumeric>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SNAPSHOTS)?;

        if let Some(value) = table.get(msisdn)? {
            let bytes = value.value();
            let snapshots: Vec<SubscriberSnapshotNumeric> = deserialize(bytes)
                .context("Failed to deserialize snapshots")?;

            // Find snapshot valid at timestamp
            for snapshot in snapshots {
                let valid_from = snapshot.valid_from;
                let valid_to = snapshot.valid_to.unwrap_or(i64::MAX);

                if timestamp >= valid_from && timestamp < valid_to {
                    return Ok(Some(snapshot));
                }
            }
        }

        Ok(None)
    }

    /// Load a chunk of subscribers by MSISDN range [start_msisdn, end_msisdn)
    /// Returns a list of (MSISDN, Vec<SubscriberSnapshotNumeric>)
    pub fn load_chunk(
        &self,
        start_msisdn: u64,
        end_msisdn: u64,
    ) -> Result<Vec<(u64, Vec<SubscriberSnapshotNumeric>)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SNAPSHOTS)?;

        let mut result = Vec::new();

        // Range scan from start_msisdn to end_msisdn
        for entry in table.range(start_msisdn..end_msisdn)? {
            let (msisdn, value) = entry?;
            let bytes = value.value();
            let snapshots: Vec<SubscriberSnapshotNumeric> = deserialize(bytes)
                .context("Failed to deserialize snapshots")?;

            result.push((msisdn.value(), snapshots));
        }

        Ok(result)
    }

    /// Get statistics about the database
    pub fn stats(&self) -> Result<DbStats> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SNAPSHOTS)?;

        let mut total_msisdns = 0u64;
        let mut total_snapshots = 0u64;

        for entry in table.iter()? {
            let (_, value) = entry?;
            let bytes = value.value();
            let snapshots: Vec<SubscriberSnapshotNumeric> = deserialize(bytes)?;

            total_msisdns += 1;
            total_snapshots += snapshots.len() as u64;
        }

        Ok(DbStats {
            total_msisdns,
            total_snapshots,
        })
    }
}

#[derive(Debug)]
pub struct DbStats {
    pub total_msisdns: u64,
    pub total_snapshots: u64,
}

/// Convert Arrow subscriber database to redb format
/// This reads the Arrow database, builds snapshots, and stores them in redb
/// grouped by MSISDN for efficient chunk loading
pub fn convert_arrow_to_redb(
    arrow_path: &Path,
    redb_path: &Path,
    start_ts: i64,
    end_ts: i64,
) -> Result<()> {
    use std::collections::HashMap;

    println!("Converting Arrow database to redb...");
    println!("  Source: {:?}", arrow_path);
    println!("  Target: {:?}", redb_path);

    // Load Arrow database and build snapshots
    println!("Loading Arrow database...");
    let mut db = SubscriberDatabase::load_from_arrow_range(arrow_path, start_ts, end_ts)?;

    println!("Building snapshots...");
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
    println!("Creating redb database...");
    let redb = SubscriberDbRedb::new(redb_path)?;

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

        println!("  Inserted {}/{} MSISDNs...", inserted, total);
    }

    println!("Conversion complete! Inserted {} MSISDNs", inserted);

    // Print statistics
    let stats = redb.stats()?;
    println!("Database statistics:");
    println!("  Total MSISDNs: {}", stats.total_msisdns);
    println!("  Total snapshots: {}", stats.total_snapshots);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_insert_and_get() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.redb");
        let db = SubscriberDbRedb::new(&db_path)?;

        let snapshots = vec![
            SubscriberSnapshotNumeric {
                imsi: 123456789,
                msisdn: 79001234567,
                imei: 111111111111111,
                mccmnc: 25001,
                valid_from: 1000,
                valid_to: Some(2000),
            },
            SubscriberSnapshotNumeric {
                imsi: 123456789,
                msisdn: 79001234567,
                imei: 222222222222222,
                mccmnc: 25001,
                valid_from: 2000,
                valid_to: None,
            },
        ];

        db.insert_snapshots(79001234567, &snapshots)?;

        // Get snapshot at timestamp 1500 (first snapshot)
        let sub = db.get_subscriber_at(79001234567, 1500)?.unwrap();
        assert_eq!(sub.imei, 111111111111111);

        // Get snapshot at timestamp 2500 (second snapshot)
        let sub = db.get_subscriber_at(79001234567, 2500)?.unwrap();
        assert_eq!(sub.imei, 222222222222222);

        // No snapshot at timestamp 500
        assert!(db.get_subscriber_at(79001234567, 500)?.is_none());

        Ok(())
    }

    #[test]
    fn test_load_chunk() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.redb");
        let db = SubscriberDbRedb::new(&db_path)?;

        // Insert snapshots for multiple MSISDNs
        for msisdn in 79001234560..79001234570 {
            let snapshot = SubscriberSnapshotNumeric {
                imsi: msisdn - 79000000000,
                msisdn,
                imei: msisdn * 1000,
                mccmnc: 25001,
                valid_from: 0,
                valid_to: None,
            };
            db.insert_snapshots(msisdn, &[snapshot])?;
        }

        // Load chunk [79001234562, 79001234567)
        let chunk = db.load_chunk(79001234562, 79001234567)?;
        assert_eq!(chunk.len(), 5);
        assert_eq!(chunk[0].0, 79001234562);
        assert_eq!(chunk[4].0, 79001234566);

        Ok(())
    }

    #[test]
    fn test_stats() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.redb");
        let db = SubscriberDbRedb::new(&db_path)?;

        // Insert 100 MSISDNs, each with 2 snapshots
        for msisdn in 79001234500..79001234600 {
            let snapshots = vec![
                SubscriberSnapshotNumeric {
                    imsi: msisdn - 79000000000,
                    msisdn,
                    imei: msisdn * 1000,
                    mccmnc: 25001,
                    valid_from: 0,
                    valid_to: Some(1000),
                },
                SubscriberSnapshotNumeric {
                    imsi: msisdn - 79000000000,
                    msisdn,
                    imei: msisdn * 2000,
                    mccmnc: 25001,
                    valid_from: 1000,
                    valid_to: None,
                },
            ];
            db.insert_snapshots(msisdn, &snapshots)?;
        }

        let stats = db.stats()?;
        assert_eq!(stats.total_msisdns, 100);
        assert_eq!(stats.total_snapshots, 200);

        Ok(())
    }
}
