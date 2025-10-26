// Subscriber database management with historical changes
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

/// Types of subscriber events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriberEventType {
    /// New subscriber joins the network (IMSI, MSISDN, IMEI all assigned)
    NewSubscriber,
    /// Subscriber changes device (IMEI changes, IMSI and MSISDN stay)
    ChangeDevice,
    /// Subscriber changes SIM card (IMSI changes, MSISDN stays, possibly IMEI changes)
    ChangeSim,
    /// Phone number is released (MSISDN freed, IMEI removed)
    ReleaseNumber,
    /// Phone number assigned to different subscriber (MSISDN assigned to new IMSI)
    AssignNumber,
}

impl SubscriberEventType {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "NEW_SUBSCRIBER" => Ok(SubscriberEventType::NewSubscriber),
            "CHANGE_DEVICE" => Ok(SubscriberEventType::ChangeDevice),
            "CHANGE_SIM" => Ok(SubscriberEventType::ChangeSim),
            "RELEASE_NUMBER" => Ok(SubscriberEventType::ReleaseNumber),
            "ASSIGN_NUMBER" => Ok(SubscriberEventType::AssignNumber),
            _ => Err(anyhow!("Unknown event type: {}", s)),
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            SubscriberEventType::NewSubscriber => "NEW_SUBSCRIBER",
            SubscriberEventType::ChangeDevice => "CHANGE_DEVICE",
            SubscriberEventType::ChangeSim => "CHANGE_SIM",
            SubscriberEventType::ReleaseNumber => "RELEASE_NUMBER",
            SubscriberEventType::AssignNumber => "ASSIGN_NUMBER",
        }
    }
}

/// A single event in subscriber history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberEvent {
    pub timestamp_ms: i64,
    pub event_type: SubscriberEventType,
    pub imsi: String,
    pub msisdn: Option<String>,
    pub imei: Option<String>,
    pub mccmnc: String,
}

/// Snapshot of subscriber state at a point in time
#[derive(Debug, Clone)]
pub struct SubscriberSnapshot {
    pub imsi: String,
    pub msisdn: String,
    pub imei: String,
    pub mccmnc: String,
    pub valid_from: i64,
    pub valid_to: Option<i64>,
}

/// Main subscriber database with history
#[derive(Debug)]
pub struct SubscriberDatabase {
    events: Vec<SubscriberEvent>,
    // Indices for fast lookup
    by_imsi: HashMap<String, Vec<usize>>,      // IMSI -> event indices
    by_msisdn: HashMap<String, Vec<usize>>,    // MSISDN -> event indices
    snapshots: Vec<SubscriberSnapshot>,        // Pre-computed snapshots
}

impl SubscriberDatabase {
    /// Create empty database
    pub fn new() -> Self {
        SubscriberDatabase {
            events: Vec::new(),
            by_imsi: HashMap::new(),
            by_msisdn: HashMap::new(),
            snapshots: Vec::new(),
        }
    }

    /// Load subscriber database from CSV file
    /// CSV format: timestamp_ms,event_type,imsi,msisdn,imei,mccmnc
    pub fn load_from_csv<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(&path)
            .with_context(|| format!("Failed to open subscriber DB: {:?}", path.as_ref()))?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();

        for (line_num, line) in reader.lines().enumerate() {
            let line = line?;

            // Skip header
            if line_num == 0 && line.starts_with("timestamp_ms") {
                continue;
            }

            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() != 6 {
                return Err(anyhow!(
                    "Line {}: Expected 6 fields, got {}",
                    line_num + 1,
                    parts.len()
                ));
            }

            let timestamp_ms = parts[0]
                .parse::<i64>()
                .with_context(|| format!("Line {}: Invalid timestamp", line_num + 1))?;

            let event_type = SubscriberEventType::from_str(parts[1])
                .with_context(|| format!("Line {}: Invalid event type", line_num + 1))?;

            let imsi = parts[2].to_string();
            let msisdn = if parts[3].is_empty() {
                None
            } else {
                Some(parts[3].to_string())
            };
            let imei = if parts[4].is_empty() {
                None
            } else {
                Some(parts[4].to_string())
            };
            let mccmnc = parts[5].to_string();

            events.push(SubscriberEvent {
                timestamp_ms,
                event_type,
                imsi,
                msisdn,
                imei,
                mccmnc,
            });
        }

        let mut db = SubscriberDatabase::new();
        db.events = events;
        db.build_indices();

        Ok(db)
    }

    /// Build indices for fast lookup
    fn build_indices(&mut self) {
        self.by_imsi.clear();
        self.by_msisdn.clear();

        for (idx, event) in self.events.iter().enumerate() {
            self.by_imsi
                .entry(event.imsi.clone())
                .or_insert_with(Vec::new)
                .push(idx);

            if let Some(ref msisdn) = event.msisdn {
                self.by_msisdn
                    .entry(msisdn.clone())
                    .or_insert_with(Vec::new)
                    .push(idx);
            }
        }
    }

    /// Validate database integrity
    pub fn validate(&self) -> Result<()> {
        // 1. Check chronological order
        for i in 1..self.events.len() {
            if self.events[i].timestamp_ms < self.events[i - 1].timestamp_ms {
                return Err(anyhow!(
                    "Events not in chronological order at index {}",
                    i
                ));
            }
        }

        // 2. Check that MSISDN is not used by multiple IMSI at the same time
        let mut msisdn_ownership: HashMap<String, (String, i64, Option<i64>)> = HashMap::new(); // msisdn -> (imsi, from, to)

        for event in &self.events {
            if let Some(ref msisdn) = event.msisdn {
                match event.event_type {
                    SubscriberEventType::NewSubscriber | SubscriberEventType::AssignNumber => {
                        // Check if this MSISDN is already owned by someone else
                        if let Some((owner_imsi, from, to)) = msisdn_ownership.get(msisdn) {
                            if to.is_none() || to.unwrap() > event.timestamp_ms {
                                if owner_imsi != &event.imsi {
                                    return Err(anyhow!(
                                        "MSISDN {} conflict: owned by {} from {} to {:?}, but assigned to {} at {}",
                                        msisdn,
                                        owner_imsi,
                                        from,
                                        to,
                                        event.imsi,
                                        event.timestamp_ms
                                    ));
                                }
                            }
                        }
                        msisdn_ownership.insert(
                            msisdn.clone(),
                            (event.imsi.clone(), event.timestamp_ms, None),
                        );
                    }
                    SubscriberEventType::ReleaseNumber => {
                        // Mark MSISDN as released
                        if let Some(ownership) = msisdn_ownership.get_mut(msisdn) {
                            if ownership.0 == event.imsi {
                                ownership.2 = Some(event.timestamp_ms);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // 3. Validate IMSI format (should be 14-15 digits)
        for event in &self.events {
            if event.imsi.len() < 14 || event.imsi.len() > 15 {
                return Err(anyhow!("Invalid IMSI length: {}", event.imsi));
            }
            if !event.imsi.chars().all(|c| c.is_ascii_digit()) {
                return Err(anyhow!("Invalid IMSI (not all digits): {}", event.imsi));
            }
        }

        // 4. Validate MSISDN format
        for event in &self.events {
            if let Some(ref msisdn) = event.msisdn {
                if msisdn.len() < 8 || msisdn.len() > 15 {
                    return Err(anyhow!("Invalid MSISDN length: {}", msisdn));
                }
                if !msisdn.chars().all(|c| c.is_ascii_digit()) {
                    return Err(anyhow!("Invalid MSISDN (not all digits): {}", msisdn));
                }
            }
        }

        // 5. Validate IMEI format (should be 15 digits)
        for event in &self.events {
            if let Some(ref imei) = event.imei {
                if imei.len() != 15 {
                    return Err(anyhow!("Invalid IMEI length: {}", imei));
                }
                if !imei.chars().all(|c| c.is_ascii_digit()) {
                    return Err(anyhow!("Invalid IMEI (not all digits): {}", imei));
                }
            }
        }

        Ok(())
    }

    /// Build snapshots for efficient querying
    pub fn build_snapshots(&mut self) {
        // Group events by IMSI and build snapshots
        let mut snapshots = Vec::new();
        let mut imsi_states: HashMap<String, SubscriberState> = HashMap::new();

        for event in &self.events {
            let imsi = &event.imsi;
            let state = imsi_states.entry(imsi.clone()).or_insert_with(|| SubscriberState {
                imsi: imsi.clone(),
                msisdn: None,
                imei: None,
                mccmnc: event.mccmnc.clone(),
                valid_from: event.timestamp_ms,
            });

            // Create snapshot for previous state if it changed
            match event.event_type {
                SubscriberEventType::NewSubscriber => {
                    state.msisdn = event.msisdn.clone();
                    state.imei = event.imei.clone();
                    state.mccmnc = event.mccmnc.clone();
                    state.valid_from = event.timestamp_ms;
                }
                SubscriberEventType::ChangeDevice => {
                    // Close previous snapshot
                    if let (Some(msisdn), Some(imei)) = (&state.msisdn, &state.imei) {
                        snapshots.push(SubscriberSnapshot {
                            imsi: state.imsi.clone(),
                            msisdn: msisdn.clone(),
                            imei: imei.clone(),
                            mccmnc: state.mccmnc.clone(),
                            valid_from: state.valid_from,
                            valid_to: Some(event.timestamp_ms),
                        });
                    }
                    // Update state
                    state.imei = event.imei.clone();
                    state.valid_from = event.timestamp_ms;
                }
                SubscriberEventType::ChangeSim => {
                    // Close previous snapshot
                    if let (Some(msisdn), Some(imei)) = (&state.msisdn, &state.imei) {
                        snapshots.push(SubscriberSnapshot {
                            imsi: state.imsi.clone(),
                            msisdn: msisdn.clone(),
                            imei: imei.clone(),
                            mccmnc: state.mccmnc.clone(),
                            valid_from: state.valid_from,
                            valid_to: Some(event.timestamp_ms),
                        });
                    }
                    // Update state (new IMSI means we need to track new state)
                    // This is handled by the new event for the new IMSI
                }
                SubscriberEventType::ReleaseNumber => {
                    // Close snapshot
                    if let (Some(msisdn), Some(imei)) = (&state.msisdn, &state.imei) {
                        snapshots.push(SubscriberSnapshot {
                            imsi: state.imsi.clone(),
                            msisdn: msisdn.clone(),
                            imei: imei.clone(),
                            mccmnc: state.mccmnc.clone(),
                            valid_from: state.valid_from,
                            valid_to: Some(event.timestamp_ms),
                        });
                    }
                    state.msisdn = None;
                    state.imei = None;
                }
                SubscriberEventType::AssignNumber => {
                    state.msisdn = event.msisdn.clone();
                    state.imei = event.imei.clone();
                    state.valid_from = event.timestamp_ms;
                }
            }
        }

        // Add final snapshots for all active subscribers
        for (_, state) in imsi_states {
            if let (Some(msisdn), Some(imei)) = (state.msisdn, state.imei) {
                snapshots.push(SubscriberSnapshot {
                    imsi: state.imsi,
                    msisdn,
                    imei,
                    mccmnc: state.mccmnc,
                    valid_from: state.valid_from,
                    valid_to: None,
                });
            }
        }

        self.snapshots = snapshots;
    }

    /// Get subscriber snapshot at specific timestamp by IMSI
    pub fn get_snapshot_at(&self, imsi: &str, timestamp_ms: i64) -> Option<SubscriberSnapshot> {
        // Use pre-computed snapshots if available
        if !self.snapshots.is_empty() {
            for snapshot in &self.snapshots {
                if snapshot.imsi == imsi
                    && snapshot.valid_from <= timestamp_ms
                    && (snapshot.valid_to.is_none() || snapshot.valid_to.unwrap() > timestamp_ms)
                {
                    return Some(snapshot.clone());
                }
            }
            return None;
        }

        // Fallback: compute from events
        let indices = self.by_imsi.get(imsi)?;
        let mut current_state: Option<SubscriberState> = None;

        for &idx in indices {
            let event = &self.events[idx];
            if event.timestamp_ms > timestamp_ms {
                break;
            }

            match event.event_type {
                SubscriberEventType::NewSubscriber => {
                    current_state = Some(SubscriberState {
                        imsi: event.imsi.clone(),
                        msisdn: event.msisdn.clone(),
                        imei: event.imei.clone(),
                        mccmnc: event.mccmnc.clone(),
                        valid_from: event.timestamp_ms,
                    });
                }
                SubscriberEventType::ChangeDevice => {
                    if let Some(ref mut state) = current_state {
                        state.imei = event.imei.clone();
                    }
                }
                SubscriberEventType::ReleaseNumber => {
                    current_state = None;
                }
                SubscriberEventType::AssignNumber => {
                    current_state = Some(SubscriberState {
                        imsi: event.imsi.clone(),
                        msisdn: event.msisdn.clone(),
                        imei: event.imei.clone(),
                        mccmnc: event.mccmnc.clone(),
                        valid_from: event.timestamp_ms,
                    });
                }
                _ => {}
            }
        }

        current_state.and_then(|state| {
            if let (Some(msisdn), Some(imei)) = (state.msisdn, state.imei) {
                Some(SubscriberSnapshot {
                    imsi: state.imsi,
                    msisdn,
                    imei,
                    mccmnc: state.mccmnc,
                    valid_from: state.valid_from,
                    valid_to: None,
                })
            } else {
                None
            }
        })
    }

    /// Get subscriber snapshot by MSISDN at specific timestamp
    pub fn get_snapshot_by_msisdn(
        &self,
        msisdn: &str,
        timestamp_ms: i64,
    ) -> Option<SubscriberSnapshot> {
        // Use pre-computed snapshots if available
        if !self.snapshots.is_empty() {
            for snapshot in &self.snapshots {
                if snapshot.msisdn == msisdn
                    && snapshot.valid_from <= timestamp_ms
                    && (snapshot.valid_to.is_none() || snapshot.valid_to.unwrap() > timestamp_ms)
                {
                    return Some(snapshot.clone());
                }
            }
            return None;
        }

        // Fallback: find IMSI that owns this MSISDN at this time
        let indices = self.by_msisdn.get(msisdn)?;

        for &idx in indices.iter().rev() {
            let event = &self.events[idx];
            if event.timestamp_ms <= timestamp_ms {
                if let Some(ref event_msisdn) = event.msisdn {
                    if event_msisdn == msisdn {
                        // Found the IMSI, now get its snapshot
                        return self.get_snapshot_at(&event.imsi, timestamp_ms);
                    }
                }
            }
        }

        None
    }

    /// Get total number of events
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Get total number of snapshots
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }

    /// Get unique IMSI count
    pub fn unique_imsi_count(&self) -> usize {
        self.by_imsi.len()
    }

    /// Get all unique IMSIs in the database
    pub fn get_all_unique_imsi(&self) -> Vec<String> {
        self.by_imsi.keys().cloned().collect()
    }

    /// Get all snapshots (requires build_snapshots() to be called first)
    pub fn get_snapshots(&self) -> &[SubscriberSnapshot] {
        &self.snapshots
    }

    /// Load subscriber database from Arrow IPC file
    pub fn load_from_arrow<P: AsRef<Path>>(path: P) -> Result<Self> {
        use crate::subscriber_db_arrow::read_events_from_arrow;

        let events = read_events_from_arrow(path)?;
        let mut db = SubscriberDatabase::new();
        db.events = events;
        db.build_indices();
        Ok(db)
    }

    /// Load subscriber database from Arrow IPC file with timestamp range filter
    pub fn load_from_arrow_range<P: AsRef<Path>>(
        path: P,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Self> {
        use crate::subscriber_db_arrow::read_events_from_arrow_range;

        let events = read_events_from_arrow_range(path, start_ts, end_ts)?;
        let mut db = SubscriberDatabase::new();
        db.events = events;
        db.build_indices();
        Ok(db)
    }

    /// Load from Arrow with filtering during read (memory-efficient)
    /// Reads batches and filters on-the-fly, avoiding loading entire file into memory
    pub fn load_from_arrow_with_msisdn_filter<P: AsRef<Path>>(
        path: P,
        start_ts: i64,
        end_ts: i64,
        start_u: usize,
        end_u: usize,
        prefixes: &[String],
    ) -> Result<Self> {
        use crate::subscriber_db_arrow::read_events_from_arrow_filtered;
        use std::collections::HashSet;

        // Generate MSISDN set for this worker's range
        let msisdn_set: HashSet<String> = (start_u..end_u)
            .map(|idx| {
                let prefix = &prefixes[idx % prefixes.len()];
                let number = idx % 10_000_000;
                format!("{}{:07}", prefix, number)
            })
            .collect();

        // Read with filtering during read (batch-by-batch)
        let events = read_events_from_arrow_filtered(path, start_ts, end_ts, &msisdn_set)?;

        let mut db = SubscriberDatabase::new();
        db.events = events;
        db.build_indices();
        Ok(db)
    }

    /// Filter database by MSISDN range (for worker partitioning)
    /// Creates a new database containing only events for subscribers in [start_u..end_u) range
    pub fn filter_by_msisdn_range(&self, start_u: usize, end_u: usize, prefixes: &[String]) -> Self {
        use std::collections::HashSet;

        // Generate expected MSISDNs for this worker's subscriber range
        let msisdn_set: HashSet<String> = (start_u..end_u)
            .map(|idx| {
                let prefix = &prefixes[idx % prefixes.len()];
                let number = idx % 10_000_000;
                format!("{}{:07}", prefix, number)
            })
            .collect();

        // Filter events to only include those for our MSISDNs
        let filtered_events: Vec<SubscriberEvent> = self.events
            .iter()
            .filter(|e| e.msisdn.as_ref().map_or(false, |m| msisdn_set.contains(m)))
            .cloned()
            .collect();

        // Create new database with filtered events
        let mut db = SubscriberDatabase::new();
        db.events = filtered_events;
        db.build_indices();
        db
    }
}

/// Internal state tracker for building snapshots
struct SubscriberState {
    imsi: String,
    msisdn: Option<String>,
    imei: Option<String>,
    mccmnc: String,
    valid_from: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_event_type_conversion() {
        assert_eq!(
            SubscriberEventType::from_str("NEW_SUBSCRIBER").unwrap(),
            SubscriberEventType::NewSubscriber
        );
        assert_eq!(
            SubscriberEventType::NewSubscriber.to_str(),
            "NEW_SUBSCRIBER"
        );
    }

    #[test]
    fn test_load_csv() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "timestamp_ms,event_type,imsi,msisdn,imei,mccmnc"
        )
        .unwrap();
        writeln!(
            file,
            "1704067200000,NEW_SUBSCRIBER,204081234567890,31612345678,123456789012345,20408"
        )
        .unwrap();
        writeln!(
            file,
            "1704153600000,CHANGE_DEVICE,204081234567890,31612345678,987654321098765,20408"
        )
        .unwrap();

        let db = SubscriberDatabase::load_from_csv(file.path()).unwrap();
        assert_eq!(db.event_count(), 2);
        assert_eq!(db.unique_imsi_count(), 1);
    }

    #[test]
    fn test_validate() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "timestamp_ms,event_type,imsi,msisdn,imei,mccmnc"
        )
        .unwrap();
        writeln!(
            file,
            "1704067200000,NEW_SUBSCRIBER,204081234567890,31612345678,123456789012345,20408"
        )
        .unwrap();

        let db = SubscriberDatabase::load_from_csv(file.path()).unwrap();
        assert!(db.validate().is_ok());
    }

    #[test]
    fn test_get_snapshot_at() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            "timestamp_ms,event_type,imsi,msisdn,imei,mccmnc"
        )
        .unwrap();
        writeln!(
            file,
            "1704067200000,NEW_SUBSCRIBER,204081234567890,31612345678,123456789012345,20408"
        )
        .unwrap();
        writeln!(
            file,
            "1704153600000,CHANGE_DEVICE,204081234567890,31612345678,987654321098765,20408"
        )
        .unwrap();

        let mut db = SubscriberDatabase::load_from_csv(file.path()).unwrap();
        db.build_snapshots();

        // Before first event
        let snapshot = db.get_snapshot_at("204081234567890", 1704000000000);
        assert!(snapshot.is_none());

        // After first event, before device change
        let snapshot = db.get_snapshot_at("204081234567890", 1704100000000).unwrap();
        assert_eq!(snapshot.imei, "123456789012345");

        // After device change
        let snapshot = db.get_snapshot_at("204081234567890", 1704200000000).unwrap();
        assert_eq!(snapshot.imei, "987654321098765");
    }
}
