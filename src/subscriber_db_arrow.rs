// Apache Arrow IPC format support for subscriber database
use crate::subscriber_db::{SubscriberEvent, SubscriberEventType};
use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, ArrayRef, UInt32Array, UInt64Array, UInt8Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Convert string identifiers to u64
/// Format: IMSI/MSISDN/IMEI are 15 digits max, fit in u64
pub fn id_string_to_u64(s: &str) -> Result<u64> {
    s.parse::<u64>()
        .with_context(|| format!("Failed to parse ID as u64: {}", s))
}

/// Convert u64 back to string identifier
pub fn id_u64_to_string(n: u64, width: usize) -> String {
    format!("{:0width$}", n, width = width)
}

/// Convert MCCMNC string to u32
pub fn mccmnc_to_u32(s: &str) -> Result<u32> {
    s.parse::<u32>()
        .with_context(|| format!("Failed to parse MCCMNC as u32: {}", s))
}

/// Convert u32 back to MCCMNC string
pub fn mccmnc_u32_to_string(n: u32) -> String {
    n.to_string()
}

/// Create Arrow schema for subscriber events
pub fn subscriber_event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("event_type", DataType::UInt8, false),
        Field::new("imsi", DataType::UInt64, false),
        Field::new("msisdn", DataType::UInt64, true), // nullable
        Field::new("imei", DataType::UInt64, true),   // nullable
        Field::new("mccmnc", DataType::UInt32, false),
    ]))
}

/// Convert SubscriberEvent to Arrow arrays (batch) - public for generator
pub fn events_to_record_batch(events: &[SubscriberEvent]) -> Result<RecordBatch> {
    let schema = subscriber_event_schema();

    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_ms).collect();
    let event_types: Vec<u8> = events.iter().map(|e| e.event_type as u8).collect();
    let imsis: Vec<u64> = events
        .iter()
        .map(|e| id_string_to_u64(&e.imsi))
        .collect::<Result<_>>()?;

    let msisdns: Vec<Option<u64>> = events
        .iter()
        .map(|e| e.msisdn.as_ref().and_then(|s| id_string_to_u64(s).ok()))
        .collect();

    let imeis: Vec<Option<u64>> = events
        .iter()
        .map(|e| e.imei.as_ref().and_then(|s| id_string_to_u64(s).ok()))
        .collect();

    let mccmncs: Vec<u32> = events
        .iter()
        .map(|e| mccmnc_to_u32(&e.mccmnc))
        .collect::<Result<_>>()?;

    let timestamp_array = Arc::new(Int64Array::from(timestamps)) as ArrayRef;
    let event_type_array = Arc::new(UInt8Array::from(event_types)) as ArrayRef;
    let imsi_array = Arc::new(UInt64Array::from(imsis)) as ArrayRef;
    let msisdn_array = Arc::new(UInt64Array::from(msisdns)) as ArrayRef;
    let imei_array = Arc::new(UInt64Array::from(imeis)) as ArrayRef;
    let mccmnc_array = Arc::new(UInt32Array::from(mccmncs)) as ArrayRef;

    RecordBatch::try_new(
        schema,
        vec![
            timestamp_array,
            event_type_array,
            imsi_array,
            msisdn_array,
            imei_array,
            mccmnc_array,
        ],
    )
    .context("Failed to create RecordBatch")
}

/// Write events to Arrow IPC file in batches
pub fn write_events_to_arrow<P: AsRef<Path>>(
    events: &[SubscriberEvent],
    path: P,
    batch_size: usize,
) -> Result<()> {
    let schema = subscriber_event_schema();
    let file = File::create(&path)
        .with_context(|| format!("Failed to create Arrow file: {:?}", path.as_ref()))?;

    let mut writer = FileWriter::try_new(file, &schema)
        .context("Failed to create Arrow writer")?;

    // Write in batches
    for chunk in events.chunks(batch_size) {
        let batch = events_to_record_batch(chunk)?;
        writer.write(&batch).context("Failed to write batch")?;
    }

    writer.finish().context("Failed to finish Arrow file")?;
    Ok(())
}

/// Read all events from Arrow IPC file
pub fn read_events_from_arrow<P: AsRef<Path>>(path: P) -> Result<Vec<SubscriberEvent>> {
    let file = File::open(&path)
        .with_context(|| format!("Failed to open Arrow file: {:?}", path.as_ref()))?;

    let reader = FileReader::try_new(file, None)
        .context("Failed to create Arrow reader")?;

    let mut events = Vec::new();

    for batch_result in reader {
        let batch = batch_result.context("Failed to read batch")?;
        events.extend(record_batch_to_events(&batch)?);
    }

    Ok(events)
}

/// Read events from Arrow IPC file with timestamp range filter
pub fn read_events_from_arrow_range<P: AsRef<Path>>(
    path: P,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<SubscriberEvent>> {
    let file = File::open(&path)
        .with_context(|| format!("Failed to open Arrow file: {:?}", path.as_ref()))?;

    let reader = FileReader::try_new(file, None)
        .context("Failed to create Arrow reader")?;

    let mut events = Vec::new();

    for batch_result in reader {
        let batch = batch_result.context("Failed to read batch")?;

        // Filter events by timestamp
        let filtered = filter_batch_by_timestamp(&batch, start_ts, end_ts)?;
        events.extend(filtered);
    }

    Ok(events)
}

/// Read events from Arrow IPC file with timestamp range and MSISDN set filter
/// This filters during reading to minimize memory usage
pub fn read_events_from_arrow_filtered<P: AsRef<Path>>(
    path: P,
    start_ts: i64,
    end_ts: i64,
    msisdn_set: &std::collections::HashSet<String>,
) -> Result<Vec<SubscriberEvent>> {
    let file = File::open(&path)
        .with_context(|| format!("Failed to open Arrow file: {:?}", path.as_ref()))?;

    let reader = FileReader::try_new(file, None)
        .context("Failed to create Arrow reader")?;

    let mut events = Vec::new();

    for batch_result in reader {
        let batch = batch_result.context("Failed to read batch")?;

        // Filter by timestamp AND MSISDN
        let filtered = filter_batch_by_timestamp_and_msisdn(&batch, start_ts, end_ts, msisdn_set)?;
        events.extend(filtered);
    }

    Ok(events)
}

/// Convert Arrow RecordBatch to SubscriberEvent vector
fn record_batch_to_events(batch: &RecordBatch) -> Result<Vec<SubscriberEvent>> {
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("Invalid timestamp column"))?;

    let event_types = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| anyhow!("Invalid event_type column"))?;

    let imsis = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("Invalid imsi column"))?;

    let msisdns = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("Invalid msisdn column"))?;

    let imeis = batch
        .column(4)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("Invalid imei column"))?;

    let mccmncs = batch
        .column(5)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| anyhow!("Invalid mccmnc column"))?;

    let mut events = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        let event_type_u8 = event_types.value(i);
        let event_type = match event_type_u8 {
            0 => SubscriberEventType::NewSubscriber,
            1 => SubscriberEventType::ChangeDevice,
            2 => SubscriberEventType::ChangeSim,
            3 => SubscriberEventType::ReleaseNumber,
            4 => SubscriberEventType::AssignNumber,
            _ => return Err(anyhow!("Invalid event type: {}", event_type_u8)),
        };

        events.push(SubscriberEvent {
            timestamp_ms: timestamps.value(i),
            event_type,
            imsi: id_u64_to_string(imsis.value(i), 15),
            msisdn: if msisdns.is_null(i) {
                None
            } else {
                Some(id_u64_to_string(msisdns.value(i), 12))
            },
            imei: if imeis.is_null(i) {
                None
            } else {
                Some(id_u64_to_string(imeis.value(i), 15))
            },
            mccmnc: mccmnc_u32_to_string(mccmncs.value(i)),
        });
    }

    Ok(events)
}

/// Filter RecordBatch by timestamp range
fn filter_batch_by_timestamp(
    batch: &RecordBatch,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<SubscriberEvent>> {
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("Invalid timestamp column"))?;

    // Quick check: if batch is entirely outside range, skip
    let min_ts = (0..timestamps.len()).map(|i| timestamps.value(i)).min();
    let max_ts = (0..timestamps.len()).map(|i| timestamps.value(i)).max();

    if let (Some(min), Some(max)) = (min_ts, max_ts) {
        if max < start_ts || min > end_ts {
            return Ok(Vec::new());
        }
    }

    // Convert all events and filter
    let all_events = record_batch_to_events(batch)?;
    Ok(all_events
        .into_iter()
        .filter(|e| e.timestamp_ms >= start_ts && e.timestamp_ms <= end_ts)
        .collect())
}

/// Filter RecordBatch by timestamp range AND MSISDN set
fn filter_batch_by_timestamp_and_msisdn(
    batch: &RecordBatch,
    start_ts: i64,
    end_ts: i64,
    msisdn_set: &std::collections::HashSet<String>,
) -> Result<Vec<SubscriberEvent>> {
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("Invalid timestamp column"))?;

    // Quick check: if batch is entirely outside time range, skip
    let min_ts = (0..timestamps.len()).map(|i| timestamps.value(i)).min();
    let max_ts = (0..timestamps.len()).map(|i| timestamps.value(i)).max();

    if let (Some(min), Some(max)) = (min_ts, max_ts) {
        if max < start_ts || min > end_ts {
            return Ok(Vec::new());
        }
    }

    // Convert all events and filter by both timestamp and MSISDN
    let all_events = record_batch_to_events(batch)?;
    Ok(all_events
        .into_iter()
        .filter(|e| {
            e.timestamp_ms >= start_ts
                && e.timestamp_ms <= end_ts
                && e.msisdn.as_ref().map_or(false, |m| msisdn_set.contains(m))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_id_conversion() {
        let id = "123456789012345";
        let u64_val = id_string_to_u64(id).unwrap();
        let back = id_u64_to_string(u64_val, 15);
        assert_eq!(id, back);
    }

    #[test]
    fn test_mccmnc_conversion() {
        let mccmnc = "25099";
        let u32_val = mccmnc_to_u32(mccmnc).unwrap();
        let back = mccmnc_u32_to_string(u32_val);
        assert_eq!(mccmnc, back);
    }

    #[test]
    fn test_write_read_arrow() {
        let events = vec![
            SubscriberEvent {
                timestamp_ms: 1704067200000,
                event_type: SubscriberEventType::NewSubscriber,
                imsi: "250990000000001".to_string(),
                msisdn: Some("79160000001".to_string()),
                imei: Some("123456789012345".to_string()),
                mccmnc: "25099".to_string(),
            },
            SubscriberEvent {
                timestamp_ms: 1704153600000,
                event_type: SubscriberEventType::ChangeDevice,
                imsi: "250990000000001".to_string(),
                msisdn: Some("79160000001".to_string()),
                imei: Some("987654321098765".to_string()),
                mccmnc: "25099".to_string(),
            },
        ];

        let temp_file = NamedTempFile::new().unwrap();
        write_events_to_arrow(&events, temp_file.path(), 1000).unwrap();

        let read_events = read_events_from_arrow(temp_file.path()).unwrap();
        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].imsi, "250990000000001");
        assert_eq!(read_events[1].event_type, SubscriberEventType::ChangeDevice);
    }

    #[test]
    fn test_range_query() {
        let events = vec![
            SubscriberEvent {
                timestamp_ms: 1704067200000,
                event_type: SubscriberEventType::NewSubscriber,
                imsi: "250990000000001".to_string(),
                msisdn: Some("79160000001".to_string()),
                imei: Some("123456789012345".to_string()),
                mccmnc: "25099".to_string(),
            },
            SubscriberEvent {
                timestamp_ms: 1704153600000,
                event_type: SubscriberEventType::ChangeDevice,
                imsi: "250990000000001".to_string(),
                msisdn: Some("79160000001".to_string()),
                imei: Some("987654321098765".to_string()),
                mccmnc: "25099".to_string(),
            },
            SubscriberEvent {
                timestamp_ms: 1704240000000,
                event_type: SubscriberEventType::ReleaseNumber,
                imsi: "250990000000001".to_string(),
                msisdn: Some("79160000001".to_string()),
                imei: None,
                mccmnc: "25099".to_string(),
            },
        ];

        let temp_file = NamedTempFile::new().unwrap();
        write_events_to_arrow(&events, temp_file.path(), 1000).unwrap();

        // Query middle event
        let filtered = read_events_from_arrow_range(
            temp_file.path(),
            1704150000000,
            1704160000000,
        ).unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].event_type, SubscriberEventType::ChangeDevice);
    }
}
