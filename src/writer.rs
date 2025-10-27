// CSV event writer with file rotation
use csv::{Writer, WriterBuilder};
use serde::Serialize;
use std::fs::File;
use std::path::{Path, PathBuf};
use crate::compression::{create_compressed_writer, CompressedWriter, CompressionType};

// EventRow with primitive types for zero-copy performance
// Serde will handle conversion to strings during serialization
#[derive(Debug, Clone, Serialize)]
pub struct EventRow {
    #[serde(serialize_with = "serialize_str")]
    pub event_type: &'static str,
    #[serde(serialize_with = "serialize_u64")]
    pub msisdn_src: u64,
    #[serde(serialize_with = "serialize_u64")]
    pub msisdn_dst: u64,
    #[serde(serialize_with = "serialize_str")]
    pub direction: &'static str,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    #[serde(serialize_with = "serialize_str")]
    pub tz_name: &'static str,
    pub tz_offset_min: i32,
    pub duration_sec: i64,
    #[serde(serialize_with = "serialize_u32")]
    pub mccmnc: u32,
    #[serde(serialize_with = "serialize_u64")]
    pub imsi: u64,
    #[serde(serialize_with = "serialize_u64")]
    pub imei: u64,
    pub cell_id: u32,
    #[serde(serialize_with = "serialize_str")]
    pub record_type: &'static str,
    #[serde(serialize_with = "serialize_str")]
    pub cause_for_record_closing: &'static str,
    #[serde(serialize_with = "serialize_u32_or_empty")]
    pub sms_segments: u32,
    #[serde(serialize_with = "serialize_str")]
    pub sms_status: &'static str,
    #[serde(serialize_with = "serialize_u64_or_empty")]
    pub data_bytes_in: u64,
    #[serde(serialize_with = "serialize_u64_or_empty")]
    pub data_bytes_out: u64,
    #[serde(serialize_with = "serialize_i64_or_empty")]
    pub data_duration_sec: i64,
    #[serde(serialize_with = "serialize_str")]
    pub apn: &'static str,
    #[serde(serialize_with = "serialize_str")]
    pub rat: &'static str,
}

// Custom serializers for efficient conversion
fn serialize_str<S>(value: &&str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(value)
}

fn serialize_u64<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if *value == 0 {
        serializer.serialize_str("")
    } else {
        serializer.serialize_str(&value.to_string())
    }
}

fn serialize_u32<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if *value == 0 {
        serializer.serialize_str("")
    } else {
        serializer.serialize_str(&value.to_string())
    }
}

fn serialize_u64_or_empty<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if *value == 0 {
        serializer.serialize_str("")
    } else {
        serializer.serialize_str(&value.to_string())
    }
}

fn serialize_u32_or_empty<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if *value == 0 {
        serializer.serialize_str("")
    } else {
        serializer.serialize_str(&value.to_string())
    }
}

fn serialize_i64_or_empty<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if *value == 0 {
        serializer.serialize_str("")
    } else {
        serializer.serialize_str(&value.to_string())
    }
}

impl Default for EventRow {
    fn default() -> Self {
        EventRow {
            event_type: "",
            msisdn_src: 0,
            msisdn_dst: 0,
            direction: "",
            start_ts_ms: 0,
            end_ts_ms: 0,
            tz_name: "",
            tz_offset_min: 0,
            duration_sec: 0,
            mccmnc: 0,
            imsi: 0,
            imei: 0,
            cell_id: 0,
            record_type: "",
            cause_for_record_closing: "",
            sms_segments: 0,
            sms_status: "",
            data_bytes_in: 0,
            data_bytes_out: 0,
            data_duration_sec: 0,
            apn: "",
            rat: "",
        }
    }
}

impl EventRow {
    /// Reset all fields to default values for object pool reuse
    pub fn reset(&mut self) {
        self.event_type = "";
        self.msisdn_src = 0;
        self.msisdn_dst = 0;
        self.direction = "";
        self.start_ts_ms = 0;
        self.end_ts_ms = 0;
        self.tz_name = "";
        self.tz_offset_min = 0;
        self.duration_sec = 0;
        self.mccmnc = 0;
        self.imsi = 0;
        self.imei = 0;
        self.cell_id = 0;
        self.record_type = "";
        self.cause_for_record_closing = "";
        self.sms_segments = 0;
        self.sms_status = "";
        self.data_bytes_in = 0;
        self.data_bytes_out = 0;
        self.data_duration_sec = 0;
        self.apn = "";
        self.rat = "";
    }
}

/// Manages rotating CSV files for CDR events
/// Auto-rotates when file size exceeds threshold
/// Each file is compressed on-the-fly with the configured compression algorithm
pub struct EventWriter {
    #[allow(dead_code)]
    out_dir: PathBuf,
    day_str: String,
    rotate_bytes: u64,
    part_num: u32,
    current_writer: Option<Writer<Box<dyn CompressedWriter>>>,
    current_size: u64,
    day_dir: PathBuf,
    shard_id: usize,
    compression_type: CompressionType,
}

impl EventWriter {
    pub fn new(out_dir: &Path, day_str: &str, rotate_bytes: u64, shard_id: usize, compression_type: CompressionType) -> anyhow::Result<Self> {
        let day_dir = out_dir.join(day_str);
        std::fs::create_dir_all(&day_dir)?;

        let mut writer = EventWriter {
            out_dir: out_dir.to_path_buf(),
            day_str: day_str.to_string(),
            rotate_bytes,
            part_num: 1,
            current_writer: None,
            current_size: 0,
            day_dir,
            shard_id,
            compression_type,
        };

        writer.open_new_file()?;
        Ok(writer)
    }

    fn open_new_file(&mut self) -> anyhow::Result<()> {
        // Close current file if any
        if let Some(mut writer) = self.current_writer.take() {
            writer.flush()?;
            // Finish compression
            let mut inner = writer.into_inner().map_err(|e| anyhow::anyhow!("Failed to get inner writer: {}", e))?;
            inner.finish_compression()?;
        }

        let extension = self.compression_type.extension();
        let filename = format!("cdr_{}_shard{:03}_part{:03}.csv{}", self.day_str, self.shard_id, self.part_num, extension);
        let filepath = self.day_dir.join(&filename);

        let file = File::create(&filepath)?;
        // Create compressed writer using factory function
        let compressed = create_compressed_writer(file, self.compression_type)?;

        let wtr = WriterBuilder::new()
            .delimiter(b';')
            .has_headers(true)
            .from_writer(compressed);
        self.current_size = std::fs::metadata(&filepath)?.len();
        self.current_writer = Some(wtr);

        Ok(())
    }

    pub fn write_row(&mut self, row: &EventRow) -> anyhow::Result<()> {
        if let Some(ref mut writer) = self.current_writer {
            writer.serialize(row)?;

            // Estimate row size instead of checking file size every time
            // Average CDR row is ~200-250 bytes
            self.current_size += 230;

            // Check if rotation needed (with periodic verification every 1000 rows)
            if self.current_size >= self.rotate_bytes {
                writer.flush()?;

                // Get actual file size for accuracy
                let extension = self.compression_type.extension();
                let filename = format!("cdr_{}_shard{:03}_part{:03}.csv{}", self.day_str, self.shard_id, self.part_num, extension);
                let filepath = self.day_dir.join(&filename);
                let actual_size = std::fs::metadata(&filepath)?.len();

                if actual_size >= self.rotate_bytes {
                    self.part_num += 1;
                    self.open_new_file()?;
                } else {
                    // Calibrate estimate
                    self.current_size = actual_size;
                }
            }
        }

        Ok(())
    }

    pub fn close(&mut self) -> anyhow::Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            writer.flush()?;
            // Finish compression and flush all buffers
            let mut inner = writer.into_inner().map_err(|e| anyhow::anyhow!("Failed to get inner writer: {}", e))?;
            inner.finish_compression()?;
        }
        Ok(())
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
