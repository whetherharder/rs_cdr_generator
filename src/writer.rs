// CSV event writer with file rotation
use csv::{Writer, WriterBuilder};
use serde::Serialize;
use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize)]
pub struct EventRow {
    pub event_type: String,
    pub msisdn_src: String,
    pub msisdn_dst: String,
    pub direction: String,
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub tz_name: String,
    pub tz_offset_min: i32,
    pub duration_sec: i64,
    pub mccmnc: String,
    pub imsi: String,
    pub imei: String,
    pub cell_id: u32,
    pub record_type: String,
    pub cause_for_record_closing: String,
    pub sms_segments: String,
    pub sms_status: String,
    pub data_bytes_in: String,
    pub data_bytes_out: String,
    pub data_duration_sec: String,
    pub apn: String,
    pub rat: String,
}

impl Default for EventRow {
    fn default() -> Self {
        EventRow {
            event_type: String::new(),
            msisdn_src: String::new(),
            msisdn_dst: String::new(),
            direction: String::new(),
            start_ts_ms: 0,
            end_ts_ms: 0,
            tz_name: String::new(),
            tz_offset_min: 0,
            duration_sec: 0,
            mccmnc: String::new(),
            imsi: String::new(),
            imei: String::new(),
            cell_id: 0,
            record_type: String::new(),
            cause_for_record_closing: String::new(),
            sms_segments: String::new(),
            sms_status: String::new(),
            data_bytes_in: String::new(),
            data_bytes_out: String::new(),
            data_duration_sec: String::new(),
            apn: String::new(),
            rat: String::new(),
        }
    }
}

/// Manages rotating CSV files for CDR events
/// Auto-rotates when file size exceeds threshold
pub struct EventWriter {
    #[allow(dead_code)]
    out_dir: PathBuf,
    day_str: String,
    rotate_bytes: u64,
    part_num: u32,
    current_writer: Option<Writer<File>>,
    current_size: u64,
    day_dir: PathBuf,
    shard_id: usize,
}

impl EventWriter {
    pub fn new(out_dir: &Path, day_str: &str, rotate_bytes: u64, shard_id: usize) -> anyhow::Result<Self> {
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
        };

        writer.open_new_file()?;
        Ok(writer)
    }

    fn open_new_file(&mut self) -> anyhow::Result<()> {
        // Close current file if any
        if let Some(mut writer) = self.current_writer.take() {
            writer.flush()?;
        }

        let filename = format!("cdr_{}_shard{:03}_part{:03}.csv", self.day_str, self.shard_id, self.part_num);
        let filepath = self.day_dir.join(&filename);

        let file = File::create(&filepath)?;
        let wtr = WriterBuilder::new()
            .delimiter(b';')
            .has_headers(true)
            .from_writer(file);
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
                let filename = format!("cdr_{}_shard{:03}_part{:03}.csv", self.day_str, self.shard_id, self.part_num);
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
        }
        Ok(())
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
