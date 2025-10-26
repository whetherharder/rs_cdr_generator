// Async batched writer for CDR events using Tokio
use crate::writer::EventRow;
use anyhow::Result;
use csv::WriterBuilder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

/// Batch of EventRow objects ready to be written
pub struct EventBatch {
    pub events: Vec<EventRow>,
    pub estimated_size: usize,
}

impl EventBatch {
    pub fn new(capacity: usize) -> Self {
        EventBatch {
            events: Vec::with_capacity(capacity),
            estimated_size: 0,
        }
    }

    pub fn push(&mut self, event: EventRow) {
        self.estimated_size += 230; // Estimated row size
        self.events.push(event);
    }

    pub fn is_full(&self, max_size: usize) -> bool {
        self.estimated_size >= max_size
    }

    pub fn clear(&mut self) {
        self.events.clear();
        self.estimated_size = 0;
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Message types for async writer communication
pub enum WriterMessage {
    Batch(EventBatch),
    Close,
}

/// Async writer task that processes batches of events
pub async fn writer_task(
    mut rx: mpsc::Receiver<WriterMessage>,
    out_dir: PathBuf,
    day_str: String,
    shard_id: usize,
) -> Result<()> {
    // Create output directory
    let day_dir = out_dir.join(&day_str);
    tokio::fs::create_dir_all(&day_dir).await?;

    let mut part_num = 1;
    let mut total_written = 0usize;

    while let Some(msg) = rx.recv().await {
        match msg {
            WriterMessage::Batch(batch) => {
                if batch.is_empty() {
                    continue;
                }

                let batch_len = batch.len();

                // Write batch to file (blocking I/O in spawn_blocking)
                let day_dir_clone = day_dir.clone();
                let day_str_clone = day_str.clone();

                tokio::task::spawn_blocking(move || {
                    write_batch_to_file(
                        &batch,
                        &day_dir_clone,
                        &day_str_clone,
                        shard_id,
                        part_num,
                    )
                })
                .await??;

                total_written += batch_len;
                part_num += 1;
            }
            WriterMessage::Close => {
                break;
            }
        }
    }

    println!(
        "Writer task for shard {} completed: {} events written",
        shard_id, total_written
    );

    Ok(())
}

/// Write a batch of events to a gzip-compressed CSV file
fn write_batch_to_file(
    batch: &EventBatch,
    day_dir: &Path,
    day_str: &str,
    shard_id: usize,
    part_num: usize,
) -> Result<()> {
    let filename = format!(
        "cdr_{}_shard{:03}_part{:03}.csv.gz",
        day_str, shard_id, part_num
    );
    let filepath = day_dir.join(&filename);

    let file = File::create(&filepath)?;
    let buffered = BufWriter::with_capacity(256 * 1024, file);
    let compressed = GzEncoder::new(buffered, Compression::default());

    let mut wtr = WriterBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .from_writer(compressed);

    for event in &batch.events {
        wtr.serialize(event)?;
    }

    wtr.flush()?;
    wtr.into_inner()?.finish()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_batch() {
        let mut batch = EventBatch::new(100);
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());

        let event = EventRow::default();
        batch.push(event);
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
        assert_eq!(batch.estimated_size, 230);
    }

    #[test]
    fn test_batch_full() {
        let mut batch = EventBatch::new(10);
        let max_size = 1000;

        // Add events until full
        for _ in 0..5 {
            batch.push(EventRow::default());
        }

        assert!(batch.is_full(max_size));
    }
}
