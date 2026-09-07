// Async batched writer for CDR events using Tokio
use crate::writer::{EventRow, EventWriter};
use anyhow::Result;
use crossbeam_channel::Receiver;
use std::path::PathBuf;

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

/// Async writer task, обрабатывающий пачки событий.
/// Партиционирование по файлам — внутри EventWriter, по (ne_id, дата)
/// (см. writer.rs); `date_str` обязан быть в формате YYYYMMDD.
/// 🔴 При нескольких writer_tasks одновременно разные задачи этой функции
/// могут получить события с одинаковым ne_id и независимо открыть/перезаписать
/// один и тот же файл CDR_{ne_id}_{date}.csv.gz — маршрутизация round-robin
/// идёт по индексу воркера (main.rs), а не по ne_id. Исправление требует
/// перевода маршрутизации на ne_id, то есть правки архитектуры параллелизма
/// (сознательно оставлено следующему этапу — docs/field-mapping.md).
pub async fn writer_task(
    rx: Receiver<WriterMessage>,
    out_dir: PathBuf,
    date_str: String,
    shard_id: usize,
) -> Result<()> {
    // Run in spawn_blocking since we're doing sync I/O with persistent writer
    tokio::task::spawn_blocking(move || writer_task_blocking(rx, out_dir, date_str, shard_id))
        .await?
}

/// Blocking writer task that reuses EventWriter for all batches (OPTIMIZATION #5)
fn writer_task_blocking(
    rx: Receiver<WriterMessage>,
    out_dir: PathBuf,
    date_str: String,
    shard_id: usize,
) -> Result<()> {
    // Create EventWriter once and reuse it for all batches (OPTIMIZATION #5)
    let mut writer = EventWriter::new(&out_dir, &date_str)?;

    let mut total_written = 0usize;

    // Process batches from channel
    loop {
        let msg = match rx.recv() {
            Ok(msg) => msg,
            Err(_) => break, // Channel closed
        };

        match msg {
            WriterMessage::Batch(batch) => {
                if batch.is_empty() {
                    continue;
                }

                // Write all events in batch using persistent writer (OPTIMIZATION #5)
                for event in &batch.events {
                    writer.write_row(&event.serving_ne_id, event)?;
                }

                total_written += batch.len();
            }
            WriterMessage::Close => {
                break;
            }
        }
    }

    // Close writer (flushes and finishes compression)
    writer.close()?;

    println!(
        "Writer task for shard {} completed: {} events written",
        shard_id, total_written
    );

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
