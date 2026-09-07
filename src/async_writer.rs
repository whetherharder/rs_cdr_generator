// Async batched writer for CDR events using Tokio
use crate::writer::{EventRow, EventWriter};
use anyhow::Result;
use crossbeam_channel::Receiver;
use std::path::PathBuf;

/// Batch of EventRow objects ready to be written.
///
/// `date_compact` (YYYYMMDD) — все события в одной пачке принадлежат одному
/// дню, потому что источник пачки (рабочий элемент параллелизма) всегда один
/// день; храним дату на пачке, а не вычисляем её из события, чтобы не парсить
/// `event_timestamp` в писателе.
pub struct EventBatch {
    pub events: Vec<EventRow>,
    pub estimated_size: usize,
    pub date_compact: String,
}

impl EventBatch {
    pub fn new(capacity: usize, date_compact: &str) -> Self {
        EventBatch {
            events: Vec::with_capacity(capacity),
            estimated_size: 0,
            date_compact: date_compact.to_string(),
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
    /// Все work item'ы конкретной даты (день × куски общего пула) отправили
    /// свои пачки — можно закрыть (флашнуть gzip-трейлер) и снять с руки
    /// файлы этой даты, не дожидаясь конца всего прогона. Без этого сигнала
    /// writer-задача, живущая весь прогон (этап 5), держит открытыми файлы
    /// ВСЕХ дней сразу — при 32 днях и 5 сетевых элементах это 160
    /// одновременно открытых `NeFile`, и пик RSS растёт с числом дней в
    /// прогоне (проверено: 2 дня → 315 МБ, 32 дня → 1236 МБ), хотя
    /// параллельно обрабатывается не больше `--workers` work item'ов.
    CloseDate(String),
    Close,
}

/// Async writer task, обрабатывающий пачки событий.
/// Партиционирование по файлам — внутри EventWriter, по (ne_id, дата)
/// (см. writer.rs). Задача живёт весь прогон (все дни), а не один день,
/// как раньше: параллелизм этапа 4 идёт по датам, и одна и та же
/// writer-задача получает пачки за разные дни — дату берём из самой пачки
/// (`EventBatch::date_compact`), а не фиксируем на входе.
///
/// Маршрутизация событий к writer-задаче — по ne_id (main.rs, `writer_idx`),
/// поэтому каждую пару (ne_id, дата) пишет ровно одна задача: коллизии
/// открытия файла нет, и `writer_tasks > 1` больше не запрещён (этап 5).
pub async fn writer_task(
    rx: Receiver<WriterMessage>,
    out_dir: PathBuf,
    shard_id: usize,
) -> Result<()> {
    // Run in spawn_blocking since we're doing sync I/O with persistent writer
    tokio::task::spawn_blocking(move || writer_task_blocking(rx, out_dir, shard_id))
        .await?
}

/// Blocking writer task that reuses EventWriter for all batches (OPTIMIZATION #5)
fn writer_task_blocking(
    rx: Receiver<WriterMessage>,
    out_dir: PathBuf,
    shard_id: usize,
) -> Result<()> {
    // Create EventWriter once and reuse it for all batches (OPTIMIZATION #5)
    let mut writer = EventWriter::new(&out_dir)?;

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
                    writer.write_row(&event.serving_ne_id, &batch.date_compact, event)?;
                }

                total_written += batch.len();
            }
            WriterMessage::CloseDate(date_str) => {
                writer.close_date(&date_str)?;
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
        let mut batch = EventBatch::new(100, "20250101");
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
        let mut batch = EventBatch::new(10, "20250101");
        let max_size = 1000;

        // Add events until full
        for _ in 0..5 {
            batch.push(EventRow::default());
        }

        assert!(batch.is_full(max_size));
    }
}
