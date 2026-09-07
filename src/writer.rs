// CSV+gzip писатель CDR-событий в формате нашего контура.
//
// Формат и порядок полей — точная копия CDR_FIELDS из
// demo/generator/upstream/cdr_generator/models/cdr.py в batch-scoring-installer
// (эталон, читать, не править). Таблица соответствий полей — docs/field-mapping.md.
//
// Партиционирование вывода — по паре (ne_id, дата), как у питоновского CsvWriter
// (writer/csv_writer.py, CDR_{ne_id}_{date}.csv.gz), а не по диапазону абонентов.
// Этап 5: события маршрутизируются к writer-задаче по ne_id (main.rs), поэтому
// каждую пару (ne_id, дата) пишет ровно одна задача — коллизии открытия файла
// больше нет, и жёсткий отказ на writer_tasks > 1 снят.
use csv::{Terminator, Writer, WriterBuilder};
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use crate::compression::{create_compressed_writer, CompressedWriter, CompressionType};

/// Строка CDR в порядке колонок CDR_FIELDS питоновской модели.
/// Поля, которых в rust-генераторе нет (нет данных на этом этапе), — пустые
/// строки; какие именно и почему — docs/field-mapping.md.
#[derive(Debug, Clone, Default, Serialize)]
pub struct EventRow {
    pub record_type: String,
    pub sequence_number: String,
    pub consolidation_id: String,
    pub charging_id: String,
    pub served_imsi: String,
    pub served_msisdn: String,
    pub served_imei: String,
    pub calling_number: String,
    pub called_number: String,
    pub redirecting_number: String,
    pub event_timestamp: String,
    pub answer_timestamp: String,
    pub release_timestamp: String,
    pub duration_seconds: String,
    pub cause_for_termination: String,
    pub first_cell_id: String,
    pub last_cell_id: String,
    pub serving_ne_id: String,
    pub record_opening_time: String,
    pub record_closure_time: String,
    pub uplink_volume_bytes: String,
    pub downlink_volume_bytes: String,
    pub apn: String,
    pub qci: String,
    pub rat_type: String,
    pub vendor_extensions: String,
}

impl EventRow {
    /// Сброс полей для переиспользования из пула объектов.
    pub fn reset(&mut self) {
        self.record_type.clear();
        self.sequence_number.clear();
        self.consolidation_id.clear();
        self.charging_id.clear();
        self.served_imsi.clear();
        self.served_msisdn.clear();
        self.served_imei.clear();
        self.calling_number.clear();
        self.called_number.clear();
        self.redirecting_number.clear();
        self.event_timestamp.clear();
        self.answer_timestamp.clear();
        self.release_timestamp.clear();
        self.duration_seconds.clear();
        self.cause_for_termination.clear();
        self.first_cell_id.clear();
        self.last_cell_id.clear();
        self.serving_ne_id.clear();
        self.record_opening_time.clear();
        self.record_closure_time.clear();
        self.uplink_volume_bytes.clear();
        self.downlink_volume_bytes.clear();
        self.apn.clear();
        self.qci.clear();
        self.rat_type.clear();
        self.vendor_extensions.clear();
    }
}

/// Один открытый файл на пару (ne_id, дата) — без ротации по размеру:
/// у питоновского эталона файл на (ne_id, день) всегда один, part-файлов нет.
struct NeFile {
    writer: Option<Writer<Box<dyn CompressedWriter>>>,
}

impl NeFile {
    fn create(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        // Формат контура фиксирован — gzip, вне зависимости от --compression:
        // имя файла CDR_{ne_id}_{date}.csv.gz предполагает именно его.
        let compressed = create_compressed_writer(file, CompressionType::Gzip)?;
        let writer = WriterBuilder::new()
            .delimiter(b',')
            .terminator(Terminator::CRLF)
            .has_headers(false) // заголовок пишем сами — фиксированный порядок CDR_FIELDS
            .from_writer(compressed);
        Ok(NeFile { writer: Some(writer) })
    }

    fn write_header(&mut self) -> anyhow::Result<()> {
        self.writer.as_mut().unwrap().write_record(CDR_FIELDS)?;
        Ok(())
    }

    fn write_row(&mut self, row: &EventRow) -> anyhow::Result<()> {
        self.writer.as_mut().unwrap().serialize(row)?;
        Ok(())
    }

    /// Закрывает файл: флашит CSV-буфер и дописывает трейлер gzip
    /// (без этого последний блок данных gzip остаётся нечитаемым).
    fn close(&mut self) -> anyhow::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            let mut inner = writer
                .into_inner()
                .map_err(|e| anyhow::anyhow!("Failed to get inner writer: {}", e))?;
            inner.finish_compression()?;
        }
        Ok(())
    }
}

/// Порядок колонок — как CDR_FIELDS в demo/generator/upstream/cdr_generator/models/cdr.py.
pub const CDR_FIELDS: &[&str] = &[
    "record_type",
    "sequence_number",
    "consolidation_id",
    "charging_id",
    "served_imsi",
    "served_msisdn",
    "served_imei",
    "calling_number",
    "called_number",
    "redirecting_number",
    "event_timestamp",
    "answer_timestamp",
    "release_timestamp",
    "duration_seconds",
    "cause_for_termination",
    "first_cell_id",
    "last_cell_id",
    "serving_ne_id",
    "record_opening_time",
    "record_closure_time",
    "uplink_volume_bytes",
    "downlink_volume_bytes",
    "apn",
    "qci",
    "rat_type",
    "vendor_extensions",
];

/// Писатель CDR-событий одной writer-задачи: держит по одному открытому файлу
/// на каждую встреченную пару (ne_id, дата), партиционируя строки по ней.
///
/// Этап 4/5: параллелизм переведён на даты, и одна writer-задача теперь
/// живёт весь прогон (а не один день, как раньше) — значит через неё за
/// время жизни проходят события НЕСКОЛЬКИХ дат, и дата больше не может быть
/// полем самого писателя. Ключ файловой карты — (ne_id, date_str).
pub struct EventWriter {
    out_dir: PathBuf,
    files: HashMap<(String, String), NeFile>,
}

impl EventWriter {
    pub fn new(out_dir: &Path) -> anyhow::Result<Self> {
        Ok(EventWriter {
            out_dir: out_dir.to_path_buf(),
            files: HashMap::new(),
        })
    }

    fn resolve_path(&self, ne_id: &str, date_str: &str) -> PathBuf {
        // out_dir/ne_id/CDR_{ne_id}_{date}.csv.gz — как _resolve_path в эталоне.
        self.out_dir
            .join(ne_id)
            .join(format!("CDR_{}_{}.csv.gz", ne_id, date_str))
    }

    /// `date_str` уже должен быть в формате YYYYMMDD (не "%Y-%m-%d") —
    /// именно так его подставляет питоновский CsvWriter в имя файла.
    pub fn write_row(&mut self, ne_id: &str, date_str: &str, row: &EventRow) -> anyhow::Result<()> {
        let key = (ne_id.to_string(), date_str.to_string());
        if !self.files.contains_key(&key) {
            let path = self.resolve_path(ne_id, date_str);
            let mut f = NeFile::create(&path)?;
            f.write_header()?;
            self.files.insert(key.clone(), f);
        }
        let f = self.files.get_mut(&key).unwrap();
        f.write_row(row)
    }

    pub fn close(&mut self) -> anyhow::Result<()> {
        for (_, f) in self.files.iter_mut() {
            f.close()?;
        }
        Ok(())
    }

    /// Закрывает и снимает с руки файлы КОНКРЕТНОЙ даты (по всем ne_id,
    /// встреченным в ней), не трогая файлы других дат. Позволяет писателю,
    /// который живёт весь многодневный прогон (этап 5), не держать открытыми
    /// файлы всех дней сразу — тот самый источник пика RSS, растущего
    /// с числом дней в прогоне.
    pub fn close_date(&mut self, date_str: &str) -> anyhow::Result<()> {
        let keys: Vec<(String, String)> = self
            .files
            .keys()
            .filter(|(_, d)| d == date_str)
            .cloned()
            .collect();
        for key in keys {
            if let Some(mut f) = self.files.remove(&key) {
                f.close()?;
            }
        }
        Ok(())
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read as _;

    /// Сквозная проверка формата: писатель должен дать имя файла, разделитель
    /// и заголовок ровно как у питоновского эталона (writer/csv_writer.py).
    /// Раньше здесь была оговорка про несовпадение схемы MSISDN между
    /// generate-subscribers и chunked-читателем — дефект устранён этапом 3
    /// (worker_generate_redb_chunked теперь берёт реальные MSISDN из базы,
    /// а не вычисляет их по индексу), сквозной прогон через CLI проверяется
    /// отдельно (docs/field-mapping.md).
    #[test]
    fn header_matches_python_reference_format() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = EventWriter::new(dir.path()).unwrap();

        let mut row = EventRow::default();
        row.record_type = "mo_call".to_string();
        row.served_imsi = "434050000001".to_string();
        row.served_msisdn = "998900000001".to_string();
        w.write_row("msc-01", "20250305", &row).unwrap();
        w.close().unwrap();

        let path = dir.path().join("msc-01").join("CDR_msc-01_20250305.csv.gz");
        assert!(path.exists(), "ожидался файл {:?}", path);

        let file = std::fs::File::open(&path).unwrap();
        let mut decoder = GzDecoder::new(file);
        let mut contents = String::new();
        decoder.read_to_string(&mut contents).unwrap();

        let expected_header = CDR_FIELDS.join(",") + "\r\n";
        let first_line_end = contents.find('\n').unwrap() + 1;
        assert_eq!(&contents[..first_line_end], expected_header);
    }
}
