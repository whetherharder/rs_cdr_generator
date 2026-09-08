// Проверка целостности всех gzip-файлов, порождённых полным прогоном бинаря
// (generate-subscribers → generate-cdr), при workers > 1 (writer_tasks > 1).
//
// Зачем именно так, а не через worker_generate() напрямую: дефект, который
// ловит этот тест (маршрутизация DATA-событий к writer-задаче не совпадала
// с реальным serving_ne_id, см. generators.rs, route_writer_idx), проявлялся
// только при НЕСКОЛЬКИХ конкурентных writer-задачах, разбирающих общий канал
// (main.rs, writer_task/async_writer.rs) — юнит-вызов одного воркера этого
// не воспроизводит. Порча тихая: запись проходит без единой ошибки в логе,
// и обнаруживается только целевой проверкой читаемости файла (`gzip -t`
// на живом прогоне нашёл 480 из 1200 битых файлов уже после того, как
// генератор отчитался об успехе).
use std::process::Command;

fn bin_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_rs_cdr_generator"))
}

/// Читает gzip-файл до конца через flate2 — так же, как `gzip -t`, ловит
/// CRC/data-stream error у файла, чей поток не был корректно завершён
/// (недописанный трейлер) или был испорчен параллельной записью.
fn assert_gzip_readable(path: &std::path::Path) {
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::io::Read;

    let file = File::open(path).unwrap_or_else(|e| panic!("не открылся {path:?}: {e}"));
    let mut decoder = GzDecoder::new(file);
    let mut buf = Vec::new();
    decoder
        .read_to_end(&mut buf)
        .unwrap_or_else(|e| panic!("gzip повреждён (не проходит gzip -t): {path:?}: {e}"));
}

#[test]
fn test_generated_output_passes_gzip_integrity() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("subs.redb");
    let out_dir = tmp.path().join("out");

    let status = Command::new(bin_path())
        .args([
            "generate-subscribers",
            "-o",
            db_path.to_str().unwrap(),
            "--size",
            "2000",
            "--history-days",
            "5",
        ])
        .status()
        .expect("не удалось запустить generate-subscribers");
    assert!(status.success(), "generate-subscribers завершился с ошибкой");

    // workers=6 → writer_tasks=3 (workers/2): нужно НЕСКОЛЬКО writer-задач,
    // иначе коллизия маршрутизации (см. докстринг файла) не воспроизводится.
    let status = Command::new(bin_path())
        .args([
            "generate-cdr",
            "--subscriber-db",
            db_path.to_str().unwrap(),
            "--start",
            "2025-01-01",
            "--days",
            "5",
            "--out",
            out_dir.to_str().unwrap(),
            "--workers",
            "6",
        ])
        .status()
        .expect("не удалось запустить generate-cdr");
    assert!(status.success(), "generate-cdr завершился с ошибкой");

    let mut checked = 0usize;
    for entry in walkdir::WalkDir::new(&out_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("gz") {
            assert_gzip_readable(path);
            checked += 1;
        }
    }

    // sgw-01 и pgw-01 — те самые файлы, что бились в проде (100% data-
    // трафика). Явно требуем, чтобы они реально были проверены, а не
    // "проверка нашла 0 файлов и тест зелёный по недоразумению".
    assert!(
        checked >= 20,
        "проверено подозрительно мало gzip-файлов: {checked}"
    );
}
