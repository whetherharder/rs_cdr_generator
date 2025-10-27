use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rs_cdr_generator::writer::EventRow;
use csv::{Writer, WriterBuilder};
use tempfile::NamedTempFile;
use std::io::Write as IoWrite;

fn create_test_event() -> EventRow {
    EventRow {
        event_type: "CALL",
        msisdn_src: 79161234567,
        msisdn_dst: 79169876543,
        direction: "MO",
        start_ts_ms: 1705320000000,
        end_ts_ms: 1705320180000,
        tz_name: "Europe/Moscow",
        tz_offset_min: 180,
        duration_sec: 180,
        mccmnc: 25001,
        imsi: 250011234567890,
        imei: 123456789012345,
        cell_id: 12345,
        record_type: "mscVoiceRecord",
        cause_for_record_closing: "normalRelease",
        sms_segments: 0,
        sms_status: "",
        data_bytes_in: 0,
        data_bytes_out: 0,
        data_duration_sec: 0,
        apn: "",
        rat: "",
    }
}

fn benchmark_csv_serialization(c: &mut Criterion) {
    let event = create_test_event();
    let mut group = c.benchmark_group("csv_serialization");

    for count in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                let temp_file = NamedTempFile::new().unwrap();
                let file = temp_file.reopen().unwrap();
                let mut writer = WriterBuilder::new()
                    .delimiter(b';')
                    .has_headers(true)
                    .from_writer(file);

                for _ in 0..count {
                    writer.serialize(black_box(&event)).unwrap();
                }
                writer.flush().unwrap();
            });
        });
    }
    group.finish();
}

fn benchmark_csv_buffer_sizes(c: &mut Criterion) {
    let event = create_test_event();
    let count = 10000;
    let mut group = c.benchmark_group("csv_buffer_sizes");

    for buffer_size in [8 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(buffer_size / 1024),
            buffer_size,
            |b, &buffer_size| {
                b.iter(|| {
                    let temp_file = NamedTempFile::new().unwrap();
                    let file = temp_file.reopen().unwrap();
                    let buffered = std::io::BufWriter::with_capacity(buffer_size, file);
                    let mut writer = WriterBuilder::new()
                        .delimiter(b';')
                        .has_headers(true)
                        .from_writer(buffered);

                    for _ in 0..count {
                        writer.serialize(black_box(&event)).unwrap();
                    }
                    writer.flush().unwrap();
                });
            },
        );
    }
    group.finish();
}

fn benchmark_event_row_clone(c: &mut Criterion) {
    let event = create_test_event();
    let mut group = c.benchmark_group("event_row_clone");

    for count in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                for _ in 0..count {
                    let _cloned = black_box(event.clone());
                }
            });
        });
    }
    group.finish();
}

fn benchmark_event_row_reset(c: &mut Criterion) {
    let mut event = create_test_event();
    let mut group = c.benchmark_group("event_row_reset");

    for count in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                for _ in 0..count {
                    black_box(&mut event).reset();
                }
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_csv_serialization,
    benchmark_csv_buffer_sizes,
    benchmark_event_row_clone,
    benchmark_event_row_reset
);
criterion_main!(benches);
