use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rs_cdr_generator::config::Config;
use rs_cdr_generator::generators::{CallGenerator, SmsGenerator, DataGenerator};
use rs_cdr_generator::writer::EventRow;
use rs_cdr_generator::identity::Subscriber;
use rs_cdr_generator::compression::{CompressionType, create_compressed_writer};
use rs_cdr_generator::event_pool::EventPool;
use rand::SeedableRng;
use rand::rngs::StdRng;
use chrono_tz::Tz;
use chrono::TimeZone;
use csv::WriterBuilder;
use std::collections::HashMap;
use tempfile::NamedTempFile;
use std::path::Path;

fn create_test_config() -> Config {
    let config_path = std::env::var("BENCH_CONFIG")
        .unwrap_or_else(|_| "benches/configs/benchmark_micro.yaml".to_string());
    rs_cdr_generator::config::load_config(Some(Path::new(&config_path)))
        .unwrap_or_else(|err| panic!("Failed to load config ({config_path}): {err}"))
}

fn create_test_subscriber() -> Subscriber {
    Subscriber {
        msisdn: 79161234567,
        imsi: 250011234567890,
        mccmnc: 25001,
        imei: 123456789012345,
    }
}

fn benchmark_full_pipeline_call(c: &mut Criterion) {
    let config = create_test_config();
    let call_gen = CallGenerator::new(&config);
    let sub = create_test_subscriber();

    let tz: Tz = "Europe/Moscow".parse().unwrap();
    let start_time = tz.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

    let mut group = c.benchmark_group("full_pipeline_call");

    for count in [1000, 5000, 10000].iter() {
        group.throughput(Throughput::Elements(*count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                let mut rng = StdRng::seed_from_u64(12345);
                let temp_file = NamedTempFile::new().unwrap();
                let file = temp_file.reopen().unwrap();

                // Create compressed writer
                let compressed = create_compressed_writer(file, CompressionType::Zstd).unwrap();
                let mut csv_writer = WriterBuilder::new()
                    .delimiter(b';')
                    .has_headers(true)
                    .from_writer(compressed);

                // Generate and write events
                let mut event = EventRow::default();
                for _ in 0..count {
                    call_gen.generate(
                        &mut event,
                        &sub,
                        start_time,
                        79169999999,
                        "Europe/Moscow",
                        12345,
                        &mut rng,
                    );
                    csv_writer.serialize(black_box(&event)).unwrap();
                }

                csv_writer.flush().unwrap();
                let mut inner = csv_writer.into_inner().unwrap();
                inner.finish_compression().unwrap();
            });
        });
    }
    group.finish();
}

fn benchmark_full_pipeline_mixed(c: &mut Criterion) {
    let config = create_test_config();
    let call_gen = CallGenerator::new(&config);
    let sms_gen = SmsGenerator::new(&config);
    let data_gen = DataGenerator::new(HashMap::new(), vec![]);
    let sub = create_test_subscriber();

    let tz: Tz = "Europe/Moscow".parse().unwrap();
    let start_time = tz.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

    let mut group = c.benchmark_group("full_pipeline_mixed");

    for count in [1000, 5000, 10000].iter() {
        group.throughput(Throughput::Elements(*count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                let mut rng = StdRng::seed_from_u64(12345);
                let temp_file = NamedTempFile::new().unwrap();
                let file = temp_file.reopen().unwrap();

                let compressed = create_compressed_writer(file, CompressionType::Zstd).unwrap();
                let mut csv_writer = WriterBuilder::new()
                    .delimiter(b';')
                    .has_headers(true)
                    .from_writer(compressed);

                let mut event = EventRow::default();
                let events_per_type = count / 3;

                // CALL events
                for _ in 0..events_per_type {
                    call_gen.generate(&mut event, &sub, start_time, 79169999999, "Europe/Moscow", 12345, &mut rng);
                    csv_writer.serialize(&event).unwrap();
                }

                // SMS events
                for _ in 0..events_per_type {
                    sms_gen.generate(&mut event, &sub, start_time, 79169999999, "Europe/Moscow", 12345, &mut rng);
                    csv_writer.serialize(&event).unwrap();
                }

                // DATA events
                for _ in 0..events_per_type {
                    data_gen.generate(&mut event, &sub, start_time, "Europe/Moscow", &mut rng);
                    csv_writer.serialize(&event).unwrap();
                }

                csv_writer.flush().unwrap();
                let mut inner = csv_writer.into_inner().unwrap();
                inner.finish_compression().unwrap();
            });
        });
    }
    group.finish();
}

fn benchmark_event_pool_performance(c: &mut Criterion) {
    let config = create_test_config();
    let call_gen = CallGenerator::new(&config);
    let sub = create_test_subscriber();

    let tz: Tz = "Europe/Moscow".parse().unwrap();
    let start_time = tz.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

    let mut group = c.benchmark_group("event_pool_performance");

    for pool_size in [100, 500, 1000, 5000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(pool_size), pool_size, |b, &pool_size| {
            b.iter(|| {
                let mut rng = StdRng::seed_from_u64(12345);
                let mut event_pool = EventPool::new(pool_size);

                for _ in 0..10000 {
                    let event = event_pool.acquire();
                    call_gen.generate(
                        black_box(event),
                        &sub,
                        start_time,
                        79169999999,
                        "Europe/Moscow",
                        12345,
                        &mut rng,
                    );
                    // Event is automatically returned to pool when dropped
                }
            });
        });
    }
    group.finish();
}

fn benchmark_compression_comparison(c: &mut Criterion) {
    let config = create_test_config();
    let call_gen = CallGenerator::new(&config);
    let sub = create_test_subscriber();

    let tz: Tz = "Europe/Moscow".parse().unwrap();
    let start_time = tz.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

    let count = 5000;
    let mut group = c.benchmark_group("compression_comparison");
    group.throughput(Throughput::Elements(count as u64));

    for compression_type in [CompressionType::None, CompressionType::Gzip, CompressionType::Zstd].iter() {
        let name = match compression_type {
            CompressionType::None => "none",
            CompressionType::Gzip => "gzip",
            CompressionType::Zstd => "zstd",
        };

        group.bench_with_input(BenchmarkId::from_parameter(name), compression_type, |b, compression_type| {
            b.iter(|| {
                let mut rng = StdRng::seed_from_u64(12345);
                let temp_file = NamedTempFile::new().unwrap();
                let file = temp_file.reopen().unwrap();

                let compressed = create_compressed_writer(file, *compression_type).unwrap();
                let mut csv_writer = WriterBuilder::new()
                    .delimiter(b';')
                    .has_headers(true)
                    .from_writer(compressed);

                let mut event = EventRow::default();
                for _ in 0..count {
                    call_gen.generate(&mut event, &sub, start_time, 79169999999, "Europe/Moscow", 12345, &mut rng);
                    csv_writer.serialize(&event).unwrap();
                }

                csv_writer.flush().unwrap();
                let mut inner = csv_writer.into_inner().unwrap();
                inner.finish_compression().unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_full_pipeline_call,
    benchmark_full_pipeline_mixed,
    benchmark_event_pool_performance,
    benchmark_compression_comparison
);
criterion_main!(benches);
