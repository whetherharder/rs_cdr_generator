use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rs_cdr_generator::config::Config;
use rs_cdr_generator::generators::{CallGenerator, SmsGenerator, DataGenerator};
use rs_cdr_generator::writer::EventRow;
use rs_cdr_generator::identity::Subscriber;
use rand::SeedableRng;
use rand::rngs::StdRng;
use chrono_tz::Tz;
use chrono::TimeZone;
use std::collections::HashMap;
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

fn benchmark_call_generation(c: &mut Criterion) {
    let config = create_test_config();
    let call_gen = CallGenerator::new(&config);
    let sub = create_test_subscriber();
    let mut rng = StdRng::seed_from_u64(12345);

    let tz: Tz = "Europe/Moscow".parse().unwrap();
    let start_time = tz.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

    let mut group = c.benchmark_group("call_generation");

    for count in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                let mut event = EventRow::default();
                for _ in 0..count {
                    call_gen.generate(
                        black_box(&mut event),
                        black_box(&sub),
                        black_box(start_time),
                        black_box(79169999999),
                        black_box("Europe/Moscow"),
                        black_box(12345),
                        black_box(&mut rng),
                    );
                }
            });
        });
    }
    group.finish();
}

fn benchmark_sms_generation(c: &mut Criterion) {
    let config = create_test_config();
    let sms_gen = SmsGenerator::new(&config);
    let sub = create_test_subscriber();
    let mut rng = StdRng::seed_from_u64(12345);

    let tz: Tz = "Europe/Moscow".parse().unwrap();
    let start_time = tz.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

    let mut group = c.benchmark_group("sms_generation");

    for count in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                let mut event = EventRow::default();
                for _ in 0..count {
                    sms_gen.generate(
                        black_box(&mut event),
                        black_box(&sub),
                        black_box(start_time),
                        black_box(79169999999),
                        black_box("Europe/Moscow"),
                        black_box(12345),
                        black_box(&mut rng),
                    );
                }
            });
        });
    }
    group.finish();
}

fn benchmark_data_generation(c: &mut Criterion) {
    let data_gen = DataGenerator::new(HashMap::new(), vec![]);
    let sub = create_test_subscriber();
    let mut rng = StdRng::seed_from_u64(12345);

    let tz: Tz = "Europe/Moscow".parse().unwrap();
    let start_time = tz.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

    let mut group = c.benchmark_group("data_generation");

    for count in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            b.iter(|| {
                let mut event = EventRow::default();
                for _ in 0..count {
                    data_gen.generate(
                        black_box(&mut event),
                        black_box(&sub),
                        black_box(start_time),
                        black_box("Europe/Moscow"),
                        black_box(&mut rng),
                    );
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, benchmark_call_generation, benchmark_sms_generation, benchmark_data_generation);
criterion_main!(benches);
