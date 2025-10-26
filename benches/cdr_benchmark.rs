// Criterion benchmarks for CDR Generator
// Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::SeedableRng;
use rs_cdr_generator::cells::{ensure_cells_catalog, load_cells_catalog};
use rs_cdr_generator::config::{load_config, Config};
use rs_cdr_generator::generators::{
    diurnal_multiplier, lognorm_params_from_quantiles, sample_call_duration, sample_poisson,
};
use rs_cdr_generator::identity::build_subscribers;
use rs_cdr_generator::subscriber_db_generator::{generate_database, GeneratorConfig};
use rs_cdr_generator::timezone_utils::tz_from_name;
use std::path::PathBuf;
use tempfile::TempDir;

/// Benchmark lognormal parameter calculation
fn bench_lognorm_params(c: &mut Criterion) {
    let mut group = c.benchmark_group("lognorm_params");

    for (p50, p90) in [(60.0, 300.0), (30.0, 180.0), (120.0, 600.0)] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("p50={}_p90={}", p50, p90)),
            &(p50, p90),
            |b, &(p50, p90)| {
                b.iter(|| lognorm_params_from_quantiles(black_box(p50), black_box(p90)));
            },
        );
    }
    group.finish();
}

/// Benchmark call duration sampling
fn bench_call_duration(c: &mut Criterion) {
    let mut group = c.benchmark_group("call_duration");
    let mut rng = StdRng::seed_from_u64(42);
    let (mu, sigma) = lognorm_params_from_quantiles(60.0, 300.0);

    group.throughput(Throughput::Elements(1000));
    group.bench_function("sample_1000", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                black_box(sample_call_duration(&mut rng, mu, sigma));
            }
        });
    });
    group.finish();
}

/// Benchmark Poisson sampling for event counts
fn bench_poisson_sampling(c: &mut Criterion) {
    let mut group = c.benchmark_group("poisson_sampling");
    let mut rng = StdRng::seed_from_u64(42);

    for mean in [5.0, 10.0, 20.0, 50.0] {
        group.bench_with_input(BenchmarkId::from_parameter(mean), &mean, |b, &mean| {
            b.iter(|| sample_poisson(black_box(mean), &mut rng));
        });
    }
    group.finish();
}

/// Benchmark diurnal multiplier calculation
fn bench_diurnal_multiplier(c: &mut Criterion) {
    let mut group = c.benchmark_group("diurnal_multiplier");
    let config = Config::default();
    let tz = tz_from_name("UTC");
    let dt = tz.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap();

    group.bench_function("calculate", |b| {
        b.iter(|| {
            diurnal_multiplier(
                black_box(&dt),
                black_box(&config),
                black_box("2025-01-15"),
            )
        });
    });
    group.finish();
}

/// Benchmark subscriber identity building
fn bench_subscriber_building(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriber_building");

    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let subs = build_subscribers(
                    black_box(count),
                    black_box(42),
                    black_box("7"),
                    black_box(&["901", "902", "903"]),
                );
                black_box(subs)
            });
        });
    }
    group.finish();
}

/// Benchmark subscriber database generation
fn bench_subscriber_database_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscriber_database");
    group.sample_size(10); // Reduce sample size for expensive operations

    for count in [1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let gen_config = GeneratorConfig {
                    num_subscribers: count,
                    seed: 42,
                    country_code: "7".to_string(),
                    prefixes: vec!["901".to_string(), "902".to_string(), "903".to_string()],
                    start_date: "2025-01-01".to_string(),
                    end_date: "2025-01-31".to_string(),
                    imei_change_prob: 0.01,
                    sim_change_prob: 0.005,
                };
                let db = generate_database(&gen_config);
                black_box(db)
            });
        });
    }
    group.finish();
}

/// Benchmark cells catalog loading
fn bench_cells_catalog(c: &mut Criterion) {
    let mut group = c.benchmark_group("cells_catalog");

    // First ensure catalog exists
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("cells_catalog.json");
    ensure_cells_catalog(&catalog_path, 42, 1000, 55.7558, 37.6173, 50.0);

    group.bench_function("load_1000_cells", |b| {
        b.iter(|| {
            let cells = load_cells_catalog(black_box(&catalog_path));
            black_box(cells)
        });
    });
    group.finish();
}

/// Benchmark config loading
fn bench_config_loading(c: &mut Criterion) {
    let mut group = c.benchmark_group("config_loading");

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("test_config.yaml");

    // Write a minimal config
    std::fs::write(
        &config_path,
        r#"
country_code: "7"
timezone: "Europe/Moscow"
mo_share_call: 0.55
mo_share_sms: 0.6
"#,
    )
    .unwrap();

    group.bench_function("load_yaml", |b| {
        b.iter(|| {
            let config = load_config(black_box(Some(&config_path))).unwrap();
            black_box(config)
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_lognorm_params,
    bench_call_duration,
    bench_poisson_sampling,
    bench_diurnal_multiplier,
    bench_subscriber_building,
    bench_subscriber_database_generation,
    bench_cells_catalog,
    bench_config_loading,
);

criterion_main!(benches);
