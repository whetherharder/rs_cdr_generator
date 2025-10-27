use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rs_cdr_generator::compression::{CompressionType, create_compressed_writer, CompressedWriter};
use std::io::Write;
use tempfile::NamedTempFile;

fn generate_test_data(size: usize) -> Vec<u8> {
    // Generate CSV-like data (realistic CDR records)
    let mut data = Vec::with_capacity(size);
    let sample_line = b"CALL;79161234567;79169876543;MO;1705320000000;1705320180000;Europe/Moscow;180;180;25001;250011234567890;123456789012345;12345;mscVoiceRecord;normalRelease;;;;;\n";

    while data.len() < size {
        data.extend_from_slice(sample_line);
    }
    data.truncate(size);
    data
}

fn benchmark_gzip_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("gzip_compression");

    for size_kb in [100, 1000, 10000].iter() {
        let size = size_kb * 1024;
        let data = generate_test_data(size);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size_kb), &data, |b, data| {
            b.iter(|| {
                let temp_file = NamedTempFile::new().unwrap();
                let file = temp_file.reopen().unwrap();
                let mut writer = create_compressed_writer(file, CompressionType::Gzip).unwrap();
                writer.write_all(black_box(data)).unwrap();
                writer.finish_compression().unwrap();
            });
        });
    }
    group.finish();
}

fn benchmark_zstd_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("zstd_compression");

    for size_kb in [100, 1000, 10000].iter() {
        let size = size_kb * 1024;
        let data = generate_test_data(size);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size_kb), &data, |b, data| {
            b.iter(|| {
                let temp_file = NamedTempFile::new().unwrap();
                let file = temp_file.reopen().unwrap();
                let mut writer = create_compressed_writer(file, CompressionType::Zstd).unwrap();
                writer.write_all(black_box(data)).unwrap();
                writer.finish_compression().unwrap();
            });
        });
    }
    group.finish();
}

fn benchmark_no_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("no_compression");

    for size_kb in [100, 1000, 10000].iter() {
        let size = size_kb * 1024;
        let data = generate_test_data(size);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size_kb), &data, |b, data| {
            b.iter(|| {
                let temp_file = NamedTempFile::new().unwrap();
                let file = temp_file.reopen().unwrap();
                let mut writer = create_compressed_writer(file, CompressionType::None).unwrap();
                writer.write_all(black_box(data)).unwrap();
                writer.finish_compression().unwrap();
            });
        });
    }
    group.finish();
}

fn benchmark_compression_ratio(c: &mut Criterion) {
    let size = 1024 * 1024; // 1MB
    let data = generate_test_data(size);

    c.bench_function("compression_ratio_comparison", |b| {
        b.iter(|| {
            // Gzip
            let temp_gzip = NamedTempFile::new().unwrap();
            let gzip_path = temp_gzip.path().to_path_buf();
            {
                let file = temp_gzip.reopen().unwrap();
                let mut writer = create_compressed_writer(file, CompressionType::Gzip).unwrap();
                writer.write_all(&data).unwrap();
                writer.finish_compression().unwrap();
            }
            let gzip_size = std::fs::metadata(&gzip_path).unwrap().len();

            // Zstd
            let temp_zstd = NamedTempFile::new().unwrap();
            let zstd_path = temp_zstd.path().to_path_buf();
            {
                let file = temp_zstd.reopen().unwrap();
                let mut writer = create_compressed_writer(file, CompressionType::Zstd).unwrap();
                writer.write_all(&data).unwrap();
                writer.finish_compression().unwrap();
            }
            let zstd_size = std::fs::metadata(&zstd_path).unwrap().len();

            black_box((gzip_size, zstd_size))
        });
    });
}

criterion_group!(
    benches,
    benchmark_gzip_compression,
    benchmark_zstd_compression,
    benchmark_no_compression,
    benchmark_compression_ratio
);
criterion_main!(benches);
