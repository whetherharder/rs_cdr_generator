// Compression abstraction for pluggable compression algorithms
use std::io::{self, Write};
use std::fs::File;
use std::io::BufWriter;
use flate2::write::GzEncoder;
use flate2::Compression as GzCompression;
use zstd::stream::write::Encoder as ZstdEncoder;

/// Compression type enum for configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Gzip,
    Zstd,
    None,
}

impl CompressionType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "gzip" | "gz" => Some(CompressionType::Gzip),
            "zstd" | "zst" => Some(CompressionType::Zstd),
            "none" | "uncompressed" => Some(CompressionType::None),
            _ => None,
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            CompressionType::Gzip => ".gz",
            CompressionType::Zstd => ".zst",
            CompressionType::None => "",
        }
    }
}

/// Trait for compressed writers that can be used with CSV writer
/// All implementations must support Write + Send
pub trait CompressedWriter: Write + Send {
    /// Finish compression and flush all data
    fn finish_compression(&mut self) -> io::Result<()>;
}

/// Gzip compression writer (single-threaded, compatible with existing code)
pub struct GzipWriter {
    encoder: GzEncoder<BufWriter<File>>,
}

impl GzipWriter {
    pub fn new(file: File, buffer_size: usize) -> io::Result<Self> {
        let buffered = BufWriter::with_capacity(buffer_size, file);
        let encoder = GzEncoder::new(buffered, GzCompression::default());
        Ok(GzipWriter { encoder })
    }
}

impl Write for GzipWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.encoder.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }
}

impl CompressedWriter for GzipWriter {
    fn finish_compression(&mut self) -> io::Result<()> {
        self.encoder.flush()?;
        self.encoder.try_finish()?;
        Ok(())
    }
}

/// Zstd compression writer with multi-threaded support
pub struct ZstdWriter {
    encoder: ZstdEncoder<'static, BufWriter<File>>,
}

impl ZstdWriter {
    /// Create a new Zstd writer with multi-threaded compression
    ///
    /// # Arguments
    /// * `file` - The output file
    /// * `buffer_size` - Buffer size for writes (recommended: 1MB+)
    /// * `compression_level` - Compression level (1-22, default: 3 for fast mode)
    /// * `num_threads` - Number of threads for compression (0 = auto-detect)
    pub fn new(file: File, buffer_size: usize, compression_level: i32, num_threads: u32) -> io::Result<Self> {
        let buffered = BufWriter::with_capacity(buffer_size, file);

        // Create Zstd encoder with specified compression level
        let mut encoder = ZstdEncoder::new(buffered, compression_level)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Enable multi-threaded compression
        if num_threads > 0 {
            encoder.multithread(num_threads)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        // Set long-distance matching for better compression on large files
        encoder.long_distance_matching(true)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(ZstdWriter { encoder })
    }

    /// Create with automatic settings (level 3, auto threads)
    pub fn new_auto(file: File) -> io::Result<Self> {
        let num_threads = num_cpus::get() as u32;
        // Use level 3 which is roughly equivalent to gzip default in speed
        // Buffer size: 1MB for efficient multi-threaded compression
        Self::new(file, 1024 * 1024, 3, num_threads)
    }
}

impl Write for ZstdWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.encoder.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }
}

impl CompressedWriter for ZstdWriter {
    fn finish_compression(&mut self) -> io::Result<()> {
        self.encoder.flush()?;
        self.encoder.do_finish()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }
}

/// Uncompressed writer (pass-through)
pub struct UncompressedWriter {
    writer: BufWriter<File>,
}

impl UncompressedWriter {
    pub fn new(file: File, buffer_size: usize) -> io::Result<Self> {
        let writer = BufWriter::with_capacity(buffer_size, file);
        Ok(UncompressedWriter { writer })
    }
}

impl Write for UncompressedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl CompressedWriter for UncompressedWriter {
    fn finish_compression(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

/// Factory function to create the appropriate compressed writer
pub fn create_compressed_writer(
    file: File,
    compression_type: CompressionType,
) -> io::Result<Box<dyn CompressedWriter>> {
    match compression_type {
        CompressionType::Gzip => {
            let writer = GzipWriter::new(file, 256 * 1024)?;
            Ok(Box::new(writer))
        }
        CompressionType::Zstd => {
            let writer = ZstdWriter::new_auto(file)?;
            Ok(Box::new(writer))
        }
        CompressionType::None => {
            let writer = UncompressedWriter::new(file, 256 * 1024)?;
            Ok(Box::new(writer))
        }
    }
}
