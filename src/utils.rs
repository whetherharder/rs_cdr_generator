// Utility functions for bundling and aggregation
use chrono::DateTime;
use chrono_tz::Tz;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};
use tar::Builder;

#[derive(Debug, Serialize, Deserialize)]
pub struct DailySummary {
    pub total_calls: usize,
    pub total_sms: usize,
    pub total_data: usize,
    pub shards: usize,
}

/// Aggregate statistics from all shards into a summary.json file
pub fn create_daily_summary(out_dir: &Path, day: &DateTime<Tz>) -> anyhow::Result<DailySummary> {
    let day_str = day.format("%Y-%m-%d").to_string();
    let day_dir = out_dir.join(&day_str);

    let mut summary = DailySummary {
        total_calls: 0,
        total_sms: 0,
        total_data: 0,
        shards: 0,
    };

    // Find all stats files
    let stats_files: Vec<_> = std::fs::read_dir(&day_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("stats_shard")
                && entry.file_name().to_string_lossy().ends_with(".json")
        })
        .collect();

    summary.shards = stats_files.len();

    for entry in stats_files {
        let contents = std::fs::read_to_string(entry.path())?;
        let shard_stats: serde_json::Value = serde_json::from_str(&contents)?;

        if let Some(calls) = shard_stats.get("calls").and_then(|v| v.as_u64()) {
            summary.total_calls += calls as usize;
        }
        if let Some(sms) = shard_stats.get("sms").and_then(|v| v.as_u64()) {
            summary.total_sms += sms as usize;
        }
        if let Some(data) = shard_stats.get("data").and_then(|v| v.as_u64()) {
            summary.total_data += data as usize;
        }
    }

    // Write summary
    let summary_path = day_dir.join("summary.json");
    let summary_json = serde_json::to_string_pretty(&summary)?;
    std::fs::write(summary_path, summary_json)?;

    Ok(summary)
}

/// Create a TAR.GZ archive for a day's worth of CDR data
pub fn bundle_day(out_dir: &Path, day: &DateTime<Tz>, cleanup: bool) -> anyhow::Result<PathBuf> {
    let day_str = day.format("%Y-%m-%d").to_string();
    let day_dir = out_dir.join(&day_str);

    if !day_dir.exists() {
        anyhow::bail!("Day directory not found: {:?}", day_dir);
    }

    let archive_path = out_dir.join(format!("{}.tar.gz", day_str));

    // Create tar.gz archive
    let tar_gz = File::create(&archive_path)?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = Builder::new(enc);

    tar.append_dir_all(&day_str, &day_dir)?;
    tar.finish()?;

    println!("Created archive: {:?}", archive_path);

    // Cleanup original files if requested
    if cleanup {
        std::fs::remove_dir_all(&day_dir)?;
        println!("Cleaned up directory: {:?}", day_dir);
    }

    Ok(archive_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_daily_summary() {
        let dir = tempdir().unwrap();
        let day = chrono_tz::Europe::Amsterdam
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap();
        let day_str = day.format("%Y-%m-%d").to_string();
        let day_dir = dir.path().join(&day_str);
        fs::create_dir_all(&day_dir).unwrap();

        // Create fake stats files
        let stats1 = r#"{"shard": 0, "calls": 100, "sms": 200, "data": 300}"#;
        fs::write(day_dir.join("stats_shard000.json"), stats1).unwrap();

        let stats2 = r#"{"shard": 1, "calls": 150, "sms": 250, "data": 350}"#;
        fs::write(day_dir.join("stats_shard001.json"), stats2).unwrap();

        let summary = create_daily_summary(dir.path(), &day).unwrap();
        assert_eq!(summary.total_calls, 250);
        assert_eq!(summary.total_sms, 450);
        assert_eq!(summary.total_data, 650);
        assert_eq!(summary.shards, 2);
    }

    #[test]
    fn test_bundle_day() {
        let dir = tempdir().unwrap();
        let day = chrono_tz::Europe::Amsterdam
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap();
        let day_str = day.format("%Y-%m-%d").to_string();
        let day_dir = dir.path().join(&day_str);
        fs::create_dir_all(&day_dir).unwrap();

        // Create a dummy file
        fs::write(day_dir.join("test.txt"), "test content").unwrap();

        let archive_path = bundle_day(dir.path(), &day, false).unwrap();
        assert!(archive_path.exists());
        assert!(archive_path.to_string_lossy().ends_with(".tar.gz"));
        assert!(day_dir.exists()); // Should still exist when cleanup=false
    }

    #[test]
    fn test_bundle_day_with_cleanup() {
        let dir = tempdir().unwrap();
        let day = chrono_tz::Europe::Amsterdam
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap();
        let day_str = day.format("%Y-%m-%d").to_string();
        let day_dir = dir.path().join(&day_str);
        fs::create_dir_all(&day_dir).unwrap();

        // Create a dummy file
        fs::write(day_dir.join("test.txt"), "test content").unwrap();

        let archive_path = bundle_day(dir.path(), &day, true).unwrap();
        assert!(archive_path.exists());
        assert!(!day_dir.exists()); // Should be deleted when cleanup=true
    }
}
