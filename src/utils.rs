// Utility functions for bundling and aggregation
use chrono::DateTime;
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

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

/// Combine all CDR shard files for a day and compress into a single .gz file
pub fn bundle_day(out_dir: &Path, day: &DateTime<Tz>, cleanup: bool) -> anyhow::Result<PathBuf> {
    let day_str = day.format("%Y-%m-%d").to_string();
    let day_dir = out_dir.join(&day_str);

    if !day_dir.exists() {
        anyhow::bail!("Day directory not found: {:?}", day_dir);
    }

    // Collect all compressed CDR shard files (sorted by name for consistent ordering)
    let mut cdr_files: Vec<_> = std::fs::read_dir(&day_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            name_str.starts_with("cdr_") && name_str.ends_with(".csv.gz")
        })
        .collect();

    cdr_files.sort_by_key(|entry| entry.file_name());

    if cdr_files.is_empty() {
        anyhow::bail!("No CDR files found in directory: {:?}", day_dir);
    }

    // Create final combined gzip file path
    let gz_path = out_dir.join(format!("cdr_{}.csv.gz", day_str));

    // Concatenate all compressed shard files
    // This is valid for gzip format - multiple gzip streams can be concatenated
    let mut output = File::create(&gz_path)?;

    for entry in &cdr_files {
        let file_path = entry.path();
        let mut input = File::open(&file_path)?;
        std::io::copy(&mut input, &mut output)?;
    }

    output.flush()?;

    println!("Combined {} shard files into: {:?}", cdr_files.len(), gz_path);

    // Cleanup original shard files if requested
    if cleanup {
        for entry in &cdr_files {
            std::fs::remove_file(entry.path())?;
        }
        println!("Cleaned up {} shard files", cdr_files.len());
    }

    Ok(gz_path)
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

        // Create dummy CDR shard files
        fs::write(
            day_dir.join("cdr_2025-01-01_shard000_part001.csv"),
            "header1;header2\ndata1;data2\n",
        )
        .unwrap();
        fs::write(
            day_dir.join("cdr_2025-01-01_shard001_part001.csv"),
            "header1;header2\ndata3;data4\n",
        )
        .unwrap();

        let gz_path = bundle_day(dir.path(), &day, false).unwrap();
        assert!(gz_path.exists());
        assert!(gz_path.to_string_lossy().ends_with(".csv.gz"));
        // Original shard files should still exist when cleanup=false
        assert!(day_dir.join("cdr_2025-01-01_shard000_part001.csv").exists());
        assert!(day_dir.join("cdr_2025-01-01_shard001_part001.csv").exists());
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

        // Create dummy CDR shard files
        fs::write(
            day_dir.join("cdr_2025-01-01_shard000_part001.csv"),
            "header1;header2\ndata1;data2\n",
        )
        .unwrap();
        fs::write(
            day_dir.join("cdr_2025-01-01_shard001_part001.csv"),
            "header1;header2\ndata3;data4\n",
        )
        .unwrap();

        let gz_path = bundle_day(dir.path(), &day, true).unwrap();
        assert!(gz_path.exists());
        assert!(gz_path.to_string_lossy().ends_with(".csv.gz"));
        // Original shard files should be deleted when cleanup=true
        assert!(!day_dir.join("cdr_2025-01-01_shard000_part001.csv").exists());
        assert!(!day_dir.join("cdr_2025-01-01_shard001_part001.csv").exists());
    }
}
