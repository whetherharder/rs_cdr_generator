// Timezone handling utilities
use chrono::{DateTime, Offset, TimeZone};
use chrono_tz::Tz;

/// Get timezone from name
/// Falls back to UTC if timezone is unknown
pub fn tz_from_name(tz_name: &str) -> Tz {
    tz_name.parse().unwrap_or(chrono_tz::Europe::Amsterdam)
}

/// Convert datetime to milliseconds since Unix epoch
pub fn to_epoch_ms<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.timestamp_millis()
}

/// Get timezone offset in minutes from datetime
pub fn tz_offset_minutes<T: TimeZone>(dt: &DateTime<T>) -> i32 {
    dt.offset().fix().local_minus_utc() / 60
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_tz_from_name() {
        let tz = tz_from_name("Europe/Amsterdam");
        assert_eq!(tz.name(), "Europe/Amsterdam");

        let tz_utc = tz_from_name("Invalid/Timezone");
        // Should fallback to Amsterdam
        assert_eq!(tz_utc.name(), "Europe/Amsterdam");
    }

    #[test]
    fn test_to_epoch_ms() {
        let dt = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let ms = to_epoch_ms(&dt);
        assert_eq!(ms, 1735689600000);
    }

    #[test]
    fn test_tz_offset_minutes() {
        let tz = tz_from_name("Europe/Amsterdam");
        let dt = tz.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap();
        let offset = tz_offset_minutes(&dt);
        assert_eq!(offset, 60); // CET is UTC+1

        let dt_summer = tz.with_ymd_and_hms(2025, 7, 1, 12, 0, 0).unwrap();
        let offset_summer = tz_offset_minutes(&dt_summer);
        assert_eq!(offset_summer, 120); // CEST is UTC+2
    }
}
