//! Timestamp conversion utilities.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert a Unix epoch (seconds as f64) to SystemTime.
pub fn epoch_to_system_time(epoch: f64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs_f64(epoch)
}

/// Convert an optional Unix epoch to an optional SystemTime.
pub fn epoch_to_system_time_opt(epoch: Option<f64>) -> Option<SystemTime> {
    epoch.map(epoch_to_system_time)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_to_system_time() {
        let epoch = 1700000000.0;
        let time = epoch_to_system_time(epoch);
        let duration = time.duration_since(UNIX_EPOCH).unwrap();
        assert_eq!(duration.as_secs_f64(), epoch);
    }

    #[test]
    fn test_epoch_to_system_time_opt() {
        assert!(epoch_to_system_time_opt(None).is_none());
        assert!(epoch_to_system_time_opt(Some(1700000000.0)).is_some());
    }
}
