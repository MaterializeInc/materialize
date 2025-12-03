//! Progress reporting utilities for user-facing output.
//!
//! This module provides helper functions for displaying progress and status
//! during command execution. It uses colors and symbols to create clear,
//! scannable output similar to tools like dbt.

use owo_colors::OwoColorize;
use std::time::Duration;

/// Print a stage start message with yellow arrow.
///
/// # Example
/// ```ignore
/// stage_start("Parsing SQL files");
/// // Output: → Parsing SQL files...
/// ```
pub fn stage_start(name: &str) {
    println!("\n{} {}...", "→".yellow(), name);
}

/// Print a stage completion message with green checkmark and duration.
///
/// # Example
/// ```ignore
/// stage_success("15 objects parsed", Duration::from_millis(100));
/// // Output:   ✓ 15 objects parsed (0.1s)
/// ```
pub fn stage_success(message: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    println!("  {} {} {}", "✓".green(), message, format!("({}s)", format_duration(seconds)).dimmed());
}

/// Print an informational message with blue info symbol.
///
/// # Example
/// ```ignore
/// info("3 external dependencies detected");
/// // Output:   ℹ 3 external dependencies detected
/// ```
pub fn info(message: &str) {
    println!("  {} {}", "ℹ".blue(), message);
}

/// Print a success message with green checkmark.
///
/// # Example
/// ```ignore
/// success("All objects validated");
/// // Output:   ✓ All objects validated
/// ```
pub fn success(message: &str) {
    println!("  {} {}", "✓".green(), message);
}

/// Print an error message with red X symbol.
///
/// # Example
/// ```ignore
/// error("Type checking failed");
/// // Output:   ✗ Type checking failed
/// ```
pub fn error(message: &str) {
    println!("  {} {}", "✗".red(), message);
}

/// Print a final summary message with green checkmark and total duration.
///
/// # Example
/// ```ignore
/// summary("Project successfully compiled", Duration::from_secs(3));
/// // Output: ✓ Project successfully compiled in 3.0s
/// ```
pub fn summary(message: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    println!("\n{} {} {}", "✓".green().bold(), message, format!("in {}s", format_duration(seconds)).dimmed());
}

/// Format duration to show appropriate precision.
/// - < 1s: show 2 decimal places
/// - >= 1s: show 1 decimal place
fn format_duration(seconds: f64) -> String {
    if seconds < 1.0 {
        format!("{:.2}", seconds)
    } else {
        format!("{:.1}", seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0.05), "0.05");
        assert_eq!(format_duration(0.123), "0.12");
        assert_eq!(format_duration(1.0), "1.0");
        assert_eq!(format_duration(2.567), "2.6");
        assert_eq!(format_duration(10.12), "10.1");
    }
}
