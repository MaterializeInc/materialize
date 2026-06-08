// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Progress reporting utilities for user-facing output.
//!
//! This module provides helper functions for displaying progress and status
//! during command execution. It uses colors and symbols to create clear,
//! scannable output similar to tools like dbt.

use crate::info;
use owo_colors::{OwoColorize, Stream, Style};
use std::time::Duration;

/// Print a stage start message with yellow arrow.
pub fn stage_start(name: &str) {
    info!(
        "{} {}...",
        "→".if_supports_color(Stream::Stderr, |t| t.yellow()),
        name
    );
}

/// Print a stage completion message with green checkmark and duration.
pub fn stage_success(message: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    let suffix = format!("({}s)", format_duration(seconds));
    info!(
        "  {} {} {}",
        "✓".if_supports_color(Stream::Stderr, |t| t.green()),
        message,
        suffix.if_supports_color(Stream::Stderr, |t| t.dimmed()),
    );
}

/// Print a success message with green checkmark.
pub fn success(message: &str) {
    info!(
        "  {} {}",
        "✓".if_supports_color(Stream::Stderr, |t| t.green()),
        message
    );
}

/// Print a warning message with yellow exclamation symbol.
pub fn warn(message: &str) {
    info!(
        "  {} {}",
        "⚠".if_supports_color(Stream::Stderr, |t| t.yellow()),
        message
    );
}

/// Print an error message with red X symbol.
pub fn error(message: &str) {
    info!(
        "  {} {}",
        "✗".if_supports_color(Stream::Stderr, |t| t.red()),
        message
    );
}

/// Print a cargo-style action line: a 12-column right-aligned bold-green
/// verb followed by `message`.
pub fn action(verb: &str, message: &str) {
    let label = format!("{:>12}", verb);
    let style = Style::new().bright_green().bold();
    info!(
        "{} {}",
        label.if_supports_color(Stream::Stderr, |t| style.style(t)),
        message
    );
}

/// Print a cargo-style "Finished" line for `action_name` after `duration`.
///
/// # Example
/// ```ignore
/// finished("compile", Duration::from_millis(80));
/// // Output:     Finished compile in 0.08s
/// ```
pub fn finished(action_name: &str, duration: Duration) {
    let seconds = duration.as_secs_f64();
    action(
        "Finished",
        &format!("{} in {}s", action_name, format_duration(seconds)),
    );
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
