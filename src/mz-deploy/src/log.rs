//! Logging utilities for mz-deploy.
//!
//! This module provides a simple verbose logging system that can be enabled
//! via the `--verbose` CLI flag. When verbose mode is enabled, diagnostic
//! messages are printed to stdout to help users understand what the tool
//! is doing.

use std::sync::atomic::{AtomicBool, Ordering};

/// Global verbose mode flag.
///
/// This is a thread-safe atomic boolean that stores whether verbose
/// logging is enabled. It uses relaxed memory ordering since the exact
/// timing of when verbose mode is enabled/disabled doesn't matter.
static VERBOSE: AtomicBool = AtomicBool::new(false);

/// Global JSON output flag.
static JSON_OUTPUT: AtomicBool = AtomicBool::new(false);

/// Enable or disable JSON output mode.
pub fn set_json_output(v: bool) {
    JSON_OUTPUT.store(v, Ordering::Relaxed);
}

/// Check if JSON output mode is currently enabled.
pub fn json_output_enabled() -> bool {
    JSON_OUTPUT.load(Ordering::Relaxed)
}

/// Enable or disable verbose logging.
pub fn set_verbose(v: bool) {
    VERBOSE.store(v, Ordering::Relaxed);
}

/// Check if verbose logging is currently enabled.
pub fn verbose_enabled() -> bool {
    VERBOSE.load(Ordering::Relaxed)
}

/// Print a message only when verbose mode is enabled.
#[macro_export]
#[allow(clippy::print_stderr)]
macro_rules! verbose {
    ($($arg:tt)*) => {
        if $crate::log::verbose_enabled() {
            eprintln!($($arg)*);
        }
    };
}

/// A value that can be rendered as human-readable text or JSON.
pub trait Render: std::fmt::Display + serde::Serialize {}
impl<T: std::fmt::Display + serde::Serialize> Render for T {}

/// Output a value: JSON to stdout when `--output json`, human text to stderr otherwise.
#[allow(clippy::print_stdout, clippy::print_stderr)]
pub fn output(value: &impl Render) {
    if json_output_enabled() {
        println!("{}", serde_json::to_string(value).unwrap());
    } else {
        eprintln!("{value}");
    }
}

/// Print an informational message unconditionally to stderr.
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        #[allow(clippy::print_stderr)]
        { eprintln!($($arg)*); }
    };
}

/// Write a JSON-only value to stdout.
///
/// Use this for paths that have no human-readable representation (NDJSON streaming,
/// machine-only dry-run plans). Prefer `output()` with a Render type when both
/// human and JSON representations exist.
#[allow(clippy::print_stdout)]
pub fn output_json(value: &impl serde::Serialize) {
    println!("{}", serde_json::to_string(value).unwrap());
}

/// Print an informational message to stderr without a trailing newline.
#[macro_export]
macro_rules! info_nonl {
    ($($arg:tt)*) => {
        #[allow(clippy::print_stderr)]
        { eprint!($($arg)*); }
    };
}
