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
macro_rules! verbose {
    ($($arg:tt)*) => {
        if $crate::utils::log::verbose_enabled() {
            println!($($arg)*);
        }
    };
}
