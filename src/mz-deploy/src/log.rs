// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

/// Global quiet mode flag.
static QUIET: AtomicBool = AtomicBool::new(false);

/// Enable or disable JSON output mode.
pub fn set_json_output(v: bool) {
    JSON_OUTPUT.store(v, Ordering::Relaxed);
}

/// Check if JSON output mode is currently enabled.
pub fn json_output_enabled() -> bool {
    JSON_OUTPUT.load(Ordering::Relaxed)
}

/// Enable or disable quiet mode.
pub fn set_quiet(v: bool) {
    QUIET.store(v, Ordering::Relaxed);
}

/// Check if quiet mode is currently enabled.
pub fn quiet_enabled() -> bool {
    QUIET.load(Ordering::Relaxed)
}

/// Enable or disable verbose logging.
pub fn set_verbose(v: bool) {
    VERBOSE.store(v, Ordering::Relaxed);
}

/// Check if verbose logging is currently enabled.
pub fn verbose_enabled() -> bool {
    VERBOSE.load(Ordering::Relaxed)
}

/// Check if color is enabled on stderr.
///
/// Reads `NO_COLOR` / `FORCE_COLOR` / `CLICOLOR_FORCE` and tty status via
/// the `supports-color` crate. This is the same source `owo-colors` consults
/// internally.
pub fn color_enabled() -> bool {
    supports_color::on_cached(supports_color::Stream::Stderr).is_some()
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

/// A value that can be rendered as both human-readable text and JSON.
///
/// Render is the core pattern for command output in mz-deploy. Instead of
/// branching on `json_output_enabled()` at every call site, commands define a
/// single struct that implements both `Display` (for humans) and `Serialize`
/// (for machines), then hand it to [`output()`]:
///
/// ```ignore
/// #[derive(serde::Serialize)]
/// struct MyResult { name: String, count: usize }
///
/// impl fmt::Display for MyResult {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "  ✓ Processed {} items for '{}'", self.count, self.name)
///     }
/// }
///
/// log::output(&MyResult { name, count });
/// ```
///
/// Guidelines:
/// - **Use `output()` with a Render struct** when a command has a single result
///   that should be available in both text and JSON form. This is the default.
/// - **Use `output_json()`** for paths with no human representation, like NDJSON
///   streaming or machine-only pre-execution plan dumps.
/// - **Use `info!()`** for supplementary stderr messages (hints, progress) that
///   shouldn't appear in JSON output.
pub trait Render: std::fmt::Display + serde::Serialize {}
impl<T: std::fmt::Display + serde::Serialize> Render for T {}

/// Output a value: JSON to stdout when `--output json`, human text to stderr otherwise.
///
/// Silenced by `--quiet`.
#[allow(clippy::print_stdout, clippy::print_stderr)]
pub fn output(value: &impl Render) {
    if quiet_enabled() {
        return;
    }
    if json_output_enabled() {
        println!("{}", serde_json::to_string(value).unwrap());
    } else {
        eprintln!("{value}");
    }
}

/// Print an informational message to stderr. Silenced by `--quiet`.
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        if !$crate::log::quiet_enabled() {
            #[allow(clippy::print_stderr)]
            { eprintln!($($arg)*); }
        }
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

/// Print a successful deployment's ID to stdout.
///
/// This is the one intentional stdout write in human mode. `output()` sends
/// the pretty summary to stderr so terminals still show it, while the deploy
/// ID — the machine-useful handoff to `wait`/`promote` — goes to stdout on
/// its own line. That split is what lets callers compose:
///
/// ```text
/// DEPLOY_ID=$(mz-deploy stage)
/// mz-deploy stage | xargs mz-deploy wait
/// ```
///
/// In JSON mode we skip this, because the structured result already carries
/// `deploy_id` on stdout and a bare ID would corrupt the JSON stream. The
/// `print_stdout` allow is intentional for exactly this reason.
#[allow(clippy::print_stdout)]
pub fn print_deploy_id(deploy_id: &str) {
    if json_output_enabled() {
        return;
    }
    println!("{deploy_id}");
}

/// Print an informational message to stderr without a trailing newline. Silenced by `--quiet`.
#[macro_export]
macro_rules! info_nonl {
    ($($arg:tt)*) => {
        if !$crate::log::quiet_enabled() {
            #[allow(clippy::print_stderr)]
            { eprint!($($arg)*); }
        }
    };
}
