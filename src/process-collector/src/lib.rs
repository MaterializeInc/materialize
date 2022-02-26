// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! # Detailed metrics for the current process
//!
//! This crate replaces the [rust-prometheus][] `process` feature, which is
//! designed to exactly match the fields documented as part of the [Prometheus
//! process metrics][prom-process].
//!
//! This currently only works on linux, due to library support.
//!
//! ## Recommended usage
//!
//! Initialize this exactly once at program startup, and then handle scrapes
//! [following the prometheus instructions]:
//!
//! ```
//! # use mz_ore::metrics::MetricsRegistry;
//! # fn handle_scrapes() {}
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let registry = MetricsRegistry::new();
//!     mz_process_collector::register_default_process_collector(&registry);
//!
//!     handle_scrapes();
//!
//!     Ok(())
//! }
//! ```
//!
//! Do *not* use this in combination with the `process` feature in the
//! `rust-prometheus` library, the metrics will clobber each other.
//!
//! ## Extra Metrics
//!
//! In addition to everything provided by rust-prometheus, this library exports:
//!
//! * Per-thread name cpu time: `process_thread_cpu_seconds_total{thread_name}`
//! * Count of threads with the same name: `process_thread_count{thread_name}`
//!
//! [rust-prometheus]: https://github.com/tikv/rust-prometheus/
//! [prom-process]: https://prometheus.io/docs/instrumenting/writing_clientlibs/#process-metrics
//! [following the prometheus instructions]: https://docs.rs/prometheus/0.10.0/prometheus/#basic-example

use mz_ore::metrics::MetricsRegistry;

#[cfg(target_os = "linux")]
mod process_collector;

#[cfg(target_os = "linux")]
pub use process_collector::ProcessCollector;

/// Configure a [`ProcessCollector`] to export prometheus metrics in the [default registry]
///
/// [default registry]: https://docs.rs/prometheus/0.10.0/prometheus/fn.default_registry.html
#[cfg(target_os = "linux")]
pub fn register_default_process_collector(registry: &MetricsRegistry) {
    let pc = ProcessCollector::for_self();
    registry.register_collector(pc);
}

/// Since the target os is not linux, this does nothing
#[cfg(not(target_os = "linux"))]
pub fn register_default_process_collector(_: &MetricsRegistry) {}
