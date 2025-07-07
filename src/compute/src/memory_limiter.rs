// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities to operate lgalloc.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use mz_compute_types::dyncfgs::{
    LGALLOC_LIMITER_BURST_FACTOR, LGALLOC_LIMITER_INTERVAL, LGALLOC_LIMITER_USAGE_BIAS,
    LGALLOC_LIMITER_USAGE_FACTOR,
};
use mz_dyncfg::ConfigSet;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
use prometheus::Histogram;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, warn};

/// A handle to the active lgalloc limiter.
///
/// The limiter is initialized by a call to [`start_limiter`]. It spawns a process-global task that
/// runs for the lifetime of the process.
static LIMITER: Mutex<Option<Limiter>> = Mutex::new(None);

/// Start the process-global lgalloc limiter.
///
/// # Panics
///
/// Panics if the limiter was already started previously.
pub fn start_limiter(memory_limit: usize, metrics_registry: &MetricsRegistry) {
    let mut limiter = LIMITER.lock().expect("poisoned");

    if limiter.is_some() {
        panic!("lgalloc limiter is already running");
    }

    let metrics = LimiterMetrics::new(metrics_registry);
    let (config_tx, config_rx) = mpsc::unbounded_channel();

    mz_ore::task::spawn(|| "lgalloc-limiter", LimiterTask::run(config_rx, metrics));

    *limiter = Some(Limiter {
        memory_limit,
        config_tx,
    });
}

/// Apply the given configuration to the active limiter.
pub fn apply_limiter_config(config: &ConfigSet) {
    if let Some(limiter) = LIMITER.lock().expect("poisoned").as_mut() {
        limiter.apply_config(config);
    }
}

/// A handle to a running lgalloc limiter task.
struct Limiter {
    /// The process memory limit.
    memory_limit: usize,
    /// A sender for limiter configuration updates.
    config_tx: UnboundedSender<LimiterConfig>,
}

impl Limiter {
    /// Apply the given configuration to the limiter.
    fn apply_config(&self, config: &ConfigSet) {
        let mut interval = LGALLOC_LIMITER_INTERVAL.get(config);
        // A zero duration means the limiter is disabled. Translate that into an ~infinite duration
        // so the limiter doesn't have to worry about the special case.
        if interval.is_zero() {
            interval = Duration::MAX;
        }

        let disk_limit = f64::cast_lossy(self.memory_limit)
            * LGALLOC_LIMITER_USAGE_FACTOR.get(config)
            * LGALLOC_LIMITER_USAGE_BIAS.get(config);
        let disk_limit = usize::cast_lossy(disk_limit);

        let burst_budget = f64::cast_lossy(disk_limit) * LGALLOC_LIMITER_BURST_FACTOR.get(config);
        let burst_budget = usize::cast_lossy(burst_budget);

        self.config_tx
            .send(LimiterConfig {
                interval,
                disk_limit,
                burst_budget,
            })
            .expect("limiter task never shuts down");
    }
}

/// Configuration for an lgalloc limiter task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LimiterConfig {
    /// The interval at which disk usage is checked against the disk limit.
    interval: Duration,
    /// The lgalloc disk limit.
    disk_limit: usize,
    /// Budget to allow disk usage above the disk limit, in byte-seconds.
    burst_budget: usize,
}

impl LimiterConfig {
    /// Return a config that disables the disk limiter.
    fn disabled() -> Self {
        Self {
            interval: Duration::MAX,
            disk_limit: 0,
            burst_budget: 0,
        }
    }
}

/// A task that enforces configured lgalloc disk limits.
///
/// The task operates by performing limit checks at a configured interval. For each check it
/// obtains the current disk utilization from lgalloc file stats, summed across all files and size
/// classes. It then compares the disk utilization against the configured disk limit, and if it
/// exceeds the limit, reduces the burst budget by the amount of disk utilization that exceeds the
/// limit. If the burst budget is exhausted, the limiter terminates the process.
struct LimiterTask {
    /// The current limiter configuration.
    config: LimiterConfig,
    /// The amount of burst budget remaining.
    burst_budget_remaining: usize,
    /// The time of the last check.
    last_check: Instant,
    /// Metrics tracked by the limiter task.
    metrics: LimiterMetrics,
}

impl LimiterTask {
    async fn run(mut config_rx: UnboundedReceiver<LimiterConfig>, metrics: LimiterMetrics) {
        info!("running lgalloc limiter task");

        let mut task = Self {
            config: LimiterConfig::disabled(),
            burst_budget_remaining: 0,
            last_check: Instant::now(),
            metrics,
        };

        loop {
            tokio::select! {
                _ = task.tick() => {
                    let start = Instant::now();

                    if let Err(err) = task.check() {
                        error!("lgalloc limit check failed: {err}");
                    }

                    let elapsed = start.elapsed();
                    task.metrics.duration.observe(elapsed.as_secs_f64());
                }
                Some(config) = config_rx.recv() => task.apply_config(config),
            }
        }
    }

    /// Wait until the next check time.
    async fn tick(&self) {
        let elapsed = self.last_check.elapsed();
        let duration = self.config.interval.saturating_sub(elapsed);
        tokio::time::sleep(duration).await
    }

    /// Perform a disk usage check, terminating the process if the configured limits are exceeded.
    fn check(&mut self) -> Result<(), anyhow::Error> {
        debug!("checking lgalloc limits");

        let mut disk_usage = 0;
        let file_stats = lgalloc::lgalloc_stats().file;
        for (_size_class, stat) in file_stats {
            disk_usage += stat?.allocated_size;
        }

        let disk_limit = self.config.disk_limit;
        let burst_budget_remaining = self.burst_budget_remaining;

        debug!(disk_usage, disk_limit, burst_budget_remaining);

        self.metrics.disk_usage.set(u64::cast_from(disk_usage));
        self.metrics
            .burst_budget
            .set(u64::cast_from(burst_budget_remaining));

        if disk_usage > disk_limit {
            // Calculate excess usage in byte-seconds.
            let elapsed = self.last_check.elapsed().as_secs_f64();
            let excess = disk_usage - disk_limit;
            let excess_bs = usize::cast_lossy(f64::cast_lossy(excess) * elapsed);

            if burst_budget_remaining >= excess_bs {
                self.burst_budget_remaining -= excess_bs;
            } else {
                // Burst budget exhausted, terminate the process.
                warn!(
                    disk_usage,
                    disk_limit, "lgalloc disk utilization exceeded configured limits",
                );
                // We terminate with a recognizable exit code so the orchestrator knows the
                // termination was caused by exceeding disk limits, as opposed to another,
                // unexpected cause.
                mz_ore::process::exit_thread_safe(167);
            }
        } else {
            // Reset burst budget if under limit.
            self.burst_budget_remaining = self.config.burst_budget;
        }

        self.last_check = Instant::now();
        Ok(())
    }

    /// Apply a new limiter config.
    fn apply_config(&mut self, config: LimiterConfig) {
        if config == self.config {
            return; // no-op config change
        }

        info!(?config, "applying lgalloc limiter config");
        self.config = config;
        self.burst_budget_remaining = config.burst_budget;

        self.metrics
            .disk_limit
            .set(u64::cast_from(config.disk_limit));
    }
}

struct LimiterMetrics {
    duration: Histogram,
    disk_limit: UIntGauge,
    disk_usage: UIntGauge,
    burst_budget: UIntGauge,
}

impl LimiterMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        Self {
            duration: registry.register(metric!(
                name: "mz_lgalloc_limiter_duration_seconds",
                help: "A histogram of the time it took to run the lgalloc limiter.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_500, 32.),
            )),
            disk_limit: registry.register(metric!(
                name: "mz_lgalloc_limiter_disk_limit_bytes",
                help: "The configured lgalloc disk limit.",
            )),
            disk_usage: registry.register(metric!(
                name: "mz_lgalloc_limiter_disk_usage_bytes",
                help: "The current lgalloc disk usage.",
            )),
            burst_budget: registry.register(metric!(
                name: "mz_lgalloc_limiter_burst_budget_byteseconds",
                help: "The remaining lgalloc burst budget.",
            )),
        }
    }
}
