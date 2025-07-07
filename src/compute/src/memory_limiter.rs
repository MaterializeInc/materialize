// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities to limit memory usage.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use mz_compute_types::dyncfgs::{
    MEMORY_LIMITER_BURST_FACTOR, MEMORY_LIMITER_INTERVAL, MEMORY_LIMITER_USAGE_BIAS,
    MEMORY_LIMITER_USAGE_FACTOR,
};
use mz_dyncfg::ConfigSet;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
use prometheus::Histogram;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, warn};

/// A handle to the active memory limiter.
///
/// The limiter is initialized by a call to [`start_limiter`]. It spawns a process-global task that
/// runs for the lifetime of the process.
static LIMITER: Mutex<Option<Limiter>> = Mutex::new(None);

/// Start the process-global memory limiter.
///
/// # Panics
///
/// Panics if the limiter was already started previously.
pub fn start_limiter(memory_limit: usize, metrics_registry: &MetricsRegistry) {
    let mut limiter = LIMITER.lock().expect("poisoned");

    if limiter.is_some() {
        panic!("memory limiter is already running");
    }

    let metrics = LimiterMetrics::new(metrics_registry);
    let (config_tx, config_rx) = mpsc::unbounded_channel();

    mz_ore::task::spawn(|| "memory-limiter", LimiterTask::run(config_rx, metrics));

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

/// A handle to a running memory limiter task.
struct Limiter {
    /// The process memory limit.
    memory_limit: usize,
    /// A sender for limiter configuration updates.
    config_tx: UnboundedSender<LimiterConfig>,
}

impl Limiter {
    /// Apply the given configuration to the limiter.
    fn apply_config(&self, config: &ConfigSet) {
        let mut interval = MEMORY_LIMITER_INTERVAL.get(config);
        // A zero duration means the limiter is disabled. Translate that into an ~infinite duration
        // so the limiter doesn't have to worry about the special case.
        if interval.is_zero() {
            interval = Duration::MAX;
        }

        let memory_limit = f64::cast_lossy(self.memory_limit)
            * MEMORY_LIMITER_USAGE_FACTOR.get(config)
            * MEMORY_LIMITER_USAGE_BIAS.get(config);
        let memory_limit = usize::cast_lossy(memory_limit);

        let burst_budget = f64::cast_lossy(memory_limit) * MEMORY_LIMITER_BURST_FACTOR.get(config);
        let burst_budget = usize::cast_lossy(burst_budget);

        self.config_tx
            .send(LimiterConfig {
                interval,
                memory_limit,
                burst_budget,
            })
            .expect("limiter task never shuts down");
    }
}

/// Configuration for an memory limiter task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LimiterConfig {
    /// The interval at which memory usage is checked against the memory limit.
    interval: Duration,
    /// The memory limit.
    memory_limit: usize,
    /// Budget to allow memory usage above the memory limit, in byte-seconds.
    burst_budget: usize,
}

impl LimiterConfig {
    /// Return a config that disables the memory limiter.
    fn disabled() -> Self {
        Self {
            interval: Duration::MAX,
            memory_limit: 0,
            burst_budget: 0,
        }
    }
}

/// A task that enforces configured memory limits.
///
/// The task operates by performing limit checks at a configured interval. For each check it
/// obtains the current memory utilization from proc stats. It then compares the utilization against
/// the configured memory limit, and if it exceeds the limit, reduces the burst budget by the amount
/// of memory utilization that exceeds the limit. If the burst budget is exhausted, the limiter
/// terminates the process.
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
        info!("running memory limiter task");

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
                        error!("memory limit check failed: {err}");
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

    fn current_utilization() -> std::io::Result<ProcStatus> {
        match ProcStatus::from_proc() {
            Ok(status) => Ok(status),
            #[cfg(target_os = "linux")]
            Err(err) => {
                error!("failed to read /proc/self/status: {err}");
                Err(err)
            }
            #[cfg(not(target_os = "linux"))]
            Err(_err) => ProcStatus {
                vm_rss: 0,
                vm_swap: 0,
            },
        }
    }

    /// Perform a memory usage check, terminating the process if the configured limits are exceeded.
    fn check(&mut self) -> Result<(), anyhow::Error> {
        debug!("checking limiter limits");

        let ProcStatus { vm_rss, vm_swap } = Self::current_utilization()?;

        let memory_limit = self.config.memory_limit;
        let burst_budget_remaining = self.burst_budget_remaining;

        let memory_usage = vm_rss + vm_swap;

        debug!(
            memory_usage,
            memory_limit, burst_budget_remaining, vm_rss, vm_swap, "memory utilization check",
        );

        self.metrics.vm_rss.set(u64::cast_from(vm_rss));
        self.metrics.vm_swap.set(u64::cast_from(vm_swap));
        self.metrics.memory_usage.set(u64::cast_from(memory_usage));
        self.metrics
            .burst_budget
            .set(u64::cast_from(burst_budget_remaining));

        if memory_usage > memory_limit {
            // Calculate excess usage in byte-seconds.
            let elapsed = self.last_check.elapsed().as_secs_f64();
            let excess = memory_usage - memory_limit;
            let excess_bs = usize::cast_lossy(f64::cast_lossy(excess) * elapsed);

            if burst_budget_remaining >= excess_bs {
                self.burst_budget_remaining -= excess_bs;
            } else {
                // Burst budget exhausted, terminate the process.
                warn!(
                    memory_usage,
                    memory_limit, "memory utilization exceeded configured limits",
                );
                // We terminate with a recognizable exit code so the orchestrator knows the
                // termination was caused by exceeding memory limits, as opposed to another,
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

        info!(?config, "applying memory limiter config");
        self.config = config;
        self.burst_budget_remaining = config.burst_budget;

        self.metrics
            .memory_limit
            .set(u64::cast_from(config.memory_limit));
    }
}

struct LimiterMetrics {
    duration: Histogram,
    memory_limit: UIntGauge,
    memory_usage: UIntGauge,
    vm_rss: UIntGauge,
    vm_swap: UIntGauge,
    burst_budget: UIntGauge,
}

impl LimiterMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        Self {
            duration: registry.register(metric!(
                name: "mz_memory_limiter_duration_seconds",
                help: "A histogram of the time it took to run the memory limiter.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_500, 32.),
            )),
            memory_limit: registry.register(metric!(
                name: "mz_memory_limiter_memory_limit_bytes",
                help: "The configured memory limit.",
            )),
            memory_usage: registry.register(metric!(
                name: "mz_memory_limiter_memory_usage_bytes",
                help: "The current memory usage.",
            )),
            vm_rss: registry.register(metric!(
                name: "mz_memory_limiter_vm_rss_bytes",
                help: "The current VmRSS metric.",
            )),
            vm_swap: registry.register(metric!(
                name: "mz_memory_limiter_vm_swap_bytes",
                help: "The current VmSwap metric.",
            )),
            burst_budget: registry.register(metric!(
                name: "mz_memory_limiter_burst_budget_byteseconds",
                help: "The remaining memory burst budget.",
            )),
        }
    }
}

struct ProcStatus {
    /// Resident Set Size (RSS) in bytes.
    vm_rss: usize,
    /// Swap memory in bytes.
    vm_swap: usize,
}

impl ProcStatus {
    fn from_proc() -> std::io::Result<Self> {
        let contents = std::fs::read_to_string("/proc/self/status")?;
        let mut vm_rss = 0;
        let mut vm_swap = 0;

        for line in contents.lines() {
            if line.starts_with("VmRSS:") {
                vm_rss = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)
                    * 1024;
            } else if line.starts_with("VmSwap:") {
                vm_swap = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)
                    * 1024;
            }
        }

        Ok(Self { vm_rss, vm_swap })
    }
}
