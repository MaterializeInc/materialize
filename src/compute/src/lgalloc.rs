// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities to operate lgalloc.

use std::time::{Duration, Instant};

use mz_compute_types::dyncfgs::{
    LGALLOC_LIMITER_BURST_FACTOR, LGALLOC_LIMITER_INTERVAL, LGALLOC_LIMITER_USAGE_BIAS,
    LGALLOC_LIMITER_USAGE_FACTOR,
};
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metrics::MetricsRegistry;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Interval;

static LIMITER: std::sync::Mutex<Option<Limiter>> = std::sync::Mutex::new(None);

/// Update the configuration of lgalloc.
pub fn update_dyncfg(config_updates: &ConfigSet) {
    if let Some(limiter) = LIMITER.lock().expect("lock poisoned").as_mut() {
        limiter.apply_dyncfg_updates(config_updates);
    }
}

/// Register all metrics into the provided registry.
///
/// We do not recommend calling this function multiple times. It is safe to call this function,
/// but it might delete previous metrics. If we ever want to change this, we should
/// remove the shared static mutex and make this function return a handle to the metrics.
///
/// This function is async, because it needs to be called from a tokio runtime context.
#[allow(clippy::unused_async)]
pub async fn register_metrics_into(
    metrics_registry: &MetricsRegistry,
    announce_memory_limit: Option<usize>,
) {
    let limiter = Limiter::create_task(
        announce_memory_limit,
        LGALLOC_LIMITER_INTERVAL.clone(),
        &metrics_registry.register(mz_ore::metric!(
            name: "mz_lgalloc_limiter_update_duration",
            help: "The time it took to update lgalloc limiter stats",
            var_labels: ["name"],
            buckets: mz_ore::stats::histogram_seconds_buckets(0.000_500, 32.),
        )),
    );

    *LIMITER.lock().expect("lock poisoned") = Some(Limiter { task: limiter });
}

struct Limiter {
    task: LimiterHandle,
}

struct LimiterHandle {
    tx: UnboundedSender<Update>,
    interval_config: Config<Duration>,
    announce_memory_limit: Option<usize>,
}

struct LimiterTask {
    disk_limit: Option<usize>,
    /// Budget to allow more disk access, in byte-seconds.
    burst_budget: usize,
    burst_budget_remaining: usize,
    last_update: Instant,
}

impl LimiterTask {
    fn update(&mut self) -> Result<(), anyhow::Error> {
        // Get lgalloc stats and obtain the disk utilization from file stats, summed across all
        // files and size classes. Compare the disk utilization against the configured disk limit,
        // and if it exceeds the limit, reduce the burst budget by the amount of disk utilization
        // that exceeds the limit. If the burst budget is exhausted, we will not allow any more disk
        // access and terminate the process.
        if let Some(disk_limit) = self.disk_limit {
            let stats = lgalloc::lgalloc_stats();
            let disk_utilization = stats
                .file
                .iter()
                .flat_map(|(_size_class, file_stat)| file_stat.as_ref().ok())
                .map(|file| file.allocated_size)
                .sum::<usize>();

            tracing::info!(
                disk_utilization,
                disk_limit,
                self.burst_budget,
                self.burst_budget_remaining,
                "lgalloc disk utilization"
            );

            if disk_utilization > disk_limit {
                // excess in byte-seconds
                let excess = (disk_utilization - disk_limit)
                    * usize::cast_from(self.last_update.elapsed().as_secs());
                if self.burst_budget_remaining >= excess {
                    self.burst_budget_remaining -= excess;
                } else {
                    // Burst budget exhausted, terminate the process.
                    tracing::error!(
                        "Disk utilization {} exceeded limit {} and burst budget exhausted",
                        disk_utilization,
                        disk_limit
                    );
                    std::process::exit(1);
                }
            } else {
                // Reset burst budget if under limit.
                self.burst_budget_remaining = self.burst_budget;
            }
        }
        self.last_update = Instant::now();
        Ok(())
    }

    fn update_config(&mut self, update: Update) {
        let updated = match update {
            Update::Interval(_new_interval) => {
                // This is handled by the task.
                false
            }
            Update::DiskLimit(new_limit) => {
                if self.disk_limit != new_limit {
                    self.disk_limit = new_limit;
                    true
                } else {
                    false
                }
            }
            Update::BurstBudget(new_budget) => {
                if self.burst_budget != new_budget {
                    self.burst_budget = new_budget;
                    true
                } else {
                    false
                }
            }
        };

        if updated {
            self.burst_budget_remaining = self.burst_budget;
            self.last_update = Instant::now();

            tracing::info!(
                self.disk_limit,
                self.burst_budget,
                self.burst_budget_remaining,
                "lgalloc limiter updated"
            );
        }
    }
}

impl Limiter {
    const NAME: &'static str = "lgalloc_limiter";
    /// Create a new metrics task that updates metrics at a given interval.
    pub fn create_task(
        announce_memory_limit: Option<usize>,
        interval_config: mz_dyncfg::Config<Duration>,
        update_duration_metric: &mz_ore::metrics::HistogramVec,
    ) -> LimiterHandle {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // Start disabled.
        let mut interval: Option<Interval> = None;

        let update_duration_metric =
            update_duration_metric.get_delete_on_drop_metric(&[Self::NAME][..]);

        let mut metrics = LimiterTask {
            disk_limit: None,
            burst_budget: 0,
            burst_budget_remaining: 0,
            last_update: Instant::now(),
        };

        mz_ore::task::spawn(
            || format!("mz_metrics_update({})", Self::NAME),
            async move {
                loop {
                    tokio::select! {
                        _ = async { interval.as_mut().unwrap().tick().await }, if interval.is_some() => {
                            tracing::debug!(metrics = Self::NAME, "updating metrics");
                            let start = std::time::Instant::now();
                            if let Err(err) = metrics.update() {
                                tracing::error!(metrics = Self::NAME, ?err, "metrics update failed");
                            }
                            let elapsed = start.elapsed();
                            update_duration_metric.observe(elapsed.as_secs_f64());
                        }
                        new_interval = rx.recv() => match new_interval {
                            Some(Update::Interval(new_interval)) => Self::update_interval(new_interval, &mut interval),
                            Some(update) => metrics.update_config(update),
                            None => break,
                        }
                    }
                }
            },
        );

        LimiterHandle {
            tx,
            interval_config,
            announce_memory_limit,
        }
    }

    fn update_interval(new_interval: Duration, interval: &mut Option<Interval>) {
        // Zero duration disables ticking.
        if new_interval == Duration::ZERO {
            *interval = None;
            return;
        }
        // Prevent no-op changes.
        if Some(new_interval) == interval.as_ref().map(Interval::period) {
            return;
        }
        tracing::debug!(metrics = Self::NAME, ?new_interval, "updating interval");
        let mut new_interval = tokio::time::interval(new_interval);
        new_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        *interval = Some(new_interval);
    }

    /// Update the dynamic configuration.
    pub fn apply_dyncfg_updates(&mut self, config_set: &ConfigSet) {
        // Notify tasks about updated configuration.
        self.task.update_dyncfg(config_set);
    }
}

enum Update {
    Interval(Duration),
    DiskLimit(Option<usize>),
    BurstBudget(usize),
}

impl LimiterHandle {
    pub(crate) fn update_dyncfg(&self, config_set: &ConfigSet) {
        self.tx
            .send(Update::Interval(self.interval_config.get(config_set)))
            .expect("Receiver exists");

        if let Some(announce_memory_limit) = self.announce_memory_limit {
            let disk_limit = f64::cast_lossy(announce_memory_limit)
                * LGALLOC_LIMITER_USAGE_FACTOR.get(config_set)
                * LGALLOC_LIMITER_USAGE_BIAS.get(config_set);
            let disk_limit = usize::cast_lossy(disk_limit);
            let burst_budget = usize::cast_lossy(
                f64::cast_lossy(disk_limit) * LGALLOC_LIMITER_BURST_FACTOR.get(config_set),
            );

            tracing::info!(
                announce_memory_limit,
                disk_limit,
                burst_budget,
                "lgalloc limiter configuration"
            );

            self.tx
                .send(Update::DiskLimit(Some(disk_limit)))
                .expect("Sender exists");
            self.tx
                .send(Update::BurstBudget(burst_budget))
                .expect("Sender exists");
        } else {
            tracing::warn!("No memory limit announced, disabling lgalloc limiter");

            self.tx
                .send(Update::DiskLimit(None))
                .expect("Sender exists");
        }
    }
}
