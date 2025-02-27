// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Internal metrics libraries for Materialize.

#![warn(missing_docs, missing_debug_implementations)]

use std::time::Duration;

use mz_dyncfg::{ConfigSet, ConfigUpdates};
use mz_ore::metrics::MetricsRegistry;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;

pub use dyncfgs::all_dyncfgs;

mod dyncfgs;
pub mod lgalloc;
pub mod rusage;

/// Handle to metrics defined in this crate.
#[derive(Debug)]
pub struct Metrics {
    config_set: ConfigSet,
    lgalloc: MetricsTask,
    rusage: MetricsTask,
}

static METRICS: std::sync::Mutex<Option<Metrics>> = std::sync::Mutex::new(None);

/// Register all metrics into the provided registry.
///
/// This function is async, because it needs to be called from a tokio runtime context.
#[allow(clippy::unused_async)]
pub async fn register_metrics_into(metrics_registry: &MetricsRegistry, config_set: ConfigSet) {
    let update_duration_metric = metrics_registry.register(mz_ore::metric!(
        name: "mz_metrics_update_duration",
        help: "The time it took to update lgalloc stats",
        var_labels: ["name"],
        buckets: mz_ore::stats::histogram_seconds_buckets(0.000_500, 32.),
    ));

    let lgalloc = Metrics::new_metrics_task(
        metrics_registry,
        lgalloc::register_metrics_into,
        dyncfgs::MZ_METRICS_LGALLOC_REFRESH_INTERVAL,
        &update_duration_metric,
    );
    let rusage = Metrics::new_metrics_task(
        metrics_registry,
        rusage::register_metrics_into,
        dyncfgs::MZ_METRICS_RUSAGE_REFRESH_INTERVAL,
        &update_duration_metric,
    );

    *METRICS.lock().unwrap() = Some(Metrics {
        lgalloc,
        rusage,
        config_set,
    })
}

/// Update the configuration of the metrics.
pub fn update_dyncfg(config_updates: &ConfigUpdates) {
    if let Some(metrics) = METRICS.lock().expect("lock poisoned").as_mut() {
        config_updates.apply(&metrics.config_set);
        metrics.apply_dyncfg_updates();
    }
}

impl Metrics {
    /// Update the dynamic configuration.
    pub fn apply_dyncfg_updates(&mut self) {
        self.lgalloc.update_dyncfg(&self.config_set);
        self.rusage.update_dyncfg(&self.config_set);
    }

    fn new_metrics_task<T: MetricsUpdate>(
        metrics_registry: &MetricsRegistry,
        constructor: impl FnOnce(&MetricsRegistry) -> T,
        interval_config: mz_dyncfg::Config<Duration>,
        update_duration_metric: &mz_ore::metrics::HistogramVec,
    ) -> MetricsTask {
        let token = CancellationToken::new();
        let token2 = token.clone();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // Start disabled.
        let mut interval: Option<Interval> = None;

        let update_duration_metric =
            update_duration_metric.get_delete_on_drop_metric(vec![T::NAME]);

        let mut metrics = constructor(metrics_registry);

        let mut update_metrics = move || {
            tracing::debug!(metrics = T::NAME, "updating metrics");
            let start = std::time::Instant::now();
            if let Err(err) = metrics.update() {
                tracing::error!(metrics = T::NAME, ?err, "metrics update failed");
            }
            let elapsed = start.elapsed();
            update_duration_metric.observe(elapsed.as_secs_f64());
        };

        let update_interval = |new_interval, interval: &mut Option<Interval>| {
            tracing::debug!(metrics = T::NAME, ?new_interval, "updating interval");
            if new_interval == Duration::ZERO {
                *interval = None;
                return;
            }
            // Prevent no-op changes.
            if Some(new_interval) == interval.as_ref().map(Interval::period) {
                return;
            }
            let mut new_interval = tokio::time::interval(new_interval);
            new_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            *interval = Some(new_interval);
        };

        mz_ore::task::spawn(|| format!("mz_metrics_update({})", T::NAME), async move {
            loop {
                tokio::select! {
                    _ = async { interval.as_mut().unwrap().tick().await }, if interval.is_some() => {
                        update_metrics()
                    }
                    _ = token2.cancelled() => break,
                    Some(new_interval) = rx.recv() => update_interval(new_interval, &mut interval),
                }
            }
        });

        MetricsTask {
            tx,
            interval_config,
            token,
        }
    }
}

/// Behavior to update metrics.
pub trait MetricsUpdate: Send + Sync + 'static {
    /// Error type to indicate updating failed.
    type Error: std::fmt::Debug;
    /// A human-readable name.
    const NAME: &'static str;
    /// Update the metrics.
    fn update(&mut self) -> Result<(), Self::Error>;
}

#[derive(Debug)]
struct MetricsTask {
    interval_config: mz_dyncfg::Config<Duration>,
    tx: tokio::sync::mpsc::UnboundedSender<Duration>,
    token: CancellationToken,
}

impl Drop for MetricsTask {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

impl MetricsTask {
    pub(crate) fn update_dyncfg(&self, config_set: &ConfigSet) {
        self.tx
            .send(self.interval_config.get(config_set))
            .expect("Receiver exists");
    }
}
