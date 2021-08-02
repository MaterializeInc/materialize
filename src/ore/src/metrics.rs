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

//! Metrics for materialize systems.
//!
//! The idea here is that each subsystem keeps its metrics in a scoped-to-it struct, which gets
//! registered (once) to the server's (or a test's) prometheus registry.
//!
//! Instead of using prometheus's (very verbose) metrics definitions, we rely on type inference to
//! reduce the verbosity a little bit. A typical subsystem will look like the following:
//!
//! ```rust
//! # use ore::metrics::{MetricsRegistry, UIntCounter};
//! # use ore::metric;
//! #[derive(Debug, Clone)] // Note that prometheus metrics can safely be cloned
//! struct Metrics {
//!     pub bytes_sent: UIntCounter,
//! }
//!
//! impl Metrics {
//!     pub fn register_into(registry: &MetricsRegistry) -> Metrics {
//!         Metrics {
//!             bytes_sent: registry.register(metric!(
//!                 name: "mz_pg_sent_bytes",
//!                 help: "total number of bytes sent here",
//!             )),
//!         }
//!     }
//! }
//! ```

use crate::stats::HISTOGRAM_BUCKETS;
use prometheus::core::{
    Atomic, Collector, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec, Opts,
};
use prometheus::proto::MetricFamily;
use prometheus::{HistogramOpts, Registry};

pub use prometheus::Opts as PrometheusOpts;
pub use prometheus::{
    Counter, CounterVec, Gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, UIntCounter, UIntCounterVec, UIntGauge, UIntGaugeVec,
};

mod delete_on_drop;
pub use delete_on_drop::*;

/// Define a metric for use in materialize.
#[macro_export]
macro_rules! metric {
    (
        name: $name:expr,
        help: $help:expr
        $(, const_labels: { $($cl_key:expr => $cl_value:expr ),* })?
        $(, var_labels: [ $($vl_name:expr),* ])?
        $(,)?
    ) => {{
        let const_labels: ::std::collections::HashMap<String, String> = (&[
            $($(
                ($cl_key.to_string(), $cl_value.to_string()),
            )*)?
        ]).into_iter().cloned().collect();
        let var_labels: ::std::vec::Vec<String> = vec![
            $(
                $($vl_name.into(),)*
            )?];
        $crate::metrics::PrometheusOpts::new($name, $help)
            .const_labels(const_labels)
            .variable_labels(var_labels)
    }}
}

/// The materialize metrics registry.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    inner: Registry,
}

impl MetricsRegistry {
    /// Creates a new metrics registry.
    pub fn new() -> Self {
        MetricsRegistry {
            inner: Registry::new(),
        }
    }

    /// Register a metric defined with the [`metric`] macro.
    pub fn register<M>(&self, opts: prometheus::Opts) -> M
    where
        M: MakeCollector,
    {
        let collector = M::make_collector(opts);
        self.inner.register(Box::new(collector.clone())).unwrap();
        collector
    }

    /// Register a pre-defined prometheus collector.
    pub fn register_collector<C: 'static + prometheus::core::Collector>(&self, collector: C) {
        self.inner
            .register(Box::new(collector))
            .expect("registering pre-defined metrics collector");
    }

    /// Gather all the metrics from the metrics registry for reporting.
    ///
    /// See also [`prometheus::Registry::gather`].
    pub fn gather(&self) -> Vec<MetricFamily> {
        self.inner.gather()
    }
}

/// A wrapper for creating prometheus metrics more conveniently.
///
/// Together with the [`metric`] macro, this trait is mainly used by [`MetricsRegistry`] and should
/// not normally be used outside the metric registration flow.
pub trait MakeCollector: Collector + Clone + 'static {
    /// Creates a new collector.
    fn make_collector(opts: Opts) -> Self;
}

impl<T> MakeCollector for GenericCounter<T>
where
    T: Atomic + 'static,
{
    fn make_collector(opts: Opts) -> Self {
        Self::with_opts(opts).expect("defining a counter")
    }
}

impl<T> MakeCollector for GenericCounterVec<T>
where
    T: Atomic + 'static,
{
    fn make_collector(opts: Opts) -> Self {
        let labels: Vec<String> = opts.variable_labels.clone();
        let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
        Self::new(opts, label_refs.as_slice()).expect("defining a counter vec")
    }
}

impl<T> MakeCollector for GenericGauge<T>
where
    T: Atomic + 'static,
{
    fn make_collector(opts: Opts) -> Self {
        Self::with_opts(opts).expect("defining a gauge")
    }
}

impl<T> MakeCollector for GenericGaugeVec<T>
where
    T: Atomic + 'static,
{
    fn make_collector(opts: Opts) -> Self {
        let labels = opts.variable_labels.clone();
        let labels = &labels.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        Self::new(opts, labels).expect("defining a gauge vec")
    }
}

impl MakeCollector for HistogramVec {
    fn make_collector(opts: Opts) -> Self {
        let labels = opts.variable_labels.clone();
        let labels = &labels.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        Self::new(
            HistogramOpts {
                common_opts: opts,
                buckets: HISTOGRAM_BUCKETS.to_vec(),
            },
            labels,
        )
        .expect("defining a histogram vec")
    }
}
