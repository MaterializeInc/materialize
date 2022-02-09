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
//! # use mz_ore::metrics::{MetricsRegistry, UIntCounter};
//! # use mz_ore::metric;
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

use std::fmt;
use std::sync::Arc;

use prometheus::core::{
    Atomic, AtomicF64, AtomicI64, AtomicU64, Collector, Desc, GenericCounter, GenericCounterVec,
    GenericGauge, GenericGaugeVec, Opts,
};
use prometheus::proto::MetricFamily;
use prometheus::{HistogramOpts, Registry};

use crate::stats::HISTOGRAM_BUCKETS;

pub use prometheus::Opts as PrometheusOpts;

mod delete_on_drop;
mod third_party_metric;

pub use delete_on_drop::*;
use std::fmt::{Debug, Formatter};
pub use third_party_metric::*;

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
    third_party: Registry,
}

/// A wrapper for metrics to require delete on drop semantics
///
/// The wrapper behaves like regular metrics but only provides functions to create delete-on-drop
/// variants. This way, no metrics if this type can be leaked.
///
/// In situations where the delete-on-drop behavior is not desired or in legacy code, use the raw
/// variants of the metrics, as defined in [self::raw].
#[derive(Clone)]
pub struct DeleteOnDropWrapper<M> {
    inner: M,
}

impl<M: MakeCollector + Debug> Debug for DeleteOnDropWrapper<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<M: Collector> Collector for DeleteOnDropWrapper<M> {
    fn desc(&self) -> Vec<&Desc> {
        self.inner.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.inner.collect()
    }
}

impl<M: MakeCollector> MakeCollector for DeleteOnDropWrapper<M> {
    fn make_collector(opts: PrometheusOpts) -> Self {
        DeleteOnDropWrapper {
            inner: M::make_collector(opts),
        }
    }
}

impl<M: GaugeVecExt> GaugeVecExt for DeleteOnDropWrapper<M> {
    type GaugeType = M::GaugeType;

    fn get_delete_on_drop_gauge<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropGauge<'a, Self::GaugeType, L> {
        self.inner.get_delete_on_drop_gauge(labels)
    }
}

impl<M: CounterVecExt> CounterVecExt for DeleteOnDropWrapper<M> {
    type CounterType = M::CounterType;

    fn get_delete_on_drop_counter<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropCounter<'a, Self::CounterType, L> {
        self.inner.get_delete_on_drop_counter(labels)
    }
}

impl<M: HistogramVecExt> HistogramVecExt for DeleteOnDropWrapper<M> {
    fn get_delete_on_drop_histogram<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropHistogram<'a, L> {
        self.inner.get_delete_on_drop_histogram(labels)
    }
}

/// Delete-on-drop shadow of Prometheus [prometheus::CounterVec].
pub type CounterVec = DeleteOnDropWrapper<prometheus::CounterVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::Gauge].
pub type Gauge = DeleteOnDropWrapper<prometheus::Gauge>;
/// Delete-on-drop shadow of Prometheus [prometheus::HistogramVec].
pub type HistogramVec = DeleteOnDropWrapper<prometheus::HistogramVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::IntCounterVec].
pub type IntCounterVec = DeleteOnDropWrapper<prometheus::IntCounterVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::IntGaugeVec].
pub type IntGaugeVec = DeleteOnDropWrapper<prometheus::IntGaugeVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::UIntCounterVec].
pub type UIntCounterVec = DeleteOnDropWrapper<prometheus::UIntCounterVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::UIntGaugeVec].
pub type UIntGaugeVec = DeleteOnDropWrapper<prometheus::UIntGaugeVec>;

pub use prometheus::{Counter, Histogram, IntCounter, IntGauge, UIntCounter, UIntGauge};
pub mod raw {
    //! Access to non-delete-on-drop vector types
    pub use prometheus::{
        CounterVec, HistogramVec, IntCounterVec, IntGaugeVec, UIntCounterVec, UIntGaugeVec,
    };
}

impl MetricsRegistry {
    /// Creates a new metrics registry.
    pub fn new() -> Self {
        MetricsRegistry {
            inner: Registry::new(),
            third_party: Registry::new(),
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

    /// Registers a gauge whose value is computed when observed.
    pub fn register_computed_gauge<F, P>(
        &self,
        opts: prometheus::Opts,
        f: F,
    ) -> ComputedGenericGauge<P>
    where
        F: Fn() -> P::T + Send + Sync + 'static,
        P: Atomic + 'static,
    {
        let gauge = ComputedGenericGauge {
            gauge: GenericGauge::make_collector(opts),
            f: Arc::new(f),
        };
        self.inner.register(Box::new(gauge.clone())).unwrap();
        gauge
    }

    /// Register a metric that can be scraped from both the "normal" registry, as well as the
    /// registry that is accessible to third parties (like cloud providers and infrastructure
    /// orchestrators).
    ///
    /// Take care to vet metrics that are visible to third parties: metrics containing sensitive
    /// information as labels (e.g. source/sink names or other user-defined identifiers), or
    /// "traffic" type labels can lead to information getting exposed that users might not be
    /// comfortable sharing.
    pub fn register_third_party_visible<M>(&self, opts: prometheus::Opts) -> ThirdPartyMetric<M>
    where
        M: MakeCollector,
    {
        let collector = M::make_collector(opts);
        self.inner.register(Box::new(collector.clone())).unwrap();
        self.third_party
            .register(Box::new(collector.clone()))
            .unwrap();
        ThirdPartyMetric { inner: collector }
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

    /// Gather all the metrics from the metrics registry that's visible to third parties.
    ///
    /// See also [`prometheus::Registry::gather`].
    pub fn gather_third_party_visible(&self) -> Vec<MetricFamily> {
        self.third_party.gather()
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

impl MakeCollector for raw::HistogramVec {
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

/// A [`Gauge`] whose value is computed whenever it is observed.
pub struct ComputedGenericGauge<P>
where
    P: Atomic,
{
    gauge: GenericGauge<P>,
    f: Arc<dyn Fn() -> P::T + Send + Sync>,
}

impl<P> fmt::Debug for ComputedGenericGauge<P>
where
    P: Atomic + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ComputedGenericGauge")
            .field("gauge", &self.gauge)
            .finish_non_exhaustive()
    }
}

impl<P> Clone for ComputedGenericGauge<P>
where
    P: Atomic,
{
    fn clone(&self) -> ComputedGenericGauge<P> {
        ComputedGenericGauge {
            gauge: self.gauge.clone(),
            f: Arc::clone(&self.f),
        }
    }
}

impl<T> Collector for ComputedGenericGauge<T>
where
    T: Atomic,
{
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.gauge.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.gauge.set((self.f)());
        self.gauge.collect()
    }
}

impl<P> ComputedGenericGauge<P>
where
    P: Atomic,
{
    /// Computes the current value of the gauge.
    pub fn get(&self) -> P::T {
        (self.f)()
    }
}

/// A [`ComputedGenericGauge`] for 64-bit floating point numbers.
pub type ComputedGauge = ComputedGenericGauge<AtomicF64>;

/// A [`ComputedGenericGauge`] for 64-bit signed integers.
pub type ComputedIntGauge = ComputedGenericGauge<AtomicI64>;

/// A [`ComputedGenericGauge`] for 64-bit unsigned integers.
pub type ComputedUIntGauge = ComputedGenericGauge<AtomicU64>;

#[cfg(test)]
mod tests;
