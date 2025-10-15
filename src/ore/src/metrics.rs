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
//! # use mz_ore::metrics::{MetricsRegistry, IntCounter};
//! # use mz_ore::metric;
//! #[derive(Debug, Clone)] // Note that prometheus metrics can safely be cloned
//! struct Metrics {
//!     pub bytes_sent: IntCounter,
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
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use derivative::Derivative;
use pin_project::pin_project;
use prometheus::core::{
    Atomic, AtomicF64, AtomicI64, AtomicU64, Collector, Desc, GenericCounter, GenericCounterVec,
    GenericGauge, GenericGaugeVec,
};
use prometheus::proto::MetricFamily;
use prometheus::{HistogramOpts, Registry};

mod delete_on_drop;

pub use delete_on_drop::*;
pub use prometheus::Opts as PrometheusOpts;

/// Define a metric for use in materialize.
#[macro_export]
macro_rules! metric {
    (
        name: $name:expr,
        help: $help:expr
        $(, subsystem: $subsystem_name:expr)?
        $(, const_labels: { $($cl_key:expr => $cl_value:expr ),* })?
        $(, var_labels: [ $($vl_name:expr),* ])?
        $(, buckets: $bk_name:expr)?
        $(,)?
    ) => {{
        let const_labels = (&[
            $($(
                ($cl_key.to_string(), $cl_value.to_string()),
            )*)?
        ]).into_iter().cloned().collect();
        let var_labels = vec![
            $(
                $($vl_name.into(),)*
            )?];
        #[allow(unused_mut)]
        let mut mk_opts = $crate::metrics::MakeCollectorOpts {
            opts: $crate::metrics::PrometheusOpts::new($name, $help)
                $(.subsystem( $subsystem_name ))?
                .const_labels(const_labels)
                .variable_labels(var_labels),
            buckets: None,
        };
        // Set buckets if passed
        $(mk_opts.buckets = Some($bk_name);)*
        mk_opts
    }}
}

/// Options for MakeCollector. This struct should be instantiated using the metric macro.
#[derive(Debug, Clone)]
pub struct MakeCollectorOpts {
    /// Common Prometheus options
    pub opts: PrometheusOpts,
    /// Buckets to be used with Histogram and HistogramVec. Must be set to create Histogram types
    /// and must not be set for other types.
    pub buckets: Option<Vec<f64>>,
}

/// The materialize metrics registry.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct MetricsRegistry {
    inner: Registry,
    #[derivative(Debug = "ignore")]
    postprocessors: Arc<Mutex<Vec<Box<dyn FnMut(&mut Vec<MetricFamily>) + Send + Sync>>>>,
}

/// A wrapper for metrics to require delete on drop semantics
///
/// The wrapper behaves like regular metrics but only provides functions to create delete-on-drop
/// variants. This way, no metrics of this type can be leaked.
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
    fn make_collector(opts: MakeCollectorOpts) -> Self {
        DeleteOnDropWrapper {
            inner: M::make_collector(opts),
        }
    }
}

impl<M: MetricVecExt> DeleteOnDropWrapper<M> {
    /// Returns a metric that deletes its labels from this metrics vector when dropped.
    pub fn get_delete_on_drop_metric<L: PromLabelsExt>(
        &self,
        labels: L,
    ) -> DeleteOnDropMetric<M, L> {
        self.inner.get_delete_on_drop_metric(labels)
    }
}

/// The unsigned integer version of [`Gauge`]. Provides better performance if
/// metric values are all unsigned integers.
pub type UIntGauge = GenericGauge<AtomicU64>;

/// Delete-on-drop shadow of Prometheus [prometheus::CounterVec].
pub type CounterVec = DeleteOnDropWrapper<prometheus::CounterVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::Gauge].
pub type Gauge = DeleteOnDropWrapper<prometheus::Gauge>;
/// Delete-on-drop shadow of Prometheus [prometheus::GaugeVec].
pub type GaugeVec = DeleteOnDropWrapper<prometheus::GaugeVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::HistogramVec].
pub type HistogramVec = DeleteOnDropWrapper<prometheus::HistogramVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::IntCounterVec].
pub type IntCounterVec = DeleteOnDropWrapper<prometheus::IntCounterVec>;
/// Delete-on-drop shadow of Prometheus [prometheus::IntGaugeVec].
pub type IntGaugeVec = DeleteOnDropWrapper<prometheus::IntGaugeVec>;
/// Delete-on-drop shadow of Prometheus [raw::UIntGaugeVec].
pub type UIntGaugeVec = DeleteOnDropWrapper<raw::UIntGaugeVec>;

use crate::assert_none;

pub use prometheus::{Counter, Histogram, IntCounter, IntGauge};

/// Access to non-delete-on-drop vector types
pub mod raw {
    use prometheus::core::{AtomicU64, GenericGaugeVec};

    /// The unsigned integer version of [`GaugeVec`].
    /// Provides better performance if metric values are all unsigned integers.
    pub type UIntGaugeVec = GenericGaugeVec<AtomicU64>;

    pub use prometheus::{CounterVec, Gauge, GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec};
}

impl MetricsRegistry {
    /// Creates a new metrics registry.
    pub fn new() -> Self {
        MetricsRegistry {
            inner: Registry::new(),
            postprocessors: Arc::new(Mutex::new(vec![])),
        }
    }

    /// Register a metric defined with the [`metric`] macro.
    pub fn register<M>(&self, opts: MakeCollectorOpts) -> M
    where
        M: MakeCollector,
    {
        let collector = M::make_collector(opts);
        self.inner.register(Box::new(collector.clone())).unwrap();
        collector
    }

    /// Registers a gauge whose value is computed when observed.
    pub fn register_computed_gauge<P>(
        &self,
        opts: MakeCollectorOpts,
        f: impl Fn() -> P::T + Send + Sync + 'static,
    ) -> ComputedGenericGauge<P>
    where
        P: Atomic + 'static,
    {
        let gauge = ComputedGenericGauge {
            gauge: GenericGauge::make_collector(opts),
            f: Arc::new(f),
        };
        self.inner.register(Box::new(gauge.clone())).unwrap();
        gauge
    }

    /// Register a pre-defined prometheus collector.
    pub fn register_collector<C: 'static + prometheus::core::Collector>(&self, collector: C) {
        self.inner
            .register(Box::new(collector))
            .expect("registering pre-defined metrics collector");
    }

    /// Registers a metric postprocessor.
    ///
    /// Postprocessors are invoked on every call to [`MetricsRegistry::gather`]
    /// in the order that they are registered.
    pub fn register_postprocessor<F>(&self, f: F)
    where
        F: FnMut(&mut Vec<MetricFamily>) + Send + Sync + 'static,
    {
        let mut postprocessors = self.postprocessors.lock().expect("lock poisoned");
        postprocessors.push(Box::new(f));
    }

    /// Gather all the metrics from the metrics registry for reporting.
    ///
    /// This function invokes the postprocessors on all gathered metrics (see
    /// [`MetricsRegistry::register_postprocessor`]) in the order the
    /// postprocessors were registered.
    ///
    /// See also [`prometheus::Registry::gather`].
    pub fn gather(&self) -> Vec<MetricFamily> {
        let mut metrics = self.inner.gather();
        let mut postprocessors = self.postprocessors.lock().expect("lock poisoned");
        for postprocessor in &mut *postprocessors {
            postprocessor(&mut metrics);
        }
        metrics
    }
}

/// A wrapper for creating prometheus metrics more conveniently.
///
/// Together with the [`metric`] macro, this trait is mainly used by [`MetricsRegistry`] and should
/// not normally be used outside the metric registration flow.
pub trait MakeCollector: Collector + Clone + 'static {
    /// Creates a new collector.
    fn make_collector(opts: MakeCollectorOpts) -> Self;
}

impl<T> MakeCollector for GenericCounter<T>
where
    T: Atomic + 'static,
{
    fn make_collector(mk_opts: MakeCollectorOpts) -> Self {
        assert_none!(mk_opts.buckets);
        Self::with_opts(mk_opts.opts).expect("defining a counter")
    }
}

impl<T> MakeCollector for GenericCounterVec<T>
where
    T: Atomic + 'static,
{
    fn make_collector(mk_opts: MakeCollectorOpts) -> Self {
        assert_none!(mk_opts.buckets);
        let labels: Vec<String> = mk_opts.opts.variable_labels.clone();
        let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
        Self::new(mk_opts.opts, label_refs.as_slice()).expect("defining a counter vec")
    }
}

impl<T> MakeCollector for GenericGauge<T>
where
    T: Atomic + 'static,
{
    fn make_collector(mk_opts: MakeCollectorOpts) -> Self {
        assert_none!(mk_opts.buckets);
        Self::with_opts(mk_opts.opts).expect("defining a gauge")
    }
}

impl<T> MakeCollector for GenericGaugeVec<T>
where
    T: Atomic + 'static,
{
    fn make_collector(mk_opts: MakeCollectorOpts) -> Self {
        assert_none!(mk_opts.buckets);
        let labels = mk_opts.opts.variable_labels.clone();
        let labels = &labels.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        Self::new(mk_opts.opts, labels).expect("defining a gauge vec")
    }
}

impl MakeCollector for Histogram {
    fn make_collector(mk_opts: MakeCollectorOpts) -> Self {
        assert!(mk_opts.buckets.is_some());
        Self::with_opts(HistogramOpts {
            common_opts: mk_opts.opts,
            buckets: mk_opts.buckets.unwrap(),
        })
        .expect("defining a histogram")
    }
}

impl MakeCollector for raw::HistogramVec {
    fn make_collector(mk_opts: MakeCollectorOpts) -> Self {
        assert!(mk_opts.buckets.is_some());
        let labels = mk_opts.opts.variable_labels.clone();
        let labels = &labels.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        Self::new(
            HistogramOpts {
                common_opts: mk_opts.opts,
                buckets: mk_opts.buckets.unwrap(),
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

/// Exposes combinators that report metrics related to the execution of a [`Future`] to prometheus.
pub trait MetricsFutureExt<F> {
    /// Records the number of seconds it takes a [`Future`] to complete according to "the clock on
    /// the wall".
    ///
    /// More specifically, it records the instant at which the `Future` was first polled, and the
    /// instant at which the `Future` completes. Then reports the duration between those two
    /// instances to the provided metric.
    ///
    /// # Wall Time vs Execution Time
    ///
    /// There is also [`MetricsFutureExt::exec_time`], which measures how long a [`Future`] spent
    /// executing, instead of how long it took to complete. For example, a network request may have
    /// a wall time of 1 second, meanwhile it's execution time may have only been 50ms. The 950ms
    /// delta would be how long the [`Future`] waited for a response from the network.
    ///
    /// # Uses
    ///
    /// Recording the wall time can be useful for monitoring latency, for example the latency of a
    /// SQL request.
    ///
    /// Note: You must call either [`observe`] to record the execution time to a [`Histogram`] or
    /// [`inc_by`] to record to a [`Counter`]. The following will not compile:
    ///
    /// ```compile_fail
    /// use mz_ore::metrics::MetricsFutureExt;
    ///
    /// # let _ = async {
    /// async { Ok(()) }
    ///     .wall_time()
    ///     .await;
    /// # };
    /// ```
    ///
    /// [`observe`]: WallTimeFuture::observe
    /// [`inc_by`]: WallTimeFuture::inc_by
    fn wall_time(self) -> WallTimeFuture<F, UnspecifiedMetric>;

    /// Records the total number of seconds for which a [`Future`] was executing.
    ///
    /// More specifically, every time the `Future` is polled it records how long that individual
    /// call took, and maintains a running sum until the `Future` completes. Then we report that
    /// duration to the provided metric.
    ///
    /// # Wall Time vs Execution Time
    ///
    /// There is also [`MetricsFutureExt::wall_time`], which measures how long a [`Future`] took to
    /// complete, instead of how long it spent executing. For example, a network request may have
    /// a wall time of 1 second, meanwhile it's execution time may have only been 50ms. The 950ms
    /// delta would be how long the [`Future`] waited for a response from the network.
    ///
    /// # Uses
    ///
    /// Recording execution time can be useful if you want to monitor [`Future`]s that could be
    /// sensitive to CPU usage. For example, if you have a single logical control thread you'll
    /// want to make sure that thread never spends too long running a single `Future`. Reporting
    /// the execution time of `Future`s running on this thread can help ensure there is no
    /// unexpected blocking.
    ///
    /// Note: You must call either [`observe`] to record the execution time to a [`Histogram`] or
    /// [`inc_by`] to record to a [`Counter`]. The following will not compile:
    ///
    /// ```compile_fail
    /// use mz_ore::metrics::MetricsFutureExt;
    ///
    /// # let _ = async {
    /// async { Ok(()) }
    ///     .exec_time()
    ///     .await;
    /// # };
    /// ```
    ///
    /// [`observe`]: ExecTimeFuture::observe
    /// [`inc_by`]: ExecTimeFuture::inc_by
    fn exec_time(self) -> ExecTimeFuture<F, UnspecifiedMetric>;
}

impl<F: Future> MetricsFutureExt<F> for F {
    fn wall_time(self) -> WallTimeFuture<F, UnspecifiedMetric> {
        WallTimeFuture {
            fut: self,
            metric: UnspecifiedMetric(()),
            start: None,
            filter: None,
        }
    }

    fn exec_time(self) -> ExecTimeFuture<F, UnspecifiedMetric> {
        ExecTimeFuture {
            fut: self,
            metric: UnspecifiedMetric(()),
            running_duration: Duration::from_millis(0),
            filter: None,
        }
    }
}

/// Future returned by [`MetricsFutureExt::wall_time`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct WallTimeFuture<F, Metric> {
    /// The inner [`Future`] that we're recording the wall time for.
    #[pin]
    fut: F,
    /// Prometheus metric that we'll report to.
    metric: Metric,
    /// [`Instant`] at which the [`Future`] was first polled.
    start: Option<Instant>,
    /// Optional filter that determines if we observe the wall time of this [`Future`].
    filter: Option<Box<dyn FnMut(Duration) -> bool + Send + Sync>>,
}

impl<F: Debug, M: Debug> fmt::Debug for WallTimeFuture<F, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WallTimeFuture")
            .field("fut", &self.fut)
            .field("metric", &self.metric)
            .field("start", &self.start)
            .field("filter", &self.filter.is_some())
            .finish()
    }
}

impl<F> WallTimeFuture<F, UnspecifiedMetric> {
    /// Sets the recored metric to be a [`prometheus::Histogram`].
    ///
    /// ```text
    /// my_future
    ///     .wall_time()
    ///     .observe(metrics.slow_queries_hist.with_label_values(&["select"]))
    /// ```
    pub fn observe(
        self,
        histogram: prometheus::Histogram,
    ) -> WallTimeFuture<F, prometheus::Histogram> {
        WallTimeFuture {
            fut: self.fut,
            metric: histogram,
            start: self.start,
            filter: self.filter,
        }
    }

    /// Sets the recored metric to be a [`prometheus::Counter`].
    ///
    /// ```text
    /// my_future
    ///     .wall_time()
    ///     .inc_by(metrics.slow_queries.with_label_values(&["select"]))
    /// ```
    pub fn inc_by(self, counter: prometheus::Counter) -> WallTimeFuture<F, prometheus::Counter> {
        WallTimeFuture {
            fut: self.fut,
            metric: counter,
            start: self.start,
            filter: self.filter,
        }
    }

    /// Sets the recorded duration in a specific f64.
    pub fn set_at(self, place: &mut f64) -> WallTimeFuture<F, &mut f64> {
        WallTimeFuture {
            fut: self.fut,
            metric: place,
            start: self.start,
            filter: self.filter,
        }
    }
}

impl<F, M> WallTimeFuture<F, M> {
    /// Specifies a filter which much return `true` for the wall time to be recorded.
    ///
    /// This can be particularly useful if you have a high volume `Future` and you only want to
    /// record ones that take a long time to complete.
    pub fn with_filter(
        mut self,
        filter: impl FnMut(Duration) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.filter = Some(Box::new(filter));
        self
    }
}

impl<F: Future, M: DurationMetric> Future for WallTimeFuture<F, M> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if this.start.is_none() {
            *this.start = Some(Instant::now());
        }

        let result = match this.fut.poll(cx) {
            Poll::Ready(r) => r,
            Poll::Pending => return Poll::Pending,
        };
        let duration = Instant::now().duration_since(this.start.expect("timer to be started"));

        let pass = this
            .filter
            .as_mut()
            .map(|filter| filter(duration))
            .unwrap_or(true);
        if pass {
            this.metric.record(duration.as_secs_f64())
        }

        Poll::Ready(result)
    }
}

/// Future returned by [`MetricsFutureExt::exec_time`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct ExecTimeFuture<F, Metric> {
    /// The inner [`Future`] that we're recording the wall time for.
    #[pin]
    fut: F,
    /// Prometheus metric that we'll report to.
    metric: Metric,
    /// Total [`Duration`] for which this [`Future`] has been executing.
    running_duration: Duration,
    /// Optional filter that determines if we observe the execution time of this [`Future`].
    filter: Option<Box<dyn FnMut(Duration) -> bool + Send + Sync>>,
}

impl<F: Debug, M: Debug> fmt::Debug for ExecTimeFuture<F, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecTimeFuture")
            .field("fut", &self.fut)
            .field("metric", &self.metric)
            .field("running_duration", &self.running_duration)
            .field("filter", &self.filter.is_some())
            .finish()
    }
}

impl<F> ExecTimeFuture<F, UnspecifiedMetric> {
    /// Sets the recored metric to be a [`prometheus::Histogram`].
    ///
    /// ```text
    /// my_future
    ///     .exec_time()
    ///     .observe(metrics.slow_queries_hist.with_label_values(&["select"]))
    /// ```
    pub fn observe(
        self,
        histogram: prometheus::Histogram,
    ) -> ExecTimeFuture<F, prometheus::Histogram> {
        ExecTimeFuture {
            fut: self.fut,
            metric: histogram,
            running_duration: self.running_duration,
            filter: self.filter,
        }
    }

    /// Sets the recored metric to be a [`prometheus::Counter`].
    ///
    /// ```text
    /// my_future
    ///     .exec_time()
    ///     .inc_by(metrics.slow_queries.with_label_values(&["select"]))
    /// ```
    pub fn inc_by(self, counter: prometheus::Counter) -> ExecTimeFuture<F, prometheus::Counter> {
        ExecTimeFuture {
            fut: self.fut,
            metric: counter,
            running_duration: self.running_duration,
            filter: self.filter,
        }
    }
}

impl<F, M> ExecTimeFuture<F, M> {
    /// Specifies a filter which much return `true` for the execution time to be recorded.
    pub fn with_filter(
        mut self,
        filter: impl FnMut(Duration) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.filter = Some(Box::new(filter));
        self
    }
}

impl<F: Future, M: DurationMetric> Future for ExecTimeFuture<F, M> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let start = Instant::now();
        let result = this.fut.poll(cx);
        let duration = Instant::now().duration_since(start);

        *this.running_duration = this.running_duration.saturating_add(duration);

        let result = match result {
            Poll::Ready(result) => result,
            Poll::Pending => return Poll::Pending,
        };

        let duration = *this.running_duration;
        let pass = this
            .filter
            .as_mut()
            .map(|filter| filter(duration))
            .unwrap_or(true);
        if pass {
            this.metric.record(duration.as_secs_f64());
        }

        Poll::Ready(result)
    }
}

/// A type level flag used to ensure callers specify the kind of metric to record for
/// [`MetricsFutureExt`].
///
/// For example, `WallTimeFuture<F, M>` only implements [`Future`] for `M` that implements
/// `DurationMetric` which [`UnspecifiedMetric`] does not. This forces users at build time to
/// call [`WallTimeFuture::observe`] or [`WallTimeFuture::inc_by`].
#[derive(Debug)]
pub struct UnspecifiedMetric(());

/// A trait makes recording a duration generic over different prometheus metrics. This allows us to
/// de-dupe the implemenation of [`Future`] for our wrapper Futures like [`WallTimeFuture`] and
/// [`ExecTimeFuture`] over different kinds of prometheus metrics.
trait DurationMetric {
    fn record(&mut self, seconds: f64);
}

impl DurationMetric for prometheus::Histogram {
    fn record(&mut self, seconds: f64) {
        self.observe(seconds)
    }
}

impl DurationMetric for prometheus::Counter {
    fn record(&mut self, seconds: f64) {
        self.inc_by(seconds)
    }
}

// An implementation of `DurationMetric` that lets the user take the recorded
// value and use it elsewhere.
impl DurationMetric for &'_ mut f64 {
    fn record(&mut self, seconds: f64) {
        **self = seconds;
    }
}

/// Register the Tokio runtime's metrics in our metrics registry.
#[cfg(feature = "async")]
pub fn register_runtime_metrics(
    name: &'static str,
    runtime_metrics: tokio::runtime::RuntimeMetrics,
    registry: &MetricsRegistry,
) {
    macro_rules! register {
        ($method:ident, $doc:literal) => {
            let metrics = runtime_metrics.clone();
            registry.register_computed_gauge::<prometheus::core::AtomicU64>(
                crate::metric!(
                    name: concat!("mz_tokio_", stringify!($method)),
                    help: $doc,
                    const_labels: {"runtime" => name},
                ),
                move || <u64 as crate::cast::CastFrom<_>>::cast_from(metrics.$method()),
            );
        };
    }

    register!(
        num_workers,
        "The number of worker threads used by the runtime."
    );
    register!(
        num_alive_tasks,
        "The current number of alive tasks in the runtime."
    );
    register!(
        global_queue_depth,
        "The number of tasks currently scheduled in the runtime's global queue."
    );
    #[cfg(tokio_unstable)]
    {
        register!(
            num_blocking_threads,
            "The number of additional threads spawned by the runtime."
        );
        register!(
            num_idle_blocking_threads,
            "The number of idle threads which have spawned by the runtime for spawn_blocking calls."
        );
        register!(
            spawned_tasks_count,
            "The number of tasks spawned in this runtime since it was created."
        );
        register!(
            remote_schedule_count,
            "The number of tasks scheduled from outside of the runtime."
        );
        register!(
            budget_forced_yield_count,
            "The number of times that tasks have been forced to yield back to the scheduler after exhausting their task budgets."
        );
        register!(
            blocking_queue_depth,
            "The number of tasks currently scheduled in the blocking thread pool, spawned using spawn_blocking."
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use prometheus::{CounterVec, HistogramVec};

    use crate::stats::histogram_seconds_buckets;

    use super::{MetricsFutureExt, MetricsRegistry};

    struct Metrics {
        pub wall_time_hist: HistogramVec,
        pub wall_time_cnt: CounterVec,
        pub exec_time_hist: HistogramVec,
        pub exec_time_cnt: CounterVec,
    }

    impl Metrics {
        pub fn register_into(registry: &MetricsRegistry) -> Self {
            Self {
                wall_time_hist: registry.register(metric!(
                    name: "wall_time_hist",
                    help: "help",
                    var_labels: ["action"],
                    buckets: histogram_seconds_buckets(0.000_128, 8.0),
                )),
                wall_time_cnt: registry.register(metric!(
                    name: "wall_time_cnt",
                    help: "help",
                    var_labels: ["action"],
                )),
                exec_time_hist: registry.register(metric!(
                    name: "exec_time_hist",
                    help: "help",
                    var_labels: ["action"],
                    buckets: histogram_seconds_buckets(0.000_128, 8.0),
                )),
                exec_time_cnt: registry.register(metric!(
                    name: "exec_time_cnt",
                    help: "help",
                    var_labels: ["action"],
                )),
            }
        }
    }

    #[crate::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: integer-to-pointer casts and `ptr::from_exposed_addr` are not supported with `-Zmiri-strict-provenance`
    fn smoke_test_metrics_future_ext() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("failed to start runtime");
        let registry = MetricsRegistry::new();
        let metrics = Metrics::register_into(&registry);

        // Record the walltime and execution time of an async sleep.
        let async_sleep_future = async {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        };
        runtime.block_on(
            async_sleep_future
                .wall_time()
                .observe(metrics.wall_time_hist.with_label_values(&["async_sleep_w"]))
                .exec_time()
                .observe(metrics.exec_time_hist.with_label_values(&["async_sleep_e"])),
        );

        let reports = registry.gather();

        let exec_family = reports
            .iter()
            .find(|m| m.name() == "exec_time_hist")
            .expect("metric not found");
        let exec_metric = exec_family.get_metric();
        assert_eq!(exec_metric.len(), 1);
        assert_eq!(exec_metric[0].get_label()[0].value(), "async_sleep_e");

        let exec_histogram = exec_metric[0].get_histogram();
        assert_eq!(exec_histogram.get_sample_count(), 1);
        // The 4th bucket is 1ms, which we should complete faster than, but is still much quicker
        // than the 5 seconds we slept for.
        assert_eq!(exec_histogram.get_bucket()[3].cumulative_count(), 1);

        let wall_family = reports
            .iter()
            .find(|m| m.name() == "wall_time_hist")
            .expect("metric not found");
        let wall_metric = wall_family.get_metric();
        assert_eq!(wall_metric.len(), 1);
        assert_eq!(wall_metric[0].get_label()[0].value(), "async_sleep_w");

        let wall_histogram = wall_metric[0].get_histogram();
        assert_eq!(wall_histogram.get_sample_count(), 1);
        // The 13th bucket is 512ms, which the wall time should be longer than, but is also much
        // faster than the actual execution time of the async sleep.
        assert_eq!(wall_histogram.get_bucket()[12].cumulative_count(), 0);

        // Reset the registery to make collecting metrics easier.
        let registry = MetricsRegistry::new();
        let metrics = Metrics::register_into(&registry);

        // Record the walltime and execution time of a thread sleep.
        let thread_sleep_future = async {
            std::thread::sleep(std::time::Duration::from_secs(1));
        };
        runtime.block_on(
            thread_sleep_future
                .wall_time()
                .with_filter(|duration| duration < Duration::from_millis(10))
                .inc_by(metrics.wall_time_cnt.with_label_values(&["thread_sleep_w"]))
                .exec_time()
                .inc_by(metrics.exec_time_cnt.with_label_values(&["thread_sleep_e"])),
        );

        let reports = registry.gather();

        let exec_family = reports
            .iter()
            .find(|m| m.name() == "exec_time_cnt")
            .expect("metric not found");
        let exec_metric = exec_family.get_metric();
        assert_eq!(exec_metric.len(), 1);
        assert_eq!(exec_metric[0].get_label()[0].value(), "thread_sleep_e");

        let exec_counter = exec_metric[0].get_counter();
        // Since we're synchronously sleeping the execution time will be long.
        assert!(exec_counter.get_value() >= 1.0);

        let wall_family = reports
            .iter()
            .find(|m| m.name() == "wall_time_cnt")
            .expect("metric not found");
        let wall_metric = wall_family.get_metric();
        assert_eq!(wall_metric.len(), 1);

        let wall_counter = wall_metric[0].get_counter();
        // We filtered wall time to < 10ms, so our wall time metric should be filtered out.
        assert_eq!(wall_counter.get_value(), 0.0);
    }
}
