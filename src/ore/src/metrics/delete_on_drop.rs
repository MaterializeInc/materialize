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

//! Support for metrics that get removed from their corresponding metrics vector when dropped.
//!
//! # Ownership & life times
//!
//! This kind of data type is realized by a struct that retains ownership of the _labels_ used to
//! create the spin-off metric. The created metric follows these rules:
//! * When passing references, the metric must not outlive the references to the labels used to create
//!   it: A `'static` slice of static strings means the metric is allowed to live for the `'static`
//!   lifetime as well.
//! * Metrics created from references to dynamically constructed labels can only live as long as those
//!   labels do.
//! * When using owned data (an extension over what Prometheus allows, which only lets you use
//!   references to refer to labels), the created metric is also allowed to live for `'static`.

use std::{collections::HashMap, marker::PhantomData, ops::Deref};

use prometheus::core::{
    Atomic, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec, MetricVec,
    MetricVecBuilder,
};
use prometheus::{Histogram, HistogramVec};

/// An extension trait for types that are valid (or convertible into) prometheus labels:
/// slices/vectors of strings, and [`HashMap`]s.
pub trait PromLabelsExt<'a> {
    /// Returns or creates a metric with the given metric label values.
    /// Panics if retrieving the metric returns an error.
    fn get_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> <P as MetricVecBuilder>::M;

    /// Removes a metric with these labels from a metrics vector.
    fn remove_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> Result<(), prometheus::Error>;
}

impl<'a> PromLabelsExt<'a> for &'a [&'a str] {
    fn get_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> <P as MetricVecBuilder>::M {
        vec.get_metric_with_label_values(self)
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> Result<(), prometheus::Error> {
        vec.remove_label_values(self)
    }
}

impl PromLabelsExt<'static> for Vec<String> {
    fn get_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> <P as MetricVecBuilder>::M {
        let labels: Vec<&str> = self.iter().map(String::as_str).collect();
        vec.get_metric_with_label_values(labels.as_slice())
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> Result<(), prometheus::Error> {
        let labels: Vec<&str> = self.iter().map(String::as_str).collect();
        vec.remove_label_values(labels.as_slice())
    }
}

impl<'a> PromLabelsExt<'a> for Vec<&'a str> {
    fn get_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> <P as MetricVecBuilder>::M {
        vec.get_metric_with_label_values(self.as_slice())
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> Result<(), prometheus::Error> {
        vec.remove_label_values(self.as_slice())
    }
}

impl PromLabelsExt<'static> for HashMap<String, String> {
    fn get_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> <P as MetricVecBuilder>::M {
        let labels: HashMap<&str, &str> =
            self.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        vec.get_metric_with(&labels)
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> Result<(), prometheus::Error> {
        let labels: HashMap<&str, &str> =
            self.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        vec.remove(&labels)
    }
}

impl<'a> PromLabelsExt<'a> for HashMap<&'a str, &'a str> {
    fn get_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> <P as MetricVecBuilder>::M {
        vec.get_metric_with(self)
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<P: MetricVecBuilder>(
        &self,
        vec: &MetricVec<P>,
    ) -> Result<(), prometheus::Error> {
        vec.remove(&self)
    }
}

/// A [`GenericCounter`] wrapper that deletes its labels from the vec when it is dropped
///
/// It adds a method to create a concrete metric from the vector that gets removed from the vector
/// when the concrete metric is dropped.
#[derive(Debug)]
pub struct DeleteOnDropHistogram<'a, L>
where
    L: PromLabelsExt<'a>,
{
    inner: Histogram,
    labels: L,
    vec: HistogramVec,
    _phantom: &'a PhantomData<()>,
}

impl<'a, L> DeleteOnDropHistogram<'a, L>
where
    L: PromLabelsExt<'a>,
{
    fn from_metric_vector(vec: HistogramVec, labels: L) -> Self {
        let inner = labels.get_from_metric_vec(&vec);
        Self {
            inner,
            labels,
            vec,
            _phantom: &PhantomData,
        }
    }
}

impl<'a, L> Deref for DeleteOnDropHistogram<'a, L>
where
    L: PromLabelsExt<'a>,
{
    type Target = Histogram;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, L> Drop for DeleteOnDropHistogram<'a, L>
where
    L: PromLabelsExt<'a>,
{
    fn drop(&mut self) {
        if self.labels.remove_from_metric_vec(&self.vec).is_err() {
            // ignore.
        }
    }
}

/// A [`GenericCounter`] wrapper that deletes its labels from the vec when it is dropped
///
/// It adds a method to create a concrete metric from the vector that gets removed from the vector
/// when the concrete metric is dropped.
#[derive(Debug)]
pub struct DeleteOnDropCounter<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    inner: GenericCounter<P>,
    labels: L,
    vec: GenericCounterVec<P>,
    _phantom: &'a PhantomData<()>,
}

impl<'a, P, L> DeleteOnDropCounter<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    fn from_metric_vector(vec: GenericCounterVec<P>, labels: L) -> Self {
        let inner = labels.get_from_metric_vec(&vec);
        Self {
            inner,
            labels,
            vec,
            _phantom: &PhantomData,
        }
    }
}

impl<'a, P, L> Deref for DeleteOnDropCounter<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    type Target = GenericCounter<P>;
    fn deref(&self) -> &GenericCounter<P> {
        &self.inner
    }
}

impl<'a, P, L> Drop for DeleteOnDropCounter<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    fn drop(&mut self) {
        if self.labels.remove_from_metric_vec(&self.vec).is_err() {
            // ignore.
        }
    }
}

/// Extension trait for all gauge metrics vectors.
///
/// It adds a method to create a concrete metric from the vector that gets removed from the vector
/// when the concrete metric is dropped.
pub trait CounterVecExt {
    /// The type of value that the counter should count.
    type CounterType: Atomic;

    /// Returns a counter that deletes its labels from this metrics vector when dropped.
    /// See [`DeleteOnDropCounter`] for a detailed description.
    fn get_delete_on_drop_counter<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropCounter<'a, Self::CounterType, L>;
}

impl<P: Atomic> CounterVecExt for GenericCounterVec<P> {
    type CounterType = P;

    fn get_delete_on_drop_counter<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropCounter<'a, Self::CounterType, L> {
        DeleteOnDropCounter::from_metric_vector(self.clone(), labels)
    }
}

/// Extension trait for all gauge metrics vectors.
///
/// It adds a method to create a concrete metric from the vector that gets removed from the vector
/// when the concrete metric is dropped.
pub trait HistogramVecExt {
    /// Returns a counter that deletes its labels from this metrics vector when dropped.
    /// See [`DeleteOnDropCounter`] for a detailed description.
    fn get_delete_on_drop_histogram<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropHistogram<'a, L>;
}

impl HistogramVecExt for HistogramVec {
    fn get_delete_on_drop_histogram<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropHistogram<'a, L> {
        DeleteOnDropHistogram::from_metric_vector(self.clone(), labels)
    }
}

/// A [`GenericGauge`] wrapper that deletes its labels from the vec when it is dropped
#[derive(Debug)]
pub struct DeleteOnDropGauge<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    inner: GenericGauge<P>,
    labels: L,
    vec: GenericGaugeVec<P>,
    _phantom: &'a PhantomData<()>,
}

impl<'a, P, L> DeleteOnDropGauge<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    fn from_metric_vector(vec: GenericGaugeVec<P>, labels: L) -> Self {
        let inner = labels.get_from_metric_vec(&vec);
        Self {
            inner,
            labels,
            vec,
            _phantom: &PhantomData,
        }
    }
}

impl<'a, P, L> Deref for DeleteOnDropGauge<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    type Target = GenericGauge<P>;
    fn deref(&self) -> &GenericGauge<P> {
        &self.inner
    }
}

impl<'a, P, L> Drop for DeleteOnDropGauge<'a, P, L>
where
    P: Atomic,
    L: PromLabelsExt<'a>,
{
    fn drop(&mut self) {
        if self.labels.remove_from_metric_vec(&self.vec).is_err() {
            // ignore.
        }
    }
}

/// Extension trait for all metrics vectors.
pub trait GaugeVecExt {
    /// The type of value that the gauge should count.
    type GaugeType: Atomic;

    /// Returns a gauge that deletes its labels from this metrics vector when dropped.
    /// See [`DeleteOnDropGauge`] for a detailed description.
    fn get_delete_on_drop_gauge<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropGauge<'a, Self::GaugeType, L>;
}

impl<P: Atomic> GaugeVecExt for GenericGaugeVec<P> {
    type GaugeType = P;

    fn get_delete_on_drop_gauge<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropGauge<'a, Self::GaugeType, L> {
        DeleteOnDropGauge::from_metric_vector(self.clone(), labels)
    }
}

#[cfg(test)]
mod test {
    use prometheus::core::AtomicI64;
    use prometheus::IntGaugeVec;

    use super::super::{IntCounterVec, MetricsRegistry};
    use super::*;
    use crate::metric;

    #[test]
    fn dropping_counters() {
        let reg = MetricsRegistry::new();
        let vec: IntCounterVec = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let dims: &[&str] = &["one"];
        let metric_1 = vec.get_delete_on_drop_counter(dims);
        metric_1.inc();

        let metrics = reg.gather();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.get_name(), "test_metric");
        let dims = reported_vec.get_metric();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].get_label()[0].get_value(), "one");

        drop(metric_1);
        let metrics = reg.gather();
        assert_eq!(metrics.len(), 0);

        let string_labels: Vec<String> = ["owned"].iter().map(ToString::to_string).collect();
        struct Ownership {
            counter: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
        }
        let metric_owned = Ownership {
            counter: vec.get_delete_on_drop_counter(string_labels),
        };
        metric_owned.counter.inc();

        let metrics = reg.gather();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.get_name(), "test_metric");
        let dims = reported_vec.get_metric();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].get_label()[0].get_value(), "owned");

        drop(metric_owned);
        let metrics = reg.gather();
        assert_eq!(metrics.len(), 0);
    }

    #[test]
    fn dropping_gauges() {
        let reg = MetricsRegistry::new();
        let vec: IntGaugeVec = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let dims: &[&str] = &["one"];
        let metric_1 = vec.get_delete_on_drop_gauge(dims);
        metric_1.set(666);

        let metrics = reg.gather();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.get_name(), "test_metric");
        let dims = reported_vec.get_metric();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].get_label()[0].get_value(), "one");

        drop(metric_1);
        let metrics = reg.gather();
        assert_eq!(metrics.len(), 0);

        let string_labels: Vec<String> = ["owned"].iter().map(ToString::to_string).collect();
        struct Ownership {
            gauge: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
        }
        let metric_owned = Ownership {
            gauge: vec.get_delete_on_drop_gauge(string_labels),
        };
        metric_owned.gauge.set(666);

        let metrics = reg.gather();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.get_name(), "test_metric");
        let dims = reported_vec.get_metric();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].get_label()[0].get_value(), "owned");

        drop(metric_owned);
        let metrics = reg.gather();
        assert_eq!(metrics.len(), 0);
    }
}
