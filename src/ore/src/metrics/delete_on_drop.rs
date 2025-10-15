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

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::Deref;

use prometheus::core::{
    Atomic, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec, Metric, MetricVec,
    MetricVecBuilder,
};
use prometheus::{Histogram, HistogramVec};

/// The `prometheus` API uses the `HashMap` type to pass metrics labels, so we have to allow its
/// usage when calling that API.
#[allow(clippy::disallowed_types)]
type PromLabelMap<'a> = std::collections::HashMap<&'a str, &'a str>;

/// A trait that allows being generic over [`MetricVec`]s.
pub trait MetricVec_: Sized {
    /// The associated Metric collected.
    type M: Metric;

    /// See [`MetricVec::get_metric_with_label_values`].
    fn get_metric_with_label_values(&self, vals: &[&str]) -> Result<Self::M, prometheus::Error>;

    /// See [`MetricVec::get_metric_with`].
    fn get_metric_with(&self, labels: &PromLabelMap) -> Result<Self::M, prometheus::Error>;

    /// See [`MetricVec::remove_label_values`].
    fn remove_label_values(&self, vals: &[&str]) -> Result<(), prometheus::Error>;

    /// See [`MetricVec::remove`].
    fn remove(&self, labels: &PromLabelMap) -> Result<(), prometheus::Error>;
}

impl<P: MetricVecBuilder> MetricVec_ for MetricVec<P> {
    type M = P::M;

    fn get_metric_with_label_values(&self, vals: &[&str]) -> prometheus::Result<Self::M> {
        self.get_metric_with_label_values(vals)
    }

    fn get_metric_with(&self, labels: &PromLabelMap) -> Result<Self::M, prometheus::Error> {
        self.get_metric_with(labels)
    }

    fn remove_label_values(&self, vals: &[&str]) -> Result<(), prometheus::Error> {
        self.remove_label_values(vals)
    }

    fn remove(&self, labels: &PromLabelMap) -> Result<(), prometheus::Error> {
        self.remove(labels)
    }
}

/// Extension trait for metrics vectors.
///
/// It adds a method to create a concrete metric from the vector that gets removed from the vector
/// when the concrete metric is dropped.
pub trait MetricVecExt: MetricVec_ {
    /// Returns a metric that deletes its labels from this metrics vector when dropped.
    fn get_delete_on_drop_metric<L: PromLabelsExt>(&self, labels: L)
    -> DeleteOnDropMetric<Self, L>;
}

impl<V: MetricVec_ + Clone> MetricVecExt for V {
    fn get_delete_on_drop_metric<L>(&self, labels: L) -> DeleteOnDropMetric<Self, L>
    where
        L: PromLabelsExt,
    {
        DeleteOnDropMetric::from_metric_vector(self.clone(), labels)
    }
}

/// An extension trait for types that are valid (or convertible into) prometheus labels:
/// slices/vectors of strings, and [`BTreeMap`]s.
pub trait PromLabelsExt {
    /// Returns or creates a metric with the given metric label values.
    /// Panics if retrieving the metric returns an error.
    fn get_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> V::M;

    /// Removes a metric with these labels from a metrics vector.
    fn remove_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> Result<(), prometheus::Error>;
}

impl PromLabelsExt for &[&str] {
    fn get_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> V::M {
        vec.get_metric_with_label_values(self)
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> Result<(), prometheus::Error> {
        vec.remove_label_values(self)
    }
}

impl PromLabelsExt for Vec<String> {
    fn get_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> V::M {
        let labels: Vec<&str> = self.iter().map(String::as_str).collect();
        vec.get_metric_with_label_values(labels.as_slice())
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> Result<(), prometheus::Error> {
        let labels: Vec<&str> = self.iter().map(String::as_str).collect();
        vec.remove_label_values(labels.as_slice())
    }
}

impl PromLabelsExt for Vec<&str> {
    fn get_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> V::M {
        vec.get_metric_with_label_values(self.as_slice())
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> Result<(), prometheus::Error> {
        vec.remove_label_values(self.as_slice())
    }
}

impl PromLabelsExt for BTreeMap<String, String> {
    fn get_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> V::M {
        let labels = self.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        vec.get_metric_with(&labels)
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> Result<(), prometheus::Error> {
        let labels = self.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        vec.remove(&labels)
    }
}

impl PromLabelsExt for BTreeMap<&str, &str> {
    fn get_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> V::M {
        let labels = self.iter().map(|(k, v)| (*k, *v)).collect();
        vec.get_metric_with(&labels)
            .expect("retrieving a metric by label values")
    }

    fn remove_from_metric_vec<V: MetricVec_>(&self, vec: &V) -> Result<(), prometheus::Error> {
        let labels = self.iter().map(|(k, v)| (*k, *v)).collect();
        vec.remove(&labels)
    }
}

/// A [`Metric`] wrapper that deletes its labels from the vec when it is dropped.
///
/// It adds a method to create a concrete metric from the vector that gets removed from the vector
/// when the concrete metric is dropped.
///
/// NOTE: This type implements [`Borrow`], which imposes some constraints on implementers. To
/// ensure these constraints, do *not* implement any of the `Eq`, `Ord`, or `Hash` traits on this.
/// type.
#[derive(Debug, Clone)]
pub struct DeleteOnDropMetric<V, L>
where
    V: MetricVec_,
    L: PromLabelsExt,
{
    inner: V::M,
    labels: L,
    vec: V,
}

impl<V, L> DeleteOnDropMetric<V, L>
where
    V: MetricVec_,
    L: PromLabelsExt,
{
    fn from_metric_vector(vec: V, labels: L) -> Self {
        let inner = labels.get_from_metric_vec(&vec);
        Self { inner, labels, vec }
    }
}

impl<V, L> Deref for DeleteOnDropMetric<V, L>
where
    V: MetricVec_,
    L: PromLabelsExt,
{
    type Target = V::M;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<V, L> Drop for DeleteOnDropMetric<V, L>
where
    V: MetricVec_,
    L: PromLabelsExt,
{
    fn drop(&mut self) {
        if self.labels.remove_from_metric_vec(&self.vec).is_err() {
            // ignore.
        }
    }
}

/// A [`GenericCounter`] wrapper that deletes its labels from the vec when it is dropped.
pub type DeleteOnDropCounter<P, L> = DeleteOnDropMetric<GenericCounterVec<P>, L>;

impl<P, L> Borrow<GenericCounter<P>> for DeleteOnDropCounter<P, L>
where
    P: Atomic,
    L: PromLabelsExt,
{
    fn borrow(&self) -> &GenericCounter<P> {
        &self.inner
    }
}

/// A [`GenericGauge`] wrapper that deletes its labels from the vec when it is dropped.
pub type DeleteOnDropGauge<P, L> = DeleteOnDropMetric<GenericGaugeVec<P>, L>;

impl<P, L> Borrow<GenericGauge<P>> for DeleteOnDropGauge<P, L>
where
    P: Atomic,
    L: PromLabelsExt,
{
    fn borrow(&self) -> &GenericGauge<P> {
        &self.inner
    }
}

/// A [`Histogram`] wrapper that deletes its labels from the vec when it is dropped.
pub type DeleteOnDropHistogram<L> = DeleteOnDropMetric<HistogramVec, L>;

impl<L> Borrow<Histogram> for DeleteOnDropHistogram<L>
where
    L: PromLabelsExt,
{
    fn borrow(&self) -> &Histogram {
        &self.inner
    }
}

#[cfg(test)]
mod test {
    use prometheus::IntGaugeVec;
    use prometheus::core::{AtomicI64, AtomicU64};

    use crate::metric;
    use crate::metrics::{IntCounterVec, MetricsRegistry};

    use super::*;

    #[crate::test]
    fn dropping_counters() {
        let reg = MetricsRegistry::new();
        let vec: IntCounterVec = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let dims: &[&str] = &["one"];
        let metric_1 = vec.get_delete_on_drop_metric(dims);
        metric_1.inc();

        let metrics = reg.gather();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.name(), "test_metric");
        let dims = reported_vec.get_metric();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].get_label()[0].value(), "one");

        drop(metric_1);
        let metrics = reg.gather();
        assert_eq!(metrics.len(), 0);

        let string_labels: Vec<String> = ["owned"].iter().map(ToString::to_string).collect();
        struct Ownership {
            counter: DeleteOnDropCounter<AtomicU64, Vec<String>>,
        }
        let metric_owned = Ownership {
            counter: vec.get_delete_on_drop_metric(string_labels),
        };
        metric_owned.counter.inc();

        let metrics = reg.gather();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.name(), "test_metric");
        let dims = reported_vec.get_metric();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].get_label()[0].value(), "owned");

        drop(metric_owned);
        let metrics = reg.gather();
        assert_eq!(metrics.len(), 0);
    }

    #[crate::test]
    fn dropping_gauges() {
        let reg = MetricsRegistry::new();
        let vec: IntGaugeVec = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let dims: &[&str] = &["one"];
        let metric_1 = vec.get_delete_on_drop_metric(dims);
        metric_1.set(666);

        let metrics = reg.gather();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.name(), "test_metric");
        let dims = reported_vec.get_metric();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].get_label()[0].value(), "one");

        drop(metric_1);
        let metrics = reg.gather();
        assert_eq!(metrics.len(), 0);

        let string_labels: Vec<String> = ["owned"].iter().map(ToString::to_string).collect();
        struct Ownership {
            gauge: DeleteOnDropGauge<AtomicI64, Vec<String>>,
        }
        let metric_owned = Ownership {
            gauge: vec.get_delete_on_drop_metric(string_labels),
        };
        metric_owned.gauge.set(666);

        let metrics = reg.gather();
        assert_eq!(metrics.len(), 1);
        let reported_vec = &metrics[0];
        assert_eq!(reported_vec.name(), "test_metric");
        let dims = reported_vec.get_metric();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].get_label()[0].value(), "owned");

        drop(metric_owned);
        let metrics = reg.gather();
        assert_eq!(metrics.len(), 0);
    }
}
