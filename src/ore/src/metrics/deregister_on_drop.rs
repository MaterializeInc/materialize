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

//! Labeled metrics that stay registered while any handle for their labels is alive.
//!
//! Each [`DeleteOnDropMetric`](super::DeleteOnDropMetric) obtained from a vector removes its
//! labels when it drops, even while another handle for the same labels, obtained separately, is
//! still recording. The survivor then records into a series that is no longer exported. This
//! happens when every timely worker creates a metric but only one keeps using it, and when a
//! restarted dataflow creates its metrics before the previous incarnation drops its own.
//!
//! [`DeregisterOnDropVec`] counts handles per label set across all calls, so the series is
//! removed only when the last handle for its labels drops.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;
use std::sync::{Arc, Mutex, Weak};

use prometheus::HistogramVec;
use prometheus::core::{Collector, Desc, GenericCounterVec, GenericGaugeVec};
use prometheus::proto::MetricFamily;

use crate::metrics::{MakeCollector, MakeCollectorOpts, MetricVec_};

/// The live registration of each label set, keyed by label values.
type Registrations<V> = Arc<Mutex<BTreeMap<Vec<String>, Weak<Registration<V>>>>>;

/// A metric vector whose handles share one registration per label set.
///
/// Construct it with [`MetricsRegistry::register`](super::MetricsRegistry::register).
#[derive(Clone)]
pub struct DeregisterOnDropVec<V: MetricVec_> {
    inner: V,
    registrations: Registrations<V>,
}

impl<V: MetricVec_ + Debug> Debug for DeregisterOnDropVec<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<V: MetricVec_ + Collector> Collector for DeregisterOnDropVec<V> {
    fn desc(&self) -> Vec<&Desc> {
        self.inner.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.inner.collect()
    }
}

impl<V: MetricVec_ + MakeCollector> MakeCollector for DeregisterOnDropVec<V> {
    fn make_collector(opts: MakeCollectorOpts) -> Self {
        DeregisterOnDropVec {
            inner: V::make_collector(opts),
            registrations: Default::default(),
        }
    }
}

impl<V: MetricVec_ + Clone> DeregisterOnDropVec<V> {
    /// Returns the metric for `labels`, which stays registered until every handle returned for
    /// the same `labels` has dropped.
    ///
    /// Panics if the number of labels does not match the vector's label names.
    pub fn get_deregister_on_drop_metric(&self, labels: Vec<String>) -> DeregisterOnDropMetric<V> {
        let mut registrations = self.registrations.lock().expect("lock poisoned");
        // Fetching while holding the lock keeps a concurrent `Registration::drop` from removing
        // the series between this fetch and the registration below.
        let inner = {
            let values: Vec<&str> = labels.iter().map(String::as_str).collect();
            self.inner
                .get_metric_with_label_values(&values)
                .expect("retrieving a metric by label values")
        };
        let registration = match registrations.get(&labels).and_then(Weak::upgrade) {
            Some(registration) => registration,
            None => {
                let registration = Arc::new(Registration {
                    labels: labels.clone(),
                    vec: self.clone(),
                });
                registrations.insert(labels, Arc::downgrade(&registration));
                registration
            }
        };
        DeregisterOnDropMetric {
            inner,
            _registration: registration,
        }
    }
}

/// Removes a label set from its vector when the last handle for it drops.
struct Registration<V: MetricVec_> {
    labels: Vec<String>,
    vec: DeregisterOnDropVec<V>,
}

impl<V: MetricVec_> Debug for Registration<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("labels", &self.labels)
            .finish_non_exhaustive()
    }
}

impl<V: MetricVec_> Drop for Registration<V> {
    fn drop(&mut self) {
        let mut registrations = self.vec.registrations.lock().expect("lock poisoned");
        // A handle obtained after the last one for these labels dropped, but before this ran,
        // re-registered the labels and still records into the series.
        if registrations
            .get(&self.labels)
            .is_some_and(|live| live.strong_count() > 0)
        {
            return;
        }
        registrations.remove(&self.labels);
        let values: Vec<&str> = self.labels.iter().map(String::as_str).collect();
        // Ignore the error for a series an earlier registration of these labels already removed.
        let _ = self.vec.inner.remove_label_values(&values);
    }
}

/// A labeled metric that stays registered while any handle for its labels is alive.
#[derive(Debug, Clone)]
pub struct DeregisterOnDropMetric<V: MetricVec_> {
    inner: V::M,
    _registration: Arc<Registration<V>>,
}

impl<V: MetricVec_> Deref for DeregisterOnDropMetric<V> {
    type Target = V::M;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// A [`GenericGauge`](prometheus::core::GenericGauge) that stays registered while any handle for
/// its labels is alive.
pub type DeregisterOnDropGauge<P> = DeregisterOnDropMetric<GenericGaugeVec<P>>;

/// A [`GenericCounter`](prometheus::core::GenericCounter) that stays registered while any handle
/// for its labels is alive.
pub type DeregisterOnDropCounter<P> = DeregisterOnDropMetric<GenericCounterVec<P>>;

/// A [`Histogram`](prometheus::Histogram) that stays registered while any handle for its labels
/// is alive.
pub type DeregisterOnDropHistogram = DeregisterOnDropMetric<HistogramVec>;

#[cfg(test)]
mod test {
    use prometheus::IntCounterVec;

    use crate::metric;
    use crate::metrics::MetricsRegistry;

    use super::*;

    fn labels(value: &str) -> Vec<String> {
        vec![value.to_string()]
    }

    #[crate::test]
    fn independent_handles_share_registration() {
        let reg = MetricsRegistry::new();
        let vec: DeregisterOnDropVec<IntCounterVec> = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let first = vec.get_deregister_on_drop_metric(labels("one"));
        let second = vec.get_deregister_on_drop_metric(labels("one"));
        first.inc();
        second.inc();

        drop(first);
        let gathered = reg.gather();
        assert_eq!(
            gathered.len(),
            1,
            "series must survive while a handle is alive"
        );
        assert_eq!(gathered[0].get_metric()[0].get_counter().value(), 2.0);

        second.inc();
        let gathered = reg.gather();
        assert_eq!(gathered[0].get_metric()[0].get_counter().value(), 3.0);

        drop(second);
        assert_eq!(
            reg.gather().len(),
            0,
            "last handle must deregister the series"
        );
        assert!(vec.registrations.lock().expect("lock poisoned").is_empty());
    }

    #[crate::test]
    fn label_sets_are_independent() {
        let reg = MetricsRegistry::new();
        let vec: DeregisterOnDropVec<IntCounterVec> = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let one = vec.get_deregister_on_drop_metric(labels("one"));
        let two = vec.get_deregister_on_drop_metric(labels("two"));
        one.inc();
        two.inc();

        drop(one);
        let gathered = reg.gather();
        let series = gathered[0].get_metric();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].get_label()[0].value(), "two");

        drop(two);
        assert_eq!(reg.gather().len(), 0);
    }

    #[crate::test]
    fn reregistration_after_last_drop_starts_fresh() {
        let reg = MetricsRegistry::new();
        let vec: DeregisterOnDropVec<IntCounterVec> = reg.register(metric!(
            name: "test_metric",
            help: "a test metric",
            var_labels: ["dimension"]));

        let first = vec.get_deregister_on_drop_metric(labels("one"));
        first.inc_by(5);
        drop(first);

        let second = vec.get_deregister_on_drop_metric(labels("one"));
        second.inc();
        let gathered = reg.gather();
        assert_eq!(gathered[0].get_metric()[0].get_counter().value(), 1.0);
    }
}
