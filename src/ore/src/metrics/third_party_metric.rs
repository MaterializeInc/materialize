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

use prometheus::{
    core::{
        Atomic, Collector, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec,
        MetricVec, MetricVecBuilder,
    },
    Error, Histogram,
};
use std::ops::Deref;

use super::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt, PromLabelsExt};

/// A metric that can be made accessible to cloud infrastructure/orchestrators, that won't betray the
/// contents of the materialize instance that it details.
#[derive(Debug, Clone)]
pub struct ThirdPartyMetric<T: Collector + Clone> {
    pub(super) inner: T,
}

// We allow people to get at non-variably-labeled metrics just like that - the assumption is that
// they are already vetted at commit time & can't leak "variable" information.

macro_rules! impl_deref {
    ($other_type:ty) => {
        impl<T> Deref for ThirdPartyMetric<$other_type>
        where
            T: Atomic + 'static,
        {
            type Target = $other_type;
            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }
    };
}

impl_deref!(GenericCounter<T>);
impl_deref!(GenericGauge<T>);

impl Deref for ThirdPartyMetric<Histogram> {
    type Target = Histogram;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// Metric vectors take more care to not leak information, and so we force programmers to declare
// that the metric they are making will be visible to potential third parties, so this can be
// spotted in code review.

impl<T: MetricVecBuilder> ThirdPartyMetric<MetricVec<T>> {
    /// Creates a metric that can be scraped by a third party, with the given label values.
    pub fn get_third_party_metric_with_label_values(&self, vals: &[&str]) -> Result<T::M, Error> {
        self.inner.get_metric_with_label_values(vals)
    }

    /// Creates a metric that can be scraped by a third party, with the given label values.
    /// # Panics
    /// Panics if the metric can not be created.
    pub fn third_party_metric_with_label_values(&self, vals: &[&str]) -> T::M {
        self.get_third_party_metric_with_label_values(vals)
            .expect("creating a metric from values")
    }
}

impl<P: Atomic> ThirdPartyMetric<GenericCounterVec<P>> {
    /// Creates a delete-on-drop counter that can be scraped by third parties.
    pub fn get_third_party_delete_on_drop_counter<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropCounter<'a, P, L> {
        self.inner.get_delete_on_drop_counter(labels)
    }
}

impl<P: Atomic> ThirdPartyMetric<GenericGaugeVec<P>> {
    /// Creates a delete-on-drop gauge that can be scraped by third parties.
    pub fn get_third_party_delete_on_drop_gauge<'a, L: PromLabelsExt<'a>>(
        &self,
        labels: L,
    ) -> DeleteOnDropGauge<'a, P, L> {
        self.inner.get_delete_on_drop_gauge(labels)
    }
}
