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

//! Scrape-time aggregation of per-object values into metric families whose
//! cardinality does not depend on the number of objects.
//!
//! A subsystem that tracks one value per object (per shard, per replica, ...)
//! would naively export one series per object. Instead, its collector pushes
//! each object's current values into an [`AggregationSnapshot`] once per
//! scrape and emits what [`AggregationSnapshot::finish`] returns. Per
//! [`AggregatedFamily`] that is:
//!
//! * `<name>_percentile`: one gauge per entry of [`PERCENTILES`], labeled
//!   `percentile`.
//! * `<name>_topk`: one gauge for each of the `k` objects with the largest
//!   non-zero value, labeled by [`AggregationKey`]. An object that leaves the
//!   top `k` is simply not emitted on the next scrape.
//!
//! Both are exact over the snapshot. Percentiles of different processes cannot
//! be combined into a percentile over all their objects, so these describe one
//! process's objects.
//!
//! NOTE: Built at runtime, not with [`metric!`](crate::metric), so
//! `bin/gen-metrics-catalog` misses them and `doc/user/data/metrics.yml` has
//! no entry for them.

use std::marker::PhantomData;

use itertools::Itertools;
use prometheus::Opts;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;

use crate::cast::CastLossy;
use crate::metrics::raw::UIntGaugeVec;

/// The `percentile` label values of a `_percentile` family and the
/// percentile each reports, as a fraction of the objects.
///
/// Each is the nearest-rank value: of `n` values sorted ascending, the one at
/// 1-based rank `ceil(q * n)`, and at least rank 1. So `min` and `max` are the
/// smallest and largest values, and every reported value is one some object
/// actually has.
pub const PERCENTILES: &[(&str, f64)] = &[
    ("min", 0.0),
    ("p25", 0.25),
    ("p50", 0.5),
    ("p75", 0.75),
    ("p90", 0.9),
    ("p99", 0.99),
    ("p999", 0.999),
    ("p9999", 0.9999),
    ("max", 1.0),
];

/// Identifies one object in the `_topk` families.
pub trait AggregationKey {
    /// The label names of a `_topk` series.
    const LABEL_NAMES: &'static [&'static str];

    /// The label values of this object, one per [`Self::LABEL_NAMES`] entry.
    /// Only called for objects that make a top-K.
    fn label_values(&self) -> Vec<String>;
}

/// The definition of one aggregated per-object value.
#[derive(Debug, Clone)]
pub struct AggregatedFamily {
    /// The name the per-object metric would have.
    pub name: String,
    /// The help text of the per-object metric.
    pub help: String,
    /// If set, also emit the `k` largest objects as `<name>_topk`.
    pub top_k: Option<usize>,
}

impl AggregatedFamily {
    fn percentile_vec(&self) -> UIntGaugeVec {
        let opts = Opts::new(
            format!("{}_percentile", self.name),
            format!("{} (percentiles over this process's objects)", self.help),
        );
        UIntGaugeVec::new(opts, &["percentile"]).expect("valid percentile family")
    }

    fn top_k_vec<K: AggregationKey>(&self, k: usize) -> UIntGaugeVec {
        let opts = Opts::new(
            format!("{}_topk", self.name),
            format!("{} (the {k} largest objects)", self.help),
        );
        UIntGaugeVec::new(opts, K::LABEL_NAMES).expect("valid top-k family")
    }
}

/// A fixed set of [`AggregatedFamily`]s over objects keyed by `K`.
#[derive(Debug)]
pub struct AggregatedFamilies<K> {
    families: Vec<AggregatedFamily>,
    descs: Vec<Desc>,
    _key: PhantomData<fn(&K)>,
}

impl<K: AggregationKey> AggregatedFamilies<K> {
    /// Panics on an invalid name, like registering an invalid
    /// [`metric!`](crate::metric) does.
    pub fn new(families: Vec<AggregatedFamily>) -> Self {
        let mut descs = Vec::new();
        for f in &families {
            descs.extend(f.percentile_vec().desc().into_iter().cloned());
            if let Some(k) = f.top_k {
                descs.extend(f.top_k_vec::<K>(k).desc().into_iter().cloned());
            }
        }
        AggregatedFamilies {
            families,
            descs,
            _key: PhantomData,
        }
    }

    /// The descriptors of every emitted family, for `Collector::desc`.
    pub fn descs(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    /// Starts an empty snapshot.
    pub fn snapshot(&self) -> AggregationSnapshot<'_, K> {
        AggregationSnapshot {
            families: self,
            keys: Vec::new(),
            columns: vec![Vec::new(); self.families.len()],
        }
    }
}

/// The current values of a set of objects.
///
/// Pushing only copies, so a caller can push under its own lock and call
/// [`Self::finish`], which does the sorting, after releasing it.
#[derive(Debug)]
pub struct AggregationSnapshot<'a, K> {
    families: &'a AggregatedFamilies<K>,
    keys: Vec<K>,
    /// Per family, `(value, index into keys)`.
    columns: Vec<Vec<(u64, usize)>>,
}

impl<K: AggregationKey> AggregationSnapshot<'_, K> {
    /// Records one object, with one value per family in definition order.
    pub fn push(&mut self, key: K, values: &[u64]) {
        for (column, value) in self.columns.iter_mut().zip_eq(values) {
            column.push((*value, self.keys.len()));
        }
        self.keys.push(key);
    }

    /// Folds the snapshot into the aggregated metric families.
    pub fn finish(self) -> Vec<MetricFamily> {
        let mut out = Vec::new();
        for (f, mut column) in self.families.families.iter().zip_eq(self.columns) {
            // Ascending by value. The index breaks ties, so the top-K is
            // deterministic.
            column.sort_unstable();

            // Built fresh every scrape, so no series outlives the snapshot.
            let percentiles = f.percentile_vec();
            if !column.is_empty() {
                let n = f64::cast_lossy(column.len());
                for (label, q) in PERCENTILES {
                    let rank = usize::cast_lossy((q * n).ceil());
                    let (value, _) = column[rank.max(1) - 1];
                    percentiles.with_label_values(&[*label]).set(value);
                }
            }
            out.extend(percentiles.collect());

            if let Some(k) = f.top_k {
                let top_k = f.top_k_vec::<K>(k);
                // An object with nothing to report is not a heavy hitter.
                for &(value, idx) in column.iter().rev().take_while(|(v, _)| *v > 0).take(k) {
                    top_k
                        .with_label_values(&self.keys[idx].label_values())
                        .set(value);
                }
                out.extend(top_k.collect());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    struct Obj(u64);

    impl AggregationKey for Obj {
        const LABEL_NAMES: &'static [&'static str] = &["obj"];
        fn label_values(&self) -> Vec<String> {
            vec![self.0.to_string()]
        }
    }

    /// Aggregates one family `m` with top-10 over objects with the given
    /// values, keyed by the value itself.
    fn aggregate(values: impl IntoIterator<Item = u64>) -> Vec<MetricFamily> {
        let families = AggregatedFamilies::<Obj>::new(vec![AggregatedFamily {
            name: "m".into(),
            help: "help".into(),
            top_k: Some(10),
        }]);
        let mut snapshot = families.snapshot();
        for v in values {
            snapshot.push(Obj(v), &[v]);
        }
        snapshot.finish()
    }

    /// The series of family `name`, as label value to value.
    fn series(families: &[MetricFamily], name: &str) -> BTreeMap<String, u64> {
        let family = families.iter().find(|f| f.name() == name).expect("family");
        family
            .get_metric()
            .iter()
            .map(|m| {
                (
                    m.get_label()[0].value().to_string(),
                    u64::cast_lossy(m.get_gauge().value()),
                )
            })
            .collect()
    }

    fn percentiles(values: [u64; 9]) -> BTreeMap<String, u64> {
        PERCENTILES
            .iter()
            .map(|(l, _)| l.to_string())
            .zip_eq(values)
            .collect()
    }

    fn objs(values: impl IntoIterator<Item = u64>) -> BTreeMap<String, u64> {
        values.into_iter().map(|v| (v.to_string(), v)).collect()
    }

    #[crate::test]
    fn percentiles_are_exact() {
        // Pushed in reverse to check that the values are sorted.
        assert_eq!(
            series(&aggregate((1..=10_000).rev()), "m_percentile"),
            percentiles([1, 2500, 5000, 7500, 9000, 9900, 9990, 9999, 10_000])
        );
        // Nearest rank rounds up: with 100 values, p99.9 and p99.99 are the max.
        assert_eq!(
            series(&aggregate(1..=100), "m_percentile"),
            percentiles([1, 25, 50, 75, 90, 99, 100, 100, 100])
        );
        assert_eq!(series(&aggregate([7]), "m_percentile"), percentiles([7; 9]));
        assert!(series(&aggregate([]), "m_percentile").is_empty());
    }

    #[crate::test]
    fn topk_is_the_largest_non_zero_values() {
        assert_eq!(series(&aggregate(1..=100), "m_topk"), objs(91..=100));
        assert_eq!(series(&aggregate([0, 7, 0]), "m_topk"), objs([7]));
        // Membership is recomputed each time, so a departed object is gone.
        assert_eq!(series(&aggregate(1..=5), "m_topk"), objs(1..=5));
    }

    #[crate::test]
    fn cardinality_is_flat_in_object_count() {
        let series_count =
            |n| -> usize { aggregate(1..=n).iter().map(|f| f.get_metric().len()).sum() };
        assert_eq!(series_count(10), PERCENTILES.len() + 10);
        assert_eq!(series_count(10), series_count(10_000));
    }
}
