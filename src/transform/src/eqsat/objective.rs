// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pluggable extraction objectives.
//!
//! An [`Objective`] orders two candidate plan costs so the extractor can pick
//! the cheaper one. The e-graph machinery and the [`crate::eqsat::cost`] model
//! are independent of which objective is chosen: the cost model computes every
//! axis, and the objective decides which axis dominates.

use crate::eqsat::cost::Cost;

/// Orders candidate plan costs for extraction. `Less` means preferred.
pub trait Objective: std::fmt::Debug {
    /// Order two candidate costs. `Less` means the first plan is preferred.
    fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering;
    /// Stable name for diagnostics and A/B selection.
    fn name(&self) -> &'static str;
}

/// The legacy objective: minimize worst-case peak arrangement degree, then
/// time, then node count. Retained for regression comparison against the new
/// arrangement-count objective.
#[derive(Debug)]
pub struct PeakDegree;

impl Objective for PeakDegree {
    fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering {
        a.cmp_memory_first(b)
    }
    fn name(&self) -> &'static str {
        "peak-degree"
    }
}

/// Time-first objective: minimize worst-case time, then peak degree, then
/// node count. The optimizer extracts a time-first alternate to surface a
/// faster-but-heavier recommendation.
#[derive(Debug)]
pub struct TimeFirst;

impl Objective for TimeFirst {
    fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering {
        a.cmp_time_first(b)
    }
    fn name(&self) -> &'static str {
        "time-first"
    }
}

/// The default objective: minimize the count of distinct maintained
/// arrangements (reuse-aware), then worst-case time, then node count.
/// Arrangement count drives steady-state memory in a differential-dataflow
/// system, so minimizing it directly targets the resource that matters.
#[derive(Debug)]
pub struct ArrangementCount;

impl Objective for ArrangementCount {
    fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering {
        a.arrangements
            .cmp(&b.arrangements)
            .then_with(|| super::cost::cmp_vecs(&a.time, &b.time))
            .then_with(|| a.nodes.cmp(&b.nodes))
    }
    fn name(&self) -> &'static str {
        "arrangement-count"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn peak_degree_matches_cmp_memory_first() {
        use crate::eqsat::cost::Cost;
        let a = Cost {
            arrangements: 0,
            memory: vec![2.0],
            time: vec![2.0, 1.5],
            nodes: 3,
        };
        let b = Cost {
            arrangements: 0,
            memory: vec![1.0, 1.0, 1.0],
            time: vec![1.5],
            nodes: 5,
        };
        let obj = PeakDegree;
        assert_eq!(obj.cmp(&a, &b), a.cmp_memory_first(&b));
        assert_eq!(obj.name(), "peak-degree");
    }

    #[mz_ore::test]
    fn arrangement_count_prefers_fewer_arrangements_over_lower_degree() {
        use crate::eqsat::cost::Cost;
        // `a` has a high peak degree but one arrangement; `b` has low degrees but
        // three arrangements. PeakDegree prefers `b`; ArrangementCount prefers `a`.
        let a = Cost {
            arrangements: 1,
            memory: vec![2.0],
            time: vec![2.0],
            nodes: 3,
        };
        let b = Cost {
            arrangements: 3,
            memory: vec![1.0, 1.0, 1.0],
            time: vec![1.5],
            nodes: 5,
        };
        assert_eq!(ArrangementCount.cmp(&a, &b), std::cmp::Ordering::Less);
        assert_eq!(PeakDegree.cmp(&a, &b), std::cmp::Ordering::Greater);
        assert_eq!(ArrangementCount.name(), "arrangement-count");
    }
}
