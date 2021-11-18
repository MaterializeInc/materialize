// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Threshold execution logic.
//!
//! Consult [ThresholdPlan] documentation for details.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::Consolidate;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use repr::{Diff, Row};

use crate::arrangement::manager::RowSpine;
use crate::render::context::CollectionBundle;
use crate::render::context::{ArrangementFlavor, Context};
use dataflow_types::plan::threshold::{
    BasicThresholdPlan, RetractionsThresholdPlan, ThresholdPlan,
};
use dataflow_types::plan::EnsureArrangement;

/// Shared function to compute an arrangement of values matching `logic`.
fn threshold_arrangement<G, T, R, L>(
    arrangement: &R,
    name: &str,
    logic: L,
) -> Arranged<G, TraceAgent<RowSpine<Row, Row, G::Timestamp, Diff>>>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
    R: ReduceCore<G, Row, Row, Diff>,
    L: Fn(&Diff) -> bool + 'static,
{
    arrangement.reduce_abelian(name, move |_key, s, t| {
        for (record, count) in s.iter() {
            if logic(count) {
                t.push(((*record).clone(), *count));
            }
        }
    })
}

/// Build a dataflow to threshold the input data.
///
/// This implementation maintains rows in the output, i.e. all rows that have a count greater than
/// zero. It returns a [CollectionBundle] populated from a local arrangement.
pub fn build_threshold_basic<G, T>(
    input: CollectionBundle<G, Row, T>,
    arrangement: EnsureArrangement,
) -> CollectionBundle<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    let all_columns = arrangement.0.clone();
    let input = input.ensure_arrangements(Some(arrangement));
    let arrangement = input
        .arrangement(&all_columns)
        .expect("Arrangement ensured to exist");
    match arrangement {
        ArrangementFlavor::Local(oks, errs, permutation) => {
            let oks = threshold_arrangement(&oks, "Threshold local", |count| *count > 0);
            CollectionBundle::from_expressions(
                all_columns,
                ArrangementFlavor::Local(oks, errs, permutation),
            )
        }
        ArrangementFlavor::Trace(_, oks, errs, permutation) => {
            let oks = threshold_arrangement(&oks, "Threshold trace", |count| *count > 0);
            use differential_dataflow::operators::arrange::ArrangeBySelf;
            let errs = errs.as_collection(|k, _| k.clone()).arrange_by_self();
            CollectionBundle::from_expressions(
                all_columns,
                ArrangementFlavor::Local(oks, errs, permutation),
            )
        }
    }
}

/// Build a dataflow to threshold the input data while maintaining retractions.
///
/// This implementation maintains rows that are not part of the output, i.e. all rows that have a
/// count of less than zero. It returns a [CollectionBundle] populated from the output collection,
/// which itself is not an arrangement.
pub fn build_threshold_retractions<G, T>(
    input: CollectionBundle<G, Row, T>,
    arrangement: EnsureArrangement,
) -> CollectionBundle<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    let all_columns = arrangement.0.clone();
    let input = input.ensure_arrangements(Some(arrangement));
    let arrangement = input
        .arrangement(&all_columns)
        .expect("Arrangement ensured to exist");
    let negatives = match &arrangement {
        ArrangementFlavor::Local(oks, _, _) => {
            threshold_arrangement(oks, "Threshold retractions local", |count| *count < 0)
        }
        ArrangementFlavor::Trace(_, oks, _, _) => {
            threshold_arrangement(oks, "Threshold retractions trace", |count| *count < 0)
        }
    };
    let (oks, errs) = arrangement.as_collection();
    let oks = negatives
        .as_collection(|k, _| k.clone())
        .negate()
        .concat(&oks)
        .consolidate();
    CollectionBundle::from_collections(oks, errs)
}

impl<G, T> Context<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_threshold(
        &self,
        input: CollectionBundle<G, Row, T>,
        threshold_plan: ThresholdPlan,
    ) -> CollectionBundle<G, Row, T> {
        match threshold_plan {
            ThresholdPlan::Basic(BasicThresholdPlan { ensure_arrangement }) => {
                build_threshold_basic(input, ensure_arrangement)
            }
            ThresholdPlan::Retractions(RetractionsThresholdPlan { ensure_arrangement }) => {
                build_threshold_retractions(input, ensure_arrangement)
            }
        }
    }
}
