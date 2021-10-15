// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Threshold planning and execution logic.

//! The threshold operator produces only rows with a positive cardinality, for example required to
//! provide SQL except and intersect semantics.
//!
//! We build a plan ([ThresholdPlan]) encapsulating all decisions and requirements on the specific
//! threshold implementation. The idea is to decouple the logic deciding which plan to select from
//! the actual implementation of each variant available.
//!
//! Currently, we provide two variants:
//! * The [BasicThresholdPlan] maintains all its outputs as an arrangement. It is beneficial if the
//!     threshold is the final operation, or a downstream operators expects arranged inputs.
//! * The [RetractionsThresholdPlan] maintains retractions, i.e. rows that are not in the output. It
//!     is beneficial to use this operator if the number of retractions is expected to be small, and
//!     if a potential downstream operator does not expect its input to be arranged.
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::Consolidate;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use repr::{Diff, Row};
use serde::{Deserialize, Serialize};

use crate::arrangement::manager::RowSpine;
use crate::render::context::CollectionBundle;
use crate::render::context::{ArrangementFlavor, Context};
use crate::render::datum_vec::DatumVec;
use crate::render::Permutation;

/// A plan describing how to compute a threshold operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ThresholdPlan {
    /// Basic threshold maintains all positive inputs.
    Basic(BasicThresholdPlan),
    /// Retractions threshold maintains all negative inputs.
    Retractions(RetractionsThresholdPlan),
}

impl ThresholdPlan {
    /// Reports all keys of produced arrangements.
    ///
    /// This is likely either an empty vector, for no arrangement,
    /// or a singleton vector containing the list of expressions
    /// that key a single arrangement.
    pub fn keys(&self) -> Vec<Vec<expr::MirScalarExpr>> {
        // Accumulate keys into this vector, and return it.
        let mut keys = Vec::new();
        match self {
            ThresholdPlan::Basic(plan) => {
                keys.push(
                    (0..plan.arity)
                        .map(|column| expr::MirScalarExpr::Column(column))
                        .collect::<Vec<_>>(),
                );
            }
            ThresholdPlan::Retractions(_plan) => {}
        }
        keys
    }
}

/// A plan to maintain all inputs with positive counts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasicThresholdPlan {
    /// The number of columns in the input and output.
    arity: usize,
}

/// A plan to maintain all inputs with negative counts, which are subtracted from the output
/// in order to maintain an equivalent collection compared to [BasicThresholdPlan].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetractionsThresholdPlan {
    /// The number of columns in the input and output.
    arity: usize,
}

impl ThresholdPlan {
    /// Construct the plan from the number of columns (`arity`). `maintain_retractions` allows to
    /// switch between an implementation that maintains rows with negative counts (`true`), or
    /// rows with positive counts (`false`).
    pub fn create_from(arity: usize, maintain_retractions: bool) -> Self {
        if maintain_retractions {
            ThresholdPlan::Retractions(RetractionsThresholdPlan { arity })
        } else {
            ThresholdPlan::Basic(BasicThresholdPlan { arity })
        }
    }
}

/// Shared function to compute an arrangement of values matching `logic`.
fn threshold_arrangement<G, T, R, L>(
    arrangement: &R,
    name: &str,
    logic: L,
    permutation: Permutation,
) -> Arranged<G, TraceAgent<RowSpine<Row, Row, G::Timestamp, Diff>>>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
    R: ReduceCore<G, Row, Row, Diff>,
    L: Fn(&Diff) -> bool + 'static,
{
    let mut datum_vec = DatumVec::new();
    arrangement.reduce_abelian(name, move |key, s, t| {
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
    arity: usize,
) -> CollectionBundle<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    // Arrange the input by all columns in order.
    let mut all_columns = Vec::new();
    for column in 0..arity {
        all_columns.push(expr::MirScalarExpr::Column(column));
    }
    let input = input.ensure_arrangements(Some(all_columns.clone()));
    let arrangement = input
        .arrangement(&all_columns)
        .expect("Arrangement ensured to exist");
    match arrangement.flavor {
        ArrangementFlavor::Local(oks, errs) => {
            let oks = threshold_arrangement(
                &oks,
                "Threshold local",
                |count| *count > 0,
                arrangement.permutation,
            );
            CollectionBundle::from_columns(0..arity, ArrangementFlavor::Local(oks, errs), arity)
        }
        ArrangementFlavor::Trace(_, oks, errs) => {
            let oks = threshold_arrangement(
                &oks,
                "Threshold trace",
                |count| *count > 0,
                arrangement.permutation,
            );
            use differential_dataflow::operators::arrange::ArrangeBySelf;
            let errs = errs.as_collection(|k, _| k.clone()).arrange_by_self();
            CollectionBundle::from_columns(0..arity, ArrangementFlavor::Local(oks, errs), arity)
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
    arity: usize,
) -> CollectionBundle<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    // Arrange the input by all columns in order.
    let mut all_columns = Vec::new();
    for column in 0..arity {
        all_columns.push(expr::MirScalarExpr::Column(column));
    }
    let input = input.ensure_arrangements(Some(all_columns.clone()));
    let arrangement = input
        .arrangement(&all_columns)
        .expect("Arrangement ensured to exist");
    let negatives = match &arrangement.flavor {
        ArrangementFlavor::Local(oks, _) => threshold_arrangement(
            oks,
            "Threshold retractions local",
            |count| *count < 0,
            arrangement.permutation.clone(),
        ),
        ArrangementFlavor::Trace(_, oks, _) => threshold_arrangement(
            oks,
            "Threshold retractions trace",
            |count| *count < 0,
            arrangement.permutation.clone(),
        ),
    };
    let (oks, errs) = arrangement.as_collection();
    let oks = negatives
        .as_collection(|k, _| k.clone())
        .negate()
        .concat(&oks)
        .consolidate();
    CollectionBundle::from_collections(oks, errs, arity)
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
            ThresholdPlan::Basic(BasicThresholdPlan { arity }) => {
                build_threshold_basic(input, arity)
            }
            ThresholdPlan::Retractions(RetractionsThresholdPlan { arity }) => {
                build_threshold_retractions(input, arity)
            }
        }
    }
}
