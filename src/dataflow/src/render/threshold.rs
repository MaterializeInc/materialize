// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use repr::{Diff, Row};

use crate::render::context::CollectionBundle;
use crate::render::context::{ArrangementFlavor, Context};

/// A plan describing how to compute a threshold operation.
#[derive(Debug)]
pub enum ThresholdPlan {
    /// Basic threshold maintains all positive inputs.
    Basic(BasicThresholdPlan),
    /// Retractions threshold maintains all negative inputs.
    Retractions(RetractionsThresholdPlan),
}

/// A plan to maintain all inputs with positive counts.
#[derive(Debug)]
pub struct BasicThresholdPlan {
    /// The number of columns in the input and output.
    arity: usize,
}

/// A plan to maintain all inputs with negative counts, which are subtracted from the output
/// in order to maintain an equivalent collection compared to [BasicThresholdPlan].
#[derive(Debug)]
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
) -> Arranged<G, TraceAgent<OrdValSpine<Row, Row, G::Timestamp, Diff>>>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
    R: ReduceCore<G, Row, Row, Diff>,
    L: Fn(&isize) -> bool + 'static,
{
    arrangement.reduce_abelian(name, move |_k, s, t| {
        for (record, count) in s.iter() {
            if logic(count) {
                t.push(((*record).clone(), *count));
            }
        }
    })
}

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
    // Different trace variants require different implementations because their
    // types are different, but the logic is identical.
    let mut all_columns = Vec::new();
    for column in 0..arity {
        all_columns.push(expr::MirScalarExpr::Column(column));
    }
    let input = input.ensure_arrangements(Some(all_columns.clone()));
    match input
        .arrangement(&all_columns)
        .expect("Arrangement ensured to exist")
    {
        ArrangementFlavor::Local(oks, errs) => {
            let oks = threshold_arrangement(&oks, "Threshold local", |count| *count > 0);
            CollectionBundle::from_columns(0..arity, ArrangementFlavor::Local(oks, errs))
        }
        ArrangementFlavor::Trace(_, oks, errs) => {
            let oks = threshold_arrangement(&oks, "Threshold trace", |count| *count > 0);
            use differential_dataflow::operators::arrange::ArrangeBySelf;
            let errs = errs.as_collection(|k, _| k.clone()).arrange_by_self();
            CollectionBundle::from_columns(0..arity, ArrangementFlavor::Local(oks, errs))
        }
    }
}

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
    // Different trace variants require different implementations because their
    // types are different, but the logic is identical.
    let mut all_columns = Vec::new();
    for column in 0..arity {
        all_columns.push(expr::MirScalarExpr::Column(column));
    }
    let input = input.ensure_arrangements(Some(all_columns.clone()));
    let arrangement = input
        .arrangement(&all_columns)
        .expect("Arrangement ensured to exist");
    let negatives = match &arrangement {
        ArrangementFlavor::Local(oks, _) => {
            threshold_arrangement(oks, "Threshold retractions local", |count| *count < 0)
        }
        ArrangementFlavor::Trace(_, oks, _) => {
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
            ThresholdPlan::Basic(BasicThresholdPlan { arity }) => {
                build_threshold_basic(input, arity)
            }
            ThresholdPlan::Retractions(RetractionsThresholdPlan { arity }) => {
                build_threshold_retractions(input, arity)
            }
        }
    }
}
