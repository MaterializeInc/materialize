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
use mz_compute_client::plan::threshold::{BasicThresholdPlan, ThresholdPlan};
use mz_expr::MirScalarExpr;
use mz_repr::{Diff, Row};
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::extensions::reduce::MzReduce;
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::typedefs::RowSpine;

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
    R: MzReduce<G, Row, Row, Diff>,
    L: Fn(&Diff) -> bool + 'static,
{
    arrangement.mz_reduce_abelian(name, move |_key, s, t| {
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
    key: Vec<MirScalarExpr>,
) -> CollectionBundle<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    let arrangement = input
        .arrangement(&key)
        .expect("Arrangement ensured to exist");
    match arrangement {
        ArrangementFlavor::Local(oks, errs) => {
            let oks = threshold_arrangement(&oks, "Threshold local", |count| *count > 0);
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
        ArrangementFlavor::Trace(_, oks, errs) => {
            let oks = threshold_arrangement(&oks, "Threshold trace", |count| *count > 0);
            let errs: KeyCollection<_, _, _> = errs.as_collection(|k, _| k.clone()).into();
            let errs = errs.mz_arrange("Arrange threshold basic err");
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
    }
}

impl<G, T> Context<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub(crate) fn render_threshold(
        &self,
        input: CollectionBundle<G, Row, T>,
        threshold_plan: ThresholdPlan,
    ) -> CollectionBundle<G, Row, T> {
        match threshold_plan {
            ThresholdPlan::Basic(BasicThresholdPlan {
                ensure_arrangement: (key, _, _),
            }) => {
                // We do not need to apply the permutation here,
                // since threshold doesn't inspect the values, but only
                // their counts.
                build_threshold_basic(input, key)
            }
        }
    }
}
