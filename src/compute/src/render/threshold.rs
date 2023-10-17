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
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::Data;
use mz_compute_types::plan::threshold::{BasicThresholdPlan, ThresholdPlan};
use mz_expr::MirScalarExpr;
use mz_repr::Diff;
use timely::container::columnation::Columnation;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::extensions::reduce::MzReduce;
use crate::render::context::{
    ArrangementFlavor, CollectionBundle, Context, SpecializedArrangement,
    SpecializedArrangementImport,
};
use crate::typedefs::TraceRowHandle;

/// Shared function to compute an arrangement of values matching `logic`.
fn threshold_arrangement<G, K, V, T, R, L>(
    arrangement: &R,
    name: &str,
    logic: L,
) -> Arranged<G, TraceRowHandle<K, V, G::Timestamp, Diff>>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
    K: Columnation + Data,
    V: Columnation + Data,
    R: MzReduce<G, K, V, Diff>,
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

/// Dispatches according to existing type-specialization to an appropriate threshold computation
/// resulting in another type-specialized arrangement.
fn dispatch_threshold_arrangement_local<G, L>(
    oks: &SpecializedArrangement<G>,
    name: &str,
    logic: L,
) -> SpecializedArrangement<G>
where
    G: Scope,
    G::Timestamp: Lattice,
    L: Fn(&Diff) -> bool + 'static,
{
    match oks {
        SpecializedArrangement::Bytes9Row(key_types, inner) => {
            let oks = threshold_arrangement(inner, name, logic);
            SpecializedArrangement::Bytes9Row(key_types.clone(), oks)
        }
        SpecializedArrangement::RowRow(inner) => {
            let oks = threshold_arrangement(inner, name, logic);
            SpecializedArrangement::RowRow(oks)
        }
    }
}

/// Dispatches threshold computation for a trace, similarly to `dispatch_threshold_arrangement_local`.
fn dispatch_threshold_arrangement_trace<G, T, L>(
    oks: &SpecializedArrangementImport<G, T>,
    name: &str,
    logic: L,
) -> SpecializedArrangement<G>
where
    G: Scope,
    T: Timestamp + Lattice,
    G::Timestamp: Lattice + Refines<T>,
    L: Fn(&Diff) -> bool + 'static,
{
    match oks {
        SpecializedArrangementImport::Bytes9Row(key_types, inner) => {
            let oks = threshold_arrangement(inner, name, logic);
            SpecializedArrangement::Bytes9Row(key_types.clone(), oks)
        }
        SpecializedArrangementImport::RowRow(inner) => {
            let oks = threshold_arrangement(inner, name, logic);
            SpecializedArrangement::RowRow(oks)
        }
    }
}

/// Build a dataflow to threshold the input data.
///
/// This implementation maintains rows in the output, i.e. all rows that have a count greater than
/// zero. It returns a [CollectionBundle] populated from a local arrangement.
pub fn build_threshold_basic<G, T>(
    input: CollectionBundle<G, T>,
    key: Vec<MirScalarExpr>,
) -> CollectionBundle<G, T>
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
            let oks =
                dispatch_threshold_arrangement_local(&oks, "Threshold local", |count| *count > 0);
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
        ArrangementFlavor::Trace(_, oks, errs) => {
            let oks =
                dispatch_threshold_arrangement_trace(&oks, "Threshold trace", |count| *count > 0);
            let errs: KeyCollection<_, _, _> = errs.as_collection(|k, _| k.clone()).into();
            let errs = errs.mz_arrange("Arrange threshold basic err");
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
    }
}

impl<G, T> Context<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub(crate) fn render_threshold(
        &self,
        input: CollectionBundle<G, T>,
        threshold_plan: ThresholdPlan,
    ) -> CollectionBundle<G, T> {
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
