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
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::{Batch, Builder, Trace, TraceReader};
use differential_dataflow::Data;
use mz_compute_types::plan::threshold::{BasicThresholdPlan, ThresholdPlan};
use mz_expr::MirScalarExpr;
use mz_repr::Diff;
use timely::container::columnation::Columnation;
use timely::container::PushInto;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::Container;

use crate::extensions::arrange::{ArrangementSize, KeyCollection, MzArrange};
use crate::extensions::reduce::MzReduce;
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::row_spine::RowRowBuilder;
use crate::typedefs::{ErrBatcher, ErrBuilder};

/// Shared function to compute an arrangement of values matching `logic`.
fn threshold_arrangement<G, K, V, T1, Bu2, T2, L>(
    arrangement: &Arranged<G, T1>,
    name: &str,
    logic: L,
) -> Arranged<G, TraceAgent<T2>>
where
    G: Scope,
    G::Timestamp: Lattice + Columnation,
    V: Data + Columnation,
    T1: TraceReader<Time = G::Timestamp, Diff = Diff> + Clone + 'static,
    for<'a> T1::Key<'a>: IntoOwned<'a, Owned = K>,
    for<'a> T1::Val<'a>: IntoOwned<'a, Owned = V>,
    K: Columnation + Data,
    Bu2: Builder<Time = G::Timestamp, Output = T2::Batch>,
    Bu2::Input: Container + PushInto<((K, V), G::Timestamp, Diff)>,
    T2: for<'a> Trace<
            Key<'a> = T1::Key<'a>,
            Val<'a> = T1::Val<'a>,
            Time = G::Timestamp,
            Diff = Diff,
        > + 'static,
    T2::Batch: Batch,
    L: Fn(&Diff) -> bool + 'static,
    Arranged<G, TraceAgent<T2>>: ArrangementSize,
{
    arrangement.mz_reduce_abelian::<_, _, _, Bu2, T2>(name, move |_key, s, t| {
        for (record, count) in s.iter() {
            if logic(count) {
                t.push(((*record).into_owned(), *count));
            }
        }
    })
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
    G::Timestamp: Lattice + Refines<T> + Columnation,
    T: Timestamp + Lattice + Columnation,
{
    let arrangement = input
        .arrangement(&key)
        .expect("Arrangement ensured to exist");
    match arrangement {
        ArrangementFlavor::Local(oks, errs) => {
            let oks = threshold_arrangement::<_, _, _, _, RowRowBuilder<_, _>, _, _>(
                &oks,
                "Threshold local",
                |count| *count > 0,
            );
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
        ArrangementFlavor::Trace(_, oks, errs) => {
            let oks = threshold_arrangement::<_, _, _, _, RowRowBuilder<_, _>, _, _>(
                &oks,
                "Threshold trace",
                |count| *count > 0,
            );
            let errs: KeyCollection<_, _, _> = errs.as_collection(|k, _| k.clone()).into();
            let errs = errs
                .mz_arrange::<ErrBatcher<_, _>, ErrBuilder<_, _>, _>("Arrange threshold basic err");
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
    }
}

impl<G, T> Context<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T> + Columnation,
    T: Timestamp + Lattice + Columnation,
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
