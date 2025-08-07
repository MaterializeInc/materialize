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

use differential_dataflow::Data;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::{Builder, Trace, TraceReader};
use mz_compute_types::plan::threshold::{BasicThresholdPlan, ThresholdPlan};
use mz_expr::MirScalarExpr;
use mz_repr::Diff;
use timely::Container;
use timely::container::PushInto;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;

use crate::extensions::arrange::{ArrangementSize, KeyCollection, MzArrange};
use crate::extensions::reduce::MzReduce;
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::row_spine::RowRowBuilder;
use crate::typedefs::{ErrBatcher, ErrBuilder, MzData, MzTimestamp};

/// Shared function to compute an arrangement of values matching `logic`.
fn threshold_arrangement<G, T1, Bu2, T2, L>(
    arrangement: &Arranged<G, T1>,
    name: &str,
    logic: L,
) -> Arranged<G, TraceAgent<T2>>
where
    G: Scope,
    G::Timestamp: MzTimestamp,
    T1: TraceReader<KeyOwn: MzData + Data, ValOwn: MzData + Data, Time = G::Timestamp, Diff = Diff>
        + Clone
        + 'static,
    Bu2: Builder<
            Time = G::Timestamp,
            Input: Container + PushInto<((T1::KeyOwn, T1::ValOwn), G::Timestamp, Diff)>,
            Output = T2::Batch,
        >,
    T2: for<'a> Trace<
            Key<'a> = T1::Key<'a>,
            Val<'a> = T1::Val<'a>,
            KeyOwn = T1::KeyOwn,
            ValOwn = T1::ValOwn,
            Time = G::Timestamp,
            Diff = Diff,
        > + 'static,
    L: Fn(&Diff) -> bool + 'static,
    Arranged<G, TraceAgent<T2>>: ArrangementSize,
{
    arrangement.mz_reduce_abelian::<_, Bu2, T2>(name, move |_key, s, t| {
        for (record, count) in s.iter() {
            if logic(count) {
                t.push((T1::owned_val(*record), *count));
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
    G::Timestamp: MzTimestamp + Refines<T>,
    T: MzTimestamp,
{
    let arrangement = input
        .arrangement(&key)
        .expect("Arrangement ensured to exist");
    match arrangement {
        ArrangementFlavor::Local(oks, errs) => {
            let oks = threshold_arrangement::<_, _, RowRowBuilder<_, _>, _, _>(
                &oks,
                "Threshold local",
                |count| count.is_positive(),
            );
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
        ArrangementFlavor::Trace(_, oks, errs) => {
            let oks = threshold_arrangement::<_, _, RowRowBuilder<_, _>, _, _>(
                &oks,
                "Threshold trace",
                |count| count.is_positive(),
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
    G::Timestamp: MzTimestamp + Refines<T>,
    T: MzTimestamp,
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
