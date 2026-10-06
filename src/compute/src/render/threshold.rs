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

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::cursor::BatchCursor;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_compute_types::plan::threshold::{BasicThresholdPlan, ThresholdPlan};
use mz_repr::{Diff, Row};
use mz_row_spine::{DatumSeq, RowRowBuilder};
use mz_timely_util::columnation::ColumnationChunker;

use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::extensions::reduce::MzReduce;
use crate::render::RenderTimestamp;
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::typedefs::{ErrBatcher, ErrBuilder, RowRowAgent, RowRowSpine};

/// Thresholds an ok arrangement of any flavor, keeping rows with a positive count.
fn threshold_arrangement<'scope, T, Tr>(
    arrangement: Arranged<'scope, Tr>,
    name: &str,
) -> Arranged<'scope, RowRowAgent<T, Diff>>
where
    T: RenderTimestamp,
    Tr: TraceReader<Time = T, Batch: Navigable> + Clone + 'static,
    // `KeyContainer` is pinned to the output spine's, so that `mz_reduce_abelian` can equate the
    // input and output keys.
    for<'a> BatchCursor<Tr>: Cursor<
            Time = T,
            Diff = Diff,
            KeyContainer = <BatchCursor<RowRowSpine<T, Diff>> as Cursor>::KeyContainer,
            Key<'a> = DatumSeq<'a>,
            Val<'a> = DatumSeq<'a>,
        >,
{
    let logic = move |_key: DatumSeq<'_>, s: &[(DatumSeq<'_>, Diff)], t: &mut Vec<(Row, Diff)>| {
        for (record, count) in s.iter() {
            if count.is_positive() {
                t.push((
                    <BatchCursor<RowRowSpine<T, Diff>> as Cursor>::owned_val(*record),
                    *count,
                ));
            }
        }
    };
    arrangement.mz_reduce_abelian::<_, RowRowBuilder<T, Diff>, RowRowSpine<T, Diff>, _>(name, logic)
}

/// Build a dataflow to threshold the input data.
///
/// This implementation maintains rows in the output, i.e. all rows that have a count greater than
/// zero. It returns a [CollectionBundle] populated from a local arrangement.
pub fn build_threshold_basic<'scope, T: RenderTimestamp>(
    input: CollectionBundle<'scope, T>,
    key: Vec<LirScalarExpr>,
) -> CollectionBundle<'scope, T> {
    let arrangement = input
        .arrangement(&key)
        .expect("Arrangement ensured to exist");
    match arrangement {
        ArrangementFlavor::Local(oks, errs) => {
            let oks = threshold_arrangement(oks, "Threshold local");
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
        ArrangementFlavor::Trace(_, oks, errs) => {
            let oks = threshold_arrangement(oks, "Threshold trace");
            let errs: KeyCollection<_, _, _> = errs.as_collection(|k, _| k.clone()).into();
            let errs = errs
                .mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, _>(
                    "Arrange threshold basic err",
                );
            CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
        }
    }
}

impl<'scope, T: RenderTimestamp> Context<'scope, T> {
    pub(crate) fn render_threshold(
        &self,
        input: CollectionBundle<'scope, T>,
        threshold_plan: ThresholdPlan,
    ) -> CollectionBundle<'scope, T> {
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
