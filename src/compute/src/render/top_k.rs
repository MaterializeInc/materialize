// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TopK execution logic.
//!
//! Consult [TopKPlan] documentation for details.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use columnar::{Columnar, Index};
use differential_dataflow::AsCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::iterate::Variable as SemigroupVariable;
use differential_dataflow::trace::cursor::{BatchCursor, BatchValOwn};
use differential_dataflow::trace::{Builder, Cursor, Navigable, Trace};
use differential_dataflow::{Data, VecCollection};
use mz_compute_types::dyncfgs::{ENABLE_COMPUTE_TEMPORAL_BUCKETING, TEMPORAL_BUCKETING_SUMMARY};
use mz_compute_types::plan::ArrangementStrategy;
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_compute_types::plan::top_k::{
    BasicTopKPlan, MonotonicTop1Plan, MonotonicTopKPlan, TopKPlan,
};
use mz_expr::func::CastUint64ToInt64;
use mz_expr::{BinaryFunc, Columns, Eval, EvalError, UnaryFunc, func, permutation_for_arrangement};
use mz_ore::cast::CastFrom;
use mz_ore::soft_assert_or_log;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{Datum, DatumVec, Diff, ReprScalarType, Row, SharedRow};
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnation::ColumnationChunker;
use mz_timely_util::operator::CollectionExt;
use timely::Container;
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use crate::extensions::arrange::{ArrangementSize, KeyCollection, MzArrange};
use crate::extensions::reduce::{ClearContainer, MzReduce};
use crate::render::Pairer;
use crate::render::columnar::{CollectionEdge, columnar_to_vec, vec_to_columnar};
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::render::errors::DataflowErrorSer;
use crate::render::errors::MaybeValidatingRow;
use crate::typedefs::{ErrBatcher, ErrBuilder, KeyBatcher, MzTimestamp, RowRowSpine, RowSpine};
use mz_row_spine::{
    DatumContainer, DatumSeq, RowBatcher, RowBuilder, RowRowBatcher, RowRowBuilder, RowValBuilder,
    RowValSpine,
};

// The implementation requires integer timestamps to be able to delay feedback for monotonic inputs.
impl<'scope, T: crate::render::RenderTimestamp + crate::render::MaybeBucketByTime>
    Context<'scope, T>
{
    pub(crate) fn render_topk(
        &self,
        input: CollectionBundle<'scope, T>,
        top_k_plan: TopKPlan,
        temporal_bucketing_strategy: ArrangementStrategy,
    ) -> CollectionBundle<'scope, T> {
        // Consume the input as a columnar-capable edge rather than decoding it to
        // `Vec` up front. The arrangement key is formed off the edge below via
        // `map_topk_key`, so no `ColumnarToVec` sits on the common input path.
        let (ok_input, err_input) = input
            .collection
            .clone()
            .expect("The unarranged collection doesn't exist.");

        // Bucket the per-row input stream when lowering chose `TemporalBucketing`.
        // `TopK` builds its own arrangement(s) inside the variants below, bypassing
        // `ensure_collections`, so the strategy is plumbed through `LirRelationNode::TopK`
        // rather than inferred at the arrangement site. `apply_bucketing_strategy`
        // is a no-op for `Direct`.
        //
        // Note: a `MonotonicTop1Plan`/`MonotonicTopKPlan` with `must_consolidate =
        // false` together with `TemporalBucketing` here would mean we install a
        // bucket operator with no downstream consolidator -- pure overhead. That
        // combination cannot actually occur: `RelaxMustConsolidate` (which is the
        // only writer of `must_consolidate = false`) runs only on single-time
        // dataflows (one-shot peeks / `COPY TO`), and in single-time dataflows
        // `ExprPrepOneShot` constant-folds `mz_now()` to the dataflow `as_of`
        // before lowering, so no temporal predicates survive into LIR and
        // `has_future_updates` is `false` everywhere -- meaning no operator (TopK
        // included) is ever lowered with `TemporalBucketing`. The assertion below
        // pins down this invariant.
        if matches!(
            temporal_bucketing_strategy,
            ArrangementStrategy::TemporalBucketing
        ) {
            let must_consolidate = match &top_k_plan {
                TopKPlan::MonotonicTop1(p) => p.must_consolidate,
                TopKPlan::MonotonicTopK(p) => p.must_consolidate,
                TopKPlan::Basic(_) => true,
            };
            soft_assert_or_log!(
                must_consolidate,
                "TopK with `TemporalBucketing` should not have `must_consolidate = false`; \
                 `RelaxMustConsolidate` only runs on single-time dataflows where \
                 `mz_now()` has been const-folded and no temporal bucketing is set",
            );
        }
        // Temporal bucketing is `Vec`-internal: it consumes and produces a `Vec`
        // stream. Decode the edge into it, then re-encode the `Vec` result to
        // columnar at the boundary so the bucketed input edge stays columnar. It
        // only fires under `ENABLE_COMPUTE_TEMPORAL_BUCKETING` and the
        // `TemporalBucketing` strategy, both off on the common path, so the
        // columnar edge otherwise flows straight through.
        let ok_input = if matches!(
            temporal_bucketing_strategy,
            ArrangementStrategy::TemporalBucketing
        ) && ENABLE_COMPUTE_TEMPORAL_BUCKETING.get(&self.config_set)
        {
            let summary: mz_repr::Timestamp = TEMPORAL_BUCKETING_SUMMARY
                .get(&self.config_set)
                .try_into()
                .expect("must fit");
            vec_to_columnar(T::maybe_apply_temporal_bucketing(
                columnar_to_vec(ok_input).inner,
                self.as_of_frontier.clone(),
                summary,
            ))
        } else {
            ok_input
        };

        // We create a new region to compartmentalize the topk logic.
        let outer_scope = ok_input.scope();
        let bundle = outer_scope.clone().region_named("TopK", |inner| {
            let ok_input = ok_input.enter_region(inner);
            let mut err_collection = err_input.enter_region(inner);

            // Determine if there should be errors due to limit evaluation; update `err_collection`.
            // TODO(vmarcos): We evaluate the limit expression below for each input update. There
            // is an opportunity to do so for every group key instead if the error handling is
            // integrated with: 1. The intra-timestamp thinning step in monotonic top-k, e.g., by
            // adding an error output there; 2. The validating reduction on basic top-k
            // (database-issues#7108).

            match top_k_plan.limit().map(|l| (l.as_literal(), l)) {
                None => {}
                Some((Some(Ok(literal)), _))
                    if literal == Datum::Null || literal.unwrap_int64() >= 0 => {}
                Some((_, expr)) => {
                    // Produce errors from limit selectors that error or are
                    // negative, and nothing from limit selectors that do
                    // not. Note that even if expr.could_error() is false,
                    // the expression might still return a negative limit and
                    // thus needs to be checked.
                    let expr = expr.clone();
                    let mut datum_vec = mz_repr::DatumVec::new();
                    // A literal, non-negative limit skips this branch entirely, so this
                    // per-row evaluation only runs for column or otherwise fallible
                    // limits. The columnar decode is a narrow sanctioned leaf confined
                    // to this rare path.
                    let errors = columnar_to_vec(ok_input.clone()).flat_map(move |row| {
                        let temp_storage = mz_repr::RowArena::new();
                        let datums = datum_vec.borrow_with(&row);
                        match expr.eval(&datums[..], &temp_storage) {
                            Ok(l) if l != Datum::Null && l.unwrap_int64() < 0 => {
                                Some(EvalError::NegLimit.into())
                            }
                            Ok(_) => None,
                            Err(e) => Some(e.into()),
                        }
                    });
                    err_collection = err_collection.concat(errors);
                }
            }

            let bundle = match top_k_plan {
                TopKPlan::MonotonicTop1(MonotonicTop1Plan {
                    group_key,
                    order_key,
                    arity,
                    must_consolidate,
                }) => {
                    let (arrangement, errs) = self.render_top1_monotonic(
                        ok_input,
                        group_key.clone(),
                        order_key,
                        arity,
                        must_consolidate,
                    );
                    err_collection = err_collection.concat(errs);

                    // Lowering advertises this group-key arrangement (see the
                    // `MirRelationExpr::TopK` arm in `lowering.rs`), so deliver it alone,
                    // mirroring `render_reduce_plan`'s `ArrangementFlavor::Local`. A consumer
                    // that needs the raw collection reconstructs it from the arrangement via
                    // the advertised permutation, exactly as for an index arrangement.
                    let errs: KeyCollection<_, _, _> = err_collection.clone().into();
                    let err_arrangement = errs
                        .mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, _>(
                            "Arrange bundle err",
                        );
                    CollectionBundle::from_columns(
                        group_key.iter().copied(),
                        ArrangementFlavor::Local(arrangement, err_arrangement),
                    )
                }
                TopKPlan::MonotonicTopK(MonotonicTopKPlan {
                    order_key,
                    group_key,
                    arity,
                    mut limit,
                    must_consolidate,
                }) => {
                    // Must permute `limit` to reference `group_key` elements as if in order.
                    if let Some(expr) = limit.as_mut() {
                        let mut map = BTreeMap::new();
                        for (index, column) in group_key.iter().enumerate() {
                            map.insert(*column, index);
                        }
                        expr.permute_map(&map);
                    }

                    // Map the group key along with the row and consolidate if required to do so.
                    let ok_scope = ok_input.scope();
                    let collection =
                        map_topk_key(ok_input, "MonotonicTopK input", move |datums, _row| {
                            SharedRow::pack(group_key.iter().map(|i| datums[*i]))
                        })
                        .consolidate_named_if::<KeyBatcher<_, _, _>>(
                            must_consolidate,
                            "Consolidated MonotonicTopK input",
                        );

                    // It should be now possible to ensure that we have a monotonic collection.
                    let error_logger = self.error_logger();
                    let (collection, errs) = collection.ensure_monotonic(move |data, diff| {
                        error_logger.log(
                            "Non-monotonic input to MonotonicTopK",
                            &format!("data={data:?}, diff={diff}"),
                        );
                        let m = "tried to build monotonic top-k on non-monotonic input".into();
                        (DataflowErrorSer::from(EvalError::Internal(m)), Diff::ONE)
                    });
                    err_collection = err_collection.concat(errs);

                    // For monotonic inputs, we are able to thin the input relation in two stages:
                    // 1. First, we can do an intra-timestamp thinning which has the advantage of
                    //    being computed in a streaming fashion, even for the initial snapshot.
                    // 2. Then, we can do inter-timestamp thinning by feeding back negations for
                    //    any records that have been invalidated.
                    let collection = if let Some(limit) = limit.clone() {
                        render_intra_ts_thinning(collection, order_key.clone(), limit)
                    } else {
                        collection
                    };

                    let pairer = Pairer::new(1);
                    let collection = collection.map(move |(group_row, row)| {
                        let hash = row.hashed();
                        let hash_key = pairer.merge(std::iter::once(Datum::from(hash)), &group_row);
                        (hash_key, row)
                    });

                    // For monotonic inputs, we are able to retract inputs that can no longer be produced
                    // as outputs. Any inputs beyond `offset + limit` will never again be produced as
                    // outputs, and can be removed. The simplest form of this is when `offset == 0` and
                    // these removable records are those in the input not produced in the output.
                    // TODO: consider broadening this optimization to `offset > 0` by first filtering
                    // down to `offset = 0` and `limit = offset + limit`, followed by a finishing act
                    // of `offset` and `limit`, discarding only the records not produced in the intermediate
                    // stage.
                    let delay = std::time::Duration::from_secs(10);
                    let (retractions_var, retractions) = SemigroupVariable::new(
                        ok_scope,
                        <T as crate::render::RenderTimestamp>::system_delay(
                            delay.try_into().expect("must fit"),
                        ),
                    );
                    let thinned = collection.clone().concat(retractions.negate());

                    // As an additional optimization, we can skip creating the full topk hierachy
                    // here since we now have an upper bound on the number records due to the
                    // intra-ts thinning. The maximum number of records per timestamp is
                    // (num_workers * limit), which we expect to be a small number and so we render
                    // a single topk stage.
                    let (result, errs) =
                        self.build_topk_stage(thinned, order_key, 1u64, 0, limit, arity, false);
                    // Consolidate the output of `build_topk_stage` because it's not guaranteed to be.
                    let result = CollectionExt::consolidate_named::<KeyBatcher<_, _, _>>(
                        result,
                        "Monotonic TopK final consolidate",
                    );
                    retractions_var.set(collection.concat(result.clone().negate()));
                    soft_assert_or_log!(
                        errs.is_none(),
                        "requested no validation, but received error collection"
                    );

                    CollectionBundle::from_edge(topk_result_to_columnar(result), err_collection)
                }
                TopKPlan::Basic(BasicTopKPlan {
                    group_key,
                    order_key,
                    offset,
                    mut limit,
                    arity,
                    buckets,
                }) => {
                    // Must permute `limit` to reference `group_key` elements as if in order.
                    if let Some(expr) = limit.as_mut() {
                        let mut map = BTreeMap::new();
                        for (index, column) in group_key.iter().enumerate() {
                            map.insert(*column, index);
                        }
                        expr.permute_map(&map);
                    }

                    let (oks, errs) = self.build_topk(
                        ok_input, group_key, order_key, offset, limit, arity, buckets,
                    );
                    err_collection = err_collection.concat(errs);
                    CollectionBundle::from_edge(oks, err_collection)
                }
            };

            // Extract the results from the region.
            bundle.leave_region(outer_scope)
        });

        bundle
    }

    /// Constructs a TopK dataflow subgraph.
    fn build_topk<'s>(
        &self,
        collection: CollectionEdge<'s, T>,
        group_key: Vec<usize>,
        order_key: Vec<mz_expr::ColumnOrder>,
        offset: usize,
        limit: Option<LirScalarExpr>,
        arity: usize,
        buckets: Vec<u64>,
    ) -> (
        CollectionEdge<'s, T>,
        VecCollection<'s, T, DataflowErrorSer, Diff>,
    ) {
        let pairer = Pairer::new(1);
        let mut collection = map_topk_key(collection, "TopK input", move |datums, row| {
            let row_hash = row.hashed();
            let iterator = group_key.iter().map(|i| datums[*i]);
            pairer.merge(std::iter::once(Datum::from(row_hash)), iterator)
        });

        let mut validating = true;
        let mut err_collection: Option<VecCollection<'s, T, _, _>> = None;

        if let Some(mut limit) = limit.clone() {
            // We may need a new `limit` that reflects the addition of `offset`.
            // Ideally we compile it down to a literal if at all possible.
            if offset > 0 {
                let new_limit = (|| {
                    let limit = limit.as_literal_int64()?;
                    let offset = i64::try_from(offset).ok()?;
                    limit.checked_add(offset)
                })();

                if let Some(new_limit) = new_limit {
                    limit =
                        LirScalarExpr::literal_ok(Datum::Int64(new_limit), ReprScalarType::Int64);
                } else {
                    limit = limit.call_binary(
                        LirScalarExpr::literal_ok(
                            Datum::UInt64(u64::cast_from(offset)),
                            ReprScalarType::UInt64,
                        )
                        .call_unary(UnaryFunc::CastUint64ToInt64(CastUint64ToInt64)),
                        BinaryFunc::AddInt64(func::AddInt64),
                    );
                }
            }

            // These bucket values define the shifts that happen to the 64 bit hash of the
            // record, and should have the properties that 1. there are not too many of them,
            // and 2. each has a modest difference to the next.
            for bucket in buckets.into_iter() {
                // here we do not apply `offset`, but instead restrict ourself with a limit
                // that includes the offset. We cannot apply `offset` until we perform the
                // final, complete reduction.
                let (oks, errs) = self.build_topk_stage(
                    collection,
                    order_key.clone(),
                    bucket,
                    0,
                    Some(limit.clone()),
                    arity,
                    validating,
                );
                collection = oks;
                if validating {
                    err_collection = errs;
                    validating = false;
                }
            }
        }

        // We do a final step, both to make sure that we complete the reduction, and to correctly
        // apply `offset` to the final group, as we have not yet been applying it to the partially
        // formed groups.
        let (oks, errs) = self.build_topk_stage(
            collection, order_key, 1u64, offset, limit, arity, validating,
        );
        // Consolidate the output of `build_topk_stage` because it's not guaranteed to be.
        let oks =
            CollectionExt::consolidate_named::<KeyBatcher<_, _, _>>(oks, "TopK final consolidate");
        collection = oks;
        if validating {
            err_collection = errs;
        }
        (
            topk_result_to_columnar(collection),
            err_collection.expect("at least one stage validated its inputs"),
        )
    }

    /// To provide a robust incremental orderby-limit experience, we want to avoid grouping *all*
    /// records (or even large groups) and then applying the ordering and limit. Instead, a more
    /// robust approach forms groups of bounded size and applies the offset and limit to each,
    /// and then increases the sizes of the groups.
    ///
    /// Builds a "stage", which uses a finer grouping than is required to reduce the volume of
    /// updates, and to reduce the amount of work on the critical path for updates. The cost is
    /// a larger number of arrangements when this optimization does nothing beneficial.
    ///
    /// The function accepts a collection of the form `(hash_key, row)`, a modulus it applies to the
    /// `hash_key`'s hash datum, an `offset` for returning results, and a `limit` to restrict the
    /// output size. `arity` represents the number of columns in the input data, and
    /// if `validating` is true, we check for negative multiplicities, which indicate
    /// an error in the input data.
    ///
    /// The output of this function is _not consolidated_.
    ///
    /// The dataflow fragment has the following shape:
    /// ```text
    ///     | input
    ///     |
    ///   arrange
    ///     |\
    ///     | \
    ///     |  reduce
    ///     |  |
    ///     concat
    ///     |
    ///     | output
    /// ```
    /// There are additional map/flat_map operators as well as error demuxing operators, but we're
    /// omitting them here for the sake of simplicity.
    fn build_topk_stage<'s>(
        &self,
        collection: VecCollection<'s, T, (Row, Row), Diff>,
        order_key: Vec<mz_expr::ColumnOrder>,
        modulus: u64,
        offset: usize,
        limit: Option<LirScalarExpr>,
        arity: usize,
        validating: bool,
    ) -> (
        VecCollection<'s, T, (Row, Row), Diff>,
        Option<VecCollection<'s, T, DataflowErrorSer, Diff>>,
    ) {
        // Form appropriate input by updating the `hash` column (first datum in `hash_key`) by
        // applying `modulus`.
        let input = collection.map(move |(hash_key, row)| {
            let mut hash_key_iter = hash_key.iter();
            let hash = hash_key_iter.next().unwrap().unwrap_uint64() % modulus;
            let hash_key = SharedRow::pack(std::iter::once(hash.into()).chain(hash_key_iter));
            (hash_key, row)
        });

        // If validating: demux errors, otherwise we cannot produce errors.
        let (input, oks, errs) = if validating {
            // Build topk stage, produce errors for invalid multiplicities.
            let (input, stage) = build_topk_negated_stage::<
                T,
                RowValBuilder<_, _, _>,
                RowValSpine<Result<Row, Row>, _, _>,
            >(&input, order_key, offset, limit, arity);
            let stage = stage.as_collection(|k, v| (k.to_row(), v.clone()));

            // Demux oks and errors.
            let error_logger = self.error_logger();
            type CB<C> = CapacityContainerBuilder<C>;
            let (oks, errs) = stage.map_fallible::<CB<_>, CB<_>, _, _, _>(
                "Demuxing Errors",
                move |(hk, result)| match result {
                    Err(v) => {
                        let mut hk_iter = hk.iter();
                        let h = hk_iter.next().unwrap().unwrap_uint64();
                        let k = SharedRow::pack(hk_iter);
                        let message = "Negative multiplicities in TopK";
                        error_logger.log(message, &format!("k={k:?}, h={h}, v={v:?}"));
                        Err(EvalError::Internal(message.into()).into())
                    }
                    Ok(t) => Ok((hk, t)),
                },
            );
            (input, oks, Some(errs))
        } else {
            // Build non-validating topk stage.
            let (input, stage) =
                build_topk_negated_stage::<T, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                    &input, order_key, offset, limit, arity,
                );
            // Turn arrangement into collection.
            let stage = stage.as_collection(|k, v| (k.to_row(), v.to_row()));

            (input, stage, None)
        };
        let input = input.as_collection(|k, v| (k.to_row(), v.to_row()));
        (oks.concat(input), errs)
    }

    fn render_top1_monotonic<'s>(
        &self,
        collection: CollectionEdge<'s, T>,
        group_key: Vec<usize>,
        order_key: Vec<mz_expr::ColumnOrder>,
        arity: usize,
        must_consolidate: bool,
    ) -> (
        Arranged<'s, TraceAgent<RowRowSpine<T, Diff>>>,
        VecCollection<'s, T, DataflowErrorSer, Diff>,
    ) {
        // The arrangement we build below is keyed by `group_key` and its value is the winning
        // row thinned to `thinning`, following the layout `permutation_for_arrangement`
        // dictates for `Reduce`-style group-key arrangements. A top-1 winner's group-key
        // columns equal the key by construction, so dropping them from the value is lossless;
        // consumers reconstruct the full row from key and value via the (unused here)
        // permutation.
        let key: Vec<LirScalarExpr> = group_key
            .iter()
            .map(|c| LirScalarExpr::column(*c))
            .collect();
        let (_permutation, thinning) = permutation_for_arrangement(&key, arity);

        // We can place our rows directly into the diff field, and only keep the relevant one
        // corresponding to evaluating our aggregate, instead of having to do a hierarchical
        // reduction. We start by mapping the group key along with the row and consolidating
        // if required to do so.
        let collection = map_topk_key(collection, "MonotonicTop1 input", move |datums, _row| {
            SharedRow::pack(group_key.iter().map(|i| datums[*i]))
        })
        .consolidate_named_if::<KeyBatcher<_, _, _>>(
            must_consolidate,
            "Consolidated MonotonicTop1 input",
        );

        // It should be now possible to ensure that we have a monotonic collection and process it.
        let error_logger = self.error_logger();
        let (partial, errs) = collection.ensure_monotonic(move |data, diff| {
            error_logger.log(
                "Non-monotonic input to MonotonicTop1",
                &format!("data={data:?}, diff={diff}"),
            );
            let m = "tried to build monotonic top-1 on non-monotonic input".into();
            (EvalError::Internal(m).into(), Diff::ONE)
        });
        let partial: KeyCollection<_, _, _> = partial
            .explode_one(move |(group_key, row)| {
                (
                    group_key,
                    monoids::Top1Monoid {
                        row,
                        order_key: order_key.clone(),
                    },
                )
            })
            .into();
        let result = partial
            .mz_arrange::<
                ColumnationChunker<_>,
                RowBatcher<_, _>,
                RowBuilder<_, _>,
                RowSpine<_, _>,
            >(
                "Arranged MonotonicTop1 partial [val: empty]",
            )
            .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>, _>(
                "MonotonicTop1",
                {
                    let mut datum_vec = mz_repr::DatumVec::new();
                    move |_key, input, output| {
                        let accum: &monoids::Top1Monoid = &input[0].1;
                        let datums = datum_vec.borrow_with(&accum.row);
                        let value = SharedRow::pack(thinning.iter().map(|i| datums[*i]));
                        output.push((value, Diff::ONE));
                    }
                },
            );
        (result, errs)
    }
}

/// Forms the `(key, value)` arrangement input the TopK stages consume, reading
/// directly from a columnar input edge.
///
/// `key` receives the borrowed datums of an input row and the owned row, and
/// returns the arrangement key. The value is always the full input row, because
/// every TopK stage carries the row through to its output.
///
/// The output is a `VecCollection<(Row, Row)>` because the TopK downstream
/// (`KeyBatcher`, `MzReduce`) is `Vec`-based, so the value `Row` of every record
/// is decoded inline via `Columnar::into_owned` (the same per-record cost as
/// `columnar_to_vec`'s body). This removes the separate `ColumnarToVec` decode
/// operator and its intermediate `Vec<(Row, T, Diff)>` container, but does not
/// avoid the per-record decode, because there is no columnar batcher to push
/// borrowed rows into here. The key is formed from the borrowed datums. Time and
/// diff are owned per record.
fn map_topk_key<'s, T, L>(
    edge: CollectionEdge<'s, T>,
    name: &str,
    mut key: L,
) -> VecCollection<'s, T, (Row, Row), Diff>
where
    T: crate::render::RenderTimestamp,
    L: FnMut(&[Datum], &Row) -> Row + 'static,
{
    let mut builder = OperatorBuilder::new(name.to_string(), edge.inner.scope());
    let (output, stream) = builder.new_output();
    let mut output =
        OutputBuilder::<_, CapacityContainerBuilder<Vec<((Row, Row), T, Diff)>>>::from(output);
    let mut input = builder.new_input(edge.inner, Pipeline);
    builder.build(move |_capabilities| {
        let mut datum_vec = mz_repr::DatumVec::new();
        move |_frontiers| {
            let mut output = output.activate();
            input.for_each(|time, data| {
                let mut session = output.session_with_builder(&time);
                // The value row is decoded to an owned `Row` here because the
                // output is `Vec`-based. This is the same per-record cost as
                // `columnar_to_vec`, just without the separate decode operator
                // and its intermediate container. The key is formed from the
                // borrowed datums. Time and diff are owned per record.
                for (row, t, d) in data.borrow().into_index_iter() {
                    let value_row: Row = Columnar::into_owned(row);
                    let key_row = {
                        let datums = datum_vec.borrow_with(&value_row);
                        key(&datums, &value_row)
                    };
                    session.give((
                        (key_row, value_row),
                        Columnar::into_owned(t),
                        Columnar::into_owned(d),
                    ));
                }
            });
        }
    });
    stream.as_collection()
}

/// Drops the hash-key pairing from a consolidated `(hash_key, row)` TopK result,
/// producing the columnar output edge.
///
/// The input is consolidated upstream and the hash key is a function of the row,
/// so distinct `(hash_key, row)` entries have distinct rows. Dropping the key is
/// therefore injective and the output carries no within-batch duplicates, so a
/// non-consolidating `ColumnBuilder` matches the prior `map`. The row is pushed
/// borrowed, materializing no owned `Row` per record.
fn topk_result_to_columnar<'s, T>(
    collection: VecCollection<'s, T, (Row, Row), Diff>,
) -> CollectionEdge<'s, T>
where
    T: crate::render::RenderTimestamp,
{
    let stream = collection
        .inner
        .unary::<ColumnBuilder<(Row, T, Diff)>, _, _, _>(Pipeline, "TopKUnkey", |_cap, _info| {
            move |input, output| {
                input.for_each(|time, data| {
                    let mut session = output.session_with_builder(&time);
                    for ((_key_hash, row), t, d) in data.drain(..) {
                        session.give((&row, &t, &d));
                    }
                });
            }
        });
    stream.as_collection()
}

/// Build a stage of a topk reduction. Maintains the _retractions_ of the output instead of emitted
/// rows. This has the benefit that we have to maintain state proportionally to size of the output
/// instead of the size of the input.
///
/// Returns two arrangements:
/// * The arranged input data without modifications, and
/// * the maintained negated output data.
fn build_topk_negated_stage<'s, T, Bu, Tr>(
    input: &VecCollection<'s, T, (Row, Row), Diff>,
    order_key: Vec<mz_expr::ColumnOrder>,
    offset: usize,
    limit: Option<LirScalarExpr>,
    arity: usize,
) -> (
    Arranged<'s, TraceAgent<RowRowSpine<T, Diff>>>,
    Arranged<'s, TraceAgent<Tr>>,
)
where
    T: MzTimestamp,
    Bu: Builder<
            Time = T,
            Input: Container + ClearContainer + PushInto<((Row, BatchValOwn<Tr>), T, Diff)>,
            Output = Tr::Batch,
        > + 'static,
    Tr: Trace<Batch: Navigable, Time = T> + 'static,
    for<'a> BatchCursor<Tr>: Cursor<
            Key<'a> = DatumSeq<'a>,
            KeyContainer = DatumContainer,
            ValOwn: Data + MaybeValidatingRow<Row, Row>,
            Time = T,
            Diff = Diff,
        >,
    Arranged<'s, TraceAgent<Tr>>: ArrangementSize,
{
    let mut datum_vec = mz_repr::DatumVec::new();

    // We only want to arrange parts of the input that are not part of the actual output
    // such that `input.concat(&negated_output)` yields the correct TopK
    // NOTE(vmarcos): The arranged input operator name below is used in the tuning advice
    // built-in view mz_introspection.mz_expected_group_size_advice.
    let arranged = input
        .clone()
        .mz_arrange::<
            ColumnationChunker<_>,
            RowRowBatcher<_, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >(
            "Arranged TopK input",
        );

    // Eagerly evaluate literal limits.
    let limit = limit.map(|l| match l.as_literal() {
        Some(Ok(Datum::Null)) => Ok(Diff::MAX),
        Some(Ok(d)) => Ok(Diff::from(d.unwrap_int64())),
        _ => Err(l),
    });

    let reduced = arranged
        .clone()
        .mz_reduce_abelian::<_, Bu, Tr, _>("Reduced TopK input", {
            move |hash_key, source, target: &mut Vec<(BatchValOwn<Tr>, Diff)>| {
                // Unpack the limit, either into an integer literal or an expression to evaluate.
                let limit = match &limit {
                    Some(Ok(lit)) => Some(*lit),
                    Some(Err(expr)) => {
                        // Unpack `key` after skipping the hash and determine the limit.
                        // If the limit errors, use a zero limit; errors are surfaced elsewhere.
                        let temp_storage = mz_repr::RowArena::new();
                        let mut key_datums = datum_vec.borrow();
                        hash_key.extend_datums(&temp_storage, &mut key_datums, None);
                        // `key_datums[0]` is the hash; the key columns follow it.
                        let datum_limit = expr
                            .eval(&key_datums[1..], &temp_storage)
                            .unwrap_or(Datum::Int64(0));
                        Some(match datum_limit {
                            Datum::Null => Diff::MAX,
                            d => Diff::from(d.unwrap_int64()),
                        })
                    }
                    None => None,
                };

                if let Some(err) = BatchValOwn::<Tr>::into_error() {
                    for (datums, diff) in source.iter() {
                        if diff.is_positive() {
                            continue;
                        }
                        target.push((err((*datums).to_row()), Diff::ONE));
                        return;
                    }
                }

                // Determine if we must actually shrink the result set.
                let must_shrink = offset > 0
                    || limit
                        .map(|l| source.iter().map(|(_, d)| *d).sum::<Diff>() > l)
                        .unwrap_or(false);
                if !must_shrink {
                    return;
                }

                // First go ahead and emit all records. Note that we ensure target
                // has the capacity to hold at least these records, and avoid any
                // dependencies on the user-provided (potentially unbounded) limit.
                target.reserve(source.len());
                for (datums, diff) in source.iter() {
                    target.push((BatchValOwn::<Tr>::ok((*datums).to_row()), -diff));
                }
                // local copies that may count down to zero.
                let mut offset = offset;
                let mut limit = limit;

                // The order in which we should produce rows.
                let mut indexes = (0..source.len()).collect::<Vec<_>>();
                // We decode the datums once, into a common buffer for efficiency.
                // Each row should contain `arity` columns; we should check that.
                let temp_storage = mz_repr::RowArena::new();
                let mut buffer = datum_vec.borrow();
                for (index, (datums, _)) in source.iter().enumerate() {
                    datums.extend_datums(&temp_storage, &mut buffer, None);
                    assert_eq!(buffer.len(), arity * (index + 1));
                }
                let width = buffer.len() / source.len();

                //todo: use arrangements or otherwise make the sort more performant?
                indexes.sort_by(|left, right| {
                    let left = &buffer[left * width..][..width];
                    let right = &buffer[right * width..][..width];
                    // Note: source was originally ordered by the u8 array representation
                    // of rows, but left.cmp(right) uses Datum::cmp.
                    mz_expr::compare_columns(&order_key, left, right, || left.cmp(right))
                });

                // We now need to lay out the data in order of `buffer`, but respecting
                // the `offset` and `limit` constraints.
                for index in indexes.into_iter() {
                    let (datums, mut diff) = source[index];
                    if !diff.is_positive() {
                        continue;
                    }
                    // If we are still skipping early records ...
                    if offset > 0 {
                        let to_skip =
                            std::cmp::min(offset, usize::try_from(diff.into_inner()).unwrap());
                        offset -= to_skip;
                        diff -= Diff::try_from(to_skip).unwrap();
                    }
                    // We should produce at most `limit` records.
                    if let Some(limit) = &mut limit {
                        diff = std::cmp::min(diff, Diff::from(*limit));
                        *limit -= diff;
                    }
                    // Output the indicated number of rows.
                    if diff.is_positive() {
                        // Emit retractions for the elements actually part of
                        // the set of TopK elements.
                        target.push((BatchValOwn::<Tr>::ok(datums.to_row()), diff));
                    }
                }
            }
        });
    (arranged, reduced)
}

fn render_intra_ts_thinning<'s, T>(
    collection: VecCollection<'s, T, (Row, Row), Diff>,
    order_key: Vec<mz_expr::ColumnOrder>,
    limit: LirScalarExpr,
) -> VecCollection<'s, T, (Row, Row), Diff>
where
    T: timely::progress::Timestamp + Lattice,
{
    let mut datum_vec = mz_repr::DatumVec::new();

    let mut aggregates = BTreeMap::new();
    let shared = Rc::new(RefCell::new(monoids::Top1MonoidShared {
        order_key,
        left: DatumVec::new(),
        right: DatumVec::new(),
    }));
    collection
        .inner
        .unary_notify(
            Pipeline,
            "TopKIntraTimeThinning",
            [],
            move |input, output, notificator| {
                input.for_each_time(|time, data| {
                    let agg_time = aggregates
                        .entry(time.time().clone())
                        .or_insert_with(BTreeMap::new);
                    for ((grp_row, row), record_time, diff) in data.flat_map(|data| data.drain(..))
                    {
                        let monoid = monoids::Top1MonoidLocal {
                            row,
                            shared: Rc::clone(&shared),
                        };

                        // Evalute the limit, first as a constant and then against the key if needed.
                        let limit = if let Some(l) = limit.as_literal_int64() {
                            l
                        } else {
                            let temp_storage = mz_repr::RowArena::new();
                            let key_datums = datum_vec.borrow_with(&grp_row);
                            // Unpack `key` and determine the limit.
                            // If the limit errors, use a zero limit; errors are surfaced elsewhere.
                            let datum_limit = limit
                                .eval(&key_datums, &temp_storage)
                                .unwrap_or(mz_repr::Datum::Int64(0));
                            if datum_limit == Datum::Null {
                                i64::MAX
                            } else {
                                datum_limit.unwrap_int64()
                            }
                        };

                        let topk = agg_time
                            .entry((grp_row, record_time))
                            .or_insert_with(move || topk_agg::TopKBatch::new(limit));
                        topk.update(monoid, diff.into_inner());
                    }
                    notificator.notify_at(time.retain(0));
                });

                notificator.for_each(|time, _, _| {
                    if let Some(aggs) = aggregates.remove(time.time()) {
                        let mut session = output.session(&time);
                        for ((grp_row, record_time), topk) in aggs {
                            session.give_iterator(topk.into_iter().map(|(monoid, diff)| {
                                (
                                    (grp_row.clone(), monoid.into_row()),
                                    record_time.clone(),
                                    diff.into(),
                                )
                            }))
                        }
                    }
                });
            },
        )
        .as_collection()
}

/// Types for in-place intra-ts aggregation of monotonic streams.
pub mod topk_agg {
    use differential_dataflow::consolidation;
    use smallvec::SmallVec;

    // TODO: This struct looks a lot like ChangeBatch and indeed its code is a modified version of
    // that. It would be nice to find a way to reuse some or all of the code from there.
    //
    // Additionally, because we're calling into DD's consolidate method we are forced to work with
    // the `Ord` trait which for the usage we do above means that we need to clone the `order_key`
    // for each record. It would be nice to also remove the need for cloning that piece of data
    pub struct TopKBatch<T> {
        updates: SmallVec<[(T, i64); 16]>,
        clean: usize,
        limit: i64,
    }

    impl<T: Ord> TopKBatch<T> {
        pub fn new(limit: i64) -> Self {
            Self {
                updates: SmallVec::new(),
                clean: 0,
                limit,
            }
        }

        /// Adds a new update, for `item` with `value`.
        ///
        /// This could be optimized to perform compaction when the number of "dirty" elements exceeds
        /// half the length of the list, which would keep the total footprint within reasonable bounds
        /// even under an arbitrary number of updates. This has a cost, and it isn't clear whether it
        /// is worth paying without some experimentation.
        #[inline]
        pub fn update(&mut self, item: T, value: i64) {
            self.updates.push((item, value));
            self.maintain_bounds();
        }

        /// Compact the internal representation.
        ///
        /// This method sort `self.updates` and consolidates elements with equal item, discarding
        /// any whose accumulation is zero. It is optimized to only do this if the number of dirty
        /// elements is non-zero.
        #[inline]
        pub fn compact(&mut self) {
            if self.clean < self.updates.len() && self.updates.len() > 1 {
                let len = consolidation::consolidate_slice(&mut self.updates);
                self.updates.truncate(len);

                // We can now retain only the first K records and throw away everything else
                let mut limit = self.limit;
                self.updates.retain(|x| {
                    if limit > 0 {
                        limit -= x.1;
                        true
                    } else {
                        false
                    }
                });
                // By the end of the loop above `limit` will either be:
                // (a) Positive, in which case all updates were retained;
                // (b) Zero, in which case we discarded all updates after limit became zero;
                // (c) Negative, in which case the last record we retained had more copies
                // than necessary. In this latter case, we need to do one final adjustment
                // of the diff field of the last record so that the total sum of the diffs
                // in the batch is K.
                if limit < 0 {
                    if let Some(item) = self.updates.last_mut() {
                        // We are subtracting the limit *negated*, therefore we are subtracting a value
                        // that is *greater* than or equal to zero, which represents the excess.
                        item.1 -= -limit;
                    }
                }
            }
            self.clean = self.updates.len();
        }

        /// Maintain the bounds of pending (non-compacted) updates versus clean (compacted) data.
        /// This function tries to minimize work by only compacting if enough work has accumulated.
        fn maintain_bounds(&mut self) {
            // if we have more than 32 elements and at least half of them are not clean, compact
            if self.updates.len() > 32 && self.updates.len() >> 1 >= self.clean {
                self.compact()
            }
        }
    }

    impl<T: Ord> IntoIterator for TopKBatch<T> {
        type Item = (T, i64);
        type IntoIter = smallvec::IntoIter<[(T, i64); 16]>;

        fn into_iter(mut self) -> Self::IntoIter {
            self.compact();
            self.updates.into_iter()
        }
    }
}

/// Monoids for in-place compaction of monotonic streams.
pub mod monoids {
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::hash::{Hash, Hasher};
    use std::rc::Rc;

    use columnation::{Columnation, Region};
    use differential_dataflow::difference::{IsZero, Multiply, Semigroup};
    use mz_expr::ColumnOrder;
    use mz_repr::{DatumVec, Diff, Row};
    use serde::{Deserialize, Serialize};

    /// A monoid containing a row and an ordering.
    #[derive(Eq, PartialEq, Debug, Serialize, Deserialize, Hash, Default)]
    pub struct Top1Monoid {
        pub row: Row,
        pub order_key: Vec<ColumnOrder>,
    }

    impl Clone for Top1Monoid {
        #[inline]
        fn clone(&self) -> Self {
            Self {
                row: self.row.clone(),
                order_key: self.order_key.clone(),
            }
        }

        #[inline]
        fn clone_from(&mut self, source: &Self) {
            self.row.clone_from(&source.row);
            self.order_key.clone_from(&source.order_key);
        }
    }

    impl Multiply<Diff> for Top1Monoid {
        type Output = Self;

        fn multiply(self, factor: &Diff) -> Self {
            // Multiplication in Top1Monoid is idempotent, and its
            // users must ascertain its monotonicity beforehand
            // (typically with ensure_monotonic) since it has no zero
            // value for us to use here.
            assert!(factor.is_positive());
            self
        }
    }

    impl Ord for Top1Monoid {
        fn cmp(&self, other: &Self) -> Ordering {
            debug_assert_eq!(self.order_key, other.order_key);

            // It might be nice to cache this row decoding like the non-monotonic codepath, but we'd
            // have to store the decoded Datums in the same struct as the Row, which gets tricky.
            let left: Vec<_> = self.row.unpack();
            let right: Vec<_> = other.row.unpack();
            mz_expr::compare_columns(&self.order_key, &left, &right, || left.cmp(&right))
        }
    }
    impl PartialOrd for Top1Monoid {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Semigroup for Top1Monoid {
        fn plus_equals(&mut self, rhs: &Self) {
            let cmp = (*self).cmp(rhs);
            // NB: Reminder that TopK returns the _minimum_ K items.
            if cmp == Ordering::Greater {
                self.clone_from(rhs);
            }
        }
    }

    impl IsZero for Top1Monoid {
        fn is_zero(&self) -> bool {
            false
        }
    }

    impl Columnation for Top1Monoid {
        type InnerRegion = Top1MonoidRegion;
    }

    #[derive(Default)]
    pub struct Top1MonoidRegion {
        row_region: <Row as Columnation>::InnerRegion,
        order_key_region: <Vec<ColumnOrder> as Columnation>::InnerRegion,
    }

    impl Region for Top1MonoidRegion {
        type Item = Top1Monoid;

        unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item {
            let row = unsafe { self.row_region.copy(&item.row) };
            let order_key = unsafe { self.order_key_region.copy(&item.order_key) };
            Self::Item { row, order_key }
        }

        fn clear(&mut self) {
            self.row_region.clear();
            self.order_key_region.clear();
        }

        fn reserve_items<'a, I>(&mut self, items1: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self::Item> + Clone,
        {
            let items2 = items1.clone();
            self.row_region
                .reserve_items(items1.into_iter().map(|s| &s.row));
            self.order_key_region
                .reserve_items(items2.into_iter().map(|s| &s.order_key));
        }

        fn reserve_regions<'a, I>(&mut self, regions1: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            let regions2 = regions1.clone();
            self.row_region
                .reserve_regions(regions1.into_iter().map(|s| &s.row_region));
            self.order_key_region
                .reserve_regions(regions2.into_iter().map(|s| &s.order_key_region));
        }

        fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
            self.row_region.heap_size(&mut callback);
            self.order_key_region.heap_size(callback);
        }
    }

    /// A shared portion of a thread-local top-1 monoid implementation.
    #[derive(Debug)]
    pub struct Top1MonoidShared {
        pub order_key: Vec<ColumnOrder>,
        pub left: DatumVec,
        pub right: DatumVec,
    }

    /// A monoid containing a row and a shared pointer to a shared structure.
    /// Only suitable for thread-local aggregations.
    #[derive(Debug, Clone)]
    pub struct Top1MonoidLocal {
        pub row: Row,
        pub shared: Rc<RefCell<Top1MonoidShared>>,
    }

    impl Top1MonoidLocal {
        pub fn into_row(self) -> Row {
            self.row
        }
    }

    impl PartialEq for Top1MonoidLocal {
        fn eq(&self, other: &Self) -> bool {
            self.row.eq(&other.row)
        }
    }

    impl Eq for Top1MonoidLocal {}

    impl Hash for Top1MonoidLocal {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.row.hash(state);
        }
    }

    impl Ord for Top1MonoidLocal {
        fn cmp(&self, other: &Self) -> Ordering {
            debug_assert!(Rc::ptr_eq(&self.shared, &other.shared));
            let Top1MonoidShared {
                left,
                right,
                order_key,
            } = &mut *self.shared.borrow_mut();

            let left = left.borrow_with(&self.row);
            let right = right.borrow_with(&other.row);
            mz_expr::compare_columns(order_key, &left, &right, || left.cmp(&right))
        }
    }

    impl PartialOrd for Top1MonoidLocal {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Semigroup for Top1MonoidLocal {
        fn plus_equals(&mut self, rhs: &Self) {
            let cmp = (*self).cmp(rhs);
            // NB: Reminder that TopK returns the _minimum_ K items.
            if cmp == Ordering::Greater {
                self.clone_from(rhs);
            }
        }
    }

    impl IsZero for Top1MonoidLocal {
        fn is_zero(&self) -> bool {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::input::Input;
    use mz_repr::{Datum, Timestamp};
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::{Event, Extract};

    use super::*;
    use crate::render::columnar::{columnar_to_vec, vec_to_columnar};

    type KeyedUpdate = ((Row, Row), Timestamp, Diff);
    type Captured = std::sync::mpsc::Receiver<Event<Timestamp, Vec<KeyedUpdate>>>;

    fn extract_sorted(captured: Captured) -> Vec<KeyedUpdate> {
        let mut updates: Vec<_> = captured
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect();
        updates.sort();
        updates
    }

    /// Input rows tagged with distinct timestamps. Feeding several timestamps
    /// makes both arms exercise per-record time handling. The columnar arm owns
    /// time and diff per record via `Columnar::into_owned` on the ok path. The
    /// two `-1` records exercise `Columnar::into_owned` on a negative diff. They
    /// retract at a `(row, time)` that has no matching insertion, so they survive
    /// the `InputSession`'s pre-send consolidation and reach the operator.
    fn test_input() -> Vec<(Row, u64, Diff)> {
        vec![
            (
                Row::pack_slice(&[Datum::Int32(1), Datum::String("a")]),
                0,
                Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(2), Datum::String("b")]),
                1,
                Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(1), Datum::String("a")]),
                2,
                Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(3), Datum::Null]),
                2,
                Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(2), Datum::String("b")]),
                2,
                -Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(4), Datum::String("d")]),
                1,
                -Diff::ONE,
            ),
        ]
    }

    /// Runs `map_topk_key` against the columnar input edge, forming a
    /// hash-and-group key exactly as `build_topk` does. Returns the sorted
    /// `(key, value)` updates.
    fn run_columnar(input: Vec<(Row, u64, Diff)>) -> Vec<KeyedUpdate> {
        let captured = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut handle, collection) = scope.new_collection();
                let pairer = Pairer::new(1);
                let group_key = [0usize];
                let keyed =
                    map_topk_key(vec_to_columnar(collection), "test", move |datums, row| {
                        let hash = row.hashed();
                        let iterator = group_key.iter().map(|i| datums[*i]);
                        pairer.merge(std::iter::once(Datum::from(hash)), iterator)
                    });
                let captured = keyed.inner.capture();
                for (row, time, diff) in input {
                    handle.update_at(row, Timestamp::from(time), diff);
                }
                handle.advance_to(Timestamp::from(3_u64));
                handle.flush();
                captured
            })
        });
        extract_sorted(captured)
    }

    /// `map_topk_key` forms the `(key, value)` updates from the columnar input
    /// across several distinct timestamps.
    ///
    /// The key closure is infallible (projection plus hash plus pack), so there is
    /// no fallible-key path. `Columnar::into_owned` for time and diff runs on every
    /// record, so the multi-timestamp, mixed-sign input exercises it on both
    /// positive and negative diffs.
    #[mz_ore::test]
    fn map_topk_key_forms_key() {
        let updates = run_columnar(test_input());
        assert!(!updates.is_empty());
        // Retractions reach the operator, so a negative diff was decoded via
        // `Columnar::into_owned`.
        assert!(updates.iter().any(|(_, _, d)| *d < Diff::ZERO));
        // The value is the full input row. The key is `(hash, group_column)`, so
        // the group component mirrors column 0 of the value row.
        for ((key, value), _t, _d) in &updates {
            let key_datums: Vec<_> = key.iter().collect();
            let value_datums: Vec<_> = value.iter().collect();
            assert_eq!(key_datums.len(), 2);
            assert_eq!(key_datums[1], value_datums[0]);
        }
    }

    /// `topk_result_to_columnar` drops the hash-key pairing and produces a
    /// columnar edge whose rows are the value component, preserving times and
    /// diffs. This produces the columnar output for the monotonic and basic TopK
    /// plans.
    #[mz_ore::test]
    fn topk_result_to_columnar_drops_key() {
        let key = Row::pack_slice(&[Datum::Int64(7)]);
        let rows = vec![
            (
                (key.clone(), Row::pack_slice(&[Datum::Int32(1)])),
                0u64,
                Diff::ONE,
            ),
            (
                (key.clone(), Row::pack_slice(&[Datum::Int32(2)])),
                1u64,
                Diff::ONE,
            ),
            // Retracts at a `(row, time)` with no insertion, so it survives the
            // `InputSession`'s pre-send consolidation and exercises a borrowed
            // negative diff.
            (
                (key.clone(), Row::pack_slice(&[Datum::Int32(1)])),
                2u64,
                -Diff::ONE,
            ),
        ];
        let mut expected: Vec<(Row, Timestamp, Diff)> = rows
            .iter()
            .map(|((_, v), t, d)| (v.clone(), Timestamp::from(*t), *d))
            .collect();
        expected.sort();

        let captured = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut handle, collection) = scope.new_collection();
                let edge = topk_result_to_columnar(collection);
                let captured = columnar_to_vec(edge).inner.capture();
                for (kv, time, diff) in rows {
                    handle.update_at(kv, Timestamp::from(time), diff);
                }
                handle.advance_to(Timestamp::from(3u64));
                handle.flush();
                captured
            })
        });

        let mut got: Vec<(Row, Timestamp, Diff)> = captured
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect();
        got.sort();
        assert_eq!(got, expected);
    }
}
