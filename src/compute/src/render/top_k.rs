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

use differential_dataflow::Data;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::merge_batcher::container::MergerChunk;
use differential_dataflow::trace::{Builder, Trace};
use differential_dataflow::{AsCollection, Collection};
use mz_compute_types::plan::top_k::{
    BasicTopKPlan, MonotonicTop1Plan, MonotonicTopKPlan, TopKPlan,
};
use mz_expr::func::CastUint64ToInt64;
use mz_expr::{BinaryFunc, EvalError, MirScalarExpr, UnaryFunc, func};
use mz_ore::cast::CastFrom;
use mz_ore::soft_assert_or_log;
use mz_repr::{Datum, DatumVec, Diff, Row, SharedRow, SqlScalarType};
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;
use timely::Container;
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;

use crate::extensions::arrange::{ArrangementSize, KeyCollection, MzArrange};
use crate::extensions::reduce::MzReduce;
use crate::render::Pairer;
use crate::render::context::{CollectionBundle, Context};
use crate::render::errors::MaybeValidatingRow;
use crate::row_spine::{
    DatumSeq, RowBatcher, RowBuilder, RowRowBatcher, RowRowBuilder, RowValBuilder, RowValSpine,
};
use crate::typedefs::{KeyBatcher, MzTimestamp, RowRowSpine, RowSpine};

// The implementation requires integer timestamps to be able to delay feedback for monotonic inputs.
impl<G> Context<G>
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
{
    pub(crate) fn render_topk(
        &self,
        input: CollectionBundle<G>,
        top_k_plan: TopKPlan,
    ) -> CollectionBundle<G> {
        let (ok_input, err_input) = input.as_specific_collection(None, &self.config_set);

        // We create a new region to compartmentalize the topk logic.
        let (ok_result, err_collection) = ok_input.scope().region_named("TopK", |inner| {
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
                    let errors = ok_input.flat_map(move |row| {
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
                    err_collection = err_collection.concat(&errors);
                }
            }

            let ok_result = match top_k_plan {
                TopKPlan::MonotonicTop1(MonotonicTop1Plan {
                    group_key,
                    order_key,
                    must_consolidate,
                }) => {
                    let (oks, errs) = self.render_top1_monotonic(
                        ok_input,
                        group_key,
                        order_key,
                        must_consolidate,
                    );
                    err_collection = err_collection.concat(&errs);
                    oks
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
                    let mut datum_vec = mz_repr::DatumVec::new();
                    let collection = ok_input
                        .map(move |row| {
                            let group_row = {
                                let datums = datum_vec.borrow_with(&row);
                                SharedRow::pack(group_key.iter().map(|i| datums[*i]))
                            };
                            (group_row, row)
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
                        (DataflowError::from(EvalError::Internal(m)), Diff::ONE)
                    });
                    err_collection = err_collection.concat(&errs);

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
                    use differential_dataflow::operators::iterate::Variable;
                    let delay = std::time::Duration::from_secs(10);
                    let retractions = Variable::new(
                        &mut ok_input.scope(),
                        <G::Timestamp as crate::render::RenderTimestamp>::system_delay(
                            delay.try_into().expect("must fit"),
                        ),
                    );
                    let thinned = collection.concat(&retractions.negate());

                    // As an additional optimization, we can skip creating the full topk hierachy
                    // here since we now have an upper bound on the number records due to the
                    // intra-ts thinning. The maximum number of records per timestamp is
                    // (num_workers * limit), which we expect to be a small number and so we render
                    // a single topk stage.
                    let (result, errs) =
                        self.build_topk_stage(thinned, order_key, 1u64, 0, limit, arity, false);
                    // Consolidate the output of `build_topk_stage` because it's not guaranteed to be.
                    let result = result.consolidate_named::<KeyBatcher<_, _, _>>(
                        "Monotonic TopK final consolidate",
                    );
                    retractions.set(&collection.concat(&result.negate()));
                    soft_assert_or_log!(
                        errs.is_none(),
                        "requested no validation, but received error collection"
                    );

                    result.map(|(_key_hash, row)| row)
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
                    err_collection = err_collection.concat(&errs);
                    oks
                }
            };

            // Extract the results from the region.
            (ok_result.leave_region(), err_collection.leave_region())
        });

        CollectionBundle::from_collections(ok_result, err_collection)
    }

    /// Constructs a TopK dataflow subgraph.
    fn build_topk<S>(
        &self,
        collection: Collection<S, Row, Diff>,
        group_key: Vec<usize>,
        order_key: Vec<mz_expr::ColumnOrder>,
        offset: usize,
        limit: Option<mz_expr::MirScalarExpr>,
        arity: usize,
        buckets: Vec<u64>,
    ) -> (Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        let pairer = Pairer::new(1);
        let mut datum_vec = mz_repr::DatumVec::new();
        let mut collection = collection.map({
            move |row| {
                let group_row = {
                    let row_hash = row.hashed();
                    let datums = datum_vec.borrow_with(&row);
                    let iterator = group_key.iter().map(|i| datums[*i]);
                    pairer.merge(std::iter::once(Datum::from(row_hash)), iterator)
                };
                (group_row, row)
            }
        });

        let mut validating = true;
        let mut err_collection: Option<Collection<S, _, _>> = None;

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
                        MirScalarExpr::literal_ok(Datum::Int64(new_limit), SqlScalarType::Int64);
                } else {
                    limit = limit.call_binary(
                        MirScalarExpr::literal_ok(
                            Datum::UInt64(u64::cast_from(offset)),
                            SqlScalarType::UInt64,
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
        let oks = oks.consolidate_named::<KeyBatcher<_, _, _>>("TopK final consolidate");
        collection = oks;
        if validating {
            err_collection = errs;
        }
        (
            collection.map(|(_key_hash, row)| row),
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
    fn build_topk_stage<S>(
        &self,
        collection: Collection<S, (Row, Row), Diff>,
        order_key: Vec<mz_expr::ColumnOrder>,
        modulus: u64,
        offset: usize,
        limit: Option<mz_expr::MirScalarExpr>,
        arity: usize,
        validating: bool,
    ) -> (
        Collection<S, (Row, Row), Diff>,
        Option<Collection<S, DataflowError, Diff>>,
    )
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
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
                S,
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
                build_topk_negated_stage::<S, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                    &input, order_key, offset, limit, arity,
                );
            // Turn arrangement into collection.
            let stage = stage.as_collection(|k, v| (k.to_row(), v.to_row()));

            (input, stage, None)
        };
        let input = input.as_collection(|k, v| (k.to_row(), v.to_row()));
        (oks.concat(&input), errs)
    }

    fn render_top1_monotonic<S>(
        &self,
        collection: Collection<S, Row, Diff>,
        group_key: Vec<usize>,
        order_key: Vec<mz_expr::ColumnOrder>,
        must_consolidate: bool,
    ) -> (Collection<S, Row, Diff>, Collection<S, DataflowError, Diff>)
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        // We can place our rows directly into the diff field, and only keep the relevant one
        // corresponding to evaluating our aggregate, instead of having to do a hierarchical
        // reduction. We start by mapping the group key along with the row and consolidating
        // if required to do so.
        let collection = collection
            .map({
                let mut datum_vec = mz_repr::DatumVec::new();
                move |row| {
                    // Scoped to allow borrow of `row` to drop.
                    let group_key = {
                        let datums = datum_vec.borrow_with(&row);
                        SharedRow::pack(group_key.iter().map(|i| datums[*i]))
                    };
                    (group_key, row)
                }
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
            .mz_arrange::<RowBatcher<_, _>, RowBuilder<_, _>, RowSpine<_, _>>(
                "Arranged MonotonicTop1 partial [val: empty]",
            )
            .mz_reduce_abelian::<_, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                "MonotonicTop1",
                move |_key, input, output| {
                    let accum: &monoids::Top1Monoid = &input[0].1;
                    output.push((accum.row.clone(), Diff::ONE));
                },
            );
        // TODO(database-issues#2288): Here we discard the arranged output.
        (result.as_collection(|_k, v| v.to_row()), errs)
    }
}

/// Build a stage of a topk reduction. Maintains the _retractions_ of the output instead of emitted
/// rows. This has the benefit that we have to maintain state proportionally to size of the output
/// instead of the size of the input.
///
/// Returns two arrangements:
/// * The arranged input data without modifications, and
/// * the maintained negated output data.
fn build_topk_negated_stage<G, Bu, Tr>(
    input: &Collection<G, (Row, Row), Diff>,
    order_key: Vec<mz_expr::ColumnOrder>,
    offset: usize,
    limit: Option<mz_expr::MirScalarExpr>,
    arity: usize,
) -> (
    Arranged<G, TraceAgent<RowRowSpine<G::Timestamp, Diff>>>,
    Arranged<G, TraceAgent<Tr>>,
)
where
    G: Scope,
    G::Timestamp: MzTimestamp,
    Bu: Builder<
            Time = G::Timestamp,
            Input: Container + MergerChunk + PushInto<((Row, Tr::ValOwn), G::Timestamp, Diff)>,
            Output = Tr::Batch,
        >,
    Tr: for<'a> Trace<
            Key<'a> = DatumSeq<'a>,
            KeyOwn = Row,
            ValOwn: Data + MaybeValidatingRow<Row, Row>,
            Time = G::Timestamp,
            Diff = Diff,
        > + 'static,
    Arranged<G, TraceAgent<Tr>>: ArrangementSize,
{
    let mut datum_vec = mz_repr::DatumVec::new();

    // We only want to arrange parts of the input that are not part of the actual output
    // such that `input.concat(&negated_output)` yields the correct TopK
    // NOTE(vmarcos): The arranged input operator name below is used in the tuning advice
    // built-in view mz_introspection.mz_expected_group_size_advice.
    let arranged = input.mz_arrange::<RowRowBatcher<_, _>, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
        "Arranged TopK input",
    );

    // Eagerly evaluate literal limits.
    let limit = limit.map(|l| match l.as_literal() {
        Some(Ok(Datum::Null)) => Ok(Diff::MAX),
        Some(Ok(d)) => Ok(Diff::from(d.unwrap_int64())),
        _ => Err(l),
    });

    let reduced = arranged.mz_reduce_abelian::<_, Bu, Tr>("Reduced TopK input", {
        move |mut hash_key, source, target: &mut Vec<(Tr::ValOwn, Diff)>| {
            // Unpack the limit, either into an integer literal or an expression to evaluate.
            let limit = match &limit {
                Some(Ok(lit)) => Some(*lit),
                Some(Err(expr)) => {
                    // Unpack `key` after skipping the hash and determine the limit.
                    // If the limit errors, use a zero limit; errors are surfaced elsewhere.
                    let temp_storage = mz_repr::RowArena::new();
                    let _hash = hash_key.next();
                    let mut key_datums = datum_vec.borrow();
                    key_datums.extend(hash_key);
                    let datum_limit = expr
                        .eval(&key_datums, &temp_storage)
                        .unwrap_or(Datum::Int64(0));
                    Some(match datum_limit {
                        Datum::Null => Diff::MAX,
                        d => Diff::from(d.unwrap_int64()),
                    })
                }
                None => None,
            };

            if let Some(err) = Tr::ValOwn::into_error() {
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
                target.push((Tr::ValOwn::ok((*datums).to_row()), -diff));
            }
            // local copies that may count down to zero.
            let mut offset = offset;
            let mut limit = limit;

            // The order in which we should produce rows.
            let mut indexes = (0..source.len()).collect::<Vec<_>>();
            // We decode the datums once, into a common buffer for efficiency.
            // Each row should contain `arity` columns; we should check that.
            let mut buffer = datum_vec.borrow();
            for (index, (datums, _)) in source.iter().enumerate() {
                buffer.extend(*datums);
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
                    target.push((Tr::ValOwn::ok(datums.to_row()), diff));
                }
            }
        }
    });
    (arranged, reduced)
}

fn render_intra_ts_thinning<S>(
    collection: Collection<S, (Row, Row), Diff>,
    order_key: Vec<mz_expr::ColumnOrder>,
    limit: mz_expr::MirScalarExpr,
) -> Collection<S, (Row, Row), Diff>
where
    S: Scope,
    S::Timestamp: Lattice,
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
                while let Some((time, data)) = input.next() {
                    let agg_time = aggregates
                        .entry(time.time().clone())
                        .or_insert_with(BTreeMap::new);
                    for ((grp_row, row), record_time, diff) in data.drain(..) {
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
                    notificator.notify_at(time.retain());
                }

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

    use differential_dataflow::containers::{Columnation, Region};
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
