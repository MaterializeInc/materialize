// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Columnar dataflow edge support.
//!
//! Defines [`CollectionEdge`], the columnar batch representation that dataflow
//! edges between Plan nodes carry. Every producer emits this representation, and
//! so does the feedback edge of a recursive binding, so an iteration costs no
//! conversion.
//!
//! Within a Plan node, operators may freely materialize `Vec` collections. Only
//! the collection edge format is constrained. A node that produces a row-based
//! collection re-encodes it to the columnar edge at its output leaf via
//! [`vec_to_columnar`]. A node that must consume rows decodes at its input leaf
//! via [`columnar_to_vec`]. Both are named operators (`VecToColumnar`,
//! `ColumnarToVec`), so those leaf seams stay visible in dataflow
//! introspection.
//!
//! # Which consumers need a decode
//!
//! A consumer that reads a record's datums and packs a fresh row only ever borrows
//! its input, so it reads the edge directly through [`flat_map_datums`] and the
//! decode would buy it nothing. A decode earns its owned [`Row`] per record only
//! where the consumer keeps that row. Reaching for [`columnar_to_vec`] without
//! checking which of the two a consumer is was the mistake this rule exists to
//! prevent.
//!
//! # Where `Vec` remains
//!
//! The cases below are not oversights. Each is either not ours to choose or waiting
//! on a change elsewhere, and all are tracked on CPU-253.
//!
//! * **Reduce and TopK internals.** Both render their stages over keyed `Vec`
//!   collections, so the step that forms the key materializes an owned `(Row, Row)`
//!   per record. Neither decodes in the sense above: TopK's `map_topk_key` and
//!   reduce's key-value flat map each read the edge and pack what they need, so no
//!   `ColumnarToVec` stands at either input. On the way out they differ. TopK
//!   re-encodes at `topk_result_to_columnar`; reduce hands out an arrangement rather
//!   than an edge, and a consumer that wants one rebuilds it from the arrangement.
//!   Both are worth removing and neither is structural. Neither is a single change
//!   either: the stages hand intermediate results to differential operators that
//!   would need to take a container builder first, and converting one stage at a
//!   time would replace one materialization with several.
//! * **The delta-join stage chain.** `VecCollection<(Row, T)>` from the seed through
//!   every `half_join`, re-encoded once at the node's output. Left alone
//!   deliberately rather than missed: differential has `half_join` input changes
//!   lined up, and every part of the chain hangs off that container, so a conversion
//!   made here would be discarded.
//! * **Sinks.** A sink serializes every row it writes, so materializing the row is
//!   the output format's requirement rather than a container choice.
//! * **The `DifferentialDataflow` linear-join arm.** `join_core` returns a
//!   `VecCollection` and takes no container builder, so that arm re-encodes what it
//!   produces. Structural until differential offers the builder.
//! * **The error half of a [`CollectionBundle`].** Errors travel as a
//!   `VecCollection`. In a healthy dataflow that path carries no records, so
//!   uniformity is the whole argument for converting it, against a change that
//!   reaches every render signature. The question becomes real at the ok/err demux
//!   inside the joins, which is the first consumer that would want both halves
//!   columnar.
//! * **Monotonic monoids.** `Top1Monoid` and `ReductionMonoid` carry owned rows
//!   inside the *diff*, for in-place aggregation within a timestamp. That is an
//!   arrangement's diff type rather than a channel's container, so the rule here
//!   does not reach it.
//!
//! [`CollectionBundle`]: crate::render::context::CollectionBundle

use columnar::{Borrow, Columnar, Container, Index, Len, Push};
use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
use differential_dataflow::{AsCollection, Collection, VecCollection};
use mz_repr::{DatumVec, DatumVecBorrow, Diff, Row};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::ColumnChunker;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::columnar_consolidate_exchange;
use mz_timely_util::columnar::merge_batcher::ColumnMergeBatcher;
use mz_timely_util::operator::consolidate_pact;
use timely::ContainerBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{Operator, OutputBuilder};
use timely::dataflow::{Scope, Stream, StreamVec};
use timely::order::Product;
use timely::progress::Antichain;

use crate::render::RenderTimestamp;
use crate::render::context::{ECB, Session};
use crate::render::errors::DataflowErrorSer;

/// A columnar collection of `(D, T, R)` updates traveling on a compute
/// dataflow edge.
///
/// Mirrors differential's [`VecCollection<'scope, T, D, R>`]; the underlying
/// container is [`Column<(D, T, R)>`] instead of `Vec<(D, T, R)>`.
pub type ColumnarCollection<'scope, T, D, R> = Collection<'scope, T, Column<(D, T, R)>>;

/// A dataflow edge between Plan nodes: a columnar collection of `(Row, Diff)` updates.
pub type CollectionEdge<'scope, T> = ColumnarCollection<'scope, T, Row, Diff>;

/// Concatenates a collection of columnar edges.
pub fn concat_many<'scope, T, I>(scope: Scope<'scope, T>, edges: I) -> CollectionEdge<'scope, T>
where
    T: RenderTimestamp,
    I: IntoIterator<Item = CollectionEdge<'scope, T>>,
{
    let cols: Vec<_> = edges.into_iter().collect();
    differential_dataflow::collection::concatenate(scope, cols)
}

/// Applies `logic` to each record in `edge`, exposing the record as a borrowed
/// [`DatumVecBorrow`] and giving it ok and err output sessions.
///
/// `name` is the rendered operator's name, so a caller that replaces a named operator
/// keeps that name in introspection. `max_demand` bounds the number of columns decoded
/// per row. Pass `usize::MAX` to decode all columns.
///
/// This is the canonical entry point for "decoding consumers" (operators that
/// read [`mz_repr::Datum`]s from each row anyway). It iterates the columnar
/// batch directly without going through an owned [`Row`].
pub fn flat_map_datums<'scope, T, DCB, L>(
    edge: CollectionEdge<'scope, T>,
    name: &str,
    max_demand: usize,
    mut logic: L,
) -> (
    Stream<'scope, T, DCB::Container>,
    StreamVec<'scope, T, (DataflowErrorSer, T, Diff)>,
)
where
    T: RenderTimestamp,
    DCB: ContainerBuilder,
    L: for<'a> FnMut(
            &'a mut DatumVecBorrow<'_>,
            T,
            Diff,
            &mut Session<T, DCB>,
            &mut Session<T, ECB<T>>,
        ) -> usize
        + 'static,
{
    let scope = edge.inner.scope();
    let mut builder = OperatorBuilder::new(name.to_string(), scope);
    let (ok_output, ok_stream) = builder.new_output();
    let mut ok_output = OutputBuilder::<_, DCB>::from(ok_output);
    let (err_output, err_stream) = builder.new_output();
    let mut err_output = OutputBuilder::<_, ECB<T>>::from(err_output);
    let mut input = builder.new_input(edge.inner, Pipeline);
    builder.build(move |_capabilities| {
        let mut datums = DatumVec::new();
        move |_frontiers| {
            let mut ok_output = ok_output.activate();
            let mut err_output = err_output.activate();
            input.for_each(|time, data| {
                // Retain the input capability to derive a `Capability` for each
                // output. The `Session` type alias is fixed to `Capability<T>`.
                let ok_cap = time.retain(0);
                let err_cap = time.retain(1);
                let mut ok_session = ok_output.session_with_builder(&ok_cap);
                let mut err_session = err_output.session_with_builder(&err_cap);
                // Rows are read from the borrowed column, never materialized as
                // owned `Row`s.
                for (v, t, d) in data.borrow().into_index_iter() {
                    logic(
                        &mut datums.borrow_with_limit(v, max_demand),
                        Columnar::into_owned(t),
                        Columnar::into_owned(d),
                        &mut ok_session,
                        &mut err_session,
                    );
                }
            });
        }
    });
    (ok_stream, err_stream)
}

/// Negates the diff of every record in `column`, rebuilding only the diff column.
///
/// A `Typed` input hands its row and time columns over untouched, so only the
/// diffs are rebuilt, which is 8 bytes per record. A serialized input keeps all
/// three columns in one buffer, so its rows and times are copied in bulk.
///
/// Negation stays checked: `Neg for Overflowing` runs `overflowing_neg` and
/// reports overflow, which `-Diff::MIN` triggers.
///
/// TODO: Negate a `Typed` input's diffs in place rather than rebuilding them.
/// This cannot go through `columnar::IndexMut`: `Overflows` stores the raw
/// integer and materializes `Overflowing` on read, so there is no wrapper in
/// memory to borrow mutably, and handing out `&mut` to the raw integer would let
/// writes bypass the checked arithmetic. It needs a checked bulk operation on the
/// container instead, keeping the overflow check inside.
fn negate_column<T>(column: Column<(Row, T, Diff)>) -> Column<(Row, T, Diff)>
where
    T: RenderTimestamp,
{
    /// Collects the negation of every diff in `diffs` into a fresh column.
    fn negated_diffs<'a, D>(diffs: &'a D) -> <Diff as Columnar>::Container
    where
        D: Len + Index<Ref = Diff> + 'a,
    {
        let mut negated = <Diff as Columnar>::Container::default();
        for index in 0..diffs.len() {
            negated.push(-diffs.get(index));
        }
        negated
    }

    match column {
        Column::Typed((rows, times, diffs)) => {
            let negated = negated_diffs(&diffs.borrow());
            Column::Typed((rows, times, negated))
        }
        column => {
            let view = column.borrow();
            let len = view.len();
            let mut negated = <(Row, T, Diff) as Columnar>::Container::default();
            let (rows, times, diffs) = &mut negated;
            rows.extend_from_self(view.0, 0..len);
            times.extend_from_self(view.1, 0..len);
            *diffs = negated_diffs(&view.2);
            Column::Typed(negated)
        }
    }
}

/// The timestamp of a scope that carries iteration coordinates.
pub type RecTimestamp = Product<mz_repr::Timestamp, PointStamp<u64>>;

/// Truncates every time in `column` to `level - 1` iteration coordinates.
///
/// A `Typed` input hands its row and diff columns over untouched, because truncation only
/// rewrites times. A serialized input keeps all three columns in one buffer, so its rows
/// and diffs are copied in bulk rather than per record.
fn truncate_times(
    column: Column<(Row, RecTimestamp, Diff)>,
    level: usize,
) -> Column<(Row, RecTimestamp, Diff)> {
    /// Collects the truncation of every time in `times` into a fresh column.
    fn truncated<'a, C>(times: &'a C, level: usize) -> <RecTimestamp as Columnar>::Container
    where
        C: Len + Index<Ref = columnar::Ref<'a, RecTimestamp>> + 'a,
    {
        let mut truncated = <RecTimestamp as Columnar>::Container::default();
        for index in 0..times.len() {
            let mut time = RecTimestamp::into_owned(times.get(index));
            let mut coordinates = std::mem::take(&mut time.inner).into_inner();
            coordinates.truncate(level - 1);
            time.inner = PointStamp::new(coordinates);
            truncated.push(&time);
        }
        truncated
    }

    match column {
        Column::Typed((rows, times, diffs)) => {
            let times = truncated(&times.borrow(), level);
            Column::Typed((rows, times, diffs))
        }
        column => {
            let view = column.borrow();
            let len = view.len();
            let mut out = <(Row, RecTimestamp, Diff) as Columnar>::Container::default();
            let (rows, times, diffs) = &mut out;
            rows.extend_from_self(view.0, 0..len);
            *times = truncated(&view.1, level);
            diffs.extend_from_self(view.2, 0..len);
            Column::Typed(out)
        }
    }
}

/// Leaves a dynamically created scope that has `level` iteration coordinates.
///
/// The columnar counterpart of differential's `leave_dynamic`, which it offers only for
/// `Vec` collections. Keeping the recursive binding's result on the edge is what lets the
/// feedback loop run without a decode and a re-encode per iteration.
pub fn columnar_leave_dynamic<'scope>(
    collection: ColumnarCollection<'scope, RecTimestamp, Row, Diff>,
    level: usize,
) -> ColumnarCollection<'scope, RecTimestamp, Row, Diff> {
    let scope = collection.inner.scope();
    let mut builder = OperatorBuilder::new("LeaveDynamic".to_string(), scope);
    let (output, stream) = builder.new_output();
    let mut output =
        OutputBuilder::<_, CapacityContainerBuilder<Column<(Row, RecTimestamp, Diff)>>>::from(
            output,
        );
    // The connection summary tells the scope that this operator drops all but `level - 1`
    // coordinates, so a downstream frontier is not held back by the iteration it leaves.
    let summary = Product {
        outer: Default::default(),
        inner: PointStampSummary {
            retain: Some(level - 1),
            actions: Vec::new(),
        },
    };
    let mut input = builder.new_input_connection(
        collection.inner,
        Pipeline,
        [(0, Antichain::from_elem(summary))],
    );

    builder.build(move |_capability| {
        move |_frontier| {
            let mut output = output.activate();
            input.for_each(|cap, data| {
                let mut time = cap.time().clone();
                let mut coordinates = std::mem::take(&mut time.inner).into_inner();
                coordinates.truncate(level - 1);
                time.inner = PointStamp::new(coordinates);
                let cap = cap.delayed(&time, 0);
                let mut truncated = truncate_times(std::mem::take(data), level);
                output.session(&cap).give_container(&mut truncated);
            });
        }
    });

    stream.as_collection()
}

/// Negates the diff of every record in a [`ColumnarCollection`].
pub fn columnar_negate<'scope, T>(
    collection: ColumnarCollection<'scope, T, Row, Diff>,
) -> ColumnarCollection<'scope, T, Row, Diff>
where
    T: RenderTimestamp,
{
    collection
        .inner
        .unary::<CapacityContainerBuilder<Column<(Row, T, Diff)>>, _, _, _>(
            Pipeline,
            "ColumnarNegate",
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut negated = negate_column(std::mem::take(data));
                        output.session(&time).give_container(&mut negated);
                    });
                }
            },
        )
        .as_collection()
}

/// Consolidates a [`ColumnarCollection`] natively, without a row round-trip.
///
/// A [`ColumnChunker`] sorts and consolidates the input columns and a
/// [`ColumnMergeBatcher`] merges them, both holding their data in [`Column`], so nothing
/// outside the exchange pact visits a record or materializes an owned [`Row`].
///
/// Uses [`consolidate_pact`] rather than `mz_arrange_core`: a consolidate emits a
/// consolidated collection, so building and reading back a maintained trace would be
/// wasted work.
pub fn columnar_consolidate<'scope, T>(
    collection: ColumnarCollection<'scope, T, Row, Diff>,
    name: &str,
) -> ColumnarCollection<'scope, T, Row, Diff>
where
    T: RenderTimestamp,
{
    // TODO: This pact re-serializes every record into a per-destination `ColumnBuilder`,
    // the one remaining full re-encode on this path. Bulk routing needs contiguous ranges
    // of records sharing a destination, which a per-record hash cannot identify.
    let exchange = ExchangeCore::<ColumnBuilder<_>, _>::new_core(
        columnar_consolidate_exchange::<Row, T, Diff>,
    );
    let consolidated = consolidate_pact::<
        ColumnChunker<(Row, T, Diff)>,
        ColumnMergeBatcher<Row, T, Diff>,
        _,
        _,
    >(collection.inner, exchange, name);

    // Flatten the sealed chain into one container per chunk, moving containers and
    // visiting no record.
    //
    // TODO: This ships a whole sealed snapshot in one activation, an un-fueled burst
    // hazard on large consolidations. `consolidate_named`'s unpack does the same, so a
    // fuel fix has to cover both.
    consolidated
        .unary::<CapacityContainerBuilder<Column<(Row, T, Diff)>>, _, _, _>(
            Pipeline,
            &format!("Flatten {name}"),
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for mut chunk in data.drain(..).flatten() {
                            session.give_container(&mut chunk);
                        }
                    });
                }
            },
        )
        .as_collection()
}

/// Repacks a row-based collection into columnar batches.
///
/// The leaf encode described in the module docs, named `VecToColumnar` in a rendered
/// dataflow. Repacking copies row bytes and allocates no per-record `Row`.
pub fn vec_to_columnar<'scope, T>(
    collection: VecCollection<'scope, T, Row, Diff>,
) -> ColumnarCollection<'scope, T, Row, Diff>
where
    T: RenderTimestamp,
{
    collection
        .inner
        .unary::<ColumnBuilder<(Row, T, Diff)>, _, _, _>(
            Pipeline,
            "VecToColumnar",
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for (v, t, d) in data.drain(..) {
                            session.give((&v, &t, &d));
                        }
                    });
                }
            },
        )
        .as_collection()
}

/// Decodes columnar batches into a row-based collection.
///
/// The leaf decode described in the module docs, named `ColumnarToVec` in a rendered
/// dataflow. It allocates an owned [`Row`] per record, which is why it stays at those
/// boundaries.
pub fn columnar_to_vec<'scope, T>(
    collection: ColumnarCollection<'scope, T, Row, Diff>,
) -> VecCollection<'scope, T, Row, Diff>
where
    T: RenderTimestamp,
{
    collection
        .inner
        .unary::<CapacityContainerBuilder<Vec<(Row, T, Diff)>>, _, _, _>(
            Pipeline,
            "ColumnarToVec",
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for (v, t, d) in data.borrow().into_index_iter() {
                            session.give((
                                Columnar::into_owned(v),
                                Columnar::into_owned(t),
                                Columnar::into_owned(d),
                            ));
                        }
                    });
                }
            },
        )
        .as_collection()
}

#[cfg(test)]
mod tests {
    use differential_dataflow::input::Input;
    use mz_ore::cast::CastFrom;
    use mz_repr::{Datum, Timestamp};
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::{Event, Extract};

    use super::*;

    type RowBuilder = CapacityContainerBuilder<Vec<(Row, Timestamp, Diff)>>;
    type CapturedRows = std::sync::mpsc::Receiver<Event<Timestamp, Vec<(Row, Timestamp, Diff)>>>;

    fn extract_sorted(captured: CapturedRows) -> Vec<(Row, Timestamp, Diff)> {
        let mut updates: Vec<_> = captured
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect();
        updates.sort();
        updates
    }

    fn test_rows() -> Vec<Row> {
        vec![
            Row::pack_slice(&[Datum::Int32(42), Datum::String("hello")]),
            Row::pack_slice(&[Datum::Int64(100), Datum::Null]),
            Row::pack_slice(&[Datum::True, Datum::False, Datum::Null]),
            Row::default(),
        ]
    }

    #[mz_ore::test]
    fn round_trip_through_columnar() {
        let rows = test_rows();
        let expected: Vec<_> = {
            let mut updates: Vec<_> = rows
                .iter()
                .enumerate()
                .map(|(i, r)| (r.clone(), Timestamp::from(u64::cast_from(i / 2)), Diff::ONE))
                .collect();
            updates.sort();
            updates
        };
        let captured = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut input, collection) = scope.new_collection();
                let captured = columnar_to_vec(vec_to_columnar(collection)).inner.capture();
                for (i, row) in rows.into_iter().enumerate() {
                    input.advance_to(Timestamp::from(u64::cast_from(i / 2)));
                    input.update(row, Diff::ONE);
                }
                input.advance_to(Timestamp::from(2_u64));
                input.flush();
                captured
            })
        });
        assert_eq!(extract_sorted(captured), expected);
    }

    #[mz_ore::test]
    fn columnar_negate_flips_diffs() {
        let rows = test_rows();
        let expected: Vec<_> = {
            let mut updates: Vec<_> = rows
                .iter()
                .map(|r| (r.clone(), Timestamp::from(0_u64), -Diff::ONE))
                .collect();
            updates.sort();
            updates
        };
        let captured = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut input, collection) = scope.new_collection();
                let edge = columnar_negate(vec_to_columnar(collection));
                let captured = columnar_to_vec(edge).inner.capture();
                for row in rows {
                    input.update(row, Diff::ONE);
                }
                input.advance_to(Timestamp::from(1_u64));
                input.flush();
                captured
            })
        });
        assert_eq!(extract_sorted(captured), expected);
    }

    #[mz_ore::test]
    fn concat_many_concatenates_columnar() {
        let rows = test_rows();
        let expected: Vec<_> = {
            let mut updates: Vec<_> = rows
                .iter()
                .map(|r| (r.clone(), Timestamp::from(0_u64), Diff::ONE))
                .collect();
            // The first row arrives on both inputs.
            updates.push((rows[0].clone(), Timestamp::from(0_u64), Diff::ONE));
            updates.sort();
            updates
        };
        let captured = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut input1, collection1) = scope.new_collection();
                let (mut input2, collection2) = scope.new_collection();
                let edge = concat_many(
                    scope,
                    [vec_to_columnar(collection1), vec_to_columnar(collection2)],
                );
                let captured = columnar_to_vec(edge).inner.capture();
                let (first, rest) = rows.split_first().unwrap();
                input1.update(first.clone(), Diff::ONE);
                input2.update(first.clone(), Diff::ONE);
                for row in rest {
                    input1.update(row.clone(), Diff::ONE);
                }
                for input in [&mut input1, &mut input2] {
                    input.advance_to(Timestamp::from(1_u64));
                    input.flush();
                }
                captured
            })
        });
        assert_eq!(extract_sorted(captured), expected);
    }

    #[mz_ore::test]
    fn flat_map_datums_arms_agree() {
        // Project the first datum of each row, exercising `max_demand`.
        let rows = test_rows();
        let captured = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut input, collection) = scope.new_collection();
                let (oks, _errs) = flat_map_datums::<_, RowBuilder, _>(
                    vec_to_columnar(collection),
                    "test",
                    1,
                    |datums, t, d, ok_session, _err_session| {
                        ok_session.give((Row::pack(datums.iter()), t, d));
                        1
                    },
                );
                let captured = oks.capture();
                for row in rows {
                    input.update(row, Diff::ONE);
                }
                input.advance_to(Timestamp::from(1_u64));
                input.flush();
                captured
            })
        });
        let updates = extract_sorted(captured);
        assert!(!updates.is_empty());
        // Each output row retains at most the first datum of its input.
        assert!(updates.iter().all(|(r, _, _)| r.iter().count() <= 1));
    }

    #[mz_ore::test]
    fn columnar_consolidate_accumulates_and_cancels() {
        let row1 = Row::pack_slice(&[Datum::Int32(1)]);
        let row2 = Row::pack_slice(&[Datum::Int32(2)]);
        let row3 = Row::pack_slice(&[Datum::Int32(3)]);
        // `row1` accumulates at t=0 and again at t=1, kept apart by time. `row2` cancels
        // at t=0 and `row3` at t=1, so neither reaches the output.
        let expected = vec![
            (row1.clone(), Timestamp::from(0_u64), Diff::from(2)),
            (row1.clone(), Timestamp::from(1_u64), Diff::ONE),
        ];

        let captured = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut input, collection) = scope.new_collection();
                let edge = columnar_consolidate(vec_to_columnar(collection), "Test");
                let captured = columnar_to_vec(edge).inner.capture();
                // t=0: row1 accumulates (+1, +1), row2 cancels (+1, -1).
                input.advance_to(Timestamp::from(0_u64));
                input.update(row1.clone(), Diff::ONE);
                input.update(row1.clone(), Diff::ONE);
                input.update(row2.clone(), Diff::ONE);
                input.update(row2, -Diff::ONE);
                // t=1: row1 survives (+1), row3 cancels (+1, -1).
                input.advance_to(Timestamp::from(1_u64));
                input.update(row1, Diff::ONE);
                input.update(row3.clone(), Diff::ONE);
                input.update(row3, -Diff::ONE);
                input.advance_to(Timestamp::from(2_u64));
                input.flush();
                captured
            })
        });
        assert_eq!(extract_sorted(captured), expected);
    }
}
