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
//! edges between Plan nodes carry. Every producer emits this representation.
//!
//! Within a Plan node, operators may freely materialize `Vec` collections. Only
//! the collection edge format is constrained. A node that produces a row-based
//! collection re-encodes it to the columnar edge at its output leaf via
//! [`vec_to_columnar`]. A node that must consume rows decodes at its input leaf
//! via [`columnar_to_vec`]. Both are named operators (`VecToColumnar`,
//! `ColumnarToVec`), so those leaf seams stay visible in dataflow
//! introspection.

use columnar::{Columnar, Index};
use differential_dataflow::{AsCollection, Collection, VecCollection};
use mz_repr::{DatumVec, DatumVecBorrow, Diff, Row};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2KeyBatcher, columnar_exchange};
use mz_timely_util::operator::consolidate_pact;
use timely::ContainerBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{Operator, OutputBuilder};
use timely::dataflow::{Scope, Stream, StreamVec};

use crate::render::RenderTimestamp;
use crate::render::context::{ECB, Session};
use crate::render::errors::DataflowErrorSer;

/// A columnar collection of `(D, T, R)` updates traveling on a compute
/// dataflow edge.
///
/// Mirrors differential's [`VecCollection<'scope, T, D, R>`]; the underlying
/// container is [`Column<(D, T, R)>`] instead of `Vec<(D, T, R)>`.
pub type ColumnarCollection<'scope, T, D, R> = Collection<'scope, T, Column<(D, T, R)>>;

/// A dataflow edge between Plan nodes: a columnar collection of `(Row, Diff)`
/// updates. Every producer emits this representation. A row-based producer
/// re-encodes to it at its output leaf via [`vec_to_columnar`].
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
/// `max_demand` bounds the number of columns decoded per row. Pass `usize::MAX`
/// to decode all columns.
///
/// This is the canonical entry point for "decoding consumers" (operators that
/// read [`mz_repr::Datum`]s from each row anyway). It iterates the columnar
/// batch directly without going through an owned [`Row`].
pub fn flat_map_datums<'scope, T, DCB, L>(
    edge: CollectionEdge<'scope, T>,
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
    let mut builder = OperatorBuilder::new("CollectionFlatMap".to_string(), scope);
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

/// Negates the diff of every record in a [`ColumnarCollection`].
///
/// Rows and times are pushed from their borrowed forms. Only the diff is
/// materialized, and it is `Copy`.
///
/// TODO: Rebuild only the diff column. Borrow the input column, build one owned
/// negated diff column from the borrowed diffs, and re-encode using the borrowed
/// row and time columns directly, so row and time bytes are copied once rather
/// than pushed per record. The serialized (`Align` / `Bytes`) input case needs
/// care, since all columns share a single buffer.
pub fn columnar_negate<'scope, T>(
    collection: ColumnarCollection<'scope, T, Row, Diff>,
) -> ColumnarCollection<'scope, T, Row, Diff>
where
    T: RenderTimestamp,
{
    collection
        .inner
        .unary::<ColumnBuilder<(Row, T, Diff)>, _, _, _>(
            Pipeline,
            "ColumnarNegate",
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for (v, t, d) in data.borrow().into_index_iter() {
                            let d = -Diff::into_owned(d);
                            session.give((v, t, &d));
                        }
                    });
                }
            },
        )
        .as_collection()
}

/// Consolidates a [`ColumnarCollection`] natively, without a row round-trip.
///
/// Mirrors the `Vec` arm's `CollectionExt::consolidate_named`, but keeps the
/// data columnar throughout: the input is reshaped into the `((Row, ()), T,
/// Diff)` shape the key batcher consumes, merged by [`Col2KeyBatcher`] under a
/// [`columnar_exchange`] pact, then unpacked back into a `Column`. Rows, times,
/// and diffs are pushed from their borrowed forms, so no owned [`Row`] is
/// materialized on the hot path.
///
/// This uses [`consolidate_pact`], not `mz_arrange_core`. A consolidate emits a
/// consolidated collection, so building and reading back a maintained trace
/// would be wasted work. [`Col2KeyBatcher`] and the `Vec` arm's `KeyBatcher`
/// produce the same `ColumnationStack` output and differ only in their input
/// chunker, so the unpack loop matches the `Vec` arm's.
pub fn columnar_consolidate<'scope, T>(
    collection: ColumnarCollection<'scope, T, Row, Diff>,
    name: &str,
) -> ColumnarCollection<'scope, T, Row, Diff>
where
    T: RenderTimestamp,
{
    // Reshape `(Row, T, Diff)` into `((Row, ()), T, Diff)`, the key-batcher
    // shape. The unit value carries no data; the whole `Row` is the key.
    let keyed = collection
        .inner
        .unary::<ColumnBuilder<((Row, ()), T, Diff)>, _, _, _>(
            Pipeline,
            &format!("ConsolidateKey {name}"),
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for (row, t, d) in data.borrow().into_index_iter() {
                            session.give(((row, ()), t, d));
                        }
                    });
                }
            },
        );

    let exchange =
        ExchangeCore::<ColumnBuilder<_>, _>::new_core(columnar_exchange::<Row, (), T, Diff>);
    let consolidated = consolidate_pact::<batcher::Chunker<_>, Col2KeyBatcher<Row, T, Diff>, _, _>(
        keyed, exchange, name,
    );

    // Unpack the sealed chains back into a `Column`, dropping the unit value.
    //
    // TODO: This drains a whole sealed snapshot in one activation, an
    // un-fueled burst hazard on large consolidations. It is the same behavior
    // as the `Vec` arm's `consolidate_named` unpack (see
    // `mz_timely_util::operator::consolidate_named`), not new here. A future
    // fuel fix should cover both arms, so the burst is not fixed on one and
    // left on the other.
    consolidated
        .unary::<ColumnBuilder<(Row, T, Diff)>, _, _, _>(
            Pipeline,
            &format!("Unpack {name}"),
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for ((row, ()), t, d) in
                            data.iter().flatten().flat_map(|chunk| chunk.iter())
                        {
                            session.give((row, t, d));
                        }
                    });
                }
            },
        )
        .as_collection()
}

/// Repacks a row-based collection into columnar batches.
///
/// The sanctioned leaf encode from `Vec` to the columnar edge, visible in
/// rendered dataflows as a `VecToColumnar` operator. Row-serializing leaves
/// (sinks, `LetRec`, temporal bucketing, join internals, TopK fallible-limit)
/// stay `Vec`-shaped internally and encode to the columnar edge at their
/// boundary through this pass. Repacking copies row bytes but allocates no
/// per-record `Row`s.
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
/// The sanctioned leaf decode from the columnar edge to `Vec`, visible in
/// rendered dataflows as a `ColumnarToVec` operator. Row-serializing leaves
/// (sinks, `LetRec`, temporal bucketing, join internals, TopK fallible-limit)
/// decode the columnar edge at their boundary through this pass. Decoding
/// allocates an owned [`Row`] per record, so it stays confined to those leaf
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
        // Accumulation and cancellation across two distinct timestamps:
        // `row1` accumulates to two at t=0 and to one at t=1 (kept separate by
        // time); `row2` cancels at t=0 and `row3` cancels at t=1, so both are
        // absent from the output.
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
