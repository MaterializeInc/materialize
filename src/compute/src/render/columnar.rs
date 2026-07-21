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
//! Defines [`CollectionEdge`], a wrapper that lets dataflow edges between Plan
//! nodes carry either row-based ([`VecCollection`]) or columnar
//! ([`ColumnarCollection`]) batches of `(D, T, R)` updates.
//!
//! # Migration model
//!
//! The migration is consumer-first: every Plan-node consumer learns to accept
//! both variants before any producer emits the columnar variant. Producers can
//! then flip to columnar one at a time.
//!
//! Within a Plan node, operators may freely materialize Vec collections; only
//! the inter-node edge format is constrained. A decode from columnar to Vec at
//! a consumer's input is acceptable only when the consumer would have decoded
//! `Row` to [`mz_repr::Datum`] anyway. Pure passthrough consumers (Negate,
//! Union) round-trip the columnar variant without decoding.
//!
//! Consumers that have not yet learned the columnar form fall back to
//! [`CollectionEdge::into_vec`], which decodes through the named
//! `ColumnarToVec` operator. Repack seams therefore stay visible in dataflow
//! introspection, so they can be found and retired.

use columnar::{Columnar, Index};
use differential_dataflow::{AsCollection, Collection, VecCollection};
use mz_repr::{DatumVec, DatumVecBorrow, Diff, Row};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2KeyBatcher, columnar_exchange};
use mz_timely_util::operator::{CollectionExt, consolidate_pact};
use timely::ContainerBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{Operator, OutputBuilder};
use timely::dataflow::{Scope, Stream, StreamVec};

use crate::render::RenderTimestamp;
use crate::render::context::{ECB, Session};
use crate::render::errors::DataflowErrorSer;
use crate::typedefs::KeyBatcher;

/// A columnar collection of `(D, T, R)` updates traveling on a compute
/// dataflow edge.
///
/// Mirrors differential's [`VecCollection<'scope, T, D, R>`]; the underlying
/// container is [`Column<(D, T, R)>`] instead of `Vec<(D, T, R)>`.
pub type ColumnarCollection<'scope, T, D, R> = Collection<'scope, T, Column<(D, T, R)>>;

/// A dataflow edge carrying records as either a row-based [`VecCollection`] or
/// a [`ColumnarCollection`].
///
/// Producers choose a variant; consumers must accept either. Variant-mixing
/// `concat`s repack the row-based inputs and produce the columnar variant.
#[derive(Clone)]
pub enum CollectionEdge<'scope, T: RenderTimestamp> {
    /// Row-formatted collection. No producer constructs this after the
    /// migration; the variant and its remaining match arms are removed when the
    /// enum collapses to a columnar alias.
    #[allow(dead_code)]
    Vec(VecCollection<'scope, T, Row, Diff>),
    /// Columnar collection. Currently unused by any producer; reserved for the
    /// producer flip at the end of the migration.
    Columnar(ColumnarCollection<'scope, T, Row, Diff>),
}

impl<'scope, T: RenderTimestamp> CollectionEdge<'scope, T> {
    /// The scope containing this edge.
    pub fn scope(&self) -> Scope<'scope, T> {
        match self {
            CollectionEdge::Vec(c) => c.inner.scope(),
            CollectionEdge::Columnar(c) => c.inner.scope(),
        }
    }

    /// Brings the edge into a sub-region of its current scope.
    pub fn enter_region<'inner>(self, region: Scope<'inner, T>) -> CollectionEdge<'inner, T> {
        match self {
            CollectionEdge::Vec(c) => CollectionEdge::Vec(c.enter_region(region)),
            CollectionEdge::Columnar(c) => CollectionEdge::Columnar(c.enter_region(region)),
        }
    }

    /// Leaves a sub-region back to the outer scope.
    pub fn leave_region<'outer>(self, outer: Scope<'outer, T>) -> CollectionEdge<'outer, T> {
        match self {
            CollectionEdge::Vec(c) => CollectionEdge::Vec(c.leave_region(outer)),
            CollectionEdge::Columnar(c) => CollectionEdge::Columnar(c.leave_region(outer)),
        }
    }

    /// The edge as a row-based [`VecCollection`].
    ///
    /// The Vec arm is returned as is. The columnar arm decodes through
    /// [`columnar_to_vec`], which allocates an owned [`Row`] per record.
    /// Consumers that can work on the columnar form directly should do so
    /// instead of calling this.
    pub fn into_vec(self) -> VecCollection<'scope, T, Row, Diff> {
        match self {
            CollectionEdge::Vec(c) => c,
            CollectionEdge::Columnar(c) => columnar_to_vec(c),
        }
    }

    /// Negates the diff on every record in this edge.
    ///
    /// Preserves variant. The columnar arm uses [`columnar_negate`], which
    /// negates diffs without decoding rows.
    pub fn negate(self) -> Self {
        match self {
            CollectionEdge::Vec(c) => CollectionEdge::Vec(c.negate()),
            CollectionEdge::Columnar(c) => CollectionEdge::Columnar(columnar_negate(c)),
        }
    }

    /// Concatenates a collection of edges.
    ///
    /// Every producer emits the columnar variant, so the inputs concatenate
    /// natively into the columnar variant.
    pub fn concat_many<I>(scope: Scope<'scope, T>, edges: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        let cols = edges.into_iter().map(|edge| match edge {
            CollectionEdge::Columnar(c) => c,
            // No producer emits `Vec` after the migration, so a `Vec` input
            // cannot reach here. The `Vec` arm and this `unreachable!` are
            // removed together when the enum collapses to a columnar alias.
            CollectionEdge::Vec(_) => unreachable!("no producer emits a `Vec` edge"),
        });
        CollectionEdge::Columnar(differential_dataflow::collection::concatenate(
            scope,
            cols.collect::<Vec<_>>(),
        ))
    }

    /// Applies `logic` to each record in this edge, exposing the record as a
    /// borrowed [`DatumVecBorrow`] and giving it ok and err output sessions.
    ///
    /// `max_demand` bounds the number of columns decoded per row; pass
    /// `usize::MAX` to decode all columns.
    ///
    /// This is the canonical unified entry point for "decoding consumers"
    /// (operators that read [`mz_repr::Datum`]s from each row anyway). The
    /// Vec arm uses [`DatumVec::borrow_with_limit`] on each [`Row`]; the
    /// Columnar arm iterates the columnar batch directly without going
    /// through an owned [`Row`].
    pub fn flat_map_datums<DCB, L>(
        self,
        max_demand: usize,
        mut logic: L,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        StreamVec<'scope, T, (DataflowErrorSer, T, Diff)>,
    )
    where
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
        match self {
            CollectionEdge::Vec(c) => {
                let scope = c.inner.scope();
                let mut builder = OperatorBuilder::new("CollectionFlatMap".to_string(), scope);
                let (ok_output, ok_stream) = builder.new_output();
                let mut ok_output = OutputBuilder::<_, DCB>::from(ok_output);
                let (err_output, err_stream) = builder.new_output();
                let mut err_output = OutputBuilder::<_, ECB<T>>::from(err_output);
                let mut input = builder.new_input(c.inner, Pipeline);
                builder.build(move |_capabilities| {
                    let mut datums = DatumVec::new();
                    move |_frontiers| {
                        let mut ok_output = ok_output.activate();
                        let mut err_output = err_output.activate();
                        input.for_each(|time, data| {
                            // Retain the input capability to derive a `Capability` for each output;
                            // the `Session` type alias is fixed to `Capability<T>`.
                            let ok_cap = time.retain(0);
                            let err_cap = time.retain(1);
                            let mut ok_session = ok_output.session_with_builder(&ok_cap);
                            let mut err_session = err_output.session_with_builder(&err_cap);
                            for (v, t, d) in data.drain(..) {
                                logic(
                                    &mut datums.borrow_with_limit(&v, max_demand),
                                    t,
                                    d,
                                    &mut ok_session,
                                    &mut err_session,
                                );
                            }
                        });
                    }
                });
                (ok_stream, err_stream)
            }
            CollectionEdge::Columnar(c) => {
                let scope = c.inner.scope();
                let mut builder = OperatorBuilder::new("CollectionFlatMap".to_string(), scope);
                let (ok_output, ok_stream) = builder.new_output();
                let mut ok_output = OutputBuilder::<_, DCB>::from(ok_output);
                let (err_output, err_stream) = builder.new_output();
                let mut err_output = OutputBuilder::<_, ECB<T>>::from(err_output);
                let mut input = builder.new_input(c.inner, Pipeline);
                builder.build(move |_capabilities| {
                    let mut datums = DatumVec::new();
                    move |_frontiers| {
                        let mut ok_output = ok_output.activate();
                        let mut err_output = err_output.activate();
                        input.for_each(|time, data| {
                            // Retain the input capability to derive a `Capability` for each output;
                            // the `Session` type alias is fixed to `Capability<T>`.
                            let ok_cap = time.retain(0);
                            let err_cap = time.retain(1);
                            let mut ok_session = ok_output.session_with_builder(&ok_cap);
                            let mut err_session = err_output.session_with_builder(&err_cap);
                            // Rows are read from the borrowed column, never
                            // materialized as owned `Row`s.
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
        }
    }

    /// Consolidates updates in the edge, preserving variant.
    pub fn consolidate_named(self, name: &str) -> Self {
        match self {
            CollectionEdge::Vec(c) => CollectionEdge::Vec(CollectionExt::consolidate_named::<
                KeyBatcher<_, _, _>,
            >(c, name)),
            CollectionEdge::Columnar(c) => CollectionEdge::Columnar(columnar_consolidate(c, name)),
        }
    }
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
/// Mirrors the `Vec` arm's [`CollectionExt::consolidate_named`], but keeps the
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
/// A transitional seam-healer, visible in rendered dataflows as a
/// `VecToColumnar` operator. Repacking copies row bytes but allocates no
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
/// A transitional seam-healer, visible in rendered dataflows as a
/// `ColumnarToVec` operator. Decoding allocates an owned [`Row`] per record,
/// so it should only guard consumers that have not yet learned the columnar
/// form.
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
    fn negate_flips_diffs_on_columnar_arm() {
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
                let edge = CollectionEdge::Columnar(vec_to_columnar(collection)).negate();
                assert!(matches!(edge, CollectionEdge::Columnar(_)));
                let captured = edge.into_vec().inner.capture();
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
    fn concat_many_mixed_upgrades_to_columnar() {
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
                let edge = CollectionEdge::concat_many(
                    scope,
                    [
                        CollectionEdge::Vec(collection1),
                        CollectionEdge::Columnar(vec_to_columnar(collection2)),
                    ],
                );
                assert!(matches!(edge, CollectionEdge::Columnar(_)));
                let captured = edge.into_vec().inner.capture();
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
        // Project the first datum of each row, exercising `max_demand` on both
        // arms. The two captures must extract identical updates.
        let rows = test_rows();
        let (vec_captured, col_captured) = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut input, collection) = scope.new_collection();
                let mut captures = Vec::new();
                for edge in [
                    CollectionEdge::Vec(collection.clone()),
                    CollectionEdge::Columnar(vec_to_columnar(collection)),
                ] {
                    let (oks, _errs) = edge.flat_map_datums::<RowBuilder, _>(
                        1,
                        |datums, t, d, ok_session, _err_session| {
                            ok_session.give((Row::pack(datums.iter()), t, d));
                            1
                        },
                    );
                    captures.push(oks.capture());
                }
                let col = captures.pop().unwrap();
                let vec = captures.pop().unwrap();
                for row in rows {
                    input.update(row, Diff::ONE);
                }
                input.advance_to(Timestamp::from(1_u64));
                input.flush();
                (vec, col)
            })
        });
        let vec_updates = extract_sorted(vec_captured);
        assert_eq!(vec_updates, extract_sorted(col_captured));
        // Each output row retains at most the first datum of its input.
        assert!(vec_updates.iter().all(|(r, _, _)| r.iter().count() <= 1));
    }

    #[mz_ore::test]
    fn consolidate_named_preserves_columnar() {
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

        // The columnar arm keeps the `Columnar` variant. No-ColumnarToVec is a
        // by-inspection property: `consolidate_named`'s columnar arm calls
        // `columnar_consolidate` (native `Col2KeyBatcher` merge), never
        // `columnar_to_vec`. The `into_vec` below is the capture harness
        // decoding for the test only, not part of the consolidate.
        let (vec_captured, col_captured) = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut input, collection) = scope.new_collection();
                let mut captures = Vec::new();
                for edge in [
                    CollectionEdge::Vec(collection.clone()),
                    CollectionEdge::Columnar(vec_to_columnar(collection)),
                ] {
                    let is_columnar = matches!(edge, CollectionEdge::Columnar(_));
                    let edge = edge.consolidate_named("Test");
                    assert_eq!(matches!(edge, CollectionEdge::Columnar(_)), is_columnar);
                    captures.push(edge.into_vec().inner.capture());
                }
                let col = captures.pop().unwrap();
                let vec = captures.pop().unwrap();
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
                (vec, col)
            })
        });
        let vec_updates = extract_sorted(vec_captured);
        assert_eq!(vec_updates, expected);
        assert_eq!(extract_sorted(col_captured), vec_updates);
    }
}
