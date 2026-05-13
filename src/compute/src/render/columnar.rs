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
//! both variants before any producer emits the columnar variant. Once all
//! consumers are ready, a single switch flips producers to columnar end-to-end.
//!
//! Within a Plan node, operators may freely materialize Vec collections; only
//! the inter-node edge format is constrained. A decode from columnar to Vec at
//! a consumer's input is acceptable only when the consumer would have decoded
//! `Row` to [`mz_repr::Datum`] anyway. Pure passthrough consumers (Negate,
//! Union) must round-trip the columnar variant without decoding.
//!
//! See `.claude/plans/columnar_consumer_first.md` for the staged plan.

#![allow(dead_code)]
// Phase A defines the type with `todo!()` arms that will be filled in when
// producers begin emitting columnar batches.
#![allow(clippy::todo)]

use columnar::Columnar;
use differential_dataflow::{Data, VecCollection};
use mz_repr::{DatumVec, DatumVecBorrow, Diff, Row};
use mz_timely_util::columnar::Column;
use mz_timely_util::operator::CollectionExt;
use timely::dataflow::{Scope, Stream, StreamVec};
use timely::progress::Timestamp;

use crate::render::RenderTimestamp;
use crate::typedefs::KeyBatcher;

/// A columnar collection of `(D, T, R)` updates traveling on a compute
/// dataflow edge.
///
/// Mirrors differential's [`VecCollection<'scope, T, D, R>`] shape and
/// parameters; the underlying container is [`Column<(D, T, R)>`] instead of
/// `Vec<(D, T, R)>`.
pub struct ColumnarCollection<'scope, T, D, R>
where
    T: Timestamp,
    (D, T, R): Columnar,
{
    /// The underlying timely stream of columnar batches.
    pub inner: Stream<'scope, T, Column<(D, T, R)>>,
}

impl<'scope, T, D, R> Clone for ColumnarCollection<'scope, T, D, R>
where
    T: Timestamp,
    (D, T, R): Columnar,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// A dataflow edge carrying records as either a row-based [`VecCollection`] or
/// a [`ColumnarCollection`].
///
/// Producers choose a variant; consumers must accept either. Mixing variants
/// across a `concat` is rejected during the transition.
#[derive(Clone)]
pub enum CollectionEdge<'scope, T: RenderTimestamp> {
    /// Row-formatted collection. Today's default for every producer.
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
            CollectionEdge::Columnar(_) => {
                // Sub-region entry for columnar collections will be wired when
                // the first producer emits this variant.
                todo!("CollectionEdge::Columnar::enter_region")
            }
        }
    }

    /// Leaves a sub-region back to the outer scope.
    pub fn leave_region<'outer>(self, outer: Scope<'outer, T>) -> CollectionEdge<'outer, T> {
        match self {
            CollectionEdge::Vec(c) => CollectionEdge::Vec(c.leave_region(outer)),
            CollectionEdge::Columnar(_) => {
                todo!("CollectionEdge::Columnar::leave_region")
            }
        }
    }

    /// Extracts the [`VecCollection`] arm, panicking on the columnar arm.
    ///
    /// This is a transitional fence used at consumer sites that have not yet
    /// been converted to handle the columnar arm natively. Each Phase-B
    /// consumer PR removes one call.
    pub fn expect_vec(self) -> VecCollection<'scope, T, Row, Diff> {
        match self {
            CollectionEdge::Vec(c) => c,
            CollectionEdge::Columnar(_) => panic!(
                "CollectionEdge::expect_vec called on columnar arm; consumer must convert first"
            ),
        }
    }

    /// Borrows the [`VecCollection`] arm mutably, panicking on the columnar arm.
    ///
    /// Transitional fence; see [`Self::expect_vec`].
    pub fn expect_vec_mut(&mut self) -> &mut VecCollection<'scope, T, Row, Diff> {
        match self {
            CollectionEdge::Vec(c) => c,
            CollectionEdge::Columnar(_) => panic!(
                "CollectionEdge::expect_vec_mut called on columnar arm; consumer must convert first"
            ),
        }
    }

    /// Negates the diff on every record in this edge.
    ///
    /// Preserves variant. The columnar arm uses [`columnar_negate`], which
    /// flips the diff column without decoding the data column.
    pub fn negate(self) -> Self {
        match self {
            CollectionEdge::Vec(c) => CollectionEdge::Vec(c.negate()),
            CollectionEdge::Columnar(c) => CollectionEdge::Columnar(columnar_negate(c)),
        }
    }

    /// Concatenates a collection of edges that all share the same variant.
    ///
    /// Mixing variants is rejected during the transition.
    pub fn concat_many<I>(scope: Scope<'scope, T>, edges: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        let mut vec_arm = Vec::new();
        for edge in edges {
            match edge {
                CollectionEdge::Vec(c) => vec_arm.push(c),
                CollectionEdge::Columnar(_) => {
                    // No producer emits the columnar arm yet; once one does,
                    // this branch must either concatenate columnar streams or
                    // reject mixed-variant inputs explicitly.
                    todo!("CollectionEdge::concat_many: columnar arm");
                }
            }
        }
        CollectionEdge::Vec(differential_dataflow::collection::concatenate(scope, vec_arm))
    }

    /// Applies `logic` to each record in this edge, exposing the record as a
    /// borrowed [`DatumVecBorrow`].
    ///
    /// `max_demand` bounds the number of columns decoded per row; pass
    /// `usize::MAX` to decode all columns.
    ///
    /// This is the canonical unified entry point for "decoding consumers"
    /// (operators that read [`mz_repr::Datum`]s from each row anyway). The
    /// Vec arm uses [`DatumVec::borrow_with_limit`] on each [`Row`]; the
    /// Columnar arm iterates the columnar batch directly without going
    /// through an owned [`Row`].
    pub fn flat_map_datums<D, I, L>(
        self,
        max_demand: usize,
        mut logic: L,
    ) -> StreamVec<'scope, T, I::Item>
    where
        I: IntoIterator<Item = (D, T, Diff)>,
        D: Data,
        L: for<'a> FnMut(&'a mut DatumVecBorrow<'_>, T, Diff) -> I + 'static,
    {
        match self {
            CollectionEdge::Vec(c) => {
                use timely::dataflow::operators::vec::Map;
                let mut datums = DatumVec::new();
                c.inner.flat_map(move |(v, t, d)| {
                    logic(&mut datums.borrow_with_limit(&v, max_demand), t, d)
                })
            }
            CollectionEdge::Columnar(_) => {
                // Implementation will iterate `Column<(Row, T, Diff)>::borrow()`
                // via the `Rows<_>::Index` impl (yielding `&RowRef`) and call
                // `DatumVec::borrow_with_limit(row_ref, max_demand)` per row.
                todo!("CollectionEdge::flat_map_datums: columnar arm")
            }
        }
    }

    /// Consolidates updates in the edge, preserving variant.
    pub fn consolidate_named(self, name: &str) -> Self {
        match self {
            CollectionEdge::Vec(c) => {
                CollectionEdge::Vec(CollectionExt::consolidate_named::<KeyBatcher<_, _, _>>(c, name))
            }
            CollectionEdge::Columnar(_) => {
                todo!("CollectionEdge::Columnar::consolidate_named")
            }
        }
    }
}

/// Negates the diff column of every batch in a [`ColumnarCollection`] without
/// decoding the data column.
pub fn columnar_negate<'scope, T, D, R>(
    collection: ColumnarCollection<'scope, T, D, R>,
) -> ColumnarCollection<'scope, T, D, R>
where
    T: Timestamp,
    (D, T, R): Columnar,
{
    // Implementation is deferred until the first producer emits the
    // [`CollectionEdge::Columnar`] variant. The signature is fixed here so
    // consumers can target it during the consumer-first phase.
    let _ = collection;
    todo!("columnar_negate: flip diff column in place over Column<(D, T, R)>")
}
