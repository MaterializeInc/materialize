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
//! ([`mz_timely_util::columnar::Column`]) batches of `(Row, T, Diff)` updates.
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

use differential_dataflow::VecCollection;
use mz_repr::{Diff, Row};
use mz_timely_util::columnar::Column;
use timely::dataflow::{Scope, Stream};

use crate::render::RenderTimestamp;

/// Container for a columnar batch of `(Row, T, Diff)` updates traveling on a
/// compute dataflow edge.
pub type ColumnarBatch<T> = Column<(Row, T, Diff)>;

/// A dataflow edge carrying records as either a row-based [`VecCollection`] or
/// a columnar [`Stream`] of [`ColumnarBatch`]es.
///
/// Producers choose a variant; consumers must accept either. Mixing variants
/// across a `concat` is rejected during the transition: see
/// [`CollectionEdge::concat`].
pub enum CollectionEdge<'scope, T: RenderTimestamp> {
    /// Row-formatted collection. Today's default for every producer.
    Vec(VecCollection<'scope, T, Row, Diff>),
    /// Columnar batch stream. Currently unused by any producer; reserved for
    /// the producer flip at the end of the migration.
    Columnar(Stream<'scope, T, ColumnarBatch<T>>),
}

impl<'scope, T: RenderTimestamp> CollectionEdge<'scope, T> {
    /// The scope containing this edge.
    pub fn scope(&self) -> Scope<'scope, T> {
        match self {
            CollectionEdge::Vec(c) => c.inner.scope(),
            CollectionEdge::Columnar(s) => s.scope(),
        }
    }

    /// Brings the edge into a sub-region of its current scope.
    pub fn enter_region<'inner>(self, region: Scope<'inner, T>) -> CollectionEdge<'inner, T> {
        match self {
            CollectionEdge::Vec(c) => CollectionEdge::Vec(c.enter_region(region)),
            CollectionEdge::Columnar(_) => {
                // Sub-region entry for columnar batches will be wired when the
                // first producer emits this variant.
                todo!("CollectionEdge::Columnar::enter_region")
            }
        }
    }

    /// Negates the diff on every record in this edge.
    ///
    /// Preserves variant. The columnar arm uses [`columnar_negate`], which
    /// flips the diff column without decoding the data column.
    pub fn negate(self) -> Self {
        match self {
            CollectionEdge::Vec(c) => CollectionEdge::Vec(c.negate()),
            CollectionEdge::Columnar(s) => CollectionEdge::Columnar(columnar_negate(s)),
        }
    }
}

/// Negates the diff column of every batch in a columnar stream without
/// decoding the data column.
pub fn columnar_negate<'scope, T>(
    stream: Stream<'scope, T, ColumnarBatch<T>>,
) -> Stream<'scope, T, ColumnarBatch<T>>
where
    T: RenderTimestamp,
{
    // Implementation is deferred until the first producer emits the
    // [`CollectionEdge::Columnar`] variant. The signature is fixed here so
    // consumers can target it during the consumer-first phase.
    let _ = stream;
    todo!("columnar_negate: flip diff column in place over Column<(Row, T, Diff)>")
}
