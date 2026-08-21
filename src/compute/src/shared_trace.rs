// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sharing arrangements across timely runtimes.
//!
//! Every spine in [`crate::typedefs`] is a
//! [`SharedSpine`](mz_timely_util::shared_trace::SharedSpine): a trace that, once attached to a
//! publication point, mirrors its batch chain and frontiers there after every mutation, and applies
//! the holds readers register there to its own compaction. This module is the Materialize-side
//! glue over that primitive.
//!
//! * [`Published`] is a publication point plus the *standing hold*, a logical hold with no reader
//!   behind it that tracks the frontier the importing runtime has applied.
//! * [`PublishArrangement::adopt`] attaches an arrangement's trace to a point on the owning worker.
//! * [`SharedReader`] is the `Clone + Send` reader, implementing
//!   [`TraceReader`] so it drives compaction and cursors
//!   like any trace handle, from any thread. [`SharedReader::import_frontier_core`] replays the
//!   shared arrangement into another scope.

mod publish;

use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_repr::{Diff, Timestamp};
use mz_timely_util::shared_trace::SharedReader;

pub(crate) use self::publish::{Diagnostics, PublishArrangement, Published, adopt_trace};

use crate::typedefs::{ErrSpine, RowRowSpine};

/// A `Send` reader handle for a published `oks` arrangement.
pub(crate) type SharedOksHandle =
    SharedReader<<RowRowSpine<Timestamp, Diff> as TraceReader>::Batch>;
/// A `Send` reader handle for a published `errs` arrangement.
pub(crate) type SharedErrsHandle = SharedReader<<ErrSpine<Timestamp, Diff> as TraceReader>::Batch>;

/// A [`SharedOksHandle`] imported as a static `as_of` snapshot, wrapped in a `TraceFrontier`.
///
/// The interactive runtime imports a shared index via [`SharedReader::import_frontier_core`],
/// which returns a `TraceFrontier`-wrapped arrangement whose times are advanced to the dataflow
/// `as_of` and bounded by `until`. Mirrors the maintenance import's `RowRowEnter`, which is likewise
/// `TraceFrontier`-wrapped.
pub(crate) type SharedOksFrontier = TraceFrontier<SharedOksHandle>;
/// An `ErrSpine` counterpart to [`SharedOksFrontier`].
pub(crate) type SharedErrsFrontier = TraceFrontier<SharedErrsHandle>;

/// A [`SharedOksFrontier`] entered into a render scope whose timestamp is `TEnter`.
pub(crate) type SharedOksEnter<TEnter> = TraceEnter<SharedOksFrontier, TEnter>;
/// A [`SharedErrsFrontier`] entered into a render scope whose timestamp is `TEnter`.
pub(crate) type SharedErrsEnter<TEnter> = TraceEnter<SharedErrsFrontier, TEnter>;

// `pub(crate)`: sibling test modules reach the probes here rather than duplicating them. The peek
// and render tests in `crate::render` and `crate::sharing` read a published arrangement through
// `SharedReaderExt::snapshot_at` and inspect holds through `Published::logical_holds`.
#[cfg(test)]
pub(crate) mod tests;
