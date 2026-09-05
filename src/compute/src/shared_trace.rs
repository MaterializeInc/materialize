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
//! An arrangement is normally readable only from the worker that maintains it. Its batches already
//! cross threads, being `Arc`-backed through `mz_row_spine::ArcBatch`, but its trace handle is a
//! `TraceAgent`, which is `Rc<RefCell<..>>` and pinned to one thread. This module lets a worker
//! publish an arrangement through a *publication point*, from which readers on other threads take
//! consistent snapshots or import the arrangement into a second timely runtime.
//!
//! The unit that crosses the thread boundary is not the
//! [`Spine`](differential_dataflow::trace::implementations::spine_fueled::Spine), which holds
//! thread-local state and has a single writer, but the spine's *contents*: a chain of immutable
//! `Arc`'d batches together with the trace's `since` and `upper` frontiers. Because batches are
//! immutable, a chain plus frontiers is a self-describing, consistent view. When the publishing
//! worker later merges batches, a reader holding an older chain is unaffected: its `Arc`s keep the
//! pre-merge batches alive until it drops them.
//!
//! ## Pieces
//!
//! * [`PublishArrangement::adopt`] attaches a publisher to an arrangement on the owning worker,
//!   filling a [`Published`] whose [`Published::handle`] hands out `Clone + Send`
//!   [`SharedTraceHandle`]s.
//! * [`SharedTraceHandle`] implements
//!   [`TraceReader`](differential_dataflow::trace::TraceReader), so it drives compaction and
//!   cursors like any trace handle, from any thread.
//!   [`SharedTraceHandle::import_snapshot_at`] replays the shared arrangement into another scope.
//!
//! ## Compaction
//!
//! A publication point is differential's `TraceBox` for readers that are not agents of the trace: it
//! accumulates their holds in a `MutableAntichain` and forwards its frontier to the publisher's own
//! `TraceAgent`, the sole writer of the trace's compaction frontiers. Each handle mirrors its own
//! frontier locally and adjusts the accumulation as a delta, the way a `TraceAgent` does.
//!
//! Logical compaction decides which times stay *distinguishable*, physical compaction which batches
//! may *merge*. A reader needs distinguishability at its `as_of`, and a batch boundary at each
//! frontier it passes to `cursor_through`. It needs no boundary at its `as_of`: an import is seeded
//! with the whole chain and wrapped in `TraceFrontier`, which advances times rather than cutting. The
//! two axes therefore carry different frontiers, and `since` is never the right physical one.
//!
//! *Coverage* is the frontier through which the published chain is complete, which is its last
//! batch's upper. A reader seeded with that chain makes its first cut there, so that is where its
//! physical hold starts. With no reader registered the publisher forwards the coverage itself, the
//! frontier an unshared index gets from `crate::arrangement::manager::TraceManager::maintenance`.
//!
//! The *standing hold* is a logical hold with no reader behind it. A reader registers only once its
//! dataflow is built, while the agent's setter joins and so only ever advances, so this hold tracks
//! the frontier the importing runtime has applied and keeps the agent at or below every `as_of` that
//! runtime can still present.

mod handle;
mod publish;
mod state;

use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_repr::{Diff, Timestamp};

pub(crate) use self::handle::SharedTraceHandle;
pub(crate) use self::publish::{Diagnostics, PublishArrangement, Published};

use crate::typedefs::{ErrSpine, RowRowSpine};

/// A `Send` reader handle for a published `oks` arrangement.
pub(crate) type SharedOksHandle = SharedTraceHandle<RowRowSpine<Timestamp, Diff>>;
/// A `Send` reader handle for a published `errs` arrangement.
pub(crate) type SharedErrsHandle = SharedTraceHandle<ErrSpine<Timestamp, Diff>>;

/// A [`SharedOksHandle`] imported as a static `as_of` snapshot, wrapped in a `TraceFrontier`.
///
/// The interactive runtime imports a shared index via [`SharedTraceHandle::import_snapshot_at`],
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
// `SharedTraceHandle::snapshot_at` and inspect holds through `Published::logical_holds`.
#[cfg(test)]
pub(crate) mod tests;
