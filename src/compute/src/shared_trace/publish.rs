// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The publisher half: the publication point's owner-facing API and the attachment of a
//! trace to it.

use std::sync::Arc;

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::{Trace, TraceReader};
use mz_timely_util::shared_trace::{Shared, SharedReader, SharedSpine};
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::worker::Worker;

/// A publication point for one arrangement.
///
/// Holding it keeps the point registered. Once it and every reader minted from it are dropped, the
/// writer detaches the point at its next mutation, see `SharedSpine::detach_unreachable`.
pub(crate) struct Published<Tr: TraceReader> {
    pub(super) shared: Arc<Shared<Tr::Batch>>,
}

impl<Tr: TraceReader> Published<Tr>
where
    // A reader cuts a totally ordered chain.
    Tr::Time: TotalOrder,
{
    /// Creates a publication point. It starts unattached: an empty chain with `since` and `upper`
    /// at the minimum time and no writer, until one attaches via [`adopt_trace`].
    ///
    /// A reader may mint handles and build imports over it before that happens, but they produce
    /// nothing (the import frontier stays at the minimum) until attachment seeds them. Attachment
    /// fills the same `Arc`, so a handle captured by value at construction (as a differential join
    /// captures its input trace) observes the filled chain: the handle is a live proxy into the
    /// shared state, not a snapshot.
    pub(crate) fn new() -> Self {
        Published {
            shared: Arc::new(Shared::new()),
        }
    }

    /// Hands out a `Clone + Send` handle to the published arrangement.
    ///
    /// The handle registers a logical hold at the current published `since`, so the arrangement
    /// will not compact past it until the handle (and all its clones) drop.
    pub(crate) fn handle(&self) -> SharedReader<Tr::Batch> {
        self.shared.reader()
    }

    /// A hold on the published arrangement for a runtime that reads it as a peer: logical only, at
    /// the join of `as_of` and the published `since`. See `Shared::reader_at_least`.
    pub(crate) fn peer_handle(&self, as_of: &Antichain<Tr::Time>) -> SharedReader<Tr::Batch> {
        self.shared.reader_at_least(as_of)
    }
}

/// Attaches `trace` to `point`, created by [`Published::new`]. `worker` must be the worker that
/// maintains `trace`.
///
/// From here on the trace mirrors its chain and frontiers into the point after every mutation,
/// and applies the point's holds to its own compaction. Attachment is late-binding: a reader may
/// build handles and imports over `point` before the trace is rendered, and they are seeded with
/// the trace's contents now.
///
/// `on_seal` fires once per publish on which the published `upper` advances, after the state
/// lock is released and `upper` reflects the advance. A fast-path peek parked on this
/// arrangement's seal is re-examined only through this callback, so it must observe the
/// advanced `upper`. See the lost-wakeup contract on
/// `crate::sharing::ArrangementSharingRegistry::notify`.
pub(crate) fn adopt_trace<Inner, F>(
    trace: &TraceAgent<SharedSpine<Inner>>,
    worker: &Worker,
    point: &Published<SharedSpine<Inner>>,
    on_seal: F,
) where
    Inner: Trace + 'static,
    Inner::Time: TotalOrder,
    F: Fn() + 'static,
{
    // A reader moving a hold wakes the arrange operator, whose `exert` applies it to the trace.
    let activator = worker.sync_activator_for(trace.operator().address.to_vec());
    trace.trace_box_unstable().borrow().trace().attach(
        Arc::clone(&point.shared),
        activator,
        on_seal,
    );
}
