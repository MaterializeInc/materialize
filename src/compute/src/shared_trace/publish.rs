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

use differential_dataflow::trace::{Trace, TraceReader};
use mz_timely_util::arrange_agent::TraceAgent;
use mz_timely_util::shared_trace::Shared;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::worker::Worker;

/// A publication point for one arrangement.
///
/// Holding it keeps the point registered. Once it and every reader minted from it are dropped, the
/// trace stops publishing into the point at its next mutation.
pub(crate) struct Published<Tr: TraceReader> {
    pub(crate) shared: Arc<Shared<Tr::Time, Tr::Batch>>,
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

    /// Whether the trace that attached to this point has closed or dropped. A frozen point never
    /// publishes again and refuses another trace.
    pub(crate) fn is_frozen(&self) -> bool {
        self.shared.is_frozen()
    }

    /// Hands out a `Clone + Send` handle to the published arrangement.
    ///
    /// The handle registers a logical hold at the current published `since`, so the arrangement
    /// will not compact past it until the handle (and all its clones) drop. Test-only: production
    /// reads hold the point through a `BindingReader`.
    #[cfg(test)]
    pub(crate) fn handle(&self) -> mz_timely_util::shared_trace::SharedReader<Tr::Time, Tr::Batch> {
        self.shared.reader()
    }

    /// The `(applied, requested)` logical compaction frontiers of this point, see
    /// `TraceFrontiers::requested_logical`.
    pub(crate) fn logical_frontiers(&self) -> (Antichain<Tr::Time>, Antichain<Tr::Time>) {
        self.shared.logical_frontiers()
    }
}

/// Attaches `trace` to `point`, created by [`Published::new`]. `worker` must be the worker that
/// maintains `trace`, and no trace may have attached to `point` before, which panics.
///
/// From here on the trace mirrors its span chain and frontiers into the point after every mutation,
/// and applies the point's holds to its own compaction. Attachment is late-binding: a reader may
/// build handles and imports over `point` before the trace is rendered, and they are seeded with
/// the trace's contents now.
///
/// `on_seal` fires once per publish on which the published `upper` advances, after the state
/// lock is released and `upper` reflects the advance.
pub(crate) fn adopt_trace<Tr, F>(
    trace: &TraceAgent<Tr>,
    worker: &Worker,
    point: &Published<Tr>,
    on_seal: F,
) where
    Tr: Trace + 'static,
    Tr::Time: TotalOrder,
    F: Fn() + 'static,
{
    // A reader moving a hold wakes the arrange operator, whose `exert` applies it to the trace.
    let activator = worker.sync_activator_for(trace.operator().address.to_vec());
    trace.publish(Arc::clone(&point.shared), activator, on_seal);
}
