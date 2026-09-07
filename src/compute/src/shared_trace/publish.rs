// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The publisher half: the publication point's owner-facing API and the attachment of an
//! arrangement to it.

use std::sync::{Arc, Mutex};

use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::{Trace, TraceReader};
use mz_timely_util::shared_trace::{Shared, SharedReader, SharedSpine};
use timely::order::TotalOrder;
use timely::progress::Antichain;

use crate::shared_trace::handle::SharedTraceHandle;

/// Why a publication point refused an `as_of`.
///
/// Read off the point rather than off a handle, so a failure path registers no hold on its way to a
/// panic, and so the caller reports the point that actually refused rather than a sibling.
pub(crate) struct Diagnostics<T> {
    /// The published `since`, the meet of the writer's own compaction frontier and every hold.
    pub(crate) since: Antichain<T>,
    /// The frontier the importing runtime has applied.
    ///
    /// A refusal with this AT the refusing `since` means that runtime had already applied the
    /// compaction before it built the importing dataflow, and no replica-side hold could have
    /// prevented it. BELOW that `since` means the writer escaped its own bound, which is a bug here
    /// rather than upstream.
    pub(crate) standing_hold: Antichain<T>,
}

/// A publication point for one arrangement plus the standing hold on it.
///
/// Holding it keeps the point registered; dropping it does not detach the writer, which lives with
/// its dataflow, but no further handles can be minted from it.
pub(crate) struct Published<Tr: TraceReader> {
    pub(super) shared: Arc<Shared<Tr::Batch>>,
    /// A logical hold with no reader behind it, tracking the frontier the runtime that may import
    /// this arrangement has applied.
    ///
    /// The two runtimes drain their command streams independently, so the owning runtime can apply
    /// a compaction the importing one has not. An importing dataflow whose `CreateDataflow` is still
    /// queued there has registered no hold, and would be built against an arrangement already
    /// compacted past its `as_of`. This hold forbids that: a shared arrangement compacts only as
    /// fast as the slowest runtime's stream position.
    pub(super) standing: Mutex<SharedReader<Tr::Batch>>,
    /// Total peer count (workers-per-process times processes) of the scope that publishes this
    /// arrangement. Pairwise import (importer worker `i` reads publisher worker `i`) is sound only
    /// when an importing scope shards keys the same way, which requires this to match the importing
    /// scope's own `peers()`.
    pub(super) peers: usize,
}

impl<Tr: TraceReader> Published<Tr>
where
    // The standing hold is a reader, and a reader cuts a totally ordered chain.
    Tr::Time: TotalOrder,
{
    /// Creates a publication point. It starts unattached: an empty chain with `since` and `upper`
    /// at the minimum time and no writer, until one attaches via [`PublishArrangement::adopt`].
    ///
    /// A reader may mint handles and build imports over it before that happens, but they produce
    /// nothing (the import frontier stays at the minimum) until attachment seeds them. Attachment
    /// fills the same `Arc`, so a handle captured by value at construction (as a differential join
    /// captures its input trace) observes the filled chain: the handle is a live proxy into the
    /// shared state, not a snapshot.
    ///
    /// `peers` must equal the total peer count of the scope that later adopts the point, the same
    /// invariant [`SharedTraceHandle::import_snapshot_at`] enforces.
    pub(crate) fn new(peers: usize) -> Self {
        let shared = Arc::new(Shared::new());
        let mut standing = shared.reader();
        // A standing hold is logical only. Joining with the empty antichain releases the physical
        // hold a reader registers by default, which would otherwise stop the spine merging.
        standing.set_physical_compaction(Antichain::new().borrow());
        Published {
            shared,
            standing: Mutex::new(standing),
            peers,
        }
    }

    /// Hands out a `Clone + Send` handle to the published arrangement.
    ///
    /// The handle registers a logical hold at the current published `since`, so the arrangement
    /// will not compact past it until the handle (and all its clones) drop.
    pub(crate) fn handle(&self) -> SharedTraceHandle<Tr> {
        SharedTraceHandle::new(self.shared.reader(), self.peers)
    }

    /// Hands out a handle whose hold is registered at `as_of`, failing when the published `since` is
    /// already beyond it.
    ///
    /// This is the mint a reader that intends to read at `as_of` must use. Observing `since`,
    /// deciding it permits `as_of`, and then advancing a hold are three separate acquisitions of the
    /// state lock, and the writer can advance `since` between any two of them. Checking and
    /// registering under one acquisition means a returned handle's hold is one the trace can still
    /// honour.
    ///
    /// `Err` carries the published `since` that ruled `as_of` out. That is a protocol-ordering
    /// failure rather than a serving failure, since the controller promises an index's `since` never
    /// passes the `as_of` of a dataflow importing it, so callers report it loudly rather than
    /// degrading.
    pub(crate) fn handle_at(
        &self,
        as_of: &Antichain<Tr::Time>,
    ) -> Result<SharedTraceHandle<Tr>, Antichain<Tr::Time>> {
        self.shared
            .reader_at(as_of)
            .map(|reader| SharedTraceHandle::new(reader, self.peers))
    }

    /// The published `upper`.
    pub(crate) fn upper(&self) -> Antichain<Tr::Time> {
        self.shared.upper()
    }

    /// Why this point would refuse an `as_of`. See [`Diagnostics`].
    pub(crate) fn diagnostics(&self) -> Diagnostics<Tr::Time> {
        Diagnostics {
            since: self.shared.since(),
            standing_hold: self.standing_hold(),
        }
    }

    /// The standing hold currently bounding this arrangement's logical compaction.
    pub(crate) fn standing_hold(&self) -> Antichain<Tr::Time> {
        self.standing
            .lock()
            .expect("standing hold poisoned")
            .get_logical_compaction()
            .to_owned()
    }

    /// Advances the standing hold to its join with `frontier`, recording that the runtime which may
    /// import this arrangement has applied the controller's compaction that far.
    ///
    /// Joins rather than assigning, so a reordered or replayed command cannot lower a bound the
    /// writer already compacted to. The empty frontier releases the hold.
    pub(crate) fn note_standing_hold(&self, frontier: &Antichain<Tr::Time>) {
        if let Ok(mut standing) = self.standing.lock() {
            standing.set_logical_compaction(frontier.borrow());
        }
    }
}

/// Publishes an [`Arranged`] arrangement through a publication point on its owning worker.
///
/// Materialize cannot add inherent methods to differential's foreign `Arranged` type, so it exposes
/// them as this extension trait instead.
pub(crate) trait PublishArrangement<Tr: TraceReader> {
    /// Attaches this arrangement's trace to `point`, created by [`Published::new`].
    ///
    /// From here on the trace mirrors its chain and frontiers into the point after every mutation,
    /// and applies the point's holds to its own compaction. Attachment is late-binding: a reader may
    /// build handles and imports over `point` before this arrangement is rendered, and they are
    /// seeded with the arrangement's contents now.
    ///
    /// Requires the arrangement's total peer count to equal `point`'s, panicking otherwise.
    ///
    /// `on_seal` fires once per publish on which the published `upper` advances, after the state
    /// lock is released and `upper` reflects the advance. A fast-path peek parked on this
    /// arrangement's seal is re-examined only through this callback, so it must observe the
    /// advanced `upper`. See the lost-wakeup contract on
    /// `crate::sharing::ArrangementSharingRegistry::notify`.
    fn adopt<F: Fn() + 'static>(&self, point: &Published<Tr>, on_seal: F);
}

impl<'scope, Inner> PublishArrangement<SharedSpine<Inner>>
    for Arranged<'scope, TraceAgent<SharedSpine<Inner>>>
where
    Inner: Trace + 'static,
    Inner::Time: TotalOrder,
{
    fn adopt<F: Fn() + 'static>(&self, point: &Published<SharedSpine<Inner>>, on_seal: F) {
        let scope = self.stream.scope();
        assert_eq!(
            scope.peers(),
            point.peers,
            "adopt requires equal total peers (workers_per_process * num_processes)"
        );

        // Seed the standing hold at the trace's own compaction frontier. The importing runtime may
        // not have applied any compaction for this collection yet, and until it has, this is the
        // frontier the trace may compact to: the controller offers no `as_of` below a collection's
        // own `since`, so no importer can need a frontier below it. Without this seed a point created
        // before attachment holds at the minimum time and stops the arrangement compacting at all.
        let since = self.trace.clone().get_logical_compaction().to_owned();
        point.note_standing_hold(&since);

        // A reader moving a hold wakes the arrange operator, whose `exert` applies it to the trace.
        let activator = scope
            .worker()
            .sync_activator_for(self.trace.operator().address.to_vec());
        self.trace.trace_box_unstable().borrow().trace().attach(
            Arc::clone(&point.shared),
            Some(activator),
            on_seal,
        );
    }
}
