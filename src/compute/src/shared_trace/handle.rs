// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The reader half: a `Send` trace handle over a publication point.

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::cursor::Navigable;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use differential_dataflow::trace::{Batch, TraceReader};
use mz_timely_util::shared_trace::SharedReader;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

/// A `Clone + Send` reader of a published arrangement.
///
/// A [`SharedReader`] that remembers the publisher's peer count, so an import can refuse a scope
/// that shards keys differently. Implements [`TraceReader`] by delegation, so downstream operators
/// drive its compaction and acquire cursors as with any trace handle. Each clone carries an
/// independent hold.
pub(crate) struct SharedTraceHandle<Tr: TraceReader> {
    pub(super) reader: SharedReader<Tr::Batch>,
    pub(super) peers: usize,
}

impl<Tr: TraceReader> SharedTraceHandle<Tr> {
    pub(super) fn new(reader: SharedReader<Tr::Batch>, peers: usize) -> Self {
        Self { reader, peers }
    }
}

impl<Tr: TraceReader> Clone for SharedTraceHandle<Tr> {
    fn clone(&self) -> Self {
        Self {
            reader: self.reader.clone(),
            peers: self.peers,
        }
    }
}

impl<Tr: TraceReader> TraceReader for SharedTraceHandle<Tr>
where
    Tr::Time: TotalOrder,
{
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn batches_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<Vec<Self::Batch>> {
        self.reader.batches_through(upper)
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        self.reader.set_logical_compaction(frontier)
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.reader.get_logical_compaction()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) {
        self.reader.set_physical_compaction(frontier)
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.reader.get_physical_compaction()
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
        self.reader.map_batches(f)
    }
}

impl<Tr: TraceReader> SharedTraceHandle<Tr>
where
    Tr: 'static,
    Tr::Time: TotalOrder,
    Tr::Batch: Batch + Navigable,
{
    /// Imports the published arrangement restricted to `[as_of, until)`, presented at `as_of`.
    ///
    /// See [`SharedReader::import_frontier_core`] for the replay contract. Requires `scope`'s total
    /// peer count (workers-per-process times processes) to equal the publisher's, panicking
    /// otherwise: pairwise import reads publisher worker `i` from importer worker `i`, which is
    /// sound only when both sides shard keys the same way.
    ///
    /// The importer registration is owned by the source operator, so dropping the import dataflow
    /// deregisters it and releases its holds even while other handle clones live on.
    pub(crate) fn import_snapshot_at<'scope>(
        &self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
        as_of: Antichain<Tr::Time>,
        until: Antichain<Tr::Time>,
    ) -> Arranged<'scope, TraceFrontier<SharedTraceHandle<Tr>>> {
        assert_eq!(
            scope.peers(),
            self.peers,
            "shared-trace import requires equal total peers (workers_per_process * num_processes)"
        );
        let Arranged { stream, trace } =
            self.reader
                .import_frontier_core(scope, name, as_of.clone(), until.clone());
        // Re-wrap the returned trace as a handle, so the arrangement's trace type carries the peer
        // count too. The inner hold drops here and the clone registers an equal one.
        drop(trace);
        let trace = TraceFrontier::make_from(self.clone(), as_of.borrow(), until.borrow());
        Arranged { stream, trace }
    }
}
