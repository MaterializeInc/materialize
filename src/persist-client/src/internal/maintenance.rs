// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! All machines need maintenance
//!
//! Maintenance operations for persist, shared among active handles

use crate::internal::compact::CompactReq;
use crate::internal::gc::GcReq;
use crate::{Compactor, GarbageCollector, Machine};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64};
use std::fmt::Debug;
use std::mem;
use timely::progress::Timestamp;

/// Every handle to this shard may be occasionally asked to perform
/// routine maintenance after a successful compare_and_set operation.
///
/// For one-shot operations (like registering a reader) handles are
/// allowed to skip routine maintenance if necessary, as the same
/// maintenance operations will be recomputed by the next successful
/// compare_and_set of any handle.
///
/// Operations that run regularly once a handle is registered, such
/// as heartbeats, are expected to always perform maintenance.
#[must_use]
#[derive(Debug, Default, PartialEq)]
pub struct RoutineMaintenance {
    pub(crate) garbage_collection: Option<GcReq>,
    pub(crate) write_rollup: Option<SeqNo>,
}

impl RoutineMaintenance {
    pub(crate) fn is_empty(&self) -> bool {
        self == &RoutineMaintenance::default()
    }

    /// Initiates any routine maintenance necessary in background tasks
    pub(crate) fn start_performing<K, V, T, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let _ = self.perform_in_background(machine, gc);
    }

    /// Performs any routine maintenance necessary. Returns when all background
    /// tasks have completed and the maintenance is done.
    ///
    /// Used for testing maintenance-related state transitions deterministically
    #[cfg(test)]
    pub(crate) async fn perform<K, V, T, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        for future in self.perform_in_background(machine, gc) {
            let _ = future.await;
        }
    }

    /// Initiates maintenance work in the background, either through spawned tasks
    /// or by sending messages to existing tasks. The returned futures may be
    /// awaited to know when the work is completed, but do not need to be polled
    /// to drive the work to completion.
    fn perform_in_background<K, V, T, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
    ) -> Vec<BoxFuture<'static, RoutineMaintenance>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let mut futures = vec![];
        if let Some(gc_req) = self.garbage_collection {
            if let Some(recv) = gc.gc_and_truncate_background(gc_req) {
                // it's safe to ignore errors on the receiver. in the
                // case of shutdown, the sender may have been dropped
                futures.push(recv.map(Result::unwrap_or_default).boxed());
            }
        }

        if let Some(rollup_seqno) = self.write_rollup {
            let mut machine = machine.clone();
            futures.push(
                mz_ore::task::spawn(|| "persist::write_rollup", async move {
                    if machine.seqno() < rollup_seqno {
                        machine.applier.fetch_and_update_state().await;
                    }
                    // We don't have to write at exactly rollup_seqno, just need
                    // something recent.
                    assert!(
                        machine.seqno() >= rollup_seqno,
                        "{} vs {}",
                        machine.seqno(),
                        rollup_seqno
                    );
                    machine.add_rollup_for_current_seqno().await
                })
                .map(Result::unwrap_or_default)
                .boxed(),
            );
        }

        futures
    }

    /// Merges another maintenance request into this one.
    ///
    /// `other` is expected to come "later", and so its maintenance might
    /// override `self`'s maintenance.
    pub fn merge(&mut self, other: RoutineMaintenance) {
        // Deconstruct other so we get a compile failure if new fields are
        // added.
        let RoutineMaintenance {
            garbage_collection,
            write_rollup,
        } = other;
        if let Some(garbage_collection) = garbage_collection {
            self.garbage_collection = Some(garbage_collection);
        }
        if let Some(write_rollup) = write_rollup {
            self.write_rollup = Some(write_rollup);
        }
    }
}

/// Writers may be asked to perform additional tasks beyond the
/// routine maintenance common to all handles. It is expected that
/// writers always perform maintenance.
#[must_use]
#[derive(Debug)]
pub struct WriterMaintenance<T> {
    pub(crate) routine: RoutineMaintenance,
    pub(crate) compaction: Vec<CompactReq<T>>,
}

impl<T> Default for WriterMaintenance<T> {
    fn default() -> Self {
        Self {
            routine: RoutineMaintenance::default(),
            compaction: Vec::default(),
        }
    }
}

impl<T> WriterMaintenance<T>
where
    T: Timestamp + Lattice + Codec64,
{
    /// Initiates any writer maintenance necessary in background tasks
    pub(crate) fn start_performing<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
        compactor: Option<&Compactor<K, V, T, D>>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let machine = machine.clone();
        let gc = gc.clone();
        let compactor = compactor.cloned();
        mz_ore::task::spawn(|| "writer-maintenance", async move {
            self.perform(&machine, &gc, compactor.as_ref()).await
        });
    }

    /// Performs any writer maintenance necessary. Returns when all background
    /// tasks have completed and the maintenance is done.
    pub(crate) async fn perform<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
        compactor: Option<&Compactor<K, V, T, D>>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let Self {
            routine,
            compaction,
        } = self;
        let mut more_maintenance = RoutineMaintenance::default();
        for future in routine.perform_in_background(machine, gc) {
            more_maintenance.merge(future.await);
        }

        if let Some(compactor) = compactor {
            for req in compaction {
                if let Some(receiver) = compactor.compact_and_apply_background(req, machine) {
                    // it's safe to ignore errors on the receiver. in the
                    // case of shutdown, the sender may have been dropped
                    let _ = receiver.await;
                }
            }
        }

        while !more_maintenance.is_empty() {
            let maintenance = mem::take(&mut more_maintenance);
            for future in maintenance.perform_in_background(machine, gc) {
                more_maintenance.merge(future.await);
            }
        }
    }
}
