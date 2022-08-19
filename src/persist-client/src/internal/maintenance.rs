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
use crate::{Compactor, GarbageCollector, Machine, ReaderId, WriterId};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use std::fmt::Debug;
use timely::progress::Timestamp;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct LeaseExpiration {
    pub(crate) readers: Vec<ReaderId>,
    pub(crate) writers: Vec<WriterId>,
}

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
#[derive(Debug, Default)]
pub struct RoutineMaintenance {
    pub(crate) garbage_collection: Option<GcReq>,
    pub(crate) lease_expiration: Option<LeaseExpiration>,
}

impl RoutineMaintenance {
    /// Initiates any routine maintenance necessary in background tasks
    pub(crate) fn perform<K, V, T, D>(self, machine: &Machine<K, V, T, D>, gc: &GarbageCollector)
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let _ = self.perform_in_background(machine, gc);
    }

    /// Performs any routine maintenance necessary in background tasks. Can be awaited
    /// to ensure all background work has completed.
    ///
    /// Used for testing maintenance-related state transitions deterministically
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn perform_awaitable<K, V, T, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        for handle in self.perform_in_background(machine, gc) {
            let _ = handle.await;
        }
    }

    fn perform_in_background<K, V, T, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector,
    ) -> Vec<JoinHandle<()>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let mut join_handles = vec![];
        if let Some(gc_req) = self.garbage_collection {
            join_handles.push(gc.gc_and_truncate_background(gc_req));
        }

        if let Some(lease_expiration) = self.lease_expiration {
            for expired in lease_expiration.readers {
                let mut machine = machine.clone();
                join_handles.push(mz_ore::task::spawn(
                    || "persist::automatic_read_expiration",
                    async move {
                        let _ = machine.expire_reader(&expired).await;
                    },
                ));
            }
            for expired in lease_expiration.writers {
                let mut machine = machine.clone();
                join_handles.push(mz_ore::task::spawn(
                    || "persist::automatic_write_expiration",
                    async move {
                        machine.expire_writer(&expired).await;
                    },
                ));
            }
        }

        join_handles
    }
}

/// Writers may be asked to perform additional tasks beyond the
/// routine maintenance common to all handles. It is expected that
/// writers always perform maintenance.
#[must_use]
#[derive(Debug, Default)]
pub struct WriterMaintenance<T> {
    pub(crate) routine: RoutineMaintenance,
    pub(crate) compaction: Vec<CompactReq<T>>,
}

impl<T> WriterMaintenance<T>
where
    T: Timestamp + Lattice + Codec64,
{
    /// Initiates any writer maintenance necessary in background tasks
    pub(crate) fn perform<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector,
        compactor: Option<&Compactor>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let _ = self.perform_in_background(machine, gc, compactor);
    }

    /// Performs any writer maintenance necessary in background tasks. Can be awaited
    /// to ensure all background work has completed.
    ///
    /// Used for testing maintenance-related state transitions deterministically
    #[cfg(test)]
    pub(crate) async fn perform_awaitable<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector,
        compactor: Option<&Compactor>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        for handle in self.perform_in_background(machine, gc, compactor) {
            let _ = handle.await;
        }
    }

    fn perform_in_background<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector,
        compactor: Option<&Compactor>,
    ) -> Vec<JoinHandle<()>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let mut handles = self.routine.perform_in_background(machine, gc);

        if let Some(compactor) = compactor {
            for req in self.compaction {
                if let Some(handle) = compactor.compact_and_apply_background(machine, req) {
                    handles.push(handle);
                }
            }
        }

        handles
    }
}
