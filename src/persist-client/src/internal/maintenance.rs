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
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64};
use std::fmt::Debug;
use timely::progress::Timestamp;
use tracing::info;

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
    pub(crate) write_rollup: Option<SeqNo>,
}

impl RoutineMaintenance {
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
    ) -> Vec<BoxFuture<'static, ()>>
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
                futures.push(recv.map(|_| ()).boxed());
            }
        }

        if let Some(rollup_seqno) = self.write_rollup {
            let mut machine = machine.clone();
            futures.push(
                mz_ore::task::spawn(|| "persist::write_rollup", async move {
                    if machine.seqno() < rollup_seqno {
                        machine.fetch_and_update_state().await;
                    }
                    // We don't have to write at exactly rollup_seqno, just need
                    // something recent.
                    assert!(
                        machine.seqno() >= rollup_seqno,
                        "{} vs {}",
                        machine.seqno(),
                        rollup_seqno
                    );
                    machine.add_rollup_for_current_seqno().await;
                })
                .map(|_| ())
                .boxed(),
            );
        }

        if let Some(lease_expiration) = self.lease_expiration {
            for expired in lease_expiration.readers {
                let mut machine = machine.clone();
                futures.push(
                    mz_ore::task::spawn(|| "persist::automatic_read_expiration", async move {
                        info!(
                            "Force expiring reader ({}) of shard ({}) due to inactivity",
                            expired,
                            machine.shard_id()
                        );
                        let _ = machine.expire_reader(&expired).await;
                    })
                    .map(|_| ())
                    .boxed(),
                );
            }
            for expired in lease_expiration.writers {
                let mut machine = machine.clone();
                futures.push(
                    mz_ore::task::spawn(|| "persist::automatic_write_expiration", async move {
                        info!(
                            "Force expiring writer ({}) of shard ({}) due to inactivity",
                            expired,
                            machine.shard_id()
                        );
                        machine.expire_writer(&expired).await;
                    })
                    .map(|_| ())
                    .boxed(),
                );
            }
        }

        futures
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
    pub(crate) fn start_performing<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
        compactor: Option<&Compactor>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let _ = self.perform_in_background(machine, gc, compactor);
    }

    /// Performs any writer maintenance necessary. Returns when all background
    /// tasks have completed and the maintenance is done.
    ///
    /// Used for testing maintenance-related state transitions deterministically
    #[cfg(test)]
    pub(crate) async fn perform<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
        compactor: Option<&Compactor>,
    ) where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        for future in self.perform_in_background(machine, gc, compactor) {
            let _ = future.await;
        }
    }

    /// Initiates maintenance work in the background, either through spawned tasks
    /// or by sending messages to existing tasks. The returned futures may be
    /// awaited to know when the work is completed, but do not need to be polled
    /// to drive the work to completion.
    fn perform_in_background<K, V, D>(
        self,
        machine: &Machine<K, V, T, D>,
        gc: &GarbageCollector<K, V, T, D>,
        compactor: Option<&Compactor>,
    ) -> Vec<BoxFuture<'static, ()>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let mut futures = self.routine.perform_in_background(machine, gc);

        if let Some(compactor) = compactor {
            for req in self.compaction {
                if let Some(handle) = compactor.compact_and_apply_background(machine, req) {
                    futures.push(handle.map(|_| ()).boxed());
                }
            }
        }

        futures
    }
}
