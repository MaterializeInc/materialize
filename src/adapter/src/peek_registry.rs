// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A sharded registry of in-flight frontend-sequenced peeks, shared between the
//! coordinator and the session-side [`PeekClient`](crate::peek_client::PeekClient)s
//! via an `Arc`.
//!
//! Frontend peeks register and unregister themselves here directly, off the
//! single coordinator task, so that the peek hot path never blocks on a
//! coordinator round-trip. The coordinator only consults the registry when it
//! must cancel a connection's peeks, in `Coordinator::cancel_pending_peeks`.
//!
//! The registry tracks what the two teardown paths need: the connection that
//! owns a peek, the cluster it runs on, and the collections it reads.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use mz_adapter_types::connection::ConnectionId;
use mz_controller_types::ClusterId;
use mz_repr::GlobalId;
use uuid::Uuid;

/// State tracked per in-flight frontend peek.
#[derive(Debug, Clone)]
pub struct PendingPeekEntry {
    pub conn_id: ConnectionId,
    pub cluster_id: ClusterId,
    /// Every `GlobalId` the peek reads. Dependency teardown matches against
    /// this to decide whether a dropped collection kills the peek.
    pub depends_on: BTreeSet<GlobalId>,
}

/// A registry peek that a catalog drop has invalidated.
#[derive(Debug)]
pub struct DroppedPeek {
    pub uuid: Uuid,
    pub cluster_id: ClusterId,
    /// The dropped collection the peek reads, or `None` when the peek matched
    /// because its cluster is going away. The caller turns this into the
    /// user-facing dependency name.
    pub dropped_collection: Option<GlobalId>,
}

/// A sharded, lock-per-shard registry of in-flight frontend peeks.
///
/// The per-uuid shards carry the hot-path inserts and removes. `by_conn` is a
/// secondary index consulted only by cancellation, which is cold.
#[derive(Debug)]
pub struct FrontendPeekRegistry {
    /// Per-uuid entries, sharded to spread lock contention across concurrent
    /// sessions. A uuid always maps to the same shard via [`Self::shard_of`].
    shards: Box<[Mutex<BTreeMap<Uuid, PendingPeekEntry>>]>,
    /// Secondary index from connection to its outstanding peek uuids. Used by
    /// cancellation to find a connection's peeks without scanning every shard.
    by_conn: Mutex<BTreeMap<ConnectionId, BTreeSet<Uuid>>>,
}

impl FrontendPeekRegistry {
    /// Creates a registry with `shards` per-uuid shards. `shards` must be
    /// non-zero.
    pub fn new(shards: usize) -> Self {
        assert!(shards > 0, "registry needs at least one shard");
        let shards = (0..shards)
            .map(|_| Mutex::new(BTreeMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            by_conn: Mutex::new(BTreeMap::new()),
        }
    }

    /// Returns the shard index for `uuid`. v4 uuids are uniformly random, so the
    /// first byte spreads uuids evenly across shards.
    fn shard_of(&self, uuid: &Uuid) -> usize {
        usize::from(uuid.as_bytes()[0]) % self.shards.len()
    }

    /// Registers an in-flight peek.
    ///
    /// Must complete before the peek is issued so that a concurrent
    /// cancellation observes the entry.
    pub fn register(&self, uuid: Uuid, entry: PendingPeekEntry) {
        let conn_id = entry.conn_id.clone();
        {
            let mut shard = self.shards[self.shard_of(&uuid)]
                .lock()
                .expect("lock poisoned");
            shard.insert(uuid, entry);
        }
        let mut by_conn = self.by_conn.lock().expect("lock poisoned");
        by_conn.entry(conn_id).or_default().insert(uuid);
    }

    /// Removes an in-flight peek, returning its entry if it was present.
    ///
    /// Also drops the uuid from the connection's secondary index.
    pub fn remove(&self, uuid: Uuid) -> Option<PendingPeekEntry> {
        let entry = {
            let mut shard = self.shards[self.shard_of(&uuid)]
                .lock()
                .expect("lock poisoned");
            shard.remove(&uuid)
        };
        if let Some(entry) = &entry {
            let mut by_conn = self.by_conn.lock().expect("lock poisoned");
            if let Some(uuids) = by_conn.get_mut(&entry.conn_id) {
                uuids.remove(&uuid);
                if uuids.is_empty() {
                    by_conn.remove(&entry.conn_id);
                }
            }
        }
        entry
    }

    /// Drains and returns all in-flight peeks for `conn_id`, removing them from
    /// both the secondary index and the shards.
    pub fn take_conn(&self, conn_id: &ConnectionId) -> Vec<(Uuid, ClusterId)> {
        let uuids = {
            let mut by_conn = self.by_conn.lock().expect("lock poisoned");
            by_conn.remove(conn_id).unwrap_or_default()
        };
        let mut result = Vec::with_capacity(uuids.len());
        for uuid in uuids {
            let mut shard = self.shards[self.shard_of(&uuid)]
                .lock()
                .expect("lock poisoned");
            if let Some(entry) = shard.remove(&uuid) {
                result.push((uuid, entry.cluster_id));
            }
        }
        result
    }

    /// Returns the in-flight peeks invalidated by dropping `collections` and
    /// `clusters`, without removing them.
    ///
    /// A dropped collection takes precedence over a dropped cluster, matching
    /// how the coordinator reports the dependency for its own pending peeks:
    /// naming the relation is more useful than naming the cluster when both
    /// went away in the same DDL.
    ///
    /// Finding and removing are separate because the caller cancels each peek
    /// on the compute instance, and only a peek it actually removed may be
    /// cancelled. A concurrent completion can remove an entry in between, and
    /// [`Self::remove`] returning `None` is what tells the caller to skip it.
    ///
    /// This scans every shard, which is fine: it runs on catalog DDL, not on
    /// the peek hot path.
    pub fn find_dropped(
        &self,
        collections: &BTreeSet<GlobalId>,
        clusters: &[ClusterId],
    ) -> Vec<DroppedPeek> {
        let mut dropped = Vec::new();
        for shard in &self.shards {
            let shard = shard.lock().expect("lock poisoned");
            for (uuid, entry) in shard.iter() {
                let dropped_collection = entry.depends_on.intersection(collections).next().copied();
                if dropped_collection.is_none() && !clusters.contains(&entry.cluster_id) {
                    continue;
                }
                dropped.push(DroppedPeek {
                    uuid: *uuid,
                    cluster_id: entry.cluster_id,
                    dropped_collection,
                });
            }
        }
        dropped
    }
}

/// RAII owner that removes a peek's registration when dropped.
///
/// Moved into the peek response stream so the registry entry is cleaned up when
/// the stream completes or is dropped, keeping the registry bounded. Removal is
/// idempotent, so a prior explicit [`FrontendPeekRegistry::remove`] or
/// [`FrontendPeekRegistry::take_conn`] makes the drop a no-op.
#[derive(Debug)]
pub struct PeekRegistrationGuard {
    registry: std::sync::Arc<FrontendPeekRegistry>,
    uuid: Uuid,
}

impl PeekRegistrationGuard {
    pub fn new(registry: std::sync::Arc<FrontendPeekRegistry>, uuid: Uuid) -> Self {
        Self { registry, uuid }
    }
}

impl Drop for PeekRegistrationGuard {
    fn drop(&mut self) {
        let _ = self.registry.remove(self.uuid);
    }
}
