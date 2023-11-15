// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Interfaces for reading txn shards as well as data shards.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use futures::Stream;
use mz_ore::collections::HashMap;
use mz_persist_client::fetch::LeasedBatchPart;
use mz_persist_client::read::{Cursor, ListenEvent, ReadHandle, Since, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::{Codec, Codec64, StepForward};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::{debug, instrument};

use crate::{TxnsCodec, TxnsCodecDefault, TxnsEntry};

/// A token exchangeable for a data shard snapshot.
///
/// - Invariant: `latest_write <= as_of < empty_to`
/// - Invariant: `(latest_write, empty_to)` has no committed writes (which means
///   we can do an empty CaA of those times if we like).
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DataSnapshot<T> {
    /// The id of the data shard this snapshot is for.
    data_id: ShardId,
    /// The latest possibly unapplied write <= as_of. None if there are no
    /// writes via txns or if they're all known to be applied.
    latest_write: Option<T>,
    /// The as_of asked for.
    as_of: T,
    /// An upper bound on the times known to be empty of writes via txns.
    empty_to: T,
}

impl<T: Timestamp + Lattice + TotalOrder + Codec64> DataSnapshot<T> {
    /// Unblocks reading a snapshot at `self.as_of` by waiting for the latest
    /// write before that time and then running an empty CaA if necessary.
    ///
    /// Returns a frontier that is greater than the as_of and less_equal the
    /// physical upper of the data shard. This is suitable for use as an initial
    /// input to `TxnsCache::data_listen_next` (after reading up to it, of
    /// course).
    #[instrument(level = "debug", skip_all, fields(shard = %self.data_id, ts = ?self.as_of))]
    pub(crate) async fn unblock_read<K, V, D>(
        &self,
        mut data_write: WriteHandle<K, V, T, D>,
    ) -> Antichain<T>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        D: Semigroup + Codec64 + Send + Sync,
    {
        debug!(
            "unblock_read latest_write={:?} as_of={:?} for {:.9}",
            self.latest_write,
            self.as_of,
            self.data_id.to_string()
        );
        // First block until the latest write has been applied.
        if let Some(latest_write) = self.latest_write.as_ref() {
            let () = data_write
                .wait_for_upper_past(&Antichain::from_elem(latest_write.clone()))
                .await;
        }

        // Now fill `(latest_write,as_of]` with empty updates, so we can read
        // the shard at as_of normally. In practice, because CaA takes an
        // exclusive upper, we actually fill `(latest_write, empty_to)`.
        //
        // It's quite counter-intuitive for reads to involve writes, but I think
        // this is fine. In particular, because writing empty updates to a
        // persist shard is a metadata-only operation. It might result in things
        // like GC maintenance or a CRDB write, but this is also true for
        // registering a reader. On the balance, I think this is a _much_ better
        // set of tradeoffs than the original plan of trying to translate read
        // timestamps to the most recent write and reading that.
        let Some(mut data_upper) = data_write.shared_upper().into_option() else {
            // If the upper is the empty antichain, then we've unblocked all
            // possible reads. Return early.
            debug!(
                "CaA data snapshot {:.9} shard finalized",
                self.data_id.to_string(),
            );
            return Antichain::new();
        };
        while data_upper < self.empty_to {
            // It would be very bad if we accidentally filled any times <=
            // latest_write with empty updates, so defensively assert on each
            // iteration through the loop.
            if let Some(latest_write) = self.latest_write.as_ref() {
                assert!(latest_write < &data_upper);
            }
            assert!(self.as_of < self.empty_to);
            let res = crate::small_caa(
                || format!("data {:.9} unblock reads", self.data_id.to_string()),
                &mut data_write,
                &[],
                data_upper.clone(),
                self.empty_to.clone(),
            )
            .await;
            match res {
                Ok(()) => {
                    // Persist registers writers on the first write, so politely
                    // expire the writer we just created, but (as a performance
                    // optimization) only if we actually wrote something.
                    data_write.expire().await;
                    break;
                }
                Err(new_data_upper) => {
                    data_upper = new_data_upper;
                    continue;
                }
            }
        }
        Antichain::from_elem(self.empty_to.clone())
    }

    /// See [ReadHandle::snapshot_and_fetch].
    pub async fn snapshot_and_fetch<K, V, D>(
        &self,
        data_read: &mut ReadHandle<K, V, T, D>,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, Since<T>>
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let data_write = WriteHandle::from_read(data_read, "unblock_read");
        self.unblock_read(data_write).await;
        data_read
            .snapshot_and_fetch(Antichain::from_elem(self.as_of.clone()))
            .await
    }

    /// See [ReadHandle::snapshot_cursor].
    pub async fn snapshot_cursor<K, V, D>(
        &self,
        data_read: &mut ReadHandle<K, V, T, D>,
    ) -> Result<Cursor<K, V, T, D>, Since<T>>
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let data_write = WriteHandle::from_read(data_read, "unblock_read");
        self.unblock_read(data_write).await;
        data_read
            .snapshot_cursor(Antichain::from_elem(self.as_of.clone()))
            .await
    }

    /// See [ReadHandle::snapshot_and_stream].
    pub async fn snapshot_and_stream<K, V, D>(
        &self,
        data_read: &mut ReadHandle<K, V, T, D>,
    ) -> Result<impl Stream<Item = ((Result<K, String>, Result<V, String>), T, D)>, Since<T>>
    where
        K: Debug + Codec + Ord,
        V: Debug + Codec + Ord,
        D: Semigroup + Codec64 + Send + Sync,
    {
        let data_write = WriteHandle::from_read(data_read, "unblock_read");
        self.unblock_read(data_write).await;
        data_read
            .snapshot_and_stream(Antichain::from_elem(self.as_of.clone()))
            .await
    }

    fn validate(&self) -> Result<(), String> {
        if let Some(latest_write) = self.latest_write.as_ref() {
            if !(latest_write <= &self.as_of) {
                return Err(format!(
                    "latest_write {:?} not <= as_of {:?}",
                    self.latest_write, self.as_of
                ));
            }
        }
        if !(self.as_of < self.empty_to) {
            return Err(format!(
                "as_of {:?} not < empty_to {:?}",
                self.as_of, self.empty_to
            ));
        }
        Ok(())
    }
}

/// The next action to take in a data shard `Listen`.
///
/// See [TxnsCache::data_listen_next].
#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub enum DataListenNext<T> {
    /// Read the data shard normally, until this timestamp is less_equal what
    /// has been read.
    ReadDataTo(T),
    /// It is known there there are no writes between the progress given to the
    /// `data_listen_next` call and this timestamp. Advance the data shard
    /// listen progress to this (exclusive) frontier.
    EmitLogicalProgress(T),
    /// The data shard listen has caught up to what has been written to the txns
    /// shard. Wait for it to progress with `update_gt` and call
    /// `data_listen_next` again.
    WaitForTxnsProgress,
    /// We've lost historical distinctions and can no longer answer queries
    /// about times before the returned one.
    CompactedTo(T),
}
