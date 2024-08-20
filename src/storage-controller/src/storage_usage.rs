// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for working with the special `mz_storage_usage_by_shard` builtin
//! table.

use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use mz_ore::now::EpochMillis;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, Row, TimestampManipulation};
use mz_storage_client::client::TimestamplessUpdate;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::sources::SourceData;
use mz_timestamp_oracle::TimestampOracle;
use mz_txn_wal::txn_read::TxnsRead;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use tracing::debug;

use crate::persist_handles;

#[allow(unused)]
pub struct PruneStorageUsageTask<
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
> {
    /// [GlobalId] of our `mz_storage_usage_by_shard` collection. Mostly for
    /// diagnostics/logging.
    pub collection_id: GlobalId,

    /// [CollectionMetadata] of our `mz_storage_usage_by_shard` collection.
    pub collection_metadata: CollectionMetadata,

    /// Configured retention period.
    pub retention_period: Duration,

    /// Handle to shared persist client cache.
    pub persist: Arc<PersistClientCache>,

    /// Process-global write lock.
    ///
    /// NOTE(aljoscha): I wish we didn't have this, but here we are. We can get
    /// rid of this when we replace the write lock by, say, optimistic
    /// concurrency control.
    pub write_lock: Arc<tokio::sync::Mutex<()>>,

    /// Write handle for table shards.
    pub persist_table_worker: persist_handles::PersistTableWriteWorker<T>,

    /// Handle to a shared [TxnsRead].
    pub txns_read: TxnsRead<T>,

    /// Timestamp oracle for the table timeline.
    pub timestamp_oracle: Arc<dyn TimestampOracle<T> + Send + Sync>,
}

impl<T> PruneStorageUsageTask<T>
where
    T: Timestamp
        + Lattice
        + TotalOrder
        + Codec64
        + From<EpochMillis>
        + TimestampManipulation
        + Display,
    u128: From<T>,
{
    #[allow(unused)]
    pub async fn run_once(mut self) {
        // We retry only until we succeed pruning once.
        loop {
            let read_ts = self.timestamp_oracle.read_ts().await;

            // TODO: Instead of snapshotting fresh every time, we could
            // continually listen while we attempt to get our retractions in at
            // the next write timestamp.
            let mut current_contents = self.snapshot(read_ts.clone()).await.expect("can snapshot");

            differential_dataflow::consolidation::consolidate(&mut current_contents);

            let cutoff_ts =
                u128::from(read_ts.clone()).saturating_sub(self.retention_period.as_millis());
            let mut retractions = Vec::new();
            for (row, diff) in current_contents {
                assert_eq!(
                    diff, 1,
                    "consolidated contents should not contain retractions: ({row:#?}, {diff:#?})"
                );

                // This logic relies on the definition of `mz_storage_usage_by_shard` not changing.
                let collection_timestamp = row.unpack()[3].unwrap_timestamptz();
                let collection_timestamp = collection_timestamp.timestamp_millis();
                let collection_timestamp: u128 = collection_timestamp
                    .try_into()
                    .expect("all collections happen after Jan 1 1970");

                if collection_timestamp < cutoff_ts {
                    debug!("pruning storage event {row:?}");
                    let retraction = TimestamplessUpdate { row, diff: -1 };
                    retractions.push(retraction);
                }
            }

            // Try to write at exactly the next timestamp, if not read more and try again.
            let write_ts = TimestampManipulation::step_forward(&read_ts);
            let advance_to = TimestampManipulation::step_forward(&write_ts);

            // TODO: Also acquire the write lock!
            let res = self
                .persist_table_worker
                .append(
                    write_ts,
                    advance_to,
                    vec![(self.collection_id, retractions)],
                )
                .await
                .expect("receiver hung up");

            match res {
                Ok(()) => {
                    // We're done!
                    tracing::info!("successfully retracted expired storage-usage entries!");
                    break;
                }
                Err(e) => {
                    tracing::info!(error = ?e, "could not retract storage-usage entries, retrying...")
                }
            }
        }
    }

    #[allow(unused)]
    async fn snapshot(&mut self, as_of: T) -> Result<Vec<(Row, Diff)>, StorageError<T>> {
        self.txns_read.update_gt(as_of.clone()).await;
        let data_snapshot = self
            .txns_read
            .data_snapshot(self.collection_metadata.data_shard, as_of.clone())
            .await;

        let mut read_handle = self.read_handle_for_snapshot().await?;
        let snapshot_raw = match data_snapshot.snapshot_and_fetch(&mut read_handle).await {
            Ok(snapshot) => snapshot,
            Err(_) => {
                return Err(StorageError::ReadBeforeSince(self.collection_id));
            }
        };

        let mut snapshot = Vec::with_capacity(snapshot_raw.len());

        for ((data, _), _, diff) in snapshot_raw {
            // TODO(petrosagg): We should accumulate the errors too and let the user
            // interpret the result
            let row = data.expect("invalid protobuf data").0?;
            snapshot.push((row, diff))
        }

        Ok(snapshot)
    }

    #[allow(unused)]
    async fn read_handle_for_snapshot(
        &self,
    ) -> Result<ReadHandle<SourceData, (), T, Diff>, StorageError<T>> {
        let persist_client = self
            .persist
            .open(self.collection_metadata.persist_location.clone())
            .await
            .unwrap();

        // We create a new read handle every time someone requests a snapshot
        // and then immediately expire it instead of keeping a read handle
        // permanently in our state to avoid having it heartbeat continously.
        // The assumption is that calls to snapshot are rare and therefore worth
        // it to always create a new handle.
        let read_handle = persist_client
            .open_leased_reader::<SourceData, (), _, _>(
                self.collection_metadata.data_shard,
                Arc::new(self.collection_metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: self.collection_id.to_string(),
                    handle_purpose: format!(
                        "snapshot of mz_storage_usage_by_shard ({})",
                        self.collection_id
                    ),
                },
                USE_CRITICAL_SINCE_SNAPSHOT.get(persist_client.dyncfgs()),
            )
            .await
            .expect("invalid persist usage");

        Ok(read_handle)
    }
}
