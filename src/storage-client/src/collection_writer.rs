// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist appends shared by introspection producers.

use mz_persist_client::batch::Batch;
use mz_persist_client::write::WriteHandle;
use mz_repr::{GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, InvalidUpper, StorageError};
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;

use crate::client::TimestamplessUpdate;
use crate::collection_reader::CollectionReader;

/// Removes rows belonging to irrevocably retired owners from a non-transactional
/// collection. `retired` must remain true for each selected row forever.
///
/// The caller must supply committed metadata and read protection for the snapshot
/// at the current upper minus one, as required by [`CollectionReader`]. Transactional
/// tables must instead reconcile through their txn-WAL.
///
/// Makes one conditional attempt. On an upper conflict the candidate is discarded,
/// not retried at a later timestamp. Repeated calls also remove signed residuals
/// from late retired-owner writes. Returns whether a nonempty deletion committed.
pub async fn retract_retired_rows(
    reader: &CollectionReader,
    writer: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    id: GlobalId,
    metadata: CollectionMetadata,
    retired: impl Fn(&Row) -> bool,
) -> Result<bool, StorageError> {
    let Some(candidate) = prepare_retired_rows(reader, writer, id, metadata, retired).await? else {
        return Ok(false);
    };
    candidate.commit(writer, id).await?;
    Ok(true)
}

struct RetiredRows {
    upper: Timestamp,
    batch: Batch<SourceData, (), Timestamp, StorageDiff>,
}

impl RetiredRows {
    async fn commit(
        mut self,
        writer: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        id: GlobalId,
    ) -> Result<(), StorageError> {
        let result = writer
            .compare_and_append_batch(
                &mut [&mut self.batch],
                Antichain::from_elem(self.upper),
                Antichain::from_elem(self.upper.step_forward()),
                true,
            )
            .await
            .expect("snapshot retractions use valid timestamps")
            .map_err(|error| {
                StorageError::InvalidUppers(vec![InvalidUpper {
                    id,
                    current_upper: error.current,
                }])
            });
        self.batch.delete().await;
        result
    }
}

async fn prepare_retired_rows(
    reader: &CollectionReader,
    writer: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    id: GlobalId,
    metadata: CollectionMetadata,
    retired: impl Fn(&Row) -> bool,
) -> Result<Option<RetiredRows>, StorageError> {
    assert!(
        metadata.txns_shard.is_none(),
        "cleanup must not bypass txn-WAL"
    );
    assert_eq!(metadata.data_shard, writer.shard_id());
    let Some(upper) = writer.fetch_recent_upper().await.as_option().copied() else {
        return Ok(None);
    };
    let Some(as_of) = upper.step_back() else {
        return Ok(None);
    };
    let mut snapshot = reader.snapshot_cursor(id, metadata, as_of).await?;
    let mut builder = None;
    while let Some(chunk) = snapshot.next().await {
        for (data, _, diff) in chunk {
            if data.0.as_ref().is_ok_and(&retired) {
                builder
                    .get_or_insert_with(|| writer.builder(Antichain::from_elem(upper)))
                    .add(&data, &(), &upper, &-diff)
                    .await
                    .expect("snapshot rows and timestamp are valid");
            }
        }
    }
    let Some(builder) = builder else {
        return Ok(None);
    };
    let batch = builder
        .finish(Antichain::from_elem(upper.step_forward()))
        .await
        .expect("snapshot retractions use valid timestamps");
    Ok(Some(RetiredRows { upper, batch }))
}

/// Appends updates at a writable timestamp no earlier than `at_least`.
///
/// Definitive upper conflicts retry at the observed upper. These must be blind
/// updates, not retractions conditioned on a snapshot of collection contents.
/// Event timestamps encoded in rows are unaffected by append timestamp selection.
///
/// Empty updates to closed collections are ignored. Nonempty updates to closed
/// collections, or invalid Persist usage, panic.
pub async fn monotonic_append(
    write_handle: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    updates: Vec<TimestamplessUpdate>,
    at_least: Timestamp,
) {
    let mut expected_upper = write_handle.shared_upper();
    loop {
        if updates.is_empty() && expected_upper.is_empty() {
            return;
        }

        let upper = expected_upper
            .into_option()
            .expect("cannot append data to closed collection");

        let lower = std::cmp::max(upper, at_least);
        let new_upper = lower.step_forward();
        let updates = updates
            .iter()
            .map(|TimestamplessUpdate { row, diff }| {
                ((SourceData(Ok(row.clone())), ()), lower, diff.into_inner())
            })
            .collect::<Vec<_>>();
        let res = write_handle
            .compare_and_append(
                updates,
                Antichain::from_elem(upper),
                Antichain::from_elem(new_upper),
            )
            .await
            .expect("valid usage");
        match res {
            Ok(()) => return,
            Err(err) => {
                expected_upper = err.current;
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
    use mz_persist_client::critical::Opaque;
    use mz_persist_client::{Diagnostics, PersistLocation, ShardId};
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_repr::{Datum, Diff, RelationDesc, SqlScalarType};
    use mz_storage_types::controller::TxnsCodecRow;
    use mz_txn_wal::metrics::Metrics;
    use mz_txn_wal::txn_read::TxnsRead;
    use mz_txn_wal::txns::TxnsHandle;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn retirement_cleanup_conflicts_and_late_signed_updates() {
        let cache = PersistClientCache::new_no_metrics();
        let mut config = mz_dyncfg::ConfigUpdates::default();
        config.add(&USE_CRITICAL_SINCE_SNAPSHOT, false);
        config.apply(cache.cfg());
        let location = PersistLocation::new_in_mem();
        let persist = cache.open(location.clone()).await.unwrap();
        let id = GlobalId::User(1);
        let metadata = CollectionMetadata {
            persist_location: location,
            data_shard: ShardId::new(),
            relation_desc: RelationDesc::builder()
                .with_column("incarnation", SqlScalarType::UInt64.nullable(false))
                .finish(),
            txns_shard: None,
        };
        let (mut writer, authority) = persist
            .open::<SourceData, (), Timestamp, StorageDiff>(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    handle_purpose: "retirement cleanup test".into(),
                },
                false,
            )
            .await
            .unwrap();
        let mut peer = WriteHandle::from_read(&authority, "peer collector");
        let txns_id = ShardId::new();
        let _wal: TxnsHandle<SourceData, (), Timestamp, StorageDiff, TxnsCodecRow> =
            TxnsHandle::open(
                0.into(),
                persist.clone(),
                mz_dyncfgs::all_dyncfgs(),
                Arc::new(Metrics::new(&MetricsRegistry::new())),
                txns_id,
                Opaque::encode(&0u64),
            )
            .await;
        let txns = TxnsRead::start::<TxnsCodecRow>(persist.clone(), txns_id).await;
        let reader = CollectionReader::new(persist, txns);
        let row = |incarnation| Row::pack([Datum::UInt64(incarnation)]);
        let retired = |row: &Row| row.iter().next() == Some(Datum::UInt64(1));
        monotonic_append(
            &mut writer,
            vec![
                TimestamplessUpdate {
                    row: row(1),
                    diff: Diff::from(2),
                },
                TimestamplessUpdate {
                    row: row(2),
                    diff: Diff::ONE,
                },
            ],
            0.into(),
        )
        .await;

        // Both collectors have read the same prefix before either submits.
        let first = prepare_retired_rows(&reader, &mut writer, id, metadata.clone(), retired)
            .await
            .unwrap()
            .unwrap();
        let second = prepare_retired_rows(&reader, &mut peer, id, metadata.clone(), retired)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.upper, second.upper);
        first.commit(&mut writer, id).await.unwrap();
        assert!(matches!(
            second.commit(&mut peer, id).await,
            Err(StorageError::InvalidUppers(_))
        ));
        let upper = writer.fetch_recent_upper().await.clone();
        assert!(
            !retract_retired_rows(&reader, &mut peer, id, metadata.clone(), retired)
                .await
                .unwrap()
        );
        assert_eq!(writer.fetch_recent_upper().await, &upper);

        // A delayed owner-local retraction can arrive after collection. A new
        // admission remains ineligible under the older retirement predicate.
        monotonic_append(
            &mut writer,
            vec![
                TimestamplessUpdate {
                    row: row(1),
                    diff: -Diff::ONE,
                },
                TimestamplessUpdate {
                    row: row(3),
                    diff: Diff::ONE,
                },
            ],
            0.into(),
        )
        .await;
        assert!(
            retract_retired_rows(&reader, &mut peer, id, metadata.clone(), retired)
                .await
                .unwrap()
        );
        let as_of = writer
            .fetch_recent_upper()
            .await
            .as_option()
            .unwrap()
            .step_back()
            .unwrap();
        let mut rows = reader
            .snapshot_and_fetch(id, metadata, as_of)
            .await
            .unwrap();
        rows.sort();
        assert_eq!(rows, vec![(row(2), 1), (row(3), 1)]);
        peer.expire().await;
        writer.expire().await;
        authority.expire().await;
    }
}
