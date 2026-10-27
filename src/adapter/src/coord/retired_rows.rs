// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conditional cleanup of retired execution-owned current observations.

use std::sync::Arc;

use mz_catalog::builtin::{
    BuiltinTable, MZ_SESSIONS_RAW, MZ_STORAGE_STATUSES_RAW, MZ_SUBSCRIPTIONS_RAW,
};
use mz_catalog::durable::ClientIncarnationRetirement;
use mz_ore::task;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{ColumnName, Datum, Diff, Row, Timestamp};
use mz_storage_client::collection_writer::retract_retired_rows;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;

use crate::catalog::Catalog;
use crate::command::{Command, WriteAttemptKind};
use crate::coord::Coordinator;
use crate::coord::appends::WriteResult;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::peek_client::CoordinatorClient;
use crate::query_client::QueryClient;

impl Coordinator {
    pub(super) fn collect_retired_rows(&mut self) {
        if self.controller.read_only()
            || !self.controller.replica_owned_compute()
            || self
                .retired_rows_sweep
                .as_ref()
                .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        let Some(client) = self.query_client.clone() else {
            return;
        };
        let catalog = self.owned_catalog();
        let persist = self.persist_client.clone();
        let coordinator = CoordinatorClient::Background {
            tx: self.internal_cmd_tx.clone(),
            metrics: self.metrics.clone(),
        };
        self.retired_rows_sweep = Some(
            task::spawn(|| "retired_rows_sweep", async move {
                // A cached certificate is conservative. Release the durable mutex
                // before requesting protection or doing any collection I/O.
                let retirement = {
                    let durable = catalog.storage().await;
                    durable.client_incarnation_retirement()
                };
                let retirement = match retirement {
                    Ok(retirement) => retirement,
                    Err(error) => {
                        tracing::warn!(%error, "unable to certify retired row owners");
                        return;
                    }
                };
                let result = collect_current_status(&catalog, &client, &persist, &retirement).await;
                if let Err(error) = result {
                    tracing::warn!(%error, "unable to collect retired current-status rows");
                }
                for table in [&*MZ_SESSIONS_RAW, &*MZ_SUBSCRIPTIONS_RAW] {
                    let result =
                        collect_session_rows(&catalog, &client, &coordinator, &retirement, table)
                            .await;
                    if let Err(error) = result {
                        tracing::warn!(
                            %error, table = table.name, "unable to collect retired session rows"
                        );
                    }
                }
            })
            .abort_on_drop(),
        );
    }
}

async fn collect_current_status(
    catalog: &Catalog,
    client: &QueryClient,
    persist: &PersistClient,
    retirement: &ClientIncarnationRetirement,
) -> anyhow::Result<()> {
    let item = catalog.resolve_builtin_storage_collection(&MZ_STORAGE_STATUSES_RAW);
    let id = catalog.get_entry(&item).latest_global_id();
    let metadata = client.collection_metadata(catalog, id)?;
    let owner_column = metadata
        .relation_desc
        .get_by_name(&ColumnName::from("client_incarnation"))
        .expect("current status has an execution owner")
        .0;
    let mut writer = persist
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            metadata.data_shard,
            Arc::new(metadata.relation_desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics {
                shard_name: id.to_string(),
                handle_purpose: "retired current-status cleanup".into(),
            },
        )
        .await?;
    let result: anyhow::Result<()> = async {
        let Some(as_of) = writer
            .fetch_recent_upper()
            .await
            .as_option()
            .and_then(|upper| upper.step_back())
        else {
            return Ok(());
        };
        let bundle = CollectionIdBundle {
            storage_ids: [id].into(),
            ..Default::default()
        };
        let (holds, _) = client
            .acquire_read_holds_and_upper(catalog, &bundle, |_| Ok(Some(as_of)))
            .await?;
        if !holds.least_valid_read().less_equal(&as_of) {
            return Ok(());
        }
        // The helper resamples the same shard's monotone upper.
        // These holds also cover its potentially newer snapshot.
        let result = retract_retired_rows(
            client.collection_reader().await,
            &mut writer,
            id,
            metadata,
            |row| match row.iter().nth(owner_column) {
                Some(Datum::UInt64(incarnation)) => retirement.is_retired(incarnation),
                _ => false,
            },
        )
        .await;
        drop(holds);
        result?;
        Ok(())
    }
    .await;
    writer.expire().await;
    result
}

async fn collect_session_rows(
    catalog: &Catalog,
    client: &QueryClient,
    coordinator: &CoordinatorClient,
    retirement: &ClientIncarnationRetirement,
    table: &'static BuiltinTable,
) -> anyhow::Result<()> {
    let item = catalog.resolve_builtin_table(table);
    let id = catalog.get_entry(&item).latest_global_id();
    let metadata = client.collection_metadata(catalog, id)?;
    assert!(
        metadata.txns_shard.is_some(),
        "session rows require txn-WAL"
    );
    let owner_column = metadata
        .relation_desc
        .get_by_name(&ColumnName::from("client_incarnation"))
        .expect("session rows have an execution owner")
        .0;
    let bundle = CollectionIdBundle {
        storage_ids: [id].into(),
        ..Default::default()
    };
    let (holds, upper) = client
        .acquire_read_holds_and_upper(catalog, &bundle, |upper| {
            Ok(upper.as_option().and_then(|upper| upper.step_back()))
        })
        .await?;
    let Some(as_of) = upper.as_option().and_then(|upper| upper.step_back()) else {
        return Ok(());
    };
    if !holds.least_valid_read().less_equal(&as_of) || as_of.checked_add(2).is_none() {
        return Ok(());
    }
    let mut snapshot = client
        .collection_reader()
        .await
        .snapshot_cursor(id, metadata, as_of)
        .await?;
    let mut diffs: Vec<(Row, Diff)> = Vec::new();
    while let Some(chunk) = snapshot.next().await {
        for (data, _, diff) in chunk {
            let row = data
                .0
                .map_err(|error| anyhow::anyhow!("invalid session row: {error}"))?;
            if let Some(Datum::UInt64(incarnation)) = row.iter().nth(owner_column)
                && retirement.is_retired(incarnation)
            {
                diffs.push((row, Diff::from(-diff)));
            }
        }
    }
    differential_dataflow::consolidation::consolidate(&mut diffs);
    if diffs.is_empty() {
        return Ok(());
    }
    // Snapshot completion establishes WAL upper > B. An exact write at B+1
    // can succeed only before any intervening WAL write. Coordinator admission
    // preserves registration FIFO and the committer owns oracle completion.
    let (tx, rx) = tokio::sync::oneshot::channel();
    coordinator.send(Command::AttemptWrite {
        attempt: WriteAttemptKind::Background {
            write_ts: as_of.step_forward(),
        },
        target_id: item,
        target_global_id: id,
        diffs,
        tx,
    });
    let result = rx.await?;
    drop(holds);
    match result {
        WriteResult::Success { .. } => Ok(()),
        // Never replay this candidate, including after a lost acknowledgement.
        result => anyhow::bail!("session cleanup did not complete: {result:?}"),
    }
}
