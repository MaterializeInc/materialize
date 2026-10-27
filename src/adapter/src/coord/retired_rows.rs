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

use mz_catalog::builtin::MZ_STORAGE_STATUSES_RAW;
use mz_ore::task;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{ColumnName, Datum, Timestamp};
use mz_storage_client::collection_writer::retract_retired_rows;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;

use crate::coord::Coordinator;
use crate::coord::id_bundle::CollectionIdBundle;

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
        self.retired_rows_sweep = Some(
            task::spawn(|| "retired_rows_sweep", async move {
                let result: anyhow::Result<()> = async {
                    // This cached prefix can only delay discovery of retirement.
                    // Do not retain the catalog mutex during protection or Persist I/O.
                    let retirement = {
                        let durable = catalog.storage().await;
                        durable.client_incarnation_retirement()?
                    };
                    let item = catalog.resolve_builtin_storage_collection(&MZ_STORAGE_STATUSES_RAW);
                    let id = catalog.get_entry(&item).latest_global_id();
                    let metadata = client.collection_metadata(&catalog, id)?;
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
                            .acquire_read_holds_and_upper(&catalog, &bundle, |_| Ok(Some(as_of)))
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
                                Some(Datum::UInt64(incarnation)) => {
                                    retirement.is_retired(incarnation)
                                }
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
                .await;
                if let Err(error) = result {
                    tracing::warn!(%error, "unable to collect retired current-status rows");
                }
            })
            .abort_on_drop(),
        );
    }
}
