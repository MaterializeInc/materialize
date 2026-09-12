// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Request-owned real-time recency reads backed by durable client protection.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc};
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::controller::StorageError;
use mz_storage_types::sources::{GenericSourceConnection, SourceData};

use super::{QueryClient, diagnostics};
use crate::CollectionIdBundle;
use crate::catalog::Catalog;

impl QueryClient {
    /// Acquire remap protection and subscribe before sampling upstream frontiers.
    /// Must be polled off the coordinator loop because grant misses use its writer.
    pub(crate) async fn real_time_recent_timestamp(
        &self,
        catalog: &Catalog,
        timestamp_objects: BTreeSet<GlobalId>,
        config: StorageConfiguration,
        timeout: Duration,
    ) -> Result<Timestamp, StorageError> {
        let mut requests = Vec::new();
        for id in timestamp_objects.into_iter().filter(GlobalId::is_user) {
            let entry = catalog
                .try_get_entry_by_global_id(&id)
                .ok_or(StorageError::RtrDropFailure(id))?;
            let CatalogItem::Source(source) = entry.item() else {
                continue;
            };
            let (desc, remap_id) = match &source.data_source {
                DataSourceDesc::Ingestion { desc, .. } => (desc, id),
                DataSourceDesc::OldSyntaxIngestion {
                    desc,
                    progress_subsource,
                    ..
                } => (
                    desc,
                    catalog
                        .try_get_entry(progress_subsource)
                        .ok_or(StorageError::RtrDropFailure(id))?
                        .latest_global_id(),
                ),
                _ => continue,
            };
            let connection = desc
                .clone()
                .into_inline_connection(catalog.state())
                .connection;
            if matches!(connection, GenericSourceConnection::LoadGenerator(_)) {
                continue;
            }
            let config = config.clone();
            requests.push(async move {
                tokio::time::timeout(timeout, async move {
                    let metadata = self
                        .collection_metadata(catalog, remap_id)
                        .map_err(|error| StorageError::Generic(error.into()))?;
                    let bundle = CollectionIdBundle {
                        storage_ids: BTreeSet::from([remap_id]),
                        compute_ids: Default::default(),
                    };
                    // Observed sinces and cached grant floors are not authority.
                    // This returns tokens only after checking committed and pending
                    // publications, or acknowledging an expanded durable grant.
                    let (holds, _) = self
                        .acquire_read_holds_and_upper(catalog, &bundle, |_| Ok(None))
                        .await
                        .map_err(|error| StorageError::Generic(error.into()))?;
                    let as_of = holds.least_valid_read();
                    if as_of.is_empty() {
                        return Err(StorageError::ReadBeforeSince(remap_id));
                    }
                    let reader = self
                        .persist
                        .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
                            metadata.data_shard,
                            Arc::new(metadata.relation_desc),
                            Arc::new(UnitSchema),
                            diagnostics(remap_id),
                            USE_CRITICAL_SINCE_SNAPSHOT.get(self.persist.dyncfgs()),
                        )
                        .await
                        .map_err(|error| StorageError::Generic(error.into()))?;
                    // The kernel samples the external frontier immediately. Establish
                    // the subscription first, while retaining the granted as-of.
                    let subscribe = reader
                        .subscribe(as_of.clone())
                        .await
                        .map_err(|_| StorageError::ReadBeforeSince(remap_id))?;
                    let result = mz_controller::real_time_recency_ts(
                        connection, id, config, as_of, subscribe,
                    )
                    .await;
                    drop(holds);
                    result
                })
                .await
                .map_err(|_| StorageError::RtrTimeout(id))?
            });
        }
        futures::future::join_all(requests)
            .await
            .into_iter()
            .try_fold(Timestamp::MIN, |time, result| result.map(|t| time.max(t)))
    }
}
