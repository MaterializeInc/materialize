// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Finite completion of committed table infrastructure before local reconstruction.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use mz_persist_client::critical::Opaque;
use mz_persist_client::schema::CaESchema;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::controller::PersistEpoch;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::TxnsCodecRow;
use mz_storage_types::sources::SourceData;
use mz_timestamp_oracle::TimestampOracle;
use mz_txn_wal::txns::{Tidy, TxnsHandle};

use crate::catalog::{Catalog, CatalogError};
use crate::memory::objects::{CatalogItem, TableDataSource};

/// The environment's table transaction handle.
pub type TableTxns = TxnsHandle<SourceData, (), Timestamp, StorageDiff, TxnsCodecRow>;

/// Completed shared registration and local routes, ready for a table writer.
pub struct TableInfrastructure {
    pub txns: TableTxns,
    pub routes: BTreeMap<GlobalId, ShardId>,
    pub tidy: Tidy,
    /// A completed WAL prefix, not permanent permission to read these tables.
    pub completed_at: Timestamp,
}

impl Catalog {
    /// Completes infrastructure for committed writable tables without a controller.
    ///
    /// Call after storage metadata initialization and before reconstructing local
    /// collections or serving SQL. This consumes caught-up catalog updates, so the
    /// caller must reconstruct from the resulting catalog rather than an earlier
    /// inventory. It neither initializes catalog definitions nor publishes their
    /// derived rows. Read-only and savepoint callers cannot perform this operation.
    ///
    /// Joining can finish a committed CREATE whose creator did not finish WAL
    /// registration. Only an initializing catalog handle upgrades the WAL format.
    pub async fn complete_table_infrastructure(
        &mut self,
        persist: PersistClient,
        metrics: Arc<mz_txn_wal::metrics::Metrics>,
        oracle: &(dyn TimestampOracle<Timestamp> + Send + Sync),
    ) -> Result<TableInfrastructure, CatalogError> {
        let initialize = {
            let storage = self.storage().await;
            if self.diagnostic_config.read_only || storage.is_read_only() || storage.is_savepoint()
            {
                return Err(CatalogError::ReadOnly);
            }
            !storage.is_join()
        };
        let mut txns = TxnsHandle::open(
            Timestamp::MIN,
            persist.clone(),
            persist.dyncfgs().clone(),
            metrics,
            self.txn_wal_shard().await?,
            Opaque::encode(&PersistEpoch::default()),
        )
        .await;
        if initialize {
            txns.upgrade_version().await;
        }

        let mut lower_bound = Timestamp::MIN;
        loop {
            // Use a genuine allocation, not a locally raised timestamp. A later
            // DROP's Forget must allocate after this attempt for WAL CAS to order
            // registration against retirement.
            let write = oracle.write_ts().await;
            if write.timestamp < lower_bound {
                tokio::task::yield_now().await;
                continue;
            }
            let upper = write.timestamp.step_forward();
            self.advance_upper(upper).await?;
            self.sync_to_current_updates().await?;
            let position = self
                .observed_position()
                .context("table initialization requires an admitted catalog position")?;
            if position.upper > upper {
                // Refresh can include newer definitions. In particular a rebased
                // DROP can precede oracle completion. Do not omit that suffix or
                // register its replacement definitions at an older timestamp.
                tokio::task::yield_now().await;
                continue;
            }

            let revision = self.transient_revision();
            let mut inventory = Vec::new();
            for entry in self.entries() {
                if let CatalogItem::Table(table) = entry.item()
                    && matches!(table.data_source, TableDataSource::TableWrites { .. })
                {
                    let mut previous_version = None;
                    for (id, version, desc) in table.collection_descs() {
                        let shard = self
                            .state()
                            .storage_metadata()
                            .get_collection_shard(id)
                            .with_context(|| format!("table {id} has no committed shard"))?;
                        inventory.push((id, shard, desc, previous_version));
                        previous_version = Some(version);
                    }
                }
            }

            let mut routes = BTreeMap::new();
            let mut handles = BTreeMap::new();
            let mut schema_error = None;
            // Versions are visited in order. Register every exact schema, while
            // retaining the newest handle for each shared physical shard.
            for (id, shard, desc, previous_version) in inventory {
                let desc = Arc::new(desc);
                let opened = persist
                    .open_writer(
                        shard,
                        Arc::clone(&desc),
                        Arc::new(UnitSchema),
                        Diagnostics {
                            shard_name: id.to_string(),
                            handle_purpose: "complete committed table infrastructure".into(),
                        },
                    )
                    .await;
                let mut handle = match opened {
                    Ok(handle) => handle,
                    Err(error) => {
                        schema_error = Some(anyhow::anyhow!("table {id}: {error:?}"));
                        break;
                    }
                };
                // Raw WAL registration expects this to succeed. Finalization can
                // win while handles open, so classify failure against fresh catalog
                // state instead of panicking in register's schema preflight.
                if handle.try_register_schema().await.is_none()
                    && let Some(previous_version) = previous_version
                {
                    // A committed ALTER can outlive the adapter that was to evolve
                    // its Persist schema. Replay the committed version chain, using
                    // the same version-to-schema mapping as ordinary ALTER.
                    let evolution = persist
                        .compare_and_evolve_schema::<SourceData, (), Timestamp, StorageDiff>(
                            shard,
                            previous_version.into(),
                            &desc,
                            &UnitSchema,
                            Diagnostics {
                                shard_name: id.to_string(),
                                handle_purpose: "complete committed table schema".into(),
                            },
                        )
                        .await;
                    match evolution {
                        Ok(CaESchema::Ok(_)) | Ok(CaESchema::ExpectedMismatch { .. }) => {}
                        error => {
                            schema_error = Some(anyhow::anyhow!(
                                "cannot evolve committed schema for table {id} in {shard}: {error:?}"
                            ));
                            break;
                        }
                    }
                }
                // A peer may have evolved beyond this version during our attempt.
                // Accept only the exact registered schema, never just a newer ID.
                if handle.try_register_schema().await.is_none() {
                    schema_error = Some(anyhow::anyhow!(
                        "cannot register committed schema for table {id} in {shard}"
                    ));
                    break;
                }
                routes.insert(id, shard);
                handles.insert(shard, handle);
            }
            if let Some(error) = schema_error {
                self.sync_to_current_updates().await?;
                if self.transient_revision() != revision {
                    continue;
                }
                return Err(error.into());
            }

            match txns.register(write.timestamp, handles.into_values()).await {
                Ok(tidy) => {
                    oracle.apply_write(write.timestamp).await;
                    return Ok(TableInfrastructure {
                        txns,
                        routes,
                        tidy,
                        completed_at: write.timestamp,
                    });
                }
                Err(upper) => {
                    // The attempt is definitively rejected. Recompute inventory
                    // after catch-up, rather than resurrecting a forgotten table
                    // by replaying the same registrations at a newer timestamp.
                    lower_bound = upper;
                }
            }
        }
    }
}
