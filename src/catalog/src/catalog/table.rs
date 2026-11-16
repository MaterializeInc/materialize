// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Finite completion of committed table infrastructure before local reconstruction.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::Context;
use mz_persist_client::critical::Opaque;
use mz_persist_client::schema::CaESchema;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, RelationVersion, Timestamp};
use mz_storage_client::controller::PersistEpoch;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::TxnsCodecRow;
use mz_storage_types::sources::SourceData;
use mz_timestamp_oracle::TimestampOracle;
use mz_txn_wal::txns::{Tidy, TxnsHandle};

use crate::catalog::{Catalog, CatalogError};
use crate::memory::objects::{CatalogEntry, CatalogItem, TableDataSource};

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

#[derive(Clone, PartialEq)]
struct TableBinding {
    id: GlobalId,
    version: RelationVersion,
    desc: RelationDesc,
    previous_version: Option<RelationVersion>,
}

/// Local handles and incremental WAL state retained across registration retries.
/// This is not a registration grant. Completion must validate the current catalog.
pub struct PreparedTableInfrastructure {
    persist: PersistClient,
    selected: Option<BTreeSet<CatalogItemId>>,
    bindings: BTreeMap<ShardId, Vec<TableBinding>>,
    txns: TableTxns,
    routes: BTreeMap<GlobalId, ShardId>,
    tidy: Tidy,
    lower_bound: Timestamp,
}

impl PreparedTableInfrastructure {
    pub(crate) fn finish(self, completed_at: Timestamp) -> TableInfrastructure {
        TableInfrastructure {
            txns: self.txns,
            routes: self.routes,
            tidy: self.tidy,
            completed_at,
        }
    }

    async fn refresh(&mut self, catalog: &mut Catalog) -> Result<bool, CatalogError> {
        catalog.sync_to_current_updates().await?;
        let bindings = catalog.table_registration_inventory(self.selected.as_ref())?;
        if bindings == self.bindings {
            return Ok(true);
        }
        let mut handles = BTreeMap::new();
        let mut schema_error = None;
        'shards: for (shard, versions) in &bindings {
            if self.bindings.get(shard) == Some(versions) {
                continue;
            }
            // Evolve changed shards in version order. Unchanged shards keep their
            // adopted handles, including through rejected WAL writes.
            for binding in versions {
                let id = binding.id;
                let desc = Arc::new(binding.desc.clone());
                let mut handle = match self
                    .persist
                    .open_writer(
                        *shard,
                        Arc::clone(&desc),
                        Arc::new(UnitSchema),
                        Diagnostics {
                            shard_name: id.to_string(),
                            handle_purpose: "complete committed table infrastructure".into(),
                        },
                    )
                    .await
                {
                    Ok(handle) => handle,
                    Err(error) => {
                        schema_error = Some(anyhow::anyhow!("table {id}: {error:?}"));
                        break 'shards;
                    }
                };
                if handle.try_register_schema().await.is_none()
                    && let Some(previous_version) = binding.previous_version
                {
                    match self
                        .persist
                        .compare_and_evolve_schema::<SourceData, (), Timestamp, StorageDiff>(
                            *shard,
                            previous_version.into(),
                            &desc,
                            &UnitSchema,
                            Diagnostics {
                                shard_name: id.to_string(),
                                handle_purpose: "complete committed table schema".into(),
                            },
                        )
                        .await
                    {
                        Ok(CaESchema::Ok(_)) | Ok(CaESchema::ExpectedMismatch { .. }) => {}
                        error => {
                            schema_error = Some(anyhow::anyhow!(
                                "cannot evolve committed schema for table {id} in {shard}: {error:?}"
                            ));
                            break 'shards;
                        }
                    }
                }
                if handle.try_register_schema().await.is_none() {
                    schema_error = Some(anyhow::anyhow!(
                        "cannot register committed schema for table {id} in {shard}"
                    ));
                    break 'shards;
                }
                handles.insert(*shard, handle);
            }
        }
        if let Some(error) = schema_error {
            // Finalization may win during preparation. Only classify the error
            // as stale preparation after observing changed catalog bindings.
            catalog.sync_to_current_updates().await?;
            if catalog.table_registration_inventory(self.selected.as_ref())? != bindings {
                return Ok(false);
            }
            return Err(error.into());
        }
        self.txns.adopt(handles.into_values());
        self.txns
            .retain_adopted(&bindings.keys().copied().collect());
        self.routes = bindings
            .iter()
            .flat_map(|(shard, versions)| versions.iter().map(|binding| (binding.id, *shard)))
            .collect();
        self.bindings = bindings;
        Ok(true)
    }

    async fn complete_observed_prefix(
        &mut self,
        upper: Timestamp,
    ) -> Result<Timestamp, CatalogError> {
        let completed_at = upper
            .step_back()
            .context("initialized WAL must have completed progress")?;
        // The observed upper already passed this cut. Do not wait for a newer
        // oracle timestamp or submit an empty registration just to attach handles.
        let tidy = self.txns.apply_le(&completed_at).await;
        self.tidy.merge(tidy);
        Ok(completed_at)
    }
}

impl Catalog {
    /// Completes committed writable tables before adapter reconstruction or fresh
    /// initialization. Handles and the WAL cache survive definitive conflicts.
    /// This consumes caught-up updates, so reconstruct from the resulting catalog.
    pub async fn complete_table_infrastructure(
        &mut self,
        persist: PersistClient,
        metrics: Arc<mz_txn_wal::metrics::Metrics>,
        oracle: &(dyn TimestampOracle<Timestamp> + Send + Sync),
    ) -> Result<TableInfrastructure, CatalogError> {
        let mut prepared = self
            .prepare_table_infrastructure(persist, metrics, None)
            .await?;
        loop {
            if let Some(completed_at) = self
                .try_complete_table_infrastructure(&mut prepared, oracle)
                .await?
            {
                return Ok(prepared.finish(completed_at));
            }
            tokio::task::yield_now().await;
        }
    }

    /// Opens retained state for completing either selected writable tables or all
    /// writable tables. Preparation and adoption grant no shared write authority.
    /// Only an initializing catalog handle may upgrade the WAL format.
    pub async fn prepare_table_infrastructure(
        &mut self,
        persist: PersistClient,
        metrics: Arc<mz_txn_wal::metrics::Metrics>,
        selected: Option<BTreeSet<CatalogItemId>>,
    ) -> Result<PreparedTableInfrastructure, CatalogError> {
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
        Ok(PreparedTableInfrastructure {
            persist,
            selected,
            bindings: BTreeMap::new(),
            txns,
            routes: BTreeMap::new(),
            tidy: Tidy::default(),
            lower_bound: Timestamp::MIN,
        })
    }

    /// Completes observed registration or attempts one registration write.
    /// Returns `None` after definitive contention or changed bindings. Retain
    /// `prepared` across retries and return to the owner's scheduling loop first.
    /// No submitted write or oracle completion is cancelled by this bound.
    pub async fn try_complete_table_infrastructure(
        &mut self,
        prepared: &mut PreparedTableInfrastructure,
        oracle: &(dyn TimestampOracle<Timestamp> + Send + Sync),
    ) -> Result<Option<Timestamp>, CatalogError> {
        if !prepared.refresh(self).await? {
            return Ok(None);
        }
        let ids: BTreeSet<_> = prepared.bindings.keys().copied().collect();
        let (upper, registered) = prepared.txns.registration_status(ids.iter().copied()).await;
        // Synchronization validates deployment admission as well as bindings.
        // Skipping registration must not let a retired writer complete bootstrap.
        self.sync_to_current_updates().await?;
        if self.table_registration_inventory(prepared.selected.as_ref())? != prepared.bindings {
            return Ok(None);
        }
        if registered == ids {
            return prepared.complete_observed_prefix(upper).await.map(Some);
        }

        for _ in 0..3 {
            // Allocate after schema/handle preparation, but before validating the
            // registration's catalog prefix. A later DROP's Forget must allocate
            // after this timestamp, so WAL CAS orders registration before Forget
            // or definitively rejects it. Cached absence alone is not authority.
            let write = oracle.write_ts().await;
            if write.timestamp < prepared.lower_bound {
                tokio::task::yield_now().await;
                continue;
            }
            let upper = write.timestamp.step_forward();
            self.advance_upper(upper).await?;
            self.sync_to_current_updates().await?;
            let position = self
                .observed_position()
                .context("table initialization requires an admitted catalog position")?;
            if self.table_registration_inventory(prepared.selected.as_ref())? != prepared.bindings {
                return Ok(None);
            }
            if position.upper > upper {
                tokio::task::yield_now().await;
                continue;
            }
            let (observed_upper, registered) =
                prepared.txns.registration_status(ids.iter().copied()).await;
            let missing: Vec<_> = ids.difference(&registered).copied().collect();
            if missing.is_empty() {
                return prepared
                    .complete_observed_prefix(observed_upper)
                    .await
                    .map(Some);
            }
            #[cfg(test)]
            if let Some(observer) = self.before_table_registration.take() {
                let (resume, wait) = tokio::sync::oneshot::channel();
                observer.send(resume).expect("registration observer");
                wait.await.expect("resume registration");
            }
            match prepared
                .txns
                .register_adopted(write.timestamp, missing)
                .await
            {
                Ok(tidy) => {
                    prepared.tidy.merge(tidy);
                    oracle.apply_write(write.timestamp).await;
                    return Ok(Some(write.timestamp));
                }
                Err(upper) => {
                    prepared.lower_bound = upper;
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    fn table_registration_inventory(
        &self,
        selected: Option<&BTreeSet<CatalogItemId>>,
    ) -> Result<BTreeMap<ShardId, Vec<TableBinding>>, CatalogError> {
        let mut inventory: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let entries: Box<dyn Iterator<Item = &CatalogEntry> + '_> = match selected {
            Some(ids) => Box::new(ids.iter().filter_map(|id| self.try_get_entry(id))),
            None => Box::new(self.entries()),
        };
        for entry in entries {
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
                    inventory.entry(shard).or_default().push(TableBinding {
                        id,
                        version,
                        desc,
                        previous_version,
                    });
                    previous_version = Some(version);
                }
            }
        }
        Ok(inventory)
    }
}
