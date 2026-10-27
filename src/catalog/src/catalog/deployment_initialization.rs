// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fresh-environment infrastructure completion before process launch.

use std::sync::Arc;

use anyhow::Context;
use mz_repr::Timestamp;
use mz_storage_client::client::TableData;
use mz_storage_types::sources::SourceData;
use mz_timestamp_oracle::TimestampOracle;

use crate::builtin::{
    MZ_CLUSTER_REPLICA_SIZE_INTERNAL, MZ_CLUSTER_REPLICA_SIZES, MZ_EGRESS_IPS, MZ_LICENSE_KEYS,
};
use crate::catalog::{Catalog, CatalogError};

impl crate::catalog::OpenCatalogResult {
    /// Reconciles active-controller launch configuration after join-only restart.
    ///
    /// The caller is the deployment's controller, not a serving adapter. Joining
    /// preserves admission and format limits. This never seeds the positive
    /// builtin snapshot or recovers interrupted initialization or migrations.
    pub async fn complete_deployment_restart(
        mut self,
        persist: mz_persist_client::PersistClient,
        metrics: &mz_ore::metrics::MetricsRegistry,
        oracle: &(dyn TimestampOracle<Timestamp> + Send + Sync),
    ) -> Result<Catalog, CatalogError> {
        {
            let storage = self.catalog.storage().await;
            if self.catalog.diagnostic_config.read_only
                || storage.is_read_only()
                || storage.is_savepoint()
                || !storage.is_join()
                || self.catalog.state().active_deployment_generation()
                    != Some(self.catalog.state().deployment_generation())
            {
                return Err(CatalogError::ReadOnly);
            }
        }
        if self.initialized_catalog || !self.migrated_storage_collections_0dt.is_empty() {
            return Err(anyhow::anyhow!(
                "controller restart requires an initialized catalog without replacement migrations"
            )
            .into());
        }
        self.catalog.initialize_storage_metadata().await?;
        let mut infrastructure = self
            .catalog
            .complete_table_infrastructure(
                persist.clone(),
                Arc::new(mz_txn_wal::metrics::Metrics::new(metrics)),
                oracle,
            )
            .await?;
        self.catalog
            .reconcile_deployment_configuration(
                &persist,
                &mut infrastructure,
                &self.builtin_table_updates,
                oracle,
            )
            .await?;
        drop(infrastructure);
        Ok(self.catalog)
    }

    /// Completes fresh active deployment infrastructure before launching children.
    ///
    /// Only the handle that committed fresh initialization may publish its positive
    /// builtin snapshot. Restart and upgrade callers must use their own recovery
    /// path, never replay this snapshot. This operation retains no maintenance task.
    pub async fn complete_fresh_deployment_initialization(
        mut self,
        persist: mz_persist_client::PersistClient,
        metrics: &mz_ore::metrics::MetricsRegistry,
        oracle: &(dyn TimestampOracle<Timestamp> + Send + Sync),
    ) -> Result<Catalog, CatalogError> {
        {
            let storage = self.catalog.storage().await;
            if self.catalog.diagnostic_config.read_only
                || storage.is_read_only()
                || storage.is_savepoint()
                || storage.is_join()
            {
                return Err(CatalogError::ReadOnly);
            }
        }
        if !self.initialized_catalog || !self.migrated_storage_collections_0dt.is_empty() {
            return Err(anyhow::anyhow!(
                "fresh initialization requires a newly initialized catalog without replacement migrations"
            ).into());
        }
        self.catalog.initialize_storage_metadata().await?;
        let mut infrastructure = self
            .catalog
            .complete_table_infrastructure(
                persist.clone(),
                Arc::new(mz_txn_wal::metrics::Metrics::new(metrics)),
                oracle,
            )
            .await?;
        {
            // Configuration is reconciled separately, including after restart.
            // Never seed it and reconcile from the same stale positive snapshot.
            let configuration_ids: std::collections::BTreeSet<_> = [
                &*MZ_CLUSTER_REPLICA_SIZES,
                &*MZ_CLUSTER_REPLICA_SIZE_INTERNAL,
                &*MZ_EGRESS_IPS,
                &*MZ_LICENSE_KEYS,
            ]
            .into_iter()
            .map(|table| self.catalog.resolve_builtin_table(table))
            .collect();
            let mut txn = infrastructure.txns.begin();
            for update in &self.builtin_table_updates {
                if configuration_ids.contains(&update.id) {
                    continue;
                }
                let table = self
                    .catalog
                    .get_entry(&update.id)
                    .table()
                    .context("builtin snapshot targets a table")?;
                let shard = infrastructure
                    .routes
                    .get(&table.global_id_writes())
                    .context("builtin snapshot requires registered table")?;
                match &update.data {
                    TableData::Rows(rows) => {
                        for (row, diff) in rows {
                            txn.write(shard, SourceData(Ok(row.clone())), (), diff.into_inner())
                                .await;
                        }
                    }
                    TableData::Batches(batches) => {
                        for batch in batches {
                            txn.write_batch(shard, batch.clone());
                        }
                    }
                }
            }
            txn.tidy(std::mem::take(&mut infrastructure.tidy));
            let mut lower_bound = infrastructure.completed_at.step_forward();
            loop {
                let write = oracle.write_ts().await;
                if write.timestamp < lower_bound {
                    tokio::task::yield_now().await;
                    continue;
                }
                self.catalog
                    .advance_upper(write.timestamp.step_forward())
                    .await?;
                match txn
                    .commit_at(&mut infrastructure.txns, write.timestamp)
                    .await
                {
                    Ok(apply) => {
                        infrastructure
                            .tidy
                            .merge(apply.apply(&mut infrastructure.txns).await);
                        oracle.apply_write(write.timestamp).await;
                        infrastructure.completed_at = write.timestamp;
                        break;
                    }
                    Err(upper) => lower_bound = upper,
                }
            }
        }
        self.catalog
            .reconcile_deployment_configuration(
                &persist,
                &mut infrastructure,
                &self.builtin_table_updates,
                oracle,
            )
            .await?;
        // Bootstrap handles are dropped here. Serving adapters adopt their own
        // routes through join-only startup, not a controller-owned table worker.
        drop(infrastructure);
        Ok(self.catalog)
    }
}
