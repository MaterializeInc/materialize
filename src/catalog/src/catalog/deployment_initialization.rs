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

use mz_repr::Timestamp;
use mz_timestamp_oracle::TimestampOracle;

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
            let mut storage = self.catalog.storage().await;
            if self.catalog.diagnostic_config.read_only
                || storage.is_read_only()
                || storage.is_savepoint()
                || !storage.is_join()
                || self.catalog.state().active_deployment_generation()
                    != Some(self.catalog.state().deployment_generation())
            {
                return Err(CatalogError::ReadOnly);
            }
            if storage.controller_initialization_pending().await? {
                return Err(anyhow::anyhow!("controller initialization is not complete").into());
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
    /// Requires explicit unfinished controller provenance. An interrupted attempt
    /// reconciles its seed before publishing completion. Completed deployments
    /// must use join-only restart, never this path. Retains no maintenance task.
    pub async fn complete_fresh_deployment_initialization(
        mut self,
        persist: mz_persist_client::PersistClient,
        metrics: &mz_ore::metrics::MetricsRegistry,
        oracle: &(dyn TimestampOracle<Timestamp> + Send + Sync),
    ) -> Result<Catalog, CatalogError> {
        {
            let mut storage = self.catalog.storage().await;
            if self.catalog.diagnostic_config.read_only
                || storage.is_read_only()
                || storage.is_savepoint()
                || storage.is_join()
            {
                return Err(CatalogError::ReadOnly);
            }
            if !storage.controller_initialization_pending().await? {
                return Err(anyhow::anyhow!(
                    "initialization requires explicit unfinished controller provenance"
                )
                .into());
            }
        }
        if !self.migrated_storage_collections_0dt.is_empty() {
            return Err(anyhow::anyhow!(
                "controller initialization does not support replacement migrations"
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
        // Fence, snapshot and compare-and-append the differences. Repeating an
        // interrupted seed must not append its positive rows a second time.
        self.catalog
            .reconcile_initial_builtin_snapshot(
                &persist,
                &mut infrastructure,
                &self.builtin_table_updates,
                oracle,
            )
            .await?;
        loop {
            self.catalog.sync_to_current_updates().await?;
            let mut storage = self.catalog.storage().await;
            let mut txn = match storage.transaction().await {
                Err(crate::durable::CatalogError::Durable(
                    crate::durable::DurableCatalogError::CatalogOutOfSync { .. },
                )) => continue,
                result => result?,
            };
            let key = crate::durable::CONTROLLER_INITIALIZATION_COMPLETE_KEY.to_string();
            if txn.get_config(key.clone()) != Some(0) {
                return Err(anyhow::anyhow!("controller initialization ownership changed").into());
            }
            txn.set_config(key, Some(1))?;
            // Config metadata has no in-memory catalog projection.
            let _ = txn.get_and_commit_op_updates();
            match txn.commit_with_upper(oracle.write_ts().await).await {
                Err(crate::durable::CatalogError::Durable(
                    crate::durable::DurableCatalogError::CatalogOutOfSync { .. },
                )) => continue,
                result => {
                    result?;
                    break;
                }
            }
        }
        // Bootstrap handles are dropped here. Serving adapters adopt their own
        // routes through join-only startup, not a controller-owned table worker.
        drop(infrastructure);
        Ok(self.catalog)
    }
}
