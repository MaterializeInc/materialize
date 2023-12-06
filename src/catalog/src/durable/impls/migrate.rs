// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use mz_ore::now::EpochMillis;

use crate::durable::debug::{DebugCatalogState, Trace};
use crate::durable::impls::persist::UnopenedPersistCatalogState;
use crate::durable::impls::stash::OpenableConnection;
use crate::durable::{
    BootstrapArgs, CatalogError, DurableCatalogState, OpenableDurableCatalogState,
};

// Note: All reads done in this file can be fenced out by a new writer. All writers start by first
// fencing the stash and then fencing persist. Therefore, when performing operations like:
//
//   1. Read tombstone from stash.
//   2. Use tombstone to decide to either read something from the stash or persist.
//
// We don't have to worry about race conditions where a new writer updates the stash or persist
// inbetween steps (1) and (2).

#[derive(Debug)]
pub struct MigrateToPersist {
    openable_stash: Box<OpenableConnection>,
    openable_persist: Box<UnopenedPersistCatalogState>,
}

#[async_trait]
impl OpenableDurableCatalogState for MigrateToPersist {
    async fn open_savepoint(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self
            .openable_stash
            .open_savepoint(boot_ts.clone(), bootstrap_args, deploy_generation.clone())
            .await?;
        let mut persist = self
            .openable_persist
            .open_savepoint(boot_ts, bootstrap_args, deploy_generation)
            .await?;
        Self::migrate_from_stash_to_persist(stash, &mut persist).await?;
        Ok(persist)
    }

    async fn open_read_only(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let mut stash = self
            .openable_stash
            .open_read_only(boot_ts.clone(), bootstrap_args)
            .await?;
        let persist = self
            .openable_persist
            .open_read_only(boot_ts, bootstrap_args)
            .await?;
        let tombstone = stash.get_tombstone().await?;
        if tombstone == Some(true) {
            Ok(persist)
        } else {
            Ok(stash)
        }
    }

    async fn open(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self
            .openable_stash
            .open(boot_ts.clone(), bootstrap_args, deploy_generation.clone())
            .await?;
        fail::fail_point!("migrate_post_stash_fence");
        let mut persist = self
            .openable_persist
            .open(boot_ts, bootstrap_args, deploy_generation)
            .await?;
        fail::fail_point!("migrate_post_fence");
        Self::migrate_from_stash_to_persist(stash, &mut persist).await?;
        Ok(persist)
    }

    async fn open_debug(self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        panic!("cannot use the debug tool with the migrate implementation")
    }

    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        let tombstone = self.get_tombstone().await?;
        if tombstone == Some(true) {
            self.openable_persist.is_initialized().await
        } else {
            self.openable_stash.is_initialized().await
        }
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        let tombstone = self.get_tombstone().await?;
        if tombstone == Some(true) {
            self.openable_persist.get_deployment_generation().await
        } else {
            self.openable_stash.get_deployment_generation().await
        }
    }

    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError> {
        self.openable_stash.get_tombstone().await
    }

    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        panic!("cannot get a trace with the migrate implementation")
    }

    async fn expire(self: Box<Self>) {
        self.openable_stash.expire().await;
        self.openable_persist.expire().await;
    }
}

impl MigrateToPersist {
    pub(crate) fn new(
        openable_stash: OpenableConnection,
        openable_persist: UnopenedPersistCatalogState,
    ) -> MigrateToPersist {
        let openable_stash = Box::new(openable_stash);
        let openable_persist = Box::new(openable_persist);
        MigrateToPersist {
            openable_stash,
            openable_persist,
        }
    }

    async fn migrate_from_stash_to_persist(
        mut stash: Box<dyn DurableCatalogState>,
        persist: &mut Box<dyn DurableCatalogState>,
    ) -> Result<(), CatalogError> {
        let tombstone = stash.get_tombstone().await?;
        if tombstone == Some(true) {
            return Ok(());
        }

        let (stash_snapshot, stash_audit_logs, stash_storage_usages) =
            stash.full_snapshot().await?;

        let (mut persist_txn, persist_audit_logs, persist_storage_usages) =
            persist.full_transaction().await?;
        persist_txn.clear(persist_audit_logs, persist_storage_usages);
        persist_txn.set_snapshot(stash_snapshot)?;
        for stash_audit_log in stash_audit_logs {
            persist_txn.insert_audit_log_event(stash_audit_log);
        }
        for stash_storage_usage in stash_storage_usages {
            persist_txn.insert_storage_usage_event(stash_storage_usage);
        }
        persist_txn.commit().await?;

        fail::fail_point!("migrate_post_write");

        let mut stash_txn = stash.transaction().await?;
        stash_txn.set_tombstone(true)?;
        stash_txn.commit().await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct RollbackToStash {
    openable_stash: Box<OpenableConnection>,
    openable_persist: Box<UnopenedPersistCatalogState>,
}

#[async_trait]
impl OpenableDurableCatalogState for RollbackToStash {
    async fn open_savepoint(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let mut stash = self
            .openable_stash
            .open_savepoint(boot_ts.clone(), bootstrap_args, deploy_generation.clone())
            .await?;
        let persist = self
            .openable_persist
            .open_savepoint(boot_ts, bootstrap_args, deploy_generation)
            .await?;
        Self::rollback_from_persist_to_stash(&mut stash, persist).await?;
        Ok(stash)
    }

    async fn open_read_only(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let mut stash = self
            .openable_stash
            .open_read_only(boot_ts.clone(), bootstrap_args)
            .await?;
        let persist = self
            .openable_persist
            .open_read_only(boot_ts, bootstrap_args)
            .await?;
        let tombstone = stash.get_tombstone().await?;
        if tombstone == Some(true) {
            Ok(persist)
        } else {
            Ok(stash)
        }
    }

    async fn open(
        self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let mut stash = self
            .openable_stash
            .open(boot_ts.clone(), bootstrap_args, deploy_generation.clone())
            .await?;
        fail::fail_point!("rollback_post_stash_fence");
        let persist = self
            .openable_persist
            .open(boot_ts, bootstrap_args, deploy_generation)
            .await?;
        fail::fail_point!("rollback_post_fence");
        Self::rollback_from_persist_to_stash(&mut stash, persist).await?;
        Ok(stash)
    }

    async fn open_debug(self: Box<Self>) -> Result<DebugCatalogState, CatalogError> {
        panic!("cannot use the debug tool with the rollback implementation")
    }

    async fn is_initialized(&mut self) -> Result<bool, CatalogError> {
        let tombstone = self.get_tombstone().await?;
        if tombstone == Some(true) {
            self.openable_persist.is_initialized().await
        } else {
            self.openable_stash.is_initialized().await
        }
    }

    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError> {
        let tombstone = self.get_tombstone().await?;
        if tombstone == Some(true) {
            self.openable_persist.get_deployment_generation().await
        } else {
            self.openable_stash.get_deployment_generation().await
        }
    }

    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError> {
        self.openable_stash.get_tombstone().await
    }

    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        panic!("cannot get a trace with the rollback implementation")
    }

    async fn expire(self: Box<Self>) {
        self.openable_stash.expire().await;
        self.openable_persist.expire().await;
    }
}

impl RollbackToStash {
    pub(crate) fn new(
        openable_stash: OpenableConnection,
        openable_persist: UnopenedPersistCatalogState,
    ) -> RollbackToStash {
        let openable_stash = Box::new(openable_stash);
        let openable_persist = Box::new(openable_persist);
        RollbackToStash {
            openable_stash,
            openable_persist,
        }
    }

    async fn rollback_from_persist_to_stash(
        stash: &mut Box<dyn DurableCatalogState>,
        mut persist: Box<dyn DurableCatalogState>,
    ) -> Result<(), CatalogError> {
        let tombstone = stash.get_tombstone().await?;
        if tombstone.is_none() || tombstone == Some(false) {
            return Ok(());
        }

        let (persist_snapshot, persist_audit_logs, persist_storage_usages) =
            persist.full_snapshot().await?;

        let (mut stash_txn, stash_audit_logs, stash_storage_usages) =
            stash.full_transaction().await?;
        stash_txn.clear(stash_audit_logs, stash_storage_usages);
        stash_txn.set_snapshot(persist_snapshot)?;
        for persist_audit_log in persist_audit_logs {
            stash_txn.insert_audit_log_event(persist_audit_log);
        }
        for persist_storage_usage in persist_storage_usages {
            stash_txn.insert_storage_usage_event(persist_storage_usage);
        }
        stash_txn.set_tombstone(false)?;
        stash_txn.commit().await?;

        Ok(())
    }
}
