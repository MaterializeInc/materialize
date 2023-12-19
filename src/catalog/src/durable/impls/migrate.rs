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
use mz_sql::session::vars::CatalogKind;
use tracing::warn;

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
pub(crate) enum Direction {
    MigrateToPersist,
    RollbackToStash,
}

impl TryFrom<CatalogKind> for Direction {
    type Error = CatalogKind;

    fn try_from(catalog_kind: CatalogKind) -> Result<Self, Self::Error> {
        match catalog_kind {
            CatalogKind::Stash => Ok(Direction::RollbackToStash),
            CatalogKind::Persist => Ok(Direction::MigrateToPersist),
            CatalogKind::Shadow => Err(catalog_kind),
        }
    }
}

#[derive(Debug)]
pub struct CatalogMigrator {
    openable_stash: Box<OpenableConnection>,
    openable_persist: Box<UnopenedPersistCatalogState>,
    direction: Direction,
}

#[async_trait]
impl OpenableDurableCatalogState for CatalogMigrator {
    async fn open_savepoint(
        mut self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self
            .openable_stash
            .open_savepoint(boot_ts.clone(), bootstrap_args, deploy_generation.clone())
            .await?;
        let persist = self
            .openable_persist
            .open_savepoint(boot_ts, bootstrap_args, deploy_generation)
            .await?;
        Self::open_inner(stash, persist, self.direction).await
    }

    async fn open_read_only(
        self: Box<Self>,
        _boot_ts: EpochMillis,
        _bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        panic!("cannot use a read only catalog with the migrate implementation")
    }

    async fn open(
        mut self: Box<Self>,
        boot_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        let stash = self
            .openable_stash
            .open(boot_ts.clone(), bootstrap_args, deploy_generation.clone())
            .await?;
        fail::fail_point!("post_stash_fence");
        let persist = self
            .openable_persist
            .open(boot_ts, bootstrap_args, deploy_generation)
            .await?;
        fail::fail_point!("post_persist_fence");
        Self::open_inner(stash, persist, self.direction).await
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

    async fn get_catalog_kind_config(&mut self) -> Result<Option<CatalogKind>, CatalogError> {
        let tombstone = self.get_tombstone().await?;
        if tombstone == Some(true) {
            self.openable_persist.get_catalog_kind_config().await
        } else {
            self.openable_stash.get_catalog_kind_config().await
        }
    }

    async fn trace(&mut self) -> Result<Trace, CatalogError> {
        panic!("cannot get a trace with the migrate implementation")
    }

    fn set_catalog_kind(&mut self, catalog_kind: CatalogKind) {
        let direction = match catalog_kind.try_into() {
            Ok(direction) => direction,
            Err(catalog_kind) => {
                warn!("unable to set catalog kind to {catalog_kind:?}");
                return;
            }
        };
        self.direction = direction;
    }

    async fn expire(self: Box<Self>) {
        self.openable_stash.expire().await;
        self.openable_persist.expire().await;
    }
}

impl CatalogMigrator {
    pub(crate) fn new(
        openable_stash: OpenableConnection,
        openable_persist: UnopenedPersistCatalogState,
        direction: Direction,
    ) -> CatalogMigrator {
        let openable_stash = Box::new(openable_stash);
        let openable_persist = Box::new(openable_persist);
        CatalogMigrator {
            openable_stash,
            openable_persist,
            direction,
        }
    }

    async fn open_inner(
        mut stash: Box<dyn DurableCatalogState>,
        mut persist: Box<dyn DurableCatalogState>,
        direction: Direction,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        match direction {
            Direction::MigrateToPersist => {
                Self::migrate_from_stash_to_persist(stash, &mut persist).await?;
                Ok(persist)
            }
            Direction::RollbackToStash => {
                Self::rollback_from_persist_to_stash(&mut stash, persist).await?;
                Ok(stash)
            }
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
            stash.whole_migration_snapshot().await?;

        let (mut persist_txn, persist_audit_logs, persist_storage_usages) =
            persist.whole_migration_transaction().await?;
        persist_txn.set_catalog(
            stash_snapshot,
            persist_audit_logs,
            persist_storage_usages,
            stash_audit_logs,
            stash_storage_usages,
        )?;
        persist_txn.commit().await?;

        fail::fail_point!("migrate_post_write");

        let mut stash_txn = stash.transaction().await?;
        stash_txn.set_tombstone(true)?;
        stash_txn.commit().await?;

        Ok(())
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
            persist.whole_migration_snapshot().await?;

        let (mut stash_txn, stash_audit_logs, stash_storage_usages) =
            stash.whole_migration_transaction().await?;
        stash_txn.set_catalog(
            persist_snapshot,
            stash_audit_logs,
            stash_storage_usages,
            persist_audit_logs,
            persist_storage_usages,
        )?;
        stash_txn.set_tombstone(false)?;
        stash_txn.commit().await?;

        Ok(())
    }
}
