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
use tracing::{error, info};

use crate::durable::debug::{DebugCatalogState, Trace};
use crate::durable::impls::persist::UnopenedPersistCatalogState;
use crate::durable::impls::stash::OpenableConnection;
use crate::durable::{
    epoch_checked_increment, BootstrapArgs, CatalogError, DurableCatalogState, Epoch,
    OpenableDurableCatalogState,
};

// Note: All reads done in this file can be fenced out by a new writer. All writers start by first
// fencing the stash and then fencing persist. Therefore, when performing operations like:
//
//   1. Read tombstone from stash.
//   2. Use tombstone to decide to either read something from the stash or persist.
//
// We don't have to worry about race conditions where a new writer updates the stash or persist
// inbetween steps (1) and (2).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TargetImplementation {
    EmergencyStash,
    MigrationDirection(Direction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    MigrateToPersist,
    RollbackToStash,
}

impl TryFrom<CatalogKind> for TargetImplementation {
    type Error = CatalogKind;

    fn try_from(catalog_kind: CatalogKind) -> Result<Self, Self::Error> {
        match catalog_kind {
            CatalogKind::Stash => Ok(TargetImplementation::MigrationDirection(
                Direction::RollbackToStash,
            )),
            CatalogKind::Persist => Ok(TargetImplementation::MigrationDirection(
                Direction::MigrateToPersist,
            )),
            CatalogKind::EmergencyStash => Ok(TargetImplementation::EmergencyStash),
        }
    }
}

#[derive(Debug)]
pub struct CatalogMigrator {
    openable_stash: Box<OpenableConnection>,
    openable_persist: Box<UnopenedPersistCatalogState>,
    target: TargetImplementation,
}

#[async_trait]
impl OpenableDurableCatalogState for CatalogMigrator {
    async fn open_savepoint(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        assert_eq!(
            epoch_lower_bound, None,
            "epoch_lower_bound shouldn't be set for `CatalogMigrator` impl"
        );

        let direction = match self.target {
            // Handle emergency stash before opening persist.
            TargetImplementation::EmergencyStash => {
                let tombstone = self.get_tombstone().await?;
                let mut stash = self
                    .openable_stash
                    .open_savepoint(initial_ts, bootstrap_args, deploy_generation, None)
                    .await?;
                // Forcibly mark the rollback as complete so we look at the correct implementation
                // (stash) on re-boot. This is really a no-op because it's a savepoint catalog, but
                // we do it anyway for consistency.
                if tombstone == Some(true) {
                    let mut txn = stash.transaction().await?;
                    txn.set_tombstone(false)?;
                    txn.commit().await?;
                }
                return Ok(stash);
            }
            TargetImplementation::MigrationDirection(direction) => direction,
        };

        let stash_epoch = self.openable_stash.epoch().await.ok();
        let persist_epoch = self.openable_persist.epoch().await.ok();
        let stash_initialized = self.openable_stash.is_initialized().await?;
        let persist_initialized = self.openable_persist.is_initialized().await?;
        let stash = self
            .openable_stash
            .open_savepoint(
                initial_ts.clone(),
                bootstrap_args,
                deploy_generation.clone(),
                persist_epoch,
            )
            .await;
        let persist = self
            .openable_persist
            .open_savepoint(initial_ts, bootstrap_args, deploy_generation, stash_epoch)
            .await;

        // If our target implementation is the stash, but persist is uninitialized, then we can
        // still proceed with only using the stash.
        if let Err(CatalogError::Durable(e)) = &persist {
            if e.can_recover_with_write_mode()
                && !persist_initialized
                && direction == Direction::RollbackToStash
            {
                return stash;
            }
        }

        // If our target implementation is the persist, but the stash is uninitialized, then we can
        // still proceed with only using persist.
        if let Err(CatalogError::Durable(e)) = &stash {
            if e.can_recover_with_write_mode()
                && !stash_initialized
                && direction == Direction::MigrateToPersist
            {
                return persist;
            }
        }

        let stash = stash?;
        let persist = persist?;
        Self::open_inner(stash, persist, direction).await
    }

    async fn open_read_only(
        self: Box<Self>,
        _bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        panic!("cannot use a read only catalog with the migrate implementation")
    }

    async fn open(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
        assert_eq!(
            epoch_lower_bound, None,
            "epoch_lower_bound shouldn't be set for `CatalogMigrator` impl"
        );

        let direction = match self.target {
            // Handle emergency stash before opening persist.
            TargetImplementation::EmergencyStash => {
                let tombstone = self.get_tombstone().await?;
                let mut stash = self
                    .openable_stash
                    .open(initial_ts, bootstrap_args, deploy_generation, None)
                    .await?;
                // Forcibly mark the rollback as complete so we look at the correct implementation
                // (stash) on re-boot.
                if tombstone == Some(true) {
                    let mut txn = stash.transaction().await?;
                    txn.set_tombstone(false)?;
                    txn.commit().await?;
                }
                return Ok(stash);
            }
            TargetImplementation::MigrationDirection(direction) => direction,
        };

        let stash_epoch = self
            .openable_stash
            .epoch()
            .await
            .ok()
            .map(|epoch| epoch_checked_increment(epoch).expect("stash epoch overflowed"));
        let persist_epoch = self
            .openable_persist
            .epoch()
            .await
            .ok()
            .map(|epoch| epoch_checked_increment(epoch).expect("persist epoch overflowed"));

        let stash = self
            .openable_stash
            .open(
                initial_ts.clone(),
                bootstrap_args,
                deploy_generation.clone(),
                persist_epoch,
            )
            .await?;
        fail::fail_point!("post_stash_fence");
        let persist = self
            .openable_persist
            .open(initial_ts, bootstrap_args, deploy_generation, stash_epoch)
            .await?;
        fail::fail_point!("post_persist_fence");
        Self::open_inner(stash, persist, direction).await
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

    async fn epoch(&mut self) -> Result<Epoch, CatalogError> {
        let tombstone = self.get_tombstone().await?;
        if tombstone == Some(true) {
            self.openable_persist.epoch().await
        } else {
            self.openable_stash.epoch().await
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

    async fn has_system_config_synced_once(&mut self) -> Result<bool, CatalogError> {
        let tombstone = self.get_tombstone().await?;
        if tombstone == Some(true) {
            self.openable_persist.has_system_config_synced_once().await
        } else {
            self.openable_stash.has_system_config_synced_once().await
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
        info!("Switching to {} backed catalog", catalog_kind.as_str());
        let direction = match catalog_kind.try_into() {
            Ok(direction) => direction,
            Err(catalog_kind) => {
                error!("unable to set catalog kind to {catalog_kind:?}");
                return;
            }
        };
        self.target = direction;
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
            target: TargetImplementation::MigrationDirection(direction),
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

        info!("migrating catalog contents from stash to persist");

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

        stash.expire().await;

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

        info!("rolling back catalog contents from persist to stash");

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

        persist.expire().await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_client::PersistClient;
    use mz_sql::session::vars::CatalogKind;
    use uuid::Uuid;

    use crate::durable::impls::migrate::{Direction, TargetImplementation};
    use crate::durable::{
        test_migrate_from_stash_to_persist_state, test_rollback_from_persist_to_stash_state,
        test_stash_config, OpenableDurableCatalogState,
    };

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_set_catalog_kind() {
        let (debug_factory, stash_config) = test_stash_config().await;
        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();

        {
            let mut catalog = test_migrate_from_stash_to_persist_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await;
            assert_eq!(
                catalog.target,
                TargetImplementation::MigrationDirection(Direction::MigrateToPersist)
            );

            catalog.set_catalog_kind(CatalogKind::Stash);
            assert_eq!(
                catalog.target,
                TargetImplementation::MigrationDirection(Direction::RollbackToStash)
            );

            catalog.set_catalog_kind(CatalogKind::Persist);
            assert_eq!(
                catalog.target,
                TargetImplementation::MigrationDirection(Direction::MigrateToPersist)
            );

            catalog.set_catalog_kind(CatalogKind::EmergencyStash);
            assert_eq!(catalog.target, TargetImplementation::EmergencyStash);
        }

        {
            let mut catalog = test_rollback_from_persist_to_stash_state(
                stash_config.clone(),
                persist_client.clone(),
                organization_id.clone(),
            )
            .await;
            assert_eq!(
                catalog.target,
                TargetImplementation::MigrationDirection(Direction::RollbackToStash)
            );

            catalog.set_catalog_kind(CatalogKind::Persist);
            assert_eq!(
                catalog.target,
                TargetImplementation::MigrationDirection(Direction::MigrateToPersist)
            );

            catalog.set_catalog_kind(CatalogKind::Stash);
            assert_eq!(
                catalog.target,
                TargetImplementation::MigrationDirection(Direction::RollbackToStash)
            );

            catalog.set_catalog_kind(CatalogKind::EmergencyStash);
            assert_eq!(catalog.target, TargetImplementation::EmergencyStash);
        }

        debug_factory.drop().await;
    }
}
