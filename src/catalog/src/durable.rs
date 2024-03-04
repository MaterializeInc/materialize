// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate is responsible for durably storing and modifying the catalog contents.

use std::fmt::Debug;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::EpochMillis;
use mz_persist_client::PersistClient;
use mz_repr::GlobalId;
use mz_sql::session::vars::CatalogKind;
use mz_stash::DebugStashFactory;
use mz_storage_types::controller::PersistTxnTablesImpl;
use uuid::Uuid;

use crate::durable::debug::{DebugCatalogState, Trace};
pub use crate::durable::error::{CatalogError, DurableCatalogError};
use crate::durable::impls::migrate::{CatalogMigrator, Direction};
pub use crate::durable::impls::persist::metrics::Metrics;
use crate::durable::impls::persist::UnopenedPersistCatalogState;
use crate::durable::impls::stash::{OpenableConnection, TestOpenableConnection};
pub use crate::durable::impls::stash::{
    StashConfig, ALL_COLLECTIONS, AUDIT_LOG_COLLECTION, CLUSTER_COLLECTION,
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION, CLUSTER_REPLICA_COLLECTION, COMMENTS_COLLECTION,
    CONFIG_COLLECTION, DATABASES_COLLECTION, DEFAULT_PRIVILEGES_COLLECTION,
    ID_ALLOCATOR_COLLECTION, ITEM_COLLECTION, ROLES_COLLECTION, SCHEMAS_COLLECTION,
    SETTING_COLLECTION, STORAGE_USAGE_COLLECTION, SYSTEM_CONFIGURATION_COLLECTION,
    SYSTEM_GID_MAPPING_COLLECTION, SYSTEM_PRIVILEGES_COLLECTION, TIMESTAMP_COLLECTION,
};
use crate::durable::objects::Snapshot;
pub use crate::durable::objects::{
    Cluster, ClusterConfig, ClusterReplica, ClusterVariant, ClusterVariantManaged, Comment,
    Database, DefaultPrivilege, Item, ReplicaConfig, ReplicaLocation, Role, Schema,
    SystemConfiguration, SystemObjectMapping, TimelineTimestamp,
};
pub use crate::durable::transaction::Transaction;
use crate::durable::transaction::TransactionBatch;
pub use crate::durable::upgrade::CATALOG_VERSION;

pub mod debug;
mod error;
mod impls;
pub mod initialize;
pub mod objects;
mod transaction;
mod upgrade;

pub const DATABASE_ID_ALLOC_KEY: &str = "database";
pub const SCHEMA_ID_ALLOC_KEY: &str = "schema";
pub const USER_ITEM_ALLOC_KEY: &str = "user";
pub const SYSTEM_ITEM_ALLOC_KEY: &str = "system";
pub const USER_ROLE_ID_ALLOC_KEY: &str = "user_role";
pub const USER_CLUSTER_ID_ALLOC_KEY: &str = "user_compute";
pub const SYSTEM_CLUSTER_ID_ALLOC_KEY: &str = "system_compute";
pub const USER_REPLICA_ID_ALLOC_KEY: &str = "replica";
pub const SYSTEM_REPLICA_ID_ALLOC_KEY: &str = "system_replica";
pub const AUDIT_LOG_ID_ALLOC_KEY: &str = "auditlog";
pub const STORAGE_USAGE_ID_ALLOC_KEY: &str = "storage_usage";
pub(crate) const CATALOG_CONTENT_VERSION_KEY: &str = "catalog_content_version";

#[derive(Clone, Debug)]
pub struct BootstrapArgs {
    pub default_cluster_replica_size: String,
    pub bootstrap_role: Option<String>,
}

pub type Epoch = NonZeroI64;

pub(crate) fn epoch_checked_increment(epoch: Epoch) -> Option<Epoch> {
    epoch.get().checked_add(1).and_then(Epoch::new)
}

/// An API for opening a durable catalog state.
///
/// If a catalog is not opened, then resources should be release via [`Self::expire`].
#[async_trait]
pub trait OpenableDurableCatalogState: Debug + Send {
    /// Opens the catalog in a mode that accepts and buffers all writes,
    /// but never durably commits them. This is used to check and see if
    /// opening the catalog would be successful, without making any durable
    /// changes.
    ///
    /// `epoch_lower_bound` is used as a lower bound for the epoch that is used by the returned
    /// catalog.
    ///
    /// Will return an error in the following scenarios:
    ///   - Catalog initialization fails.
    ///   - Catalog migrations fail.
    ///
    /// `initial_ts` is used as the initial timestamp for new environments.
    async fn open_savepoint(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Opens the catalog in read only mode. All mutating methods
    /// will return an error.
    ///
    /// If the catalog is uninitialized or requires a migrations, then
    /// it will fail to open in read only mode.
    async fn open_read_only(
        mut self: Box<Self>,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Opens the catalog in a writeable mode. Optionally initializes the
    /// catalog, if it has not been initialized, and perform any migrations
    /// needed.
    ///
    /// `initial_ts` is used as the initial timestamp for new environments.
    /// `epoch_lower_bound` is used as a lower bound for the epoch that is used by the returned
    /// catalog.
    async fn open(
        mut self: Box<Self>,
        initial_ts: EpochMillis,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
        epoch_lower_bound: Option<Epoch>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Opens the catalog for manual editing of the underlying data. This is helpful for
    /// fixing a corrupt catalog.
    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError>;

    /// Reports if the catalog state has been initialized.
    async fn is_initialized(&mut self) -> Result<bool, CatalogError>;

    /// Returns the epoch of the current durable catalog state. The epoch acts as
    /// a fencing token to prevent split brain issues across two
    /// [`DurableCatalogState`]s. When a new [`DurableCatalogState`] opens the
    /// catalog, it will increment the epoch by one (or initialize it to some
    /// value if there's no existing epoch) and store the value in memory. It's
    /// guaranteed that no two [`DurableCatalogState`]s will return the same value
    /// for their epoch.
    ///
    /// NB: We may remove this in later iterations of Pv2.
    async fn epoch(&mut self) -> Result<Epoch, CatalogError>;

    /// Get the deployment generation of this instance.
    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError>;

    /// Reports if the remote configuration was synchronized at least once.
    async fn has_system_config_synced_once(&mut self) -> Result<bool, CatalogError>;

    /// Get the tombstone value of this instance.
    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError>;

    /// Get the `catalog_kind` config value of this instance.
    ///
    /// This mirrors the `catalog_kind` "system var" so that we can toggle
    /// the flag with Launch Darkly, but use it in boot before Launch Darkly is
    /// available.
    async fn get_catalog_kind_config(&mut self) -> Result<Option<CatalogKind>, CatalogError>;

    /// Generate an unconsolidated [`Trace`] of catalog contents.
    async fn trace(&mut self) -> Result<Trace, CatalogError>;

    /// Sets the kind of catalog opened to `catalog_kind` iff this `OpenableDurableCatalogState`
    /// knows how to open a catalog of kind `catalog_kind`, otherwise does nothing.
    fn set_catalog_kind(&mut self, catalog_kind: CatalogKind);

    /// Politely releases all external resources that can only be released in an async context.
    async fn expire(self: Box<Self>);
}

// TODO(jkosh44) No method should take &mut self, but due to stash implementations we need it.
/// A read only API for the durable catalog state.
#[async_trait]
pub trait ReadOnlyDurableCatalogState: Debug + Send {
    /// Returns the epoch of the current durable catalog state. The epoch acts as
    /// a fencing token to prevent split brain issues across two
    /// [`DurableCatalogState`]s. When a new [`DurableCatalogState`] opens the
    /// catalog, it will increment the epoch by one (or initialize it to some
    /// value if there's no existing epoch) and store the value in memory. It's
    /// guaranteed that no two [`DurableCatalogState`]s will return the same value
    /// for their epoch.
    ///
    /// NB: We may remove this in later iterations of Pv2.
    fn epoch(&mut self) -> Epoch;

    /// Politely releases all external resources that can only be released in an async context.
    async fn expire(self: Box<Self>);

    /// Get all audit log events.
    ///
    /// Results are guaranteed to be sorted by ID.
    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError>;

    /// Get the next ID of `id_type`, without allocating it.
    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError>;

    /// Get the next system replica id without allocating it.
    async fn get_next_system_replica_id(&mut self) -> Result<u64, CatalogError> {
        self.get_next_id(SYSTEM_REPLICA_ID_ALLOC_KEY).await
    }

    /// Get the next user replica id without allocating it.
    async fn get_next_user_replica_id(&mut self) -> Result<u64, CatalogError> {
        self.get_next_id(USER_REPLICA_ID_ALLOC_KEY).await
    }

    /// Get the `persist_txn_tables` config value of this instance.
    ///
    /// This mirrors the `persist_txn_tables` "system var" so that we can toggle
    /// the flag with Launch Darkly, but use it in boot before Launch Darkly is
    /// available.
    async fn get_persist_txn_tables(
        &mut self,
    ) -> Result<Option<PersistTxnTablesImpl>, CatalogError>;

    /// Get the tombstone value of this instance.
    async fn get_tombstone(&mut self) -> Result<Option<bool>, CatalogError>;

    /// Get a snapshot of the catalog.
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError>;

    /// Get a full snapshot of all data in the catalog. This includes all audit logs and storage
    /// usages that isn't included in [`Self::snapshot`].
    async fn whole_migration_snapshot(
        &mut self,
    ) -> Result<(Snapshot, Vec<VersionedEvent>, Vec<VersionedStorageUsage>), CatalogError>;
}

/// A read-write API for the durable catalog state.
#[async_trait]
pub trait DurableCatalogState: ReadOnlyDurableCatalogState {
    /// Returns true if the catalog is opened in read only mode, false otherwise.
    fn is_read_only(&self) -> bool;

    /// Creates a new durable catalog state transaction.
    async fn transaction(&mut self) -> Result<Transaction, CatalogError>;

    /// Creates a new durable catalog state transaction. Also returns all the audit logs and storage
    /// usages that exist at the start of the transaction, which isn't included in
    /// [`Self::transaction`].
    async fn whole_migration_transaction(
        &mut self,
    ) -> Result<(Transaction, Vec<VersionedEvent>, Vec<VersionedStorageUsage>), CatalogError>;

    /// Commits a durable catalog state transaction.
    async fn commit_transaction(&mut self, txn_batch: TransactionBatch)
        -> Result<(), CatalogError>;

    /// Confirms that this catalog is connected as the current leader.
    ///
    /// NB: We may remove this in later iterations of Pv2.
    async fn confirm_leadership(&mut self) -> Result<(), CatalogError>;

    /// Gets all storage usage events and permanently deletes from the catalog those
    /// that happened more than the retention period ago from boot_ts.
    ///
    /// Results are guaranteed to be sorted by ID.
    async fn get_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: mz_repr::Timestamp,
        wait_for_consolidation: bool,
    ) -> Result<Vec<VersionedStorageUsage>, CatalogError>;

    /// Allocates and returns `amount` IDs of `id_type`.
    async fn allocate_id(&mut self, id_type: &str, amount: u64) -> Result<Vec<u64>, CatalogError>;

    /// Allocates and returns `amount` system [`GlobalId`]s.
    async fn allocate_system_ids(&mut self, amount: u64) -> Result<Vec<GlobalId>, CatalogError> {
        let id = self.allocate_id(SYSTEM_ITEM_ALLOC_KEY, amount).await?;

        Ok(id.into_iter().map(GlobalId::System).collect())
    }

    /// Allocates and returns a user [`GlobalId`].
    async fn allocate_user_id(&mut self) -> Result<GlobalId, CatalogError> {
        let id = self.allocate_id(USER_ITEM_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(GlobalId::User(id))
    }

    /// Allocates and returns a system [`ClusterId`].
    async fn allocate_system_cluster_id(&mut self) -> Result<ClusterId, CatalogError> {
        let id = self.allocate_id(SYSTEM_CLUSTER_ID_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(ClusterId::System(id))
    }

    /// Allocates and returns a user [`ClusterId`].
    async fn allocate_user_cluster_id(&mut self) -> Result<ClusterId, CatalogError> {
        let id = self.allocate_id(USER_CLUSTER_ID_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(ClusterId::User(id))
    }

    /// Allocates and returns a user [`ReplicaId`].
    async fn allocate_user_replica_id(&mut self) -> Result<ReplicaId, CatalogError> {
        let id = self.allocate_id(USER_REPLICA_ID_ALLOC_KEY, 1).await?;
        let id = id.into_element();
        Ok(ReplicaId::User(id))
    }
}

/// Creates a openable durable catalog state implemented using the stash.
pub fn stash_backed_catalog_state(config: StashConfig) -> OpenableConnection {
    OpenableConnection::new(config)
}

/// Creates an openable durable catalog state implemented using the stash that is meant to be
/// used in tests.
pub fn test_stash_backed_catalog_state(
    debug_stash_factory: &DebugStashFactory,
) -> TestOpenableConnection {
    TestOpenableConnection::new(debug_stash_factory)
}

/// Creates an openable durable catalog state implemented using persist.
pub async fn persist_backed_catalog_state(
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
    metrics: Arc<Metrics>,
) -> Result<UnopenedPersistCatalogState, DurableCatalogError> {
    UnopenedPersistCatalogState::new(persist_client, organization_id, version, metrics).await
}

/// Creates an openable durable catalog state implemented using persist that is meant to be used in
/// tests.
pub async fn test_persist_backed_catalog_state(
    persist_client: PersistClient,
    organization_id: Uuid,
) -> UnopenedPersistCatalogState {
    test_persist_backed_catalog_state_with_version(
        persist_client,
        organization_id,
        semver::Version::new(0, 0, 0),
    )
    .await
    .expect("failed to open catalog state")
}

/// Creates an openable durable catalog state implemented using persist that is meant to be used in
/// tests.
pub async fn test_persist_backed_catalog_state_with_version(
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
) -> Result<UnopenedPersistCatalogState, DurableCatalogError> {
    let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
    persist_backed_catalog_state(persist_client, organization_id, version, metrics).await
}

/// Creates an openable durable catalog state that migrates the current state from the stash to
/// persist.
pub async fn migrate_from_stash_to_persist_state(
    stash_config: StashConfig,
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
    persist_metrics: Arc<Metrics>,
) -> Result<CatalogMigrator, CatalogError> {
    let openable_stash = stash_backed_catalog_state(stash_config);
    let openable_persist =
        persist_backed_catalog_state(persist_client, organization_id, version, persist_metrics)
            .await?;
    Ok(CatalogMigrator::new(
        openable_stash,
        openable_persist,
        Direction::MigrateToPersist,
    ))
}

/// Creates an openable durable catalog state that migrates the current state from the stash to
/// persist that is meant to be used in tests.
pub async fn test_migrate_from_stash_to_persist_state(
    stash_config: StashConfig,
    persist_client: PersistClient,
    organization_id: Uuid,
) -> CatalogMigrator {
    let openable_stash = stash_backed_catalog_state(stash_config);
    let openable_persist = test_persist_backed_catalog_state(persist_client, organization_id).await;
    CatalogMigrator::new(
        openable_stash,
        openable_persist,
        Direction::MigrateToPersist,
    )
}

/// Creates an openable durable catalog state that rolls back the current state from persist to
/// the stash.
pub async fn rollback_from_persist_to_stash_state(
    stash_config: StashConfig,
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
    persist_metrics: Arc<Metrics>,
) -> Result<CatalogMigrator, CatalogError> {
    let openable_stash = stash_backed_catalog_state(stash_config);
    let openable_persist =
        persist_backed_catalog_state(persist_client, organization_id, version, persist_metrics)
            .await?;
    Ok(CatalogMigrator::new(
        openable_stash,
        openable_persist,
        Direction::RollbackToStash,
    ))
}

/// Creates an openable durable catalog state that rolls back the current state from persist to
/// the stash that is meant to be used in tests.
pub async fn test_rollback_from_persist_to_stash_state(
    stash_config: StashConfig,
    persist_client: PersistClient,
    organization_id: Uuid,
) -> CatalogMigrator {
    let openable_stash = stash_backed_catalog_state(stash_config);
    let openable_persist = test_persist_backed_catalog_state(persist_client, organization_id).await;
    CatalogMigrator::new(openable_stash, openable_persist, Direction::RollbackToStash)
}

pub fn test_bootstrap_args() -> BootstrapArgs {
    BootstrapArgs {
        default_cluster_replica_size: "1".into(),
        bootstrap_role: None,
    }
}

pub async fn test_stash_config() -> (DebugStashFactory, StashConfig) {
    // Creating a debug stash factory does a lot of nice stuff like creating a random schema for us.
    // Dropping the factory will drop the schema.
    let debug_stash_factory = DebugStashFactory::new().await;
    let config = StashConfig {
        stash_factory: debug_stash_factory.stash_factory().clone(),
        stash_url: debug_stash_factory.url().to_string(),
        schema: Some(debug_stash_factory.schema().to_string()),
        tls: debug_stash_factory.tls().clone(),
    };
    (debug_stash_factory, config)
}
