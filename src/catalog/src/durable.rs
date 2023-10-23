// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate is responsible for durably storing and modifying the catalog contents.

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::num::NonZeroI64;
use std::time::Duration;
use uuid::Uuid;

use mz_stash::DebugStashFactory;

pub use crate::durable::error::{CatalogError, DurableCatalogError};
pub use crate::durable::objects::{
    Cluster, ClusterConfig, ClusterReplica, ClusterVariant, ClusterVariantManaged, Comment,
    Database, DefaultPrivilege, Item, ReplicaConfig, ReplicaLocation, Role, Schema,
    SystemConfiguration, SystemObjectMapping, TimelineTimestamp,
};
use crate::durable::objects::{IntrospectionSourceIndex, Snapshot};
use crate::durable::persist::PersistHandle;
use crate::durable::stash::{DebugOpenableConnection, OpenableConnection};
pub use crate::durable::stash::{
    StashConfig, ALL_COLLECTIONS, AUDIT_LOG_COLLECTION, CLUSTER_COLLECTION,
    CLUSTER_INTROSPECTION_SOURCE_INDEX_COLLECTION, CLUSTER_REPLICA_COLLECTION, COMMENTS_COLLECTION,
    CONFIG_COLLECTION, DATABASES_COLLECTION, DEFAULT_PRIVILEGES_COLLECTION,
    ID_ALLOCATOR_COLLECTION, ITEM_COLLECTION, ROLES_COLLECTION, SCHEMAS_COLLECTION,
    SETTING_COLLECTION, STORAGE_USAGE_COLLECTION, SYSTEM_CONFIGURATION_COLLECTION,
    SYSTEM_GID_MAPPING_COLLECTION, SYSTEM_PRIVILEGES_COLLECTION, TIMESTAMP_COLLECTION,
};
pub use crate::durable::transaction::Transaction;
use crate::durable::transaction::TransactionBatch;
use mz_audit_log::{VersionedEvent, VersionedStorageUsage};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_persist_client::PersistClient;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_storage_types::sources::Timeline;

mod error;
pub mod initialize;
pub mod objects;
mod persist;
mod stash;
mod transaction;

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

#[derive(Clone, Debug)]
pub struct BootstrapArgs {
    pub default_cluster_replica_size: String,
    pub builtin_cluster_replica_size: String,
    pub bootstrap_role: Option<String>,
}

pub type Epoch = NonZeroI64;

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
    /// Will return an error in the following scenarios:
    ///   - Catalog initialization fails.
    ///   - Catalog migrations fail.
    async fn open_savepoint(
        mut self: Box<Self>,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Opens the catalog in read only mode. All mutating methods
    /// will return an error.
    ///
    /// If the catalog is uninitialized or requires a migrations, then
    /// it will fail to open in read only mode.
    async fn open_read_only(
        mut self: Box<Self>,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Opens the catalog in a writeable mode. Optionally initializes the
    /// catalog, if it has not been initialized, and perform any migrations
    /// needed.
    async fn open(
        mut self: Box<Self>,
        now: NowFn,
        bootstrap_args: &BootstrapArgs,
        deploy_generation: Option<u64>,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Reports if the catalog state has been initialized.
    async fn is_initialized(&mut self) -> Result<bool, CatalogError>;

    /// Get the deployment generation of this instance.
    async fn get_deployment_generation(&mut self) -> Result<Option<u64>, CatalogError>;

    /// Politely releases all external resources that can only be released in an async context.
    async fn expire(self);
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

    /// Returns the version of Materialize that last wrote to the catalog.
    ///
    /// If the catalog is uninitialized this will return None.
    async fn get_catalog_content_version(&mut self) -> Result<Option<String>, CatalogError>;

    /// Get all clusters.
    async fn get_clusters(&mut self) -> Result<Vec<Cluster>, CatalogError>;

    /// Get all cluster replicas.
    async fn get_cluster_replicas(&mut self) -> Result<Vec<ClusterReplica>, CatalogError>;

    /// Get all databases.
    async fn get_databases(&mut self) -> Result<Vec<Database>, CatalogError>;

    /// Get all schemas.
    async fn get_schemas(&mut self) -> Result<Vec<Schema>, CatalogError>;

    /// Get all system items.
    async fn get_system_items(&mut self) -> Result<Vec<SystemObjectMapping>, CatalogError>;

    /// Get all introspection source indexes.
    ///
    /// Returns (index-name, global-id).
    async fn get_introspection_source_indexes(
        &mut self,
        cluster_id: ClusterId,
    ) -> Result<BTreeMap<String, GlobalId>, CatalogError>;

    /// Get all roles.
    async fn get_roles(&mut self) -> Result<Vec<Role>, CatalogError>;

    /// Get all default privileges.
    async fn get_default_privileges(&mut self) -> Result<Vec<DefaultPrivilege>, CatalogError>;

    /// Get all system privileges.
    async fn get_system_privileges(&mut self) -> Result<Vec<MzAclItem>, CatalogError>;

    /// Get all system configurations.
    async fn get_system_configurations(&mut self)
        -> Result<Vec<SystemConfiguration>, CatalogError>;

    /// Get all comments.
    async fn get_comments(&mut self) -> Result<Vec<Comment>, CatalogError>;

    /// Get all timelines and their persisted timestamps.
    // TODO(jkosh44) This should be removed once the timestamp oracle is extracted.
    async fn get_timestamps(&mut self) -> Result<Vec<TimelineTimestamp>, CatalogError>;

    /// Get the persisted timestamp of a timeline.
    // TODO(jkosh44) This should be removed once the timestamp oracle is extracted.
    async fn get_timestamp(
        &mut self,
        timeline: &Timeline,
    ) -> Result<Option<mz_repr::Timestamp>, CatalogError>;

    /// Get all audit log events.
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

    /// Get a snapshot of the catalog.
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError>;

    // TODO(jkosh44) Implement this for the catalog debug tool.
    /*    /// Dumps the entire catalog contents in human readable JSON.
    async fn dump(&self) -> Result<String, Error>;*/
}

/// A read-write API for the durable catalog state.
#[async_trait]
pub trait DurableCatalogState: ReadOnlyDurableCatalogState {
    /// Returns true if the catalog is opened in read only mode, false otherwise.
    fn is_read_only(&self) -> bool;

    /// Creates a new durable catalog state transaction.
    async fn transaction(&mut self) -> Result<Transaction, CatalogError>;

    /// Commits a durable catalog state transaction.
    async fn commit_transaction(&mut self, txn_batch: TransactionBatch)
        -> Result<(), CatalogError>;

    /// Confirms that this catalog is connected as the current leader.
    ///
    /// NB: We may remove this in later iterations of Pv2.
    async fn confirm_leadership(&mut self) -> Result<(), CatalogError>;

    /// Set's the connection timeout for the underlying durable store.
    async fn set_connect_timeout(&mut self, connect_timeout: Duration);

    /// Persist the version of Materialize that last wrote to the catalog.
    async fn set_catalog_content_version(&mut self, new_version: &str) -> Result<(), CatalogError>;

    /// Gets all storage usage events and permanently deletes from the catalog those
    /// that happened more than the retention period ago from boot_ts.
    async fn get_and_prune_storage_usage(
        &mut self,
        retention_period: Option<Duration>,
        boot_ts: mz_repr::Timestamp,
    ) -> Result<Vec<VersionedStorageUsage>, CatalogError>;

    /// Persist system items.
    async fn set_system_items(
        &mut self,
        mappings: Vec<SystemObjectMapping>,
    ) -> Result<(), CatalogError>;

    /// Persist introspection source indexes.
    ///
    /// `mappings` has the format (cluster-id, index-name, global-id).
    ///
    /// Panics if the provided id is not a system id.
    async fn set_introspection_source_indexes(
        &mut self,
        mappings: Vec<IntrospectionSourceIndex>,
    ) -> Result<(), CatalogError>;

    /// Persist the configuration of a replica.
    /// This accepts only one item, as we currently use this only for the default cluster
    async fn set_replica_config(
        &mut self,
        replica_id: ReplicaId,
        cluster_id: ClusterId,
        name: String,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) -> Result<(), CatalogError>;

    /// Persist new global timestamp for a timeline.
    async fn set_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), CatalogError>;

    /// Persist the deployment generation of this instance.
    async fn set_deploy_generation(&mut self, deploy_generation: u64) -> Result<(), CatalogError>;

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

/// Creates an openable debug durable catalog state implemented using the stash that is meant to be
/// used in tests.
pub fn debug_stash_backed_catalog_state(
    debug_stash_factory: &DebugStashFactory,
) -> DebugOpenableConnection {
    DebugOpenableConnection::new(debug_stash_factory)
}

/// Creates an openable durable catalog state implemented using persist.
pub async fn persist_backed_catalog_state(
    persist_client: PersistClient,
    organization_id: Uuid,
) -> PersistHandle {
    PersistHandle::new(persist_client, organization_id).await
}

pub fn debug_bootstrap_args() -> BootstrapArgs {
    BootstrapArgs {
        default_cluster_replica_size: "1".into(),
        builtin_cluster_replica_size: "1".into(),
        bootstrap_role: None,
    }
}
