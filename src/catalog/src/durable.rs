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
use itertools::Itertools;
use mz_audit_log::VersionedEvent;
use mz_controller_types::ClusterId;
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::PersistClient;
use mz_repr::{CatalogItemId, Diff, GlobalId};
use mz_sql::catalog::CatalogError as SqlCatalogError;
use uuid::Uuid;

use crate::config::ClusterReplicaSizeMap;
use crate::durable::debug::{DebugCatalogState, Trace};
pub use crate::durable::error::{CatalogError, DurableCatalogError, FenceError};
pub use crate::durable::metrics::Metrics;
pub use crate::durable::objects::state_update::StateUpdate;
use crate::durable::objects::state_update::{StateUpdateKindJson, TryIntoStateUpdateKind};
use crate::durable::objects::{AuditLog, Snapshot};
pub use crate::durable::objects::{
    Cluster, ClusterConfig, ClusterReplica, ClusterVariant, ClusterVariantManaged, Comment,
    Database, DefaultPrivilege, IntrospectionSourceIndex, Item, NetworkPolicy, ReplicaConfig,
    ReplicaLocation, Role, RoleAuth, Schema, SourceReference, SourceReferences,
    StorageCollectionMetadata, SystemConfiguration, SystemObjectDescription, SystemObjectMapping,
    UnfinalizedShard,
};
pub use crate::durable::persist::shard_id;
use crate::durable::persist::{Timestamp, UnopenedPersistCatalogState};
pub use crate::durable::transaction::Transaction;
use crate::durable::transaction::TransactionBatch;
pub use crate::durable::upgrade::CATALOG_VERSION;
use crate::memory;

pub mod debug;
mod error;
pub mod initialize;
mod metrics;
pub mod objects;
mod persist;
mod traits;
mod transaction;
mod upgrade;

pub const DATABASE_ID_ALLOC_KEY: &str = "database";
pub const SCHEMA_ID_ALLOC_KEY: &str = "schema";
pub const USER_ITEM_ALLOC_KEY: &str = "user";
pub const SYSTEM_ITEM_ALLOC_KEY: &str = "system";
pub const USER_ROLE_ID_ALLOC_KEY: &str = "user_role";
pub const AUDIT_LOG_ID_ALLOC_KEY: &str = "auditlog";
pub const STORAGE_USAGE_ID_ALLOC_KEY: &str = "storage_usage";
pub const USER_NETWORK_POLICY_ID_ALLOC_KEY: &str = "user_network_policy";
pub const OID_ALLOC_KEY: &str = "oid";
pub(crate) const CATALOG_CONTENT_VERSION_KEY: &str = "catalog_content_version";
pub const BUILTIN_MIGRATION_SHARD_KEY: &str = "builtin_migration_shard";
pub const EXPRESSION_CACHE_SHARD_KEY: &str = "expression_cache_shard";
pub const MOCK_AUTHENTICATION_NONCE_KEY: &str = "mock_authentication_nonce";

// Note: these ID types are generally merged with the main system and user item keys,
// but are kept separate here for backwards-compatibility reasons.
pub const USER_CLUSTER_ID_ALLOC_KEY: &str = "user_compute";
pub const SYSTEM_CLUSTER_ID_ALLOC_KEY: &str = "system_compute";
pub const USER_REPLICA_ID_ALLOC_KEY: &str = "replica";
pub const SYSTEM_REPLICA_ID_ALLOC_KEY: &str = "system_replica";

pub const SYSTEM_ALLOC_KEYS: &[&str] = &[
    SYSTEM_ITEM_ALLOC_KEY,
    SYSTEM_CLUSTER_ID_ALLOC_KEY,
    SYSTEM_REPLICA_ID_ALLOC_KEY,
];

pub const USER_ALLOC_KEYS: &[&str] = &[
    USER_ITEM_ALLOC_KEY,
    USER_CLUSTER_ID_ALLOC_KEY,
    USER_REPLICA_ID_ALLOC_KEY,
];

#[derive(Clone, Debug)]
pub struct BootstrapArgs {
    pub cluster_replica_size_map: ClusterReplicaSizeMap,
    pub default_cluster_replica_size: String,
    pub default_cluster_replication_factor: u32,
    pub bootstrap_role: Option<String>,
}

pub type Epoch = NonZeroI64;

/// An API for opening a durable catalog state.
///
/// If a catalog is not opened, then resources should be release via [`Self::expire`].
#[async_trait]
pub trait OpenableDurableCatalogState: Debug + Send {
    // TODO(jkosh44) Teaching savepoint mode how to listen to additional
    // durable updates will be necessary for zero down time upgrades.
    /// Opens the catalog in a mode that accepts and buffers all writes,
    /// but never durably commits them. This is used to check and see if
    /// opening the catalog would be successful, without making any durable
    /// changes.
    ///
    /// Once a savepoint catalog reads an initial snapshot from durable
    /// storage, it will never read another update from durable storage. As a
    /// consequence, savepoint catalogs can never be fenced.
    ///
    /// Will return an error in the following scenarios:
    ///   - Catalog initialization fails.
    ///   - Catalog migrations fail.
    ///
    /// `initial_ts` is used as the initial timestamp for new environments.
    ///
    /// Also returns a handle to a thread that is deserializing all of the audit logs.
    async fn open_savepoint(
        mut self: Box<Self>,
        initial_ts: Timestamp,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<(Box<dyn DurableCatalogState>, AuditLogIterator), CatalogError>;

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
    ///
    /// Also returns a handle to a thread that is deserializing all of the audit logs.
    async fn open(
        mut self: Box<Self>,
        initial_ts: Timestamp,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<(Box<dyn DurableCatalogState>, AuditLogIterator), CatalogError>;

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

    /// Get the most recent deployment generation written to the catalog. Not necessarily the
    /// deploy generation of this instance.
    async fn get_deployment_generation(&mut self) -> Result<u64, CatalogError>;

    /// Get the `with_0dt_deployment_max_wait` config value of this instance.
    ///
    /// This mirrors the `with_0dt_deployment_max_wait` "system var" so that we can
    /// toggle the flag with LaunchDarkly, but use it in boot before
    /// LaunchDarkly is available.
    async fn get_0dt_deployment_max_wait(&mut self) -> Result<Option<Duration>, CatalogError>;

    /// Get the `with_0dt_deployment_ddl_check_interval` config value of this instance.
    ///
    /// This mirrors the `with_0dt_deployment_ddl_check_interval` "system var" so that we can
    /// toggle the flag with LaunchDarkly, but use it in boot before
    /// LaunchDarkly is available.
    async fn get_0dt_deployment_ddl_check_interval(
        &mut self,
    ) -> Result<Option<Duration>, CatalogError>;

    /// Get the `enable_0dt_deployment_panic_after_timeout` config value of this
    /// instance.
    ///
    /// This mirrors the `enable_0dt_deployment_panic_after_timeout` "system var"
    /// so that we can toggle the flag with LaunchDarkly, but use it in boot
    /// before LaunchDarkly is available.
    async fn get_enable_0dt_deployment_panic_after_timeout(
        &mut self,
    ) -> Result<Option<bool>, CatalogError>;

    /// Reports if the remote configuration was synchronized at least once.
    async fn has_system_config_synced_once(&mut self) -> Result<bool, DurableCatalogError>;

    /// Generate an unconsolidated [`Trace`] of catalog contents.
    async fn trace_unconsolidated(&mut self) -> Result<Trace, CatalogError>;

    /// Generate a consolidated [`Trace`] of catalog contents.
    async fn trace_consolidated(&mut self) -> Result<Trace, CatalogError>;

    /// Politely releases all external resources that can only be released in an async context.
    async fn expire(self: Box<Self>);
}

/// A read only API for the durable catalog state.
#[async_trait]
pub trait ReadOnlyDurableCatalogState: Debug + Send + Sync {
    /// Returns the epoch of the current durable catalog state. The epoch acts as
    /// a fencing token to prevent split brain issues across two
    /// [`DurableCatalogState`]s. When a new [`DurableCatalogState`] opens the
    /// catalog, it will increment the epoch by one (or initialize it to some
    /// value if there's no existing epoch) and store the value in memory. It's
    /// guaranteed that no two [`DurableCatalogState`]s will return the same value
    /// for their epoch.
    ///
    /// NB: We may remove this in later iterations of Pv2.
    fn epoch(&self) -> Epoch;

    /// Politely releases all external resources that can only be released in an async context.
    async fn expire(self: Box<Self>);

    /// Returns true if the system bootstrapping process is complete, false otherwise.
    fn is_bootstrap_complete(&self) -> bool;

    /// Get all audit log events.
    ///
    /// Results are guaranteed to be sorted by ID.
    ///
    /// WARNING: This is meant for use in integration tests and has bad performance.
    async fn get_audit_logs(&mut self) -> Result<Vec<VersionedEvent>, CatalogError>;

    /// Get the next ID of `id_type`, without allocating it.
    async fn get_next_id(&mut self, id_type: &str) -> Result<u64, CatalogError>;

    /// Get the next user ID without allocating it.
    async fn get_next_user_item_id(&mut self) -> Result<u64, CatalogError> {
        self.get_next_id(USER_ITEM_ALLOC_KEY).await
    }

    /// Get the next system ID without allocating it.
    async fn get_next_system_item_id(&mut self) -> Result<u64, CatalogError> {
        self.get_next_id(SYSTEM_ITEM_ALLOC_KEY).await
    }

    /// Get the next system replica id without allocating it.
    async fn get_next_system_replica_id(&mut self) -> Result<u64, CatalogError> {
        self.get_next_id(SYSTEM_REPLICA_ID_ALLOC_KEY).await
    }

    /// Get the next user replica id without allocating it.
    async fn get_next_user_replica_id(&mut self) -> Result<u64, CatalogError> {
        self.get_next_id(USER_REPLICA_ID_ALLOC_KEY).await
    }

    /// Get the deployment generation of this instance.
    async fn get_deployment_generation(&mut self) -> Result<u64, CatalogError>;

    /// Get a snapshot of the catalog.
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError>;

    /// Listen and return all updates that are currently in the catalog.
    ///
    /// IMPORTANT: This excludes updates to storage usage.
    ///
    /// Returns an error if this instance has been fenced out.
    async fn sync_to_current_updates(
        &mut self,
    ) -> Result<Vec<memory::objects::StateUpdate>, CatalogError>;

    // TODO(jkosh44) The fact that the timestamp argument is an exclusive upper bound makes
    // it difficult to use for readers. For now it's correct and easy to implement, but we should
    // consider a better API.
    /// Listen and return all updates in the catalog up to `target_upper`.
    ///
    /// IMPORTANT: This excludes updates to storage usage.
    ///
    /// Returns an error if this instance has been fenced out.
    async fn sync_updates(
        &mut self,
        target_upper: Timestamp,
    ) -> Result<Vec<memory::objects::StateUpdate>, CatalogError>;

    /// Fetch the current upper of the catalog state.
    async fn current_upper(&mut self) -> Timestamp;
}

/// A read-write API for the durable catalog state.
#[async_trait]
#[allow(mismatched_lifetime_syntaxes)]
pub trait DurableCatalogState: ReadOnlyDurableCatalogState {
    /// Returns true if the catalog is opened in read only mode, false otherwise.
    fn is_read_only(&self) -> bool;

    /// Returns true if the catalog is opened is savepoint mode, false otherwise.
    fn is_savepoint(&self) -> bool;

    /// Marks the bootstrap process as complete.
    async fn mark_bootstrap_complete(&mut self);

    /// Creates a new durable catalog state transaction.
    async fn transaction(&mut self) -> Result<Transaction, CatalogError>;

    /// Commits a durable catalog state transaction. The transaction will be committed at
    /// `commit_ts`.
    ///
    /// Returns what the upper was directly after the transaction committed.
    ///
    /// Panics if `commit_ts` is not greater than or equal to the most recent upper seen by this
    /// process.
    async fn commit_transaction(
        &mut self,
        txn_batch: TransactionBatch,
        commit_ts: Timestamp,
    ) -> Result<Timestamp, CatalogError>;

    /// Confirms that this catalog is connected as the current leader.
    ///
    /// NB: We may remove this in later iterations of Pv2.
    async fn confirm_leadership(&mut self) -> Result<(), CatalogError>;

    /// Allocates and returns `amount` IDs of `id_type`.
    ///
    /// See [`Self::commit_transaction`] for details on `commit_ts`.
    #[mz_ore::instrument(level = "debug")]
    async fn allocate_id(
        &mut self,
        id_type: &[&str],
        amount: u64,
        commit_ts: Timestamp,
    ) -> Result<Vec<u64>, CatalogError> {
        if amount == 0 {
            return Ok(Vec::new());
        }
        let mut txn = self.transaction().await?;
        let ids = txn.get_and_increment_id_by(id_type, amount)?;
        txn.commit_internal(commit_ts).await?;
        Ok(ids)
    }

    /// Allocates and returns `amount` many user [`CatalogItemId`] and [`GlobalId`].
    ///
    /// See [`Self::commit_transaction`] for details on `commit_ts`.
    async fn allocate_user_ids(
        &mut self,
        amount: u64,
        commit_ts: Timestamp,
    ) -> Result<Vec<(CatalogItemId, GlobalId)>, CatalogError> {
        let ids = self.allocate_id(USER_ALLOC_KEYS, amount, commit_ts).await?;
        let ids = ids
            .iter()
            .map(|id| (CatalogItemId::User(*id), GlobalId::User(*id)))
            .collect();
        Ok(ids)
    }

    /// Allocates and returns both a user [`CatalogItemId`] and [`GlobalId`].
    ///
    /// See [`Self::commit_transaction`] for details on `commit_ts`.
    async fn allocate_user_id(
        &mut self,
        commit_ts: Timestamp,
    ) -> Result<(CatalogItemId, GlobalId), CatalogError> {
        let id = self.allocate_id(USER_ALLOC_KEYS, 1, commit_ts).await?;
        let id = id.into_element();
        Ok((CatalogItemId::User(id), GlobalId::User(id)))
    }

    /// Allocates and returns a user [`ClusterId`].
    ///
    /// See [`Self::commit_transaction`] for details on `commit_ts`.
    async fn allocate_user_cluster_id(
        &mut self,
        commit_ts: Timestamp,
    ) -> Result<ClusterId, CatalogError> {
        let id = self
            .allocate_id(USER_ALLOC_KEYS, 1, commit_ts)
            .await?
            .into_element();
        Ok(ClusterId::user(id).ok_or(SqlCatalogError::IdExhaustion)?)
    }
}

trait AuditLogIteratorTrait: Iterator<Item = (AuditLog, Timestamp)> + Send + Sync + Debug {}
impl<T: Iterator<Item = (AuditLog, Timestamp)> + Send + Sync + Debug> AuditLogIteratorTrait for T {}

/// An iterator that returns audit log events in reverse ID order.
#[derive(Debug)]
pub struct AuditLogIterator {
    // We store an interator instead of a sorted `Vec`, so we can lazily sort the contents on the
    // first call to `next`, instead of sorting the contents on initialization.
    audit_logs: Box<dyn AuditLogIteratorTrait>,
}

impl AuditLogIterator {
    fn new(audit_logs: Vec<(StateUpdateKindJson, Timestamp, Diff)>) -> Self {
        let audit_logs = audit_logs
            .into_iter()
            .map(|(kind, ts, diff)| {
                assert_eq!(
                    diff,
                    Diff::ONE,
                    "audit log is append only: ({kind:?}, {ts:?}, {diff:?})"
                );
                assert!(
                    kind.is_audit_log(),
                    "unexpected update kind: ({kind:?}, {ts:?}, {diff:?})"
                );
                let id = kind.audit_log_id();
                (kind, ts, id)
            })
            .sorted_by_key(|(_, ts, id)| (*ts, *id))
            .map(|(kind, ts, _id)| (kind, ts))
            .rev()
            .map(|(kind, ts)| {
                // Each event will be deserialized lazily on a call to `next`.
                let kind = TryIntoStateUpdateKind::try_into(kind).expect("kind decoding error");
                let kind: Option<memory::objects::StateUpdateKind> = (&kind)
                    .try_into()
                    .expect("invalid persisted update: {update:#?}");
                let kind = kind.expect("audit log always produces im-memory updates");
                let audit_log = match kind {
                    memory::objects::StateUpdateKind::AuditLog(audit_log) => audit_log,
                    kind => unreachable!("invalid kind: {kind:?}"),
                };
                (audit_log, ts)
            });
        Self {
            audit_logs: Box::new(audit_logs),
        }
    }
}

impl Iterator for AuditLogIterator {
    type Item = (AuditLog, Timestamp);

    fn next(&mut self) -> Option<Self::Item> {
        self.audit_logs.next()
    }
}

/// A builder to help create an [`OpenableDurableCatalogState`] for tests.
#[derive(Debug, Clone)]
pub struct TestCatalogStateBuilder {
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
    deploy_generation: Option<u64>,
    metrics: Arc<Metrics>,
}

impl TestCatalogStateBuilder {
    pub fn new(persist_client: PersistClient) -> Self {
        Self {
            persist_client,
            organization_id: Uuid::new_v4(),
            version: semver::Version::new(0, 0, 0),
            deploy_generation: None,
            metrics: Arc::new(Metrics::new(&MetricsRegistry::new())),
        }
    }

    pub fn with_organization_id(mut self, organization_id: Uuid) -> Self {
        self.organization_id = organization_id;
        self
    }

    pub fn with_version(mut self, version: semver::Version) -> Self {
        self.version = version;
        self
    }

    pub fn with_deploy_generation(mut self, deploy_generation: u64) -> Self {
        self.deploy_generation = Some(deploy_generation);
        self
    }

    pub fn with_default_deploy_generation(self) -> Self {
        self.with_deploy_generation(0)
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    pub async fn build(self) -> Result<Box<dyn OpenableDurableCatalogState>, DurableCatalogError> {
        persist_backed_catalog_state(
            self.persist_client,
            self.organization_id,
            self.version,
            self.deploy_generation,
            self.metrics,
        )
        .await
    }

    pub async fn unwrap_build(self) -> Box<dyn OpenableDurableCatalogState> {
        self.expect_build("failed to build").await
    }

    pub async fn expect_build(self, msg: &str) -> Box<dyn OpenableDurableCatalogState> {
        self.build().await.expect(msg)
    }
}

/// Creates an openable durable catalog state implemented using persist.
///
/// `deploy_generation` MUST be `Some` to initialize a new catalog.
pub async fn persist_backed_catalog_state(
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
    deploy_generation: Option<u64>,
    metrics: Arc<Metrics>,
) -> Result<Box<dyn OpenableDurableCatalogState>, DurableCatalogError> {
    let state = UnopenedPersistCatalogState::new(
        persist_client,
        organization_id,
        version,
        deploy_generation,
        metrics,
    )
    .await?;
    Ok(Box::new(state))
}

pub fn test_bootstrap_args() -> BootstrapArgs {
    BootstrapArgs {
        default_cluster_replica_size: "scale=1,workers=1".into(),
        default_cluster_replication_factor: 1,
        bootstrap_role: None,
        cluster_replica_size_map: ClusterReplicaSizeMap::for_tests(),
    }
}
