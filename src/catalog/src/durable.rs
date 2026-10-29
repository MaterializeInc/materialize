// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate is responsible for durably storing and modifying the catalog contents.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mz_audit_log::VersionedEvent;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_persist_client::PersistClient;
use mz_persist_types::ShardId;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, SqlScalarType};
use mz_sql::catalog::CatalogError as SqlCatalogError;
use uuid::Uuid;

use crate::config::ClusterReplicaSizeMap;
use crate::durable::debug::{DebugCatalogState, Trace};
pub use crate::durable::error::{CatalogError, DurableCatalogError, FenceError};
pub use crate::durable::metrics::Metrics;
pub use crate::durable::objects::Snapshot;
pub use crate::durable::objects::state_update::StateUpdate;
pub use crate::durable::objects::{
    AdapterDeclaration, AdapterEndpoint, AdapterRuntime, BurstState, ClientIncarnation,
    ClientReadRequirement, ClientRealization, Cluster, ClusterConfig, ClusterReplica,
    ClusterReplicaDeclaration, ClusterRuntime, ClusterSystemConfiguration, ClusterVariant,
    ClusterVariantManaged, Comment, Database, DefaultPrivilege, IntrospectionSourceIndex, Item,
    NetworkPolicy, ReconfigurationState, ReconfigurationStatus, ReconfigurationTarget,
    ReplicaConfig, ReplicaLocation, ReplicaSystemConfiguration, Role, RoleAuth, Schema,
    SourceReference, SourceReferences, StorageCollectionMetadata, SystemConfiguration,
    SystemObjectDescription, SystemObjectMapping, UnfinalizedShard, WrittenPlan,
    managed_cluster_replica_name,
};
pub use crate::durable::persist::{CatalogSnapshotReader, catalog_shard_id, shard_id};
use crate::durable::persist::{Timestamp, UnopenedPersistCatalogState};
use crate::durable::transaction::TransactionBatch;
pub use crate::durable::transaction::{DryRunTransaction, Transaction};
pub use crate::durable::upgrade::CATALOG_VERSION;
use crate::memory;

pub mod debug;
mod error;
pub mod initialize;
mod metrics;
pub mod objects;
mod persist;
mod promotion;
mod traits;
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
pub const ADAPTER_ID_ALLOC_KEY: &str = "adapter";
pub const CLIENT_INCARNATION_ALLOC_KEY: &str = "client_incarnation";
pub const DEFAULT_ADAPTER_ID: mz_repr::AdapterId = mz_repr::AdapterId(1);
pub const DEFAULT_ADAPTER_SIZE: &str = "1";
pub const SYSTEM_REPLICA_ID_ALLOC_KEY: &str = "system_replica";
pub const AUDIT_LOG_ID_ALLOC_KEY: &str = "auditlog";
pub const STORAGE_USAGE_ID_ALLOC_KEY: &str = "storage_usage";
pub const USER_NETWORK_POLICY_ID_ALLOC_KEY: &str = "user_network_policy";
pub const OID_ALLOC_KEY: &str = "oid";
pub(crate) const CATALOG_CONTENT_VERSION_KEY: &str = "catalog_content_version";
pub const BUILTIN_MIGRATION_SHARD_KEY: &str = "builtin_migration_shard";
pub const EXPRESSION_CACHE_SHARD_KEY: &str = "expression_cache_shard";
pub const MOCK_AUTHENTICATION_NONCE_KEY: &str = "mock_authentication_nonce";

/// Provenance established at controller-owned creation: 0 pending, 1 complete.
/// Absence does not grant permission to initialize an existing environment.
pub const CONTROLLER_INITIALIZATION_COMPLETE_KEY: &str = "controller_initialization_complete";

#[derive(Clone, Debug)]
pub struct BootstrapArgs {
    pub controller_initialization: bool,
    pub default_adapter_size: String,
    pub cluster_replica_size_map: ClusterReplicaSizeMap,
    pub default_cluster_replica_size: String,
    pub default_cluster_replication_factor: u32,
    pub bootstrap_role: Option<String>,
}

pub type Epoch = NonZeroI64;

/// A catalog commit's lower bound and optional operation-scoped oracle allocation.
///
/// A plain timestamp is only a lower bound and requires a durable-layer allocation.
/// A [`mz_timestamp_oracle::WriteTimestamp`] must come from the shared
/// EpochMilliseconds oracle after this operation starts. Its allocation may be
/// retained across this operation's frontier-only retries, but not cached for
/// another operation. In particular, an oracle peek is not an allocation.
#[derive(Clone, Copy, Debug)]
pub struct CatalogCommitTimestamp {
    lower_bound: Timestamp,
    // At or below the lower bound, and unchanged by rebasing.
    allocation: Option<Timestamp>,
}

impl CatalogCommitTimestamp {
    pub fn lower_bound(self) -> Timestamp {
        self.lower_bound
    }

    pub(crate) fn advance_to(mut self, lower_bound: Timestamp) -> Self {
        self.lower_bound = self.lower_bound.max(lower_bound);
        self
    }
}

impl From<Timestamp> for CatalogCommitTimestamp {
    fn from(lower_bound: Timestamp) -> Self {
        Self {
            lower_bound,
            allocation: None,
        }
    }
}

impl From<mz_timestamp_oracle::WriteTimestamp> for CatalogCommitTimestamp {
    fn from(allocated: mz_timestamp_oracle::WriteTimestamp) -> Self {
        Self {
            lower_bound: allocated.timestamp,
            allocation: Some(allocated.timestamp),
        }
    }
}

/// The shared EpochMilliseconds oracle and clock used by a durable catalog writer.
/// Every production writer supplies this before opening, including generation
/// admission and independent replica publication.
#[derive(Clone, Debug)]
pub struct CatalogTimestampOracle {
    oracle: Arc<dyn mz_timestamp_oracle::TimestampOracle<mz_repr::Timestamp> + Send + Sync>,
    now: NowFn,
}

impl CatalogTimestampOracle {
    pub fn new(
        oracle: Arc<dyn mz_timestamp_oracle::TimestampOracle<mz_repr::Timestamp> + Send + Sync>,
        now: NowFn,
    ) -> Self {
        Self { oracle, now }
    }

    /// Refuse new future jumps, but preserve inherited progress and the next
    /// timestamp needed to advance it. A backwards clock step must not prevent
    /// bootstrap or heartbeat publication.
    fn check_timestamp(
        &self,
        timestamp: mz_repr::Timestamp,
        inherited: mz_repr::Timestamp,
    ) -> Result<(), DurableCatalogError> {
        let now = (self.now)().into();
        let wall_limit = mz_timestamp_oracle::write_ts_upper_bound(&now);
        let limit = wall_limit.max(inherited);
        if timestamp > limit {
            return Err(DurableCatalogError::TimestampTooFarAhead { timestamp, limit });
        }
        if timestamp > wall_limit {
            tracing::warn!(%timestamp, %now, %inherited,
                "preserving inherited catalog progress beyond the wall-clock bound");
        }
        Ok(())
    }
}

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
    async fn open_savepoint(
        mut self: Box<Self>,
        initial_ts: Timestamp,
        bootstrap_args: &BootstrapArgs,
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
    /// Unprotected catalogs increment the process epoch and reclaim ephemeral items.
    /// In protected catalogs, same-generation opens preserve owners, including those left by a
    /// crashed process. Promotion fences the outgoing generation, then reclaims
    /// temporary owners captured in the admitted snapshot. Cleanup retries preserve
    /// owners created after that snapshot. A crash between fencing and cleanup can
    /// leave items behind. Reopening in the active generation does not reclaim them.
    ///
    /// `initial_ts` is used as the initial timestamp for new environments.
    async fn open(
        mut self: Box<Self>,
        initial_ts: Timestamp,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Opens for warm handover, validating protected-catalog promotion policy
    /// against the same snapshot used to fence the serving generation.
    async fn open_for_promotion(
        self: Box<Self>,
        initial_ts: Timestamp,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Joins an initialized, current-version catalog in the supplied generation.
    /// Requires `catalog_read_protection_enabled != 0`.
    /// Does not initialize, migrate, promote, reclaim sessions, or bootstrap adapter.
    async fn join(self: Box<Self>) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Joins without promoting or migrating, using this writer's provisioned plan namespace.
    /// Requires an initialized, protected catalog with the current schema version.
    /// Before promotion, only protection and build-owned plan metadata may be committed.
    /// Joining does not grant output-write authority.
    async fn join_prewarming(
        self: Box<Self>,
        plan_build: &str,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Joins a prewarming deployment already admitted by its initializer.
    /// Unlike `join_prewarming`, this cannot admit a new deployment member.
    async fn join_admitted_prewarming(
        self: Box<Self>,
        plan_build: &str,
    ) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Joins the active durable generation, ignoring the builder's pending generation.
    /// Promotion racing admission or a later write returns a generation fence.
    /// Rebuild and join again to discover the newly active generation.
    async fn join_active(self: Box<Self>) -> Result<Box<dyn DurableCatalogState>, CatalogError>;

    /// Opens the catalog for manual editing of the underlying data. This is helpful for
    /// fixing a corrupt catalog.
    async fn open_debug(mut self: Box<Self>) -> Result<DebugCatalogState, CatalogError>;

    /// Reports if the catalog state has been initialized.
    async fn is_initialized(&mut self) -> Result<bool, CatalogError>;

    /// Returns the process epoch, which fences writers in unprotected catalogs.
    async fn epoch(&mut self) -> Result<Epoch, CatalogError>;

    /// Get the most recent deployment generation written to the catalog. Not necessarily the
    /// deploy generation of this instance.
    async fn get_deployment_generation(&mut self) -> Result<u64, CatalogError>;

    /// Whether this catalog uses durable read protection and native deployment admission.
    async fn catalog_read_protection_enabled(&mut self) -> Result<bool, CatalogError>;

    /// Whether explicitly controller-owned initialization is unfinished.
    /// Unmarked environments return false, not permission to initialize.
    async fn controller_initialization_pending(&mut self) -> Result<bool, CatalogError>;

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

/// Conservative eligibility for discarding execution-owned rows in this catalog's
/// environment, derived from one committed catalog prefix.
///
/// Admission allocates a fresh incarnation and inserts its live record atomically.
/// Identities below the allocation limit cannot be admitted again. Keeping both
/// the limit and live set from the same prefix makes a stale certificate safe:
/// it delays cleanup rather than treating a newer admission as retired.
#[derive(Debug, Clone)]
pub struct ClientIncarnationRetirement {
    next_id: Option<u64>,
    live: BTreeSet<u64>,
}

impl ClientIncarnationRetirement {
    /// Whether this incarnation can never be live again in this catalog history.
    /// A false result means live or unknown, not permission to discard its rows.
    pub fn is_retired(&self, incarnation: u64) -> bool {
        self.next_id.is_some_and(|next| incarnation < next) && !self.live.contains(&incarnation)
    }
}

/// A read only API for the durable catalog state.
#[async_trait]
pub trait ReadOnlyDurableCatalogState: Debug + Send + Sync {
    /// Returns the process epoch. Protected catalogs fence by deployment generation only.
    fn epoch(&self) -> Epoch;

    /// Returns the metrics for this catalog state.
    fn metrics(&self) -> &Metrics;

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

    /// Get the deployment generation of this instance, not its output-write authority.
    async fn get_deployment_generation(&mut self) -> Result<u64, CatalogError>;

    /// Get a snapshot of the catalog.
    ///
    /// Validates fencing and the runtime identity bound by
    /// [`DurableCatalogState::mark_bootstrap_complete`] before returning it.
    async fn snapshot(&mut self) -> Result<Snapshot, CatalogError>;

    /// Reads initialization provenance without consuming pending catalog updates.
    async fn controller_initialization_pending(&mut self) -> Result<bool, CatalogError> {
        let snapshot = self.snapshot().await?;
        let key = objects::ConfigKey {
            key: CONTROLLER_INITIALIZATION_COMPLETE_KEY.into(),
        };
        Ok(snapshot.configs.get(&key).map(|value| value.value) == Some(0))
    }

    /// Returns cleanup eligibility from the cached committed prefix, across all
    /// deployments. Validates the cached runtime context, but performs no I/O.
    /// Missing allocation metadata conservatively leaves every identity unknown.
    /// Requires writable mode, since other modes can contain uncommitted changes.
    /// Sync first if a more recent certificate is needed.
    fn client_incarnation_retirement(&self) -> Result<ClientIncarnationRetirement, CatalogError>;

    /// Fetches a current durable prefix and returns the requested collection mappings.
    ///
    /// Unlike a cached upper check, this observes commits above a caller's allocated
    /// timestamp. It does not consume updates awaiting projection application.
    /// Requires writable mode because savepoints can contain uncommitted mappings.
    async fn collection_shards_at_current(
        &mut self,
        ids: &BTreeSet<GlobalId>,
    ) -> Result<BTreeMap<GlobalId, ShardId>, CatalogError>;

    /// Listen and return all updates that are currently in the catalog.
    ///
    /// IMPORTANT: This excludes updates to storage usage.
    ///
    /// Returns fencing or bootstrap-bound `RestartRequired` errors before yielding
    /// memory updates. See [`DurableCatalogState::mark_bootstrap_complete`].
    async fn sync_to_current_updates(
        &mut self,
    ) -> Result<Vec<memory::objects::StateUpdate>, CatalogError>;

    /// Synchronizes once and drains the complete pending prefix only if every
    /// update is accepted. Otherwise returns `CatalogOutOfSync` without consuming
    /// any updates. Includes updates buffered by other users of this handle.
    async fn sync_to_current_updates_if(
        &mut self,
        accept: &(dyn for<'a> Fn(&'a memory::objects::StateUpdate) -> bool + Send + Sync),
    ) -> Result<Vec<memory::objects::StateUpdate>, CatalogError>;

    // TODO(jkosh44) The fact that the timestamp argument is an exclusive upper bound makes
    // it difficult to use for readers. For now it's correct and easy to implement, but we should
    // consider a better API.
    /// Listen and return all updates in the catalog up to `target_upper`.
    ///
    /// IMPORTANT: This excludes updates to storage usage.
    ///
    /// Returns fencing or bootstrap-bound `RestartRequired` errors before yielding
    /// memory updates. See [`DurableCatalogState::mark_bootstrap_complete`].
    async fn sync_updates(
        &mut self,
        target_upper: Timestamp,
    ) -> Result<Vec<memory::objects::StateUpdate>, CatalogError>;

    /// Checks for unapplied catalog content before `target_upper` without consuming it.
    ///
    /// Fencing and bootstrap-bound `RestartRequired` errors take precedence over
    /// `CatalogOutOfSync`. See [`DurableCatalogState::mark_bootstrap_complete`].
    async fn ensure_not_out_of_sync(&mut self, target_upper: Timestamp)
    -> Result<(), CatalogError>;

    /// Fetch the current upper of the catalog state.
    async fn current_upper(&mut self) -> Timestamp;

    /// The exclusive upper already synchronized by this handle, without remote I/O.
    ///
    /// This can include pending projection updates. It certifies an in-memory
    /// projection only after applying the corresponding drained updates or
    /// reconstructing a captured transaction prefix. Capture it under the same
    /// lock as the drain, after successful fence validation. Savepoint uppers
    /// do not certify durable history.
    ///
    /// [`Self::sync_to_current_updates`] drains through this upper, whereas
    /// [`Self::sync_updates`] can leave a suffix queued beyond its target.
    fn synced_upper(&self) -> Timestamp;
}

/// Owned durable input for independent catalog reconstruction, with no persist handles.
#[derive(Debug)]
pub struct CatalogSnapshot {
    pub snapshot: Snapshot,
    pub updates: Vec<memory::objects::StateUpdate>,
    pub upper: Timestamp,
    pub deployment_generation: u64,
    pub is_bootstrap_complete: bool,
}

/// A read-write API for the durable catalog state.
#[async_trait]
#[allow(mismatched_lifetime_syntaxes)]
pub trait DurableCatalogState: ReadOnlyDurableCatalogState {
    /// Returns true if the catalog is opened in read only mode, false otherwise.
    fn is_read_only(&self) -> bool;

    /// Returns true if the catalog is opened is savepoint mode, false otherwise.
    fn is_savepoint(&self) -> bool;

    /// Whether this handle joined an initialized deployment rather than opening
    /// it for initialization or migration. Joining does not authorize bootstrap.
    fn is_join(&self) -> bool;

    /// Whether this writable handle committed fresh catalog initialization.
    ///
    /// This is transient, handle-local provenance, not the catalog's current
    /// initialization status. Reopens, joins, read-only and savepoint handles
    /// return false.
    fn initialized_catalog(&self) -> bool;

    /// Marks the bootstrap process as complete.
    ///
    /// Captures the consumed read-protection mode and transaction WAL identity used
    /// to construct this runtime. Call after reconstruction and before serving,
    /// including when joining an existing writer. Repeated calls do not rebind it.
    /// Subsequent consumption, snapshots, transactions, and upper advancement return
    /// `RestartRequired` on an observed effective change to either value. The caller
    /// must halt and rebuild, not refresh and retry. Fencing takes precedence.
    /// Unmarked bootstrap and diagnostic readers may consume these changes freely.
    /// Only a non-joined writable bootstrap may upgrade shared Persist metadata.
    /// Joined writers preserve the active generation's metadata format.
    async fn mark_bootstrap_complete(&mut self);

    /// Creates a transaction only if no durable catalog content is pending.
    ///
    /// Empty upper progress is accepted.
    async fn transaction(&mut self) -> Result<Transaction, CatalogError>;

    /// Opens at the already synchronized prefix without another upper fetch.
    /// The caller must have applied all drained updates to its projection and
    /// retained exclusive handle ownership. Pending updates still reject open.
    async fn transaction_at_synced_upper(&mut self) -> Result<Transaction, CatalogError>;

    /// Creates a non-committable dry-run transaction from the given [`Snapshot`].
    ///
    /// The snapshot may include changes accumulated by earlier incremental dry runs.
    /// Its derived indexes must match its tables. Manual name or scope edits require
    /// [`Snapshot::rebuild_name_indexes`] before import. OID or OID-bearing table
    /// membership edits require [`Snapshot::rebuild_oid_index`].
    fn transaction_from_snapshot(
        &mut self,
        snapshot: Snapshot,
    ) -> Result<DryRunTransaction, CatalogError>;

    /// Commits a durable catalog state transaction at or after `commit_ts` and the
    /// current catalog upper.
    ///
    /// Returns what the upper was directly after the transaction committed.
    /// A production writer supplies an operation-scoped shared-oracle allocation
    /// or obtains one in the durable layer. Acknowledgement waits until oracle
    /// completion covers the actual durable timestamp. The
    /// future-timestamp bound is enforced before each durable attempt.
    ///
    /// Empty concurrent progress is retried. Concurrent content returns
    /// `CatalogOutOfSync` without replaying the batch, so callers must refresh and
    /// revalidate their decisions. Generation fencing is propagated unchanged.
    /// Panics if the batch's snapshot upper differs from this handle's cached upper.
    async fn commit_transaction(
        &mut self,
        txn_batch: TransactionBatch,
        commit_ts: CatalogCommitTimestamp,
    ) -> Result<Timestamp, CatalogError>;

    /// Advances the catalog upper to at least `new_upper`.
    ///
    /// A durable attempt observes fencing and retries concurrent content while retaining
    /// projection updates. Bootstrap-bound changes require restart rather than retry.
    /// A no-op only validates fencing and runtime identity already observed by this handle.
    /// Empty progress does not allocate or complete an oracle write.
    async fn advance_upper(&mut self, new_upper: Timestamp) -> Result<(), CatalogError>;

    /// Makes at most one attempt to advance the catalog upper to `new_upper`.
    ///
    /// Returns false after a definitive conflict if the target remains unmet.
    /// Consumed content remains pending for the caller's projection. Callers may
    /// release exclusive access before retrying the same target. Fencing, runtime
    /// identity and future-timestamp checks match [`Self::advance_upper`].
    async fn try_advance_upper(&mut self, new_upper: Timestamp) -> Result<bool, CatalogError>;

    /// Attempts to retire an explicitly identified prewarming deployment.
    ///
    /// Only the serving deployment may call this. Success is durable, includes
    /// target-owned cleanup and prevents readmission. Returns false on a catalog
    /// conflict, requiring a fresh attempt after relinquishing the caller's lock.
    async fn retire_deployment(&mut self, generation: u64) -> Result<bool, CatalogError>;

    /// Allocates and returns `amount` IDs of `id_type`.
    ///
    /// Retries content conflicts by allocating from a fresh durable snapshot. Pending
    /// projection updates remain available through `sync_updates` and
    /// `sync_to_current_updates`. Generation fencing is not retried.
    async fn allocate_id(
        &mut self,
        id_type: &str,
        amount: u64,
        commit_ts: Timestamp,
    ) -> Result<Vec<u64>, CatalogError>;

    /// Allocates and returns `amount` many user [`CatalogItemId`] and [`GlobalId`].
    ///
    /// See [`Self::commit_transaction`] for details on `commit_ts`.
    async fn allocate_user_ids(
        &mut self,
        amount: u64,
        commit_ts: Timestamp,
    ) -> Result<Vec<(CatalogItemId, GlobalId)>, CatalogError> {
        let ids = self
            .allocate_id(USER_ITEM_ALLOC_KEY, amount, commit_ts)
            .await?;
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
        let id = self.allocate_id(USER_ITEM_ALLOC_KEY, 1, commit_ts).await?;
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
            .allocate_id(USER_CLUSTER_ID_ALLOC_KEY, 1, commit_ts)
            .await?
            .into_element();
        Ok(ClusterId::user(id).ok_or(SqlCatalogError::IdExhaustion)?)
    }

    /// Allocates and returns `amount` many user [`ReplicaId`]s.
    ///
    /// See [`Self::commit_transaction`] for details on `commit_ts`.
    async fn allocate_user_replica_ids(
        &mut self,
        amount: u64,
        commit_ts: Timestamp,
    ) -> Result<Vec<ReplicaId>, CatalogError> {
        let ids = self
            .allocate_id(USER_REPLICA_ID_ALLOC_KEY, amount, commit_ts)
            .await?;
        let ids = ids.into_iter().map(ReplicaId::User).collect();
        Ok(ids)
    }

    /// Allocates and returns `amount` many system [`ReplicaId`]s.
    ///
    /// See [`Self::commit_transaction`] for details on `commit_ts`.
    async fn allocate_system_replica_ids(
        &mut self,
        amount: u64,
        commit_ts: Timestamp,
    ) -> Result<Vec<ReplicaId>, CatalogError> {
        let ids = self
            .allocate_id(SYSTEM_REPLICA_ID_ALLOC_KEY, amount, commit_ts)
            .await?;
        let ids = ids.into_iter().map(ReplicaId::System).collect();
        Ok(ids)
    }

    fn shard_id(&self) -> ShardId;
}

/// Returns the schema of the `Row`s/`SourceData`s stored in the persist
/// shard backing the catalog.
pub fn persist_desc() -> RelationDesc {
    RelationDesc::builder()
        .with_column("data", SqlScalarType::Jsonb.nullable(false))
        .finish()
}

/// A builder to help create an [`OpenableDurableCatalogState`] for tests.
#[derive(Debug, Clone)]
pub struct TestCatalogStateBuilder {
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
    deploy_generation: Option<u64>,
    metrics: Arc<Metrics>,
    timestamp_oracle: Option<CatalogTimestampOracle>,
}

impl TestCatalogStateBuilder {
    pub fn new(persist_client: PersistClient) -> Self {
        Self {
            persist_client,
            organization_id: Uuid::new_v4(),
            version: semver::Version::new(0, 0, 0),
            deploy_generation: None,
            metrics: Arc::new(Metrics::new(&MetricsRegistry::new())),
            timestamp_oracle: None,
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

    pub fn with_timestamp_oracle(mut self, timestamp_oracle: CatalogTimestampOracle) -> Self {
        self.timestamp_oracle = Some(timestamp_oracle);
        self
    }

    pub async fn build(self) -> Result<Box<dyn OpenableDurableCatalogState>, DurableCatalogError> {
        persist_backed_catalog_state(
            self.persist_client,
            self.organization_id,
            self.version,
            self.deploy_generation,
            self.metrics,
            self.timestamp_oracle,
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
/// Production writers must supply the shared EpochMilliseconds `timestamp_oracle`.
/// `None` is for readers, savepoints, and isolated durable-catalog unit fixtures.
pub async fn persist_backed_catalog_state(
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
    deploy_generation: Option<u64>,
    metrics: Arc<Metrics>,
    timestamp_oracle: Option<CatalogTimestampOracle>,
) -> Result<Box<dyn OpenableDurableCatalogState>, DurableCatalogError> {
    let state = UnopenedPersistCatalogState::new(
        persist_client,
        organization_id,
        version,
        deploy_generation,
        metrics,
        timestamp_oracle,
    )
    .await?;
    Ok(Box::new(state))
}

/// Acquires a secondary writer in the active durable generation for prewarming.
/// No pending deployment generation is accepted. A racing promotion returns a fence,
/// which the caller can handle by acquiring again. Does not bootstrap adapter.
pub async fn persist_backed_catalog_join_active(
    persist_client: PersistClient,
    organization_id: Uuid,
    version: semver::Version,
    metrics: Arc<Metrics>,
    timestamp_oracle: Option<CatalogTimestampOracle>,
) -> Result<Box<dyn DurableCatalogState>, CatalogError> {
    persist_backed_catalog_state(
        persist_client,
        organization_id,
        version,
        None,
        metrics,
        timestamp_oracle,
    )
    .await?
    .join_active()
    .await
}

pub fn test_bootstrap_args() -> BootstrapArgs {
    BootstrapArgs {
        controller_initialization: false,
        default_adapter_size: DEFAULT_ADAPTER_SIZE.into(),
        default_cluster_replica_size: "scale=1,workers=1".into(),
        default_cluster_replication_factor: 1,
        bootstrap_role: None,
        cluster_replica_size_map: ClusterReplicaSizeMap::for_tests(),
    }
}
