// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistent metadata storage for the coordinator.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::convert;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::builtin::{
    BUILTIN_PREFIXES, BuiltinCluster, BuiltinLog, BuiltinSource, BuiltinTable,
    MZ_CATALOG_SERVER_CLUSTER,
};
use crate::config::{
    AwsPrincipalContext, BuiltinItemMigrationConfig, ClusterReplicaSizeMap, Config, StateConfig,
};
use crate::durable::CatalogError as DurableError;
use crate::durable::{
    BootstrapArgs, DurableCatalogState, STORAGE_USAGE_ID_ALLOC_KEY, TestCatalogStateBuilder,
    test_bootstrap_args,
};
use crate::expr_cache::{ExpressionCacheHandle, GlobalExpressions, LocalExpressions};
use crate::memory::error::{Error, ErrorKind};
use crate::memory::objects::{
    CatalogCollectionEntry, CatalogEntry, CatalogItem, Cluster, ClusterReplica, Database,
    NetworkPolicy, Role, RoleAuth, Schema,
};
use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use itertools::Itertools;
use mz_adapter_types::bootstrap_builtin_cluster_config::{
    ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR, BootstrapBuiltinClusterConfig,
    CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR, PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR,
    SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR, SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR,
};
use mz_adapter_types::connection::ConnectionId;
use mz_audit_log::{EventType, FullNameV1, ObjectType, VersionedStorageUsage};
use mz_build_info::{BuildInfo, DUMMY_BUILD_INFO};
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_compute_types::dataflows::DataflowDescription;
use mz_controller_types::clusters::ReplicaLocation;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::OptimizedMirRelationExpr;
use mz_license_keys::ValidatedLicenseKey;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn, SYSTEM_TIME};
use mz_ore::result::ResultExt as _;
use mz_persist_client::PersistClient;
use mz_repr::adt::mz_acl_item::{AclMode, PrivilegeMap};
use mz_repr::explain::ExprHumanizer;
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::role_id::RoleId;
use mz_repr::{
    CatalogItemId, Diff, GlobalId, RelationVersion, RelationVersionSelector, SqlScalarType,
};
use mz_secrets::InMemorySecretsController;
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogDatabase, CatalogError as SqlCatalogError,
    CatalogItem as SqlCatalogItem, CatalogItemType as SqlCatalogItemType, CatalogNetworkPolicy,
    CatalogRole, CatalogSchema, DefaultPrivilegeAclItem, DefaultPrivilegeObject, EnvironmentId,
    SessionCatalog, SystemObjectType,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, FullItemName, FullSchemaName, ItemQualifiers, ObjectId,
    PUBLIC_ROLE_NAME, PartialItemName, QualifiedItemName, QualifiedSchemaName,
    ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::{Plan, PlanNotice, StatementDesc};
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::SystemVars;
use mz_sql_parser::ast::QualifiedReplica;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::connections::inline::{ConnectionResolver, InlinedConnection};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;
use tokio::sync::MutexGuard;
use uuid::Uuid;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
pub use crate::catalog::builtin_table_updates::BuiltinTableUpdate;
pub use crate::catalog::open::{InitializeStateResult, OpenCatalogResult};
pub use crate::catalog::state::CatalogState;
pub use crate::catalog::transact::{
    DropObjectInfo, InjectedAuditEvent, Op, ReplicaCreateDropReason, TransactionResult,
};
use crate::memory::implications::ParsedStateUpdate;

mod builtin_table_updates;
pub mod consistency;
mod migrate;

mod apply;
pub mod cluster_state;
mod error;
mod open;
mod retention;
mod state;
#[cfg(any(test, feature = "test"))]
pub mod test_support;
mod transact;
pub mod transaction_context;
mod util;
pub use error::CatalogError;
pub use util::sort_topological;
use util::{ResultExt, index_sql};

/// A `Catalog` keeps track of the SQL objects known to the planner.
///
/// For each object, it keeps track of both forward and reverse dependencies:
/// i.e., which objects are depended upon by the object, and which objects
/// depend upon the object. It enforces the SQL rules around dropping: an object
/// cannot be dropped until all of the objects that depend upon it are dropped.
/// It also enforces uniqueness of names.
///
/// SQL mandates a hierarchy of exactly three layers. A catalog contains
/// databases, databases contain schemas, and schemas contain catalog items,
/// like sources, sinks, view, and indexes.
///
/// To the outside world, databases, schemas, and items are all identified by
/// name. Items can be referred to by their [`FullItemName`], which fully and
/// unambiguously specifies the item, or a [`PartialItemName`], which can omit the
/// database name and/or the schema name. Partial names can be converted into
/// full names via a complicated resolution process documented by the
/// [`CatalogState::resolve`] method.
///
/// The catalog also maintains special "ambient schemas": virtual schemas,
/// implicitly present in all databases, that house various system views.
/// The big examples of ambient schemas are `pg_catalog` and `mz_catalog`.
#[derive(Debug)]
pub struct Catalog {
    state: CatalogState,
    expr_cache_handle: Option<ExpressionCacheHandle>,
    storage: Arc<tokio::sync::Mutex<Box<dyn crate::durable::DurableCatalogState>>>,
    transient_revision: u64,
    /// Opening context needed to reconstruct persisted state independently of this catalog.
    diagnostic_config: Arc<StateConfig>,
    /// The latest `transient_revision`, shared by all clones of this catalog.
    /// While `transient_revision` is this clone's own revision, frozen when
    /// the snapshot was taken, this field always tracks the latest revision
    /// across all clones. Comparing the two lets a snapshot holder detect
    /// from off-thread whether its planning-visible state is still current, via
    /// [`Catalog::transient_revision_is_current`], without a Coordinator
    /// round-trip (see `PeekClient::catalog_snapshot`).
    ///
    /// The store happens in `transact`, before the transaction's effects can
    /// be observed anywhere (responses, notices, builtin table writes), so a
    /// session that has observed any evidence of a planning-visible change is
    /// guaranteed to see the corresponding bump.
    shared_transient_revision: Arc<AtomicU64>,
}

/// A handle for advancing the durable catalog upper off the coordinator loop.
#[derive(Debug, Clone)]
pub struct CatalogUpperHandle {
    storage: Arc<tokio::sync::Mutex<Box<dyn crate::durable::DurableCatalogState>>>,
}

/// A committed projection and the initialization metadata from that same prefix.
/// Shard identities are immutable once initialized. An absent identity requires
/// completing initialization and reopening before the corresponding runtime starts.
pub struct OpenCommittedCatalog {
    pub catalog: Catalog,
    pub initial_updates: Vec<ParsedStateUpdate>,
    pub expression_cache_shard: Option<mz_persist_client::ShardId>,
    pub txn_wal_shard: Option<mz_persist_client::ShardId>,
}

impl CatalogUpperHandle {
    /// Advances the durable catalog upper to at least `new_upper`.
    pub async fn advance_upper(
        &self,
        new_upper: mz_repr::Timestamp,
    ) -> Result<(), crate::durable::CatalogError> {
        self.storage.lock().await.advance_upper(new_upper).await
    }
}

// Implement our own Clone because derive can't unless S is Clone, which it's
// not (hence the Arc).
impl Clone for Catalog {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            expr_cache_handle: self.expr_cache_handle.clone(),
            storage: Arc::clone(&self.storage),
            transient_revision: self.transient_revision,
            diagnostic_config: Arc::clone(&self.diagnostic_config),
            shared_transient_revision: Arc::clone(&self.shared_transient_revision),
        }
    }
}

impl Catalog {
    /// Non-durable environment context needed to reconstruct this catalog in a replica.
    pub fn replica_config(&self) -> crate::config::ReplicaCatalogConfig {
        let mut config = crate::config::ReplicaCatalogConfig::from_state(&self.diagnostic_config);
        config.system_parameter_defaults = self.state.system_config().defaults();
        config
    }

    /// Set the optimized plan for the item identified by `id`.
    ///
    /// # Panics
    /// If the item is not an `Index`, `MaterializedView`, or
    /// `ContinualTask`.
    #[mz_ore::instrument(level = "trace")]
    pub fn set_optimized_plan(
        &mut self,
        id: GlobalId,
        plan: DataflowDescription<OptimizedMirRelationExpr>,
    ) {
        self.state.set_optimized_plan(id, plan);
    }

    /// Set the physical plan for the item identified by `id`.
    ///
    /// # Panics
    /// If the item is not an `Index`, `MaterializedView`, or
    /// `ContinualTask`.
    #[mz_ore::instrument(level = "trace")]
    pub fn set_physical_plan(
        &mut self,
        id: GlobalId,
        plan: DataflowDescription<mz_compute_types::plan::LirRelationExpr>,
    ) {
        self.state.set_physical_plan(id, plan);
    }

    /// Try to get the optimized plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn try_get_optimized_plan(
        &self,
        id: &GlobalId,
    ) -> Option<&DataflowDescription<OptimizedMirRelationExpr>> {
        let entry = self.state.try_get_entry_by_global_id(id)?;
        entry.item().optimized_plan().map(AsRef::as_ref)
    }

    /// Try to get the physical plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn try_get_physical_plan(
        &self,
        id: &GlobalId,
    ) -> Option<&DataflowDescription<mz_compute_types::plan::LirRelationExpr>> {
        let entry = self.state.try_get_entry_by_global_id(id)?;
        entry.item().physical_plan().map(AsRef::as_ref)
    }

    /// Set the `DataflowMetainfo` for the item identified by `id`.
    ///
    /// # Panics
    /// If the item is not an `Index`, `MaterializedView`, or
    /// `ContinualTask`.
    #[mz_ore::instrument(level = "trace")]
    pub fn set_dataflow_metainfo(
        &mut self,
        id: GlobalId,
        metainfo: DataflowMetainfo<Arc<OptimizerNotice>>,
    ) {
        self.state.set_dataflow_metainfo(id, metainfo);
    }

    /// Try to get the `DataflowMetainfo` for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn try_get_dataflow_metainfo(
        &self,
        id: &GlobalId,
    ) -> Option<&DataflowMetainfo<Arc<OptimizerNotice>>> {
        let entry = self.state.try_get_entry_by_global_id(id)?;
        entry.item().dataflow_metainfo()
    }
}

/// A catalog snapshot and resolution context, independent of a serving session.
///
/// Prepared statements and portals are absent, and plan notices are discarded.
#[derive(Debug)]
pub struct CatalogStateView<'a> {
    state: Cow<'a, CatalogState>,
    /// Because we don't have any way of removing items from the catalog
    /// temporarily, we allow the catalog view to pretend that a set of items
    /// don't exist during resolution.
    ///
    /// This feature is necessary to allow re-planning of statements, which is
    /// either incredibly useful or required when altering item definitions.
    ///
    /// Note that uses of this field should be used by short-lived
    /// catalogs.
    unresolvable_ids: BTreeSet<CatalogItemId>,
    conn_id: ConnectionId,
    cluster: String,
    database: Option<DatabaseId>,
    search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    role_id: RoleId,
    restrict_to_user_objects: bool,
}

impl CatalogStateView<'_> {
    /// Resolves against `state` using the caller's already-resolved session names.
    pub fn new<'a>(
        state: &'a CatalogState,
        conn_id: ConnectionId,
        cluster: String,
        database: Option<DatabaseId>,
        search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        role_id: RoleId,
        restrict_to_user_objects: bool,
    ) -> CatalogStateView<'a> {
        CatalogStateView {
            state: Cow::Borrowed(state),
            unresolvable_ids: BTreeSet::new(),
            conn_id,
            cluster,
            database,
            search_path,
            role_id,
            restrict_to_user_objects,
        }
    }

    pub fn conn_id(&self) -> &ConnectionId {
        &self.conn_id
    }

    pub fn state(&self) -> &CatalogState {
        &*self.state
    }

    /// Prevent planning from resolving item with the provided ID. Instead,
    /// return an error as if the item did not exist.
    ///
    /// This feature is meant exclusively to permit re-planning statements
    /// during update operations and should not be used otherwise given its
    /// extremely "powerful" semantics.
    ///
    /// # Panics
    /// If the catalog's role ID is not [`MZ_SYSTEM_ROLE_ID`].
    pub fn mark_id_unresolvable_for_replanning(&mut self, id: CatalogItemId) {
        assert_eq!(
            self.role_id, MZ_SYSTEM_ROLE_ID,
            "only the system role can mark IDs unresolvable",
        );
        self.unresolvable_ids.insert(id);
    }

    /// Returns the schemas:
    /// - mz_catalog
    /// - pg_catalog
    /// - temp (if requested)
    /// - all schemas from the session's search_path var that exist
    pub fn effective_search_path(
        &self,
        include_temp_schema: bool,
    ) -> Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)> {
        self.state
            .effective_search_path(&self.search_path, include_temp_schema)
    }
}

impl ConnectionResolver for CatalogStateView<'_> {
    fn resolve_connection(
        &self,
        id: CatalogItemId,
    ) -> mz_storage_types::connections::Connection<InlinedConnection> {
        self.state().resolve_connection(id)
    }
}

/// AWS environment context for a debug catalog.
///
/// The `mz_aws_connections` and `mz_aws_privatelink_connections` builtin
/// materialized views fold `mz_aws_account_id()`, `mz_aws_external_id_prefix()`,
/// and `mz_aws_connection_role_arn()` into their optimized expressions. A debug
/// catalog opened to be compared against a live environment (the testdrive
/// consistency check) must fold those functions to the same values the live
/// environment used, otherwise the optimized expressions diverge. This mirrors
/// how `environment_id` is threaded into the catalog copy. Fields left `None`
/// fold to SQL NULL, matching an environment without that context.
#[derive(Debug, Clone, Default)]
pub struct DebugAwsContext {
    pub aws_account_id: Option<String>,
    pub aws_external_id_prefix: Option<String>,
    pub aws_connection_role_arn: Option<String>,
}

impl Catalog {
    /// Returns the catalog's transient revision, which starts at 1 and is
    /// incremented on every planning-visible change, including system configuration.
    /// Audit logs, read protection, and shard finalization bookkeeping do not affect it.
    /// It is not persisted to disk and restarts on every load.
    pub fn transient_revision(&self) -> u64 {
        self.transient_revision
    }

    /// Reports whether this catalog's transient revision is still the latest,
    /// i.e., whether its planning-visible state is equivalent to the current
    /// catalog's. Can be called on a snapshot from off-thread, without a
    /// Coordinator round-trip. See the field documentation on
    /// `shared_transient_revision`.
    pub fn transient_revision_is_current(&self) -> bool {
        self.transient_revision
            == self
                .shared_transient_revision
                .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Creates a debug catalog from the current
    /// `METADATA_BACKEND_URL` with parameters set appropriately for debug contexts,
    /// like in tests.
    ///
    /// WARNING! This function can arbitrarily fail because it does not make any
    /// effort to adjust the catalog's contents' structure or semantics to the
    /// currently running version, i.e. it does not apply any migrations.
    ///
    /// This function must not be called in production contexts. Use
    /// [`Catalog::open`] with appropriately set configuration parameters
    /// instead.
    pub async fn with_debug<F, Fut, T>(f: F) -> T
    where
        F: FnOnce(Catalog) -> Fut,
        Fut: Future<Output = T>,
    {
        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let bootstrap_args = test_bootstrap_args();
        let catalog = Self::open_debug_catalog(persist_client, organization_id, &bootstrap_args)
            .await
            .expect("can open debug catalog");
        f(catalog).await
    }

    /// Like [`Catalog::with_debug`], but folds the given AWS context into the
    /// catalog. Used to exercise builtin materialized views that reproduce AWS
    /// environment context in SQL (see [`DebugAwsContext`]).
    pub async fn with_debug_aws_context<F, Fut, T>(aws_context: DebugAwsContext, f: F) -> T
    where
        F: FnOnce(Catalog) -> Fut,
        Fut: Future<Output = T>,
    {
        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let bootstrap_args = test_bootstrap_args();
        let catalog = Self::open_debug_catalog_with_aws_context(
            persist_client,
            organization_id,
            &bootstrap_args,
            Some(aws_context),
        )
        .await
        .expect("can open debug catalog");
        f(catalog).await
    }

    /// Like [`Catalog::with_debug`], but the catalog created believes that bootstrap is still
    /// in progress.
    pub async fn with_debug_in_bootstrap<F, Fut, T>(f: F) -> T
    where
        F: FnOnce(Catalog) -> Fut,
        Fut: Future<Output = T>,
    {
        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let bootstrap_args = test_bootstrap_args();
        let mut catalog =
            Self::open_debug_catalog(persist_client.clone(), organization_id, &bootstrap_args)
                .await
                .expect("can open debug catalog");

        // Replace `storage` in `catalog` with one that doesn't think bootstrap is over.
        let now = SYSTEM_TIME.clone();
        let openable_storage = TestCatalogStateBuilder::new(persist_client)
            .with_organization_id(organization_id)
            .with_default_deploy_generation()
            .build()
            .await
            .expect("can create durable catalog");
        let mut storage = openable_storage
            .open(now().into(), &bootstrap_args)
            .await
            .expect("can open durable catalog");
        // Drain updates.
        let _ = storage
            .sync_to_current_updates()
            .await
            .expect("can sync to current updates");
        catalog.storage = Arc::new(tokio::sync::Mutex::new(storage));

        f(catalog).await
    }

    /// Opens a debug catalog.
    ///
    /// See [`Catalog::with_debug`].
    pub async fn open_debug_catalog(
        persist_client: PersistClient,
        organization_id: Uuid,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Catalog, anyhow::Error> {
        let now = SYSTEM_TIME.clone();
        let environment_id = Some(
            format!("local-az1-{organization_id}-0")
                .parse()
                .expect("valid debug environment ID"),
        );
        let openable_storage = TestCatalogStateBuilder::new(persist_client.clone())
            .with_organization_id(organization_id)
            .with_default_deploy_generation()
            .build()
            .await?;
        let storage = openable_storage.open(now().into(), bootstrap_args).await?;
        let system_parameter_defaults = BTreeMap::default();
        Self::open_debug_catalog_inner(
            persist_client,
            storage,
            now,
            environment_id,
            &DUMMY_BUILD_INFO,
            system_parameter_defaults,
            bootstrap_args,
            None,
            None,
        )
        .await
    }

    /// Like [`Catalog::open_debug_catalog`], but folds `aws_context` into the
    /// catalog (see [`DebugAwsContext`]).
    ///
    /// Calls `open_debug_catalog_inner` directly rather than delegating through
    /// [`Catalog::open_debug_catalog`], to avoid adding an `async fn` layer to
    /// the returned future. The catalog-open future is close to the crate
    /// `recursion_limit`, and the extra nesting overflows it when computing the
    /// layout of callers such as the `catalog` benchmark.
    pub async fn open_debug_catalog_with_aws_context(
        persist_client: PersistClient,
        organization_id: Uuid,
        bootstrap_args: &BootstrapArgs,
        aws_context: Option<DebugAwsContext>,
    ) -> Result<Catalog, anyhow::Error> {
        let now = SYSTEM_TIME.clone();
        let environment_id = Some(
            format!("local-az1-{organization_id}-0")
                .parse()
                .expect("valid debug environment ID"),
        );
        let openable_storage = TestCatalogStateBuilder::new(persist_client.clone())
            .with_organization_id(organization_id)
            .with_default_deploy_generation()
            .build()
            .await?;
        let storage = openable_storage.open(now().into(), bootstrap_args).await?;
        let system_parameter_defaults = BTreeMap::default();
        Self::open_debug_catalog_inner(
            persist_client,
            storage,
            now,
            environment_id,
            &DUMMY_BUILD_INFO,
            system_parameter_defaults,
            bootstrap_args,
            None,
            aws_context,
        )
        .await
    }

    /// Opens a read only debug persist backed catalog defined by `persist_client` and
    /// `organization_id`.
    ///
    /// See [`Catalog::with_debug`].
    pub async fn open_debug_read_only_catalog(
        persist_client: PersistClient,
        organization_id: Uuid,
        bootstrap_args: &BootstrapArgs,
    ) -> Result<Catalog, anyhow::Error> {
        let now = SYSTEM_TIME.clone();
        let environment_id = Some(
            format!("local-az1-{organization_id}-0")
                .parse()
                .expect("valid debug environment ID"),
        );
        let openable_storage = TestCatalogStateBuilder::new(persist_client.clone())
            .with_organization_id(organization_id)
            .build()
            .await?;
        let storage = openable_storage
            .open_read_only(&test_bootstrap_args())
            .await?;
        let system_parameter_defaults = BTreeMap::default();
        Self::open_debug_catalog_inner(
            persist_client,
            storage,
            now,
            environment_id,
            &DUMMY_BUILD_INFO,
            system_parameter_defaults,
            bootstrap_args,
            None,
            None,
        )
        .await
    }

    /// Opens a read only debug persist backed catalog defined by `persist_client` and
    /// `organization_id`.
    ///
    /// See [`Catalog::with_debug`].
    pub async fn open_debug_read_only_persist_catalog_config(
        persist_client: PersistClient,
        now: NowFn,
        environment_id: EnvironmentId,
        system_parameter_defaults: BTreeMap<String, String>,
        build_info: &'static BuildInfo,
        bootstrap_args: &BootstrapArgs,
        enable_expression_cache_override: Option<bool>,
        aws_context: Option<DebugAwsContext>,
    ) -> Result<Catalog, anyhow::Error> {
        let openable_storage = TestCatalogStateBuilder::new(persist_client.clone())
            .with_organization_id(environment_id.organization_id())
            .with_version(
                build_info
                    .version
                    .parse()
                    .expect("build version is parseable"),
            )
            .build()
            .await?;
        let storage = openable_storage.open_read_only(bootstrap_args).await?;
        Self::open_debug_catalog_inner(
            persist_client,
            storage,
            now,
            Some(environment_id),
            build_info,
            system_parameter_defaults,
            bootstrap_args,
            enable_expression_cache_override,
            aws_context,
        )
        .await
    }

    /// Reconstructs a debug catalog from caller-prepared durable storage without reopening it.
    ///
    /// Storage must retain its initial update stream.
    pub async fn open_debug_catalog_inner(
        persist_client: PersistClient,
        storage: Box<dyn DurableCatalogState>,
        now: NowFn,
        environment_id: Option<EnvironmentId>,
        build_info: &'static BuildInfo,
        system_parameter_defaults: BTreeMap<String, String>,
        bootstrap_args: &BootstrapArgs,
        enable_expression_cache_override: Option<bool>,
        aws_context: Option<DebugAwsContext>,
    ) -> Result<Catalog, anyhow::Error> {
        let metrics_registry = &MetricsRegistry::new();
        let secrets_reader = Arc::new(InMemorySecretsController::new());
        // Used as a lower boundary of the boot_ts, but it's ok to use now() for
        // debugging/testing.
        let previous_ts = now().into();
        let replica_size = &bootstrap_args.default_cluster_replica_size;
        let read_only = false;

        // Fold the requested AWS context into the connection context and
        // principal context. The builtin AWS connection views reproduce these
        // values in SQL, so a catalog compared against a live environment must
        // resolve them identically. When `aws_context` is `None` the default
        // `for_tests` context is used unchanged.
        let mut connection_context = ConnectionContext::for_tests(secrets_reader);
        let aws_principal_context = match aws_context {
            None => None,
            Some(aws_context) => {
                connection_context.aws_external_id_prefix =
                    aws_context.aws_external_id_prefix.as_deref().map(|prefix| {
                        AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable(prefix)
                            .expect("infallible")
                    });
                connection_context.aws_connection_role_arn = aws_context.aws_connection_role_arn;
                aws_context.aws_account_id.map(|aws_account_id| {
                    AwsPrincipalContext {
                        aws_account_id,
                        // Only `aws_account_id` reaches the `mz_aws_account_id()`
                        // fold. The external id prefix is read from the connection
                        // context above, so any placeholder works here.
                        aws_external_id_prefix:
                            AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable(
                                "debug",
                            )
                            .expect("infallible"),
                    }
                })
            }
        };

        let OpenCatalogResult {
            catalog,
            last_seen_version: _,
            migrated_storage_collections_0dt: _,
            new_builtin_collections: _,
            builtin_table_updates: _,
            cached_global_exprs: _,
            uncached_local_exprs: _,
        } = Catalog::open(Config {
            storage,
            metrics_registry,
            state: StateConfig {
                unsafe_mode: true,
                all_features: false,
                build_info,
                environment_id: environment_id.unwrap_or_else(EnvironmentId::for_tests),
                read_only,
                now,
                boot_ts: previous_ts,
                skip_migrations: true,
                cluster_replica_sizes: bootstrap_args.cluster_replica_size_map.clone(),
                builtin_system_cluster_config: BootstrapBuiltinClusterConfig {
                    size: replica_size.clone(),
                    replication_factor: SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR,
                },
                builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig {
                    size: replica_size.clone(),
                    replication_factor: CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR,
                },
                builtin_probe_cluster_config: BootstrapBuiltinClusterConfig {
                    size: replica_size.clone(),
                    replication_factor: PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR,
                },
                builtin_support_cluster_config: BootstrapBuiltinClusterConfig {
                    size: replica_size.clone(),
                    replication_factor: SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR,
                },
                builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig {
                    size: replica_size.clone(),
                    replication_factor: ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR,
                },
                system_parameter_defaults,
                remote_system_parameters: None,
                availability_zones: vec![],
                egress_addresses: vec![],
                aws_principal_context,
                aws_privatelink_availability_zones: None,
                http_host_name: None,
                connection_context,
                builtin_item_migration_config: BuiltinItemMigrationConfig {
                    persist_client: persist_client.clone(),
                    read_only,
                    force_migration: None,
                },
                persist_client,
                enable_expression_cache_override,
                helm_chart_version: None,
                external_login_password_mz_system: None,
                license_key: ValidatedLicenseKey::for_tests(),
            },
        })
        .await?;
        Ok(catalog)
    }

    pub fn for_sessionless_user(&self, role_id: RoleId) -> CatalogStateView<'_> {
        self.state.for_sessionless_user(role_id)
    }

    pub fn for_system_session(&self) -> CatalogStateView<'_> {
        self.state.for_system_session()
    }

    pub async fn storage<'a>(
        &'a self,
    ) -> MutexGuard<'a, Box<dyn crate::durable::DurableCatalogState>> {
        self.storage.lock().await
    }

    pub async fn current_upper(&self) -> mz_repr::Timestamp {
        self.storage().await.current_upper().await
    }

    /// Read authoritative replica membership for cleanup after listing services.
    /// Does not substitute the caller's potentially older installed inventory.
    pub async fn committed_cluster_replicas(
        &self,
    ) -> Result<BTreeSet<(ClusterId, ReplicaId)>, CatalogError> {
        use crate::durable::objects::{ClusterReplica, DurableType};
        use mz_proto::RustType;
        let snapshot = self.storage().await.snapshot().await?;
        snapshot
            .cluster_replicas
            .into_iter()
            .map(|(key, value)| {
                let replica = ClusterReplica::from_key_value(
                    RustType::from_proto(key)?,
                    RustType::from_proto(value)?,
                );
                Ok((replica.cluster_id, replica.replica_id))
            })
            .collect::<Result<_, mz_proto::TryFromProtoError>>()
            .map_err(|error| CatalogError::Unstructured(error.into()))
    }

    /// Returns the catalog-owned transaction WAL identity after storage initialization.
    pub async fn txn_wal_shard(&self) -> Result<mz_persist_client::ShardId, CatalogError> {
        // This identity is immutable during runtime. Reading it requires a
        // fenced durable snapshot, not an up-to-date SQL working copy. Snapshot
        // reads leave peer updates queued for normal catalog application.
        let snapshot = self.storage().await.snapshot().await?;
        let value = snapshot.txn_wal_shard.get(&()).ok_or_else(|| {
            CatalogError::internal(
                "query client initialization",
                "transaction WAL has not been initialized",
            )
        })?;
        value
            .shard
            .parse()
            .map_err(|error| CatalogError::internal("transaction WAL identity", error))
    }

    /// Certifies a durable prefix while the caller serializes catalog snapshot capture.
    pub async fn current_upper_if_in_sync(&self) -> Result<mz_repr::Timestamp, CatalogError> {
        let mut storage = self.storage().await;
        if storage.is_savepoint() || storage.is_read_only() {
            return Err(CatalogError::ReadOnly);
        }
        let upper = storage.current_upper().await;
        // Allocations and empty upper advancement can run off-loop. Neither may
        // certify unapplied catalog content as part of the memory snapshot.
        storage.ensure_not_out_of_sync(upper).await?;
        Ok(upper)
    }

    /// Allocates and returns both a user [`CatalogItemId`] and [`GlobalId`], delegating to
    /// [`DurableCatalogState::allocate_user_id`].
    pub async fn allocate_user_id(
        &self,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<(CatalogItemId, GlobalId), Error> {
        Ok(self
            .storage()
            .await
            .allocate_user_id(commit_ts)
            .await
            .maybe_terminate("allocating user ids")?)
    }

    /// Allocate `amount` many user IDs. See [`DurableCatalogState::allocate_user_ids`].
    pub async fn allocate_user_ids(
        &self,
        amount: u64,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<Vec<(CatalogItemId, GlobalId)>, Error> {
        Ok(self
            .storage()
            .await
            .allocate_user_ids(amount, commit_ts)
            .await
            .maybe_terminate("allocating user ids")?)
    }

    pub async fn allocate_user_id_for_test(&self) -> Result<(CatalogItemId, GlobalId), Error> {
        let commit_ts = self.storage().await.current_upper().await;
        self.allocate_user_id(commit_ts).await
    }

    /// Allocates a single durable id for a storage usage collection batch.
    ///
    /// Bumps the durable `STORAGE_USAGE_ID_ALLOC_KEY` allocator by one and
    /// returns the previous value. The bump is committed at `commit_ts`.
    /// One id is shared by every row produced by a collection cycle (see
    /// `Coordinator::storage_usage_update`), so the durable cost is one
    /// allocator round-trip per cycle, not per shard.
    pub async fn allocate_storage_usage_id(
        &self,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<u64, Error> {
        use mz_ore::collections::CollectionExt;

        self.storage()
            .await
            .allocate_id(STORAGE_USAGE_ID_ALLOC_KEY, 1, commit_ts)
            .await
            .maybe_terminate("allocating storage usage id")
            .map(|ids| ids.into_element())
            .err_into()
    }

    /// Get the next user item ID without allocating it.
    pub async fn get_next_user_item_id(&self) -> Result<u64, Error> {
        self.storage()
            .await
            .get_next_user_item_id()
            .await
            .err_into()
    }

    #[cfg(test)]
    pub async fn allocate_system_id(
        &self,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<(CatalogItemId, GlobalId), Error> {
        use mz_ore::collections::CollectionExt;

        let mut storage = self.storage().await;
        let mut txn = storage.transaction().await?;
        let id = txn
            .allocate_system_item_ids(1)
            .maybe_terminate("allocating system ids")?
            .into_element();
        // Drain transaction.
        let _ = txn.get_and_commit_op_updates();
        txn.commit(commit_ts).await?;
        Ok(id)
    }

    /// Get the next system item ID without allocating it.
    pub async fn get_next_system_item_id(&self) -> Result<u64, Error> {
        self.storage()
            .await
            .get_next_system_item_id()
            .await
            .err_into()
    }

    /// Allocates and returns a user [`ClusterId`], delegating to
    /// [`DurableCatalogState::allocate_user_cluster_id`].
    pub async fn allocate_user_cluster_id(
        &self,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<ClusterId, Error> {
        Ok(self
            .storage()
            .await
            .allocate_user_cluster_id(commit_ts)
            .await
            .maybe_terminate("allocating user cluster ids")?)
    }

    /// Allocate `amount` many user replica IDs. See
    /// [`DurableCatalogState::allocate_user_replica_ids`].
    pub async fn allocate_user_replica_ids(
        &self,
        amount: u64,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<Vec<ReplicaId>, Error> {
        Ok(self
            .storage()
            .await
            .allocate_user_replica_ids(amount, commit_ts)
            .await
            .maybe_terminate("allocating user replica ids")?)
    }

    /// Allocate `amount` many system replica IDs. See
    /// [`DurableCatalogState::allocate_system_replica_ids`].
    pub async fn allocate_system_replica_ids(
        &self,
        amount: u64,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<Vec<ReplicaId>, Error> {
        Ok(self
            .storage()
            .await
            .allocate_system_replica_ids(amount, commit_ts)
            .await
            .maybe_terminate("allocating system replica ids")?)
    }

    /// Allocate `amount` many replica IDs for `cluster_id`, picking user or
    /// system IDs based on the cluster's ID type.
    pub async fn allocate_replica_ids(
        &self,
        cluster_id: ClusterId,
        amount: u64,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<Vec<ReplicaId>, Error> {
        if cluster_id.is_system() {
            self.allocate_system_replica_ids(amount, commit_ts).await
        } else {
            self.allocate_user_replica_ids(amount, commit_ts).await
        }
    }

    /// Get the next system replica id without allocating it.
    pub async fn get_next_system_replica_id(&self) -> Result<u64, Error> {
        self.storage()
            .await
            .get_next_system_replica_id()
            .await
            .err_into()
    }

    /// Get the next user replica id without allocating it.
    pub async fn get_next_user_replica_id(&self) -> Result<u64, Error> {
        self.storage()
            .await
            .get_next_user_replica_id()
            .await
            .err_into()
    }

    pub fn resolve_database(&self, database_name: &str) -> Result<&Database, SqlCatalogError> {
        self.state.resolve_database(database_name)
    }

    pub fn resolve_schema(
        &self,
        current_database: Option<&DatabaseId>,
        database_name: Option<&str>,
        schema_name: &str,
        conn_id: &ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        self.state
            .resolve_schema(current_database, database_name, schema_name, conn_id)
    }

    pub fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: &ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        self.state
            .resolve_schema_in_database(database_spec, schema_name, conn_id)
    }

    pub fn resolve_replica_in_cluster(
        &self,
        cluster_id: &ClusterId,
        replica_name: &str,
    ) -> Result<&ClusterReplica, SqlCatalogError> {
        self.state
            .resolve_replica_in_cluster(cluster_id, replica_name)
    }

    pub fn resolve_system_schema(&self, name: &'static str) -> SchemaId {
        self.state.resolve_system_schema(name)
    }

    pub fn resolve_search_path(
        &self,
        session: &dyn SessionMetadata,
    ) -> Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)> {
        self.state.resolve_search_path(session)
    }

    /// Resolves `name` to a non-function [`CatalogEntry`].
    pub fn resolve_entry(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.state
            .resolve_entry(current_database, search_path, name, conn_id)
    }

    /// Resolves a `BuiltinTable`.
    pub fn resolve_builtin_table(&self, builtin: &'static BuiltinTable) -> CatalogItemId {
        self.state.resolve_builtin_table(builtin)
    }

    /// Resolves a `BuiltinLog`.
    pub fn resolve_builtin_log(&self, builtin: &'static BuiltinLog) -> CatalogItemId {
        self.state.resolve_builtin_log(builtin).0
    }

    /// Resolves a `BuiltinSource`.
    pub fn resolve_builtin_storage_collection(
        &self,
        builtin: &'static BuiltinSource,
    ) -> CatalogItemId {
        self.state.resolve_builtin_source(builtin)
    }

    /// Resolves `name` to a function [`CatalogEntry`].
    pub fn resolve_function(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.state
            .resolve_function(current_database, search_path, name, conn_id)
    }

    /// Resolves `name` to a type [`CatalogEntry`].
    pub fn resolve_type(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.state
            .resolve_type(current_database, search_path, name, conn_id)
    }

    pub fn resolve_cluster(&self, name: &str) -> Result<&Cluster, SqlCatalogError> {
        self.state.resolve_cluster(name)
    }

    /// Resolves a [`Cluster`] for a [`BuiltinCluster`].
    ///
    /// # Panics
    /// * If the [`BuiltinCluster`] doesn't exist.
    ///
    pub fn resolve_builtin_cluster(&self, cluster: &BuiltinCluster) -> &Cluster {
        self.state.resolve_builtin_cluster(cluster)
    }

    pub fn get_mz_catalog_server_cluster_id(&self) -> &ClusterId {
        &self.resolve_builtin_cluster(&MZ_CATALOG_SERVER_CLUSTER).id
    }

    pub fn state(&self) -> &CatalogState {
        &self.state
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedItemName,
        conn_id: Option<&ConnectionId>,
    ) -> FullItemName {
        self.state.resolve_full_name(name, conn_id)
    }

    pub fn try_get_entry(&self, id: &CatalogItemId) -> Option<&CatalogEntry> {
        self.state.try_get_entry(id)
    }

    pub fn try_get_entry_by_global_id(&self, id: &GlobalId) -> Option<&CatalogEntry> {
        self.state.try_get_entry_by_global_id(id)
    }

    pub fn get_entry(&self, id: &CatalogItemId) -> &CatalogEntry {
        self.state.get_entry(id)
    }

    pub fn get_entry_by_global_id(&self, id: &GlobalId) -> CatalogCollectionEntry {
        self.state.get_entry_by_global_id(id)
    }

    pub fn get_global_ids<'a>(
        &'a self,
        id: &CatalogItemId,
    ) -> impl Iterator<Item = GlobalId> + use<'a> {
        self.get_entry(id).global_ids()
    }

    pub fn resolve_item_id(&self, id: &GlobalId) -> CatalogItemId {
        self.get_entry_by_global_id(id).id()
    }

    pub fn try_resolve_item_id(&self, id: &GlobalId) -> Option<CatalogItemId> {
        let item = self.try_get_entry_by_global_id(id)?;
        Some(item.id())
    }

    pub fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: &ConnectionId,
    ) -> &Schema {
        self.state.get_schema(database_spec, schema_spec, conn_id)
    }

    pub fn try_get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: &ConnectionId,
    ) -> Option<&Schema> {
        self.state
            .try_get_schema(database_spec, schema_spec, conn_id)
    }

    pub fn get_mz_catalog_schema_id(&self) -> SchemaId {
        self.state.get_mz_catalog_schema_id()
    }

    pub fn get_pg_catalog_schema_id(&self) -> SchemaId {
        self.state.get_pg_catalog_schema_id()
    }

    pub fn get_information_schema_id(&self) -> SchemaId {
        self.state.get_information_schema_id()
    }

    pub fn get_mz_internal_schema_id(&self) -> SchemaId {
        self.state.get_mz_internal_schema_id()
    }

    pub fn get_mz_introspection_schema_id(&self) -> SchemaId {
        self.state.get_mz_introspection_schema_id()
    }

    pub fn get_mz_unsafe_schema_id(&self) -> SchemaId {
        self.state.get_mz_unsafe_schema_id()
    }

    pub fn system_schema_ids(&self) -> impl Iterator<Item = SchemaId> + '_ {
        self.state.system_schema_ids()
    }

    pub fn get_database(&self, id: &DatabaseId) -> &Database {
        self.state.get_database(id)
    }

    pub fn try_get_role(&self, id: &RoleId) -> Option<&Role> {
        self.state.try_get_role(id)
    }

    pub fn get_role(&self, id: &RoleId) -> &Role {
        self.state.get_role(id)
    }

    pub fn try_get_role_by_name(&self, role_name: &str) -> Option<&Role> {
        self.state.try_get_role_by_name(role_name)
    }

    pub fn try_get_role_auth_by_id(&self, id: &RoleId) -> Option<&RoleAuth> {
        self.state.try_get_role_auth_by_id(id)
    }

    /// Registers the connection's temporary namespace: the `uuid` <->
    /// `conn_id` mapping used to stamp and apply durable temporary items
    /// owned by the session. The `mz_temp` schema itself is created when
    /// the first temporary item is applied.
    ///
    /// The coordinator calls this at a session's first temporary-item
    /// creation, strictly before the transaction that persists the item, and
    /// guards on [`CatalogState::has_temporary_namespace`], so registering
    /// an already-registered namespace is a bug.
    pub fn register_temporary_namespace(&mut self, conn_id: &ConnectionId, uuid: Uuid) {
        self.state
            .temporary_namespaces
            .register(conn_id.clone(), uuid);
    }

    /// Removes the connection's temporary namespace, if it has one.
    pub fn drop_temporary_namespace(&mut self, conn_id: &ConnectionId) {
        self.state.temporary_namespaces.unregister(conn_id)
    }

    pub fn object_dependents(
        &self,
        object_ids: &Vec<ObjectId>,
        conn_id: &ConnectionId,
    ) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state.object_dependents(object_ids, conn_id, &mut seen)
    }

    fn full_name_detail(name: &FullItemName) -> FullNameV1 {
        FullNameV1 {
            database: name.database.to_string(),
            schema: name.schema.clone(),
            item: name.item.clone(),
        }
    }

    pub fn find_available_cluster_name(&self, name: &str) -> String {
        let mut i = 0;
        let mut candidate = name.to_string();
        while self.state.clusters_by_name.contains_key(&candidate) {
            i += 1;
            candidate = format!("{}{}", name, i);
        }
        candidate
    }

    pub fn get_role_allowed_cluster_sizes(&self, role_id: &Option<RoleId>) -> Vec<String> {
        if role_id == &Some(MZ_SYSTEM_ROLE_ID) {
            self.cluster_replica_sizes()
                .enabled_allocations()
                .map(|a| a.0.to_owned())
                .collect::<Vec<_>>()
        } else {
            self.system_config().allowed_cluster_replica_sizes()
        }
    }

    pub fn concretize_replica_location(
        &self,
        location: crate::durable::ReplicaLocation,
        allowed_sizes: &Vec<String>,
        allowed_availability_zones: Option<&[String]>,
        allow_disabled: bool,
    ) -> Result<ReplicaLocation, Error> {
        self.state.concretize_replica_location(
            location,
            allowed_sizes,
            allowed_availability_zones,
            allow_disabled,
        )
    }

    pub fn ensure_valid_replica_size(
        &self,
        allowed_sizes: &[String],
        size: &String,
        allow_disabled: bool,
    ) -> Result<(), Error> {
        self.state
            .ensure_valid_replica_size(allowed_sizes, size, allow_disabled)
    }

    pub fn cluster_replica_sizes(&self) -> &ClusterReplicaSizeMap {
        &self.state.cluster_replica_sizes
    }

    /// Returns the privileges of an object by its ID.
    pub fn get_privileges(
        &self,
        id: &SystemObjectId,
        conn_id: &ConnectionId,
    ) -> Option<&PrivilegeMap> {
        match id {
            SystemObjectId::Object(id) => match id {
                ObjectId::Cluster(id) => Some(self.get_cluster(*id).privileges()),
                ObjectId::Database(id) => Some(self.get_database(id).privileges()),
                ObjectId::Schema((database_spec, schema_spec)) => Some(
                    self.get_schema(database_spec, schema_spec, conn_id)
                        .privileges(),
                ),
                ObjectId::Item(id) => Some(self.get_entry(id).privileges()),
                ObjectId::ClusterReplica(_) | ObjectId::Role(_) => None,
                ObjectId::NetworkPolicy(id) => Some(self.get_network_policy(*id).privileges()),
            },
            SystemObjectId::System => Some(&self.state.system_privileges),
        }
    }

    /// Advances the catalog upper to at least `new_upper`.
    ///
    /// Concurrent content is retained for this writer's next projection refresh.
    /// Upper advancement retries contention without replaying content. See
    /// [`crate::durable::DurableCatalogState::advance_upper`] for fencing semantics.
    #[mz_ore::instrument(level = "debug")]
    pub async fn advance_upper(&self, new_upper: mz_repr::Timestamp) -> Result<(), CatalogError> {
        Ok(self.storage().await.advance_upper(new_upper).await?)
    }

    /// Returns a durable-upper handle that shares the catalog storage mutex.
    pub fn upper_handle(&self) -> CatalogUpperHandle {
        CatalogUpperHandle {
            storage: Arc::clone(&self.storage),
        }
    }

    /// Return the ids of all log sources the given object depends on.
    pub fn introspection_dependencies(&self, id: CatalogItemId) -> Vec<CatalogItemId> {
        self.state.introspection_dependencies(id)
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> Result<String, Error> {
        self.state.dump(None)
    }

    pub async fn open_diagnostic_reader(
        &self,
    ) -> Result<crate::durable::CatalogSnapshotReader, CatalogError> {
        let config = &self.diagnostic_config;
        let bootstrap = BootstrapArgs {
            cluster_replica_size_map: config.cluster_replica_sizes.clone(),
            default_cluster_replica_size: config.builtin_system_cluster_config.size.clone(),
            default_cluster_replication_factor: config
                .builtin_system_cluster_config
                .replication_factor,
            bootstrap_role: None,
        };
        Ok(crate::durable::CatalogSnapshotReader::open(
            config.persist_client.clone(),
            config.environment_id.organization_id(),
            config.build_info.semver_version(),
            &bootstrap,
        )
        .await?)
    }

    /// Reconstructs a joined writer's own committed projection without bootstrap
    /// or sharing revision notifications with the serving SQL catalog.
    pub async fn writer_projection(
        &self,
        storage: Box<dyn DurableCatalogState>,
    ) -> Result<Self, CatalogError> {
        let mut config = Self::diagnostic_state_config(&self.diagnostic_config);
        config.system_parameter_defaults = self.state.system_config().defaults();
        Self::open_committed(config, storage)
            .await
            .map(|opened| opened.catalog)
    }

    /// Opens an independent projection of an initialized, same-version catalog.
    ///
    /// `storage` must be an already joined handle whose initial updates have not
    /// been consumed. This does not bootstrap, migrate, reconcile, or produce
    /// executable dataflow plans. It rejects reconstruction that would require durable changes.
    /// The caller owns generation admission and any downstream runtime setup.
    /// Initial parsed updates are returned before subsequent stream updates.
    pub async fn open_committed(
        config: StateConfig,
        mut storage: Box<dyn DurableCatalogState>,
    ) -> Result<OpenCommittedCatalog, CatalogError> {
        use mz_storage_client::controller::StorageTxn;
        let diagnostic_config = Arc::new(Self::diagnostic_state_config(&config));
        let deployment_generation = storage.get_deployment_generation().await?;
        let is_bootstrap_complete = storage.is_bootstrap_complete();
        let mut updates = Vec::new();
        let (snapshot, upper, expression_cache_shard, txn_wal_shard) = loop {
            updates.extend(storage.sync_to_current_updates().await?);
            match storage.transaction().await {
                Ok(tx) => {
                    break (
                        tx.current_snapshot(),
                        tx.upper(),
                        tx.get_expression_cache_shard(),
                        tx.get_txn_wal_shard(),
                    );
                }
                Err(DurableError::Durable(
                    crate::durable::DurableCatalogError::CatalogOutOfSync { .. },
                )) => continue,
                Err(error) => return Err(error.into()),
            }
        };
        let (state, catalog_updates) = Self::reconstruct_state_and_updates(
            config,
            crate::durable::CatalogSnapshot {
                snapshot,
                updates,
                upper,
                deployment_generation,
                is_bootstrap_complete,
            },
        )
        .await?;
        storage.mark_bootstrap_complete().await;
        Ok(OpenCommittedCatalog {
            catalog: Self {
                state,
                expr_cache_handle: None,
                storage: Arc::new(tokio::sync::Mutex::new(storage)),
                transient_revision: 1,
                shared_transient_revision: Arc::new(AtomicU64::new(1)),
                diagnostic_config,
            },
            initial_updates: catalog_updates,
            expression_cache_shard,
            txn_wal_shard,
        })
    }

    /// Checks the [`Catalog`]s internal consistency.
    ///
    /// Returns a JSON object describing the inconsistencies, if there are any.
    pub fn check_consistency(&self) -> Result<(), serde_json::Value> {
        self.state.check_consistency().map_err(|inconsistencies| {
            serde_json::to_value(inconsistencies).unwrap_or_else(|_| {
                serde_json::Value::String("failed to serialize inconsistencies".to_string())
            })
        })
    }

    pub fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        self.state.config()
    }

    pub fn entries(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.state.entry_by_id.values()
    }

    pub fn user_connections(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_connection() && entry.id().is_user())
    }

    pub fn user_tables(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_table() && entry.id().is_user())
    }

    pub fn user_sources(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_source() && entry.id().is_user())
    }

    pub fn user_sinks(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_sink() && entry.id().is_user())
    }

    pub fn user_materialized_views(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_materialized_view() && entry.id().is_user())
    }

    pub fn user_secrets(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_secret() && entry.id().is_user())
    }

    pub fn get_network_policy(&self, network_policy_id: NetworkPolicyId) -> &NetworkPolicy {
        self.state.get_network_policy(&network_policy_id)
    }

    pub fn get_network_policy_by_name(&self, name: &str) -> Option<&NetworkPolicy> {
        self.state.try_get_network_policy_by_name(name)
    }

    pub fn clusters(&self) -> impl Iterator<Item = &Cluster> {
        self.state.clusters_by_id.values()
    }

    pub fn get_cluster(&self, cluster_id: ClusterId) -> &Cluster {
        self.state.get_cluster(cluster_id)
    }

    pub fn try_get_cluster(&self, cluster_id: ClusterId) -> Option<&Cluster> {
        self.state.try_get_cluster(cluster_id)
    }

    pub fn user_clusters(&self) -> impl Iterator<Item = &Cluster> {
        self.clusters().filter(|cluster| cluster.id.is_user())
    }

    pub fn get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &ClusterReplica {
        self.state.get_cluster_replica(cluster_id, replica_id)
    }

    pub fn try_get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Option<&ClusterReplica> {
        self.state.try_get_cluster_replica(cluster_id, replica_id)
    }

    pub fn user_cluster_replicas(&self) -> impl Iterator<Item = &ClusterReplica> {
        self.user_clusters()
            .flat_map(|cluster| cluster.user_replicas())
    }

    pub fn databases(&self) -> impl Iterator<Item = &Database> {
        self.state.database_by_id.values()
    }

    pub fn user_roles(&self) -> impl Iterator<Item = &Role> {
        self.state
            .roles_by_id
            .values()
            .filter(|role| role.is_user())
    }

    pub fn user_network_policies(&self) -> impl Iterator<Item = &NetworkPolicy> {
        self.state
            .network_policies_by_id
            .iter()
            .filter(|(id, _)| id.is_user())
            .map(|(_, policy)| policy)
    }

    pub fn system_privileges(&self) -> &PrivilegeMap {
        &self.state.system_privileges
    }

    pub fn default_privileges(
        &self,
    ) -> impl Iterator<
        Item = (
            &DefaultPrivilegeObject,
            impl Iterator<Item = &DefaultPrivilegeAclItem>,
        ),
    > {
        self.state.default_privileges.iter()
    }

    pub fn pack_item_update(&self, id: CatalogItemId, diff: Diff) -> Vec<BuiltinTableUpdate> {
        self.state
            .resolve_builtin_table_updates(self.state.pack_item_update(id, diff))
    }

    pub fn pack_storage_usage_update(
        &self,
        event: VersionedStorageUsage,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        self.state
            .resolve_builtin_table_update(self.state.pack_storage_usage_update(event, diff))
    }

    pub fn system_config(&self) -> &SystemVars {
        self.state.system_config()
    }

    pub fn system_config_mut(&mut self) -> &mut SystemVars {
        self.state.system_config_mut()
    }

    pub fn ensure_not_reserved_role(&self, role_id: &RoleId) -> Result<(), Error> {
        self.state.ensure_not_reserved_role(role_id)
    }

    pub fn ensure_grantable_role(&self, role_id: &RoleId) -> Result<(), Error> {
        self.state.ensure_grantable_role(role_id)
    }

    pub fn ensure_not_system_role(&self, role_id: &RoleId) -> Result<(), Error> {
        self.state.ensure_not_system_role(role_id)
    }

    pub fn ensure_not_predefined_role(&self, role_id: &RoleId) -> Result<(), Error> {
        self.state.ensure_not_predefined_role(role_id)
    }

    pub fn ensure_not_reserved_network_policy(
        &self,
        network_policy_id: &NetworkPolicyId,
    ) -> Result<(), Error> {
        self.state
            .ensure_not_reserved_network_policy(network_policy_id)
    }

    pub fn ensure_not_reserved_object(
        &self,
        object_id: &ObjectId,
        conn_id: &ConnectionId,
    ) -> Result<(), Error> {
        match object_id {
            ObjectId::Cluster(cluster_id) => {
                if cluster_id.is_system() {
                    let cluster = self.get_cluster(*cluster_id);
                    Err(Error::new(ErrorKind::ReadOnlyCluster(
                        cluster.name().to_string(),
                    )))
                } else {
                    Ok(())
                }
            }
            ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                if replica_id.is_system() {
                    let replica = self.get_cluster_replica(*cluster_id, *replica_id);
                    Err(Error::new(ErrorKind::ReadOnlyClusterReplica(
                        replica.name().to_string(),
                    )))
                } else {
                    Ok(())
                }
            }
            ObjectId::Database(database_id) => {
                if database_id.is_system() {
                    let database = self.get_database(database_id);
                    Err(Error::new(ErrorKind::ReadOnlyDatabase(
                        database.name().to_string(),
                    )))
                } else {
                    Ok(())
                }
            }
            ObjectId::Schema((database_spec, schema_spec)) => {
                if schema_spec.is_system() {
                    let schema = self.get_schema(database_spec, schema_spec, conn_id);
                    Err(Error::new(ErrorKind::ReadOnlySystemSchema(
                        schema.name().schema.clone(),
                    )))
                } else {
                    Ok(())
                }
            }
            ObjectId::Role(role_id) => self.ensure_not_reserved_role(role_id),
            ObjectId::Item(item_id) => {
                if item_id.is_system() {
                    let item = self.get_entry(item_id);
                    let name = self.resolve_full_name(item.name(), Some(conn_id));
                    Err(Error::new(ErrorKind::ReadOnlyItem(name.to_string())))
                } else {
                    Ok(())
                }
            }
            ObjectId::NetworkPolicy(network_policy_id) => {
                self.ensure_not_reserved_network_policy(network_policy_id)
            }
        }
    }

    /// Reconstructs a stored statement with catalog-item parsing features enabled.
    /// Temporary feature overrides are restored before returning.
    pub fn deserialize_plan_with_enable_for_item_parsing(
        &mut self,
        create_sql: &str,
        force_if_exists_skip: bool,
    ) -> Result<(Plan, ResolvedIds), CatalogError> {
        self.state
            .deserialize_plan_with_enable_for_item_parsing(create_sql, force_if_exists_skip)
            .map_err(Into::into)
    }

    pub fn expression_build_version(build_info: &mz_build_info::BuildInfo) -> semver::Version {
        crate::expr_cache::expression_build_version(build_info)
    }

    /// Durably prepares a new item's plan and returns the selection to commit with its DDL.
    /// Unprotected environments use the optional expression cache and return no selection.
    pub async fn prepare_item_plan(
        &self,
        id: GlobalId,
        local_mir: Option<OptimizedMirRelationExpr>,
        mut global_mir: DataflowDescription<OptimizedMirRelationExpr>,
        mut physical_plan: DataflowDescription<mz_compute_types::plan::LirRelationExpr>,
        dataflow_metainfos: DataflowMetainfo<Arc<OptimizerNotice>>,
        optimizer_features: OptimizerFeatures,
    ) -> Result<Vec<Op>, CatalogError> {
        // Make sure we're not caching the result of timestamp selection, as
        // it will almost certainly be wrong if we re-install the dataflow at
        // a later time.
        global_mir.as_of = None;
        global_mir.until = Default::default();
        physical_plan.as_of = None;
        physical_plan.until = Default::default();

        let mut local_exprs = Vec::new();
        if let Some(local_mir) = local_mir {
            local_exprs.push((
                id,
                LocalExpressions {
                    local_mir,
                    optimizer_features: optimizer_features.clone(),
                    item_version: RelationVersion::root(),
                },
            ));
        }
        let global = GlobalExpressions {
            global_mir,
            physical_plan,
            dataflow_metainfos,
            optimizer_features,
            item_version: RelationVersion::root(),
        };
        let selection = if self.state.catalog_read_protection_enabled() {
            let selection = self.write_plans(BTreeMap::from([(id, global)])).await?;
            self.update_expression_cache(local_exprs, Vec::new(), Default::default())
                .await;
            selection
        } else {
            self.update_expression_cache(local_exprs, vec![(id, global)], Default::default())
                .await;
            Vec::new()
        };
        Ok(selection)
    }

    /// Writes immutable candidates as one batch before selecting them in a catalog transaction.
    /// The transaction must validate their imports and predecessors against its final state.
    pub async fn write_plans(
        &self,
        plans: BTreeMap<GlobalId, GlobalExpressions>,
    ) -> Result<Vec<Op>, CatalogError> {
        let store = self.expr_cache_handle.as_ref().ok_or_else(|| {
            CatalogError::internal("write maintained plan", "expression store is not open")
        })?;
        let build_version =
            Self::expression_build_version(self.state.config().build_info).to_string();
        let mut entries = Vec::with_capacity(plans.len());
        let mut selections = Vec::with_capacity(plans.len());
        for (id, mut plan) in plans {
            // Installation and recovery select timestamps from current protected history.
            plan.global_mir.as_of = None;
            plan.global_mir.until = Default::default();
            plan.physical_plan.as_of = None;
            plan.physical_plan.until = Default::default();
            let expected_revision = self.state.written_plan(id, &build_version);
            let revision = Uuid::new_v4();
            let imports = plan.collection_imports().copied().collect();
            entries.push((id, revision, plan));
            selections.push(Op::SetWrittenPlan {
                id,
                build_version: build_version.clone(),
                expected_revision,
                revision: Some(revision),
                imports,
                replica_owner: None,
            });
        }
        store
            .write_plans(entries)
            .await
            .map_err(|error| CatalogError::internal("write maintained plan", error.to_string()))?;
        Ok(selections)
    }

    /// Reads specified immutable revisions for this build. Missing entries are omitted.
    pub async fn read_written_plans(
        &self,
        revisions: Vec<(GlobalId, Uuid)>,
    ) -> Result<BTreeMap<GlobalId, GlobalExpressions>, CatalogError> {
        let store = self.expr_cache_handle.as_ref().ok_or_else(|| {
            CatalogError::internal("read maintained plans", "expression store is not open")
        })?;
        Ok(store
            .read_plans(revisions)
            .await
            .map_err(|error| CatalogError::internal("read maintained plans", error.to_string()))?
            .into_iter()
            .map(|((id, _), plan)| (id, plan))
            .collect())
    }

    /// Reads this snapshot's selected plan for this build, independently of installation.
    /// A missing selection or immutable entry is not a request to replan.
    pub async fn selected_plan(
        &self,
        id: GlobalId,
    ) -> Result<Option<GlobalExpressions>, CatalogError> {
        let build = Self::expression_build_version(self.state.config().build_info).to_string();
        let Some(revision) = self.state.written_plan(id, &build) else {
            return Ok(None);
        };
        Ok(self
            .read_written_plans(vec![(id, revision)])
            .await?
            .remove(&id))
    }

    /// Returns a best-effort cached plan, whose compatibility the caller must validate.
    pub async fn cached_global_expressions(&self, id: GlobalId) -> Option<GlobalExpressions> {
        self.expr_cache_handle.as_ref()?.get_global(id).await
    }

    pub fn update_expression_cache<'a, 'b>(
        &'a self,
        new_local_expressions: Vec<(GlobalId, LocalExpressions)>,
        new_global_expressions: Vec<(GlobalId, GlobalExpressions)>,
        invalidate_ids: BTreeSet<GlobalId>,
    ) -> BoxFuture<'b, ()> {
        if let Some(expr_cache) = &self.expr_cache_handle {
            expr_cache
                .update(
                    new_local_expressions,
                    new_global_expressions,
                    invalidate_ids,
                )
                .boxed()
        } else {
            async {}.boxed()
        }
    }

    /// Classify an update against the catalog state before applying its batch.
    fn update_affects_planning(
        state: &CatalogState,
        update: &crate::memory::objects::StateUpdate,
    ) -> bool {
        use crate::memory::objects::{StateDiff, StateUpdateKind};
        match &update.kind {
            StateUpdateKind::CollectionCompactionBound(_)
            | StateUpdateKind::MaintainedReadRequirement(_)
            | StateUpdateKind::ClientIncarnation(_)
            | StateUpdateKind::ClientReadRequirement(_)
            | StateUpdateKind::UnfinalizedShard(_)
            | StateUpdateKind::AuditLog(_) => false,
            // Client release can retire storage metadata after SQL has dropped
            // the collection. That cleanup does not change planning, but live
            // mappings (including foreign temporary items) and additions do.
            StateUpdateKind::StorageCollectionMetadata(metadata)
                if update.diff == StateDiff::Retraction
                    && !state.contains_live_collection(&metadata.id) =>
            {
                false
            }
            _ => true,
        }
    }

    /// Apply this writer's unconsumed committed updates. Downstream owners must
    /// enact the returned implications even when their own transaction failed.
    /// Unapplicable committed state requires process recovery, never continuation
    /// with a stale or partially updated projection.
    pub async fn sync_to_current_updates(
        &mut self,
    ) -> Result<
        (
            Vec<BuiltinTableUpdate<&'static BuiltinTable>>,
            Vec<ParsedStateUpdate>,
        ),
        DurableError,
    > {
        let updates = match mz_ore::future::OreFutureExt::ore_catch_unwind(
            std::panic::AssertUnwindSafe(async {
                self.storage().await.sync_to_current_updates().await
            }),
        )
        .await
        {
            Ok(Ok(updates)) => updates,
            Ok(Err(
                error @ DurableError::Durable(crate::durable::DurableCatalogError::Fence(_)),
            )) => return Err(error),
            Ok(Err(error)) => {
                mz_ore::halt!("cannot decode committed catalog changes, restart required: {error}")
            }
            Err(payload) => {
                let cause = mz_ore::panic::downcast_panic_message(&*payload);
                mz_ore::halt!("cannot decode committed catalog changes, restart required: {cause}")
            }
        };
        let planning_changed = updates
            .iter()
            .any(|update| Self::update_affects_planning(&self.state, update));
        let (builtin_table_updates, catalog_updates) =
            mz_ore::future::OreFutureExt::ore_catch_unwind(std::panic::AssertUnwindSafe(
                self.state
                    .apply_updates(updates, &mut state::LocalExpressionCache::Closed),
            ))
            .await
            .unwrap_or_else(|payload| {
                let cause = mz_ore::panic::downcast_panic_message(&*payload);
                mz_ore::halt!("cannot apply committed catalog changes, restart required: {cause}")
            });
        if planning_changed {
            self.transient_revision += 1;
            self.shared_transient_revision
                .store(self.transient_revision, std::sync::atomic::Ordering::SeqCst);
        }
        Ok((builtin_table_updates, catalog_updates))
    }
}

pub fn is_reserved_name(name: &str) -> bool {
    BUILTIN_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

/// Role names that PostgreSQL reserves for role specifications in statements
/// like `GRANT ... TO CURRENT_USER` and `SET ROLE NONE`. A role with such a
/// name would be ambiguous there, so creating one is not allowed.
///
/// PostgreSQL rejects most of these at parse time, only when they appear as
/// unquoted keywords. Our parser does not track whether an identifier was
/// quoted, so we instead reject the names themselves. Only the lowercase
/// spellings are reserved, which is what the unquoted forms normalize to, so
/// quoted names like `"CURRENT_USER"` remain valid, as in PostgreSQL.
const RESERVED_ROLE_SPECIFICATION_NAMES: [&str; 5] = [
    "current_user",
    "current_role",
    "session_user",
    "user",
    "none",
];

pub fn is_reserved_role_name(name: &str) -> bool {
    is_reserved_name(name)
        || is_public_role(name)
        || RESERVED_ROLE_SPECIFICATION_NAMES.contains(&name)
}

pub fn is_public_role(name: &str) -> bool {
    name == &*PUBLIC_ROLE_NAME
}

pub fn catalog_type_to_audit_object_type(sql_type: SqlCatalogItemType) -> ObjectType {
    object_type_to_audit_object_type(sql_type.into())
}

pub fn comment_id_to_audit_object_type(id: CommentObjectId) -> ObjectType {
    match id {
        CommentObjectId::Table(_) => ObjectType::Table,
        CommentObjectId::View(_) => ObjectType::View,
        CommentObjectId::MaterializedView(_) => ObjectType::MaterializedView,
        CommentObjectId::Source(_) => ObjectType::Source,
        CommentObjectId::Sink(_) => ObjectType::Sink,
        // Unreachable by construction: `COMMENT ON METRIC SINK` is rejected at parse, so no
        // metric-sink comment id is ever built. The arm exists only for exhaustiveness.
        CommentObjectId::MetricSink(_) => ObjectType::MetricSink,
        CommentObjectId::Index(_) => ObjectType::Index,
        CommentObjectId::Func(_) => ObjectType::Func,
        CommentObjectId::Connection(_) => ObjectType::Connection,
        CommentObjectId::Type(_) => ObjectType::Type,
        CommentObjectId::Secret(_) => ObjectType::Secret,
        CommentObjectId::Role(_) => ObjectType::Role,
        CommentObjectId::Database(_) => ObjectType::Database,
        CommentObjectId::Schema(_) => ObjectType::Schema,
        CommentObjectId::Cluster(_) => ObjectType::Cluster,
        CommentObjectId::ClusterReplica(_) => ObjectType::ClusterReplica,
        CommentObjectId::NetworkPolicy(_) => ObjectType::NetworkPolicy,
    }
}

pub fn object_type_to_audit_object_type(object_type: mz_sql::catalog::ObjectType) -> ObjectType {
    system_object_type_to_audit_object_type(&SystemObjectType::Object(object_type))
}

pub fn system_object_type_to_audit_object_type(system_type: &SystemObjectType) -> ObjectType {
    match system_type {
        SystemObjectType::Object(object_type) => match object_type {
            mz_sql::catalog::ObjectType::Table => ObjectType::Table,
            mz_sql::catalog::ObjectType::View => ObjectType::View,
            mz_sql::catalog::ObjectType::MaterializedView => ObjectType::MaterializedView,
            mz_sql::catalog::ObjectType::Source => ObjectType::Source,
            mz_sql::catalog::ObjectType::Sink => ObjectType::Sink,
            mz_sql::catalog::ObjectType::MetricSink => ObjectType::MetricSink,
            mz_sql::catalog::ObjectType::Index => ObjectType::Index,
            mz_sql::catalog::ObjectType::Type => ObjectType::Type,
            mz_sql::catalog::ObjectType::Role => ObjectType::Role,
            mz_sql::catalog::ObjectType::Cluster => ObjectType::Cluster,
            mz_sql::catalog::ObjectType::ClusterReplica => ObjectType::ClusterReplica,
            mz_sql::catalog::ObjectType::Secret => ObjectType::Secret,
            mz_sql::catalog::ObjectType::Connection => ObjectType::Connection,
            mz_sql::catalog::ObjectType::Database => ObjectType::Database,
            mz_sql::catalog::ObjectType::Schema => ObjectType::Schema,
            mz_sql::catalog::ObjectType::Func => ObjectType::Func,
            mz_sql::catalog::ObjectType::NetworkPolicy => ObjectType::NetworkPolicy,
        },
        SystemObjectType::System => ObjectType::System,
    }
}

#[derive(Debug, Copy, Clone)]
pub enum UpdatePrivilegeVariant {
    Grant,
    Revoke,
}

impl From<UpdatePrivilegeVariant> for EventType {
    fn from(variant: UpdatePrivilegeVariant) -> Self {
        match variant {
            UpdatePrivilegeVariant::Grant => EventType::Grant,
            UpdatePrivilegeVariant::Revoke => EventType::Revoke,
        }
    }
}

impl CatalogStateView<'_> {
    fn resolve_item_name(
        &self,
        name: &PartialItemName,
    ) -> Result<&QualifiedItemName, SqlCatalogError> {
        self.resolve_item(name).map(|entry| entry.name())
    }

    fn resolve_function_name(
        &self,
        name: &PartialItemName,
    ) -> Result<&QualifiedItemName, SqlCatalogError> {
        self.resolve_function(name).map(|entry| entry.name())
    }

    fn resolve_type_name(
        &self,
        name: &PartialItemName,
    ) -> Result<&QualifiedItemName, SqlCatalogError> {
        self.resolve_type(name).map(|entry| entry.name())
    }
}

impl ExprHumanizer for CatalogStateView<'_> {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        let entry = self.state.try_get_entry_by_global_id(&id)?;
        Some(self.resolve_full_name(entry.name()).to_string())
    }

    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String> {
        let entry = self.state.try_get_entry_by_global_id(&id)?;
        Some(entry.name().item.clone())
    }

    fn humanize_id_parts(&self, id: GlobalId) -> Option<Vec<String>> {
        let entry = self.state.try_get_entry_by_global_id(&id)?;
        Some(self.resolve_full_name(entry.name()).into_parts())
    }

    fn humanize_sql_scalar_type(&self, typ: &SqlScalarType, postgres_compat: bool) -> String {
        use SqlScalarType::*;

        match typ {
            Array(t) => format!("{}[]", self.humanize_sql_scalar_type(t, postgres_compat)),
            List {
                custom_id: Some(item_id),
                ..
            }
            | Map {
                custom_id: Some(item_id),
                ..
            } => {
                let item = self.get_item(item_id);
                self.minimal_qualification(item.name()).to_string()
            }
            List { element_type, .. } => {
                format!(
                    "{} list",
                    self.humanize_sql_scalar_type(element_type, postgres_compat)
                )
            }
            Map { value_type, .. } => format!(
                "map[{}=>{}]",
                self.humanize_sql_scalar_type(&SqlScalarType::String, postgres_compat),
                self.humanize_sql_scalar_type(value_type, postgres_compat)
            ),
            Record {
                custom_id: Some(item_id),
                ..
            } => {
                let item = self.get_item(item_id);
                self.minimal_qualification(item.name()).to_string()
            }
            Record { fields, .. } => format!(
                "record({})",
                fields
                    .iter()
                    .map(|f| format!(
                        "{}: {}",
                        f.0,
                        self.humanize_sql_column_type(&f.1, postgres_compat)
                    ))
                    .join(",")
            ),
            PgLegacyChar => "\"char\"".into(),
            Char { length } if !postgres_compat => match length {
                None => "char".into(),
                Some(length) => format!("char({})", length.into_u32()),
            },
            VarChar { max_length } if !postgres_compat => match max_length {
                None => "varchar".into(),
                Some(length) => format!("varchar({})", length.into_u32()),
            },
            UInt16 => "uint2".into(),
            UInt32 => "uint4".into(),
            UInt64 => "uint8".into(),
            ty => {
                let pgrepr_type = mz_pgrepr::Type::from(ty);
                let pg_catalog_schema = SchemaSpecifier::Id(self.state.get_pg_catalog_schema_id());

                let res = if self
                    .effective_search_path(true)
                    .iter()
                    .any(|(_, schema)| schema == &pg_catalog_schema)
                {
                    pgrepr_type.name().to_string()
                } else {
                    // If PG_CATALOG_SCHEMA is not in search path, you need
                    // qualified object name to refer to type.
                    let name = QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: pg_catalog_schema,
                        },
                        item: pgrepr_type.name().to_string(),
                    };
                    self.resolve_full_name(&name).to_string()
                };
                res
            }
        }
    }

    fn column_names_for_id(&self, id: GlobalId) -> Option<Vec<String>> {
        let entry = self.state.try_get_entry_by_global_id(&id)?;

        match entry.index() {
            Some(index) => {
                let on_desc = self.state.try_get_desc_by_global_id(&index.on)?;
                let mut on_names = on_desc
                    .iter_names()
                    .map(|col_name| col_name.to_string())
                    .collect::<Vec<_>>();

                let (p, _) = mz_expr::permutation_for_arrangement(&index.keys, on_desc.arity());

                // Init ix_names with unknown column names. Unknown columns are
                // represented as an empty String and rendered as `#c` by the
                // Display::fmt implementation for HumanizedExpr<'a, usize, M>.
                let ix_arity = p.iter().map(|x| *x + 1).max().unwrap_or(0);
                let mut ix_names = vec![String::new(); ix_arity];

                // Apply the permutation by swapping on_names with ix_names.
                for (on_pos, ix_pos) in p.into_iter().enumerate() {
                    let on_name = on_names.get_mut(on_pos).expect("on_name");
                    let ix_name = ix_names.get_mut(ix_pos).expect("ix_name");
                    std::mem::swap(on_name, ix_name);
                }

                Some(ix_names) // Return the updated ix_names vector.
            }
            None => {
                let desc = self.state.try_get_desc_by_global_id(&id)?;
                let column_names = desc
                    .iter_names()
                    .map(|col_name| col_name.to_string())
                    .collect();

                Some(column_names)
            }
        }
    }

    fn humanize_column(&self, id: GlobalId, column: usize) -> Option<String> {
        let desc = self.state.try_get_desc_by_global_id(&id)?;
        Some(desc.get_name(column).to_string())
    }

    fn id_exists(&self, id: GlobalId) -> bool {
        self.state.entry_by_global_id.contains_key(&id)
    }
}

impl SessionCatalog for CatalogStateView<'_> {
    fn active_role_id(&self) -> &RoleId {
        &self.role_id
    }

    fn restrict_to_user_objects(&self) -> bool {
        self.restrict_to_user_objects
    }

    fn get_prepared_statement_desc(&self, _name: &str) -> Option<&StatementDesc> {
        None
    }

    fn get_portal_desc_unverified(&self, _portal_name: &str) -> Option<&StatementDesc> {
        None
    }

    fn active_database(&self) -> Option<&DatabaseId> {
        self.database.as_ref()
    }

    fn active_cluster(&self) -> &str {
        &self.cluster
    }

    fn search_path(&self) -> &[(ResolvedDatabaseSpecifier, SchemaSpecifier)] {
        &self.search_path
    }

    fn resolve_database(
        &self,
        database_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogDatabase, SqlCatalogError> {
        Ok(self.state.resolve_database(database_name)?)
    }

    fn get_database(&self, id: &DatabaseId) -> &dyn mz_sql::catalog::CatalogDatabase {
        self.state
            .database_by_id
            .get(id)
            .expect("database doesn't exist")
    }

    // `as` is ok to use to cast to a trait object.
    #[allow(clippy::as_conversions)]
    fn get_databases(&self) -> Vec<&dyn CatalogDatabase> {
        self.state
            .database_by_id
            .values()
            .map(|database| database as &dyn CatalogDatabase)
            .collect()
    }

    fn resolve_schema(
        &self,
        database_name: Option<&str>,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        Ok(self.state.resolve_schema(
            self.database.as_ref(),
            database_name,
            schema_name,
            &self.conn_id,
        )?)
    }

    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        Ok(self
            .state
            .resolve_schema_in_database(database_spec, schema_name, &self.conn_id)?)
    }

    fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> &dyn CatalogSchema {
        self.state
            .get_schema(database_spec, schema_spec, &self.conn_id)
    }

    // `as` is ok to use to cast to a trait object.
    #[allow(clippy::as_conversions)]
    fn get_schemas(&self) -> Vec<&dyn CatalogSchema> {
        self.get_databases()
            .into_iter()
            .flat_map(|database| database.schemas().into_iter())
            .chain(
                self.state
                    .ambient_schemas_by_id
                    .values()
                    .chain(self.state.temporary_namespaces.schemas())
                    .map(|schema| schema as &dyn CatalogSchema),
            )
            .collect()
    }

    fn get_mz_internal_schema_id(&self) -> SchemaId {
        self.state().get_mz_internal_schema_id()
    }

    fn get_mz_unsafe_schema_id(&self) -> SchemaId {
        self.state().get_mz_unsafe_schema_id()
    }

    fn is_system_schema_specifier(&self, schema: SchemaSpecifier) -> bool {
        self.state.is_system_schema_specifier(schema)
    }

    fn resolve_role(
        &self,
        role_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogRole, SqlCatalogError> {
        match self.state.try_get_role_by_name(role_name) {
            Some(role) => Ok(role),
            None => Err(SqlCatalogError::UnknownRole(role_name.into())),
        }
    }

    fn resolve_network_policy(
        &self,
        policy_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogNetworkPolicy, SqlCatalogError> {
        match self.state.try_get_network_policy_by_name(policy_name) {
            Some(policy) => Ok(policy),
            None => Err(SqlCatalogError::UnknownNetworkPolicy(policy_name.into())),
        }
    }

    fn try_get_role(&self, id: &RoleId) -> Option<&dyn CatalogRole> {
        Some(self.state.roles_by_id.get(id)?)
    }

    fn get_role(&self, id: &RoleId) -> &dyn mz_sql::catalog::CatalogRole {
        self.state.get_role(id)
    }

    fn get_roles(&self) -> Vec<&dyn CatalogRole> {
        // `as` is ok to use to cast to a trait object.
        #[allow(clippy::as_conversions)]
        self.state
            .roles_by_id
            .values()
            .map(|role| role as &dyn CatalogRole)
            .collect()
    }

    fn mz_system_role_id(&self) -> RoleId {
        MZ_SYSTEM_ROLE_ID
    }

    fn collect_role_membership(&self, id: &RoleId) -> BTreeSet<RoleId> {
        self.state.collect_role_membership(id)
    }

    fn get_network_policy(
        &self,
        id: &NetworkPolicyId,
    ) -> &dyn mz_sql::catalog::CatalogNetworkPolicy {
        self.state.get_network_policy(id)
    }

    fn get_network_policies(&self) -> Vec<&dyn mz_sql::catalog::CatalogNetworkPolicy> {
        // `as` is ok to use to cast to a trait object.
        #[allow(clippy::as_conversions)]
        self.state
            .network_policies_by_id
            .values()
            .map(|policy| policy as &dyn CatalogNetworkPolicy)
            .collect()
    }

    fn resolve_cluster(
        &self,
        cluster_name: Option<&str>,
    ) -> Result<&dyn mz_sql::catalog::CatalogCluster<'_>, SqlCatalogError> {
        Ok(self
            .state
            .resolve_cluster(cluster_name.unwrap_or_else(|| self.active_cluster()))?)
    }

    fn resolve_cluster_replica(
        &self,
        cluster_replica_name: &QualifiedReplica,
    ) -> Result<&dyn CatalogClusterReplica<'_>, SqlCatalogError> {
        Ok(self.state.resolve_cluster_replica(cluster_replica_name)?)
    }

    fn resolve_item(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        let r = self.state.resolve_entry(
            self.database.as_ref(),
            &self.effective_search_path(true),
            name,
            &self.conn_id,
        )?;
        if self.unresolvable_ids.contains(&r.id()) {
            Err(SqlCatalogError::UnknownItem(name.to_string()))
        } else {
            Ok(r)
        }
    }

    fn resolve_function(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        let r = self.state.resolve_function(
            self.database.as_ref(),
            &self.effective_search_path(false),
            name,
            &self.conn_id,
        )?;

        if self.unresolvable_ids.contains(&r.id()) {
            Err(SqlCatalogError::UnknownFunction {
                name: name.to_string(),
                alternative: None,
            })
        } else {
            Ok(r)
        }
    }

    fn resolve_type(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        let r = self.state.resolve_type(
            self.database.as_ref(),
            &self.effective_search_path(false),
            name,
            &self.conn_id,
        )?;

        if self.unresolvable_ids.contains(&r.id()) {
            Err(SqlCatalogError::UnknownType {
                name: name.to_string(),
            })
        } else {
            Ok(r)
        }
    }

    fn get_system_type(&self, name: &str) -> &dyn mz_sql::catalog::CatalogItem {
        self.state.get_system_type(name)
    }

    fn try_get_item(&self, id: &CatalogItemId) -> Option<&dyn mz_sql::catalog::CatalogItem> {
        Some(self.state.try_get_entry(id)?)
    }

    fn try_get_item_by_global_id(
        &self,
        id: &GlobalId,
    ) -> Option<Box<dyn mz_sql::catalog::CatalogCollectionItem>> {
        let entry = self.state.try_get_entry_by_global_id(id)?;
        let collections = match &entry.item {
            CatalogItem::Table(table) => Some(&table.collections),
            CatalogItem::MaterializedView(mv) => Some(&mv.collections),
            _ => None,
        };
        let entry = match collections {
            Some(collections) => {
                let (version, _gid) = collections
                    .iter()
                    .find(|(_version, gid)| *gid == id)
                    .expect("catalog out of sync, mismatched GlobalId");
                entry.at_version(RelationVersionSelector::Specific(*version))
            }
            _ => entry.at_version(RelationVersionSelector::Latest),
        };
        Some(entry)
    }

    fn get_item(&self, id: &CatalogItemId) -> &dyn mz_sql::catalog::CatalogItem {
        self.state.get_entry(id)
    }

    fn get_item_by_global_id(
        &self,
        id: &GlobalId,
    ) -> Box<dyn mz_sql::catalog::CatalogCollectionItem> {
        let entry = self.state.get_entry_by_global_id(id);
        let collections = match &entry.item {
            CatalogItem::Table(table) => Some(&table.collections),
            CatalogItem::MaterializedView(mv) => Some(&mv.collections),
            _ => None,
        };
        let entry = match collections {
            Some(collections) => {
                let (version, _gid) = collections
                    .iter()
                    .find(|(_version, gid)| *gid == id)
                    .expect("catalog out of sync, mismatched GlobalId");
                entry.at_version(RelationVersionSelector::Specific(*version))
            }
            _ => entry.at_version(RelationVersionSelector::Latest),
        };
        entry
    }

    fn get_items(&self) -> Vec<&dyn mz_sql::catalog::CatalogItem> {
        self.get_schemas()
            .into_iter()
            .flat_map(|schema| schema.item_ids())
            .map(|id| self.get_item(&id))
            .collect()
    }

    fn get_item_by_name(&self, name: &QualifiedItemName) -> Option<&dyn SqlCatalogItem> {
        self.state
            .get_item_by_name(name, &self.conn_id)
            .map(|item| convert::identity::<&dyn SqlCatalogItem>(item))
    }

    fn get_type_by_name(&self, name: &QualifiedItemName) -> Option<&dyn SqlCatalogItem> {
        self.state
            .get_type_by_name(name, &self.conn_id)
            .map(|item| convert::identity::<&dyn SqlCatalogItem>(item))
    }

    fn get_cluster(&self, id: ClusterId) -> &dyn mz_sql::catalog::CatalogCluster<'_> {
        &self.state.clusters_by_id[&id]
    }

    fn get_clusters(&self) -> Vec<&dyn mz_sql::catalog::CatalogCluster<'_>> {
        self.state
            .clusters_by_id
            .values()
            .map(|cluster| convert::identity::<&dyn mz_sql::catalog::CatalogCluster>(cluster))
            .collect()
    }

    fn get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &dyn mz_sql::catalog::CatalogClusterReplica<'_> {
        let cluster = self.get_cluster(cluster_id);
        cluster.replica(replica_id)
    }

    fn get_cluster_replicas(&self) -> Vec<&dyn mz_sql::catalog::CatalogClusterReplica<'_>> {
        self.get_clusters()
            .into_iter()
            .flat_map(|cluster| cluster.replicas().into_iter())
            .collect()
    }

    fn get_system_privileges(&self) -> &PrivilegeMap {
        &self.state.system_privileges
    }

    fn get_default_privileges(
        &self,
    ) -> Vec<(&DefaultPrivilegeObject, Vec<&DefaultPrivilegeAclItem>)> {
        self.state
            .default_privileges
            .iter()
            .map(|(object, acl_items)| (object, acl_items.collect()))
            .collect()
    }

    fn find_available_name(&self, name: QualifiedItemName) -> QualifiedItemName {
        self.state.find_available_name(name, &self.conn_id)
    }

    fn resolve_full_name(&self, name: &QualifiedItemName) -> FullItemName {
        self.state.resolve_full_name(name, Some(&self.conn_id))
    }

    fn resolve_full_schema_name(&self, name: &QualifiedSchemaName) -> FullSchemaName {
        self.state.resolve_full_schema_name(name)
    }

    fn resolve_item_id(&self, global_id: &GlobalId) -> CatalogItemId {
        self.state.get_entry_by_global_id(global_id).id()
    }

    fn resolve_global_id(
        &self,
        item_id: &CatalogItemId,
        version: RelationVersionSelector,
    ) -> GlobalId {
        self.state
            .get_entry(item_id)
            .at_version(version)
            .global_id()
    }

    fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        self.state.config()
    }

    fn now(&self) -> EpochMillis {
        (self.state.config().now)()
    }

    fn aws_privatelink_availability_zones(&self) -> Option<BTreeSet<String>> {
        self.state.aws_privatelink_availability_zones.clone()
    }

    fn system_vars(&self) -> &SystemVars {
        &self.state.system_configuration
    }

    fn system_vars_mut(&mut self) -> &mut SystemVars {
        Arc::make_mut(&mut self.state.to_mut().system_configuration)
    }

    fn get_owner_id(&self, id: &ObjectId) -> Option<RoleId> {
        self.state().get_owner_id(id, self.conn_id())
    }

    fn get_privileges(&self, id: &SystemObjectId) -> Option<&PrivilegeMap> {
        match id {
            SystemObjectId::System => Some(&self.state.system_privileges),
            SystemObjectId::Object(ObjectId::Cluster(id)) => {
                Some(self.get_cluster(*id).privileges())
            }
            SystemObjectId::Object(ObjectId::Database(id)) => {
                Some(self.get_database(id).privileges())
            }
            SystemObjectId::Object(ObjectId::Schema((database_spec, schema_spec))) => {
                // For temporary schemas that haven't been created yet (lazy creation),
                // we return None - the RBAC check will need to handle this case.
                self.state
                    .try_get_schema(database_spec, schema_spec, &self.conn_id)
                    .map(|schema| schema.privileges())
            }
            SystemObjectId::Object(ObjectId::Item(id)) => Some(self.get_item(id).privileges()),
            SystemObjectId::Object(ObjectId::NetworkPolicy(id)) => {
                Some(self.get_network_policy(id).privileges())
            }
            SystemObjectId::Object(ObjectId::ClusterReplica(_))
            | SystemObjectId::Object(ObjectId::Role(_)) => None,
        }
    }

    fn object_dependents(&self, ids: &Vec<ObjectId>) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state.object_dependents(ids, &self.conn_id, &mut seen)
    }

    fn item_dependents(&self, id: CatalogItemId) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state.item_dependents(id, &mut seen)
    }

    fn all_object_privileges(&self, object_type: mz_sql::catalog::SystemObjectType) -> AclMode {
        rbac::all_object_privileges(object_type)
    }

    fn get_object_type(&self, object_id: &ObjectId) -> mz_sql::catalog::ObjectType {
        self.state.get_object_type(object_id)
    }

    fn get_system_object_type(&self, id: &SystemObjectId) -> mz_sql::catalog::SystemObjectType {
        self.state.get_system_object_type(id)
    }

    /// Returns a [`PartialItemName`] with the minimum amount of qualifiers to unambiguously resolve
    /// the object.
    ///
    /// Warning: This is broken for temporary objects. Don't use this function for serious stuff,
    /// i.e., don't expect that what you get back is a thing you can resolve. Current usages are
    /// only for error msgs and other humanizations.
    fn minimal_qualification(&self, qualified_name: &QualifiedItemName) -> PartialItemName {
        if qualified_name.qualifiers.schema_spec.is_temporary() {
            // All bets are off. Just give up and return the qualified name as is.
            // TODO: Figure out what's going on with temporary objects.

            // See e.g. `temporary_objects.slt` fail if you comment this out, which has the repro
            // from https://github.com/MaterializeInc/database-issues/issues/9973#issuecomment-3646382143
            // There is also https://github.com/MaterializeInc/database-issues/issues/9974, for
            // which we don't have a simple repro.
            return qualified_name.item.clone().into();
        }

        let database_id = match &qualified_name.qualifiers.database_spec {
            ResolvedDatabaseSpecifier::Ambient => None,
            ResolvedDatabaseSpecifier::Id(id)
                if self.database.is_some() && self.database == Some(*id) =>
            {
                None
            }
            ResolvedDatabaseSpecifier::Id(id) => Some(id.clone()),
        };

        let schema_spec = if database_id.is_none()
            && self.resolve_item_name(&PartialItemName {
                database: None,
                schema: None,
                item: qualified_name.item.clone(),
            }) == Ok(qualified_name)
            || self.resolve_function_name(&PartialItemName {
                database: None,
                schema: None,
                item: qualified_name.item.clone(),
            }) == Ok(qualified_name)
            || self.resolve_type_name(&PartialItemName {
                database: None,
                schema: None,
                item: qualified_name.item.clone(),
            }) == Ok(qualified_name)
        {
            None
        } else {
            // If `search_path` does not contain `full_name.schema`, the
            // `PartialName` must contain it.
            Some(qualified_name.qualifiers.schema_spec.clone())
        };

        let res = PartialItemName {
            database: database_id.map(|id| self.get_database(&id).name().to_string()),
            schema: schema_spec.map(|spec| {
                self.get_schema(&qualified_name.qualifiers.database_spec, &spec)
                    .name()
                    .schema
                    .clone()
            }),
            item: qualified_name.item.clone(),
        };
        assert!(
            self.resolve_item_name(&res) == Ok(qualified_name)
                || self.resolve_function_name(&res) == Ok(qualified_name)
                || self.resolve_type_name(&res) == Ok(qualified_name)
        );
        res
    }

    fn add_notice(&self, _notice: PlanNotice) {}

    fn get_item_comments(&self, id: &CatalogItemId) -> Option<&BTreeMap<Option<usize>, String>> {
        let comment_id = self.state.get_comment_id(ObjectId::Item(*id));
        self.state.comments.get_object_comments(comment_id)
    }

    fn is_cluster_size_cc(&self, size: &str) -> bool {
        self.state
            .cluster_replica_sizes
            .0
            .get(size)
            .map_or(false, |a| a.is_cc)
    }
}

#[cfg(test)]
mod tests;
