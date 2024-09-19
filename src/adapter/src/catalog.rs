// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO(jkosh44) Move to mz_catalog crate.

//! Persistent metadata storage for the coordinator.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::convert;
use std::sync::Arc;

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
use mz_catalog::builtin::{
    BUILTIN_PREFIXES, BuiltinCluster, BuiltinLog, BuiltinSource, BuiltinTable,
    MZ_CATALOG_SERVER_CLUSTER,
};
use mz_catalog::config::{BuiltinItemMigrationConfig, ClusterReplicaSizeMap, Config, StateConfig};
#[cfg(test)]
use mz_catalog::durable::CatalogError;
use mz_catalog::durable::{
    BootstrapArgs, DurableCatalogState, TestCatalogStateBuilder, test_bootstrap_args,
};
use mz_catalog::expr_cache::{ExpressionCacheHandle, GlobalExpressions, LocalExpressions};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogCollectionEntry, CatalogEntry, CatalogItem, Cluster, ClusterReplica, Database,
    NetworkPolicy, Role, RoleAuth, Schema,
};
use mz_compute_types::dataflows::DataflowDescription;
use mz_controller::clusters::ReplicaLocation;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::OptimizedMirRelationExpr;
use mz_license_keys::ValidatedLicenseKey;
use mz_ore::collections::HashSet;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn, SYSTEM_TIME};
use mz_ore::result::ResultExt as _;
use mz_ore::{soft_assert_eq_or_log, soft_assert_or_log};
use mz_persist_client::PersistClient;
use mz_repr::adt::mz_acl_item::{AclMode, PrivilegeMap};
use mz_repr::explain::ExprHumanizer;
use mz_repr::namespaces::MZ_TEMP_SCHEMA;
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationVersionSelector, SqlScalarType};
use mz_secrets::InMemorySecretsController;
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogDatabase, CatalogError as SqlCatalogError,
    CatalogItem as SqlCatalogItem, CatalogItemType as SqlCatalogItemType, CatalogItemType,
    CatalogNetworkPolicy, CatalogRole, CatalogSchema, DefaultPrivilegeAclItem,
    DefaultPrivilegeObject, EnvironmentId, SessionCatalog, SystemObjectType,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, FullItemName, FullSchemaName, ItemQualifiers, ObjectId,
    PUBLIC_ROLE_NAME, PartialItemName, QualifiedItemName, QualifiedSchemaName,
    ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::{Plan, PlanNotice, StatementDesc};
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::{MZ_SYSTEM_ROLE_ID, SUPPORT_USER, SYSTEM_USER};
use mz_sql::session::vars::SystemVars;
use mz_sql_parser::ast::QualifiedReplica;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::connections::inline::{ConnectionResolver, InlinedConnection};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;
use smallvec::SmallVec;
use tokio::sync::MutexGuard;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;
use uuid::Uuid;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
pub use crate::catalog::builtin_table_updates::BuiltinTableUpdate;
pub use crate::catalog::open::{InitializeStateResult, OpenCatalogResult};
pub use crate::catalog::state::CatalogState;
pub use crate::catalog::transact::{
    DropObjectInfo, Op, ReplicaCreateDropReason, TransactionResult,
};
use crate::command::CatalogDump;
use crate::coord::TargetCluster;
#[cfg(test)]
use crate::coord::controller_commands::parsed_state_updates::ParsedStateUpdate;
use crate::session::{Portal, PreparedStatement, Session};
use crate::util::ResultExt;
use crate::{AdapterError, AdapterNotice, ExecuteResponse};

mod builtin_table_updates;
pub(crate) mod consistency;
mod migrate;

mod apply;
mod open;
mod state;
mod timeline;
mod transact;

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
    plans: CatalogPlans,
    expr_cache_handle: Option<ExpressionCacheHandle>,
    storage: Arc<tokio::sync::Mutex<Box<dyn mz_catalog::durable::DurableCatalogState>>>,
    transient_revision: u64,
}

// Implement our own Clone because derive can't unless S is Clone, which it's
// not (hence the Arc).
impl Clone for Catalog {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            plans: self.plans.clone(),
            expr_cache_handle: self.expr_cache_handle.clone(),
            storage: Arc::clone(&self.storage),
            transient_revision: self.transient_revision,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct CatalogPlans {
    optimized_plan_by_id: BTreeMap<GlobalId, Arc<DataflowDescription<OptimizedMirRelationExpr>>>,
    physical_plan_by_id: BTreeMap<GlobalId, Arc<DataflowDescription<mz_compute_types::plan::Plan>>>,
    dataflow_metainfos: BTreeMap<GlobalId, DataflowMetainfo<Arc<OptimizerNotice>>>,
    notices_by_dep_id: BTreeMap<GlobalId, SmallVec<[Arc<OptimizerNotice>; 4]>>,
}

impl Catalog {
    /// Set the optimized plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn set_optimized_plan(
        &mut self,
        id: GlobalId,
        plan: DataflowDescription<OptimizedMirRelationExpr>,
    ) {
        self.plans.optimized_plan_by_id.insert(id, plan.into());
    }

    /// Set the physical plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn set_physical_plan(
        &mut self,
        id: GlobalId,
        plan: DataflowDescription<mz_compute_types::plan::Plan>,
    ) {
        self.plans.physical_plan_by_id.insert(id, plan.into());
    }

    /// Try to get the optimized plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn try_get_optimized_plan(
        &self,
        id: &GlobalId,
    ) -> Option<&DataflowDescription<OptimizedMirRelationExpr>> {
        self.plans.optimized_plan_by_id.get(id).map(AsRef::as_ref)
    }

    /// Try to get the physical plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn try_get_physical_plan(
        &self,
        id: &GlobalId,
    ) -> Option<&DataflowDescription<mz_compute_types::plan::Plan>> {
        self.plans.physical_plan_by_id.get(id).map(AsRef::as_ref)
    }

    /// Set the `DataflowMetainfo` for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn set_dataflow_metainfo(
        &mut self,
        id: GlobalId,
        metainfo: DataflowMetainfo<Arc<OptimizerNotice>>,
    ) {
        // Add entries to the `notices_by_dep_id` lookup map.
        for notice in metainfo.optimizer_notices.iter() {
            for dep_id in notice.dependencies.iter() {
                let entry = self.plans.notices_by_dep_id.entry(*dep_id).or_default();
                entry.push(Arc::clone(notice))
            }
            if let Some(item_id) = notice.item_id {
                soft_assert_eq_or_log!(
                    item_id,
                    id,
                    "notice.item_id should match the id for whom we are saving the notice"
                );
            }
        }
        // Add the dataflow with the scoped entries.
        self.plans.dataflow_metainfos.insert(id, metainfo);
    }

    /// Try to get the `DataflowMetainfo` for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn try_get_dataflow_metainfo(
        &self,
        id: &GlobalId,
    ) -> Option<&DataflowMetainfo<Arc<OptimizerNotice>>> {
        self.plans.dataflow_metainfos.get(id)
    }

    /// Drop all optimized and physical plans and `DataflowMetainfo`s for the
    /// item identified by `id`.
    ///
    /// Ignore requests for non-existing plans or `DataflowMetainfo`s.
    ///
    /// Return a set containing all dropped notices. Note that if for some
    /// reason we end up with two identical notices being dropped by the same
    /// call, the result will contain only one instance of that notice.
    #[mz_ore::instrument(level = "trace")]
    pub fn drop_plans_and_metainfos(
        &mut self,
        drop_ids: &BTreeSet<GlobalId>,
    ) -> BTreeSet<Arc<OptimizerNotice>> {
        // Collect dropped notices in this set.
        let mut dropped_notices = BTreeSet::new();

        // Remove plans and metainfo.optimizer_notices entries.
        for id in drop_ids {
            self.plans.optimized_plan_by_id.remove(id);
            self.plans.physical_plan_by_id.remove(id);
            if let Some(mut metainfo) = self.plans.dataflow_metainfos.remove(id) {
                soft_assert_or_log!(
                    metainfo.optimizer_notices.iter().all_unique(),
                    "should have been pushed there by `push_optimizer_notice_dedup`"
                );
                for n in metainfo.optimizer_notices.drain(..) {
                    // Remove the corresponding notices_by_dep_id entries.
                    for dep_id in n.dependencies.iter() {
                        if let Some(notices) = self.plans.notices_by_dep_id.get_mut(dep_id) {
                            soft_assert_or_log!(
                                notices.iter().any(|x| &n == x),
                                "corrupt notices_by_dep_id"
                            );
                            notices.retain(|x| &n != x)
                        }
                    }
                    dropped_notices.insert(n);
                }
            }
        }

        // Remove notices_by_dep_id entries.
        for id in drop_ids {
            if let Some(mut notices) = self.plans.notices_by_dep_id.remove(id) {
                for n in notices.drain(..) {
                    // Remove the corresponding metainfo.optimizer_notices entries.
                    if let Some(item_id) = n.item_id.as_ref() {
                        if let Some(metainfo) = self.plans.dataflow_metainfos.get_mut(item_id) {
                            metainfo.optimizer_notices.iter().for_each(|n2| {
                                if let Some(item_id_2) = n2.item_id {
                                    soft_assert_eq_or_log!(item_id_2, *item_id, "a notice's item_id should match the id for whom we have saved the notice");
                                }
                            });
                            metainfo.optimizer_notices.retain(|x| &n != x);
                        }
                    }
                    dropped_notices.insert(n);
                }
            }
        }

        // Collect dependency ids not in drop_ids with at least one dropped
        // notice.
        let mut todo_dep_ids = BTreeSet::new();
        for notice in dropped_notices.iter() {
            for dep_id in notice.dependencies.iter() {
                if !drop_ids.contains(dep_id) {
                    todo_dep_ids.insert(*dep_id);
                }
            }
        }
        // Remove notices in `dropped_notices` for all `notices_by_dep_id`
        // entries in `todo_dep_ids`.
        for id in todo_dep_ids {
            if let Some(notices) = self.plans.notices_by_dep_id.get_mut(&id) {
                notices.retain(|n| !dropped_notices.contains(n))
            }
        }

        if dropped_notices.iter().any(|n| Arc::strong_count(n) != 1) {
            use mz_ore::str::{bracketed, separated};
            let bad_notices = dropped_notices.iter().filter(|n| Arc::strong_count(n) != 1);
            let bad_notices = bad_notices.map(|n| {
                // Try to find where the bad reference is.
                // Maybe in `dataflow_metainfos`?
                let mut dataflow_metainfo_occurrences = Vec::new();
                for (id, meta_info) in self.plans.dataflow_metainfos.iter() {
                    if meta_info.optimizer_notices.contains(n) {
                        dataflow_metainfo_occurrences.push(id);
                    }
                }
                // Or `notices_by_dep_id`?
                let mut notices_by_dep_id_occurrences = Vec::new();
                for (id, notices) in self.plans.notices_by_dep_id.iter() {
                    if notices.iter().contains(n) {
                        notices_by_dep_id_occurrences.push(id);
                    }
                }
                format!(
                    "(id = {}, kind = {:?}, deps = {:?}, strong_count = {}, \
                    dataflow_metainfo_occurrences = {:?}, notices_by_dep_id_occurrences = {:?})",
                    n.id,
                    n.kind,
                    n.dependencies,
                    Arc::strong_count(n),
                    dataflow_metainfo_occurrences,
                    notices_by_dep_id_occurrences
                )
            });
            let bad_notices = bracketed("{", "}", separated(", ", bad_notices));
            error!(
                "all dropped_notices entries should have `Arc::strong_count(_) == 1`; \
                 bad_notices = {bad_notices}; \
                 drop_ids = {drop_ids:?}"
            );
        }

        dropped_notices
    }

    /// Return a set of [`GlobalId`]s for items that need to have their cache entries invalidated
    /// as a result of creating new indexes on the items in `ons`.
    ///
    /// When creating and inserting a new index, we need to invalidate some entries that may
    /// optimize to new expressions. When creating index `i` on object `o`, we need to invalidate
    /// the following objects:
    ///
    ///   - `o`.
    ///   - All compute objects that depend directly on `o`.
    ///   - All compute objects that would directly depend on `o`, if all views were inlined.
    pub(crate) fn invalidate_for_index(
        &self,
        ons: impl Iterator<Item = GlobalId>,
    ) -> BTreeSet<GlobalId> {
        let mut dependencies = BTreeSet::new();
        let mut queue = VecDeque::new();
        let mut seen = HashSet::new();
        for on in ons {
            let entry = self.get_entry_by_global_id(&on);
            dependencies.insert(on);
            seen.insert(entry.id);
            let uses = entry.uses();
            queue.extend(uses.clone());
        }

        while let Some(cur) = queue.pop_front() {
            let entry = self.get_entry(&cur);
            if seen.insert(cur) {
                let global_ids = entry.global_ids();
                match entry.item_type() {
                    CatalogItemType::Table
                    | CatalogItemType::Source
                    | CatalogItemType::MaterializedView
                    | CatalogItemType::Sink
                    | CatalogItemType::Index
                    | CatalogItemType::Type
                    | CatalogItemType::Func
                    | CatalogItemType::Secret
                    | CatalogItemType::Connection
                    | CatalogItemType::ContinualTask => {
                        dependencies.extend(global_ids);
                    }
                    CatalogItemType::View => {
                        dependencies.extend(global_ids);
                        queue.extend(entry.uses());
                    }
                }
            }
        }
        dependencies
    }
}

#[derive(Debug)]
pub struct ConnCatalog<'a> {
    state: Cow<'a, CatalogState>,
    /// Because we don't have any way of removing items from the catalog
    /// temporarily, we allow the ConnCatalog to pretend that a set of items
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
    prepared_statements: Option<&'a BTreeMap<String, PreparedStatement>>,
    portals: Option<&'a BTreeMap<String, Portal>>,
    notices_tx: UnboundedSender<AdapterNotice>,
}

impl ConnCatalog<'_> {
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

impl ConnectionResolver for ConnCatalog<'_> {
    fn resolve_connection(
        &self,
        id: CatalogItemId,
    ) -> mz_storage_types::connections::Connection<InlinedConnection> {
        self.state().resolve_connection(id)
    }
}

impl Catalog {
    /// Returns the catalog's transient revision, which starts at 1 and is
    /// incremented on every change. This is not persisted to disk, and will
    /// restart on every load.
    pub fn transient_revision(&self) -> u64 {
        self.transient_revision
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
            .expect("can open durable catalog")
            .0;
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
        let environment_id = None;
        let openable_storage = TestCatalogStateBuilder::new(persist_client.clone())
            .with_organization_id(organization_id)
            .with_default_deploy_generation()
            .build()
            .await?;
        let storage = openable_storage.open(now().into(), bootstrap_args).await?.0;
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
        let environment_id = None;
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
        )
        .await
    }

    async fn open_debug_catalog_inner(
        persist_client: PersistClient,
        storage: Box<dyn DurableCatalogState>,
        now: NowFn,
        environment_id: Option<EnvironmentId>,
        build_info: &'static BuildInfo,
        system_parameter_defaults: BTreeMap<String, String>,
        bootstrap_args: &BootstrapArgs,
        enable_expression_cache_override: Option<bool>,
    ) -> Result<Catalog, anyhow::Error> {
        let metrics_registry = &MetricsRegistry::new();
        let secrets_reader = Arc::new(InMemorySecretsController::new());
        // Used as a lower boundary of the boot_ts, but it's ok to use now() for
        // debugging/testing.
        let previous_ts = now().into();
        let replica_size = &bootstrap_args.default_cluster_replica_size;
        let read_only = false;

        let OpenCatalogResult {
            catalog,
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
                deploy_generation: 0,
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
                aws_principal_context: None,
                aws_privatelink_availability_zones: None,
                http_host_name: None,
                connection_context: ConnectionContext::for_tests(secrets_reader),
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

    pub fn for_session<'a>(&'a self, session: &'a Session) -> ConnCatalog<'a> {
        self.state.for_session(session)
    }

    pub fn for_sessionless_user(&self, role_id: RoleId) -> ConnCatalog<'_> {
        self.state.for_sessionless_user(role_id)
    }

    pub fn for_system_session(&self) -> ConnCatalog<'_> {
        self.state.for_system_session()
    }

    async fn storage<'a>(
        &'a self,
    ) -> MutexGuard<'a, Box<dyn mz_catalog::durable::DurableCatalogState>> {
        self.storage.lock().await
    }

    pub async fn current_upper(&self) -> mz_repr::Timestamp {
        self.storage().await.current_upper().await
    }

    pub async fn allocate_user_id(
        &self,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<(CatalogItemId, GlobalId), Error> {
        self.storage()
            .await
            .allocate_user_id(commit_ts)
            .await
            .maybe_terminate("allocating user ids")
            .err_into()
    }

    /// Allocate `amount` many user IDs. See [`DurableCatalogState::allocate_user_ids`].
    pub async fn allocate_user_ids(
        &self,
        amount: u64,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<Vec<(CatalogItemId, GlobalId)>, Error> {
        self.storage()
            .await
            .allocate_user_ids(amount, commit_ts)
            .await
            .maybe_terminate("allocating user ids")
            .err_into()
    }

    pub async fn allocate_user_id_for_test(&self) -> Result<(CatalogItemId, GlobalId), Error> {
        let commit_ts = self.storage().await.current_upper().await;
        self.allocate_user_id(commit_ts).await
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

    pub async fn allocate_user_cluster_id(
        &self,
        commit_ts: mz_repr::Timestamp,
    ) -> Result<ClusterId, Error> {
        self.storage()
            .await
            .allocate_user_cluster_id(commit_ts)
            .await
            .maybe_terminate("allocating user cluster ids")
            .err_into()
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
        session: &Session,
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

    /// Resolves a [`Cluster`] for a TargetCluster.
    pub fn resolve_target_cluster(
        &self,
        target_cluster: TargetCluster,
        session: &Session,
    ) -> Result<&Cluster, AdapterError> {
        match target_cluster {
            TargetCluster::CatalogServer => {
                Ok(self.resolve_builtin_cluster(&MZ_CATALOG_SERVER_CLUSTER))
            }
            TargetCluster::Active => self.active_cluster(session),
            TargetCluster::Transaction(cluster_id) => self
                .try_get_cluster(cluster_id)
                .ok_or(AdapterError::ConcurrentClusterDrop),
        }
    }

    pub fn active_cluster(&self, session: &Session) -> Result<&Cluster, AdapterError> {
        // TODO(benesch): this check here is not sufficiently protective. It'd
        // be very easy for a code path to accidentally avoid this check by
        // calling `resolve_cluster(session.vars().cluster())`.
        if session.user().name != SYSTEM_USER.name
            && session.user().name != SUPPORT_USER.name
            && session.vars().cluster() == SYSTEM_USER.name
        {
            coord_bail!(
                "system cluster '{}' cannot execute user queries",
                SYSTEM_USER.name
            );
        }
        let cluster = self.resolve_cluster(session.vars().cluster())?;
        Ok(cluster)
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

    /// Creates a new schema in the `Catalog` for temporary items
    /// indicated by the TEMPORARY or TEMP keywords.
    pub fn create_temporary_schema(
        &mut self,
        conn_id: &ConnectionId,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        self.state.create_temporary_schema(conn_id, owner_id)
    }

    fn item_exists_in_temp_schemas(&self, conn_id: &ConnectionId, item_name: &str) -> bool {
        self.state.temporary_schemas[conn_id]
            .items
            .contains_key(item_name)
    }

    /// Drops schema for connection if it exists. Returns an error if it exists and has items.
    /// Returns Ok if conn_id's temp schema does not exist.
    pub fn drop_temporary_schema(&mut self, conn_id: &ConnectionId) -> Result<(), Error> {
        let Some(schema) = self.state.temporary_schemas.remove(conn_id) else {
            return Ok(());
        };
        if !schema.items.is_empty() {
            return Err(Error::new(ErrorKind::SchemaNotEmpty(MZ_TEMP_SCHEMA.into())));
        }
        Ok(())
    }

    pub(crate) fn object_dependents(
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
        location: mz_catalog::durable::ReplicaLocation,
        allowed_sizes: &Vec<String>,
        allowed_availability_zones: Option<&[String]>,
    ) -> Result<ReplicaLocation, Error> {
        self.state
            .concretize_replica_location(location, allowed_sizes, allowed_availability_zones)
    }

    pub(crate) fn ensure_valid_replica_size(
        &self,
        allowed_sizes: &[String],
        size: &String,
    ) -> Result<(), Error> {
        self.state.ensure_valid_replica_size(allowed_sizes, size)
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

    #[mz_ore::instrument(level = "debug")]
    pub async fn confirm_leadership(&self) -> Result<(), AdapterError> {
        Ok(self.storage().await.confirm_leadership().await?)
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
    pub fn dump(&self) -> Result<CatalogDump, Error> {
        Ok(CatalogDump::new(self.state.dump(None)?))
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

    pub fn user_continual_tasks(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_continual_task() && entry.id().is_user())
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

    /// See [`CatalogState::deserialize_plan_with_enable_for_item_parsing`].
    pub(crate) fn deserialize_plan_with_enable_for_item_parsing(
        &mut self,
        create_sql: &str,
        force_if_exists_skip: bool,
    ) -> Result<(Plan, ResolvedIds), AdapterError> {
        self.state
            .deserialize_plan_with_enable_for_item_parsing(create_sql, force_if_exists_skip)
    }

    pub(crate) fn update_expression_cache<'a, 'b>(
        &'a self,
        new_local_expressions: Vec<(GlobalId, LocalExpressions)>,
        new_global_expressions: Vec<(GlobalId, GlobalExpressions)>,
    ) -> BoxFuture<'b, ()> {
        if let Some(expr_cache) = &self.expr_cache_handle {
            let ons = new_local_expressions
                .iter()
                .map(|(id, _)| id)
                .chain(new_global_expressions.iter().map(|(id, _)| id))
                .map(|id| self.get_entry_by_global_id(id))
                .filter_map(|entry| entry.index().map(|index| index.on));
            let invalidate_ids = self.invalidate_for_index(ons);
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

    /// Listen for and apply all unconsumed updates to the durable catalog state.
    // TODO(jkosh44) When this method is actually used outside of a test we can remove the
    // `#[cfg(test)]` annotation.
    #[cfg(test)]
    async fn sync_to_current_updates(
        &mut self,
    ) -> Result<
        (
            Vec<BuiltinTableUpdate<&'static BuiltinTable>>,
            Vec<ParsedStateUpdate>,
        ),
        CatalogError,
    > {
        let updates = self.storage().await.sync_to_current_updates().await?;
        let (builtin_table_updates, controller_state_updates) =
            self.state.apply_updates(updates)?;
        Ok((builtin_table_updates, controller_state_updates))
    }
}

pub fn is_reserved_name(name: &str) -> bool {
    BUILTIN_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

pub fn is_reserved_role_name(name: &str) -> bool {
    is_reserved_name(name) || is_public_role(name)
}

pub fn is_public_role(name: &str) -> bool {
    name == &*PUBLIC_ROLE_NAME
}

pub(crate) fn catalog_type_to_audit_object_type(sql_type: SqlCatalogItemType) -> ObjectType {
    object_type_to_audit_object_type(sql_type.into())
}

pub(crate) fn comment_id_to_audit_object_type(id: CommentObjectId) -> ObjectType {
    match id {
        CommentObjectId::Table(_) => ObjectType::Table,
        CommentObjectId::View(_) => ObjectType::View,
        CommentObjectId::MaterializedView(_) => ObjectType::MaterializedView,
        CommentObjectId::Source(_) => ObjectType::Source,
        CommentObjectId::Sink(_) => ObjectType::Sink,
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
        CommentObjectId::ContinualTask(_) => ObjectType::ContinualTask,
        CommentObjectId::NetworkPolicy(_) => ObjectType::NetworkPolicy,
    }
}

pub(crate) fn object_type_to_audit_object_type(
    object_type: mz_sql::catalog::ObjectType,
) -> ObjectType {
    system_object_type_to_audit_object_type(&SystemObjectType::Object(object_type))
}

pub(crate) fn system_object_type_to_audit_object_type(
    system_type: &SystemObjectType,
) -> ObjectType {
    match system_type {
        SystemObjectType::Object(object_type) => match object_type {
            mz_sql::catalog::ObjectType::Table => ObjectType::Table,
            mz_sql::catalog::ObjectType::View => ObjectType::View,
            mz_sql::catalog::ObjectType::MaterializedView => ObjectType::MaterializedView,
            mz_sql::catalog::ObjectType::Source => ObjectType::Source,
            mz_sql::catalog::ObjectType::Sink => ObjectType::Sink,
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
            mz_sql::catalog::ObjectType::ContinualTask => ObjectType::ContinualTask,
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

impl From<UpdatePrivilegeVariant> for ExecuteResponse {
    fn from(variant: UpdatePrivilegeVariant) -> Self {
        match variant {
            UpdatePrivilegeVariant::Grant => ExecuteResponse::GrantedPrivilege,
            UpdatePrivilegeVariant::Revoke => ExecuteResponse::RevokedPrivilege,
        }
    }
}

impl From<UpdatePrivilegeVariant> for EventType {
    fn from(variant: UpdatePrivilegeVariant) -> Self {
        match variant {
            UpdatePrivilegeVariant::Grant => EventType::Grant,
            UpdatePrivilegeVariant::Revoke => EventType::Revoke,
        }
    }
}

impl ConnCatalog<'_> {
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

impl ExprHumanizer for ConnCatalog<'_> {
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

    fn humanize_scalar_type(&self, typ: &SqlScalarType, postgres_compat: bool) -> String {
        use SqlScalarType::*;

        match typ {
            Array(t) => format!("{}[]", self.humanize_scalar_type(t, postgres_compat)),
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
                    self.humanize_scalar_type(element_type, postgres_compat)
                )
            }
            Map { value_type, .. } => format!(
                "map[{}=>{}]",
                self.humanize_scalar_type(&SqlScalarType::String, postgres_compat),
                self.humanize_scalar_type(value_type, postgres_compat)
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
                        self.humanize_column_type(&f.1, postgres_compat)
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

impl SessionCatalog for ConnCatalog<'_> {
    fn active_role_id(&self) -> &RoleId {
        &self.role_id
    }

    fn get_prepared_statement_desc(&self, name: &str) -> Option<&StatementDesc> {
        self.prepared_statements
            .as_ref()
            .map(|ps| ps.get(name).map(|ps| ps.desc()))
            .flatten()
    }

    fn get_portal_desc_unverified(&self, portal_name: &str) -> Option<&StatementDesc> {
        self.portals
            .and_then(|portals| portals.get(portal_name).map(|portal| &portal.desc))
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
                    .chain(self.state.temporary_schemas.values())
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
        let entry = match &entry.item {
            CatalogItem::Table(table) => {
                let (version, _gid) = table
                    .collections
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
        let entry = match &entry.item {
            CatalogItem::Table(table) => {
                let (version, _gid) = table
                    .collections
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
        &mut self.state.to_mut().system_configuration
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
                Some(self.get_schema(database_spec, schema_spec).privileges())
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
    fn minimal_qualification(&self, qualified_name: &QualifiedItemName) -> PartialItemName {
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

    fn add_notice(&self, notice: PlanNotice) {
        let _ = self.notices_tx.send(notice.into());
    }

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
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::{env, iter};

    use itertools::Itertools;
    use mz_catalog::memory::objects::CatalogItem;
    use tokio_postgres::NoTls;
    use tokio_postgres::types::Type;
    use uuid::Uuid;

    use mz_catalog::SYSTEM_CONN_ID;
    use mz_catalog::builtin::{BUILTINS, Builtin, BuiltinType};
    use mz_catalog::durable::{CatalogError, DurableCatalogError, FenceError, test_bootstrap_args};
    use mz_controller_types::{ClusterId, ReplicaId};
    use mz_expr::MirScalarExpr;
    use mz_ore::now::to_datetime;
    use mz_ore::{assert_err, assert_ok, task};
    use mz_persist_client::PersistClient;
    use mz_pgrepr::oid::{FIRST_MATERIALIZE_OID, FIRST_UNPINNED_OID, FIRST_USER_OID};
    use mz_repr::namespaces::{INFORMATION_SCHEMA, PG_CATALOG_SCHEMA};
    use mz_repr::role_id::RoleId;
    use mz_repr::{
        CatalogItemId, Datum, GlobalId, RelationVersionSelector, RowArena, SqlRelationType,
        SqlScalarType, Timestamp,
    };
    use mz_sql::catalog::{BuiltinsConfig, CatalogSchema, CatalogType, SessionCatalog};
    use mz_sql::func::{Func, FuncImpl, OP_IMPLS, Operation};
    use mz_sql::names::{
        self, DatabaseId, ItemQualifiers, ObjectId, PartialItemName, QualifiedItemName,
        ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier, SystemObjectId,
    };
    use mz_sql::plan::{
        CoercibleScalarExpr, ExprContext, HirScalarExpr, PlanContext, QueryContext, QueryLifetime,
        Scope, StatementContext,
    };
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_sql::session::vars::{SystemVars, VarInput};

    use crate::catalog::state::LocalExpressionCache;
    use crate::catalog::{Catalog, Op};
    use crate::optimize::dataflows::{EvalTime, ExprPrepStyle, prep_scalar_expr};
    use crate::session::Session;

    /// System sessions have an empty `search_path` so it's necessary to
    /// schema-qualify all referenced items.
    ///
    /// Dummy (and ostensibly client) sessions contain system schemas in their
    /// search paths, so do not require schema qualification on system objects such
    /// as types.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_minimal_qualification() {
        Catalog::with_debug(|catalog| async move {
            struct TestCase {
                input: QualifiedItemName,
                system_output: PartialItemName,
                normal_output: PartialItemName,
            }

            let test_cases = vec![
                TestCase {
                    input: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(catalog.get_pg_catalog_schema_id()),
                        },
                        item: "numeric".to_string(),
                    },
                    system_output: PartialItemName {
                        database: None,
                        schema: None,
                        item: "numeric".to_string(),
                    },
                    normal_output: PartialItemName {
                        database: None,
                        schema: None,
                        item: "numeric".to_string(),
                    },
                },
                TestCase {
                    input: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(catalog.get_mz_catalog_schema_id()),
                        },
                        item: "mz_array_types".to_string(),
                    },
                    system_output: PartialItemName {
                        database: None,
                        schema: None,
                        item: "mz_array_types".to_string(),
                    },
                    normal_output: PartialItemName {
                        database: None,
                        schema: None,
                        item: "mz_array_types".to_string(),
                    },
                },
            ];

            for tc in test_cases {
                assert_eq!(
                    catalog
                        .for_system_session()
                        .minimal_qualification(&tc.input),
                    tc.system_output
                );
                assert_eq!(
                    catalog
                        .for_session(&Session::dummy())
                        .minimal_qualification(&tc.input),
                    tc.normal_output
                );
            }
            catalog.expire().await;
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_catalog_revision() {
        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let bootstrap_args = test_bootstrap_args();
        {
            let mut catalog = Catalog::open_debug_catalog(
                persist_client.clone(),
                organization_id.clone(),
                &bootstrap_args,
            )
            .await
            .expect("unable to open debug catalog");
            assert_eq!(catalog.transient_revision(), 1);
            let commit_ts = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    commit_ts,
                    None,
                    vec![Op::CreateDatabase {
                        name: "test".to_string(),
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                )
                .await
                .expect("failed to transact");
            assert_eq!(catalog.transient_revision(), 2);
            catalog.expire().await;
        }
        {
            let catalog =
                Catalog::open_debug_catalog(persist_client, organization_id, &bootstrap_args)
                    .await
                    .expect("unable to open debug catalog");
            // Re-opening the same catalog resets the transient_revision to 1.
            assert_eq!(catalog.transient_revision(), 1);
            catalog.expire().await;
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_effective_search_path() {
        Catalog::with_debug(|catalog| async move {
            let mz_catalog_schema = (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(catalog.state().get_mz_catalog_schema_id()),
            );
            let pg_catalog_schema = (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(catalog.state().get_pg_catalog_schema_id()),
            );
            let mz_temp_schema = (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Temporary,
            );

            // Behavior with the default search_schema (public)
            let session = Session::dummy();
            let conn_catalog = catalog.for_session(&session);
            assert_ne!(
                conn_catalog.effective_search_path(false),
                conn_catalog.search_path
            );
            assert_ne!(
                conn_catalog.effective_search_path(true),
                conn_catalog.search_path
            );
            assert_eq!(
                conn_catalog.effective_search_path(false),
                vec![
                    mz_catalog_schema.clone(),
                    pg_catalog_schema.clone(),
                    conn_catalog.search_path[0].clone()
                ]
            );
            assert_eq!(
                conn_catalog.effective_search_path(true),
                vec![
                    mz_temp_schema.clone(),
                    mz_catalog_schema.clone(),
                    pg_catalog_schema.clone(),
                    conn_catalog.search_path[0].clone()
                ]
            );

            // missing schemas are added when missing
            let mut session = Session::dummy();
            session
                .vars_mut()
                .set(
                    &SystemVars::new(),
                    "search_path",
                    VarInput::Flat(mz_repr::namespaces::PG_CATALOG_SCHEMA),
                    false,
                )
                .expect("failed to set search_path");
            let conn_catalog = catalog.for_session(&session);
            assert_ne!(
                conn_catalog.effective_search_path(false),
                conn_catalog.search_path
            );
            assert_ne!(
                conn_catalog.effective_search_path(true),
                conn_catalog.search_path
            );
            assert_eq!(
                conn_catalog.effective_search_path(false),
                vec![mz_catalog_schema.clone(), pg_catalog_schema.clone()]
            );
            assert_eq!(
                conn_catalog.effective_search_path(true),
                vec![
                    mz_temp_schema.clone(),
                    mz_catalog_schema.clone(),
                    pg_catalog_schema.clone()
                ]
            );

            let mut session = Session::dummy();
            session
                .vars_mut()
                .set(
                    &SystemVars::new(),
                    "search_path",
                    VarInput::Flat(mz_repr::namespaces::MZ_CATALOG_SCHEMA),
                    false,
                )
                .expect("failed to set search_path");
            let conn_catalog = catalog.for_session(&session);
            assert_ne!(
                conn_catalog.effective_search_path(false),
                conn_catalog.search_path
            );
            assert_ne!(
                conn_catalog.effective_search_path(true),
                conn_catalog.search_path
            );
            assert_eq!(
                conn_catalog.effective_search_path(false),
                vec![pg_catalog_schema.clone(), mz_catalog_schema.clone()]
            );
            assert_eq!(
                conn_catalog.effective_search_path(true),
                vec![
                    mz_temp_schema.clone(),
                    pg_catalog_schema.clone(),
                    mz_catalog_schema.clone()
                ]
            );

            let mut session = Session::dummy();
            session
                .vars_mut()
                .set(
                    &SystemVars::new(),
                    "search_path",
                    VarInput::Flat(mz_repr::namespaces::MZ_TEMP_SCHEMA),
                    false,
                )
                .expect("failed to set search_path");
            let conn_catalog = catalog.for_session(&session);
            assert_ne!(
                conn_catalog.effective_search_path(false),
                conn_catalog.search_path
            );
            assert_ne!(
                conn_catalog.effective_search_path(true),
                conn_catalog.search_path
            );
            assert_eq!(
                conn_catalog.effective_search_path(false),
                vec![
                    mz_catalog_schema.clone(),
                    pg_catalog_schema.clone(),
                    mz_temp_schema.clone()
                ]
            );
            assert_eq!(
                conn_catalog.effective_search_path(true),
                vec![mz_catalog_schema, pg_catalog_schema, mz_temp_schema]
            );
            catalog.expire().await;
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_normalized_create() {
        use mz_ore::collections::CollectionExt;
        Catalog::with_debug(|catalog| async move {
            let conn_catalog = catalog.for_system_session();
            let scx = &mut StatementContext::new(None, &conn_catalog);

            let parsed = mz_sql_parser::parser::parse_statements(
                "create view public.foo as select 1 as bar",
            )
            .expect("")
            .into_element()
            .ast;

            let (stmt, _) = names::resolve(scx.catalog, parsed).expect("");

            // Ensure that all identifiers are quoted.
            assert_eq!(
                r#"CREATE VIEW "materialize"."public"."foo" AS SELECT 1 AS "bar""#,
                mz_sql::normalize::create_statement(scx, stmt).expect(""),
            );
            catalog.expire().await;
        })
        .await;
    }

    // Test that if a large catalog item is somehow committed, then we can still load the catalog.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // slow
    async fn test_large_catalog_item() {
        let view_def = "CREATE VIEW \"materialize\".\"public\".\"v\" AS SELECT 1 FROM (SELECT 1";
        let column = ", 1";
        let view_def_size = view_def.bytes().count();
        let column_size = column.bytes().count();
        let column_count =
            (mz_sql_parser::parser::MAX_STATEMENT_BATCH_SIZE - view_def_size) / column_size + 1;
        let columns = iter::repeat(column).take(column_count).join("");
        let create_sql = format!("{view_def}{columns})");
        let create_sql_check = create_sql.clone();
        assert_ok!(mz_sql_parser::parser::parse_statements(&create_sql));
        assert_err!(mz_sql_parser::parser::parse_statements_with_limit(
            &create_sql
        ));

        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let id = CatalogItemId::User(1);
        let gid = GlobalId::User(1);
        let bootstrap_args = test_bootstrap_args();
        {
            let mut catalog = Catalog::open_debug_catalog(
                persist_client.clone(),
                organization_id.clone(),
                &bootstrap_args,
            )
            .await
            .expect("unable to open debug catalog");
            let item = catalog
                .state()
                .deserialize_item(
                    gid,
                    &create_sql,
                    &BTreeMap::new(),
                    &mut LocalExpressionCache::Closed,
                    None,
                )
                .expect("unable to parse view");
            let commit_ts = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    commit_ts,
                    None,
                    vec![Op::CreateItem {
                        item,
                        name: QualifiedItemName {
                            qualifiers: ItemQualifiers {
                                database_spec: ResolvedDatabaseSpecifier::Id(DatabaseId::User(1)),
                                schema_spec: SchemaSpecifier::Id(SchemaId::User(3)),
                            },
                            item: "v".to_string(),
                        },
                        id,
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                )
                .await
                .expect("failed to transact");
            catalog.expire().await;
        }
        {
            let catalog =
                Catalog::open_debug_catalog(persist_client, organization_id, &bootstrap_args)
                    .await
                    .expect("unable to open debug catalog");
            let view = catalog.get_entry(&id);
            assert_eq!("v", view.name.item);
            match &view.item {
                CatalogItem::View(view) => assert_eq!(create_sql_check, view.create_sql),
                item => panic!("expected view, got {}", item.typ()),
            }
            catalog.expire().await;
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_object_type() {
        Catalog::with_debug(|catalog| async move {
            let conn_catalog = catalog.for_system_session();

            assert_eq!(
                mz_sql::catalog::ObjectType::ClusterReplica,
                conn_catalog.get_object_type(&ObjectId::ClusterReplica((
                    ClusterId::user(1).expect("1 is a valid ID"),
                    ReplicaId::User(1)
                )))
            );
            assert_eq!(
                mz_sql::catalog::ObjectType::Role,
                conn_catalog.get_object_type(&ObjectId::Role(RoleId::User(1)))
            );
            catalog.expire().await;
        })
        .await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_get_privileges() {
        Catalog::with_debug(|catalog| async move {
            let conn_catalog = catalog.for_system_session();

            assert_eq!(
                None,
                conn_catalog.get_privileges(&SystemObjectId::Object(ObjectId::ClusterReplica((
                    ClusterId::user(1).expect("1 is a valid ID"),
                    ReplicaId::User(1),
                ))))
            );
            assert_eq!(
                None,
                conn_catalog
                    .get_privileges(&SystemObjectId::Object(ObjectId::Role(RoleId::User(1))))
            );
            catalog.expire().await;
        })
        .await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn verify_builtin_descs() {
        Catalog::with_debug(|catalog| async move {
            let conn_catalog = catalog.for_system_session();

            let builtins_cfg = BuiltinsConfig {
                include_continual_tasks: true,
            };
            for builtin in BUILTINS::iter(&builtins_cfg) {
                let (schema, name, expected_desc) = match builtin {
                    Builtin::Table(t) => (&t.schema, &t.name, &t.desc),
                    Builtin::View(v) => (&v.schema, &v.name, &v.desc),
                    Builtin::Source(s) => (&s.schema, &s.name, &s.desc),
                    Builtin::Log(_)
                    | Builtin::Type(_)
                    | Builtin::Func(_)
                    | Builtin::ContinualTask(_)
                    | Builtin::Index(_)
                    | Builtin::Connection(_) => continue,
                };
                let item = conn_catalog
                    .resolve_item(&PartialItemName {
                        database: None,
                        schema: Some(schema.to_string()),
                        item: name.to_string(),
                    })
                    .expect("unable to resolve item")
                    .at_version(RelationVersionSelector::Latest);

                let full_name = conn_catalog.resolve_full_name(item.name());
                let actual_desc = item.desc(&full_name).expect("invalid item type");
                for (index, ((actual_name, actual_typ), (expected_name, expected_typ))) in
                    actual_desc.iter().zip_eq(expected_desc.iter()).enumerate()
                {
                    assert_eq!(
                        actual_name, expected_name,
                        "item {schema}.{name} column {index} name did not match its expected name"
                    );
                    assert_eq!(
                        actual_typ, expected_typ,
                        "item {schema}.{name} column {index} ('{actual_name}') type did not match its expected type"
                    );
                }
                assert_eq!(
                    &*actual_desc, expected_desc,
                    "item {schema}.{name} did not match its expected RelationDesc"
                );
            }
            catalog.expire().await;
        })
        .await
    }

    // Connect to a running Postgres server and verify that our builtin
    // types and functions match it, in addition to some other things.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_compare_builtins_postgres() {
        async fn inner(catalog: Catalog) {
            // Verify that all builtin functions:
            // - have a unique OID
            // - if they have a postgres counterpart (same oid) then they have matching name
            let (client, connection) = tokio_postgres::connect(
                &env::var("POSTGRES_URL").unwrap_or_else(|_| "host=localhost user=postgres".into()),
                NoTls,
            )
            .await
            .expect("failed to connect to Postgres");

            task::spawn(|| "compare_builtin_postgres", async move {
                if let Err(e) = connection.await {
                    panic!("connection error: {}", e);
                }
            });

            struct PgProc {
                name: String,
                arg_oids: Vec<u32>,
                ret_oid: Option<u32>,
                ret_set: bool,
            }

            struct PgType {
                name: String,
                ty: String,
                elem: u32,
                array: u32,
                input: u32,
                receive: u32,
            }

            struct PgOper {
                oprresult: u32,
                name: String,
            }

            let pg_proc: BTreeMap<_, _> = client
                .query(
                    "SELECT
                    p.oid,
                    proname,
                    proargtypes,
                    prorettype,
                    proretset
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid",
                    &[],
                )
                .await
                .expect("pg query failed")
                .into_iter()
                .map(|row| {
                    let oid: u32 = row.get("oid");
                    let pg_proc = PgProc {
                        name: row.get("proname"),
                        arg_oids: row.get("proargtypes"),
                        ret_oid: row.get("prorettype"),
                        ret_set: row.get("proretset"),
                    };
                    (oid, pg_proc)
                })
                .collect();

            let pg_type: BTreeMap<_, _> = client
                .query(
                    "SELECT oid, typname, typtype::text, typelem, typarray, typinput::oid, typreceive::oid as typreceive FROM pg_type",
                    &[],
                )
                .await
                .expect("pg query failed")
                .into_iter()
                .map(|row| {
                    let oid: u32 = row.get("oid");
                    let pg_type = PgType {
                        name: row.get("typname"),
                        ty: row.get("typtype"),
                        elem: row.get("typelem"),
                        array: row.get("typarray"),
                        input: row.get("typinput"),
                        receive: row.get("typreceive"),
                    };
                    (oid, pg_type)
                })
                .collect();

            let pg_oper: BTreeMap<_, _> = client
                .query("SELECT oid, oprname, oprresult FROM pg_operator", &[])
                .await
                .expect("pg query failed")
                .into_iter()
                .map(|row| {
                    let oid: u32 = row.get("oid");
                    let pg_oper = PgOper {
                        name: row.get("oprname"),
                        oprresult: row.get("oprresult"),
                    };
                    (oid, pg_oper)
                })
                .collect();

            let conn_catalog = catalog.for_system_session();
            let resolve_type_oid = |item: &str| {
                conn_catalog
                    .resolve_type(&PartialItemName {
                        database: None,
                        // All functions we check exist in PG, so the types must, as
                        // well
                        schema: Some(PG_CATALOG_SCHEMA.into()),
                        item: item.to_string(),
                    })
                    .expect("unable to resolve type")
                    .oid()
            };

            let func_oids: BTreeSet<_> = BUILTINS::funcs()
                .flat_map(|f| f.inner.func_impls().into_iter().map(|f| f.oid))
                .collect();

            let mut all_oids = BTreeSet::new();

            // A function to determine if two oids are equivalent enough for these tests. We don't
            // support some types, so map exceptions here.
            let equivalent_types: BTreeSet<(Option<u32>, Option<u32>)> = BTreeSet::from_iter(
                [
                    // We don't support NAME.
                    (Type::NAME, Type::TEXT),
                    (Type::NAME_ARRAY, Type::TEXT_ARRAY),
                    // We don't support time with time zone.
                    (Type::TIME, Type::TIMETZ),
                    (Type::TIME_ARRAY, Type::TIMETZ_ARRAY),
                ]
                .map(|(a, b)| (Some(a.oid()), Some(b.oid()))),
            );
            let ignore_return_types: BTreeSet<u32> = BTreeSet::from([
                1619, // pg_typeof: TODO: We now have regtype and can correctly implement this.
            ]);
            let is_same_type = |fn_oid: u32, a: Option<u32>, b: Option<u32>| -> bool {
                if ignore_return_types.contains(&fn_oid) {
                    return true;
                }
                if equivalent_types.contains(&(a, b)) || equivalent_types.contains(&(b, a)) {
                    return true;
                }
                a == b
            };

            let builtins_cfg = BuiltinsConfig {
                include_continual_tasks: true,
            };
            for builtin in BUILTINS::iter(&builtins_cfg) {
                match builtin {
                    Builtin::Type(ty) => {
                        assert!(all_oids.insert(ty.oid), "{} reused oid {}", ty.name, ty.oid);

                        if ty.oid >= FIRST_MATERIALIZE_OID {
                            // High OIDs are reserved in Materialize and don't have
                            // PostgreSQL counterparts.
                            continue;
                        }

                        // For types that have a PostgreSQL counterpart, verify that
                        // the name and oid match.
                        let pg_ty = pg_type.get(&ty.oid).unwrap_or_else(|| {
                            panic!("pg_proc missing type {}: oid {}", ty.name, ty.oid)
                        });
                        assert_eq!(
                            ty.name, pg_ty.name,
                            "oid {} has name {} in postgres; expected {}",
                            ty.oid, pg_ty.name, ty.name,
                        );

                        let (typinput_oid, typreceive_oid) = match &ty.details.pg_metadata {
                            None => (0, 0),
                            Some(pgmeta) => (pgmeta.typinput_oid, pgmeta.typreceive_oid),
                        };
                        assert_eq!(
                            typinput_oid, pg_ty.input,
                            "type {} has typinput OID {:?} in mz but {:?} in pg",
                            ty.name, typinput_oid, pg_ty.input,
                        );
                        assert_eq!(
                            typreceive_oid, pg_ty.receive,
                            "type {} has typreceive OID {:?} in mz but {:?} in pg",
                            ty.name, typreceive_oid, pg_ty.receive,
                        );
                        if typinput_oid != 0 {
                            assert!(
                                func_oids.contains(&typinput_oid),
                                "type {} has typinput OID {} that does not exist in pg_proc",
                                ty.name,
                                typinput_oid,
                            );
                        }
                        if typreceive_oid != 0 {
                            assert!(
                                func_oids.contains(&typreceive_oid),
                                "type {} has typreceive OID {} that does not exist in pg_proc",
                                ty.name,
                                typreceive_oid,
                            );
                        }

                        // Ensure the type matches.
                        match &ty.details.typ {
                            CatalogType::Array { element_reference } => {
                                let elem_ty = BUILTINS::iter(&builtins_cfg)
                                    .filter_map(|builtin| match builtin {
                                        Builtin::Type(ty @ BuiltinType { name, .. })
                                            if element_reference == name =>
                                        {
                                            Some(ty)
                                        }
                                        _ => None,
                                    })
                                    .next();
                                let elem_ty = match elem_ty {
                                    Some(ty) => ty,
                                    None => {
                                        panic!("{} is unexpectedly not a type", element_reference)
                                    }
                                };
                                assert_eq!(
                                    pg_ty.elem, elem_ty.oid,
                                    "type {} has mismatched element OIDs",
                                    ty.name
                                )
                            }
                            CatalogType::Pseudo => {
                                assert_eq!(
                                    pg_ty.ty, "p",
                                    "type {} is not a pseudo type as expected",
                                    ty.name
                                )
                            }
                            CatalogType::Range { .. } => {
                                assert_eq!(
                                    pg_ty.ty, "r",
                                    "type {} is not a range type as expected",
                                    ty.name
                                );
                            }
                            _ => {
                                assert_eq!(
                                    pg_ty.ty, "b",
                                    "type {} is not a base type as expected",
                                    ty.name
                                )
                            }
                        }

                        // Ensure the array type reference is correct.
                        let schema = catalog
                            .resolve_schema_in_database(
                                &ResolvedDatabaseSpecifier::Ambient,
                                ty.schema,
                                &SYSTEM_CONN_ID,
                            )
                            .expect("unable to resolve schema");
                        let allocated_type = catalog
                            .resolve_type(
                                None,
                                &vec![(ResolvedDatabaseSpecifier::Ambient, schema.id().clone())],
                                &PartialItemName {
                                    database: None,
                                    schema: Some(schema.name().schema.clone()),
                                    item: ty.name.to_string(),
                                },
                                &SYSTEM_CONN_ID,
                            )
                            .expect("unable to resolve type");
                        let ty = if let CatalogItem::Type(ty) = &allocated_type.item {
                            ty
                        } else {
                            panic!("unexpectedly not a type")
                        };
                        match ty.details.array_id {
                            Some(array_id) => {
                                let array_ty = catalog.get_entry(&array_id);
                                assert_eq!(
                                    pg_ty.array, array_ty.oid,
                                    "type {} has mismatched array OIDs",
                                    allocated_type.name.item,
                                );
                            }
                            None => assert_eq!(
                                pg_ty.array, 0,
                                "type {} does not have an array type in mz but does in pg",
                                allocated_type.name.item,
                            ),
                        }
                    }
                    Builtin::Func(func) => {
                        for imp in func.inner.func_impls() {
                            assert!(
                                all_oids.insert(imp.oid),
                                "{} reused oid {}",
                                func.name,
                                imp.oid
                            );

                            assert!(
                                imp.oid < FIRST_USER_OID,
                                "built-in function {} erroneously has OID in user space ({})",
                                func.name,
                                imp.oid,
                            );

                            // For functions that have a postgres counterpart, verify that the name and
                            // oid match.
                            let pg_fn = if imp.oid >= FIRST_UNPINNED_OID {
                                continue;
                            } else {
                                pg_proc.get(&imp.oid).unwrap_or_else(|| {
                                    panic!(
                                        "pg_proc missing function {}: oid {}",
                                        func.name, imp.oid
                                    )
                                })
                            };
                            assert_eq!(
                                func.name, pg_fn.name,
                                "funcs with oid {} don't match names: {} in mz, {} in pg",
                                imp.oid, func.name, pg_fn.name
                            );

                            // Complain, but don't fail, if argument oids don't match.
                            // TODO: make these match.
                            let imp_arg_oids = imp
                                .arg_typs
                                .iter()
                                .map(|item| resolve_type_oid(item))
                                .collect::<Vec<_>>();

                            if imp_arg_oids != pg_fn.arg_oids {
                                println!(
                                    "funcs with oid {} ({}) don't match arguments: {:?} in mz, {:?} in pg",
                                    imp.oid, func.name, imp_arg_oids, pg_fn.arg_oids
                                );
                            }

                            let imp_return_oid = imp.return_typ.map(resolve_type_oid);

                            assert!(
                                is_same_type(imp.oid, imp_return_oid, pg_fn.ret_oid),
                                "funcs with oid {} ({}) don't match return types: {:?} in mz, {:?} in pg",
                                imp.oid,
                                func.name,
                                imp_return_oid,
                                pg_fn.ret_oid
                            );

                            assert_eq!(
                                imp.return_is_set, pg_fn.ret_set,
                                "funcs with oid {} ({}) don't match set-returning value: {:?} in mz, {:?} in pg",
                                imp.oid, func.name, imp.return_is_set, pg_fn.ret_set
                            );
                        }
                    }
                    _ => (),
                }
            }

            for (op, func) in OP_IMPLS.iter() {
                for imp in func.func_impls() {
                    assert!(all_oids.insert(imp.oid), "{} reused oid {}", op, imp.oid);

                    // For operators that have a postgres counterpart, verify that the name and oid match.
                    let pg_op = if imp.oid >= FIRST_UNPINNED_OID {
                        continue;
                    } else {
                        pg_oper.get(&imp.oid).unwrap_or_else(|| {
                            panic!("pg_operator missing operator {}: oid {}", op, imp.oid)
                        })
                    };

                    assert_eq!(*op, pg_op.name);

                    let imp_return_oid =
                        imp.return_typ.map(resolve_type_oid).expect("must have oid");
                    if imp_return_oid != pg_op.oprresult {
                        panic!(
                            "operators with oid {} ({}) don't match return typs: {} in mz, {} in pg",
                            imp.oid, op, imp_return_oid, pg_op.oprresult
                        );
                    }
                }
            }
            catalog.expire().await;
        }

        Catalog::with_debug(inner).await
    }

    // Execute all builtin functions with all combinations of arguments from interesting datums.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_smoketest_all_builtins() {
        fn inner(catalog: Catalog) -> Vec<mz_ore::task::JoinHandle<()>> {
            let catalog = Arc::new(catalog);
            let conn_catalog = catalog.for_system_session();

            let resolve_type_oid = |item: &str| conn_catalog.state().get_system_type(item).oid();
            let mut handles = Vec::new();

            // Extracted during planning; always panics when executed.
            let ignore_names = BTreeSet::from([
                "avg",
                "avg_internal_v1",
                "bool_and",
                "bool_or",
                "has_table_privilege", // > 3 s each
                "has_type_privilege",  // > 3 s each
                "mod",
                "mz_panic",
                "mz_sleep",
                "pow",
                "stddev_pop",
                "stddev_samp",
                "stddev",
                "var_pop",
                "var_samp",
                "variance",
            ]);

            let fns = BUILTINS::funcs()
                .map(|func| (&func.name, func.inner))
                .chain(OP_IMPLS.iter());

            for (name, func) in fns {
                if ignore_names.contains(name) {
                    continue;
                }
                let Func::Scalar(impls) = func else {
                    continue;
                };

                'outer: for imp in impls {
                    let details = imp.details();
                    let mut styps = Vec::new();
                    for item in details.arg_typs.iter() {
                        let oid = resolve_type_oid(item);
                        let Ok(pgtyp) = mz_pgrepr::Type::from_oid(oid) else {
                            continue 'outer;
                        };
                        styps.push(SqlScalarType::try_from(&pgtyp).expect("must exist"));
                    }
                    let datums = styps
                        .iter()
                        .map(|styp| {
                            let mut datums = vec![Datum::Null];
                            datums.extend(styp.interesting_datums());
                            datums
                        })
                        .collect::<Vec<_>>();
                    // Skip nullary fns.
                    if datums.is_empty() {
                        continue;
                    }

                    let return_oid = details
                        .return_typ
                        .map(resolve_type_oid)
                        .expect("must exist");
                    let return_styp = mz_pgrepr::Type::from_oid(return_oid)
                        .ok()
                        .map(|typ| SqlScalarType::try_from(&typ).expect("must exist"));

                    let mut idxs = vec![0; datums.len()];
                    while idxs[0] < datums[0].len() {
                        let mut args = Vec::with_capacity(idxs.len());
                        for i in 0..(datums.len()) {
                            args.push(datums[i][idxs[i]]);
                        }

                        let op = &imp.op;
                        let scalars = args
                            .iter()
                            .enumerate()
                            .map(|(i, datum)| {
                                CoercibleScalarExpr::Coerced(HirScalarExpr::literal(
                                    datum.clone(),
                                    styps[i].clone(),
                                ))
                            })
                            .collect();

                        let call_name = format!(
                            "{name}({}) (oid: {})",
                            args.iter()
                                .map(|d| d.to_string())
                                .collect::<Vec<_>>()
                                .join(", "),
                            imp.oid
                        );
                        let catalog = Arc::clone(&catalog);
                        let call_name_fn = call_name.clone();
                        let return_styp = return_styp.clone();
                        let handle = task::spawn_blocking(
                            || call_name,
                            move || {
                                smoketest_fn(
                                    name,
                                    call_name_fn,
                                    op,
                                    imp,
                                    args,
                                    catalog,
                                    scalars,
                                    return_styp,
                                )
                            },
                        );
                        handles.push(handle);

                        // Advance to the next datum combination.
                        for i in (0..datums.len()).rev() {
                            idxs[i] += 1;
                            if idxs[i] >= datums[i].len() {
                                if i == 0 {
                                    break;
                                }
                                idxs[i] = 0;
                                continue;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            handles
        }

        let handles = Catalog::with_debug(|catalog| async { inner(catalog) }).await;
        for handle in handles {
            handle.await;
        }
    }

    fn smoketest_fn(
        name: &&str,
        call_name: String,
        op: &Operation<HirScalarExpr>,
        imp: &FuncImpl<HirScalarExpr>,
        args: Vec<Datum<'_>>,
        catalog: Arc<Catalog>,
        scalars: Vec<CoercibleScalarExpr>,
        return_styp: Option<SqlScalarType>,
    ) {
        let conn_catalog = catalog.for_system_session();
        let pcx = PlanContext::zero();
        let scx = StatementContext::new(Some(&pcx), &conn_catalog);
        let qcx = QueryContext::root(&scx, QueryLifetime::OneShot);
        let ecx = ExprContext {
            qcx: &qcx,
            name: "smoketest",
            scope: &Scope::empty(),
            relation_type: &SqlRelationType::empty(),
            allow_aggregates: false,
            allow_subqueries: false,
            allow_parameters: false,
            allow_windows: false,
        };
        let arena = RowArena::new();
        let mut session = Session::<Timestamp>::dummy();
        session
            .start_transaction(to_datetime(0), None, None)
            .expect("must succeed");
        let prep_style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Time(Timestamp::MIN),
            session: &session,
            catalog_state: &catalog.state,
        };

        // Execute the function as much as possible, ensuring no panics occur, but
        // otherwise ignoring eval errors. We also do various other checks.
        let res = (op.0)(&ecx, scalars, &imp.params, vec![]);
        if let Ok(hir) = res {
            if let Ok(mut mir) = hir.lower_uncorrelated() {
                // Populate unmaterialized functions.
                prep_scalar_expr(&mut mir, prep_style.clone()).expect("must succeed");

                if let Ok(eval_result_datum) = mir.eval(&[], &arena) {
                    if let Some(return_styp) = return_styp {
                        let mir_typ = mir.typ(&[]);
                        // MIR type inference should be consistent with the type
                        // we get from the catalog.
                        assert_eq!(mir_typ.scalar_type, return_styp);
                        // The following will check not just that the scalar type
                        // is ok, but also catches if the function returned a null
                        // but the MIR type inference said "non-nullable".
                        if !eval_result_datum.is_instance_of_sql(&mir_typ) {
                            panic!(
                                "{call_name}: expected return type of {return_styp:?}, got {eval_result_datum}"
                            );
                        }
                        // Check the consistency of `introduces_nulls` and
                        // `propagates_nulls` with `MirScalarExpr::typ`.
                        if let Some((introduces_nulls, propagates_nulls)) =
                            call_introduces_propagates_nulls(&mir)
                        {
                            if introduces_nulls {
                                // If the function introduces_nulls, then the return
                                // type should always be nullable, regardless of
                                // the nullability of the input types.
                                assert!(
                                    mir_typ.nullable,
                                    "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}",
                                    name, args, mir, mir_typ.nullable
                                );
                            } else {
                                let any_input_null = args.iter().any(|arg| arg.is_null());
                                if !any_input_null {
                                    assert!(
                                        !mir_typ.nullable,
                                        "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}",
                                        name, args, mir, mir_typ.nullable
                                    );
                                } else {
                                    assert_eq!(
                                        mir_typ.nullable, propagates_nulls,
                                        "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}",
                                        name, args, mir, mir_typ.nullable
                                    );
                                }
                            }
                        }
                        // Check that `MirScalarExpr::reduce` yields the same result
                        // as the real evaluation.
                        let mut reduced = mir.clone();
                        reduced.reduce(&[]);
                        match reduced {
                            MirScalarExpr::Literal(reduce_result, ctyp) => {
                                match reduce_result {
                                    Ok(reduce_result_row) => {
                                        let reduce_result_datum = reduce_result_row.unpack_first();
                                        assert_eq!(
                                            reduce_result_datum,
                                            eval_result_datum,
                                            "eval/reduce datum mismatch: fn named `{}` called on args `{:?}` (lowered to `{}`) evaluated to `{}` with typ `{:?}`, but reduced to `{}` with typ `{:?}`",
                                            name,
                                            args,
                                            mir,
                                            eval_result_datum,
                                            mir_typ.scalar_type,
                                            reduce_result_datum,
                                            ctyp.scalar_type
                                        );
                                        // Let's check that the types also match.
                                        // (We are not checking nullability here,
                                        // because it's ok when we know a more
                                        // precise nullability after actually
                                        // evaluating a function than before.)
                                        assert_eq!(
                                            ctyp.scalar_type,
                                            mir_typ.scalar_type,
                                            "eval/reduce type mismatch: fn named `{}` called on args `{:?}` (lowered to `{}`) evaluated to `{}` with typ `{:?}`, but reduced to `{}` with typ `{:?}`",
                                            name,
                                            args,
                                            mir,
                                            eval_result_datum,
                                            mir_typ.scalar_type,
                                            reduce_result_datum,
                                            ctyp.scalar_type
                                        );
                                    }
                                    Err(..) => {} // It's ok, we might have given invalid args to the function
                                }
                            }
                            _ => unreachable!(
                                "all args are literals, so should have reduced to a literal"
                            ),
                        }
                    }
                }
            }
        }
    }

    /// If the given MirScalarExpr
    ///  - is a function call, and
    ///  - all arguments are literals
    /// then it returns whether the called function (introduces_nulls, propagates_nulls).
    fn call_introduces_propagates_nulls(mir_func_call: &MirScalarExpr) -> Option<(bool, bool)> {
        match mir_func_call {
            MirScalarExpr::CallUnary { func, expr } => {
                if expr.is_literal() {
                    Some((func.introduces_nulls(), func.propagates_nulls()))
                } else {
                    None
                }
            }
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                if expr1.is_literal() && expr2.is_literal() {
                    Some((func.introduces_nulls(), func.propagates_nulls()))
                } else {
                    None
                }
            }
            MirScalarExpr::CallVariadic { func, exprs } => {
                if exprs.iter().all(|arg| arg.is_literal()) {
                    Some((func.introduces_nulls(), func.propagates_nulls()))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    // Make sure pg views don't use types that only exist in Materialize.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_pg_views_forbidden_types() {
        Catalog::with_debug(|catalog| async move {
            let conn_catalog = catalog.for_system_session();

            for view in BUILTINS::views().filter(|view| {
                view.schema == PG_CATALOG_SCHEMA || view.schema == INFORMATION_SCHEMA
            }) {
                let item = conn_catalog
                    .resolve_item(&PartialItemName {
                        database: None,
                        schema: Some(view.schema.to_string()),
                        item: view.name.to_string(),
                    })
                    .expect("unable to resolve view")
                    // TODO(alter_table)
                    .at_version(RelationVersionSelector::Latest);
                let full_name = conn_catalog.resolve_full_name(item.name());
                for col_type in item
                    .desc(&full_name)
                    .expect("invalid item type")
                    .iter_types()
                {
                    match &col_type.scalar_type {
                        typ @ SqlScalarType::UInt16
                        | typ @ SqlScalarType::UInt32
                        | typ @ SqlScalarType::UInt64
                        | typ @ SqlScalarType::MzTimestamp
                        | typ @ SqlScalarType::List { .. }
                        | typ @ SqlScalarType::Map { .. }
                        | typ @ SqlScalarType::MzAclItem => {
                            panic!("{typ:?} type found in {full_name}");
                        }
                        SqlScalarType::AclItem
                        | SqlScalarType::Bool
                        | SqlScalarType::Int16
                        | SqlScalarType::Int32
                        | SqlScalarType::Int64
                        | SqlScalarType::Float32
                        | SqlScalarType::Float64
                        | SqlScalarType::Numeric { .. }
                        | SqlScalarType::Date
                        | SqlScalarType::Time
                        | SqlScalarType::Timestamp { .. }
                        | SqlScalarType::TimestampTz { .. }
                        | SqlScalarType::Interval
                        | SqlScalarType::PgLegacyChar
                        | SqlScalarType::Bytes
                        | SqlScalarType::String
                        | SqlScalarType::Char { .. }
                        | SqlScalarType::VarChar { .. }
                        | SqlScalarType::Jsonb
                        | SqlScalarType::Uuid
                        | SqlScalarType::Array(_)
                        | SqlScalarType::Record { .. }
                        | SqlScalarType::Oid
                        | SqlScalarType::RegProc
                        | SqlScalarType::RegType
                        | SqlScalarType::RegClass
                        | SqlScalarType::Int2Vector
                        | SqlScalarType::Range { .. }
                        | SqlScalarType::PgLegacyName => {}
                    }
                }
            }
            catalog.expire().await;
        })
        .await
    }

    // Make sure objects reside in the `mz_introspection` schema iff they depend on per-replica
    // introspection relations.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn test_mz_introspection_builtins() {
        Catalog::with_debug(|catalog| async move {
            let conn_catalog = catalog.for_system_session();

            let introspection_schema_id = catalog.get_mz_introspection_schema_id();
            let introspection_schema_spec = SchemaSpecifier::Id(introspection_schema_id);

            for entry in catalog.entries() {
                let schema_spec = entry.name().qualifiers.schema_spec;
                let introspection_deps = catalog.introspection_dependencies(entry.id);
                if introspection_deps.is_empty() {
                    assert!(
                        schema_spec != introspection_schema_spec,
                        "entry does not depend on introspection sources but is in \
                         `mz_introspection`: {}",
                        conn_catalog.resolve_full_name(entry.name()),
                    );
                } else {
                    assert!(
                        schema_spec == introspection_schema_spec,
                        "entry depends on introspection sources but is not in \
                         `mz_introspection`: {}",
                        conn_catalog.resolve_full_name(entry.name()),
                    );
                }
            }
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_multi_subscriber_catalog() {
        let persist_client = PersistClient::new_for_tests().await;
        let bootstrap_args = test_bootstrap_args();
        let organization_id = Uuid::new_v4();
        let db_name = "DB";

        let mut writer_catalog = Catalog::open_debug_catalog(
            persist_client.clone(),
            organization_id.clone(),
            &bootstrap_args,
        )
        .await
        .expect("open_debug_catalog");
        let mut read_only_catalog = Catalog::open_debug_read_only_catalog(
            persist_client.clone(),
            organization_id.clone(),
            &bootstrap_args,
        )
        .await
        .expect("open_debug_read_only_catalog");
        assert_err!(writer_catalog.resolve_database(db_name));
        assert_err!(read_only_catalog.resolve_database(db_name));

        let commit_ts = writer_catalog.current_upper().await;
        writer_catalog
            .transact(
                None,
                commit_ts,
                None,
                vec![Op::CreateDatabase {
                    name: db_name.to_string(),
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("failed to transact");

        let write_db = writer_catalog
            .resolve_database(db_name)
            .expect("resolve_database");
        read_only_catalog
            .sync_to_current_updates()
            .await
            .expect("sync_to_current_updates");
        let read_db = read_only_catalog
            .resolve_database(db_name)
            .expect("resolve_database");

        assert_eq!(write_db, read_db);

        let writer_catalog_fencer =
            Catalog::open_debug_catalog(persist_client, organization_id, &bootstrap_args)
                .await
                .expect("open_debug_catalog for fencer");
        let fencer_db = writer_catalog_fencer
            .resolve_database(db_name)
            .expect("resolve_database for fencer");
        assert_eq!(fencer_db, read_db);

        let write_fence_err = writer_catalog
            .sync_to_current_updates()
            .await
            .expect_err("sync_to_current_updates for fencer");
        assert!(matches!(
            write_fence_err,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
        ));
        let read_fence_err = read_only_catalog
            .sync_to_current_updates()
            .await
            .expect_err("sync_to_current_updates after fencer");
        assert!(matches!(
            read_fence_err,
            CatalogError::Durable(DurableCatalogError::Fence(FenceError::Epoch { .. }))
        ));

        writer_catalog.expire().await;
        read_only_catalog.expire().await;
        writer_catalog_fencer.expire().await;
    }
}
