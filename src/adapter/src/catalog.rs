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
use std::collections::{BTreeMap, BTreeSet};
use std::convert;
use std::sync::Arc;

use futures::Future;
use itertools::Itertools;
use mz_adapter_types::compaction::CompactionWindow;
use mz_sql::session::metadata::SessionMetadata;
use smallvec::SmallVec;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::MutexGuard;
use tracing::{info, trace};
use uuid::Uuid;

use mz_adapter_types::connection::ConnectionId;
use mz_audit_log::{EventDetails, EventType, FullNameV1, IdFullNameV1, ObjectType, VersionedEvent};
use mz_build_info::DUMMY_BUILD_INFO;
use mz_catalog::builtin::{
    BuiltinCluster, BuiltinLog, BuiltinSource, BuiltinTable, BuiltinType, BUILTINS,
    BUILTIN_PREFIXES, MZ_INTROSPECTION_CLUSTER,
};
use mz_catalog::config::{ClusterReplicaSizeMap, Config, StateConfig};
use mz_catalog::durable::{
    test_bootstrap_args, DurableCatalogState, OpenableDurableCatalogState, StashConfig, Transaction,
};
use mz_catalog::memory::error::{AmbiguousRename, Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogEntry, CatalogItem, Cluster, ClusterConfig, ClusterReplica, ClusterReplicaProcessStatus,
    DataSourceDesc, Database, Index, MaterializedView, Role, Schema, Sink, Source,
};
use mz_catalog::SYSTEM_CONN_ID;
use mz_compute_types::dataflows::DataflowDescription;
use mz_controller::clusters::{
    ClusterEvent, ManagedReplicaLocation, ReplicaConfig, ReplicaLocation,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::OptimizedMirRelationExpr;
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashSet;
use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::option::FallibleMapExt;
use mz_ore::result::ResultExt as _;
use mz_ore::soft_panic_or_log;
use mz_persist_client::PersistClient;
use mz_repr::adt::mz_acl_item::{merge_mz_acl_items, AclMode, MzAclItem, PrivilegeMap};
use mz_repr::explain::ExprHumanizer;
use mz_repr::namespaces::MZ_TEMP_SCHEMA;
use mz_repr::role_id::RoleId;
use mz_repr::{Diff, GlobalId, ScalarType};
use mz_secrets::InMemorySecretsController;
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogDatabase, CatalogError as SqlCatalogError,
    CatalogItem as SqlCatalogItem, CatalogItemType as SqlCatalogItemType, CatalogRecordField,
    CatalogRole, CatalogSchema, CatalogType, CatalogTypeDetails, DefaultPrivilegeAclItem,
    DefaultPrivilegeObject, EnvironmentId, IdReference, NameReference, RoleAttributes,
    RoleMembership, RoleVars, SessionCatalog, SystemObjectType,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, FullItemName, FullSchemaName, ItemQualifiers, ObjectId,
    PartialItemName, QualifiedItemName, QualifiedSchemaName, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier, SystemObjectId, PUBLIC_ROLE_NAME,
};
use mz_sql::plan::{PlanContext, PlanNotice, StatementDesc};
use mz_sql::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID, SUPPORT_USER, SYSTEM_USER};
use mz_sql::session::vars::{
    ConnectionCounter, OwnedVarInput, SystemVars, Var, VarInput, CATALOG_KIND_IMPL,
    PERSIST_TXN_TABLES,
};
use mz_sql::{rbac, DEFAULT_SCHEMA};
use mz_sql_parser::ast::QualifiedReplica;
use mz_stash::StashFactory;
use mz_storage_types::connections::inline::{ConnectionResolver, InlinedConnection};
use mz_storage_types::connections::ConnectionContext;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
pub use crate::catalog::builtin_table_updates::BuiltinTableUpdate;
pub use crate::catalog::open::BuiltinMigrationMetadata;
pub use crate::catalog::state::CatalogState;
use crate::command::CatalogDump;
use crate::coord::{ConnMeta, TargetCluster};
use crate::session::{PreparedStatement, Session};
use crate::util::ResultExt;
use crate::{AdapterError, AdapterNotice, ExecuteResponse};

mod builtin_table_updates;
pub(crate) mod consistency;
mod migrate;

mod inner;
mod open;
mod state;

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
            storage: Arc::clone(&self.storage),
            transient_revision: self.transient_revision,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct CatalogPlans {
    optimized_plan_by_id: BTreeMap<GlobalId, DataflowDescription<OptimizedMirRelationExpr>>,
    physical_plan_by_id: BTreeMap<GlobalId, DataflowDescription<mz_compute_types::plan::Plan>>,
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
        self.plans.optimized_plan_by_id.insert(id, plan);
    }

    /// Set the optimized plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn set_physical_plan(
        &mut self,
        id: GlobalId,
        plan: DataflowDescription<mz_compute_types::plan::Plan>,
    ) {
        self.plans.physical_plan_by_id.insert(id, plan);
    }

    /// Try to get the optimized plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn try_get_optimized_plan(
        &self,
        id: &GlobalId,
    ) -> Option<&DataflowDescription<OptimizedMirRelationExpr>> {
        self.plans.optimized_plan_by_id.get(id)
    }

    /// Try to get the optimized plan for the item identified by `id`.
    #[mz_ore::instrument(level = "trace")]
    pub fn try_get_physical_plan(
        &self,
        id: &GlobalId,
    ) -> Option<&DataflowDescription<mz_compute_types::plan::Plan>> {
        self.plans.physical_plan_by_id.get(id)
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
                for n in metainfo.optimizer_notices.drain(..) {
                    // Remove the corresponding notices_by_dep_id entries.
                    for dep_id in n.dependencies.iter() {
                        if let Some(notices) = self.plans.notices_by_dep_id.get_mut(dep_id) {
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
                            metainfo.optimizer_notices.retain(|x| &n != x)
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
                format!(
                    "(id = {}, kind = {:?}, deps = {:?}, strong_count = {})",
                    n.id,
                    n.kind,
                    n.dependencies,
                    Arc::strong_count(n)
                )
            });
            let bad_notices = bracketed("{", "}", separated(", ", bad_notices));
            soft_panic_or_log!(
                "all dropped_notices entries have `Arc::strong_count(_) == 1`; \
                 bad_notices = {bad_notices}; \
                 drop_ids = {drop_ids:?}"
            );
        }

        return dropped_notices;
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
    /// Note that uses of this should field should be used by short-lived
    /// catalogs.
    unresolvable_ids: BTreeSet<GlobalId>,
    conn_id: ConnectionId,
    cluster: String,
    database: Option<DatabaseId>,
    search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    role_id: RoleId,
    prepared_statements: Option<&'a BTreeMap<String, PreparedStatement>>,
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
    pub fn mark_id_unresolvable_for_replanning(&mut self, id: GlobalId) {
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
        id: GlobalId,
    ) -> mz_storage_types::connections::Connection<InlinedConnection> {
        self.state().resolve_connection(id)
    }
}

pub struct TransactionResult {
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
    pub audit_events: Vec<VersionedEvent>,
}

impl Catalog {
    fn resolve_builtin_type(
        builtin: &BuiltinType<NameReference>,
        name_to_id_map: &BTreeMap<&str, GlobalId>,
    ) -> BuiltinType<IdReference> {
        let typ: CatalogType<IdReference> = match &builtin.details.typ {
            CatalogType::AclItem => CatalogType::AclItem,
            CatalogType::Array { element_reference } => CatalogType::Array {
                element_reference: name_to_id_map[element_reference],
            },
            CatalogType::List {
                element_reference,
                element_modifiers,
            } => CatalogType::List {
                element_reference: name_to_id_map[element_reference],
                element_modifiers: element_modifiers.clone(),
            },
            CatalogType::Map {
                key_reference,
                value_reference,
                key_modifiers,
                value_modifiers,
            } => CatalogType::Map {
                key_reference: name_to_id_map[key_reference],
                value_reference: name_to_id_map[value_reference],
                key_modifiers: key_modifiers.clone(),
                value_modifiers: value_modifiers.clone(),
            },
            CatalogType::Range { element_reference } => CatalogType::Range {
                element_reference: name_to_id_map[element_reference],
            },
            CatalogType::Record { fields } => CatalogType::Record {
                fields: fields
                    .into_iter()
                    .map(|f| CatalogRecordField {
                        name: f.name.clone(),
                        type_reference: name_to_id_map[f.type_reference],
                        type_modifiers: f.type_modifiers.clone(),
                    })
                    .collect(),
            },
            CatalogType::Bool => CatalogType::Bool,
            CatalogType::Bytes => CatalogType::Bytes,
            CatalogType::Char => CatalogType::Char,
            CatalogType::Date => CatalogType::Date,
            CatalogType::Float32 => CatalogType::Float32,
            CatalogType::Float64 => CatalogType::Float64,
            CatalogType::Int16 => CatalogType::Int16,
            CatalogType::Int32 => CatalogType::Int32,
            CatalogType::Int64 => CatalogType::Int64,
            CatalogType::UInt16 => CatalogType::UInt16,
            CatalogType::UInt32 => CatalogType::UInt32,
            CatalogType::UInt64 => CatalogType::UInt64,
            CatalogType::MzTimestamp => CatalogType::MzTimestamp,
            CatalogType::Interval => CatalogType::Interval,
            CatalogType::Jsonb => CatalogType::Jsonb,
            CatalogType::Numeric => CatalogType::Numeric,
            CatalogType::Oid => CatalogType::Oid,
            CatalogType::PgLegacyChar => CatalogType::PgLegacyChar,
            CatalogType::PgLegacyName => CatalogType::PgLegacyName,
            CatalogType::Pseudo => CatalogType::Pseudo,
            CatalogType::RegClass => CatalogType::RegClass,
            CatalogType::RegProc => CatalogType::RegProc,
            CatalogType::RegType => CatalogType::RegType,
            CatalogType::String => CatalogType::String,
            CatalogType::Time => CatalogType::Time,
            CatalogType::Timestamp => CatalogType::Timestamp,
            CatalogType::TimestampTz => CatalogType::TimestampTz,
            CatalogType::Uuid => CatalogType::Uuid,
            CatalogType::VarChar => CatalogType::VarChar,
            CatalogType::Int2Vector => CatalogType::Int2Vector,
            CatalogType::MzAclItem => CatalogType::MzAclItem,
        };

        BuiltinType {
            name: builtin.name,
            schema: builtin.schema,
            oid: builtin.oid,
            details: CatalogTypeDetails {
                array_id: builtin.details.array_id,
                typ,
                pg_metadata: builtin.details.pg_metadata.clone(),
            },
        }
    }

    /// Returns the catalog's transient revision, which starts at 1 and is
    /// incremented on every change. This is not persisted to disk, and will
    /// restart on every load.
    pub fn transient_revision(&self) -> u64 {
        self.transient_revision
    }

    /// Creates a debug catalog from the current
    /// `COCKROACH_URL` with parameters set appropriately for debug contexts,
    /// like in tests.
    ///
    /// WARNING! This function can arbitrarily fail because it does not make any
    /// effort to adjust the catalog's contents' structure or semantics to the
    /// currently running version, i.e. it does not apply any migrations.
    ///
    /// This function must not be called in production contexts. Use
    /// [`Catalog::open`] with appropriately set configuration parameters
    /// instead.
    pub async fn with_debug<F, Fut, T>(now: NowFn, f: F) -> T
    where
        F: FnOnce(Catalog) -> Fut,
        Fut: Future<Output = T>,
    {
        let persist_client = PersistClient::new_for_tests().await;
        let environmentd_id = Uuid::new_v4();
        let catalog =
            match Self::open_debug_catalog(persist_client, environmentd_id, now, None).await {
                Ok(catalog) => catalog,
                Err(err) => {
                    panic!("unable to open debug stash: {err}");
                }
            };
        f(catalog).await
    }

    /// Opens a debug catalog.
    ///
    /// See [`Catalog::with_debug`].
    pub async fn open_debug_catalog(
        persist_client: PersistClient,
        organization_id: Uuid,
        now: NowFn,
        environment_id: Option<EnvironmentId>,
    ) -> Result<Catalog, anyhow::Error> {
        let openable_storage = Box::new(
            mz_catalog::durable::test_persist_backed_catalog_state(persist_client, organization_id)
                .await,
        );
        let storage = openable_storage
            .open(now(), &test_bootstrap_args(), None, None)
            .await?;
        Self::open_debug_catalog_inner(storage, now, environment_id).await
    }

    /// Opens a debug stash backed catalog at `url`, using `schema` as the connection's search_path.
    ///
    /// See [`Catalog::with_debug`].
    pub async fn open_debug_stash_catalog_url(
        url: String,
        schema: String,
        now: NowFn,
        environment_id: Option<EnvironmentId>,
    ) -> Result<Catalog, anyhow::Error> {
        let tls = mz_tls_util::make_tls(&tokio_postgres::Config::new())
            .expect("unable to create TLS connector");
        let factory = StashFactory::new(&MetricsRegistry::new());
        let stash_config = StashConfig {
            stash_factory: factory,
            stash_url: url,
            schema: Some(schema),
            tls,
        };
        let openable_storage = Box::new(mz_catalog::durable::stash_backed_catalog_state(
            stash_config,
        ));
        let storage = openable_storage
            .open(now(), &test_bootstrap_args(), None, None)
            .await?;
        Self::open_debug_catalog_inner(storage, now, environment_id).await
    }

    /// Opens a read only debug stash backed catalog defined by `stash_config`.
    ///
    /// See [`Catalog::with_debug`].
    pub async fn open_debug_read_only_stash_catalog_config(
        stash_config: StashConfig,
        now: NowFn,
        environment_id: Option<EnvironmentId>,
    ) -> Result<Catalog, anyhow::Error> {
        let openable_storage = Box::new(mz_catalog::durable::stash_backed_catalog_state(
            stash_config,
        ));
        let storage = openable_storage
            .open_read_only(&test_bootstrap_args())
            .await?;
        Self::open_debug_catalog_inner(storage, now, environment_id).await
    }

    /// Opens a read only debug persist backed catalog defined by `persist_client` and
    /// `organization_id`.
    ///
    /// See [`Catalog::with_debug`].
    pub async fn open_debug_read_only_persist_catalog_config(
        persist_client: PersistClient,
        now: NowFn,
        environment_id: EnvironmentId,
    ) -> Result<Catalog, anyhow::Error> {
        let openable_storage = Box::new(
            mz_catalog::durable::test_persist_backed_catalog_state(
                persist_client,
                environment_id.organization_id(),
            )
            .await,
        );
        let storage = openable_storage
            .open_read_only(&test_bootstrap_args())
            .await?;
        Self::open_debug_catalog_inner(storage, now, Some(environment_id)).await
    }

    async fn open_debug_catalog_inner(
        storage: Box<dyn DurableCatalogState>,
        now: NowFn,
        environment_id: Option<EnvironmentId>,
    ) -> Result<Catalog, anyhow::Error> {
        let metrics_registry = &MetricsRegistry::new();
        let active_connection_count = Arc::new(std::sync::Mutex::new(ConnectionCounter::new(0, 0)));
        let secrets_reader = Arc::new(InMemorySecretsController::new());
        // Used as a lower boundary of the boot_ts, but it's ok to use now() for
        // debugging/testing.
        let previous_ts = now().into();
        let (catalog, _, _, _) = Catalog::open(
            Config {
                storage,
                metrics_registry,
                // when debugging, no reaping
                storage_usage_retention_period: None,
                state: StateConfig {
                    unsafe_mode: true,
                    all_features: false,
                    build_info: &DUMMY_BUILD_INFO,
                    environment_id: environment_id.unwrap_or(EnvironmentId::for_tests()),
                    now,
                    skip_migrations: true,
                    cluster_replica_sizes: Default::default(),
                    builtin_cluster_replica_size: "1".into(),
                    system_parameter_defaults: Default::default(),
                    remote_system_parameters: None,
                    availability_zones: vec![],
                    egress_ips: vec![],
                    aws_principal_context: None,
                    aws_privatelink_availability_zones: None,
                    http_host_name: None,
                    connection_context: ConnectionContext::for_tests(secrets_reader),
                    active_connection_count,
                },
            },
            previous_ts,
        )
        .await?;
        Ok(catalog)
    }

    pub fn for_session<'a>(&'a self, session: &'a Session) -> ConnCatalog<'a> {
        self.state.for_session(session)
    }

    pub fn for_sessionless_user(&self, role_id: RoleId) -> ConnCatalog {
        self.state.for_sessionless_user(role_id)
    }

    pub fn for_system_session(&self) -> ConnCatalog {
        self.state.for_system_session()
    }

    async fn storage<'a>(
        &'a self,
    ) -> MutexGuard<'a, Box<dyn mz_catalog::durable::DurableCatalogState>> {
        self.storage.lock().await
    }

    pub async fn allocate_user_id(&self) -> Result<GlobalId, Error> {
        self.storage().await.allocate_user_id().await.err_into()
    }

    #[cfg(test)]
    pub async fn allocate_system_id(&self) -> Result<GlobalId, Error> {
        use mz_ore::collections::CollectionExt;
        self.storage()
            .await
            .allocate_system_ids(1)
            .await
            .map(|ids| ids.into_element())
            .err_into()
    }

    pub async fn allocate_user_cluster_id(&self) -> Result<ClusterId, Error> {
        self.storage()
            .await
            .allocate_user_cluster_id()
            .await
            .err_into()
    }

    pub async fn allocate_user_replica_id(&self) -> Result<ReplicaId, Error> {
        self.storage()
            .await
            .allocate_user_replica_id()
            .await
            .err_into()
    }

    pub fn allocate_oid(&mut self) -> Result<u32, Error> {
        self.state.allocate_oid()
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
    pub fn resolve_builtin_table(&self, builtin: &'static BuiltinTable) -> GlobalId {
        self.state.resolve_builtin_table(builtin)
    }

    /// Resolves a `BuiltinLog`.
    pub fn resolve_builtin_log(&self, builtin: &'static BuiltinLog) -> GlobalId {
        self.state.resolve_builtin_log(builtin)
    }

    /// Resolves a `BuiltinSource`.
    pub fn resolve_builtin_storage_collection(&self, builtin: &'static BuiltinSource) -> GlobalId {
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

    pub fn get_mz_introspections_cluster_id(&self) -> &ClusterId {
        &self.resolve_builtin_cluster(&MZ_INTROSPECTION_CLUSTER).id
    }

    /// Resolves a [`Cluster`] for a TargetCluster.
    pub fn resolve_target_cluster(
        &self,
        target_cluster: TargetCluster,
        session: &Session,
    ) -> Result<&Cluster, AdapterError> {
        match target_cluster {
            TargetCluster::Introspection => {
                Ok(self.resolve_builtin_cluster(&MZ_INTROSPECTION_CLUSTER))
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

    pub fn try_get_entry(&self, id: &GlobalId) -> Option<&CatalogEntry> {
        self.state.try_get_entry(id)
    }

    pub fn get_entry(&self, id: &GlobalId) -> &CatalogEntry {
        self.state.get_entry(id)
    }

    pub fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: &ConnectionId,
    ) -> &Schema {
        self.state.get_schema(database_spec, schema_spec, conn_id)
    }

    pub fn get_mz_catalog_schema_id(&self) -> &SchemaId {
        self.state.get_mz_catalog_schema_id()
    }

    pub fn get_pg_catalog_schema_id(&self) -> &SchemaId {
        self.state.get_pg_catalog_schema_id()
    }

    pub fn get_information_schema_id(&self) -> &SchemaId {
        self.state.get_information_schema_id()
    }

    pub fn get_mz_internal_schema_id(&self) -> &SchemaId {
        self.state.get_mz_internal_schema_id()
    }

    pub fn get_mz_unsafe_schema_id(&self) -> &SchemaId {
        self.state.get_mz_unsafe_schema_id()
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

    pub fn drop_temp_item_ops(&mut self, conn_id: &ConnectionId) -> Vec<Op> {
        let temp_ids = self.state.temporary_schemas[conn_id]
            .items
            .values()
            .cloned()
            .map(ObjectId::Item)
            .collect();
        self.object_dependents(&temp_ids, conn_id)
            .into_iter()
            .map(Op::DropObject)
            .collect()
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
        let seen = BTreeSet::new();
        self.object_dependents_except(object_ids, conn_id, seen)
    }

    pub(crate) fn object_dependents_except(
        &self,
        object_ids: &Vec<ObjectId>,
        conn_id: &ConnectionId,
        mut except: BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        self.state
            .object_dependents(object_ids, conn_id, &mut except)
    }

    pub(crate) fn cluster_replica_dependents(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state
            .cluster_replica_dependents(cluster_id, replica_id, &mut seen)
    }

    /// Gets GlobalIds of temporary items to be created, checks for name collisions
    /// within a connection id.
    fn temporary_ids(
        &self,
        ops: &[Op],
        temporary_drops: BTreeSet<(&ConnectionId, String)>,
    ) -> Result<Vec<GlobalId>, Error> {
        let mut creating = BTreeSet::new();
        let mut temporary_ids = Vec::with_capacity(ops.len());
        for op in ops.iter() {
            if let Op::CreateItem {
                id,
                oid: _,
                name,
                item,
                owner_id: _,
            } = op
            {
                if let Some(conn_id) = item.conn_id() {
                    if self.item_exists_in_temp_schemas(conn_id, &name.item)
                        && !temporary_drops.contains(&(conn_id, name.item.clone()))
                        || creating.contains(&(conn_id, &name.item))
                    {
                        return Err(
                            SqlCatalogError::ItemAlreadyExists(*id, name.item.clone()).into()
                        );
                    } else {
                        creating.insert((conn_id, &name.item));
                        temporary_ids.push(id.clone());
                    }
                }
            }
        }
        Ok(temporary_ids)
    }

    fn should_audit_log_item(item: &CatalogItem) -> bool {
        !item.is_temporary()
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
            },
            SystemObjectId::System => Some(&self.state.system_privileges),
        }
    }

    #[instrument(name = "catalog::transact")]
    pub async fn transact(
        &mut self,
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        ops: Vec<Op>,
    ) -> Result<TransactionResult, AdapterError> {
        trace!("transact: {:?}", ops);
        fail::fail_point!("catalog_transact", |arg| {
            Err(AdapterError::Unstructured(anyhow::anyhow!(
                "failpoint: {arg:?}"
            )))
        });

        let drop_ids: BTreeSet<_> = ops
            .iter()
            .filter_map(|op| match op {
                Op::DropObject(ObjectId::Item(id)) => Some(*id),
                _ => None,
            })
            .collect();
        let temporary_drops = drop_ids
            .iter()
            .filter_map(|id| {
                let entry = self.get_entry(id);
                match entry.item().conn_id() {
                    Some(conn_id) => Some((conn_id, entry.name().item.clone())),
                    None => None,
                }
            })
            .collect();
        let temporary_ids = self.temporary_ids(&ops, temporary_drops)?;
        let mut builtin_table_updates = vec![];
        let mut audit_events = vec![];
        let mut storage = self.storage().await;
        let mut tx = storage.transaction().await?;
        // Prepare a candidate catalog state.
        let mut state = self.state.clone();

        Self::transact_inner(
            oracle_write_ts,
            session,
            ops,
            temporary_ids,
            &mut builtin_table_updates,
            &mut audit_events,
            &mut tx,
            &mut state,
        )?;

        // The user closure was successful, apply the updates. Terminate the
        // process if this fails, because we have to restart envd due to
        // indeterminate catalog state, which we only reconcile during catalog
        // init.
        tx.commit()
            .await
            .unwrap_or_terminate("catalog storage transaction commit must succeed");

        // Dropping here keeps the mutable borrow on self, preventing us accidentally
        // mutating anything until after f is executed.
        drop(storage);
        self.state = state;
        self.transient_revision += 1;

        // Drop in-memory planning metadata.
        let dropped_notices = self.drop_plans_and_metainfos(&drop_ids);
        if self.state.system_config().enable_mz_notices() {
            // Generate retractions for the Builtin tables.
            self.state().pack_optimizer_notices(
                &mut builtin_table_updates,
                dropped_notices.iter(),
                -1,
            );
        }

        Ok(TransactionResult {
            builtin_table_updates,
            audit_events,
        })
    }

    /// Performs the transaction described by `ops`.
    ///
    /// # Panics
    /// - If `ops` contains [`Op::TransactionDryRun`] and the value is not the
    ///   final element.
    /// - If the only element of `ops` is [`Op::TransactionDryRun`].
    #[instrument(name = "catalog::transact_inner")]
    fn transact_inner(
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        mut ops: Vec<Op>,
        temporary_ids: Vec<GlobalId>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        tx: &mut Transaction<'_>,
        state: &mut CatalogState,
    ) -> Result<(), AdapterError> {
        let dry_run_ops = match ops.last() {
            Some(Op::TransactionDryRun) => {
                // Remove dry run marker.
                ops.pop();
                assert!(!ops.is_empty(), "TransactionDryRun must not be the only op");
                ops.clone()
            }
            Some(_) => vec![],
            None => return Ok(()),
        };

        for op in ops {
            match op {
                Op::TransactionDryRun => {
                    unreachable!("TransactionDryRun can only be used a final element of ops")
                }
                Op::AlterRole {
                    id,
                    name,
                    attributes,
                    vars,
                } => {
                    state.ensure_not_reserved_role(&id)?;
                    if let Some(builtin_update) = state.pack_role_update(id, -1) {
                        builtin_table_updates.push(builtin_update);
                    }
                    let existing_role = state.get_role_mut(&id);
                    existing_role.attributes = attributes;
                    existing_role.vars = vars;
                    tx.update_role(id, existing_role.clone().into())?;
                    if let Some(builtin_update) = state.pack_role_update(id, 1) {
                        builtin_table_updates.push(builtin_update);
                    }

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        ObjectType::Role,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: id.to_string(),
                            name: name.clone(),
                        }),
                    )?;

                    info!("update role {name} ({id})");
                }
                Op::AlterSetCluster { id, cluster } => Self::transact_alter_set_cluster(
                    state,
                    tx,
                    builtin_table_updates,
                    oracle_write_ts,
                    audit_events,
                    session,
                    id,
                    cluster,
                )?,
                Op::CreateDatabase {
                    name,
                    oid,
                    public_schema_oid,
                    owner_id,
                } => {
                    let database_owner_privileges = vec![rbac::owner_privilege(
                        mz_sql::catalog::ObjectType::Database,
                        owner_id,
                    )];
                    let database_default_privileges = state
                        .default_privileges
                        .get_applicable_privileges(
                            owner_id,
                            None,
                            None,
                            mz_sql::catalog::ObjectType::Database,
                        )
                        .map(|item| item.mz_acl_item(owner_id));
                    let database_privileges: Vec<_> = merge_mz_acl_items(
                        database_owner_privileges
                            .into_iter()
                            .chain(database_default_privileges),
                    )
                    .collect();

                    let schema_owner_privileges = vec![rbac::owner_privilege(
                        mz_sql::catalog::ObjectType::Schema,
                        owner_id,
                    )];
                    let schema_default_privileges = state
                        .default_privileges
                        .get_applicable_privileges(
                            owner_id,
                            None,
                            None,
                            mz_sql::catalog::ObjectType::Schema,
                        )
                        .map(|item| item.mz_acl_item(owner_id))
                        // Special default privilege on public schemas.
                        .chain(std::iter::once(MzAclItem {
                            grantee: RoleId::Public,
                            grantor: owner_id,
                            acl_mode: AclMode::USAGE,
                        }));
                    let schema_privileges: Vec<_> = merge_mz_acl_items(
                        schema_owner_privileges
                            .into_iter()
                            .chain(schema_default_privileges),
                    )
                    .collect();

                    let database_id =
                        tx.insert_user_database(&name, owner_id, database_privileges.clone())?;
                    let schema_id = tx.insert_user_schema(
                        database_id,
                        DEFAULT_SCHEMA,
                        owner_id,
                        schema_privileges.clone(),
                    )?;
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Database,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: database_id.to_string(),
                            name: name.clone(),
                        }),
                    )?;
                    info!("create database {}", name);
                    state.database_by_id.insert(
                        database_id.clone(),
                        Database {
                            name: name.clone(),
                            id: database_id.clone(),
                            oid,
                            schemas_by_id: BTreeMap::new(),
                            schemas_by_name: BTreeMap::new(),
                            owner_id,
                            privileges: PrivilegeMap::from_mz_acl_items(database_privileges),
                        },
                    );
                    state
                        .database_by_name
                        .insert(name.clone(), database_id.clone());
                    builtin_table_updates
                        .push(state.pack_database_update(&state.database_by_id[&database_id], 1));

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Schema,
                        EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
                            id: schema_id.to_string(),
                            name: DEFAULT_SCHEMA.to_string(),
                            database_name: Some(name),
                        }),
                    )?;
                    Self::create_schema(
                        state,
                        builtin_table_updates,
                        schema_id,
                        public_schema_oid,
                        database_id,
                        DEFAULT_SCHEMA.to_string(),
                        owner_id,
                        PrivilegeMap::from_mz_acl_items(schema_privileges),
                    )?;
                }
                Op::CreateSchema {
                    database_id,
                    schema_name,
                    oid,
                    owner_id,
                } => {
                    if is_reserved_name(&schema_name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedSchemaName(schema_name),
                        )));
                    }
                    let database_id = match database_id {
                        ResolvedDatabaseSpecifier::Id(id) => id,
                        ResolvedDatabaseSpecifier::Ambient => {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlySystemSchema(schema_name),
                            )));
                        }
                    };
                    let owner_privileges = vec![rbac::owner_privilege(
                        mz_sql::catalog::ObjectType::Schema,
                        owner_id,
                    )];
                    let default_privileges = state
                        .default_privileges
                        .get_applicable_privileges(
                            owner_id,
                            Some(database_id),
                            None,
                            mz_sql::catalog::ObjectType::Schema,
                        )
                        .map(|item| item.mz_acl_item(owner_id));
                    let privileges: Vec<_> =
                        merge_mz_acl_items(owner_privileges.into_iter().chain(default_privileges))
                            .collect();
                    let schema_id = tx.insert_user_schema(
                        database_id,
                        &schema_name,
                        owner_id,
                        privileges.clone(),
                    )?;
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Schema,
                        EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
                            id: schema_id.to_string(),
                            name: schema_name.clone(),
                            database_name: Some(state.database_by_id[&database_id].name.clone()),
                        }),
                    )?;
                    Self::create_schema(
                        state,
                        builtin_table_updates,
                        schema_id,
                        oid,
                        database_id,
                        schema_name,
                        owner_id,
                        PrivilegeMap::from_mz_acl_items(privileges),
                    )?;
                }
                Op::CreateRole {
                    name,
                    oid,
                    attributes,
                } => {
                    if is_reserved_role_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedRoleName(name),
                        )));
                    }
                    let membership = RoleMembership::new();
                    let vars = RoleVars::default();
                    let id = tx.insert_user_role(
                        name.clone(),
                        attributes.clone(),
                        membership.clone(),
                        vars.clone(),
                    )?;
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Role,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: id.to_string(),
                            name: name.clone(),
                        }),
                    )?;
                    info!("create role {}", name);
                    state.roles_by_name.insert(name.clone(), id);
                    state.roles_by_id.insert(
                        id,
                        Role {
                            name,
                            id,
                            oid,
                            attributes,
                            membership,
                            vars,
                        },
                    );
                    if let Some(builtin_update) = state.pack_role_update(id, 1) {
                        builtin_table_updates.push(builtin_update);
                    }
                }
                Op::CreateCluster {
                    id,
                    name,
                    introspection_sources,
                    owner_id,
                    config,
                } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedClusterName(name),
                        )));
                    }
                    let owner_privileges = vec![rbac::owner_privilege(
                        mz_sql::catalog::ObjectType::Cluster,
                        owner_id,
                    )];
                    let default_privileges = state
                        .default_privileges
                        .get_applicable_privileges(
                            owner_id,
                            None,
                            None,
                            mz_sql::catalog::ObjectType::Cluster,
                        )
                        .map(|item| item.mz_acl_item(owner_id));
                    let privileges: Vec<_> =
                        merge_mz_acl_items(owner_privileges.into_iter().chain(default_privileges))
                            .collect();

                    tx.insert_user_cluster(
                        id,
                        &name,
                        introspection_sources.clone(),
                        owner_id,
                        privileges.clone(),
                        config.clone().into(),
                    )?;
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Cluster,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: id.to_string(),
                            name: name.clone(),
                        }),
                    )?;
                    info!("create cluster {}", name);
                    let introspection_source_ids: Vec<GlobalId> =
                        introspection_sources.iter().map(|(_, id)| *id).collect();
                    state.insert_cluster(
                        id,
                        name.clone(),
                        introspection_sources,
                        owner_id,
                        PrivilegeMap::from_mz_acl_items(privileges),
                        config,
                    );
                    builtin_table_updates.push(state.pack_cluster_update(&name, 1));
                    for id in introspection_source_ids {
                        builtin_table_updates.extend(state.pack_item_update(id, 1));
                    }
                }
                Op::CreateClusterReplica {
                    cluster_id,
                    id,
                    name,
                    config,
                    owner_id,
                } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedReplicaName(name),
                        )));
                    }
                    let cluster = state.get_cluster(cluster_id);
                    if cluster_id.is_system()
                        && !session
                            .map(|session| session.user().is_internal())
                            .unwrap_or(false)
                    {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlyCluster(cluster.name.clone()),
                        )));
                    }
                    tx.insert_cluster_replica(
                        cluster_id,
                        id,
                        &name,
                        config.clone().into(),
                        owner_id,
                    )?;
                    if let ReplicaLocation::Managed(ManagedReplicaLocation {
                        size,
                        disk,
                        billed_as,
                        internal,
                        ..
                    }) = &config.location
                    {
                        let details = EventDetails::CreateClusterReplicaV1(
                            mz_audit_log::CreateClusterReplicaV1 {
                                cluster_id: cluster_id.to_string(),
                                cluster_name: cluster.name.clone(),
                                replica_id: Some(id.to_string()),
                                replica_name: name.clone(),
                                logical_size: size.clone(),
                                disk: *disk,
                                billed_as: billed_as.clone(),
                                internal: *internal,
                            },
                        );
                        state.add_to_audit_log(
                            oracle_write_ts,
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Create,
                            ObjectType::ClusterReplica,
                            details,
                        )?;
                    }
                    let num_processes = config.location.num_processes();
                    state.insert_cluster_replica(cluster_id, name.clone(), id, config, owner_id);
                    builtin_table_updates
                        .extend(state.pack_cluster_replica_update(cluster_id, &name, 1));
                    for process_id in 0..num_processes {
                        let update = state.pack_cluster_replica_status_update(
                            cluster_id,
                            id,
                            u64::cast_from(process_id),
                            1,
                        );
                        builtin_table_updates.push(update);
                    }
                }
                Op::CreateItem {
                    id,
                    oid,
                    name,
                    item,
                    owner_id,
                } => {
                    state.check_unstable_dependencies(&item)?;

                    if let Some(id @ ClusterId::System(_)) = item.cluster_id() {
                        let cluster_name = state.clusters_by_id[&id].name.clone();
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlyCluster(cluster_name),
                        )));
                    }

                    let owner_privileges = vec![rbac::owner_privilege(item.typ().into(), owner_id)];
                    let default_privileges = state
                        .default_privileges
                        .get_applicable_privileges(
                            owner_id,
                            name.qualifiers.database_spec.id(),
                            Some(name.qualifiers.schema_spec.into()),
                            item.typ().into(),
                        )
                        .map(|item| item.mz_acl_item(owner_id));
                    // mz_support can read all progress sources.
                    let progress_source_privilege = if item.is_progress_source() {
                        Some(MzAclItem {
                            grantee: MZ_SUPPORT_ROLE_ID,
                            grantor: owner_id,
                            acl_mode: AclMode::SELECT,
                        })
                    } else {
                        None
                    };
                    let privileges: Vec<_> = merge_mz_acl_items(
                        owner_privileges
                            .into_iter()
                            .chain(default_privileges)
                            .chain(progress_source_privilege),
                    )
                    .collect();

                    if item.is_temporary() {
                        if name.qualifiers.database_spec != ResolvedDatabaseSpecifier::Ambient
                            || name.qualifiers.schema_spec != SchemaSpecifier::Temporary
                        {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::InvalidTemporarySchema,
                            )));
                        }
                    } else {
                        if let Some(temp_id) =
                            item.uses().iter().find(|id| match state.try_get_entry(id) {
                                Some(entry) => entry.item().is_temporary(),
                                None => temporary_ids.contains(id),
                            })
                        {
                            let temp_item = state.get_entry(temp_id);
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::InvalidTemporaryDependency(
                                    temp_item.name().item.clone(),
                                ),
                            )));
                        }
                        if let ResolvedDatabaseSpecifier::Ambient = name.qualifiers.database_spec {
                            let schema_name = state
                                .resolve_full_name(&name, session.map(|session| session.conn_id()))
                                .schema;
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlySystemSchema(schema_name),
                            )));
                        }
                        let schema_id = name.qualifiers.schema_spec.clone().into();
                        let serialized_item = item.to_serialized();
                        tx.insert_item(
                            id,
                            schema_id,
                            &name.item,
                            serialized_item,
                            owner_id,
                            privileges.clone(),
                        )?;
                    }

                    if Self::should_audit_log_item(&item) {
                        let name = Self::full_name_detail(
                            &state
                                .resolve_full_name(&name, session.map(|session| session.conn_id())),
                        );
                        let details = match &item {
                            CatalogItem::Source(s) => {
                                EventDetails::CreateSourceSinkV3(mz_audit_log::CreateSourceSinkV3 {
                                    id: id.to_string(),
                                    name,
                                    external_type: s.source_type().to_string(),
                                })
                            }
                            CatalogItem::Sink(s) => {
                                EventDetails::CreateSourceSinkV3(mz_audit_log::CreateSourceSinkV3 {
                                    id: id.to_string(),
                                    name,
                                    external_type: s.sink_type().to_string(),
                                })
                            }
                            _ => EventDetails::IdFullNameV1(IdFullNameV1 {
                                id: id.to_string(),
                                name,
                            }),
                        };
                        state.add_to_audit_log(
                            oracle_write_ts,
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Create,
                            catalog_type_to_audit_object_type(item.typ()),
                            details,
                        )?;
                    }
                    state.insert_item(
                        id,
                        oid,
                        name,
                        item,
                        owner_id,
                        PrivilegeMap::from_mz_acl_items(privileges),
                    );
                    builtin_table_updates.extend(state.pack_item_update(id, 1));
                }
                Op::Comment {
                    object_id,
                    sub_component,
                    comment,
                } => {
                    tx.update_comment(object_id, sub_component, comment.clone())?;
                    let prev_comment =
                        state
                            .comments
                            .update_comment(object_id, sub_component, comment.clone());

                    // If we're replacing or deleting a comment, we need to issue a retraction for
                    // the previous value.
                    if let Some(prev) = prev_comment {
                        builtin_table_updates.push(state.pack_comment_update(
                            object_id,
                            sub_component,
                            &prev,
                            -1,
                        ));
                    }

                    if let Some(new) = comment {
                        builtin_table_updates.push(state.pack_comment_update(
                            object_id,
                            sub_component,
                            &new,
                            1,
                        ));
                    }
                }
                Op::DropObject(id) => {
                    // Drop any associated comments.
                    let comment_id = state.get_comment_id(id.clone());
                    let deleted = tx.drop_comments(comment_id)?;
                    let dropped = state.comments.drop_comments(comment_id);
                    mz_ore::soft_assert_eq_or_log!(
                        deleted,
                        dropped,
                        "transaction and state out of sync"
                    );

                    let updates = dropped.into_iter().map(|(id, col_pos, comment)| {
                        state.pack_comment_update(id, col_pos, &comment, -1)
                    });
                    builtin_table_updates.extend(updates);

                    // Drop the object.
                    match id {
                        ObjectId::Database(id) => {
                            let database = &state.database_by_id[&id];
                            if id.is_system() {
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlyDatabase(database.name().to_string()),
                                )));
                            }
                            tx.remove_database(&id)?;
                            builtin_table_updates.push(state.pack_database_update(database, -1));
                            state.add_to_audit_log(
                                oracle_write_ts,
                                session,
                                tx,
                                builtin_table_updates,
                                audit_events,
                                EventType::Drop,
                                ObjectType::Database,
                                EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                                    id: id.to_string(),
                                    name: database.name.clone(),
                                }),
                            )?;
                            let db = state.database_by_id.get(&id).expect("catalog out of sync");
                            state.database_by_name.remove(db.name());
                            state.database_by_id.remove(&id);
                        }
                        ObjectId::Schema((database_spec, schema_spec)) => {
                            let schema = state.get_schema(
                                &database_spec,
                                &schema_spec,
                                session
                                    .map(|session| session.conn_id())
                                    .unwrap_or(&SYSTEM_CONN_ID),
                            );
                            let database_id = match database_spec {
                                ResolvedDatabaseSpecifier::Ambient => None,
                                ResolvedDatabaseSpecifier::Id(database_id) => Some(database_id),
                            };
                            let schema_id: SchemaId = schema_spec.into();
                            if schema_id.is_system() {
                                let name = schema.name();
                                let full_name = state.resolve_full_schema_name(name);
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlySystemSchema(full_name.to_string()),
                                )));
                            }
                            tx.remove_schema(&database_id, &schema_id)?;
                            builtin_table_updates.push(state.pack_schema_update(
                                &database_spec,
                                &schema_id,
                                -1,
                            ));
                            state.add_to_audit_log(
                                oracle_write_ts,
                                session,
                                tx,
                                builtin_table_updates,
                                audit_events,
                                EventType::Drop,
                                ObjectType::Schema,
                                EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
                                    id: schema_id.to_string(),
                                    name: schema.name.schema.to_string(),
                                    database_name: database_id.map(|database_id| {
                                        state.database_by_id[&database_id].name.clone()
                                    }),
                                }),
                            )?;
                            if let ResolvedDatabaseSpecifier::Id(database_id) = database_spec {
                                let db = state
                                    .database_by_id
                                    .get_mut(&database_id)
                                    .expect("catalog out of sync");
                                let schema = &db.schemas_by_id[&schema_id];
                                db.schemas_by_name.remove(&schema.name.schema);
                                db.schemas_by_id.remove(&schema_id);
                            }
                        }
                        ObjectId::Role(id) => {
                            let name = state.get_role(&id).name().to_string();
                            state.ensure_not_reserved_role(&id)?;
                            tx.remove_role(&name)?;
                            if let Some(builtin_update) = state.pack_role_update(id, -1) {
                                builtin_table_updates.push(builtin_update);
                            }
                            let role = state.roles_by_id.remove(&id).expect("catalog out of sync");
                            state.roles_by_name.remove(role.name());
                            state.add_to_audit_log(
                                oracle_write_ts,
                                session,
                                tx,
                                builtin_table_updates,
                                audit_events,
                                EventType::Drop,
                                ObjectType::Role,
                                EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                                    id: role.id.to_string(),
                                    name: name.clone(),
                                }),
                            )?;
                            info!("drop role {}", role.name());
                        }
                        ObjectId::Cluster(id) => {
                            let cluster = state.get_cluster(id);
                            let name = &cluster.name;
                            if id.is_system() {
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlyCluster(name.clone()),
                                )));
                            }
                            tx.remove_cluster(id)?;
                            builtin_table_updates.push(state.pack_cluster_update(name, -1));
                            for id in cluster.log_indexes.values() {
                                builtin_table_updates.extend(state.pack_item_update(*id, -1));
                            }
                            state.add_to_audit_log(
                                oracle_write_ts,
                                session,
                                tx,
                                builtin_table_updates,
                                audit_events,
                                EventType::Drop,
                                ObjectType::Cluster,
                                EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                                    id: cluster.id.to_string(),
                                    name: name.clone(),
                                }),
                            )?;
                            let cluster = state
                                .clusters_by_id
                                .remove(&id)
                                .expect("can only drop known clusters");
                            state.clusters_by_name.remove(&cluster.name);

                            for id in cluster.log_indexes.values() {
                                state.drop_item(*id);
                            }

                            assert!(
                                cluster.bound_objects.is_empty()
                                    && cluster.replicas().next().is_none(),
                                "not all items dropped before cluster"
                            );
                        }
                        ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                            let cluster = state.get_cluster(cluster_id);
                            let replica = cluster.replica(replica_id).expect("Must exist");
                            if is_reserved_name(&replica.name) {
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReservedReplicaName(replica.name.clone()),
                                )));
                            }
                            if cluster_id.is_system()
                                && !session
                                    .map(|session| session.user().is_internal())
                                    .unwrap_or(false)
                            {
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlyCluster(cluster.name.clone()),
                                )));
                            }
                            tx.remove_cluster_replica(replica_id)?;

                            for process_id in replica.process_status.keys() {
                                let update = state.pack_cluster_replica_status_update(
                                    cluster_id,
                                    replica_id,
                                    *process_id,
                                    -1,
                                );
                                builtin_table_updates.push(update);
                            }

                            builtin_table_updates.extend(state.pack_cluster_replica_update(
                                cluster_id,
                                &replica.name,
                                -1,
                            ));

                            let details = EventDetails::DropClusterReplicaV1(
                                mz_audit_log::DropClusterReplicaV1 {
                                    cluster_id: cluster_id.to_string(),
                                    cluster_name: cluster.name.clone(),
                                    replica_id: Some(replica_id.to_string()),
                                    replica_name: replica.name.clone(),
                                },
                            );
                            state.add_to_audit_log(
                                oracle_write_ts,
                                session,
                                tx,
                                builtin_table_updates,
                                audit_events,
                                EventType::Drop,
                                ObjectType::ClusterReplica,
                                details,
                            )?;

                            let cluster = state
                                .clusters_by_id
                                .get_mut(&cluster_id)
                                .expect("can only drop replicas from known instances");
                            cluster.remove_replica(replica_id);
                        }
                        ObjectId::Item(id) => {
                            let entry = state.get_entry(&id);
                            if id.is_system() {
                                let name = entry.name();
                                let full_name = state.resolve_full_name(
                                    name,
                                    session.map(|session| session.conn_id()),
                                );
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlyItem(full_name.to_string()),
                                )));
                            }
                            if !entry.item().is_temporary() {
                                tx.remove_item(id)?;
                            }

                            builtin_table_updates.extend(state.pack_item_update(id, -1));
                            if Self::should_audit_log_item(entry.item()) {
                                state.add_to_audit_log(
                                    oracle_write_ts,
                                    session,
                                    tx,
                                    builtin_table_updates,
                                    audit_events,
                                    EventType::Drop,
                                    catalog_type_to_audit_object_type(entry.item().typ()),
                                    EventDetails::IdFullNameV1(IdFullNameV1 {
                                        id: id.to_string(),
                                        name: Self::full_name_detail(&state.resolve_full_name(
                                            entry.name(),
                                            session.map(|session| session.conn_id()),
                                        )),
                                    }),
                                )?;
                            }
                            state.drop_item(id);
                        }
                    }
                }
                Op::GrantRole {
                    role_id,
                    member_id,
                    grantor_id,
                } => {
                    state.ensure_not_reserved_role(&member_id)?;
                    state.ensure_grantable_role(&role_id)?;
                    if state.collect_role_membership(&role_id).contains(&member_id) {
                        let group_role = state.get_role(&role_id);
                        let member_role = state.get_role(&member_id);
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::CircularRoleMembership {
                                role_name: group_role.name().to_string(),
                                member_name: member_role.name().to_string(),
                            },
                        )));
                    }
                    let member_role = state.get_role_mut(&member_id);
                    member_role.membership.map.insert(role_id, grantor_id);
                    tx.update_role(member_id, member_role.clone().into())?;
                    builtin_table_updates
                        .push(state.pack_role_members_update(role_id, member_id, 1));

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Grant,
                        ObjectType::Role,
                        EventDetails::GrantRoleV2(mz_audit_log::GrantRoleV2 {
                            role_id: role_id.to_string(),
                            member_id: member_id.to_string(),
                            grantor_id: grantor_id.to_string(),
                            executed_by: session
                                .map(|session| session.authenticated_role_id())
                                .unwrap_or(&MZ_SYSTEM_ROLE_ID)
                                .to_string(),
                        }),
                    )?;
                }
                Op::RevokeRole {
                    role_id,
                    member_id,
                    grantor_id,
                } => {
                    state.ensure_not_reserved_role(&member_id)?;
                    state.ensure_grantable_role(&role_id)?;
                    builtin_table_updates
                        .push(state.pack_role_members_update(role_id, member_id, -1));
                    let member_role = state.get_role_mut(&member_id);
                    member_role.membership.map.remove(&role_id);
                    tx.update_role(member_id, member_role.clone().into())?;

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Revoke,
                        ObjectType::Role,
                        EventDetails::RevokeRoleV2(mz_audit_log::RevokeRoleV2 {
                            role_id: role_id.to_string(),
                            member_id: member_id.to_string(),
                            grantor_id: grantor_id.to_string(),
                            executed_by: session
                                .map(|session| session.authenticated_role_id())
                                .unwrap_or(&MZ_SYSTEM_ROLE_ID)
                                .to_string(),
                        }),
                    )?;
                }
                Op::UpdatePrivilege {
                    target_id,
                    privilege,
                    variant,
                } => {
                    let update_privilege_fn = |privileges: &mut PrivilegeMap| match variant {
                        UpdatePrivilegeVariant::Grant => {
                            privileges.grant(privilege);
                        }
                        UpdatePrivilegeVariant::Revoke => {
                            privileges.revoke(&privilege);
                        }
                    };
                    match &target_id {
                        SystemObjectId::Object(object_id) => match object_id {
                            ObjectId::Cluster(id) => {
                                let cluster_name = state.get_cluster(*id).name().to_string();
                                builtin_table_updates
                                    .push(state.pack_cluster_update(&cluster_name, -1));
                                let cluster = state.get_cluster_mut(*id);
                                update_privilege_fn(&mut cluster.privileges);
                                tx.update_cluster(*id, cluster.clone().into())?;
                                builtin_table_updates
                                    .push(state.pack_cluster_update(&cluster_name, 1));
                            }
                            ObjectId::Database(id) => {
                                let database = state.get_database(id);
                                builtin_table_updates
                                    .push(state.pack_database_update(database, -1));
                                let database = state.get_database_mut(id);
                                update_privilege_fn(&mut database.privileges);
                                let database = state.get_database(id);
                                tx.update_database(*id, database.clone().into())?;
                                builtin_table_updates.push(state.pack_database_update(database, 1));
                            }
                            ObjectId::Schema((database_spec, schema_spec)) => {
                                let schema_id = schema_spec.clone().into();
                                builtin_table_updates.push(state.pack_schema_update(
                                    database_spec,
                                    &schema_id,
                                    -1,
                                ));
                                let schema = state.get_schema_mut(
                                    database_spec,
                                    schema_spec,
                                    session
                                        .map(|session| session.conn_id())
                                        .unwrap_or(&SYSTEM_CONN_ID),
                                );
                                update_privilege_fn(&mut schema.privileges);
                                let database_id = match &database_spec {
                                    ResolvedDatabaseSpecifier::Ambient => None,
                                    ResolvedDatabaseSpecifier::Id(id) => Some(*id),
                                };
                                tx.update_schema(
                                    schema_id,
                                    schema.clone().into_durable_schema(database_id),
                                )?;
                                builtin_table_updates.push(state.pack_schema_update(
                                    database_spec,
                                    &schema_id,
                                    1,
                                ));
                            }
                            ObjectId::Item(id) => {
                                builtin_table_updates.extend(state.pack_item_update(*id, -1));
                                let entry = state.get_entry_mut(id);
                                update_privilege_fn(&mut entry.privileges);
                                if !entry.item().is_temporary() {
                                    tx.update_item(*id, entry.clone().into())?;
                                }
                                builtin_table_updates.extend(state.pack_item_update(*id, 1));
                            }
                            ObjectId::Role(_) | ObjectId::ClusterReplica(_) => {}
                        },
                        SystemObjectId::System => {
                            if let Some(existing_privilege) = state
                                .system_privileges
                                .get_acl_item(&privilege.grantee, &privilege.grantor)
                            {
                                builtin_table_updates.push(
                                    state.pack_system_privileges_update(
                                        existing_privilege.clone(),
                                        -1,
                                    ),
                                );
                            }
                            update_privilege_fn(&mut state.system_privileges);
                            let new_privilege = state
                                .system_privileges
                                .get_acl_item(&privilege.grantee, &privilege.grantor);
                            tx.set_system_privilege(
                                privilege.grantee,
                                privilege.grantor,
                                new_privilege.map(|new_privilege| new_privilege.acl_mode),
                            )?;
                            if let Some(new_privilege) = new_privilege {
                                builtin_table_updates.push(
                                    state.pack_system_privileges_update(new_privilege.clone(), 1),
                                );
                            }
                        }
                    }
                    let object_type = state.get_system_object_type(&target_id);
                    let object_id_str = match &target_id {
                        SystemObjectId::System => "SYSTEM".to_string(),
                        SystemObjectId::Object(id) => id.to_string(),
                    };
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        variant.into(),
                        system_object_type_to_audit_object_type(&object_type),
                        EventDetails::UpdatePrivilegeV1(mz_audit_log::UpdatePrivilegeV1 {
                            object_id: object_id_str,
                            grantee_id: privilege.grantee.to_string(),
                            grantor_id: privilege.grantor.to_string(),
                            privileges: privilege.acl_mode.to_string(),
                        }),
                    )?;
                }
                Op::UpdateDefaultPrivilege {
                    privilege_object,
                    privilege_acl_item,
                    variant,
                } => {
                    if let Some(acl_mode) = state
                        .default_privileges
                        .get_privileges_for_grantee(&privilege_object, &privilege_acl_item.grantee)
                    {
                        builtin_table_updates.push(state.pack_default_privileges_update(
                            &privilege_object,
                            &privilege_acl_item.grantee,
                            acl_mode,
                            -1,
                        ));
                    }
                    match variant {
                        UpdatePrivilegeVariant::Grant => state
                            .default_privileges
                            .grant(privilege_object.clone(), privilege_acl_item.clone()),
                        UpdatePrivilegeVariant::Revoke => state
                            .default_privileges
                            .revoke(&privilege_object, &privilege_acl_item),
                    }
                    let new_acl_mode = state
                        .default_privileges
                        .get_privileges_for_grantee(&privilege_object, &privilege_acl_item.grantee);
                    tx.set_default_privilege(
                        privilege_object.role_id,
                        privilege_object.database_id,
                        privilege_object.schema_id,
                        privilege_object.object_type,
                        privilege_acl_item.grantee,
                        new_acl_mode.cloned(),
                    )?;
                    if let Some(new_acl_mode) = new_acl_mode {
                        builtin_table_updates.push(state.pack_default_privileges_update(
                            &privilege_object,
                            &privilege_acl_item.grantee,
                            new_acl_mode,
                            1,
                        ));
                    }
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        variant.into(),
                        object_type_to_audit_object_type(privilege_object.object_type),
                        EventDetails::AlterDefaultPrivilegeV1(
                            mz_audit_log::AlterDefaultPrivilegeV1 {
                                role_id: privilege_object.role_id.to_string(),
                                database_id: privilege_object.database_id.map(|id| id.to_string()),
                                schema_id: privilege_object.schema_id.map(|id| id.to_string()),
                                grantee_id: privilege_acl_item.grantee.to_string(),
                                privileges: privilege_acl_item.acl_mode.to_string(),
                            },
                        ),
                    )?;
                }
                Op::RenameCluster {
                    id,
                    name,
                    to_name,
                    check_reserved_names,
                } => {
                    if id.is_system() {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlyCluster(name.clone()),
                        )));
                    }
                    if check_reserved_names && is_reserved_name(&to_name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedClusterName(to_name),
                        )));
                    }
                    tx.rename_cluster(id, &name, &to_name)?;
                    builtin_table_updates.push(state.pack_cluster_update(&name, -1));
                    state.rename_cluster(id, to_name.clone());
                    builtin_table_updates.push(state.pack_cluster_update(&to_name, 1));
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        ObjectType::Cluster,
                        EventDetails::RenameClusterV1(mz_audit_log::RenameClusterV1 {
                            id: id.to_string(),
                            old_name: name.clone(),
                            new_name: to_name.clone(),
                        }),
                    )?;
                    info!("rename cluster {name} to {to_name}");
                }
                Op::RenameClusterReplica {
                    cluster_id,
                    replica_id,
                    name,
                    to_name,
                } => {
                    if cluster_id.is_system() {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlyCluster(name.cluster.into_string()),
                        )));
                    }
                    if is_reserved_name(&to_name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedClusterName(to_name),
                        )));
                    }
                    tx.rename_cluster_replica(replica_id, &name, &to_name)?;
                    builtin_table_updates.extend(state.pack_cluster_replica_update(
                        cluster_id,
                        name.replica.as_str(),
                        -1,
                    ));
                    state.rename_cluster_replica(cluster_id, replica_id, to_name.clone());
                    builtin_table_updates
                        .extend(state.pack_cluster_replica_update(cluster_id, &to_name, 1));
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        ObjectType::ClusterReplica,
                        EventDetails::RenameClusterReplicaV1(
                            mz_audit_log::RenameClusterReplicaV1 {
                                cluster_id: cluster_id.to_string(),
                                replica_id: replica_id.to_string(),
                                old_name: name.replica.as_str().to_string(),
                                new_name: to_name.clone(),
                            },
                        ),
                    )?;
                    info!("rename cluster replica {name} to {to_name}");
                }
                Op::RenameItem {
                    id,
                    to_name,
                    current_full_name,
                } => {
                    let mut updates = Vec::new();

                    let entry = state.get_entry(&id);
                    if let CatalogItem::Type(_) = entry.item() {
                        return Err(AdapterError::Catalog(Error::new(ErrorKind::TypeRename(
                            current_full_name.to_string(),
                        ))));
                    }

                    if entry.id().is_system() {
                        let schema_name = state
                            .resolve_full_name(
                                entry.name(),
                                session.map(|session| session.conn_id()),
                            )
                            .schema;
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlySystemSchema(schema_name),
                        )));
                    }

                    let mut to_full_name = current_full_name.clone();
                    to_full_name.item = to_name.clone();

                    let mut to_qualified_name = entry.name().clone();
                    to_qualified_name.item = to_name.clone();

                    let details = EventDetails::RenameItemV1(mz_audit_log::RenameItemV1 {
                        id: id.to_string(),
                        old_name: Self::full_name_detail(&current_full_name),
                        new_name: Self::full_name_detail(&to_full_name),
                    });
                    if Self::should_audit_log_item(entry.item()) {
                        state.add_to_audit_log(
                            oracle_write_ts,
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Alter,
                            catalog_type_to_audit_object_type(entry.item().typ()),
                            details,
                        )?;
                    }

                    // Rename item itself.
                    let mut new_entry = entry.clone();
                    new_entry.name.item = to_name.clone();
                    new_entry.item = entry
                        .item()
                        .rename_item_refs(
                            current_full_name.clone(),
                            to_full_name.item.clone(),
                            true,
                        )
                        .map_err(|e| {
                            Error::new(ErrorKind::from(AmbiguousRename {
                                depender: state
                                    .resolve_full_name(entry.name(), entry.conn_id())
                                    .to_string(),
                                dependee: state
                                    .resolve_full_name(entry.name(), entry.conn_id())
                                    .to_string(),
                                message: e,
                            }))
                        })?;

                    for id in entry.referenced_by() {
                        let dependent_item = state.get_entry(id);
                        let mut to_entry = dependent_item.clone();
                        to_entry.item = dependent_item
                            .item()
                            .rename_item_refs(
                                current_full_name.clone(),
                                to_full_name.item.clone(),
                                false,
                            )
                            .map_err(|e| {
                                Error::new(ErrorKind::from(AmbiguousRename {
                                    depender: state
                                        .resolve_full_name(
                                            dependent_item.name(),
                                            dependent_item.conn_id(),
                                        )
                                        .to_string(),
                                    dependee: state
                                        .resolve_full_name(entry.name(), entry.conn_id())
                                        .to_string(),
                                    message: e,
                                }))
                            })?;

                        if !new_entry.item().is_temporary() {
                            tx.update_item(*id, to_entry.clone().into())?;
                        }
                        builtin_table_updates.extend(state.pack_item_update(*id, -1));

                        updates.push((id.clone(), dependent_item.name().clone(), to_entry.item));
                    }
                    if !new_entry.item().is_temporary() {
                        tx.update_item(id, new_entry.clone().into())?;
                    }
                    builtin_table_updates.extend(state.pack_item_update(id, -1));
                    updates.push((id, to_qualified_name, new_entry.item));
                    for (id, to_name, to_item) in updates {
                        Self::update_item(state, builtin_table_updates, id, to_name, to_item)?;
                    }
                }
                Op::RenameSchema {
                    database_spec,
                    schema_spec,
                    new_name,
                    check_reserved_names,
                } => {
                    if check_reserved_names && is_reserved_name(&new_name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedSchemaName(new_name),
                        )));
                    }

                    let conn_id = session
                        .map(|session| session.conn_id())
                        .unwrap_or(&SYSTEM_CONN_ID);

                    let schema = state.get_schema(&database_spec, &schema_spec, conn_id);
                    let cur_name = schema.name().schema.clone();

                    let ResolvedDatabaseSpecifier::Id(database_id) = database_spec else {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::AmbientSchemaRename(cur_name),
                        )));
                    };
                    let database = state.get_database(&database_id);
                    let database_name = &database.name;

                    let mut updates = Vec::new();
                    let mut already_updated = HashSet::new();

                    let mut update_item = |id| {
                        if already_updated.contains(id) {
                            return Ok(());
                        }

                        let entry = state.get_entry(id);

                        // Update our item.
                        let mut new_entry = entry.clone();
                        new_entry.item = entry
                            .item
                            .rename_schema_refs(database_name, &cur_name, &new_name)
                            .map_err(|(s, _i)| {
                                Error::new(ErrorKind::from(AmbiguousRename {
                                    depender: state
                                        .resolve_full_name(entry.name(), entry.conn_id())
                                        .to_string(),
                                    dependee: format!("{database_name}.{cur_name}"),
                                    message: format!("ambiguous reference to schema named {s}"),
                                }))
                            })?;

                        // Update the catalog storage and Builtin Tables.
                        if !new_entry.item().is_temporary() {
                            tx.update_item(*id, new_entry.clone().into())?;
                        }
                        builtin_table_updates.extend(state.pack_item_update(*id, -1));
                        updates.push((id.clone(), entry.name().clone(), new_entry.item));

                        // Track which IDs we update.
                        already_updated.insert(id);

                        Ok::<_, AdapterError>(())
                    };

                    // Update all of the items in the schema.
                    for (_name, item_id) in &schema.items {
                        // Update the item itself.
                        update_item(item_id)?;

                        // Update everything that depends on this item.
                        for id in state.get_entry(item_id).referenced_by() {
                            update_item(id)?;
                        }
                    }

                    // Renaming temporary schemas is not supported.
                    let SchemaSpecifier::Id(schema_id) = *schema.id() else {
                        let schema_name = schema.name().schema.clone();
                        return Err(AdapterError::Catalog(crate::catalog::Error::new(
                            crate::catalog::ErrorKind::ReadOnlySystemSchema(schema_name),
                        )));
                    };
                    // Delete the old schema from the builtin table.
                    builtin_table_updates.push(state.pack_schema_update(
                        &database_spec,
                        &schema_id,
                        -1,
                    ));

                    // Add an entry to the audit log.
                    let database_name = database_spec
                        .id()
                        .map(|id| state.get_database(&id).name.clone());
                    let details = EventDetails::RenameSchemaV1(mz_audit_log::RenameSchemaV1 {
                        id: schema_id.to_string(),
                        old_name: schema.name().schema.clone(),
                        new_name: new_name.clone(),
                        database_name,
                    });
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        mz_audit_log::ObjectType::Schema,
                        details,
                    )?;

                    // Update the schema itself.
                    let schema = state.get_schema_mut(&database_spec, &schema_spec, conn_id);
                    let old_name = schema.name().schema.clone();
                    schema.name.schema = new_name.clone();
                    let new_schema = schema.clone().into_durable_schema(database_spec.id());
                    tx.update_schema(schema_id, new_schema)?;

                    // Update the references to this schema.
                    match (&database_spec, &schema_spec) {
                        (ResolvedDatabaseSpecifier::Id(db_id), SchemaSpecifier::Id(_)) => {
                            let database = state.get_database_mut(db_id);
                            let Some(prev_id) = database.schemas_by_name.remove(&old_name) else {
                                panic!("Catalog state inconsistency! Expected to find schema with name {old_name}");
                            };
                            database.schemas_by_name.insert(new_name.clone(), prev_id);
                        }
                        (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(_)) => {
                            let Some(prev_id) = state.ambient_schemas_by_name.remove(&old_name)
                            else {
                                panic!("Catalog state inconsistency! Expected to find schema with name {old_name}");
                            };
                            state
                                .ambient_schemas_by_name
                                .insert(new_name.clone(), prev_id);
                        }
                        (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => {
                            // No external references to rename.
                        }
                        (ResolvedDatabaseSpecifier::Id(_), SchemaSpecifier::Temporary) => {
                            unreachable!("temporary schemas are in the ambient database")
                        }
                    }

                    // Update the new schema in the builtin table.
                    builtin_table_updates.push(state.pack_schema_update(
                        &database_spec,
                        &schema_id,
                        1,
                    ));

                    for (id, new_name, new_item) in updates {
                        Self::update_item(state, builtin_table_updates, id, new_name, new_item)?;
                    }
                }
                Op::UpdateOwner { id, new_owner } => {
                    let conn_id = session
                        .map(|session| session.conn_id())
                        .unwrap_or(&SYSTEM_CONN_ID);
                    let old_owner = state
                        .get_owner_id(&id, conn_id)
                        .expect("cannot update the owner of an object without an owner");
                    match &id {
                        ObjectId::Cluster(id) => {
                            let cluster_name = state.get_cluster(*id).name().to_string();
                            if id.is_system() {
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlyCluster(cluster_name),
                                )));
                            }
                            builtin_table_updates
                                .push(state.pack_cluster_update(&cluster_name, -1));
                            let cluster = state.get_cluster_mut(*id);
                            Self::update_privilege_owners(
                                &mut cluster.privileges,
                                cluster.owner_id,
                                new_owner,
                            );
                            cluster.owner_id = new_owner;
                            tx.update_cluster(*id, cluster.clone().into())?;
                            builtin_table_updates.push(state.pack_cluster_update(&cluster_name, 1));
                        }
                        ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                            let cluster = state.get_cluster(*cluster_id);
                            if cluster_id.is_system() {
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlyCluster(cluster.name().to_string()),
                                )));
                            }
                            let replica_name = cluster
                                .replica(*replica_id)
                                .expect("catalog out of sync")
                                .name
                                .clone();
                            if is_reserved_name(&replica_name) {
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReservedReplicaName(replica_name),
                                )));
                            }
                            builtin_table_updates.extend(state.pack_cluster_replica_update(
                                *cluster_id,
                                &replica_name,
                                -1,
                            ));
                            let cluster = state.get_cluster_mut(*cluster_id);
                            let replica = cluster
                                .replica_mut(*replica_id)
                                .expect("catalog out of sync");
                            replica.owner_id = new_owner;
                            tx.update_cluster_replica(*replica_id, replica.clone().into())?;
                            builtin_table_updates.extend(state.pack_cluster_replica_update(
                                *cluster_id,
                                &replica_name,
                                1,
                            ));
                        }
                        ObjectId::Database(id) => {
                            let database = state.get_database(id);
                            builtin_table_updates.push(state.pack_database_update(database, -1));
                            let database = state.get_database_mut(id);
                            Self::update_privilege_owners(
                                &mut database.privileges,
                                database.owner_id,
                                new_owner,
                            );
                            database.owner_id = new_owner;
                            let database = state.get_database(id);
                            tx.update_database(*id, database.clone().into())?;
                            builtin_table_updates.push(state.pack_database_update(database, 1));
                        }
                        ObjectId::Schema((database_spec, schema_spec)) => {
                            let schema_id = schema_spec.clone().into();
                            builtin_table_updates.push(state.pack_schema_update(
                                database_spec,
                                &schema_id,
                                -1,
                            ));
                            let schema = state.get_schema_mut(database_spec, schema_spec, conn_id);
                            Self::update_privilege_owners(
                                &mut schema.privileges,
                                schema.owner_id,
                                new_owner,
                            );
                            schema.owner_id = new_owner;
                            let database_id = match database_spec {
                                ResolvedDatabaseSpecifier::Ambient => None,
                                ResolvedDatabaseSpecifier::Id(id) => Some(id),
                            };
                            tx.update_schema(
                                schema_id,
                                schema.clone().into_durable_schema(database_id.copied()),
                            )?;
                            builtin_table_updates.push(state.pack_schema_update(
                                database_spec,
                                &schema_id,
                                1,
                            ));
                        }
                        ObjectId::Item(id) => {
                            if id.is_system() {
                                let entry = state.get_entry(id);
                                let full_name = state.resolve_full_name(
                                    entry.name(),
                                    session.map(|session| session.conn_id()),
                                );
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlyItem(full_name.to_string()),
                                )));
                            }
                            builtin_table_updates.extend(state.pack_item_update(*id, -1));
                            let entry = state.get_entry_mut(id);
                            Self::update_privilege_owners(
                                &mut entry.privileges,
                                entry.owner_id,
                                new_owner,
                            );
                            entry.owner_id = new_owner;
                            if !entry.item().is_temporary() {
                                tx.update_item(*id, entry.clone().into())?;
                            }
                            builtin_table_updates.extend(state.pack_item_update(*id, 1));
                        }
                        ObjectId::Role(_) => unreachable!("roles have no owner"),
                    }
                    let object_type = state.get_object_type(&id);
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        object_type_to_audit_object_type(object_type),
                        EventDetails::UpdateOwnerV1(mz_audit_log::UpdateOwnerV1 {
                            object_id: id.to_string(),
                            old_owner_id: old_owner.to_string(),
                            new_owner_id: new_owner.to_string(),
                        }),
                    )?;
                }
                Op::UpdateClusterConfig { id, name, config } => {
                    builtin_table_updates.push(state.pack_cluster_update(&name, -1));
                    let cluster = state.get_cluster_mut(id);
                    cluster.config = config;
                    tx.update_cluster(id, cluster.clone().into())?;
                    builtin_table_updates.push(state.pack_cluster_update(&name, 1));
                    info!("update cluster {}", name);
                }
                Op::UpdateClusterReplicaStatus { event } => {
                    builtin_table_updates.push(state.pack_cluster_replica_status_update(
                        event.cluster_id,
                        event.replica_id,
                        event.process_id,
                        -1,
                    ));
                    state.ensure_cluster_status(
                        event.cluster_id,
                        event.replica_id,
                        event.process_id,
                        ClusterReplicaProcessStatus {
                            status: event.status,
                            time: event.time,
                        },
                    );
                    builtin_table_updates.push(state.pack_cluster_replica_status_update(
                        event.cluster_id,
                        event.replica_id,
                        event.process_id,
                        1,
                    ));
                }
                Op::UpdateItem { id, name, to_item } => {
                    builtin_table_updates.extend(state.pack_item_update(id, -1));
                    Self::update_item(
                        state,
                        builtin_table_updates,
                        id,
                        name.clone(),
                        to_item.clone(),
                    )?;
                    let entry = state.get_entry(&id);
                    tx.update_item(id, entry.clone().into())?;

                    if Self::should_audit_log_item(&to_item) {
                        let name = Self::full_name_detail(
                            &state
                                .resolve_full_name(&name, session.map(|session| session.conn_id())),
                        );

                        state.add_to_audit_log(
                            oracle_write_ts,
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Alter,
                            catalog_type_to_audit_object_type(to_item.typ()),
                            EventDetails::UpdateItemV1(mz_audit_log::UpdateItemV1 {
                                id: id.to_string(),
                                name,
                            }),
                        )?;
                    }
                }
                Op::UpdateStorageUsage {
                    shard_id,
                    size_bytes,
                    collection_timestamp,
                } => {
                    state.add_to_storage_usage(
                        tx,
                        builtin_table_updates,
                        shard_id,
                        size_bytes,
                        collection_timestamp,
                    )?;
                }
                Op::UpdateSystemConfiguration { name, value } => {
                    Self::update_system_configuration(state, tx, &name, value.borrow())?;
                }
                Op::ResetSystemConfiguration { name } => {
                    state.remove_system_configuration(&name)?;
                    tx.remove_system_config(&name);
                    // This mirrors the `persist_txn_tables` "system var" into the catalog
                    // storage "config" collection so that we can toggle the flag with
                    // Launch Darkly, but use it in boot before Launch Darkly is available.
                    if name == PERSIST_TXN_TABLES.name() {
                        tx.set_persist_txn_tables(state.system_configuration.persist_txn_tables())?;
                    } else if name == CATALOG_KIND_IMPL.name() {
                        tx.set_catalog_kind(None)?;
                    }
                }
                Op::ResetAllSystemConfiguration => {
                    state.clear_system_configuration();
                    tx.clear_system_configs();
                    tx.set_persist_txn_tables(state.system_configuration.persist_txn_tables())?;
                    tx.set_catalog_kind(None)?;
                }
                Op::UpdateRotatedKeys {
                    id,
                    previous_public_key_pair,
                    new_public_key_pair,
                } => {
                    let entry = state.get_entry(&id);
                    // Retract old keys
                    builtin_table_updates.extend(state.pack_ssh_tunnel_connection_update(
                        id,
                        &previous_public_key_pair,
                        -1,
                    ));
                    // Insert the new rotated keys
                    builtin_table_updates.extend(state.pack_ssh_tunnel_connection_update(
                        id,
                        &new_public_key_pair,
                        1,
                    ));

                    let mut connection = entry.connection()?.clone();
                    if let mz_storage_types::connections::Connection::Ssh(ref mut ssh) =
                        connection.connection
                    {
                        ssh.public_keys = Some(new_public_key_pair)
                    }
                    let new_item = CatalogItem::Connection(connection);

                    let old_entry = state.entry_by_id.remove(&id).expect("catalog out of sync");
                    info!(
                        "update {} {} ({})",
                        old_entry.item_type(),
                        state.resolve_full_name(old_entry.name(), old_entry.conn_id()),
                        id
                    );
                    let mut new_entry = old_entry;
                    new_entry.item = new_item;
                    state.entry_by_id.insert(id, new_entry);
                }
            };
        }

        if dry_run_ops.is_empty() {
            Ok(())
        } else {
            Err(AdapterError::TransactionDryRun {
                new_ops: dry_run_ops,
                new_state: state.clone(),
            })
        }
    }

    pub(crate) fn update_item(
        state: &mut CatalogState,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        id: GlobalId,
        to_name: QualifiedItemName,
        to_item: CatalogItem,
    ) -> Result<(), AdapterError> {
        let old_entry = state.entry_by_id.remove(&id).expect("catalog out of sync");
        info!(
            "update {} {} ({})",
            old_entry.item_type(),
            state.resolve_full_name(old_entry.name(), old_entry.conn_id()),
            id
        );

        let conn_id = old_entry.item().conn_id().unwrap_or(&SYSTEM_CONN_ID);
        let schema = state.get_schema_mut(
            &old_entry.name().qualifiers.database_spec,
            &old_entry.name().qualifiers.schema_spec,
            conn_id,
        );
        schema.items.remove(&old_entry.name().item);

        // Dropped deps
        let dropped_references: Vec<_> = old_entry
            .references()
            .0
            .difference(&to_item.references().0)
            .cloned()
            .collect();
        let dropped_uses: Vec<_> = old_entry
            .uses()
            .difference(&to_item.uses())
            .cloned()
            .collect();

        // We only need to install this item on items in the `referenced_by` of new
        // dependencies.
        let new_references: Vec<_> = to_item
            .references()
            .0
            .difference(&old_entry.references().0)
            .cloned()
            .collect();
        // We only need to install this item on items in the `used_by` of new
        // dependencies.
        let new_uses: Vec<_> = to_item
            .uses()
            .difference(&old_entry.uses())
            .cloned()
            .collect();

        let mut new_entry = old_entry.clone();
        new_entry.name = to_name;
        new_entry.item = to_item;

        schema.items.insert(new_entry.name().item.clone(), id);

        for u in dropped_references {
            // OK if we no longer have this entry because we are dropping our
            // dependency on it.
            if let Some(metadata) = state.entry_by_id.get_mut(&u) {
                metadata.referenced_by.retain(|dep_id| *dep_id != id)
            }
        }

        for u in dropped_uses {
            // OK if we no longer have this entry because we are dropping our
            // dependency on it.
            if let Some(metadata) = state.entry_by_id.get_mut(&u) {
                metadata.used_by.retain(|dep_id| *dep_id != id)
            }
        }

        for u in new_references {
            match state.entry_by_id.get_mut(&u) {
                Some(metadata) => metadata.referenced_by.push(new_entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while updating {}",
                    &u,
                    state.resolve_full_name(new_entry.name(), new_entry.conn_id())
                ),
            }
        }
        for u in new_uses {
            match state.entry_by_id.get_mut(&u) {
                Some(metadata) => metadata.used_by.push(new_entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while updating {}",
                    &u,
                    state.resolve_full_name(new_entry.name(), new_entry.conn_id())
                ),
            }
        }

        state.entry_by_id.insert(id, new_entry);
        builtin_table_updates.extend(state.pack_item_update(id, 1));
        Ok(())
    }

    pub(crate) fn update_system_configuration(
        state: &mut CatalogState,
        tx: &mut Transaction,
        name: &str,
        value: VarInput,
    ) -> Result<(), AdapterError> {
        state.insert_system_configuration(name, value)?;
        let var = state.get_system_configuration(name)?;
        tx.upsert_system_config(name, var.value())?;
        // This mirrors the `persist_txn_tables` "system var" into the catalog
        // storage "config" collection so that we can toggle the flag with
        // Launch Darkly, but use it in boot before Launch Darkly is available.
        if name == PERSIST_TXN_TABLES.name() {
            tx.set_persist_txn_tables(state.system_configuration.persist_txn_tables())?;
        } else if name == CATALOG_KIND_IMPL.name() {
            tx.set_catalog_kind(state.system_configuration.catalog_kind())?;
        }
        Ok(())
    }

    fn create_schema(
        state: &mut CatalogState,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        id: SchemaId,
        oid: u32,
        database_id: DatabaseId,
        schema_name: String,
        owner_id: RoleId,
        privileges: PrivilegeMap,
    ) -> Result<(), AdapterError> {
        info!(
            "create schema {}.{}",
            state.get_database(&database_id).name,
            schema_name
        );
        let db = state
            .database_by_id
            .get_mut(&database_id)
            .expect("catalog out of sync");
        db.schemas_by_id.insert(
            id.clone(),
            Schema {
                name: QualifiedSchemaName {
                    database: ResolvedDatabaseSpecifier::Id(database_id.clone()),
                    schema: schema_name.clone(),
                },
                id: SchemaSpecifier::Id(id.clone()),
                oid,
                items: BTreeMap::new(),
                functions: BTreeMap::new(),
                types: BTreeMap::new(),
                owner_id,
                privileges,
            },
        );
        db.schemas_by_name.insert(schema_name, id.clone());
        builtin_table_updates.push(state.pack_schema_update(
            &ResolvedDatabaseSpecifier::Id(database_id.clone()),
            &id,
            1,
        ));
        Ok(())
    }

    /// Update privileges to reflect the new owner. Based off of PostgreSQL's
    /// implementation:
    /// <https://github.com/postgres/postgres/blob/43a33ef54e503b61f269d088f2623ba3b9484ad7/src/backend/utils/adt/acl.c#L1078-L1177>
    fn update_privilege_owners(
        privileges: &mut PrivilegeMap,
        old_owner: RoleId,
        new_owner: RoleId,
    ) {
        // TODO(jkosh44) Would be nice not to clone every privilege.
        let mut flat_privileges: Vec<_> = privileges.all_values_owned().collect();

        let mut new_present = false;
        for privilege in flat_privileges.iter_mut() {
            // Old owner's granted privilege are updated to be granted by the new
            // owner.
            if privilege.grantor == old_owner {
                privilege.grantor = new_owner;
            } else if privilege.grantor == new_owner {
                new_present = true;
            }
            // Old owner's privileges is given to the new owner.
            if privilege.grantee == old_owner {
                privilege.grantee = new_owner;
            } else if privilege.grantee == new_owner {
                new_present = true;
            }
        }

        // If the old privilege list contained references to the new owner, we may
        // have created duplicate entries. Here we try and consolidate them. This
        // is inspired by PostgreSQL's algorithm but not identical.
        if new_present {
            // Group privileges by (grantee, grantor).
            let privilege_map: BTreeMap<_, Vec<_>> =
                flat_privileges
                    .into_iter()
                    .fold(BTreeMap::new(), |mut accum, privilege| {
                        accum
                            .entry((privilege.grantee, privilege.grantor))
                            .or_default()
                            .push(privilege);
                        accum
                    });

            // Consolidate and update all privileges.
            flat_privileges = privilege_map
                .into_iter()
                .map(|((grantee, grantor), values)|
                    // Combine the acl_mode of all mz_aclitems with the same grantee and grantor.
                    values.into_iter().fold(
                        MzAclItem::empty(grantee, grantor),
                        |mut accum, mz_aclitem| {
                            accum.acl_mode =
                                accum.acl_mode.union(mz_aclitem.acl_mode);
                            accum
                        },
                    ))
                .collect();
        }

        *privileges = PrivilegeMap::from_mz_acl_items(flat_privileges);
    }

    #[mz_ore::instrument(level = "debug")]
    pub async fn confirm_leadership(&self) -> Result<(), AdapterError> {
        Ok(self.storage().await.confirm_leadership().await?)
    }

    /// Parses the given SQL string into a `CatalogItem`.
    #[mz_ore::instrument]
    fn parse_item(
        &self,
        id: GlobalId,
        create_sql: String,
        pcx: Option<&PlanContext>,
        is_retained_metrics_object: bool,
        custom_logical_compaction_window: Option<CompactionWindow>,
    ) -> Result<CatalogItem, AdapterError> {
        self.state.parse_item(
            id,
            create_sql,
            pcx,
            is_retained_metrics_object,
            custom_logical_compaction_window,
        )
    }

    /// Return the ids of all log sources the given object depends on.
    pub fn introspection_dependencies(&self, id: GlobalId) -> Vec<GlobalId> {
        self.state.introspection_dependencies(id)
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> Result<CatalogDump, Error> {
        Ok(CatalogDump::new(self.state.dump()?))
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

    pub fn user_cluster_replicas(&self) -> impl Iterator<Item = &ClusterReplica> {
        self.user_clusters().flat_map(|cluster| cluster.replicas())
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

    /// Allocate ids for introspection sources. Called once per cluster creation.
    pub async fn allocate_introspection_sources(&self) -> Vec<(&'static BuiltinLog, GlobalId)> {
        let log_amount = BUILTINS::logs().count();
        let system_ids = self
            .storage()
            .await
            .allocate_system_ids(
                log_amount
                    .try_into()
                    .expect("builtin logs should fit into u64"),
            )
            .await
            .unwrap_or_terminate("cannot fail to allocate system ids");
        BUILTINS::logs().zip(system_ids.into_iter()).collect()
    }

    pub fn pack_item_update(&self, id: GlobalId, diff: Diff) -> Vec<BuiltinTableUpdate> {
        self.state.pack_item_update(id, diff)
    }

    pub fn system_config(&self) -> &SystemVars {
        self.state.system_config()
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

    pub fn ensure_not_reserved_object(
        &self,
        object_id: &ObjectId,
        conn_id: &ConnectionId,
    ) -> Result<(), Error> {
        match object_id {
            ObjectId::Cluster(cluster_id) | ObjectId::ClusterReplica((cluster_id, _)) => {
                if cluster_id.is_system() {
                    let cluster = self.get_cluster(*cluster_id);
                    Err(Error::new(ErrorKind::ReadOnlyCluster(
                        cluster.name().to_string(),
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
        }
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

#[derive(Debug, Clone)]
pub enum Op {
    AlterSetCluster {
        id: GlobalId,
        cluster: ClusterId,
    },
    AlterRole {
        id: RoleId,
        name: String,
        attributes: RoleAttributes,
        vars: RoleVars,
    },
    CreateDatabase {
        name: String,
        oid: u32,
        public_schema_oid: u32,
        owner_id: RoleId,
    },
    CreateSchema {
        database_id: ResolvedDatabaseSpecifier,
        schema_name: String,
        oid: u32,
        owner_id: RoleId,
    },
    CreateRole {
        name: String,
        oid: u32,
        attributes: RoleAttributes,
    },
    CreateCluster {
        id: ClusterId,
        name: String,
        introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        config: ClusterConfig,
    },
    CreateClusterReplica {
        cluster_id: ClusterId,
        id: ReplicaId,
        name: String,
        config: ReplicaConfig,
        owner_id: RoleId,
    },
    CreateItem {
        id: GlobalId,
        oid: u32,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
    },
    Comment {
        object_id: CommentObjectId,
        sub_component: Option<usize>,
        comment: Option<String>,
    },
    DropObject(ObjectId),
    GrantRole {
        role_id: RoleId,
        member_id: RoleId,
        grantor_id: RoleId,
    },
    RenameCluster {
        id: ClusterId,
        name: String,
        to_name: String,
        check_reserved_names: bool,
    },
    RenameClusterReplica {
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        name: QualifiedReplica,
        to_name: String,
    },
    RenameItem {
        id: GlobalId,
        current_full_name: FullItemName,
        to_name: String,
    },
    RenameSchema {
        database_spec: ResolvedDatabaseSpecifier,
        schema_spec: SchemaSpecifier,
        new_name: String,
        check_reserved_names: bool,
    },
    UpdateOwner {
        id: ObjectId,
        new_owner: RoleId,
    },
    UpdatePrivilege {
        target_id: SystemObjectId,
        privilege: MzAclItem,
        variant: UpdatePrivilegeVariant,
    },
    UpdateDefaultPrivilege {
        privilege_object: DefaultPrivilegeObject,
        privilege_acl_item: DefaultPrivilegeAclItem,
        variant: UpdatePrivilegeVariant,
    },
    RevokeRole {
        role_id: RoleId,
        member_id: RoleId,
        grantor_id: RoleId,
    },
    UpdateClusterConfig {
        id: ClusterId,
        name: String,
        config: ClusterConfig,
    },
    UpdateClusterReplicaStatus {
        event: ClusterEvent,
    },
    UpdateItem {
        id: GlobalId,
        name: QualifiedItemName,
        to_item: CatalogItem,
    },
    UpdateStorageUsage {
        shard_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
    },
    UpdateSystemConfiguration {
        name: String,
        value: OwnedVarInput,
    },
    ResetSystemConfiguration {
        name: String,
    },
    ResetAllSystemConfiguration,
    UpdateRotatedKeys {
        id: GlobalId,
        previous_public_key_pair: (String, String),
        new_public_key_pair: (String, String),
    },
    /// Performs a dry run of the commit, but errors with
    /// [`AdapterError::TransactionDryRun`].
    ///
    /// When using this value, it should be included only as the last element of
    /// the transaction and should not be the only value in the transaction.
    TransactionDryRun,
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
        self.state
            .entry_by_id
            .get(&id)
            .map(|entry| entry.name())
            .map(|name| self.resolve_full_name(name).to_string())
    }

    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String> {
        self.state
            .entry_by_id
            .get(&id)
            .map(|entry| entry.name())
            .map(|name| name.item.clone())
    }

    fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        use ScalarType::*;

        match typ {
            Array(t) => format!("{}[]", self.humanize_scalar_type(t)),
            List {
                custom_id: Some(global_id),
                ..
            }
            | Map {
                custom_id: Some(global_id),
                ..
            } => {
                let item = self.get_item(global_id);
                self.minimal_qualification(item.name()).to_string()
            }
            List { element_type, .. } => {
                format!("{} list", self.humanize_scalar_type(element_type))
            }
            Map { value_type, .. } => format!(
                "map[{}=>{}]",
                self.humanize_scalar_type(&ScalarType::String),
                self.humanize_scalar_type(value_type)
            ),
            Record {
                custom_id: Some(id),
                ..
            } => {
                let item = self.get_item(id);
                self.minimal_qualification(item.name()).to_string()
            }
            Record { fields, .. } => format!(
                "record({})",
                fields
                    .iter()
                    .map(|f| format!("{}: {}", f.0, self.humanize_column_type(&f.1)))
                    .join(",")
            ),
            PgLegacyChar => "\"char\"".into(),
            UInt16 => "uint2".into(),
            UInt32 => "uint4".into(),
            UInt64 => "uint8".into(),
            ty => {
                let pgrepr_type = mz_pgrepr::Type::from(ty);
                let pg_catalog_schema =
                    SchemaSpecifier::Id(self.state.get_pg_catalog_schema_id().clone());

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
        self.state
            .entry_by_id
            .get(&id)
            .try_map(|entry| {
                Ok::<_, SqlCatalogError>(
                    entry
                        .desc(&self.resolve_full_name(entry.name()))?
                        .iter_names()
                        .cloned()
                        .map(|col_name| col_name.to_string())
                        .collect(),
                )
            })
            .unwrap_or(None)
    }

    fn id_exists(&self, id: GlobalId) -> bool {
        self.state.entry_by_id.get(&id).is_some()
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

    fn get_mz_internal_schema_id(&self) -> &SchemaId {
        self.state().get_mz_internal_schema_id()
    }

    fn get_mz_unsafe_schema_id(&self) -> &SchemaId {
        self.state().get_mz_unsafe_schema_id()
    }

    fn is_system_schema(&self, schema: &str) -> bool {
        self.state.is_system_schema(schema)
    }

    fn is_system_schema_specifier(&self, schema: &SchemaSpecifier) -> bool {
        match schema {
            SchemaSpecifier::Temporary => false,
            SchemaSpecifier::Id(id) => self.state.is_system_schema_id(id),
        }
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

    fn resolve_cluster(
        &self,
        cluster_name: Option<&str>,
    ) -> Result<&dyn mz_sql::catalog::CatalogCluster, SqlCatalogError> {
        Ok(self
            .state
            .resolve_cluster(cluster_name.unwrap_or_else(|| self.active_cluster()))?)
    }

    fn resolve_cluster_replica(
        &self,
        cluster_replica_name: &QualifiedReplica,
    ) -> Result<&dyn CatalogClusterReplica, SqlCatalogError> {
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

    fn try_get_item(&self, id: &GlobalId) -> Option<&dyn mz_sql::catalog::CatalogItem> {
        Some(self.state.try_get_entry(id)?)
    }

    fn get_item(&self, id: &GlobalId) -> &dyn mz_sql::catalog::CatalogItem {
        self.state.get_entry(id)
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

    fn get_cluster(&self, id: ClusterId) -> &dyn mz_sql::catalog::CatalogCluster {
        &self.state.clusters_by_id[&id]
    }

    fn get_clusters(&self) -> Vec<&dyn mz_sql::catalog::CatalogCluster> {
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
    ) -> &dyn mz_sql::catalog::CatalogClusterReplica {
        let cluster = self.get_cluster(cluster_id);
        cluster.replica(replica_id)
    }

    fn get_cluster_replicas(&self) -> Vec<&dyn mz_sql::catalog::CatalogClusterReplica> {
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
            SystemObjectId::Object(ObjectId::ClusterReplica(_))
            | SystemObjectId::Object(ObjectId::Role(_)) => None,
        }
    }

    fn object_dependents(&self, ids: &Vec<ObjectId>) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state.object_dependents(ids, &self.conn_id, &mut seen)
    }

    fn item_dependents(&self, id: GlobalId) -> Vec<ObjectId> {
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

    fn get_item_comments(&self, id: &GlobalId) -> Option<&BTreeMap<Option<usize>, String>> {
        let comment_id = self.state.get_comment_id(ObjectId::Item(*id));
        self.state.comments.get_object_comments(comment_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use std::{env, iter};

    use itertools::Itertools;
    use tokio_postgres::types::Type;
    use tokio_postgres::NoTls;
    use uuid::Uuid;

    use mz_catalog::builtin::{
        Builtin, BuiltinType, BUILTINS,
        REALLY_DANGEROUS_DO_NOT_CALL_THIS_IN_PRODUCTION_VIEW_FINGERPRINT_WHITESPACE,
    };
    use mz_catalog::durable::initialize::CATALOG_KIND_KEY;
    use mz_catalog::durable::objects::serialization::proto;
    use mz_controller_types::{ClusterId, ReplicaId};
    use mz_expr::MirScalarExpr;
    use mz_ore::now::{to_datetime, NOW_ZERO, SYSTEM_TIME};
    use mz_ore::task;
    use mz_persist_client::PersistClient;
    use mz_pgrepr::oid::{FIRST_MATERIALIZE_OID, FIRST_UNPINNED_OID, FIRST_USER_OID};
    use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
    use mz_repr::namespaces::{INFORMATION_SCHEMA, PG_CATALOG_SCHEMA};
    use mz_repr::role_id::RoleId;
    use mz_repr::{Datum, GlobalId, RelationType, RowArena, ScalarType, Timestamp};
    use mz_sql::catalog::{CatalogSchema, CatalogType, SessionCatalog};
    use mz_sql::func::{Func, FuncImpl, Operation, OP_IMPLS};
    use mz_sql::names::{
        self, DatabaseId, ItemQualifiers, ObjectId, PartialItemName, QualifiedItemName,
        ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier, SystemObjectId,
    };
    use mz_sql::plan::{
        CoercibleScalarExpr, ExprContext, HirScalarExpr, PlanContext, QueryContext, QueryLifetime,
        Scope, StatementContext,
    };
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_sql::session::vars::{CatalogKind, OwnedVarInput, Var, VarInput, CATALOG_KIND_IMPL};

    use crate::catalog::{Catalog, CatalogItem, Op, PrivilegeMap, SYSTEM_CONN_ID};
    use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
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
        Catalog::with_debug(NOW_ZERO.clone(), |catalog| async move {
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
                            schema_spec: SchemaSpecifier::Id(
                                catalog.get_pg_catalog_schema_id().clone(),
                            ),
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
                            schema_spec: SchemaSpecifier::Id(
                                catalog.get_mz_catalog_schema_id().clone(),
                            ),
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
        {
            let mut catalog = Catalog::open_debug_catalog(
                persist_client.clone(),
                organization_id.clone(),
                NOW_ZERO.clone(),
                None,
            )
            .await
            .expect("unable to open debug catalog");
            assert_eq!(catalog.transient_revision(), 1);
            catalog
                .transact(
                    mz_repr::Timestamp::MIN,
                    None,
                    vec![Op::CreateDatabase {
                        name: "test".to_string(),
                        oid: 1,
                        public_schema_oid: 2,
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                )
                .await
                .expect("failed to transact");
            assert_eq!(catalog.transient_revision(), 2);
            catalog.expire().await;
        }
        {
            let catalog = Catalog::open_debug_catalog(
                persist_client,
                organization_id,
                NOW_ZERO.clone(),
                None,
            )
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
        Catalog::with_debug(NOW_ZERO.clone(), |catalog| async move {
            let mz_catalog_schema = (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(catalog.state().get_mz_catalog_schema_id().clone()),
            );
            let pg_catalog_schema = (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(catalog.state().get_pg_catalog_schema_id().clone()),
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
                    None,
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
                    None,
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
                    None,
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
        Catalog::with_debug(NOW_ZERO.clone(), |catalog| async move {
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
        assert!(mz_sql_parser::parser::parse_statements(&create_sql).is_ok());
        assert!(mz_sql_parser::parser::parse_statements_with_limit(&create_sql).is_err());

        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let id = GlobalId::User(1);
        {
            let mut catalog = Catalog::open_debug_catalog(
                persist_client.clone(),
                organization_id.clone(),
                SYSTEM_TIME.clone(),
                None,
            )
            .await
            .expect("unable to open debug catalog");
            let item = catalog
                .state()
                .parse_view_item(create_sql)
                .expect("unable to parse view");
            catalog
                .transact(
                    SYSTEM_TIME().into(),
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
                        oid: 1,
                        id,
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                )
                .await
                .expect("failed to transact");
            catalog.expire().await;
        }
        {
            let catalog = Catalog::open_debug_catalog(
                persist_client,
                organization_id,
                SYSTEM_TIME.clone(),
                None,
            )
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

    #[mz_ore::test]
    fn test_update_privilege_owners() {
        let old_owner = RoleId::User(1);
        let new_owner = RoleId::User(2);
        let other_role = RoleId::User(3);

        // older owner exists as grantor.
        let mut privileges = PrivilegeMap::from_mz_acl_items(vec![
            MzAclItem {
                grantee: other_role,
                grantor: old_owner,
                acl_mode: AclMode::UPDATE,
            },
            MzAclItem {
                grantee: other_role,
                grantor: new_owner,
                acl_mode: AclMode::SELECT,
            },
        ]);
        Catalog::update_privilege_owners(&mut privileges, old_owner, new_owner);
        assert_eq!(1, privileges.all_values().count());
        assert_eq!(
            vec![MzAclItem {
                grantee: other_role,
                grantor: new_owner,
                acl_mode: AclMode::SELECT.union(AclMode::UPDATE)
            }],
            privileges.all_values_owned().collect::<Vec<_>>()
        );

        // older owner exists as grantee.
        let mut privileges = PrivilegeMap::from_mz_acl_items(vec![
            MzAclItem {
                grantee: old_owner,
                grantor: other_role,
                acl_mode: AclMode::UPDATE,
            },
            MzAclItem {
                grantee: new_owner,
                grantor: other_role,
                acl_mode: AclMode::SELECT,
            },
        ]);
        Catalog::update_privilege_owners(&mut privileges, old_owner, new_owner);
        assert_eq!(1, privileges.all_values().count());
        assert_eq!(
            vec![MzAclItem {
                grantee: new_owner,
                grantor: other_role,
                acl_mode: AclMode::SELECT.union(AclMode::UPDATE)
            }],
            privileges.all_values_owned().collect::<Vec<_>>()
        );

        // older owner exists as grantee and grantor.
        let mut privileges = PrivilegeMap::from_mz_acl_items(vec![
            MzAclItem {
                grantee: old_owner,
                grantor: old_owner,
                acl_mode: AclMode::UPDATE,
            },
            MzAclItem {
                grantee: new_owner,
                grantor: new_owner,
                acl_mode: AclMode::SELECT,
            },
        ]);
        Catalog::update_privilege_owners(&mut privileges, old_owner, new_owner);
        assert_eq!(1, privileges.all_values().count());
        assert_eq!(
            vec![MzAclItem {
                grantee: new_owner,
                grantor: new_owner,
                acl_mode: AclMode::SELECT.union(AclMode::UPDATE)
            }],
            privileges.all_values_owned().collect::<Vec<_>>()
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_object_type() {
        Catalog::with_debug(SYSTEM_TIME.clone(), |catalog| async move {
            let conn_catalog = catalog.for_system_session();

            assert_eq!(
                mz_sql::catalog::ObjectType::ClusterReplica,
                conn_catalog.get_object_type(&ObjectId::ClusterReplica((
                    ClusterId::User(1),
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
        Catalog::with_debug(SYSTEM_TIME.clone(), |catalog| async move {
            let conn_catalog = catalog.for_system_session();

            assert_eq!(
                None,
                conn_catalog.get_privileges(&SystemObjectId::Object(ObjectId::ClusterReplica((
                    ClusterId::User(1),
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

            for builtin in BUILTINS::iter() {
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
                                let elem_ty = BUILTINS::iter()
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
                                imp.oid, func.name, imp_return_oid, pg_fn.ret_oid
                            );

                            assert_eq!(
                                imp.return_is_set,
                                pg_fn.ret_set,
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
                            imp.oid,
                            op,
                            imp_return_oid,
                            pg_op.oprresult
                        );
                    }
                }
            }
            catalog.expire().await;
        }

        Catalog::with_debug(NOW_ZERO.clone(), inner).await
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
                        styps.push(ScalarType::try_from(&pgtyp).expect("must exist"));
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
                        .map(|typ| ScalarType::try_from(&typ).expect("must exist"));

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

        let handles =
            Catalog::with_debug(NOW_ZERO.clone(), |catalog| async { inner(catalog) }).await;
        for handle in handles {
            handle.await.expect("must succeed");
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
        return_styp: Option<ScalarType>,
    ) {
        let conn_catalog = catalog.for_system_session();
        let pcx = PlanContext::zero();
        let scx = StatementContext::new(Some(&pcx), &conn_catalog);
        let qcx = QueryContext::root(&scx, QueryLifetime::OneShot);
        let ecx = ExprContext {
            qcx: &qcx,
            name: "smoketest",
            scope: &Scope::empty(),
            relation_type: &RelationType::empty(),
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
        let start = Instant::now();
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
                        if !eval_result_datum.is_instance_of(&mir_typ) {
                            panic!("{call_name}: expected return type of {return_styp:?}, got {eval_result_datum}");
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
                                assert!(mir_typ.nullable, "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}", name, args, mir, mir_typ.nullable);
                            } else {
                                let any_input_null = args.iter().any(|arg| arg.is_null());
                                if !any_input_null {
                                    assert!(!mir_typ.nullable, "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}", name, args, mir, mir_typ.nullable);
                                } else {
                                    assert_eq!(mir_typ.nullable, propagates_nulls, "fn named `{}` called on args `{:?}` (lowered to `{}`) yielded mir_typ.nullable: {}", name, args, mir, mir_typ.nullable);
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
                                        assert_eq!(reduce_result_datum, eval_result_datum, "eval/reduce datum mismatch: fn named `{}` called on args `{:?}` (lowered to `{}`) evaluated to `{}` with typ `{:?}`, but reduced to `{}` with typ `{:?}`", name, args, mir, eval_result_datum, mir_typ.scalar_type, reduce_result_datum, ctyp.scalar_type);
                                        // Let's check that the types also match.
                                        // (We are not checking nullability here,
                                        // because it's ok when we know a more
                                        // precise nullability after actually
                                        // evaluating a function than before.)
                                        assert_eq!(ctyp.scalar_type, mir_typ.scalar_type, "eval/reduce type mismatch: fn named `{}` called on args `{:?}` (lowered to `{}`) evaluated to `{}` with typ `{:?}`, but reduced to `{}` with typ `{:?}`", name, args, mir, eval_result_datum, mir_typ.scalar_type, reduce_result_datum, ctyp.scalar_type);
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
        // Because the tests are run with one task per fn, these execution times go up compared to
        // running them serially on a single task/thread. Thus choose a fairly high timeout.
        // Additionally, CI infra has variable performance and we want to avoid flakes. This timeout
        // is designed to detect something taking an unexpectedly long time, but that's hard to
        // define. If this causes problems in CI it should probably be removed instead of getting
        // bumped to a higher timeout.
        let elapsed = start.elapsed();
        if elapsed > Duration::from_millis(5_000) {
            panic!("LONG EXECUTION ({elapsed:?}): {call_name}");
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
        Catalog::with_debug(SYSTEM_TIME.clone(), |catalog| async move {
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
                    .expect("unable to resolve view");
                let full_name = conn_catalog.resolve_full_name(item.name());
                for col_type in item
                    .desc(&full_name)
                    .expect("invalid item type")
                    .iter_types()
                {
                    match &col_type.scalar_type {
                        typ @ ScalarType::UInt16
                        | typ @ ScalarType::UInt32
                        | typ @ ScalarType::UInt64
                        | typ @ ScalarType::MzTimestamp
                        | typ @ ScalarType::List { .. }
                        | typ @ ScalarType::Map { .. }
                        | typ @ ScalarType::MzAclItem => {
                            panic!("{typ:?} type found in {full_name}");
                        }
                        ScalarType::AclItem
                        | ScalarType::Bool
                        | ScalarType::Int16
                        | ScalarType::Int32
                        | ScalarType::Int64
                        | ScalarType::Float32
                        | ScalarType::Float64
                        | ScalarType::Numeric { .. }
                        | ScalarType::Date
                        | ScalarType::Time
                        | ScalarType::Timestamp { .. }
                        | ScalarType::TimestampTz { .. }
                        | ScalarType::Interval
                        | ScalarType::PgLegacyChar
                        | ScalarType::Bytes
                        | ScalarType::String
                        | ScalarType::Char { .. }
                        | ScalarType::VarChar { .. }
                        | ScalarType::Jsonb
                        | ScalarType::Uuid
                        | ScalarType::Array(_)
                        | ScalarType::Record { .. }
                        | ScalarType::Oid
                        | ScalarType::RegProc
                        | ScalarType::RegType
                        | ScalarType::RegClass
                        | ScalarType::Int2Vector
                        | ScalarType::Range { .. }
                        | ScalarType::PgLegacyName => {}
                    }
                }
            }
            catalog.expire().await;
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_builtin_migrations() {
        let persist_client = PersistClient::new_for_tests().await;
        let organization_id = Uuid::new_v4();
        let id = {
            let catalog = Catalog::open_debug_catalog(
                persist_client.clone(),
                organization_id.clone(),
                NOW_ZERO.clone(),
                None,
            )
            .await
            .expect("unable to open debug catalog");
            let id = catalog
                .entries()
                .find(|entry| &entry.name.item == "mz_objects" && entry.is_view())
                .expect("mz_objects doesn't exist")
                .id();
            catalog.expire().await;
            id
        };
        // Forcibly migrate all views.
        {
            let mut guard =
                REALLY_DANGEROUS_DO_NOT_CALL_THIS_IN_PRODUCTION_VIEW_FINGERPRINT_WHITESPACE
                    .lock()
                    .expect("lock poisoned");
            *guard = Some("\n".to_string());
        }
        {
            let catalog = Catalog::open_debug_catalog(
                persist_client,
                organization_id,
                NOW_ZERO.clone(),
                None,
            )
            .await
            .expect("unable to open debug catalog");

            let new_id = catalog
                .entries()
                .find(|entry| &entry.name.item == "mz_objects" && entry.is_view())
                .expect("mz_objects doesn't exist")
                .id();
            // Assert that the view was migrated and got a new ID.
            assert_ne!(new_id, id);

            catalog.expire().await;
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_catalog_kind_feature_flag() {
        Catalog::with_debug(SYSTEM_TIME.clone(), |mut catalog| async move {
            // Update "catalog_kind" system variable.
            let op = Op::UpdateSystemConfiguration {
                name: CATALOG_KIND_IMPL.name().to_string(),
                value: OwnedVarInput::Flat(CatalogKind::Persist.as_str().to_string()),
            };
            catalog
                .transact(mz_repr::Timestamp::MIN, None, vec![op])
                .await
                .expect("failed to transact");

            // Check that the config was also updated.
            let configs = catalog
                .storage()
                .await
                .snapshot()
                .await
                .expect("failed to get snapshot")
                .configs;
            let catalog_kind_value = configs
                .get(&proto::ConfigKey {
                    key: CATALOG_KIND_KEY.to_string(),
                })
                .expect("config should exist")
                .value;
            let catalog_kind =
                CatalogKind::try_from(catalog_kind_value).expect("invalid value persisted");
            assert_eq!(catalog_kind, CatalogKind::Persist);

            // Remove "catalog_kind" system variable.
            let op = Op::ResetSystemConfiguration {
                name: CATALOG_KIND_IMPL.name().to_string(),
            };
            catalog
                .transact(mz_repr::Timestamp::MIN, None, vec![op])
                .await
                .expect("failed to transact");

            // Check that the config was also removed.
            let configs = catalog
                .storage()
                .await
                .snapshot()
                .await
                .expect("failed to get snapshot")
                .configs;
            let catalog_kind_value = configs.get(&proto::ConfigKey {
                key: CATALOG_KIND_KEY.to_string(),
            });
            assert_eq!(catalog_kind_value, None);

            catalog.expire().await;
        })
        .await;
    }
}
