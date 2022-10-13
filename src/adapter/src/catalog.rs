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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use itertools::Itertools;
use mz_storage::controller::IntrospectionType;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{info, trace};
use uuid::Uuid;

use mz_audit_log::{
    EventDetails, EventType, FullNameV1, IdFullNameV1, ObjectType, VersionedEvent,
    VersionedStorageUsage,
};
use mz_build_info::DUMMY_BUILD_INFO;
use mz_compute_client::command::{ProcessId, ReplicaId};
use mz_compute_client::controller::{
    ComputeInstanceEvent, ComputeInstanceId, ComputeReplicaAllocation, ComputeReplicaConfig,
    ComputeReplicaLocation, ComputeReplicaLogging,
};
use mz_compute_client::logging::{LogVariant, LogView};
use mz_expr::{MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_pgrepr::oid::FIRST_USER_OID;
use mz_repr::{explain_new::ExprHumanizer, Diff, GlobalId, RelationDesc, ScalarType};
use mz_secrets::InMemorySecretsController;
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Expr;
use mz_sql::catalog::{
    CatalogDatabase, CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem,
    CatalogItemType as SqlCatalogItemType, CatalogItemType, CatalogSchema, CatalogType,
    CatalogTypeDetails, IdReference, NameReference, SessionCatalog, TypeReference,
};
use mz_sql::names::{
    Aug, DatabaseId, FullObjectName, ObjectQualifiers, PartialObjectName, QualifiedObjectName,
    QualifiedSchemaName, RawDatabaseSpecifier, ResolvedDatabaseSpecifier, RoleId, SchemaId,
    SchemaSpecifier,
};
use mz_sql::plan::{
    AlterOptionParameter, CreateConnectionPlan, CreateIndexPlan, CreateMaterializedViewPlan,
    CreateSecretPlan, CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan,
    CreateViewPlan, Params, Plan, PlanContext, StatementDesc,
    StorageHostConfig as PlanStorageHostConfig,
};
use mz_sql::{plan, DEFAULT_SCHEMA};
use mz_sql_parser::ast::{CreateSinkOption, CreateSourceOption, Statement, WithOptionValue};
use mz_stash::{Append, Postgres, Sqlite};
use mz_storage::types::hosts::{StorageHostConfig, StorageHostResourceAllocation};
use mz_storage::types::sinks::{SinkEnvelope, StorageSinkConnection, StorageSinkConnectionBuilder};
use mz_storage::types::sources::{SourceDesc, Timeline};
use mz_transform::Optimizer;

use crate::catalog::builtin::{
    Builtin, BuiltinLog, BuiltinTable, BuiltinType, Fingerprint, BUILTINS, BUILTIN_PREFIXES,
    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_TEMP_SCHEMA, PG_CATALOG_SCHEMA,
};
pub use crate::catalog::builtin_table_updates::BuiltinTableUpdate;
pub use crate::catalog::config::{ClusterReplicaSizeMap, Config, StorageHostSizeMap};
pub use crate::catalog::error::{AmbiguousRename, Error, ErrorKind};
use crate::catalog::storage::{BootstrapArgs, Transaction};
use crate::client::ConnectionId;
use crate::session::vars::SystemVars;
use crate::session::{PreparedStatement, Session, User, DEFAULT_DATABASE_NAME};
use crate::util::index_sql;
use crate::{AdapterError, DUMMY_AVAILABILITY_ZONE};

use self::builtin::BuiltinSource;

mod builtin_table_updates;
mod config;
mod error;
mod migrate;

pub mod builtin;
pub mod storage;

pub const SYSTEM_CONN_ID: ConnectionId = 0;

pub static SYSTEM_USER: Lazy<User> = Lazy::new(|| User {
    name: "mz_system".into(),
    external_metadata: None,
});

pub static HTTP_DEFAULT_USER: Lazy<User> = Lazy::new(|| User {
    name: "anonymous_http_user".into(),
    external_metadata: None,
});

const CREATE_SQL_TODO: &str = "TODO";

pub const DEFAULT_CLUSTER_REPLICA_NAME: &str = "r1";

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
/// name. Items can be referred to by their [`FullObjectName`], which fully and
/// unambiguously specifies the item, or a [`PartialObjectName`], which can omit the
/// database name and/or the schema name. Partial names can be converted into
/// full names via a complicated resolution process documented by the
/// [`CatalogState::resolve`] method.
///
/// The catalog also maintains special "ambient schemas": virtual schemas,
/// implicitly present in all databases, that house various system views.
/// The big examples of ambient schemas are `pg_catalog` and `mz_catalog`.
#[derive(Debug)]
pub struct Catalog<S> {
    state: CatalogState,
    storage: Arc<Mutex<storage::Connection<S>>>,
    transient_revision: u64,
}

// Implement our own Clone because derive can't unless S is Clone, which it's
// not (hence the Arc).
impl<S> Clone for Catalog<S> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            storage: Arc::clone(&self.storage),
            transient_revision: self.transient_revision,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CatalogState {
    database_by_name: BTreeMap<String, DatabaseId>,
    database_by_id: BTreeMap<DatabaseId, Database>,
    entry_by_id: BTreeMap<GlobalId, CatalogEntry>,
    ambient_schemas_by_name: BTreeMap<String, SchemaId>,
    ambient_schemas_by_id: BTreeMap<SchemaId, Schema>,
    temporary_schemas: HashMap<ConnectionId, Schema>,
    compute_instances_by_id: HashMap<ComputeInstanceId, ComputeInstance>,
    compute_instances_by_name: HashMap<String, ComputeInstanceId>,
    roles: HashMap<String, Role>,
    config: mz_sql::catalog::CatalogConfig,
    oid_counter: u32,
    cluster_replica_sizes: ClusterReplicaSizeMap,
    storage_host_sizes: StorageHostSizeMap,
    default_storage_host_size: Option<String>,
    availability_zones: Vec<String>,
    system_configuration: SystemVars,
}

impl CatalogState {
    pub fn allocate_oid(&mut self) -> Result<u32, Error> {
        let oid = self.oid_counter;
        if oid == u32::max_value() {
            return Err(Error::new(ErrorKind::OidExhaustion));
        }
        self.oid_counter += 1;
        Ok(oid)
    }

    /// Computes the IDs of any indexes that transitively depend on this catalog
    /// entry.
    pub fn dependent_indexes(&self, id: GlobalId) -> Vec<GlobalId> {
        let mut out = Vec::new();
        self.dependent_indexes_inner(id, &mut out);
        out
    }

    fn dependent_indexes_inner(&self, id: GlobalId, out: &mut Vec<GlobalId>) {
        let entry = self.get_entry(&id);
        match entry.item() {
            CatalogItem::Index(_) => out.push(id),
            _ => {
                for id in entry.used_by() {
                    self.dependent_indexes_inner(*id, out)
                }
            }
        }
    }

    /// Computes the IDs of any log sources this catalog entry transitively
    /// depends on.
    pub fn arranged_introspection_dependencies(&self, id: GlobalId) -> Vec<GlobalId> {
        let mut out = Vec::new();
        self.arranged_introspection_dependencies_inner(id, &mut out);

        // Filter out persisted logs
        let mut persisted_source_ids = HashSet::new();
        for instance in self.compute_instances_by_id.values() {
            for replica in instance.replicas_by_id.values() {
                persisted_source_ids.extend(replica.config.logging.source_ids());
            }
        }

        out.into_iter()
            .filter(|x| !persisted_source_ids.contains(x))
            .collect()
    }

    fn arranged_introspection_dependencies_inner(&self, id: GlobalId, out: &mut Vec<GlobalId>) {
        match self.get_entry(&id).item() {
            CatalogItem::Log(_) => out.push(id),
            item @ (CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Connection(_)) => {
                for id in item.uses() {
                    self.arranged_introspection_dependencies_inner(*id, out);
                }
            }
            CatalogItem::Sink(sink) => {
                self.arranged_introspection_dependencies_inner(sink.from, out)
            }
            CatalogItem::Index(idx) => self.arranged_introspection_dependencies_inner(idx.on, out),
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_) => (),
        }
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        match self.get_entry(&id).item() {
            CatalogItem::Table(_) => true,
            item @ (CatalogItem::View(_) | CatalogItem::MaterializedView(_)) => {
                item.uses().iter().any(|id| self.uses_tables(*id))
            }
            CatalogItem::Index(idx) => self.uses_tables(idx.on),
            CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::Func(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
        }
    }

    /// Indicates whether the indicated item is considered stable or not.
    ///
    /// Only stable items can be used as dependencies of other catalog items.
    fn is_stable(&self, id: GlobalId) -> bool {
        let mz_internal_id = self.ambient_schemas_by_name[MZ_INTERNAL_SCHEMA];
        match &self.get_entry(&id).name().qualifiers.schema_spec {
            SchemaSpecifier::Temporary => true,
            SchemaSpecifier::Id(id) => *id != mz_internal_id,
        }
    }

    fn ensure_no_unstable_uses(&self, item: &CatalogItem) -> Result<(), AdapterError> {
        let unstable_dependencies: Vec<_> = item
            .uses()
            .iter()
            .filter(|id| !self.is_stable(**id))
            .map(|id| self.get_entry(id).name().item.clone())
            .collect();

        if unstable_dependencies.is_empty() {
            Ok(())
        } else {
            let object_type = item.typ().to_string();
            Err(AdapterError::UnstableDependency {
                object_type,
                unstable_dependencies,
            })
        }
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedObjectName,
        conn_id: Option<ConnectionId>,
    ) -> FullObjectName {
        let conn_id = conn_id.unwrap_or(SYSTEM_CONN_ID);

        let database = match &name.qualifiers.database_spec {
            ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
            ResolvedDatabaseSpecifier::Id(id) => {
                RawDatabaseSpecifier::Name(self.get_database(id).name().to_string())
            }
        };
        let schema = self
            .get_schema(
                &name.qualifiers.database_spec,
                &name.qualifiers.schema_spec,
                conn_id,
            )
            .name()
            .schema
            .clone();
        FullObjectName {
            database,
            schema,
            item: name.item.clone(),
        }
    }

    pub fn get_entry(&self, id: &GlobalId) -> &CatalogEntry {
        &self.entry_by_id[id]
    }

    pub fn try_get_entry_in_schema(
        &self,
        name: &QualifiedObjectName,
        conn_id: ConnectionId,
    ) -> Option<&CatalogEntry> {
        self.get_schema(
            &name.qualifiers.database_spec,
            &name.qualifiers.schema_spec,
            conn_id,
        )
        .items
        .get(&name.item)
        .and_then(|id| self.try_get_entry(id))
    }

    /// Gets an entry named `item` from exactly one of system schemas.
    ///
    /// # Panics
    /// - If `item` is not an entry in any system schema
    /// - If more than one system schema has an entry named `item`.
    fn get_entry_in_system_schemas(&self, item: &str) -> &CatalogEntry {
        let mut res = None;
        for system_schema in &[
            PG_CATALOG_SCHEMA,
            INFORMATION_SCHEMA,
            MZ_CATALOG_SCHEMA,
            MZ_INTERNAL_SCHEMA,
        ] {
            let schema_id = &self.ambient_schemas_by_name[*system_schema];
            let schema = &self.ambient_schemas_by_id[schema_id];
            if let Some(global_id) = schema.items.get(item) {
                match res {
                    None => res = Some(self.get_entry(global_id)),
                    Some(_) => panic!("only call get_entry_in_system_schemas on objects uniquely identifiable in one system schema"),
                }
            }
        }

        res.unwrap_or_else(|| panic!("cannot find {} in system schema", item))
    }

    pub fn item_exists(&self, name: &QualifiedObjectName, conn_id: ConnectionId) -> bool {
        self.try_get_entry_in_schema(name, conn_id).is_some()
    }

    fn find_available_name(
        &self,
        mut name: QualifiedObjectName,
        conn_id: ConnectionId,
    ) -> QualifiedObjectName {
        let mut i = 0;
        let orig_item_name = name.item.clone();
        while self.item_exists(&name, conn_id) {
            i += 1;
            name.item = format!("{}{}", orig_item_name, i);
        }
        name
    }

    pub fn try_get_entry(&self, id: &GlobalId) -> Option<&CatalogEntry> {
        self.entry_by_id.get(id)
    }

    /// Create and insert the per replica log sources and log views.
    fn insert_replica_introspection_items(
        &mut self,
        logging: &ComputeReplicaLogging,
        replica_id: u64,
    ) {
        for (variant, source_id) in &logging.sources {
            let oid = self.allocate_oid().expect("cannot return error here");
            // TODO(lh): Once we get rid of legacy active logs, we should refactor the
            // CatalogItem::Log. For now  we just use the log variant to lookup the unique CatalogItem
            // in BUILTINS.
            let log = BUILTINS::logs()
                .find(|log| log.variant == *variant)
                .expect("variant must be included in builtins");

            let source_name = QualifiedObjectName {
                qualifiers: ObjectQualifiers {
                    database_spec: ResolvedDatabaseSpecifier::Ambient,
                    schema_spec: SchemaSpecifier::Id(self.get_mz_internal_schema_id().clone()),
                },
                item: format!("{}_{}", log.name, replica_id),
            };
            self.insert_item(
                *source_id,
                oid,
                source_name,
                CatalogItem::Log(Log {
                    variant: variant.clone(),
                    has_storage_collection: true,
                }),
            );
        }

        for (logview, id) in &logging.views {
            let (sql_template, name_template) = logview.get_template();
            assert!(sql_template.find("{}").is_some());
            assert!(name_template.find("{}").is_some());
            let name = name_template.replace("{}", &replica_id.to_string());
            let sql = "CREATE VIEW ".to_string()
                + MZ_INTERNAL_SCHEMA
                + "."
                + &name
                + " AS "
                + &sql_template.replace("{}", &replica_id.to_string());

            let item = self.parse_view_item(sql);

            match item {
                Ok(item) => {
                    let oid = self.allocate_oid().expect("cannot return error here");
                    let view_name = QualifiedObjectName {
                        qualifiers: ObjectQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(
                                self.get_mz_internal_schema_id().clone(),
                            ),
                        },
                        item: name,
                    };

                    self.insert_item(*id, oid, view_name, item);
                }
                Err(e) => {
                    // This error should never happen, but if we add a logging
                    // source + view pair and make a mistake in the migration
                    // it's better to bring up the instance without the view
                    // than to crash.
                    tracing::error!("Could not create log view: {}", e);
                }
            }
        }
    }

    /// Parse a SQL string into a catalog view item with only a limited
    /// context.
    pub fn parse_view_item(&self, create_sql: String) -> Result<CatalogItem, anyhow::Error> {
        let session_catalog = ConnCatalog {
            state: Cow::Borrowed(self),
            conn_id: SYSTEM_CONN_ID,
            compute_instance: "default".into(),
            database: self
                .resolve_database(DEFAULT_DATABASE_NAME)
                .ok()
                .map(|db| db.id()),
            search_path: Vec::new(),
            user: SYSTEM_USER.clone(),
            prepared_statements: None,
        };
        let stmt = mz_sql::parse::parse(&create_sql)?.into_element();
        let (stmt, depends_on) = mz_sql::names::resolve(&session_catalog, stmt)?;
        let depends_on = depends_on.into_iter().collect();
        let plan = mz_sql::plan::plan(None, &session_catalog, stmt, &Params::empty())?;
        Ok(match plan {
            Plan::CreateView(CreateViewPlan { view, .. }) => {
                let mut optimizer = Optimizer::logical_optimizer();
                let optimized_expr = optimizer.optimize(view.expr)?;
                let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    optimized_expr,
                    desc,
                    conn_id: None,
                    depends_on,
                })
            }
            _ => bail!("Expected valid CREATE VIEW statement"),
        })
    }

    /// Returns all indexes on the given object and compute instance known in
    /// the catalog.
    pub fn get_indexes_on(
        &self,
        id: GlobalId,
        compute_instance: ComputeInstanceId,
    ) -> impl Iterator<Item = (GlobalId, &Index)> {
        let index_matches =
            move |idx: &Index| idx.on == id && idx.compute_instance == compute_instance;

        self.try_get_entry(&id)
            .into_iter()
            .map(move |e| {
                e.used_by()
                    .iter()
                    .filter_map(move |uses_id| match self.get_entry(uses_id).item() {
                        CatalogItem::Index(index) if index_matches(index) => {
                            Some((*uses_id, index))
                        }
                        _ => None,
                    })
            })
            .flatten()
    }

    /// Associates a name, `GlobalId`, and entry.
    fn insert_item(
        &mut self,
        id: GlobalId,
        oid: u32,
        name: QualifiedObjectName,
        item: CatalogItem,
    ) {
        if !id.is_system() && !item.is_placeholder() {
            info!(
                "create {} {} ({})",
                item.typ(),
                self.resolve_full_name(&name, None),
                id
            );
        }

        if !id.is_system() {
            if let CatalogItem::Index(Index {
                compute_instance, ..
            })
            | CatalogItem::MaterializedView(MaterializedView {
                compute_instance, ..
            }) = item
            {
                self.compute_instances_by_id
                    .get_mut(&compute_instance)
                    .unwrap()
                    .exports
                    .insert(id);
            };
        }

        let entry = CatalogEntry {
            item,
            name,
            id,
            oid,
            used_by: Vec::new(),
        };
        for u in entry.uses() {
            match self.entry_by_id.get_mut(u) {
                Some(metadata) => metadata.used_by.push(entry.id),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while installing {}",
                    &u,
                    self.resolve_full_name(&entry.name, entry.conn_id())
                ),
            }
        }
        let conn_id = entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &entry.name().qualifiers.database_spec,
            &entry.name().qualifiers.schema_spec,
            conn_id,
        );

        let prev_id = if let CatalogItem::Func(_) = entry.item() {
            schema.functions.insert(entry.name.item.clone(), entry.id)
        } else {
            schema.items.insert(entry.name.item.clone(), entry.id)
        };

        assert!(
            prev_id.is_none(),
            "builtin name collision on {:?}",
            entry.name.item.clone()
        );

        self.entry_by_id.insert(entry.id, entry.clone());
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn drop_item(&mut self, id: GlobalId) {
        let metadata = self.entry_by_id.remove(&id).unwrap();
        if !metadata.item.is_placeholder() {
            info!(
                "drop {} {} ({})",
                metadata.item_type(),
                self.resolve_full_name(&metadata.name, metadata.conn_id()),
                id
            );
        }
        for u in metadata.uses() {
            if let Some(dep_metadata) = self.entry_by_id.get_mut(u) {
                dep_metadata.used_by.retain(|u| *u != metadata.id)
            }
        }

        let conn_id = metadata.item.conn_id().unwrap_or(SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &metadata.name().qualifiers.database_spec,
            &metadata.name().qualifiers.schema_spec,
            conn_id,
        );
        schema
            .items
            .remove(&metadata.name().item)
            .expect("catalog out of sync");

        if let CatalogItem::Index(Index {
            compute_instance, ..
        })
        | CatalogItem::MaterializedView(MaterializedView {
            compute_instance, ..
        }) = metadata.item
        {
            if !id.is_system() {
                assert!(
                    self.compute_instances_by_id
                        .get_mut(&compute_instance)
                        .unwrap()
                        .exports
                        .remove(&id),
                    "catalog out of sync"
                );
            }
        };
    }

    fn get_database(&self, database_id: &DatabaseId) -> &Database {
        &self.database_by_id[database_id]
    }

    fn insert_compute_instance(
        &mut self,
        id: ComputeInstanceId,
        name: String,
        introspection_source_indexes: Vec<(&'static BuiltinLog, GlobalId)>,
    ) {
        let mut log_indexes = BTreeMap::new();
        for (log, index_id) in introspection_source_indexes {
            let source_name = FullObjectName {
                database: RawDatabaseSpecifier::Ambient,
                schema: log.schema.into(),
                item: log.name.into(),
            };
            let index_name = format!("{}_{}_primary_idx", log.name, id);
            let mut index_name = QualifiedObjectName {
                qualifiers: ObjectQualifiers {
                    database_spec: ResolvedDatabaseSpecifier::Ambient,
                    schema_spec: SchemaSpecifier::Id(self.get_mz_internal_schema_id().clone()),
                },
                item: index_name.clone(),
            };
            index_name = self.find_available_name(index_name, SYSTEM_CONN_ID);
            let index_item_name = index_name.item.clone();
            // TODO(clusters): Avoid panicking here on ID exhaustion
            // before stabilization.
            //
            // The OID counter is an i32, and could plausibly be exhausted.
            // Preallocating OIDs for each logging index is eminently
            // doable, but annoying enough that we don't bother now.
            let oid = self.allocate_oid().expect("cannot return error here");
            let log_id = self.resolve_builtin_log(log);
            self.insert_item(
                index_id,
                oid,
                index_name,
                CatalogItem::Index(Index {
                    on: log_id,
                    keys: log
                        .variant
                        .index_by()
                        .into_iter()
                        .map(MirScalarExpr::Column)
                        .collect(),
                    create_sql: index_sql(
                        index_item_name,
                        id,
                        source_name,
                        &log.variant.desc(),
                        &log.variant.index_by(),
                    ),
                    conn_id: None,
                    depends_on: vec![log_id],
                    compute_instance: id,
                }),
            );
            log_indexes.insert(log.variant.clone(), index_id);
        }

        self.compute_instances_by_id.insert(
            id,
            ComputeInstance {
                name: name.clone(),
                id,
                exports: HashSet::new(),
                log_indexes,
                replica_id_by_name: HashMap::new(),
                replicas_by_id: HashMap::new(),
            },
        );
        assert!(self.compute_instances_by_name.insert(name, id).is_none());
    }

    fn insert_compute_replica(
        &mut self,
        on_instance: ComputeInstanceId,
        replica_name: String,
        replica_id: ReplicaId,
        config: ComputeReplicaConfig,
    ) {
        self.insert_replica_introspection_items(&config.logging, replica_id);
        let replica = ComputeReplica {
            config,
            process_status: HashMap::new(),
        };
        let compute_instance = self.compute_instances_by_id.get_mut(&on_instance).unwrap();
        assert!(compute_instance
            .replica_id_by_name
            .insert(replica_name, replica_id)
            .is_none());
        assert!(compute_instance
            .replicas_by_id
            .insert(replica_id, replica)
            .is_none());
    }

    /// Try inserting/updating the status of a compute instance process as
    /// described by the given event.
    ///
    /// This method returns `true` if the insert was successful. It returns
    /// `false` if the insert was unsuccessful, i.e., the given compute instance
    /// replica is not found.
    ///
    /// This treatment of non-existing replicas allows us to gracefully handle
    /// scenarios where we receive status updates for replicas that we have
    /// already removed from the catalog.
    fn try_insert_compute_instance_status(&mut self, event: ComputeInstanceEvent) -> bool {
        self.compute_instances_by_id
            .get_mut(&event.instance_id)
            .and_then(|instance| instance.replicas_by_id.get_mut(&event.replica_id))
            .map(|replica| replica.process_status.insert(event.process_id, event))
            .is_some()
    }

    /// Try getting the status of the given compute instance process.
    ///
    /// This method returns `None` if no status was found for the given
    /// compute instance process because:
    ///   * The given compute replica is not found. This can occur
    ///     if we already dropped the replica from the catalog, but we still
    ///     receive status updates.
    ///   * The given replica process is not found. This is the case when we
    ///     receive the first status update for a new replica process.
    fn try_get_compute_instance_status(
        &self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        process_id: ProcessId,
    ) -> Option<ComputeInstanceEvent> {
        self.compute_instances_by_id
            .get(&instance_id)
            .and_then(|instance| instance.replicas_by_id.get(&replica_id))
            .and_then(|replica| replica.process_status.get(&process_id))
            .cloned()
    }

    /// Insert system configuration `name` with `value`.
    fn insert_system_configuration(&mut self, name: &str, value: &str) -> Result<(), AdapterError> {
        self.system_configuration.set(name, value)
    }

    /// Remove system configuration `name`.
    fn remove_system_configuration(&mut self, name: &str) -> Result<(), AdapterError> {
        self.system_configuration.reset(name)
    }

    /// Remove all system configurations.
    fn clear_system_configuration(&mut self) {
        self.system_configuration = SystemVars::default();
    }

    /// Gets the schema map for the database matching `database_spec`.
    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        let schema = match database_spec {
            ResolvedDatabaseSpecifier::Ambient if schema_name == MZ_TEMP_SCHEMA => {
                self.temporary_schemas.get(&conn_id)
            }
            ResolvedDatabaseSpecifier::Ambient => self
                .ambient_schemas_by_name
                .get(schema_name)
                .and_then(|id| self.ambient_schemas_by_id.get(id)),
            ResolvedDatabaseSpecifier::Id(id) => self.database_by_id.get(id).and_then(|db| {
                db.schemas_by_name
                    .get(schema_name)
                    .and_then(|id| db.schemas_by_id.get(id))
            }),
        };
        schema.ok_or_else(|| SqlCatalogError::UnknownSchema(schema_name.into()))
    }

    pub fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: ConnectionId,
    ) -> &Schema {
        // Keep in sync with `get_schemas_mut`
        match (database_spec, schema_spec) {
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => {
                &self.temporary_schemas[&conn_id]
            }
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(id)) => {
                &self.ambient_schemas_by_id[id]
            }

            (ResolvedDatabaseSpecifier::Id(database_id), SchemaSpecifier::Id(schema_id)) => {
                &self.database_by_id[database_id].schemas_by_id[schema_id]
            }
            (ResolvedDatabaseSpecifier::Id(_), SchemaSpecifier::Temporary) => {
                unreachable!("temporary schemas are in the ambient database")
            }
        }
    }

    fn get_schema_mut(
        &mut self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: ConnectionId,
    ) -> &mut Schema {
        // Keep in sync with `get_schemas`
        match (database_spec, schema_spec) {
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => self
                .temporary_schemas
                .get_mut(&conn_id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(id)) => self
                .ambient_schemas_by_id
                .get_mut(id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Id(database_id), SchemaSpecifier::Id(schema_id)) => self
                .database_by_id
                .get_mut(database_id)
                .expect("catalog out of sync")
                .schemas_by_id
                .get_mut(schema_id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Id(_), SchemaSpecifier::Temporary) => {
                unreachable!("temporary schemas are in the ambient database")
            }
        }
    }

    pub fn get_mz_catalog_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[MZ_CATALOG_SCHEMA]
    }

    pub fn get_pg_catalog_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[PG_CATALOG_SCHEMA]
    }

    pub fn get_information_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[INFORMATION_SCHEMA]
    }

    pub fn get_mz_internal_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[MZ_INTERNAL_SCHEMA]
    }

    pub fn is_system_schema(&self, schema: &str) -> bool {
        schema == MZ_CATALOG_SCHEMA
            || schema == PG_CATALOG_SCHEMA
            || schema == INFORMATION_SCHEMA
            || schema == MZ_INTERNAL_SCHEMA
    }

    /// Optimized lookup for a builtin table
    ///
    /// Panics if the builtin table doesn't exist in the catalog
    pub fn resolve_builtin_table(&self, builtin: &'static BuiltinTable) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Table(builtin))
    }

    /// Optimized lookup for a builtin log
    ///
    /// Panics if the builtin log doesn't exist in the catalog
    pub fn resolve_builtin_log(&self, builtin: &'static BuiltinLog) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Log(builtin))
    }

    /// Optimized lookup for a builtin storage collection
    ///
    /// Panics if the builtin storage collection doesn't exist in the catalog
    pub fn resolve_builtin_source(&self, builtin: &'static BuiltinSource) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Source(builtin))
    }

    /// Optimized lookup for a builtin object
    ///
    /// Panics if the builtin object doesn't exist in the catalog
    pub fn resolve_builtin_object<T: TypeReference>(&self, builtin: &Builtin<T>) -> GlobalId {
        let schema_id = &self.ambient_schemas_by_name[builtin.schema()];
        let schema = &self.ambient_schemas_by_id[schema_id];
        schema.items[builtin.name()].clone()
    }

    pub fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        &self.config
    }

    pub fn resolve_database(&self, database_name: &str) -> Result<&Database, SqlCatalogError> {
        match self.database_by_name.get(database_name) {
            Some(id) => Ok(&self.database_by_id[id]),
            None => Err(SqlCatalogError::UnknownDatabase(database_name.into())),
        }
    }

    pub fn resolve_schema(
        &self,
        current_database: Option<&DatabaseId>,
        database_name: Option<&str>,
        schema_name: &str,
        conn_id: ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        let database_spec = match database_name {
            // If a database is explicitly specified, validate it. Note that we
            // intentionally do not validate `current_database` to permit
            // querying `mz_catalog` with an invalid session database, e.g., so
            // that you can run `SHOW DATABASES` to *find* a valid database.
            Some(database) => Some(ResolvedDatabaseSpecifier::Id(
                self.resolve_database(database)?.id().clone(),
            )),
            None => current_database.map(|id| ResolvedDatabaseSpecifier::Id(id.clone())),
        };

        // First try to find the schema in the named database.
        if let Some(database_spec) = database_spec {
            if let Ok(schema) =
                self.resolve_schema_in_database(&database_spec, schema_name, conn_id)
            {
                return Ok(schema);
            }
        }

        // Then fall back to the ambient database.
        if let Ok(schema) = self.resolve_schema_in_database(
            &ResolvedDatabaseSpecifier::Ambient,
            schema_name,
            conn_id,
        ) {
            return Ok(schema);
        }

        Err(SqlCatalogError::UnknownSchema(schema_name.into()))
    }

    pub fn resolve_compute_instance(
        &self,
        name: &str,
    ) -> Result<&ComputeInstance, SqlCatalogError> {
        let id = self
            .compute_instances_by_name
            .get(name)
            .ok_or_else(|| SqlCatalogError::UnknownComputeInstance(name.to_string()))?;
        Ok(&self.compute_instances_by_id[id])
    }

    /// Resolves [`PartialObjectName`] into a [`CatalogEntry`].
    ///
    /// If `name` does not specify a database, the `current_database` is used.
    /// If `name` does not specify a schema, then the schemas in `search_path`
    /// are searched in order.
    #[allow(clippy::useless_let_if_seq)]
    pub fn resolve(
        &self,
        get_schema_entries: fn(&Schema) -> &BTreeMap<String, GlobalId>,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialObjectName,
        conn_id: ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        // If a schema name was specified, just try to find the item in that
        // schema. If no schema was specified, try to find the item in the connection's
        // temporary schema. If the item is not found, try to find the item in every
        // schema in the search path.
        let schemas = match &name.schema {
            Some(schema_name) => {
                match self.resolve_schema(
                    current_database,
                    name.database.as_deref(),
                    schema_name,
                    conn_id,
                ) {
                    Ok(schema) => vec![(schema.name.database.clone(), schema.id.clone())],
                    Err(e) => return Err(e),
                }
            }
            None => match self
                .get_schema(
                    &ResolvedDatabaseSpecifier::Ambient,
                    &SchemaSpecifier::Temporary,
                    conn_id,
                )
                .items
                .get(&name.item)
            {
                Some(id) => return Ok(self.get_entry(id)),
                None => search_path.to_vec(),
            },
        };

        for (database_spec, schema_spec) in schemas {
            let schema = self.get_schema(&database_spec, &schema_spec, conn_id);

            if let Some(id) = get_schema_entries(schema).get(&name.item) {
                return Ok(&self.entry_by_id[id]);
            }
        }
        Err(SqlCatalogError::UnknownItem(name.to_string()))
    }

    /// Resolves `name` to a non-function [`CatalogEntry`].
    pub fn resolve_entry(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialObjectName,
        conn_id: ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.items,
            current_database,
            search_path,
            name,
            conn_id,
        )
    }

    /// Resolves `name` to a function [`CatalogEntry`].
    pub fn resolve_function(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialObjectName,
        conn_id: ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.functions,
            current_database,
            search_path,
            name,
            conn_id,
        )
    }

    pub fn resolve_storage_host_config(
        &self,
        storage_host_config: PlanStorageHostConfig,
        allow_undefined: bool,
    ) -> Result<StorageHostConfig, AdapterError> {
        let host_sizes = &self.storage_host_sizes;
        let mut entries = host_sizes.0.iter().collect::<Vec<_>>();
        entries.sort_by_key(
            |(
                _name,
                StorageHostResourceAllocation {
                    workers,
                    memory_limit,
                    ..
                },
            )| (workers, memory_limit),
        );
        let expected = entries.into_iter().map(|(name, _)| name.clone()).collect();
        let storage_host_config = match storage_host_config {
            PlanStorageHostConfig::Remote { addr } => StorageHostConfig::Remote { addr },
            PlanStorageHostConfig::Managed { size } => {
                let allocation = host_sizes.0.get(&size).ok_or_else(|| {
                    AdapterError::InvalidStorageHostSize {
                        size: size.clone(),
                        expected,
                    }
                })?;
                StorageHostConfig::Managed {
                    allocation: allocation.clone(),
                    size,
                }
            }
            PlanStorageHostConfig::Undefined => {
                if !allow_undefined {
                    return Err(AdapterError::StorageHostSizeRequired { expected });
                }
                let (size, allocation) = self.default_storage_host_size();
                StorageHostConfig::Managed { allocation, size }
            }
        };
        Ok(storage_host_config)
    }

    /// Return current system configuration.
    pub fn system_config(&self) -> &SystemVars {
        &self.system_configuration
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> String {
        serde_json::to_string(&self.database_by_id).expect("serialization cannot fail")
    }

    pub fn availability_zones(&self) -> &[String] {
        &self.availability_zones
    }

    /// Returns the default storage host size
    ///
    /// If a default size was given as configuration, it is always used, otherwise the
    /// smallest host size is used instead.
    pub fn default_storage_host_size(&self) -> (String, StorageHostResourceAllocation) {
        match &self.default_storage_host_size {
            Some(default_storage_host_size) => {
                // The default is guaranteed to be in the size map during startup
                let allocation = self
                    .storage_host_sizes
                    .0
                    .get(default_storage_host_size)
                    .expect("default storage host size must exist in size map");
                (default_storage_host_size.clone(), allocation.clone())
            }
            None => {
                let (size, allocation) = self
                    .storage_host_sizes
                    .0
                    .iter()
                    .min_by_key(|(_, a)| (a.workers, a.memory_limit))
                    .expect("should have at least one valid storage instance size");
                (size.clone(), allocation.clone())
            }
        }
    }

    // TODO(mjibson): Is there a way to make this a closure to avoid explicitly
    // passing tx, session, and builtin_table_updates?
    fn add_to_audit_log<S: Append>(
        &self,
        session: Option<&Session>,
        tx: &mut storage::Transaction<S>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        event_type: EventType,
        object_type: ObjectType,
        details: EventDetails,
    ) -> Result<(), Error> {
        let user = session.map(|session| session.user().name.to_string());
        let occurred_at = (self.config.now)();
        let id = tx.get_and_increment_id(storage::AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
        let event = VersionedEvent::new(id, event_type, object_type, details, user, occurred_at);
        builtin_table_updates.push(self.pack_audit_log_update(&event)?);
        audit_events.push(event.clone());
        tx.insert_audit_log_event(event);
        Ok(())
    }

    fn add_to_storage_usage<S: Append>(
        &self,
        tx: &mut storage::Transaction<S>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        shard_id: Option<String>,
        size_bytes: u64,
    ) -> Result<(), Error> {
        let collection_timestamp = (self.config.now)();
        let id = tx.get_and_increment_id(storage::STORAGE_USAGE_ID_ALLOC_KEY.to_string())?;

        let details = VersionedStorageUsage::new(id, shard_id, size_bytes, collection_timestamp);
        builtin_table_updates.push(self.pack_storage_usage_update(&details)?);
        tx.insert_storage_usage_event(details);
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConnCatalog<'a> {
    state: Cow<'a, CatalogState>,
    conn_id: ConnectionId,
    compute_instance: String,
    database: Option<DatabaseId>,
    search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    user: User,
    prepared_statements: Option<Cow<'a, HashMap<String, PreparedStatement>>>,
}

impl ConnCatalog<'_> {
    pub fn conn_id(&self) -> ConnectionId {
        self.conn_id
    }

    pub fn state(&self) -> &CatalogState {
        &*self.state
    }

    pub fn into_owned(self) -> ConnCatalog<'static> {
        ConnCatalog {
            state: Cow::Owned(self.state.into_owned()),
            conn_id: self.conn_id,
            compute_instance: self.compute_instance,
            database: self.database,
            search_path: self.search_path,
            user: self.user,
            prepared_statements: self.prepared_statements.map(|s| Cow::Owned(s.into_owned())),
        }
    }

    fn effective_search_path(
        &self,
        include_temp_schema: bool,
    ) -> Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)> {
        let mut v = Vec::with_capacity(self.search_path.len() + 3);
        // Temp schema is only included for relations and data types, not for functions and operators
        let temp_schema = (
            ResolvedDatabaseSpecifier::Ambient,
            SchemaSpecifier::Temporary,
        );
        if include_temp_schema && !self.search_path.contains(&temp_schema) {
            v.push(temp_schema);
        }
        let default_schemas = [
            (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(self.state.get_mz_catalog_schema_id().clone()),
            ),
            (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(self.state.get_pg_catalog_schema_id().clone()),
            ),
        ];
        for schema in default_schemas.into_iter() {
            if !self.search_path.contains(&schema) {
                v.push(schema);
            }
        }
        v.extend_from_slice(&self.search_path);
        v
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Database {
    pub name: String,
    pub id: DatabaseId,
    #[serde(skip)]
    pub oid: u32,
    pub schemas_by_id: BTreeMap<SchemaId, Schema>,
    pub schemas_by_name: BTreeMap<String, SchemaId>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Schema {
    pub name: QualifiedSchemaName,
    pub id: SchemaSpecifier,
    #[serde(skip)]
    pub oid: u32,
    pub items: BTreeMap<String, GlobalId>,
    pub functions: BTreeMap<String, GlobalId>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Role {
    pub name: String,
    pub id: RoleId,
    #[serde(skip)]
    pub oid: u32,
}

impl Role {
    pub fn is_user(&self) -> bool {
        self.id.is_user()
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct ComputeInstance {
    pub name: String,
    pub id: ComputeInstanceId,
    pub log_indexes: BTreeMap<LogVariant, GlobalId>,
    /// Indexes and materialized views exported by this compute instance.
    /// Does not include introspection source indexes.
    pub exports: HashSet<GlobalId>,
    pub replica_id_by_name: HashMap<String, ReplicaId>,
    pub replicas_by_id: HashMap<ReplicaId, ComputeReplica>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ComputeReplica {
    pub config: ComputeReplicaConfig,
    pub process_status: HashMap<ProcessId, ComputeInstanceEvent>,
}

#[derive(Clone, Debug)]
pub struct CatalogEntry {
    item: CatalogItem,
    used_by: Vec<GlobalId>,
    id: GlobalId,
    oid: u32,
    name: QualifiedObjectName,
}

#[derive(Debug, Clone, Serialize)]
pub enum CatalogItem {
    Table(Table),
    Source(Source),
    Log(Log),
    View(View),
    MaterializedView(MaterializedView),
    Sink(Sink),
    Index(Index),
    Type(Type),
    Func(Func),
    Secret(Secret),
    Connection(Connection),
}

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    pub create_sql: String,
    pub desc: RelationDesc,
    #[serde(skip)]
    pub defaults: Vec<Expr<Aug>>,
    pub conn_id: Option<ConnectionId>,
    pub depends_on: Vec<GlobalId>,
}

impl Table {
    // The Coordinator controls insertions for tables (including system tables),
    // so they are realtime.
    pub fn timeline(&self) -> Timeline {
        Timeline::EpochMilliseconds
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum DataSourceDesc {
    /// Receives data from an external system
    Ingestion(Ingestion),
    /// Receives data from some other source
    Source,
    /// Receives introspection data from an internal system
    Introspection(IntrospectionType),
}

#[derive(Debug, Clone, Serialize)]
pub struct Source {
    pub create_sql: String,
    pub data_source: DataSourceDesc,
    pub desc: RelationDesc,
    pub timeline: Timeline,
    pub depends_on: Vec<GlobalId>,
}

impl Source {
    pub fn size(&self) -> Option<&str> {
        match &self.data_source {
            DataSourceDesc::Ingestion(Ingestion { host_config, .. }) => host_config.size(),
            DataSourceDesc::Introspection(_) | DataSourceDesc::Source => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Log {
    pub variant: LogVariant,
    /// Whether the log is backed by a storage collection.
    pub has_storage_collection: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ingestion {
    // TODO(benesch): this field contains connection information that could be
    // derived from the connection ID. Too hard to fix at the moment.
    pub desc: SourceDesc,
    pub source_imports: HashSet<GlobalId>,
    /// The *additional* subsource exports of this ingestion. Each collection identified by its
    /// GlobalId will contain the contents of this ingestion's output stream that is identified by
    /// the index.
    ///
    /// This map does *not* include the export of the source associated with the ingestion itself
    pub subsource_exports: HashMap<GlobalId, usize>,
    pub host_config: StorageHostConfig,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sink {
    pub create_sql: String,
    pub from: GlobalId,
    // TODO(benesch): this field duplicates information that could be derived
    // from the connection ID. Too hard to fix at the moment.
    pub connection: StorageSinkConnectionState,
    pub envelope: SinkEnvelope,
    pub with_snapshot: bool,
    pub depends_on: Vec<GlobalId>,
    pub host_config: StorageHostConfig,
}

impl Sink {
    pub fn connection_id(&self) -> Option<GlobalId> {
        match &self.connection {
            StorageSinkConnectionState::Pending(pending) => pending.connection_id(),
            StorageSinkConnectionState::Ready(ready) => ready.connection_id(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum StorageSinkConnectionState {
    Pending(StorageSinkConnectionBuilder),
    Ready(StorageSinkConnection),
}

#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub create_sql: String,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub conn_id: Option<ConnectionId>,
    pub depends_on: Vec<GlobalId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaterializedView {
    pub create_sql: String,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub depends_on: Vec<GlobalId>,
    pub compute_instance: ComputeInstanceId,
}

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<MirScalarExpr>,
    pub conn_id: Option<ConnectionId>,
    pub depends_on: Vec<GlobalId>,
    pub compute_instance: ComputeInstanceId,
}

#[derive(Debug, Clone, Serialize)]
pub struct Type {
    pub create_sql: String,
    #[serde(skip)]
    pub details: CatalogTypeDetails<IdReference>,
    pub depends_on: Vec<GlobalId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Func {
    #[serde(skip)]
    pub inner: &'static mz_sql::func::Func,
}

#[derive(Debug, Clone, Serialize)]
pub struct Secret {
    pub create_sql: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Connection {
    pub create_sql: String,
    pub connection: mz_storage::types::connections::Connection,
    pub depends_on: Vec<GlobalId>,
}

pub struct TransactionResult<R> {
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
    pub audit_events: Vec<VersionedEvent>,
    pub collections: Vec<mz_stash::Id>,
    pub result: R,
}

impl CatalogItem {
    /// Returns a string indicating the type of this catalog entry.
    pub(crate) fn typ(&self) -> mz_sql::catalog::CatalogItemType {
        match self {
            CatalogItem::Table(_) => mz_sql::catalog::CatalogItemType::Table,
            CatalogItem::Source(_) => mz_sql::catalog::CatalogItemType::Source,
            CatalogItem::Log(_) => mz_sql::catalog::CatalogItemType::Source,
            CatalogItem::Sink(_) => mz_sql::catalog::CatalogItemType::Sink,
            CatalogItem::View(_) => mz_sql::catalog::CatalogItemType::View,
            CatalogItem::MaterializedView(_) => mz_sql::catalog::CatalogItemType::MaterializedView,
            CatalogItem::Index(_) => mz_sql::catalog::CatalogItemType::Index,
            CatalogItem::Type(_) => mz_sql::catalog::CatalogItemType::Type,
            CatalogItem::Func(_) => mz_sql::catalog::CatalogItemType::Func,
            CatalogItem::Secret(_) => mz_sql::catalog::CatalogItemType::Secret,
            CatalogItem::Connection(_) => mz_sql::catalog::CatalogItemType::Connection,
        }
    }

    pub fn desc(&self, name: &FullObjectName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        match &self {
            CatalogItem::Source(src) => Ok(Cow::Borrowed(&src.desc)),
            CatalogItem::Log(log) => Ok(Cow::Owned(log.variant.desc())),
            CatalogItem::Table(tbl) => Ok(Cow::Borrowed(&tbl.desc)),
            CatalogItem::View(view) => Ok(Cow::Borrowed(&view.desc)),
            CatalogItem::MaterializedView(mview) => Ok(Cow::Borrowed(&mview.desc)),
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => Err(SqlCatalogError::InvalidDependency {
                name: name.to_string(),
                typ: self.typ(),
            }),
        }
    }

    pub fn func(
        &self,
        name: &QualifiedObjectName,
    ) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        match &self {
            CatalogItem::Func(func) => Ok(func.inner),
            _ => Err(SqlCatalogError::UnexpectedType(
                name.item.to_string(),
                CatalogItemType::Func,
            )),
        }
    }

    pub fn source_desc(
        &self,
        name: &QualifiedObjectName,
    ) -> Result<Option<&SourceDesc>, SqlCatalogError> {
        match &self {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => Ok(Some(&ingestion.desc)),
                DataSourceDesc::Source | DataSourceDesc::Introspection(_) => Ok(None),
            },
            _ => Err(SqlCatalogError::UnexpectedType(
                name.item.clone(),
                CatalogItemType::Source,
            )),
        }
    }

    /// Collects the identifiers of the dataflows that this item depends
    /// upon.
    pub fn uses(&self) -> &[GlobalId] {
        match self {
            CatalogItem::Func(_) => &[],
            CatalogItem::Index(idx) => &idx.depends_on,
            CatalogItem::Sink(sink) => &sink.depends_on,
            CatalogItem::Source(source) => &source.depends_on,
            CatalogItem::Log(_) => &[],
            CatalogItem::Table(table) => &table.depends_on,
            CatalogItem::Type(typ) => &typ.depends_on,
            CatalogItem::View(view) => &view.depends_on,
            CatalogItem::MaterializedView(mview) => &mview.depends_on,
            CatalogItem::Secret(_) => &[],
            CatalogItem::Connection(connection) => &connection.depends_on,
        }
    }

    /// Indicates whether this item is a placeholder for a future item
    /// or if it's actually a real item.
    pub fn is_placeholder(&self) -> bool {
        match self {
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::Table(_)
            | CatalogItem::Type(_)
            | CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
            CatalogItem::Sink(s) => match s.connection {
                StorageSinkConnectionState::Pending(_) => true,
                StorageSinkConnectionState::Ready(_) => false,
            },
        }
    }

    /// Returns the connection ID that this item belongs to, if this item is
    /// temporary.
    pub fn conn_id(&self) -> Option<ConnectionId> {
        match self {
            CatalogItem::View(view) => view.conn_id,
            CatalogItem::Index(index) => index.conn_id,
            CatalogItem::Table(table) => table.conn_id,
            CatalogItem::Log(_)
            | CatalogItem::Source(_)
            | CatalogItem::Sink(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Connection(_) => None,
        }
    }

    /// Indicates whether this item is temporary or not.
    pub fn is_temporary(&self) -> bool {
        self.conn_id().is_some()
    }

    /// Returns a clone of `self` with all instances of `from` renamed to `to`
    /// (with the option of including the item's own name) or errors if request
    /// is ambiguous.
    fn rename_item_refs(
        &self,
        from: FullObjectName,
        to_item_name: String,
        rename_self: bool,
    ) -> Result<CatalogItem, String> {
        let do_rewrite = |create_sql: String| -> Result<String, String> {
            let mut create_stmt = mz_sql::parse::parse(&create_sql).unwrap().into_element();
            if rename_self {
                mz_sql::ast::transform::create_stmt_rename(&mut create_stmt, to_item_name.clone());
            }
            // Determination of what constitutes an ambiguous request is done here.
            mz_sql::ast::transform::create_stmt_rename_refs(&mut create_stmt, from, to_item_name)?;
            Ok(create_stmt.to_ast_string_stable())
        };

        match self {
            CatalogItem::Table(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Table(i))
            }
            CatalogItem::Log(i) => Ok(CatalogItem::Log(i.clone())),
            CatalogItem::Source(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Source(i))
            }
            CatalogItem::Sink(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Sink(i))
            }
            CatalogItem::View(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::View(i))
            }
            CatalogItem::MaterializedView(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::MaterializedView(i))
            }
            CatalogItem::Index(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Index(i))
            }
            CatalogItem::Secret(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Secret(i))
            }
            CatalogItem::Func(_) | CatalogItem::Type(_) => {
                unreachable!("{}s cannot be renamed", self.typ())
            }
            CatalogItem::Connection(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Connection(i))
            }
        }
    }

    fn compute_instance_id(&self) -> Option<ComputeInstanceId> {
        match self {
            CatalogItem::MaterializedView(mv) => Some(mv.compute_instance),
            CatalogItem::Index(index) => Some(index.compute_instance),
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => None,
        }
    }
}

impl CatalogEntry {
    /// Reports the description of the datums produced by this catalog item.
    pub fn desc(&self, name: &FullObjectName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        self.item.desc(name)
    }

    /// Returns the [`mz_sql::func::Func`] associated with this `CatalogEntry`.
    pub fn func(&self) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        self.item.func(self.name())
    }

    /// Returns the inner [`Index`] if this entry is an index, else `None`.
    pub fn index(&self) -> Option<&Index> {
        match self.item() {
            CatalogItem::Index(idx) => Some(idx),
            _ => None,
        }
    }

    /// Returns the inner [`Source`] if this entry is a source, else `None`.
    pub fn source(&self) -> Option<&Source> {
        match self.item() {
            CatalogItem::Source(src) => Some(src),
            _ => None,
        }
    }

    /// Returns the inner [`Sink`] if this entry is a sink, else `None`.
    pub fn sink(&self) -> Option<&Sink> {
        match self.item() {
            CatalogItem::Sink(sink) => Some(sink),
            _ => None,
        }
    }

    /// Returns the inner [`Secret`] if this entry is a secret, else `None`.
    pub fn secret(&self) -> Option<&Secret> {
        match self.item() {
            CatalogItem::Secret(secret) => Some(secret),
            _ => None,
        }
    }

    pub fn connection(&self) -> Result<&Connection, SqlCatalogError> {
        match self.item() {
            CatalogItem::Connection(connection) => Ok(connection),
            _ => Err(SqlCatalogError::UnknownConnection(self.name().to_string())),
        }
    }

    /// Returns the [`mz_storage::types::sources::SourceDesc`] associated with
    /// this `CatalogEntry`, if any.
    pub fn source_desc(&self) -> Result<Option<&SourceDesc>, SqlCatalogError> {
        self.item.source_desc(self.name())
    }

    /// Reports whether this catalog entry is a table.
    pub fn is_table(&self) -> bool {
        matches!(self.item(), CatalogItem::Table(_))
    }

    /// Reports whether this catalog entry is a source.
    pub fn is_source(&self) -> bool {
        matches!(self.item(), CatalogItem::Source(_))
    }

    /// Reports whether this catalog entry is a sink.
    pub fn is_sink(&self) -> bool {
        matches!(self.item(), CatalogItem::Sink(_))
    }

    /// Reports whether this catalog entry is a materialized view.
    pub fn is_materialized_view(&self) -> bool {
        matches!(self.item(), CatalogItem::MaterializedView(_))
    }

    /// Reports whether this catalog entry is a secret.
    pub fn is_secret(&self) -> bool {
        matches!(self.item(), CatalogItem::Secret(_))
    }

    /// Collects the identifiers of the dataflows that this dataflow depends
    /// upon.
    pub fn uses(&self) -> &[GlobalId] {
        self.item.uses()
    }

    /// Returns the `CatalogItem` associated with this catalog entry.
    pub fn item(&self) -> &CatalogItem {
        &self.item
    }

    /// Returns the global ID of this catalog entry.
    pub fn id(&self) -> GlobalId {
        self.id
    }

    /// Returns the OID of this catalog entry.
    pub fn oid(&self) -> u32 {
        self.oid
    }

    /// Returns the fully qualified name of this catalog entry.
    pub fn name(&self) -> &QualifiedObjectName {
        &self.name
    }

    /// Returns the identifiers of the dataflows that depend upon this dataflow.
    pub fn used_by(&self) -> &[GlobalId] {
        &self.used_by
    }

    /// Returns the connection ID that this item belongs to, if this item is
    /// temporary.
    pub fn conn_id(&self) -> Option<ConnectionId> {
        self.item.conn_id()
    }
}

struct AllocatedBuiltinSystemIds<T> {
    all_builtins: Vec<(T, GlobalId)>,
    new_builtins: Vec<(T, GlobalId)>,
    migrated_builtins: Vec<(GlobalId, String)>,
}

/// Functions can share the same name as any other catalog item type
/// within a given schema.
/// For example, a function can have the same name as a type, e.g.
/// 'date'.
/// As such, system objects are keyed in the catalog storage by the
/// tuple (schema_name, object_type, object_name), which is guaranteed
/// to be unique.
pub struct SystemObjectMapping {
    schema_name: String,
    object_type: CatalogItemType,
    object_name: String,
    id: GlobalId,
    fingerprint: String,
}

pub enum CatalogItemRebuilder {
    SystemTable(CatalogItem),
    Object(String),
}

impl CatalogItemRebuilder {
    fn new(entry: &CatalogEntry, id: GlobalId, ancestor_ids: &HashMap<GlobalId, GlobalId>) -> Self {
        if id.is_system() && entry.is_table() {
            Self::SystemTable(entry.item().clone())
        } else {
            let create_sql = entry.create_sql().to_string();
            assert_ne!(create_sql.to_lowercase(), CREATE_SQL_TODO.to_lowercase());
            let mut create_stmt = mz_sql::parse::parse(&create_sql).unwrap().into_element();
            mz_sql::ast::transform::create_stmt_replace_ids(&mut create_stmt, ancestor_ids);
            Self::Object(create_stmt.to_ast_string_stable())
        }
    }

    fn build<S: Append>(self, catalog: &Catalog<S>) -> CatalogItem {
        match self {
            Self::SystemTable(item) => item,
            Self::Object(create_sql) => catalog
                .parse_item(create_sql.clone(), None)
                .unwrap_or_else(|error| {
                    panic!("invalid persisted create sql ({error:?}): {create_sql}")
                }),
        }
    }
}

pub struct BuiltinMigrationMetadata {
    // Used to drop objects on STORAGE nodes
    pub previous_sink_ids: Vec<GlobalId>,
    pub previous_materialized_view_ids: Vec<GlobalId>,
    pub previous_source_ids: Vec<GlobalId>,
    // Used to update in memory catalog state
    pub all_drop_ops: Vec<GlobalId>,
    pub all_create_ops: Vec<(GlobalId, u32, QualifiedObjectName, CatalogItemRebuilder)>,
    pub introspection_source_index_updates: HashMap<ComputeInstanceId, Vec<(LogVariant, GlobalId)>>,
    // Used to update persisted on disk catalog state
    pub migrated_system_table_mappings: HashMap<GlobalId, SystemObjectMapping>,
    pub user_drop_ops: Vec<GlobalId>,
    pub user_create_ops: Vec<(GlobalId, SchemaId, String)>,
}

impl BuiltinMigrationMetadata {
    fn new() -> BuiltinMigrationMetadata {
        BuiltinMigrationMetadata {
            previous_sink_ids: Vec::new(),
            previous_materialized_view_ids: Vec::new(),
            previous_source_ids: Vec::new(),
            all_drop_ops: Vec::new(),
            all_create_ops: Vec::new(),
            introspection_source_index_updates: HashMap::new(),
            migrated_system_table_mappings: HashMap::new(),
            user_drop_ops: Vec::new(),
            user_create_ops: Vec::new(),
        }
    }
}

impl Catalog<Sqlite> {
    /// Opens a debug in-memory sqlite catalog.
    ///
    /// See [`Catalog::open_debug`].
    pub async fn open_debug_sqlite(now: NowFn) -> Result<Catalog<Sqlite>, anyhow::Error> {
        let stash = mz_stash::Sqlite::open(None)?;
        Catalog::open_debug(stash, now).await
    }
}

impl Catalog<Postgres> {
    /// Opens a debug postgres catalog at `url`.
    ///
    /// If specified, `schema` will set the connection's `search_path` to `schema`.
    ///
    /// See [`Catalog::open_debug`].
    pub async fn open_debug_postgres(
        url: String,
        schema: Option<String>,
        now: NowFn,
    ) -> Result<Catalog<Postgres>, anyhow::Error> {
        let tls = mz_postgres_util::make_tls(&tokio_postgres::Config::new()).unwrap();
        let stash = mz_stash::Postgres::new(url, schema, tls).await?;
        Catalog::open_debug(stash, now).await
    }
}

impl<S: Append> Catalog<S> {
    /// Opens or creates a catalog that stores data at `path`.
    ///
    /// Returns the catalog, metadata about builtin objects that have
    /// changed schemas since last restart, and a list of updates to builtin
    /// tables that describe the initial state of the catalog.
    pub async fn open(
        config: Config<'_, S>,
    ) -> Result<
        (
            Catalog<S>,
            BuiltinMigrationMetadata,
            Vec<BuiltinTableUpdate>,
        ),
        AdapterError,
    > {
        let mut catalog = Catalog {
            state: CatalogState {
                database_by_name: BTreeMap::new(),
                database_by_id: BTreeMap::new(),
                entry_by_id: BTreeMap::new(),
                ambient_schemas_by_name: BTreeMap::new(),
                ambient_schemas_by_id: BTreeMap::new(),
                temporary_schemas: HashMap::new(),
                compute_instances_by_id: HashMap::new(),
                compute_instances_by_name: HashMap::new(),
                roles: HashMap::new(),
                config: mz_sql::catalog::CatalogConfig {
                    start_time: to_datetime((config.now)()),
                    start_instant: Instant::now(),
                    nonce: rand::random(),
                    unsafe_mode: config.unsafe_mode,
                    environment_id: config.environment_id,
                    session_id: Uuid::new_v4(),
                    build_info: config.build_info,
                    timestamp_interval: Duration::from_secs(1),
                    now: config.now.clone(),
                },
                oid_counter: FIRST_USER_OID,
                cluster_replica_sizes: config.cluster_replica_sizes,
                storage_host_sizes: config.storage_host_sizes,
                default_storage_host_size: config.default_storage_host_size,
                availability_zones: config.availability_zones,
                system_configuration: SystemVars::default(),
            },
            transient_revision: 0,
            storage: Arc::new(Mutex::new(config.storage)),
        };

        catalog.create_temporary_schema(SYSTEM_CONN_ID)?;

        let databases = catalog.storage().await.load_databases().await?;
        for (id, name) in databases {
            let oid = catalog.allocate_oid()?;
            catalog.state.database_by_id.insert(
                id.clone(),
                Database {
                    name: name.clone(),
                    id,
                    oid,
                    schemas_by_id: BTreeMap::new(),
                    schemas_by_name: BTreeMap::new(),
                },
            );
            catalog
                .state
                .database_by_name
                .insert(name.clone(), id.clone());
        }

        let schemas = catalog.storage().await.load_schemas().await?;
        for (schema_id, schema_name, database_id) in schemas {
            let oid = catalog.allocate_oid()?;
            let (schemas_by_id, schemas_by_name, database_spec) = match &database_id {
                Some(database_id) => {
                    let db = catalog
                        .state
                        .database_by_id
                        .get_mut(database_id)
                        .expect("catalog out of sync");
                    (
                        &mut db.schemas_by_id,
                        &mut db.schemas_by_name,
                        ResolvedDatabaseSpecifier::Id(*database_id),
                    )
                }
                None => (
                    &mut catalog.state.ambient_schemas_by_id,
                    &mut catalog.state.ambient_schemas_by_name,
                    ResolvedDatabaseSpecifier::Ambient,
                ),
            };
            schemas_by_id.insert(
                schema_id.clone(),
                Schema {
                    name: QualifiedSchemaName {
                        database: database_spec,
                        schema: schema_name.clone(),
                    },
                    id: SchemaSpecifier::Id(schema_id.clone()),
                    oid,
                    items: BTreeMap::new(),
                    functions: BTreeMap::new(),
                },
            );
            schemas_by_name.insert(schema_name.clone(), schema_id);
        }

        let roles = catalog.storage().await.load_roles().await?;
        for (id, name) in roles {
            let oid = catalog.allocate_oid()?;
            catalog.state.roles.insert(
                name.clone(),
                Role {
                    name: name.clone(),
                    id,
                    oid,
                },
            );
        }

        catalog.load_builtin_types().await?;

        let persisted_builtin_ids = catalog.storage().await.load_system_gids().await?;
        let AllocatedBuiltinSystemIds {
            all_builtins,
            new_builtins,
            migrated_builtins,
        } = catalog
            .allocate_system_ids(
                BUILTINS::iter()
                    .filter(|builtin| !matches!(builtin, Builtin::Type(_)))
                    .collect(),
                |builtin| {
                    persisted_builtin_ids
                        .get(&(
                            builtin.schema().to_string(),
                            builtin.catalog_item_type(),
                            builtin.name().to_string(),
                        ))
                        .cloned()
                },
            )
            .await?;

        for (builtin, id) in all_builtins {
            let schema_id = catalog.state.ambient_schemas_by_name[builtin.schema()];
            let name = QualifiedObjectName {
                qualifiers: ObjectQualifiers {
                    database_spec: ResolvedDatabaseSpecifier::Ambient,
                    schema_spec: SchemaSpecifier::Id(schema_id),
                },
                item: builtin.name().into(),
            };
            match builtin {
                Builtin::Log(log) => {
                    let oid = catalog.allocate_oid()?;
                    catalog.state.insert_item(
                        id,
                        oid,
                        name.clone(),
                        CatalogItem::Log(Log {
                            variant: log.variant.clone(),
                            has_storage_collection: false,
                        }),
                    );
                }

                Builtin::Table(table) => {
                    let oid = catalog.allocate_oid()?;
                    catalog.state.insert_item(
                        id,
                        oid,
                        name.clone(),
                        CatalogItem::Table(Table {
                            create_sql: CREATE_SQL_TODO.to_string(),
                            desc: table.desc.clone(),
                            defaults: vec![Expr::null(); table.desc.arity()],
                            conn_id: None,
                            depends_on: vec![],
                        }),
                    );
                }

                Builtin::View(view) => {
                    let item = catalog
                        .parse_item(
                            view.sql.into(),
                            None,
                        )
                        .unwrap_or_else(|e| {
                            panic!(
                                "internal error: failed to load bootstrap view:\n\
                                    {}\n\
                                    error:\n\
                                    {:?}\n\n\
                                    make sure that the schema name is specified in the builtin view's create sql statement.",
                                view.name, e
                            )
                        });
                    let oid = catalog.allocate_oid()?;
                    catalog.state.insert_item(id, oid, name, item);
                }

                Builtin::Type(_) => unreachable!("loaded separately"),

                Builtin::Func(func) => {
                    let oid = catalog.allocate_oid()?;
                    catalog.state.insert_item(
                        id,
                        oid,
                        name.clone(),
                        CatalogItem::Func(Func { inner: func.inner }),
                    );
                }

                Builtin::Source(coll) => {
                    let introspection_type = match &coll.data_source {
                        Some(i) => i.clone(),
                        None => continue,
                    };

                    let oid = catalog.allocate_oid()?;
                    catalog.state.insert_item(
                        id,
                        oid,
                        name.clone(),
                        CatalogItem::Source(Source {
                            create_sql: CREATE_SQL_TODO.to_string(),
                            data_source: DataSourceDesc::Introspection(introspection_type),
                            desc: coll.desc.clone(),
                            timeline: Timeline::EpochMilliseconds,
                            depends_on: vec![],
                        }),
                    );
                }
            }
        }
        let new_system_id_mappings = new_builtins
            .iter()
            .map(|(builtin, id)| SystemObjectMapping {
                schema_name: builtin.schema().to_string(),
                object_type: builtin.catalog_item_type(),
                object_name: builtin.name().to_string(),
                id: *id,
                fingerprint: builtin.fingerprint(),
            })
            .collect();
        catalog
            .storage()
            .await
            .set_system_object_mapping(new_system_id_mappings)
            .await?;

        let compute_instances = catalog.storage().await.load_compute_instances().await?;
        for (id, name) in compute_instances {
            let introspection_source_index_gids = catalog
                .storage()
                .await
                .load_introspection_source_index_gids(id)
                .await?;

            let AllocatedBuiltinSystemIds {
                all_builtins: all_indexes,
                new_builtins: new_indexes,
                ..
            } = catalog
                .allocate_system_ids(BUILTINS::logs().collect(), |log| {
                    introspection_source_index_gids
                        .get(log.name)
                        .cloned()
                        // We don't migrate indexes so we can hardcode the fingerprint as 0
                        .map(|id| (id, "".to_string()))
                })
                .await?;

            catalog
                .storage()
                .await
                .set_introspection_source_index_gids(
                    new_indexes
                        .iter()
                        .map(|(log, index_id)| (id, log.name, *index_id))
                        .collect(),
                )
                .await?;

            catalog.state.insert_compute_instance(id, name, all_indexes);
        }

        let replicas = catalog.storage().await.load_compute_replicas().await?;
        for (instance_id, replica_id, name, serialized_config) in replicas {
            let log_sources = match serialized_config.logging.sources {
                Some(sources) => sources,
                None => catalog.allocate_persisted_introspection_sources().await,
            };
            let log_views = match serialized_config.logging.views {
                Some(views) => views,
                None => catalog.allocate_persisted_introspection_views().await,
            };

            let logging = ComputeReplicaLogging {
                log_logging: serialized_config.logging.log_logging,
                interval: serialized_config.logging.interval,
                sources: log_sources,
                views: log_views,
            };
            let config = ComputeReplicaConfig {
                location: catalog.concretize_replica_location(serialized_config.location)?,
                logging,
            };

            // And write the allocated sources back to storage
            catalog
                .storage()
                .await
                .set_replica_config(replica_id, instance_id, name.clone(), &config)
                .await?;

            catalog
                .state
                .insert_compute_replica(instance_id, name, replica_id, config);
        }

        let system_config = catalog.storage().await.load_system_configuration().await?;
        for (name, value) in system_config {
            catalog.state.insert_system_configuration(&name, &value)?;
        }

        if !config.skip_migrations {
            let last_seen_version = catalog
                .storage()
                .await
                .get_catalog_content_version()
                .await?
                // `new` means that it hasn't been initialized
                .unwrap_or_else(|| "new".to_string());

            migrate::migrate(&mut catalog).await.map_err(|e| {
                Error::new(ErrorKind::FailedMigration {
                    last_seen_version,
                    this_version: catalog.config().build_info.version,
                    cause: e.to_string(),
                })
            })?;
            catalog
                .storage()
                .await
                .set_catalog_content_version(catalog.config().build_info.version)
                .await?;
        }

        let mut catalog = {
            let mut storage = catalog.storage().await;
            let mut tx = storage.transaction().await?;
            let catalog = Self::load_catalog_items(&mut tx, &catalog)?;
            tx.commit().await?;
            catalog
        };

        let mut builtin_migration_metadata = catalog
            .generate_builtin_migration_metadata(migrated_builtins)
            .await?;
        catalog.apply_in_memory_builtin_migration(&mut builtin_migration_metadata)?;
        catalog
            .apply_persisted_builtin_migration(&mut builtin_migration_metadata)
            .await?;

        // Load public keys for SSH connections from the secrets store to the catalog
        for (id, entry) in catalog.state.entry_by_id.iter_mut() {
            if let CatalogItem::Connection(ref mut connection) = entry.item {
                if let mz_storage::types::connections::Connection::Ssh(ref mut ssh) =
                    connection.connection
                {
                    let secret = config.secrets_reader.read(*id).await?;
                    let keyset = mz_ore::ssh_key::SshKeyset::from_bytes(&secret)?;
                    let public_keypair = keyset.public_keys();
                    ssh.public_keys = Some(public_keypair);
                }
            }
        }

        let mut builtin_table_updates = vec![];
        for (schema_id, schema) in &catalog.state.ambient_schemas_by_id {
            let db_spec = ResolvedDatabaseSpecifier::Ambient;
            builtin_table_updates.push(catalog.state.pack_schema_update(&db_spec, schema_id, 1));
            for (_item_name, item_id) in &schema.items {
                builtin_table_updates.extend(catalog.state.pack_item_update(*item_id, 1));
            }
            for (_item_name, function_id) in &schema.functions {
                builtin_table_updates.extend(catalog.state.pack_item_update(*function_id, 1));
            }
        }
        for (_id, db) in &catalog.state.database_by_id {
            builtin_table_updates.push(catalog.state.pack_database_update(db, 1));
            let db_spec = ResolvedDatabaseSpecifier::Id(db.id.clone());
            for (schema_id, schema) in &db.schemas_by_id {
                builtin_table_updates
                    .push(catalog.state.pack_schema_update(&db_spec, schema_id, 1));
                for (_item_name, item_id) in &schema.items {
                    builtin_table_updates.extend(catalog.state.pack_item_update(*item_id, 1));
                }
                for (_item_name, function_id) in &schema.functions {
                    builtin_table_updates.extend(catalog.state.pack_item_update(*function_id, 1));
                }
            }
        }
        for (_name, role) in &catalog.state.roles {
            builtin_table_updates.push(catalog.state.pack_role_update(role, 1));
        }
        for (name, id) in &catalog.state.compute_instances_by_name {
            builtin_table_updates.push(catalog.state.pack_compute_instance_update(name, 1));
            let instance = &catalog.state.compute_instances_by_id[id];
            for (replica_name, _replica_id) in &instance.replica_id_by_name {
                builtin_table_updates.push(catalog.state.pack_compute_replica_update(
                    *id,
                    replica_name,
                    1,
                ));
            }
        }
        let audit_logs = catalog.storage().await.load_audit_log().await?;
        for event in audit_logs {
            builtin_table_updates.push(catalog.state.pack_audit_log_update(&event)?);
        }

        let storage_usage_events = catalog.storage().await.storage_usage().await?;
        for event in storage_usage_events {
            builtin_table_updates.push(catalog.state.pack_storage_usage_update(&event)?);
        }

        Ok((catalog, builtin_migration_metadata, builtin_table_updates))
    }

    /// Loads built-in system types into the catalog.
    ///
    /// Built-in types sometimes have references to other built-in types, and sometimes these
    /// references are circular. This makes loading built-in types more complicated than other
    /// built-in objects, and requires us to make multiple passes over the types to correctly
    /// resolve all references.
    async fn load_builtin_types(&mut self) -> Result<(), Error> {
        let persisted_builtin_ids = self.storage().await.load_system_gids().await?;

        let AllocatedBuiltinSystemIds {
            all_builtins,
            new_builtins,
            migrated_builtins,
        } = self
            .allocate_system_ids(BUILTINS::types().collect(), |typ| {
                persisted_builtin_ids
                    .get(&(
                        typ.schema.to_string(),
                        CatalogItemType::Type,
                        typ.name.to_string(),
                    ))
                    .cloned()
            })
            .await?;
        assert!(migrated_builtins.is_empty(), "types cannot be migrated");
        let name_to_id_map: HashMap<&str, GlobalId> = all_builtins
            .into_iter()
            .map(|(typ, id)| (typ.name, id))
            .collect();

        // Replace named references with id references
        let mut builtin_types: Vec<_> = BUILTINS::types()
            .map(|typ| Self::resolve_builtin_type(typ, &name_to_id_map))
            .collect();

        // Resolve array_id for types
        let mut element_id_to_array_id = HashMap::new();
        for typ in &builtin_types {
            match &typ.details.typ {
                CatalogType::Array { element_reference } => {
                    let array_id = name_to_id_map[typ.name];
                    element_id_to_array_id.insert(*element_reference, array_id);
                }
                _ => {}
            }
        }
        let pg_catalog_schema_id = self.state.get_pg_catalog_schema_id().clone();
        for typ in &mut builtin_types {
            let element_id = name_to_id_map[typ.name];
            typ.details.array_id = element_id_to_array_id.get(&element_id).map(|id| id.clone());
        }

        // Insert into catalog
        for typ in builtin_types {
            let element_id = name_to_id_map[typ.name];
            self.state.insert_item(
                element_id,
                typ.oid,
                QualifiedObjectName {
                    qualifiers: ObjectQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(pg_catalog_schema_id),
                    },
                    item: typ.name.to_owned(),
                },
                CatalogItem::Type(Type {
                    create_sql: format!("CREATE TYPE {}", typ.name),
                    details: typ.details.clone(),
                    depends_on: vec![],
                }),
            );
        }

        let new_system_id_mappings = new_builtins
            .iter()
            .map(|(typ, id)| SystemObjectMapping {
                schema_name: typ.schema.to_string(),
                object_type: CatalogItemType::Type,
                object_name: typ.name.to_string(),
                id: *id,
                fingerprint: typ.fingerprint(),
            })
            .collect();
        self.storage()
            .await
            .set_system_object_mapping(new_system_id_mappings)
            .await?;

        Ok(())
    }

    fn resolve_builtin_type(
        builtin: &BuiltinType<NameReference>,
        name_to_id_map: &HashMap<&str, GlobalId>,
    ) -> BuiltinType<IdReference> {
        let typ: CatalogType<IdReference> = match &builtin.details.typ {
            CatalogType::Array { element_reference } => CatalogType::Array {
                element_reference: name_to_id_map[element_reference],
            },
            CatalogType::List { element_reference } => CatalogType::List {
                element_reference: name_to_id_map[element_reference],
            },
            CatalogType::Map {
                key_reference,
                value_reference,
            } => CatalogType::Map {
                key_reference: name_to_id_map[key_reference],
                value_reference: name_to_id_map[value_reference],
            },
            CatalogType::Record { fields } => CatalogType::Record {
                fields: fields
                    .into_iter()
                    .map(|(column_name, reference)| {
                        (column_name.clone(), name_to_id_map[reference])
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
        };

        BuiltinType {
            name: builtin.name,
            schema: builtin.schema,
            oid: builtin.oid,
            details: CatalogTypeDetails {
                array_id: builtin.details.array_id,
                typ,
            },
        }
    }

    /// The objects in the catalog form one or more DAGs (directed acyclic graph) via object
    /// dependencies. To migrate a builtin object we must drop that object along with all of its
    /// descendants, and then recreate that object along with all of its descendants using new
    /// GlobalId`s. To achieve this we perform a BFS (breadth first search) on the catalog items
    /// starting with the nodes that correspond to builtin objects that have changed schemas.
    ///
    /// Objects need to be dropped starting from the leafs of the DAG going up towards the roots,
    /// and they need to be recreated starting at the root of the DAG and going towards the leafs.
    pub async fn generate_builtin_migration_metadata(
        &mut self,
        migrated_ids: Vec<(GlobalId, String)>,
    ) -> Result<BuiltinMigrationMetadata, Error> {
        let mut migration_metadata = BuiltinMigrationMetadata::new();

        let mut object_queue: VecDeque<_> = migrated_ids.iter().map(|(id, _)| (*id)).collect();
        let mut visited_set: HashSet<_> = migrated_ids.iter().map(|(id, _)| (*id)).collect();
        let mut topological_sort = Vec::new();
        let mut ancestor_ids = HashMap::new();

        let id_fingerprint_map: HashMap<GlobalId, String> = migrated_ids.into_iter().collect();

        while let Some(id) = object_queue.pop_front() {
            let entry = self.get_entry(&id);

            let new_id = match id {
                GlobalId::System(_) => self
                    .storage()
                    .await
                    .allocate_system_ids(1)
                    .await?
                    .into_element(),
                GlobalId::User(_) => self.storage().await.allocate_user_id().await?,
                _ => unreachable!("can't migrate id: {id}"),
            };

            // Generate value to update fingerprint and global ID persisted mapping.
            if let Some(fingerprint) = id_fingerprint_map.get(&id) {
                let schema_name = self
                    .get_schema(
                        &entry.name.qualifiers.database_spec,
                        &entry.name.qualifiers.schema_spec,
                        entry.conn_id().unwrap_or(SYSTEM_CONN_ID),
                    )
                    .name
                    .schema
                    .as_str();
                migration_metadata.migrated_system_table_mappings.insert(
                    id,
                    SystemObjectMapping {
                        schema_name: schema_name.to_string(),
                        object_type: entry.item_type(),
                        object_name: entry.name.item.clone(),
                        id: new_id,
                        fingerprint: fingerprint.clone(),
                    },
                );
            }

            // Defer adding the create/drop ops until we know more about the dependency graph.
            topological_sort.push((entry, new_id));

            ancestor_ids.insert(id, new_id);

            // Add children to queue.
            for dependant in &entry.used_by {
                if !visited_set.contains(dependant) {
                    object_queue.push_back(*dependant);
                    visited_set.insert(*dependant);
                } else {
                    // If dependant is a child of the current node, then we need to make sure that
                    // it appears later in the topologically sorted list.
                    if let Some(idx) = topological_sort.iter().position(|(_, id)| id == dependant) {
                        let dependant = topological_sort.remove(idx);
                        topological_sort.push(dependant);
                    }
                }
            }
        }

        for (entry, new_id) in topological_sort {
            let id = entry.id();
            // Push drop commands.
            match entry.item() {
                CatalogItem::Table(_) | CatalogItem::Source(_) => {
                    migration_metadata.previous_source_ids.push(id)
                }
                CatalogItem::Sink(_) => migration_metadata.previous_sink_ids.push(id),
                CatalogItem::MaterializedView(_) => {
                    migration_metadata.previous_materialized_view_ids.push(id)
                }
                // TODO(jkosh44) Implement log migration
                CatalogItem::Log(_) => {
                    panic!("Log migration is unimplemented")
                }
                CatalogItem::View(_) | CatalogItem::Index(_) => {
                    // Views and indexes don't have any objects in STORAGE to drop.
                }
                CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_)
                | CatalogItem::Connection(_) => unreachable!(
                    "impossible to migrate schema for builtin {}",
                    entry.item().typ()
                ),
            }
            if id.is_user() {
                migration_metadata.user_drop_ops.push(id);
            }
            migration_metadata.all_drop_ops.push(id);

            // Push create commands.
            let name = entry.name.clone();
            if id.is_user() {
                let schema_id = name.qualifiers.schema_spec.clone().into();
                migration_metadata
                    .user_create_ops
                    .push((new_id, schema_id, name.item.clone()));
            }
            let item_rebuilder = CatalogItemRebuilder::new(entry, new_id, &ancestor_ids);
            migration_metadata
                .all_create_ops
                .push((new_id, entry.oid, name, item_rebuilder));
        }

        // Reverse drop commands.
        migration_metadata.previous_sink_ids.reverse();
        migration_metadata.previous_source_ids.reverse();
        migration_metadata.all_drop_ops.reverse();
        migration_metadata.user_drop_ops.reverse();

        Ok(migration_metadata)
    }

    pub fn apply_in_memory_builtin_migration(
        &mut self,
        migration_metadata: &mut BuiltinMigrationMetadata,
    ) -> Result<(), Error> {
        assert_eq!(
            migration_metadata.all_drop_ops.len(),
            migration_metadata.all_create_ops.len(),
            "we should be re-creating every dropped object"
        );
        for id in migration_metadata.all_drop_ops.drain(..) {
            self.state.drop_item(id);
        }
        for (id, oid, name, item_rebuilder) in migration_metadata.all_create_ops.drain(..) {
            let item = item_rebuilder.build(self);
            self.state.insert_item(id, oid, name, item);
        }
        for (compute_instance, updates) in migration_metadata
            .introspection_source_index_updates
            .drain()
        {
            let log_indexes = &mut self
                .state
                .compute_instances_by_id
                .get_mut(&compute_instance)
                .expect("invalid compute instance {compute_instance}")
                .log_indexes;
            for (variant, new_id) in updates {
                log_indexes.remove(&variant);
                log_indexes.insert(variant, new_id);
            }
        }

        Ok(())
    }

    pub async fn apply_persisted_builtin_migration(
        &mut self,
        migration_metadata: &mut BuiltinMigrationMetadata,
    ) -> Result<(), Error> {
        let mut storage = self.storage().await;
        let mut tx = storage.transaction().await?;
        for id in migration_metadata.user_drop_ops.drain(..) {
            tx.remove_item(id)?;
        }
        for (id, schema_id, name) in migration_metadata.user_create_ops.drain(..) {
            let item = self.get_entry(&id).item();
            let serialized_item = Self::serialize_item(item);
            tx.insert_item(id, schema_id, &name, serialized_item)?;
        }
        tx.update_system_object_mappings(
            migration_metadata
                .migrated_system_table_mappings
                .drain()
                .collect(),
        )?;
        tx.commit().await?;

        Ok(())
    }

    /// Returns the catalog's transient revision, which starts at 1 and is
    /// incremented on every change. This is not persisted to disk, and will
    /// restart on every load.
    pub fn transient_revision(&self) -> u64 {
        self.transient_revision
    }

    /// Takes a catalog which only has items in its on-disk storage ("unloaded")
    /// and cannot yet resolve names, and returns a catalog loaded with those
    /// items.
    ///
    /// This function requires transactions to support loading a catalog with
    /// the transaction's currently in-flight updates to existing catalog
    /// objects, which is necessary for at least one catalog migration.
    ///
    /// TODO(justin): it might be nice if these were two different types.
    pub fn load_catalog_items<'a>(
        tx: &mut storage::Transaction<'a, S>,
        c: &Catalog<S>,
    ) -> Result<Catalog<S>, Error> {
        let mut c = c.clone();
        let items = tx.loaded_items();
        for (id, name, def) in items {
            // TODO(benesch): a better way of detecting when a view has depended
            // upon a non-existent logging view. This is fine for now because
            // the only goal is to produce a nicer error message; we'll bail out
            // safely even if the error message we're sniffing out changes.
            static LOGGING_ERROR: Lazy<Regex> =
                Lazy::new(|| Regex::new("unknown catalog item 'mz_catalog.[^']*'").unwrap());
            let item = match c.deserialize_item(def) {
                Ok(item) => item,
                Err(e) if LOGGING_ERROR.is_match(&e.to_string()) => {
                    return Err(Error::new(ErrorKind::UnsatisfiableLoggingDependency {
                        depender_name: name.to_string(),
                    }));
                }
                Err(e) => {
                    return Err(Error::new(ErrorKind::Corruption {
                        detail: format!("failed to deserialize item {} ({}): {}", id, name, e),
                    }))
                }
            };
            let oid = c.allocate_oid()?;
            c.state.insert_item(id, oid, name, item);
        }
        c.transient_revision = 1;
        Ok(c)
    }

    /// Opens the catalog from `stash` with parameters set appropriately for debug
    /// contexts, like in tests.
    ///
    /// WARNING! This function can arbitrarily fail because it does not make any
    /// effort to adjust the catalog's contents' structure or semantics to the
    /// currently running version, i.e. it does not apply any migrations.
    ///
    /// This function should not be called in production contexts. Use
    /// [`Catalog::open`] with appropriately set configuration parameters
    /// instead.
    pub async fn open_debug(stash: S, now: NowFn) -> Result<Catalog<S>, anyhow::Error> {
        let metrics_registry = &MetricsRegistry::new();
        let storage = storage::Connection::open(
            stash,
            &BootstrapArgs {
                now: (now)(),
                default_cluster_replica_size: "1".into(),
                builtin_cluster_replica_size: "1".into(),
                default_availability_zone: DUMMY_AVAILABILITY_ZONE.into(),
            },
        )
        .await?;
        let secrets_reader = Arc::new(InMemorySecretsController::new());
        let (catalog, _, _) = Catalog::open(Config {
            storage,
            unsafe_mode: true,
            build_info: &DUMMY_BUILD_INFO,
            environment_id: format!("environment-{}-0", Uuid::from_u128(0)),
            now,
            skip_migrations: true,
            metrics_registry,
            cluster_replica_sizes: Default::default(),
            storage_host_sizes: Default::default(),
            default_storage_host_size: None,
            availability_zones: vec![],
            secrets_reader,
        })
        .await?;
        Ok(catalog)
    }

    pub fn for_session<'a>(&'a self, session: &'a Session) -> ConnCatalog<'a> {
        let database = self
            .state
            .database_by_name
            .get(session.vars().database())
            .map(|id| id.clone());
        let search_path = session
            .vars()
            .search_path()
            .iter()
            .map(|schema| self.resolve_schema(database.as_ref(), None, schema, session.conn_id()))
            .filter_map(|schema| schema.ok())
            .map(|schema| (schema.name().database.clone(), schema.id().clone()))
            .collect();
        ConnCatalog {
            state: Cow::Borrowed(&self.state),
            conn_id: session.conn_id(),
            compute_instance: session.vars().cluster().into(),
            database,
            search_path,
            user: session.user().clone(),
            prepared_statements: Some(Cow::Borrowed(session.prepared_statements())),
        }
    }

    pub fn for_sessionless_user(&self, user: User) -> ConnCatalog {
        ConnCatalog {
            state: Cow::Borrowed(&self.state),
            conn_id: SYSTEM_CONN_ID,
            compute_instance: "default".into(),
            database: self
                .resolve_database(DEFAULT_DATABASE_NAME)
                .ok()
                .map(|db| db.id()),
            search_path: Vec::new(),
            user,
            prepared_statements: None,
        }
    }

    // Leaving the system's search path empty allows us to catch issues
    // where catalog object names have not been normalized correctly.
    pub fn for_system_session(&self) -> ConnCatalog {
        self.for_sessionless_user(SYSTEM_USER.clone())
    }

    async fn storage<'a>(&'a self) -> MutexGuard<'a, storage::Connection<S>> {
        self.storage.lock().await
    }

    /// Allocate new system ids for any new builtin objects and looks up existing system ids for
    /// existing builtin objects
    async fn allocate_system_ids<T, F>(
        &mut self,
        builtins: Vec<T>,
        builtin_lookup: F,
    ) -> Result<AllocatedBuiltinSystemIds<T>, Error>
    where
        T: Copy + Fingerprint,
        F: Fn(&T) -> Option<(GlobalId, String)>,
    {
        let new_builtin_amount = builtins
            .iter()
            .filter(|builtin| builtin_lookup(builtin).is_none())
            .count();

        let mut global_ids = self
            .storage()
            .await
            .allocate_system_ids(
                new_builtin_amount
                    .try_into()
                    .expect("builtins should fit into u64"),
            )
            .await?
            .into_iter();

        let mut all_builtins = Vec::new();
        let mut new_builtins = Vec::new();
        let mut migrated_builtins = Vec::new();
        for builtin in &builtins {
            match builtin_lookup(builtin) {
                Some((id, fingerprint)) => {
                    all_builtins.push((*builtin, id));
                    if fingerprint != builtin.fingerprint() {
                        migrated_builtins.push((id, builtin.fingerprint()));
                    }
                }
                None => {
                    let id = global_ids.next().expect("not enough global IDs");
                    all_builtins.push((*builtin, id));
                    new_builtins.push((*builtin, id));
                }
            }
        }

        Ok(AllocatedBuiltinSystemIds {
            all_builtins,
            new_builtins,
            migrated_builtins,
        })
    }

    pub async fn allocate_user_id(&mut self) -> Result<GlobalId, Error> {
        self.storage().await.allocate_user_id().await
    }

    #[cfg(test)]
    pub async fn allocate_system_id(&mut self) -> Result<GlobalId, Error> {
        self.storage()
            .await
            .allocate_system_ids(1)
            .await
            .map(|ids| ids.into_element())
    }

    pub fn allocate_oid(&mut self) -> Result<u32, Error> {
        self.state.allocate_oid()
    }

    /// Get all global timestamps that has been persisted to disk.
    pub async fn get_all_persisted_timestamps(
        &mut self,
    ) -> Result<BTreeMap<Timeline, mz_repr::Timestamp>, Error> {
        self.storage().await.get_all_persisted_timestamps().await
    }

    /// Get a global timestamp for a timeline that has been persisted to disk.
    pub async fn get_persisted_timestamp(
        &mut self,
        timeline: &Timeline,
    ) -> Result<mz_repr::Timestamp, Error> {
        self.storage().await.get_persisted_timestamp(timeline).await
    }

    /// Persist new global timestamp for a timeline to disk.
    pub async fn persist_timestamp(
        &mut self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), Error> {
        self.storage()
            .await
            .persist_timestamp(timeline, timestamp)
            .await
    }

    pub fn resolve_database(&self, database_name: &str) -> Result<&Database, SqlCatalogError> {
        self.state.resolve_database(database_name)
    }

    pub fn resolve_schema(
        &self,
        current_database: Option<&DatabaseId>,
        database_name: Option<&str>,
        schema_name: &str,
        conn_id: ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        self.state
            .resolve_schema(current_database, database_name, schema_name, conn_id)
    }

    pub fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        self.state
            .resolve_schema_in_database(database_spec, schema_name, conn_id)
    }

    /// Resolves `name` to a non-function [`CatalogEntry`].
    pub fn resolve_entry(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialObjectName,
        conn_id: ConnectionId,
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
        name: &PartialObjectName,
        conn_id: ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.state
            .resolve_function(current_database, search_path, name, conn_id)
    }

    pub fn resolve_compute_instance(
        &self,
        name: &str,
    ) -> Result<&ComputeInstance, SqlCatalogError> {
        self.state.resolve_compute_instance(name)
    }

    pub fn active_compute_instance(
        &self,
        session: &Session,
    ) -> Result<&ComputeInstance, AdapterError> {
        // TODO(benesch): this check here is not sufficiently protective. It'd
        // be very easy for a code path to accidentally avoid this check by
        // calling `resolve_compute_instance(session.vars().cluster()`.
        if session.user().name != SYSTEM_USER.name && session.vars().cluster() == SYSTEM_USER.name {
            coord_bail!(
                "system cluster '{}' cannot execute user queries",
                SYSTEM_USER.name
            );
        }
        Ok(self.resolve_compute_instance(session.vars().cluster())?)
    }

    pub fn state(&self) -> &CatalogState {
        &self.state
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedObjectName,
        conn_id: Option<ConnectionId>,
    ) -> FullObjectName {
        self.state.resolve_full_name(name, conn_id)
    }

    /// Returns the named catalog item, if it exists.
    pub fn try_get_entry_in_schema(
        &self,
        name: &QualifiedObjectName,
        conn_id: ConnectionId,
    ) -> Option<&CatalogEntry> {
        self.state.try_get_entry_in_schema(name, conn_id)
    }

    pub fn item_exists(&self, name: &QualifiedObjectName, conn_id: ConnectionId) -> bool {
        self.state.item_exists(name, conn_id)
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
        conn_id: ConnectionId,
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

    pub fn get_database(&self, id: &DatabaseId) -> &Database {
        self.state.get_database(id)
    }

    /// Creates a new schema in the `Catalog` for temporary items
    /// indicated by the TEMPORARY or TEMP keywords.
    pub fn create_temporary_schema(&mut self, conn_id: ConnectionId) -> Result<(), Error> {
        let oid = self.allocate_oid()?;
        self.state.temporary_schemas.insert(
            conn_id,
            Schema {
                name: QualifiedSchemaName {
                    database: ResolvedDatabaseSpecifier::Ambient,
                    schema: MZ_TEMP_SCHEMA.into(),
                },
                id: SchemaSpecifier::Temporary,
                oid,
                items: BTreeMap::new(),
                functions: BTreeMap::new(),
            },
        );
        Ok(())
    }

    fn item_exists_in_temp_schemas(&self, conn_id: ConnectionId, item_name: &str) -> bool {
        self.state.temporary_schemas[&conn_id]
            .items
            .contains_key(item_name)
    }

    pub fn drop_temp_item_ops(&mut self, conn_id: ConnectionId) -> Vec<Op> {
        let ids: Vec<GlobalId> = self.state.temporary_schemas[&conn_id]
            .items
            .values()
            .cloned()
            .collect();
        self.drop_items_ops(&ids)
    }

    pub fn drop_temporary_schema(&mut self, conn_id: ConnectionId) -> Result<(), Error> {
        if !self.state.temporary_schemas[&conn_id].items.is_empty() {
            return Err(Error::new(ErrorKind::SchemaNotEmpty(MZ_TEMP_SCHEMA.into())));
        }
        self.state.temporary_schemas.remove(&conn_id);
        Ok(())
    }

    pub fn drop_database_ops(&mut self, id: Option<DatabaseId>) -> Vec<Op> {
        let mut ops = vec![];
        let mut seen = HashSet::new();
        if let Some(id) = id {
            let database = self.get_database(&id);
            for (schema_id, schema) in &database.schemas_by_id {
                Self::drop_schema_items(schema, &self.state.entry_by_id, &mut ops, &mut seen);
                ops.push(Op::DropSchema {
                    database_id: id.clone(),
                    schema_id: schema_id.clone(),
                });
            }
            ops.push(Op::DropDatabase { id });
        }
        ops
    }

    pub fn drop_schema_ops(&mut self, id: Option<(DatabaseId, SchemaId)>) -> Vec<Op> {
        let mut ops = vec![];
        let mut seen = HashSet::new();
        if let Some((database_id, schema_id)) = id {
            let database = self.get_database(&database_id);
            let schema = &database.schemas_by_id[&schema_id];
            Self::drop_schema_items(schema, &self.state.entry_by_id, &mut ops, &mut seen);
            ops.push(Op::DropSchema {
                database_id,
                schema_id,
            })
        }
        ops
    }

    pub fn drop_items_ops(&mut self, ids: &[GlobalId]) -> Vec<Op> {
        let mut ops = vec![];
        let mut seen = HashSet::new();
        for &id in ids {
            Self::drop_item_cascade(id, &self.state.entry_by_id, &mut ops, &mut seen);
        }
        ops
    }

    fn drop_schema_items(
        schema: &Schema,
        by_id: &BTreeMap<GlobalId, CatalogEntry>,
        ops: &mut Vec<Op>,
        seen: &mut HashSet<GlobalId>,
    ) {
        for &id in schema.items.values() {
            Self::drop_item_cascade(id, by_id, ops, seen)
        }
    }

    fn drop_item_cascade(
        id: GlobalId,
        by_id: &BTreeMap<GlobalId, CatalogEntry>,
        ops: &mut Vec<Op>,
        seen: &mut HashSet<GlobalId>,
    ) {
        if !seen.contains(&id) {
            seen.insert(id);
            for &u in &by_id[&id].used_by {
                Self::drop_item_cascade(u, by_id, ops, seen)
            }
            for subsource in by_id[&id].subsources() {
                Self::drop_item_cascade(subsource, by_id, ops, seen)
            }
            ops.push(Op::DropItem(id));
        }
    }

    /// Gets GlobalIds of temporary items to be created, checks for name collisions
    /// within a connection id.
    fn temporary_ids(
        &mut self,
        ops: &[Op],
        temporary_drops: HashSet<(ConnectionId, String)>,
    ) -> Result<Vec<GlobalId>, Error> {
        let mut creating = HashSet::with_capacity(ops.len());
        let mut temporary_ids = Vec::with_capacity(ops.len());
        for op in ops.iter() {
            if let Op::CreateItem {
                id,
                oid: _,
                name,
                item,
            } = op
            {
                if let Some(conn_id) = item.conn_id() {
                    if self.item_exists_in_temp_schemas(conn_id, &name.item)
                        && !temporary_drops.contains(&(conn_id, name.item.clone()))
                        || creating.contains(&(conn_id, &name.item))
                    {
                        return Err(Error::new(ErrorKind::ItemAlreadyExists(
                            *id,
                            name.item.clone(),
                        )));
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

    fn full_name_detail(name: &FullObjectName) -> FullNameV1 {
        FullNameV1 {
            database: name.database.to_string(),
            schema: name.schema.clone(),
            item: name.item.clone(),
        }
    }

    pub fn concretize_replica_location(
        &self,
        location: SerializedComputeReplicaLocation,
    ) -> Result<ComputeReplicaLocation, AdapterError> {
        let cluster_replica_sizes = &self.state.cluster_replica_sizes;
        let location = match location {
            SerializedComputeReplicaLocation::Remote {
                addrs,
                compute_addrs,
                workers,
            } => ComputeReplicaLocation::Remote {
                addrs,
                compute_addrs,
                workers,
            },
            SerializedComputeReplicaLocation::Managed {
                size,
                availability_zone,
                az_user_specified,
            } => {
                let allocation = cluster_replica_sizes.0.get(&size).ok_or_else(|| {
                    let mut entries = cluster_replica_sizes.0.iter().collect::<Vec<_>>();
                    entries.sort_by_key(
                        |(
                            _name,
                            ComputeReplicaAllocation {
                                scale, cpu_limit, ..
                            },
                        )| (scale, cpu_limit),
                    );
                    let expected = entries.into_iter().map(|(name, _)| name.clone()).collect();
                    AdapterError::InvalidClusterReplicaSize {
                        size: size.clone(),
                        expected,
                    }
                })?;
                ComputeReplicaLocation::Managed {
                    allocation: allocation.clone(),
                    availability_zone,
                    size,
                    az_user_specified,
                }
            }
        };
        Ok(location)
    }

    pub fn resolve_storage_host_config(
        &self,
        storage_host_config: PlanStorageHostConfig,
        allow_undefined: bool,
    ) -> Result<StorageHostConfig, AdapterError> {
        self.state
            .resolve_storage_host_config(storage_host_config, allow_undefined)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn transact<F, R>(
        &mut self,
        session: Option<&Session>,
        ops: Vec<Op>,
        f: F,
    ) -> Result<TransactionResult<R>, AdapterError>
    where
        F: FnOnce(&CatalogState) -> Result<R, AdapterError>,
    {
        trace!("transact: {:?}", ops);

        let drop_ids: HashSet<_> = ops
            .iter()
            .filter_map(|op| match op {
                Op::DropItem(id) => Some(*id),
                _ => None,
            })
            .collect();
        let temporary_drops = drop_ids
            .iter()
            .filter_map(|id| {
                let entry = self.get_entry(id);
                match entry.item.conn_id() {
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
            session,
            ops,
            temporary_ids,
            &mut builtin_table_updates,
            &mut audit_events,
            &mut tx,
            &mut state,
        )?;

        let result = f(&state)?;

        // The user closure was successful, apply the updates.
        let (_stash, collections) = tx.commit_without_consolidate().await?;
        // Dropping here keeps the mutable borrow on self, preventing us accidentally
        // mutating anything until after f is executed.
        drop(storage);
        self.state = state;
        self.transient_revision += 1;

        Ok(TransactionResult {
            builtin_table_updates,
            audit_events,
            collections,
            result,
        })
    }

    fn transact_inner(
        session: Option<&Session>,
        ops: Vec<Op>,
        temporary_ids: Vec<GlobalId>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        tx: &mut Transaction<'_, S>,
        state: &mut CatalogState,
    ) -> Result<(), AdapterError> {
        #[derive(Debug, Clone)]
        enum Action {
            CreateDatabase {
                id: DatabaseId,
                oid: u32,
                name: String,
            },
            CreateSchema {
                id: SchemaId,
                oid: u32,
                database_id: DatabaseId,
                schema_name: String,
            },
            CreateRole {
                id: RoleId,
                oid: u32,
                name: String,
            },
            CreateComputeInstance {
                id: ComputeInstanceId,
                name: String,
                // These are the legacy, active logs of this compute instance
                arranged_introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
            },
            CreateComputeReplica {
                id: ReplicaId,
                name: String,
                on_cluster_name: String,
                config: ComputeReplicaConfig,
            },
            CreateItem {
                id: GlobalId,
                oid: u32,
                name: QualifiedObjectName,
                item: CatalogItem,
            },

            DropDatabase {
                id: DatabaseId,
            },
            DropSchema {
                database_id: DatabaseId,
                schema_id: SchemaId,
            },
            DropRole {
                name: String,
            },
            DropComputeInstance {
                name: String,
                introspection_source_index_ids: Vec<GlobalId>,
            },
            DropComputeReplica {
                name: String,
                compute_id: ComputeInstanceId,
            },
            DropItem(GlobalId),
            UpdateItem {
                id: GlobalId,
                to_name: QualifiedObjectName,
                to_item: CatalogItem,
            },
            UpdateComputeInstanceStatus {
                event: ComputeInstanceEvent,
            },
            UpdateSysytemConfiguration {
                name: String,
                value: String,
            },
            ResetSystemConfiguration {
                name: String,
            },
            ResetAllSystemConfiguration,
            UpdateRotatedKeys {
                id: GlobalId,
                new_item: CatalogItem,
            },
        }

        fn sql_type_to_object_type(sql_type: SqlCatalogItemType) -> ObjectType {
            match sql_type {
                SqlCatalogItemType::Connection => ObjectType::Connection,
                SqlCatalogItemType::Func => ObjectType::Func,
                SqlCatalogItemType::Index => ObjectType::Index,
                SqlCatalogItemType::MaterializedView => ObjectType::MaterializedView,
                SqlCatalogItemType::Secret => ObjectType::Secret,
                SqlCatalogItemType::Sink => ObjectType::Sink,
                SqlCatalogItemType::Source => ObjectType::Source,
                SqlCatalogItemType::Table => ObjectType::Table,
                SqlCatalogItemType::Type => ObjectType::Type,
                SqlCatalogItemType::View => ObjectType::View,
            }
        }

        for op in ops {
            match op {
                Op::AlterSink { id, size, remote } => {
                    use mz_sql::ast::Value;
                    use mz_sql_parser::ast::CreateSinkOptionName::*;

                    let entry = state.get_entry(&id);
                    let name = entry.name().clone();
                    let old_sink = match entry.item() {
                        CatalogItem::Sink(sink) => sink.clone(),
                        other => {
                            coord_bail!("ALTER SINK entry was not a sink: {}", other.typ())
                        }
                    };

                    // Since the catalog serializes the items using only their creation statement
                    // and context, we need to parse and rewrite the with options in that statement.
                    // (And then make any other changes to the source definition to match.)
                    let mut stmt = mz_sql::parse::parse(&old_sink.create_sql)
                        .unwrap()
                        .into_element();

                    let create_stmt = match &mut stmt {
                        Statement::CreateSink(s) => s,
                        _ => coord_bail!("sink {id} was not created with a CREATE SINK statement"),
                    };

                    let new_config = alter_host_config(&old_sink.host_config, size, remote)?;

                    if let Some(config) = new_config {
                        create_stmt
                            .with_options
                            .retain(|x| ![Remote, Size].contains(&x.name));

                        let new_host_option = match &config {
                            plan::StorageHostConfig::Managed { size } => Some((Size, size.clone())),
                            plan::StorageHostConfig::Remote { addr } => {
                                Some((Remote, addr.clone()))
                            }
                            plan::StorageHostConfig::Undefined => None,
                        };

                        if let Some((name, value)) = new_host_option {
                            create_stmt.with_options.push(CreateSinkOption {
                                name,
                                value: Some(WithOptionValue::Value(Value::String(value))),
                            });
                        }

                        // Undefined size sinks only allowed in unsafe mode.
                        let allow_undefined_size = state.config().unsafe_mode;

                        let host_config =
                            state.resolve_storage_host_config(config, allow_undefined_size)?;
                        let create_sql = stmt.to_ast_string_stable();
                        let sink = CatalogItem::Sink(Sink {
                            create_sql,
                            host_config: host_config.clone(),
                            ..old_sink
                        });

                        let ser = Self::serialize_item(&sink);
                        tx.update_item(id, &name.item, &ser)?;

                        // NB: this will be re-incremented by the action below.
                        builtin_table_updates.extend(state.pack_item_update(id, -1));

                        state.add_to_audit_log(
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Alter,
                            ObjectType::Sink,
                            EventDetails::AlterSourceSinkV1(mz_audit_log::AlterSourceSinkV1 {
                                id: id.to_string(),
                                name: Self::full_name_detail(&state.resolve_full_name(
                                    &name,
                                    session.map(|session| session.conn_id()),
                                )),
                                old_size: old_sink.host_config.size().map(|x| x.to_string()),
                                new_size: host_config.size().map(|x| x.to_string()),
                            }),
                        )?;

                        let to_name = entry.name().clone();
                        catalog_action(
                            state,
                            builtin_table_updates,
                            Action::UpdateItem {
                                id,
                                to_name,
                                to_item: sink,
                            },
                        )?;
                    }
                }
                Op::AlterSource { id, size, remote } => {
                    use mz_sql::ast::Value;
                    use mz_sql_parser::ast::CreateSourceOptionName::*;

                    let entry = state.get_entry(&id);
                    let name = entry.name().clone();
                    let old_source = match entry.item() {
                        CatalogItem::Source(source) => source.clone(),
                        other => {
                            coord_bail!("ALTER SOURCE entry was not a source: {}", other.typ())
                        }
                    };

                    // Since the catalog serializes the items using only their creation statement
                    // and context, we need to parse and rewrite the with options in that statement.
                    // (And then make any other changes to the source definition to match.)
                    let mut stmt = mz_sql::parse::parse(&old_source.create_sql)
                        .unwrap()
                        .into_element();

                    let create_stmt = match &mut stmt {
                        Statement::CreateSource(s) => s,
                        _ => coord_bail!(
                            "source {id} was not created with a CREATE SOURCE statement"
                        ),
                    };

                    let new_config = match &old_source.data_source {
                        DataSourceDesc::Ingestion(ingestion) => {
                            alter_host_config(&ingestion.host_config, size, remote)?
                        }
                        DataSourceDesc::Introspection(_) | DataSourceDesc::Source => None,
                    };

                    if let Some(config) = new_config {
                        create_stmt
                            .with_options
                            .retain(|x| ![Size, Remote].contains(&x.name));

                        let new_host_option = match &config {
                            plan::StorageHostConfig::Managed { size } => Some((Size, size.clone())),
                            plan::StorageHostConfig::Remote { addr } => {
                                Some((Remote, addr.clone()))
                            }
                            plan::StorageHostConfig::Undefined => None,
                        };

                        if let Some((name, value)) = new_host_option {
                            create_stmt.with_options.push(CreateSourceOption {
                                name,
                                value: Some(WithOptionValue::Value(Value::String(value))),
                            });
                        }

                        // Only introspection + subsources can have an undefined size outside of
                        // unsafe mode.
                        let allow_undefined_size = state.config().unsafe_mode
                            || match old_source.data_source {
                                DataSourceDesc::Introspection(_) | DataSourceDesc::Source => true,
                                DataSourceDesc::Ingestion(_) => false,
                            };

                        let old_size = old_source.size().map(|s| s.to_string());
                        let host_config =
                            state.resolve_storage_host_config(config, allow_undefined_size)?;
                        let new_size = host_config.size().map(|s| s.to_string());
                        let data_source = match old_source.data_source {
                            DataSourceDesc::Ingestion(ingestion) => {
                                    DataSourceDesc::Ingestion(Ingestion {
                                    host_config,
                                    ..ingestion
                                })
                            }
                            _ => unreachable!("already guaranteed that we do not permit modifying either SIZE or REMOTE of subsource or introspection source"),
                        };

                        let create_sql = stmt.to_ast_string_stable();
                        let source = CatalogItem::Source(Source {
                            create_sql,
                            data_source,
                            ..old_source
                        });

                        let ser = Self::serialize_item(&source);
                        tx.update_item(id, &name.item, &ser)?;

                        // NB: this will be re-incremented by the action below.
                        builtin_table_updates.extend(state.pack_item_update(id, -1));

                        state.add_to_audit_log(
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Alter,
                            ObjectType::Source,
                            EventDetails::AlterSourceSinkV1(mz_audit_log::AlterSourceSinkV1 {
                                id: id.to_string(),
                                name: Self::full_name_detail(&state.resolve_full_name(
                                    &name,
                                    session.map(|session| session.conn_id()),
                                )),
                                old_size,
                                new_size,
                            }),
                        )?;

                        let to_name = entry.name().clone();
                        catalog_action(
                            state,
                            builtin_table_updates,
                            Action::UpdateItem {
                                id,
                                to_name,
                                to_item: source,
                            },
                        )?;
                    }
                }
                Op::CreateDatabase {
                    name,
                    oid,
                    public_schema_oid,
                } => {
                    let database_id = tx.insert_database(&name)?;
                    let schema_id = tx.insert_schema(database_id, DEFAULT_SCHEMA)?;
                    state.add_to_audit_log(
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
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::CreateDatabase {
                            id: database_id,
                            oid,
                            name: name.clone(),
                        },
                    )?;
                    state.add_to_audit_log(
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Schema,
                        EventDetails::SchemaV1(mz_audit_log::SchemaV1 {
                            id: schema_id.to_string(),
                            name: DEFAULT_SCHEMA.to_string(),
                            database_name: name,
                        }),
                    )?;
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::CreateSchema {
                            id: schema_id,
                            oid: public_schema_oid,
                            database_id,
                            schema_name: DEFAULT_SCHEMA.to_string(),
                        },
                    )?;
                }
                Op::CreateSchema {
                    database_id,
                    schema_name,
                    oid,
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
                    let schema_id = tx.insert_schema(database_id, &schema_name)?;
                    state.add_to_audit_log(
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Schema,
                        EventDetails::SchemaV1(mz_audit_log::SchemaV1 {
                            id: schema_id.to_string(),
                            name: schema_name.clone(),
                            database_name: state.database_by_id[&database_id].name.clone(),
                        }),
                    )?;
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::CreateSchema {
                            id: schema_id,
                            oid,
                            database_id,
                            schema_name,
                        },
                    )?;
                }
                Op::CreateRole { name, oid } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedRoleName(name),
                        )));
                    }
                    let role_id = tx.insert_user_role(&name)?;
                    state.add_to_audit_log(
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Role,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: role_id.to_string(),
                            name: name.clone(),
                        }),
                    )?;
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::CreateRole {
                            id: role_id,
                            oid,
                            name,
                        },
                    )?;
                }
                Op::CreateComputeInstance {
                    name,
                    arranged_introspection_sources,
                } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedClusterName(name),
                        )));
                    }
                    let id =
                        tx.insert_user_compute_instance(&name, &arranged_introspection_sources)?;
                    state.add_to_audit_log(
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
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::CreateComputeInstance {
                            id,
                            name,
                            arranged_introspection_sources,
                        },
                    )?;
                }
                Op::CreateComputeReplica {
                    name,
                    on_cluster_name,
                    config,
                } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedReplicaName(name),
                        )));
                    }
                    let (replica_id, compute_instance_id) =
                        tx.insert_compute_replica(&on_cluster_name, &name, &config.clone().into())?;
                    if let ComputeReplicaLocation::Managed { size, .. } = &config.location {
                        let details = EventDetails::CreateComputeReplicaV1(
                            mz_audit_log::CreateComputeReplicaV1 {
                                cluster_id: compute_instance_id.to_string(),
                                cluster_name: on_cluster_name.clone(),
                                replica_name: name.clone(),
                                logical_size: size.clone(),
                            },
                        );
                        state.add_to_audit_log(
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Create,
                            ObjectType::ClusterReplica,
                            details,
                        )?;
                    }
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::CreateComputeReplica {
                            id: replica_id,
                            name,
                            on_cluster_name,
                            config,
                        },
                    )?;
                }
                Op::CreateItem {
                    id,
                    oid,
                    name,
                    item,
                } => {
                    state.ensure_no_unstable_uses(&item)?;

                    if let Some(id @ ComputeInstanceId::System(_)) = item.compute_instance_id() {
                        let compute_instance_name = state.compute_instances_by_id[&id].name.clone();
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlyComputeInstance(compute_instance_name),
                        )));
                    }

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
                            item.uses()
                                .iter()
                                .find(|id| match state.try_get_entry(*id) {
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
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlySystemSchema(name.to_string()),
                            )));
                        }
                        let schema_id = name.qualifiers.schema_spec.clone().into();
                        let serialized_item = Self::serialize_item(&item);
                        tx.insert_item(id, schema_id, &name.item, serialized_item)?;
                    }

                    if Self::should_audit_log_item(&item) {
                        let id = id.to_string();
                        let name = Self::full_name_detail(
                            &state
                                .resolve_full_name(&name, session.map(|session| session.conn_id())),
                        );
                        let details = match &item {
                            CatalogItem::Source(s) => {
                                EventDetails::CreateSourceSinkV1(mz_audit_log::CreateSourceSinkV1 {
                                    id,
                                    name,
                                    size: s.size().map(|s| s.to_string()),
                                })
                            }
                            CatalogItem::Sink(s) => {
                                EventDetails::CreateSourceSinkV1(mz_audit_log::CreateSourceSinkV1 {
                                    id: id.to_string(),
                                    name,
                                    size: s.host_config.size().map(|x| x.to_string()),
                                })
                            }
                            _ => EventDetails::IdFullNameV1(IdFullNameV1 { id, name }),
                        };
                        state.add_to_audit_log(
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Create,
                            sql_type_to_object_type(item.typ()),
                            details,
                        )?;
                    }

                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::CreateItem {
                            id,
                            oid,
                            name,
                            item,
                        },
                    )?;
                }
                Op::DropDatabase { id } => {
                    let database = &state.database_by_id[&id];
                    tx.remove_database(&id)?;
                    builtin_table_updates.push(state.pack_database_update(database, -1));
                    state.add_to_audit_log(
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
                    catalog_action(state, builtin_table_updates, Action::DropDatabase { id })?;
                }
                Op::DropSchema {
                    database_id,
                    schema_id,
                } => {
                    let schema = &state.database_by_id[&database_id].schemas_by_id[&schema_id];
                    tx.remove_schema(&database_id, &schema_id)?;
                    builtin_table_updates.push(state.pack_schema_update(
                        &ResolvedDatabaseSpecifier::Id(database_id.clone()),
                        &schema_id,
                        -1,
                    ));
                    state.add_to_audit_log(
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Drop,
                        ObjectType::Schema,
                        EventDetails::SchemaV1(mz_audit_log::SchemaV1 {
                            id: schema_id.to_string(),
                            name: schema.name.schema.to_string(),
                            database_name: state.database_by_id[&database_id].name.clone(),
                        }),
                    )?;
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::DropSchema {
                            database_id,
                            schema_id,
                        },
                    )?;
                }
                Op::DropRole { name } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedRoleName(name),
                        )));
                    }
                    tx.remove_role(&name)?;
                    let role = &state.roles[&name];
                    builtin_table_updates.push(state.pack_role_update(role, -1));
                    state.add_to_audit_log(
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
                    catalog_action(state, builtin_table_updates, Action::DropRole { name })?;
                }
                Op::DropComputeInstance { name } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedClusterName(name),
                        )));
                    }
                    let (instance_id, introspection_source_index_ids) =
                        tx.remove_compute_instance(&name)?;
                    builtin_table_updates.push(state.pack_compute_instance_update(&name, -1));
                    for id in &introspection_source_index_ids {
                        builtin_table_updates.extend(state.pack_item_update(*id, -1));
                    }
                    state.add_to_audit_log(
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Drop,
                        ObjectType::Cluster,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: instance_id.to_string(),
                            name: name.clone(),
                        }),
                    )?;
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::DropComputeInstance {
                            name,
                            introspection_source_index_ids,
                        },
                    )?;
                }
                Op::DropComputeReplica { name, compute_name } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedReplicaName(name),
                        )));
                    }
                    let instance = state.resolve_compute_instance(&compute_name)?;
                    tx.remove_compute_replica(&name, instance.id)?;

                    let replica_id = instance.replica_id_by_name[&name];
                    let replica = &instance.replicas_by_id[&replica_id];
                    for process_id in replica.process_status.keys() {
                        let update = state.pack_compute_instance_status_update(
                            instance.id,
                            replica_id,
                            *process_id,
                            -1,
                        );
                        builtin_table_updates.push(update);
                    }

                    builtin_table_updates.push(state.pack_compute_replica_update(
                        instance.id,
                        &name,
                        -1,
                    ));

                    let details =
                        EventDetails::DropComputeReplicaV1(mz_audit_log::DropComputeReplicaV1 {
                            cluster_id: instance.id.to_string(),
                            cluster_name: instance.name.clone(),
                            replica_name: name.clone(),
                        });
                    state.add_to_audit_log(
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Drop,
                        ObjectType::ClusterReplica,
                        details,
                    )?;

                    let compute_id = instance.id;
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::DropComputeReplica { name, compute_id },
                    )?;
                }
                Op::DropItem(id) => {
                    let entry = state.get_entry(&id);
                    if !entry.item().is_temporary() {
                        tx.remove_item(id)?;
                    }
                    builtin_table_updates.extend(state.pack_item_update(id, -1));
                    if Self::should_audit_log_item(&entry.item) {
                        state.add_to_audit_log(
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Drop,
                            sql_type_to_object_type(entry.item().typ()),
                            EventDetails::IdFullNameV1(IdFullNameV1 {
                                id: id.to_string(),
                                name: Self::full_name_detail(&state.resolve_full_name(
                                    &entry.name,
                                    session.map(|session| session.conn_id()),
                                )),
                            }),
                        )?;
                    }
                    catalog_action(state, builtin_table_updates, Action::DropItem(id))?;
                }
                Op::DropTimeline(timeline) => {
                    tx.remove_timestamp(timeline);
                }
                Op::RenameItem {
                    id,
                    to_name,
                    current_full_name,
                } => {
                    let mut actions = Vec::new();

                    let entry = state.get_entry(&id);
                    if let CatalogItem::Type(_) = entry.item() {
                        return Err(AdapterError::Catalog(Error::new(ErrorKind::TypeRename(
                            current_full_name.to_string(),
                        ))));
                    }

                    let mut to_full_name = current_full_name.clone();
                    to_full_name.item = to_name.clone();

                    let mut to_qualified_name = entry.name().clone();
                    to_qualified_name.item = to_name;

                    let details = EventDetails::RenameItemV1(mz_audit_log::RenameItemV1 {
                        id: id.to_string(),
                        old_name: Self::full_name_detail(&current_full_name),
                        new_name: Self::full_name_detail(&to_full_name),
                    });
                    if Self::should_audit_log_item(&entry.item) {
                        state.add_to_audit_log(
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Alter,
                            sql_type_to_object_type(entry.item().typ()),
                            details,
                        )?;
                    }

                    // Rename item itself.
                    let item = entry
                        .item
                        .rename_item_refs(
                            current_full_name.clone(),
                            to_full_name.item.clone(),
                            true,
                        )
                        .map_err(|e| {
                            Error::new(ErrorKind::from(AmbiguousRename {
                                depender: state
                                    .resolve_full_name(&entry.name, entry.conn_id())
                                    .to_string(),
                                dependee: state
                                    .resolve_full_name(&entry.name, entry.conn_id())
                                    .to_string(),
                                message: e,
                            }))
                        })?;
                    let serialized_item = Self::serialize_item(&item);

                    for id in entry.used_by() {
                        let dependent_item = state.get_entry(id);
                        let to_item = dependent_item
                            .item
                            .rename_item_refs(
                                current_full_name.clone(),
                                to_full_name.item.clone(),
                                false,
                            )
                            .map_err(|e| {
                                Error::new(ErrorKind::from(AmbiguousRename {
                                    depender: state
                                        .resolve_full_name(
                                            &dependent_item.name,
                                            dependent_item.conn_id(),
                                        )
                                        .to_string(),
                                    dependee: state
                                        .resolve_full_name(&entry.name, entry.conn_id())
                                        .to_string(),
                                    message: e,
                                }))
                            })?;

                        if !item.is_temporary() {
                            let serialized_item = Self::serialize_item(&to_item);
                            tx.update_item(*id, &dependent_item.name().item, &serialized_item)?;
                        }
                        builtin_table_updates.extend(state.pack_item_update(*id, -1));

                        actions.push(Action::UpdateItem {
                            id: id.clone(),
                            to_name: dependent_item.name().clone(),
                            to_item,
                        });
                    }
                    if !item.is_temporary() {
                        tx.update_item(id, &to_full_name.item, &serialized_item)?;
                    }
                    builtin_table_updates.extend(state.pack_item_update(id, -1));
                    actions.push(Action::UpdateItem {
                        id,
                        to_name: to_qualified_name,
                        to_item: item,
                    });
                    for action in actions {
                        catalog_action(state, builtin_table_updates, action)?;
                    }
                }
                Op::UpdateComputeInstanceStatus { event } => {
                    // When we receive the first status update for a given
                    // replica process, there is no entry in the builtin table
                    // yet, so we must make sure to not try to delete one.
                    let status_known = state
                        .try_get_compute_instance_status(
                            event.instance_id,
                            event.replica_id,
                            event.process_id,
                        )
                        .is_some();
                    if status_known {
                        let update = state.pack_compute_instance_status_update(
                            event.instance_id,
                            event.replica_id,
                            event.process_id,
                            -1,
                        );
                        builtin_table_updates.push(update);
                    }

                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::UpdateComputeInstanceStatus { event },
                    )?;
                }
                Op::UpdateItem { id, name, to_item } => {
                    let ser = Self::serialize_item(&to_item);
                    tx.update_item(id, &name.item, &ser)?;
                    builtin_table_updates.extend(state.pack_item_update(id, -1));
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::UpdateItem {
                            id,
                            to_name: name,
                            to_item,
                        },
                    )?;
                }
                Op::UpdateStorageUsage {
                    shard_id,
                    size_bytes,
                } => {
                    state.add_to_storage_usage(tx, builtin_table_updates, shard_id, size_bytes)?;
                }
                Op::UpdateSystemConfiguration { name, value } => {
                    tx.upsert_system_config(&name, &value)?;
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::UpdateSysytemConfiguration { name, value },
                    )?;
                }
                Op::ResetSystemConfiguration { name } => {
                    tx.remove_system_config(&name);
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::ResetSystemConfiguration { name },
                    )?;
                }
                Op::ResetAllSystemConfiguration {} => {
                    tx.clear_system_configs();
                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::ResetAllSystemConfiguration,
                    )?;
                }
                Op::UpdateRotatedKeys {
                    id,
                    previous_public_keypair,
                    new_public_keypair,
                } => {
                    let entry = state.get_entry(&id);
                    // Retract old keys
                    builtin_table_updates.extend(state.pack_ssh_tunnel_connection_update(
                        id,
                        &previous_public_keypair,
                        -1,
                    ));
                    // Insert the new rotated keys
                    builtin_table_updates.extend(state.pack_ssh_tunnel_connection_update(
                        id,
                        &new_public_keypair,
                        1,
                    ));

                    let mut connection = entry.connection()?.clone();
                    if let mz_storage::types::connections::Connection::Ssh(ref mut ssh) =
                        connection.connection
                    {
                        ssh.public_keys = Some(new_public_keypair)
                    }
                    let new_item = CatalogItem::Connection(connection);

                    catalog_action(
                        state,
                        builtin_table_updates,
                        Action::UpdateRotatedKeys { id, new_item },
                    )?;
                }
            };
        }

        fn catalog_action(
            state: &mut CatalogState,
            builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
            action: Action,
        ) -> Result<(), AdapterError> {
            match action {
                Action::CreateDatabase { id, oid, name } => {
                    info!("create database {}", name);
                    state.database_by_id.insert(
                        id.clone(),
                        Database {
                            name: name.clone(),
                            id: id.clone(),
                            oid,
                            schemas_by_id: BTreeMap::new(),
                            schemas_by_name: BTreeMap::new(),
                        },
                    );
                    state.database_by_name.insert(name, id.clone());
                    builtin_table_updates
                        .push(state.pack_database_update(&state.database_by_id[&id], 1));
                }

                Action::CreateSchema {
                    id,
                    oid,
                    database_id,
                    schema_name,
                } => {
                    info!(
                        "create schema {}.{}",
                        state.get_database(&database_id).name,
                        schema_name
                    );
                    let db = state.database_by_id.get_mut(&database_id).unwrap();
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
                        },
                    );
                    db.schemas_by_name.insert(schema_name, id.clone());
                    builtin_table_updates.push(state.pack_schema_update(
                        &ResolvedDatabaseSpecifier::Id(database_id.clone()),
                        &id,
                        1,
                    ));
                }

                Action::CreateRole { id, oid, name } => {
                    info!("create role {}", name);
                    state.roles.insert(
                        name.clone(),
                        Role {
                            name: name.clone(),
                            id,
                            oid,
                        },
                    );
                    let role = &state.roles[&name];
                    builtin_table_updates.push(state.pack_role_update(role, 1));
                }

                Action::CreateComputeInstance {
                    id,
                    name,
                    arranged_introspection_sources,
                } => {
                    info!("create cluster {}", name);
                    let arranged_introspection_source_ids: Vec<GlobalId> =
                        arranged_introspection_sources
                            .iter()
                            .map(|(_, id)| *id)
                            .collect();
                    state.insert_compute_instance(id, name.clone(), arranged_introspection_sources);
                    builtin_table_updates.push(state.pack_compute_instance_update(&name, 1));
                    for id in arranged_introspection_source_ids {
                        builtin_table_updates.extend(state.pack_item_update(id, 1));
                    }
                }

                Action::CreateComputeReplica {
                    id,
                    name,
                    on_cluster_name,
                    config,
                } => {
                    let compute_instance_id = state.compute_instances_by_name[&on_cluster_name];
                    let introspection_ids: Vec<_> = config.logging.source_and_view_ids().collect();
                    state.insert_compute_replica(compute_instance_id, name.clone(), id, config);
                    for id in introspection_ids {
                        builtin_table_updates.extend(state.pack_item_update(id, 1));
                    }
                    builtin_table_updates.push(state.pack_compute_replica_update(
                        compute_instance_id,
                        &name,
                        1,
                    ));
                }

                Action::CreateItem {
                    id,
                    oid,
                    name,
                    item,
                } => {
                    state.insert_item(id, oid, name, item);
                    builtin_table_updates.extend(state.pack_item_update(id, 1));
                }

                Action::DropDatabase { id } => {
                    let db = state.database_by_id.get(&id).unwrap();
                    state.database_by_name.remove(db.name());
                    state.database_by_id.remove(&id);
                }

                Action::DropSchema {
                    database_id,
                    schema_id,
                } => {
                    let db = state.database_by_id.get_mut(&database_id).unwrap();
                    let schema = db.schemas_by_id.get(&schema_id).unwrap();
                    db.schemas_by_name.remove(&schema.name.schema);
                    db.schemas_by_id.remove(&schema_id);
                }

                Action::DropRole { name } => {
                    if state.roles.remove(&name).is_some() {
                        info!("drop role {}", name);
                    }
                }

                Action::DropComputeInstance {
                    name,
                    introspection_source_index_ids,
                } => {
                    for id in introspection_source_index_ids {
                        state.drop_item(id);
                    }

                    let id = state
                        .compute_instances_by_name
                        .remove(&name)
                        .expect("can only drop known instances");

                    let instance = state
                        .compute_instances_by_id
                        .remove(&id)
                        .expect("can only drop known instances");

                    assert!(
                        instance.exports.is_empty() && instance.replicas_by_id.is_empty(),
                        "not all items dropped before compute instance"
                    );
                }

                Action::DropComputeReplica { name, compute_id } => {
                    let instance = state
                        .compute_instances_by_id
                        .get_mut(&compute_id)
                        .expect("can only drop replicas from known instances");
                    let replica_id = instance.replica_id_by_name.remove(&name).unwrap();
                    let replica = instance.replicas_by_id.remove(&replica_id).unwrap();
                    let persisted_log_ids = replica.config.logging.source_and_view_ids();
                    assert!(instance.replica_id_by_name.len() == instance.replicas_by_id.len());

                    for id in persisted_log_ids {
                        builtin_table_updates.extend(state.pack_item_update(id, -1));
                        state.drop_item(id);
                    }
                }

                Action::DropItem(id) => {
                    state.drop_item(id);
                }

                Action::UpdateItem {
                    id,
                    to_name,
                    to_item,
                } => {
                    let old_entry = state.entry_by_id.remove(&id).unwrap();
                    info!(
                        "update {} {} ({})",
                        old_entry.item_type(),
                        state.resolve_full_name(&old_entry.name, old_entry.conn_id()),
                        id
                    );
                    assert_eq!(old_entry.uses(), to_item.uses());
                    let conn_id = old_entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
                    let schema = &mut state.get_schema_mut(
                        &old_entry.name().qualifiers.database_spec,
                        &old_entry.name().qualifiers.schema_spec,
                        conn_id,
                    );
                    schema.items.remove(&old_entry.name().item);
                    let mut new_entry = old_entry.clone();
                    new_entry.name = to_name;
                    new_entry.item = to_item;
                    schema.items.insert(new_entry.name().item.clone(), id);
                    state.entry_by_id.insert(id, new_entry);
                    builtin_table_updates.extend(state.pack_item_update(id, 1));
                }

                Action::UpdateComputeInstanceStatus { event } => {
                    // It is possible that we receive a status update for a
                    // replica that has already been dropped from the catalog.
                    // In this case, `try_insert_compute_instance_status`
                    // returns `false` and we ignore the event.
                    if state.try_insert_compute_instance_status(event.clone()) {
                        builtin_table_updates.push(state.pack_compute_instance_status_update(
                            event.instance_id,
                            event.replica_id,
                            event.process_id,
                            1,
                        ));
                    }
                }
                Action::UpdateSysytemConfiguration { name, value } => {
                    state.insert_system_configuration(&name, &value)?;
                }
                Action::ResetSystemConfiguration { name } => {
                    state.remove_system_configuration(&name)?;
                }
                Action::ResetAllSystemConfiguration {} => {
                    state.clear_system_configuration();
                }

                Action::UpdateRotatedKeys { id, new_item } => {
                    let old_entry = state.entry_by_id.remove(&id).unwrap();
                    info!(
                        "update {} {} ({})",
                        old_entry.item_type(),
                        state.resolve_full_name(&old_entry.name, old_entry.conn_id()),
                        id
                    );
                    let mut new_entry = old_entry;
                    new_entry.item = new_item;
                    state.entry_by_id.insert(id, new_entry);
                }
            }
            Ok(())
        }
        Ok(())
    }

    pub async fn consolidate(&mut self, collections: &[mz_stash::Id]) -> Result<(), AdapterError> {
        Ok(self.storage().await.consolidate(collections).await?)
    }

    pub async fn confirm_leadership(&mut self) -> Result<(), AdapterError> {
        Ok(self.storage().await.confirm_leadership().await?)
    }

    fn serialize_item(item: &CatalogItem) -> SerializedCatalogItem {
        match item {
            CatalogItem::Table(table) => SerializedCatalogItem::V1 {
                create_sql: table.create_sql.clone(),
            },
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => {
                assert!(
                    match source.data_source {
                        DataSourceDesc::Introspection(_) => false,
                        _ => true,
                    },
                    "cannot serialize introspection/builtin sources",
                );
                SerializedCatalogItem::V1 {
                    create_sql: source.create_sql.clone(),
                }
            }
            CatalogItem::View(view) => SerializedCatalogItem::V1 {
                create_sql: view.create_sql.clone(),
            },
            CatalogItem::MaterializedView(mview) => SerializedCatalogItem::V1 {
                create_sql: mview.create_sql.clone(),
            },
            CatalogItem::Index(index) => SerializedCatalogItem::V1 {
                create_sql: index.create_sql.clone(),
            },
            CatalogItem::Sink(sink) => SerializedCatalogItem::V1 {
                create_sql: sink.create_sql.clone(),
            },
            CatalogItem::Type(typ) => SerializedCatalogItem::V1 {
                create_sql: typ.create_sql.clone(),
            },
            CatalogItem::Secret(secret) => SerializedCatalogItem::V1 {
                create_sql: secret.create_sql.clone(),
            },
            CatalogItem::Connection(connection) => SerializedCatalogItem::V1 {
                create_sql: connection.create_sql.clone(),
            },
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
        }
    }

    fn deserialize_item(
        &self,
        SerializedCatalogItem::V1 { create_sql }: SerializedCatalogItem,
    ) -> Result<CatalogItem, anyhow::Error> {
        self.parse_item(create_sql, Some(&PlanContext::zero()))
    }

    // Parses the given SQL string into a `CatalogItem`.
    fn parse_item(
        &self,
        create_sql: String,
        pcx: Option<&PlanContext>,
    ) -> Result<CatalogItem, anyhow::Error> {
        let session_catalog = self.for_system_session();
        let stmt = mz_sql::parse::parse(&create_sql)?.into_element();
        let (stmt, depends_on) = mz_sql::names::resolve(&session_catalog, stmt)?;
        let depends_on = depends_on.into_iter().collect();
        let plan = mz_sql::plan::plan(pcx, &session_catalog, stmt, &Params::empty())?;
        Ok(match plan {
            Plan::CreateTable(CreateTablePlan { table, .. }) => CatalogItem::Table(Table {
                create_sql: table.create_sql,
                desc: table.desc,
                defaults: table.defaults,
                conn_id: None,
                depends_on,
            }),
            Plan::CreateSource(CreateSourcePlan {
                source,
                timeline,
                host_config,
                ..
            }) => {
                let allow_undefined_size = true;
                CatalogItem::Source(Source {
                    create_sql: source.create_sql,
                    data_source: match source.ingestion {
                        Some(ingestion) => DataSourceDesc::Ingestion(Ingestion {
                            desc: ingestion.desc,
                            source_imports: ingestion.source_imports,
                            subsource_exports: ingestion.subsource_exports,
                            host_config: self
                                .resolve_storage_host_config(host_config, allow_undefined_size)?,
                        }),
                        None => DataSourceDesc::Source,
                    },
                    desc: source.desc,
                    timeline,
                    depends_on,
                })
            }
            Plan::CreateView(CreateViewPlan { view, .. }) => {
                let mut optimizer = Optimizer::logical_optimizer();
                let optimized_expr = optimizer.optimize(view.expr)?;
                let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    optimized_expr,
                    desc,
                    conn_id: None,
                    depends_on,
                })
            }
            Plan::CreateMaterializedView(CreateMaterializedViewPlan {
                materialized_view, ..
            }) => {
                let mut optimizer = Optimizer::logical_optimizer();
                let optimized_expr = optimizer.optimize(materialized_view.expr)?;
                let desc = RelationDesc::new(optimized_expr.typ(), materialized_view.column_names);
                CatalogItem::MaterializedView(MaterializedView {
                    create_sql: materialized_view.create_sql,
                    optimized_expr,
                    desc,
                    depends_on,
                    compute_instance: materialized_view.compute_instance,
                })
            }
            Plan::CreateIndex(CreateIndexPlan { index, .. }) => CatalogItem::Index(Index {
                create_sql: index.create_sql,
                on: index.on,
                keys: index.keys,
                conn_id: None,
                depends_on,
                compute_instance: index.compute_instance,
            }),
            Plan::CreateSink(CreateSinkPlan {
                sink,
                with_snapshot,
                host_config,
                ..
            }) => {
                let allow_undefined_size = true;
                CatalogItem::Sink(Sink {
                    create_sql: sink.create_sql,
                    from: sink.from,
                    connection: StorageSinkConnectionState::Pending(sink.connection_builder),
                    envelope: sink.envelope,
                    with_snapshot,
                    depends_on,
                    host_config: self
                        .resolve_storage_host_config(host_config, allow_undefined_size)?,
                })
            }
            Plan::CreateType(CreateTypePlan { typ, .. }) => CatalogItem::Type(Type {
                create_sql: typ.create_sql,
                details: CatalogTypeDetails {
                    array_id: None,
                    typ: typ.inner,
                },
                depends_on,
            }),
            Plan::CreateSecret(CreateSecretPlan { secret, .. }) => CatalogItem::Secret(Secret {
                create_sql: secret.create_sql,
            }),
            Plan::CreateConnection(CreateConnectionPlan { connection, .. }) => {
                CatalogItem::Connection(Connection {
                    create_sql: connection.create_sql,
                    connection: connection.connection,
                    depends_on,
                })
            }
            _ => bail!("catalog entry generated inappropriate plan"),
        })
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        self.state.uses_tables(id)
    }

    /// Return the ids of all active log sources the given object depends on.
    pub fn arranged_introspection_dependencies(&self, id: GlobalId) -> Vec<GlobalId> {
        self.state.arranged_introspection_dependencies(id)
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> String {
        self.state.dump()
    }

    pub fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        self.state.config()
    }

    pub fn entries(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.state.entry_by_id.values()
    }

    pub fn user_tables(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_table() && entry.id.is_user())
    }

    pub fn user_sources(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_source() && entry.id.is_user())
    }

    pub fn user_sinks(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_sink() && entry.id.is_user())
    }

    pub fn user_materialized_views(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_materialized_view() && entry.id.is_user())
    }

    pub fn user_secrets(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_secret() && entry.id.is_user())
    }

    pub fn compute_instances(&self) -> impl Iterator<Item = &ComputeInstance> {
        self.state.compute_instances_by_id.values()
    }

    pub fn user_compute_instances(&self) -> impl Iterator<Item = &ComputeInstance> {
        self.compute_instances()
            .filter(|compute_instance| compute_instance.id.is_user())
    }

    pub fn databases(&self) -> impl Iterator<Item = &Database> {
        self.state.database_by_id.values()
    }

    pub fn user_roles(&self) -> impl Iterator<Item = &Role> {
        self.state.roles.values().filter(|role| role.is_user())
    }

    /// Allocate ids for legacy, active logs. Called once per compute instance creation
    pub async fn allocate_arranged_introspection_sources(
        &mut self,
    ) -> Vec<(&'static BuiltinLog, GlobalId)> {
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
            .expect("cannot fail to allocate system ids");
        BUILTINS::logs().zip(system_ids.into_iter()).collect()
    }

    /// Allocate ids for persisted introspection views. Called once per compute replica creation
    #[allow(clippy::unused_async)]
    pub async fn allocate_persisted_introspection_views(&mut self) -> Vec<(LogView, GlobalId)> {
        vec![]
        //let log_amount = DEFAULT_LOG_VIEWS
        //    .len()
        //    .try_into()
        //    .expect("default log variants should fit into u64");
        //let system_ids = self
        //    .storage()
        //    .await
        //    .allocate_system_ids(log_amount)
        //    .await
        //    .expect("cannot fail to allocate system ids");

        //DEFAULT_LOG_VIEWS
        //    .clone()
        //    .into_iter()
        //    .zip(system_ids.into_iter())
        //    .collect()
    }

    /// Allocate ids for persisted introspection sources.
    /// Called once per compute replica creation.
    #[allow(clippy::unused_async)]
    pub async fn allocate_persisted_introspection_sources(
        &mut self,
    ) -> Vec<(LogVariant, GlobalId)> {
        vec![]
        // let log_amount = DEFAULT_LOG_VARIANTS
        //     .len()
        //     .try_into()
        //     .expect("default log variants should fit into u64");
        // let system_ids = self
        //     .storage()
        //     .await
        //     .allocate_system_ids(log_amount)
        //     .await
        //     .expect("cannot fail to allocate system ids");

        // DEFAULT_LOG_VARIANTS
        //     .clone()
        //     .into_iter()
        //     .zip(system_ids.into_iter())
        //     .collect()
    }

    pub fn pack_item_update(&self, id: GlobalId, diff: Diff) -> Vec<BuiltinTableUpdate> {
        self.state.pack_item_update(id, diff)
    }

    pub fn system_config(&self) -> &SystemVars {
        self.state.system_config()
    }

    pub async fn most_recent_storage_usage_collection(&self) -> Result<Option<EpochMillis>, Error> {
        Ok(self
            .storage()
            .await
            .storage_usage()
            .await?
            .map(|usage| usage.timestamp())
            .max())
    }
}

pub fn is_reserved_name(name: &str) -> bool {
    BUILTIN_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

/// Return a [`plan::StorageHostConfig`] based on an existing [`StorageHostConfig`] and a set of potentially altered parameters.
///
/// If a [`None`] is returned, it means the existing config does not need to be updated.
fn alter_host_config(
    old_config: &StorageHostConfig,
    size: AlterOptionParameter,
    remote: AlterOptionParameter,
) -> Result<Option<plan::StorageHostConfig>, AdapterError> {
    use plan::AlterOptionParameter::*;

    match (old_config, size, remote) {
        // Should be caught during planning, but it is better to be safe
        (_, Set(_), Set(_)) => {
            coord_bail!("only one of REMOTE and SIZE can be set")
        }
        (_, Set(size), _) => Ok(Some(plan::StorageHostConfig::Managed { size })),
        (_, _, Set(addr)) => Ok(Some(plan::StorageHostConfig::Remote { addr })),
        (StorageHostConfig::Remote { .. }, _, Reset)
        | (StorageHostConfig::Managed { .. }, Reset, _) => {
            Ok(Some(plan::StorageHostConfig::Undefined))
        }
        (_, _, _) => Ok(None),
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    AlterSink {
        id: GlobalId,
        size: AlterOptionParameter,
        remote: AlterOptionParameter,
    },
    AlterSource {
        id: GlobalId,
        size: AlterOptionParameter,
        remote: AlterOptionParameter,
    },
    CreateDatabase {
        name: String,
        oid: u32,
        public_schema_oid: u32,
    },
    CreateSchema {
        database_id: ResolvedDatabaseSpecifier,
        schema_name: String,
        oid: u32,
    },
    CreateRole {
        name: String,
        oid: u32,
    },
    CreateComputeInstance {
        name: String,
        arranged_introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
    },
    CreateComputeReplica {
        name: String,
        on_cluster_name: String,
        config: ComputeReplicaConfig,
    },
    CreateItem {
        id: GlobalId,
        oid: u32,
        name: QualifiedObjectName,
        item: CatalogItem,
    },
    DropDatabase {
        id: DatabaseId,
    },
    DropSchema {
        database_id: DatabaseId,
        schema_id: SchemaId,
    },
    DropRole {
        name: String,
    },
    DropComputeInstance {
        name: String,
    },
    DropComputeReplica {
        name: String,
        compute_name: String,
    },
    /// Unconditionally removes the identified items. It is required that the
    /// IDs come from the output of `plan_remove`; otherwise consistency rules
    /// may be violated.
    DropItem(GlobalId),
    DropTimeline(Timeline),
    RenameItem {
        id: GlobalId,
        current_full_name: FullObjectName,
        to_name: String,
    },
    UpdateComputeInstanceStatus {
        event: ComputeInstanceEvent,
    },
    UpdateItem {
        id: GlobalId,
        name: QualifiedObjectName,
        to_item: CatalogItem,
    },
    UpdateStorageUsage {
        shard_id: Option<String>,
        size_bytes: u64,
    },
    UpdateSystemConfiguration {
        name: String,
        value: String,
    },
    ResetSystemConfiguration {
        name: String,
    },
    ResetAllSystemConfiguration {},
    UpdateRotatedKeys {
        id: GlobalId,
        previous_public_keypair: (String, String),
        new_public_keypair: (String, String),
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SerializedCatalogItem {
    V1 { create_sql: String },
}

/// Serialized (stored alongside the replica) logging configuration of
/// a replica. Serialized variant of `ComputeReplicaLogging`.
///
/// A `None` value for `sources` or `views` indicates that we did not yet create them in the
/// catalog. This is only used for initializing the default replica of the default compute
/// instance.
/// To indicate the absence of sources/views, use `Some(Vec::new())`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerializedComputeReplicaLogging {
    log_logging: bool,
    interval: Option<Duration>,
    sources: Option<Vec<(LogVariant, GlobalId)>>,
    views: Option<Vec<(LogView, GlobalId)>>,
}

impl From<ComputeReplicaLogging> for SerializedComputeReplicaLogging {
    fn from(
        ComputeReplicaLogging {
            log_logging,
            interval,
            sources,
            views,
        }: ComputeReplicaLogging,
    ) -> Self {
        Self {
            log_logging,
            interval,
            sources: Some(sources),
            views: Some(views),
        }
    }
}

/// A [`mz_compute_client::controller::ComputeReplicaConfig`] that is serialized as JSON
/// and persisted to the catalog stash. This is a separate type to allow us to evolve the on-disk
/// format independently from the SQL layer.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct SerializedComputeReplicaConfig {
    pub location: SerializedComputeReplicaLocation,
    pub logging: SerializedComputeReplicaLogging,
}

impl From<ComputeReplicaConfig> for SerializedComputeReplicaConfig {
    fn from(ComputeReplicaConfig { location, logging }: ComputeReplicaConfig) -> Self {
        SerializedComputeReplicaConfig {
            location: location.into(),
            logging: logging.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SerializedComputeReplicaLocation {
    Remote {
        addrs: BTreeSet<String>,
        compute_addrs: BTreeSet<String>,
        workers: NonZeroUsize,
    },
    Managed {
        size: String,
        availability_zone: String,
        /// `true` if the AZ was specified by the user and must be respected;
        /// `false` if it was picked arbitrarily by Materialize.
        az_user_specified: bool,
    },
}

impl From<ComputeReplicaLocation> for SerializedComputeReplicaLocation {
    fn from(loc: ComputeReplicaLocation) -> Self {
        match loc {
            ComputeReplicaLocation::Remote {
                addrs,
                compute_addrs,
                workers,
            } => Self::Remote {
                addrs,
                compute_addrs,
                workers,
            },
            ComputeReplicaLocation::Managed {
                allocation: _,
                size,
                availability_zone,
                az_user_specified,
            } => Self::Managed {
                size,
                availability_zone,
                az_user_specified,
            },
        }
    }
}

impl ConnCatalog<'_> {
    fn resolve_item_name(
        &self,
        name: &PartialObjectName,
    ) -> Result<&QualifiedObjectName, SqlCatalogError> {
        self.resolve_item(name).map(|entry| entry.name())
    }

    /// returns a `PartialObjectName` with the minimum amount of qualifiers to unambiguously resolve
    /// the object.
    fn minimal_qualification(&self, qualified_name: &QualifiedObjectName) -> PartialObjectName {
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
            && self.resolve_item_name(&PartialObjectName {
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

        let res = PartialObjectName {
            database: database_id.map(|id| self.get_database(&id).name().to_string()),
            schema: schema_spec.map(|spec| {
                self.get_schema(&qualified_name.qualifiers.database_spec, &spec)
                    .name()
                    .schema
                    .clone()
            }),
            item: qualified_name.item.clone(),
        };
        assert_eq!(self.resolve_item_name(&res), Ok(qualified_name));
        res
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
                    let name = QualifiedObjectName {
                        qualifiers: ObjectQualifiers {
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
}

impl SessionCatalog for ConnCatalog<'_> {
    fn active_user(&self) -> &str {
        &self.user.name
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

    fn active_compute_instance(&self) -> &str {
        &self.compute_instance
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

    fn resolve_schema(
        &self,
        database_name: Option<&str>,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        Ok(self.state.resolve_schema(
            self.database.as_ref(),
            database_name,
            schema_name,
            self.conn_id,
        )?)
    }

    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        Ok(self
            .state
            .resolve_schema_in_database(database_spec, schema_name, self.conn_id)?)
    }

    fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> &dyn CatalogSchema {
        self.state
            .get_schema(database_spec, schema_spec, self.conn_id)
    }

    fn is_system_schema(&self, schema: &str) -> bool {
        self.state.is_system_schema(schema)
    }

    fn resolve_role(
        &self,
        role_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogRole, SqlCatalogError> {
        match self.state.roles.get(role_name) {
            Some(role) => Ok(role),
            None => Err(SqlCatalogError::UnknownRole(role_name.into())),
        }
    }

    fn resolve_compute_instance(
        &self,
        compute_instance_name: Option<&str>,
    ) -> Result<&dyn mz_sql::catalog::CatalogComputeInstance, SqlCatalogError> {
        self.state
            .resolve_compute_instance(
                compute_instance_name.unwrap_or_else(|| self.active_compute_instance()),
            )
            .map(|compute_instance| {
                compute_instance as &dyn mz_sql::catalog::CatalogComputeInstance
            })
    }

    fn resolve_item(
        &self,
        name: &PartialObjectName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        Ok(self.state.resolve_entry(
            self.database.as_ref(),
            &self.effective_search_path(true),
            name,
            self.conn_id,
        )?)
    }

    fn resolve_function(
        &self,
        name: &PartialObjectName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        Ok(self.state.resolve_function(
            self.database.as_ref(),
            &self.effective_search_path(false),
            name,
            self.conn_id,
        )?)
    }

    fn try_get_item(&self, id: &GlobalId) -> Option<&dyn mz_sql::catalog::CatalogItem> {
        self.state
            .try_get_entry(id)
            .map(|item| item as &dyn mz_sql::catalog::CatalogItem)
    }

    fn get_item(&self, id: &GlobalId) -> &dyn mz_sql::catalog::CatalogItem {
        self.state.get_entry(id)
    }

    fn item_exists(&self, name: &QualifiedObjectName) -> bool {
        self.state.item_exists(name, self.conn_id)
    }

    fn get_compute_instance(
        &self,
        id: ComputeInstanceId,
    ) -> &dyn mz_sql::catalog::CatalogComputeInstance {
        &self.state.compute_instances_by_id[&id]
    }

    fn find_available_name(&self, name: QualifiedObjectName) -> QualifiedObjectName {
        self.state.find_available_name(name, self.conn_id)
    }

    fn resolve_full_name(&self, name: &QualifiedObjectName) -> FullObjectName {
        self.state.resolve_full_name(name, Some(self.conn_id))
    }

    fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        self.state.config()
    }

    fn now(&self) -> EpochMillis {
        (self.state.config().now)()
    }
}

impl mz_sql::catalog::CatalogDatabase for Database {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> DatabaseId {
        self.id
    }

    fn has_schemas(&self) -> bool {
        !self.schemas_by_name.is_empty()
    }
}

impl mz_sql::catalog::CatalogSchema for Schema {
    fn database(&self) -> &ResolvedDatabaseSpecifier {
        &self.name.database
    }

    fn name(&self) -> &QualifiedSchemaName {
        &self.name
    }

    fn id(&self) -> &SchemaSpecifier {
        &self.id
    }

    fn has_items(&self) -> bool {
        !self.items.is_empty()
    }
}

impl mz_sql::catalog::CatalogRole for Role {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> RoleId {
        self.id
    }
}

impl mz_sql::catalog::CatalogComputeInstance<'_> for ComputeInstance {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> ComputeInstanceId {
        self.id
    }

    fn exports(&self) -> &HashSet<GlobalId> {
        &self.exports
    }

    fn replica_names(&self) -> HashSet<&String> {
        self.replica_id_by_name.keys().collect::<HashSet<_>>()
    }
}

impl mz_sql::catalog::CatalogItem for CatalogEntry {
    fn name(&self) -> &QualifiedObjectName {
        self.name()
    }

    fn id(&self) -> GlobalId {
        self.id()
    }

    fn oid(&self) -> u32 {
        self.oid()
    }

    fn desc(&self, name: &FullObjectName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        self.desc(name)
    }

    fn func(&self) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        self.func()
    }

    fn source_desc(&self) -> Result<Option<&SourceDesc>, SqlCatalogError> {
        self.source_desc()
    }

    fn connection(&self) -> Result<&mz_storage::types::connections::Connection, SqlCatalogError> {
        Ok(&self.connection()?.connection)
    }

    fn create_sql(&self) -> &str {
        match self.item() {
            CatalogItem::Table(Table { create_sql, .. }) => create_sql,
            CatalogItem::Source(Source { create_sql, .. }) => create_sql,
            CatalogItem::Sink(Sink { create_sql, .. }) => create_sql,
            CatalogItem::View(View { create_sql, .. }) => create_sql,
            CatalogItem::MaterializedView(MaterializedView { create_sql, .. }) => create_sql,
            CatalogItem::Index(Index { create_sql, .. }) => create_sql,
            CatalogItem::Type(Type { create_sql, .. }) => create_sql,
            CatalogItem::Secret(Secret { create_sql, .. }) => create_sql,
            CatalogItem::Connection(Connection { create_sql, .. }) => create_sql,
            CatalogItem::Func(_) => "<builtin>",
            CatalogItem::Log(_) => "<builtin>",
        }
    }

    fn item_type(&self) -> SqlCatalogItemType {
        self.item().typ()
    }

    fn index_details(&self) -> Option<(&[MirScalarExpr], GlobalId)> {
        if let CatalogItem::Index(Index { keys, on, .. }) = self.item() {
            Some((keys, *on))
        } else {
            None
        }
    }

    fn table_details(&self) -> Option<&[Expr<Aug>]> {
        if let CatalogItem::Table(Table { defaults, .. }) = self.item() {
            Some(defaults)
        } else {
            None
        }
    }

    fn type_details(&self) -> Option<&CatalogTypeDetails<IdReference>> {
        if let CatalogItem::Type(Type { details, .. }) = self.item() {
            Some(details)
        } else {
            None
        }
    }

    fn uses(&self) -> &[GlobalId] {
        self.uses()
    }

    fn used_by(&self) -> &[GlobalId] {
        self.used_by()
    }

    fn subsources(&self) -> Vec<GlobalId> {
        match &self.item {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => {
                    ingestion.subsource_exports.keys().copied().collect()
                }
                DataSourceDesc::Source | DataSourceDesc::Introspection(_) => vec![],
            },
            CatalogItem::Table(_)
            | CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Index(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use mz_compute_client::controller::ComputeInstanceId;
    use std::collections::HashMap;
    use std::error::Error;

    use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
    use mz_ore::now::NOW_ZERO;
    use mz_repr::{GlobalId, RelationDesc, RelationType, ScalarType};
    use mz_sql::catalog::CatalogDatabase;
    use mz_sql::names::{
        ObjectQualifiers, PartialObjectName, QualifiedObjectName, ResolvedDatabaseSpecifier,
        SchemaSpecifier,
    };
    use mz_sql::DEFAULT_SCHEMA;
    use mz_sql_parser::ast::Expr;
    use mz_stash::Sqlite;

    use crate::catalog::{Catalog, CatalogItem, MaterializedView, Op, Table, SYSTEM_CONN_ID};
    use crate::session::{Session, DEFAULT_DATABASE_NAME};

    /// System sessions have an empty `search_path` so it's necessary to
    /// schema-qualify all referenced items.
    ///
    /// Dummy (and ostensibly client) sessions contain system schemas in their
    /// search paths, so do not require schema qualification on system objects such
    /// as types.
    #[tokio::test]
    async fn test_minimal_qualification() -> Result<(), anyhow::Error> {
        struct TestCase {
            input: QualifiedObjectName,
            system_output: PartialObjectName,
            normal_output: PartialObjectName,
        }

        let catalog = Catalog::open_debug_sqlite(NOW_ZERO.clone()).await?;

        let test_cases = vec![
            TestCase {
                input: QualifiedObjectName {
                    qualifiers: ObjectQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(
                            catalog.get_pg_catalog_schema_id().clone(),
                        ),
                    },
                    item: "numeric".to_string(),
                },
                system_output: PartialObjectName {
                    database: None,
                    schema: None,
                    item: "numeric".to_string(),
                },
                normal_output: PartialObjectName {
                    database: None,
                    schema: None,
                    item: "numeric".to_string(),
                },
            },
            TestCase {
                input: QualifiedObjectName {
                    qualifiers: ObjectQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(
                            catalog.get_mz_catalog_schema_id().clone(),
                        ),
                    },
                    item: "mz_array_types".to_string(),
                },
                system_output: PartialObjectName {
                    database: None,
                    schema: None,
                    item: "mz_array_types".to_string(),
                },
                normal_output: PartialObjectName {
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
        Ok(())
    }

    #[tokio::test]
    async fn test_catalog_revision() -> Result<(), anyhow::Error> {
        let mut catalog = Catalog::open_debug_sqlite(NOW_ZERO.clone()).await?;
        assert_eq!(catalog.transient_revision(), 1);
        catalog
            .transact(
                None,
                vec![Op::CreateDatabase {
                    name: "test".to_string(),
                    oid: 1,
                    public_schema_oid: 2,
                }],
                |_catalog| Ok(()),
            )
            .await
            .unwrap();
        assert_eq!(catalog.transient_revision(), 2);
        drop(catalog);

        let catalog = Catalog::open_debug_sqlite(NOW_ZERO.clone()).await?;
        assert_eq!(catalog.transient_revision(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_effective_search_path() -> Result<(), anyhow::Error> {
        let catalog = Catalog::open_debug_sqlite(NOW_ZERO.clone()).await?;
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
        session.vars_mut().set("search_path", "pg_catalog", false)?;
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
        session.vars_mut().set("search_path", "mz_catalog", false)?;
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
        session.vars_mut().set("search_path", "mz_temp", false)?;
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

        Ok(())
    }

    #[tokio::test]
    async fn test_builtin_migration() -> Result<(), Box<dyn Error>> {
        enum ItemNamespace {
            System,
            User,
        }

        enum SimplifiedItem {
            Table,
            MaterializedView { depends_on: Vec<String> },
        }

        struct SimplifiedCatalogEntry {
            name: String,
            namespace: ItemNamespace,
            item: SimplifiedItem,
        }

        impl SimplifiedCatalogEntry {
            // A lot of the fields here aren't actually used in the test so we can fill them in with dummy
            // values.
            fn to_catalog_item(
                self,
                id_mapping: &HashMap<String, GlobalId>,
            ) -> (String, ItemNamespace, CatalogItem) {
                let item = match self.item {
                    SimplifiedItem::Table => CatalogItem::Table(Table {
                        create_sql: "TODO".to_string(),
                        desc: RelationDesc::empty()
                            .with_column("a", ScalarType::Int32.nullable(true))
                            .with_key(vec![0]),
                        defaults: vec![Expr::null(); 1],
                        conn_id: None,
                        depends_on: vec![],
                    }),
                    SimplifiedItem::MaterializedView { depends_on } => {
                        let table_list = depends_on.iter().join(",");
                        let depends_on = convert_name_vec_to_id_vec(depends_on, id_mapping);
                        CatalogItem::MaterializedView(MaterializedView {
                            create_sql: format!(
                                "CREATE MATERIALIZED VIEW mv AS SELECT * FROM {table_list}"
                            ),
                            optimized_expr: OptimizedMirRelationExpr(MirRelationExpr::Constant {
                                rows: Ok(Vec::new()),
                                typ: RelationType {
                                    column_types: Vec::new(),
                                    keys: Vec::new(),
                                },
                            }),
                            desc: RelationDesc::empty()
                                .with_column("a", ScalarType::Int32.nullable(true))
                                .with_key(vec![0]),
                            depends_on,
                            compute_instance: ComputeInstanceId::User(1),
                        })
                    }
                };
                (self.name, self.namespace, item)
            }
        }

        struct BuiltinMigrationTestCase {
            test_name: &'static str,
            initial_state: Vec<SimplifiedCatalogEntry>,
            migrated_names: Vec<String>,
            expected_previous_sink_names: Vec<String>,
            expected_previous_materialized_view_names: Vec<String>,
            expected_previous_source_names: Vec<String>,
            expected_all_drop_ops: Vec<String>,
            expected_user_drop_ops: Vec<String>,
            expected_all_create_ops: Vec<String>,
            expected_user_create_ops: Vec<String>,
        }

        async fn add_item(
            catalog: &mut Catalog<Sqlite>,
            name: String,
            item: CatalogItem,
            item_namespace: ItemNamespace,
        ) -> GlobalId {
            let id = match item_namespace {
                ItemNamespace::User => catalog.allocate_user_id().await.unwrap(),
                ItemNamespace::System => catalog.allocate_system_id().await.unwrap(),
            };
            let oid = catalog.allocate_oid().unwrap();
            let database_id = catalog
                .resolve_database(DEFAULT_DATABASE_NAME)
                .unwrap()
                .id();
            let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
            let schema_spec = catalog
                .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, SYSTEM_CONN_ID)
                .unwrap()
                .id
                .clone();
            catalog
                .transact(
                    None,
                    vec![Op::CreateItem {
                        id,
                        oid,
                        name: QualifiedObjectName {
                            qualifiers: ObjectQualifiers {
                                database_spec,
                                schema_spec,
                            },
                            item: name,
                        },
                        item,
                    }],
                    |_| Ok(()),
                )
                .await
                .unwrap();
            id
        }

        fn convert_name_vec_to_id_vec(
            name_vec: Vec<String>,
            id_lookup: &HashMap<String, GlobalId>,
        ) -> Vec<GlobalId> {
            name_vec.into_iter().map(|name| id_lookup[&name]).collect()
        }

        let test_cases = vec![
            BuiltinMigrationTestCase {
                test_name: "no_migrations",
                initial_state: vec![SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                }],
                migrated_names: vec![],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec![],
                expected_previous_source_names: vec![],
                expected_all_drop_ops: vec![],
                expected_user_drop_ops: vec![],
                expected_all_create_ops: vec![],
                expected_user_create_ops: vec![],
            },
            BuiltinMigrationTestCase {
                test_name: "single_migrations",
                initial_state: vec![SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                }],
                migrated_names: vec!["s1".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec![],
                expected_previous_source_names: vec!["s1".to_string()],
                expected_all_drop_ops: vec!["s1".to_string()],
                expected_user_drop_ops: vec![],
                expected_all_create_ops: vec!["s1".to_string()],
                expected_user_create_ops: vec![],
            },
            BuiltinMigrationTestCase {
                test_name: "child_migrations",
                initial_state: vec![
                    SimplifiedCatalogEntry {
                        name: "s1".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "u1".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s1".to_string()],
                        },
                    },
                ],
                migrated_names: vec!["s1".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec!["u1".to_string()],
                expected_previous_source_names: vec!["s1".to_string()],
                expected_all_drop_ops: vec!["u1".to_string(), "s1".to_string()],
                expected_user_drop_ops: vec!["u1".to_string()],
                expected_all_create_ops: vec!["s1".to_string(), "u1".to_string()],
                expected_user_create_ops: vec!["u1".to_string()],
            },
            BuiltinMigrationTestCase {
                test_name: "multi_child_migrations",
                initial_state: vec![
                    SimplifiedCatalogEntry {
                        name: "s1".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "u1".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s1".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "u2".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s1".to_string()],
                        },
                    },
                ],
                migrated_names: vec!["s1".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec!["u1".to_string(), "u2".to_string()],
                expected_previous_source_names: vec!["s1".to_string()],
                expected_all_drop_ops: vec!["u2".to_string(), "u1".to_string(), "s1".to_string()],
                expected_user_drop_ops: vec!["u2".to_string(), "u1".to_string()],
                expected_all_create_ops: vec!["s1".to_string(), "u1".to_string(), "u2".to_string()],
                expected_user_create_ops: vec!["u1".to_string(), "u2".to_string()],
            },
            BuiltinMigrationTestCase {
                test_name: "topological_sort",
                initial_state: vec![
                    SimplifiedCatalogEntry {
                        name: "s1".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "s2".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "u1".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s2".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "u2".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s1".to_string(), "u1".to_string()],
                        },
                    },
                ],
                migrated_names: vec!["s1".to_string(), "s2".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec!["u2".to_string(), "u1".to_string()],
                expected_previous_source_names: vec!["s2".to_string(), "s1".to_string()],
                expected_all_drop_ops: vec![
                    "u1".to_string(),
                    "u2".to_string(),
                    "s2".to_string(),
                    "s1".to_string(),
                ],
                expected_user_drop_ops: vec!["u1".to_string(), "u2".to_string()],
                expected_all_create_ops: vec![
                    "s1".to_string(),
                    "s2".to_string(),
                    "u2".to_string(),
                    "u1".to_string(),
                ],
                expected_user_create_ops: vec!["u2".to_string(), "u1".to_string()],
            },
        ];

        for test_case in test_cases {
            let mut catalog = Catalog::open_debug_sqlite(NOW_ZERO.clone()).await?;

            let mut id_mapping = HashMap::new();
            for entry in test_case.initial_state {
                let (name, namespace, item) = entry.to_catalog_item(&id_mapping);
                let id = add_item(&mut catalog, name.clone(), item, namespace).await;
                id_mapping.insert(name, id);
            }

            let migrated_ids = test_case
                .migrated_names
                .into_iter()
                // We don't use the new fingerprint in this test, so we can just hard code it
                .map(|name| (id_mapping[&name], "".to_string()))
                .collect();
            let migration_metadata = catalog
                .generate_builtin_migration_metadata(migrated_ids)
                .await?;

            assert_eq!(
                migration_metadata.previous_sink_ids,
                convert_name_vec_to_id_vec(test_case.expected_previous_sink_names, &id_mapping),
                "{} test failed with wrong previous sink ids",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata.previous_materialized_view_ids,
                convert_name_vec_to_id_vec(
                    test_case.expected_previous_materialized_view_names,
                    &id_mapping
                ),
                "{} test failed with wrong previous materialized view ids",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata.previous_source_ids,
                convert_name_vec_to_id_vec(test_case.expected_previous_source_names, &id_mapping),
                "{} test failed with wrong previous source ids",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata.all_drop_ops,
                convert_name_vec_to_id_vec(test_case.expected_all_drop_ops, &id_mapping),
                "{} test failed with wrong all drop ops",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata.user_drop_ops,
                convert_name_vec_to_id_vec(test_case.expected_user_drop_ops, &id_mapping),
                "{} test failed with wrong user drop ops",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata
                    .all_create_ops
                    .into_iter()
                    .map(|(_, _, name, _)| name.item)
                    .collect::<Vec<_>>(),
                test_case.expected_all_create_ops,
                "{} test failed with wrong all create ops",
                test_case.test_name
            );
            assert_eq!(
                migration_metadata
                    .user_create_ops
                    .into_iter()
                    .map(|(_, _, name)| name)
                    .collect::<Vec<_>>(),
                test_case.expected_user_create_ops,
                "{} test failed with wrong user create ops",
                test_case.test_name
            );
        }

        Ok(())
    }
}
