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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{info, trace};

use mz_audit_log::{EventDetails, EventType, FullNameV1, ObjectType, VersionedEvent};
use mz_build_info::DUMMY_BUILD_INFO;
use mz_dataflow_types::client::controller::ComputeInstanceEvent;
use mz_dataflow_types::client::{
    ComputeInstanceId, ConcreteComputeInstanceReplicaConfig, ProcessId, ReplicaId,
};
use mz_dataflow_types::logging::LoggingConfig as DataflowLoggingConfig;
use mz_dataflow_types::sinks::{SinkConnector, SinkConnectorBuilder, SinkEnvelope};
use mz_dataflow_types::sources::{ExternalSourceConnector, SourceConnector, Timeline};
use mz_expr::{ExprHumanizer, MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_pgrepr::oid::FIRST_USER_OID;
use mz_repr::{GlobalId, RelationDesc, ScalarType};
use mz_secrets::{SecretsReader, SecretsReaderConfig};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Expr;
use mz_sql::catalog::{
    CatalogDatabase, CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem,
    CatalogItemType as SqlCatalogItemType, CatalogSchema, CatalogType, CatalogTypeDetails,
    IdReference, NameReference, SessionCatalog, TypeReference,
};
use mz_sql::names::{
    Aug, DatabaseId, FullObjectName, ObjectQualifiers, PartialObjectName, QualifiedObjectName,
    QualifiedSchemaName, RawDatabaseSpecifier, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier,
};
use mz_sql::plan::{
    ComputeInstanceIntrospectionConfig, CreateConnectorPlan, CreateIndexPlan, CreateSecretPlan,
    CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan, Params,
    Plan, PlanContext, StatementDesc,
};
use mz_sql::DEFAULT_SCHEMA;
use mz_stash::{Append, Postgres, Sqlite};
use mz_transform::Optimizer;
use uuid::Uuid;

use crate::catalog::builtin::{
    Builtin, BuiltinLog, BuiltinTable, BuiltinType, Fingerprint, BUILTINS, BUILTIN_ROLES,
    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_TEMP_SCHEMA, PG_CATALOG_SCHEMA,
};
use crate::session::{PreparedStatement, Session, DEFAULT_DATABASE_NAME};
use crate::CoordError;

mod builtin_table_updates;
mod config;
mod error;
mod migrate;

pub mod builtin;
pub mod storage;

pub use crate::catalog::builtin_table_updates::BuiltinTableUpdate;
pub use crate::catalog::config::Config;
pub use crate::catalog::error::AmbiguousRename;
pub use crate::catalog::error::Error;
pub use crate::catalog::error::ErrorKind;

pub const SYSTEM_CONN_ID: u32 = 0;
const SYSTEM_USER: &str = "mz_system";

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
    temporary_schemas: HashMap<u32, Schema>,
    compute_instances_by_id: HashMap<ComputeInstanceId, ComputeInstance>,
    compute_instances_by_name: HashMap<String, ComputeInstanceId>,
    roles: HashMap<String, Role>,
    config: mz_sql::catalog::CatalogConfig,
    oid_counter: u32,
    secrets_reader: SecretsReader,
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

    /// Encapsulates the logic for creating a source description for a source or table in the catalog.
    pub fn source_description_for(
        &self,
        id: GlobalId,
    ) -> Option<mz_dataflow_types::sources::SourceDesc> {
        let entry = self.get_entry(&id);

        match entry.item() {
            CatalogItem::Table(table) => {
                let connector = SourceConnector::Local {
                    timeline: table.timeline(),
                };
                Some(mz_dataflow_types::sources::SourceDesc {
                    connector,
                    desc: table.desc.clone(),
                })
            }
            CatalogItem::Source(source) => {
                let connector = source.connector.clone();
                Some(mz_dataflow_types::sources::SourceDesc {
                    connector,
                    desc: source.desc.clone(),
                })
            }
            _ => None,
        }
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
    pub fn log_dependencies(&self, id: GlobalId) -> Vec<GlobalId> {
        let mut out = Vec::new();
        self.log_dependencies_inner(id, &mut out);
        out
    }

    fn log_dependencies_inner(&self, id: GlobalId, out: &mut Vec<GlobalId>) {
        match self.get_entry(&id).item() {
            CatalogItem::Log(_) => out.push(id),
            CatalogItem::View(view) => {
                for id in view.depends_on.iter() {
                    self.log_dependencies_inner(*id, out);
                }
            }
            CatalogItem::Sink(sink) => self.log_dependencies_inner(sink.from, out),
            CatalogItem::Index(idx) => self.log_dependencies_inner(idx.on, out),
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connector(_) => (),
        }
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        match self.get_entry(&id).item() {
            CatalogItem::Table(_) => true,
            item @ CatalogItem::View(_) => item.uses().iter().any(|id| self.uses_tables(*id)),
            CatalogItem::Index(idx) => self.uses_tables(idx.on),
            CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::Func(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connector(_) => false,
        }
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedObjectName,
        conn_id: Option<u32>,
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
        conn_id: u32,
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

    pub fn item_exists(&self, name: &QualifiedObjectName, conn_id: u32) -> bool {
        self.try_get_entry_in_schema(name, conn_id).is_some()
    }

    fn find_available_name(
        &self,
        mut name: QualifiedObjectName,
        conn_id: u32,
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

    /// Returns all indexes on the given object and compute instance known in
    /// the catalog.
    pub fn get_indexes_on(
        &self,
        id: GlobalId,
        compute_instance: ComputeInstanceId,
    ) -> impl Iterator<Item = (GlobalId, &Index)> {
        let index_matches =
            move |idx: &Index| idx.on == id && idx.compute_instance == compute_instance;

        self.get_entry(&id)
            .used_by()
            .iter()
            .filter_map(move |uses_id| match self.get_entry(uses_id).item() {
                CatalogItem::Index(index) if index_matches(index) => Some((*uses_id, index)),
                _ => None,
            })
    }

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
            | CatalogItem::Sink(Sink {
                compute_instance, ..
            }) = item
            {
                self.compute_instances_by_id
                    .get_mut(&compute_instance)
                    .unwrap()
                    .indexes
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
            match self.entry_by_id.get_mut(&u) {
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
        if let CatalogItem::Func(_) = entry.item() {
            schema.functions.insert(entry.name.item.clone(), entry.id);
        } else {
            schema.items.insert(entry.name.item.clone(), entry.id);
        }

        self.entry_by_id.insert(entry.id, entry.clone());
    }

    fn get_database(&self, database_id: &DatabaseId) -> &Database {
        &self.database_by_id[database_id]
    }

    async fn insert_compute_instance(
        &mut self,
        id: ComputeInstanceId,
        name: String,
        introspection: Option<ComputeInstanceIntrospectionConfig>,
        introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
    ) {
        let logging = match introspection {
            None => None,
            Some(introspection) => {
                let mut active_logs = HashMap::new();
                for (log, index_id) in introspection_sources {
                    let source_name = FullObjectName {
                        database: RawDatabaseSpecifier::Ambient,
                        schema: log.schema.into(),
                        item: log.name.into(),
                    };
                    let index_name = format!("{}_{}_primary_idx", log.name, id);
                    let mut index_name = QualifiedObjectName {
                        qualifiers: ObjectQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(
                                self.get_mz_catalog_schema_id().clone(),
                            ),
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
                    let log_id = self.resolve_builtin_log(&log);
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
                            create_sql: super::coord::index_sql(
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
                    active_logs.insert(log.variant.clone(), index_id);
                }
                Some(DataflowLoggingConfig {
                    granularity_ns: introspection.granularity.as_nanos(),
                    log_logging: introspection.debugging,
                    active_logs,
                })
            }
        };

        self.compute_instances_by_id.insert(
            id,
            ComputeInstance {
                name: name.clone(),
                id,
                indexes: HashSet::new(),
                logging,
                replica_id_by_name: HashMap::new(),
                replicas_by_id: HashMap::new(),
            },
        );
        assert!(self.compute_instances_by_name.insert(name, id).is_none());
    }

    fn insert_compute_instance_replica(
        &mut self,
        on_instance: ComputeInstanceId,
        replica_name: String,
        replica_id: ReplicaId,
        config: ConcreteComputeInstanceReplicaConfig,
    ) {
        let replica = ComputeInstanceReplica {
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
    ///   * The given compute instance replica is not found. This can occur
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

    /// Gets the schema map for the database matching `database_spec`.
    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
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
        conn_id: u32,
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
        conn_id: u32,
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

    /// Optimized lookup for a builtin object
    ///
    /// Panics if the builtin object doesn't exist in the catalog
    pub fn resolve_builtin_object<T: TypeReference>(&self, builtin: &Builtin<T>) -> GlobalId {
        let schema_id = &self.ambient_schemas_by_name[builtin.schema()];
        let schema = &self.ambient_schemas_by_id[schema_id];
        schema.items[builtin.name()].clone()
    }

    /// Reports whether the item identified by `id` is considered volatile.
    ///
    /// `None` indicates that the volatility of `id` is unknown.
    pub fn is_volatile(&self, id: GlobalId) -> Volatility {
        use Volatility::*;

        let item = self.get_entry(&id).item();
        match item {
            CatalogItem::Source(source) => match &source.connector {
                SourceConnector::External { connector, .. } => match &connector {
                    ExternalSourceConnector::PubNub(_) => Volatile,
                    ExternalSourceConnector::Kinesis(_) => Volatile,
                    _ => Unknown,
                },
                SourceConnector::Local { .. } => Volatile,
            },
            CatalogItem::Log(_) => Volatile,
            CatalogItem::Index(_) | CatalogItem::View(_) | CatalogItem::Sink(_) => {
                // Volatility follows trinary logic like SQL. If even one
                // volatile dependency exists, then this item is volatile.
                // Otherwise, if a single dependency with unknown volatility
                // exists, then this item is also of unknown volatility. Only if
                // all dependencies are nonvolatile (including the trivial case
                // of no dependencies) is this item nonvolatile.
                item.uses().iter().fold(Nonvolatile, |memo, id| {
                    match (memo, self.is_volatile(*id)) {
                        (Volatile, _) | (_, Volatile) => Volatile,
                        (Unknown, _) | (_, Unknown) => Unknown,
                        (Nonvolatile, Nonvolatile) => Nonvolatile,
                    }
                })
            }
            CatalogItem::Table(_) => Volatile,
            CatalogItem::Type(_) => Unknown,
            CatalogItem::Func(_) => Unknown,
            CatalogItem::Secret(_) => Nonvolatile,
            CatalogItem::Connector(_) => Unknown,
        }
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
        conn_id: u32,
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
        conn_id: u32,
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
                Some(id) => return Ok(&self.get_entry(id)),
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
        conn_id: u32,
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
        conn_id: u32,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.functions,
            current_database,
            search_path,
            name,
            conn_id,
        )
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> String {
        serde_json::to_string(&self.database_by_id).expect("serialization cannot fail")
    }
}

#[derive(Debug)]
pub struct ConnCatalog<'a> {
    state: Cow<'a, CatalogState>,
    conn_id: u32,
    compute_instance: String,
    database: Option<DatabaseId>,
    search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    user: String,
    prepared_statements: Option<Cow<'a, HashMap<String, PreparedStatement>>>,
}

impl ConnCatalog<'_> {
    pub fn conn_id(&self) -> u32 {
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
    pub id: i64,
    #[serde(skip)]
    pub oid: u32,
}

#[derive(Debug, Serialize, Clone)]
pub struct ComputeInstance {
    pub name: String,
    pub id: ComputeInstanceId,
    pub logging: Option<DataflowLoggingConfig>,
    // does not include introspection source indexes
    pub indexes: HashSet<GlobalId>,
    pub replica_id_by_name: HashMap<String, ReplicaId>,
    pub replicas_by_id: HashMap<ReplicaId, ComputeInstanceReplica>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ComputeInstanceReplica {
    pub config: ConcreteComputeInstanceReplicaConfig,
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
    Log(&'static BuiltinLog),
    View(View),
    Sink(Sink),
    Index(Index),
    Type(Type),
    Func(Func),
    Secret(Secret),
    Connector(Connector),
}

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    pub create_sql: String,
    pub desc: RelationDesc,
    #[serde(skip)]
    pub defaults: Vec<Expr<Aug>>,
    pub conn_id: Option<u32>,
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
pub struct Source {
    pub create_sql: String,
    pub connector: SourceConnector,
    pub desc: RelationDesc,
    pub depends_on: Vec<GlobalId>,
}

impl Source {
    pub fn requires_single_materialization(&self) -> bool {
        self.connector.requires_single_materialization()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Sink {
    pub create_sql: String,
    pub from: GlobalId,
    pub connector: SinkConnectorState,
    pub envelope: SinkEnvelope,
    pub with_snapshot: bool,
    pub depends_on: Vec<GlobalId>,
    pub compute_instance: ComputeInstanceId,
}

#[derive(Debug, Clone, Serialize)]
pub enum SinkConnectorState {
    Pending(SinkConnectorBuilder),
    Ready(SinkConnector),
}

#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub create_sql: String,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub conn_id: Option<u32>,
    pub depends_on: Vec<GlobalId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<MirScalarExpr>,
    pub conn_id: Option<u32>,
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
pub struct Connector {
    pub create_sql: String,
    pub connector: mz_dataflow_types::connectors::Connector,
}

#[derive(Debug, Clone, Serialize)]
pub enum Volatility {
    Volatile,
    Nonvolatile,
    Unknown,
}

impl Volatility {
    fn as_str(&self) -> &'static str {
        match self {
            Volatility::Volatile => "volatile",
            Volatility::Nonvolatile => "nonvolatile",
            Volatility::Unknown => "unknown",
        }
    }
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
            CatalogItem::Index(_) => mz_sql::catalog::CatalogItemType::Index,
            CatalogItem::Type(_) => mz_sql::catalog::CatalogItemType::Type,
            CatalogItem::Func(_) => mz_sql::catalog::CatalogItemType::Func,
            CatalogItem::Secret(_) => mz_sql::catalog::CatalogItemType::Secret,
            CatalogItem::Connector(_) => mz_sql::catalog::CatalogItemType::Connector,
        }
    }

    pub fn desc(&self, name: &FullObjectName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        match &self {
            CatalogItem::Source(src) => Ok(Cow::Borrowed(&src.desc)),
            CatalogItem::Log(log) => Ok(Cow::Owned(log.variant.desc())),
            CatalogItem::Table(tbl) => Ok(Cow::Borrowed(&tbl.desc)),
            CatalogItem::View(view) => Ok(Cow::Borrowed(&view.desc)),
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connector(_) => Err(SqlCatalogError::InvalidDependency {
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
            _ => Err(SqlCatalogError::UnknownFunction(name.item.to_string())),
        }
    }

    pub fn source_connector(
        &self,
        name: &QualifiedObjectName,
    ) -> Result<&SourceConnector, SqlCatalogError> {
        match &self {
            CatalogItem::Source(source) => Ok(&source.connector),
            _ => Err(SqlCatalogError::UnknownSource(name.item.clone())),
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
            CatalogItem::Secret(_) => &[],
            CatalogItem::Connector(_) => &[],
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
            | CatalogItem::Secret(_)
            | CatalogItem::Connector(_) => false,
            CatalogItem::Sink(s) => match s.connector {
                SinkConnectorState::Pending(_) => true,
                SinkConnectorState::Ready(_) => false,
            },
        }
    }

    /// Returns the connection ID that this item belongs to, if this item is
    /// temporary.
    pub fn conn_id(&self) -> Option<u32> {
        match self {
            CatalogItem::View(view) => view.conn_id,
            CatalogItem::Index(index) => index.conn_id,
            CatalogItem::Table(table) => table.conn_id,
            CatalogItem::Log(_) => None,
            CatalogItem::Source(_) => None,
            CatalogItem::Sink(_) => None,
            CatalogItem::Secret(_) => None,
            CatalogItem::Type(_) => None,
            CatalogItem::Func(_) => None,
            CatalogItem::Connector(_) => None,
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
            CatalogItem::Log(i) => Ok(CatalogItem::Log(i)),
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
            CatalogItem::Connector(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Connector(i))
            }
        }
    }

    pub fn requires_single_materialization(&self) -> bool {
        if let CatalogItem::Source(Source {
            connector: SourceConnector::External { ref connector, .. },
            ..
        }) = self
        {
            connector.requires_single_materialization()
        } else {
            false
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

    pub fn connector(&self) -> Result<&Connector, SqlCatalogError> {
        match self.item() {
            CatalogItem::Connector(connector) => Ok(connector),
            _ => Err(SqlCatalogError::UnknownConnector(self.name().to_string())),
        }
    }

    /// Returns the [`mz_dataflow_types::sources::SourceConnector`] associated with
    /// this `CatalogEntry`.
    pub fn source_connector(&self) -> Result<&SourceConnector, SqlCatalogError> {
        self.item.source_connector(self.name())
    }

    /// Reports whether this catalog entry is a table.
    pub fn is_table(&self) -> bool {
        matches!(self.item(), CatalogItem::Table(_))
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
    pub fn conn_id(&self) -> Option<u32> {
        self.item.conn_id()
    }
}

struct AllocatedBuiltinSystemIds<T> {
    all_builtins: Vec<(T, GlobalId)>,
    new_builtins: Vec<(T, GlobalId)>,
    migrated_builtins: Vec<(T, GlobalId)>,
}

impl Catalog<Sqlite> {
    /// Opens a debug in-memory sqlite catalog.
    ///
    /// See [`Catalog::open_debug`].
    pub async fn open_debug_sqlite(now: NowFn) -> Result<Catalog<Sqlite>, anyhow::Error> {
        let stash = mz_stash::Sqlite::open(None)?;
        // N.B. sqlite stash is on its way out and none of the tests that actually use this deal with secrets anyway.
        // If new tests are added that do, we'll quickly fail
        let secrets_dir = Component::RootDir;
        Catalog::open_debug(stash, secrets_dir.as_ref(), now).await
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
        secrets_path: &Path,
        now: NowFn,
    ) -> Result<Catalog<Postgres>, anyhow::Error> {
        let tls = mz_postgres_util::make_tls(&tokio_postgres::Config::new()).unwrap();
        let stash = mz_stash::Postgres::new(url, schema, tls).await?;
        Catalog::open_debug(stash, secrets_path, now).await
    }
}

impl<S: Append> Catalog<S> {
    /// Opens or creates a catalog that stores data at `path`.
    ///
    /// Returns the catalog and a list of updates to builtin tables that
    /// describe the initial state of the catalog.
    pub async fn open(
        config: Config<'_, S>,
    ) -> Result<(Catalog<S>, Vec<BuiltinTableUpdate>), Error> {
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
                    cluster_id: config.storage.cluster_id(),
                    session_id: Uuid::new_v4(),
                    build_info: config.build_info,
                    timestamp_frequency: config.timestamp_frequency,
                    now: config.now.clone(),
                },
                oid_counter: FIRST_USER_OID,
                secrets_reader: config.secrets_reader,
            },
            transient_revision: 0,
            storage: Arc::new(Mutex::new(config.storage)),
        };

        catalog.create_temporary_schema(SYSTEM_CONN_ID).await?;

        let databases = catalog.storage().await.load_databases().await?;
        for (id, name) in databases {
            let oid = catalog.allocate_oid().await?;
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
            let oid = catalog.allocate_oid().await?;
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
        let builtin_roles = BUILTIN_ROLES.iter().map(|b| (b.id, b.name.to_owned()));
        for (id, name) in roles.into_iter().chain(builtin_roles) {
            let oid = catalog.allocate_oid().await?;
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
                        .get(&(builtin.schema().to_string(), builtin.name().to_string()))
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
                    let oid = catalog.allocate_oid().await?;
                    catalog
                        .state
                        .insert_item(id, oid, name.clone(), CatalogItem::Log(log));
                }

                Builtin::Table(table) => {
                    let oid = catalog.allocate_oid().await?;
                    catalog.state.insert_item(
                        id,
                        oid,
                        name.clone(),
                        CatalogItem::Table(Table {
                            create_sql: "TODO".to_string(),
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
                    let oid = catalog.allocate_oid().await?;
                    catalog.state.insert_item(id, oid, name, item);
                }

                Builtin::Type(_) => unreachable!("loaded separately"),

                Builtin::Func(func) => {
                    let oid = catalog.allocate_oid().await?;
                    catalog.state.insert_item(
                        id,
                        oid,
                        name.clone(),
                        CatalogItem::Func(Func { inner: func.inner }),
                    );
                }
            }
        }
        let new_system_id_mappings = new_builtins
            .iter()
            .map(|(builtin, id)| (builtin.schema(), builtin.name(), *id, builtin.fingerprint()))
            .collect();
        catalog
            .storage()
            .await
            .set_system_gids(new_system_id_mappings)
            .await?;

        // TODO(jkosh44) actually migrate builtins
        let migrated_system_id_mappings = migrated_builtins
            .iter()
            .map(|(builtin, id)| (builtin.schema(), builtin.name(), *id, builtin.fingerprint()))
            .collect();
        catalog
            .storage()
            .await
            .set_system_gids(migrated_system_id_mappings)
            .await?;

        let compute_instances = catalog.storage().await.load_compute_instances().await?;
        for (id, name, introspection) in compute_instances {
            let introspection_sources = if introspection.is_some() {
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
                            .map(|id| (id, 0))
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

                all_indexes
            } else {
                Vec::new()
            };
            catalog
                .state
                .insert_compute_instance(id, name, introspection, introspection_sources)
                .await;
        }

        let replicas = catalog
            .storage()
            .await
            .load_compute_instance_replicas()
            .await?;
        for (instance_id, replica_id, name, config) in replicas {
            catalog
                .state
                .insert_compute_instance_replica(instance_id, name, replica_id, config);
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

        let mut storage = catalog.storage().await;
        let mut tx = storage.transaction().await?;
        let catalog = Self::load_catalog_items(&mut tx, &catalog).await?;
        tx.commit().await?;

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
        for (db_id, db) in &catalog.state.database_by_id {
            builtin_table_updates.push(catalog.state.pack_database_update(db_id, 1));
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
        for (role_name, _role) in &catalog.state.roles {
            builtin_table_updates.push(catalog.state.pack_role_update(role_name, 1));
        }
        for (name, id) in &catalog.state.compute_instances_by_name {
            builtin_table_updates.push(catalog.state.pack_compute_instance_update(name, 1));
            let instance = &catalog.state.compute_instances_by_id[id];
            for (replica_name, _replica_id) in &instance.replica_id_by_name {
                builtin_table_updates.push(catalog.state.pack_compute_instance_replica_update(
                    *id,
                    &replica_name,
                    1,
                ));
            }
        }
        let audit_logs = storage.load_audit_log().await?;
        for event in audit_logs {
            let event = VersionedEvent::deserialize(&event).unwrap();
            builtin_table_updates.push(catalog.state.pack_audit_log_update(&event)?);
        }

        Ok((catalog, builtin_table_updates))
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
            ..
        } = self
            .allocate_system_ids(BUILTINS::types().collect(), |typ| {
                persisted_builtin_ids
                    .get(&(typ.schema.to_string(), typ.name.to_string()))
                    .cloned()
            })
            .await?;
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
            .map(|(typ, id)| (typ.schema, typ.name, *id, typ.fingerprint()))
            .collect();
        self.storage()
            .await
            .set_system_gids(new_system_id_mappings)
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
    pub async fn load_catalog_items<'a>(
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
            let oid = c.allocate_oid().await?;
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
    pub async fn open_debug(
        stash: S,
        secrets_path: &Path,
        now: NowFn,
    ) -> Result<Catalog<S>, anyhow::Error> {
        let metrics_registry = &MetricsRegistry::new();
        let storage = storage::Connection::open(stash).await?;
        let secrets_reader = SecretsReader::new(SecretsReaderConfig {
            mount_path: secrets_path.to_path_buf(),
        });
        let (catalog, _) = Catalog::open(Config {
            storage,
            unsafe_mode: true,
            build_info: &DUMMY_BUILD_INFO,
            timestamp_frequency: Duration::from_secs(1),
            now,
            skip_migrations: true,
            metrics_registry,
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
            user: session.user().into(),
            prepared_statements: Some(Cow::Borrowed(session.prepared_statements())),
        }
    }

    pub fn for_sessionless_user(&self, user: String) -> ConnCatalog {
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
        self.for_sessionless_user(SYSTEM_USER.into())
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
        F: Fn(&T) -> Option<(GlobalId, u64)>,
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
                        migrated_builtins.push((*builtin, id));
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

    pub async fn allocate_oid(&mut self) -> Result<u32, Error> {
        self.state.allocate_oid()
    }

    pub fn resolve_database(&self, database_name: &str) -> Result<&Database, SqlCatalogError> {
        self.state.resolve_database(database_name)
    }

    pub fn resolve_schema(
        &self,
        current_database: Option<&DatabaseId>,
        database_name: Option<&str>,
        schema_name: &str,
        conn_id: u32,
    ) -> Result<&Schema, SqlCatalogError> {
        self.state
            .resolve_schema(current_database, database_name, schema_name, conn_id)
    }

    pub fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
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
        conn_id: u32,
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

    /// Resolves `name` to a function [`CatalogEntry`].
    pub fn resolve_function(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialObjectName,
        conn_id: u32,
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

    pub fn state(&self) -> &CatalogState {
        &self.state
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedObjectName,
        conn_id: Option<u32>,
    ) -> FullObjectName {
        self.state.resolve_full_name(name, conn_id)
    }

    /// Returns the named catalog item, if it exists.
    pub fn try_get_entry_in_schema(
        &self,
        name: &QualifiedObjectName,
        conn_id: u32,
    ) -> Option<&CatalogEntry> {
        self.state.try_get_entry_in_schema(name, conn_id)
    }

    pub fn item_exists(&self, name: &QualifiedObjectName, conn_id: u32) -> bool {
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
        conn_id: u32,
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

    pub fn get_database(&self, id: &DatabaseId) -> &Database {
        self.state.get_database(id)
    }

    /// Creates a new schema in the `Catalog` for temporary items
    /// indicated by the TEMPORARY or TEMP keywords.
    pub async fn create_temporary_schema(&mut self, conn_id: u32) -> Result<(), Error> {
        let oid = self.allocate_oid().await?;
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

    fn item_exists_in_temp_schemas(&self, conn_id: u32, item_name: &str) -> bool {
        self.state.temporary_schemas[&conn_id]
            .items
            .contains_key(item_name)
    }

    pub fn drop_temp_item_ops(&mut self, conn_id: u32) -> Vec<Op> {
        let ids: Vec<GlobalId> = self.state.temporary_schemas[&conn_id]
            .items
            .values()
            .cloned()
            .collect();
        self.drop_items_ops(&ids)
    }

    pub fn drop_temporary_schema(&mut self, conn_id: u32) -> Result<(), Error> {
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
            ops.push(Op::DropItem(id));
        }
    }

    /// Gets GlobalIds of temporary items to be created, checks for name collisions
    /// within a connection id.
    fn temporary_ids(
        &mut self,
        ops: &[Op],
        temporary_drops: HashSet<(u32, String)>,
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
                        return Err(Error::new(ErrorKind::ItemAlreadyExists(name.item.clone())));
                    } else {
                        creating.insert((conn_id, &name.item));
                        temporary_ids.push(id.clone());
                    }
                }
            }
        }
        Ok(temporary_ids)
    }

    // TODO(mjibson): Is there a way to make this a closure to avoid explicitly
    // passing tx, session, and builtin_table_updates?
    fn add_to_audit_log(
        &self,
        session: Option<&Session>,
        tx: &mut storage::Transaction<S>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        event_type: EventType,
        object_type: ObjectType,
        event_details: EventDetails,
    ) -> Result<(), Error> {
        let session = match session {
            Some(session) => session,
            None => return Ok(()),
        };
        let user = session.user().to_string();
        let occurred_at = self.state.config.start_time.timestamp_nanos() as u64
            + self.state.config.start_instant.elapsed().as_nanos() as u64;
        let event = VersionedEvent::new(event_type, object_type, event_details, user, occurred_at);
        builtin_table_updates.push(self.state.pack_audit_log_update(&event)?);
        tx.insert_audit_log_event(event);
        Ok(())
    }

    fn should_audit_log_item(item: &CatalogItem) -> bool {
        !item.is_temporary()
            && matches!(
                item.typ(),
                SqlCatalogItemType::View
                    | SqlCatalogItemType::Source
                    | SqlCatalogItemType::Sink
                    | SqlCatalogItemType::Index
            )
    }

    fn resolve_full_name_detail(
        &self,
        name: &QualifiedObjectName,
        session: Option<&Session>,
    ) -> FullNameV1 {
        let name = self
            .state
            .resolve_full_name(name, session.map(|session| session.conn_id()));
        self.full_name_detail(&name)
    }

    fn full_name_detail(&self, name: &FullObjectName) -> FullNameV1 {
        FullNameV1 {
            database: name.database.to_string(),
            schema: name.schema.clone(),
            item: name.item.clone(),
        }
    }

    pub async fn transact<F, T>(
        &mut self,
        session: Option<&Session>,
        ops: Vec<Op>,
        f: F,
    ) -> Result<(Vec<BuiltinTableUpdate>, T), CoordError>
    where
        F: FnOnce(&CatalogState) -> Result<T, CoordError>,
    {
        trace!("transact: {:?}", ops);

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
                id: i64,
                oid: u32,
                name: String,
            },
            CreateComputeInstance {
                id: ComputeInstanceId,
                name: String,
                config: Option<ComputeInstanceIntrospectionConfig>,
                introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
            },
            CreateComputeInstanceReplica {
                id: ReplicaId,
                name: String,
                on_cluster_name: String,
                config: ConcreteComputeInstanceReplicaConfig,
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
            DropComputeInstanceReplica {
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
        }

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
        let mut actions = Vec::with_capacity(ops.len());
        let mut storage = self.storage().await;
        let mut tx = storage.transaction().await?;

        fn sql_type_to_object_type(sql_type: SqlCatalogItemType) -> ObjectType {
            match sql_type {
                SqlCatalogItemType::View => ObjectType::View,
                SqlCatalogItemType::Source => ObjectType::Source,
                SqlCatalogItemType::Sink => ObjectType::Sink,
                SqlCatalogItemType::Index => ObjectType::Index,
                _ => unreachable!(),
            }
        }

        for op in ops {
            actions.extend(match op {
                Op::CreateDatabase {
                    name,
                    oid,
                    public_schema_oid,
                } => {
                    let database_id = tx.insert_database(&name)?;
                    vec![
                        Action::CreateDatabase {
                            id: database_id,
                            oid,
                            name,
                        },
                        Action::CreateSchema {
                            id: tx.insert_schema(database_id, DEFAULT_SCHEMA)?,
                            oid: public_schema_oid,
                            database_id,
                            schema_name: DEFAULT_SCHEMA.to_string(),
                        },
                    ]
                }
                Op::CreateSchema {
                    database_id,
                    schema_name,
                    oid,
                } => {
                    if is_reserved_name(&schema_name) {
                        return Err(CoordError::Catalog(Error::new(
                            ErrorKind::ReservedSchemaName(schema_name),
                        )));
                    }
                    let database_id = match database_id {
                        ResolvedDatabaseSpecifier::Id(id) => id,
                        ResolvedDatabaseSpecifier::Ambient => {
                            return Err(CoordError::Catalog(Error::new(
                                ErrorKind::ReadOnlySystemSchema(schema_name),
                            )));
                        }
                    };
                    vec![Action::CreateSchema {
                        id: tx.insert_schema(database_id, &schema_name)?,
                        oid,
                        database_id,
                        schema_name,
                    }]
                }
                Op::CreateRole { name, oid } => {
                    if is_reserved_name(&name) {
                        return Err(CoordError::Catalog(Error::new(
                            ErrorKind::ReservedRoleName(name),
                        )));
                    }
                    vec![Action::CreateRole {
                        id: tx.insert_role(&name)?,
                        oid,
                        name,
                    }]
                }
                Op::CreateComputeInstance {
                    name,
                    config,
                    introspection_sources,
                } => {
                    if is_reserved_name(&name) {
                        return Err(CoordError::Catalog(Error::new(
                            ErrorKind::ReservedClusterName(name),
                        )));
                    }
                    self.add_to_audit_log(
                        session,
                        &mut tx,
                        &mut builtin_table_updates,
                        EventType::Create,
                        ObjectType::Cluster,
                        EventDetails::NameV1(mz_audit_log::NameV1 { name: name.clone() }),
                    )?;
                    vec![Action::CreateComputeInstance {
                        id: tx.insert_compute_instance(&name, &config, &introspection_sources)?,
                        name,
                        config,
                        introspection_sources,
                    }]
                }
                Op::CreateComputeInstanceReplica {
                    name,
                    on_cluster_name,
                    config,
                    logical_size,
                } => {
                    if is_reserved_name(&name) {
                        return Err(CoordError::Catalog(Error::new(
                            ErrorKind::ReservedReplicaName(name),
                        )));
                    }
                    if let Some(logical_size) = logical_size {
                        let details = EventDetails::CreateComputeInstanceReplicaV1(
                            mz_audit_log::CreateComputeInstanceReplicaV1 {
                                cluster_name: on_cluster_name.clone(),
                                replica_name: name.clone(),
                                logical_size,
                            },
                        );
                        self.add_to_audit_log(
                            session,
                            &mut tx,
                            &mut builtin_table_updates,
                            EventType::Create,
                            ObjectType::ClusterReplica,
                            details,
                        )?;
                    }
                    vec![Action::CreateComputeInstanceReplica {
                        id: tx.insert_compute_instance_replica(&on_cluster_name, &name, &config)?,
                        name,
                        on_cluster_name,
                        config,
                    }]
                }
                Op::CreateItem {
                    id,
                    oid,
                    name,
                    item,
                } => {
                    if item.is_temporary() {
                        if name.qualifiers.database_spec != ResolvedDatabaseSpecifier::Ambient
                            || name.qualifiers.schema_spec != SchemaSpecifier::Temporary
                        {
                            return Err(CoordError::Catalog(Error::new(
                                ErrorKind::InvalidTemporarySchema,
                            )));
                        }
                    } else {
                        if let Some(temp_id) =
                            item.uses().iter().find(|id| match self.try_get_entry(*id) {
                                Some(entry) => entry.item().is_temporary(),
                                None => temporary_ids.contains(id),
                            })
                        {
                            let temp_item = self.get_entry(temp_id);
                            return Err(CoordError::Catalog(Error::new(
                                ErrorKind::InvalidTemporaryDependency(
                                    temp_item.name().item.clone(),
                                ),
                            )));
                        }
                        if let ResolvedDatabaseSpecifier::Ambient = name.qualifiers.database_spec {
                            return Err(CoordError::Catalog(Error::new(
                                ErrorKind::ReadOnlySystemSchema(name.to_string()),
                            )));
                        }
                        let schema_id = name.qualifiers.schema_spec.clone().into();
                        let serialized_item = self.serialize_item(&item);
                        tx.insert_item(id, schema_id, &name.item, &serialized_item)?;
                    }

                    if Self::should_audit_log_item(&item) {
                        self.add_to_audit_log(
                            session,
                            &mut tx,
                            &mut builtin_table_updates,
                            EventType::Create,
                            sql_type_to_object_type(item.typ()),
                            EventDetails::FullNameV1(self.resolve_full_name_detail(&name, session)),
                        )?;
                    }

                    vec![Action::CreateItem {
                        id,
                        oid,
                        name,
                        item,
                    }]
                }
                Op::DropDatabase { id } => {
                    tx.remove_database(&id)?;
                    builtin_table_updates.push(self.state.pack_database_update(&id, -1));
                    vec![Action::DropDatabase { id }]
                }
                Op::DropSchema {
                    database_id,
                    schema_id,
                } => {
                    tx.remove_schema(&database_id, &schema_id)?;
                    builtin_table_updates.push(self.state.pack_schema_update(
                        &ResolvedDatabaseSpecifier::Id(database_id.clone()),
                        &schema_id,
                        -1,
                    ));
                    vec![Action::DropSchema {
                        database_id,
                        schema_id,
                    }]
                }
                Op::DropRole { name } => {
                    tx.remove_role(&name)?;
                    builtin_table_updates.push(self.state.pack_role_update(&name, -1));
                    vec![Action::DropRole { name }]
                }
                Op::DropComputeInstance { name } => {
                    let introspection_source_index_ids = tx.remove_compute_instance(&name)?;
                    builtin_table_updates.push(self.state.pack_compute_instance_update(&name, -1));
                    for id in &introspection_source_index_ids {
                        builtin_table_updates.extend(self.state.pack_item_update(*id, -1));
                    }
                    self.add_to_audit_log(
                        session,
                        &mut tx,
                        &mut builtin_table_updates,
                        EventType::Drop,
                        ObjectType::Cluster,
                        EventDetails::NameV1(mz_audit_log::NameV1 { name: name.clone() }),
                    )?;
                    vec![Action::DropComputeInstance {
                        name,
                        introspection_source_index_ids,
                    }]
                }
                Op::DropComputeInstanceReplica { name, compute_id } => {
                    tx.remove_compute_instance_replica(&name, compute_id)?;

                    let instance = &self.state.compute_instances_by_id[&compute_id];
                    let replica_id = instance.replica_id_by_name[&name];
                    let replica = &instance.replicas_by_id[&replica_id];
                    for process_id in replica.process_status.keys() {
                        let update = self.state.pack_compute_instance_status_update(
                            compute_id,
                            replica_id,
                            *process_id,
                            -1,
                        );
                        builtin_table_updates.push(update);
                    }

                    builtin_table_updates.push(
                        self.state
                            .pack_compute_instance_replica_update(compute_id, &name, -1),
                    );

                    let details = EventDetails::DropComputeInstanceReplicaV1(
                        mz_audit_log::DropComputeInstanceReplicaV1 {
                            cluster_name: instance.name.clone(),
                            replica_name: name.clone(),
                        },
                    );
                    self.add_to_audit_log(
                        session,
                        &mut tx,
                        &mut builtin_table_updates,
                        EventType::Drop,
                        ObjectType::ClusterReplica,
                        details,
                    )?;

                    vec![Action::DropComputeInstanceReplica { name, compute_id }]
                }
                Op::DropItem(id) => {
                    let entry = self.get_entry(&id);
                    if !entry.item().is_temporary() {
                        tx.remove_item(id)?;
                    }
                    builtin_table_updates.extend(self.state.pack_item_update(id, -1));
                    if Self::should_audit_log_item(&entry.item) {
                        self.add_to_audit_log(
                            session,
                            &mut tx,
                            &mut builtin_table_updates,
                            EventType::Drop,
                            sql_type_to_object_type(entry.item().typ()),
                            EventDetails::FullNameV1(
                                self.resolve_full_name_detail(&entry.name, session),
                            ),
                        )?;
                    }
                    vec![Action::DropItem(id)]
                }
                Op::RenameItem {
                    id,
                    to_name,
                    current_full_name,
                } => {
                    let mut actions = Vec::new();

                    let entry = self.get_entry(&id);
                    if let CatalogItem::Type(_) = entry.item() {
                        return Err(CoordError::Catalog(Error::new(ErrorKind::TypeRename(
                            current_full_name.to_string(),
                        ))));
                    }

                    let details = EventDetails::RenameItemV1(mz_audit_log::RenameItemV1 {
                        previous_name: self.full_name_detail(&current_full_name),
                        new_name: to_name.clone(),
                    });
                    if Self::should_audit_log_item(&entry.item) {
                        self.add_to_audit_log(
                            session,
                            &mut tx,
                            &mut builtin_table_updates,
                            EventType::Rename,
                            sql_type_to_object_type(entry.item().typ()),
                            details,
                        )?;
                    }

                    let mut to_full_name = current_full_name.clone();
                    to_full_name.item = to_name.clone();

                    let mut to_qualified_name = entry.name().clone();
                    to_qualified_name.item = to_name;

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
                                depender: self
                                    .resolve_full_name(&entry.name, entry.conn_id())
                                    .to_string(),
                                dependee: self
                                    .resolve_full_name(&entry.name, entry.conn_id())
                                    .to_string(),
                                message: e,
                            }))
                        })?;
                    let serialized_item = self.serialize_item(&item);

                    for id in entry.used_by() {
                        let dependent_item = self.get_entry(id);
                        let to_item = dependent_item
                            .item
                            .rename_item_refs(
                                current_full_name.clone(),
                                to_full_name.item.clone(),
                                false,
                            )
                            .map_err(|e| {
                                Error::new(ErrorKind::from(AmbiguousRename {
                                    depender: self
                                        .resolve_full_name(
                                            &dependent_item.name,
                                            dependent_item.conn_id(),
                                        )
                                        .to_string(),
                                    dependee: self
                                        .resolve_full_name(&entry.name, entry.conn_id())
                                        .to_string(),
                                    message: e,
                                }))
                            })?;

                        if !item.is_temporary() {
                            let serialized_item = self.serialize_item(&to_item);
                            tx.update_item(*id, &dependent_item.name().item, &serialized_item)?;
                        }
                        builtin_table_updates.extend(self.state.pack_item_update(*id, -1));

                        actions.push(Action::UpdateItem {
                            id: id.clone(),
                            to_name: dependent_item.name().clone(),
                            to_item,
                        });
                    }
                    if !item.is_temporary() {
                        tx.update_item(id, &to_full_name.item, &serialized_item)?;
                    }
                    builtin_table_updates.extend(self.state.pack_item_update(id, -1));
                    actions.push(Action::UpdateItem {
                        id,
                        to_name: to_qualified_name,
                        to_item: item,
                    });
                    actions
                }
                Op::UpdateComputeInstanceStatus { event } => {
                    // When we receive the first status update for a given
                    // replica process, there is no entry in the builtin table
                    // yet, so we must make sure to not try to delete one.
                    let status_known = self
                        .state
                        .try_get_compute_instance_status(
                            event.instance_id,
                            event.replica_id,
                            event.process_id,
                        )
                        .is_some();
                    if status_known {
                        let update = self.state.pack_compute_instance_status_update(
                            event.instance_id,
                            event.replica_id,
                            event.process_id,
                            -1,
                        );
                        builtin_table_updates.push(update);
                    }

                    vec![Action::UpdateComputeInstanceStatus { event }]
                }
            });
        }

        // Prepare a candidate catalog state.
        let mut state = self.state.clone();

        for action in actions {
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
                    state.database_by_name.insert(name.clone(), id.clone());
                    builtin_table_updates.push(state.pack_database_update(&id, 1));
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
                    db.schemas_by_name.insert(schema_name.clone(), id.clone());
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
                    builtin_table_updates.push(state.pack_role_update(&name, 1));
                }

                Action::CreateComputeInstance {
                    id,
                    name,
                    config,
                    introspection_sources,
                } => {
                    info!("create cluster {}", name);
                    let introspection_source_index_ids: Vec<GlobalId> =
                        introspection_sources.iter().map(|(_, id)| *id).collect();
                    state
                        .insert_compute_instance(id, name.clone(), config, introspection_sources)
                        .await;
                    builtin_table_updates.push(state.pack_compute_instance_update(&name, 1));
                    for id in introspection_source_index_ids {
                        builtin_table_updates.extend(state.pack_item_update(id, 1));
                    }
                }

                Action::CreateComputeInstanceReplica {
                    id,
                    name,
                    on_cluster_name,
                    config,
                } => {
                    info!("create replica {} of instance {}", name, on_cluster_name);
                    let compute_instance_id = state.compute_instances_by_name[&on_cluster_name];
                    state.insert_compute_instance_replica(
                        compute_instance_id,
                        name.clone(),
                        id,
                        config,
                    );
                    builtin_table_updates.push(state.pack_compute_instance_replica_update(
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
                        state
                            .entry_by_id
                            .remove(&id)
                            .expect("introspection source index does not exist");
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
                        instance.indexes.is_empty() && instance.replicas_by_id.is_empty(),
                        "not all items dropped before compute instance"
                    );
                }

                Action::DropComputeInstanceReplica { name, compute_id } => {
                    let instance = state
                        .compute_instances_by_id
                        .get_mut(&compute_id)
                        .expect("can only drop replicas from known instances");
                    let replica_id = instance.replica_id_by_name.remove(&name).unwrap();
                    assert!(instance.replicas_by_id.remove(&replica_id).is_some());
                    assert!(instance.replica_id_by_name.len() == instance.replicas_by_id.len());
                }

                Action::DropItem(id) => {
                    let metadata = state.entry_by_id.remove(&id).unwrap();
                    if !metadata.item.is_placeholder() {
                        info!(
                            "drop {} {} ({})",
                            metadata.item_type(),
                            state.resolve_full_name(&metadata.name, metadata.conn_id()),
                            id
                        );
                    }
                    for u in metadata.uses() {
                        if let Some(dep_metadata) = state.entry_by_id.get_mut(&u) {
                            dep_metadata.used_by.retain(|u| *u != metadata.id)
                        }
                    }

                    let conn_id = metadata.item.conn_id().unwrap_or(SYSTEM_CONN_ID);
                    let schema = state.get_schema_mut(
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
                    | CatalogItem::Sink(Sink {
                        compute_instance, ..
                    }) = metadata.item
                    {
                        assert!(
                            state
                                .compute_instances_by_id
                                .get_mut(&compute_instance)
                                .unwrap()
                                .indexes
                                .remove(&id),
                            "catalog out of sync"
                        );
                    };
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
                    state.entry_by_id.insert(id, new_entry.clone());
                    builtin_table_updates.extend(state.pack_item_update(id, 1));
                }

                Action::UpdateComputeInstanceStatus { event } => {
                    // It is possible that we receive a status update for a
                    // replica that has already been dropped from the catalog.
                    // In this case, `try_insert_compute_instance_status`
                    // returns `false` and we ignore the event.
                    if state.try_insert_compute_instance_status(event.clone()) {
                        let update = state.pack_compute_instance_status_update(
                            event.instance_id,
                            event.replica_id,
                            event.process_id,
                            1,
                        );
                        builtin_table_updates.push(update);
                    }
                }
            }
        }

        let result = f(&state)?;

        // The user closure was successful, apply the updates.
        tx.commit().await?;
        // Dropping here keeps the mutable borrow on self, preventing us accidentally
        // mutating anything until after f is executed.
        drop(storage);
        self.state = state;
        self.transient_revision += 1;

        Ok((builtin_table_updates, result))
    }

    fn serialize_item(&self, item: &CatalogItem) -> Vec<u8> {
        let item = match item {
            CatalogItem::Table(table) => SerializedCatalogItem::V1 {
                create_sql: table.create_sql.clone(),
                eval_env: None,
            },
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => SerializedCatalogItem::V1 {
                create_sql: source.create_sql.clone(),
                eval_env: None,
            },
            CatalogItem::View(view) => SerializedCatalogItem::V1 {
                create_sql: view.create_sql.clone(),
                eval_env: None,
            },
            CatalogItem::Index(index) => SerializedCatalogItem::V1 {
                create_sql: index.create_sql.clone(),
                eval_env: None,
            },
            CatalogItem::Sink(sink) => SerializedCatalogItem::V1 {
                create_sql: sink.create_sql.clone(),
                eval_env: None,
            },
            CatalogItem::Type(typ) => SerializedCatalogItem::V1 {
                create_sql: typ.create_sql.clone(),
                eval_env: None,
            },
            CatalogItem::Secret(secret) => SerializedCatalogItem::V1 {
                create_sql: secret.create_sql.clone(),
                eval_env: None,
            },
            CatalogItem::Connector(connector) => SerializedCatalogItem::V1 {
                create_sql: connector.create_sql.clone(),
                eval_env: None,
            },
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
        };
        serde_json::to_vec(&item).expect("catalog serialization cannot fail")
    }

    fn deserialize_item(&self, bytes: Vec<u8>) -> Result<CatalogItem, anyhow::Error> {
        let SerializedCatalogItem::V1 {
            create_sql,
            eval_env: _,
        } = serde_json::from_slice(&bytes)?;
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
            Plan::CreateSource(CreateSourcePlan { source, .. }) => CatalogItem::Source(Source {
                create_sql: source.create_sql,
                connector: source.connector,
                desc: source.desc,
                depends_on,
            }),
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
                ..
            }) => CatalogItem::Sink(Sink {
                create_sql: sink.create_sql,
                from: sink.from,
                connector: SinkConnectorState::Pending(sink.connector_builder),
                envelope: sink.envelope,
                with_snapshot,
                depends_on,
                compute_instance: sink.compute_instance,
            }),
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
            Plan::CreateConnector(CreateConnectorPlan { connector, .. }) => {
                CatalogItem::Connector(Connector {
                    create_sql: connector.create_sql,
                    connector: connector.connector,
                })
            }
            _ => bail!("catalog entry generated inappropriate plan"),
        })
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        self.state.uses_tables(id)
    }

    /// Return the names of all log sources the given object depends on.
    pub fn log_dependencies(&self, id: GlobalId) -> Vec<GlobalId> {
        self.state.log_dependencies(id)
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

    pub fn compute_instances(&self) -> impl Iterator<Item = &ComputeInstance> {
        self.state.compute_instances_by_id.values()
    }

    pub async fn allocate_introspection_source_indexes(
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
}

fn is_reserved_name(name: &str) -> bool {
    name.starts_with("mz_") || name.starts_with("pg_")
}

#[derive(Debug, Clone)]
pub enum Op {
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
        config: Option<ComputeInstanceIntrospectionConfig>,
        introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
    },
    CreateComputeInstanceReplica {
        name: String,
        config: ConcreteComputeInstanceReplicaConfig,
        on_cluster_name: String,
        logical_size: Option<String>,
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
    DropComputeInstanceReplica {
        name: String,
        compute_id: ComputeInstanceId,
    },
    /// Unconditionally removes the identified items. It is required that the
    /// IDs come from the output of `plan_remove`; otherwise consistency rules
    /// may be violated.
    DropItem(GlobalId),
    RenameItem {
        id: GlobalId,
        current_full_name: FullObjectName,
        to_name: String,
    },
    UpdateComputeInstanceStatus {
        event: ComputeInstanceEvent,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SerializedCatalogItem {
    V1 {
        create_sql: String,
        // The name "eval_env" is historical.
        eval_env: Option<SerializedPlanContext>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedPlanContext {
    pub logical_time: Option<u64>,
    pub wall_time: Option<DateTime<Utc>>,
}

impl From<SerializedPlanContext> for PlanContext {
    fn from(cx: SerializedPlanContext) -> PlanContext {
        PlanContext {
            wall_time: cx.wall_time.unwrap_or_else(|| Utc.timestamp(0, 0)),
            qgm_optimizations: false,
        }
    }
}

impl From<PlanContext> for SerializedPlanContext {
    fn from(cx: PlanContext) -> SerializedPlanContext {
        SerializedPlanContext {
            logical_time: None,
            wall_time: Some(cx.wall_time),
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
        &self.user
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

    fn find_available_name(&self, name: QualifiedObjectName) -> QualifiedObjectName {
        self.state.find_available_name(name, self.conn_id)
    }

    fn resolve_full_name(&self, name: &QualifiedObjectName) -> FullObjectName {
        self.state.resolve_full_name(name, Some(self.conn_id))
    }

    fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        &self.state.config()
    }

    fn now(&self) -> EpochMillis {
        (self.state.config().now)()
    }

    fn secrets_reader(&self) -> &SecretsReader {
        &self.state.secrets_reader
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

    fn id(&self) -> i64 {
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

    fn indexes(&self) -> &HashSet<GlobalId> {
        &self.indexes
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
        Ok(self.desc(name)?)
    }

    fn func(&self) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        Ok(self.func()?)
    }

    fn source_connector(&self) -> Result<&SourceConnector, SqlCatalogError> {
        Ok(self.source_connector()?)
    }

    fn connector(&self) -> Result<&mz_dataflow_types::connectors::Connector, SqlCatalogError> {
        Ok(&self.connector()?.connector)
    }

    fn create_sql(&self) -> &str {
        match self.item() {
            CatalogItem::Table(Table { create_sql, .. }) => create_sql,
            CatalogItem::Source(Source { create_sql, .. }) => create_sql,
            CatalogItem::Sink(Sink { create_sql, .. }) => create_sql,
            CatalogItem::View(View { create_sql, .. }) => create_sql,
            CatalogItem::Index(Index { create_sql, .. }) => create_sql,
            CatalogItem::Type(Type { create_sql, .. }) => create_sql,
            CatalogItem::Secret(Secret { create_sql, .. }) => create_sql,
            CatalogItem::Connector(Connector { create_sql, .. }) => create_sql,
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
}

#[cfg(test)]
mod tests {
    use mz_ore::now::NOW_ZERO;
    use mz_sql::names::{
        ObjectQualifiers, PartialObjectName, QualifiedObjectName, ResolvedDatabaseSpecifier,
        SchemaSpecifier,
    };

    use crate::catalog::{Catalog, Op};
    use crate::session::Session;

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
}
