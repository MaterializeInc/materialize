// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistent metadata storage for the coordinator.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{info, trace};

use mz_build_info::DUMMY_BUILD_INFO;
use mz_dataflow_types::client::{ComputeInstanceId, InstanceConfig};
use mz_dataflow_types::logging::{LogVariant, LoggingConfig as DataflowLoggingConfig};
use mz_dataflow_types::sinks::{SinkConnector, SinkConnectorBuilder, SinkEnvelope};
use mz_dataflow_types::sources::persistence::{EnvelopePersistDesc, SourcePersistDesc};
use mz_dataflow_types::sources::{
    AwsExternalId, ExternalSourceConnector, SourceConnector, Timeline,
};
use mz_expr::{ExprHumanizer, GlobalId, MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_pgrepr::oid::FIRST_USER_OID;
use mz_repr::{RelationDesc, ScalarType};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Expr;
use mz_sql::catalog::{
    CatalogDatabase, CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem,
    CatalogItemType as SqlCatalogItemType, CatalogSchema, CatalogTypeDetails, SessionCatalog,
};
use mz_sql::names::{
    Aug, DatabaseId, FullObjectName, ObjectQualifiers, PartialObjectName, QualifiedObjectName,
    QualifiedSchemaName, RawDatabaseSpecifier, ResolvedDatabaseSpecifier, SchemaId,
    SchemaSpecifier,
};
use mz_sql::plan::{
    ComputeInstanceConfig, ComputeInstanceIntrospectionConfig, CreateIndexPlan, CreateSecretPlan,
    CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan, Params,
    Plan, PlanContext, StatementDesc,
};
use mz_sql::DEFAULT_SCHEMA;
use mz_transform::Optimizer;
use uuid::Uuid;

use crate::catalog::builtin::{
    Builtin, BuiltinInner, BuiltinLog, BuiltinTable, BuiltinType, BUILTINS, BUILTIN_ROLES,
    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_TEMP_SCHEMA, PG_CATALOG_SCHEMA,
};
use crate::persistcfg::PersistConfig;
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
/// [`Catalog::resolve`] method.
///
/// The catalog also maintains special "ambient schemas": virtual schemas,
/// implicitly present in all databases, that house various system views.
/// The big examples of ambient schemas are `pg_catalog` and `mz_catalog`.
#[derive(Debug, Clone)]
pub struct Catalog {
    state: CatalogState,
    storage: Arc<Mutex<storage::Connection>>,
    transient_revision: u64,
}

#[derive(Debug, Clone)]
pub struct CatalogState {
    database_by_name: BTreeMap<String, DatabaseId>,
    database_by_id: BTreeMap<DatabaseId, Database>,
    entry_by_id: BTreeMap<GlobalId, CatalogEntry>,
    entry_by_oid: HashMap<u32, GlobalId>,
    ambient_schemas_by_name: BTreeMap<String, SchemaId>,
    ambient_schemas_by_id: BTreeMap<SchemaId, Schema>,
    temporary_schemas: HashMap<u32, Schema>,
    compute_instances_by_id: HashMap<ComputeInstanceId, ComputeInstance>,
    compute_instances_by_name: HashMap<String, ComputeInstanceId>,
    roles: HashMap<String, Role>,
    config: mz_sql::catalog::CatalogConfig,
    oid_counter: u32,
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
                    persisted_name: table.persist_name.clone(),
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

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        match self.get_entry(&id).item() {
            CatalogItem::Table(_) => true,
            item @ CatalogItem::View(_) => item.uses().iter().any(|id| self.uses_tables(*id)),
            CatalogItem::Index(idx) => self.uses_tables(idx.on),
            CatalogItem::Source(_)
            | CatalogItem::Func(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_) => false,
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

    pub fn get_entry_by_oid(&self, oid: &u32) -> &CatalogEntry {
        let id = &self.entry_by_oid[oid];
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

    /// Returns all indexes on this object known in the catalog.
    pub fn get_indexes_on(&self, id: GlobalId) -> impl Iterator<Item = (GlobalId, &Index)> {
        self.get_entry(&id)
            .used_by()
            .iter()
            .filter_map(move |uses_id| match self.get_entry(uses_id).item() {
                CatalogItem::Index(index) if index.on == id => Some((*uses_id, index)),
                _ => None,
            })
    }

    pub fn get_compute_instance(&self, id: ComputeInstanceId) -> &ComputeInstance {
        &self.compute_instances_by_id[&id]
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn insert_item(
        &mut self,
        id: GlobalId,
        oid: u32,
        name: QualifiedObjectName,
        item: CatalogItem,
    ) {
        if !id.is_system() && !item.is_placeholder() {
            info!("create {} {} ({})", item.typ(), name, id);
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

        self.entry_by_oid.insert(oid, entry.id);
        self.entry_by_id.insert(entry.id, entry.clone());
    }

    fn get_database(&self, database_id: &DatabaseId) -> &Database {
        &self.database_by_id[database_id]
    }

    fn insert_compute_instance(
        &mut self,
        id: ComputeInstanceId,
        name: String,
        config: ComputeInstanceConfig,
        local_compute_introspection: Option<ComputeInstanceIntrospectionConfig>,
        introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
    ) {
        let (config, introspection) = match config {
            ComputeInstanceConfig::Local => (InstanceConfig::Local, local_compute_introspection),
            ComputeInstanceConfig::Remote {
                replicas,
                introspection,
            } => (InstanceConfig::Remote { replicas }, introspection),
            ComputeInstanceConfig::Managed {
                size,
                introspection,
            } => (InstanceConfig::Managed { size }, introspection),
        };
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
                            enabled: true,
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
                config,
                id,
                indexes: HashSet::new(),
                logging,
            },
        );
        self.compute_instances_by_name.insert(name, id);
    }

    /// Gets the schema map for the database matching `database_spec`.
    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
    ) -> Option<&Schema> {
        match database_spec {
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
        }
    }

    fn get_schema(
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
        self.resolve_builtin_object(&BuiltinInner::Table(builtin))
    }

    /// Optimized lookup for a builtin log
    ///
    /// Panics if the builtin log doesn't exist in the catalog
    pub fn resolve_builtin_log(&self, builtin: &'static BuiltinLog) -> GlobalId {
        self.resolve_builtin_object(&BuiltinInner::Log(builtin))
    }

    /// Optimized lookup for a builtin object
    ///
    /// Panics if the builtin object doesn't exist in the catalog
    pub fn resolve_builtin_object(&self, builtin: &BuiltinInner) -> GlobalId {
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
            // TODO: Persisted tables should be Nonvolatile.
            CatalogItem::Table(_) => Volatile,
            CatalogItem::Type(_) => Unknown,
            CatalogItem::Func(_) => Unknown,
            CatalogItem::Secret(_) => Nonvolatile,
        }
    }

    pub fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        &self.config
    }
}

#[derive(Debug)]
pub struct ConnCatalog<'a> {
    catalog: &'a Catalog,
    conn_id: u32,
    compute_instance: String,
    database: Option<DatabaseId>,
    search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    user: String,
    prepared_statements: Option<&'a HashMap<String, PreparedStatement>>,
}

impl ConnCatalog<'_> {
    pub fn conn_id(&self) -> u32 {
        self.conn_id
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
    pub config: InstanceConfig,
    pub logging: Option<DataflowLoggingConfig>,
    pub indexes: HashSet<GlobalId>,
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
    View(View),
    Sink(Sink),
    Index(Index),
    Type(Type),
    Func(Func),
    Secret(Secret),
}

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    pub create_sql: String,
    pub desc: RelationDesc,
    #[serde(skip)]
    pub defaults: Vec<Expr<Aug>>,
    pub conn_id: Option<u32>,
    pub depends_on: Vec<GlobalId>,
    pub persist_name: Option<String>,
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
    pub persist_details: Option<SerializedSourcePersistDetails>,
    pub desc: RelationDesc,
}

impl Source {
    pub fn requires_single_materialization(&self) -> bool {
        // Persisted sources must only be persisted once because we use the source ID to derive the
        // names of the persistent collections that back it. If we allowed multiple instances,
        // those would clash when trying to write to those collections.
        self.connector.requires_single_materialization() || self.persist_details.is_some()
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
    // TODO(benesch): we'd ideally delete this field and instead derive it from
    // whether the index is present in the compute controller or not. But it is
    // presently hard to have an `enabled` field in `mz_indexes` if this field
    // does not exist in the catalog.
    pub enabled: bool,
    pub compute_instance: ComputeInstanceId,
}

#[derive(Debug, Clone, Serialize)]
pub struct Type {
    pub create_sql: String,
    #[serde(skip)]
    pub details: CatalogTypeDetails,
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
    fn typ(&self) -> mz_sql::catalog::CatalogItemType {
        match self {
            CatalogItem::Table(_) => mz_sql::catalog::CatalogItemType::Table,
            CatalogItem::Source(_) => mz_sql::catalog::CatalogItemType::Source,
            CatalogItem::Sink(_) => mz_sql::catalog::CatalogItemType::Sink,
            CatalogItem::View(_) => mz_sql::catalog::CatalogItemType::View,
            CatalogItem::Index(_) => mz_sql::catalog::CatalogItemType::Index,
            CatalogItem::Type(_) => mz_sql::catalog::CatalogItemType::Type,
            CatalogItem::Func(_) => mz_sql::catalog::CatalogItemType::Func,
            CatalogItem::Secret(_) => mz_sql::catalog::CatalogItemType::Secret,
        }
    }

    pub fn desc(&self, name: &FullObjectName) -> Result<&RelationDesc, SqlCatalogError> {
        match &self {
            CatalogItem::Source(src) => Ok(&src.desc),
            CatalogItem::Table(tbl) => Ok(&tbl.desc),
            CatalogItem::View(view) => Ok(&view.desc),
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_) => Err(SqlCatalogError::InvalidDependency {
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
            CatalogItem::Source(_) => &[],
            CatalogItem::Table(table) => &table.depends_on,
            CatalogItem::Type(typ) => &typ.depends_on,
            CatalogItem::View(view) => &view.depends_on,
            CatalogItem::Secret(_) => &[],
        }
    }

    /// Indicates whether this item is a placeholder for a future item
    /// or if it's actually a real item.
    pub fn is_placeholder(&self) -> bool {
        match self {
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Source(_)
            | CatalogItem::Table(_)
            | CatalogItem::Type(_)
            | CatalogItem::View(_)
            | CatalogItem::Secret(_) => false,
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
            CatalogItem::Source(_) => None,
            CatalogItem::Sink(_) => None,
            CatalogItem::Secret(_) => None,
            CatalogItem::Type(_) => None,
            CatalogItem::Func(_) => None,
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
    pub fn desc(&self, name: &FullObjectName) -> Result<&RelationDesc, SqlCatalogError> {
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

impl Catalog {
    /// Opens or creates a catalog that stores data at `path`.
    ///
    /// Returns the catalog and a list of updates to builtin tables that
    /// describe the initial state of the catalog.
    pub async fn open(
        mut config: Config<'_>,
    ) -> Result<
        (
            Catalog,
            Vec<BuiltinTableUpdate>,
            BTreeMap<GlobalId, LogVariant>,
        ),
        Error,
    > {
        let mut catalog = Catalog {
            state: CatalogState {
                database_by_name: BTreeMap::new(),
                database_by_id: BTreeMap::new(),
                entry_by_id: BTreeMap::new(),
                entry_by_oid: HashMap::new(),
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
                    experimental_mode: config.storage.experimental_mode(),
                    safe_mode: config.safe_mode,
                    cluster_id: config.storage.cluster_id(),
                    session_id: Uuid::new_v4(),
                    build_info: config.build_info,
                    aws_external_id: config.aws_external_id.clone(),
                    timestamp_frequency: config.timestamp_frequency,
                    now: config.now.clone(),
                    disable_user_indexes: config.disable_user_indexes,
                },
                oid_counter: FIRST_USER_OID,
            },
            transient_revision: 0,
            storage: Arc::new(Mutex::new(config.storage)),
        };

        catalog.create_temporary_schema(SYSTEM_CONN_ID)?;

        let databases = catalog.storage().load_databases()?;
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

        let schemas = catalog.storage().load_schemas()?;
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

        let roles = catalog.storage().load_roles()?;
        let builtin_roles = BUILTIN_ROLES.iter().map(|b| (b.id, b.name.to_owned()));
        for (id, name) in roles.into_iter().chain(builtin_roles) {
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

        let pg_catalog_schema_id = catalog.state.get_pg_catalog_schema_id().clone();

        let mut builtin_logs = BTreeMap::new();
        let builtin_ids = catalog.storage().load_system_gids()?;
        let new_system_id_amount = BUILTINS
            .iter()
            .filter(|builtin| {
                !matches!(builtin, BuiltinInner::Type(_))
                    && !builtin_ids
                        .contains_key(&(builtin.schema().to_string(), builtin.name().to_string()))
            })
            .count();
        let mut new_system_ids = catalog
            .allocate_system_ids(
                new_system_id_amount
                    .try_into()
                    .expect("builtins should fit into u64"),
            )?
            .into_iter();
        let mut new_system_id_mappings = Vec::new();

        for builtin_inner in BUILTINS.iter() {
            let id = match builtin_inner {
                // Builtin Types are currently statically assigned Ids
                BuiltinInner::Type(BuiltinType { id, .. }) => *id,
                _ => match builtin_ids.get(&(
                    builtin_inner.schema().to_string(),
                    builtin_inner.name().to_string(),
                )) {
                    // TODO(jkosh44) These items may need to undergo schema migrations
                    // Builtin has already been assigned a global Id
                    Some(id) => *id,
                    // New Builtin which needs a global Id
                    None => {
                        let id = new_system_ids.next().expect("should be enough system ids");
                        new_system_id_mappings.push((
                            builtin_inner.schema(),
                            builtin_inner.name(),
                            id,
                        ));
                        id
                    }
                },
            };
            let builtin = Builtin {
                inner: builtin_inner,
                id,
            };
            let schema_id = catalog.state.ambient_schemas_by_name[builtin.schema()];
            let name = QualifiedObjectName {
                qualifiers: ObjectQualifiers {
                    database_spec: ResolvedDatabaseSpecifier::Ambient,
                    schema_spec: SchemaSpecifier::Id(schema_id),
                },
                item: builtin.name().into(),
            };
            let full_name = FullObjectName {
                database: RawDatabaseSpecifier::Ambient,
                schema: builtin.schema().into(),
                item: builtin.name().into(),
            };
            match builtin.inner {
                BuiltinInner::Log(log) => {
                    let oid = catalog.allocate_oid()?;
                    catalog.state.insert_item(
                        builtin.id(),
                        oid,
                        name.clone(),
                        CatalogItem::Source(Source {
                            create_sql: "TODO".to_string(),
                            connector: mz_dataflow_types::sources::SourceConnector::Local {
                                timeline: Timeline::EpochMilliseconds,
                                persisted_name: None,
                            },
                            persist_details: None,
                            desc: log.variant.desc(),
                        }),
                    );
                    builtin_logs.insert(builtin.id(), log.variant.clone());
                }

                BuiltinInner::Table(table) => {
                    let oid = catalog.allocate_oid()?;
                    let persist_name = if table.persistent {
                        config
                            .persister
                            .new_table_persist_name(builtin.id(), &full_name.to_string())
                    } else {
                        None
                    };
                    catalog.state.insert_item(
                        builtin.id(),
                        oid,
                        name.clone(),
                        CatalogItem::Table(Table {
                            create_sql: "TODO".to_string(),
                            desc: table.desc.clone(),
                            defaults: vec![Expr::null(); table.desc.arity()],
                            conn_id: None,
                            depends_on: vec![],
                            persist_name,
                        }),
                    );
                }

                BuiltinInner::View(view) => {
                    let table_persist_name = None;
                    let source_persist_details = None;
                    let item = catalog
                        .parse_item(
                            builtin.id(),
                            view.sql.into(),
                            None,
                            table_persist_name,
                            source_persist_details,
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
                    catalog.state.insert_item(builtin.id(), oid, name, item);
                }

                BuiltinInner::Type(typ) => {
                    catalog.state.insert_item(
                        builtin.id(),
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

                BuiltinInner::Func(func) => {
                    let oid = catalog.allocate_oid()?;
                    catalog.state.insert_item(
                        builtin.id(),
                        oid,
                        name.clone(),
                        CatalogItem::Func(Func { inner: func.inner }),
                    );
                }
            }
        }
        catalog.storage().set_system_gids(new_system_id_mappings)?;

        let compute_instances = catalog.storage().load_compute_instances()?;
        for (id, name, conf) in compute_instances {
            // Only one virtual compute instance can configure logging or
            // else the virtual compute host will panic. We arbitrarily
            // choose to attach the virtual compute host's logging to the
            // first virtual cluster we see. If the user drops this cluster
            // then they lose access to the logs. This is a bit silly, but
            // it preserves the existing behavior of the binary without
            // inventing a bunch of new concepts just to support virtual
            // clusters.
            let local_logging = config.local_compute_introspection.take();
            let introspection_sources = if conf.introspection().is_some()
                || matches!(conf, ComputeInstanceConfig::Local if local_logging.is_some())
            {
                let introspection_source_index_gids =
                    catalog.storage().load_introspection_source_index_gids(id)?;
                // Previously allocated introspection source indexes
                let existing_indexes = BUILTINS
                    .logs()
                    .filter(|log| introspection_source_index_gids.contains_key(log.name))
                    .map(|log| (log, introspection_source_index_gids[log.name]));

                // New introspection source indexes that need GlobalIds
                let new_indexes: Vec<_> = BUILTINS
                    .logs()
                    .filter(|log| !introspection_source_index_gids.contains_key(log.name))
                    .collect();
                let global_ids = catalog.allocate_system_ids(
                    new_indexes
                        .len()
                        .try_into()
                        .expect("builtin logs should fit into u64"),
                )?;
                let new_indexes = new_indexes.into_iter().zip(global_ids.into_iter());

                catalog.storage().set_introspection_source_index_gids(
                    new_indexes
                        .clone()
                        .map(|(log, index_id)| (id, log.name, index_id))
                        .collect(),
                )?;

                existing_indexes.chain(new_indexes).collect()
            } else {
                Vec::new()
            };
            catalog.state.insert_compute_instance(
                id,
                name,
                conf,
                local_logging,
                introspection_sources,
            );
        }

        if !config.skip_migrations {
            let last_seen_version = catalog.storage().get_catalog_content_version()?;
            crate::catalog::migrate::migrate(&mut catalog).map_err(|e| {
                Error::new(ErrorKind::FailedMigration {
                    last_seen_version,
                    this_version: catalog.config().build_info.version,
                    cause: e.to_string(),
                })
            })?;
            catalog
                .storage()
                .set_catalog_content_version(catalog.config().build_info.version)?;
        }

        let mut storage = catalog.storage();
        let mut tx = storage.transaction()?;
        let catalog = Self::load_catalog_items(&mut tx, &catalog)?;
        tx.commit()?;

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
        for (name, _id) in &catalog.state.compute_instances_by_name {
            builtin_table_updates.push(catalog.state.pack_compute_instance_update(name, 1));
        }

        Ok((catalog, builtin_table_updates, builtin_logs))
    }

    /// Retuns the catalog's transient revision, which starts at 1 and is
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
    pub fn load_catalog_items(
        tx: &mut storage::Transaction,
        c: &Catalog,
    ) -> Result<Catalog, Error> {
        let mut c = c.clone();
        let items = tx.load_items()?;
        for (id, name, def) in items {
            // TODO(benesch): a better way of detecting when a view has depended
            // upon a non-existent logging view. This is fine for now because
            // the only goal is to produce a nicer error message; we'll bail out
            // safely even if the error message we're sniffing out changes.
            lazy_static! {
                static ref LOGGING_ERROR: Regex =
                    Regex::new("unknown catalog item 'mz_catalog.[^']*'").unwrap();
            }
            let item = match c.deserialize_item(id, def) {
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

    /// Opens the catalog at `path` with parameters set appropriately for debug
    /// contexts, like in tests.
    ///
    /// WARNING! This function can arbitrarily fail because it does not make any
    /// effort to adjust the catalog's contents' structure or semantics to the
    /// currently running version, i.e. it does not apply any migrations.
    ///
    /// This function should not be called in production contexts. Use
    /// [`Catalog::open`] with appropriately set configuration parameters
    /// instead.
    pub async fn open_debug(data_dir_path: &Path, now: NowFn) -> Result<Catalog, anyhow::Error> {
        let experimental_mode = None;
        let metrics_registry = &MetricsRegistry::new();
        let storage = storage::Connection::open(data_dir_path, experimental_mode)?;
        let (catalog, _, _) = Self::open(Config {
            storage,
            local_compute_introspection: Some(ComputeInstanceIntrospectionConfig {
                granularity: Duration::from_secs(1),
                debugging: false,
            }),
            experimental_mode,
            safe_mode: false,
            build_info: &DUMMY_BUILD_INFO,
            aws_external_id: AwsExternalId::NotProvided,
            timestamp_frequency: Duration::from_secs(1),
            now,
            skip_migrations: true,
            metrics_registry,
            disable_user_indexes: false,
            persister: &PersistConfig::disabled()
                .init(Uuid::new_v4(), DUMMY_BUILD_INFO, metrics_registry)
                .await?,
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
            catalog: self,
            conn_id: session.conn_id(),
            compute_instance: session.vars().cluster().into(),
            database,
            search_path,
            user: session.user().into(),
            prepared_statements: Some(session.prepared_statements()),
        }
    }

    pub fn for_sessionless_user(&self, user: String) -> ConnCatalog {
        ConnCatalog {
            catalog: self,
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

    fn storage(&self) -> MutexGuard<storage::Connection> {
        self.storage.lock().expect("lock poisoned")
    }

    pub fn allocate_system_ids(&mut self, amount: u64) -> Result<Vec<GlobalId>, Error> {
        self.storage().allocate_system_ids(amount)
    }

    pub fn allocate_user_id(&mut self) -> Result<GlobalId, Error> {
        self.storage().allocate_user_id()
    }

    pub fn allocate_oid(&mut self) -> Result<u32, Error> {
        self.state.allocate_oid()
    }

    pub fn resolve_database(&self, database_name: &str) -> Result<&Database, SqlCatalogError> {
        match self.state.database_by_name.get(database_name) {
            Some(id) => Ok(&self.state.database_by_id[id]),
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

    pub fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
    ) -> Result<&Schema, SqlCatalogError> {
        self.state
            .resolve_schema_in_database(database_spec, schema_name, conn_id)
            .ok_or_else(|| SqlCatalogError::UnknownSchema(schema_name.into()))
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

    /// Resolves a `BuiltinTable`.
    pub fn resolve_builtin_table(&self, builtin: &'static BuiltinTable) -> GlobalId {
        self.state.resolve_builtin_table(builtin)
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

    pub fn resolve_compute_instance(
        &self,
        name: &str,
    ) -> Result<&ComputeInstance, SqlCatalogError> {
        let id = self
            .state
            .compute_instances_by_name
            .get(name)
            .ok_or_else(|| SqlCatalogError::UnknownComputeInstance(name.to_string()))?;
        Ok(&self.state.compute_instances_by_id[id])
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
                return Ok(&self.state.entry_by_id[id]);
            }
        }
        Err(SqlCatalogError::UnknownItem(name.to_string()))
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

    fn find_available_name(&self, name: QualifiedObjectName, conn_id: u32) -> QualifiedObjectName {
        self.state.find_available_name(name, conn_id)
    }

    pub fn try_get_entry(&self, id: &GlobalId) -> Option<&CatalogEntry> {
        self.state.try_get_entry(id)
    }

    pub fn get_entry(&self, id: &GlobalId) -> &CatalogEntry {
        self.state.get_entry(id)
    }

    pub fn get_entry_by_oid(&self, oid: &u32) -> &CatalogEntry {
        self.state.get_entry_by_oid(oid)
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
    pub fn create_temporary_schema(&mut self, conn_id: u32) -> Result<(), Error> {
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

    pub fn transact<F, T>(
        &mut self,
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
                config: ComputeInstanceConfig,
                introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
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
            DropItem(GlobalId),
            UpdateItem {
                id: GlobalId,
                to_name: QualifiedObjectName,
                to_item: CatalogItem,
            },
            UpdateComputeInstanceConfig {
                id: ComputeInstanceId,
                config: InstanceConfig,
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
        let mut storage = self.storage();
        let mut tx = storage.transaction()?;
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
                    vec![Action::CreateComputeInstance {
                        id: tx.insert_compute_instance(&name, &config, &introspection_sources)?,
                        name,
                        config,
                        introspection_sources,
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
                    if name == "default" {
                        coord_bail!("cannot drop the default cluster");
                    }
                    tx.remove_compute_instance(&name)?;
                    builtin_table_updates.push(self.state.pack_compute_instance_update(&name, -1));
                    vec![Action::DropComputeInstance { name }]
                }
                Op::DropItem(id) => {
                    if !self.get_entry(&id).item().is_temporary() {
                        tx.remove_item(id)?;
                    }
                    builtin_table_updates.extend(self.state.pack_item_update(id, -1));
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
                            Error::new(ErrorKind::AmbiguousRename {
                                depender: self
                                    .resolve_full_name(&entry.name, entry.conn_id())
                                    .to_string(),
                                dependee: self
                                    .resolve_full_name(&entry.name, entry.conn_id())
                                    .to_string(),
                                message: e,
                            })
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
                                Error::new(ErrorKind::AmbiguousRename {
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
                                })
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
                Op::UpdateItem { id, to_item } => {
                    let entry = self.get_entry(&id);

                    if !to_item.is_temporary() {
                        let serialized_item = self.serialize_item(&to_item);
                        tx.update_item(id, &entry.name().item, &serialized_item)?;
                    }

                    builtin_table_updates.extend(self.state.pack_item_update(id, -1));

                    vec![Action::UpdateItem {
                        id,
                        to_name: entry.name().clone(),
                        to_item,
                    }]
                }
                Op::UpdateComputeInstanceConfig { id, config } => {
                    tx.update_compute_instance_config(id, &config)?;
                    let config = match config {
                        ComputeInstanceConfig::Local => InstanceConfig::Local,
                        ComputeInstanceConfig::Remote {
                            replicas,
                            introspection,
                        } => {
                            if introspection.is_some() {
                                coord_bail!(
                                    "cannot change introspection options on existing cluster"
                                );
                            }
                            InstanceConfig::Remote { replicas }
                        }
                        ComputeInstanceConfig::Managed {
                            size,
                            introspection,
                        } => {
                            if introspection.is_some() {
                                coord_bail!(
                                    "cannot change introspection options on existing cluster"
                                );
                            }
                            InstanceConfig::Managed { size }
                        }
                    };
                    vec![Action::UpdateComputeInstanceConfig { id, config }]
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
                    info!("create schema {}.{}", database_id, schema_name);
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
                    state.insert_compute_instance(
                        id,
                        name.clone(),
                        config,
                        None,
                        introspection_sources,
                    );
                    builtin_table_updates.push(state.pack_compute_instance_update(&name, 1));
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

                Action::DropComputeInstance { name } => {
                    let id = state
                        .compute_instances_by_name
                        .remove(&name)
                        .expect("can only drop known instances");

                    let instance = state
                        .compute_instances_by_id
                        .remove(&id)
                        .expect("can only drop known instances");

                    assert!(
                        instance.indexes.is_empty(),
                        "not all items dropped before compute instance"
                    );
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

                Action::UpdateComputeInstanceConfig { id, config } => {
                    state.compute_instances_by_id.get_mut(&id).unwrap().config = config;
                }
            }
        }

        let result = f(&state)?;

        // The user closure was successful, apply the updates.
        tx.commit().map_err(|err| CoordError::Catalog(err.into()))?;
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
                table_persist_name: table.persist_name.clone(),
                source_persist_details: None,
            },
            CatalogItem::Source(source) => SerializedCatalogItem::V1 {
                create_sql: source.create_sql.clone(),
                eval_env: None,
                table_persist_name: None,
                source_persist_details: source.persist_details.clone(),
            },
            CatalogItem::View(view) => SerializedCatalogItem::V1 {
                create_sql: view.create_sql.clone(),
                eval_env: None,
                table_persist_name: None,
                source_persist_details: None,
            },
            CatalogItem::Index(index) => SerializedCatalogItem::V1 {
                create_sql: index.create_sql.clone(),
                eval_env: None,
                table_persist_name: None,
                source_persist_details: None,
            },
            CatalogItem::Sink(sink) => SerializedCatalogItem::V1 {
                create_sql: sink.create_sql.clone(),
                eval_env: None,
                table_persist_name: None,
                source_persist_details: None,
            },
            CatalogItem::Type(typ) => SerializedCatalogItem::V1 {
                create_sql: typ.create_sql.clone(),
                eval_env: None,
                table_persist_name: None,
                source_persist_details: None,
            },
            CatalogItem::Secret(secret) => SerializedCatalogItem::V1 {
                create_sql: secret.create_sql.clone(),
                eval_env: None,
                table_persist_name: None,
                source_persist_details: None,
            },
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
        };
        serde_json::to_vec(&item).expect("catalog serialization cannot fail")
    }

    fn deserialize_item(&self, id: GlobalId, bytes: Vec<u8>) -> Result<CatalogItem, anyhow::Error> {
        let SerializedCatalogItem::V1 {
            create_sql,
            eval_env: _,
            table_persist_name,
            source_persist_details,
        } = serde_json::from_slice(&bytes)?;
        self.parse_item(
            id,
            create_sql,
            Some(&PlanContext::zero()),
            table_persist_name,
            source_persist_details,
        )
    }

    // Parses the given SQL string into a `CatalogItem`.
    //
    // The given `persist_details` are an optional description of the persisted streams that this
    // source uses, if it is a persisted source.
    fn parse_item(
        &self,
        id: GlobalId,
        create_sql: String,
        pcx: Option<&PlanContext>,
        table_persist_name: Option<String>,
        source_persist_details: Option<SerializedSourcePersistDetails>,
    ) -> Result<CatalogItem, anyhow::Error> {
        let stmt = mz_sql::parse::parse(&create_sql)?.into_element();
        let plan = mz_sql::plan::plan(pcx, &self.for_system_session(), stmt, &Params::empty())?;
        Ok(match plan {
            Plan::CreateTable(CreateTablePlan { table, .. }) => {
                assert!(
                    source_persist_details.is_none(),
                    "got some source_persist_details while we didn't expect them for a table"
                );
                CatalogItem::Table(Table {
                    create_sql: table.create_sql,
                    desc: table.desc,
                    defaults: table.defaults,
                    conn_id: None,
                    depends_on: table.depends_on,
                    persist_name: table_persist_name,
                })
            }
            Plan::CreateSource(CreateSourcePlan { source, .. }) => {
                assert!(
                    table_persist_name.is_none(),
                    "got some table_persist_name while we didn't expect them for a source"
                );
                CatalogItem::Source(Source {
                    create_sql: source.create_sql,
                    connector: source.connector,
                    persist_details: source_persist_details,
                    desc: source.desc,
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
                    depends_on: view.depends_on,
                })
            }
            Plan::CreateIndex(CreateIndexPlan { index, .. }) => CatalogItem::Index(Index {
                create_sql: index.create_sql,
                on: index.on,
                keys: index.keys,
                conn_id: None,
                depends_on: index.depends_on,
                enabled: self.index_enabled_by_default(&id),
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
                depends_on: sink.depends_on,
                compute_instance: sink.compute_instance,
            }),
            Plan::CreateType(CreateTypePlan { typ, .. }) => CatalogItem::Type(Type {
                create_sql: typ.create_sql,
                details: CatalogTypeDetails {
                    array_id: None,
                    typ: typ.inner,
                },
                depends_on: typ.depends_on,
            }),
            Plan::CreateSecret(CreateSecretPlan { secret, .. }) => CatalogItem::Secret(Secret {
                create_sql: secret.create_sql,
            }),
            _ => bail!("catalog entry generated inappropriate plan"),
        })
    }

    /// Returns the default value for an [`Index`]'s `enabled` field.
    ///
    /// Note that it is the caller's responsibility to ensure that the `id` is
    /// used for an `Index`.
    pub fn index_enabled_by_default(&self, id: &GlobalId) -> bool {
        !self.config().disable_user_indexes || !id.is_user()
    }

    /// Returns whether or not an index is enabled.
    ///
    /// # Panics
    /// Panics if `id` does not belong to a [`CatalogItem::Index`].
    pub fn is_index_enabled(&self, id: &GlobalId) -> bool {
        let index_entry = self.get_entry(&id);
        match index_entry.item() {
            CatalogItem::Index(index) => index.enabled,
            _ => unreachable!("cannot call is_index_enabled on non-idex"),
        }
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        self.state.uses_tables(id)
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> String {
        serde_json::to_string(&self.state.database_by_id).expect("serialization cannot fail")
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

    pub fn allocate_introspection_source_indexes(
        &mut self,
    ) -> Vec<(&'static BuiltinLog, GlobalId)> {
        let log_amount = BUILTINS.logs().count();
        let system_ids = self
            .allocate_system_ids(
                log_amount
                    .try_into()
                    .expect("builtin logs should fit into u64"),
            )
            .expect("cannot fail to allocate system ids");
        BUILTINS.logs().zip(system_ids.into_iter()).collect()
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
        config: ComputeInstanceConfig,
        introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
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
    /// Unconditionally removes the identified items. It is required that the
    /// IDs come from the output of `plan_remove`; otherwise consistency rules
    /// may be violated.
    DropItem(GlobalId),
    RenameItem {
        id: GlobalId,
        current_full_name: FullObjectName,
        to_name: String,
    },
    UpdateItem {
        id: GlobalId,
        to_item: CatalogItem,
    },
    UpdateComputeInstanceConfig {
        id: ComputeInstanceId,
        config: ComputeInstanceConfig,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SerializedCatalogItem {
    V1 {
        create_sql: String,
        // The name "eval_env" is historical.
        eval_env: Option<SerializedPlanContext>,
        // Previous versions used "persist_name" as the field name here.
        #[serde(alias = "persist_name")]
        table_persist_name: Option<String>,
        source_persist_details: Option<SerializedSourcePersistDetails>,
    },
}

/// Serialized source persistence details. See `SourcePersistDesc` for an explanation of the
/// fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedSourcePersistDetails {
    /// Name of the primary persisted stream of this source. This is what a consumer of the
    /// persisted data would be interested in while the secondary stream(s) of the source are an
    /// internal implementation detail.
    pub primary_stream: String,

    /// Persisted stream of timestamp bindings.
    pub timestamp_bindings_stream: String,

    /// Any additional details that we need to make the envelope logic stateful.
    pub envelope_details: SerializedEnvelopePersistDetails,
}

/// See `EnvelopePersistDesc` for an explanation of the fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializedEnvelopePersistDetails {
    Upsert,
    None,
}

impl From<SourcePersistDesc> for SerializedSourcePersistDetails {
    fn from(source_persist_desc: SourcePersistDesc) -> Self {
        SerializedSourcePersistDetails {
            primary_stream: source_persist_desc.primary_stream,
            timestamp_bindings_stream: source_persist_desc.timestamp_bindings_stream,
            envelope_details: source_persist_desc.envelope_desc.into(),
        }
    }
}

impl From<EnvelopePersistDesc> for SerializedEnvelopePersistDetails {
    fn from(persist_desc: EnvelopePersistDesc) -> Self {
        match persist_desc {
            EnvelopePersistDesc::Upsert => SerializedEnvelopePersistDetails::Upsert,
            EnvelopePersistDesc::None => SerializedEnvelopePersistDetails::None,
        }
    }
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
        self.catalog
            .state
            .entry_by_id
            .get(&id)
            .map(|entry| entry.name())
            .map(|name| self.resolve_full_name(name).to_string())
    }

    fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        use ScalarType::*;

        match typ {
            Array(t) => format!("{}[]", self.humanize_scalar_type(t)),
            List { custom_oid, .. } | Map { custom_oid, .. } if custom_oid.is_some() => {
                let item = self.get_item_by_oid(&custom_oid.unwrap());
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
                custom_oid: Some(oid),
                ..
            } => {
                let item = self.get_item_by_oid(oid);
                self.minimal_qualification(item.name()).to_string()
            }
            Record {
                custom_name: Some(name),
                ..
            } => name.clone(),
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
                    SchemaSpecifier::Id(self.catalog.get_pg_catalog_schema_id().clone());
                let res = if self
                    .search_path
                    .iter()
                    .any(|(_, schema)| schema == &pg_catalog_schema)
                {
                    pgrepr_type.name().to_string()
                } else {
                    // If PG_CATALOG_SCHEMA is not in search path, you need
                    // qualified object name to refer to type.
                    let name = self.get_item_by_oid(&pgrepr_type.oid()).name();
                    self.resolve_full_name(name).to_string()
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
        Ok(self.catalog.resolve_database(database_name)?)
    }

    fn get_database(&self, id: &DatabaseId) -> &dyn mz_sql::catalog::CatalogDatabase {
        self.catalog
            .state
            .database_by_id
            .get(id)
            .expect("database doesn't exist")
    }

    fn resolve_schema(
        &self,
        database_name: Option<&str>,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        Ok(self.catalog.resolve_schema(
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
            .catalog
            .resolve_schema_in_database(database_spec, schema_name, self.conn_id)?)
    }

    fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> &dyn CatalogSchema {
        self.catalog
            .get_schema(database_spec, schema_spec, self.conn_id)
    }

    fn is_system_schema(&self, schema: &str) -> bool {
        self.catalog.state.is_system_schema(schema)
    }

    fn resolve_role(
        &self,
        role_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogRole, SqlCatalogError> {
        match self.catalog.state.roles.get(role_name) {
            Some(role) => Ok(role),
            None => Err(SqlCatalogError::UnknownRole(role_name.into())),
        }
    }

    fn resolve_compute_instance(
        &self,
        compute_instance_name: Option<&str>,
    ) -> Result<&dyn mz_sql::catalog::CatalogComputeInstance, SqlCatalogError> {
        self.catalog
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
        Ok(self.catalog.resolve_entry(
            self.database.as_ref(),
            &self.search_path,
            name,
            self.conn_id,
        )?)
    }

    fn resolve_function(
        &self,
        name: &PartialObjectName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        Ok(self.catalog.resolve_function(
            self.database.as_ref(),
            &self.search_path,
            name,
            self.conn_id,
        )?)
    }

    fn try_get_item(&self, id: &GlobalId) -> Option<&dyn mz_sql::catalog::CatalogItem> {
        self.catalog
            .try_get_entry(id)
            .map(|item| item as &dyn mz_sql::catalog::CatalogItem)
    }

    fn get_item(&self, id: &GlobalId) -> &dyn mz_sql::catalog::CatalogItem {
        self.catalog.get_entry(id)
    }

    fn get_item_by_oid(&self, oid: &u32) -> &dyn mz_sql::catalog::CatalogItem {
        let id = self.catalog.state.entry_by_oid[oid];
        self.catalog.get_entry(&id)
    }

    fn item_exists(&self, name: &QualifiedObjectName) -> bool {
        self.catalog.item_exists(name, self.conn_id)
    }

    fn find_available_name(&self, name: QualifiedObjectName) -> QualifiedObjectName {
        self.catalog.find_available_name(name, self.conn_id)
    }

    fn resolve_full_name(&self, name: &QualifiedObjectName) -> FullObjectName {
        self.catalog.resolve_full_name(name, Some(self.conn_id))
    }

    fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        &self.catalog.config()
    }

    fn now(&self) -> EpochMillis {
        (self.catalog.config().now)()
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

impl mz_sql::catalog::CatalogComputeInstance for ComputeInstance {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> ComputeInstanceId {
        self.id
    }

    fn indexes(&self) -> &HashSet<GlobalId> {
        &self.indexes
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

    fn desc(&self, name: &FullObjectName) -> Result<&RelationDesc, SqlCatalogError> {
        Ok(self.desc(name)?)
    }

    fn func(&self) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        Ok(self.func()?)
    }

    fn source_connector(&self) -> Result<&SourceConnector, SqlCatalogError> {
        Ok(self.source_connector()?)
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
            CatalogItem::Func(_) => "TODO",
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

    fn type_details(&self) -> Option<&CatalogTypeDetails> {
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
    use tempfile::TempDir;

    use mz_ore::now::NOW_ZERO;
    use mz_sql::names::{
        ObjectQualifiers, PartialObjectName, QualifiedObjectName, ResolvedDatabaseSpecifier,
        SchemaSpecifier,
    };

    use crate::catalog::{Catalog, Op, MZ_CATALOG_SCHEMA, PG_CATALOG_SCHEMA};
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

        let data_dir = TempDir::new()?;
        let catalog = Catalog::open_debug(data_dir.path(), NOW_ZERO.clone()).await?;

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
                    schema: Some(PG_CATALOG_SCHEMA.to_string()),
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
                    schema: Some(MZ_CATALOG_SCHEMA.to_string()),
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
        let data_dir = TempDir::new()?;
        let mut catalog = Catalog::open_debug(data_dir.path(), NOW_ZERO.clone()).await?;
        assert_eq!(catalog.transient_revision(), 1);
        catalog
            .transact(
                vec![Op::CreateDatabase {
                    name: "test".to_string(),
                    oid: 1,
                    public_schema_oid: 2,
                }],
                |_catalog| Ok(()),
            )
            .unwrap();
        assert_eq!(catalog.transient_revision(), 2);
        drop(catalog);

        let catalog = Catalog::open_debug(data_dir.path(), NOW_ZERO.clone()).await?;
        assert_eq!(catalog.transient_revision(), 1);

        Ok(())
    }
}
