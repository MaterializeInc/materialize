// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use anyhow::bail;
use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{info, trace};
use ore::collections::CollectionExt;
use regex::Regex;
use serde::{Deserialize, Serialize};

use build_info::DUMMY_BUILD_INFO;
use dataflow_types::{SinkConnector, SinkConnectorBuilder, SourceConnector};
use expr::{ExprHumanizer, GlobalId, OptimizedRelationExpr, ScalarExpr};
use repr::{ColumnType, RelationDesc, ScalarType};
use sql::ast::display::AstDisplay;
use sql::ast::Expr;
use sql::catalog::{Catalog as SqlCatalog, CatalogError as SqlCatalogError};
use sql::names::{DatabaseSpecifier, FullName, PartialName, SchemaName};
use sql::plan::{Params, Plan, PlanContext};
use transform::Optimizer;

use crate::catalog::builtin::{
    Builtin, BUILTINS, MZ_CATALOG_SCHEMA, MZ_TEMP_SCHEMA, PG_CATALOG_SCHEMA,
};
use crate::catalog::error::{Error, ErrorKind};
use crate::catalog::migrate::CONTENT_MIGRATIONS;
use crate::session::Session;

mod config;
mod error;
mod migrate;

pub mod builtin;
pub mod storage;

pub use crate::catalog::config::Config;

const SYSTEM_CONN_ID: u32 = 0;

// TODO@jldlaughlin: Better assignment strategy for system type OIDs.
// https://github.com/MaterializeInc/materialize/pull/4316#discussion_r496238962
pub const FIRST_USER_OID: u32 = 20_000;

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
/// name. Items can be referred to by their [`FullName`], which fully and
/// unambiguously specifies the item, or a [`PartialName`], which can omit the
/// database name and/or the schema name. Partial names can be converted into
/// full names via a complicated resolution process documented by the
/// [`resolve`] method.
///
/// The catalog also maintains special "ambient schemas": virtual schemas,
/// implicitly present in all databases, that house various system views.
/// The big examples of ambient schemas are `pg_catalog` and `mz_catalog`.
#[derive(Debug)]
pub struct Catalog {
    by_name: BTreeMap<String, Database>,
    by_id: BTreeMap<GlobalId, CatalogEntry>,
    by_oid: HashMap<u32, GlobalId>,
    indexes: HashMap<GlobalId, Vec<(GlobalId, Vec<ScalarExpr>)>>,
    ambient_schemas: BTreeMap<String, Schema>,
    temporary_schemas: HashMap<u32, Schema>,
    storage: Arc<Mutex<storage::Connection>>,
    oid_counter: u32,
    config: sql::catalog::CatalogConfig,
}

#[derive(Debug)]
pub struct ConnCatalog<'a> {
    catalog: &'a Catalog,
    conn_id: u32,
    database: String,
    search_path: &'a [&'a str],
}

impl ConnCatalog<'_> {
    pub fn conn_id(&self) -> u32 {
        self.conn_id
    }
}

#[derive(Debug, Serialize)]
pub struct Database {
    pub name: String,
    pub id: i64,
    #[serde(skip)]
    pub oid: u32,
    pub schemas: BTreeMap<String, Schema>,
}

#[derive(Debug, Serialize)]
pub struct Schema {
    pub name: SchemaName,
    pub id: i64,
    #[serde(skip)]
    pub oid: u32,
    pub items: BTreeMap<String, GlobalId>,
}

#[derive(Clone, Debug)]
pub struct CatalogEntry {
    item: CatalogItem,
    used_by: Vec<GlobalId>,
    id: GlobalId,
    oid: u32,
    name: FullName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogItem {
    Table(Table),
    Source(Source),
    View(View),
    Sink(Sink),
    Index(Index),
    Type(Type),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub create_sql: String,
    pub plan_cx: PlanContext,
    pub desc: RelationDesc,
    #[serde(skip)]
    pub defaults: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub create_sql: String,
    pub plan_cx: PlanContext,
    pub connector: SourceConnector,
    pub desc: RelationDesc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sink {
    pub create_sql: String,
    pub plan_cx: PlanContext,
    pub from: GlobalId,
    pub connector: SinkConnectorState,
    pub with_snapshot: bool,
    pub as_of: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SinkConnectorState {
    Pending(SinkConnectorBuilder),
    Ready(SinkConnector),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub create_sql: String,
    pub plan_cx: PlanContext,
    pub optimized_expr: OptimizedRelationExpr,
    pub desc: RelationDesc,
    pub conn_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub create_sql: String,
    pub plan_cx: PlanContext,
    pub on: GlobalId,
    pub keys: Vec<ScalarExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Type {
    pub create_sql: String,
    pub plan_cx: PlanContext,
    pub inner: TypeInner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypeInner {
    Array {
        element_id: GlobalId,
    },
    Base,
    List {
        element_id: GlobalId,
    },
    Map {
        key_id: GlobalId,
        value_id: GlobalId,
    },
}

impl From<sql::plan::TypeInner> for TypeInner {
    fn from(t: sql::plan::TypeInner) -> TypeInner {
        match t {
            sql::plan::TypeInner::List { element_id } => TypeInner::List { element_id },
            sql::plan::TypeInner::Map { key_id, value_id } => TypeInner::Map { key_id, value_id },
        }
    }
}

impl CatalogItem {
    /// Returns a string indicating the type of this catalog entry.
    pub fn type_string(&self) -> &'static str {
        match self {
            CatalogItem::Table(_) => "table",
            CatalogItem::Source(_) => "source",
            CatalogItem::Sink(_) => "sink",
            CatalogItem::View(_) => "view",
            CatalogItem::Index(_) => "index",
            CatalogItem::Type(_) => "type",
        }
    }

    pub fn desc(&self, name: &FullName) -> Result<&RelationDesc, SqlCatalogError> {
        match &self {
            CatalogItem::Table(tbl) => Ok(&tbl.desc),
            CatalogItem::Source(src) => Ok(&src.desc),
            CatalogItem::Sink(_) => Err(SqlCatalogError::InvalidSinkDependency(name.to_string())),
            CatalogItem::View(view) => Ok(&view.desc),
            CatalogItem::Index(_) => Err(SqlCatalogError::InvalidIndexDependency(name.to_string())),
            CatalogItem::Type(_) => Err(SqlCatalogError::InvalidTypeDependency(name.to_string())),
        }
    }

    /// Collects the identifiers of the dataflows that this item depends
    /// upon.
    pub fn uses(&self) -> Vec<GlobalId> {
        match self {
            CatalogItem::Table(_) => vec![],
            CatalogItem::Source(_) => vec![],
            CatalogItem::Sink(sink) => vec![sink.from],
            CatalogItem::View(view) => view.optimized_expr.as_ref().global_uses(),
            CatalogItem::Index(idx) => vec![idx.on],
            CatalogItem::Type(typ) => match &typ.inner {
                TypeInner::Array { element_id } => vec![*element_id],
                TypeInner::Base { .. } => vec![],
                TypeInner::List { element_id } => vec![*element_id],
                TypeInner::Map { key_id, value_id } => vec![*key_id, *value_id],
            },
        }
    }

    /// Indicates whether this item is a placeholder for a future item
    /// or if it's actually a real item.
    pub fn is_placeholder(&self) -> bool {
        match self {
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::View(_)
            | CatalogItem::Index(_)
            | CatalogItem::Type(_) => false,
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
            _ => None,
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
        from: FullName,
        to_item_name: String,
        rename_self: bool,
    ) -> Result<CatalogItem, String> {
        let do_rewrite = |create_sql: String| -> Result<String, String> {
            let mut create_stmt = sql::parse::parse(&create_sql).unwrap().into_element();
            if rename_self {
                sql::ast::transform::create_stmt_rename(&mut create_stmt, to_item_name.clone());
            }
            // Determination of what constitutes an ambiguous request is done here.
            sql::ast::transform::create_stmt_rename_refs(&mut create_stmt, from, to_item_name)?;
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
            CatalogItem::Type(_) => unreachable!("types cannot be renamed"),
        }
    }
}

impl CatalogEntry {
    /// Reports the description of the datums produced by this catalog item.
    pub fn desc(&self) -> Result<&RelationDesc, SqlCatalogError> {
        self.item.desc(&self.name)
    }

    /// Reports whether this catalog entry is a table.
    pub fn is_table(&self) -> bool {
        matches!(self.item(), CatalogItem::Table(_))
    }

    /// Collects the identifiers of the dataflows that this dataflow depends
    /// upon.
    pub fn uses(&self) -> Vec<GlobalId> {
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

    /// Returns the name of this catalog entry.
    pub fn name(&self) -> &FullName {
        &self.name
    }

    /// Returns the identifiers of the dataflows that depend upon this dataflow.
    pub fn used_by(&self) -> &[GlobalId] {
        &self.used_by
    }
}

impl Catalog {
    /// Opens or creates a catalog that stores data at `path`.
    ///
    /// Returns the catalog and a list of events that describe the initial state
    /// of the catalog.
    pub fn open(config: &Config) -> Result<(Catalog, Vec<Event>), Error> {
        let (storage, experimental_mode, cluster_id) = storage::Connection::open(&config)?;

        let mut catalog = Catalog {
            by_name: BTreeMap::new(),
            by_id: BTreeMap::new(),
            by_oid: HashMap::new(),
            indexes: HashMap::new(),
            ambient_schemas: BTreeMap::new(),
            temporary_schemas: HashMap::new(),
            storage: Arc::new(Mutex::new(storage)),
            oid_counter: FIRST_USER_OID,
            config: sql::catalog::CatalogConfig {
                startup_time: SystemTime::now(),
                nonce: rand::random(),
                experimental_mode,
                cluster_id,
                cache_directory: config.cache_directory.clone(),
                build_info: config.build_info,
            },
        };
        let mut events = vec![];

        catalog.create_temporary_schema(SYSTEM_CONN_ID)?;

        let databases = catalog.storage().load_databases()?;
        for (id, name) in databases {
            let oid = catalog.allocate_oid()?;
            catalog.by_name.insert(
                name.clone(),
                Database {
                    name: name.clone(),
                    id,
                    oid,
                    schemas: BTreeMap::new(),
                },
            );
            events.push(Event::CreatedDatabase { name, id, oid });
        }

        let schemas = catalog.storage().load_schemas()?;
        for (id, database_name, schema_name) in schemas {
            let oid = catalog.allocate_oid()?;
            let (database_id, schemas) = match &database_name {
                Some(database_name) => catalog
                    .by_name
                    .get_mut(database_name)
                    .map(|db| (Some(db.id), &mut db.schemas))
                    .expect("catalog out of sync"),
                None => (None, &mut catalog.ambient_schemas),
            };
            schemas.insert(
                schema_name.clone(),
                Schema {
                    name: SchemaName {
                        database: database_name.into(),
                        schema: schema_name.clone(),
                    },
                    id,
                    oid,
                    items: BTreeMap::new(),
                },
            );
            events.push(Event::CreatedSchema {
                database_id,
                schema_id: id,
                schema_name,
                oid,
            })
        }

        for builtin in BUILTINS.values() {
            let name = FullName {
                database: DatabaseSpecifier::Ambient,
                schema: builtin.schema().into(),
                item: builtin.name().into(),
            };
            match builtin {
                Builtin::Log(log) if config.enable_logging => {
                    let index_name = format!("{}_primary_idx", log.name);
                    let oid = catalog.allocate_oid()?;
                    events.push(catalog.insert_item(
                        log.id,
                        oid,
                        name.clone(),
                        CatalogItem::Source(Source {
                            create_sql: "TODO".to_string(),
                            plan_cx: PlanContext::default(),
                            connector: dataflow_types::SourceConnector::Local,
                            desc: log.variant.desc(),
                        }),
                    ));
                    let oid = catalog.allocate_oid()?;
                    events.push(
                        catalog.insert_item(
                            log.index_id,
                            oid,
                            FullName {
                                database: DatabaseSpecifier::Ambient,
                                schema: MZ_CATALOG_SCHEMA.into(),
                                item: index_name.clone(),
                            },
                            CatalogItem::Index(Index {
                                on: log.id,
                                keys: log
                                    .variant
                                    .index_by()
                                    .into_iter()
                                    .map(ScalarExpr::Column)
                                    .collect(),
                                create_sql: super::coord::index_sql(
                                    index_name,
                                    name,
                                    &log.variant.desc(),
                                    &log.variant.index_by(),
                                ),
                                plan_cx: PlanContext::default(),
                            }),
                        ),
                    );
                }

                Builtin::Table(table) => {
                    let index_name = format!("{}_primary_idx", table.name);
                    let index_columns = table.desc.typ().default_key();
                    let index_sql = super::coord::index_sql(
                        index_name.clone(),
                        name.clone(),
                        &table.desc,
                        &index_columns,
                    );
                    let oid = catalog.allocate_oid()?;
                    events.push(catalog.insert_item(
                        table.id,
                        oid,
                        name.clone(),
                        CatalogItem::Table(Table {
                            create_sql: "TODO".to_string(),
                            plan_cx: PlanContext::default(),
                            desc: table.desc.clone(),
                            defaults: vec![Expr::null(); table.desc.arity()],
                        }),
                    ));
                    let oid = catalog.allocate_oid()?;
                    events.push(
                        catalog.insert_item(
                            table.index_id,
                            oid,
                            FullName {
                                database: DatabaseSpecifier::Ambient,
                                schema: MZ_CATALOG_SCHEMA.into(),
                                item: index_name,
                            },
                            CatalogItem::Index(Index {
                                on: table.id,
                                keys: index_columns
                                    .iter()
                                    .map(|i| ScalarExpr::Column(*i))
                                    .collect(),
                                create_sql: index_sql,
                                plan_cx: PlanContext::default(),
                            }),
                        ),
                    );
                }

                Builtin::View(view) if config.enable_logging || !view.needs_logs => {
                    let item = catalog
                        .parse_item(view.sql.into(), PlanContext::default())
                        .unwrap_or_else(|e| {
                            panic!(
                                "internal error: failed to load bootstrap view:\n\
                                    {}\n\
                                    error:\n\
                                    {:?}",
                                view.name, e
                            )
                        });
                    let oid = catalog.allocate_oid()?;
                    events.push(catalog.insert_item(view.id, oid, name, item));
                }

                Builtin::Type(typ) => {
                    events.push(catalog.insert_item(
                        typ.id,
                        typ.oid(),
                        FullName {
                            database: DatabaseSpecifier::Ambient,
                            schema: PG_CATALOG_SCHEMA.into(),
                            item: typ.name().to_owned(),
                        },
                        CatalogItem::Type(Type {
                            create_sql: format!("CREATE TYPE {}", typ.name()),
                            plan_cx: PlanContext::default(),
                            inner: match typ.kind() {
                                postgres_types::Kind::Array(element_type) => {
                                    let element_id = catalog.ambient_schemas[PG_CATALOG_SCHEMA]
                                        .items[element_type.name()];
                                    TypeInner::Array { element_id }
                                }
                                _ => TypeInner::Base,
                            },
                        }),
                    ));
                }

                _ => (),
            }
        }

        let mut catalog_content_version = catalog.storage().get_catalog_content_version()?;

        while CONTENT_MIGRATIONS.len() > catalog_content_version {
            if let Err(e) = CONTENT_MIGRATIONS[catalog_content_version](&mut catalog) {
                return Err(Error::new(ErrorKind::FailedMigration {
                    last_version: catalog_content_version,
                    cause: e.to_string(),
                }));
            }
            catalog_content_version += 1;
            catalog
                .storage()
                .set_catalog_content_version(catalog_content_version)?;
        }

        let items = catalog.storage().load_items()?;
        for (id, name, def) in items {
            // TODO(benesch): a better way of detecting when a view has depended
            // upon a non-existent logging view. This is fine for now because
            // the only goal is to produce a nicer error message; we'll bail out
            // safely even if the error message we're sniffing out changes.
            lazy_static! {
                static ref LOGGING_ERROR: Regex =
                    Regex::new("unknown catalog item 'mz_catalog.[^']*'").unwrap();
            }
            let item = match catalog.deserialize_item(def) {
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
            let oid = catalog.allocate_oid()?;
            events.push(catalog.insert_item(id, oid, name, item));
        }
        Ok((catalog, events))
    }

    /// Opens the catalog at `path` with parameters set appropriately for debug
    /// contexts, like in tests.
    ///
    /// This function should not be called in production contexts. Use
    /// [`Catalog::open`] with appropriately set configuration parameters
    /// instead.
    pub fn open_debug(path: &Path) -> Result<Catalog, anyhow::Error> {
        let (catalog, _) = Self::open(&Config {
            path,
            enable_logging: true,
            experimental_mode: None,
            cache_directory: None,
            build_info: &DUMMY_BUILD_INFO,
        })?;
        Ok(catalog)
    }

    pub fn for_session(&self, session: &Session) -> ConnCatalog {
        ConnCatalog {
            catalog: self,
            conn_id: session.conn_id(),
            database: session.vars().database().into(),
            search_path: session.vars().search_path(),
        }
    }

    // Leaving the system's search path empty allows us to catch issues
    // where catalog object names have not been normalized correctly.
    pub fn for_system_session(&self) -> ConnCatalog {
        ConnCatalog {
            catalog: self,
            conn_id: SYSTEM_CONN_ID,
            database: "materialize".into(),
            search_path: &[],
        }
    }

    fn storage(&self) -> MutexGuard<storage::Connection> {
        self.storage.lock().expect("lock poisoned")
    }

    pub fn allocate_id(&mut self) -> Result<GlobalId, Error> {
        self.storage().allocate_id()
    }

    pub fn allocate_oid(&mut self) -> Result<u32, Error> {
        let oid = self.oid_counter;
        if oid == u32::max_value() {
            return Err(Error::new(ErrorKind::OidExhaustion));
        }
        self.oid_counter += 1;
        Ok(oid)
    }

    pub fn resolve_schema(
        &self,
        current_database: &str,
        database: Option<String>,
        schema_name: &str,
        conn_id: u32,
    ) -> Result<&Schema, SqlCatalogError> {
        let database_spec = match database {
            // If a database is explicitly specified, validate it. Note that we
            // intentionally do not validate `current_database` to permit
            // querying `mz_catalog` with an invalid session database, e.g., so
            // that you can run `SHOW DATABASES` to *find* a valid database.
            Some(database) if !self.by_name.contains_key(&database) => {
                return Err(SqlCatalogError::UnknownDatabase(database));
            }
            Some(database) => DatabaseSpecifier::Name(database),
            None => DatabaseSpecifier::Name(current_database.into()),
        };

        // First try to find the schema in the named database.
        if let Some(schema) = self.get_schema(&database_spec, schema_name, conn_id) {
            return Ok(schema);
        }

        // Then fall back to the ambient database.
        if let Some(schema) = self.get_schema(&DatabaseSpecifier::Ambient, schema_name, conn_id) {
            return Ok(schema);
        }

        Err(SqlCatalogError::UnknownSchema(schema_name.into()))
    }

    /// Resolves [`PartialName`] into a [`FullName`].
    ///
    /// If `name` does not specify a database, the `current_database` is used.
    /// If `name` does not specify a schema, then the schemas in `search_path`
    /// are searched in order.
    #[allow(clippy::useless_let_if_seq)]
    pub fn resolve(
        &self,
        current_database: &str,
        search_path: &[&str],
        name: &PartialName,
        conn_id: u32,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        // If a schema name was specified, just try to find the item in that
        // schema. If no schema was specified, try to find the item in every
        // schema in the search path.
        //
        // This is written strangely to work around limitations in Rust's
        // temporary lifetime inference [0]. Ideally the following would work,
        // but it does not:
        //
        //     let schemas = match name.schema {
        //         Some(name) => &[name],
        //         None => search_path,
        //     }
        //
        // [0]: https://github.com/rust-lang/rust/issues/15023
        let mut schemas = &[name.schema.as_deref().unwrap_or("")][..];
        if name.schema.is_none() {
            schemas = search_path;
        }

        for &schema_name in schemas {
            let database_name = name.database.clone();
            let schema =
                match self.resolve_schema(&current_database, database_name, schema_name, conn_id) {
                    Ok(schema) => schema,
                    Err(SqlCatalogError::UnknownSchema(_)) => continue,
                    Err(e) => return Err(e),
                };
            if let Some(id) = schema.items.get(&name.item) {
                return Ok(&self.by_id[id]);
            }
        }
        Err(SqlCatalogError::UnknownItem(name.to_string()))
    }

    /// Returns the named catalog item, if it exists.
    ///
    /// See also [`Catalog::get`].
    pub fn try_get(&self, name: &FullName, conn_id: u32) -> Option<&CatalogEntry> {
        self.get_schema(&name.database, &name.schema, conn_id)
            .and_then(|schema| schema.items.get(&name.item))
            .map(|id| &self.by_id[id])
    }

    pub fn try_get_by_id(&self, id: GlobalId) -> Option<&CatalogEntry> {
        self.by_id.get(&id)
    }

    pub fn get_by_id(&self, id: &GlobalId) -> &CatalogEntry {
        &self.by_id[id]
    }

    /// Creates a new schema in the `Catalog` for temporary items
    /// indicated by the TEMPORARY or TEMP keywords.
    pub fn create_temporary_schema(&mut self, conn_id: u32) -> Result<(), Error> {
        let oid = self.allocate_oid()?;
        self.temporary_schemas.insert(
            conn_id,
            Schema {
                name: SchemaName {
                    database: DatabaseSpecifier::Ambient,
                    schema: MZ_TEMP_SCHEMA.into(),
                },
                id: -1,
                oid,
                items: BTreeMap::new(),
            },
        );
        Ok(())
    }

    fn item_exists_in_temp_schemas(&mut self, conn_id: u32, item_name: &str) -> bool {
        self.temporary_schemas[&conn_id]
            .items
            .contains_key(item_name)
    }

    pub fn drop_temp_item_ops(&mut self, conn_id: u32) -> Vec<Op> {
        self.temporary_schemas[&conn_id]
            .items
            .values()
            .map(|id| Op::DropItem(*id))
            .collect()
    }

    pub fn drop_temporary_schema(&mut self, conn_id: u32) -> Result<(), Error> {
        if !self.temporary_schemas[&conn_id].items.is_empty() {
            return Err(Error::new(ErrorKind::SchemaNotEmpty(MZ_TEMP_SCHEMA.into())));
        }
        self.temporary_schemas.remove(&conn_id);
        Ok(())
    }

    /// Gets the schema map for the database matching `database_spec`.
    fn get_schema(
        &self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
    ) -> Option<&Schema> {
        // Keep in sync with `get_schemas_mut`.
        match database_spec {
            DatabaseSpecifier::Ambient if schema_name == MZ_TEMP_SCHEMA => {
                self.temporary_schemas.get(&conn_id)
            }
            DatabaseSpecifier::Ambient => self.ambient_schemas.get(schema_name),
            DatabaseSpecifier::Name(name) => self
                .by_name
                .get(name)
                .and_then(|db| db.schemas.get(schema_name)),
        }
    }

    /// Like `get_schemas`, but returns a `mut` reference.
    fn get_schema_mut(
        &mut self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
    ) -> Option<&mut Schema> {
        // Keep in sync with `get_schemas`.
        match database_spec {
            DatabaseSpecifier::Ambient if schema_name == MZ_TEMP_SCHEMA => {
                self.temporary_schemas.get_mut(&conn_id)
            }
            DatabaseSpecifier::Ambient => self.ambient_schemas.get_mut(schema_name),
            DatabaseSpecifier::Name(name) => self
                .by_name
                .get_mut(name)
                .and_then(|db| db.schemas.get_mut(schema_name)),
        }
    }

    #[must_use]
    pub fn insert_item(
        &mut self,
        id: GlobalId,
        oid: u32,
        name: FullName,
        item: CatalogItem,
    ) -> Event {
        if !item.is_placeholder() {
            info!("create {} {} ({})", item.type_string(), name, id);
        }

        let entry = CatalogEntry {
            item: item.clone(),
            name: name.clone(),
            id,
            oid,
            used_by: Vec::new(),
        };
        for u in entry.uses() {
            match self.by_id.get_mut(&u) {
                Some(metadata) => metadata.used_by.push(entry.id),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while installing {}",
                    u, entry.name
                ),
            }
        }

        match entry.item() {
            CatalogItem::Table(_) | CatalogItem::Source(_) | CatalogItem::View(_) => {
                self.indexes.insert(id, vec![]);
            }
            CatalogItem::Index(index) => {
                self.indexes
                    .get_mut(&index.on)
                    .unwrap()
                    .push((id, index.keys.clone()));
            }
            CatalogItem::Sink(_) | CatalogItem::Type(_) => (),
        }

        let conn_id = entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
        let schema = self
            .get_schema_mut(&entry.name.database, &entry.name.schema, conn_id)
            .expect("catalog out of sync");
        let schema_id = schema.id;
        schema.items.insert(entry.name.item.clone(), entry.id);
        self.by_oid.insert(oid, entry.id);
        self.by_id.insert(entry.id, entry);

        Event::CreatedItem {
            schema_id,
            id,
            oid,
            name,
            item,
        }
    }

    pub fn drop_database_ops(&mut self, name: String) -> Vec<Op> {
        let mut ops = vec![];
        let mut seen = HashSet::new();
        if let Some(database) = self.by_name.get(&name) {
            for (schema_name, schema) in &database.schemas {
                Self::drop_schema_items(schema, &self.by_id, &mut ops, &mut seen);
                ops.push(Op::DropSchema {
                    database_name: DatabaseSpecifier::Name(name.clone()),
                    schema_name: schema_name.clone(),
                });
            }
            ops.push(Op::DropDatabase { name });
        }
        ops
    }

    pub fn drop_schema_ops(&mut self, name: SchemaName) -> Vec<Op> {
        let mut ops = vec![];
        let mut seen = HashSet::new();
        if let DatabaseSpecifier::Name(database_name) = name.database {
            if let Some(database) = self.by_name.get(&database_name) {
                if let Some(schema) = database.schemas.get(&name.schema) {
                    Self::drop_schema_items(schema, &self.by_id, &mut ops, &mut seen);
                    ops.push(Op::DropSchema {
                        database_name: DatabaseSpecifier::Name(database_name),
                        schema_name: name.schema,
                    })
                }
            }
        }
        ops
    }

    pub fn drop_items_ops(&mut self, ids: &[GlobalId]) -> Vec<Op> {
        let mut ops = vec![];
        let mut seen = HashSet::new();
        for &id in ids {
            Self::drop_item_cascade(id, &self.by_id, &mut ops, &mut seen);
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
    fn temporary_ids(&mut self, ops: &[Op]) -> Result<Vec<GlobalId>, Error> {
        let mut creating = HashSet::new();
        let mut temporary_ids = Vec::new();
        for op in ops.iter() {
            if let Op::CreateItem {
                id,
                oid: _,
                name,
                item:
                    CatalogItem::View(View {
                        conn_id: Some(conn_id),
                        ..
                    }),
            } = op
            {
                if self.item_exists_in_temp_schemas(*conn_id, &name.item)
                    || creating.contains(&(conn_id, &name.item))
                {
                    return Err(Error::new(ErrorKind::ItemAlreadyExists(name.item.clone())));
                } else {
                    creating.insert((conn_id, &name.item));
                    temporary_ids.push(id.clone());
                }
            }
        }
        Ok(temporary_ids)
    }

    pub fn transact(&mut self, ops: Vec<Op>) -> Result<Vec<Event>, Error> {
        trace!("transact: {:?}", ops);

        #[derive(Debug, Clone)]
        enum Action {
            CreateDatabase {
                id: i64,
                oid: u32,
                name: String,
            },
            CreateSchema {
                id: i64,
                oid: u32,
                database_name: String,
                schema_name: String,
            },
            CreateItem {
                id: GlobalId,
                oid: u32,
                name: FullName,
                item: CatalogItem,
            },
            DropDatabase {
                name: String,
            },
            DropSchema {
                database_name: String,
                schema_name: String,
            },
            DropItem(GlobalId),
            UpdateItem {
                id: GlobalId,
                from_name: Option<FullName>,
                to_name: FullName,
                item: CatalogItem,
            },
        }

        let temporary_ids = self.temporary_ids(&ops)?;
        let drop_ids: HashSet<_> = ops
            .iter()
            .filter_map(|op| match op {
                Op::DropItem(id) => Some(*id),
                _ => None,
            })
            .collect();
        let mut actions = Vec::with_capacity(ops.len());
        let mut storage = self.storage();
        let mut tx = storage.transaction()?;
        for op in ops {
            actions.extend(match op {
                Op::CreateDatabase { name, oid } => vec![Action::CreateDatabase {
                    id: tx.insert_database(&name)?,
                    oid,
                    name,
                }],
                Op::CreateSchema {
                    database_name,
                    schema_name,
                    oid,
                } => {
                    if schema_name.starts_with("mz_") || schema_name.starts_with("pg_") {
                        return Err(Error::new(ErrorKind::UnacceptableSchemaName(schema_name)));
                    }
                    let (database_id, database_name) = match database_name {
                        DatabaseSpecifier::Name(name) => (tx.load_database_id(&name)?, name),
                        DatabaseSpecifier::Ambient => {
                            return Err(Error::new(ErrorKind::ReadOnlySystemSchema(schema_name)));
                        }
                    };
                    vec![Action::CreateSchema {
                        id: tx.insert_schema(database_id, &schema_name)?,
                        oid,
                        database_name,
                        schema_name,
                    }]
                }
                Op::CreateItem {
                    id,
                    oid,
                    name,
                    item,
                } => {
                    if item.is_temporary() {
                        if name.database != DatabaseSpecifier::Ambient
                            || name.schema != MZ_TEMP_SCHEMA
                        {
                            return Err(Error::new(ErrorKind::InvalidTemporarySchema));
                        }
                    } else {
                        if item.uses().iter().any(|id| match self.try_get_by_id(*id) {
                            Some(entry) => entry.item().is_temporary(),
                            None => temporary_ids.contains(&id),
                        }) {
                            return Err(Error::new(ErrorKind::InvalidTemporaryDependency(
                                id.to_string(),
                            )));
                        }
                        let database_id = match &name.database {
                            DatabaseSpecifier::Name(name) => tx.load_database_id(&name)?,
                            DatabaseSpecifier::Ambient => {
                                return Err(Error::new(ErrorKind::ReadOnlySystemSchema(
                                    name.to_string(),
                                )));
                            }
                        };
                        if let CatalogItem::Type(Type {
                            inner: TypeInner::Base { .. },
                            ..
                        }) = item
                        {
                            return Err(Error::new(ErrorKind::ReadOnlyItem(name.item)));
                        }

                        let schema_id = tx.load_schema_id(database_id, &name.schema)?;
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
                Op::DropDatabase { name } => {
                    tx.remove_database(&name)?;
                    vec![Action::DropDatabase { name }]
                }
                Op::DropSchema {
                    database_name,
                    schema_name,
                } => {
                    let (database_id, database_name) = match database_name {
                        DatabaseSpecifier::Name(name) => (tx.load_database_id(&name)?, name),
                        DatabaseSpecifier::Ambient => {
                            return Err(Error::new(ErrorKind::ReadOnlySystemSchema(schema_name)));
                        }
                    };
                    tx.remove_schema(database_id, &schema_name)?;
                    vec![Action::DropSchema {
                        database_name,
                        schema_name,
                    }]
                }
                Op::DropItem(id) => {
                    let entry = self.get_by_id(&id);
                    // Prevent dropping a table's default index unless the table
                    // is being dropped too.
                    if let CatalogItem::Index(Index { on, .. }) = entry.item() {
                        if self.get_by_id(on).is_table()
                            && self.default_index_for(*on) == Some(id)
                            && !drop_ids.contains(on)
                        {
                            return Err(Error::new(ErrorKind::MandatoryTableIndex(
                                entry.name().to_string(),
                            )));
                        }
                    }
                    if !entry.item().is_temporary() {
                        tx.remove_item(id)?;
                    }
                    vec![Action::DropItem(id)]
                }
                Op::RenameItem { id, to_name } => {
                    let mut actions = Vec::new();

                    let entry = self.by_id.get(&id).unwrap();
                    if let CatalogItem::Type(_) = entry.item() {
                        return Err(Error::new(ErrorKind::TypeRename(entry.name().to_string())));
                    }

                    let mut to_full_name = entry.name.clone();
                    to_full_name.item = to_name;

                    // Rename item itself.
                    let item = entry
                        .item
                        .rename_item_refs(entry.name.clone(), to_full_name.item.clone(), true)
                        .map_err(|e| {
                            Error::new(ErrorKind::AmbiguousRename {
                                depender: entry.name.to_string(),
                                dependee: entry.name.to_string(),
                                message: e,
                            })
                        })?;
                    let serialized_item = self.serialize_item(&item);

                    for id in entry.used_by() {
                        let dependent_item = self.by_id.get(&id).unwrap();
                        let updated_item = dependent_item
                            .item
                            .rename_item_refs(entry.name.clone(), to_full_name.item.clone(), false)
                            .map_err(|e| {
                                Error::new(ErrorKind::AmbiguousRename {
                                    depender: dependent_item.name.to_string(),
                                    dependee: entry.name.to_string(),
                                    message: e,
                                })
                            })?;

                        let serialized_item = self.serialize_item(&updated_item);

                        tx.update_item(id.clone(), &dependent_item.name.item, &serialized_item)?;
                        actions.push(Action::UpdateItem {
                            id: id.clone(),
                            from_name: None,
                            to_name: dependent_item.name.clone(),
                            item: updated_item,
                        });
                    }
                    tx.update_item(id.clone(), &to_full_name.item, &serialized_item)?;
                    actions.push(Action::UpdateItem {
                        id,
                        from_name: Some(entry.name.clone()),
                        to_name: to_full_name,
                        item,
                    });
                    actions
                }
            });
        }
        tx.commit()?;
        drop(storage); // release immutable borrow on `self` so we can borrow mutably below

        Ok(actions
            .into_iter()
            .map(|action| match action {
                Action::CreateDatabase { id, oid, name } => {
                    info!("create database {}", name);
                    self.by_name.insert(
                        name.clone(),
                        Database {
                            name: name.clone(),
                            id,
                            oid,
                            schemas: BTreeMap::new(),
                        },
                    );
                    Event::CreatedDatabase { name, id, oid }
                }

                Action::CreateSchema {
                    id,
                    oid,
                    database_name,
                    schema_name,
                } => {
                    info!("create schema {}.{}", database_name, schema_name);
                    let db = self.by_name.get_mut(&database_name).unwrap();
                    db.schemas.insert(
                        schema_name.clone(),
                        Schema {
                            name: SchemaName {
                                database: DatabaseSpecifier::Name(database_name),
                                schema: schema_name.clone(),
                            },
                            id,
                            oid,
                            items: BTreeMap::new(),
                        },
                    );
                    Event::CreatedSchema {
                        database_id: Some(db.id),
                        schema_id: id,
                        schema_name,
                        oid,
                    }
                }

                Action::CreateItem {
                    id,
                    oid,
                    name,
                    item,
                } => self.insert_item(id, oid, name, item),

                Action::DropDatabase { name } => match self.by_name.remove(&name) {
                    Some(db) => Event::DroppedDatabase {
                        name,
                        id: db.id,
                        oid: db.oid,
                    },
                    None => Event::NoOp,
                },

                Action::DropSchema {
                    database_name,
                    schema_name,
                } => {
                    let db = self.by_name.get_mut(&database_name).unwrap();
                    match db.schemas.remove(&schema_name) {
                        Some(schema) => Event::DroppedSchema {
                            database_id: db.id,
                            schema_id: schema.id,
                            schema_name,
                            oid: schema.oid,
                        },
                        None => Event::NoOp,
                    }
                }

                Action::DropItem(id) => {
                    let metadata = self.by_id.remove(&id).unwrap();
                    if !metadata.item.is_placeholder() {
                        info!(
                            "drop {} {} ({})",
                            metadata.item.type_string(),
                            metadata.name,
                            id
                        );
                    }
                    for u in metadata.uses() {
                        if let Some(dep_metadata) = self.by_id.get_mut(&u) {
                            dep_metadata.used_by.retain(|u| *u != metadata.id)
                        }
                    }

                    let conn_id = metadata.item.conn_id().unwrap_or(SYSTEM_CONN_ID);
                    let schema = self
                        .get_schema_mut(&metadata.name.database, &metadata.name.schema, conn_id)
                        .expect("catalog out of sync");
                    let schema_id = schema.id;
                    schema
                        .items
                        .remove(&metadata.name.item)
                        .expect("catalog out of sync");
                    if let CatalogItem::Index(index) = &metadata.item {
                        let indexes = self
                            .indexes
                            .get_mut(&index.on)
                            .expect("catalog out of sync");
                        let i = indexes
                            .iter()
                            .position(|(idx_id, _keys)| *idx_id == id)
                            .expect("catalog out of sync");
                        indexes.remove(i);
                        let nullable: Vec<bool> = index
                            .keys
                            .iter()
                            .map(|key| {
                                key.typ(self.get_by_id(&index.on).desc().unwrap().typ())
                                    .nullable
                            })
                            .collect();
                        Event::DroppedIndex {
                            entry: metadata,
                            nullable,
                        }
                    } else {
                        self.indexes.remove(&id);
                        Event::DroppedItem {
                            schema_id,
                            entry: metadata,
                        }
                    }
                }

                Action::UpdateItem {
                    id,
                    from_name,
                    to_name,
                    item,
                } => {
                    let mut entry = self.by_id.remove(&id).unwrap();
                    info!(
                        "update {} {} ({})",
                        entry.item.type_string(),
                        entry.name,
                        id
                    );
                    assert_eq!(entry.uses(), item.uses());
                    let conn_id = entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
                    let schema = &mut self
                        .get_schema_mut(&entry.name.database, &entry.name.schema, conn_id)
                        .expect("catalog out of sync");
                    let schema_id = schema.id;
                    schema.items.remove(&entry.name.item);
                    entry.name = to_name.clone();
                    entry.item = item.clone();
                    schema.items.insert(entry.name.item.clone(), id);
                    let oid = entry.oid();
                    self.by_id.insert(id, entry);

                    match from_name {
                        Some(from_name) => Event::UpdatedItem {
                            schema_id,
                            id,
                            oid,
                            from_name,
                            to_name,
                            item,
                        },
                        None => Event::NoOp, // If name didn't change, don't update system tables.
                    }
                }
            })
            .collect())
    }

    fn serialize_item(&self, item: &CatalogItem) -> Vec<u8> {
        let item = match item {
            CatalogItem::Table(table) => SerializedCatalogItem::V1 {
                create_sql: table.create_sql.clone(),
                eval_env: Some(table.plan_cx.clone().into()),
            },
            CatalogItem::Source(source) => SerializedCatalogItem::V1 {
                create_sql: source.create_sql.clone(),
                eval_env: Some(source.plan_cx.clone().into()),
            },
            CatalogItem::View(view) => SerializedCatalogItem::V1 {
                create_sql: view.create_sql.clone(),
                eval_env: Some(view.plan_cx.clone().into()),
            },
            CatalogItem::Index(index) => SerializedCatalogItem::V1 {
                create_sql: index.create_sql.clone(),
                eval_env: Some(index.plan_cx.clone().into()),
            },
            CatalogItem::Sink(sink) => SerializedCatalogItem::V1 {
                create_sql: sink.create_sql.clone(),
                eval_env: Some(sink.plan_cx.clone().into()),
            },
            CatalogItem::Type(typ) => SerializedCatalogItem::V1 {
                create_sql: typ.create_sql.clone(),
                eval_env: Some(typ.plan_cx.clone().into()),
            },
        };
        serde_json::to_vec(&item).expect("catalog serialization cannot fail")
    }

    fn deserialize_item(&self, bytes: Vec<u8>) -> Result<CatalogItem, anyhow::Error> {
        let SerializedCatalogItem::V1 {
            create_sql,
            eval_env,
        } = serde_json::from_slice(&bytes)?;
        let pcx = match eval_env {
            // Old sources and sinks don't have plan contexts, but it's safe to
            // just give them a default, as they clearly don't depend on the
            // plan context.
            None => PlanContext::default(),
            Some(eval_env) => eval_env.into(),
        };
        self.parse_item(create_sql, pcx)
    }

    fn parse_item(
        &self,
        create_sql: String,
        pcx: PlanContext,
    ) -> Result<CatalogItem, anyhow::Error> {
        let stmt = sql::parse::parse(&create_sql)?.into_element();
        let plan = sql::plan::plan(&pcx, &self.for_system_session(), stmt, &Params::empty())?;
        Ok(match plan {
            Plan::CreateTable { table, .. } => CatalogItem::Table(Table {
                create_sql: table.create_sql,
                plan_cx: pcx,
                desc: table.desc,
                defaults: table.defaults,
            }),
            Plan::CreateSource { source, .. } => CatalogItem::Source(Source {
                create_sql: source.create_sql,
                plan_cx: pcx,
                connector: source.connector,
                desc: source.desc,
            }),
            Plan::CreateView { view, .. } => {
                let mut optimizer = Optimizer::default();
                let optimized_expr = optimizer.optimize(view.expr, self.indexes())?;
                let desc = RelationDesc::new(optimized_expr.as_ref().typ(), view.column_names);
                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    plan_cx: pcx,
                    optimized_expr,
                    desc,
                    conn_id: None,
                })
            }
            Plan::CreateIndex { index, .. } => CatalogItem::Index(Index {
                create_sql: index.create_sql,
                plan_cx: pcx,
                on: index.on,
                keys: index.keys,
            }),
            Plan::CreateSink {
                sink,
                with_snapshot,
                as_of,
                ..
            } => CatalogItem::Sink(Sink {
                create_sql: sink.create_sql,
                plan_cx: pcx,
                from: sink.from,
                connector: SinkConnectorState::Pending(sink.connector_builder),
                with_snapshot,
                as_of,
            }),
            Plan::CreateType { typ, .. } => CatalogItem::Type(Type {
                create_sql: typ.create_sql,
                plan_cx: pcx,
                inner: typ.inner.into(),
            }),
            _ => bail!("catalog entry generated inappropriate plan"),
        })
    }

    /// Returns a mapping that indicates all indices that are available for
    /// each item in the catalog.
    pub fn indexes(&self) -> &HashMap<GlobalId, Vec<(GlobalId, Vec<ScalarExpr>)>> {
        &self.indexes
    }

    /// Returns the default index for the specified `id`.
    ///
    /// Panics if `id` does not exist, or if `id` is not an object on which
    /// indexes can be built.
    pub fn default_index_for(&self, id: GlobalId) -> Option<GlobalId> {
        // The default index is just whatever index happens to appear first in
        // self.indexes.
        self.indexes[&id].first().map(|(id, _keys)| *id)
    }

    /// Finds the nearest indexes that can satisfy the views or sources whose
    /// identifiers are listed in `ids`.
    ///
    /// Returns the identifiers of all discovered indexes, along with a boolean
    /// indicating whether the set of indexes is complete. If incomplete, then
    /// one of the provided identifiers transitively depends on an
    /// unmaterialized source.
    pub fn nearest_indexes(&self, ids: &[GlobalId]) -> (Vec<GlobalId>, bool) {
        fn inner(
            catalog: &Catalog,
            id: GlobalId,
            indexes: &mut Vec<GlobalId>,
            complete: &mut bool,
        ) {
            // If an index exists for `id`, record it in the output set and stop
            // searching.
            if let Some((index_id, _)) = catalog.indexes[&id].first() {
                indexes.push(*index_id);
                return;
            }

            match catalog.get_by_id(&id).item() {
                view @ CatalogItem::View(_) => {
                    // Unmaterialized view. Recursively search its dependencies.
                    for id in view.uses() {
                        inner(catalog, id, indexes, complete)
                    }
                }
                CatalogItem::Source(_) => {
                    // Unmaterialized source. Record that we are missing at
                    // least one index.
                    *complete = false;
                }
                CatalogItem::Table(_) => {
                    unreachable!("tables always have at least one index");
                }
                CatalogItem::Sink(_) | CatalogItem::Index(_) | CatalogItem::Type(_) => {
                    unreachable!("sinks, indexes, and user-defined types cannot be depended upon");
                }
            }
        }

        let mut indexes = vec![];
        let mut complete = true;
        for id in ids {
            inner(self, *id, &mut indexes, &mut complete)
        }
        indexes.sort();
        indexes.dedup();
        (indexes, complete)
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        match self.get_by_id(&id).item() {
            CatalogItem::Table(_) => true,
            CatalogItem::Source(_) => false,
            item @ CatalogItem::View(_) => item.uses().into_iter().any(|id| self.uses_tables(id)),
            CatalogItem::Sink(_) | CatalogItem::Index(_) | CatalogItem::Type(_) => {
                unreachable!("sinks, indexes, and user-defined types cannot be depended upon");
            }
        }
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> String {
        serde_json::to_string(&self.by_name).expect("serialization cannot fail")
    }

    pub fn config(&self) -> &sql::catalog::CatalogConfig {
        &self.config
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    CreateDatabase {
        name: String,
        oid: u32,
    },
    CreateSchema {
        database_name: DatabaseSpecifier,
        schema_name: String,
        oid: u32,
    },
    CreateItem {
        id: GlobalId,
        oid: u32,
        name: FullName,
        item: CatalogItem,
    },
    DropDatabase {
        name: String,
    },
    DropSchema {
        database_name: DatabaseSpecifier,
        schema_name: String,
    },
    /// Unconditionally removes the identified items. It is required that the
    /// IDs come from the output of `plan_remove`; otherwise consistency rules
    /// may be violated.
    DropItem(GlobalId),
    RenameItem {
        id: GlobalId,
        to_name: String,
    },
}

#[derive(Debug, Clone)]
pub enum Event {
    CreatedDatabase {
        name: String,
        id: i64,
        oid: u32,
    },
    CreatedSchema {
        database_id: Option<i64>,
        schema_id: i64,
        schema_name: String,
        oid: u32,
    },
    CreatedItem {
        schema_id: i64,
        id: GlobalId,
        oid: u32,
        name: FullName,
        item: CatalogItem,
    },
    DroppedDatabase {
        name: String,
        id: i64,
        oid: u32,
    },
    DroppedSchema {
        database_id: i64,
        schema_id: i64,
        schema_name: String,
        oid: u32,
    },
    DroppedIndex {
        entry: CatalogEntry,
        nullable: Vec<bool>,
    },
    DroppedItem {
        schema_id: i64,
        entry: CatalogEntry,
    },
    UpdatedItem {
        schema_id: i64,
        id: GlobalId,
        oid: u32,
        from_name: FullName,
        to_name: FullName,
        item: CatalogItem,
    },
    NoOp,
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
    fn resolve_item_name(&self, name: &PartialName) -> Result<&FullName, SqlCatalogError> {
        self.resolve_item(name).map(|entry| entry.name())
    }

    fn minimal_qualification(&self, full_name: &FullName) -> PartialName {
        let database = match &full_name.database {
            DatabaseSpecifier::Ambient => None,
            DatabaseSpecifier::Name(n) if *n == self.database => None,
            DatabaseSpecifier::Name(n) => Some(n.clone()),
        };

        let schema = if database.is_none()
            && self.resolve_item_name(&PartialName {
                database: None,
                schema: None,
                item: full_name.item.clone(),
            }) == Ok(full_name)
        {
            None
        } else {
            // If `search_path` does not contain `full_name.schema`, the
            // `PartialName` must contain it.
            Some(full_name.schema.clone())
        };

        let res = PartialName {
            database,
            schema,
            item: full_name.item.clone(),
        };
        assert_eq!(self.resolve_item_name(&res), Ok(full_name));
        res
    }
}

impl ExprHumanizer for ConnCatalog<'_> {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        self.catalog
            .by_id
            .get(&id)
            .map(|entry| entry.name.to_string())
    }

    fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        use ScalarType::*;

        match typ {
            Array(t) => format!("{}[]", self.humanize_scalar_type(t)),
            List { custom_oid, .. } | Map { custom_oid, .. } if custom_oid.is_some() => {
                let full_name = self.get_item_by_oid(&custom_oid.unwrap()).name();
                self.minimal_qualification(full_name).to_string()
            }
            List { element_type, .. } => {
                format!("{} list", self.humanize_scalar_type(element_type))
            }
            Map { value_type, .. } => format!(
                "map[{}=>{}]",
                self.humanize_scalar_type(&ScalarType::String),
                self.humanize_scalar_type(value_type)
            ),
            Record { fields } => format!(
                "record({})",
                fields
                    .iter()
                    .map(|f| format!("{}: {}", f.0, self.humanize_column_type(&f.1)))
                    .join(",")
            ),
            ty => {
                let pgrepr_type = pgrepr::Type::from(ty);
                let res = if self
                    .search_path
                    .iter()
                    .any(|schema| schema == &PG_CATALOG_SCHEMA)
                {
                    pgrepr_type.name().to_string()
                } else {
                    // If PG_CATALOG_SCHEMA is not in search path, you need
                    // qualified object name to refer to type.
                    self.get_item_by_oid(&pgrepr_type.oid()).name().to_string()
                };
                if let ScalarType::Decimal(p, s) = typ {
                    format!("{}({},{})", res, p, s)
                } else {
                    res
                }
            }
        }
    }

    fn humanize_column_type(&self, typ: &ColumnType) -> String {
        format!(
            "{}{}",
            self.humanize_scalar_type(&typ.scalar_type),
            if typ.nullable { "?" } else { "" }
        )
    }
}

impl SqlCatalog for ConnCatalog<'_> {
    fn search_path(&self, include_system_schemas: bool) -> Vec<&str> {
        if include_system_schemas {
            self.search_path.to_vec()
        } else {
            self.search_path
                .iter()
                .filter(|s| {
                    (**s != PG_CATALOG_SCHEMA)
                        && (**s != MZ_CATALOG_SCHEMA)
                        && (**s != MZ_TEMP_SCHEMA)
                })
                .cloned()
                .collect()
        }
    }

    fn default_database(&self) -> &str {
        &self.database
    }

    fn resolve_database(
        &self,
        database_name: &str,
    ) -> Result<&dyn sql::catalog::CatalogDatabase, SqlCatalogError> {
        match self.catalog.by_name.get(database_name) {
            Some(database) => Ok(database),
            None => Err(SqlCatalogError::UnknownDatabase(database_name.into())),
        }
    }

    fn resolve_schema(
        &self,
        database: Option<String>,
        schema_name: &str,
    ) -> Result<&dyn sql::catalog::CatalogSchema, SqlCatalogError> {
        Ok(self
            .catalog
            .resolve_schema(&self.database, database, schema_name, self.conn_id)?)
    }

    fn resolve_item(
        &self,
        name: &PartialName,
    ) -> Result<&dyn sql::catalog::CatalogItem, SqlCatalogError> {
        Ok(self
            .catalog
            .resolve(&self.database, self.search_path, name, self.conn_id)?)
    }

    fn list_items<'a>(
        &'a self,
        schema: &SchemaName,
    ) -> Box<dyn Iterator<Item = &'a dyn sql::catalog::CatalogItem> + 'a> {
        let schema = self
            .catalog
            .get_schema(&schema.database, &schema.schema, self.conn_id)
            .unwrap();
        Box::new(
            schema
                .items
                .values()
                .map(move |id| self.catalog.get_by_id(id) as &dyn sql::catalog::CatalogItem),
        )
    }

    fn get_item_by_id(&self, id: &GlobalId) -> &dyn sql::catalog::CatalogItem {
        self.catalog.get_by_id(id)
    }

    fn get_item_by_oid(&self, oid: &u32) -> &dyn sql::catalog::CatalogItem {
        let id = self.catalog.by_oid[oid];
        self.catalog.get_by_id(&id)
    }

    fn item_exists(&self, name: &FullName) -> bool {
        self.catalog.try_get(name, self.conn_id).is_some()
    }

    fn try_get_lossy_scalar_type_by_id(&self, id: &GlobalId) -> Option<ScalarType> {
        let entry = self.catalog.get_by_id(id);
        let t = match entry.item() {
            CatalogItem::Type(t) => t,
            _ => return None,
        };

        Some(match t.inner {
            TypeInner::Array { element_id } => {
                let element_type = self
                    .try_get_lossy_scalar_type_by_id(&element_id)
                    .expect("array's element_id refers to a valid type");
                ScalarType::Array(Box::new(element_type))
            }
            TypeInner::Base => pgrepr::Type::from_oid(entry.oid())?.to_scalar_type_lossy(),
            TypeInner::List { element_id } => {
                let element_type = self
                    .try_get_lossy_scalar_type_by_id(&element_id)
                    .expect("list's element_id refers to a valid type");
                ScalarType::List {
                    element_type: Box::new(element_type),
                    custom_oid: Some(entry.oid),
                }
            }
            TypeInner::Map { key_id, value_id } => {
                let key_type = self
                    .try_get_lossy_scalar_type_by_id(&key_id)
                    .expect("map's key_id refers to a valid type");
                assert!(matches!(key_type, ScalarType::String));
                let value_type = Box::new(
                    self.try_get_lossy_scalar_type_by_id(&value_id)
                        .expect("map's value_id refers to a valid type"),
                );
                ScalarType::Map {
                    value_type,
                    custom_oid: Some(entry.oid),
                }
            }
        })
    }

    fn config(&self) -> &sql::catalog::CatalogConfig {
        &self.catalog.config
    }
}

impl sql::catalog::CatalogDatabase for Database {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> i64 {
        self.id
    }
}

impl sql::catalog::CatalogSchema for Schema {
    fn name(&self) -> &SchemaName {
        &self.name
    }

    fn id(&self) -> i64 {
        self.id
    }
}

impl sql::catalog::CatalogItem for CatalogEntry {
    fn name(&self) -> &FullName {
        self.name()
    }

    fn id(&self) -> GlobalId {
        self.id()
    }

    fn oid(&self) -> u32 {
        self.oid()
    }

    fn desc(&self) -> Result<&RelationDesc, SqlCatalogError> {
        Ok(self.desc()?)
    }

    fn create_sql(&self) -> &str {
        match self.item() {
            CatalogItem::Table(Table { create_sql, .. }) => create_sql,
            CatalogItem::Source(Source { create_sql, .. }) => create_sql,
            CatalogItem::Sink(Sink { create_sql, .. }) => create_sql,
            CatalogItem::View(View { create_sql, .. }) => create_sql,
            CatalogItem::Index(Index { create_sql, .. }) => create_sql,
            CatalogItem::Type(Type { create_sql, .. }) => create_sql,
        }
    }

    fn plan_cx(&self) -> &PlanContext {
        match self.item() {
            CatalogItem::Table(Table { plan_cx, .. }) => plan_cx,
            CatalogItem::Source(Source { plan_cx, .. }) => plan_cx,
            CatalogItem::Sink(Sink { plan_cx, .. }) => plan_cx,
            CatalogItem::View(View { plan_cx, .. }) => plan_cx,
            CatalogItem::Index(Index { plan_cx, .. }) => plan_cx,
            CatalogItem::Type(Type { plan_cx, .. }) => plan_cx,
        }
    }

    fn item_type(&self) -> sql::catalog::CatalogItemType {
        match self.item() {
            CatalogItem::Table(_) => sql::catalog::CatalogItemType::Table,
            CatalogItem::Source(_) => sql::catalog::CatalogItemType::Source,
            CatalogItem::Sink(_) => sql::catalog::CatalogItemType::Sink,
            CatalogItem::View(_) => sql::catalog::CatalogItemType::View,
            CatalogItem::Index(_) => sql::catalog::CatalogItemType::Index,
            CatalogItem::Type(_) => sql::catalog::CatalogItemType::Type,
        }
    }

    fn index_details(&self) -> Option<(&[ScalarExpr], GlobalId)> {
        if let CatalogItem::Index(Index { keys, on, .. }) = self.item() {
            Some((keys, *on))
        } else {
            None
        }
    }

    fn table_details(&self) -> Option<&[Expr]> {
        if let CatalogItem::Table(Table { defaults, .. }) = self.item() {
            Some(defaults)
        } else {
            None
        }
    }

    fn uses(&self) -> Vec<GlobalId> {
        self.uses()
    }

    fn used_by(&self) -> &[GlobalId] {
        self.used_by()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use sql::names::{DatabaseSpecifier, FullName, PartialName};

    use crate::catalog::{Catalog, MZ_CATALOG_SCHEMA, PG_CATALOG_SCHEMA};
    use crate::session::Session;

    /// System sessions have an empty `search_path` so it's necessary to
    /// schema-qualify all referenced items.
    ///
    /// Dummy (and ostensibly client) sessions contain system schemas in their
    /// search paths, so do not require schema qualification on system objects such
    /// as types.
    #[test]
    fn test_minimal_qualification() -> Result<(), anyhow::Error> {
        struct TestCase {
            input: FullName,
            system_output: PartialName,
            normal_output: PartialName,
        }

        let test_cases = vec![
            TestCase {
                input: FullName {
                    database: DatabaseSpecifier::Ambient,
                    schema: PG_CATALOG_SCHEMA.to_string(),
                    item: "numeric".to_string(),
                },
                system_output: PartialName {
                    database: None,
                    schema: Some(PG_CATALOG_SCHEMA.to_string()),
                    item: "numeric".to_string(),
                },
                normal_output: PartialName {
                    database: None,
                    schema: None,
                    item: "numeric".to_string(),
                },
            },
            TestCase {
                input: FullName {
                    database: DatabaseSpecifier::Ambient,
                    schema: MZ_CATALOG_SCHEMA.to_string(),
                    item: "mz_array_types".to_string(),
                },
                system_output: PartialName {
                    database: None,
                    schema: Some(MZ_CATALOG_SCHEMA.to_string()),
                    item: "mz_array_types".to_string(),
                },
                normal_output: PartialName {
                    database: None,
                    schema: None,
                    item: "mz_array_types".to_string(),
                },
            },
        ];

        let catalog_file = NamedTempFile::new()?;
        let catalog = Catalog::open_debug(catalog_file.path())?;
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
}
