// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use anyhow::bail;
use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use log::{info, trace};
use ore::collections::CollectionExt;
use regex::Regex;
use serde::{Deserialize, Serialize};

use dataflow_types::{SinkConnector, SinkConnectorBuilder, SourceConnector};
use expr::{GlobalId, Id, IdHumanizer, OptimizedRelationExpr, ScalarExpr};
use repr::{RelationDesc, Row};
use sql::ast::display::AstDisplay;
use sql::catalog::CatalogError as SqlCatalogError;
use sql::names::{DatabaseSpecifier, FullName, PartialName};
use sql::plan::{Params, Plan, PlanContext};
use transform::Optimizer;

use crate::catalog::error::{Error, ErrorKind};
use crate::session::Session;

pub mod builtin;
mod error;
pub mod storage;

const SYSTEM_CONN_ID: u32 = 0;

pub const MZ_TEMP_SCHEMA: &str = "mz_temp";

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
    indexes: HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ambient_schemas: BTreeMap<String, Schema>,
    temporary_schemas: HashMap<u32, Schema>,
    storage: Arc<Mutex<storage::Connection>>,
    startup_time: SystemTime,
    nonce: u64,
    experimental_mode: bool,
}

#[derive(Debug)]
pub struct ConnCatalog<'a> {
    catalog: &'a Catalog,
    conn_id: u32,
    database: String,
    search_path: &'a [&'a str],
}

impl ConnCatalog<'_> {
    fn database_spec(&self) -> DatabaseSpecifier {
        DatabaseSpecifier::Name(self.database.clone())
    }
}

#[derive(Debug, Serialize)]
struct Database {
    id: i64,
    schemas: BTreeMap<String, Schema>,
}

lazy_static! {
    static ref EMPTY_DATABASE: Database = Database {
        id: 0,
        schemas: BTreeMap::new(),
    };
}

#[derive(Debug, Serialize)]
pub struct Schema {
    id: i64,
    items: BTreeMap<String, GlobalId>,
}

#[derive(Clone, Debug)]
pub struct CatalogEntry {
    item: CatalogItem,
    used_by: Vec<GlobalId>,
    id: GlobalId,
    name: FullName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogItem {
    Source(Source),
    View(View),
    Sink(Sink),
    Index(Index),
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

impl CatalogItem {
    /// Returns a string indicating the type of this catalog entry.
    pub fn type_string(&self) -> &'static str {
        match self {
            CatalogItem::Source(_) => "source",
            CatalogItem::Sink(_) => "sink",
            CatalogItem::View(_) => "view",
            CatalogItem::Index(_) => "index",
        }
    }

    /// Collects the identifiers of the dataflows that this item depends
    /// upon.
    pub fn uses(&self) -> Vec<GlobalId> {
        match self {
            CatalogItem::Source(_) => vec![],
            CatalogItem::Sink(sink) => vec![sink.from],
            CatalogItem::View(view) => {
                let mut out = Vec::new();
                view.optimized_expr.as_ref().global_uses(&mut out);
                out
            }
            CatalogItem::Index(idx) => vec![idx.on],
        }
    }

    /// Indicates whether this item is a placeholder for a future item
    /// or if it's actually a real item.
    pub fn is_placeholder(&self) -> bool {
        match self {
            CatalogItem::Source(_) | CatalogItem::View(_) | CatalogItem::Index(_) => false,
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
            let mut create_stmt = sql::parse::parse(create_sql).unwrap().into_element();
            if rename_self {
                sql::ast::transform::create_stmt_rename(&mut create_stmt, to_item_name.clone());
            }
            // Determination of what constitutes an ambiguous request is done here.
            sql::ast::transform::create_stmt_rename_refs(&mut create_stmt, from, to_item_name)?;
            Ok(create_stmt.to_ast_string_stable())
        };

        match self {
            CatalogItem::Index(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Index(i))
            }
            CatalogItem::Sink(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Sink(i))
            }
            CatalogItem::Source(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Source(i))
            }
            CatalogItem::View(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::View(i))
            }
        }
    }
}

impl CatalogEntry {
    /// Reports the description of the datums produced by this catalog item.
    pub fn desc(&self) -> Result<&RelationDesc, SqlCatalogError> {
        match &self.item {
            CatalogItem::Source(src) => Ok(&src.desc),
            CatalogItem::Sink(_) => Err(SqlCatalogError::InvalidSinkDependency(
                self.name.to_string(),
            )),
            CatalogItem::View(view) => Ok(&view.desc),
            CatalogItem::Index(_) => Err(SqlCatalogError::InvalidIndexDependency(
                self.name.to_string(),
            )),
        }
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
    /// Opens or creates a `Catalog` that stores data at `path`. The
    /// `initialize` callback will be invoked after database and schemas are
    /// loaded but before any persisted user items are loaded.
    pub fn open<F>(
        path: Option<&Path>,
        experimental_mode: Option<bool>,
        f: F,
    ) -> Result<Catalog, Error>
    where
        F: FnOnce(&mut Self),
    {
        let (storage, experimental_mode) = storage::Connection::open(path, experimental_mode)?;

        let mut catalog = Catalog {
            by_name: BTreeMap::new(),
            by_id: BTreeMap::new(),
            indexes: HashMap::new(),
            ambient_schemas: BTreeMap::new(),
            temporary_schemas: HashMap::new(),
            storage: Arc::new(Mutex::new(storage)),
            startup_time: SystemTime::now(),
            nonce: rand::random(),
            experimental_mode,
        };
        catalog.create_temporary_schema(SYSTEM_CONN_ID);

        let databases = catalog.storage().load_databases()?;
        for (id, name) in databases {
            catalog.by_name.insert(
                name,
                Database {
                    id,
                    schemas: BTreeMap::new(),
                },
            );
        }

        let schemas = catalog.storage().load_schemas()?;
        for (id, database_name, schema_name) in schemas {
            let schemas = match database_name {
                Some(database_name) => {
                    &mut catalog
                        .by_name
                        .get_mut(&database_name)
                        .expect("catalog out of sync")
                        .schemas
                }
                None => &mut catalog.ambient_schemas,
            };
            schemas.insert(
                schema_name,
                Schema {
                    id,
                    items: BTreeMap::new(),
                },
            );
        }

        // Invoke callback so that it can install system items. This has to be
        // done after databases and schemas are loaded, but before any items, as
        // items might depend on these system items, but these system items
        // depend on the system database/schema being installed.
        f(&mut catalog);

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
            catalog.insert_item(id, name, item);
        }

        Ok(catalog)
    }

    pub fn for_session(&self, session: &Session) -> ConnCatalog {
        ConnCatalog {
            catalog: self,
            conn_id: session.conn_id(),
            database: session.database().into(),
            search_path: session.search_path(),
        }
    }

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

    pub fn resolve_schema(
        &self,
        current_database: &DatabaseSpecifier,
        database: Option<String>,
        schema_name: &str,
        conn_id: u32,
    ) -> Result<DatabaseSpecifier, SqlCatalogError> {
        if let Some(database) = database {
            let database_spec = DatabaseSpecifier::Name(database);
            self.get_schema(&database_spec, schema_name, conn_id)
                .map(|_| database_spec)
        } else {
            match self.get_schema(current_database, schema_name, conn_id) {
                Ok(_) => Ok(current_database.clone()),
                Err(SqlCatalogError::UnknownSchema(_)) => self
                    .get_schema(&DatabaseSpecifier::Ambient, schema_name, conn_id)
                    .map(|_| DatabaseSpecifier::Ambient),
                Err(e) => Err(e),
            }
        }
    }

    /// Resolves [`PartialName`] into a [`FullName`].
    ///
    /// If `name` does not specify a database, the `current_database` is used.
    /// If `name` does not specify a schema, then the schemas in `search_path`
    /// are searched in order.
    #[allow(clippy::useless_let_if_seq)]
    pub fn resolve(
        &self,
        current_database: DatabaseSpecifier,
        search_path: &[&str],
        name: &PartialName,
        conn_id: u32,
    ) -> Result<FullName, SqlCatalogError> {
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
            let res = self.resolve_schema(&current_database, database_name, schema_name, conn_id);
            let database_spec = match res {
                Ok(database_spec) => database_spec,
                Err(SqlCatalogError::UnknownSchema(_)) => continue,
                Err(e) => return Err(e),
            };
            if let Ok(schema) = self.get_schema(&database_spec, schema_name, conn_id) {
                if schema.items.contains_key(&name.item) {
                    return Ok(FullName {
                        database: database_spec,
                        schema: schema_name.to_owned(),
                        item: name.item.to_owned(),
                    });
                }
            }
        }
        Err(SqlCatalogError::UnknownItem(name.to_string()))
    }

    /// Returns the named catalog item, if it exists.
    ///
    /// See also [`Catalog::get`].
    pub fn try_get(&self, name: &FullName, conn_id: u32) -> Option<&CatalogEntry> {
        self.get_schema(&name.database, &name.schema, conn_id)
            .ok()
            .and_then(|schema| schema.items.get(&name.item))
            .map(|id| &self.by_id[id])
    }

    /// Returns the named catalog item, or an error if it does not exist.
    ///
    /// See also [`Catalog::try_get`].
    pub fn get(&self, name: &FullName, conn_id: u32) -> Result<&CatalogEntry, SqlCatalogError> {
        self.try_get(name, conn_id)
            .ok_or_else(|| SqlCatalogError::UnknownItem(name.to_string()))
    }

    pub fn try_get_by_id(&self, id: GlobalId) -> Option<&CatalogEntry> {
        self.by_id.get(&id)
    }

    pub fn get_by_id(&self, id: &GlobalId) -> &CatalogEntry {
        &self.by_id[id]
    }

    /// Returns an iterator over the name of each database in the catalog.
    pub fn databases(&self) -> impl Iterator<Item = &str> {
        self.by_name.keys().map(String::as_str)
    }

    /// Creates a new schema in the `Catalog` for temporary items
    /// indicated by the TEMPORARY or TEMP keywords.
    pub fn create_temporary_schema(&mut self, conn_id: u32) {
        self.temporary_schemas.insert(
            conn_id,
            Schema {
                id: -1,
                items: BTreeMap::new(),
            },
        );
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
            return Err(Error::new(ErrorKind::SchemaNotEmpty("mz_temp".into())));
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
    ) -> Result<&Schema, SqlCatalogError> {
        // Keep in sync with `get_schemas_mut`.
        match database_spec {
            DatabaseSpecifier::Ambient if schema_name == MZ_TEMP_SCHEMA => {
                Ok(&self.temporary_schemas[&conn_id])
            }
            DatabaseSpecifier::Ambient => match self.ambient_schemas.get(schema_name) {
                Some(schema) => Ok(schema),
                None => Err(SqlCatalogError::UnknownSchema(schema_name.into())),
            },
            DatabaseSpecifier::Name(name) => match self.by_name.get(name) {
                Some(db) => match db.schemas.get(schema_name) {
                    Some(schema) => Ok(schema),
                    None => Err(SqlCatalogError::UnknownSchema(schema_name.into())),
                },
                None => Err(SqlCatalogError::UnknownDatabase(name.to_owned())),
            },
        }
    }

    /// Like `get_schemas`, but returns a `mut` reference.
    fn get_schema_mut(
        &mut self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
    ) -> Result<&mut Schema, SqlCatalogError> {
        // Keep in sync with `get_schemas`.
        match database_spec {
            DatabaseSpecifier::Ambient if schema_name == MZ_TEMP_SCHEMA => {
                Ok(self.temporary_schemas.get_mut(&conn_id).unwrap())
            }
            DatabaseSpecifier::Ambient => match self.ambient_schemas.get_mut(schema_name) {
                Some(schema) => Ok(schema),
                None => Err(SqlCatalogError::UnknownSchema(schema_name.into())),
            },
            DatabaseSpecifier::Name(name) => match self.by_name.get_mut(name) {
                Some(db) => match db.schemas.get_mut(schema_name) {
                    Some(schema) => Ok(schema),
                    None => Err(SqlCatalogError::UnknownSchema(schema_name.into())),
                },
                None => Err(SqlCatalogError::UnknownDatabase(name.to_owned())),
            },
        }
    }

    pub fn insert_item(&mut self, id: GlobalId, name: FullName, item: CatalogItem) {
        if !item.is_placeholder() {
            info!("create {} {} ({})", item.type_string(), name, id);
        }

        let entry = CatalogEntry {
            item,
            name,
            id,
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

        if let CatalogItem::Index(index) = entry.item() {
            self.indexes
                .entry(index.on)
                .or_insert_with(Vec::new)
                .push(index.keys.clone());
        }
        let conn_id = entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
        self.get_schema_mut(&entry.name.database, &entry.name.schema, conn_id)
            .expect("catalog out of sync")
            .items
            .insert(entry.name.item.clone(), entry.id);
        self.by_id.insert(entry.id, entry);
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

    pub fn drop_schema_ops(
        &mut self,
        database_spec: DatabaseSpecifier,
        schema_name: String,
    ) -> Vec<Op> {
        let mut ops = vec![];
        let mut seen = HashSet::new();
        if let DatabaseSpecifier::Name(database_name) = database_spec {
            if let Some(database) = self.by_name.get(&database_name) {
                if let Some(schema) = database.schemas.get(&schema_name) {
                    Self::drop_schema_items(schema, &self.by_id, &mut ops, &mut seen);
                    ops.push(Op::DropSchema {
                        database_name: DatabaseSpecifier::Name(database_name),
                        schema_name,
                    })
                }
            }
        }
        ops
    }

    pub fn drop_items_ops(&mut self, ids: &[GlobalId]) -> Vec<Op> {
        let mut ops = vec![];
        for &id in ids {
            Self::drop_item_cascade(id, &self.by_id, &mut ops, &mut HashSet::new());
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

    pub fn transact(&mut self, ops: Vec<Op>) -> Result<Vec<OpStatus>, Error> {
        trace!("transact: {:?}", ops);

        #[derive(Debug, Clone)]
        enum Action {
            CreateDatabase {
                id: i64,
                name: String,
            },
            CreateSchema {
                id: i64,
                database_name: String,
                schema_name: String,
            },
            CreateItem {
                id: GlobalId,
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
                name: FullName,
                item: CatalogItem,
            },
        }

        let temporary_ids = self.temporary_ids(&ops)?;
        let mut actions = Vec::with_capacity(ops.len());
        let mut storage = self.storage();
        let mut tx = storage.transaction()?;
        for op in ops {
            actions.extend(match op {
                Op::CreateDatabase { name } => vec![Action::CreateDatabase {
                    id: tx.insert_database(&name)?,
                    name,
                }],
                Op::CreateSchema {
                    database_name,
                    schema_name,
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
                        database_name,
                        schema_name,
                    }]
                }
                Op::CreateItem { id, name, item } => {
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
                        let schema_id = tx.load_schema_id(database_id, &name.schema)?;
                        let serialized_item = self.serialize_item(&item);
                        tx.insert_item(id, schema_id, &name.item, &serialized_item)?;
                    }

                    vec![Action::CreateItem { id, name, item }]
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
                    if !self.get_by_id(&id).item().is_temporary() {
                        tx.remove_item(id)?;
                    }
                    vec![Action::DropItem(id)]
                }
                Op::RenameItem { id, to_name } => {
                    let mut actions = Vec::new();

                    let entry = self.by_id.get(&id).unwrap();

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
                            name: dependent_item.name.clone(),
                            item: updated_item,
                        });
                    }
                    tx.update_item(id.clone(), &to_full_name.item, &serialized_item)?;
                    actions.push(Action::UpdateItem {
                        id,
                        name: to_full_name,
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
                Action::CreateDatabase { id, name } => {
                    info!("create database {}", name);
                    self.by_name.insert(
                        name,
                        Database {
                            id,
                            schemas: BTreeMap::new(),
                        },
                    );
                    OpStatus::CreatedDatabase
                }

                Action::CreateSchema {
                    id,
                    database_name,
                    schema_name,
                } => {
                    info!("create schema {}.{}", database_name, schema_name);
                    self.by_name
                        .get_mut(&database_name)
                        .unwrap()
                        .schemas
                        .insert(
                            schema_name,
                            Schema {
                                id,
                                items: BTreeMap::new(),
                            },
                        );
                    OpStatus::CreatedSchema
                }

                Action::CreateItem { id, name, item } => {
                    self.insert_item(id, name, item);
                    OpStatus::CreatedItem(id)
                }

                Action::DropDatabase { name } => {
                    self.by_name.remove(&name);
                    OpStatus::DroppedDatabase
                }

                Action::DropSchema {
                    database_name,
                    schema_name,
                } => {
                    self.by_name
                        .get_mut(&database_name)
                        .unwrap()
                        .schemas
                        .remove(&schema_name);
                    OpStatus::DroppedSchema
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
                    self.get_schema_mut(&metadata.name.database, &metadata.name.schema, conn_id)
                        .expect("catalog out of sync")
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
                            .position(|keys| keys == &index.keys)
                            .expect("catalog out of sync");
                        indexes.remove(i);
                    }
                    OpStatus::DroppedItem(metadata)
                }

                Action::UpdateItem { id, name, item } => {
                    let mut entry = self.by_id.remove(&id).unwrap();
                    info!(
                        "update {} {} ({})",
                        entry.item.type_string(),
                        entry.name,
                        id
                    );
                    assert_eq!(entry.uses(), item.uses());
                    let conn_id = entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
                    let schema_items = &mut self
                        .get_schema_mut(&entry.name.database, &entry.name.schema, conn_id)
                        .expect("catalog out of sync")
                        .items;
                    schema_items.remove(&entry.name.item);
                    entry.name = name;
                    entry.item = item;
                    schema_items.insert(entry.name.item.clone(), id);
                    self.by_id.insert(id, entry);

                    OpStatus::UpdatedItem
                }
            })
            .collect())
    }

    fn serialize_item(&self, item: &CatalogItem) -> Vec<u8> {
        let item = match item {
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
        };
        serde_json::to_vec(&item).expect("catalog serialization cannot fail")
    }

    fn deserialize_item(&self, bytes: Vec<u8>) -> Result<CatalogItem, anyhow::Error> {
        let SerializedCatalogItem::V1 {
            create_sql,
            eval_env,
        } = serde_json::from_slice(&bytes)?;
        let params = Params {
            datums: Row::pack(&[]),
            types: vec![],
        };
        let pcx = match eval_env {
            // Old sources and sinks don't have plan contexts, but it's safe to
            // just give them a default, as they clearly don't depend on the
            // plan context.
            None => PlanContext::default(),
            Some(eval_env) => eval_env.into(),
        };
        let stmt = sql::parse::parse(create_sql)?.into_element();
        let plan = sql::plan::plan(&pcx, &self.for_system_session(), stmt, &params)?;
        Ok(match plan {
            Plan::CreateSource { source, .. } | Plan::CreateTable { source, .. } => {
                CatalogItem::Source(Source {
                    create_sql: source.create_sql,
                    plan_cx: pcx,
                    connector: source.connector,
                    desc: source.desc,
                })
            }
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
            _ => bail!("catalog entry generated inappropriate plan"),
        })
    }

    /// Iterates over the items in the catalog in order of increasing ID.
    pub fn iter(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.by_id.iter().map(|(_id, entry)| entry)
    }

    /// Returns a mapping that indicates all indices that are available for
    /// each item in the catalog.
    pub fn indexes(&self) -> &HashMap<GlobalId, Vec<Vec<ScalarExpr>>> {
        &self.indexes
    }

    pub fn dump(&self) -> String {
        serde_json::to_string(&self.by_name).expect("serialization cannot fail")
    }
}

impl IdHumanizer for Catalog {
    fn humanize_id(&self, id: Id) -> Option<String> {
        match id {
            Id::Global(id) => self.by_id.get(&id).map(|entry| entry.name.to_string()),
            Id::Local(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    CreateDatabase {
        name: String,
    },
    CreateSchema {
        database_name: DatabaseSpecifier,
        schema_name: String,
    },
    CreateItem {
        id: GlobalId,
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
pub enum OpStatus {
    CreatedDatabase,
    CreatedSchema,
    CreatedItem(GlobalId),
    DroppedDatabase,
    DroppedSchema,
    DroppedItem(CatalogEntry),
    UpdatedItem,
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

impl sql::catalog::Catalog for ConnCatalog<'_> {
    fn startup_time(&self) -> SystemTime {
        self.catalog.startup_time
    }

    fn nonce(&self) -> u64 {
        self.catalog.nonce
    }

    fn default_database(&self) -> &str {
        &self.database
    }

    fn resolve_database(&self, database_name: &str) -> Result<(), SqlCatalogError> {
        match self.catalog.by_name.get(database_name) {
            Some(_) => Ok(()),
            None => Err(SqlCatalogError::UnknownDatabase(database_name.into())),
        }
    }

    fn resolve_schema(
        &self,
        database: Option<String>,
        schema_name: &str,
    ) -> Result<DatabaseSpecifier, SqlCatalogError> {
        Ok(self.catalog.resolve_schema(
            &self.database_spec(),
            database,
            schema_name,
            self.conn_id,
        )?)
    }

    fn resolve_item(&self, name: &PartialName) -> Result<FullName, SqlCatalogError> {
        Ok(self
            .catalog
            .resolve(self.database_spec(), self.search_path, name, self.conn_id)?)
    }

    fn list_databases<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        Box::new(self.catalog.databases())
    }

    fn list_schemas<'a>(
        &'a self,
        database_spec: &DatabaseSpecifier,
    ) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        match database_spec {
            DatabaseSpecifier::Ambient => Box::new(
                self.catalog
                    .ambient_schemas
                    .keys()
                    .map(|s| s.as_str())
                    .chain(iter::once(MZ_TEMP_SCHEMA)),
            ),
            DatabaseSpecifier::Name(n) => {
                let schemas = &self.catalog.by_name[n].schemas;
                Box::new(schemas.keys().map(|s| s.as_str()))
            }
        }
    }

    fn list_items<'a>(
        &'a self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
    ) -> Box<dyn Iterator<Item = &'a dyn sql::catalog::CatalogItem> + 'a> {
        let schema = self
            .catalog
            .get_schema(database_spec, schema_name, self.conn_id)
            .unwrap();
        Box::new(
            schema
                .items
                .values()
                .map(move |id| self.catalog.get_by_id(id) as &dyn sql::catalog::CatalogItem),
        )
    }

    fn get_item(&self, name: &FullName) -> &dyn sql::catalog::CatalogItem {
        self.catalog.get(name, self.conn_id).unwrap()
    }

    fn get_item_by_id(&self, id: &GlobalId) -> &dyn sql::catalog::CatalogItem {
        self.catalog.get_by_id(id)
    }

    fn experimental_mode(&self) -> bool {
        self.catalog.experimental_mode
    }
}

impl sql::catalog::CatalogItem for CatalogEntry {
    fn name(&self) -> &FullName {
        self.name()
    }

    fn id(&self) -> GlobalId {
        self.id()
    }

    fn desc(&self) -> Result<&RelationDesc, SqlCatalogError> {
        Ok(self.desc()?)
    }

    fn create_sql(&self) -> &str {
        match self.item() {
            CatalogItem::Source(Source { create_sql, .. }) => create_sql,
            CatalogItem::Sink(Sink { create_sql, .. }) => create_sql,
            CatalogItem::View(View { create_sql, .. }) => create_sql,
            CatalogItem::Index(Index { create_sql, .. }) => create_sql,
        }
    }

    fn plan_cx(&self) -> &PlanContext {
        match self.item() {
            CatalogItem::Source(Source { plan_cx, .. }) => plan_cx,
            CatalogItem::Sink(Sink { plan_cx, .. }) => plan_cx,
            CatalogItem::View(View { plan_cx, .. }) => plan_cx,
            CatalogItem::Index(Index { plan_cx, .. }) => plan_cx,
        }
    }

    fn item_type(&self) -> sql::catalog::CatalogItemType {
        match self.item() {
            CatalogItem::Source(_) => sql::catalog::CatalogItemType::Source,
            CatalogItem::Sink(_) => sql::catalog::CatalogItemType::Sink,
            CatalogItem::View(_) => sql::catalog::CatalogItemType::View,
            CatalogItem::Index(_) => sql::catalog::CatalogItemType::Index,
        }
    }

    fn index_details(&self) -> Option<(&[ScalarExpr], GlobalId)> {
        if let CatalogItem::Index(Index { keys, on, .. }) = self.item() {
            Some((keys, *on))
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
