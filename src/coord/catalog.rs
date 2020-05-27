// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::iter;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use chrono::{DateTime, TimeZone, Utc};
use failure::bail;
use lazy_static::lazy_static;
use log::{error, info, trace};
use ore::collections::CollectionExt;
use regex::Regex;
use serde::{Deserialize, Serialize};

use ::sql::catalog::{CatalogItemType, PlanCatalog, PlanCatalogEntry};
use ::sql::{DatabaseSpecifier, FullName, Params, PartialName, Plan, PlanContext};
use dataflow_types::{SinkConnector, SinkConnectorBuilder, SourceConnector};
use expr::{GlobalId, Id, IdHumanizer, OptimizedRelationExpr, ScalarExpr};
use repr::{RelationDesc, Row};
use transform::Optimizer;

use crate::catalog::error::{Error, ErrorKind};

mod error;
pub mod sql;

pub const SYSTEM_CONN_ID: u32 = 0;

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
pub struct Catalog {
    by_name: BTreeMap<String, Database>,
    by_id: BTreeMap<GlobalId, CatalogEntry>,
    indexes: HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ambient_schemas: Schemas,
    temporary_schemas: HashMap<u32, Schemas>,
    storage: Arc<Mutex<sql::Connection>>,
    creation_time: SystemTime,
    nonce: u64,
}

#[derive(Debug)]
pub struct ConnCatalog<'a> {
    catalog: &'a Catalog,
    conn_id: u32,
}

impl ConnCatalog<'_> {
    pub fn new(catalog: &Catalog, conn_id: u32) -> ConnCatalog {
        ConnCatalog { catalog, conn_id }
    }
}

#[derive(Debug, Serialize)]
struct Database {
    id: i64,
    schemas: Schemas,
}

lazy_static! {
    static ref EMPTY_DATABASE: Database = Database {
        id: 0,
        schemas: Schemas(BTreeMap::new()),
    };
}

#[derive(Debug, Serialize)]
struct Schemas(BTreeMap<String, Schema>);

#[derive(Debug, Serialize)]
pub struct Schema {
    id: i64,
    items: Items,
}

#[derive(Debug, Serialize)]
struct Items(BTreeMap<String, GlobalId>);

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
}

impl CatalogEntry {
    /// Reports the description of the datums produced by this catalog item.
    pub fn desc(&self) -> Result<&RelationDesc, failure::Error> {
        match &self.item {
            CatalogItem::Source(src) => Ok(&src.desc),
            CatalogItem::Sink(_) => bail!(
                "catalog item '{}' is a sink and so cannot be depended upon",
                self.name
            ),
            CatalogItem::View(view) => Ok(&view.desc),
            CatalogItem::Index(_) => bail!(
                "catalog item '{}' is an index and so cannot be depended upon",
                self.name
            ),
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
    pub fn open<F>(path: Option<&Path>, f: F) -> Result<Catalog, Error>
    where
        F: FnOnce(&mut Self),
    {
        let storage = sql::Connection::open(path)?;

        let mut catalog = Catalog {
            by_name: BTreeMap::new(),
            by_id: BTreeMap::new(),
            indexes: HashMap::new(),
            ambient_schemas: Schemas(BTreeMap::new()),
            temporary_schemas: HashMap::new(),
            storage: Arc::new(Mutex::new(storage)),
            creation_time: SystemTime::now(),
            nonce: rand::random(),
        };

        let databases = catalog.storage().load_databases()?;
        for (id, name) in databases {
            catalog.by_name.insert(
                name,
                Database {
                    id,
                    schemas: Schemas(BTreeMap::new()),
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
            schemas.0.insert(
                schema_name,
                Schema {
                    id,
                    items: Items(BTreeMap::new()),
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

    fn storage(&self) -> MutexGuard<sql::Connection> {
        self.storage.lock().expect("lock poisoned")
    }

    pub fn storage_handle(&self) -> Arc<Mutex<sql::Connection>> {
        self.storage.clone()
    }

    pub fn allocate_id(&mut self) -> Result<GlobalId, Error> {
        self.storage().allocate_id()
    }

    /// Resolves [`PartialName`] into a [`FullName`].
    ///
    /// If `name` does not specify a database, the `current_database` is used.
    /// If `name` does not specify a schema, then the schemas in `search_path`
    /// are searched in order.
    pub fn resolve(
        &self,
        current_database: DatabaseSpecifier,
        search_path: &[&str],
        name: &PartialName,
        conn_id: u32,
    ) -> Result<FullName, Error> {
        if let (Some(database_name), Some(schema_name)) = (&name.database, &name.schema) {
            // `name` is fully specified already. No resolution required.
            return Ok(FullName {
                database: DatabaseSpecifier::Name(database_name.to_owned()),
                schema: schema_name.to_owned(),
                item: name.item.clone(),
            });
        }

        // Find the specified database, or `current_database` if no database was
        // specified.
        let database_name = name
            .database
            .as_ref()
            .map(|n| DatabaseSpecifier::Name(n.clone()))
            .unwrap_or(current_database);
        let resolver = self.database_resolver(database_name)?;

        if let Some(schema_name) = &name.schema {
            // A schema name was specified, so just try to find the item in
            // that schema.
            if let Some(out) = resolver.resolve_item(schema_name, &name.item, conn_id) {
                return Ok(out);
            }
        } else {
            // No schema was specified, so try to find the item in every schema
            // in the search path, in order.
            for &schema_name in search_path {
                if let Some(out) = resolver.resolve_item(schema_name, &name.item, conn_id) {
                    return Ok(out);
                }
            }
        }

        Err(Error::new(ErrorKind::UnknownItem(name.to_string())))
    }

    /// Returns the named catalog item, if it exists.
    ///
    /// See also [`Catalog::get`].
    pub fn try_get(&self, name: &FullName, conn_id: u32) -> Option<&CatalogEntry> {
        self.get_schema(&name.database, &name.schema, conn_id)
            .ok()
            .and_then(|schema| schema.items.0.get(&name.item))
            .map(|id| &self.by_id[id])
    }

    /// Returns the named catalog item, or an error if it does not exist.
    ///
    /// See also [`Catalog::try_get`].
    pub fn get(&self, name: &FullName, conn_id: u32) -> Result<&CatalogEntry, Error> {
        self.try_get(name, conn_id)
            .ok_or_else(|| Error::new(ErrorKind::UnknownItem(name.to_string())))
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

    pub fn database_resolver<'a>(
        &'a self,
        database_spec: DatabaseSpecifier,
    ) -> Result<DatabaseResolver<'a>, Error> {
        match &database_spec {
            DatabaseSpecifier::Ambient | DatabaseSpecifier::Temporary => Ok(DatabaseResolver {
                database_spec,
                database: &EMPTY_DATABASE,
                ambient_schemas: &self.ambient_schemas,
                temporary_schemas: &self.temporary_schemas,
            }),
            DatabaseSpecifier::Name(name) => match self.by_name.get(name) {
                Some(database) => Ok(DatabaseResolver {
                    database_spec,
                    database,
                    ambient_schemas: &self.ambient_schemas,
                    temporary_schemas: &self.temporary_schemas,
                }),
                None => Err(Error::new(ErrorKind::UnknownDatabase(name.to_owned()))),
            },
        }
    }

    /// Creates a new schema in the `Catalog` for temporary items
    /// indicated by the TEMPORARY or TEMP keywords.
    pub fn create_temporary_schema(&mut self, conn_id: u32) {
        let mut temp_schema_for_conn_id = BTreeMap::new();
        temp_schema_for_conn_id.insert(
            "mz_temp".into(),
            Schema {
                id: -1,
                items: Items(BTreeMap::new()),
            },
        );
        self.temporary_schemas
            .insert(conn_id, Schemas(temp_schema_for_conn_id));
    }

    fn get_temp_schemas(&mut self, conn_id: u32) -> &Schemas {
        self.temporary_schemas
            .get(&conn_id)
            .expect("missing temporary schema for connection")
    }

    fn item_exists_in_temp_schemas(&mut self, conn_id: u32, item_name: String) -> bool {
        for schema in self.get_temp_schemas(conn_id).0.values() {
            if schema.items.0.contains_key(&item_name) {
                return true;
            }
        }
        false
    }

    pub fn drop_temp_item_ops(&mut self, conn_id: u32) -> Vec<Op> {
        self.get_temp_schemas(conn_id)
            .0
            .values()
            .flat_map(|schema| {
                schema
                    .items
                    .0
                    .values()
                    .map(|id| Op::DropItem(*id))
                    .collect::<Vec<Op>>()
            })
            .collect()
    }

    pub fn drop_temporary_schema(&mut self, conn_id: u32) {
        if self
            .get_temp_schemas(conn_id)
            .0
            .get("mz_temp")
            .expect("missing temporary schema mz_temp for conn_id: {}")
            .items
            .0
            .is_empty()
        {
            error!(
                "items leftover in temporary schema for conn_id: {}",
                conn_id
            );
        }

        self.temporary_schemas.remove(&conn_id);
    }

    /// Gets the schema map for the database matching `database_spec`.
    fn get_schema(
        &self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
    ) -> Result<&Schema, Error> {
        // Keep in sync with `get_schemas_mut`.
        match database_spec {
            DatabaseSpecifier::Ambient => match self.ambient_schemas.0.get(schema_name) {
                Some(schema) => Ok(schema),
                None => Err(Error::new(ErrorKind::UnknownSchema(schema_name.into()))),
            },
            DatabaseSpecifier::Temporary => match self
                .temporary_schemas
                .get(&conn_id)
                .unwrap()
                .0
                .get(schema_name)
            {
                Some(schema) => Ok(schema),
                None => Err(Error::new(ErrorKind::UnknownSchema(schema_name.into()))),
            },
            DatabaseSpecifier::Name(name) => match self.by_name.get(name) {
                Some(db) => match db.schemas.0.get(schema_name) {
                    Some(schema) => Ok(schema),
                    None => Err(Error::new(ErrorKind::UnknownSchema(schema_name.into()))),
                },
                None => Err(Error::new(ErrorKind::UnknownDatabase(name.to_owned()))),
            },
        }
    }

    /// Like `get_schemas`, but returns a `mut` reference.
    fn get_schema_mut(
        &mut self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
        conn_id: u32,
    ) -> Result<&mut Schema, Error> {
        // Keep in sync with `get_schemas`.
        match database_spec {
            DatabaseSpecifier::Ambient => match self.ambient_schemas.0.get_mut(schema_name) {
                Some(schema) => Ok(schema),
                None => Err(Error::new(ErrorKind::UnknownSchema(schema_name.into()))),
            },
            DatabaseSpecifier::Temporary => match self
                .temporary_schemas
                .get_mut(&conn_id)
                .unwrap()
                .0
                .get_mut(schema_name)
            {
                Some(schema) => Ok(schema),
                None => Err(Error::new(ErrorKind::UnknownSchema(schema_name.into()))),
            },
            DatabaseSpecifier::Name(name) => match self.by_name.get_mut(name) {
                Some(db) => match db.schemas.0.get_mut(schema_name) {
                    Some(schema) => Ok(schema),
                    None => Err(Error::new(ErrorKind::UnknownSchema(schema_name.into()))),
                },
                None => Err(Error::new(ErrorKind::UnknownDatabase(name.to_owned()))),
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
            .0
            .insert(entry.name.item.clone(), entry.id);
        self.by_id.insert(entry.id, entry);
    }

    pub fn drop_database_ops(&mut self, name: String) -> Vec<Op> {
        let mut ops = vec![];
        if let Some(database) = self.by_name.get(&name) {
            for (schema_name, schema) in &database.schemas.0 {
                Self::drop_schema_items(schema, &self.by_id, &mut ops);
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
        if let DatabaseSpecifier::Name(database_name) = database_spec {
            if let Some(database) = self.by_name.get(&database_name) {
                if let Some(schema) = database.schemas.0.get(&schema_name) {
                    Self::drop_schema_items(schema, &self.by_id, &mut ops);
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
    ) {
        let mut seen = HashSet::new();
        for &id in schema.items.0.values() {
            Self::drop_item_cascade(id, by_id, ops, &mut seen)
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
                if self.item_exists_in_temp_schemas(*conn_id, name.item.clone())
                    || creating.contains(&(conn_id, name.item.clone()))
                {
                    return Err(Error::new(ErrorKind::ItemAlreadyExists(name.item.clone())));
                } else {
                    creating.insert((conn_id, name.item.clone()));
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
        }

        let temporary_ids = self.temporary_ids(&ops)?;
        let mut actions = Vec::with_capacity(ops.len());
        let mut storage = self.storage();
        let mut tx = storage.transaction()?;
        for op in ops {
            actions.push(match op {
                Op::CreateDatabase { name } => Action::CreateDatabase {
                    id: tx.insert_database(&name)?,
                    name,
                },
                Op::CreateSchema {
                    database_name,
                    schema_name,
                } => {
                    if schema_name.starts_with("mz_") || schema_name.starts_with("pg_") {
                        return Err(Error::new(ErrorKind::UnacceptableSchemaName(schema_name)));
                    }
                    let (database_id, database_name) = match database_name {
                        DatabaseSpecifier::Name(name) => (tx.load_database_id(&name)?, name),
                        DatabaseSpecifier::Ambient | DatabaseSpecifier::Temporary => {
                            return Err(Error::new(ErrorKind::ReadOnlySystemSchema(schema_name)));
                        }
                    };
                    Action::CreateSchema {
                        id: tx.insert_schema(database_id, &schema_name)?,
                        database_name,
                        schema_name,
                    }
                }
                Op::CreateItem { id, name, item } => {
                    if !item.is_temporary() {
                        if item.uses().iter().any(|id| match self.try_get_by_id(*id) {
                            Some(entry) => entry.item().is_temporary(),
                            None => temporary_ids.contains(&id),
                        }) {
                            return Err(Error::new(ErrorKind::TemporaryItem(id.to_string())));
                        }
                        let database_id = match &name.database {
                            DatabaseSpecifier::Name(name) => tx.load_database_id(&name)?,
                            DatabaseSpecifier::Ambient => {
                                return Err(Error::new(ErrorKind::ReadOnlySystemSchema(
                                    name.to_string(),
                                )));
                            }
                            DatabaseSpecifier::Temporary => unreachable!(),
                        };
                        let schema_id = tx.load_schema_id(database_id, &name.schema)?;
                        let serialized_item = self.serialize_item(&item);
                        tx.insert_item(id, schema_id, &name.item, &serialized_item)?;
                    }

                    Action::CreateItem { id, name, item }
                }
                Op::DropDatabase { name } => {
                    tx.remove_database(&name)?;
                    Action::DropDatabase { name }
                }
                Op::DropSchema {
                    database_name,
                    schema_name,
                } => {
                    let (database_id, database_name) = match database_name {
                        DatabaseSpecifier::Name(name) => (tx.load_database_id(&name)?, name),
                        DatabaseSpecifier::Ambient | DatabaseSpecifier::Temporary => {
                            return Err(Error::new(ErrorKind::ReadOnlySystemSchema(schema_name)));
                        }
                    };
                    tx.remove_schema(database_id, &schema_name)?;
                    Action::DropSchema {
                        database_name,
                        schema_name,
                    }
                }
                Op::DropItem(id) => {
                    if !self.get_by_id(&id).item().is_temporary() {
                        tx.remove_item(id)?;
                    }
                    Action::DropItem(id)
                }
            })
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
                            schemas: Schemas(BTreeMap::new()),
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
                        .0
                        .insert(
                            schema_name,
                            Schema {
                                id,
                                items: Items(BTreeMap::new()),
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
                        .0
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
                        .0
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

    fn deserialize_item(&self, bytes: Vec<u8>) -> Result<CatalogItem, failure::Error> {
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
        let stmt = ::sql::parse(create_sql)?.into_element();
        let plan = ::sql::plan(
            &pcx,
            &ConnCatalog::new(self, SYSTEM_CONN_ID),
            &::sql::InternalSession,
            stmt,
            &params,
        )?;
        Ok(match plan {
            Plan::CreateSource { source, .. } => CatalogItem::Source(Source {
                create_sql: source.create_sql,
                plan_cx: pcx,
                connector: source.connector,
                desc: source.desc,
            }),
            Plan::CreateView { view, .. } => {
                let mut optimizer = Optimizer::default();
                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    plan_cx: pcx,
                    optimized_expr: optimizer.optimize(view.expr, self.indexes())?,
                    desc: view.desc,
                    conn_id: None,
                })
            }
            Plan::CreateIndex { index, .. } => CatalogItem::Index(Index {
                create_sql: index.create_sql,
                plan_cx: pcx,
                on: index.on,
                keys: index.keys,
            }),
            Plan::CreateSink { sink, .. } => CatalogItem::Sink(Sink {
                create_sql: sink.create_sql,
                plan_cx: pcx,
                from: sink.from,
                connector: SinkConnectorState::Pending(sink.connector_builder),
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

    pub fn creation_time(&self) -> SystemTime {
        self.creation_time
    }

    pub fn nonce(&self) -> u64 {
        self.nonce
    }
}

impl fmt::Debug for Catalog {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Catalog")
            .field("by_name", &self.by_name)
            .field("by_id", &self.by_id)
            .field("ambient_schemas", &self.ambient_schemas)
            .field("storage", &self.storage)
            .finish()
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
}

#[derive(Debug, Clone)]
pub enum OpStatus {
    CreatedDatabase,
    CreatedSchema,
    CreatedItem(GlobalId),
    DroppedDatabase,
    DroppedSchema,
    DroppedItem(CatalogEntry),
}

/// A helper for resolving schema and item names within one database.
pub struct DatabaseResolver<'a> {
    database_spec: DatabaseSpecifier,
    database: &'a Database,
    ambient_schemas: &'a Schemas,
    temporary_schemas: &'a HashMap<u32, Schemas>,
}

impl<'a> DatabaseResolver<'a> {
    /// Attempts to resolve the item specified by `schema_name` and `item_name`
    /// in the database that this resolver is attached to, or in the set of
    /// ambient schemas.
    pub fn resolve_item(
        &self,
        schema_name: &str,
        item_name: &str,
        conn_id: u32,
    ) -> Option<FullName> {
        if let Some(schema) = self.database.schemas.0.get(schema_name) {
            if schema.items.0.contains_key(item_name) {
                return Some(FullName {
                    database: self.database_spec.clone(),
                    schema: schema_name.to_owned(),
                    item: item_name.to_owned(),
                });
            }
        }
        if let Some(schema) = self.ambient_schemas.0.get(schema_name) {
            if schema.items.0.contains_key(item_name) {
                return Some(FullName {
                    database: DatabaseSpecifier::Ambient,
                    schema: schema_name.to_owned(),
                    item: item_name.to_owned(),
                });
            }
        }
        if let Some(temp_schema_for_conn_id) = self.temporary_schemas.get(&conn_id) {
            if let Some(schema) = temp_schema_for_conn_id.0.get(schema_name) {
                if schema.items.0.contains_key(item_name) {
                    return Some(FullName {
                        database: DatabaseSpecifier::Temporary,
                        schema: schema_name.to_owned(),
                        item: item_name.to_owned(),
                    });
                }
            }
        }

        None
    }
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

impl PlanCatalog for ConnCatalog<'_> {
    fn creation_time(&self) -> SystemTime {
        self.catalog.creation_time()
    }

    fn nonce(&self) -> u64 {
        self.catalog.nonce()
    }

    fn databases<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        Box::new(self.catalog.databases())
    }

    fn get(&self, name: &FullName) -> Result<&dyn PlanCatalogEntry, failure::Error> {
        Ok(self.catalog.get(name, self.conn_id)?)
    }

    fn get_by_id(&self, id: &GlobalId) -> &dyn PlanCatalogEntry {
        self.catalog.get_by_id(id)
    }

    fn get_schemas<'a>(
        &'a self,
        database_spec: &DatabaseSpecifier,
    ) -> Result<Box<dyn Iterator<Item = &'a str> + 'a>, failure::Error> {
        match database_spec {
            DatabaseSpecifier::Ambient => Ok(Box::new(
                self.catalog.ambient_schemas.0.keys().map(|s| s.as_str()),
            )),
            DatabaseSpecifier::Temporary => Ok(Box::new(iter::once("mz_temp"))),
            DatabaseSpecifier::Name(n) => match self.catalog.by_name.get(n) {
                Some(db) => Ok(Box::new(db.schemas.0.keys().map(|s| s.as_str()))),
                None => Err(Error::new(ErrorKind::UnknownDatabase(n.into())).into()),
            },
        }
    }

    fn get_items<'a>(
        &'a self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
    ) -> Result<Box<dyn Iterator<Item = &'a dyn PlanCatalogEntry> + 'a>, failure::Error> {
        let schema = self
            .catalog
            .get_schema(database_spec, schema_name, self.conn_id)?;
        Ok(Box::new(schema.items.0.values().map(move |id| {
            self.catalog.get_by_id(id) as &dyn PlanCatalogEntry
        })))
    }

    fn resolve(
        &self,
        current_database: DatabaseSpecifier,
        search_path: &[&str],
        name: &PartialName,
    ) -> Result<FullName, failure::Error> {
        Ok(self
            .catalog
            .resolve(current_database, search_path, name, self.conn_id)?)
    }

    fn resolve_schema(
        &self,
        current_database: &DatabaseSpecifier,
        database: Option<&DatabaseSpecifier>,
        schema_name: &str,
    ) -> Result<DatabaseSpecifier, failure::Error> {
        let database_specs = if let Some(database) = database {
            vec![database]
        } else {
            vec![
                current_database,
                &DatabaseSpecifier::Ambient,
                &DatabaseSpecifier::Temporary,
            ]
        };
        for database_spec in database_specs {
            if self
                .catalog
                .get_schema(&database_spec, schema_name, self.conn_id)
                .is_ok()
            {
                return Ok(database_spec.clone());
            }
        }
        Err(Error::new(ErrorKind::UnknownSchema(schema_name.into())).into())
    }
}

impl PlanCatalogEntry for CatalogEntry {
    fn name(&self) -> &FullName {
        self.name()
    }

    fn id(&self) -> GlobalId {
        self.id()
    }

    fn desc(&self) -> Result<&RelationDesc, failure::Error> {
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

    fn item_type(&self) -> CatalogItemType {
        match self.item() {
            CatalogItem::Source(_) => CatalogItemType::Source,
            CatalogItem::Sink(_) => CatalogItemType::Sink,
            CatalogItem::View(_) => CatalogItemType::View,
            CatalogItem::Index(_) => CatalogItemType::Index,
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
