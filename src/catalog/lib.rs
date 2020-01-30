// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use failure::bail;
use log::{info, trace};
use serde::{Deserialize, Serialize};

use dataflow_types::{Index, Sink, Source, View};
use expr::{GlobalId, Id, IdHumanizer, OptimizedRelationExpr};
use repr::RelationDesc;

use crate::names::{DatabaseSpecifier, FullName, PartialName};

pub mod names;

mod sql;

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
    id: usize,
    by_name: HashMap<String, Database>,
    by_id: BTreeMap<GlobalId, CatalogEntry>,
    ambient_schemas: HashMap<String, Schema>,
    storage: sql::Connection,
}

#[derive(Debug)]
struct Database {
    id: i64,
    schemas: HashMap<String, Schema>,
}

#[derive(Debug)]
pub struct Schema {
    pub id: i64,
    pub items: HashMap<String, GlobalId>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SchemaType {
    Ambient,
    Normal,
}

#[derive(Clone, Debug)]
pub struct CatalogEntry {
    inner: CatalogItem,
    used_by: Vec<GlobalId>,
    id: GlobalId,
    name: FullName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogItem {
    Source(Source),
    View(View<OptimizedRelationExpr>),
    Sink(Sink),
    Index(Index),
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
}

impl CatalogEntry {
    /// Reports the description of the datums produced by this catalog item.
    pub fn desc(&self) -> Result<&RelationDesc, failure::Error> {
        match &self.inner {
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
        match &self.inner {
            CatalogItem::Source(_src) => Vec::new(),
            CatalogItem::Sink(sink) => vec![sink.from.0],
            CatalogItem::View(view) => {
                let mut out = Vec::new();
                view.relation_expr.as_ref().global_uses(&mut out);
                out
            }
            CatalogItem::Index(idx) => {
                let mut out = Vec::new();
                out.push(idx.desc.on_id);
                out
            }
        }
    }

    /// Returns the `CatalogItem` associated with this catalog entry.
    pub fn item(&self) -> &CatalogItem {
        &self.inner
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
    /// Opens or creates a `Catalog` that stores data at `path`. Initial
    /// catalog items in `items` will be inserted into the catalog before any
    /// persisted catalog items are loaded.
    pub fn open<I>(path: Option<&Path>, items: I) -> Result<Catalog, failure::Error>
    where
        I: Iterator<Item = (GlobalId, FullName, CatalogItem)>,
    {
        let storage = sql::Connection::open(path)?;

        let mut catalog = Catalog {
            id: 0,
            by_name: HashMap::new(),
            by_id: BTreeMap::new(),
            ambient_schemas: HashMap::new(),
            storage,
        };

        for (id, name) in catalog.storage.load_databases()? {
            catalog.by_name.insert(
                name,
                Database {
                    id,
                    schemas: HashMap::new(),
                },
            );
        }

        for (id, database_name, schema_name) in catalog.storage.load_schemas()? {
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
                    items: HashMap::new(),
                },
            );
        }

        // Insert system items. This has to be done after databases and schemas
        // are loaded, but before any items, as items might depend on these
        // system items, but these system items depend on the system
        // database/schema being installed.
        for (id, name, item) in items {
            catalog.insert_item(id, name, item);
        }

        for (id, name, def) in catalog.storage.load_items()? {
            catalog.insert_item(id, name, def);
            if let GlobalId::User(id) = id {
                catalog.id = cmp::max(catalog.id, id);
            }
        }

        Ok(catalog)
    }
}

impl Catalog {
    pub fn allocate_id(&mut self) -> GlobalId {
        self.id += 1;
        GlobalId::user(self.id)
    }

    /// Resolves [`PartialName`] into a [`FullName`].
    ///
    /// If `name` does not specify a database, the `current_database` is used.
    /// If `name` does not specify a schema, then the schemas in `search_path`
    /// are searched in order.
    pub fn resolve(
        &self,
        current_database: &str,
        search_path: &[&str],
        name: &PartialName,
    ) -> Result<FullName, failure::Error> {
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
        let database_name = name.database.as_deref().unwrap_or(current_database);
        let resolver = self.database_resolver(database_name)?;

        if let Some(schema_name) = &name.schema {
            // A schema name was specified, so just try to find the item in
            // that schema.
            if let Some(out) = resolver.resolve_item(schema_name, &name.item) {
                return Ok(out);
            }
        } else {
            // No schema was specified, so try to find the item in every schema
            // in the search path, in order.
            for &schema_name in search_path {
                if let Some(out) = resolver.resolve_item(schema_name, &name.item) {
                    return Ok(out);
                }
            }
        }

        bail!("catalog item '{}' does not exist", name);
    }

    /// Returns the named catalog item, if it exists.
    ///
    /// See also [`Catalog::get`].
    pub fn try_get(&self, name: &FullName) -> Option<&CatalogEntry> {
        self.get_schemas(&name.database)
            .and_then(|schemas| schemas.get(&name.schema))
            .and_then(|schema| schema.items.get(&name.item))
            .map(|id| &self.by_id[id])
    }

    /// Returns the named catalog item, or an error if it does not exist.
    ///
    /// See also [`Catalog::try_get`].
    pub fn get(&self, name: &FullName) -> Result<&CatalogEntry, failure::Error> {
        self.try_get(name)
            .ok_or_else(|| failure::err_msg(format!("catalog item '{}' does not exist", name)))
    }

    pub fn get_by_id(&self, id: &GlobalId) -> &CatalogEntry {
        &self.by_id[id]
    }

    /// Returns an iterator over the name of each database in the catalog.
    pub fn databases(&self) -> impl Iterator<Item = &str> {
        self.by_name.keys().map(String::as_str)
    }

    pub fn database_resolver<'a, 'b>(
        &'a self,
        database_name: &'b str,
    ) -> Result<DatabaseResolver<'a, 'b>, failure::Error> {
        match self.by_name.get(database_name) {
            Some(database) => Ok(DatabaseResolver {
                database_name,
                database,
                ambient_schemas: &self.ambient_schemas,
            }),
            None => bail!("unknown database '{}'", database_name),
        }
    }

    /// Gets the schema map for the database matching `database_spec`.
    pub fn get_schemas(
        &self,
        database_spec: &DatabaseSpecifier,
    ) -> Option<&HashMap<String, Schema>> {
        // Keep in sync with `get_schemas_mut`.
        match database_spec {
            DatabaseSpecifier::Ambient => Some(&self.ambient_schemas),
            DatabaseSpecifier::Name(name) => self.by_name.get(name).map(|db| &db.schemas),
        }
    }

    /// Like `get_schemas`, but returns a `mut` reference.
    fn get_schemas_mut(
        &mut self,
        database_spec: &DatabaseSpecifier,
    ) -> Option<&mut HashMap<String, Schema>> {
        // Keep in sync with `get_schemas`.
        match database_spec {
            DatabaseSpecifier::Ambient => Some(&mut self.ambient_schemas),
            DatabaseSpecifier::Name(name) => self.by_name.get_mut(name).map(|db| &mut db.schemas),
        }
    }

    fn insert_item(&mut self, id: GlobalId, name: FullName, item: CatalogItem) {
        info!("create {} {} ({})", item.type_string(), name, id);
        let entry = CatalogEntry {
            inner: item,
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
        self.get_schemas_mut(&entry.name.database)
            .expect("catalog out of sync")
            .get_mut(&entry.name.schema)
            .expect("catalog out of sync")
            .items
            .insert(entry.name.item.clone(), entry.id);
        self.by_id.insert(entry.id, entry);
    }

    pub fn create_database(&mut self, name: String) -> Result<(), failure::Error> {
        self.transact(vec![
            Op::CreateDatabase { name: name.clone() },
            Op::CreateSchema {
                database_name: name,
                schema_name: "public".into(),
            },
        ])?;
        Ok(())
    }

    pub fn create_schema(
        &mut self,
        database_name: String,
        schema_name: String,
    ) -> Result<(), failure::Error> {
        self.transact(vec![Op::CreateSchema {
            database_name,
            schema_name,
        }])?;
        Ok(())
    }

    pub fn create_item(
        &mut self,
        name: FullName,
        item: CatalogItem,
    ) -> Result<GlobalId, failure::Error> {
        let id = match self
            .transact(vec![Op::CreateItem { name, item }])?
            .as_slice()
        {
            [OpStatus::CreatedItem(id)] => *id,
            _ => unreachable!(),
        };
        Ok(id)
    }

    pub fn drop_database(&mut self, name: String) -> Result<Vec<CatalogEntry>, failure::Error> {
        let mut ops = vec![];
        if let Some(database) = self.by_name.get(&name) {
            for (schema_name, schema) in &database.schemas {
                Self::drop_schema_items(schema, &self.by_id, &mut ops);
                ops.push(Op::DropSchema {
                    database_name: name.clone(),
                    schema_name: schema_name.clone(),
                });
            }
            ops.push(Op::DropDatabase { name });
        }
        Ok(self
            .transact(ops)?
            .into_iter()
            .filter_map(|status| match status {
                OpStatus::DroppedItem(entry) => Some(entry),
                _ => None,
            })
            .collect())
    }

    pub fn drop_schema(
        &mut self,
        database_name: String,
        schema_name: String,
    ) -> Result<Vec<CatalogEntry>, failure::Error> {
        let mut ops = vec![];
        if let Some(database) = self.by_name.get(&database_name) {
            if let Some(schema) = database.schemas.get(&schema_name) {
                Self::drop_schema_items(schema, &self.by_id, &mut ops);
                ops.push(Op::DropSchema {
                    database_name,
                    schema_name,
                })
            }
        }
        Ok(self
            .transact(ops)?
            .into_iter()
            .filter_map(|status| match status {
                OpStatus::DroppedItem(entry) => Some(entry),
                _ => None,
            })
            .collect())
    }

    pub fn drop_items(&mut self, ids: &[GlobalId]) -> Result<Vec<CatalogEntry>, failure::Error> {
        let mut ops = vec![];
        for &id in ids {
            Self::drop_item_cascade(id, &self.by_id, &mut ops, &mut HashSet::new());
        }
        Ok(self
            .transact(ops)?
            .into_iter()
            .filter_map(|status| match status {
                OpStatus::DroppedItem(entry) => Some(entry),
                _ => None,
            })
            .collect())
    }

    fn drop_schema_items(
        schema: &Schema,
        by_id: &BTreeMap<GlobalId, CatalogEntry>,
        ops: &mut Vec<Op>,
    ) {
        let mut seen = HashSet::new();
        for &id in schema.items.values() {
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

    fn transact(&mut self, ops: Vec<Op>) -> Result<Vec<OpStatus>, failure::Error> {
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

        let mut actions = Vec::with_capacity(ops.len());
        let mut tx = self.storage.transaction()?;
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
                    if schema_name.starts_with("mz_") || schema_name.starts_with("pg") {
                        bail!("unacceptable schema name '{}'", schema_name);
                    }
                    let database_id = tx.load_database_id(&database_name)?;
                    Action::CreateSchema {
                        id: tx.insert_schema(database_id, &schema_name)?,
                        database_name,
                        schema_name,
                    }
                }
                Op::CreateItem { name, item } => {
                    let database_id = match &name.database {
                        DatabaseSpecifier::Name(name) => tx.load_database_id(&name)?,
                        DatabaseSpecifier::Ambient => {
                            bail!("writing to {} is not allowed", name.schema)
                        }
                    };
                    let schema_id = tx.load_schema_id(database_id, &name.schema)?;
                    self.id += 1;
                    let id = GlobalId::user(self.id);
                    tx.insert_item(id, schema_id, &name.item, &item)?;
                    Action::CreateItem { name, item, id }
                }
                Op::DropDatabase { name } => {
                    tx.remove_database(&name)?;
                    Action::DropDatabase { name }
                }
                Op::DropSchema {
                    database_name,
                    schema_name,
                } => {
                    let database_id = tx.load_database_id(&database_name)?;
                    tx.remove_schema(database_id, &schema_name)?;
                    Action::DropSchema {
                        database_name,
                        schema_name,
                    }
                }
                Op::DropItem(id) => {
                    tx.remove_item(id)?;
                    Action::DropItem(id)
                }
            })
        }
        tx.commit()?;

        Ok(actions
            .into_iter()
            .map(|action| match action {
                Action::CreateDatabase { id, name } => {
                    info!("create database {}", name);
                    self.by_name.insert(
                        name,
                        Database {
                            id,
                            schemas: HashMap::new(),
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
                                items: HashMap::new(),
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
                    info!(
                        "drop {} {} ({})",
                        metadata.inner.type_string(),
                        metadata.name,
                        id
                    );
                    for u in metadata.uses() {
                        if let Some(dep_metadata) = self.by_id.get_mut(&u) {
                            dep_metadata.used_by.retain(|u| *u != metadata.id)
                        }
                    }
                    self.get_schemas_mut(&metadata.name.database)
                        .expect("catalog out of sync")
                        .get_mut(&metadata.name.schema)
                        .expect("catalog out of sync")
                        .items
                        .remove(&metadata.name.item)
                        .expect("catalog out of sync");
                    OpStatus::DroppedItem(metadata)
                }
            })
            .collect())
    }

    /// Iterates over the items in the catalog in order of increasing ID.
    pub fn iter(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.by_id.iter().map(|(_id, entry)| entry)
    }

    /// Whether this catalog instance bootstrapped its on-disk state.
    pub fn bootstrapped(&self) -> bool {
        self.storage.bootstrapped()
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
enum Op {
    CreateDatabase {
        name: String,
    },
    CreateSchema {
        database_name: String,
        schema_name: String,
    },
    CreateItem {
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
pub struct DatabaseResolver<'a, 'b> {
    database_name: &'b str,
    database: &'a Database,
    ambient_schemas: &'a HashMap<String, Schema>,
}

impl<'a, 'b> DatabaseResolver<'a, 'b> {
    /// Attempts to resolve the item specified by `schema_name` and `item_name`
    /// in the database that this resolver is attached to, or in the set of
    /// ambient schemas.
    pub fn resolve_item(&self, schema_name: &str, item_name: &str) -> Option<FullName> {
        if let Some(schema) = self.database.schemas.get(schema_name) {
            if schema.items.contains_key(item_name) {
                return Some(FullName {
                    database: DatabaseSpecifier::Name(self.database_name.to_owned()),
                    schema: schema_name.to_owned(),
                    item: item_name.to_owned(),
                });
            }
        }
        if let Some(schema) = self.ambient_schemas.get(schema_name) {
            if schema.items.contains_key(item_name) {
                return Some(FullName {
                    database: DatabaseSpecifier::Ambient,
                    schema: schema_name.to_owned(),
                    item: item_name.to_owned(),
                });
            }
        }
        None
    }

    /// Attempts to find the schema specified by `schema_name` in the database
    /// that this resolver is attached to, or in the set of ambient schemas.
    pub fn resolve_schema(&self, schema_name: &str) -> Option<(&'a Schema, SchemaType)> {
        self.database
            .schemas
            .get(schema_name)
            .map(|s| (s, SchemaType::Normal))
            .or_else(|| {
                self.ambient_schemas
                    .get(schema_name)
                    .map(|s| (s, SchemaType::Ambient))
            })
    }
}
