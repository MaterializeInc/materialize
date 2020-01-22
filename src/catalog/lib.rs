// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use failure::bail;
use log::info;
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
    id: i32,
    schemas: HashMap<String, Schema>,
}

#[derive(Debug)]
pub struct Schema {
    pub id: i32,
    pub items: HashMap<String, GlobalId>,
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
            catalog.insert_id(id, name, item)?;
        }

        for (id, name, def) in catalog.storage.load_items()? {
            catalog.insert_id_core(id, name, def);
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

    /// Inserts a new catalog item, returning an error if a catalog item with
    /// the same name already exists.
    ///
    /// The internal dependency graph is updated accordingly. The function will
    /// panic if any of `item`'s dependencies are not present in the store.
    pub fn insert(
        &mut self,
        name: FullName,
        item: CatalogItem,
    ) -> Result<GlobalId, failure::Error> {
        let id = self.allocate_id();
        self.insert_id(id, name, item)?;
        Ok(id)
    }

    fn insert_id(
        &mut self,
        id: GlobalId,
        name: FullName,
        item: CatalogItem,
    ) -> Result<(), failure::Error> {
        // Validate that we can insert the item.
        if self.by_id.contains_key(&id) {
            bail!("catalog item with id {} already exists", id)
        }
        let schema = match self
            .get_schemas_mut(&name.database)
            .and_then(|schemas| schemas.get(&name.schema))
        {
            Some(schema) => schema,
            None => bail!("catalog does not contain schema '{}'", name.schema),
        };
        if schema.items.contains_key(&name.item) {
            bail!("catalog item '{}' already exists", name);
        }
        let schema_id = schema.id;

        // Maybe update on-disk state.
        // At the moment, system sources are always ephemeral.
        if let GlobalId::User(_) = id {
            // TODO: tables created by symbiosis are considered user sources,
            // but they are ephemeral and should not be inserted into the
            // catalog.
            self.storage.insert_item(id, schema_id, &name.item, &item)?;
        }

        // Update in-memory state.
        self.insert_id_core(id, name, item);
        Ok(())
    }

    fn insert_id_core(&mut self, id: GlobalId, name: FullName, item: CatalogItem) {
        info!("new {} {} ({})", item.type_string(), name, id);
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

    /// Determines whether it is feasible to remove the view named `name`
    /// according to `mode`. If `mode` is [`RemoveMode::Restrict`], then
    /// an error will be returned if any existing views depend upon the view
    /// specified for removal. If `mode` is [`RemoveMode::Cascade`], then the
    /// views that transitively depend upon the view specified for removal will
    /// be collected into the `to_remove` vector. In either mode, the identifier
    /// that corresponds to `name` is included in `to_remove`.
    ///
    /// Note that even in RemoveMode::Restrict, dependent indexes do not hinder
    /// view removal and will be removed along with the view.
    ///
    /// To actually remove the views, call [`Catalog::remove`] on each
    /// name in `to_remove`.
    pub fn plan_remove(
        &self,
        name: &FullName,
        mode: RemoveMode,
        to_remove: &mut Vec<GlobalId>,
    ) -> Result<(), failure::Error> {
        let metadata = match self.try_get(name) {
            Some(metadata) => metadata,
            None => return Ok(()),
        };
        match mode {
            RemoveMode::Restrict => {
                for user in metadata.used_by.iter() {
                    match self.by_id[user].item() {
                        CatalogItem::Index { .. } => {
                            to_remove.push(*user);
                        }
                        _ => {
                            if !to_remove.iter().any(|r| r == user) {
                                bail!(
                                    "cannot delete {}: still depended upon by catalog item '{}'",
                                    name,
                                    self.by_id[user].name()
                                )
                            }
                        }
                    }
                }
                to_remove.push(metadata.id);
                Ok(())
            }
            RemoveMode::Cascade => {
                self.plan_remove_cascade(metadata, to_remove);
                Ok(())
            }
        }
    }

    fn plan_remove_cascade(&self, metadata: &CatalogEntry, to_remove: &mut Vec<GlobalId>) {
        let used_by = metadata.used_by.clone();
        for u in used_by {
            self.plan_remove_cascade(&self.by_id[&u], to_remove);
        }
        to_remove.push(metadata.id);
    }

    /// Unconditionally removes the named view. It is required that `id`
    /// come from the output of `plan_remove`; otherwise consistency rules may
    /// be violated.
    pub fn remove(&mut self, id: GlobalId) {
        if let Some(metadata) = self.by_id.remove(&id) {
            info!(
                "remove {} {} ({})",
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
        }
        self.storage.remove_item(id);
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
    pub fn resolve_schema(&self, schema_name: &str) -> Option<&'a Schema> {
        self.database
            .schemas
            .get(schema_name)
            .or_else(|| self.ambient_schemas.get(schema_name))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveMode {
    Cascade,
    Restrict,
}

impl RemoveMode {
    pub fn from_cascade(cascade: bool) -> RemoveMode {
        if cascade {
            RemoveMode::Cascade
        } else {
            RemoveMode::Restrict
        }
    }
}
