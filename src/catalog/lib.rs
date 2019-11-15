// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use failure::bail;
use rusqlite::params;
use serde::{Deserialize, Serialize};

use dataflow_types::{Index, Sink, Source, SourceConnector, View};
use expr::{GlobalId, Id, IdHumanizer};
use repr::QualName;
use repr::RelationDesc;

use crate::sql::SqlVal;

mod sql;

/// A `Catalog` keeps track of the SQL objects known to the planner.
///
/// For each object, it keeps track of both forward and reverse dependencies:
/// i.e., which objects are depended upon by the object, and which objects
/// depend upon the object. It enforces the SQL rules around dropping: an object
/// cannot be dropped until all of the objects that depend upon it are dropped.
/// It also enforces uniqueness of names.
#[derive(Debug)]
pub struct Catalog {
    id: usize,
    by_name: HashMap<QualName, GlobalId>,
    by_id: BTreeMap<GlobalId, CatalogEntry>,
    sqlite: rusqlite::Connection,
}

#[derive(Clone, Debug)]
pub struct CatalogEntry {
    inner: CatalogItem,
    used_by: Vec<GlobalId>,
    id: GlobalId,
    name: QualName,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogItem {
    Source(Source),
    View(View),
    Sink(Sink),
    Index(Index),
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
                view.relation_expr.global_uses(&mut out);
                out
            }
            CatalogItem::Index(idx) => {
                let mut out = Vec::new();
                out.push(idx.on_id);
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
    pub fn name(&self) -> &QualName {
        &self.name
    }
}

impl Catalog {
    /// Constructs a new `Catalog`.
    pub fn open(path: Option<&Path>) -> Result<Catalog, failure::Error> {
        let sqlite = match path {
            Some(path) => rusqlite::Connection::open(path)?,
            None => rusqlite::Connection::open_in_memory()?,
        };
        let mut catalog = Catalog {
            id: 0,
            by_name: HashMap::new(),
            by_id: BTreeMap::new(),
            sqlite,
        };

        // Create the on-disk schema, if it doesn't already exist.
        const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS catalog (
            id   blob PRIMARY KEY,
            name blob NOT NULL,
            item blob NOT NULL
        );";
        catalog.sqlite.execute(&SCHEMA, params![])?;

        // Load any existing catalog entries.
        let rows: Vec<_> = catalog
            .sqlite
            .prepare("SELECT id, name, item FROM catalog ORDER BY rowid")?
            .query_and_then(params![], |row| -> Result<_, failure::Error> {
                let id: SqlVal<GlobalId> = row.get(0)?;
                let name: SqlVal<QualName> = row.get(1)?;
                let item: SqlVal<CatalogItem> = row.get(2)?;
                Ok((id.0, name.0, item.0))
            })?
            .collect();
        for row in rows {
            let (id, name, item) = row?;
            catalog.insert_id_core(id, name, item);
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

    /// Returns the named catalog item, if it exists.
    ///
    /// See also [`Catalog::get`].
    pub fn try_get(&self, name: &QualName) -> Option<&CatalogEntry> {
        self.by_name.get(name).map(|id| &self.by_id[id])
    }

    /// Returns the named catalog item, or an error if it does not exist.
    ///
    /// See also [`Catalog::try_get`].
    pub fn get(&self, name: &QualName) -> Result<&CatalogEntry, failure::Error> {
        self.try_get(name)
            .ok_or_else(|| failure::err_msg(format!("catalog item '{}' does not exist", name)))
    }

    /// Inserts a new catalog item, returning an error if a catalog item with
    /// the same name already exists.
    ///
    /// The internal dependency graph is updated accordingly. The function will
    /// panic if any of `item`'s dependencies are not present in the store.
    pub fn insert(
        &mut self,
        name: QualName,
        item: CatalogItem,
    ) -> Result<GlobalId, failure::Error> {
        let id = self.allocate_id();
        self.insert_id(id, name, item)?;
        Ok(id)
    }

    pub fn insert_id(
        &mut self,
        id: GlobalId,
        name: QualName,
        item: CatalogItem,
    ) -> Result<(), failure::Error> {
        // Validate that we can insert the item.
        if self.by_name.contains_key(&name) {
            bail!("catalog item '{}' already exists", name)
        }
        if self.by_id.contains_key(&id) {
            bail!("catalog item with id {} already exists", id)
        }

        // Maybe update on-disk state.
        if let CatalogItem::Source(Source {
            connector: SourceConnector::Local,
            ..
        }) = item
        {
            // At the moment, local sources are always ephemeral.
        } else {
            let mut stmt = self
                .sqlite
                .prepare_cached("INSERT INTO catalog (id, name, item) VALUES (?, ?, ?)")?;
            stmt.execute(params![SqlVal(&id), SqlVal(&name), SqlVal(&item)])?;
        }

        // Update in-memory state.
        self.insert_id_core(id, name, item);
        Ok(())
    }

    fn insert_id_core(&mut self, id: GlobalId, name: QualName, item: CatalogItem) {
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
        self.by_name.insert(entry.name.clone(), entry.id);
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
    /// To actually remove the views, call [`Catalog::remove`] on each
    /// name in `to_remove`.
    pub fn plan_remove(
        &self,
        name: &QualName,
        mode: RemoveMode,
        to_remove: &mut Vec<GlobalId>,
    ) -> Result<(), failure::Error> {
        let metadata = match self.try_get(name) {
            Some(metadata) => metadata,
            None => return Ok(()),
        };
        match mode {
            RemoveMode::Restrict => {
                if !metadata
                    .used_by
                    .iter()
                    .all(|u| to_remove.iter().any(|r| r == u))
                {
                    bail!(
                        "cannot delete {}: still depended upon by catalog item '{}'",
                        name,
                        self.by_id[&metadata.used_by[0]].name()
                    )
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
            for u in metadata.uses() {
                if let Some(dep_metadata) = self.by_id.get_mut(&u) {
                    dep_metadata.used_by.retain(|u| *u != metadata.id)
                }
            }
            self.by_name
                .remove(&metadata.name)
                .expect("catalog out of sync");
        }
        let mut stmt = self
            .sqlite
            .prepare_cached("DELETE FROM catalog WHERE id = ?")
            .expect("catalog: sqlite failed");
        stmt.execute(params![SqlVal(id)])
            .expect("catalog: sqlite failed");
    }

    /// Iterates over the items in the catalog in order of increasing ID.
    pub fn iter(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.by_id.iter().map(|(_id, entry)| entry)
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
