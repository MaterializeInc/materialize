// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::{self, FromIterator};

use dataflow_types::logging::LoggingConfig;
use dataflow_types::{LocalSourceConnector, Sink, Source, SourceConnector, View};
use repr::{RelationDesc, RelationType};

/// A `Catalog` keeps track of the SQL objects known to the planner.
///
/// For each object, it keeps track of both forward and reverse dependencies:
/// i.e., which objects are depended upon by the object, and which objects
/// depend upon the object. It enforces the SQL rules around dropping: an object
/// cannot be dropped until all of the objects that depend upon it are dropped.
/// It also enforces uniqueness of names.
#[derive(Debug)]
pub struct Catalog {
    inner: HashMap<String, CatalogItemAndMetadata>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CatalogItem {
    Source(Source),
    View(View),
    Sink(Sink),
}

impl CatalogItem {
    /// Reports the name of this calatog item.
    pub fn name(&self) -> &str {
        match self {
            CatalogItem::Source(src) => &src.name,
            CatalogItem::Sink(sink) => &sink.name,
            CatalogItem::View(view) => &view.name,
        }
    }

    /// Reports the description of the datums produced by this catalog item.
    pub fn desc(&self) -> &RelationDesc {
        match self {
            CatalogItem::Source(src) => &src.desc,
            CatalogItem::Sink(_) => panic!(
                "programming error: CatalogItem.typ called on Sink variant, \
                 but sinks don't have a type"
            ),
            CatalogItem::View(view) => &view.desc,
        }
    }

    /// Reports the type of the datums produced by this catalog item.
    pub fn typ(&self) -> &RelationType {
        match self {
            CatalogItem::Source(src) => src.desc.typ(),
            CatalogItem::Sink(_) => panic!(
                "programming error: Dataflow.typ called on Sink variant, \
                 but sinks don't have a type"
            ),
            CatalogItem::View(view) => view.desc.typ(),
        }
    }
    /// Collects the names of the dataflows that this dataflow depends upon.
    pub fn uses(&self) -> Vec<&str> {
        match self {
            CatalogItem::Source(_src) => Vec::new(),
            CatalogItem::Sink(sink) => vec![&sink.from.0],
            CatalogItem::View(view) => {
                let mut out = Vec::new();
                view.relation_expr.unbound_uses(&mut out);
                out
            }
        }
    }
}

#[derive(Debug)]
struct CatalogItemAndMetadata {
    inner: CatalogItem,
    used_by: Vec<String>,
}

impl Catalog {
    /// Constructs a new `Catalog`.
    pub fn new(logging_config: Option<&LoggingConfig>) -> Catalog {
        match logging_config {
            Some(logging_config) => {
                Catalog::from_iter(logging_config.active_logs().iter().map(|log| {
                    CatalogItem::Source(Source {
                        name: log.name().to_string(),
                        connector: SourceConnector::Local(LocalSourceConnector {
                            uuid: uuid::Uuid::new_v4(),
                        }),
                        desc: log.schema(),
                    })
                }))
            }
            None => Catalog::from_iter(iter::empty()),
        }
    }

    /// Returns the named catalog item, if it exists.
    ///
    /// See also [`Catalog::get`].
    pub fn try_get(&self, name: &str) -> Option<&CatalogItem> {
        self.inner.get(name).map(|dm| &dm.inner)
    }

    /// Returns the named catalog item, or an error if it does not exist.
    ///
    /// See also [`Catalog::try_get`].
    pub fn get(&self, name: &str) -> Result<&CatalogItem, failure::Error> {
        self.try_get(name)
            .ok_or_else(|| failure::err_msg(format!("catalog item {} does not exist", name)))
    }

    /// Returns the descriptor for the named catalog item, or an error if the named
    /// catalog item does not exist.
    pub fn get_desc(&self, name: &str) -> Result<&RelationDesc, failure::Error> {
        match self.get(name)? {
            CatalogItem::Sink { .. } => bail!(
                "catalog item {} is a sink and cannot be depended upon",
                name
            ),
            item => Ok(item.desc()),
        }
    }

    /// Returns the type for the named catalog item, or an error if the named
    /// catalog item does not exist.
    pub fn get_type(&self, name: &str) -> Result<&RelationType, failure::Error> {
        match self.get(name)? {
            CatalogItem::Sink { .. } => bail!(
                "catalog item {} is a sink and cannot be depended upon",
                name
            ),
            item => Ok(item.typ()),
        }
    }

    /// Inserts a new catalog item, returning an error if a catalog item with the same
    /// name already exists.
    ///
    /// The internal dependency graph is updated accordingly. The function will
    /// panic if any of `item`'s dependencies are not present in the store.
    pub fn insert(&mut self, item: CatalogItem) -> Result<(), failure::Error> {
        let name = item.name().to_owned();
        match self.inner.entry(name.clone()) {
            Entry::Occupied(_) => bail!("catalog item {} already exists", name),
            Entry::Vacant(vacancy) => {
                vacancy.insert(CatalogItemAndMetadata {
                    inner: item.clone(),
                    used_by: Vec::new(),
                });
            }
        }
        for u in item.uses() {
            match self.inner.get_mut(u) {
                Some(entry) => entry.used_by.push(name.clone()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while installing {}",
                    u, name
                ),
            }
        }
        Ok(())
    }

    /// Determines whether it is feasible to remove the view named `name`
    /// according to `mode`. If `mode` is [`RemoveMode::Restrict`], then
    /// an error will be returned if any existing views depend upon the view
    /// specified for removal. If `mode` is [`RemoveMode::Cascade`], then the
    /// views that transitively depend upon the view specified for removal will
    /// be collected into the `to_remove` vector. In either mode, `name` is
    /// included in `to_remove`.
    ///
    /// To actually remove the views, call [`Catalog::remove`] on each
    /// name in `to_remove`.
    pub fn plan_remove(
        &self,
        name: &str,
        mode: RemoveMode,
        to_remove: &mut Vec<String>,
    ) -> Result<(), failure::Error> {
        let metadata = match self.inner.get(name) {
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
                        metadata.used_by[0]
                    )
                }
            }
            RemoveMode::Cascade => {
                let used_by = metadata.used_by.clone();
                for u in used_by {
                    self.plan_remove(&u, RemoveMode::Cascade, to_remove)?;
                }
            }
        }

        to_remove.push(name.to_owned());

        Ok(())
    }

    /// Unconditionally removes the named view. It is required that `name`
    /// come from the output of `plan_remove`; otherwise consistency rules may
    /// be violated.
    pub fn remove(&mut self, name: &str) {
        if let Some(metadata) = self.inner.remove(name) {
            for u in metadata.inner.uses() {
                if let Some(entry) = self.inner.get_mut(u) {
                    entry.used_by.retain(|u| u != name)
                }
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &CatalogItem)> {
        self.inner.iter().map(|(k, v)| (k.as_str(), &v.inner))
    }
}

impl FromIterator<CatalogItem> for Catalog {
    fn from_iter<I: IntoIterator<Item = CatalogItem>>(iter: I) -> Self {
        let mut store = Catalog {
            inner: std::collections::HashMap::new(),
        };
        for item in iter {
            store.insert(item).unwrap();
        }
        store
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
