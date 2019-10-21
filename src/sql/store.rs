// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::{self, FromIterator};

use dataflow_types::logging::LoggingConfig;
use dataflow_types::{Dataflow, LocalSourceConnector, Source, SourceConnector};
use repr::{RelationDesc, RelationType};

/// A `DataflowStore` keeps track of the SQL objects known to the planner.
///
/// For each object, it keeps track of both forward and reverse dependencies:
/// i.e., which objects are depended upon by the object, and which objects
/// depend upon the object. It enforces the SQL rules around dropping: an object
/// cannot be dropped until all of the objects that depend upon it are dropped.
/// It also enforces uniqueness of names.
#[derive(Debug)]
pub struct DataflowStore {
    inner: HashMap<String, DataflowAndMetadata>,
}

#[derive(Debug)]
struct DataflowAndMetadata {
    inner: Dataflow,
    used_by: Vec<String>,
}

impl DataflowStore {
    /// Constructs a new `DataflowStore`.
    pub fn new(logging_config: Option<&LoggingConfig>) -> DataflowStore {
        match logging_config {
            Some(logging_config) => {
                DataflowStore::from_iter(logging_config.active_logs().iter().map(|log| {
                    Dataflow::Source(Source {
                        name: log.name().to_string(),
                        connector: SourceConnector::Local(LocalSourceConnector {
                            uuid: uuid::Uuid::new_v4(),
                        }),
                        desc: log.schema(),
                    })
                }))
            }
            None => DataflowStore::from_iter(iter::empty()),
        }
    }

    /// Returns the named dataflow, if it exists.
    ///
    /// See also [`DataflowStore::get`].
    pub fn try_get(&self, name: &str) -> Option<&Dataflow> {
        self.inner.get(name).map(|dm| &dm.inner)
    }

    /// Returns the named dataflow, or an error if it does not exist.
    ///
    /// See also [`DataflowStore::try_get`].
    pub fn get(&self, name: &str) -> Result<&Dataflow, failure::Error> {
        self.try_get(name)
            .ok_or_else(|| failure::err_msg(format!("dataflow {} does not exist", name)))
    }

    /// Returns the descriptor for the named dataflow, or an error if the named
    /// dataflow does not exist.
    pub fn get_desc(&self, name: &str) -> Result<&RelationDesc, failure::Error> {
        match self.get(name)? {
            Dataflow::Sink { .. } => {
                bail!("dataflow {} is a sink and cannot be depended upon", name)
            }
            dataflow => Ok(dataflow.desc()),
        }
    }

    /// Returns the type for the named dataflow, or an error if the named
    /// dataflow does not exist.
    pub fn get_type(&self, name: &str) -> Result<&RelationType, failure::Error> {
        match self.get(name)? {
            Dataflow::Sink { .. } => {
                bail!("dataflow {} is a sink and cannot be depended upon", name)
            }
            dataflow => Ok(dataflow.typ()),
        }
    }

    /// Inserts a new dataflow, returning an error if a dataflow with the same
    /// name already exists.
    ///
    /// The internal dependency graph is updated accordingly. The function will
    /// panic if any of `dataflow`'s dependencies are not present in the store.
    pub fn insert(&mut self, dataflow: Dataflow) -> Result<(), failure::Error> {
        let name = dataflow.name().to_owned();
        match self.inner.entry(name.clone()) {
            Entry::Occupied(_) => bail!("dataflow {} already exists", name),
            Entry::Vacant(vacancy) => {
                vacancy.insert(DataflowAndMetadata {
                    inner: dataflow.clone(),
                    used_by: Vec::new(),
                });
            }
        }
        for u in dataflow.uses() {
            match self.inner.get_mut(u) {
                Some(entry) => entry.used_by.push(name.clone()),
                None => panic!(
                    "DataflowStore: missing dependent dataflow {} while installing {}",
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
    /// To actually remove the views, call [`DataflowStore::remove`] on each
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
                        "cannot delete {}: still depended upon by dataflow '{}'",
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

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Dataflow)> {
        self.inner.iter().map(|(k, v)| (k.as_str(), &v.inner))
    }
}

impl FromIterator<Dataflow> for DataflowStore {
    fn from_iter<I: IntoIterator<Item = Dataflow>>(iter: I) -> Self {
        let mut store = DataflowStore {
            inner: std::collections::HashMap::new(),
        };
        for dataflow in iter {
            store.insert(dataflow).unwrap();
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
