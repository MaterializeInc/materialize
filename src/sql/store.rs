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
use repr::RelationType;

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
    pub fn new(logging_config: Option<&LoggingConfig>) -> DataflowStore {
        match logging_config {
            Some(logging_config) => {
                DataflowStore::from_iter(logging_config.active_logs().iter().map(|log| {
                    Dataflow::Source(Source {
                        name: log.name().to_string(),
                        connector: SourceConnector::Local(LocalSourceConnector {
                            uuid: uuid::Uuid::new_v4(),
                        }),
                        typ: log.schema(),
                        pkey_indices: Vec::new(),
                    })
                }))
            }
            None => DataflowStore::from_iter(iter::empty()),
        }
    }

    pub fn try_get(&self, name: &str) -> Option<&Dataflow> {
        self.inner.get(name).map(|dm| &dm.inner)
    }

    pub fn get(&self, name: &str) -> Result<&Dataflow, failure::Error> {
        self.try_get(name)
            .ok_or_else(|| failure::err_msg(format!("dataflow {} does not exist", name)))
    }

    pub fn get_type(&self, name: &str) -> Result<&RelationType, failure::Error> {
        match self.get(name)? {
            Dataflow::Sink { .. } => {
                bail!("dataflow {} is a sink and cannot be depended upon", name)
            }
            dataflow => Ok(dataflow.typ()),
        }
    }

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

    pub fn remove(
        &mut self,
        name: &str,
        mode: RemoveMode,
        removed: &mut Vec<Dataflow>,
    ) -> Result<(), failure::Error> {
        let metadata = match self.inner.get(name) {
            Some(metadata) => metadata,
            None => return Ok(()),
        };

        match mode {
            RemoveMode::Restrict => {
                if !metadata.used_by.is_empty() {
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
                    // We may have removed other dependent dataflows on a prior
                    // turn of the loop, so cascading removes must not fail, or
                    // we'll have violated atomicity. Therefore unwrap instead
                    // of propagating the error.
                    self.remove(&u, RemoveMode::Cascade, removed).unwrap();
                }
            }
        }

        // Safe to unwrap, because we already proved above that name exists in
        // self.inner.
        let metadata = self.inner.remove(name).unwrap();
        for u in metadata.inner.uses() {
            match self.inner.get_mut(u) {
                Some(entry) => entry.used_by.retain(|u| u != name),
                None => panic!(
                    "DataflowStore: missing dependent dataflow {} while removing {}",
                    u, name
                ),
            }
        }

        removed.push(metadata.inner);

        Ok(())
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
