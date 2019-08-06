// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::FromIterator;

use crate::dataflow::SourceConnector;

use crate::dataflow::{Dataflow, LocalSourceConnector, Source};
use crate::repr::{ColumnType, RelationType, ScalarType};

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

impl Default for DataflowStore {
    fn default() -> DataflowStore {
        let mut store = DataflowStore {
            inner: HashMap::new(),
        };

        let operates_dataflow = Dataflow::Source(Source {
            name: "logs_operates".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::String).name("address"),
                    ColumnType::new(ScalarType::String).name("name"),
                ],
            },
        });
        store.insert(operates_dataflow).unwrap();

        let channels_dataflow = Dataflow::Source(Source {
            name: "logs_channels".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::String).name("scope"),
                    ColumnType::new(ScalarType::Int64).name("source node"),
                    ColumnType::new(ScalarType::Int64).name("source port"),
                    ColumnType::new(ScalarType::Int64).name("target node"),
                    ColumnType::new(ScalarType::Int64).name("target port"),
                ],
            },
        });
        store.insert(channels_dataflow).unwrap();

        let shutdown_dataflow = Dataflow::Source(Source {
            name: "logs_shutdown".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                ],
            },
        });
        store.insert(shutdown_dataflow).unwrap();

        let text_dataflow = Dataflow::Source(Source {
            name: "logs_text".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("text"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                ],
            },
        });
        store.insert(text_dataflow).unwrap();

        let elapsed_dataflow = Dataflow::Source(Source {
            name: "logs_elapsed".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("elapsed_ns"),
                ],
            },
        });
        store.insert(elapsed_dataflow).unwrap();

        let duration_dataflow = Dataflow::Source(Source {
            name: "logs_histogram".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("id"),
                    ColumnType::new(ScalarType::Int64).name("duration_ns"),
                    ColumnType::new(ScalarType::Int64).name("count"),
                ],
            },
        });
        store.insert(duration_dataflow).unwrap();

        let duration_dataflow = Dataflow::Source(Source {
            name: "logs_arrangement".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::Int64).name("operator"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::Int64).name("records"),
                    ColumnType::new(ScalarType::Int64).name("batches"),
                ],
            },
        });
        store.insert(duration_dataflow).unwrap();

        let duration_dataflow = Dataflow::Source(Source {
            name: "logs_peek_duration".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::String).name("UUID"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::Int64).name("duration_ns"),
                ],
            },
        });
        store.insert(duration_dataflow).unwrap();

        let duration_dataflow = Dataflow::Source(Source {
            name: "logs_peek_active".into(),
            connector: SourceConnector::Local(LocalSourceConnector {
                uuid: uuid::Uuid::new_v4(),
            }),
            typ: RelationType {
                column_types: vec![
                    ColumnType::new(ScalarType::String).name("UUID"),
                    ColumnType::new(ScalarType::Int64).name("worker"),
                    ColumnType::new(ScalarType::String).name("view"),
                    ColumnType::new(ScalarType::Int64).name("timestamp"),
                ],
            },
        });
        store.insert(duration_dataflow).unwrap();

        store
    }
}

impl FromIterator<Dataflow> for DataflowStore {
    fn from_iter<I: IntoIterator<Item = Dataflow>>(iter: I) -> Self {
        let mut store = DataflowStore::default();
        for dataflow in iter {
            store.insert(dataflow).unwrap();
        }
        store
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
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
