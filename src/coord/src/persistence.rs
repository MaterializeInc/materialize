// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use futures::stream::StreamExt;
use log::{error, info};

use dataflow_types::{Persistence, WorkerPersistenceData};
use expr::GlobalId;

#[derive(Debug)]
pub struct Source {
    id: GlobalId,
    path: PathBuf,
}

#[derive(Debug)]
pub enum PersistenceMetadata {
    AddSource(GlobalId, Persistence),
    DropSource(GlobalId),
}

pub struct Persister {
    data_rx: comm::mpsc::Receiver<WorkerPersistenceData>,
    metadata_rx: std::sync::mpsc::Receiver<PersistenceMetadata>,
    sources: HashMap<GlobalId, Source>,
}

impl Persister {
    pub fn new(
        data_rx: comm::mpsc::Receiver<WorkerPersistenceData>,
        metadata_rx: std::sync::mpsc::Receiver<PersistenceMetadata>,
    ) -> Self {
        Persister {
            data_rx,
            metadata_rx,
            sources: HashMap::new(),
        }
    }

    pub fn update(&mut self) {
        loop {
            thread::sleep(Duration::from_secs(1));
            block_on(async {
                self.update_persistence().await;
            });
        }
    }

    async fn update_persistence(&mut self) {
        while let Ok(metadata) = self.metadata_rx.try_recv() {
            match metadata {
                PersistenceMetadata::AddSource(id, persistence) => {
                    info!("[remove]: persister told to add a source");
                    // Check if we already have a source
                    if self.sources.contains_key(&id) {
                        error!(
                            "trying to start persisting {} but it is already persisted",
                            id
                        );
                        continue;
                    }
                    // Allocate a new source
                    let source = Source {
                        id,
                        path: persistence.path.clone(),
                    };

                    self.sources.insert(id, source);
                }
                PersistenceMetadata::DropSource(id) => {
                    info!("[remove]: persister told to drop a source");
                    if !self.sources.contains_key(&id) {
                        error!("trying to stop persisting {} but it is not persisted", id);
                        continue;
                    }

                    self.sources.remove(&id);
                }
            };
        }

        loop {
            match tokio::time::timeout(Duration::from_secs(1), self.data_rx.next()).await {
                Ok(None) => break,
                Ok(Some(_)) => {
                    info!("received some data to be persisted");
                }
                Err(tokio::time::Elapsed { .. }) => break,
            }
        }
    }
}
