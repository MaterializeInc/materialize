// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use futures::stream::StreamExt;
use log::{error, info};

use dataflow_types::{Persistence, Timestamp, WorkerPersistenceData};
use expr::GlobalId;
use repr::{Datum, Row};

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PersistedRecord {
    offset: i64,
    timestamp: Timestamp,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct Partition {
    last_persisted_offset: Option<i64>,
    pending: Vec<PersistedRecord>,
}

#[derive(Debug)]
pub struct Source {
    id: GlobalId,
    path: PathBuf,
    // TODO have this use partition id
    partitions: BTreeMap<i32, Partition>,
}

impl Source {
    pub fn new(id: GlobalId, path: PathBuf) -> Self {
        Source {
            id,
            path,
            partitions: BTreeMap::new(),
        }
    }

    pub fn insert_record(
        &mut self,
        partition_id: i32,
        offset: i64,
        timestamp: Timestamp,
        data: Vec<u8>,
    ) {
        // Start tracking this partition id if we are not already
        self.partitions.entry(partition_id).or_insert(Partition {
            last_persisted_offset: None,
            pending: Vec::new(),
        });

        if let Some(partition) = self.partitions.get_mut(&partition_id) {
            partition.pending.push(PersistedRecord {
                offset,
                timestamp,
                data,
            });
        }
    }

    pub fn persist(&mut self) {
        for (partition_id, partition) in self.partitions.iter_mut() {
            if partition.pending.is_empty() {
                // No data to persist here
                continue;
            }

            partition.pending.sort();

            let mut prev = partition.last_persisted_offset;
            let mut prefix_length = 0;
            let mut start_offset = None;

            for p in partition.pending.iter() {
                if start_offset.is_none() {
                    start_offset = Some(p.offset);
                }

                match prev {
                    None => {
                        prefix_length += 1;
                        prev = Some(p.offset);
                    }
                    Some(offset) => {
                        if p.offset == offset + 1 {
                            prefix_length += 1;
                            prev = Some(p.offset);
                        } else {
                            break;
                        }
                    }
                }
            }

            let end_offset = prev.expect("known to exist");
            info!(
                "partition {} found a prefix of {:?}",
                partition_id, prefix_length
            );

            // TODO make this configurable
            if prefix_length > 10 {
                let mut buf = Vec::new();
                // We have a "large enough" prefix. Lets write it to a file
                for record in partition.pending.drain(..prefix_length) {
                    let row = Row::pack(&[
                        Datum::Int64(record.offset),
                        Datum::Int64(record.timestamp as i64),
                        Datum::Bytes(&record.data),
                    ]);

                    row.encode(&mut buf);
                }

                let filename = format!(
                    "materialize-source-{}-{}-{}-{}",
                    self.id,
                    partition_id,
                    start_offset.unwrap(),
                    end_offset + 1
                );
                let mut path = self.path.clone();
                path.push(format!("{}-tmp", filename));
                // TODO fix this
                std::fs::write(&path, buf).expect("file writes expected to work");
                let final_path = path.with_file_name(filename);
                std::fs::rename(path, final_path).expect("file renames expected to work");
                partition.last_persisted_offset = Some(end_offset);
            }
        }
    }
}

#[derive(Debug)]
pub enum PersistenceMetadata {
    AddSource(GlobalId, Persistence),
    DropSource(GlobalId),
    Shutdown,
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
            let shutdown = block_on(async { self.update_persistence().await });

            if shutdown {
                break;
            }
        }
    }

    async fn update_persistence(&mut self) -> bool {
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
                    let source = Source::new(id, persistence.path.clone());
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
                PersistenceMetadata::Shutdown => {
                    // TODO more robust cleanup
                    return true;
                }
            };
        }

        loop {
            match tokio::time::timeout(Duration::from_secs(1), self.data_rx.next()).await {
                Ok(None) => break,
                Ok(Some(Ok(data))) => {
                    info!("received some data to be persisted");
                    if !self.sources.contains_key(&data.source_id) {
                        error!(
                            "received some data for source {} that we are not currently tracking.",
                            data.source_id
                        );
                        continue;
                    }

                    let source = self
                        .sources
                        .get_mut(&data.source_id)
                        .expect("source known to exist");

                    source.insert_record(data.partition, data.offset, data.timestamp, data.data);
                }
                Ok(Some(Err(e))) => {
                    error!("received error from persistence rx: {}", e);
                    break;
                }
                Err(tokio::time::Elapsed { .. }) => break,
            }
        }

        for (_, s) in self.sources.iter_mut() {
            s.persist();
        }

        false
    }
}
