// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use byteorder::{NetworkEndian, WriteBytesExt};
use futures::executor::block_on;
use futures::stream::StreamExt;
use log::{error, info, trace};

use dataflow::WorkerPersistenceData;
use dataflow_types::Timestamp;
use expr::GlobalId;
use repr::{Datum, Row};

#[derive(Clone, Debug)]
pub struct PersistenceConfig {
    /// Amount of time persister thread will sleep between consecutive attempts
    /// to update persistent state.
    pub flush_interval: Duration,
    /// Mininum number of records that need to have accumulated in a prefix of
    /// input data before that prefix can be flushed to persistent storage.
    pub flush_min_records: usize,
    /// Directory where all persistence information is stored.
    pub path: PathBuf,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PersistedRecord {
    offset: i64,
    timestamp: Timestamp,
    key: Vec<u8>,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct Partition {
    last_persisted_offset: Option<i64>,
    pending: Vec<PersistedRecord>,
}

#[derive(Debug)]
struct Source {
    id: GlobalId,
    path: PathBuf,
    // TODO: in a future where persistence supports more than just Kafka this
    // probably should be keyed on PartitionId
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
        key: Vec<u8>,
        payload: Vec<u8>,
    ) {
        // Start tracking this partition id if we are not already
        self.partitions.entry(partition_id).or_insert(Partition {
            last_persisted_offset: None,
            pending: Vec::new(),
        });

        if let Some(partition) = self.partitions.get_mut(&partition_id) {
            if let Some(last_persisted_offset) = partition.last_persisted_offset {
                if offset <= last_persisted_offset {
                    // Note that even though the dataflow workers receive data in
                    // order from the upstream source and they use a FIFO queue
                    // to send that data, we can't assume that the dataflow
                    // workers sent the data in order (for example, there may have
                    // been a delay to resolve the timestamp at an offset).
                    error!("Received an offset ({}) for source: {} partition: {} that was
                           lower than the most recent offset flushed to persistent storage {}. Ignoring.",
                           offset, self.id, partition_id, last_persisted_offset);
                    return;
                }
            }
            partition.pending.push(PersistedRecord {
                offset,
                timestamp,
                key,
                payload,
            });
        }
    }

    /// Determine the longest contiguous prefix of offsets available per partition
    // and if the prefix is sufficiently large, write it to disk.
    pub fn persist(&mut self, flush_min_records: usize) -> Result<(), anyhow::Error> {
        for (partition_id, partition) in self.partitions.iter_mut() {
            if partition.pending.is_empty() {
                // No data to persist here
                continue;
            }

            // TODO we should probably be extra careful and deduplicate by offset
            // here as well.
            partition.pending.sort();

            let mut prev = partition.last_persisted_offset;
            let mut prefix_length = 0;
            let mut prefix_start_offset = None;

            for p in partition.pending.iter() {
                if prefix_start_offset.is_none() {
                    prefix_start_offset = Some(p.offset);
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

            let prefix_end_offset = prev.expect("known to exist");
            trace!(
                "partition {} found a prefix of {:?}",
                partition_id,
                prefix_length
            );

            if prefix_length > flush_min_records {
                // We have a "large enough" prefix. Lets write it to a file
                let mut buf = Vec::new();
                for record in partition.pending.drain(..prefix_length) {
                    let row = Row::pack(&[
                        Datum::Int64(record.offset),
                        Datum::Int64(record.timestamp as i64),
                        Datum::Bytes(&record.key),
                        Datum::Bytes(&record.payload),
                    ]);

                    encode_row(&row, &mut buf);
                }

                // The offsets we put in this filename are 1-indexed
                // MzOffsets, so the starting number is off by 1 for something like
                // Kafka
                let filename = format!(
                    "materialize-{}-{}-{}-{}",
                    self.id,
                    partition_id,
                    prefix_start_offset.unwrap(),
                    prefix_end_offset
                );

                // We'll write down the data to a file with a `-tmp` prefix to
                // indicate a write was in progress, and then atomically rename
                // when we are done to indicate the write is complete.
                let mut path = self.path.clone();
                path.push(format!("{}-tmp", filename));

                std::fs::write(&path, buf)?;
                let final_path = path.with_file_name(filename);
                std::fs::rename(path, final_path)?;
                partition.last_persisted_offset = Some(prefix_end_offset);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum PersistenceMetadata {
    AddSource(GlobalId),
    DropSource(GlobalId),
    Shutdown,
}

pub struct Persister {
    data_rx: comm::mpsc::Receiver<WorkerPersistenceData>,
    metadata_rx: std::sync::mpsc::Receiver<PersistenceMetadata>,
    sources: HashMap<GlobalId, Source>,
    config: PersistenceConfig,
}

impl Persister {
    pub fn new(
        data_rx: comm::mpsc::Receiver<WorkerPersistenceData>,
        metadata_rx: std::sync::mpsc::Receiver<PersistenceMetadata>,
        config: PersistenceConfig,
    ) -> Self {
        Persister {
            data_rx,
            metadata_rx,
            sources: HashMap::new(),
            config,
        }
    }

    async fn update_persistence(&mut self) -> Result<bool, anyhow::Error> {
        while let Ok(metadata) = self.metadata_rx.try_recv() {
            match metadata {
                PersistenceMetadata::AddSource(id) => {
                    // Check if we already have a source
                    if self.sources.contains_key(&id) {
                        error!(
                            "Received signal to enable persistence for {} but it is already persisted. Ignoring.",
                            id
                        );
                        continue;
                    }

                    // Create a new subdirectory to store this source's data.
                    let mut source_path = self.config.path.clone();
                    source_path.push(format!("{}/", id));
                    fs::create_dir_all(&source_path).with_context(|| {
                        anyhow!(
                            "trying to create persistence directory: {:#?} for source: {}",
                            source_path,
                            id
                        )
                    })?;

                    let source = Source::new(id, source_path);
                    self.sources.insert(id, source);
                    info!("Enabled persistence for source: {}", id);
                }
                PersistenceMetadata::DropSource(id) => {
                    if !self.sources.contains_key(&id) {
                        // This will actually happen fairly often because the
                        // coordinator doesn't see which sources had persistence
                        // enabled on delete, so notifies the persistence thread
                        // for all drops.
                        trace!("Received signal to disable persistence for {} but it is not persisted. Ignoring.", id);
                        continue;
                    }

                    self.sources.remove(&id);
                    info!("Disabled persistence for source: {}", id);
                }
                PersistenceMetadata::Shutdown => {
                    return Ok(true);
                }
            };
        }

        // We need to bound the amount of time spent reading from the data channel to ensure we
        // don't neglect our other tasks.
        let timer = Instant::now();
        loop {
            match tokio::time::timeout(Duration::from_millis(5), self.data_rx.next()).await {
                Ok(None) => break,
                Ok(Some(Ok(data))) => {
                    if !self.sources.contains_key(&data.source_id) {
                        // TODO: dropping here is not actually a correct thing to do, as
                        // there may be a race between when we see the metadata informing
                        // us to track this source vs when we see the data itself.
                        error!(
                            "Received data for source {} that is not currently persisted. Ignoring.",
                            data.source_id
                        );
                        continue;
                    }

                    if let Some(source) = self.sources.get_mut(&data.source_id) {
                        source.insert_record(
                            data.partition,
                            data.offset,
                            data.timestamp,
                            data.key,
                            data.payload,
                        );
                    }
                }
                Ok(Some(Err(e))) => {
                    error!("received error from persistence rx: {}", e);
                    break;
                }
                Err(tokio::time::Elapsed { .. }) => break,
            }

            if timer.elapsed() > self.config.flush_interval {
                break;
            }
        }

        for (_, s) in self.sources.iter_mut() {
            s.persist(self.config.flush_min_records)?;
        }

        Ok(false)
    }
}

pub fn update(persister: Option<Persister>) {
    if let Some(mut persister) = persister {
        info!("Persistence thread starting with flush_interval: {:#?}, flush_min_records: {}, path: {}",
              persister.config.flush_interval,
              persister.config.flush_min_records,
              persister.config.path.display());
        loop {
            thread::sleep(persister.config.flush_interval);
            trace!("Persistence thread checking for updates.");
            let ret = block_on(async { persister.update_persistence().await });

            match ret {
                Ok(true) => break,
                Err(e) => {
                    error!("Persistence thread encountered error: {:#}", e);
                    error!("Persistence thread shutting down.
                           All current and future persisted sources on this materialized process will not continue to be persisted.");
                    break;
                }
                Ok(false) => continue,
            }
        }
    }
}

/// Write a length-prefixed Row to a buffer
fn encode_row(row: &Row, buf: &mut Vec<u8>) {
    // TODO assert that the row is small enough for its length to fit in 4 bytes
    let data = row.data();
    buf.write_u32::<NetworkEndian>(data.len() as u32)
        .expect("writes to vec cannot fail");
    buf.extend_from_slice(data);
}
