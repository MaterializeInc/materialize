// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use log::{error, info, trace};
use tokio::select;
use tokio::sync::mpsc;
use uuid::Uuid;

use dataflow::source::cache::RecordFileMetadata;
use dataflow::CacheMessage;
use dataflow_types::{ExternalSourceConnector, SourceConnector};
use expr::GlobalId;
use repr::CachedRecord;

// Interval at which Cacher will try to flush out pending records
static CACHE_FLUSH_INTERVAL: Duration = Duration::from_secs(600);

/// Configures the coordinator's source caching.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Maximum number of records that are allowed to be pending for a source
    /// before the cacher will attempt to flush that source immediately.
    pub max_pending_records: usize,
    /// Directory where all cache information is stored.
    pub path: PathBuf,
}

#[derive(Debug)]
struct Partition {
    last_cached_offset: Option<i64>,
    pending: Vec<CachedRecord>,
}

#[derive(Debug)]
struct Source {
    // Multiple source instances will send data to the same Source object
    // but the data will be deduplicated before it is cached.
    id: GlobalId,
    cluster_id: Uuid,
    path: PathBuf,
    // Number of records currently waiting to be written out.
    pending_records: usize,
    // Maximum number of records that are allowed to be pending before we
    // attempt to write immediately.
    max_pending_records: usize,
    // TODO: in a future where caching supports more than just Kafka this
    // probably should be keyed on PartitionId
    partitions: BTreeMap<i32, Partition>,
}

impl Source {
    fn new(id: GlobalId, cluster_id: Uuid, path: PathBuf, max_pending_records: usize) -> Self {
        Source {
            id,
            cluster_id,
            path,
            pending_records: 0,
            max_pending_records,
            partitions: BTreeMap::new(),
        }
    }

    fn insert_record(
        &mut self,
        partition_id: i32,
        record: CachedRecord,
    ) -> Result<(), anyhow::Error> {
        // Start tracking this partition id if we are not already
        self.partitions.entry(partition_id).or_insert(Partition {
            last_cached_offset: None,
            pending: Vec::new(),
        });

        if let Some(partition) = self.partitions.get_mut(&partition_id) {
            if let Some(last_cached_offset) = partition.last_cached_offset {
                if record.offset <= last_cached_offset {
                    // The cacher does not assume that dataflow workers will send data in
                    // any ordering. We can filter out the records that we have obviously do not
                    // need to think about here.
                    trace!(
                        "Received an offset ({}) for source: {} partition: {} that was
                           lower than the most recent offset flushed to cache {}. Ignoring.",
                        record.offset,
                        self.id,
                        partition_id,
                        last_cached_offset
                    );
                    return Ok(());
                }
            }
            partition.pending.push(record);
            self.pending_records += 1;
            if self.pending_records > self.max_pending_records {
                self.maybe_flush()?;
            }
        }

        Ok(())
    }

    /// Determine the longest contiguous prefix of offsets available per partition
    // and if the prefix is sufficiently large, flush it to cache.
    fn maybe_flush(&mut self) -> Result<(), anyhow::Error> {
        let mut cached_records = 0;
        for (partition_id, partition) in self.partitions.iter_mut() {
            if partition.pending.is_empty() {
                // No data to cache here
                continue;
            }

            let prefix = extract_prefix(partition.last_cached_offset, &mut partition.pending);

            let len = prefix.len();

            if len > 0 {
                let prefix_start_offset = prefix[0].offset;
                let prefix_end_offset = prefix.last().unwrap().offset;
                trace!("partition {} found a prefix of {:?}", partition_id, len);

                // We have a prefix. Lets write it to a file
                let mut buf = Vec::new();
                for record in prefix {
                    record.write_record(&mut buf)?;
                }

                // The offsets we put in this filename are 1-indexed
                // MzOffsets, so the starting number is off by 1 for something like
                // Kafka
                let file_name = RecordFileMetadata::generate_file_name(
                    self.cluster_id,
                    self.id,
                    *partition_id,
                    prefix_start_offset,
                    prefix_end_offset,
                );

                // We'll write down the data to a file with a `-tmp` prefix to
                // indicate a write was in progress, and then atomically rename
                // when we are done to indicate the write is complete.
                let tmp_path = self.path.join(format!("{}-tmp", file_name));
                let path = self.path.join(file_name);

                std::fs::write(&tmp_path, buf)?;
                std::fs::rename(tmp_path, path)?;
                partition.last_cached_offset = Some(prefix_end_offset);
                cached_records += len;
            }
        }

        assert!(
            cached_records <= self.pending_records,
            "cached {} records but only had {} pending",
            cached_records,
            self.pending_records
        );

        self.pending_records -= cached_records;

        // TODO(rkhaitan): Reevaluate this. On the one hand, we need to have something like this to make sure
        // we don't get spammed into trying to write every time on a topic with a missing offset / extreme
        // out of order-ness. On the other hand, this error is extremely opaque for the end user, and it's
        // unclear what a user could do to resolve it.
        if self.pending_records > self.max_pending_records {
            bail!(
                "failed to flush enough for source {}. Currently {} pending but max allowed is {}",
                self.id,
                self.pending_records,
                self.max_pending_records
            );
        }

        Ok(())
    }
}

pub struct Cacher {
    rx: mpsc::UnboundedReceiver<CacheMessage>,
    sources: HashMap<GlobalId, Source>,
    disabled_sources: HashSet<GlobalId>,
    pub config: CacheConfig,
}

impl Cacher {
    pub fn new(rx: mpsc::UnboundedReceiver<CacheMessage>, config: CacheConfig) -> Self {
        Cacher {
            rx,
            sources: HashMap::new(),
            disabled_sources: HashSet::new(),
            config,
        }
    }

    async fn cache(&mut self) -> Result<(), anyhow::Error> {
        // We need to bound the amount of time spent reading from the data channel to ensure we
        // don't neglect our other tasks of writing the data down.
        let mut interval = tokio::time::interval(CACHE_FLUSH_INTERVAL);
        loop {
            select! {
                data = self.rx.recv() => {
                    if let Some(data) = data {
                        self.handle_cache_message(data)?
                    } else {
                        break;
                    }
                }
                _ = interval.tick() => {
                    for (_, s) in self.sources.iter_mut() {
                        s.maybe_flush()?;
                    }

                }
            }
        }

        Ok(())
    }

    /// Process a new CacheMessage.
    fn handle_cache_message(&mut self, data: CacheMessage) -> Result<(), anyhow::Error> {
        match data {
            CacheMessage::Data(data) => {
                if !self.sources.contains_key(&data.source_id) {
                    if self.disabled_sources.contains(&data.source_id) {
                        // It's possible that there was a delay between when the coordinator
                        // deleted a source and when dataflow threads learned about that delete.
                        error!(
                            "Received data for source {} that has disabled caching. Ignoring.",
                            data.source_id
                        );
                    } else {
                        // We got data for a source that we don't currently track cache data for
                        // and we've never deleted. This isn't possible in the current implementation,
                        // as the coordinatr sends a CreateSource message to the cacher before sending
                        // anything to the dataflow workers, but this could become possible in the future.

                        self.disabled_sources.insert(data.source_id);
                        error!(
                            "Received data for unknown source {}. Disabling caching on the source.",
                            data.source_id
                        );
                    }

                    return Ok(());
                }

                if let Some(source) = self.sources.get_mut(&data.source_id) {
                    source.insert_record(data.partition_id, data.record)?;
                }
            }
            CacheMessage::AddSource(cluster_id, source_id) => {
                // Check if we already have a source
                if self.sources.contains_key(&source_id) {
                    error!(
                            "Received signal to enable caching for {} but it is already cached. Ignoring.",
                            source_id
                        );
                    return Ok(());
                }

                if self.disabled_sources.contains(&source_id) {
                    error!("Received signal to enable caching for {} but it has already been disabled. Ignoring.", source_id);
                    return Ok(());
                }

                // Create a new subdirectory to store this source's data.
                let source_path = self.config.path.join(source_id.to_string());
                fs::create_dir_all(&source_path).with_context(|| {
                    anyhow!(
                        "trying to create cache directory: {:#?} for source: {}",
                        source_path,
                        source_id
                    )
                })?;

                let source = Source::new(
                    source_id,
                    cluster_id,
                    source_path,
                    self.config.max_pending_records,
                );
                self.sources.insert(source_id, source);
                info!("Enabled caching for source: {}", source_id);
            }
            CacheMessage::DropSource(id) => {
                if !self.sources.contains_key(&id) {
                    // This will actually happen fairly often because the
                    // coordinator doesn't see which sources had caching
                    // enabled on delete, so notifies the cacher thread
                    // for all drops.
                    trace!(
                        "Received signal to disable caching for {} but it is not cached. Ignoring.",
                        id
                    );
                } else {
                    self.sources.remove(&id);
                    self.disabled_sources.insert(id);
                    info!("Disabled caching for source: {}", id);
                }
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        info!(
            "Caching thread starting with max_pending_records: {}, path: {}",
            self.config.max_pending_records,
            self.config.path.display()
        );
        trace!("Caching thread checking for updates.");
        let ret = self.cache().await;

        match ret {
            Ok(_) => (),
            Err(e) => {
                error!("Caching thread encountered error: {:#}", e);
                error!("All cached sources on this process will not continue to be cached.");
            }
        }
    }
}

/// Check if there are any cached files available for this source and if so,
/// use them, and start reading from the source at the appropriate offsets.
/// Returns a newly constructed source connector if the source was caching enabled
/// which may or may not have new data around previously cached files.
pub fn augment_connector(
    mut source_connector: SourceConnector,
    cache_directory: &Path,
    cluster_id: Uuid,
    source_id: GlobalId,
) -> Result<Option<SourceConnector>, anyhow::Error> {
    match &mut source_connector {
        SourceConnector::External {
            ref mut connector, ..
        } => {
            if !connector.caching_enabled() {
                // This connector has no caching, so do nothing.
                Ok(None)
            } else {
                augment_connector_inner(connector, cache_directory, cluster_id, source_id)?;
                Ok(Some(source_connector))
            }
        }
        SourceConnector::Local => Ok(None),
    }
}

fn augment_connector_inner(
    connector: &mut ExternalSourceConnector,
    cache_directory: &Path,
    cluster_id: Uuid,
    source_id: GlobalId,
) -> Result<(), anyhow::Error> {
    match connector {
        ExternalSourceConnector::Kafka(k) => {
            let mut read_offsets: HashMap<i32, i64> = HashMap::new();
            let mut paths = Vec::new();

            let source_path = cache_directory.join(source_id.to_string());

            // Not safe to assume that the directory we are trying to read will be there
            // so if its not lets just early exit.
            let entries = match std::fs::read_dir(&source_path) {
                Ok(entries) => entries,
                Err(e) => {
                    error!(
                        "Failed to read from cached source data from {}: {}",
                        source_path.display(),
                        e
                    );
                    return Ok(());
                }
            };

            for entry in entries {
                if let Ok(file) = entry {
                    let path = file.path();
                    if let Some(metadata) = RecordFileMetadata::from_path(&path)? {
                        if metadata.source_id != source_id {
                            error!("Ignoring cache file with invalid source id. Received: {} expected: {} path: {}",
                                   metadata.source_id,
                                   source_id,
                                   path.display());
                            continue;
                        }

                        if metadata.cluster_id != cluster_id {
                            error!("Ignoring cache file with invalid cluster id. Received: {} expected: {} path: {}",
                            metadata.cluster_id,
                            cluster_id,
                            path.display());
                            continue;
                        }

                        paths.push(path);

                        // TODO: we need to be more careful here to handle the case where we are for
                        // some reason missing some values here.
                        match read_offsets.get(&metadata.partition_id) {
                            None => {
                                read_offsets.insert(metadata.partition_id, metadata.end_offset);
                            }
                            Some(o) => {
                                if metadata.end_offset > *o {
                                    read_offsets.insert(metadata.partition_id, metadata.end_offset);
                                }
                            }
                        };
                    }
                }
            }

            k.start_offsets = read_offsets;
            k.cached_files = Some(paths);
        }
        _ => bail!("caching only enabled for Kafka sources at this time"),
    }

    Ok(())
}

// Given the input records, extract a prefix of records that are "dense," meaning they contain all
// the data for that range.
fn extract_prefix(
    starting_offset: Option<i64>,
    records: &mut Vec<CachedRecord>,
) -> Vec<CachedRecord> {
    records.sort_by(|a, b| (a.offset, a.predecessor.map(|p| -p)).cmp(&(b.offset, b.predecessor)));

    // Keep only the minimum predecessor we received for every offset
    records.dedup_by_key(|x| x.offset);
    if let Some(offset) = starting_offset {
        records.retain(|x| x.offset > offset);
    }

    let mut watermark = starting_offset;
    let mut prefix_length = 0;

    // Find the longest unbroken prefix where each subsequent record refers to the
    // preceding one as its predecessor.
    for p in records.iter() {
        // Note: None < Some(x) for all x.
        if p.predecessor <= watermark {
            prefix_length += 1;
            watermark = Some(p.offset);
        }
    }

    records.drain(..prefix_length).collect()
}

#[test]
fn test_extract_prefix() {
    use repr::Timestamp;

    fn record(offset: i64, predecessor: Option<i64>) -> CachedRecord {
        CachedRecord {
            offset,
            predecessor,
            timestamp: Timestamp::default(),
            key: Vec::new(),
            value: Vec::new(),
        }
    }

    for ((starting_offset, records), (expected_prefix, expected_remaining)) in vec![
        ((None, vec![]), (vec![], vec![])),
        // Basic case, just a single record.
        ((None, vec![(1, None)]), (vec![(1, None)], vec![])),
        // Two records, both in the prefix.
        (
            (None, vec![(1, None), (2, Some(1))]),
            (vec![(1, None), (2, Some(1))], vec![]),
        ),
        // Three records with a gap.
        (
            (None, vec![(1, None), (2, Some(1)), (4, Some(2))]),
            (vec![(1, None), (2, Some(1)), (4, Some(2))], vec![]),
        ),
        // Two records, but one supercedes the other.
        (
            (None, vec![(2, None), (2, Some(1))]),
            (vec![(2, None)], vec![]),
        ),
        // Three records, one is not in the prefix.
        (
            (None, vec![(1, None), (2, Some(1)), (4, Some(3))]),
            (vec![(1, None), (2, Some(1))], vec![(4, Some(3))]),
        ),
        // Four records, one gets superceded.
        (
            (
                None,
                vec![(1, None), (2, Some(1)), (4, Some(3)), (4, Some(2))],
            ),
            (vec![(1, None), (2, Some(1)), (4, Some(2))], vec![]),
        ),
        // Throw away a record whose data has already been accounted for.
        ((Some(2), vec![(1, None)]), (vec![], vec![])),
    ]
    .drain(..)
    {
        // TODO(justin): we could shuffle the input here, but non deterministic tests kind of suck,
        // maybe some number of seeded shuffles?
        let mut recs = records.iter().cloned().map(|(o, p)| record(o, p)).collect();
        let prefix = extract_prefix(starting_offset, &mut recs);
        assert_eq!(
            (prefix, recs),
            (
                expected_prefix
                    .iter()
                    .cloned()
                    .map(|(o, p)| record(o, p))
                    .collect(),
                expected_remaining
                    .iter()
                    .cloned()
                    .map(|(o, p)| record(o, p))
                    .collect()
            )
        );
    }
}
