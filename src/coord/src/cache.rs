// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use log::{error, info, trace};
use tokio::select;
use tokio::sync::mpsc;
use uuid::Uuid;

use dataflow::source::cache::RecordFileMetadata;
use dataflow::CacheMessage;
use dataflow_types::{ExternalSourceConnector, SourceConnector, Update};
use expr::GlobalId;
use repr::{CachedRecord, Row, Timestamp};

// Interval at which Cacher will try to flush out pending records
static CACHE_FLUSH_INTERVAL: Duration = Duration::from_secs(600);

// Limit at which we will make a new log segment
// TODO: make this configurable and / or bump default to be larger.
static TABLE_MAX_LOG_SEGMENT_SIZE: usize = 50;

#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Maximum number of records that are allowed to be pending for a source
    /// before we attempt to flush that source immediately.
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

pub struct TableLogSegments {
    id: GlobalId,
    base_path: PathBuf,
    current_file: File,
    current_bytes_written: usize,
    total_bytes_written: usize,
    next_sequence_number: usize,
}

impl TableLogSegments {
    fn create_table(id: GlobalId, base_path: PathBuf) -> Result<Self, anyhow::Error> {
        // First lets create the file where these log segments will live
        fs::create_dir_all(&base_path).with_context(|| {
            anyhow!(
                "trying to create directory for table: {} path: {:#?}",
                id,
                base_path
            )
        })?;

        // TODO: clean this up
        let file_path = base_path.join(format!("log-0"));
        let file = fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&file_path)
            .with_context(|| {
                anyhow!(
                    "trying to create file for table: {} path: {:#?}",
                    id,
                    file_path
                )
            })?;

        let mut ret = Self {
            id,
            base_path,
            current_file: file,
            current_bytes_written: 0,
            total_bytes_written: 0,
            next_sequence_number: 1,
        };

        // Write a timestamp message here so we can establish `lower` for the first log
        // segment.
        ret.write_progress(0)?;
        Ok(ret)
    }

    fn write_updates(&mut self, updates: &[Update]) -> Result<(), anyhow::Error> {
        // TODO The allocation discipline for writes could probably be a lot
        // better - each WAL writer could keep a buffer that it writes into
        // for everything and fall back to allocating stuff if we run out of
        // space.
        let mut buf = Vec::new();
        for update in updates {
            encode_update(update, &mut buf)?;
        }

        let len = buf.len();
        self.current_file.write_all(&buf)?;
        self.current_file.flush()?;
        self.current_bytes_written += len;
        self.total_bytes_written += len;

        Ok(())
    }

    fn write_progress(&mut self, timestamp: Timestamp) -> Result<(), anyhow::Error> {
        let buf = encode_progress(timestamp)?;
        let len = buf.len();
        self.current_file.write_all(&buf)?;
        self.current_file.flush()?;
        self.current_bytes_written += len;
        self.total_bytes_written += len;

        Ok(())
    }

    fn close_up_to(&mut self, timestamp: Timestamp) -> Result<(), anyhow::Error> {
        self.write_progress(timestamp)?;

        // We only want to rotate the log segments at progress messages
        // so that we can extract batches.
        if self.current_bytes_written > TABLE_MAX_LOG_SEGMENT_SIZE {
            self.rotate_log_segment()?;
            // Write the progress update to the new file to make sure we
            // know the `lower` for the new file
            self.write_progress(timestamp)?;
        }

        Ok(())
    }

    fn drop_table(self) -> Result<(), anyhow::Error> {
        let path = self.base_path.clone();

        // I never remember the rules for what happens if you delete
        // a directory with a open fd from that directory, so explicitly
        // dropping the fd first
        // Maybe a Option<File> is a better interface?
        drop(self);
        fs::remove_dir_all(path)?;

        Ok(())
    }

    fn rotate_log_segment(&mut self) -> Result<(), anyhow::Error> {
        let old_file_path = self
            .base_path
            .join(format!("log-{}", self.next_sequence_number - 1));
        let old_file_rename = self
            .base_path
            .join(format!("log-{}-final", self.next_sequence_number - 1));
        let new_file_path = self
            .base_path
            .join(format!("log-{}", self.next_sequence_number));

        // First lets open the new file
        let file = fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&new_file_path)
            .with_context(|| {
                anyhow!(
                    "trying to create file for table: {} path: {:#?}",
                    self.id,
                    new_file_path
                )
            })?;

        let old_file = std::mem::replace(&mut self.current_file, file);
        old_file.sync_all()?;
        drop(old_file);
        fs::rename(old_file_path, old_file_rename)?;

        // Update our own local state
        self.next_sequence_number += 1;
        self.current_bytes_written = 0;
        Ok(())
    }

    fn reload_table(
        id: GlobalId,
        base_path: PathBuf,
    ) -> Result<(Self, Vec<TableUpdates>), anyhow::Error> {
        // First lets create the directory
        fs::create_dir_all(&base_path).with_context(|| {
            anyhow!(
                "trying to create directory for table: {} path: {:#?}",
                id,
                base_path
            )
        })?;

        let mut unfinished_file = None;
        let mut finished_files = vec![];
        // If its not empty, list out all of the files. There should be
        // exactly one unfinished file, and potentially more than one
        // finished file. Go ahead and read in all of the finished files in
        // sequence number order, and then set yourself up to write from the
        // unfinished log file
        let entries = std::fs::read_dir(&base_path)?;
        for entry in entries {
            if let Ok(file) = entry {
                let path = file.path();
                let file_name = path.file_name();
                if file_name.is_none() {
                    continue;
                }

                let file_name = file_name.unwrap().to_str();

                if file_name.is_none() {
                    continue;
                }

                let file_name = file_name.unwrap();

                if !file_name.starts_with("log-") {
                    continue;
                }

                if !file_name.ends_with("final") {
                    assert!(unfinished_file.is_none());
                    let parts: Vec<_> = file_name.split('-').collect();
                    let sequence_number = parts[1].parse::<usize>()?;
                    unfinished_file = Some((path.to_path_buf(), sequence_number));
                } else {
                    finished_files.push(path.to_path_buf());
                }
            }
        }

        if let Some((unfinished_file, sequence_number)) = unfinished_file {
            // Go ahead and load the old data
            let mut updates = vec![];
            for path in finished_files {
                let data = fs::read(path).unwrap();
                updates.extend(TableIter::new(data));
            }

            let unfinished_data = fs::read(&unfinished_file).unwrap();
            updates.extend(TableIter::new(unfinished_data));

            // lets start writing from the file
            let file = fs::OpenOptions::new()
                .append(true)
                .open(&unfinished_file)
                .unwrap();

            let ret = Self {
                id,
                base_path,
                current_file: file,
                current_bytes_written: 0,
                total_bytes_written: 0,
                next_sequence_number: sequence_number + 1,
            };

            Ok((ret, updates))
        } else {
            // Then if its empty, we are probably migrating from a version that didn't
            // have persistent tables. Go ahead and set things up normally as you
            let ret = Self::create_table(id, base_path)?;
            Ok((ret, vec![]))
        }
    }
}

pub struct Tables {
    path: PathBuf,
    tables: HashMap<GlobalId, TableLogSegments>,
}

impl Tables {
    pub fn new(path: PathBuf) -> Result<Self, anyhow::Error> {
        fs::create_dir_all(&path)
            .with_context(|| anyhow!("trying to create tables directory: {:#?}", path))?;
        Ok(Self {
            path,
            tables: HashMap::new(),
        })
    }

    // TODO make this path use cluster_id
    fn get_path(&self, id: GlobalId) -> PathBuf {
        self.path.join(id.to_string())
    }

    pub fn create_table(&mut self, id: GlobalId) -> Result<(), anyhow::Error> {
        if self.tables.contains_key(&id) {
            bail!("asked to create table {:?} that was already created", id)
        }

        let table_base_path = self.get_path(id);
        let table_log = TableLogSegments::create_table(id, table_base_path)?;
        self.tables.insert(id, table_log);
        Ok(())
    }

    pub fn drop_table(&mut self, id: GlobalId) -> Result<(), anyhow::Error> {
        if !self.tables.contains_key(&id) {
            bail!("asked to delete table {:?} that doesn't exist", id)
        }

        let table = self.tables.remove(&id).expect("table known to exist");
        table.drop_table()?;

        Ok(())
    }

    pub fn write_updates(&mut self, id: GlobalId, updates: &[Update]) -> Result<(), anyhow::Error> {
        if !self.tables.contains_key(&id) {
            // TODO get rid of this later
            panic!("asked to write to unknown table: {}", id)
        }

        let table = self.tables.get_mut(&id).expect("table known to exist");
        table.write_updates(updates)?;
        Ok(())
    }

    pub fn reload_table(&mut self, id: GlobalId) -> Result<Vec<TableUpdates>, anyhow::Error> {
        let table_base_path = self.get_path(id);
        let (table, updates) = TableLogSegments::reload_table(id, table_base_path)?;
        self.tables.insert(id, table);

        Ok(updates)
    }

    pub fn close_up_to(&mut self, timestamp: Timestamp) -> Result<(), anyhow::Error> {
        // For all tables, indicate that <timestamp> is now the min timestamp going
        // forward
        for (_, table) in self.tables.iter_mut() {
            table.close_up_to(timestamp)?;
        }

        Ok(())
    }
}

/// Write a 20 byte header + length-prefixed Row to a buffer
/// The header contains 4 byte bool - is it a progress update true / false (0x1, 0x0)?
/// TODO could use the other bytes for some magic
/// 8 bytes timestamp
/// 8 bytes diff
fn encode_update(update: &Update, buf: &mut Vec<u8>) -> Result<(), anyhow::Error> {
    assert!(update.diff != 0);
    assert!(update.timestamp > 0);
    let data = update.row.data();

    if data.len() >= u32::MAX as usize {
        bail!("failed to encode row: row too large");
    }

    // Write out the header
    // Not a progress update so - 0
    buf.write_u32::<NetworkEndian>(0).unwrap();
    buf.write_u64::<NetworkEndian>(update.timestamp).unwrap();
    buf.write_i64::<NetworkEndian>(update.diff as i64).unwrap();

    // Now write out the data
    buf.write_u32::<NetworkEndian>(data.len() as u32)
        .expect("writes to vec cannot fail");
    buf.extend_from_slice(data);
    Ok(())
}

fn encode_progress(timestamp: Timestamp) -> Result<Vec<u8>, anyhow::Error> {
    let mut buf: Vec<u8> = Vec::with_capacity(12);
    // This is a progress update so - 1
    buf.write_u32::<NetworkEndian>(1).unwrap();
    buf.write_u64::<NetworkEndian>(timestamp).unwrap();

    Ok(buf)
}

fn read_updates(
    buf: &[u8],
    mut offset: usize,
    mut lower: Option<Timestamp>,
) -> Option<(TableUpdates, usize)> {
    if offset >= buf.len() {
        return None;
    }

    let mut updates = Vec::new();
    let mut upper = None;
    while offset < buf.len() {
        // Let's start by only looking at the buffer at the offset.
        let (_, data) = buf.split_at(offset);

        // Let's read the header first
        let mut cursor = Cursor::new(&data);

        let is_progress = cursor.read_u32::<NetworkEndian>().unwrap();

        if is_progress != 0 {
            // If this is a progress message let's seal a new
            // set of updates.

            // Lets figure out the upper bound.
            let timestamp = cursor.read_u64::<NetworkEndian>().unwrap();
            upper = Some(timestamp);
            if lower.is_none() {
                lower = Some(timestamp);
            }
            // Advance the offset past what we've read.
            offset += 12;
            break;
        }

        // Otherwise lets read the data.
        let timestamp = cursor.read_u64::<NetworkEndian>().unwrap();
        let diff = cursor.read_i64::<NetworkEndian>().unwrap() as isize;
        let len = cursor.read_u32::<NetworkEndian>().unwrap() as usize;

        //assert!(timestamp > 0);
        assert!(diff != 0);

        // Grab the next len bytes after the 24 byte length header, and turn
        // it into a vector so that we can extract things from it as a Row.
        // TODO: could we avoid the extra allocation here?
        let (_, rest) = data.split_at(24);
        let row = rest[..len].to_vec();

        let row = unsafe { Row::new(row) };
        updates.push(Update {
            row,
            timestamp,
            diff,
        });

        // Update the offset to account for the data we just read
        offset = offset + 24 + len
    }

    // At the end of the loop lets check some invariants.
    let lower = lower.unwrap();
    let upper = upper.unwrap();
    assert!(upper >= lower);
    if upper == lower {
        assert!(updates.len() == 0);
    }

    Some((
        TableUpdates {
            upper,
            lower,
            updates,
        },
        offset,
    ))
}

/// Iterator through a set of table updates.
#[derive(Debug)]
pub struct TableIter {
    /// Underlying data from which we read the records.
    pub data: Vec<u8>,
    /// Offset into the data.
    pub offset: usize,
    pub lower: Option<Timestamp>,
}

pub struct TableUpdates {
    pub upper: Timestamp,
    pub lower: Timestamp,
    pub updates: Vec<Update>,
}

impl Iterator for TableIter {
    type Item = TableUpdates;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((updates, next_offset)) = read_updates(&self.data, self.offset, self.lower) {
            self.offset = next_offset;
            self.lower = Some(updates.upper);
            Some(updates)
        } else {
            None
        }
    }
}

impl TableIter {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            offset: 0,
            lower: None,
        }
    }
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
