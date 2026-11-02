// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The log of a Kafka listener source, which is the durable copy of every record it accepts.
//!
//! The log is a persist shard with three kinds of rows: a row per produced record, a row per batch
//! from an idempotent producer, recording the batch's topic, producer, epoch, sequence numbers and
//! base offset, and a row per compacted record. A batch's rows commit in one append, and the
//! source acknowledges the batch once that append commits, so an acknowledged batch is in the log
//! whole and a batch that is not in the log was never acknowledged.
//!
//! The source's exports are derived from the log: a record is emitted to the exports of its
//! topic once its append commits, and an export that restarts is sent the records at and beyond
//! its upper again, read back from the log. Several exports can therefore hold the same topic,
//! each at its own pace, and an export created later is sent the topic's records from the start.
//!
//! Record rows are never retracted, so the log grows with every record. Batch rows are retracted
//! once [`ProducerStates`] forgets their batch, so the log holds a few batch rows per producer.
//! The log's timestamps count appends, and have no relation to offsets.
//!
//! A compacted row stores a record that a transaction committed with the source's records, such
//! as a Materialize Kafka sink's progress record, with the topic whose batch carried it. A key's
//! earlier row is retracted once a newer one is in the log. Rows carry a version that increases
//! with every compacted row, so that recovery can tell which of a key's rows is the latest.
//!
//! `compare_and_append` also fences writers. If another replica appends to the log, the next
//! append of this one fails with an upper mismatch and the source restarts.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use mz_kafka_server::{AppendedBatch, CompactedRecord, ProducedRecord, ProducerStates};
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::batch::Batch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistLocation};
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, GlobalId, RelationDesc, Row, SqlScalarType, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::sync::mpsc;

/// How often the log's since follows its upper.
const COMPACTION_INTERVAL: Duration = Duration::from_secs(1);

/// The schema of the log shard. A row is a compacted row if `topic` is set, a record row if
/// `offset` is set, and a batch row otherwise. Each kind leaves the others' columns null, except
/// `source_topic`, which all set: the topic of a batch or record, and the topic whose batch
/// carried a compacted record. `key` and `value` hold both records' and compacted records' keys
/// and values.
///
/// Persist sorts and merges rows by comparing their columns in order, so `offset` leads: record
/// rows then compare by offset instead of by their value bytes. With the key and value first,
/// row comparisons took 1.07 of the 3.7 cores the replica used at 400k records/s, and 0.45
/// with `source_topic` and `offset` first.
static LOG_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("offset", SqlScalarType::UInt64.nullable(true))
        .with_column("source_topic", SqlScalarType::String.nullable(false))
        .with_column("producer_id", SqlScalarType::Int64.nullable(true))
        .with_column("epoch", SqlScalarType::Int16.nullable(true))
        .with_column("first_sequence", SqlScalarType::Int32.nullable(true))
        .with_column("last_sequence", SqlScalarType::Int32.nullable(true))
        .with_column("base_offset", SqlScalarType::Int64.nullable(true))
        .with_column("appended_at_ms", SqlScalarType::Int64.nullable(true))
        .with_column("topic", SqlScalarType::String.nullable(true))
        .with_column("key", SqlScalarType::Bytes.nullable(true))
        .with_column("value", SqlScalarType::Bytes.nullable(true))
        .with_column("upper", SqlScalarType::UInt64.nullable(true))
        .with_column("version", SqlScalarType::UInt64.nullable(true))
        .with_column("headers", SqlScalarType::Bytes.nullable(true))
        .with_column("timestamp", SqlScalarType::Int64.nullable(true))
        .finish()
});

/// A row of the log.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Entry {
    Batch(BatchEntry),
    Compacted(CompactedEntry),
    Record(RecordEntry),
}

/// A produced record, as recorded in the log.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RecordEntry {
    pub(super) topic: Arc<str>,
    pub(super) offset: u64,
    pub(super) record: ProducedRecord,
}

/// A batch of an idempotent producer, as recorded in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BatchEntry {
    pub(super) topic: Arc<str>,
    pub(super) producer_id: i64,
    pub(super) epoch: i16,
    pub(super) batch: AppendedBatch,
}

/// A compacted record, as recorded in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompactedEntry {
    /// The topic whose batch carried the record.
    pub(super) home: Arc<str>,
    pub(super) record: CompactedRecord,
    /// The offset after the records of the transaction that wrote the record.
    pub(super) upper: u64,
    pub(super) version: u64,
}

/// Encodes record headers as a sequence of `key length, key, value length or u32::MAX, value`,
/// with lengths as big-endian u32s.
fn encode_headers(headers: &[(String, Option<Bytes>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (key, value) in headers {
        let len = |n: usize| {
            u32::try_from(n)
                .expect("header shorter than 4 GiB")
                .to_be_bytes()
        };
        buf.extend_from_slice(&len(key.len()));
        buf.extend_from_slice(key.as_bytes());
        match value {
            Some(value) => {
                buf.extend_from_slice(&len(value.len()));
                buf.extend_from_slice(value);
            }
            None => buf.extend_from_slice(&u32::MAX.to_be_bytes()),
        }
    }
    buf
}

fn decode_headers(mut buf: &[u8]) -> anyhow::Result<Vec<(String, Option<Bytes>)>> {
    fn take<'a>(buf: &mut &'a [u8], n: usize) -> anyhow::Result<&'a [u8]> {
        anyhow::ensure!(buf.len() >= n, "truncated headers");
        let (head, tail) = buf.split_at(n);
        *buf = tail;
        Ok(head)
    }
    fn len(buf: &mut &[u8]) -> anyhow::Result<u32> {
        Ok(u32::from_be_bytes(
            take(buf, 4)?.try_into().expect("4 bytes"),
        ))
    }
    let mut headers = Vec::new();
    while !buf.is_empty() {
        let key_len = usize::try_from(len(&mut buf)?).expect("u32 fits in usize");
        let key = String::from_utf8(take(&mut buf, key_len)?.to_vec())?;
        let value = match len(&mut buf)? {
            u32::MAX => None,
            n => Some(Bytes::copy_from_slice(take(
                &mut buf,
                usize::try_from(n).expect("u32 fits in usize"),
            )?)),
        };
        headers.push((key, value));
    }
    Ok(headers)
}

impl Entry {
    /// Approximately how many bytes the entry takes in memory.
    fn size(&self) -> usize {
        // Covers the fixed fields of batch and compacted entries.
        const OVERHEAD: usize = 64;
        match self {
            Entry::Batch(_) => OVERHEAD,
            Entry::Compacted(e) => {
                let value = e.record.value.as_ref().map_or(0, |v| v.len());
                OVERHEAD + e.record.topic.len() + e.record.key.len() + value
            }
            Entry::Record(e) => e.record.size(),
        }
    }

    fn encode(&self) -> Row {
        match self {
            Entry::Batch(e) => Row::pack_slice(&[
                Datum::Null,
                Datum::String(&e.topic),
                Datum::Int64(e.producer_id),
                Datum::Int16(e.epoch),
                Datum::Int32(e.batch.first_sequence),
                Datum::Int32(e.batch.last_sequence),
                Datum::Int64(e.batch.base_offset),
                Datum::Int64(e.batch.appended_at_ms),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]),
            Entry::Compacted(e) => Row::pack_slice(&[
                Datum::Null,
                Datum::String(&e.home),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::String(&e.record.topic),
                Datum::Bytes(&e.record.key),
                e.record.value.as_deref().map_or(Datum::Null, Datum::Bytes),
                Datum::UInt64(e.upper),
                Datum::UInt64(e.version),
                Datum::Null,
                Datum::Null,
            ]),
            Entry::Record(e) => {
                let headers = encode_headers(&e.record.headers);
                Row::pack_slice(&[
                    Datum::UInt64(e.offset),
                    Datum::String(&e.topic),
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    e.record.key.as_deref().map_or(Datum::Null, Datum::Bytes),
                    e.record.value.as_deref().map_or(Datum::Null, Datum::Bytes),
                    Datum::Null,
                    Datum::Null,
                    Datum::Bytes(&headers),
                    Datum::Int64(e.record.timestamp),
                ])
            }
        }
    }

    fn decode(row: &Row) -> anyhow::Result<Self> {
        let datums = row.unpack();
        anyhow::ensure!(datums.len() == 15, "log row has {} columns", datums.len());
        let source_topic: Arc<str> = datums[1].unwrap_str().into();
        let bytes = |d: Datum| (!d.is_null()).then(|| Bytes::copy_from_slice(d.unwrap_bytes()));
        if !datums[0].is_null() {
            Ok(Entry::Record(RecordEntry {
                topic: source_topic,
                offset: datums[0].unwrap_uint64(),
                record: ProducedRecord {
                    key: bytes(datums[9]),
                    value: bytes(datums[10]),
                    headers: decode_headers(datums[13].unwrap_bytes())?,
                    timestamp: datums[14].unwrap_int64(),
                },
            }))
        } else if datums[8].is_null() {
            Ok(Entry::Batch(BatchEntry {
                topic: source_topic,
                producer_id: datums[2].unwrap_int64(),
                epoch: datums[3].unwrap_int16(),
                batch: AppendedBatch {
                    first_sequence: datums[4].unwrap_int32(),
                    last_sequence: datums[5].unwrap_int32(),
                    base_offset: datums[6].unwrap_int64(),
                    appended_at_ms: datums[7].unwrap_int64(),
                },
            }))
        } else {
            Ok(Entry::Compacted(CompactedEntry {
                home: source_topic,
                record: CompactedRecord {
                    topic: datums[8].unwrap_str().into(),
                    key: Bytes::copy_from_slice(datums[9].unwrap_bytes()),
                    value: bytes(datums[10]),
                },
                upper: datums[11].unwrap_uint64(),
                version: datums[12].unwrap_uint64(),
            }))
        }
    }
}

/// Another writer appended to the log.
#[derive(Debug)]
struct Fenced {
    expected: u64,
    actual: Antichain<Timestamp>,
}

impl std::fmt::Display for Fenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "another replica wrote to the Kafka listener log: expected upper {}, found {:?}",
            self.expected,
            self.actual.elements()
        )
    }
}

impl std::error::Error for Fenced {}

/// What a restarting source finds in its log.
pub(super) struct Recovered {
    /// The batches of idempotent producers that the log remembers, by topic.
    pub(super) producers: BTreeMap<Arc<str>, ProducerStates>,
    /// The latest compacted record of each key.
    pub(super) compacted: Vec<CompactedEntry>,
    /// The version of the next compacted row.
    pub(super) next_version: u64,
    /// Entries that are no longer needed and should be retracted with the next append.
    pub(super) retractions: Vec<Entry>,
    /// The records that some export has yet to commit, in offset order.
    pub(super) records: Vec<RecordEntry>,
    /// The offset after the log's last record.
    pub(super) next_offset: u64,
}

type LogWriteHandle = WriteHandle<SourceData, (), Timestamp, StorageDiff>;
type LogReadHandle = ReadHandle<SourceData, (), Timestamp, StorageDiff>;

/// A Kafka listener source's log, opened for recovery.
pub(super) struct Log {
    write: LogWriteHandle,
    /// Holds back the since of the log, so that it can be read on restart. Kept at most one
    /// below the upper, which lets persist compact retracted rows away.
    read: LogReadHandle,
    upper: u64,
}

impl Log {
    pub(super) async fn open(
        persist_clients: &PersistClientCache,
        location: PersistLocation,
        shard: ShardId,
        source_id: GlobalId,
    ) -> anyhow::Result<Self> {
        let client = persist_clients
            .open(location)
            .await
            .context("opening persist client")?;
        let (mut write, read) = client
            .open(
                shard,
                Arc::new(LOG_DESC.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: format!("{source_id} kafka listener log"),
                    handle_purpose: format!("kafka listener log of {source_id}"),
                },
                false,
            )
            .await
            .context("opening log shard")?;
        let upper = upper_of(write.fetch_recent_upper().await)?;
        Ok(Log { write, read, upper })
    }

    /// Reads the log for a source whose topics' exports resume at `resume_offsets`, at the
    /// lowest offset among each topic's exports.
    ///
    /// NOTE: the records to replay are held in memory, so recovering an export created after
    /// many records, which replays its topic from the start, needs memory for all of them.
    pub(super) async fn recover(
        &mut self,
        resume_offsets: &BTreeMap<Arc<str>, u64>,
    ) -> anyhow::Result<Recovered> {
        let mut recovered = Recovered {
            producers: BTreeMap::new(),
            compacted: Vec::new(),
            next_version: 0,
            retractions: Vec::new(),
            records: Vec::new(),
            next_offset: 0,
        };
        if self.upper == 0 {
            return Ok(recovered);
        }

        let as_of = Antichain::from_elem(Timestamp::from(self.upper - 1));
        let mut cursor = self
            .read
            .snapshot_cursor(as_of, |_| true)
            .await
            .map_err(|since| anyhow::anyhow!("log since {since:?} is beyond its upper"))?;
        let mut batches = Vec::new();
        let mut compacted: BTreeMap<(String, Bytes), CompactedEntry> = BTreeMap::new();
        let mut log_records = 0u64;
        while let Some(updates) = cursor.next().await {
            for ((data, _unit), _time, diff) in updates {
                let row = data.0.map_err(|e| anyhow::anyhow!("error in log: {e:?}"))?;
                anyhow::ensure!(diff == 1, "log row with multiplicity {diff}: {row:?}");
                match Entry::decode(&row)? {
                    Entry::Record(entry) => {
                        log_records += 1;
                        recovered.next_offset = recovered.next_offset.max(entry.offset + 1);
                        let needed = resume_offsets
                            .get(&entry.topic)
                            .is_some_and(|resume| entry.offset >= *resume);
                        if needed {
                            recovered.records.push(entry);
                        }
                    }
                    Entry::Batch(entry) => batches.push(entry),
                    Entry::Compacted(entry) => {
                        recovered.next_version = recovered.next_version.max(entry.version + 1);
                        let key = (entry.record.topic.clone(), entry.record.key.clone());
                        let superseded = match compacted.remove(&key) {
                            Some(other) if other.version > entry.version => {
                                compacted.insert(key, other);
                                entry
                            }
                            Some(other) => {
                                compacted.insert(key, entry);
                                other
                            }
                            None => {
                                compacted.insert(key, entry);
                                continue;
                            }
                        };
                        recovered.retractions.push(Entry::Compacted(superseded));
                    }
                }
            }
        }
        recovered.compacted = compacted.into_values().collect();
        recovered.records.sort_by_key(|entry| entry.offset);

        batches.sort_by_key(|entry| entry.batch.base_offset);
        for entry in batches {
            let BatchEntry {
                topic,
                producer_id,
                epoch,
                batch,
            } = entry;
            let producers = recovered.producers.entry(Arc::clone(&topic)).or_default();
            for (epoch, batch) in producers.restore(producer_id, epoch, batch) {
                recovered.retractions.push(Entry::Batch(BatchEntry {
                    topic: Arc::clone(&topic),
                    producer_id,
                    epoch,
                    batch,
                }));
            }
        }
        tracing::info!(
            upper = self.upper,
            ?resume_offsets,
            log_records,
            replay = recovered.records.len(),
            next_offset = recovered.next_offset,
            producers = recovered
                .producers
                .values()
                .map(ProducerStates::len)
                .sum::<usize>(),
            compacted = recovered.compacted.len(),
            "read Kafka listener log",
        );
        Ok(recovered)
    }

    /// Starts appending to the log.
    ///
    /// The returned receiver yields the token of each append once it commits, in order, or the
    /// error that stopped the writer. Nothing is appended after an error.
    pub(super) fn into_writer(self) -> (LogWriter, mpsc::UnboundedReceiver<anyhow::Result<u64>>) {
        let (appends_tx, appends_rx) = mpsc::unbounded_channel();
        let (commits_tx, commits_rx) = mpsc::unbounded_channel();
        let appender = mz_ore::task::spawn(
            || "kafka_listener_log_appender",
            append_in_order(self.write, self.read, self.upper, appends_rx, commits_tx),
        )
        .abort_on_drop();
        let writer = LogWriter {
            next_token: 0,
            appends: appends_tx,
            _appender: appender,
        };
        (writer, commits_rx)
    }
}

/// Appends to a Kafka listener source's log in the background.
pub(super) struct LogWriter {
    next_token: u64,
    appends: mpsc::UnboundedSender<Vec<(Entry, StorageDiff)>>,
    _appender: AbortOnDropHandle<()>,
}

impl LogWriter {
    /// Starts appending `updates`, and returns a token that the commits receiver yields once
    /// they commit. Tokens increase with every append.
    pub(super) fn append(&mut self, updates: Vec<(Entry, StorageDiff)>) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        // A send error means the appender stopped, which it reports on the commits channel.
        let _ = self.appends.send(updates);
        token
    }
}

/// How many log appends may be in flight at once.
const MAX_APPENDS_IN_FLIGHT: usize = 2;

/// The least time between the starts of two log appends, which makes the log commit in groups.
/// An append costs a blob write, a consensus write and a batch for compaction to merge, whatever
/// its size. At 400k records/s on 8 workers, this halved the appends and cut the replica's CPU
/// by 15 percent, and ack latency did not change: an append takes longer than this to commit.
const MIN_APPEND_INTERVAL: Duration = Duration::from_millis(50);

/// The most bytes of entries in one batch of the log. A larger append is split into batches of
/// this size that build on their own tasks and commit together, so the work of building an
/// append spreads over the replica's cores.
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;

/// The most bytes of entries that may be building at once, which bounds the appender's memory.
/// Beyond it, appends queue until an append in flight commits.
const MAX_BUILDING_BYTES: usize = 256 * 1024 * 1024;

type LogBatch = Batch<SourceData, (), Timestamp, StorageDiff>;

/// Appends the updates of `appends` to the log in order, and reports each one's token on
/// `commits`. Appends that queue while others are in flight are combined into one.
///
/// An append's timestamp is fixed when it starts, so appends commit in the order they arrived
/// even though they build concurrently.
async fn append_in_order(
    mut write: LogWriteHandle,
    mut read: LogReadHandle,
    mut upper: u64,
    mut appends: mpsc::UnboundedReceiver<Vec<(Entry, StorageDiff)>>,
    commits: mpsc::UnboundedSender<anyhow::Result<u64>>,
) {
    let mut next_token = 0;
    let mut next_time = upper;
    let mut building = FuturesOrdered::new();
    let mut building_bytes = 0;
    let mut appends_open = true;
    let mut last_compaction = Instant::now();
    let mut next_start = tokio::time::Instant::now();
    loop {
        let can_start = appends_open
            && building.len() < MAX_APPENDS_IN_FLIGHT
            && building_bytes < MAX_BUILDING_BYTES;
        tokio::select! {
            first = async {
                tokio::time::sleep_until(next_start).await;
                appends.recv().await
            }, if can_start => {
                next_start = tokio::time::Instant::now() + MIN_APPEND_INTERVAL;
                let Some(mut updates) = first else {
                    appends_open = false;
                    continue;
                };
                let first_token = next_token;
                let mut bytes: usize = updates.iter().map(|(e, _)| e.size()).sum();
                while building_bytes + bytes < MAX_BUILDING_BYTES {
                    let Ok(more) = appends.try_recv() else {
                        break;
                    };
                    bytes += more.iter().map(|(e, _)| e.size()).sum::<usize>();
                    updates.extend(more);
                    next_token += 1;
                }
                let tokens = first_token..=next_token;
                next_token += 1;
                building_bytes += bytes;
                let time = Timestamp::from(next_time);
                next_time += 1;
                let builds: Vec<_> = chunks(updates)
                    .map(|chunk| {
                        let mut builder = write.builder(Antichain::from_elem(time));
                        mz_ore::task::spawn(|| "kafka_listener_log_batch", async move {
                            for (entry, diff) in chunk {
                                builder
                                    .add(&SourceData(Ok(entry.encode())), &(), &time, &diff)
                                    .await?;
                            }
                            builder.finish(Antichain::from_elem(time.step_forward())).await
                        })
                    })
                    .collect();
                building.push_back(async move {
                    let mut batches = Vec::with_capacity(builds.len());
                    for build in builds {
                        batches.push(build.await.context("invalid log batch"));
                    }
                    (batches, time, tokens, bytes)
                });
            }
            Some((batches, time, tokens, bytes)) = building.next() => {
                building_bytes -= bytes;
                let result = append_batches(&mut write, batches, time).await;
                if let Err(e) = result {
                    let _ = commits.send(Err(e));
                    // The appends still building are never appended.
                    while let Some((batches, ..)) = building.next().await {
                        delete_batches(batches).await;
                    }
                    return;
                }
                upper += 1;
                for token in tokens {
                    if commits.send(Ok(token)).is_err() {
                        return;
                    }
                }
                if last_compaction.elapsed() >= COMPACTION_INTERVAL {
                    read.downgrade_since(&Antichain::from_elem(Timestamp::from(upper - 1)))
                        .await;
                    last_compaction = Instant::now();
                }
            }
            else => return,
        }
    }
}

/// Splits `updates` into runs of at most [`MAX_BATCH_BYTES`], each with at least one update.
fn chunks(updates: Vec<(Entry, StorageDiff)>) -> impl Iterator<Item = Vec<(Entry, StorageDiff)>> {
    let mut updates = updates.into_iter().peekable();
    std::iter::from_fn(move || {
        let mut chunk = vec![updates.next()?];
        let mut bytes = chunk[0].0.size();
        while let Some((entry, _)) = updates.peek() {
            bytes += entry.size();
            if bytes > MAX_BATCH_BYTES {
                break;
            }
            chunk.push(updates.next().expect("peeked"));
        }
        Some(chunk)
    })
}

/// Appends the batches of one append at `time`, or deletes them if any failed to build or the
/// append fails.
async fn append_batches(
    write: &mut LogWriteHandle,
    batches: Vec<anyhow::Result<LogBatch>>,
    time: Timestamp,
) -> anyhow::Result<()> {
    let mut built = Vec::with_capacity(batches.len());
    let mut error = None;
    for batch in batches {
        match batch {
            Ok(batch) => built.push(batch),
            Err(e) => error = Some(e),
        }
    }
    if let Some(e) = error {
        delete_batches(built.into_iter().map(Ok).collect()).await;
        return Err(e);
    }
    let result = write
        .compare_and_append_batch(
            &mut built.iter_mut().collect::<Vec<_>>(),
            Antichain::from_elem(time),
            Antichain::from_elem(time.step_forward()),
            true,
        )
        .await
        .context("invalid log append");
    let result = match result {
        Ok(Ok(())) => return Ok(()),
        Ok(Err(mismatch)) => Err(anyhow::Error::new(Fenced {
            expected: u64::from(time),
            actual: mismatch.current,
        })),
        Err(e) => Err(e),
    };
    delete_batches(built.into_iter().map(Ok).collect()).await;
    result
}

async fn delete_batches(batches: Vec<anyhow::Result<LogBatch>>) {
    for batch in batches.into_iter().flatten() {
        batch.delete().await;
    }
}

fn upper_of(upper: &Antichain<Timestamp>) -> anyhow::Result<u64> {
    upper
        .as_option()
        .map(|t| u64::from(*t))
        .context("log shard is closed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn entries_round_trip() {
        let batch = Entry::Batch(BatchEntry {
            topic: "events".into(),
            producer_id: 9,
            epoch: 2,
            batch: AppendedBatch {
                first_sequence: 5,
                last_sequence: 9,
                base_offset: 100,
                appended_at_ms: 1234,
            },
        });
        assert_eq!(Entry::decode(&batch.encode()).unwrap(), batch);
        for value in [Some(Bytes::from("v")), None] {
            let compacted = Entry::Compacted(CompactedEntry {
                home: "events".into(),
                record: CompactedRecord {
                    topic: "progress".into(),
                    key: Bytes::from("k"),
                    value,
                },
                upper: 7,
                version: 3,
            });
            assert_eq!(Entry::decode(&compacted.encode()).unwrap(), compacted);
        }
        for (key, value, headers) in [
            (
                Some(Bytes::from("k")),
                Some(Bytes::from("v")),
                vec![
                    ("h".to_string(), Some(Bytes::from("x"))),
                    ("e".into(), None),
                ],
            ),
            (None, None, vec![]),
        ] {
            let record = Entry::Record(RecordEntry {
                topic: "events".into(),
                offset: 42,
                record: ProducedRecord {
                    key,
                    value,
                    headers,
                    timestamp: 1234,
                },
            });
            assert_eq!(Entry::decode(&record.encode()).unwrap(), record);
        }
    }
}
