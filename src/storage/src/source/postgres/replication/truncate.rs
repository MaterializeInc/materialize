// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, VecDeque};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream as AsyncStream, StreamExt, TryStreamExt};
use mz_persist_client::Diagnostics;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::codec_impls::UnitSchema;
use mz_postgres_util::Client;
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::MzOffset;
use mz_storage_types::sources::SourceData;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::progress::Antichain;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::metrics::source::postgres::PgSourceMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::postgres::{SourceOutputInfo, TransientError};
use crate::source::types::{FuelSize, ResumeUppers};

use super::decode::{ExtractMode, TxEvent, extract_transaction};
use super::{DataOutputs, LogicalReplMsg, ReplData};

/// The updates of one export that its shard may not reflect yet.
///
/// With `base` at `None`, the export's contents at the replication stream position are the
/// contents of its shard at `into - 1` plus every update at or beyond `upper`, for any
/// `(into, upper)` pair in [`ResumeUppers::exports_into`] and [`ResumeUppers::exports`]. With `base`
/// at `Some(lsn)`, a TRUNCATE at `lsn` emptied the export, and while `upper <= lsn` the contents
/// are exactly the updates after that TRUNCATE.
///
/// `updates` holds those updates unless some were dropped to bound memory, which `lost_upto`
/// records. Dropped updates are recovered by replaying the replication slot.
#[derive(Default)]
struct Tail {
    updates: VecDeque<(Result<Row, DataflowError>, MzOffset, Diff)>,
    bytes: usize,
    base: Option<MzOffset>,
    /// Updates below this LSN were dropped to bound memory.
    lost_upto: Option<MzOffset>,
}

impl Tail {
    /// Records an update the operator gave at `lsn`.
    ///
    /// Callers push in non-decreasing LSN order. Once the recorded updates exceed `max_bytes`,
    /// the tail drops all of them and sets `lost_upto` to `lsn + 1`. It then ignores updates
    /// below `lost_upto`, and a retraction that needs them replays the replication stream
    /// instead. A `max_bytes` of zero records nothing.
    fn push(
        &mut self,
        event: &Result<Row, DataflowError>,
        lsn: MzOffset,
        diff: Diff,
        max_bytes: usize,
    ) {
        if self.lost_upto.is_some_and(|lost| lost > lsn) {
            return;
        }
        if max_bytes == 0 {
            self.lost_upto = Some(lsn + 1);
            return;
        }
        self.bytes += event.fuel_size();
        self.updates.push_back((event.clone(), lsn, diff));
        if self.bytes > max_bytes {
            self.updates.clear();
            self.bytes = 0;
            self.lost_upto = Some(lsn + 1);
        }
    }

    /// Drops the updates the shard reflects once it is durable through `upper`.
    fn trim(&mut self, upper: MzOffset) {
        if self.base.is_some_and(|base| upper <= base) {
            return;
        }
        self.base = None;
        while let Some((event, lsn, _)) = self.updates.front() {
            if *lsn >= upper {
                break;
            }
            self.bytes -= event.fuel_size();
            self.updates.pop_front();
        }
        if self.lost_upto.is_some_and(|lost| lost <= upper) {
            self.lost_upto = None;
        }
    }

    /// Whether `updates` holds every update at or beyond `start`.
    fn complete_from(&self, start: MzOffset) -> bool {
        self.lost_upto.is_none_or(|lost| lost <= start)
    }

    /// Restarts the tail at a TRUNCATE at `lsn`.
    fn reset(&mut self, lsn: MzOffset) {
        self.updates.clear();
        self.bytes = 0;
        self.base = Some(lsn);
        self.lost_upto = None;
    }
}

/// The retractions of the exports that TRUNCATEs empty, for one replication operator.
///
/// Each retraction runs as a job that reads the export's shard back concurrently with the
/// replication stream. The operator gives the job's updates at the TRUNCATE's LSN, with a
/// capability the job retains until it completes.
pub(super) struct Retractions {
    id: GlobalId,
    worker_id: usize,
    /// Whether a TRUNCATE is retracted rather than errored.
    enabled: bool,
    buffer_max_bytes: usize,
    persist_clients: Arc<PersistClientCache>,
    exports: BTreeMap<usize, (GlobalId, CollectionMetadata)>,
    tails: BTreeMap<usize, Tail>,
    latest_uppers: watch::Receiver<Option<ResumeUppers<MzOffset>>>,
    jobs: BTreeMap<u64, RetractionJob>,
    next_job_id: u64,
    job_tx: mpsc::Sender<(u64, JobMsg)>,
    job_rx: mpsc::Receiver<(u64, JobMsg)>,
}

/// A message from a retraction job, for [`Retractions::on_job_msg`].
pub(super) struct JobEvent {
    job_id: u64,
    msg: JobMsg,
}

/// A retraction that needs its export's updates from `from` up to the TRUNCATE, which the
/// operator recovers by replaying the replication stream.
pub(super) struct Replay {
    job: RetractionJob,
    from: MzOffset,
    skip_tx: Option<MzOffset>,
}

impl Replay {
    /// The LSN the replication stream must restart at.
    pub(super) fn from(&self) -> MzOffset {
        self.from
    }
}

impl Retractions {
    /// Creates the retractions of the outputs in `table_info`. `latest_uppers` receives the
    /// operator's latest resume uppers.
    pub(super) fn new(
        config: &RawSourceCreationConfig,
        table_info: &BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
        enabled: bool,
        buffer_max_bytes: usize,
        latest_uppers: watch::Receiver<Option<ResumeUppers<MzOffset>>>,
    ) -> Self {
        let exports: BTreeMap<_, _> = table_info
            .values()
            .flat_map(|outputs| outputs.iter())
            .map(|(output_index, info)| {
                let metadata = config.source_exports[&info.export_id]
                    .storage_metadata
                    .clone();
                (*output_index, (info.export_id, metadata))
            })
            .collect();
        let tails = exports.keys().map(|idx| (*idx, Tail::default())).collect();
        let (job_tx, job_rx) = mpsc::channel(16);
        Self {
            id: config.id,
            worker_id: config.worker_id,
            enabled,
            buffer_max_bytes,
            persist_clients: Arc::clone(&config.persist_clients),
            exports,
            tails,
            latest_uppers,
            jobs: BTreeMap::new(),
            next_job_id: 0,
            job_tx,
            job_rx,
        }
    }

    /// Records an update the operator gave for `output_index` at `lsn`.
    pub(super) fn on_emit(
        &mut self,
        output_index: usize,
        event: &Result<Row, DataflowError>,
        lsn: MzOffset,
        diff: Diff,
    ) {
        if !self.enabled {
            return;
        }
        if let Some(tail) = self.tails.get_mut(&output_index) {
            tail.push(event, lsn, diff, self.buffer_max_bytes);
        }
    }

    /// Drops the recorded updates that the latest resume uppers show durable.
    pub(super) fn on_resume_uppers(&mut self) {
        if !self.latest_uppers.has_changed().unwrap_or(false) {
            return;
        }
        let Some(uppers) = &*self.latest_uppers.borrow_and_update() else {
            return;
        };
        for (output_index, tail) in self.tails.iter_mut() {
            let (export_id, _) = &self.exports[output_index];
            if let Some(upper) = uppers.exports.get(export_id).and_then(|u| u.as_option()) {
                tail.trim(*upper);
            }
        }
    }

    /// Retracts the contents of `output_index` before a TRUNCATE at `lsn`, either from its
    /// recorded updates alone or by starting a job that retains a capability of `cap_set` at
    /// `lsn`.
    pub(super) async fn on_truncate(
        &mut self,
        output_index: usize,
        info: SourceOutputInfo,
        lsn: MzOffset,
        cap_set: &CapabilitySet<MzOffset>,
        data_outputs: &mut DataOutputs,
    ) {
        let tail = self.tails.get_mut(&output_index).expect("tail per output");
        if tail.base.is_some_and(|base| tail.complete_from(base)) {
            // An earlier TRUNCATE emptied the export, so the tail alone describes its contents.
            for (event, _, diff) in tail.updates.iter() {
                let update = (ReplData::Raw(event.clone()), lsn, -*diff);
                data_outputs.give(output_index, &cap_set[0], update).await;
            }
            tail.reset(lsn);
            return;
        }
        let frozen = std::mem::take(tail);
        tail.reset(lsn);
        let (export_id, metadata) = self.exports[&output_index].clone();
        let job_id = self.next_job_id;
        self.next_job_id += 1;
        let handle = mz_ore::task::spawn(
            || format!("pg_truncate_retraction:{export_id}"),
            retraction_job(
                Arc::clone(&self.persist_clients),
                metadata,
                export_id,
                lsn,
                frozen,
                self.latest_uppers.clone(),
                job_id,
                self.job_tx.clone(),
            ),
        )
        .abort_on_drop();
        let job = RetractionJob {
            output_index,
            lsn,
            cap: cap_set.delayed(&lsn),
            info,
            started: Instant::now(),
            _handle: handle,
        };
        self.jobs.insert(job_id, job);
    }

    /// The next message from a retraction job.
    pub(super) async fn next_job_msg(&mut self) -> JobEvent {
        let (job_id, msg) = self
            .job_rx
            .recv()
            .await
            .expect("the retractions hold a sender");
        JobEvent { job_id, msg }
    }

    /// Handles a message from a retraction job, returning the replay the job needs, if any.
    pub(super) async fn on_job_msg(
        &mut self,
        JobEvent { job_id, msg }: JobEvent,
        data_outputs: &mut DataOutputs,
    ) -> Result<Option<Replay>, TransientError> {
        let (id, worker_id) = (self.id, self.worker_id);
        let job = self
            .jobs
            .get(&job_id)
            .expect("job messages only for live jobs");
        match msg {
            JobMsg::Updates(updates) => {
                for (data, diff) in updates {
                    let update = (data, job.lsn, diff);
                    data_outputs.give(job.output_index, &job.cap, update).await;
                }
                Ok(None)
            }
            JobMsg::Done { snapshot, tail } => {
                let job = self.jobs.remove(&job_id).expect("known job");
                tracing::info!(
                    %id,
                    "timely-{worker_id} retracted truncate of output {} at {}: \
                        {snapshot} snapshot updates, {tail} tail updates, {:?}",
                    job.output_index,
                    job.lsn,
                    job.started.elapsed(),
                );
                Ok(None)
            }
            JobMsg::NeedsReplay {
                snapshot,
                from,
                skip_tx,
            } => {
                let job = self.jobs.remove(&job_id).expect("known job");
                tracing::info!(
                    %id,
                    "timely-{worker_id} truncate of output {} at {}: \
                        {snapshot} snapshot updates, replaying from {from}",
                    job.output_index,
                    job.lsn,
                );
                Ok(Some(Replay { job, from, skip_tx }))
            }
            JobMsg::Failed(err) => Err(TransientError::from(anyhow::anyhow!(
                "truncate retraction failed: {err}"
            ))),
        }
    }

    /// Completes the retraction of `replay` from `stream`, which restarts at `replay.from()`.
    /// See [`replay_retraction`].
    pub(super) async fn replay<S>(
        &self,
        replay: Replay,
        stream: std::pin::Pin<&mut futures::stream::Peekable<S>>,
        data_upper: MzOffset,
        metadata_client: &Client,
        metrics: &PgSourceMetrics,
        publication: &str,
        data_outputs: &mut DataOutputs,
    ) -> Result<(), TransientError>
    where
        S: AsyncStream<Item = Result<LogicalReplMsg, TransientError>>,
    {
        let (id, worker_id) = (self.id, self.worker_id);
        let Replay { job, from, skip_tx } = replay;
        let started = Instant::now();
        let (transactions, replayed) = replay_retraction(
            stream,
            &job,
            from,
            skip_tx,
            data_upper,
            metadata_client,
            metrics,
            publication,
            data_outputs,
        )
        .await?;
        tracing::info!(
            %id,
            "timely-{worker_id} retracted truncate of output {} at {}: replayed \
                {transactions} transactions from {from} to {data_upper}, {replayed} \
                updates, in {:?}, {:?} since truncate",
            job.output_index,
            job.lsn,
            started.elapsed(),
            job.started.elapsed(),
        );
        Ok(())
    }
}

/// A retraction of the contents of an export at a TRUNCATE, read back concurrently.
struct RetractionJob {
    output_index: usize,
    lsn: MzOffset,
    /// The output info at the TRUNCATE, which a replay of updates before it decodes with.
    info: SourceOutputInfo,
    /// Holds the export's frontier at `lsn` until the retraction is complete.
    cap: Capability<MzOffset>,
    started: Instant,
    _handle: mz_ore::task::AbortOnDropHandle<()>,
}

/// A message from a [`retraction_job`] to the replication operator.
enum JobMsg {
    /// Updates to give at the job's LSN.
    Updates(Vec<(ReplData, Diff)>),
    /// All updates were sent.
    Done { snapshot: u64, tail: usize },
    /// The snapshot was sent, and the buffered updates are incomplete. The export's updates from
    /// `from` up to the job's LSN must be negated by replaying the replication stream, skipping
    /// the updates before the TRUNCATE in the transaction at `skip_tx`.
    NeedsReplay {
        snapshot: u64,
        from: MzOffset,
        skip_tx: Option<MzOffset>,
    },
    /// A transient failure. Retrying from the resume upper redoes the retraction.
    Failed(String),
}

/// Sends the retraction of the contents of `export_id` just before `lsn`.
///
/// For a consistent `(into, upper)` pair from `latest_uppers`, the contents are either the
/// export's persist shard at `into - 1` plus its updates at or beyond `upper`, or, if an earlier
/// TRUNCATE in `tail` is not yet durable, only the updates after that TRUNCATE.
async fn retraction_job(
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    export_id: GlobalId,
    lsn: MzOffset,
    mut tail: Tail,
    mut latest_uppers: watch::Receiver<Option<ResumeUppers<MzOffset>>>,
    job_id: u64,
    tx: mpsc::Sender<(u64, JobMsg)>,
) {
    const BATCH: usize = 1024;
    let send = |msg| tx.send((job_id, msg));
    if let Some(delay) = retraction_delay() {
        tokio::time::sleep(delay).await;
    }
    let mut read_handle = None;
    loop {
        let pair = latest_uppers
            .borrow_and_update()
            .as_ref()
            .and_then(|uppers| {
                let into = uppers.exports_into.get(&export_id)?.as_option()?;
                let upper = uppers.exports.get(&export_id)?.as_option()?;
                Some((*into, *upper))
            });
        if let Some((into, upper)) = pair {
            assert!(
                upper <= lsn,
                "export {export_id} durable at {upper} beyond {lsn}"
            );
            tail.trim(upper);
            if let Some(base) = tail.base {
                let msg = if tail.complete_from(base) {
                    let tail_len = tail.updates.len();
                    let batch: Vec<_> = tail
                        .updates
                        .drain(..)
                        .map(|(event, _, diff)| (ReplData::Raw(event), -diff))
                        .collect();
                    if send(JobMsg::Updates(batch)).await.is_err() {
                        return;
                    }
                    JobMsg::Done {
                        snapshot: 0,
                        tail: tail_len,
                    }
                } else {
                    JobMsg::NeedsReplay {
                        snapshot: 0,
                        from: base,
                        skip_tx: Some(base),
                    }
                };
                let _ = send(msg).await;
                return;
            }
            // Snapshot updates sit at LSN 0, so the shard reflects all of them once `upper` is
            // beyond it.
            if let (true, Some(as_of)) = (upper > MzOffset::from(0), into.step_back()) {
                let read = match &mut read_handle {
                    Some(read) => read,
                    None => {
                        let client = match persist_clients
                            .open(metadata.persist_location.clone())
                            .await
                        {
                            Ok(client) => client,
                            Err(err) => {
                                let _ = send(JobMsg::Failed(err.to_string())).await;
                                return;
                            }
                        };
                        let read = client
                            .open_leased_reader::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                                metadata.data_shard,
                                Arc::new(metadata.relation_desc.clone()),
                                Arc::new(UnitSchema),
                                Diagnostics {
                                    shard_name: export_id.to_string(),
                                    handle_purpose: format!("pg truncate retraction {export_id}"),
                                },
                                false,
                            )
                            .await
                            .expect("schema matches the shard");
                        read_handle.insert(read)
                    }
                };
                match read
                    .snapshot_cursor(Antichain::from_elem(as_of), |_| true)
                    .await
                {
                    Ok(mut cursor) => {
                        let mut rows = 0_u64;
                        let mut batch = Vec::with_capacity(BATCH);
                        while let Some(chunk) = cursor.next().await {
                            for ((SourceData(data), ()), _, diff) in chunk {
                                batch.push((ReplData::Cast(data), -Diff::from(diff)));
                                rows += 1;
                                if batch.len() == BATCH {
                                    let full =
                                        std::mem::replace(&mut batch, Vec::with_capacity(BATCH));
                                    if send(JobMsg::Updates(full)).await.is_err() {
                                        return;
                                    }
                                    // The cursor can stay ready across many batches, and the
                                    // bounded channel only waits once it is full.
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                        drop(cursor);
                        let complete = tail.complete_from(upper);
                        let tail_len = tail.updates.len();
                        if complete {
                            batch.extend(
                                tail.updates
                                    .drain(..)
                                    .map(|(event, _, diff)| (ReplData::Raw(event), -diff)),
                            );
                        }
                        if !batch.is_empty() && send(JobMsg::Updates(batch)).await.is_err() {
                            return;
                        }
                        tracing::debug!(
                            %export_id,
                            "truncate at {lsn}: snapshot at {as_of}, tail from {upper}, \
                                complete {complete}"
                        );
                        if let Some(read) = read_handle.take() {
                            read.expire().await;
                        }
                        let msg = if complete {
                            JobMsg::Done {
                                snapshot: rows,
                                tail: tail_len,
                            }
                        } else {
                            JobMsg::NeedsReplay {
                                snapshot: rows,
                                from: upper,
                                skip_tx: None,
                            }
                        };
                        let _ = send(msg).await;
                        return;
                    }
                    Err(since) => {
                        tracing::debug!(
                            %export_id,
                            "since {:?} beyond {as_of}, waiting for a newer upper",
                            since.0
                        );
                    }
                }
            }
        }
        if latest_uppers.changed().await.is_err() {
            return;
        }
    }
}

/// Negates at the job's LSN the updates of the job's export that `stream`, replaying from `from`,
/// decodes below that LSN, and returns the number of transactions and of negated updates.
///
/// The replay consumes the transactions below `data_upper`, which the main loop already processed,
/// and stops before the first one at or beyond it. Within the transaction at `skip_tx` only the
/// updates after its TRUNCATE of the export count.
///
/// The failpoints `pg_truncate_replay_start`, `pg_truncate_replay_mid` (after 1000 negated
/// updates), and `pg_truncate_replay_end` fail the replay with a transient error.
async fn replay_retraction<S>(
    mut stream: std::pin::Pin<&mut futures::stream::Peekable<S>>,
    job: &RetractionJob,
    from: MzOffset,
    skip_tx: Option<MzOffset>,
    data_upper: MzOffset,
    metadata_client: &Client,
    metrics: &PgSourceMetrics,
    publication: &str,
    data_outputs: &mut DataOutputs,
) -> Result<(u64, u64), TransientError>
where
    S: AsyncStream<Item = Result<LogicalReplMsg, TransientError>>,
{
    use LogicalReplicationMessage::*;
    use ReplicationMessage::*;
    // Only the export's own output, as of its TRUNCATE. Later schema changes, and the outputs
    // that they errored, do not affect the replay.
    let mut replay_info = BTreeMap::from([(
        job.info.desc.oid,
        BTreeMap::from([(job.output_index, job.info.clone())]),
    )]);
    let mut replayed = 0_u64;
    let mut transactions = 0_u64;
    fail::fail_point!("pg_truncate_replay_start", |_| replay_failpoint("start"));
    loop {
        let commit_lsn = match stream.as_mut().peek().await {
            None => return Err(TransientError::ReplicationEOF),
            Some(Ok(XLogData(data))) => match data.data() {
                Begin(begin) => MzOffset::from(begin.final_lsn()),
                _ => return Err(TransientError::BareTransactionEvent),
            },
            Some(Ok(PrimaryKeepAlive(keepalive))) => {
                if MzOffset::from(keepalive.wal_end()) >= data_upper {
                    break;
                }
                let _ = stream.next().await;
                continue;
            }
            Some(_) => {
                return match stream.next().await {
                    Some(Err(err)) => Err(err),
                    _ => Err(TransientError::UnknownReplicationMessage),
                };
            }
        };
        if commit_lsn >= data_upper {
            break;
        }
        let _ = stream.next().await;
        transactions += 1;
        let mut empty = BTreeMap::new();
        let info = if commit_lsn < job.lsn {
            &mut replay_info
        } else {
            &mut empty
        };
        let mut armed = skip_tx != Some(commit_lsn);
        // The first pass verified these transactions against the upstream schema when it decoded
        // them. Verifying again against the current schema could disagree with it.
        let mut tx = pin!(extract_transaction(
            stream.as_mut(),
            metadata_client,
            commit_lsn,
            info,
            metrics,
            publication,
            ExtractMode::Replay,
        ));
        while let Some(tx_event) = tx.try_next().await? {
            match tx_event {
                TxEvent::Row((_oid, output_index, event, diff)) => {
                    if armed && output_index == job.output_index {
                        let update = (ReplData::Raw(event.map_err(Into::into)), job.lsn, -diff);
                        data_outputs.give(job.output_index, &job.cap, update).await;
                        replayed += 1;
                        if replayed == 1000 {
                            fail::fail_point!("pg_truncate_replay_mid", |_| {
                                replay_failpoint("mid")
                            });
                        }
                    }
                }
                TxEvent::Truncate(targets) => {
                    // A later TRUNCATE of the export would have become the replay's start.
                    if targets.iter().any(|(o, _)| *o == job.output_index) {
                        assert_eq!(
                            skip_tx,
                            Some(commit_lsn),
                            "replay of {} from {from} met a truncate at {commit_lsn}",
                            job.output_index,
                        );
                        armed = true;
                    }
                }
            }
        }
    }
    fail::fail_point!("pg_truncate_replay_end", |_| replay_failpoint("end"));
    Ok((transactions, replayed))
}

/// The delay the failpoint `pg_truncate_retraction_delay` configures with `return(<millis>)`
/// before a retraction job starts.
fn retraction_delay() -> Option<Duration> {
    fail::fail_point!("pg_truncate_retraction_delay", |millis| {
        millis
            .and_then(|millis| millis.parse().ok())
            .map(Duration::from_millis)
    });
    None
}

/// The error a replay failpoint returns.
fn replay_failpoint<T>(point: &str) -> Result<T, TransientError> {
    tracing::warn!("failpoint pg_truncate_replay_{point} fired");
    Err(TransientError::SyntheticError)
}
