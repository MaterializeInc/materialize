// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Durable subscriptions: a stream written to a sink under a checkpoint, so
//! that a restarted worker resumes where the last one committed, and a newer
//! worker fences an older one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use materialize_sdk_core::catalog::{self, ObjectName};
use materialize_sdk_core::{
    Change, Checkpoint, CheckpointError, CheckpointWrite, Classification, Envelope, ErrorKind,
    Event, Fingerprint, Frontier, IdentityChange, PayloadColumns, Phase, Settlement, SinkType,
    Start, Subscribe, Update, UsageError, WriteError, classify, compare, resolve_index_check,
};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config, Socket};

use crate::error::Error;
use crate::live::{
    AbortOnDrop, LiveOptions, LiveSubscription, query, server_error_with, with_network_defaults,
};
use crate::sink::{Rejected, Sink, SinkBatch, SinkError, WriteRequest, WriteResult};
use crate::store::CheckpointStore;

/// How a durable subscription fetches, commits, and retries.
#[derive(Clone, Debug)]
pub struct DurableOptions {
    live: LiveOptions,
    commit_interval: Duration,
    write_timeout: Duration,
    store_timeout: Duration,
    max_attempts: u32,
}

impl Default for DurableOptions {
    fn default() -> DurableOptions {
        DurableOptions {
            live: LiveOptions::default().buffer(1),
            commit_interval: Duration::from_secs(1),
            write_timeout: Duration::from_secs(60),
            store_timeout: Duration::from_secs(30),
            max_attempts: 10,
        }
    }
}

impl DurableOptions {
    /// How the stream is fetched and buffered. By default every batch that
    /// arrives while a write runs is appended to the one waiting, a
    /// [`LiveOptions::buffer`] of 1, so the next write takes them all and a
    /// sink whose writes are slower than the stream's progress keeps up.
    pub fn live(mut self, live: LiveOptions) -> DurableOptions {
        self.live = live;
        self
    }

    /// The least time between two commits that carry no changes. A batch with
    /// changes always commits. One second by default.
    pub fn commit_interval(mut self, interval: Duration) -> DurableOptions {
        self.commit_interval = interval;
        self
    }

    /// How long a [`Sink::write`] with a batch may run before its result
    /// counts as unknown, and a [`Sink::sweep_generations`] before it counts as
    /// failed. One minute by default.
    pub fn write_timeout(mut self, timeout: Duration) -> DurableOptions {
        self.write_timeout = timeout;
        self
    }

    /// How long a checkpoint read, or a checkpoint write without a batch, may
    /// run, in the store or in the sink. A write that runs longer counts as
    /// unknown, and a read as failed. 30 seconds by default.
    pub fn store_timeout(mut self, timeout: Duration) -> DurableOptions {
        self.store_timeout = timeout;
        self
    }

    /// How many times a failed call is sent before the subscription stops,
    /// with backoff from one second to one minute. Ten by default.
    pub fn max_attempts(mut self, attempts: u32) -> DurableOptions {
        self.max_attempts = attempts.max(1);
        self
    }
}

/// The checkpoint store of a sink that keeps its own checkpoint.
#[derive(Debug, Default)]
pub struct NoStore;

impl CheckpointStore for NoStore {
    async fn load(&mut self, _: &str) -> Result<Option<String>, SinkError> {
        Err(SinkError::NotSupported)
    }

    async fn write(&mut self, _: &str, _: &CheckpointWrite) -> WriteResult {
        WriteResult::Failed(SinkError::NotSupported)
    }
}

/// A sink, and the store of its checkpoint, run as one durable subscription.
///
/// Each start takes the checkpoint over, and the worker it took over from
/// stops at its next checkpoint write. Until then an older worker of a
/// non-transactional sink keeps writing, during a snapshot until the write
/// that completes it.
#[derive(Debug)]
pub struct DurableSink<S, C = NoStore> {
    sink_id: String,
    sink: S,
    store: C,
    options: DurableOptions,
    /// Whether the checkpoint lives in `store`, decided by the first load.
    in_store: Option<bool>,
    /// When the last attempt opened its stream.
    opened_at: Option<Instant>,
}

impl<S: Sink> DurableSink<S> {
    /// A durable subscription writing to `sink` under `sink_id`, the stable
    /// identity of its checkpoint across restarts.
    pub fn new(sink_id: impl Into<String>, sink: S, options: DurableOptions) -> DurableSink<S> {
        DurableSink {
            sink_id: sink_id.into(),
            sink,
            store: NoStore,
            options,
            in_store: None,
            opened_at: None,
        }
    }
}

impl<S: Sink, C: CheckpointStore> DurableSink<S, C> {
    /// Keeps the checkpoint in `store`, for a sink whose [`Sink::load`] returns
    /// [`SinkError::NotSupported`].
    pub fn with_store<D: CheckpointStore>(self, store: D) -> DurableSink<S, D> {
        DurableSink {
            sink_id: self.sink_id,
            sink: self.sink,
            store,
            options: self.options,
            in_store: None,
            opened_at: None,
        }
    }

    /// The sink.
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Creates the first checkpoint, for a subscription to `object` in
    /// `envelope`, unless one exists. A setup step runs this once, before any
    /// worker starts. Returns whether it created the checkpoint.
    pub async fn setup<T>(
        &mut self,
        config: &Config,
        tls: T,
        object: &ObjectName,
        envelope: Envelope,
    ) -> Result<bool, Error>
    where
        T: MakeTlsConnect<Socket>,
        T::Stream: Send + 'static,
    {
        let (catalog, _connection) = connect(config, tls).await?;
        let Some(found) = lookup(&catalog, object).await? else {
            return Err(UsageError::new(format!("{object:?} does not exist")).into());
        };
        let fingerprint = found.fingerprint(envelope)?;
        fingerprint.payload_columns()?;
        let (write, _) =
            CheckpointWrite::create(BTreeMap::from([(object.name.clone(), fingerprint)]))
                .map_err(write_error)?;
        self.check_size(&write).await?;
        self.create_checkpoint(&write).await
    }

    /// Writes the first checkpoint, and returns whether this call created it.
    async fn create_checkpoint(&mut self, write: &CheckpointWrite) -> Result<bool, Error> {
        let in_store = self.checkpoint_in_store().await?;
        let text = write.new.as_str();
        let mut attempt = Attempts::new(&self.options, &self.sink_id);
        let mut lost = false;
        loop {
            let stored = match self.write_checkpoint(write).await {
                WriteResult::Applied { .. } => return Ok(true),
                WriteResult::Failed(SinkError::NotSupported) if in_store => {
                    return Err(missing_store());
                }
                WriteResult::Failed(error) => {
                    attempt.failed(error).await?;
                    continue;
                }
                WriteResult::Unknown => {
                    lost = true;
                    self.load_text().await?
                }
                WriteResult::NotApplied => self.load_text().await?,
            };
            let Some(stored) = stored else {
                attempt.retried().await?;
                continue;
            };
            // A create whose response was lost can still have applied.
            if lost && stored == text {
                return Ok(true);
            }
            // A replacement keeps its subscription, and `run` records its new
            // fingerprint.
            let requested = Checkpoint::decode(text)?.subscriptions;
            let stored = Checkpoint::decode(&stored)?.subscriptions;
            let same = |(name, fingerprint): (&String, &Fingerprint)| {
                stored.get(name).is_some_and(|stored| {
                    matches!(
                        compare(stored, Some(fingerprint)),
                        IdentityChange::Unchanged | IdentityChange::Replaced
                    )
                })
            };
            if stored.len() != requested.len() || !requested.iter().all(same) {
                return Err(UsageError::new(format!(
                    "sink {:?} already has a checkpoint for another subscription",
                    self.sink_id
                ))
                .into());
            }
            return Ok(false);
        }
    }

    /// Takes the checkpoint over and writes the stream to the sink until it
    /// ends, as a materialized view's does after its last refresh, or fails.
    /// A stream that fails with a transient error resumes from the checkpoint,
    /// with backoff.
    ///
    /// `tls` connects both the stream and a session for the catalog checks.
    pub async fn run<T>(&mut self, config: &Config, tls: T) -> Result<(), Error>
    where
        T: MakeTlsConnect<Socket> + Clone,
        T::Stream: Send + 'static,
    {
        // An invalid option must not fence the worker that runs now.
        self.options.live.validate()?;
        let mut current = self.take_over().await?;
        let (name, _) = only_subscription(&current)?;
        if current.frontier.is_some() {
            // A crash can fall between the write that completed a snapshot
            // and its sweep.
            self.sweep(current.generation).await?;
        }
        let mut reconnects = Attempts::new(&self.options, &self.sink_id);
        // An error that can come from the object changing, which the next
        // attempt's identity check resolves.
        let mut unresolved = None;
        // TODO: Refuse to start when an index on the subscribing cluster
        // serves the object, or when its retention is below the commit lag,
        // and watch the margin, as "History loss and retention margin"
        // describes.
        loop {
            let committed = current.frontier;
            self.opened_at = None;
            let attempt = self
                .attempt(config, tls.clone(), &name, &mut current, unresolved.take())
                .await;
            match attempt {
                Err(error) if checks_index(&error) => {
                    return Err(resolve_index(config, tls, &current, &name, error).await);
                }
                Err(error) if resumable(&error) || checks_object(&error) => {
                    // A stream that stayed open for a while counts as healthy even
                    // while its frontier stands still, as a quiet object's does.
                    // A snapshot that keeps failing does not: each attempt starts
                    // it again under a new generation, so only committed
                    // progress resets the budget until one completes.
                    let healthy = committed.is_some()
                        && self
                            .opened_at
                            .is_some_and(|at| at.elapsed() >= HEALTHY_STREAM);
                    if healthy || progressed(committed, current.frontier) {
                        reconnects = Attempts::new(&self.options, &self.sink_id);
                    }
                    if checks_object(&error) {
                        unresolved = Some(error.clone());
                    }
                    reconnects.again(error).await?;
                }
                result => return result,
            }
        }
    }

    /// Checks the object's identity, then streams from `current`. Every
    /// reconnect checks again, because the object can change while the
    /// stream is down. An `unresolved` error that the check finds no change
    /// for is returned as its own kind.
    async fn attempt<T>(
        &mut self,
        config: &Config,
        tls: T,
        name: &str,
        current: &mut Checkpoint,
        unresolved: Option<Error>,
    ) -> Result<(), Error>
    where
        T: MakeTlsConnect<Socket> + Clone,
        T::Stream: Send + 'static,
    {
        let (_, stored) = only_subscription(current)?;
        {
            let (catalog, _connection) = connect(config, tls.clone()).await?;
            *current = self
                .check_identity(&catalog, current.clone(), name, &stored)
                .await?;
        }
        if let Some(error) = unresolved
            && current.subscriptions.get(name) == Some(&stored)
        {
            return Err(resolved(error));
        }
        let fingerprint = current.subscriptions.get(name).cloned().unwrap_or(stored);
        self.stream(config, tls, &fingerprint, current).await
    }

    /// Streams from `current` into the sink until the stream ends or fails,
    /// moving `current` forward with each applied checkpoint write.
    async fn stream<T>(
        &mut self,
        config: &Config,
        tls: T,
        fingerprint: &Fingerprint,
        current: &mut Checkpoint,
    ) -> Result<(), Error>
    where
        T: MakeTlsConnect<Socket>,
        T::Stream: Send + 'static,
    {
        // A snapshot that did not complete starts again, under a new
        // generation, even when it gets the same `AS OF`.
        let start = match current.frontier {
            Some(frontier) => Start::Resume { frontier },
            None => Start::Snapshot { as_of: None },
        };
        let mut started = current.frontier.is_some();
        let columns = Arc::new(fingerprint.payload_columns()?);
        let mut checked_columns = false;
        let subscribe = Subscribe::object(fingerprint.object.clone())
            .with_envelope(fingerprint.envelope.clone());
        let mut live =
            LiveSubscription::connect_at(config, tls, subscribe, start, self.options.live.clone())
                .await?;
        self.opened_at = Some(Instant::now());
        let mut last_commit = Instant::now();
        while let Some(event) = live.next().await {
            let event = event?;
            if !checked_columns {
                // The object can change between the identity check and the
                // subscribe. This catches changed names, and the next
                // reconnect's identity check catches a changed type.
                let names = columns.key.iter().chain(&columns.value).map(|c| &c.name);
                if !live.columns().is_some_and(|live| names.eq(live)) {
                    return Err(Error::ObjectChanged {
                        change: IdentityChange::SchemaChanged,
                    });
                }
                checked_columns = true;
            }
            if !started {
                let as_of = live.as_of().ok_or_else(|| {
                    UsageError::new("the stream released a snapshot before its AS OF")
                })?;
                let (write, next) =
                    CheckpointWrite::start_snapshot(current, current.subscriptions.clone(), as_of)
                        .map_err(write_error)?;
                self.commit_until_applied(&write).await?;
                *current = next;
                started = true;
            }
            match event {
                Event::SnapshotChunk { updates } => {
                    let batch = self.batch(updates, &columns, current.generation, true);
                    let write = match self.sink.sink_type() {
                        SinkType::Transactional => {
                            Some(CheckpointWrite::snapshot_write(current).map_err(write_error)?)
                        }
                        _ => None,
                    };
                    // An event target sees no empty writes.
                    if write.is_some() || !batch.updates().is_empty() {
                        self.write_until_applied(&batch, write.as_ref()).await?;
                    }
                }
                Event::Batch(batch) => {
                    let completes = current.snapshot_as_of.is_some();
                    if batch.is_empty()
                        && !completes
                        && last_commit.elapsed() < self.options.commit_interval
                    {
                        continue;
                    }
                    let (write, next) =
                        CheckpointWrite::commit(current, batch.upper()).map_err(write_error)?;
                    let batch = self.batch(
                        batch.into_updates(),
                        &columns,
                        current.generation,
                        completes,
                    );
                    match self.sink.sink_type() {
                        SinkType::Transactional => {
                            self.write_until_applied(&batch, Some(&write)).await?;
                        }
                        _ => {
                            if !batch.updates().is_empty() {
                                self.write_until_applied(&batch, None).await?;
                            }
                            self.commit_until_applied(&write).await?;
                        }
                    }
                    *current = next;
                    last_commit = Instant::now();
                    if completes {
                        self.sweep(current.generation).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// A batch of `updates` without their key violations, which the SDK
    /// rejects so that the sink never sees them.
    fn batch(
        &self,
        updates: Vec<Update>,
        columns: &Arc<PayloadColumns>,
        generation: u64,
        snapshot: bool,
    ) -> SinkBatch {
        let (violations, updates): (Vec<_>, Vec<_>) = updates
            .into_iter()
            .partition(|update| matches!(update.change, Change::KeyViolation { .. }));
        for violation in violations {
            self.warn(Some(violation.timestamp), "key_violation", "sdk");
        }
        SinkBatch::new(updates, Arc::clone(columns), generation, snapshot)
    }

    /// Whether the checkpoint lives in the store, which a sink says by
    /// returning [`SinkError::NotSupported`] from [`Sink::load`].
    async fn checkpoint_in_store(&mut self) -> Result<bool, Error> {
        if let Some(in_store) = self.in_store {
            return Ok(in_store);
        }
        let mut attempt = Attempts::new(&self.options, &self.sink_id);
        loop {
            let loaded = tokio::time::timeout(self.options.store_timeout, self.sink.load()).await;
            match loaded {
                Ok(Err(SinkError::NotSupported)) => {
                    if self.sink.sink_type() == SinkType::Transactional {
                        return Err(UsageError::new(
                            "a transactional sink must store its checkpoint with its data",
                        )
                        .into());
                    }
                    self.in_store = Some(true);
                    return Ok(true);
                }
                Ok(Ok(_)) => {
                    self.in_store = Some(false);
                    return Ok(false);
                }
                Ok(Err(error)) => attempt.failed(error).await?,
                Err(_) => attempt.failed(timed_out("reading the checkpoint")).await?,
            }
        }
    }

    /// The largest checkpoint text the sink or the store holds.
    async fn max_checkpoint_bytes(&mut self) -> Result<usize, Error> {
        Ok(match self.checkpoint_in_store().await? {
            true => self.store.max_checkpoint_bytes(),
            false => self.sink.max_checkpoint_bytes(),
        })
    }

    /// The latest applied checkpoint text, or `None` for a sink that was never
    /// set up.
    async fn load_text(&mut self) -> Result<Option<String>, Error> {
        let in_store = self.checkpoint_in_store().await?;
        let mut attempt = Attempts::new(&self.options, &self.sink_id);
        loop {
            let loaded = if in_store {
                tokio::time::timeout(self.options.store_timeout, self.store.load(&self.sink_id))
                    .await
            } else {
                tokio::time::timeout(self.options.store_timeout, self.sink.load()).await
            };
            match loaded {
                Ok(Ok(text)) => return Ok(text),
                Ok(Err(SinkError::NotSupported)) if in_store => return Err(missing_store()),
                // Never a reason to read the checkpoint from elsewhere: that
                // would resume from a checkpoint this sink did not write.
                Ok(Err(error)) => attempt.failed(error).await?,
                Err(_) => attempt.failed(timed_out("reading the checkpoint")).await?,
            }
        }
    }

    /// The latest applied checkpoint.
    async fn load(&mut self) -> Result<Checkpoint, Error> {
        match self.load_text().await? {
            Some(text) => Ok(Checkpoint::decode(&text)?),
            None => Err(Error::NotSetUp {
                sink_id: self.sink_id.clone(),
            }),
        }
    }

    /// Takes the checkpoint over from its owner.
    async fn take_over(&mut self) -> Result<Checkpoint, Error> {
        let mut stored = self.load().await?;
        let mut attempt = Attempts::new(&self.options, &self.sink_id);
        loop {
            let (write, next) = CheckpointWrite::take_over(&stored).map_err(write_error)?;
            self.check_size(&write).await?;
            match self.write_checkpoint(&write).await {
                WriteResult::Applied { .. } => return Ok(next),
                // Two workers that start from the same checkpoint write the
                // same take-over, so a read cannot tell whose landed. Only an
                // applied response settles it, so take over again.
                // The owner keeps committing, so wait first and send right
                // after the read, or every attempt loses to a newer commit.
                WriteResult::NotApplied | WriteResult::Unknown => {
                    attempt.retried().await?;
                    stored = self.load().await?;
                }
                WriteResult::Failed(error) => attempt.failed(error).await?,
            }
        }
    }

    /// Compares the object the stored name resolves to with the stored
    /// fingerprint, and records a replacement's new fingerprint.
    async fn check_identity(
        &mut self,
        catalog: &Client,
        current: Checkpoint,
        name: &str,
        stored: &Fingerprint,
    ) -> Result<Checkpoint, Error> {
        let object = ObjectName {
            database: stored.object.database.clone(),
            schema: stored.object.schema.clone(),
            name: stored.object.name.clone(),
        };
        let found = lookup(catalog, &object).await?;
        let now = found
            .map(|found| found.fingerprint(stored.envelope.clone()))
            .transpose()?;
        match compare(stored, now.as_ref()) {
            IdentityChange::Unchanged => Ok(current),
            // TODO: Check the replacement's retention margin before resuming.
            IdentityChange::Replaced => {
                let mut subscriptions = current.subscriptions.clone();
                if let Some(now) = now {
                    subscriptions.insert(name.to_string(), now);
                }
                let (write, next) =
                    CheckpointWrite::fingerprints(&current, subscriptions).map_err(write_error)?;
                self.commit_until_applied(&write).await?;
                Ok(next)
            }
            change => Err(Error::ObjectChanged { change }),
        }
    }

    /// Refuses a checkpoint write larger than the sink or the store holds.
    async fn check_size(&mut self, write: &CheckpointWrite) -> Result<(), Error> {
        let limit = self.max_checkpoint_bytes().await?;
        if write.new.len() > limit {
            return Err(CheckpointError::TooLarge {
                size: write.new.len(),
                limit,
            }
            .into());
        }
        Ok(())
    }

    /// Writes the checkpoint, through the store or the sink.
    async fn write_checkpoint(&mut self, write: &CheckpointWrite) -> WriteResult {
        let written = if self.in_store == Some(true) {
            tokio::time::timeout(
                self.options.store_timeout,
                self.store.write(&self.sink_id, write),
            )
            .await
        } else {
            tokio::time::timeout(
                self.options.store_timeout,
                self.sink.write(WriteRequest::Checkpoint(write)),
            )
            .await
        };
        written.unwrap_or(WriteResult::Unknown)
    }

    async fn commit_until_applied(&mut self, write: &CheckpointWrite) -> Result<(), Error> {
        self.check_size(write).await?;
        let mut attempt = Attempts::new(&self.options, &self.sink_id);
        loop {
            let result = self.write_checkpoint(write).await;
            if self.settled(write, result, &mut attempt).await?.is_some() {
                return Ok(());
            }
        }
    }

    /// Writes `batch` until it applies. A `checkpoint` write that was refused or
    /// whose result is unknown is settled by reading the checkpoint back, and
    /// a failed write is sent again.
    async fn write_until_applied(
        &mut self,
        batch: &SinkBatch,
        checkpoint: Option<&CheckpointWrite>,
    ) -> Result<(), Error> {
        if let Some(write) = checkpoint {
            self.check_size(write).await?;
        }
        let request = match checkpoint {
            Some(write) => WriteRequest::BatchAndCheckpoint(batch, write),
            None => WriteRequest::Batch(batch),
        };
        let mut attempt = Attempts::new(&self.options, &self.sink_id);
        loop {
            let written =
                tokio::time::timeout(self.options.write_timeout, self.sink.write(request))
                    .await
                    .unwrap_or(WriteResult::Unknown);
            let rejected = match checkpoint {
                Some(write) => self.settled(write, written, &mut attempt).await?,
                // Without a checkpoint write, sending the batch again is safe.
                None => match written {
                    WriteResult::Applied { rejected } => Some(rejected),
                    WriteResult::Failed(error) => {
                        attempt.failed(error).await?;
                        None
                    }
                    WriteResult::NotApplied | WriteResult::Unknown => {
                        attempt.retried().await?;
                        None
                    }
                },
            };
            if let Some(rejected) = rejected {
                self.report(batch, &rejected);
                return Ok(());
            }
        }
    }

    /// What a call carrying the checkpoint `write` came to: `Some` once it
    /// applied, with the rejected updates, and `None` to send it again.
    async fn settled(
        &mut self,
        write: &CheckpointWrite,
        result: WriteResult,
        attempt: &mut Attempts,
    ) -> Result<Option<Vec<Rejected>>, Error> {
        match result {
            WriteResult::Applied { rejected } => Ok(Some(rejected)),
            WriteResult::Failed(error) => {
                attempt.failed(error).await?;
                Ok(None)
            }
            WriteResult::NotApplied | WriteResult::Unknown => {
                let stored = self.load().await?;
                // The first checkpoint is settled by `create_checkpoint`.
                let expected = write
                    .expected
                    .as_deref()
                    .ok_or_else(|| UsageError::new("a first checkpoint settles on its own"))?;
                let expected = Checkpoint::decode(expected)?;
                let new = Checkpoint::decode(&write.new)?;
                match stored.settle(&expected, &new) {
                    Settlement::Applied => Ok(Some(Vec::new())),
                    Settlement::SendAgain => {
                        attempt.retried().await?;
                        Ok(None)
                    }
                    Settlement::Conflict { conflict } => Err(conflict.into()),
                    _ => Err(UsageError::new("only a take-over takes over again").into()),
                }
            }
        }
    }

    async fn sweep(&mut self, generation: u64) -> Result<(), Error> {
        let mut attempt = Attempts::new(&self.options, &self.sink_id);
        loop {
            let swept = tokio::time::timeout(
                self.options.write_timeout,
                self.sink.sweep_generations(generation),
            )
            .await;
            match swept {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => attempt.failed(error).await?,
                Err(_) => {
                    attempt
                        .failed(timed_out("sweeping old generations"))
                        .await?
                }
            }
        }
    }

    fn report(&self, batch: &SinkBatch, rejected: &[Rejected]) {
        for rejected in rejected {
            let timestamp = batch.updates().get(rejected.index).map(|u| u.timestamp);
            self.warn(timestamp, &rejected.category, "sink");
        }
    }

    /// Logs a rejected update by its `mz_timestamp`, its category, and who
    /// rejected it, without its row or the target's message, which can hold
    /// row data.
    fn warn(&self, timestamp: Option<u64>, category: &str, by: &str) {
        tracing::warn!(
            sink_id = %self.sink_id,
            mz_timestamp = timestamp,
            category,
            rejected_by = by,
            "an update was rejected"
        );
    }
}

/// The attempts left for one call, with exponential backoff between them.
struct Attempts {
    left: u32,
    backoff: Duration,
    /// Throttling backs off on its own, so that it does not lengthen the
    /// waits after failures.
    throttle_backoff: Duration,
    /// How long this call has waited out throttling.
    throttled: Duration,
    sink_id: String,
}

/// How long a stream must stay open for a later failure to get a fresh
/// reconnect budget without committing. A stream that fails sooner keeps using
/// up its attempts.
const HEALTHY_STREAM: Duration = Attempts::MAX_BACKOFF;

impl Attempts {
    const MIN_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    /// How long one call waits out throttling before the SDK warns, since a
    /// limit that never passes looks the same as a long one.
    const THROTTLED_WARNING: Duration = Duration::from_secs(300);

    fn new(options: &DurableOptions, sink_id: &str) -> Attempts {
        Attempts {
            left: options.max_attempts,
            backoff: Attempts::MIN_BACKOFF,
            throttle_backoff: Attempts::MIN_BACKOFF,
            throttled: Duration::ZERO,
            sink_id: sink_id.to_string(),
        }
    }

    /// Waits before the next attempt after `error`, or fails with it once it
    /// is permanent or the attempts run out. Throttling waits without using up
    /// an attempt, for the time the target asked within the backoff's bounds,
    /// so that a zero does not spin.
    async fn failed(&mut self, error: SinkError) -> Result<(), Error> {
        if let SinkError::Throttled { retry_after } = error {
            let wait = match retry_after {
                Some(wait) => wait.clamp(Attempts::MIN_BACKOFF, Attempts::MAX_BACKOFF),
                None => {
                    let wait = self.throttle_backoff;
                    self.throttle_backoff = wait.saturating_mul(2).min(Attempts::MAX_BACKOFF);
                    wait
                }
            };
            let before = self.throttled;
            self.throttled = self.throttled.saturating_add(wait);
            if before < Attempts::THROTTLED_WARNING && self.throttled >= Attempts::THROTTLED_WARNING
            {
                tracing::warn!(
                    sink_id = %self.sink_id,
                    throttled = ?self.throttled,
                    "the sink has been throttled for minutes"
                );
            } else {
                tracing::info!(sink_id = %self.sink_id, ?wait, "the sink is throttled");
            }
            tokio::time::sleep(wait).await;
            return Ok(());
        }
        if matches!(
            error,
            SinkError::NotSupported
                | SinkError::Target {
                    permanent: true,
                    ..
                }
        ) {
            return Err(Error::Sink(error));
        }
        self.again(Error::Sink(error)).await
    }

    /// Counts a call sent again without a failure, such as after a lost
    /// response.
    async fn retried(&mut self) -> Result<(), Error> {
        self.again(Error::Sink(SinkError::Target {
            message: "a write did not settle within its attempts".into(),
            permanent: false,
        }))
        .await
    }

    /// Waits before the next attempt, or fails with `error` once the attempts
    /// run out.
    async fn again(&mut self, error: Error) -> Result<(), Error> {
        if self.left <= 1 {
            return Err(error);
        }
        self.wait().await;
        Ok(())
    }

    async fn wait(&mut self) {
        self.left = self.left.saturating_sub(1);
        tokio::time::sleep(self.backoff).await;
        self.backoff = self.backoff.saturating_mul(2).min(Attempts::MAX_BACKOFF);
    }
}

/// Whether a stream that failed with `error` resumes from the checkpoint.
fn resumable(error: &Error) -> bool {
    match error {
        Error::ConsumerFellBehind { .. } => true,
        Error::Server {
            classification: Classification::Kind { kind },
            ..
        } => matches!(kind, ErrorKind::Transient | ErrorKind::FellBehind),
        _ => false,
    }
}

fn missing_store() -> Error {
    UsageError::new(
        "the sink does not store its checkpoint, so give it a store with `DurableSink::with_store`",
    )
    .into()
}

/// A failed catalog request, classified as if it started a subscription, so
/// that a transient failure is retried.
fn catalog_error(error: &tokio_postgres::Error) -> Error {
    server_error_with(error, |sqlstate, message| {
        classify(sqlstate, message, Phase::Starting)
    })
}

/// Whether an attempt that committed `before` and stopped at `after` made
/// progress: a restarted snapshot moves the checkpoint without committing a
/// frontier.
fn progressed(before: Option<Frontier>, after: Option<Frontier>) -> bool {
    after.is_some() && after != before
}

/// What a resume that found no valid timestamp comes to: an index on the
/// session's cluster that limits the object's history, or history loss.
/// Returns `error` unchanged if the indexes cannot be read.
async fn resolve_index<T>(
    config: &Config,
    tls: T,
    current: &Checkpoint,
    name: &str,
    error: Error,
) -> Error
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    let Some(fingerprint) = current.subscriptions.get(name) else {
        return error;
    };
    let indexes = async {
        let (catalog, _connection) = connect(config, tls).await.ok()?;
        let (_, rows) = query(&catalog, "SHOW cluster").await.ok()?;
        let cluster = rows.first()?.first()?.clone()?;
        let sql = catalog::indexes_sql(&fingerprint.object.id, &cluster);
        let (columns, rows) = query(&catalog, &sql).await.ok()?;
        catalog::parse_indexes(&columns, rows).ok()
    };
    let Some(indexes) = indexes.await else {
        return error;
    };
    match error {
        Error::Server {
            sqlstate,
            message,
            detail,
            hint,
            ..
        } => Error::Server {
            sqlstate,
            message,
            detail,
            hint,
            classification: Classification::Kind {
                kind: resolve_index_check(&indexes),
            },
        },
        error => error,
    }
}

/// Whether `error` can come from an index on the subscribing cluster.
fn checks_index(error: &Error) -> bool {
    matches!(
        error,
        Error::Server {
            classification: Classification::CheckIndex,
            ..
        }
    )
}

/// Whether `error` can come from the subscribed object changing.
fn checks_object(error: &Error) -> bool {
    matches!(
        error,
        Error::Server {
            classification: Classification::CheckObject { .. },
            ..
        }
    )
}

/// `error` once the identity check found the object unchanged.
fn resolved(error: Error) -> Error {
    match error {
        Error::Server {
            sqlstate,
            message,
            detail,
            hint,
            classification: Classification::CheckObject { otherwise },
        } => Error::Server {
            sqlstate,
            message,
            detail,
            hint,
            classification: Classification::Kind { kind: otherwise },
        },
        error => error,
    }
}

fn timed_out(what: &str) -> SinkError {
    SinkError::Target {
        message: format!("{what} timed out"),
        permanent: false,
    }
}

fn only_subscription(checkpoint: &Checkpoint) -> Result<(String, Fingerprint), Error> {
    let mut subscriptions = checkpoint.subscriptions.iter();
    match (subscriptions.next(), subscriptions.next()) {
        (Some((name, fingerprint)), None) => Ok((name.clone(), fingerprint.clone())),
        _ => Err(UsageError::new("a durable sink reads exactly one subscription").into()),
    }
}

fn write_error(error: WriteError) -> Error {
    match error {
        WriteError::Checkpoint(error) => error.into(),
        WriteError::Conflict(conflict) => conflict.into(),
        WriteError::Usage(error) => error.into(),
        _ => UsageError::new(error.to_string()).into(),
    }
}

async fn connect<T>(config: &Config, tls: T) -> Result<(Client, AbortOnDrop), Error>
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    let (client, connection) = with_network_defaults(config)
        .connect(tls)
        .await
        .map_err(|error| catalog_error(&error))?;
    let connection = AbortOnDrop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    Ok((client, connection))
}

async fn lookup(
    client: &Client,
    object: &ObjectName,
) -> Result<Option<catalog::CatalogObject>, Error> {
    let (columns, rows) = query(client, &catalog::lookup_sql(object))
        .await
        .map_err(|error| catalog_error(&error))?;
    Ok(catalog::parse_lookup(&columns, rows)?)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use std::time::Duration;

    use materialize_sdk_core::CommitConflict;

    use super::*;

    /// A sink whose writes and loads return what the test scripted.
    #[derive(Default)]
    struct Scripted {
        results: VecDeque<WriteResult>,
        loads: VecDeque<String>,
        calls: usize,
        /// Every request sent, as its variant and its checkpoint write's kind.
        requests: Vec<String>,
        /// The expected text of every checkpoint write sent.
        expected: Vec<Option<String>>,
    }

    impl Sink for Scripted {
        fn sink_type(&self) -> SinkType {
            SinkType::Transactional
        }

        async fn load(&mut self) -> Result<Option<String>, SinkError> {
            Ok(self.loads.pop_front())
        }

        async fn write(&mut self, request: WriteRequest<'_>) -> WriteResult {
            self.calls += 1;
            let kind = |write: &CheckpointWrite| {
                serde_json::to_value(write).unwrap()["kind"]
                    .as_str()
                    .unwrap()
                    .to_string()
            };
            self.requests.push(match request {
                WriteRequest::Batch(_) => "batch".into(),
                WriteRequest::BatchAndCheckpoint(_, write) => format!("batch and {}", kind(write)),
                WriteRequest::Checkpoint(write) => kind(write),
            });
            if let Some(write) = request.checkpoint() {
                self.expected.push(write.expected.clone());
            }
            self.results.pop_front().unwrap()
        }
    }

    fn durable(results: Vec<WriteResult>, loads: Vec<String>) -> DurableSink<Scripted> {
        let sink = Scripted {
            results: results.into(),
            loads: loads.into(),
            ..Scripted::default()
        };
        let mut durable = DurableSink::new("s", sink, DurableOptions::default().max_attempts(3));
        durable.in_store = Some(false);
        durable
    }

    /// A commit from frontier 10 to 20, and the texts before and after.
    fn commit() -> (CheckpointWrite, String, String) {
        let before = Checkpoint::new(BTreeMap::new()).committed(Frontier::new(10).unwrap());
        let (write, _) = CheckpointWrite::commit(&before, Frontier::new(20).unwrap()).unwrap();
        let (expected, new) = (write.expected.clone().unwrap(), write.new.clone());
        (write, expected, new)
    }

    fn failed(permanent: bool) -> WriteResult {
        WriteResult::Failed(SinkError::Target {
            message: "down".into(),
            permanent,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn an_unknown_write_that_applied_is_not_sent_again() {
        let (write, _, new) = commit();
        let mut d = durable(vec![WriteResult::Unknown], vec![new]);
        d.commit_until_applied(&write).await.unwrap();
        assert_eq!(d.sink.calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unknown_write_that_did_not_apply_is_sent_again() {
        let (write, expected, _) = commit();
        let mut d = durable(
            vec![WriteResult::Unknown, WriteResult::applied()],
            vec![expected],
        );
        d.commit_until_applied(&write).await.unwrap();
        assert_eq!(d.sink.calls, 2);
        assert_eq!(d.sink.requests, ["commit", "commit"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_write_after_a_take_over_is_fenced() {
        let (write, expected, _) = commit();
        let taken = Checkpoint::decode(&expected).unwrap().take_over().unwrap();
        let mut d = durable(vec![WriteResult::NotApplied], vec![taken.encode().unwrap()]);
        let result = d.commit_until_applied(&write).await;
        assert!(
            matches!(result, Err(Error::Conflict(CommitConflict::Fenced { .. }))),
            "{result:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_stops_at_once() {
        let (write, _, _) = commit();
        let mut d = durable(vec![failed(true)], vec![]);
        assert!(matches!(
            d.commit_until_applied(&write).await,
            Err(Error::Sink(_))
        ));
        assert_eq!(d.sink.calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_stop_once_the_attempts_run_out() {
        let (write, _, _) = commit();
        let mut d = durable(vec![failed(false), failed(false), failed(false)], vec![]);
        assert!(matches!(
            d.commit_until_applied(&write).await,
            Err(Error::Sink(_))
        ));
        assert_eq!(d.sink.calls, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn throttling_waits_without_using_up_attempts() {
        let (write, _, _) = commit();
        let throttled = |retry_after| WriteResult::Failed(SinkError::Throttled { retry_after });
        let mut results: Vec<_> = (0..5).map(|_| throttled(None)).collect();
        results.push(throttled(Some(Duration::from_secs(300))));
        results.extend([failed(false), failed(false), WriteResult::applied()]);
        let mut d = durable(results, vec![]);
        let started = tokio::time::Instant::now();
        d.commit_until_applied(&write).await.unwrap();
        assert_eq!(d.sink.calls, 9);
        // 1 + 2 + 4 + 8 + 16 seconds of throttling backoff, the 300 seconds
        // the target asked for capped at 60, then 1 + 2 for the failures.
        assert_eq!(started.elapsed(), Duration::from_secs(31 + 60 + 3));
    }

    /// A checkpoint that every write replaces with a compare-and-set.
    #[derive(Clone)]
    struct Shared(std::sync::Arc<std::sync::Mutex<String>>);

    impl Sink for Shared {
        fn sink_type(&self) -> SinkType {
            SinkType::Transactional
        }

        /// Returns what it read 100ms earlier, as a read over a network does.
        async fn load(&mut self) -> Result<Option<String>, SinkError> {
            let read = self.0.lock().unwrap().clone();
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(Some(read))
        }

        async fn write(&mut self, request: WriteRequest<'_>) -> WriteResult {
            let WriteRequest::Checkpoint(write) = request else {
                unreachable!("a take-over writes no batch")
            };
            let mut stored = self.0.lock().unwrap();
            if write.expected.as_deref() != Some(stored.as_str()) {
                return WriteResult::NotApplied;
            }
            *stored = write.new.clone();
            WriteResult::applied()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_take_over_wins_against_an_owner_that_keeps_committing() {
        let first = Checkpoint::new(BTreeMap::new()).committed(Frontier::new(1).unwrap());
        let shared = Shared(std::sync::Arc::new(std::sync::Mutex::new(
            first.encode().unwrap(),
        )));
        // The owner commits every half second, more often than the backoff.
        let mut owner = shared.clone();
        let owning = tokio::spawn(async move {
            let mut current = first;
            for frontier in 2.. {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let (write, next) =
                    CheckpointWrite::commit(&current, Frontier::new(frontier).unwrap()).unwrap();
                let written = owner.write(WriteRequest::Checkpoint(&write)).await;
                if !matches!(written, WriteResult::Applied { .. }) {
                    return;
                }
                current = next;
            }
        });
        // The first attempt reads just before the owner's first commit, so it
        // loses, and every later read is followed by a write at once.
        tokio::time::sleep(Duration::from_millis(450)).await;
        let mut d = DurableSink::new("s", shared, DurableOptions::default().max_attempts(3));
        d.in_store = Some(false);
        let owned = d.take_over().await.unwrap();
        assert_eq!(owned.epoch, 2);
        owning.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_take_over_with_an_unknown_result_takes_over_again() {
        // The first take-over landed, but its response was lost, and another
        // worker could have written the same text.
        let stored = Checkpoint::new(BTreeMap::new());
        let landed = stored.take_over().unwrap();
        let mut d = durable(
            vec![WriteResult::Unknown, WriteResult::applied()],
            vec![stored.encode().unwrap(), landed.encode().unwrap()],
        );
        let owned = d.take_over().await.unwrap();
        assert_eq!(
            owned.epoch, 3,
            "only an applied response settles a take-over"
        );
        assert_eq!(d.sink.expected[1], Some(landed.encode().unwrap()));
        assert_eq!(d.sink.requests, ["take_over", "take_over"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_first_checkpoint_is_not_settled_as_a_later_write() {
        let mut d = durable(vec![], vec![first_text()]);
        let mut attempt = Attempts::new(&d.options, "s");
        let settled = d
            .settled(
                &create(BTreeMap::new()),
                WriteResult::NotApplied,
                &mut attempt,
            )
            .await;
        assert!(matches!(settled, Err(Error::Usage(_))), "{settled:?}");
    }

    fn first_text() -> String {
        Checkpoint::new(BTreeMap::new()).encode().unwrap()
    }

    fn create(subscriptions: BTreeMap<String, Fingerprint>) -> CheckpointWrite {
        CheckpointWrite::create(subscriptions).unwrap().0
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_create_that_applied_counts_as_created() {
        let mut d = durable(vec![WriteResult::Unknown], vec![first_text()]);
        assert!(d.create_checkpoint(&create(BTreeMap::new())).await.unwrap());
        assert_eq!(d.sink.requests, ["create"]);
        assert_eq!(d.sink.expected, [None]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_create_that_did_not_apply_is_sent_again() {
        let mut d = durable(vec![WriteResult::Unknown, WriteResult::applied()], vec![]);
        assert!(d.create_checkpoint(&create(BTreeMap::new())).await.unwrap());
        assert_eq!(d.sink.requests, ["create", "create"]);
        assert_eq!(d.sink.expected, [None, None]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_create_that_finds_the_same_subscription_did_not_create() {
        let owned = Checkpoint::decode(&first_text())
            .unwrap()
            .take_over()
            .unwrap();
        let mut d = durable(vec![WriteResult::NotApplied], vec![owned.encode().unwrap()]);
        assert!(!d.create_checkpoint(&create(BTreeMap::new())).await.unwrap());
    }

    fn subscription(id: &str, shard: &str) -> BTreeMap<String, Fingerprint> {
        BTreeMap::from([(
            "orders".to_string(),
            Fingerprint {
                object: materialize_sdk_core::ObjectRef {
                    id: id.into(),
                    database: None,
                    schema: "public".into(),
                    name: "orders".into(),
                },
                shard: shard.into(),
                envelope: Envelope::Diff,
                columns: Vec::new(),
            },
        )])
    }

    #[tokio::test(start_paused = true)]
    async fn a_setup_again_after_a_replacement_keeps_the_checkpoint() {
        let stored = Checkpoint::new(subscription("u1", "s1")).encode().unwrap();
        let replaced = create(subscription("u2", "s1"));
        let mut d = durable(vec![WriteResult::NotApplied], vec![stored.clone()]);
        assert!(!d.create_checkpoint(&replaced).await.unwrap());

        // Other storage behind the name is another subscription.
        let swapped = create(subscription("u1", "s2"));
        let mut d = durable(vec![WriteResult::NotApplied], vec![stored]);
        assert!(d.create_checkpoint(&swapped).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn a_create_that_finds_another_subscription_is_refused() {
        let other = Checkpoint::new(BTreeMap::from([(
            "other".to_string(),
            Fingerprint {
                object: materialize_sdk_core::ObjectRef {
                    id: "u1".into(),
                    database: None,
                    schema: "public".into(),
                    name: "other".into(),
                },
                shard: "s1".into(),
                envelope: Envelope::Diff,
                columns: Vec::new(),
            },
        )]));
        let mut d = durable(vec![WriteResult::NotApplied], vec![other.encode().unwrap()]);
        assert!(matches!(
            d.create_checkpoint(&create(BTreeMap::new())).await,
            Err(Error::Usage(_))
        ));
    }

    /// A transactional sink that leaves its checkpoint to the SDK.
    struct Unstored;

    impl Sink for Unstored {
        fn sink_type(&self) -> SinkType {
            SinkType::Transactional
        }

        async fn write(&mut self, _: WriteRequest<'_>) -> WriteResult {
            unreachable!()
        }
    }

    #[test]
    fn only_a_newly_committed_frontier_is_progress() {
        let f = |t| Frontier::new(t).unwrap();
        assert!(progressed(None, Some(f(5))), "a completed snapshot");
        assert!(progressed(Some(f(5)), Some(f(6))));
        assert!(!progressed(Some(f(5)), Some(f(5))));
        assert!(!progressed(Some(f(5)), None), "a restarted snapshot");
        assert!(!progressed(None, None));
    }

    #[tokio::test(start_paused = true)]
    async fn setup_without_a_store_names_the_fix() {
        let mut d = DurableSink::new("s", Events, DurableOptions::default());
        d.in_store = Some(true);
        assert!(matches!(
            d.create_checkpoint(&create(BTreeMap::new())).await,
            Err(Error::Usage(error)) if error.to_string().contains("with_store")
        ));
    }

    /// A sink that leaves its checkpoint to the SDK.
    struct Events;

    impl Sink for Events {
        fn sink_type(&self) -> SinkType {
            SinkType::NonTransactional
        }

        async fn write(&mut self, _: WriteRequest<'_>) -> WriteResult {
            unreachable!()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_transactional_sink_must_store_its_checkpoint() {
        let mut d = DurableSink::new("s", Unstored, DurableOptions::default());
        assert!(matches!(
            d.checkpoint_in_store().await,
            Err(Error::Usage(_))
        ));
    }
}
