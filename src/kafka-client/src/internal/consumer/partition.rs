// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One partition of a consumer: its fetch state, its queue of fetched data,
//! and the application's read cursor and position.
//!
//! # Locking
//!
//! [`Partition::state`] is the handoff between the fetcher (I/O tasks) and
//! readers: the fetcher appends items under it once per Fetch response, and
//! readers take one [`Chunk`] at a time out of it. [`Partition::reader`]
//! holds the chunk being decoded and is only locked by threads that poll,
//! so decompression and decoding never block the fetcher. The lock order
//! is `reader` before `state`. Positions and queue sizes are atomics, so
//! that `position()` and the fetcher's backpressure checks take no reader
//! lock.
//!
//! # Versions
//!
//! Every (re)start and stop of fetching bumps [`PartState::version`] and
//! clears the queue and cursor. The fetcher tags requests with the version
//! and drops responses for older versions, so nothing fetched before a
//! restart is served after it.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

use crate::error::{KafkaError, KafkaResult};
use crate::internal::consumer::reader::{Chunk, Cursor, Progress, Step};
use crate::internal::records::{Record, TimestampType};
use crate::message::{BorrowedHeaders, BorrowedMessage, Timestamp};
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

use super::ConsumerInner;

/// librdkafka's `RD_KAFKA_OFFSET_INVALID`.
pub(super) const OFFSET_INVALID: i64 = -1001;
/// librdkafka's `RD_KAFKA_OFFSET_STORED`.
pub(super) const OFFSET_STORED: i64 = -1000;
/// librdkafka's `RD_KAFKA_OFFSET_BEGINNING`.
pub(super) const OFFSET_BEGINNING: i64 = -2;
/// librdkafka's `RD_KAFKA_OFFSET_END`.
pub(super) const OFFSET_END: i64 = -1;
/// librdkafka's `RD_KAFKA_OFFSET_TAIL_BASE`.
pub(super) const OFFSET_TAIL_BASE: i64 = -2000;

/// An entry of a partition queue.
#[derive(Debug)]
pub(super) enum Item {
    /// Fetched records.
    Chunk(Chunk),
    /// A consumer error, returned by `poll`.
    Error(KafkaError),
    /// The end of the partition was reached, returned by `poll` as
    /// [`KafkaError::PartitionEOF`] when `enable.partition.eof` is set.
    Eof,
    /// An error passed to `ClientContext::error` when the queue is served,
    /// not returned by `poll`, like librdkafka's `RD_KAFKA_OP_ERR`.
    Notice(RDKafkaErrorCode, String),
}

/// What serving a queue produced.
#[derive(Debug)]
pub(super) enum Served {
    /// A message or error for `poll` to return.
    Message(KafkaResult<BorrowedMessage<'static>>),
    /// A notice for `ClientContext::error`. The caller delivers it after
    /// releasing the queue's locks, so that the callback can use the
    /// consumer.
    Notice(RDKafkaErrorCode, String),
}

/// How the start offset of a fetch is determined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FetchPhase {
    /// Not assigned.
    Idle,
    /// Waiting for the logical offset (librdkafka raw encoding: beginning,
    /// end, stored, tail) to be resolved.
    Resolve(i64),
    /// Fetching from `FetchState::offset`.
    Active,
    /// Stopped after an error that needs the application, e.g. an offset
    /// reset with `auto.offset.reset=error`.
    Stopped,
}

/// The fetcher's view of a partition.
#[derive(Debug)]
pub(super) struct FetchState {
    pub phase: FetchPhase,
    /// The next offset to fetch, when active.
    pub offset: i64,
    /// The leader epoch of the last fetched batch, for logs.
    pub leader_epoch: i32,
    /// The request that currently fetches this partition.
    pub in_flight: Option<u64>,
    /// A resolution of the start offset is running.
    pub resolving: bool,
    pub backoff_until: Option<Instant>,
    /// Consecutive failed attempts to resolve the start offset.
    pub resolve_failures: u32,
    /// The end offset at which EOF was last signaled.
    pub eof_offset: i64,
    /// `PartitionMaxBytes` of the next fetch; grows when a single batch does
    /// not fit.
    pub max_bytes: i32,
    /// Bytes per message in the last fetched record set, or 0 if unknown,
    /// to convert the queue's message budget into bytes.
    pub avg_msg_bytes: u64,
    /// The prefetch target in queued bytes, see the fetcher.
    pub target: u64,
    /// Bytes per second the reader drained, smoothed.
    pub drain_rate: u64,
    /// `Partition::consumed` and the time when `drain_rate` was last
    /// sampled.
    pub drain_mark: Option<(u64, Instant)>,
    /// When a sample last saw the reader drain.
    pub drained_at: Option<Instant>,
    /// The last error reported to the application, for errors reported once.
    pub last_error: Option<RDKafkaErrorCode>,
    /// Log start offset, high watermark and last stable offset from the last
    /// Fetch response, or -1.
    pub lo_offset: i64,
    pub hi_offset: i64,
    pub ls_offset: i64,
}

/// State shared between the fetcher and readers.
#[derive(Debug)]
pub(super) struct PartState {
    pub version: u64,
    pub fetch: FetchState,
    items: VecDeque<Item>,
    /// The partition is on the main queue's ready list.
    in_main: bool,
    /// Threads waiting in a blocking poll of the split queue.
    waiters: usize,
}

/// What to notify after appending to a partition queue.
#[must_use]
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Wake {
    /// The queue went from empty to non-empty.
    edge: bool,
    /// Add the partition to the main queue's ready list.
    main: bool,
    /// Blocking pollers of the split queue wait.
    waiters: bool,
}

impl PartState {
    /// Appends an item and reports whom to notify.
    ///
    /// The queue counts as empty when no item waits, even while a reader is
    /// still serving the records of the previous chunk: like librdkafka,
    /// whose queue is empty once the last message was taken, this may cause a
    /// wakeup that finds data already served, but never misses one.
    pub fn push(&mut self, item: Item, split: bool) -> Wake {
        let edge = self.items.is_empty();
        self.items.push_back(item);
        let main = !split && !self.in_main;
        if main {
            self.in_main = true;
        }
        Wake {
            edge,
            main,
            waiters: self.waiters > 0,
        }
    }

    /// Appends a notice unless the queue already ends with the same one, so
    /// that a queue nobody polls does not grow with repeated notices.
    pub fn push_notice(
        &mut self,
        code: RDKafkaErrorCode,
        reason: String,
        split: bool,
    ) -> Option<Wake> {
        if matches!(self.items.back(), Some(Item::Notice(c, r)) if *c == code && *r == reason) {
            return None;
        }
        Some(self.push(Item::Notice(code, reason), split))
    }
}

/// A registered nonempty callback.
struct Callback {
    /// The queue handle that registered it.
    owner: u64,
    f: Arc<dyn Fn() + Send + Sync>,
}

/// One partition, kept for the life of the consumer once used, like
/// librdkafka's toppars, so that split queues survive reassignment.
pub(crate) struct Partition {
    pub(super) topic: Arc<str>,
    pub(super) id: i32,
    pub(super) state: Mutex<PartState>,
    cv: Condvar,
    reader: Mutex<Option<Cursor>>,
    /// The application position in librdkafka's raw encoding.
    position: AtomicI64,
    /// The offset store, raw.
    stored: AtomicI64,
    /// The last offset committed through this consumer, raw.
    pub(super) committed: AtomicI64,
    /// Messages go to a split queue, not the main queue.
    split: AtomicBool,
    /// Bytes and messages queued and not yet consumed by a reader.
    queued_bytes: AtomicU64,
    queued_msgs: AtomicU64,
    /// Bytes readers consumed in total, for the drain rate.
    consumed: AtomicU64,
    /// When non-zero, the fetcher waits for the queue to hold fewer bytes
    /// than this, and the reader that drains it below wakes the fetchers.
    resume_below: AtomicU64,
    callback: Mutex<Option<Callback>>,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Partition {
    pub(super) fn new(topic: Arc<str>, id: i32, max_bytes: i32) -> Partition {
        Partition {
            topic,
            id,
            state: Mutex::new(PartState {
                version: 0,
                fetch: FetchState {
                    phase: FetchPhase::Idle,
                    offset: OFFSET_INVALID,
                    leader_epoch: -1,
                    in_flight: None,
                    resolving: false,
                    backoff_until: None,
                    resolve_failures: 0,
                    eof_offset: OFFSET_INVALID,
                    max_bytes,
                    avg_msg_bytes: 0,
                    target: 0,
                    drain_rate: 0,
                    drain_mark: None,
                    drained_at: None,
                    last_error: None,
                    lo_offset: -1,
                    hi_offset: -1,
                    ls_offset: -1,
                },
                items: VecDeque::new(),
                in_main: false,
                waiters: 0,
            }),
            cv: Condvar::new(),
            reader: Mutex::new(None),
            position: AtomicI64::new(OFFSET_INVALID),
            stored: AtomicI64::new(OFFSET_INVALID),
            committed: AtomicI64::new(OFFSET_INVALID),
            split: AtomicBool::new(false),
            queued_bytes: AtomicU64::new(0),
            queued_msgs: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            resume_below: AtomicU64::new(0),
            callback: Mutex::new(None),
        }
    }

    pub(super) fn lock_state(&self) -> MutexGuard<'_, PartState> {
        self.state.lock().expect("lock poisoned")
    }

    /// The application position, raw.
    pub(super) fn position(&self) -> i64 {
        self.position.load(Ordering::Acquire)
    }

    /// The offset store, raw.
    pub(super) fn stored(&self) -> i64 {
        self.stored.load(Ordering::Acquire)
    }

    pub(super) fn store(&self, offset: i64) {
        self.stored.store(offset, Ordering::Release);
    }

    pub(super) fn is_split(&self) -> bool {
        self.split.load(Ordering::Acquire)
    }

    /// Bytes and messages queued, for backpressure.
    pub(super) fn queued(&self) -> (u64, u64) {
        (
            self.queued_bytes.load(Ordering::Acquire),
            self.queued_msgs.load(Ordering::Acquire),
        )
    }

    /// Accounts for data added to the queue.
    pub(super) fn add_queued(&self, bytes: u64, msgs: u64) {
        self.queued_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.queued_msgs.fetch_add(msgs, Ordering::AcqRel);
    }

    /// Bytes consumed by readers in total.
    pub(super) fn consumed(&self) -> u64 {
        self.consumed.load(Ordering::Acquire)
    }

    /// Reports whether the queue holds fewer than `limit` bytes. If not,
    /// arms readers to wake the fetchers once it does.
    pub(super) fn fetchable_below(&self, limit: u64) -> bool {
        // Arm, then check: a reader that drains in between either sees the
        // armed limit or its subtraction is seen here (both sides are
        // sequentially consistent), so the wakeup is not lost.
        self.resume_below.store(limit, Ordering::SeqCst);
        if self.queued_bytes.load(Ordering::SeqCst) < limit {
            self.resume_below.store(0, Ordering::SeqCst);
            return true;
        }
        false
    }

    /// Routes the partition's data to its own queue from now on.
    pub(super) fn set_split(&self) {
        // Under the state lock, so that a concurrent push sees either the old
        // or the new routing, and its notification matches.
        let _state = self.lock_state();
        self.split.store(true, Ordering::Release);
    }

    /// (Re)starts fetching at `offset` (raw), discarding everything queued
    /// and resetting the position, as a librdkafka (re)assign does.
    pub(super) fn start(&self, offset: i64, max_bytes: i32) {
        let mut reader = self.reader.lock().expect("lock poisoned");
        let mut state = self.lock_state();
        self.reset_locked(&mut reader, &mut state);
        let fetch = &mut state.fetch;
        fetch.max_bytes = max_bytes;
        if offset >= 0 {
            fetch.phase = FetchPhase::Active;
            fetch.offset = offset;
        } else {
            fetch.phase = FetchPhase::Resolve(offset);
            fetch.offset = OFFSET_INVALID;
        }
        drop(state);
        self.stored.store(OFFSET_INVALID, Ordering::Release);
    }

    /// Stops fetching and discards everything queued.
    pub(super) fn stop(&self) {
        let mut reader = self.reader.lock().expect("lock poisoned");
        let mut state = self.lock_state();
        self.reset_locked(&mut reader, &mut state);
        state.fetch.phase = FetchPhase::Idle;
    }

    fn reset_locked(&self, reader: &mut Option<Cursor>, state: &mut PartState) {
        *reader = None;
        state.version += 1;
        state.items.clear();
        let fetch = &mut state.fetch;
        fetch.offset = OFFSET_INVALID;
        fetch.leader_epoch = -1;
        fetch.in_flight = None;
        fetch.resolving = false;
        fetch.backoff_until = None;
        fetch.resolve_failures = 0;
        fetch.eof_offset = OFFSET_INVALID;
        fetch.last_error = None;
        fetch.target = 0;
        fetch.drain_rate = 0;
        fetch.drain_mark = None;
        fetch.drained_at = None;
        self.queued_bytes.store(0, Ordering::SeqCst);
        self.queued_msgs.store(0, Ordering::Release);
        self.resume_below.store(0, Ordering::SeqCst);
        self.position.store(OFFSET_INVALID, Ordering::Release);
    }

    /// Notifies waiters after [`PartState::push`], with no lock held.
    pub(super) fn notify(self: &Arc<Self>, inner: &ConsumerInner, wake: Wake) {
        if wake.waiters {
            self.cv.notify_all();
        }
        if wake.main {
            inner.main_ready(Arc::clone(self));
        } else if wake.edge && self.is_split() {
            let f = self
                .callback
                .lock()
                .expect("lock poisoned")
                .as_ref()
                .map(|c| Arc::clone(&c.f));
            if let Some(f) = f {
                f();
            }
        }
    }

    /// Installs the nonempty callback of queue handle `owner`.
    pub(super) fn set_callback(&self, owner: u64, f: Arc<dyn Fn() + Send + Sync>) {
        let old = self
            .callback
            .lock()
            .expect("lock poisoned")
            .replace(Callback { owner, f });
        // Dropped outside the lock: the closure may own arbitrary state.
        drop(old);
    }

    /// Removes the nonempty callback if queue handle `owner` installed it.
    pub(super) fn clear_callback(&self, owner: u64) {
        let mut callback = self.callback.lock().expect("lock poisoned");
        let old = if callback.as_ref().is_some_and(|c| c.owner == owner) {
            callback.take()
        } else {
            None
        };
        drop(callback);
        drop(old);
    }

    /// Serves the next message or error without blocking.
    ///
    /// `from_main` is set when serving the main queue, which stops serving a
    /// partition once it is split off.
    pub(super) fn poll_once(&self, inner: &ConsumerInner, from_main: bool) -> Option<Served> {
        let mut reader = self.reader.lock().expect("lock poisoned");
        loop {
            if from_main && self.is_split() {
                self.lock_state().in_main = false;
                return None;
            }
            if let Some(cursor) = reader.as_mut() {
                let mut progress = Progress::default();
                let step = cursor.next(&mut progress);
                self.apply(inner, &progress);
                match step {
                    Step::Record(record) => return Some(Served::Message(Ok(self.message(record)))),
                    Step::Error(e) => return Some(Served::Message(Err(e))),
                    Step::Done => *reader = None,
                }
            }
            let item = {
                let mut state = self.lock_state();
                match state.items.pop_front() {
                    Some(item) => item,
                    None => {
                        if from_main {
                            state.in_main = false;
                        }
                        return None;
                    }
                }
            };
            match item {
                Item::Chunk(chunk) => {
                    *reader = Some(Cursor::new(
                        chunk,
                        inner.read_committed(),
                        inner.settings().consumer.check_crcs,
                    ));
                }
                Item::Error(e) => return Some(Served::Message(Err(e))),
                Item::Eof => return Some(Served::Message(Err(KafkaError::PartitionEOF(self.id)))),
                Item::Notice(code, reason) => return Some(Served::Notice(code, reason)),
            }
        }
    }

    /// Serves the next message or error of a split queue, waiting up to
    /// `deadline`.
    pub(super) fn poll(
        &self,
        inner: &ConsumerInner,
        deadline: Deadline,
    ) -> Option<KafkaResult<BorrowedMessage<'static>>> {
        loop {
            match self.poll_once(inner, false) {
                Some(Served::Message(result)) => return Some(result),
                Some(Served::Notice(code, reason)) => {
                    inner.hooks().error(KafkaError::Global(code), &reason);
                    continue;
                }
                None => {}
            }
            let mut state = self.lock_state();
            if !state.items.is_empty() {
                continue;
            }
            let remaining = deadline.remaining_duration();
            if remaining.is_some_and(|r| r.is_zero()) {
                return None;
            }
            state.waiters += 1;
            let mut state = match remaining {
                Some(r) => self.cv.wait_timeout(state, r).expect("lock poisoned").0,
                None => self.cv.wait(state).expect("lock poisoned"),
            };
            state.waiters -= 1;
        }
    }

    fn apply(&self, inner: &ConsumerInner, progress: &Progress) {
        if let Some(position) = progress.position {
            self.position.store(position, Ordering::Release);
            if inner.settings().consumer.enable_auto_offset_store {
                self.stored.store(position, Ordering::Release);
            }
        }
        if progress.bytes > 0 || progress.msgs > 0 {
            self.consumed.fetch_add(progress.bytes, Ordering::Release);
            let bytes = saturating_sub(&self.queued_bytes, progress.bytes);
            saturating_sub(&self.queued_msgs, progress.msgs);
            let resume_below = self.resume_below.load(Ordering::SeqCst);
            if resume_below != 0
                && bytes < resume_below
                && self.resume_below.swap(0, Ordering::SeqCst) != 0
            {
                inner.wake();
            }
            inner.queue_drained();
        }
    }

    fn message(&self, record: Record) -> BorrowedMessage<'static> {
        let timestamp = match (record.timestamp_type, record.timestamp) {
            (TimestampType::NotAvailable, _) | (_, -1) => Timestamp::NotAvailable,
            (TimestampType::CreateTime, t) => Timestamp::CreateTime(t),
            (TimestampType::LogAppendTime, t) => Timestamp::LogAppendTime(t),
        };
        let headers = if record.headers.is_empty() {
            None
        } else {
            let (count, entries) = record.headers.into_parts();
            // Like librdkafka, malformed headers read as no headers.
            BorrowedHeaders::from_entries(count, entries).ok().flatten()
        };
        BorrowedMessage::new(
            Arc::clone(&self.topic),
            self.id,
            record.offset,
            timestamp,
            record.key,
            record.value,
            headers,
        )
    }
}

/// Subtracts `amount` from `counter`, stopping at zero. Returns the new
/// value.
fn saturating_sub(counter: &AtomicU64, amount: u64) -> u64 {
    let mut current = counter.load(Ordering::SeqCst);
    loop {
        let new = current.saturating_sub(amount);
        match counter.compare_exchange_weak(current, new, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return new,
            Err(actual) => current = actual,
        }
    }
}
