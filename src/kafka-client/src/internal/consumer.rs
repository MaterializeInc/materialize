// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The consumer engine: fetcher, partition queues, offsets and group
//! membership.
//!
//! # Structure
//!
//! * [`Partition`] (in [`partition`]): one partition's fetch state, its queue
//!   of fetched record sets, and the application's read cursor and position.
//!   Partitions live in a registry for the life of the consumer.
//! * The main queue serves the partitions that are not split off, in the
//!   order their data arrived, plus consumer-level errors.
//! * The manager task ([`manage`]) resolves logical start offsets, starts a
//!   fetcher per partition leader, refreshes metadata for partitions without
//!   a leader, and auto-commits.
//! * Fetcher tasks ([`fetcher`]), one per leader broker, send one Fetch
//!   request at a time for all fetchable partitions the broker leads, and
//!   queue each partition's record set without decoding it.
//! * The group task ([`group`]) runs the classic consumer group protocol for
//!   [`ConsumerInner::subscribe`].
//!
//! Records are decoded only when polled, on the polling thread
//! ([`reader`]). All tasks run on the `kafka-io` runtime and stop when the
//! cluster shuts down.

mod assignor;
mod fetcher;
mod group;
mod offsets;
mod partition;
mod reader;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::Waker;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::RDKafkaLogLevel;
use crate::config::settings::{AutoOffsetReset, ClientSettings, IsolationLevel};
use crate::consumer::CommitMode;
use crate::error::{KafkaError, KafkaResult};
use crate::internal::cluster::{Cluster, MetadataSnapshot};
use crate::internal::hooks::{ConsumerHooks, Logger};
use crate::internal::runtime;
use crate::message::BorrowedMessage;
use crate::topic_partition_list::TopicPartitionList;
use crate::types::RDKafkaErrorCode;
use crate::util::{Deadline, Timeout};

pub(crate) use self::partition::Partition;
use self::partition::{FetchPhase, Item, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID};
use self::partition::{OFFSET_STORED, OFFSET_TAIL_BASE};

/// A partition in the current assignment.
#[derive(Clone, Debug)]
struct Assigned {
    partition: Arc<Partition>,
    /// The start offset it was assigned with, raw.
    offset: i64,
}

#[derive(Default)]
struct State {
    /// All partitions ever used, by topic and id.
    partitions: BTreeMap<String, BTreeMap<i32, Arc<Partition>>>,
    /// Sorted by topic and partition, like librdkafka's assignment.
    assignment: Vec<Assigned>,
    /// Brokers with a running fetcher task.
    fetchers: BTreeSet<i32>,
    group: Option<group::GroupHandle>,
}

#[derive(Default)]
struct MainQueue {
    /// Partitions with data that are not split off, in arrival order.
    ready: VecDeque<Arc<Partition>>,
    /// Consumer-level errors.
    events: VecDeque<KafkaError>,
    /// Wakers of `StreamConsumer` streams waiting for the main queue.
    wakers: Vec<Waker>,
    /// Threads waiting in a blocking poll.
    waiters: usize,
}

/// The state of one consumer, shared by its facade handles and its tasks.
pub(crate) struct ConsumerInner {
    cluster: Arc<Cluster>,
    hooks: Arc<dyn ConsumerHooks>,
    group_id: Option<String>,
    state: Mutex<State>,
    main: Mutex<MainQueue>,
    main_cv: Condvar,
    /// The partition the main queue is serving.
    main_current: Mutex<Option<Arc<Partition>>>,
    /// The length of `MainQueue::events`, readable without the lock.
    main_events: AtomicUsize,
    /// Bumped whenever fetchers and the manager should re-evaluate.
    wake: watch::Sender<u64>,
    /// A fetcher skipped a partition because its queue was full.
    throttled: AtomicBool,
    /// When the application last polled the main queue.
    last_poll: Mutex<Instant>,
    next_request_id: AtomicU64,
    next_queue_id: AtomicU64,
    shutdown: CancellationToken,
}

impl std::fmt::Debug for ConsumerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerInner")
            .field("cluster", &self.cluster)
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}

/// A partition split off into its own queue.
pub(crate) struct PartitionQueueInner {
    partition: Arc<Partition>,
    /// Identifies this handle's nonempty callback.
    id: u64,
}

impl ConsumerInner {
    /// Creates the engine and starts its manager task.
    pub(crate) fn new(cluster: Arc<Cluster>, hooks: Arc<dyn ConsumerHooks>) -> Arc<ConsumerInner> {
        let group_id = cluster.settings().consumer.group_id.clone();
        let shutdown = cluster.shutdown_token().child_token();
        let (wake, _) = watch::channel(0);
        let inner = Arc::new(ConsumerInner {
            cluster,
            hooks,
            group_id,
            state: Mutex::new(State::default()),
            main: Mutex::new(MainQueue::default()),
            main_cv: Condvar::new(),
            main_current: Mutex::new(None),
            main_events: AtomicUsize::new(0),
            wake,
            throttled: AtomicBool::new(false),
            last_poll: Mutex::new(Instant::now()),
            next_request_id: AtomicU64::new(1),
            next_queue_id: AtomicU64::new(1),
            shutdown,
        });
        // Client errors and statistics wait for the application to poll, as
        // on librdkafka's main queue; wake a waiting poll or stream for them.
        let weak = Arc::downgrade(&inner);
        inner.cluster.events().set_wakeup(move || {
            if let Some(inner) = weak.upgrade() {
                inner.notify_main(inner.lock_main());
            }
        });
        runtime::spawn("kafka-consumer-manager", manage(Arc::clone(&inner)));
        inner
    }

    pub(super) fn settings(&self) -> &Arc<ClientSettings> {
        self.cluster.settings()
    }

    /// Whether `group.id` is set.
    pub(crate) fn has_group(&self) -> bool {
        self.group_id.is_some()
    }

    pub(super) fn hooks(&self) -> &Arc<dyn ConsumerHooks> {
        &self.hooks
    }

    fn logger(&self) -> &Logger {
        self.cluster.logger()
    }

    pub(super) fn read_committed(&self) -> bool {
        self.settings().isolation_level == IsolationLevel::ReadCommitted
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("lock poisoned")
    }

    fn lock_main(&self) -> MutexGuard<'_, MainQueue> {
        self.main.lock().expect("lock poisoned")
    }

    /// Asks the fetchers and the manager to re-evaluate.
    fn wake(&self) {
        self.wake.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// Called by readers after consuming queued data: resumes fetchers that
    /// skipped full queues.
    fn queue_drained(&self) {
        if self.throttled.load(Ordering::Relaxed) && self.throttled.swap(false, Ordering::AcqRel) {
            self.wake();
        }
    }

    fn request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// The registered partition, created on first use.
    fn partition_locked(
        state: &mut State,
        settings: &ClientSettings,
        topic: &str,
        id: i32,
    ) -> Arc<Partition> {
        let partitions = match state.partitions.get_mut(topic) {
            Some(p) => p,
            None => state.partitions.entry(topic.to_string()).or_default(),
        };
        Arc::clone(partitions.entry(id).or_insert_with(|| {
            Arc::new(Partition::new(
                Arc::from(topic),
                id,
                settings.consumer.fetch_message_max_bytes,
            ))
        }))
    }

    /// The partitions of the current assignment.
    fn assigned_partitions(&self) -> Vec<Arc<Partition>> {
        self.lock_state()
            .assignment
            .iter()
            .map(|a| Arc::clone(&a.partition))
            .collect()
    }

    // Main queue.

    /// Adds a partition with new data to the main queue.
    fn main_ready(&self, partition: Arc<Partition>) {
        let mut main = self.lock_main();
        main.ready.push_back(partition);
        self.notify_main(main);
    }

    /// Queues a consumer-level error on the main queue.
    fn push_event(&self, error: KafkaError) {
        let mut main = self.lock_main();
        main.events.push_back(error);
        self.main_events.store(main.events.len(), Ordering::Release);
        self.notify_main(main);
    }

    fn notify_main(&self, mut main: MutexGuard<'_, MainQueue>) {
        let wakers = std::mem::take(&mut main.wakers);
        let waiters = main.waiters > 0;
        drop(main);
        if waiters {
            self.main_cv.notify_all();
        }
        for waker in wakers {
            waker.wake();
        }
    }

    /// Registers the waker of a stream that found the main queue empty.
    pub(crate) fn register_waker(&self, waker: &Waker) {
        let mut main = self.lock_main();
        if !main.wakers.iter().any(|w| w.will_wake(waker)) {
            main.wakers.push(waker.clone());
        }
    }

    /// Polls the main queue: messages and errors of partitions that are not
    /// split off, and consumer-level errors.
    ///
    /// Without `group.id`, returns `UnknownGroup` immediately, like
    /// librdkafka's `rd_kafka_consumer_poll`.
    ///
    /// Delivers pending client errors and statistics to the context first,
    /// and while waiting, like rust-rdkafka's `rd_kafka_poll(rk, 0)` calls.
    pub(crate) fn poll(&self, timeout: Timeout) -> Option<KafkaResult<BorrowedMessage<'static>>> {
        let events = self.cluster.events();
        events.serve();
        if self.group_id.is_none() {
            return Some(Err(KafkaError::MessageConsumption(
                RDKafkaErrorCode::UnknownGroup,
            )));
        }
        *self.last_poll.lock().expect("lock poisoned") = Instant::now();
        let deadline = Deadline::new(timeout);
        loop {
            if let Some(result) = self.poll_main_once() {
                return Some(result);
            }
            let mut main = self.lock_main();
            if !main.ready.is_empty() || !main.events.is_empty() {
                continue;
            }
            // Checked under the main lock, which the event queue's wakeup
            // takes before notifying, so that no wakeup is lost.
            if events.has_pending() {
                drop(main);
                events.serve();
                continue;
            }
            let remaining = deadline.remaining_duration();
            if remaining.is_some_and(|r| r.is_zero()) {
                return None;
            }
            main.waiters += 1;
            let mut main = match remaining {
                Some(r) => self.main_cv.wait_timeout(main, r).expect("lock poisoned").0,
                None => self.main_cv.wait(main).expect("lock poisoned"),
            };
            main.waiters -= 1;
        }
    }

    fn poll_main_once(&self) -> Option<KafkaResult<BorrowedMessage<'static>>> {
        let mut current = self.main_current.lock().expect("lock poisoned");
        loop {
            if self.main_events.load(Ordering::Acquire) > 0 {
                let mut main = self.lock_main();
                if let Some(e) = main.events.pop_front() {
                    self.main_events.store(main.events.len(), Ordering::Release);
                    return Some(Err(e));
                }
            }
            if let Some(partition) = current.as_ref() {
                if let Some(result) = partition.poll_once(self, true) {
                    return Some(result);
                }
                *current = None;
            }
            *current = Some(self.lock_main().ready.pop_front()?);
        }
    }

    // Assignment.

    /// Replaces the assignment, like `rd_kafka_assign`.
    ///
    /// NOTE: Unlike librdkafka, a partition that stays assigned with the same
    /// start offset keeps fetching where it is, with its queue, position and
    /// offset store, instead of restarting at the start offset. Materialize
    /// re-assigns all partitions with their original offsets whenever it adds
    /// one; restarting would refetch everything since the start offset only
    /// for the application to drop it again.
    pub(crate) fn assign(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        let err = |code: RDKafkaErrorCode| Err(KafkaError::Subscription(code.err2str()));
        if self.group_id.is_none() {
            return err(RDKafkaErrorCode::UnknownGroup);
        }
        let mut entries = Vec::with_capacity(tpl.count());
        for (i, elem) in tpl.elements().iter().enumerate() {
            let mut offset = tpl.raw_offset_at(i);
            let valid = offset >= 0
                || matches!(
                    offset,
                    OFFSET_BEGINNING | OFFSET_END | OFFSET_STORED | OFFSET_INVALID
                )
                || offset <= OFFSET_TAIL_BASE;
            if !valid || elem.partition() < 0 {
                return err(RDKafkaErrorCode::InvalidArgument);
            }
            // librdkafka reads INVALID as "use the committed offset".
            if offset == OFFSET_INVALID {
                offset = OFFSET_STORED;
            }
            entries.push((elem.topic().to_string(), elem.partition(), offset));
        }
        entries.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        if entries
            .windows(2)
            .any(|w| w[0].0 == w[1].0 && w[0].1 == w[1].1)
        {
            return err(RDKafkaErrorCode::InvalidArgument);
        }
        self.replace_assignment(entries, true);
        Ok(())
    }

    /// Replaces the assignment with `entries` (sorted by topic and
    /// partition). With `preserve`, partitions that stay assigned at the same
    /// start offset keep their state (see [`ConsumerInner::assign`]).
    fn replace_assignment(&self, entries: Vec<(String, i32, i64)>, preserve: bool) {
        let settings = Arc::clone(self.settings());
        let max_bytes = settings.consumer.fetch_message_max_bytes;
        let mut state = self.lock_state();
        let old = std::mem::take(&mut state.assignment);
        let old_offsets: BTreeMap<(&str, i32), (i64, &Arc<Partition>)> = old
            .iter()
            .map(|a| {
                (
                    (&*a.partition.topic, a.partition.id),
                    (a.offset, &a.partition),
                )
            })
            .collect();
        let mut new = Vec::with_capacity(entries.len());
        for (topic, id, offset) in &entries {
            let partition = Self::partition_locked(&mut state, &settings, topic, *id);
            let keep = preserve
                && old_offsets
                    .get(&(topic.as_str(), *id))
                    .is_some_and(|(old_offset, _)| *old_offset == *offset)
                && matches!(
                    partition.lock_state().fetch.phase,
                    FetchPhase::Active | FetchPhase::Resolve(_)
                );
            if !keep {
                partition.start(*offset, max_bytes);
            }
            new.push(Assigned {
                partition,
                offset: *offset,
            });
        }
        let new_keys: BTreeSet<(&str, i32)> =
            entries.iter().map(|(t, p, _)| (t.as_str(), *p)).collect();
        for (key, (_, partition)) in &old_offsets {
            if !new_keys.contains(key) {
                partition.stop();
            }
        }
        drop(old_offsets);
        let topics: BTreeSet<&str> = entries.iter().map(|(t, _, _)| t.as_str()).collect();
        self.cluster.add_known_topics(topics.iter().copied());
        state.assignment = new;
        drop(state);
        drop(old);
        self.wake();
    }

    /// The current assignment with the offsets it was assigned with.
    pub(crate) fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        if self.group_id.is_none() {
            return Err(KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownGroup));
        }
        let state = self.lock_state();
        let mut tpl = TopicPartitionList::with_capacity(state.assignment.len());
        for a in &state.assignment {
            tpl.push_entry(
                &a.partition.topic,
                a.partition.id,
                a.offset,
                String::new(),
                RDKafkaErrorCode::NoError,
            );
        }
        Ok(tpl)
    }

    /// The application positions of the assigned partitions.
    pub(crate) fn position(&self) -> KafkaResult<TopicPartitionList> {
        if self.group_id.is_none() {
            return Err(KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownGroup));
        }
        let state = self.lock_state();
        let mut tpl = TopicPartitionList::with_capacity(state.assignment.len());
        for a in &state.assignment {
            tpl.push_entry(
                &a.partition.topic,
                a.partition.id,
                a.partition.position(),
                String::new(),
                RDKafkaErrorCode::NoError,
            );
        }
        Ok(tpl)
    }

    /// Routes a partition's messages and errors to its own queue.
    pub(crate) fn split_partition_queue(
        &self,
        topic: &str,
        id: i32,
    ) -> Option<PartitionQueueInner> {
        if id < 0 || topic.contains('\0') {
            return None;
        }
        let partition = {
            let mut state = self.lock_state();
            Self::partition_locked(&mut state, self.settings(), topic, id)
        };
        partition.set_split();
        Some(PartitionQueueInner {
            partition,
            id: self.next_queue_id.fetch_add(1, Ordering::Relaxed),
        })
    }

    // Offsets.

    /// Stores `offset` (raw) as the next offset to commit for a partition.
    pub(crate) fn store_offset(&self, topic: &str, id: i32, offset: i64) -> KafkaResult<()> {
        let state = self.lock_state();
        let Some(partition) = state.partitions.get(topic).and_then(|p| p.get(&id)) else {
            return Err(KafkaError::StoreOffset(RDKafkaErrorCode::UnknownPartition));
        };
        if !state
            .assignment
            .iter()
            .any(|a| Arc::ptr_eq(&a.partition, partition))
        {
            return Err(KafkaError::StoreOffset(RDKafkaErrorCode::State));
        }
        partition.store(offset);
        Ok(())
    }

    /// Commits offsets to the group, like `rd_kafka_commit`.
    pub(crate) fn commit(&self, tpl: &TopicPartitionList, mode: CommitMode) -> KafkaResult<()> {
        if self.group_id.is_none() {
            return Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::UnknownGroup));
        }
        let offsets: Vec<offsets::CommitEntry> = tpl
            .elements()
            .iter()
            .enumerate()
            .filter(|(i, _)| tpl.raw_offset_at(*i) >= 0)
            .map(|(i, e)| offsets::CommitEntry {
                topic: e.topic().to_string(),
                partition: e.partition(),
                offset: tpl.raw_offset_at(i),
                metadata: e.metadata().to_string(),
            })
            .collect();
        self.commit_entries(offsets, mode)
    }

    /// Commits the offset store of the assigned partitions, like
    /// `rd_kafka_commit` without offsets. Only offsets that changed since the
    /// last commit are committed; `NoOffset` if there are none.
    pub(crate) fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()> {
        if self.group_id.is_none() {
            return Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::UnknownGroup));
        }
        let offsets = self.stored_offsets();
        self.commit_entries(offsets, mode)
    }

    /// The changed offset store entries of the assigned partitions.
    fn stored_offsets(&self) -> Vec<offsets::CommitEntry> {
        self.lock_state()
            .assignment
            .iter()
            .filter_map(|a| {
                let stored = a.partition.stored();
                let committed = a.partition.committed.load(Ordering::Acquire);
                (stored >= 0 && stored != committed).then(|| offsets::CommitEntry {
                    topic: a.partition.topic.to_string(),
                    partition: a.partition.id,
                    offset: stored,
                    metadata: String::new(),
                })
            })
            .collect()
    }

    fn commit_entries(
        self: &ConsumerInner,
        entries: Vec<offsets::CommitEntry>,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        if entries.is_empty() {
            let result = Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset));
            self.hooks
                .commit_callback(result.clone(), &TopicPartitionList::new());
            return result;
        }
        let commit = self.commit_task(entries);
        match mode {
            CommitMode::Sync => runtime::block_on(commit),
            CommitMode::Async => {
                runtime::spawn("kafka-consumer-commit", commit);
                Ok(())
            }
        }
    }

    /// Builds the commit of `entries` with the current group membership. It
    /// records the committed offsets and calls the commit callback.
    fn commit_task(
        &self,
        entries: Vec<offsets::CommitEntry>,
    ) -> impl std::future::Future<Output = KafkaResult<()>> + Send + 'static {
        let cluster = Arc::clone(&self.cluster);
        let hooks = Arc::clone(&self.hooks);
        let group_id = self.group_id.clone().expect("checked by callers");
        let member = self.member_identity();
        let partitions: Vec<Option<Arc<Partition>>> = {
            let state = self.lock_state();
            entries
                .iter()
                .map(|e| {
                    state
                        .partitions
                        .get(&e.topic)
                        .and_then(|p| p.get(&e.partition))
                        .cloned()
                })
                .collect()
        };
        async move {
            let result =
                offsets::commit(&cluster, &group_id, &member, &entries, Deadline::never()).await;
            let mut tpl = TopicPartitionList::with_capacity(entries.len());
            let mut first_error = None;
            match &result {
                Ok(errors) => {
                    assert_eq!(errors.len(), entries.len());
                    for (i, entry) in entries.iter().enumerate() {
                        let (error, partition) = (&errors[i], &partitions[i]);
                        tpl.push_entry(
                            &entry.topic,
                            entry.partition,
                            entry.offset,
                            entry.metadata.clone(),
                            error.unwrap_or(RDKafkaErrorCode::NoError),
                        );
                        match error {
                            None => {
                                if let Some(p) = partition {
                                    p.committed.store(entry.offset, Ordering::Release);
                                }
                            }
                            Some(code) => {
                                first_error.get_or_insert(*code);
                            }
                        }
                    }
                }
                Err(code) => {
                    first_error = Some(*code);
                    for entry in &entries {
                        tpl.push_entry(
                            &entry.topic,
                            entry.partition,
                            entry.offset,
                            entry.metadata.clone(),
                            *code,
                        );
                    }
                }
            }
            let result = match first_error {
                None => Ok(()),
                Some(code) => Err(KafkaError::ConsumerCommit(code)),
            };
            hooks.commit_callback(result.clone(), &tpl);
            result
        }
    }

    /// The generation and member id to commit with: the group membership if
    /// subscribed, else generation -1 and an empty member id.
    fn member_identity(&self) -> offsets::MemberIdentity {
        self.lock_state()
            .group
            .as_ref()
            .map(|g| g.identity())
            .unwrap_or_default()
    }

    /// The committed offsets of the partitions in `tpl`, like
    /// `rd_kafka_committed`.
    pub(crate) fn committed_offsets(
        &self,
        mut tpl: TopicPartitionList,
        timeout: Timeout,
    ) -> KafkaResult<TopicPartitionList> {
        let Some(group_id) = self.group_id.clone() else {
            return Err(KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownGroup));
        };
        let items: Vec<(String, i32)> = tpl
            .elements()
            .iter()
            .map(|e| (e.topic().to_string(), e.partition()))
            .collect();
        let cluster = Arc::clone(&self.cluster);
        let require_stable = self.read_committed();
        let deadline = Deadline::new(timeout);
        let results = runtime::block_on(async move {
            offsets::fetch_committed(&cluster, &group_id, &items, require_stable, deadline).await
        })
        .map_err(KafkaError::MetadataFetch)?;
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok((offset, metadata)) => {
                    tpl.set_raw_offset_at(i, if offset < 0 { OFFSET_INVALID } else { offset });
                    tpl.set_metadata_at(i, metadata);
                }
                Err(code) => {
                    tpl.set_raw_offset_at(i, OFFSET_INVALID);
                    tpl.set_error_at(i, code);
                }
            }
        }
        Ok(tpl)
    }

    /// Looks up offsets by timestamp, like `rd_kafka_offsets_for_times`.
    pub(crate) fn offsets_for_times(
        &self,
        mut tpl: TopicPartitionList,
        timeout: Timeout,
    ) -> KafkaResult<TopicPartitionList> {
        if tpl.count() == 0 {
            return Err(KafkaError::MetadataFetch(RDKafkaErrorCode::InvalidArgument));
        }
        let items: Vec<offsets::ListOffsetsItem> = tpl
            .elements()
            .iter()
            .enumerate()
            .map(|(i, e)| offsets::ListOffsetsItem {
                topic: e.topic().to_string(),
                partition: e.partition(),
                timestamp: tpl.raw_offset_at(i),
            })
            .collect();
        let cluster = Arc::clone(&self.cluster);
        let deadline = Deadline::new(timeout);
        let isolation = self.settings().isolation_level;
        let results = runtime::block_on(async move {
            offsets::list_offsets(&cluster, isolation, &items, deadline).await
        });
        // Like librdkafka's `rd_kafka_offsets_for_times`: partitions without
        // a known leader are left out of the requests (and keep their input
        // value), failing the call with `UnknownPartition` only if no
        // partition has one; a leader that stays unavailable is a timeout;
        // any partition error fails the call and leaves the list unchanged.
        let mut lookup_error = None;
        let mut request_error = None;
        let mut any_leader = false;
        for result in &results {
            match result {
                Ok(_) => any_leader = true,
                Err(e) if e.leader_lookup => match e.code {
                    RDKafkaErrorCode::UnknownTopicOrPartition
                    | RDKafkaErrorCode::UnknownPartition => {}
                    RDKafkaErrorCode::BrokerTransportFailure => {
                        lookup_error.get_or_insert(RDKafkaErrorCode::AllBrokersDown);
                    }
                    RDKafkaErrorCode::LeaderNotAvailable
                    | RDKafkaErrorCode::TimedOutQueue
                    | RDKafkaErrorCode::OperationTimedOut => {
                        lookup_error.get_or_insert(RDKafkaErrorCode::OperationTimedOut);
                    }
                    code => {
                        lookup_error.get_or_insert(code);
                    }
                },
                Err(e) => {
                    any_leader = true;
                    request_error.get_or_insert(e.code);
                }
            }
        }
        if let Some(code) = lookup_error {
            return Err(KafkaError::MetadataFetch(code));
        }
        if !any_leader {
            return Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::UnknownPartition,
            ));
        }
        if let Some(code) = request_error {
            return Err(KafkaError::MetadataFetch(code));
        }
        for (i, result) in results.into_iter().enumerate() {
            if let Ok(offset) = result {
                tpl.set_raw_offset_at(i, offset);
            }
        }
        Ok(tpl)
    }

    // Groups.

    /// Subscribes to `topics`, joining the group.
    pub(crate) fn subscribe(self: &Arc<Self>, topics: &[&str]) -> KafkaResult<()> {
        let err = |code: RDKafkaErrorCode| Err(KafkaError::Subscription(code.err2str()));
        let Some(group_id) = self.group_id.clone() else {
            return err(RDKafkaErrorCode::UnknownGroup);
        };
        let mut topics: Vec<String> = topics.iter().map(|t| t.to_string()).collect();
        topics.sort();
        topics.dedup();
        if topics.is_empty() || topics.iter().any(|t| t.is_empty()) {
            return err(RDKafkaErrorCode::InvalidArgument);
        }
        if topics.iter().any(|t| t.starts_with('^')) {
            // Regex subscriptions are not supported.
            return err(RDKafkaErrorCode::InvalidArgument);
        }
        let previous = self.lock_state().group.take();
        if let Some(previous) = previous {
            runtime::block_on(previous.leave());
        }
        self.cluster
            .add_known_topics(topics.iter().map(String::as_str));
        let handle = group::GroupHandle::start(Arc::clone(self), group_id, topics);
        self.lock_state().group = Some(handle);
        Ok(())
    }

    /// Leaves the group and, with `enable.auto.commit`, commits the offset
    /// store first, waiting at most a few seconds. Called when the consumer
    /// is dropped.
    pub(crate) fn close(&self) {
        if runtime::is_io_thread() {
            return;
        }
        let group = self.lock_state().group.take();
        let auto_commit = self.group_id.is_some() && self.settings().consumer.enable_auto_commit;
        let commit = if auto_commit {
            let offsets = self.stored_offsets();
            (!offsets.is_empty()).then(|| self.commit_task(offsets))
        } else {
            None
        };
        if group.is_none() && commit.is_none() {
            return;
        }
        let timeout = self.settings().socket_timeout.min(Duration::from_secs(5));
        runtime::block_on(async move {
            let close = async move {
                if let Some(commit) = commit {
                    let _ = commit.await;
                }
                if let Some(group) = group {
                    group.leave().await;
                }
            };
            let _ = tokio::time::timeout(timeout, close).await;
        });
    }

    /// Logs a line as librdkafka's main thread would.
    fn log(&self, level: RDKafkaLogLevel, fac: &str, args: std::fmt::Arguments<'_>) {
        self.logger()
            .log_fmt(level, fac, format_args!("[thrd:main]: {args}"));
    }

    /// When the application last polled the main queue.
    fn last_poll(&self) -> Instant {
        *self.last_poll.lock().expect("lock poisoned")
    }
}

impl PartitionQueueInner {
    pub(crate) fn poll(
        &self,
        inner: &ConsumerInner,
        timeout: Timeout,
    ) -> Option<KafkaResult<BorrowedMessage<'static>>> {
        self.partition.poll(inner, Deadline::new(timeout))
    }

    pub(crate) fn set_nonempty_callback(&self, f: Arc<dyn Fn() + Send + Sync>) {
        self.partition.set_callback(self.id, f);
    }
}

impl Drop for PartitionQueueInner {
    fn drop(&mut self) {
        self.partition.clear_callback(self.id);
    }
}

/// Where a partition's leader stands in the metadata.
enum Leader {
    Broker(i32),
    /// The topic is not in the metadata yet, or the partition has no leader.
    Unknown,
    /// The topic or partition does not exist.
    Missing(RDKafkaErrorCode),
}

fn leader_of(md: &MetadataSnapshot, partition: &Partition) -> Leader {
    let Some(topic) = md.topic(&partition.topic) else {
        return Leader::Unknown;
    };
    match topic.error {
        Some(code @ RDKafkaErrorCode::UnknownTopicOrPartition) => return Leader::Missing(code),
        Some(_) => return Leader::Unknown,
        None => {}
    }
    match topic.partition(partition.id) {
        Some(p) if p.leader >= 0 => Leader::Broker(p.leader),
        Some(_) => Leader::Unknown,
        None => Leader::Missing(RDKafkaErrorCode::UnknownPartition),
    }
}

/// The manager task: offset resolution, fetcher tasks, metadata refreshes
/// for partitions without a leader, missing-topic notices and auto-commit.
async fn manage(inner: Arc<ConsumerInner>) {
    let shutdown = inner.shutdown.clone();
    let mut wake_rx = inner.wake.subscribe();
    let mut md_rx = inner.cluster.subscribe_metadata();
    let settings = Arc::clone(inner.settings());
    let fast = settings.metadata_refresh_fast_interval;
    let mut refresh_backoff = fast;
    let mut next_refresh: Option<Instant> = None;
    // Topics seen missing, and since when: like librdkafka, missing topics
    // are reported only after `topic.metadata.propagation.max.ms`.
    let mut missing_since: BTreeMap<String, Instant> = BTreeMap::new();
    // Assigned partitions seen with a leader, which are reported as no
    // longer available when they disappear.
    let mut existed: BTreeSet<(String, i32)> = BTreeSet::new();
    let mut noticed_version = 0;
    let auto_commit = settings.consumer.enable_auto_commit && inner.group_id.is_some();
    let auto_commit_interval = settings.consumer.auto_commit_interval;
    let mut next_auto_commit = (auto_commit && !auto_commit_interval.is_zero())
        .then(|| Instant::now() + auto_commit_interval);
    loop {
        wake_rx.borrow_and_update();
        let md = Arc::clone(&md_rx.borrow_and_update());
        let now = Instant::now();
        let mut next_timer = next_auto_commit;
        let mut leaderless = false;
        let mut to_resolve = Vec::new();
        let notice = md.version != noticed_version;
        noticed_version = md.version;
        for partition in inner.assigned_partitions() {
            match leader_of(&md, &partition) {
                Leader::Broker(id) => {
                    if !existed.contains(&(partition.topic.to_string(), partition.id)) {
                        existed.insert((partition.topic.to_string(), partition.id));
                    }
                    let new = inner.lock_state().fetchers.insert(id);
                    if new {
                        runtime::spawn(
                            "kafka-consumer-fetcher",
                            fetcher::run(Arc::clone(&inner), id),
                        );
                    }
                    missing_since.remove(&*partition.topic);
                }
                Leader::Unknown => leaderless = true,
                Leader::Missing(code) => {
                    leaderless = true;
                    let since = *missing_since
                        .entry(partition.topic.to_string())
                        .or_insert(now);
                    if notice && now.duration_since(since) >= settings.metadata_propagation_max {
                        // Like librdkafka: a partition that disappears is
                        // first reported as no longer available, then, like
                        // one that never existed, on every metadata update.
                        let mut notices = Vec::new();
                        if existed.remove(&(partition.topic.to_string(), partition.id)) {
                            notices.push((
                                RDKafkaErrorCode::UnknownPartition,
                                "desired partition is no longer available",
                            ));
                        }
                        notices.push(match code {
                            RDKafkaErrorCode::UnknownPartition => {
                                (code, "desired partition is not available")
                            }
                            _ => (code, "topic does not exist"),
                        });
                        for (code, reason) in notices {
                            let text = format!(
                                "{} [{}]: {reason} ({})",
                                partition.topic,
                                partition.id,
                                code.err2str()
                            );
                            let mut state = partition.lock_state();
                            if state.fetch.phase == FetchPhase::Idle {
                                break;
                            }
                            let wake = state.push_notice(code, text, partition.is_split());
                            drop(state);
                            if let Some(wake) = wake {
                                partition.notify(&inner, wake);
                            }
                        }
                    }
                }
            }
            let mut state = partition.lock_state();
            if let FetchPhase::Resolve(target) = state.fetch.phase {
                if !state.fetch.resolving {
                    match state.fetch.backoff_until {
                        Some(at) if at > now => {
                            next_timer = Some(next_timer.map_or(at, |t| t.min(at)));
                        }
                        _ => {
                            state.fetch.resolving = true;
                            to_resolve.push((Arc::clone(&partition), state.version, target));
                        }
                    }
                }
            }
        }
        if !to_resolve.is_empty() {
            runtime::spawn(
                "kafka-consumer-resolve",
                offsets::resolve(Arc::clone(&inner), to_resolve),
            );
        }
        if leaderless {
            match next_refresh {
                Some(at) if at > now => {}
                _ => {
                    inner.cluster.request_metadata_refresh();
                    next_refresh = Some(now + refresh_backoff);
                    refresh_backoff =
                        (refresh_backoff * 2).min(settings.retry_backoff_max.max(fast));
                }
            }
            if let Some(at) = next_refresh {
                next_timer = Some(next_timer.map_or(at, |t| t.min(at)));
            }
        } else {
            refresh_backoff = fast;
            next_refresh = None;
        }
        if let Some(at) = next_auto_commit {
            if at <= now {
                let offsets = inner.stored_offsets();
                if !offsets.is_empty() {
                    runtime::spawn("kafka-consumer-auto-commit", {
                        let commit = inner.commit_task(offsets);
                        async move {
                            let _ = commit.await;
                        }
                    });
                }
                let at = now + auto_commit_interval;
                next_auto_commit = Some(at);
                next_timer = Some(next_timer.map_or(at, |t| t.min(at)));
            }
        }
        let sleep = async {
            match next_timer {
                Some(at) => tokio::time::sleep_until(at.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = shutdown.cancelled() => return,
            r = wake_rx.changed() => if r.is_err() { return },
            r = md_rx.changed() => if r.is_err() { return },
            _ = sleep => {}
        }
    }
}

/// Applies `auto.offset.reset` to a partition whose offset is unusable:
/// fetching resumes at the earliest or latest offset, or stops with an
/// `AutoOffsetReset` consumer error. `state` must be for `partition`.
///
/// `err_offset` is the offset that failed, `code` why, and `broker` the
/// broker that reported it (-1 for none). Errors other than "no committed
/// offset" are logged as warnings, like librdkafka's `rd_kafka_offset_reset`.
fn offset_reset(
    inner: &ConsumerInner,
    partition: &Arc<Partition>,
    state: &mut partition::PartState,
    broker: i32,
    err_offset: i64,
    code: RDKafkaErrorCode,
    reason: &str,
) -> Option<partition::Wake> {
    let mut wake = None;
    let mut extra = "";
    let target = match inner.settings().consumer.auto_offset_reset {
        AutoOffsetReset::Error => {
            state.fetch.phase = FetchPhase::Stopped;
            wake = Some(state.push(
                Item::Error(KafkaError::MessageConsumption(
                    RDKafkaErrorCode::AutoOffsetReset,
                )),
                partition.is_split(),
            ));
            OFFSET_INVALID
        }
        AutoOffsetReset::Earliest if state.fetch.lo_offset >= 0 => {
            extra = "cached BEGINNING offset ";
            state.fetch.phase = FetchPhase::Active;
            state.fetch.offset = state.fetch.lo_offset;
            state.fetch.lo_offset
        }
        AutoOffsetReset::Earliest => {
            state.fetch.phase = FetchPhase::Resolve(OFFSET_BEGINNING);
            OFFSET_BEGINNING
        }
        AutoOffsetReset::Latest => {
            state.fetch.phase = FetchPhase::Resolve(OFFSET_END);
            OFFSET_END
        }
    };
    if code != RDKafkaErrorCode::NoOffset && target != OFFSET_INVALID {
        inner.log(
            RDKafkaLogLevel::Warning,
            "OFFSET",
            format_args!(
                "{} [{}]: offset reset (at {}, broker {broker}) to {extra}{}: {reason}: {}",
                partition.topic,
                partition.id,
                fetch_pos_str(err_offset, state.fetch.leader_epoch),
                fetch_pos_str(target, -1),
                code.err2str(),
            ),
        );
    }
    wake
}

/// librdkafka's `rd_kafka_offset2str`.
fn offset_str(offset: i64) -> String {
    match offset {
        o if o >= 0 => o.to_string(),
        OFFSET_BEGINNING => "BEGINNING".into(),
        OFFSET_END => "END".into(),
        OFFSET_STORED => "STORED".into(),
        OFFSET_INVALID => "INVALID".into(),
        o if o <= OFFSET_TAIL_BASE => format!("TAIL({})", OFFSET_TAIL_BASE - o),
        o => format!("{o}?"),
    }
}

/// librdkafka's `rd_kafka_fetch_pos2str`.
fn fetch_pos_str(offset: i64, leader_epoch: i32) -> String {
    format!(
        "offset {} (leader epoch {leader_epoch})",
        offset_str(offset)
    )
}
