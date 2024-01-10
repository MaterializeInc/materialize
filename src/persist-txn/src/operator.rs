// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timely operators for the crate

use std::fmt::Debug;
use std::future::Future;
use std::pin::pin;
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_persist_client::operators::shard_source::{shard_source, SnapshotMode};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use mz_persist_types::{Codec, Codec64, StepForward};
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::EventCore;
use timely::dataflow::operators::{
    Broadcast, Capability, CapabilitySet, Capture, Leave, Map, Probe,
};
use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::Scheduler;
use timely::worker::Worker;
use timely::{Data, WorkerConfig};
use tokio::time::Duration;
use tracing::{debug, trace};

use crate::txn_cache::{TxnsCache, TxnsCacheState};
use crate::txn_read::DataListenNext;
use crate::{TxnsCodec, TxnsCodecDefault, TxnsEntry};

/// An operator for translating physical data shard frontiers into logical ones.
///
/// A data shard in the txns set logically advances its upper each time a txn is
/// committed, but the upper is not physically advanced unless that data shard
/// was involved in the txn. This means that a shard_source (or any read)
/// pointed at a data shard would appear to stall at the time of the most recent
/// write. We fix this for shard_source by flowing its output through a new
/// `txns_progress` dataflow operator, which ensures that the
/// frontier/capability is advanced as the txns shard progresses, as long as the
/// shard_source is up to date with the latest committed write to that data
/// shard.
///
/// Example:
///
/// - A data shard has most recently been written to at 3.
/// - The txns shard's upper is at 6.
/// - We render a dataflow containing a shard_source with an as_of of 5.
/// - A txn NOT involving the data shard is committed at 7.
/// - A txn involving the data shard is committed at 9.
///
/// How it works:
///
/// - The shard_source operator is rendered. Its single output is hooked up as a
///   _disconnected_ input to txns_progress. The txns_progress single output is
///   a stream of the same type, which is used by downstream operators. This
///   txns_progress operator is targeted at one data_shard; rendering a
///   shard_source for a second data shard requires a second txns_progress
///   operator.
/// - The shard_source operator emits data through 3 and advances the frontier.
/// - The txns_progress operator passes through these writes and frontier
///   advancements unchanged. (Recall that it's always correct to read a data
///   shard "normally", it just might stall.) Because the txns_progress operator
///   knows there are no writes in `[3,5]`, it then downgrades its own
///   capability past 5 (to 6). Because the input is disconnected, this means
///   the overall frontier of the output is downgraded to 6.
/// - The txns_progress operator learns about the write at 7 (the upper is now
///   8). Because it knows that the data shard was not involved in this, it's
///   free to downgrade its capability to 8.
/// - The txns_progress operator learns about the write at 9 (the upper is now
///   10). It knows that the data shard _WAS_ involved in this, so it forwards
///   on data from its input until the input has progressed to 10, at which
///   point it can itself downgrade to 10.
pub fn txns_progress<K, V, T, D, P, C, F, G>(
    passthrough: Stream<G, P>,
    name: &str,
    client_fn: impl Fn() -> F,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
) -> (Stream<G, P>, Vec<PressOnDropButton>)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Data + Semigroup + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec,
    F: Future<Output = PersistClient> + Send + 'static,
    G: Scope<Timestamp = T>,
{
    let unique_id = (name, passthrough.scope().addr()).hashed();
    let (txns, source_button) = txns_progress_source::<K, V, T, D, P, C, G>(
        passthrough.scope(),
        name,
        client_fn(),
        txns_id,
        data_id,
        unique_id,
    );
    // Each of the `txns_frontiers` workers wants the full copy of the txns
    // shard (modulo filtered for data_id).
    let txns = txns.broadcast();
    let (passthrough, frontiers_button) = txns_progress_frontiers::<K, V, T, D, P, C, G>(
        txns,
        passthrough,
        name,
        client_fn(),
        txns_id,
        data_id,
        as_of,
        data_key_schema,
        data_val_schema,
        unique_id,
    );
    (passthrough, vec![source_button, frontiers_button])
}

fn txns_progress_source<K, V, T, D, P, C, G>(
    scope: G,
    name: &str,
    client: impl Future<Output = PersistClient> + 'static,
    txns_id: ShardId,
    data_id: ShardId,
    unique_id: u64,
) -> (Stream<G, (TxnsEntry, T, i64)>, PressOnDropButton)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Data + Semigroup + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec,
    G: Scope<Timestamp = T>,
{
    let worker_idx = scope.index();
    let chosen_worker = usize::cast_from(name.hashed()) % scope.peers();
    let name = format!("txns_progress_source({}) [{}]", name, unique_id);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), scope);
    let name = format!("{} [{}]", name, unique_id);
    let (mut txns_output, txns_stream) = builder.new_output();

    let shutdown_button = builder.build(move |capabilities| async move {
        if worker_idx != chosen_worker {
            return;
        }

        let [mut cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let client = client.await;
        let mut txns_subscribe = {
            let cache = TxnsCache::<T, C>::open(&client, txns_id, Some(data_id)).await;
            assert!(cache.buf.is_empty());
            cache.txns_subscribe
        };
        loop {
            let events = txns_subscribe.next(None).await;
            for event in events {
                let parts = match event {
                    ListenEvent::Progress(frontier) => {
                        let progress = frontier
                            .into_option()
                            .expect("nothing should close the txns shard");
                        debug!("{} emitting progress {:?}", name, progress);
                        cap.downgrade(&progress);
                        continue;
                    }
                    ListenEvent::Updates(parts) => parts,
                };
                let mut updates = Vec::new();
                TxnsCache::<T, C>::fetch_parts(
                    Some(data_id),
                    &mut txns_subscribe,
                    parts,
                    &mut updates,
                )
                .await;
                if !updates.is_empty() {
                    debug!("{} emitting updates {:?}", name, updates);
                    txns_output.give_container(&cap, &mut updates).await;
                }
            }
        }
    });
    (txns_stream, shutdown_button.press_on_drop())
}

fn txns_progress_frontiers<K, V, T, D, P, C, G>(
    txns: Stream<G, (TxnsEntry, T, i64)>,
    passthrough: Stream<G, P>,
    name: &str,
    client: impl Future<Output = PersistClient> + 'static,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
    unique_id: u64,
) -> (Stream<G, P>, PressOnDropButton)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Data + Semigroup + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec,
    G: Scope<Timestamp = T>,
{
    let name = format!("txns_progress_frontiers({}) [{}]", name, unique_id);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), passthrough.scope());
    let name = format!(
        "{} {}/{}",
        name,
        passthrough.scope().index(),
        passthrough.scope().peers(),
    );
    let (mut passthrough_output, passthrough_stream) = builder.new_output();
    let mut txns_input = builder.new_input_connection(&txns, Pipeline, vec![Antichain::new()]);
    let mut passthrough_input =
        builder.new_input_connection(&passthrough, Pipeline, vec![Antichain::new()]);

    let (worker_idx, num_workers) = (passthrough.scope().index(), passthrough.scope().peers());
    let shutdown_button = builder.build(move |capabilities| async move {
        let [cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let mut cap = CapabilitySet::from_elem(cap);

        let client = timeout_log(client, "client").await;

        let state = TxnsCacheState::new(txns_id, T::minimum(), Some(data_id));
        let cache_name = format!(
            "{} {:.9} {}/{}",
            name,
            data_id.to_string(),
            worker_idx,
            num_workers
        );
        let mut txns_cache = TxnsCacheTimely {
            name: cache_name,
            state,
            buf: Vec::new(),
        };

        while let Some(txns_event) = txns_input.next_mut().await {
            txns_cache.push_event(txns_event);
            if txns_cache.ready_gt(&as_of) {
                break;
            }
        }

        let snap = txns_cache.state.data_snapshot(data_id, as_of.clone());
        let data_write = client.open_writer::<K, V, T, D>(
            data_id,
            Arc::clone(&data_key_schema),
            Arc::clone(&data_val_schema),
            Diagnostics::from_purpose("data read physical upper"),
        );
        let data_write = timeout_log(data_write, "client.open_writer(..)")
            .await
            .expect("schema shouldn't change");
        let empty_to = snap.unblock_read(data_write);
        let empty_to = timeout_log(empty_to, "snap.unblock_read(data_write)").await;
        debug!(
            "{} {:.9} {}/{} starting as_of={:?} empty_to={:?}",
            name,
            data_id.to_string(),
            worker_idx,
            num_workers,
            as_of,
            empty_to.elements()
        );

        // When our state machine tells us that there is an oustanding read we
        // stash a token for it here, which locks in place a capability as of
        // the logical time at which we learned of that outstanding read from
        // the state machine. Only when the frontier of the data stream advances
        // beyond the given target timestamp do we remove stashed outstanding
        // reads, which drops the capability which will eventually allow the
        // downstream frontier to advance.
        let mut outstanding_reads = Vec::new();

        // We likely have an initial outstanding read.
        if let Some(empty_to) = empty_to.as_option() {
            debug!(
                empty_to = ?empty_to,
                as_of = ?as_of,
                fallback_cap = ?cap.first().unwrap().time(),
                "initial outstanding read, up to empty_to!");

            let outstanding_read = OutstandingRead {
                cap: cap.delayed(&as_of),
                target_ts: empty_to.clone(),
            };

            outstanding_reads.push(outstanding_read);
        } else {
            debug!(
                empty_to = ?empty_to,
                as_of = ?as_of,
                fallback_cap = ?cap.first().unwrap().time(),
                "initial outstanding read, up to as_of!");

            let outstanding_read = OutstandingRead {
                cap: cap.delayed(&as_of),
                target_ts: as_of.clone(),
            };

            outstanding_reads.push(outstanding_read);
        }
        let mut output_progress_exclusive = T::minimum();

        loop {
            tokio::select! {
                Some(data_event) = passthrough_input.next_mut() => {
                    debug!(
                        data_event = ?data_event,
                        "data input"
                    );
                    match data_event {
                       Event::Data(_data_cap, data) => {
                           let ts = _data_cap.time();

                           // SUBTLE: We see if this is a data read that we have
                           // learned about from the txns state machine before.
                           // This is the case when the data timestamp is less
                           // than the target_ts of an outstanding read. If so,
                           // we use the capability that we stored with this
                           // outstanding read.
                           //
                           let valid_cap = outstanding_reads
                               .iter()
                               .filter(|read| ts < &read.target_ts)
                               .min_by_key(|read| &read.target_ts)
                               .map(|read| &read.cap);

                           // SUBTLE, continued: If this is not a read we have
                           // learned about before, we use the current output
                           // capability. Because this operator reads from both
                           // the data input and the txns input concurrently it
                           // can happen that a read shows up before we learn
                           // about it. This is not a problem, though, the
                           // output capability is valid, since all that
                           // learning about it from the txns input could do is
                           // downgrade the output capability some more and/or
                           // put in place an outstanding read that has a
                           // capability that is >= the current output
                           // capability.
                           let valid_cap = match valid_cap {
                               Some(valid_cap) => {
                                   debug!(
                                       ts = ?ts,
                                       as_of = ?as_of,
                                       outstanding_reads = ?outstanding_reads,
                                       fallback_cap = ?cap.first().unwrap().time(),
                                       "using cap from oustanding read: {:?}!", valid_cap.time());
                                   valid_cap
                               },
                               None => {
                                   let valid_cap = cap.first().expect("no output cap");
                                   debug!(
                                       ts = ?ts,
                                       as_of = ?as_of,
                                       outstanding_reads = ?outstanding_reads,
                                       fallback_cap = ?cap.first().unwrap().time(),
                                       "no cap from outstanding read cap! using fallback cap {:?}", valid_cap.time());
                                   valid_cap
                               }
                           };

                           passthrough_output.give_container(valid_cap, data).await;
                       }
                       Event::Progress(progress) => {
                           // We reached the empty frontier! Shut down.
                           let Some(input_progress_exclusive) = progress.as_option() else {
                               break;
                           };

                           // Recall that any reads of the data shard are always
                           // correct, so given that we've passed through any data
                           // from the input, that means we're free to pass through
                           // frontier updates too.
                           if &output_progress_exclusive < input_progress_exclusive {
                               output_progress_exclusive.clone_from(input_progress_exclusive);
                               debug!(
                                   "{} {:.9} {}/{} downgrading cap to {:?}, based on data progress",
                                   name,
                                   data_id.to_string(),
                                   worker_idx,
                                   num_workers,
                                   output_progress_exclusive
                               );
                               cap.downgrade(Antichain::from_elem(output_progress_exclusive.clone()));
                           }

                           debug!(
                               outstanding_reads = ?outstanding_reads,
                               input_progress_exclusive = ?input_progress_exclusive,
                               "{} {:.9} {}/{} retaining outstanding reads, before",
                               name,
                               data_id.to_string(),
                               worker_idx,
                               num_workers,
                           );
                           // Remove outstanding reads whose `target_ts` has now
                           // been surpassed. That is, we retain those
                           // outstanding reads for which this is not the case.
                           outstanding_reads.retain(|read| &read.target_ts > input_progress_exclusive);
                           debug!(
                               outstanding_reads = ?outstanding_reads,
                               input_progress_exclusive = ?input_progress_exclusive,
                               "{} {:.9} {}/{} retaining outstanding reads, after",
                               name,
                               data_id.to_string(),
                               worker_idx,
                               num_workers,
                           );
                       }
                    }
                }
                Some(txns_event) = txns_input.next_mut() => {
                    debug!(
                        txns_event = ?txns_event,
                        "txns input"
                    );
                    txns_cache.push_event(txns_event);
                }
            }


            if !txns_cache.ready_ge(&output_progress_exclusive) {
                debug!(
                    outstanding_reads = ?outstanding_reads,
                    state_progress = ?txns_cache.state.progress_exclusive,
                    output_progress_exclusive = ?output_progress_exclusive,
                    "{} {:.9} {}/{} txns_cache not yet ready",
                    name,
                    data_id.to_string(),
                    worker_idx,
                    num_workers,
                );
                // We're not yet ready to pull actions from the state
                // machine.
                continue;
            }

            txns_cache.compact_to(&output_progress_exclusive);

            // Read from the state machine until it tells us that we
            // have to wait for the txns frontier to advance.
            loop {
                let data_listen_next = txns_cache
                    .state
                    .data_listen_next(&data_id, output_progress_exclusive.clone());

                debug!(
                    "{} {:.9} {}/{} data_listen_next at ({:?}): {:?}",
                    name,
                    data_id.to_string(),
                    worker_idx,
                    num_workers,
                    output_progress_exclusive,
                    data_listen_next,
                );

                match data_listen_next {
                    // We've caught up to the txns upper and we have to wait for
                    // it to advance before asking again.
                    DataListenNext::WaitForTxnsProgress => {
                        // WIP: The previous implementation did a `update_gt`,
                        // reading from the txns input until the input frontier
                        // was >= output_progress_exclusive.
                        //
                        // Do we need that still? I think not:
                        //  - It's always safe to pass through data updates,
                        //  so the other select! branch is fine.
                        //  - We go into the txn select branch when we get
                        //  updates from the txn input, we push them into
                        //  the state machine, then do a `read_ge` check.
                        //
                        // The worst that can happen is that we push updates
                        // (both data and frontiers) into the state machine
                        // and it keeps telling us that we have to wait,
                        // i.e. we go into this branch again.
                        break;
                    }
                    DataListenNext::ReadDataTo(new_target) => {
                        // WIP: Is this correct? I think it mirrors the
                        // previous behaviour where, when we saw this, we
                        // switched over to reading from the data input,
                        // that is we were not downgrading out capability in
                        // reaction to any txn updates.
                        //
                        // Now, we achieve the same behaviour by minting a
                        // capability at our current frontier/cap, and we
                        // only drop that once the frontier of the data
                        // input advances past the `target_ts`.
                        let outstanding_read = OutstandingRead {
                            cap: cap.delayed(&output_progress_exclusive),
                            target_ts: new_target,
                        };
                        outstanding_reads.push(outstanding_read);

                        // Nothing else from the state machine for
                        // now. Give the operator a chance to ingest
                        // the outstanding read.
                        break;

                        // TODO: This is a very strong hint that the data shard
                        // is about to be written to. Because the data shard's
                        // upper advances sparsely (on write, but not on passage
                        // of time) which invalidates the "every 1s" assumption
                        // of the default tuning, we've had to de-tune the
                        // listen sleeps on the paired persist_source. Maybe we
                        // use "one state" to wake it up in case pubsub doesn't
                        // and remove the listen polling entirely? (NB: This
                        // would have to happen in each worker so that it's
                        // guaranteed to happen in each process.)
                    }
                    // We know there are no writes in
                    // `[output_progress_exclusive, new_progress)`, so advance
                    // our output frontier.
                    DataListenNext::EmitLogicalProgress(new_progress) => {
                        assert!(output_progress_exclusive < new_progress);
                        output_progress_exclusive = new_progress.clone();
                        trace!(
                            "{} {:.9} {}/{} downgrading cap to {:?}",
                            name,
                            data_id.to_string(),
                            worker_idx,
                            num_workers,
                            output_progress_exclusive
                        );
                        cap.downgrade(Antichain::from_elem(new_progress));
                    }
                    DataListenNext::CompactedTo(since_ts) => {
                        unreachable!(
                            "internal logic error: {} unexpectedly compacted past {:?} to {:?}",
                            data_id, output_progress_exclusive, since_ts
                        )
                    }
                }
            }
        }

        tracing::info!(
            "{} {:.9} {}/{} shutting down",
            name,
            data_id.to_string(),
            worker_idx,
            num_workers,
        );
    });
    (passthrough_stream, shutdown_button.press_on_drop())
}

/// Represents a read from the data/passthrough input that we are waiting to
/// observe. We stash these in operator state until we see the data input
/// frontier advancing past the `target_ts`. This "locks" in place a capability
/// at the time at which we recorded this outstanding read.
struct OutstandingRead<T: Timestamp> {
    /// A capability for the time at which we created this hold.
    cap: Capability<T>,
    /// The timestamp up to which we have to read until we release this "lock".
    target_ts: T,
}

impl<T: Debug + Timestamp> Debug for OutstandingRead<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutstandingRead")
            .field("cap", &self.cap.time())
            .field("target_ts", &self.target_ts)
            .finish()
    }
}

/// Logs the given message when the future times out, and then never returns.
async fn timeout_log<F, T, M>(future: F, msg: M) -> T
where
    F: Future<Output = T>,
    M: AsRef<str>,
{
    // // Very long timeout, to avoid firing when the cluster is just overloaded.
    // match tokio::time::timeout(Duration::from_secs(60 * 5), future).await {
    //     Ok(t) => t,
    //     Err(timeout) => {
    //         // Log the timeout and never return.
    //         tracing::error!("timeout elapsed, {}: {}", msg, timeout);
    //         std::future::pending().await
    //     }
    // }
    // Petros' alternative: retry/log continually.
    let mut fut = pin!(future);
    loop {
        match tokio::time::timeout(Duration::from_secs(2), fut.as_mut()).await {
            Ok(value) => break value,
            Err(_) => tracing::error!("timeout: {}, continue waiting ...", msg.as_ref()),
        }
    }
}

// NB: The API of this intentionally mirrors TxnsCache and TxnsRead. Consider
// making them all implement the same trait?
struct TxnsCacheTimely<T: Timestamp + Lattice + Codec64> {
    name: String,
    state: TxnsCacheState<T>,
    buf: Vec<(TxnsEntry, T, i64)>,
}

impl<T> TxnsCacheTimely<T>
where
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
{
    /// See [TxnsCacheState::compact_to].
    fn compact_to(&mut self, since_ts: &T) {
        self.state.compact_to(since_ts)
    }

    /// Has this [TxnsCacheTimely] caught up to `ts` (`>=`).
    fn ready_gt(&mut self, ts: &T) -> bool {
        debug!("{} ready_gt {:?}", self.name, ts);
        debug_assert_eq!(self.state.validate(), Ok(()));
        &self.state.progress_exclusive > ts
    }

    /// Has this [TxnsCacheTimely] caught up to `ts` (`>`).
    fn ready_ge(&mut self, ts: &T) -> bool {
        debug!("{} ready_ge {:?}", self.name, ts);
        debug_assert_eq!(self.state.validate(), Ok(()));
        &self.state.progress_exclusive >= ts
    }

    fn push_event(&mut self, event: Event<T, &mut Vec<(TxnsEntry, T, i64)>>) {
        match event {
            Event::Progress(frontier) => {
                let progress = frontier
                    .into_option()
                    .expect("nothing should close the txns shard");
                debug!("{} got progress {:?}", self.name, progress);
                self.state
                    .push_entries(std::mem::take(&mut self.buf), progress);
            }
            Event::Data(_cap, entries) => {
                debug!("{} got updates {:?}", self.name, entries);
                self.buf.append(entries);
            }
        }
        debug!(
            "{} cache correct before {:?} len={} least_ts={:?}",
            self.name,
            self.state.progress_exclusive,
            self.state.unapplied_batches.len(),
            self.state
                .unapplied_batches
                .first_key_value()
                .map(|(_, (_, _, ts))| ts),
        );
    }
}

/// A helper for subscribing to a data shard using the timely operators.
///
/// This could instead be a wrapper around a [Subscribe], but it's only used in
/// tests and maelstrom, so do it by wrapping the timely operators to get
/// additional coverage. For the same reason, hardcode the K, V, T, D types.
///
/// [Subscribe]: mz_persist_client::read::Subscribe
pub struct DataSubscribe {
    pub(crate) as_of: u64,
    pub(crate) worker: Worker<timely::communication::allocator::Thread>,
    data: ProbeHandle<u64>,
    txns: ProbeHandle<u64>,
    capture: mpsc::Receiver<EventCore<u64, Vec<(String, u64, i64)>>>,
    output: Vec<(String, u64, i64)>,

    _tokens: Vec<PressOnDropButton>,
}

impl std::fmt::Debug for DataSubscribe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let DataSubscribe {
            as_of,
            worker: _,
            data,
            txns,
            capture: _,
            output,
            _tokens: _,
        } = self;
        f.debug_struct("DataSubscribe")
            .field("as_of", as_of)
            .field("data", data)
            .field("txns", txns)
            .field("output", output)
            .finish_non_exhaustive()
    }
}

impl DataSubscribe {
    /// Creates a new [DataSubscribe].
    pub fn new(
        name: &str,
        client: PersistClient,
        txns_id: ShardId,
        data_id: ShardId,
        as_of: u64,
    ) -> Self {
        let mut worker = Worker::new(
            WorkerConfig::default(),
            timely::communication::allocator::Thread::new(),
        );
        let (data, txns, capture, tokens) = worker.dataflow::<u64, _, _>(|scope| {
            let (data_stream, shard_source_token) = scope.scoped::<u64, _, _>("hybrid", |scope| {
                let client = client.clone();
                let (data_stream, token) = shard_source::<String, (), u64, i64, _, _, _, _>(
                    scope,
                    name,
                    move || std::future::ready(client.clone()),
                    data_id,
                    Some(Antichain::from_elem(as_of)),
                    SnapshotMode::Include,
                    Antichain::new(),
                    false.then_some(|_, _: &_, _| unreachable!()),
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                    |_, _| true,
                    false.then_some(|| unreachable!()),
                );
                (data_stream.leave(), token)
            });
            let (mut data, mut txns) = (ProbeHandle::new(), ProbeHandle::new());
            let data_stream = data_stream.flat_map(|part| {
                part.map(|((k, v), t, d)| {
                    let (k, ()) = (k.unwrap(), v.unwrap());
                    (k, t, d)
                })
            });
            let data_stream = data_stream.probe_with(&mut data);
            let (data_stream, mut txns_progress_token) =
                txns_progress::<String, (), u64, i64, _, TxnsCodecDefault, _, _>(
                    data_stream,
                    name,
                    || std::future::ready(client.clone()),
                    txns_id,
                    data_id,
                    as_of,
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                );
            let data_stream = data_stream.probe_with(&mut txns);
            let mut tokens = shard_source_token;
            tokens.append(&mut txns_progress_token);
            (data, txns, data_stream.capture(), tokens)
        });
        Self {
            as_of,
            worker,
            data,
            txns,
            capture,
            output: Vec::new(),
            _tokens: tokens,
        }
    }

    /// Returns the exclusive progress of the dataflow.
    pub fn progress(&self) -> u64 {
        self.txns
            .with_frontier(|f| f.as_option().expect("txns is not closed").clone())
    }

    /// Steps the dataflow, capturing output.
    pub fn step(&mut self) {
        // TODO: The activations based stepping in `sub.step` should be enough,
        // but somehow it isn't in the `data_subscribe` unit test. The dataflow
        // gets woken up again by something (I think a semaphore) after
        // initially going to sleep. If this doesn't happen, we end up
        // deadlocking in the first call to register a writer.
        for _ in 0..1000 {
            self.worker.step();
        }
        // Step while any worker thinks it has work to do.
        while self.worker.activations().borrow().empty_for().is_some() {
            self.worker.step();
        }
        self.capture_output()
    }

    pub(crate) fn capture_output(&mut self) {
        loop {
            let event = match self.capture.try_recv() {
                Ok(x) => x,
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            };
            match event {
                EventCore::Progress(_) => {}
                EventCore::Messages(_, mut msgs) => self.output.append(&mut msgs),
            }
        }
    }

    /// Steps the dataflow past the given time, capturing output.
    #[cfg(test)]
    pub async fn step_past(&mut self, ts: u64) {
        while self.txns.less_equal(&ts) {
            trace!(
                "progress at {:?}",
                self.txns.with_frontier(|x| x.to_owned()).elements()
            );
            self.step();
            tokio::task::yield_now().await;
        }
    }

    /// Returns captured output.
    pub fn output(&self) -> &Vec<(String, u64, i64)> {
        &self.output
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::writer;
    use crate::txns::TxnsHandle;

    use super::*;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    #[ignore] // TODO(txn): Get this turned back on.
    async fn data_subscribe() {
        fn step(subs: &mut Vec<DataSubscribe>) {
            for sub in subs.iter_mut() {
                sub.step();
            }
        }

        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = ShardId::new();

        // Start a subscription before the shard gets registered.
        let mut subs = Vec::new();
        subs.push(txns.read_cache().expect_subscribe(&client, d0, 5));
        step(&mut subs);

        // Now register the shard. Also start a new subscription and step the
        // previous one (plus repeat this for ever later step).
        txns.register(1, [writer(&client, d0).await]).await.unwrap();
        subs.push(txns.read_cache().expect_subscribe(&client, d0, 5));
        step(&mut subs);

        // Now write something unrelated.
        let d1 = txns.expect_register(2).await;
        txns.expect_commit_at(3, d1, &["nope"], &log).await;
        subs.push(txns.read_cache().expect_subscribe(&client, d0, 5));
        step(&mut subs);

        // Now write to our shard before.
        txns.expect_commit_at(4, d0, &["4"], &log).await;
        subs.push(txns.read_cache().expect_subscribe(&client, d0, 5));
        step(&mut subs);

        // Now write to our shard at the as_of.
        txns.expect_commit_at(5, d0, &["5"], &log).await;
        subs.push(txns.read_cache().expect_subscribe(&client, d0, 5));
        step(&mut subs);

        // Now write to our shard past the as_of.
        txns.expect_commit_at(6, d0, &["6"], &log).await;
        subs.push(txns.read_cache().expect_subscribe(&client, d0, 5));
        step(&mut subs);

        // Now write something unrelated again.
        txns.expect_commit_at(7, d1, &["nope"], &log).await;
        subs.push(txns.read_cache().expect_subscribe(&client, d0, 5));
        step(&mut subs);

        // Verify that the dataflows can progress to the expected point.
        for sub in subs.iter_mut() {
            sub.step_past(7).await;
            assert_eq!(sub.progress(), 8);
        }

        // Now verify that we read the right thing no matter when the dataflow
        // started.
        for mut sub in subs {
            sub.step_past(7).await;
            log.assert_eq(d0, 5, 8, sub.output().clone());
        }
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn subscribe_shard_finalize() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = txns.expect_register(1).await;

        // Start the operator as_of the register ts.
        let mut sub = txns.read_cache().expect_subscribe(&client, d0, 1);
        sub.step_past(1).await;

        // Write to it via txns.
        txns.expect_commit_at(2, d0, &["foo"], &log).await;
        sub.step_past(2).await;

        // Unregister it.
        txns.forget(3, d0).await.unwrap();
        sub.step_past(3).await;

        // TODO: Hard mode, see if we can get the rest of this test to work even
        // _without_ the txns shard advancing.
        txns.begin().commit_at(&mut txns, 7).await.unwrap();

        // The operator should continue to emit data written directly even
        // though it's no longer in the txns set.
        let mut d0_write = writer(&client, d0).await;
        let key = "bar".to_owned();
        crate::small_caa(|| "test", &mut d0_write, &[((&key, &()), &5, 1)], 4, 6)
            .await
            .unwrap();
        log.record((d0, key, 5, 1));
        sub.step_past(4).await;

        // Now finalize the shard to writes.
        let () = d0_write
            .compare_and_append_batch(&mut [], Antichain::from_elem(6), Antichain::new())
            .await
            .unwrap()
            .unwrap();
        while sub.txns.less_than(&u64::MAX) {
            sub.step();
            tokio::task::yield_now().await;
        }

        // Make sure we read the correct things.
        log.assert_eq(d0, 1, u64::MAX, sub.output().clone());

        // Also make sure that we can read the right things if we start up after
        // the forget but before the direct write and ditto after the direct
        // write.
        log.assert_subscribe(d0, 4, u64::MAX).await;
        log.assert_subscribe(d0, 6, u64::MAX).await;
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn subscribe_shard_register_forget() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let d0 = ShardId::new();

        // Start a subscription on the data shard.
        let mut sub = txns.read_cache().expect_subscribe(&client, d0, 0);
        assert_eq!(sub.progress(), 0);

        // Register the shard at 10.
        txns.register(10, [writer(&client, d0).await])
            .await
            .unwrap();
        sub.step_past(10).await;
        assert!(
            sub.progress() > 10,
            "operator should advance past 10 when shard is registered"
        );

        // Forget the shard at 20.
        txns.forget(20, d0).await.unwrap();
        sub.step_past(20).await;
        assert!(
            sub.progress() > 20,
            "operator should advance past 20 when shard is forgotten"
        );
    }
}
