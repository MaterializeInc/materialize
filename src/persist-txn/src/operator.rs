// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timely operators for the crate

use std::any::Any;
use std::fmt::Debug;
use std::future::Future;
use std::rc::Rc;
use std::sync::mpsc::TryRecvError;
use std::sync::{mpsc, Arc};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_client::operators::shard_source::shard_source;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use mz_persist_types::{Codec, Codec64, StepForward};
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::EventCore;
use timely::dataflow::operators::{Capture, Leave, Map, Probe};
use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::Scheduler;
use timely::worker::Worker;
use timely::{Data, WorkerConfig};
use tracing::debug;

use crate::txn_read::{DataListenNext, TxnsCache};
use crate::{TxnsCodec, TxnsCodecDefault};

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
pub fn txns_progress<K, V, T, D, P, C, G>(
    passthrough: Stream<G, P>,
    name: &str,
    client: impl Future<Output = PersistClient> + 'static,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    // TODO(txn): Get rid of these when we have a schemaless WriteHandle.
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
) -> (Stream<G, P>, Rc<dyn Any>)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64,
    D: Data + Semigroup + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec,
    G: Scope<Timestamp = T>,
{
    let mut builder =
        AsyncOperatorBuilder::new(format!("txns_progress({})", name), passthrough.scope());
    let (mut passthrough_output, passthrough_stream) = builder.new_output();
    let mut passthrough_input =
        builder.new_input_connection(&passthrough, Pipeline, vec![Antichain::new()]);

    let name = name.to_owned();
    let shutdown_button = builder.build(move |capabilities| async move {
        let [mut cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let client = client.await;
        let mut txns_cache = TxnsCache::<T, C>::open(&client, txns_id).await;

        txns_cache.update_gt(&as_of).await;
        let snap = txns_cache.data_snapshot(data_id, as_of.clone());
        let data_write = client
            .open_writer::<K, V, T, D>(
                data_id,
                data_key_schema,
                data_val_schema,
                Diagnostics::from_purpose("data read physical upper"),
            )
            .await
            .expect("schema shouldn't change");
        let () = snap.unblock_read(data_write).await;

        // We've ensured that the data shard's physical upper is past as_of, so
        // start by passing through data and frontier updates from the input
        // until it is past the as_of.
        let mut read_data_to = as_of.step_forward();
        let mut output_progress_exclusive = T::minimum();
        loop {
            // TODO(txn): Do something more specific when the input returns None
            // (is shut down). I _think_ we'll always return before this happens
            // because we'll get a Progress event with an empty frontier, and so
            // we could assert on this, but I want to be more sure before making
            // that change.
            while let Some(event) = passthrough_input.next_mut().await {
                match event {
                    // NB: Ignore the data_cap because this input is
                    // disconnected.
                    Event::Data(_data_cap, data) => {
                        for data in data.drain(..) {
                            passthrough_output.give(&cap, data).await;
                        }
                    }
                    Event::Progress(progress) => {
                        // We reached the empty frontier! Shut down.
                        let Some(input_progress_exclusive) = progress.as_option() else {
                            return;
                        };

                        // Recall that any reads of the data shard are always
                        // correct, so given that we've passed through any data
                        // from the input, that means we're free to pass through
                        // frontier updates too.
                        if &output_progress_exclusive < input_progress_exclusive {
                            output_progress_exclusive.clone_from(input_progress_exclusive);
                            cap.downgrade(&output_progress_exclusive);
                        }
                        if read_data_to <= output_progress_exclusive {
                            break;
                        }
                    }
                }
            }

            // Any time we hit this point, we've emitted everything known to be
            // physically written to the data shard. Query the txns shard to
            // find out what to do next given our current progress.
            loop {
                txns_cache.update_ge(&output_progress_exclusive).await;
                let data_listen_next =
                    txns_cache.data_listen_next(&data_id, output_progress_exclusive.clone());
                debug!(
                    "txns_progress({}): data_listen_next {:.9} at {:?}: {:?}",
                    name,
                    data_id.to_string(),
                    output_progress_exclusive,
                    data_listen_next
                );
                match data_listen_next {
                    // We've caught up to the txns upper and we have to wait for
                    // it to advance before asking again.
                    //
                    // Note that we're asking again with the same input, but
                    // once the cache is past progress_exclusive (as it will be
                    // after this update_gt call), we're guaranteed to get an
                    // answer.
                    DataListenNext::WaitForTxnsProgress => {
                        txns_cache.update_gt(&output_progress_exclusive).await;
                        continue;
                    }
                    // The data shard got a write! Loop back above and pass
                    // through data until we see it.
                    DataListenNext::ReadDataTo(new_target) => {
                        read_data_to = new_target;
                        break;
                    }
                    // We know there are no writes in
                    // `[output_progress_exclusive, new_progress)`, so advance
                    // our output frontier.
                    DataListenNext::EmitLogicalProgress(new_progress) => {
                        assert!(output_progress_exclusive < new_progress);
                        output_progress_exclusive = new_progress;
                        cap.downgrade(&output_progress_exclusive);
                        continue;
                    }
                }
            }
        }
    });
    (passthrough_stream, Rc::new(shutdown_button.press_on_drop()))
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

    _tokens: Rc<dyn Any>,
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
                    Antichain::new(),
                    false.then_some(|_, _: &_, _| unreachable!()),
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                    |_| true,
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
            let (data_stream, txns_progress_token) =
                txns_progress::<String, (), u64, i64, _, TxnsCodecDefault, _>(
                    data_stream,
                    name,
                    std::future::ready(client.clone()),
                    txns_id,
                    data_id,
                    as_of,
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                );
            let data_stream = data_stream.probe_with(&mut txns);
            let tokens: Rc<dyn Any> = Rc::new((shard_source_token, txns_progress_token));
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
}
