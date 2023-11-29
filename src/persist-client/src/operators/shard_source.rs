// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from a persist shard.

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::slice;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_persist_types::{Codec, Codec64};
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{CapabilitySet, ConnectLoop, Enter, Feedback, Leave};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::frontier::AntichainRef;
use timely::progress::{timestamp::Refines, Antichain, Timestamp};
use timely::scheduling::Activator;
use timely::PartialOrder;
use tracing::{debug, trace};

use crate::cfg::RetryParameters;
use crate::fetch::{FetchedPart, SerdeLeasedBatchPart};
use crate::read::{ListenEvent, SubscriptionLeaseReturner};
use crate::stats::PartStats;
use crate::{Diagnostics, PersistClient, ShardId};

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
/// All updates at times greater or equal to `until` will be suppressed.
/// The `map_filter_project` argument, if supplied, may be partially applied,
/// and any un-applied part of the argument will be left behind in the argument.
///
/// The `desc_transformer` interposes an operator in the stream before the
/// chosen data is fetched. This is currently used to provide flow control... see
/// usages for details.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn shard_source<'g, K, V, T, D, F, DT, G, C>(
    scope: &mut Child<'g, G, T>,
    name: &str,
    client: impl Fn() -> C,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    until: Antichain<G::Timestamp>,
    desc_transformer: Option<DT>,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    should_fetch_part: F,
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + 'static>,
) -> (
    Stream<Child<'g, G, T>, FetchedPart<K, V, G::Timestamp, D>>,
    Vec<PressOnDropButton>,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    F: FnMut(&PartStats, AntichainRef<G::Timestamp>) -> bool + 'static,
    G: Scope,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder,
    T: Refines<G::Timestamp>,
    DT: FnOnce(
        Child<'g, G, T>,
        &Stream<Child<'g, G, T>, (usize, SerdeLeasedBatchPart)>,
        usize,
    ) -> (
        Stream<Child<'g, G, T>, (usize, SerdeLeasedBatchPart)>,
        Vec<PressOnDropButton>,
    ),
    C: Future<Output = PersistClient> + Send + 'static,
{
    // WARNING! If emulating any of this code, you should read the doc string on
    // [`LeasedBatchPart`] and [`Subscribe`] or will likely run into intentional
    // panics.
    //
    // This source is split as such:
    // 1. Sets up `async_stream`, which only yields data (parts) on one chosen
    //    worker. Generating also generates SeqNo leases on the chosen worker,
    //    ensuring `part`s do not get GCed while in flight.
    // 2. Part distribution: A timely source operator which continuously reads
    //    from that stream, and distributes the data among workers.
    // 3. Part fetcher: A timely operator which downloads the part's contents
    //    from S3, and outputs them to a timely stream. Additionally, the
    //    operator returns the `LeasedBatchPart` to the original worker, so it
    //    can release the SeqNo lease.

    let chosen_worker = usize::cast_from(name.hashed()) % scope.peers();

    let mut tokens = vec![];

    // we can safely pass along a zero summary from this feedback edge,
    // as the input is disconnected from the operator's output
    let (completed_fetches_feedback_handle, completed_fetches_feedback_stream) =
        scope.feedback(T::Summary::default());

    let (descs, descs_token) = shard_source_descs::<K, V, D, _, G>(
        &scope.parent,
        name,
        client(),
        shard_id.clone(),
        as_of,
        until,
        completed_fetches_feedback_stream.leave(),
        chosen_worker,
        Arc::clone(&key_schema),
        Arc::clone(&val_schema),
        should_fetch_part,
        listen_sleep,
    );
    tokens.push(descs_token);

    let descs = descs.enter(scope);
    let descs = match desc_transformer {
        Some(desc_transformer) => {
            let (descs, extra_tokens) = desc_transformer(scope.clone(), &descs, chosen_worker);
            tokens.extend(extra_tokens);
            descs
        }
        None => descs,
    };

    let (parts, completed_fetches_stream, fetch_token) =
        shard_source_fetch(&descs, name, client(), shard_id, key_schema, val_schema);
    completed_fetches_stream.connect_loop(completed_fetches_feedback_handle);
    tokens.push(fetch_token);

    (parts, tokens)
}

#[derive(Debug)]
struct ActivateOnDrop {
    token_rx: tokio::sync::mpsc::Receiver<()>,
    activator: Activator,
}

impl Drop for ActivateOnDrop {
    fn drop(&mut self) {
        self.token_rx.close();
        self.activator.activate();
    }
}

pub(crate) fn shard_source_descs<K, V, D, F, G>(
    scope: &G,
    name: &str,
    client: impl Future<Output = PersistClient> + Send + 'static,
    shard_id: ShardId,
    as_of: Option<Antichain<G::Timestamp>>,
    until: Antichain<G::Timestamp>,
    completed_fetches_stream: Stream<G, SerdeLeasedBatchPart>,
    chosen_worker: usize,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
    mut should_fetch_part: F,
    // If Some, an override for the default listen sleep retry parameters.
    listen_sleep: Option<impl Fn() -> RetryParameters + 'static>,
) -> (Stream<G, (usize, SerdeLeasedBatchPart)>, PressOnDropButton)
where
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
    F: FnMut(&PartStats, AntichainRef<G::Timestamp>) -> bool + 'static,
    G: Scope,
    // TODO: Figure out how to get rid of the TotalOrder bound :(.
    G::Timestamp: Timestamp + Lattice + Codec64 + TotalOrder,
{
    let worker_index = scope.index();
    let num_workers = scope.peers();

    // This is a generator that sets up an async `Stream` that can be continuously polled to get the
    // values that are `yield`-ed from it's body.
    let name_owned = name.to_owned();

    // Create a shared slot between the operator to store the subscription handle
    let subscription_handle = Rc::new(RefCell::new(None));
    let return_subscription_handle = Rc::clone(&subscription_handle);

    // Create a oneshot channel to give the part returner a SubscriptionLeaseReturner
    let (tx, rx) = tokio::sync::oneshot::channel::<SubscriptionLeaseReturner>();
    let mut builder = AsyncOperatorBuilder::new(
        format!("shard_source_descs_return({})", name),
        scope.clone(),
    );
    let mut completed_fetches = builder.new_input(
        &completed_fetches_stream,
        // We must ensure all completed fetches are fed into
        // the worker responsible for managing part leases
        Exchange::new(move |_| u64::cast_from(chosen_worker)),
    );
    // This operator doesn't need to use a token because it naturally exits when its input
    // frontier reaches the empty antichain.
    builder.build(move |_caps| async move {
        let Ok(mut lease_returner) = rx.await else {
            // Either we're not the chosen worker or the dataflow was shutdown before the
            // subscriber was even created.
            return;
        };
        while let Some(event) = completed_fetches.next_mut().await {
            let Event::Data(_cap, data) = event else {
                continue;
            };
            for part in data.drain(..) {
                lease_returner.return_leased_part(
                    lease_returner.leased_part_from_exchangeable::<G::Timestamp>(part),
                );
            }
        }
        // Make it explicit that the subscriber is kept alive until we have finished returning parts
        drop(return_subscription_handle);
    });

    let mut builder =
        AsyncOperatorBuilder::new(format!("shard_source_descs({})", name), scope.clone());
    let (mut descs_output, descs_stream) = builder.new_output();

    #[allow(clippy::await_holding_refcell_ref)]
    let shutdown_button = builder.build(move |caps| async move {
        let mut cap_set = CapabilitySet::from_elem(caps.into_element());

        // Only one worker is responsible for distributing parts
        if worker_index != chosen_worker {
            trace!(
                "We are not the chosen worker ({}), exiting...",
                chosen_worker
            );
            return;
        }

        // Internally, the `open_leased_reader` call registers a new LeasedReaderId and then fires
        // up a background tokio task to heartbeat it. It is possible that we might get a
        // particularly adversarial scheduling where the CRDB query to register the id is sent and
        // then our Future is not polled again for a long time, resulting is us never spawning the
        // heartbeat task. Run reader creation in a task to attempt to defend against this.
        //
        // TODO: Really we likely need to swap the inners of all persist operators to be
        // communicating with a tokio task over a channel, but that's much much harder, so for now
        // we whack the moles as we see them.
        let read = mz_ore::task::spawn(|| format!("shard_source_reader({})", name_owned), {
            let diagnostics = Diagnostics {
                handle_purpose: format!("shard_source({})", name_owned),
                shard_name: name_owned.clone(),
            };
            async move {
                client
                    .await
                    .open_leased_reader::<K, V, G::Timestamp, D>(
                        shard_id,
                        key_schema,
                        val_schema,
                        diagnostics,
                    )
                    .await
            }
        })
        .await
        .expect("reader creation shouldn't panic")
        .expect("could not open persist shard");

        let cfg = read.cfg.clone();
        let metrics = Arc::clone(&read.metrics);

        let as_of = as_of.unwrap_or_else(|| read.since().clone());

        // Eagerly downgrade our frontier to the initial as_of. This makes sure
        // that the output frontier of the `persist_source` closely tracks the
        // `upper` frontier of the persist shard. It might be that the snapshot
        // for `as_of` is not initially available yet, but this makes sure we
        // already downgrade to it.
        //
        // Downstream consumers might rely on close frontier tracking for making
        // progress. For example, the `persist_sink` needs to know the
        // up-to-date upper of the output shard to make progress because it will
        // only write out new data once it knows that earlier writes went
        // through, including the initial downgrade of the shard upper to the
        // `as_of`.
        //
        // NOTE: We have to do this before our `subscribe()` call (which
        // internally calls `snapshot()` because that call will block when there
        // is no data yet available in the shard.
        cap_set.downgrade(as_of.clone());
        let mut current_ts = match as_of.clone().into_option() {
            Some(ts) => ts,
            None => {
                return;
            }
        };

        // Store the subscription handle in the shared slot so that it stays alive until both
        // operators exit
        let mut subscription = subscription_handle.borrow_mut();
        let subscription =
            subscription.insert(read.subscribe(as_of.clone()).await.unwrap_or_else(|e| {
                panic!(
                    "{}: {} cannot serve requested as_of {:?}: {:?}",
                    name_owned, shard_id, as_of, e
                )
            }));

        // We're about to start producing parts to be fetched whose leases will be returned by the
        // `shard_source_descs_return` operator above. In order for that operator to successfully
        // return the leases we send it the lease returner associated with our shared subscriber.
        tx.send(subscription.lease_returner().clone())
            .expect("lease returner exited before desc producer");
        let mut lease_returner = subscription.lease_returner().clone();

        // Read from the subscription and pass them on.
        let mut batch_parts = vec![];
        let listen_retry = listen_sleep.as_ref().map(|retry| retry());
        let mut upper = Antichain::from_elem(Timestamp::minimum());
        // If `until.less_equal(progress)`, it means that all subsequent batches will contain only
        // times greater or equal to `until`, which means they can be dropped in their entirety.
        while !PartialOrder::less_equal(&until, &upper) {
            for event in subscription.next(listen_retry).await {
                match event {
                    ListenEvent::Updates(mut parts) => {
                        batch_parts.append(&mut parts);
                    }
                    ListenEvent::Progress(progress) => {
                        // Emit the part at the `(ts, 0)` time. The `granular_backpressure`
                        // operator will refine this further, if its enabled.
                        let session_cap = cap_set.delayed(&current_ts);

                        for mut part_desc in std::mem::take(&mut batch_parts) {
                            // TODO: Push the filter down into the Subscribe?
                            if cfg.dynamic.stats_filter_enabled() {
                                let should_fetch = part_desc.stats.as_ref().map_or(true, |stats| {
                                    should_fetch_part(
                                        &stats.decode(),
                                        AntichainRef::new(slice::from_ref(&current_ts)),
                                    )
                                });
                                let bytes = u64::cast_from(part_desc.encoded_size_bytes);
                                if should_fetch {
                                    metrics.pushdown.parts_fetched_count.inc();
                                    metrics.pushdown.parts_fetched_bytes.inc_by(bytes);
                                } else {
                                    metrics.pushdown.parts_filtered_count.inc();
                                    metrics.pushdown.parts_filtered_bytes.inc_by(bytes);
                                    let should_audit = {
                                        let mut h = DefaultHasher::new();
                                        part_desc.key.hash(&mut h);
                                        usize::cast_from(h.finish()) % 100
                                            < cfg.dynamic.stats_audit_percent()
                                    };
                                    if should_audit {
                                        metrics.pushdown.parts_audited_count.inc();
                                        metrics.pushdown.parts_audited_bytes.inc_by(bytes);
                                        part_desc.request_filter_pushdown_audit();
                                    } else {
                                        debug!(
                                            "skipping part because of stats filter {:?}",
                                            part_desc.stats
                                        );
                                        lease_returner.return_leased_part(part_desc);
                                        continue;
                                    }
                                }
                            }

                            // Give the part to a random worker. This isn't
                            // round robin in an attempt to avoid skew issues:
                            // if your parts alternate size large, small, then
                            // you'll end up only using half of your workers.
                            //
                            // There's certainly some other things we could be
                            // doing instead here, but this has seemed to work
                            // okay so far. Continue to revisit as necessary.
                            let worker_idx =
                                usize::cast_from(Instant::now().hashed()) % num_workers;
                            descs_output
                                .give(
                                    &session_cap,
                                    (worker_idx, part_desc.into_exchangeable_part()),
                                )
                                .await;
                        }

                        if let Some(ts) = progress.as_option() {
                            current_ts = ts.clone();
                        }
                        cap_set.downgrade(progress.iter());
                        upper = progress;
                    }
                }
            }
        }
    });

    (descs_stream, shutdown_button.press_on_drop())
}

pub(crate) fn shard_source_fetch<K, V, T, D, G>(
    descs: &Stream<G, (usize, SerdeLeasedBatchPart)>,
    name: &str,
    client: impl Future<Output = PersistClient> + 'static,
    shard_id: ShardId,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
) -> (
    Stream<G, FetchedPart<K, V, T, D>>,
    Stream<G, SerdeLeasedBatchPart>,
    PressOnDropButton,
)
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
    G: Scope,
    G::Timestamp: Refines<T>,
{
    let mut builder =
        AsyncOperatorBuilder::new(format!("shard_source_fetch({})", name), descs.scope());
    let mut descs_input = builder.new_input(
        descs,
        Exchange::new(|&(i, _): &(usize, _)| u64::cast_from(i)),
    );
    let (mut fetched_output, fetched_stream) = builder.new_output();
    let (mut completed_fetches_output, completed_fetches_stream) = builder.new_output();
    let name_owned = name.to_owned();

    let shutdown_button = builder.build(move |_capabilities| async move {
        let fetcher = {
            client
                .await
                .create_batch_fetcher::<K, V, T, D>(
                    shard_id,
                    key_schema,
                    val_schema,
                    Diagnostics {
                        shard_name: name_owned.clone(),
                        handle_purpose: format!("shard_source_fetch batch fetcher {}", name_owned),
                    },
                )
                .await
        };

        while let Some(event) = descs_input.next_mut().await {
            if let Event::Data(cap, data) = event {
                // `LeasedBatchPart`es cannot be dropped at this point w/o
                // panicking, so swap them to an owned version.
                for (_idx, part) in data.drain(..) {
                    let leased_part = fetcher.leased_part_from_exchangeable(part);
                    let fetched = fetcher
                        .fetch_leased_part(&leased_part)
                        .await
                        .expect("shard_id should match across all workers");
                    {
                        // Do very fine-grained output activation/session
                        // creation to ensure that we don't hold activated
                        // outputs or sessions across await points, which
                        // would prevent messages from being flushed from
                        // the shared timely output buffer.
                        fetched_output.give(&cap, fetched).await;
                        completed_fetches_output
                            .give(&cap, leased_part.into_exchangeable_part())
                            .await;
                    }
                }
            }
        }
    });

    (
        fetched_stream,
        completed_fetches_stream,
        shutdown_button.press_on_drop(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use timely::dataflow::operators::Leave;
    use timely::dataflow::operators::Probe;
    use timely::dataflow::Scope;
    use timely::progress::Antichain;

    use crate::operators::shard_source::shard_source;
    use crate::{Diagnostics, ShardId};

    /// Verifies that a `shard_source` will downgrade it's output frontier to
    /// the `since` of the shard when no explicit `as_of` is given. Even if
    /// there is no data/no snapshot available in the
    /// shard.
    ///
    /// NOTE: This test is weird: if everything is good it will pass. If we
    /// break the assumption that we test this will time out and we will notice.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn test_shard_source_implicit_initial_as_of() {
        let persist_client = PersistClient::new_for_tests().await;

        let expected_frontier = 42;
        let shard_id = ShardId::new();

        initialize_shard(
            &persist_client,
            shard_id,
            Antichain::from_elem(expected_frontier),
        )
        .await;

        let res = timely::execute::execute_directly(move |worker| {
            let until = Antichain::new();

            let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
                let (stream, token) = scope.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs: &Stream<_, _>, _| (descs.clone(), vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _, _>(
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        None, // No explicit as_of!
                        until,
                        Some(transformer),
                        Arc::new(
                            <std::string::String as mz_persist_types::Codec>::Schema::default(),
                        ),
                        Arc::new(
                            <std::string::String as mz_persist_types::Codec>::Schema::default(),
                        ),
                        |_fetch, _frontier| true,
                        false.then_some(|| unreachable!()),
                    );
                    (stream.leave(), tokens)
                });

                let probe = stream.probe();

                (probe, token)
            });

            while probe.less_than(&expected_frontier) {
                worker.step();
            }

            let mut probe_frontier = Antichain::new();
            probe.with_frontier(|f| probe_frontier.extend(f.iter().cloned()));

            probe_frontier
        });

        assert_eq!(res, Antichain::from_elem(expected_frontier));
    }

    /// Verifies that a `shard_source` will downgrade it's output frontier to
    /// the given `as_of`. Even if there is no data/no snapshot available in the
    /// shard.
    ///
    /// NOTE: This test is weird: if everything is good it will pass. If we
    /// break the assumption that we test this will time out and we will notice.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn test_shard_source_explicit_initial_as_of() {
        let persist_client = PersistClient::new_for_tests().await;

        let expected_frontier = 42;
        let shard_id = ShardId::new();

        initialize_shard(
            &persist_client,
            shard_id,
            Antichain::from_elem(expected_frontier),
        )
        .await;

        let res = timely::execute::execute_directly(move |worker| {
            let as_of = Antichain::from_elem(expected_frontier);
            let until = Antichain::new();

            let (probe, _token) = worker.dataflow::<u64, _, _>(|scope| {
                let (stream, token) = scope.scoped::<u64, _, _>("hybrid", |scope| {
                    let transformer = move |_, descs: &Stream<_, _>, _| (descs.clone(), vec![]);
                    let (stream, tokens) = shard_source::<String, String, u64, u64, _, _, _, _>(
                        scope,
                        "test_source",
                        move || std::future::ready(persist_client.clone()),
                        shard_id,
                        Some(as_of), // We specify the as_of explicitly!
                        until,
                        Some(transformer),
                        Arc::new(
                            <std::string::String as mz_persist_types::Codec>::Schema::default(),
                        ),
                        Arc::new(
                            <std::string::String as mz_persist_types::Codec>::Schema::default(),
                        ),
                        |_fetch, _frontier| true,
                        false.then_some(|| unreachable!()),
                    );
                    (stream.leave(), tokens)
                });

                let probe = stream.probe();

                (probe, token)
            });

            while probe.less_than(&expected_frontier) {
                worker.step();
            }

            let mut probe_frontier = Antichain::new();
            probe.with_frontier(|f| probe_frontier.extend(f.iter().cloned()));

            probe_frontier
        });

        assert_eq!(res, Antichain::from_elem(expected_frontier));
    }

    async fn initialize_shard(
        persist_client: &PersistClient,
        shard_id: ShardId,
        since: Antichain<u64>,
    ) {
        let mut read_handle = persist_client
            .open_leased_reader::<String, String, u64, u64>(
                shard_id,
                Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                Arc::new(<std::string::String as mz_persist_types::Codec>::Schema::default()),
                Diagnostics::for_tests(),
            )
            .await
            .expect("invalid usage");

        read_handle.downgrade_since(&since).await;
    }
}
