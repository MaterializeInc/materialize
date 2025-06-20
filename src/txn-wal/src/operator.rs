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
use std::sync::mpsc::TryRecvError;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use differential_dataflow::Hashable;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures::StreamExt;
use mz_dyncfg::{Config, ConfigSet, ConfigUpdates};
use mz_ore::cast::CastFrom;
use mz_ore::task::JoinHandleExt;
use mz_persist_client::cfg::{RetryParameters, USE_GLOBAL_TXN_CACHE_SOURCE};
use mz_persist_client::operators::shard_source::{
    ErrorHandler, FilterResult, SnapshotMode, shard_source,
};
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use mz_persist_types::txn::TxnsCodec;
use mz_persist_types::{Codec, Codec64, StepForward};
use mz_timely_util::builder_async::{
    AsyncInputHandle, Event as AsyncEvent, InputConnection,
    OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::{Broadcast, Capture, Leave, Map, Probe};
use timely::dataflow::{ProbeHandle, Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::worker::Worker;
use timely::{Data, PartialOrder, WorkerConfig};
use tracing::debug;
use tracing::instrument::WithSubscriber;

use crate::TxnsCodecDefault;
use crate::txn_cache::TxnsCache;
use crate::txn_read::{DataListenNext, DataRemapEntry, TxnsRead};

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
    ctx: &TxnsContext,
    worker_dyncfgs: &ConfigSet,
    client_fn: impl Fn() -> F,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    until: Antichain<T>,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
) -> (Stream<G, P>, Vec<PressOnDropButton>)
where
    K: Debug + Codec + Send + Sync,
    V: Debug + Codec + Send + Sync,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    D: Debug + Data + Semigroup + Ord + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec + 'static,
    F: Future<Output = PersistClient> + Send + 'static,
    G: Scope<Timestamp = T>,
{
    let unique_id = (name, passthrough.scope().addr()).hashed();
    let (remap, source_button) = if USE_GLOBAL_TXN_CACHE_SOURCE.get(worker_dyncfgs) {
        txns_progress_source_global::<K, V, T, D, P, C, G>(
            passthrough.scope(),
            name,
            ctx.clone(),
            client_fn(),
            txns_id,
            data_id,
            as_of,
            data_key_schema,
            data_val_schema,
            unique_id,
        )
    } else {
        txns_progress_source_local::<K, V, T, D, P, C, G>(
            passthrough.scope(),
            name,
            client_fn(),
            txns_id,
            data_id,
            as_of,
            data_key_schema,
            data_val_schema,
            unique_id,
        )
    };
    // Each of the `txns_frontiers` workers wants the full copy of the remap
    // information.
    let remap = remap.broadcast();
    let (passthrough, frontiers_button) = txns_progress_frontiers::<K, V, T, D, P, C, G>(
        remap,
        passthrough,
        name,
        data_id,
        until,
        unique_id,
    );
    (passthrough, vec![source_button, frontiers_button])
}

/// An alternative implementation of [`txns_progress_source_global`] that opens
/// a new [`TxnsCache`] local to the operator.
fn txns_progress_source_local<K, V, T, D, P, C, G>(
    scope: G,
    name: &str,
    client: impl Future<Output = PersistClient> + 'static,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
    unique_id: u64,
) -> (Stream<G, DataRemapEntry<T>>, PressOnDropButton)
where
    K: Debug + Codec + Send + Sync,
    V: Debug + Codec + Send + Sync,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    D: Debug + Data + Semigroup + Ord + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec + 'static,
    G: Scope<Timestamp = T>,
{
    let worker_idx = scope.index();
    let chosen_worker = usize::cast_from(name.hashed()) % scope.peers();
    let name = format!("txns_progress_source({})", name);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), scope);
    let name = format!("{} [{}] {:.9}", name, unique_id, data_id.to_string());
    let (remap_output, remap_stream) = builder.new_output();

    let shutdown_button = builder.build(move |capabilities| async move {
        if worker_idx != chosen_worker {
            return;
        }

        let [mut cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let client = client.await;
        let mut txns_cache = TxnsCache::<T, C>::open(&client, txns_id, Some(data_id)).await;

        let _ = txns_cache.update_gt(&as_of).await;
        let mut subscribe = txns_cache.data_subscribe(data_id, as_of.clone());
        let data_write = client
            .open_writer::<K, V, T, D>(
                data_id,
                Arc::clone(&data_key_schema),
                Arc::clone(&data_val_schema),
                Diagnostics::from_purpose("data read physical upper"),
            )
            .await
            .expect("schema shouldn't change");
        if let Some(snapshot) = subscribe.snapshot.take() {
            snapshot.unblock_read(data_write).await;
        }

        debug!("{} emitting {:?}", name, subscribe.remap);
        remap_output.give(&cap, subscribe.remap.clone());

        loop {
            let _ = txns_cache.update_ge(&subscribe.remap.logical_upper).await;
            cap.downgrade(&subscribe.remap.logical_upper);
            let data_listen_next =
                txns_cache.data_listen_next(&subscribe.data_id, &subscribe.remap.logical_upper);
            debug!(
                "{} data_listen_next at {:?}: {:?}",
                name, subscribe.remap.logical_upper, data_listen_next,
            );
            match data_listen_next {
                // We've caught up to the txns upper and we have to wait for it
                // to advance before asking again.
                //
                // Note that we're asking again with the same input, but once
                // the cache is past remap.logical_upper (as it will be after
                // this update_gt call), we're guaranteed to get an answer.
                DataListenNext::WaitForTxnsProgress => {
                    let _ = txns_cache.update_gt(&subscribe.remap.logical_upper).await;
                }
                // The data shard got a write!
                DataListenNext::ReadDataTo(new_upper) => {
                    // A write means both the physical and logical upper advance.
                    subscribe.remap = DataRemapEntry {
                        physical_upper: new_upper.clone(),
                        logical_upper: new_upper,
                    };
                    debug!("{} emitting {:?}", name, subscribe.remap);
                    remap_output.give(&cap, subscribe.remap.clone());
                }
                // We know there are no writes in `[logical_upper,
                // new_progress)`, so advance our output frontier.
                DataListenNext::EmitLogicalProgress(new_progress) => {
                    assert!(subscribe.remap.physical_upper < new_progress);
                    assert!(subscribe.remap.logical_upper < new_progress);

                    subscribe.remap.logical_upper = new_progress;
                    // As mentioned in the docs on `DataRemapEntry`, we only
                    // emit updates when the physical upper changes (which
                    // happens to makes the protocol a tiny bit more
                    // remap-like).
                    debug!("{} not emitting {:?}", name, subscribe.remap);
                }
            }
        }
    });
    (remap_stream, shutdown_button.press_on_drop())
}

/// TODO: I'd much prefer the communication protocol between the two operators
/// to be exactly remap as defined in the [reclocking design doc]. However, we
/// can't quite recover exactly the information necessary to construct that at
/// the moment. Seems worth doing, but in the meantime, intentionally make this
/// look fairly different (`Stream` of `DataRemapEntry` instead of
/// `Collection<FromTime>`) to hopefully minimize confusion. As a performance
/// optimization, we only re-emit this when the _physical_ upper has changed,
/// which means that the frontier of the `Stream<DataRemapEntry<T>>` indicates
/// updates to the logical_upper of the most recent `DataRemapEntry` (i.e. the
/// one with the largest physical_upper).
///
/// [reclocking design doc]:
///     https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210714_reclocking.md
fn txns_progress_source_global<K, V, T, D, P, C, G>(
    scope: G,
    name: &str,
    ctx: TxnsContext,
    client: impl Future<Output = PersistClient> + 'static,
    txns_id: ShardId,
    data_id: ShardId,
    as_of: T,
    data_key_schema: Arc<K::Schema>,
    data_val_schema: Arc<V::Schema>,
    unique_id: u64,
) -> (Stream<G, DataRemapEntry<T>>, PressOnDropButton)
where
    K: Debug + Codec + Send + Sync,
    V: Debug + Codec + Send + Sync,
    T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
    D: Debug + Data + Semigroup + Ord + Codec64 + Send + Sync,
    P: Debug + Data,
    C: TxnsCodec + 'static,
    G: Scope<Timestamp = T>,
{
    let worker_idx = scope.index();
    let chosen_worker = usize::cast_from(name.hashed()) % scope.peers();
    let name = format!("txns_progress_source({})", name);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), scope);
    let name = format!("{} [{}] {:.9}", name, unique_id, data_id.to_string());
    let (remap_output, remap_stream) = builder.new_output();

    let shutdown_button = builder.build(move |capabilities| async move {
        if worker_idx != chosen_worker {
            return;
        }

        let [mut cap]: [_; 1] = capabilities.try_into().expect("one capability per output");
        let client = client.await;
        let txns_read = ctx.get_or_init::<T, C>(&client, txns_id).await;

        let _ = txns_read.update_gt(as_of.clone()).await;
        let data_write = client
            .open_writer::<K, V, T, D>(
                data_id,
                Arc::clone(&data_key_schema),
                Arc::clone(&data_val_schema),
                Diagnostics::from_purpose("data read physical upper"),
            )
            .await
            .expect("schema shouldn't change");
        let mut rx = txns_read
            .data_subscribe(data_id, as_of.clone(), Box::new(data_write))
            .await;
        debug!("{} starting as_of={:?}", name, as_of);

        let mut physical_upper = T::minimum();

        while let Some(remap) = rx.recv().await {
            assert!(physical_upper <= remap.physical_upper);
            assert!(physical_upper < remap.logical_upper);

            let logical_upper = remap.logical_upper.clone();
            // As mentioned in the docs on this function, we only
            // emit updates when the physical upper changes (which
            // happens to makes the protocol a tiny bit more
            // remap-like).
            if remap.physical_upper != physical_upper {
                physical_upper = remap.physical_upper.clone();
                debug!("{} emitting {:?}", name, remap);
                remap_output.give(&cap, remap);
            } else {
                debug!("{} not emitting {:?}", name, remap);
            }
            cap.downgrade(&logical_upper);
        }
    });
    (remap_stream, shutdown_button.press_on_drop())
}

fn txns_progress_frontiers<K, V, T, D, P, C, G>(
    remap: Stream<G, DataRemapEntry<T>>,
    passthrough: Stream<G, P>,
    name: &str,
    data_id: ShardId,
    until: Antichain<T>,
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
    let name = format!("txns_progress_frontiers({})", name);
    let mut builder = AsyncOperatorBuilder::new(name.clone(), passthrough.scope());
    let name = format!(
        "{} [{}] {}/{} {:.9}",
        name,
        unique_id,
        passthrough.scope().index(),
        passthrough.scope().peers(),
        data_id.to_string(),
    );
    let (passthrough_output, passthrough_stream) =
        builder.new_output::<CapacityContainerBuilder<_>>();
    let mut remap_input = builder.new_disconnected_input(&remap, Pipeline);
    let mut passthrough_input = builder.new_disconnected_input(&passthrough, Pipeline);

    let shutdown_button = builder.build(move |capabilities| async move {
        let [mut cap]: [_; 1] = capabilities.try_into().expect("one capability per output");

        // None is used to indicate that both uppers are the empty antichain.
        let mut remap = Some(DataRemapEntry {
            physical_upper: T::minimum(),
            logical_upper: T::minimum(),
        });
        // NB: The following loop uses `cap.time()`` to track how far we've
        // progressed in copying along the passthrough input.
        loop {
            debug!("{} remap {:?}", name, remap);
            if let Some(r) = remap.as_ref() {
                assert!(r.physical_upper <= r.logical_upper);
                // If we've passed through data to at least `physical_upper`,
                // then it means we can artificially advance the upper of the
                // output to `logical_upper`. This also indicates that we need
                // to wait for the next DataRemapEntry. It can either (A) have
                // the same physical upper or (B) have a larger physical upper.
                //
                // - If (A), then we would again satisfy this `physical_upper`
                //   check, again advance the logical upper again, ...
                // - If (B), then we'd fall down to the code below, which copies
                //   the passthrough data until the frontier passes
                //   `physical_upper`, then loops back up here.
                if r.physical_upper.less_equal(cap.time()) {
                    if cap.time() < &r.logical_upper {
                        cap.downgrade(&r.logical_upper);
                    }
                    remap = txns_progress_frontiers_read_remap_input(
                        &name,
                        &mut remap_input,
                        r.clone(),
                    )
                    .await;
                    continue;
                }
            }

            // This only returns None when there are no more data left. Turn it
            // into an empty frontier progress so we can re-use the shutdown
            // code below.
            let event = passthrough_input
                .next()
                .await
                .unwrap_or_else(|| AsyncEvent::Progress(Antichain::new()));
            match event {
                // NB: Ignore the data_cap because this input is disconnected.
                AsyncEvent::Data(_data_cap, mut data) => {
                    // NB: Nothing to do here for `until` because both the
                    // `shard_source` (before this operator) and
                    // `mfp_and_decode` (after this operator) do the necessary
                    // filtering.
                    debug!("{} emitting data {:?}", name, data);
                    passthrough_output.give_container(&cap, &mut data);
                }
                AsyncEvent::Progress(new_progress) => {
                    // If `until.less_equal(new_progress)`, it means that all
                    // subsequent batches will contain only times greater or
                    // equal to `until`, which means they can be dropped in
                    // their entirety.
                    //
                    // Ideally this check would live in `txns_progress_source`,
                    // but that turns out to be much more invasive (requires
                    // replacing lots of `T`s with `Antichain<T>`s). Given that
                    // we've been thinking about reworking the operators, do the
                    // easy but more wasteful thing for now.
                    if PartialOrder::less_equal(&until, &new_progress) {
                        debug!(
                            "{} progress {:?} has passed until {:?}",
                            name,
                            new_progress.elements(),
                            until.elements()
                        );
                        return;
                    }
                    // We reached the empty frontier! Shut down.
                    let Some(new_progress) = new_progress.into_option() else {
                        return;
                    };

                    // Recall that any reads of the data shard are always
                    // correct, so given that we've passed through any data
                    // from the input, that means we're free to pass through
                    // frontier updates too.
                    if cap.time() < &new_progress {
                        debug!("{} downgrading cap to {:?}", name, new_progress);
                        cap.downgrade(&new_progress);
                    }
                }
            }
        }
    });
    (passthrough_stream, shutdown_button.press_on_drop())
}

async fn txns_progress_frontiers_read_remap_input<T, C>(
    name: &str,
    input: &mut AsyncInputHandle<T, Vec<DataRemapEntry<T>>, C>,
    mut remap: DataRemapEntry<T>,
) -> Option<DataRemapEntry<T>>
where
    T: Timestamp + TotalOrder,
    C: InputConnection<T>,
{
    while let Some(event) = input.next().await {
        let xs = match event {
            AsyncEvent::Progress(logical_upper) => {
                if let Some(logical_upper) = logical_upper.into_option() {
                    if remap.logical_upper < logical_upper {
                        remap.logical_upper = logical_upper;
                        return Some(remap);
                    }
                }
                continue;
            }
            AsyncEvent::Data(_cap, xs) => xs,
        };
        for x in xs {
            debug!("{} got remap {:?}", name, x);
            // Don't assume anything about the ordering.
            if remap.logical_upper < x.logical_upper {
                assert!(
                    remap.physical_upper <= x.physical_upper,
                    "previous remap physical upper {:?} is ahead of new remap physical upper {:?}",
                    remap.physical_upper,
                    x.physical_upper,
                );
                // TODO: If the physical upper has advanced, that's a very
                // strong hint that the data shard is about to be written to.
                // Because the data shard's upper advances sparsely (on write,
                // but not on passage of time) which invalidates the "every 1s"
                // assumption of the default tuning, we've had to de-tune the
                // listen sleeps on the paired persist_source. Maybe we use "one
                // state" to wake it up in case pubsub doesn't and remove the
                // listen polling entirely? (NB: This would have to happen in
                // each worker so that it's guaranteed to happen in each
                // process.)
                remap = x;
            }
        }
        return Some(remap);
    }
    // remap_input is closed, which indicates the data shard is finished.
    None
}

/// The process global [`TxnsRead`] that any operator can communicate with.
#[derive(Default, Debug, Clone)]
pub struct TxnsContext {
    read: Arc<tokio::sync::OnceCell<Box<dyn Any + Send + Sync>>>,
}

impl TxnsContext {
    async fn get_or_init<T, C>(&self, client: &PersistClient, txns_id: ShardId) -> TxnsRead<T>
    where
        T: Timestamp + Lattice + Codec64 + TotalOrder + StepForward + Sync,
        C: TxnsCodec + 'static,
    {
        let read = self
            .read
            .get_or_init(|| {
                let client = client.clone();
                async move {
                    let read: Box<dyn Any + Send + Sync> =
                        Box::new(TxnsRead::<T>::start::<C>(client, txns_id).await);
                    read
                }
            })
            .await
            .downcast_ref::<TxnsRead<T>>()
            .expect("timestamp types should match");
        // We initially only have one txns shard in the system.
        assert_eq!(&txns_id, read.txns_id());
        read.clone()
    }
}

// Existing configs use the prefix "persist_txns_" for historical reasons. New
// configs should use the prefix "txn_wal_".

pub(crate) const DATA_SHARD_RETRYER_INITIAL_BACKOFF: Config<Duration> = Config::new(
    "persist_txns_data_shard_retryer_initial_backoff",
    Duration::from_millis(1024),
    "The initial backoff when polling for new batches from a txns data shard persist_source.",
);

pub(crate) const DATA_SHARD_RETRYER_MULTIPLIER: Config<u32> = Config::new(
    "persist_txns_data_shard_retryer_multiplier",
    2,
    "The backoff multiplier when polling for new batches from a txns data shard persist_source.",
);

pub(crate) const DATA_SHARD_RETRYER_CLAMP: Config<Duration> = Config::new(
    "persist_txns_data_shard_retryer_clamp",
    Duration::from_secs(16),
    "The backoff clamp duration when polling for new batches from a txns data shard persist_source.",
);

/// Retry configuration for txn-wal data shard override of
/// `next_listen_batch`.
pub fn txns_data_shard_retry_params(cfg: &ConfigSet) -> RetryParameters {
    RetryParameters {
        fixed_sleep: Duration::ZERO,
        initial_backoff: DATA_SHARD_RETRYER_INITIAL_BACKOFF.get(cfg),
        multiplier: DATA_SHARD_RETRYER_MULTIPLIER.get(cfg),
        clamp: DATA_SHARD_RETRYER_CLAMP.get(cfg),
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
    capture: mpsc::Receiver<Event<u64, Vec<(String, u64, i64)>>>,
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
        until: Antichain<u64>,
        use_global_txn_cache: bool,
    ) -> Self {
        let mut worker = Worker::new(
            WorkerConfig::default(),
            timely::communication::allocator::Thread::default(),
            Some(std::time::Instant::now()),
        );
        let (data, txns, capture, tokens) = worker.dataflow::<u64, _, _>(|scope| {
            let persist_cfg = client.cfg().clone();
            let (data_stream, shard_source_token) = scope.scoped::<u64, _, _>("hybrid", |scope| {
                let client = client.clone();
                let (data_stream, token) = shard_source::<String, (), u64, i64, _, _, _>(
                    scope,
                    name,
                    move || std::future::ready(client.clone()),
                    data_id,
                    Some(Antichain::from_elem(as_of)),
                    SnapshotMode::Include,
                    until.clone(),
                    false.then_some(|_, _: &_, _| unreachable!()),
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                    FilterResult::keep_all,
                    false.then_some(|| unreachable!()),
                    async {},
                    ErrorHandler::Halt("data_subscribe"),
                );
                (data_stream.leave(), token)
            });
            let (data, txns) = (ProbeHandle::new(), ProbeHandle::new());
            let data_stream = data_stream.flat_map(move |part| {
                let part = part.parse(persist_cfg.clone());
                part.part.map(|((k, v), t, d)| {
                    let (k, ()) = (k.unwrap(), v.unwrap());
                    (k, t, d)
                })
            });
            let data_stream = data_stream.probe_with(&data);
            // We purposely do not use the `ConfigSet` in `client` so that
            // different tests can set different values.
            let config_set = ConfigSet::default().add(&USE_GLOBAL_TXN_CACHE_SOURCE);
            let mut updates = ConfigUpdates::default();
            updates.add(&USE_GLOBAL_TXN_CACHE_SOURCE, use_global_txn_cache);
            updates.apply(&config_set);
            let (data_stream, mut txns_progress_token) =
                txns_progress::<String, (), u64, i64, _, TxnsCodecDefault, _, _>(
                    data_stream,
                    name,
                    &TxnsContext::default(),
                    &config_set,
                    || std::future::ready(client.clone()),
                    txns_id,
                    data_id,
                    as_of,
                    until,
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                );
            let data_stream = data_stream.probe_with(&txns);
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
            .with_frontier(|f| *f.as_option().unwrap_or(&u64::MAX))
    }

    /// Steps the dataflow, capturing output.
    pub fn step(&mut self) {
        self.worker.step();
        self.capture_output()
    }

    pub(crate) fn capture_output(&mut self) {
        loop {
            let event = match self.capture.try_recv() {
                Ok(x) => x,
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            };
            match event {
                Event::Progress(_) => {}
                Event::Messages(_, mut msgs) => self.output.append(&mut msgs),
            }
        }
    }

    /// Steps the dataflow past the given time, capturing output.
    #[cfg(test)]
    pub async fn step_past(&mut self, ts: u64) {
        while self.txns.less_equal(&ts) {
            tracing::trace!(
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

/// A handle to a [DataSubscribe] running in a task.
#[derive(Debug)]
pub struct DataSubscribeTask {
    /// Carries step requests. A `None` timestamp requests one step, a
    /// `Some(ts)` requests stepping until we progress beyond `ts`.
    tx: std::sync::mpsc::Sender<(
        Option<u64>,
        tokio::sync::oneshot::Sender<(Vec<(String, u64, i64)>, u64)>,
    )>,
    task: mz_ore::task::JoinHandle<Vec<(String, u64, i64)>>,
    output: Vec<(String, u64, i64)>,
    progress: u64,
}

impl DataSubscribeTask {
    /// Creates a new [DataSubscribeTask].
    pub async fn new(
        client: PersistClient,
        txns_id: ShardId,
        data_id: ShardId,
        as_of: u64,
    ) -> Self {
        let cache = TxnsCache::open(&client, txns_id, Some(data_id)).await;
        let (tx, rx) = std::sync::mpsc::channel();
        let task = mz_ore::task::spawn_blocking(
            || "data_subscribe task",
            move || Self::task(client, cache, data_id, as_of, rx),
        );
        DataSubscribeTask {
            tx,
            task,
            output: Vec::new(),
            progress: 0,
        }
    }

    #[cfg(test)]
    async fn step(&mut self) {
        self.send(None).await;
    }

    /// Steps the dataflow past the given time, capturing output.
    pub async fn step_past(&mut self, ts: u64) -> u64 {
        self.send(Some(ts)).await;
        self.progress
    }

    /// Returns captured output.
    pub fn output(&self) -> &Vec<(String, u64, i64)> {
        &self.output
    }

    async fn send(&mut self, ts: Option<u64>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send((ts, tx)).expect("task should be running");
        let (mut new_output, new_progress) = rx.await.expect("task should be running");
        self.output.append(&mut new_output);
        assert!(self.progress <= new_progress);
        self.progress = new_progress;
    }

    /// Signals for the task to exit, and then waits for this to happen.
    ///
    /// _All_ output from the lifetime of the task (not just what was previously
    /// captured) is returned.
    pub async fn finish(self) -> Vec<(String, u64, i64)> {
        // Closing the channel signals the task to exit.
        drop(self.tx);
        self.task.wait_and_assert_finished().await
    }

    fn task(
        client: PersistClient,
        cache: TxnsCache<u64>,
        data_id: ShardId,
        as_of: u64,
        rx: std::sync::mpsc::Receiver<(
            Option<u64>,
            tokio::sync::oneshot::Sender<(Vec<(String, u64, i64)>, u64)>,
        )>,
    ) -> Vec<(String, u64, i64)> {
        let mut subscribe = DataSubscribe::new(
            "DataSubscribeTask",
            client.clone(),
            cache.txns_id(),
            data_id,
            as_of,
            Antichain::new(),
            true,
        );
        let mut output = Vec::new();
        loop {
            let (ts, tx) = match rx.try_recv() {
                Ok(x) => x,
                Err(TryRecvError::Empty) => {
                    // No requests, continue stepping so nothing deadlocks.
                    subscribe.step();
                    continue;
                }
                Err(TryRecvError::Disconnected) => {
                    // All done! Return our output.
                    return output;
                }
            };
            // Always step at least once.
            subscribe.step();
            // If we got a ts, make sure to step past it.
            if let Some(ts) = ts {
                while subscribe.progress() <= ts {
                    subscribe.step();
                }
            }
            let new_output = std::mem::take(&mut subscribe.output);
            output.extend(new_output.iter().cloned());
            let _ = tx.send((new_output, subscribe.progress()));
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::{Either, Itertools};
    use mz_persist_types::Opaque;

    use crate::tests::writer;
    use crate::txns::TxnsHandle;

    use super::*;

    impl<K, V, T, D, O, C> TxnsHandle<K, V, T, D, O, C>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + TotalOrder + StepForward + Codec64 + Sync,
        D: Debug + Semigroup + Ord + Codec64 + Send + Sync,
        O: Opaque + Debug + Codec64,
        C: TxnsCodec,
    {
        async fn subscribe_task(
            &self,
            client: &PersistClient,
            data_id: ShardId,
            as_of: u64,
        ) -> DataSubscribeTask {
            DataSubscribeTask::new(client.clone(), self.txns_id(), data_id, as_of).await
        }
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn data_subscribe() {
        async fn step(subs: &mut Vec<DataSubscribeTask>) {
            for sub in subs.iter_mut() {
                sub.step().await;
            }
        }

        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();
        let d0 = ShardId::new();

        // Start a subscription before the shard gets registered.
        let mut subs = Vec::new();
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now register the shard. Also start a new subscription and step the
        // previous one (plus repeat this for every later step).
        txns.register(1, [writer(&client, d0).await]).await.unwrap();
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write something unrelated.
        let d1 = txns.expect_register(2).await;
        txns.expect_commit_at(3, d1, &["nope"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write to our shard before.
        txns.expect_commit_at(4, d0, &["4"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write to our shard at the as_of.
        txns.expect_commit_at(5, d0, &["5"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write to our shard past the as_of.
        txns.expect_commit_at(6, d0, &["6"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Now write something unrelated again.
        txns.expect_commit_at(7, d1, &["nope"], &log).await;
        subs.push(txns.subscribe_task(&client, d0, 5).await);
        step(&mut subs).await;

        // Verify that the dataflows can progress to the expected point and that
        // we read the right thing no matter when the dataflow started.
        for mut sub in subs {
            let progress = sub.step_past(7).await;
            assert_eq!(progress, 8);
            log.assert_eq(d0, 5, 8, sub.finish().await);
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
        txns.forget(3, [d0]).await.unwrap();
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
            .compare_and_append_batch(&mut [], Antichain::from_elem(6), Antichain::new(), true)
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
        txns.forget(20, [d0]).await.unwrap();
        sub.step_past(20).await;
        assert!(
            sub.progress() > 20,
            "operator should advance past 20 when shard is forgotten"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn as_of_until() {
        let client = PersistClient::new_for_tests().await;
        let mut txns = TxnsHandle::expect_open(client.clone()).await;
        let log = txns.new_log();

        let d0 = txns.expect_register(1).await;
        txns.expect_commit_at(2, d0, &["2"], &log).await;
        txns.expect_commit_at(3, d0, &["3"], &log).await;
        txns.expect_commit_at(4, d0, &["4"], &log).await;
        txns.expect_commit_at(5, d0, &["5"], &log).await;
        txns.expect_commit_at(6, d0, &["6"], &log).await;
        txns.expect_commit_at(7, d0, &["7"], &log).await;

        let until = 5;
        let mut sub = DataSubscribe::new(
            "as_of_until",
            client,
            txns.txns_id(),
            d0,
            3,
            Antichain::from_elem(until),
            true,
        );
        // Manually step the dataflow, instead of going through the
        // `DataSubscribe` helper because we're interested in all captured
        // events.
        while sub.txns.less_equal(&5) {
            sub.worker.step();
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let (actual_progresses, actual_events): (Vec<_>, Vec<_>) =
            sub.capture.into_iter().partition_map(|event| match event {
                Event::Progress(progress) => Either::Left(progress),
                Event::Messages(ts, data) => Either::Right((ts, data)),
            });
        let expected = vec![
            (3, vec![("2".to_owned(), 3, 1), ("3".to_owned(), 3, 1)]),
            (3, vec![("4".to_owned(), 4, 1)]),
        ];
        assert_eq!(actual_events, expected);

        // The number and contents of progress messages is not guaranteed and
        // depends on the downgrade behavior. The only thing we can assert is
        // the max progress timestamp, if there is one, is less than the until.
        if let Some(max_progress_ts) = actual_progresses
            .into_iter()
            .flatten()
            .map(|(ts, _diff)| ts)
            .max()
        {
            assert!(max_progress_ts < until, "{max_progress_ts} < {until}");
        }
    }
}
