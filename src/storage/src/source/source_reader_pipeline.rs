// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow raw sources.
//!
//! Raw sources are differential dataflow  collections of data directly produced by the
//! upstream service. The main export of this module is [`create_raw_source`],
//! which turns [`RawSourceCreationConfig`]s into the aforementioned streams.
//!
//! The full source, which is the _differential_ stream that represents the actual object
//! created by a `CREATE SOURCE` statement, is created by composing
//! [`create_raw_source`] with
//! decoding, `SourceEnvelope` rendering, and more.
//!

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]
#![allow(clippy::needless_borrow)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection, Hashable};
use futures::stream::StreamExt;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::channel::{InstrumentedChannelMetric, InstrumentedUnboundedReceiver};
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::now::NowFn;
use mz_ore::vec::VecExt;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::SourceError;
use mz_storage_types::sources::{SourceConnection, SourceExport, SourceTimestamp};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::capture::UnboundedTokioCapture;
use mz_timely_util::operator::StreamExt as _;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::{Broadcast, CapabilitySet, Concat, Leave, Partition};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{info, trace};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate};
use crate::metrics::StorageMetrics;
use crate::source::reclock::{ReclockBatch, ReclockError, ReclockFollower, ReclockOperator};
use crate::source::types::{SourceMessage, SourceOutput, SourceReaderError, SourceRender};
use crate::statistics::SourceStatistics;

/// Shared configuration information for all source types. This is used in the
/// `create_raw_source` functions, which produce raw sources.
#[derive(Clone)]
pub struct RawSourceCreationConfig {
    /// The name to attach to the underlying timely operator.
    pub name: String,
    /// The ID of this instantiation of this source.
    pub id: GlobalId,
    /// The details of the outputs from this ingestion.
    pub source_exports: BTreeMap<GlobalId, SourceExport<CollectionMetadata>>,
    /// The ID of the worker on which this operator is executing
    pub worker_id: usize,
    /// The total count of workers
    pub worker_count: usize,
    /// Granularity with which timestamps should be closed (and capabilities
    /// downgraded).
    pub timestamp_interval: Duration,
    /// The function to return a now time.
    pub now: NowFn,
    /// The metrics & registry that each source instantiates.
    pub metrics: StorageMetrics,
    /// Storage Metadata
    pub storage_metadata: CollectionMetadata,
    /// The upper frontier this source should resume ingestion at
    pub as_of: Antichain<mz_repr::Timestamp>,
    /// For each source export, the upper frontier this source should resume ingestion at in the
    /// system time domain.
    pub resume_uppers: BTreeMap<GlobalId, Antichain<mz_repr::Timestamp>>,
    /// For each source export, the upper frontier this source should resume ingestion at in the
    /// source time domain.
    ///
    /// Since every source has a different timestamp type we carry the timestamps of this frontier
    /// in an encoded `Vec<Row>` form which will get decoded once we reach the connection
    /// specialized functions.
    pub source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
    /// A handle to the persist client cache
    pub persist_clients: Arc<PersistClientCache>,
    /// Place to share statistics updates with storage state.
    pub source_statistics: SourceStatistics,
    /// Enables reporting the remap operator's write frontier.
    pub shared_remap_upper: Rc<RefCell<Antichain<mz_repr::Timestamp>>>,
    /// Configuration parameters, possibly from LaunchDarkly
    pub config: StorageConfiguration,
    /// The ID of this source remap/progress collection.
    pub remap_collection_id: GlobalId,
}

impl RawSourceCreationConfig {
    /// Returns the worker id responsible for handling the given partition.
    pub fn responsible_worker<P: Hash>(&self, partition: P) -> usize {
        let key = usize::cast_from((self.id, partition).hashed());
        key % self.worker_count
    }

    /// Returns true if this worker is responsible for handling the given partition.
    pub fn responsible_for<P: Hash>(&self, partition: P) -> bool {
        self.responsible_worker(partition) == self.worker_id
    }
}

/// Creates a source dataflow operator graph from a source connection. The type of SourceConnection
/// determines the type of connection that _should_ be created.
///
/// This is also the place where _reclocking_
/// (<https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210714_reclocking.md>)
/// happens.
///
/// See the [`source` module docs](crate::source) for more details about how raw
/// sources are used.
///
/// The `resume_stream` parameter will contain frontier updates whenever times are durably
/// recorded which allows the ingestion to release upstream resources.
pub fn create_raw_source<'g, G: Scope<Timestamp = ()>, C>(
    scope: &mut Child<'g, G, mz_repr::Timestamp>,
    resume_stream: &Stream<Child<'g, G, mz_repr::Timestamp>, ()>,
    config: RawSourceCreationConfig,
    source_connection: C,
    start_signal: impl std::future::Future<Output = ()> + 'static,
) -> (
    Vec<(
        Collection<Child<'g, G, mz_repr::Timestamp>, SourceOutput<C::Time>, Diff>,
        Collection<Child<'g, G, mz_repr::Timestamp>, SourceError, Diff>,
    )>,
    Stream<G, HealthStatusMessage>,
    Vec<PressOnDropButton>,
)
where
    C: SourceConnection + SourceRender + Clone + 'static,
{
    let worker_id = config.worker_id;
    let id = config.id;
    info!(
        %id,
        as_of = %config.as_of.pretty(),
        "timely-{worker_id} building source pipeline",
    );

    let mut tokens = vec![];

    let reclock_follower = ReclockFollower::new(config.as_of.clone());

    let (resume_tx, resume_rx) = config.metrics.get_instrumented_source_channel(
        config.id,
        config.worker_id,
        config.worker_count,
        "resume_upper_reclocking",
    );
    let (source_tx, source_rx) = config.metrics.get_instrumented_source_channel(
        config.id,
        config.worker_id,
        config.worker_count,
        "source_data",
    );
    let (source_upper_tx, source_upper_rx) = config.metrics.get_instrumented_source_channel(
        config.id,
        config.worker_id,
        config.worker_count,
        "source_upper",
    );

    // The use of an _unbounded_ queue here is justified as it matches the unbounded buffers that
    // lie between ordinary timely operators.
    resume_stream.capture_into(UnboundedTokioCapture(resume_tx));
    let reclocked_resume_stream =
        reclock_resume_upper(resume_rx, reclock_follower.share(), worker_id, id);

    let timestamp_desc = source_connection.timestamp_desc();

    let (health, source_tokens) = {
        let config = config.clone();
        scope.parent.scoped("SourceTimeDomain", move |scope| {
            let (source, source_upper, health_stream, source_tokens) = source_render_operator(
                scope,
                config.clone(),
                source_connection,
                reclocked_resume_stream,
                start_signal,
            );

            // The use of an _unbounded_ queue here is justified as it matches the unbounded
            // buffers that lie between ordinary timely operators.
            source.inner.capture_into(UnboundedTokioCapture(source_tx));
            source_upper.capture_into(UnboundedTokioCapture(source_upper_tx));

            (health_stream.leave(), source_tokens)
        })
    };
    tokens.extend(source_tokens);

    let (remap_stream, remap_token) =
        remap_operator(scope, config.clone(), source_upper_rx, timestamp_desc);
    tokens.push(remap_token);

    let streams = reclock_operator(scope, config, reclock_follower, source_rx, remap_stream);

    (streams, health, tokens)
}

/// Renders the source dataflow fragment from the given [SourceConnection]. This returns a
/// collection timestamped with the source specific timestamp type. Also returns a second stream
/// that can be used to learn about the `source_upper` that all the source reader instances know
/// about. This second stream will be used by the `remap_operator` to mint new timestamp bindings
/// into the remap shard.
fn source_render_operator<G, C>(
    scope: &mut G,
    config: RawSourceCreationConfig,
    source_connection: C,
    resume_uppers: impl futures::Stream<Item = Antichain<C::Time>> + 'static,
    start_signal: impl std::future::Future<Output = ()> + 'static,
) -> (
    Collection<G, (usize, Result<SourceMessage, SourceReaderError>), Diff>,
    Stream<G, Infallible>,
    Stream<G, HealthStatusMessage>,
    Vec<PressOnDropButton>,
)
where
    G: Scope<Timestamp = C::Time>,
    C: SourceRender + 'static,
{
    let source_id = config.id;
    let worker_id = config.worker_id;
    let source_statistics = config.source_statistics.clone();

    let resume_uppers = resume_uppers.inspect(move |upper| {
        let upper = upper.pretty();
        trace!(%upper, "timely-{worker_id} source({source_id}) received resume upper");
    });

    let (input_data, progress, health, stats, tokens) =
        source_connection.render(scope, config, resume_uppers, start_signal);

    crate::source::statistics::process_statistics(
        scope.clone(),
        source_id,
        worker_id,
        stats,
        source_statistics.clone(),
    );

    let name = format!("SourceGenericStats({})", source_id);
    let mut builder = AsyncOperatorBuilder::new(name, scope.clone());

    let (mut data_output, data) = builder.new_output();
    let (progress_output, derived_progress) = builder.new_output();
    let mut data_input = builder.new_input_for_many(
        &input_data.inner,
        Pipeline,
        [&data_output, &progress_output],
    );
    let (mut health_output, derived_health) = builder.new_output();

    builder.build(move |mut caps| async move {
        let health_cap = caps.pop().unwrap();
        drop(caps);

        let mut statuses_by_idx = BTreeMap::new();

        while let Some(event) = data_input.next().await {
            let AsyncEvent::Data([cap_data, _cap_progress], mut data) = event else {
                continue;
            };
            for ((output_index, message), _, diff) in data.iter() {
                let status = match message {
                    Ok(_) => HealthStatusUpdate::running(),
                    // All errors coming into the data stream are definite.
                    // Downstream consumers of this data will preserve this
                    // status.
                    Err(ref error) => {
                        mz_ore::soft_assert_or_log!(
                            *diff > 0,
                            "unexpected retraction of definite error"
                        );
                        HealthStatusUpdate::ceasing(error.inner.to_string())
                    }
                };

                let statuses: &mut Vec<_> = statuses_by_idx.entry(*output_index).or_default();

                let status = HealthStatusMessage {
                    index: *output_index,
                    namespace: C::STATUS_NAMESPACE.clone(),
                    update: status,
                };
                if statuses.last() != Some(&status) {
                    statuses.push(status);
                }

                match message {
                    Ok(message) => {
                        source_statistics.inc_messages_received_by(1);
                        let key_len = u64::cast_from(message.key.byte_len());
                        let value_len = u64::cast_from(message.value.byte_len());
                        source_statistics.inc_bytes_received_by(key_len + value_len);
                    }
                    Err(_) => {}
                }
            }
            data_output.give_container(&cap_data, &mut data).await;

            for statuses in statuses_by_idx.values_mut() {
                if statuses.is_empty() {
                    continue;
                }

                health_output.give_container(&health_cap, statuses).await;
                statuses.clear()
            }
        }
    });

    (
        data.as_collection(),
        progress.unwrap_or(derived_progress),
        health.concat(&derived_health),
        tokens,
    )
}

struct RemapClock {
    now: NowFn,
    update_interval_ms: u64,
    upper: Antichain<mz_repr::Timestamp>,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl RemapClock {
    fn new(now: NowFn, update_interval: Duration) -> Self {
        Self {
            now,
            update_interval_ms: update_interval
                .as_millis()
                .try_into()
                .expect("huge duration"),
            upper: Antichain::from_elem(Timestamp::minimum()),
            sleep: Box::pin(tokio::time::sleep_until(tokio::time::Instant::now())),
        }
    }
}

impl futures::Stream for RemapClock {
    type Item = (mz_repr::Timestamp, Antichain<mz_repr::Timestamp>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            futures::ready!(self.sleep.as_mut().poll(cx));
            let now = (self.now)();
            let mut new_ts = now - now % self.update_interval_ms;
            if (now % self.update_interval_ms) != 0 {
                new_ts += self.update_interval_ms;
            }
            let new_ts: mz_repr::Timestamp = new_ts.try_into().expect("must fit");

            if self.upper.less_equal(&new_ts) {
                self.upper = Antichain::from_elem(new_ts.step_forward());
                return Poll::Ready(Some((new_ts, self.upper.clone())));
            } else {
                let upper_ts = self.upper.as_option().expect("no more timestamps to mint");
                let upper: u64 = upper_ts.into();
                let deadline = tokio::time::Instant::now()
                    .checked_add(Duration::from_millis(upper - now))
                    .unwrap();
                self.sleep.as_mut().reset(deadline);
            }
        }
    }
}

/// Mints new contents for the remap shard based on summaries about the source
/// upper it receives from the raw reader operators.
///
/// Only one worker will be active and write to the remap shard. All source
/// upper summaries will be exchanged to it.
fn remap_operator<G, FromTime, M>(
    scope: &G,
    config: RawSourceCreationConfig,
    mut source_upper_rx: InstrumentedUnboundedReceiver<Event<FromTime, Infallible>, M>,
    remap_relation_desc: RelationDesc,
) -> (Collection<G, FromTime, Diff>, PressOnDropButton)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    FromTime: SourceTimestamp,
    M: InstrumentedChannelMetric + 'static,
{
    let RawSourceCreationConfig {
        name,
        id,
        source_exports: _,
        worker_id,
        worker_count,
        timestamp_interval,
        storage_metadata,
        as_of,
        resume_uppers: _,
        source_resume_uppers: _,
        metrics: _,
        now,
        persist_clients,
        source_statistics: _,
        shared_remap_upper,
        config: _,
        remap_collection_id,
    } = config;

    let chosen_worker = usize::cast_from(id.hashed() % u64::cast_from(worker_count));
    let active_worker = chosen_worker == worker_id;

    let operator_name = format!("remap({})", id);
    let mut remap_op = AsyncOperatorBuilder::new(operator_name, scope.clone());
    let (mut remap_output, remap_stream) = remap_op.new_output();

    let button = remap_op.build(move |capabilities| async move {
        if !active_worker {
            // This worker is not writing, so make sure it's "taken out" of the
            // calculation by advancing to the empty frontier.
            shared_remap_upper.borrow_mut().clear();
            return;
        }

        let mut cap_set = CapabilitySet::from_elem(capabilities.into_element());

        let remap_handle = crate::source::reclock::compat::PersistHandle::<FromTime, _>::new(
            Arc::clone(&persist_clients),
            storage_metadata.clone(),
            as_of.clone(),
            shared_remap_upper,
            id,
            "remap",
            worker_id,
            worker_count,
            remap_relation_desc,
            remap_collection_id,
        )
        .await
        .unwrap_or_else(|e| panic!("Failed to create remap handle for source {}: {}", name, e.display_with_causes()));
        let clock = RemapClock::new(now.clone(), timestamp_interval);
        let (mut timestamper, mut initial_batch) = ReclockOperator::new(remap_handle, clock).await;

        let mut source_upper = MutableAntichain::new_bottom(FromTime::minimum());

        // Emit initial snapshot of the remap_shard, bootstrapping
        // downstream reclock operators.
        trace!(
            "timely-{worker_id} remap({id}) emitting remap snapshot: \
                source_upper={} \
                trace_updates={:?}",
            source_upper.pretty(),
            &initial_batch.updates
        );

        let cap = cap_set.delayed(cap_set.first().unwrap());
        remap_output.give_container(&cap, &mut initial_batch.updates).await;
        drop(cap);
        cap_set.downgrade(initial_batch.upper);

        let mut ticker = tokio::time::interval(timestamp_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while !cap_set.is_empty() {
            // AsyncInputHandle::next is cancel safe
            tokio::select! {
                // We only take this branch of the source upper frontier is not the minimum
                // frontier. This makes it so the first binding corresponds to the snapshot of the
                // source, and because the first binding always maps to the minimum *target*
                // frontier we guarantee that the source will never appear empty.
                _ = ticker.tick(), if *source_upper.frontier() != [FromTime::minimum()] => {
                    let mut remap_trace_batch = timestamper.mint(source_upper.frontier()).await;

                    trace!(
                        "timely-{worker_id} remap({id}) minted new bindings: \
                        updates={:?} \
                        source_upper={} \
                        trace_upper={}",
                        &remap_trace_batch.updates,
                        source_upper.pretty(),
                        remap_trace_batch.upper.pretty()
                    );

                    let cap = cap_set.delayed(cap_set.first().unwrap());
                    remap_output.give_container(&cap, &mut remap_trace_batch.updates).await;

                    // If the last remap trace closed the input, we no longer
                    // need to (or can) advance the timestamper.
                    if remap_trace_batch.upper.is_empty() {
                        return;
                    }

                    cap_set.downgrade(remap_trace_batch.upper);

                    let mut remap_trace_batch = timestamper.advance().await;

                    let cap = cap_set.delayed(cap_set.first().unwrap());
                    remap_output.give_container(&cap, &mut remap_trace_batch.updates).await;

                    cap_set.downgrade(remap_trace_batch.upper);
                }
                Some(event) = source_upper_rx.recv() => {
                    let head = std::iter::once(event);
                    let tail = std::iter::from_fn(|| source_upper_rx.try_recv().ok());
                    let progress = head.chain(tail).flat_map(|event| match event {
                        Event::Progress(progress) => progress,
                        Event::Messages(_, _) => unreachable!(),
                    });
                    source_upper.update_iter(progress);
                    trace!("timely-{worker_id} remap({id}) received source upper: {}", source_upper.pretty());
                }
            }
        }
    });

    (remap_stream.as_collection(), button.press_on_drop())
}

/// Receives un-timestamped batches from the source reader and updates to the
/// remap trace on a second input. This operator takes the remap information,
/// reclocks incoming batches and sends them forward.
fn reclock_operator<G, FromTime, D, M>(
    scope: &G,
    config: RawSourceCreationConfig,
    mut timestamper: ReclockFollower<FromTime, mz_repr::Timestamp>,
    mut source_rx: InstrumentedUnboundedReceiver<
        Event<
            FromTime,
            (
                (usize, Result<SourceMessage, SourceReaderError>),
                FromTime,
                D,
            ),
        >,
        M,
    >,
    remap_trace_updates: Collection<G, FromTime, Diff>,
) -> Vec<(
    Collection<G, SourceOutput<FromTime>, D>,
    Collection<G, SourceError, Diff>,
)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    FromTime: SourceTimestamp,
    D: Semigroup + Into<Diff>,
    M: InstrumentedChannelMetric + 'static,
{
    let RawSourceCreationConfig {
        name,
        id,
        source_exports,
        worker_id,
        worker_count: _,
        timestamp_interval: _,
        storage_metadata: _,
        as_of: _,
        resume_uppers,
        source_resume_uppers: _,
        metrics,
        now: _,
        persist_clients: _,
        source_statistics: _,
        shared_remap_upper: _,
        config: _,
        remap_collection_id: _,
    } = config;

    // TODO(guswynn): expose function
    let bytes_read_counter = metrics.source_defs.bytes_read.clone();

    let operator_name = format!("reclock({})", id);
    let mut reclock_op = AsyncOperatorBuilder::new(operator_name, scope.clone());
    let (mut reclocked_output, reclocked_stream) = reclock_op.new_output();

    // Need to broadcast the remap changes to all workers.
    let remap_trace_updates = remap_trace_updates.inner.broadcast();
    let mut remap_input = reclock_op.new_disconnected_input(&remap_trace_updates, Pipeline);

    reclock_op.build(move |capabilities| async move {
        // The capability of the output after reclocking the source frontier
        let mut cap_set = CapabilitySet::from_elem(capabilities.into_element());

        let source_metrics = metrics.get_source_metrics(&name, id, worker_id);

        // Compute the overall resume upper to report for the ingestion
        let resume_upper = Antichain::from_iter(resume_uppers.values().flat_map(|f| f.iter().cloned()));
        source_metrics.resume_upper.set(mz_persist_client::metrics::encode_ts_metric(&resume_upper));

        let mut source_upper = MutableAntichain::new_bottom(FromTime::minimum());

        // Stash of batches that have not yet been timestamped.
        type Batch<T, D> = Vec<((usize, Result<SourceMessage, SourceReaderError>), T, D)>;
        let mut untimestamped_batches: Vec<(FromTime, Batch<FromTime, D>)> = Vec::new();

        // Stash of reclock updates that are still beyond the upper frontier
        let mut remap_updates_stash = vec![];
        let work_to_do = tokio::sync::Notify::new();
        loop {
            tokio::select! {
                Some(event) = remap_input.next() => match event {
                    AsyncEvent::Data(_cap, mut data) => remap_updates_stash.append(&mut data),
                    // If the remap frontier advanced it's time to carve out a batch that includes
                    // all updates not beyond the upper
                    AsyncEvent::Progress(remap_upper) => {
                        let remap_trace_batch = ReclockBatch {
                            updates: remap_updates_stash
                                .drain_filter_swapping(|(_, ts, _)| !remap_upper.less_equal(ts))
                                .collect(),
                            upper: remap_upper.to_owned(),
                        };
                        trace!(
                            "timely-{worker_id} reclock({id}) \
                            received remap batch: updates={:?} upper={}",
                            &remap_trace_batch.updates,
                            remap_upper.pretty()
                        );
                        timestamper.push_trace_batch(remap_trace_batch);
                        work_to_do.notify_one();
                    }
                },
                Some(event) = source_rx.recv() => match event {
                    Event::Progress(changes) => {
                        // In some sense, this is the core place where we connect the two scopes
                        // (the source-timestamp one, and the `mz_repr::Timestamp` one).
                        //
                        // The source reader produces messages using normal capabilities, which are
                        // `Capture::capture`'d into the sender-side of the `source_rx` channel.
                        // While `Messages` may be received out of order, timely ensures that
                        // `Progress` messages represent frontiers that later `Messages` are never
                        // beyond (note that these times can be, and in our case ARE, partially
                        // ordered).
                        //
                        // This is in fact the _core_ behavior that timely frontier tracking
                        // offers, and it allows us to in some sense, "not think" about timestamps
                        // here, and simply update the `MutableAntichain`, which will be
                        // interpreted by the `ReclockFollower`.
                        //
                        // Effectively, we let timely and the `reclock` module worry about partial
                        // orders, and simply write "classic" timely code here, whereby we store
                        // messages until we see frontiers progress.
                        source_upper.update_iter(changes);
                        trace!(
                            "timely-{worker_id} reclock({id}) \
                            received source progress: source_upper={}",
                            source_upper.pretty()
                        );
                        work_to_do.notify_one();
                    }
                    Event::Messages(time, batch) => {
                        untimestamped_batches.push((time, batch))
                    }
                },
                _ = work_to_do.notified(), if timestamper.initialized() => {
                    source_metrics.inmemory_remap_bindings.set(u64::cast_from(timestamper.size()));

                    // Drain all messages that can be reclocked from all the batches
                    let total_buffered: usize = untimestamped_batches.iter().map(|(_, b)| b.len()).sum();
                    let reclock_source_upper = timestamper.source_upper();

                    // Peel as many consequtive reclockable items as possible. It is not benefitial
                    // to go further even if theoretically there may be more messages ready to be
                    // reclocked further along because in the common case the message order is
                    // correleated with time and therefore in the common case we would be wasting
                    // work trying to compare all the buffered messages with the frontier.
                    let mut reclockable_count = untimestamped_batches
                        .iter()
                        .flat_map(|(_, batch)| batch)
                        .take_while(|(_, ts, _)| !reclock_source_upper.less_equal(ts))
                        .count();

                    let msgs = untimestamped_batches
                        .iter_mut()
                        .flat_map(|(_, batch)| {
                            let drain_count = std::cmp::min(batch.len(), reclockable_count);
                            reclockable_count = reclockable_count.saturating_sub(drain_count);
                            batch.drain(0..drain_count)
                        })
                        .map(|(data, time, diff)| ((data, time.clone(), diff), time));

                    // Accumulate updates to bytes_read for Prometheus metrics collection
                    let mut bytes_read = 0;

                    let mut total_processed = 0;
                    for (((idx, msg), from_ts, diff), into_ts) in timestamper.reclock(msgs) {
                        let into_ts = into_ts.expect("reclock for update not beyond upper failed");
                        let output = match msg {
                            Ok(message) => {
                                bytes_read += message.key.byte_len() + message.value.byte_len();
                                let ok = SourceOutput {
                                    key: message.key,
                                    value: message.value,
                                    metadata: message.metadata,
                                    from_time: from_ts,
                                };
                                (idx, Ok(ok))
                            }
                            Err(SourceReaderError { inner }) => {
                                let err = SourceError {
                                    source_id: id,
                                    error: inner,
                                };
                                (idx, Err(err))
                            }
                        };

                        let ts_cap = cap_set.delayed(&into_ts);
                        reclocked_output.give(&ts_cap, (output, into_ts, diff)).await;
                        total_processed += 1;
                    }
                    // The loop above might have completely emptied batches. We can now remove them
                    untimestamped_batches.retain(|(_, batch)| !batch.is_empty());

                    let total_skipped = total_buffered - total_processed;
                    trace!(
                        "timely-{worker_id} reclock({id}): processed {}, skipped {} messages",
                        total_processed,
                        total_skipped
                    );

                    bytes_read_counter.inc_by(u64::cast_from(bytes_read));

                    // This is correct for totally ordered times because there can be at
                    // most one entry in the `CapabilitySet`. If this ever changes we
                    // need to rethink how we surface this in metrics. We will notice
                    // when that happens because the `expect()` will fail.
                    source_metrics.capability.set(
                        cap_set
                            .iter()
                            .at_most_one()
                            .expect("there can be at most one element for totally ordered times")
                            .map(|c| c.time())
                            .cloned()
                            .unwrap_or(mz_repr::Timestamp::MAX)
                            .into(),
                    );


                    // We must downgrade our capability to the meet of the timestamper frontier,
                    // the source frontier, and the lower timestamp of all the pending batches
                    // because it's only when both advance past some time `t` that we are
                    // guaranteed that we'll not need to produce more data at time `t`.
                    let mut ready_upper = reclock_source_upper;
                    ready_upper.extend(
                        source_upper.frontier().iter().cloned()
                        .chain(untimestamped_batches.iter().map(|(time, _)| time.clone()))
                    );

                    let into_ready_upper = timestamper
                        .reclock_frontier(ready_upper.borrow())
                        .expect("uninitialized reclock follower");
                    trace!(
                        "timely-{worker_id} reclock({id}) downgrading timestamper: since={}",
                        into_ready_upper.pretty()
                    );

                    cap_set.downgrade(into_ready_upper.elements());
                    timestamper.compact(into_ready_upper.clone());
                    if into_ready_upper.is_empty() {
                        return;
                    }
                }
            }
        }
    });

    // TODO(petrosagg): output the two streams directly
    let (ok_muxed_stream, err_muxed_stream) =
        reclocked_stream.map_fallible("reclock-demux-ok-err", |((output, r), ts, diff)| match r {
            Ok(ok) => Ok(((output, ok), ts, diff)),
            Err(err) => Err(((output, err), ts, diff.into())),
        });

    // We use the output index from the source export to route values to its ok and err streams. We
    // do this obliquely by generating as many partitions as there are output indices and then
    // dropping all unused partitions.
    let partition_count = u64::cast_from(
        source_exports
            .iter()
            .map(|(_, SourceExport { output_index, .. })| *output_index)
            .max()
            .unwrap_or_default()
            + 1,
    );

    let ok_streams: Vec<_> = ok_muxed_stream
        .partition(partition_count, |((output, data), time, diff)| {
            (u64::cast_from(output), (data, time, diff))
        })
        .into_iter()
        .map(|stream| stream.as_collection())
        .collect();

    let err_streams: Vec<_> = err_muxed_stream
        .partition(partition_count, |((output, err), time, diff)| {
            (u64::cast_from(output), (err, time, diff))
        })
        .into_iter()
        .map(|stream| stream.as_collection())
        .collect();

    ok_streams.into_iter().zip_eq(err_streams).collect()
}

/// Reclocks an `IntoTime` frontier stream into a `FromTime` frontier stream. This is used for the
/// virtual (through persist) feedback edge so that we convert the `IntoTime` resumption frontier
/// into the `FromTime` frontier that is used with the source's `OffsetCommiter`.
///
/// Note that we also use this async-`Stream` converter as a convenient place to compact the
/// `ReclockFollower` trace that is currently shared between this and the `reclock_operator`.
fn reclock_resume_upper<FromTime, IntoTime, M>(
    mut resume_rx: InstrumentedUnboundedReceiver<Event<IntoTime, ()>, M>,
    mut reclock_follower: ReclockFollower<FromTime, IntoTime>,
    worker_id: usize,
    id: GlobalId,
) -> impl futures::stream::Stream<Item = Antichain<FromTime>>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + Display,
    M: InstrumentedChannelMetric,
{
    async_stream::stream!({
        let mut resume_upper = MutableAntichain::new_bottom(Timestamp::minimum());
        let mut prev_upper = reclock_follower.since().to_owned();

        while let Some(event) = resume_rx.recv().await {
            if let Event::Progress(changes) = event {
                resume_upper.update_iter(changes);

                let source_upper = loop {
                    match reclock_follower.source_upper_at_frontier(resume_upper.frontier()) {
                        Ok(frontier) => break frontier,
                        Err(ReclockError::BeyondUpper(_) | ReclockError::Uninitialized) => {
                            // Note that we can hold back ingestion of the `resume_rx`
                            // indefinitely, because it will not produce unbounded progress
                            // messages unless the reclock trace is being progressed, which will
                            // unblock this loop!
                            tokio::time::sleep(Duration::from_millis(100)).await
                        }
                        Err(err) => panic!("unexpected reclock error {:?}", err),
                    }
                };

                trace!(
                    "timely-{worker_id} source({id}) converted resume upper: into_upper={} source_upper={}",
                    resume_upper.pretty(), source_upper.pretty()
                );

                // In order to *invert* a frontier the since frontier must be strictly less than
                // the frontier we're inverting. This means we can't compact all the way to
                // `resume_upper`, since that would make it potentially equal and lead to wrong
                // results. Since this is a generic method we can't do the "time minus one" thing
                // we usually do. So instead we keep track of the previous since frontier and we
                // use this lagging frontier to compact our trace.
                if PartialOrder::less_than(&prev_upper.borrow(), &resume_upper.frontier()) {
                    reclock_follower.compact(prev_upper.clone());
                    prev_upper = resume_upper.frontier().to_owned();
                }

                yield source_upper;
            }
        }
    })
}
