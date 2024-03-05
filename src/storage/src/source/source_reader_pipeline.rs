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
use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection, Hashable};
use futures::stream::StreamExt;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::channel::{InstrumentedChannelMetric, InstrumentedUnboundedReceiver};
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::now::NowFn;
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
use mz_timely_util::capture::{PusherCapture, UnboundedTokioCapture};
use mz_timely_util::operator::StreamExt as _;
use timely::container::CapacityContainerBuilder;
use mz_timely_util::reclock::reclock;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Broadcast, CapabilitySet, Concat, Map, Inspect, Leave, Partition};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio_stream::wrappers::WatchStream;
use tracing::{info, trace};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate};
use crate::metrics::source::SourceMetrics;
use crate::metrics::StorageMetrics;
use crate::source::reclock::ReclockOperator;
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
    committed_upper: &Stream<Child<'g, G, mz_repr::Timestamp>, ()>,
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

    let (source_upper_tx, source_upper_rx) = config.metrics.get_instrumented_source_channel(
        config.id,
        config.worker_id,
        config.worker_count,
        "source_upper",
    );

    let source_metrics = Arc::new(
        config
            .metrics
            .get_source_metrics(&config.name, id, worker_id),
    );

    let timestamp_desc = source_connection.timestamp_desc();

    let (remap_collection, remap_token) =
        remap_operator(scope, config.clone(), source_upper_rx, timestamp_desc);
    // Need to broadcast the remap changes to all workers.
    let remap_collection = remap_collection.inner.broadcast().as_collection();
    tokens.push(remap_token);

    let committed_upper = reclock_committed_upper(
        &remap_collection,
        config.as_of.clone(),
        committed_upper,
        id,
        Arc::clone(&source_metrics),
    );

    let (reclock_pusher, reclocked) = reclock(
        &remap_collection,
        config.as_of.clone(),
    );

    let streams = demux_subsources(config.clone(), reclocked);

    let (health, source_tokens) = {
        let config = config.clone();
        scope.parent.scoped("SourceTimeDomain", move |scope| {
            let (source, source_upper, health_stream, source_tokens) = source_render_operator(
                scope,
                config.clone(),
                source_connection,
                committed_upper,
                start_signal,
            );

            source.inner.map(move |((output, result), from_time, diff)| {
                let result = match result {
                    Ok(msg) => Ok(SourceOutput {
                        key: msg.key,
                        value: msg.value,
                        metadata: msg.metadata,
                        from_time: from_time.clone(),
                    }),
                    Err(SourceReaderError { inner }) => Err(SourceError {
                        source_id: id,
                        error: inner,
                    }),
                };
                ((output, result), from_time, diff)
            }).capture_into(PusherCapture(reclock_pusher));
            // The use of an _unbounded_ queue here is justified as it matches the unbounded
            // buffers that lie between ordinary timely operators.
            source_upper.capture_into(UnboundedTokioCapture(source_upper_tx));

            (health_stream.leave(), source_tokens)
        })
    };
    tokens.extend(source_tokens);

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

    let (mut data_output, data) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (progress_output, derived_progress) = builder.new_output::<CapacityContainerBuilder<_>>();
    let mut data_input = builder.new_input_for_many(
        &input_data.inner,
        Pipeline,
        [&data_output, &progress_output],
    );
    let (mut health_output, derived_health) = builder.new_output::<CapacityContainerBuilder<_>>();

    builder.build(move |mut caps| async move {
        let health_cap = caps.pop().unwrap();
        drop(caps);

        let mut statuses_by_idx = BTreeMap::new();

        while let Some(event) = data_input.next().await {
            let AsyncEvent::Data([cap_data, _cap_progress], mut data) = event else {
                continue;
            };
            for ((output_index, message), _, _) in data.iter() {
                let status = match message {
                    Ok(_) => HealthStatusUpdate::running(),
                    // All errors coming into the data stream are definite.
                    // Downstream consumers of this data will preserve this
                    // status.
                    Err(ref error) => HealthStatusUpdate::stalled(
                        error.inner.to_string(),
                        Some("retracting the errored value may resume the source".to_string()),
                    ),
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
    tick_interval_ms: u64,
    upper: Antichain<mz_repr::Timestamp>,
    sleep: Pin<Box<tokio::time::Sleep>>,
}

impl RemapClock {
    fn new(now: NowFn, tick_interval: Duration) -> Self {
        Self {
            now,
            tick_interval_ms: tick_interval.as_millis().try_into().expect("huge duration"),
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
            let mut new_ts = now - now % self.tick_interval_ms;
            if (now % self.tick_interval_ms) != 0 {
                new_ts += self.tick_interval_ms;
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
    mut source_upper_rx: InstrumentedUnboundedReceiver<Event<FromTime, Vec<Infallible>>, M>,
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
    let (mut remap_output, remap_stream) = remap_op.new_output::<CapacityContainerBuilder<_>>();

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

/// Demultiplexes a combined stream of all subsources into individual collections per subsource
fn demux_subsources<G, FromTime>(
    config: RawSourceCreationConfig,
    input: Collection<G, (usize, Result<SourceOutput<FromTime>, SourceError>), Diff>,
) -> Vec<(
    Collection<G, SourceOutput<FromTime>, Diff>,
    Collection<G, SourceError, Diff>,
)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    FromTime: SourceTimestamp,
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
    let source_metrics = metrics.get_source_metrics(&name, id, worker_id);

    // Compute the overall resume upper to report for the ingestion
    let resume_upper = Antichain::from_iter(resume_uppers.values().flat_map(|f| f.iter().cloned()));
    source_metrics
        .resume_upper
        .set(mz_persist_client::metrics::encode_ts_metric(&resume_upper));

    let input = input.inner.inspect_core(move |event| match event {
        Ok((_, data)) => {
            for ((_idx, result), _time, _diff) in data.iter() {
                if let Ok(msg) = result {
                    bytes_read_counter.inc_by(u64::cast_from(msg.key.byte_len()));
                    bytes_read_counter.inc_by(u64::cast_from(msg.value.byte_len()));
                }
            }
        }
        Err([time]) => source_metrics.capability.set(time.into()),
        Err([]) => source_metrics
            .capability
            .set(mz_repr::Timestamp::MAX.into()),
        // `mz_repr::Timestamp` is totally ordered and so there can be at most one element in the
        // frontier. If this ever changes we need to rethink how we surface this in metrics. We
        // will notice when that happens because the `expect()` will fail.
        Err(_) => unreachable!("there can be at most one element for totally ordered times"),
    });

    // TODO(petrosagg): output the two streams directly
    let (ok_muxed_stream, err_muxed_stream) =
        input.map_fallible("reclock-demux-ok-err", |((output, r), ts, diff)| match r {
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
fn reclock_committed_upper<G, FromTime>(
    bindings: &Collection<G, FromTime, Diff>,
    as_of: Antichain<G::Timestamp>,
    committed_upper: &Stream<G, ()>,
    id: GlobalId,
    metrics: Arc<SourceMetrics>,
) -> impl futures::stream::Stream<Item = Antichain<FromTime>> + 'static
where
    G: Scope,
    G::Timestamp: Lattice + TotalOrder,
    FromTime: SourceTimestamp,
{
    let (tx, rx) = tokio::sync::watch::channel(Antichain::from_elem(FromTime::minimum()));
    let scope = bindings.scope().clone();

    let name = format!("ReclockCommitUpper({id})");
    let mut builder = OperatorBuilder::new(name, scope);

    let mut bindings = builder.new_input(&bindings.inner, Pipeline);
    let _ = builder.new_input(committed_upper, Pipeline);

    builder.build(move |_| {
        // Remap bindings beyond the upper
        let mut accepted_times = Vec::new();
        // The upper frontier of the bindings
        let mut upper = Antichain::from_elem(Timestamp::minimum());
        // Remap bindings not beyond upper
        let mut ready_times = VecDeque::new();
        let mut source_upper = MutableAntichain::new();

        let mut vector = Vec::new();
        move |frontiers| {
            // Accept new bindings
            while let Some((_, data)) = bindings.next() {
                data.swap(&mut vector);
                accepted_times.extend(vector.drain(..).map(|(from, mut into, diff)| {
                    into.advance_by(as_of.borrow());
                    (from, into, diff)
                }));
            }
            // Extract ready bindings
            let new_upper = frontiers[0].frontier();
            if PartialOrder::less_than(&upper.borrow(), &new_upper) {
                accepted_times.sort_unstable_by(|a, b| a.1.cmp(&b.1));
                // The times are totally ordered so we can binary search to find the prefix that is
                // not beyond the upper and extract it into a batch.
                let idx = accepted_times.partition_point(|(_, t, _)| !upper.less_equal(t));
                ready_times.extend(accepted_times.drain(0..idx));
                upper = new_upper.to_owned();
            }

            // The received times only accumulate correctly for times beyond the as_of.
            if as_of.iter().all(|t| !upper.less_equal(t)) {
                let committed_upper = frontiers[1].frontier();
                if as_of.iter().all(|t| !committed_upper.less_equal(t)) {
                    // We have committed this source up until `committed_upper`. Because we have
                    // required that IntoTime is a total order this will be either a singleton set
                    // or the empty set.
                    //
                    // * Case 1: committed_upper is the empty set {}
                    //
                    // There won't be any future IntoTime timestamps that we will produce so we can
                    // provide feedback to the source that it can forget about everything.
                    //
                    // * Case 2: committed_upper is a singleton set {t_next}
                    //
                    // We know that t_next cannot be the minimum timestamp because we have required
                    // that all times of the as_of frontier are not beyond some time of
                    // committed_upper. Therefore t_next has a predecessor timestamp t_prev.
                    //
                    // We don't know what remap[t_next] is yet, but we do know that we will have to
                    // emit all source updates `u: remap[t_prev] <= time(u) <= remap[t_next]`.
                    // Since `t_next` is the minimum undetermined timestamp and we know that t1 <=
                    // t2 => remap[t1] <= remap[t2] we know that we will never need any source
                    // updates `u: !(remap[t_prev] <= time(u))`.
                    //
                    // Therefore we can provide feedback to the source that it can forget about any
                    // updates that are not beyond remap[t_prev].
                    //
                    // Important: We are *NOT* saying that the source can *compact* its data using
                    // remap[t_prev] as the compaction frontier. If the source were to compact its
                    // collection to remap[t_prev] we would lose the distinction between updates
                    // that happened *at* t_prev versus updates that happened ealier and were
                    // advanced to t_prev. If the source needs to communicate a compaction frontier
                    // upstream then the specific source implementation needs to further adjust the
                    // reclocked committed_upper and calculate a suitable compaction frontier in
                    // the same way we adjust uppers of collections in the controller with the
                    // LagWriteFrontier read policy.
                    //
                    // == What about IntoTime times that are general lattices?
                    //
                    // Reversing the upper for a general lattice is much more involved but it boils
                    // down to computing the meet of all the times in `committed_upper` and then
                    // treating that as `t_next` (I think). Until we need to deal with that though
                    // we can just assume TotalOrder.
                    let reclocked_upper = match committed_upper.as_option() {
                        Some(t_next) => {
                            let idx = ready_times.partition_point(|(_, t, _)| t < t_next);
                            let updates = ready_times
                                .drain(0..idx)
                                .map(|(from_time, _, diff)| (from_time, diff));
                            source_upper.update_iter(updates);
                            // At this point source_upper contains all updates that are less than
                            // t_next, which is equal to remap[t_prev]
                            source_upper.frontier().to_owned()
                        }
                        None => Antichain::new(),
                    };
                    tx.send_replace(reclocked_upper);
                }
            }

            metrics
                .commit_upper_accepted_times
                .set(u64::cast_from(accepted_times.len()));
            metrics
                .commit_upper_ready_times
                .set(u64::cast_from(ready_times.len()));
        }
    });

    WatchStream::from_changes(rx)
}
