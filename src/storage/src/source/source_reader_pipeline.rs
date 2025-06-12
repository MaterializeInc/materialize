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
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection, Hashable};
use futures::stream::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::dyncfgs;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::{SourceConnection, SourceExport, SourceTimestamp};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::capture::PusherCapture;
use mz_timely_util::operator::ConcatenateFlatten;
use mz_timely_util::reclock::reclock;
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::{Event, EventPusher};
use timely::dataflow::operators::core::Map as _;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::{Broadcast, CapabilitySet, Inspect, Leave};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::{Semaphore, watch};
use tokio_stream::wrappers::WatchStream;
use tracing::trace;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate};
use crate::metrics::StorageMetrics;
use crate::metrics::source::SourceMetrics;
use crate::source::probe;
use crate::source::reclock::ReclockOperator;
use crate::source::types::{Probe, SourceMessage, SourceOutput, SourceRender, StackedCollection};
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
    pub now_fn: NowFn,
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
    /// Collection of `SourceStatistics` for source and exports to share updates.
    pub statistics: BTreeMap<GlobalId, SourceStatistics>,
    /// Enables reporting the remap operator's write frontier.
    pub shared_remap_upper: Rc<RefCell<Antichain<mz_repr::Timestamp>>>,
    /// Configuration parameters, possibly from LaunchDarkly
    pub config: StorageConfiguration,
    /// The ID of this source remap/progress collection.
    pub remap_collection_id: GlobalId,
    // A semaphore that should be acquired by async operators in order to signal that upstream
    // operators should slow down.
    pub busy_signal: Arc<Semaphore>,
}

/// Reduced version of [`RawSourceCreationConfig`] that is used when rendering
/// each export.
#[derive(Clone)]
pub struct SourceExportCreationConfig {
    /// The ID of this instantiation of this source.
    pub id: GlobalId,
    /// The ID of the worker on which this operator is executing
    pub worker_id: usize,
    /// The metrics & registry that each source instantiates.
    pub metrics: StorageMetrics,
    /// Place to share statistics updates with storage state.
    pub source_statistics: SourceStatistics,
}

impl RawSourceCreationConfig {
    /// Returns the worker id responsible for handling the given partition.
    pub fn responsible_worker<P: Hash>(&self, partition: P) -> usize {
        let mut h = std::hash::DefaultHasher::default();
        (self.id, partition).hash(&mut h);
        let key = usize::cast_from(h.finish());
        key % self.worker_count
    }

    /// Returns true if this worker is responsible for handling the given partition.
    pub fn responsible_for<P: Hash>(&self, partition: P) -> bool {
        self.responsible_worker(partition) == self.worker_id
    }

    /// Returns a `SourceStatistics` for the source.
    pub fn source_statistics(&self) -> &SourceStatistics {
        self.statistics
            .get(&self.id)
            .expect("statistics exist for the source")
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
    storage_state: &crate::storage_state::StorageState,
    committed_upper: &Stream<Child<'g, G, mz_repr::Timestamp>, ()>,
    config: &RawSourceCreationConfig,
    source_connection: C,
    start_signal: impl std::future::Future<Output = ()> + 'static,
) -> (
    BTreeMap<
        GlobalId,
        Collection<
            Child<'g, G, mz_repr::Timestamp>,
            Result<SourceOutput<C::Time>, DataflowError>,
            Diff,
        >,
    >,
    Stream<G, HealthStatusMessage>,
    Vec<PressOnDropButton>,
)
where
    C: SourceConnection + SourceRender + Clone + 'static,
{
    let worker_id = config.worker_id;
    let id = config.id;

    let mut tokens = vec![];

    let (ingested_upper_tx, ingested_upper_rx) =
        watch::channel(MutableAntichain::new_bottom(C::Time::minimum()));
    let (probed_upper_tx, probed_upper_rx) = watch::channel(None);

    let source_metrics = Arc::new(config.metrics.get_source_metrics(id, worker_id));

    let timestamp_desc = source_connection.timestamp_desc();

    let (remap_collection, remap_token) = remap_operator(
        scope,
        storage_state,
        config.clone(),
        probed_upper_rx,
        ingested_upper_rx,
        timestamp_desc,
    );
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

    let mut reclocked_exports = BTreeMap::new();

    let reclocked_exports2 = &mut reclocked_exports;
    let (health, source_tokens) = scope.parent.scoped("SourceTimeDomain", move |scope| {
        let (exports, source_upper, health_stream, source_tokens) = source_render_operator(
            scope,
            config,
            source_connection,
            probed_upper_tx,
            committed_upper,
            start_signal,
        );

        for (id, export) in exports {
            let (reclock_pusher, reclocked) = reclock(&remap_collection, config.as_of.clone());
            export
                .inner
                .map(move |(result, from_time, diff)| {
                    let result = match result {
                        Ok(msg) => Ok(SourceOutput {
                            key: msg.key.clone(),
                            value: msg.value.clone(),
                            metadata: msg.metadata.clone(),
                            from_time: from_time.clone(),
                        }),
                        Err(err) => Err(err.clone()),
                    };
                    (result, from_time.clone(), *diff)
                })
                .capture_into(PusherCapture(reclock_pusher));
            reclocked_exports2.insert(id, reclocked);
        }

        source_upper.capture_into(FrontierCapture(ingested_upper_tx));

        (health_stream.leave(), source_tokens)
    });

    tokens.extend(source_tokens);

    (reclocked_exports, health, tokens)
}

pub struct FrontierCapture<T>(watch::Sender<MutableAntichain<T>>);

impl<T: Timestamp> EventPusher<T, Vec<Infallible>> for FrontierCapture<T> {
    fn push(&mut self, event: Event<T, Vec<Infallible>>) {
        match event {
            Event::Progress(changes) => self.0.send_modify(|frontier| {
                frontier.update_iter(changes);
            }),
            Event::Messages(_, _) => unreachable!(),
        }
    }
}

/// Renders the source dataflow fragment from the given [SourceConnection]. This returns a
/// collection timestamped with the source specific timestamp type. Also returns a second stream
/// that can be used to learn about the `source_upper` that all the source reader instances know
/// about. This second stream will be used by the `remap_operator` to mint new timestamp bindings
/// into the remap shard.
fn source_render_operator<G, C>(
    scope: &mut G,
    config: &RawSourceCreationConfig,
    source_connection: C,
    probed_upper_tx: watch::Sender<Option<Probe<C::Time>>>,
    resume_uppers: impl futures::Stream<Item = Antichain<C::Time>> + 'static,
    start_signal: impl std::future::Future<Output = ()> + 'static,
) -> (
    BTreeMap<GlobalId, StackedCollection<G, Result<SourceMessage, DataflowError>>>,
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
    let source_statistics = config.source_statistics().clone();
    let now_fn = config.now_fn.clone();
    let timestamp_interval = config.timestamp_interval;

    let resume_uppers = resume_uppers.inspect(move |upper| {
        let upper = upper.pretty();
        trace!(%upper, "timely-{worker_id} source({source_id}) received resume upper");
    });

    let (exports, progress, health, probes, tokens) =
        source_connection.render(scope, config, resume_uppers, start_signal);

    let mut export_collections = BTreeMap::new();

    let source_metrics = config.metrics.get_source_metrics(config.id, worker_id);

    // Compute the overall resume upper to report for the ingestion
    let resume_upper = Antichain::from_iter(
        config
            .resume_uppers
            .values()
            .flat_map(|f| f.iter().cloned()),
    );
    source_metrics
        .resume_upper
        .set(mz_persist_client::metrics::encode_ts_metric(&resume_upper));

    let mut health_streams = vec![];

    for (id, export) in exports {
        let name = format!("SourceGenericStats({})", id);
        let mut builder = OperatorBuilderRc::new(name, scope.clone());

        let (mut health_output, derived_health) =
            builder.new_output::<CapacityContainerBuilder<_>>();
        health_streams.push(derived_health);

        let (mut output, new_export) = builder.new_output::<CapacityContainerBuilder<_>>();

        let mut input = builder.new_input(&export.inner, Pipeline);
        export_collections.insert(id, new_export.as_collection());

        let bytes_read_counter = config.metrics.source_defs.bytes_read.clone();
        let source_statistics = source_statistics.clone();

        builder.build(move |mut caps| {
            let mut health_cap = Some(caps.remove(0));

            move |frontiers| {
                let mut last_status = None;
                let mut health_output = health_output.activate();

                if frontiers[0].is_empty() {
                    health_cap = None;
                    return;
                }
                let health_cap = health_cap.as_mut().unwrap();

                while let Some((cap, data)) = input.next() {
                    for (message, _, _) in data.iter() {
                        let status = match &message {
                            Ok(_) => HealthStatusUpdate::running(),
                            // All errors coming into the data stream are definite.
                            // Downstream consumers of this data will preserve this
                            // status.
                            Err(error) => HealthStatusUpdate::stalled(
                                error.to_string(),
                                Some(
                                    "retracting the errored value may resume the source"
                                        .to_string(),
                                ),
                            ),
                        };

                        let status = HealthStatusMessage {
                            id: Some(id),
                            namespace: C::STATUS_NAMESPACE.clone(),
                            update: status,
                        };
                        if last_status.as_ref() != Some(&status) {
                            last_status = Some(status.clone());
                            health_output.session(&health_cap).give(status);
                        }

                        match message {
                            Ok(message) => {
                                source_statistics.inc_messages_received_by(1);
                                let key_len = u64::cast_from(message.key.byte_len());
                                let value_len = u64::cast_from(message.value.byte_len());
                                bytes_read_counter.inc_by(key_len + value_len);
                                source_statistics.inc_bytes_received_by(key_len + value_len);
                            }
                            Err(_) => {}
                        }
                    }
                    let mut output = output.activate();
                    output.session(&cap).give_container(data);
                }
            }
        });
    }

    let probe_stream = match probes {
        Some(stream) => stream,
        None => synthesize_probes(source_id, &progress, timestamp_interval, now_fn),
    };

    // Broadcasting does more work than necessary, which would be to exchange the probes to the
    // worker that will be the one minting the bindings but we'd have to thread this information
    // through and couple the two functions enough that it's not worth the optimization (I think).
    probe_stream.broadcast().inspect(move |probe| {
        // We don't care if the receiver is gone
        let _ = probed_upper_tx.send(Some(probe.clone()));
    });

    (
        export_collections,
        progress,
        health.concatenate_flatten::<_, CapacityContainerBuilder<_>>(health_streams),
        tokens,
    )
}

/// Mints new contents for the remap shard based on summaries about the source
/// upper it receives from the raw reader operators.
///
/// Only one worker will be active and write to the remap shard. All source
/// upper summaries will be exchanged to it.
fn remap_operator<G, FromTime>(
    scope: &G,
    storage_state: &crate::storage_state::StorageState,
    config: RawSourceCreationConfig,
    mut probed_upper: watch::Receiver<Option<Probe<FromTime>>>,
    mut ingested_upper: watch::Receiver<MutableAntichain<FromTime>>,
    remap_relation_desc: RelationDesc,
) -> (Collection<G, FromTime, Diff>, PressOnDropButton)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    FromTime: SourceTimestamp,
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
        now_fn,
        persist_clients,
        statistics: _,
        shared_remap_upper,
        config: _,
        remap_collection_id,
        busy_signal: _,
    } = config;

    let read_only_rx = storage_state.read_only_rx.clone();
    let error_handler = storage_state.error_handler("remap_operator", id);

    let chosen_worker = usize::cast_from(id.hashed() % u64::cast_from(worker_count));
    let active_worker = chosen_worker == worker_id;

    let operator_name = format!("remap({})", id);
    let mut remap_op = AsyncOperatorBuilder::new(operator_name, scope.clone());
    let (remap_output, remap_stream) = remap_op.new_output::<CapacityContainerBuilder<_>>();

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
            read_only_rx,
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
        .await;

        let remap_handle = match remap_handle {
            Ok(handle) => handle,
            Err(e) => {
                error_handler
                    .report_and_stop(
                        e.context(format!("Failed to create remap handle for source {name}")),
                    )
                    .await
            }
        };

        let (mut timestamper, mut initial_batch) = ReclockOperator::new(remap_handle).await;

        // Emit initial snapshot of the remap_shard, bootstrapping
        // downstream reclock operators.
        trace!(
            "timely-{worker_id} remap({id}) emitting remap snapshot: trace_updates={:?}",
            &initial_batch.updates
        );

        let cap = cap_set.delayed(cap_set.first().unwrap());
        remap_output.give_container(&cap, &mut initial_batch.updates);
        drop(cap);
        cap_set.downgrade(initial_batch.upper);

        let mut ticker = tokio::time::interval(timestamp_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut prev_probe_ts: Option<mz_repr::Timestamp> = None;
        let timestamp_interval_ms: u64 = timestamp_interval
            .as_millis()
            .try_into()
            .expect("huge duration");

        while !cap_set.is_empty() {
            // Check the reclocking strategy in every iteration, to make it possible to change it
            // without restarting the source pipeline.
            let reclock_to_latest =
                dyncfgs::STORAGE_RECLOCK_TO_LATEST.get(&config.config.config_set());

            // If we are reclocking to the latest offset then we only mint bindings after a
            // successful probe. Otherwise we fall back to the earlier behavior where we just
            // record the ingested frontier.
            let mut new_probe = None;
            if reclock_to_latest {
                new_probe = probed_upper
                    .wait_for(|new_probe| match (prev_probe_ts, new_probe) {
                        (None, Some(_)) => true,
                        (Some(prev_ts), Some(new)) => prev_ts < new.probe_ts,
                        _ => false,
                    })
                    .await
                    .map(|probe| (*probe).clone())
                    .unwrap_or_else(|_| {
                        Some(Probe {
                            probe_ts: now_fn().into(),
                            upstream_frontier: Antichain::new(),
                        })
                    });
            } else {
                while prev_probe_ts >= new_probe.as_ref().map(|p| p.probe_ts) {
                    ticker.tick().await;
                    // We only proceed if the source upper frontier is not the minimum frontier. This
                    // makes it so the first binding corresponds to the snapshot of the source, and
                    // because the first binding always maps to the minimum *target* frontier we
                    // guarantee that the source will never appear empty.
                    let upstream_frontier = ingested_upper
                        .wait_for(|f| *f.frontier() != [FromTime::minimum()])
                        .await
                        .unwrap()
                        .frontier()
                        .to_owned();

                    let now = (now_fn)();
                    let mut probe_ts = now - now % timestamp_interval_ms;
                    if (now % timestamp_interval_ms) != 0 {
                        probe_ts += timestamp_interval_ms;
                    }
                    new_probe = Some(Probe {
                        probe_ts: probe_ts.into(),
                        upstream_frontier,
                    });
                }
            };

            let probe = new_probe.expect("known to be Some");
            prev_probe_ts = Some(probe.probe_ts);

            let binding_ts = probe.probe_ts;
            let cur_source_upper = probe.upstream_frontier;

            let new_into_upper = Antichain::from_elem(binding_ts.step_forward());

            let mut remap_trace_batch = timestamper
                .mint(binding_ts, new_into_upper, cur_source_upper.borrow())
                .await;

            trace!(
                "timely-{worker_id} remap({id}) minted new bindings: \
                updates={:?} \
                source_upper={} \
                trace_upper={}",
                &remap_trace_batch.updates,
                cur_source_upper.pretty(),
                remap_trace_batch.upper.pretty()
            );

            let cap = cap_set.delayed(cap_set.first().unwrap());
            remap_output.give_container(&cap, &mut remap_trace_batch.updates);
            cap_set.downgrade(remap_trace_batch.upper);
        }
    });

    (remap_stream.as_collection(), button.press_on_drop())
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
    let (tx, rx) = watch::channel(Antichain::from_elem(FromTime::minimum()));
    let scope = bindings.scope().clone();

    let name = format!("ReclockCommitUpper({id})");
    let mut builder = OperatorBuilderRc::new(name, scope);

    let mut bindings = builder.new_input(&bindings.inner, Pipeline);
    let _ = builder.new_input(committed_upper, Pipeline);

    builder.build(move |_| {
        // Remap bindings beyond the upper
        use timely::progress::ChangeBatch;
        let mut accepted_times: ChangeBatch<(G::Timestamp, FromTime)> = ChangeBatch::new();
        // The upper frontier of the bindings
        let mut upper = Antichain::from_elem(Timestamp::minimum());
        // Remap bindings not beyond upper
        let mut ready_times = VecDeque::new();
        let mut source_upper = MutableAntichain::new();

        move |frontiers| {
            // Accept new bindings
            while let Some((_, data)) = bindings.next() {
                accepted_times.extend(data.drain(..).map(|(from, mut into, diff)| {
                    into.advance_by(as_of.borrow());
                    ((into, from), diff.into_inner())
                }));
            }
            // Extract ready bindings
            let new_upper = frontiers[0].frontier();
            if PartialOrder::less_than(&upper.borrow(), &new_upper) {
                upper = new_upper.to_owned();
                // Drain consolidated accepted times not greater or equal to `upper` into `ready_times`.
                // Retain accepted times greater or equal to `upper` in
                let mut pending_times = std::mem::take(&mut accepted_times).into_inner();
                // These should already be sorted, as part of `.into_inner()`, but sort defensively in case.
                pending_times.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                for ((into, from), diff) in pending_times.drain(..) {
                    if !upper.less_equal(&into) {
                        ready_times.push_back((from, into, diff));
                    } else {
                        accepted_times.update((into, from), diff);
                    }
                }
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

/// Synthesizes a probe stream that produces the frontier of the given progress stream at the given
/// interval.
///
/// This is used as a fallback for sources that don't support probing the frontier of the upstream
/// system.
fn synthesize_probes<G>(
    source_id: GlobalId,
    progress: &Stream<G, Infallible>,
    interval: Duration,
    now_fn: NowFn,
) -> Stream<G, Probe<G::Timestamp>>
where
    G: Scope,
{
    let scope = progress.scope();

    let active_worker = usize::cast_from(source_id.hashed()) % scope.peers();
    let is_active_worker = active_worker == scope.index();

    let mut op = AsyncOperatorBuilder::new("synthesize_probes".into(), scope);
    let (output, output_stream) = op.new_output();
    let mut input = op.new_input_for(progress, Pipeline, &output);

    op.build(|caps| async move {
        if !is_active_worker {
            return;
        }

        let [cap] = caps.try_into().expect("one capability per output");

        let mut ticker = probe::Ticker::new(move || interval, now_fn.clone());

        let minimum_frontier = Antichain::from_elem(Timestamp::minimum());
        let mut frontier = minimum_frontier.clone();
        loop {
            tokio::select! {
                event = input.next() => match event {
                    Some(AsyncEvent::Progress(progress)) => frontier = progress,
                    Some(AsyncEvent::Data(..)) => unreachable!(),
                    None => break,
                },
                // We only report a probe if the source upper frontier is not the minimum frontier.
                // This makes it so the first remap binding corresponds to the snapshot of the
                // source, and because the first binding always maps to the minimum *target*
                // frontier we guarantee that the source will never appear empty.
                probe_ts = ticker.tick(), if frontier != minimum_frontier => {
                    let probe = Probe {
                        probe_ts,
                        upstream_frontier: frontier.clone(),
                    };
                    output.give(&cap, probe);
                }
            }
        }

        let probe = Probe {
            probe_ts: now_fn().into(),
            upstream_frontier: Antichain::new(),
        };
        output.give(&cap, probe);
    });

    output_stream
}
