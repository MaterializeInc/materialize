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
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Hashable, VecCollection};
use futures::stream::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::{SourceConnection, SourceExport, SourceTimestamp};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use mz_timely_util::capture::PusherCapture;
use mz_timely_util::operator::ConcatenateFlatten;
use mz_timely_util::reclock::reclock;
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::core::Map as _;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::{CapabilitySet, InspectCore, Leave};
use timely::dataflow::{Scope, StreamVec};
use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::{Semaphore, watch};
use tokio_stream::wrappers::WatchStream;
use tracing::trace;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate};
use crate::metrics::StorageMetrics;
use crate::metrics::source::SourceMetrics;
use crate::source::reclock::ReclockOperator;
use crate::source::types::{
    Probe, ResumeUppers, SourceMessage, SourceOutput, SourceRender, StackedCollection,
};
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
    /// The storage metadata for the remap/progress collection
    pub remap_metadata: CollectionMetadata,
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
/// The `committed_uppers` parameter contains one stream per export whose frontier advances
/// whenever times are durably recorded for that export. They are reclocked into the
/// [`ResumeUppers`] the source observes.
///
/// Alongside the reclocked exports this returns a no-data stream whose frontier is the remap upper.
pub fn create_raw_source<'scope, 'root, C>(
    scope: Scope<'scope, mz_repr::Timestamp>,
    root_scope: Scope<'root, ()>,
    storage_state: &crate::storage_state::StorageState,
    committed_uppers: BTreeMap<GlobalId, StreamVec<'scope, mz_repr::Timestamp, ()>>,
    config: &RawSourceCreationConfig,
    source_connection: C,
    start_signal: impl std::future::Future<Output = ()> + 'static,
) -> (
    BTreeMap<
        GlobalId,
        VecCollection<
            'scope,
            mz_repr::Timestamp,
            Result<SourceOutput<C::Time>, DataflowError>,
            Diff,
        >,
    >,
    StreamVec<'root, (), HealthStatusMessage>,
    StreamVec<'scope, mz_repr::Timestamp, ()>,
    Vec<PressOnDropButton>,
)
where
    C: SourceConnection + SourceRender + Clone + 'static,
{
    let worker_id = config.worker_id;
    let id = config.id;

    let mut tokens = vec![];

    let (probed_upper_tx, probed_upper_rx) = watch::channel(None);

    let source_metrics = Arc::new(config.metrics.get_source_metrics(id, worker_id));

    let timestamp_desc = source_connection.timestamp_desc();

    let (remap_collection, remap_token) = remap_operator(
        scope,
        storage_state,
        config.clone(),
        probed_upper_rx,
        timestamp_desc,
    );
    // Need to broadcast the remap changes to all workers.
    let remap_collection = remap_collection.inner.broadcast().as_collection();
    tokens.push(remap_token);

    // Drops the bidings, as this stream is only used to track the remap upper, which drives
    // ceiling calculation in the persist sink during snapshots.
    let remap_upper = remap_collection
        .inner
        .clone()
        .flat_map::<Vec<()>, _, _>(|_| None::<()>);

    let resume_uppers = reclock_committed_upper(
        remap_collection.clone(),
        config.as_of.clone(),
        committed_uppers,
        id,
        Arc::clone(&source_metrics),
    );

    let mut reclocked_exports = BTreeMap::new();

    let reclocked_exports2 = &mut reclocked_exports;
    let (health, source_tokens) = root_scope.scoped("SourceTimeDomain", move |scope| {
        let (exports, health_stream, source_tokens) = source_render_operator(
            scope,
            config,
            source_connection,
            probed_upper_tx,
            resume_uppers,
            start_signal,
        );

        for (id, export) in exports {
            let (reclock_pusher, reclocked) =
                reclock(remap_collection.clone(), config.as_of.clone());
            export
                .inner
                .map(move |(result, from_time, diff)| {
                    let result = match result {
                        Ok(msg) => Ok(SourceOutput {
                            key: msg.key,
                            value: msg.value,
                            metadata: msg.metadata,
                            from_time: from_time.clone(),
                        }),
                        Err(err) => Err(err),
                    };
                    (result, from_time, diff)
                })
                .capture_into(PusherCapture(reclock_pusher));
            reclocked_exports2.insert(id, reclocked);
        }

        (health_stream.leave(root_scope), source_tokens)
    });

    tokens.extend(source_tokens);

    (reclocked_exports, health, remap_upper, tokens)
}

/// Renders the source dataflow fragment from the given [SourceConnection]. This returns a
/// collection timestamped with the source specific timestamp type.
fn source_render_operator<'scope, C>(
    scope: Scope<'scope, C::Time>,
    config: &RawSourceCreationConfig,
    source_connection: C,
    probed_upper_tx: watch::Sender<Option<Probe<C::Time>>>,
    resume_uppers: impl futures::Stream<Item = ResumeUppers<C::Time>> + 'static,
    start_signal: impl std::future::Future<Output = ()> + 'static,
) -> (
    BTreeMap<GlobalId, StackedCollection<'scope, C::Time, Result<SourceMessage, DataflowError>>>,
    StreamVec<'scope, C::Time, HealthStatusMessage>,
    Vec<PressOnDropButton>,
)
where
    C: SourceRender + 'static,
{
    let source_id = config.id;
    let worker_id = config.worker_id;

    let resume_uppers = resume_uppers.inspect(move |uppers| {
        trace!(
            %uppers,
            "timely-{worker_id} source({source_id}) received resume uppers"
        );
    });

    let (exports, health, probe_stream, tokens) =
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

        let (health_output, derived_health) = builder.new_output();
        let mut health_output =
            OutputBuilder::<_, CapacityContainerBuilder<_>>::from(health_output);
        health_streams.push(derived_health);

        let (output, new_export) = builder.new_output();
        let mut output = OutputBuilder::<_, CapacityContainerBuilder<_>>::from(output);

        let mut input = builder.new_input(export.inner, Pipeline);
        export_collections.insert(id, new_export.as_collection());

        let bytes_read_counter = config.metrics.source_defs.bytes_read.clone();
        let source_statistics = config
            .statistics
            .get(&id)
            .expect("statistics initialized")
            .clone();

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

                input.for_each(|cap, data| {
                    for (message, _, _) in data.iter() {
                        match message {
                            Ok(message) => {
                                source_statistics.inc_messages_received_by(1);
                                let key_len = u64::cast_from(message.key.byte_len());
                                let value_len = u64::cast_from(message.value.byte_len());
                                bytes_read_counter.inc_by(key_len + value_len);
                                source_statistics.inc_bytes_received_by(key_len + value_len);
                            }
                            Err(error) => {
                                // All errors coming into the data stream are definite.
                                // Downstream consumers of this data will preserve this
                                // status.
                                let hint = match error {
                                    DataflowError::SourceError(e) if e.hint.is_some() => {
                                        e.hint.as_deref().map(str::to_string)
                                    }
                                    _ => Some(
                                        "retracting the errored value may resume the source"
                                            .to_string(),
                                    ),
                                };
                                let update = HealthStatusUpdate::stalled(error.to_string(), hint);
                                let status = HealthStatusMessage {
                                    id: Some(id),
                                    namespace: C::STATUS_NAMESPACE.clone(),
                                    update,
                                };
                                if last_status.as_ref() != Some(&status) {
                                    last_status = Some(status.clone());
                                    health_output.session(&health_cap).give(status);
                                }
                            }
                        }
                    }
                    let mut output = output.activate();
                    output.session(&cap).give_container(data);
                });
            }
        });
    }

    // Broadcasting does more work than necessary, which would be to exchange the probes to the
    // worker that will be the one minting the bindings but we'd have to thread this information
    // through and couple the two functions enough that it's not worth the optimization (I think).
    // Use `InspectCore::inspect_container` instead of `Inspect::inspect`.
    // `Inspect` carries a `where for<'a> &'a C: IntoIterator` bound, and on
    // macOS the solver can satisfy that bound by chasing objc2's
    // `&Retained<T>: IntoIterator` blanket impl into an endless
    // `Retained<Retained<…>>` chain, overflowing the recursion limit.
    // `InspectCore` has no such bound, so the cascade never starts. We
    // iterate the container by hand to recover the per-item callback.
    probe_stream.broadcast().inspect_container(move |event| {
        if let Ok((_, data)) = event {
            for probe in data {
                // We don't care if the receiver is gone
                let _ = probed_upper_tx.send(Some(probe.clone()));
            }
        }
    });

    (
        export_collections,
        health.concatenate_flatten::<_, CapacityContainerBuilder<_>>(health_streams),
        tokens,
    )
}

/// Mints new contents for the remap shard based on summaries about the source
/// upper it receives from the raw reader operators.
///
/// Only one worker will be active and write to the remap shard. All source
/// upper summaries will be exchanged to it.
fn remap_operator<'scope, FromTime>(
    scope: Scope<'scope, mz_repr::Timestamp>,
    storage_state: &crate::storage_state::StorageState,
    config: RawSourceCreationConfig,
    mut probed_upper: watch::Receiver<Option<Probe<FromTime>>>,
    remap_relation_desc: RelationDesc,
) -> (
    VecCollection<'scope, mz_repr::Timestamp, FromTime, Diff>,
    PressOnDropButton,
)
where
    FromTime: SourceTimestamp,
{
    let RawSourceCreationConfig {
        name,
        id,
        source_exports: _,
        worker_id,
        worker_count,
        timestamp_interval: _,
        remap_metadata,
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
            remap_metadata.clone(),
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

        let mut prev_probe_ts: Option<mz_repr::Timestamp> = None;

        while !cap_set.is_empty() {
            // We only mint bindings after a successful probe.
            let new_probe = probed_upper
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

/// Reclocks the per-export `IntoTime` committed upper streams into `FromTime` [`ResumeUppers`].
/// This is used for the virtual (through persist) feedback edge so that we convert the `IntoTime`
/// resumption frontiers into the `FromTime` frontiers that are used with the source's
/// `OffsetCommiter`.
fn reclock_committed_upper<'scope, T, FromTime>(
    bindings: VecCollection<'scope, T, FromTime, Diff>,
    as_of: Antichain<T>,
    committed_uppers: BTreeMap<GlobalId, StreamVec<'scope, T, ()>>,
    id: GlobalId,
    metrics: Arc<SourceMetrics>,
) -> impl futures::stream::Stream<Item = ResumeUppers<FromTime>> + 'static
where
    T: Timestamp + Lattice + TotalOrder,
    FromTime: SourceTimestamp,
{
    // Only used within this function, rather than create a comment to explain the fields.
    struct ExportState<T, FromTime> {
        id: GlobalId,
        input_index: usize,
        /// The committed upper that `source_upper` and `applied` reflect.
        committed_upper: Antichain<T>,
        source_upper: MutableAntichain<FromTime>,
        applied: usize,
    }

    impl<T: Timestamp, FromTime: Timestamp> ExportState<T, FromTime> {
        fn reclocked_upper(&self) -> Antichain<FromTime> {
            if self.committed_upper.is_empty() {
                Antichain::new()
            } else {
                self.source_upper.frontier().to_owned()
            }
        }
    }

    let (tx, rx) = watch::channel(ResumeUppers {
        source: None,
        exports: BTreeMap::new(),
    });
    let scope = bindings.scope().clone();

    let name = format!("ReclockCommitUpper({id})");
    let mut builder = OperatorBuilderRc::new(name, scope);

    let mut bindings = builder.new_input(bindings.inner.clone(), Pipeline);

    let mut exports: Vec<_> = committed_uppers
        .into_iter()
        .map(|(export_id, committed_upper)| {
            let input_index = builder.shape().inputs();
            // not reading data, just the frontiers
            let _ = builder.new_input(committed_upper, Pipeline);
            ExportState {
                id: export_id,
                input_index,
                committed_upper: Antichain::from_elem(T::minimum()),
                source_upper: MutableAntichain::new(),
                applied: 0,
            }
        })
        .collect();

    builder.build(move |_| {
        // Remap bindings beyond the upper
        use timely::progress::ChangeBatch;
        let mut accepted_times: ChangeBatch<(T, FromTime)> = ChangeBatch::new();
        // The upper frontier of the bindings
        let mut bindings_upper = Antichain::from_elem(Timestamp::minimum());
        // Remap bindings not beyond upper that some export has not yet applied, in `into` order.
        // This is a shared queue of remap bindings. A binding is retained until every export has
        // applied it.
        let mut ready_times = VecDeque::new();
        // The number of bindings dropped from the front of `ready_times`, which makes the
        // per-export positions absolute rather than deque indices.
        let mut drained = 0;
        // Indices of the exports whose reclocked upper may have changed in this invocation.
        let mut touched = Vec::new();

        move |frontiers| {
            // Accept new bindings
            bindings.for_each(|_, data| {
                accepted_times.extend(data.drain(..).map(|(from, mut into, diff)| {
                    into.advance_by(as_of.borrow());
                    ((into, from), diff.into_inner())
                }));
            });
            // Extract ready bindings
            let new_bindings_upper = frontiers[0].frontier();
            let ready_before = ready_times.len();
            if PartialOrder::less_than(&bindings_upper.borrow(), &new_bindings_upper) {
                bindings_upper = new_bindings_upper.to_owned();
                // Drain consolidated accepted times not greater or equal to `bindings_upper` into `ready_times`.
                // Retain accepted times greater or equal to `bindings_upper` in
                let mut pending_times = std::mem::take(&mut accepted_times).into_inner();
                // These should already be sorted, as part of `.into_inner()`, but sort defensively in case.
                pending_times.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                for ((into, from), diff) in pending_times.drain(..) {
                    if !bindings_upper.less_equal(&into) {
                        ready_times.push_back((from, into, diff));
                    } else {
                        accepted_times.update((into, from), diff);
                    }
                }
            }
            let bindings_ready = ready_times.len() > ready_before;

            // The received times only accumulate correctly for times beyond the as_of.
            if as_of.iter().all(|t| !bindings_upper.less_equal(t)) {
                let mut all_beyond_as_of = true;
                // The export with the least committed upper. Its reclocked upper is the
                // source-wide one, because t1 <= t2 => remap[t1] <= remap[t2].
                let mut least_committed_upper: Option<(T, usize)> = None;
                let mut min_applied = drained + ready_times.len();
                for (index, export) in exports.iter_mut().enumerate() {
                    let committed_upper = frontiers[export.input_index].frontier();
                    if !as_of.iter().all(|t| !committed_upper.less_equal(t)) {
                        all_beyond_as_of = false;
                        min_applied = min_applied.min(export.applied);
                        continue;
                    }
                    let advanced =
                        PartialOrder::less_than(&export.committed_upper.borrow(), &committed_upper);
                    if advanced {
                        export.committed_upper = committed_upper.to_owned();
                    }
                    // We have committed this export up until `committed_upper`. Because we have
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
                    match committed_upper.as_option() {
                        Some(t_next) => {
                            // The remap input and this export's committed upper input advance
                            // independently, so bindings below `t_next` can become ready after
                            // the export committed `t_next`. Either input moving can therefore
                            // add bindings for this export to apply.
                            if advanced || bindings_ready {
                                let start = export.applied - drained;
                                let end = ready_times.partition_point(|(_, t, _)| t < t_next);
                                if end > start {
                                    let updates = ready_times
                                        .range(start..end)
                                        .map(|(from_time, _, diff)| (from_time.clone(), *diff));
                                    export.source_upper.update_iter(updates);
                                    export.applied = drained + end;
                                    touched.push(index);
                                }
                            }
                            if least_committed_upper
                                .as_ref()
                                .is_none_or(|(t, _)| t_next < t)
                            {
                                least_committed_upper = Some((t_next.clone(), index));
                            }
                        }
                        None => {
                            export.applied = drained + ready_times.len();
                            if advanced {
                                touched.push(index);
                            }
                        }
                    }
                    min_applied = min_applied.min(export.applied);
                }

                // If all exports are beyond the as_of, the source upper is the meet of the exports
                // committed uppers, or all exports are complete and it's the empty frontier.
                let source = all_beyond_as_of.then(|| match least_committed_upper {
                    Some((_, index)) => exports[index].reclocked_upper(),
                    None => Antichain::new(),
                });
                ready_times.drain(..min_applied - drained);
                drained = min_applied;
                // From testing, the common case is seeing a few exports updated in a given
                // invocation. The published value is updated in place for the touched exports,
                // rather than wholly replace the existing map with a new allocation.
                tx.send_if_modified(|published| {
                    let mut modified = false;
                    for export in touched.drain(..).map(|index| &exports[index]) {
                        let reclocked_upper = export.reclocked_upper();
                        match published.exports.entry(export.id) {
                            Entry::Occupied(entry) if *entry.get() == reclocked_upper => {}
                            Entry::Occupied(mut entry) => {
                                entry.insert(reclocked_upper);
                                modified = true;
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(reclocked_upper);
                                modified = true;
                            }
                        }
                    }
                    if published.source != source {
                        published.source = source;
                        modified = true;
                    }
                    modified
                });
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

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use futures::stream::LocalBoxStream;
    use mz_ore::metrics::MetricsRegistry;
    use mz_repr::Timestamp;
    use mz_storage_types::sources::MzOffset;
    use timely::dataflow::operators::Input;
    use timely::dataflow::operators::vec::input::Handle;
    use timely::worker::Worker;

    use super::*;
    use crate::metrics::source::GeneralSourceMetricDefs;

    const A: GlobalId = GlobalId::User(1);
    const B: GlobalId = GlobalId::User(2);

    /// Drives `reclock_committed_upper` with an as_of of `{0}`. [`Harness::bind`] makes the source
    /// frontier `10 * t` at each `IntoTime` `t`, so a committed upper of `{t}` reclocks to
    /// `{10 * (t - 1)}`.
    struct Harness {
        bindings: Handle<Timestamp, (MzOffset, Timestamp, Diff)>,
        committed_uppers: BTreeMap<GlobalId, Handle<Timestamp, ()>>,
        resume_uppers: LocalBoxStream<'static, ResumeUppers<MzOffset>>,
        metrics: Arc<SourceMetrics>,
    }

    impl Harness {
        fn new(worker: &mut Worker, exports: &[GlobalId]) -> Self {
            let defs = GeneralSourceMetricDefs::register_with(&MetricsRegistry::new());
            let metrics = Arc::new(SourceMetrics::new(&defs, GlobalId::User(0), 0));
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (bindings, bindings_stream) = scope.new_input();
                let mut committed_uppers = BTreeMap::new();
                let mut streams = BTreeMap::new();
                for id in exports {
                    let (handle, stream) = scope.new_input();
                    committed_uppers.insert(*id, handle);
                    streams.insert(*id, stream);
                }
                let resume_uppers = reclock_committed_upper(
                    bindings_stream.as_collection(),
                    Antichain::from_elem(Timestamp::MIN),
                    streams,
                    GlobalId::User(0),
                    Arc::clone(&metrics),
                )
                .boxed_local();
                Harness {
                    bindings,
                    committed_uppers,
                    resume_uppers,
                    metrics,
                }
            })
        }

        /// Binds `IntoTime` `t` to the source frontier `{10 * t}` and closes the bindings through
        /// `t`.
        fn bind(&mut self, t: u64) {
            if t > 0 {
                self.bindings.send((
                    MzOffset::from(10 * (t - 1)),
                    Timestamp::from(t),
                    Diff::MINUS_ONE,
                ));
            }
            self.bindings
                .send((MzOffset::from(10 * t), Timestamp::from(t), Diff::ONE));
            self.bindings.advance_to(Timestamp::from(t + 1));
        }

        fn commit(&mut self, id: GlobalId, t: u64) {
            self.committed_uppers
                .get_mut(&id)
                .expect("export is open")
                .advance_to(Timestamp::from(t));
        }

        fn close(&mut self, id: GlobalId) {
            self.committed_uppers.remove(&id);
        }

        /// Steps the dataflow until it is quiescent and returns the last value it published, if
        /// it published any.
        fn step(&mut self, worker: &mut Worker) -> Option<ResumeUppers<MzOffset>> {
            // A single worker reaches quiescence within a few steps. Extra steps are no-ops.
            for _ in 0..10 {
                worker.step();
            }
            let mut latest = None;
            while let Some(Some(uppers)) = self.resume_uppers.next().now_or_never() {
                latest = Some(uppers);
            }
            latest
        }

        fn ready_times(&self) -> u64 {
            self.metrics.commit_upper_ready_times.get()
        }
    }

    fn uppers(
        source: Option<Antichain<MzOffset>>,
        exports: impl IntoIterator<Item = (GlobalId, Antichain<MzOffset>)>,
    ) -> Option<ResumeUppers<MzOffset>> {
        Some(ResumeUppers {
            source,
            exports: exports.into_iter().collect(),
        })
    }

    fn offset(offset: u64) -> Antichain<MzOffset> {
        Antichain::from_elem(MzOffset::from(offset))
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn reclock_committed_upper_reports_each_export() {
        timely::execute_directly(|worker| {
            let mut h = Harness::new(worker, &[A, B]);
            for t in 0..=3 {
                h.bind(t);
            }
            assert_eq!(h.step(worker), None, "no export is beyond the as_of");

            h.commit(A, 3);
            assert_eq!(
                h.step(worker),
                uppers(None, [(A, offset(20))]),
                "B holds back the source upper until it is beyond the as_of",
            );

            h.commit(B, 2);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(10)), [(A, offset(20)), (B, offset(10))]),
                "the source upper is B's, the least",
            );

            h.commit(B, 4);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(20)), [(A, offset(20)), (B, offset(30))]),
                "the source upper is A's once B passes it",
            );
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn reclock_committed_upper_retains_bindings_for_slowest_export() {
        timely::execute_directly(|worker| {
            let mut h = Harness::new(worker, &[A, B]);
            for t in 0..=3 {
                h.bind(t);
            }
            h.commit(A, 4);
            h.commit(B, 2);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(10)), [(A, offset(30)), (B, offset(10))]),
            );
            // Seven updates bind times 0 through 3. B has applied the three below time 2.
            assert_eq!(h.ready_times(), 4);

            h.bind(4);
            h.commit(A, 5);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(10)), [(A, offset(40)), (B, offset(10))]),
            );
            assert_eq!(h.ready_times(), 6, "B has not applied the new bindings");

            h.commit(B, 3);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(20)), [(A, offset(40)), (B, offset(20))]),
            );
            assert_eq!(h.ready_times(), 4);

            h.commit(B, 5);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(40)), [(A, offset(40)), (B, offset(40))]),
            );
            assert_eq!(h.ready_times(), 0, "every export has applied every binding");
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn reclock_committed_upper_applies_bindings_arriving_after_the_commit() {
        timely::execute_directly(|worker| {
            let mut h = Harness::new(worker, &[A]);
            for t in 0..=3 {
                h.bind(t);
            }
            h.commit(A, 5);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(30)), [(A, offset(30))]),
                "only bindings through time 3 exist",
            );
            assert_eq!(
                h.step(worker),
                None,
                "nothing changed, nothing is published"
            );

            h.bind(4);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(40)), [(A, offset(40))]),
                "the binding for time 4 applies without A committing again",
            );
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn reclock_committed_upper_closed_exports() {
        timely::execute_directly(|worker| {
            let mut h = Harness::new(worker, &[A, B]);
            for t in 0..=3 {
                h.bind(t);
            }
            h.commit(B, 2);
            h.close(A);
            assert_eq!(
                h.step(worker),
                uppers(Some(offset(10)), [(A, Antichain::new()), (B, offset(10))]),
                "a closed export does not constrain the source upper",
            );

            h.close(B);
            assert_eq!(
                h.step(worker),
                uppers(
                    Some(Antichain::new()),
                    [(A, Antichain::new()), (B, Antichain::new())],
                ),
            );
            assert_eq!(h.ready_times(), 0);
        });
    }
}
