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

use std::any::Any;
use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::future::Future;
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
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::{Broadcast, CapabilitySet, Concat, Enter, Leave, Map, Partition};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, trace, warn};

use mz_expr::PartitionId;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::now::NowFn;
use mz_ore::vec::VecExt;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{Diff, GlobalId, RelationDesc, Row};
use mz_storage_client::client::SourceStatisticsUpdate;
use mz_storage_client::controller::{CollectionMetadata, ResumptionFrontierCalculator};
use mz_storage_client::healthcheck::MZ_SOURCE_STATUS_HISTORY_DESC;
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::errors::SourceError;
use mz_storage_client::types::sources::encoding::SourceDataEncoding;
use mz_storage_client::types::sources::{
    MzOffset, SourceConnection, SourceExport, SourceTimestamp, SourceToken,
};
use mz_timely_util::antichain::AntichainExt;
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
};
use mz_timely_util::capture::UnboundedTokioCapture;
use mz_timely_util::operator::StreamExt as _;

use crate::healthcheck::write_to_persist;
use crate::internal_control::InternalStorageCommand;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::reclock::{ReclockBatch, ReclockError, ReclockFollower, ReclockOperator};
use crate::source::types::{
    HealthStatus, HealthStatusUpdate, MaybeLength, SourceMessage, SourceMetrics, SourceOutput,
    SourceReaderError, SourceRender,
};
use crate::statistics::{SourceStatisticsMetrics, StorageStatistics};

/// How long to wait before initiating a `SuspendAndRestart` command, to
/// prevent hot restart loops.
const SUSPEND_AND_RESTART_DELAY: Duration = Duration::from_secs(30);

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
    /// Data encoding
    pub encoding: SourceDataEncoding,
    /// The function to return a now time.
    pub now: NowFn,
    /// The metrics & registry that each source instantiates.
    pub base_metrics: SourceBaseMetrics,
    /// Storage Metadata
    pub storage_metadata: CollectionMetadata,
    /// The upper frontier this source should resume ingestion at
    pub resume_upper: Antichain<mz_repr::Timestamp>,
    /// The upper frontier this source should resume ingestion at in the source time domain. Since
    /// every source has a different timestamp type we carry the timestamps of this frontier in an
    /// encoded `Vec<Row>` form which will get decoded once we reach the connection specialized
    /// functions.
    pub source_resume_upper: Vec<Row>,
    /// A handle to the persist client cache
    pub persist_clients: Arc<PersistClientCache>,
    /// Place to share statistics updates with storage state.
    pub source_statistics: StorageStatistics<SourceStatisticsUpdate, SourceStatisticsMetrics>,
    /// Enables reporting the remap operator's write frontier.
    pub shared_remap_upper: Rc<RefCell<Antichain<mz_repr::Timestamp>>>,
    /// Configuration parameters, possibly from LaunchDarkly
    pub params: SourceCreationParams,
}

#[derive(Clone)]
pub struct SourceCreationParams {
    /// Sets timeouts specific to PG replication streams
    pub pg_replication_timeouts: mz_postgres_util::ReplicationTimeouts,
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
pub fn create_raw_source<'g, G: Scope<Timestamp = ()>, C, R>(
    scope: &mut Child<'g, G, mz_repr::Timestamp>,
    config: RawSourceCreationConfig,
    source_connection: C,
    connection_context: ConnectionContext,
    calc: R,
) -> (
    Vec<(
        Collection<Child<'g, G, mz_repr::Timestamp>, SourceOutput<C::Key, C::Value>, Diff>,
        Collection<Child<'g, G, mz_repr::Timestamp>, SourceError, Diff>,
        Stream<G, (usize, HealthStatusUpdate)>,
    )>,
    Option<Rc<dyn Any>>,
)
where
    C: SourceConnection + SourceRender + Clone + 'static,
    R: ResumptionFrontierCalculator<mz_repr::Timestamp> + 'static,
{
    let worker_id = config.worker_id;
    let id = config.id;
    info!(
        %id,
        resume_upper = %config.resume_upper.pretty(),
        "timely-{worker_id} building source pipeline",
    );
    let (resume_stream, resume_token) =
        super::resumption::resumption_operator(scope, config.clone(), calc);

    let reclock_follower = {
        let upper_ts = config.resume_upper.as_option().copied().unwrap();
        // Same value as our use of `derive_new_compaction_since`.
        let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
        ReclockFollower::new(as_of)
    };

    let (resume_tx, resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let (source_tx, source_rx) = tokio::sync::mpsc::unbounded_channel();
    let (source_upper_tx, source_upper_rx) = tokio::sync::mpsc::unbounded_channel();

    // The use of an _unbounded_ queue here is justified as it matches the unbounded buffers that
    // lie between ordinary timely operators.
    resume_stream.capture_into(UnboundedTokioCapture(resume_tx));
    let reclocked_resume_stream =
        reclock_resume_upper(resume_rx, reclock_follower.share(), worker_id, id);

    let timestamp_desc = source_connection.timestamp_desc();

    let (health_stream, token) = {
        let config = config.clone();
        scope.parent.scoped("SourceTimeDomain", move |scope| {
            let (source, source_upper, health_stream, token) = source_render_operator(
                scope,
                config.clone(),
                source_connection,
                connection_context,
                reclocked_resume_stream,
            );

            // The use of an _unbounded_ queue here is justified as it matches the unbounded
            // buffers that lie between ordinary timely operators.
            source.inner.capture_into(UnboundedTokioCapture(source_tx));
            source_upper.capture_into(UnboundedTokioCapture(source_upper_tx));

            (health_stream.leave(), token)
        })
    };

    // Demux the health streams outside of the reclock operator because reclocking has nothing to do
    // with health streams.
    let partition_count = u64::cast_from(
        config
            .source_exports
            .iter()
            .map(|(_, SourceExport { output_index, .. })| *output_index)
            .max()
            .unwrap_or_default()
            + 1,
    );

    let health_streams: Vec<_> = health_stream
        .partition(partition_count, |(worker_idx, update)| {
            (u64::cast_from(update.output_index), (worker_idx, update))
        });

    let (remap_stream, remap_token) =
        remap_operator(scope, config.clone(), source_upper_rx, timestamp_desc);

    let (streams, _reclock_token) =
        reclock_operator(scope, config, reclock_follower, source_rx, remap_stream);

    let streams = streams
        .into_iter()
        .zip_eq(health_streams.into_iter())
        .map(|((ok, err), health)| (ok, err, health))
        .collect();

    let token = Rc::new((token, remap_token, resume_token));

    (streams, Some(token))
}

type WorkerId = usize;

/// Renders the source dataflow fragment from the given [SourceConnection]. This returns a
/// collection timestamped with the source specific timestamp type. Also returns a second stream
/// that can be used to learn about the `source_upper` that all the source reader instances know
/// about. This second stream will be used by the `remap_operator` to mint new timestamp bindings
/// into the remap shard.
fn source_render_operator<G, C>(
    scope: &mut G,
    config: RawSourceCreationConfig,
    source_connection: C,
    connection_context: ConnectionContext,
    resume_uppers: impl futures::Stream<Item = Antichain<C::Time>> + 'static,
) -> (
    Collection<G, Result<SourceMessage<C::Key, C::Value>, SourceReaderError>, Diff>,
    Stream<G, Infallible>,
    Stream<G, (WorkerId, HealthStatusUpdate)>,
    Rc<dyn Any>,
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

    let (data, progress, health, token) =
        source_connection.render(scope, config, connection_context, resume_uppers);

    let name = format!("SourceStats({})", source_id);
    let mut builder = AsyncOperatorBuilder::new(name, scope.clone());

    let mut data_input = builder.new_input(&data.inner, Pipeline);
    let (mut data_output, data) = builder.new_output();
    let (mut _progress_output, derived_progress) = builder.new_output();
    let (mut health_output, derived_health) = builder.new_output();

    builder.build(move |mut caps| async move {
        let health_cap = caps.pop().unwrap();
        drop(caps);

        let mut statuses_by_idx = BTreeMap::new();

        while let Some(event) = data_input.next_mut().await {
            let AsyncEvent::Data(cap, data) = event else {
                continue;
            };
            for (message, _, _) in data.iter() {
                let status = match message {
                    Ok(source_message) => {
                        HealthStatusUpdate::status(source_message.output, HealthStatus::Running)
                    }
                    Err(ref error) => HealthStatusUpdate::status(
                        error.output,
                        HealthStatus::StalledWithError {
                            error: error.inner.to_string(),
                            hint: None,
                        },
                    ),
                };

                let statuses: &mut Vec<_> = statuses_by_idx.entry(status.output_index).or_default();
                if statuses.last() != Some(&status) {
                    statuses.push(status);
                }

                match message {
                    Ok(message) => {
                        source_statistics.inc_messages_received_by(1);
                        let key_len = u64::cast_from(message.key.len().unwrap_or(0));
                        let value_len = u64::cast_from(message.value.len().unwrap_or(0));
                        source_statistics.inc_bytes_received_by(key_len + value_len);
                    }
                    Err(_) => {}
                }
            }
            data_output.give_container(&cap, data).await;
            for (_, statuses) in statuses_by_idx.iter_mut() {
                health_output.give_container(&health_cap, statuses).await;
            }
        }
    });

    let health = health
        .concat(&derived_health)
        .map(move |status| (worker_id, status));

    (
        data.as_collection(),
        progress.unwrap_or(derived_progress),
        health,
        token,
    )
}

/// Mints new contents for the remap shard based on summaries about the source
/// upper it receives from the raw reader operators.
///
/// Only one worker will be active and write to the remap shard. All source
/// upper summaries will be exchanged to it.
pub(crate) fn health_operator<'g, G: Scope<Timestamp = ()>>(
    scope: &mut Child<'g, G, mz_repr::Timestamp>,
    storage_state: &crate::storage_state::StorageState,
    source_id: GlobalId,
    storage_metadata: CollectionMetadata,
    health_stream: &Stream<G, (usize, HealthStatusUpdate)>,
) -> Rc<dyn Any> {
    // Derived config options
    let healthcheck_worker_id = scope.index();
    let worker_count = scope.peers();
    let now = storage_state.now.clone();
    let persist_clients = Arc::clone(&storage_state.persist_clients);
    let internal_cmd_tx = Rc::clone(&storage_state.internal_cmd_tx);

    // We'll route all the work to a single arbitrary worker;
    // there's not much to do, and we need a global view.
    let chosen_worker_id = usize::cast_from(source_id.hashed()) % worker_count;
    let is_active_worker = chosen_worker_id == healthcheck_worker_id;

    let mut healths = vec![None; worker_count];

    let operator_name = format!("healthcheck({})", healthcheck_worker_id);
    let mut health_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    let health = health_stream.enter(&scope);

    let mut input = health_op.new_input(
        &health,
        Exchange::new(move |_| u64::cast_from(chosen_worker_id)),
    );

    fn overall_status(healths: &[Option<HealthStatus>]) -> Option<&HealthStatus> {
        healths.iter().filter_map(Option::as_ref).max()
    }

    let mut last_reported_status = overall_status(&healths).cloned();

    let button = health_op.build(move |mut _capabilities| async move {
        let persist_client = persist_clients
            .open(storage_metadata.persist_location.clone())
            .await
            .expect("error creating persist client for Healthchecker");

        if is_active_worker {
            if let Some(status_shard) = storage_metadata.status_shard {
                info!("Health for source {source_id} being written to {status_shard}");
            } else {
                trace!("Health for source {source_id} not being written to status shard");
            }
        }

        while let Some(event) = input.next_mut().await {
            if let AsyncEvent::Data(_cap, rows) = event {
                let mut halt_with = None;
                for (worker_id, health_event) in rows.drain(..) {
                    if !is_active_worker {
                        warn!(
                            "Health messages for source {source_id} passed to \
                              an unexpected worker id: {healthcheck_worker_id}"
                        )
                    }

                    let HealthStatusUpdate {
                        update,
                        should_halt,
                        output_index: _,
                    } = health_event;
                    if should_halt {
                        halt_with = Some(update.clone());
                    }
                    healths[worker_id] = Some(update);
                }

                if let Some(new_status) = overall_status(&healths) {
                    if last_reported_status.as_ref() != Some(&new_status) {
                        info!(
                            "Health transition for source {source_id}: \
                              {last_reported_status:?} -> {new_status:?}"
                        );
                        if let Some(status_shard) = storage_metadata.status_shard {
                            write_to_persist(
                                source_id,
                                new_status.name(),
                                new_status.error(),
                                now.clone(),
                                &persist_client,
                                status_shard,
                                &*MZ_SOURCE_STATUS_HISTORY_DESC,
                                new_status.hint(),
                            )
                            .await;
                        }

                        last_reported_status = Some(new_status.clone());
                    }
                }
                // TODO(aljoscha): Instead of threading through the
                // `should_halt` bit, we can give an internal command sender
                // directly to the places where `should_halt = true` originates.
                // We should definitely do that, but this is okay for a PoC.
                if let Some(halt_with) = halt_with {
                    info!(
                        "Broadcasting suspend-and-restart command because of {:?} after {:?} delay",
                        halt_with, SUSPEND_AND_RESTART_DELAY
                    );
                    tokio::time::sleep(SUSPEND_AND_RESTART_DELAY).await;
                    internal_cmd_tx.borrow_mut().broadcast(
                        InternalStorageCommand::SuspendAndRestart {
                            id: source_id,
                            reason: format!("{:?}", halt_with),
                        },
                    );
                }
            }
        }
    });

    Rc::new(button.press_on_drop())
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
fn remap_operator<G, FromTime>(
    scope: &G,
    config: RawSourceCreationConfig,
    mut source_upper_rx: UnboundedReceiver<Event<FromTime, Infallible>>,
    remap_relation_desc: RelationDesc,
) -> (Collection<G, FromTime, Diff>, Rc<dyn Any>)
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
        encoding: _,
        storage_metadata,
        resume_upper,
        source_resume_upper: _,
        base_metrics: _,
        now,
        persist_clients,
        source_statistics: _,
        shared_remap_upper,
        params: _,
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

        let upper_ts = resume_upper.as_option().copied().unwrap();

        // Same value as our use of `derive_new_compaction_since`.
        let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
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

        loop {
            // AsyncInputHandle::next is cancel safe
            tokio::select! {
                _ = ticker.tick() => {
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

    (
        remap_stream.as_collection(),
        Rc::new(button.press_on_drop()),
    )
}

/// Receives un-timestamped batches from the source reader and updates to the
/// remap trace on a second input. This operator takes the remap information,
/// reclocks incoming batches and sends them forward.
fn reclock_operator<G, K, V, FromTime, D>(
    scope: &G,
    config: RawSourceCreationConfig,
    mut timestamper: ReclockFollower<FromTime, mz_repr::Timestamp>,
    mut source_rx: UnboundedReceiver<
        Event<FromTime, (Result<SourceMessage<K, V>, SourceReaderError>, FromTime, D)>,
    >,
    remap_trace_updates: Collection<G, FromTime, Diff>,
) -> (
    Vec<(
        Collection<G, SourceOutput<K, V>, D>,
        Collection<G, SourceError, Diff>,
    )>,
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    K: timely::Data + MaybeLength,
    V: timely::Data + MaybeLength,
    FromTime: SourceTimestamp,
    D: Semigroup + Into<Diff>,
{
    let RawSourceCreationConfig {
        name,
        id,
        source_exports,
        worker_id,
        worker_count: _,
        timestamp_interval: _,
        encoding: _,
        storage_metadata: _,
        resume_upper,
        source_resume_upper: _,
        base_metrics,
        now: _,
        persist_clients: _,
        source_statistics: _,
        shared_remap_upper: _,
        params: _,
    } = config;

    let bytes_read_counter = base_metrics.bytes_read.clone();

    // TODO(petrosagg): figure out what this operator's read requirements are. The code currently
    // relies on the other handle that is present in the source operator to not over compact, which
    // is currently true since it's driven by the resumption frontier. Nevertheless, we should fix
    // this and reason locally instead of globally.
    timestamper.compact(Antichain::new());

    let operator_name = format!("reclock({})", id);
    let mut reclock_op = AsyncOperatorBuilder::new(operator_name, scope.clone());
    let (mut reclocked_output, reclocked_stream) = reclock_op.new_output();

    // Need to broadcast the remap changes to all workers.
    let remap_trace_updates = remap_trace_updates.inner.broadcast();
    let mut remap_input = reclock_op.new_input(&remap_trace_updates, Pipeline);

    reclock_op.build(move |capabilities| async move {
        // The capability of the output after reclocking the source frontier
        let mut cap_set = CapabilitySet::from_elem(capabilities.into_element());

        let mut source_metrics = SourceMetrics::new(&base_metrics, &name, id, &worker_id.to_string());

        source_metrics.resume_upper.set(mz_persist_client::metrics::encode_ts_metric(&resume_upper));

        let mut source_upper = MutableAntichain::new_bottom(FromTime::minimum());

        // Stash of batches that have not yet been timestamped.
        type Batch<K, V, T, D> = Vec<(Result<SourceMessage<K, V>, SourceReaderError>, T, D)>;
        let mut untimestamped_batches: Vec<(FromTime, Batch<K, V, FromTime, D>)> = Vec::new();

        // Stash of reclock updates that are still beyond the upper frontier
        let mut remap_updates_stash = vec![];
        let work_to_do = tokio::sync::Notify::new();
        loop {
            tokio::select! {
                Some(event) = remap_input.next_mut() => match event {
                    AsyncEvent::Data(_cap, data) => remap_updates_stash.append(data),
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
                    // Accumulate updates to offsets for Prometheus and system table metrics collection
                    let mut metric_updates = BTreeMap::new();

                    let mut total_processed = 0;
                    for ((message, from_ts, diff), into_ts) in timestamper.reclock(msgs) {
                        let into_ts = into_ts.expect("reclock for update not beyond upper failed");
                        handle_message(
                            message,
                            from_ts,
                            diff,
                            &mut bytes_read,
                            &cap_set,
                            &mut reclocked_output,
                            &mut metric_updates,
                            into_ts,
                            id,
                        ).await;
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
                    source_metrics.record_partition_offsets(metric_updates);

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

                    // TODO(aljoscha&guswynn): will these be overwritten with multi-worker
                    let ts = into_ready_upper.as_option().cloned().unwrap_or(mz_repr::Timestamp::MAX);
                    for partition_metrics in source_metrics.partition_metrics.values_mut() {
                        partition_metrics.closed_ts.set(ts.into());
                    }

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

    (
        ok_streams
            .into_iter()
            .zip_eq(err_streams.into_iter())
            .collect(),
        None,
    )
}

/// Reclocks an `IntoTime` frontier stream into a `FromTime` frontier stream. This is used for the
/// virtual (through persist) feedback edge so that we convert the `IntoTime` resumption frontier
/// into the `FromTime` frontier that is used with the source's `OffsetCommiter`.
///
/// Note that we also use this async-`Stream` converter as a convenient place to compact the
/// `ReclockFollower` trace that is currently shared between this and the `reclock_operator`.
fn reclock_resume_upper<FromTime, IntoTime>(
    mut resume_rx: UnboundedReceiver<Event<IntoTime, ()>>,
    mut reclock_follower: ReclockFollower<FromTime, IntoTime>,
    worker_id: usize,
    id: GlobalId,
) -> impl futures::stream::Stream<Item = Antichain<FromTime>>
where
    FromTime: SourceTimestamp,
    IntoTime: Timestamp + Lattice + Display,
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

/// Take `message` and assign it the appropriate timestamps and push it into the
/// dataflow layer, if possible.
///
/// TODO: This function is a bit of a mess rn but hopefully this function makes
/// the existing mess more obvious and points towards ways to improve it.
async fn handle_message<K, V, T, D>(
    message: Result<SourceMessage<K, V>, SourceReaderError>,
    time: T,
    diff: D,
    bytes_read: &mut usize,
    cap_set: &CapabilitySet<mz_repr::Timestamp>,
    output_handle: &mut AsyncOutputHandle<
        mz_repr::Timestamp,
        Vec<(
            (usize, Result<SourceOutput<K, V>, SourceError>),
            mz_repr::Timestamp,
            D,
        )>,
        Tee<
            mz_repr::Timestamp,
            (
                (usize, Result<SourceOutput<K, V>, SourceError>),
                mz_repr::Timestamp,
                D,
            ),
        >,
    >,
    metric_updates: &mut BTreeMap<PartitionId, (MzOffset, mz_repr::Timestamp, Diff)>,
    ts: mz_repr::Timestamp,
    source_id: GlobalId,
) where
    K: timely::Data + MaybeLength,
    V: timely::Data + MaybeLength,
    T: SourceTimestamp,
    D: Semigroup,
{
    let (partition, offset) = time
        .try_into_compat_ts()
        .expect("data at invalid timestamp");

    let output = match message {
        Ok(message) => {
            // Note: empty and null payload/keys are currently treated as the same thing.
            if let Some(len) = message.key.len() {
                *bytes_read += len;
            }
            if let Some(len) = message.value.len() {
                *bytes_read += len;
            }

            (
                message.output,
                Ok(SourceOutput::new(
                    message.key,
                    message.value,
                    offset,
                    message.upstream_time_millis,
                    partition.clone(),
                    message.headers,
                )),
            )
        }
        Err(SourceReaderError { output, inner }) => {
            let err = SourceError {
                source_id,
                error: inner,
            };
            (output, Err(err))
        }
    };

    let ts_cap = cap_set.delayed(&ts);
    output_handle.give(&ts_cap, (output, ts, diff)).await;
    match metric_updates.entry(partition) {
        Entry::Occupied(mut entry) => {
            entry.insert((offset, ts, entry.get().2 + 1));
        }
        Entry::Vacant(entry) => {
            entry.insert((offset, ts, 1));
        }
    }
}
