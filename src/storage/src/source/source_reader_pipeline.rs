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
//! Raw sources are streams (currently, Timely streams) of data directly produced by the
//! upstream service. The main export of this module is [`create_raw_source`],
//! which turns [`RawSourceCreationConfig`]s, [`SourceConnection`]s,
//! and [`SourceReader`] implementations into the aforementioned streams.
//!
//! The full source, which is the _differential_ stream that represents the actual object
//! created by a `CREATE SOURCE` statement, is created by composing
//! [`create_raw_source`] with
//! decoding, `SourceEnvelope` rendering, and more.

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]

use std::any::Any;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use differential_dataflow::Hashable;
use futures::future::Either;
use itertools::Itertools;
use mz_timely_util::builder_async::Event;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::feedback::ConnectLoop;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Broadcast, CapabilitySet, Partition};
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;
use tokio_stream::{Stream, StreamExt};
use tracing::trace;

use mz_expr::PartitionId;
use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{GlobalId, Timestamp};
use mz_timely_util::operator::StreamExt as _;

use crate::controller::{CollectionMetadata, ResumptionFrontierCalculator};
use crate::source::antichain::OffsetAntichain;
use crate::source::healthcheck::Healthchecker;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::reclock::ReclockFollower;
use crate::source::reclock::ReclockOperator;
use crate::source::types::SourceConnection;
use crate::source::types::SourceOutput;
use crate::source::types::SourceReaderError;
use crate::source::types::{
    AsyncSourceToken, SourceMessage, SourceMetrics, SourceReader, SourceReaderMetrics, SourceToken,
};
use crate::source::types::{MaybeLength, SourceMessageType};
use crate::source::util::async_source;
use crate::types::connections::ConnectionContext;
use crate::types::errors::SourceError;
use crate::types::sources::encoding::SourceDataEncoding;
use crate::types::sources::MzOffset;

// Interval after which the source operator will yield control.
const YIELD_INTERVAL: Duration = Duration::from_millis(10);

/// Shared configuration information for all source types. This is used in the
/// `create_raw_source` functions, which produce raw sources.
#[derive(Clone)]
pub struct RawSourceCreationConfig {
    /// The name to attach to the underlying timely operator.
    pub name: String,
    /// The name of the upstream resource this source corresponds to
    /// (For example, a Kafka topic)
    pub upstream_name: Option<String>,
    /// The ID of this instantiation of this source.
    pub id: GlobalId,
    /// The number of expected outputs from this ingestion
    pub num_outputs: usize,
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
    pub resume_upper: Antichain<Timestamp>,
    /// A handle to the persist client cache
    pub persist_clients: Arc<Mutex<PersistClientCache>>,
}

/// A batch of messages from a source reader, along with the current upper and
/// any errors that occured while reading that batch.
struct SourceMessageBatch<Key, Value, Diff> {
    messages: HashMap<PartitionId, Vec<(SourceMessage<Key, Value, Diff>, MzOffset)>>,
    /// Any errors that occured while obtaining this batch. TODO: These
    /// non-definite errors should not show up in the dataflows/the persist
    /// shard but it's the current "correct" behaviour. We need to fix this as a
    /// follow-up issue because it's a bigger thing that breaks with the current
    /// behaviour.
    non_definite_errors: Vec<SourceError>,
    /// The current upper of the `SourceReader`, at the time this batch was
    /// emitted. Source uppers of emitted batches must never regress.
    source_upper: OffsetAntichain,
}

/// The source upper at the time of emitting a batch. This contains only the
/// partitions that a given source reader operator is responsible for, so a
/// downstream consumer needs summaries of all source reader operators in order
/// to form a full view of the upper.
#[derive(Clone, Serialize, Deserialize)]
struct SourceUpperSummary {
    source_upper: OffsetAntichain,
}

/// Creates a source dataflow operator graph from a connection that has a
/// corresponding [`SourceReader`] implementation. The type of SourceConnection
/// determines the type of connection that _should_ be created.
///
/// This is also the place where _reclocking_
/// (<https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210714_reclocking.md>)
/// happens.
///
/// See the [`source` module docs](crate::source) for more details about how raw
/// sources are used.
pub fn create_raw_source<G, S: 'static, R>(
    scope: &G,
    config: RawSourceCreationConfig,
    source_connection: S::Connection,
    connection_context: ConnectionContext,
    calc: R,
) -> (
    (
        Vec<timely::dataflow::Stream<G, SourceOutput<S::Key, S::Value, S::Diff>>>,
        timely::dataflow::Stream<G, SourceError>,
    ),
    Option<Rc<dyn Any>>,
)
where
    G: Scope<Timestamp = Timestamp> + Clone,
    S: SourceReader,
    R: ResumptionFrontierCalculator<Timestamp> + 'static,
{
    let (resume_stream, source_reader_feedback_handle) =
        super::resumption::resumption_operator(scope, config.clone(), calc);

    let reclock_follower = {
        let upper_ts = config.resume_upper.as_option().copied().unwrap();
        let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
        ReclockFollower::new(as_of)
    };

    let ((batches, source_upper_summaries, resumption_feedback_stream), source_reader_token) =
        source_reader_operator::<G, S>(
            scope,
            config.clone(),
            source_connection,
            connection_context,
            reclock_follower.share(),
            &resume_stream,
        );
    resumption_feedback_stream.connect_loop(source_reader_feedback_handle);

    let (remap_stream, remap_token) = remap_operator::<G, S>(
        scope,
        config.clone(),
        source_upper_summaries,
        &resume_stream,
    );

    let ((reclocked_stream, reclocked_err_stream), _reclock_token) =
        reclock_operator::<G, S>(scope, config, reclock_follower, batches, remap_stream);

    let token = Rc::new((source_reader_token, remap_token));

    ((reclocked_stream, reclocked_err_stream), Some(token))
}

/// A type-alias that represents actual data coming out of the source reader.
// Rust doesn't actually type-check the aliases until they are used, so we
// can do `<S as SourceReader>` as we please here.
type MessageAndOffset<S> = (
    SourceMessage<<S as SourceReader>::Key, <S as SourceReader>::Value, <S as SourceReader>::Diff>,
    MzOffset,
);

/// A type that represents data coming out of the source reader, in addition
/// to other information it needs to communicate to various operators.
struct SourceReaderOperatorOutput<S: SourceReader> {
    /// Messages and their offsets from the source reader.
    messages: HashMap<PartitionId, Vec<MessageAndOffset<S>>>,
    /// See `SourceMessageBatch`.
    non_definite_errors: Vec<SourceReaderError>,
    /// A list of partitions that this source reader instance
    /// is sure it doesn't care about. Required so the
    /// remap operator can eventually determine whether
    /// a timestamp is closed.
    unconsumed_partitions: Vec<PartitionId>,
    /// See `SourceMessageBatch`.
    source_upper: OffsetAntichain,
    /// Marks whether or not this batch contains
    /// a `Finalized` message, or if its empty.
    /// Used to downgrade the `batch_counter` frontier.
    is_final: bool,
}

fn build_source_reader_stream<G, S>(
    source_reader: S,
    config: RawSourceCreationConfig,
    source_connection: S::Connection,
    initial_source_upper: OffsetAntichain,
    mut source_upper: OffsetAntichain,
) -> Pin<
    // TODO(guswynn): determine if this boxing is necessary
    Box<impl Stream<Item = Option<SourceReaderOperatorOutput<S>>>>,
>
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader + 'static,
{
    let RawSourceCreationConfig {
        name,
        upstream_name,
        id,
        num_outputs: _,
        worker_id,
        worker_count,
        timestamp_interval,
        encoding: _,
        storage_metadata,
        resume_upper: _,
        base_metrics: _,
        now,
        persist_clients,
    } = config;
    Box::pin(async_stream::stream!({
        let mut healthchecker = if storage_metadata.status_shard.is_some() {
            match Healthchecker::new(
                name.clone(),
                upstream_name,
                id,
                source_connection.name(),
                worker_id,
                worker_count,
                true,
                &persist_clients,
                &storage_metadata,
                now.clone(),
            )
            .await
            {
                Ok(h) => Some(h),
                Err(e) => {
                    panic!(
                        "Failed to create healthchecker for source {}: {:#}",
                        &name, e
                    );
                }
            }
        } else {
            None
        };

        // Send along an empty batch, so that the reclock operator knows
        // about the current frontier. Otherwise, if there are no new
        // messages after a restart, the reclock operator would be stuck and
        // not advance its downstream frontier.
        yield Some(SourceReaderOperatorOutput {
            messages: HashMap::new(),
            non_definite_errors: Vec::new(),
            unconsumed_partitions: Vec::new(),
            source_upper: initial_source_upper,
            is_final: true,
        });

        let source_stream = source_reader.into_stream(timestamp_interval).fuse();

        tokio::pin!(source_stream);

        // Emit batches more frequently than we mint new timestamps. We're
        // hoping that most of the batches that we emit will end up making
        // it into the freshly minted bindings when remap_operator ticks.
        let mut emission_interval = tokio::time::interval(timestamp_interval / 5);
        emission_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut untimestamped_messages = HashMap::<_, Vec<_>>::new();
        let mut unconsumed_partitions = Vec::new();
        let mut non_definite_errors = vec![];
        // `None` is the default value, reset after we have sent a `Finalized` batch.
        let mut is_final = None;
        loop {
            // TODO(guswyn): move lots of this out of the macro so rustfmt works better
            tokio::select! {
                // N.B. This branch is cancel-safe because `next` only borrows the underlying stream.
                item = source_stream.next() => {
                    match item {
                        Some(Ok(message)) => {

                            // Note that this
                            // 1. Requires that sources that produce
                            //    `InProgress` messages ALWAYS produce a
                            //    `Finalized` for the final message.
                            // 2. Requires that sources that produce
                            //    `InProgress` messages NEVER produces
                            //    messages at offsets below the most recent
                            //    `Finalized` message.
                            if matches!(message, SourceMessageType::Finalized(_)) {
                                is_final = Some(true);
                            }
                            if matches!(message, SourceMessageType::InProgress(_)) {
                                is_final = Some(false);
                            }
                            match message {
                                SourceMessageType::DropPartitionCapabilities(mut pids) => {
                                    unconsumed_partitions.append(&mut pids);
                                }
                                SourceMessageType::Finalized(message) | SourceMessageType::InProgress(message) => {
                                    let pid = message.partition.clone();
                                    let offset = message.offset;
                                    // advance the _offset_ frontier if this the final message for that offset
                                    //
                                    // TODO(guswynn): when we remove this, we can simplify some of
                                    // the code in the reclock operator (probably), but using
                                    // subtraction/addition on the upper we pass through.
                                    if let Some(true) = is_final {
                                        source_upper.insert_data_up_to(pid.clone(), offset);
                                    }
                                    untimestamped_messages.entry(pid).or_default().push((message, offset));
                                }
                                SourceMessageType::SourceStatus(update) => {
                                    if let Some(healthchecker) = &mut healthchecker {
                                        healthchecker.update_status(update).await;
                                    }
                                }
                            }
                        }
                        // TODO: Report these errors to the Healthchecker!
                        Some(Err(e)) => {
                            non_definite_errors.push(e);
                        }
                        None => {
                            // This source reader is done. Yield one final
                            // update of the source_upper.
                            yield Some(
                                SourceReaderOperatorOutput {
                                    messages: std::mem::take(&mut untimestamped_messages),
                                    non_definite_errors: non_definite_errors.drain(..).collect_vec(),
                                    unconsumed_partitions,
                                    source_upper: source_upper.clone(),
                                    is_final: true
                                }
                            );

                            // Then, let the consumer know we're done.
                            yield None;

                            return;
                        },
                    }
                }
                // It's time to emit a batch of messages
                _ = emission_interval.tick() => {

                    if !untimestamped_messages.is_empty() {
                        trace!("source_reader({id}) {worker_id}/{worker_count}: \
                              emitting new batch. \
                              untimestamped_messages.len(): {} \
                              unconsumed_partitions: {:?} \
                              source_upper: {:?}",
                              untimestamped_messages.len(),
                              unconsumed_partitions.clone(),
                              source_upper);
                    }

                    // Emit empty batches as well. Just to keep downstream
                    // operators informed about the unconsumed partitions
                    // and the source upper.
                    yield Some(
                        SourceReaderOperatorOutput {
                            messages: std::mem::take(&mut untimestamped_messages),
                            non_definite_errors: non_definite_errors.drain(..).collect_vec(),
                            unconsumed_partitions: unconsumed_partitions.clone(),
                            source_upper: source_upper.clone(),
                            // Consider empty batches that do not fall in the middle of
                            // InProgress and Finalized messages to be final.
                            is_final: is_final.unwrap_or(true),
                        }
                    );
                    if let Some(true) = is_final {
                        // reset is_final so empty batches between sets of messages with
                        // the same offset can be considered final.
                        is_final = None;
                    } else {
                        is_final = Some(false);
                    }
                }
            }
        }
    }))
}

/// Reads from a [`SourceReader`] and returns a stream of "un-timestamped"
/// [`SourceMessageBatch`]. Also returns a second stream that can be used to
/// learn about the `source_upper` that all the source reader instances now
/// about. This second stream will be used by `remap_operator` to mint new
/// timestamp bindings into the remap shard. The third stream is to feedback
/// a frontier for the `resumption_operator` to inspect. For now, this
/// stream produces NO data.
fn source_reader_operator<G, S: 'static>(
    scope: &G,
    config: RawSourceCreationConfig,
    source_connection: S::Connection,
    connection_context: ConnectionContext,
    reclock_follower: ReclockFollower,
    resume_stream: &timely::dataflow::Stream<G, ()>,
) -> (
    (
        timely::dataflow::Stream<
            G,
            Rc<RefCell<Option<SourceMessageBatch<S::Key, S::Value, S::Diff>>>>,
        >,
        timely::dataflow::Stream<G, SourceUpperSummary>,
        timely::dataflow::Stream<G, ()>,
    ),
    Option<AsyncSourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader,
{
    let sub_config = config.clone();
    let RawSourceCreationConfig {
        name,
        upstream_name: _,
        id,
        num_outputs: _,
        worker_id,
        worker_count,
        timestamp_interval: _,
        encoding,
        storage_metadata: _,
        resume_upper: initial_resume_upper,
        base_metrics,
        now: _,
        persist_clients: _,
    } = config;

    let resume_upper = initial_resume_upper.clone();
    let (stream, capability) = async_source(
        scope,
        name.clone(),
        resume_stream,
        move |info: OperatorInfo, mut cap_set, mut resume_input, mut output| {
            // TODO(guswynn): should sources still be able to self-activate?
            // probably not, so we should remove this
            let sync_activator = scope.sync_activator_for(&info.address[..]);

            async move {
                // Setup time!
                let mut source_metrics = SourceReaderMetrics::new(&base_metrics, id);

                // Required to build the initial source_upper and to ensure the offset committer
                // operator works correctly.
                reclock_follower
                    .ensure_initialized_to(initial_resume_upper.borrow())
                    .await;

                let mut source_upper = reclock_follower
                    .source_upper_at_frontier(resume_upper.borrow())
                    .expect("source_upper_at_frontier to be used correctly");

                for (pid, offset) in source_upper.iter() {
                    source_metrics
                        .metrics_for_partition(pid)
                        .source_resume_upper
                        .set(offset.offset)
                }

                // Save this to pass into the stream creation
                let initial_source_upper = source_upper.clone();

                trace!("source_reader({id}) {worker_id}/{worker_count}: source_upper before thinning: {source_upper:?}");
                source_upper.filter_by_partition(|pid| {
                    crate::source::responsible_for(&id, worker_id, worker_count, pid)
                });
                trace!("source_reader({id}) {worker_id}/{worker_count}: source_upper after thinning: {source_upper:?}");

                let (source_reader, offset_committer) = S::new(
                    name.clone(),
                    id,
                    worker_id,
                    worker_count,
                    sync_activator,
                    source_connection.clone(),
                    source_upper.as_vec(),
                    encoding,
                    base_metrics,
                    connection_context.clone(),
                )
                .expect("Failed to create source");

                let offset_commit_handle = crate::source::commit::drive_offset_committer(
                    offset_committer,
                    id,
                    worker_id,
                    worker_count,
                );

                let mut source_reader = build_source_reader_stream::<G, S>(
                    source_reader,
                    sub_config,
                    source_connection,
                    initial_source_upper,
                    source_upper,
                );

                // WIP: Should we have these metrics for all three involved
                // operators?
                // source_metrics.operator_scheduled_counter.inc();

                let mut timer = Instant::now();

                // We just use an advancing number for our capability. No one cares
                // about what this actually is, downstream.
                let mut batch_counter = timely::progress::Timestamp::minimum();

                loop {
                    let srnf = source_reader.next();
                    tokio::pin!(srnf);
                    let ri = resume_input.next();
                    tokio::pin!(ri);

                    // `StreamExt::next` and `AsyncInputHandle::next` are both cancel-safe.
                    let changes = futures::future::select(srnf, ri).await;
                    let update = match changes {
                        Either::Left((update, _)) => update,
                        Either::Right((Some(Event::Progress(resume_frontier_update)), _)) => {
                            // The first message from the resumption frontier source
                            // could be the same frontier as the initialization frontier, so we
                            // just move on
                            if PartialOrder::less_equal(
                                &resume_frontier_update,
                                &initial_resume_upper,
                            ) {
                                continue;
                            }
                            tracing::trace!(
                                %id,
                                resumption_frontier = ?resume_frontier_update,
                                "received new resumption frontier"
                            );
                            let mut offset_upper = reclock_follower
                                .source_upper_at_frontier(resume_frontier_update.borrow())
                                .unwrap();
                            offset_upper.filter_by_partition(|pid| {
                                crate::source::responsible_for(&id, worker_id, worker_count, pid)
                            });
                            offset_commit_handle.commit_offsets(offset_upper.as_data_offsets());

                            // Compact the in-memory remap trace shared between this
                            // operator and the reclock operator. We do this here for convenience! The
                            // ordering doesn't really matter. `unwrap` is fine
                            // because the `resumption_frontier` never goes to
                            // `[]` currently.
                            let upper_ts = resume_upper.as_option().copied().unwrap();
                            let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
                            reclock_follower.compact(as_of);
                            continue;
                        }
                        _ => {
                            continue;
                        }
                    };

                    let update = match update {
                        Some(update) => update,
                        None => {
                            return;
                        }
                    };
                    let SourceReaderOperatorOutput {
                        messages,
                        non_definite_errors,
                        unconsumed_partitions,
                        source_upper,
                        is_final,
                    } = match update {
                        Some(update) => update,
                        None => {
                            trace!("source_reader({id}) {worker_id}/{worker_count}: is terminated");
                            // We will never produce more data, clear our capabilities to
                            // communicate this downstream.
                            cap_set.downgrade(&[]);
                            return;
                        }
                    };

                    trace!(
                        "create_source_raw({id}) {worker_id}/\
                        {worker_count}: message_batch.len(): {:?}",
                        messages.len()
                    );
                    trace!(
                        "create_source_raw({id}) {worker_id}/{worker_count}: source_upper: {:?}",
                        source_upper
                    );

                    // We forward only the partitions that we are responsible for to
                    // the remap operator.
                    let source_upper_summary = SourceUpperSummary {
                        source_upper: source_upper.clone(),
                    };

                    // Pull the upper to `max` for partitions that we are not
                    // responsible for. That way, the downstream reclock operator
                    // can correctly decide when a reclocked timestamp is closed. We
                    // basically take those partitions "out of the calculation".
                    //
                    // TODO(guswynn): factor this into `OffsetAntichain` in a way that makes sense
                    let mut extended_source_upper = source_upper.clone();
                    extended_source_upper.extend(
                        unconsumed_partitions
                            .into_iter()
                            .map(|pid| (pid, MzOffset { offset: u64::MAX })),
                    );

                    let non_definite_errors = non_definite_errors
                        .into_iter()
                        .map(|e| SourceError {
                            source_id: id,
                            error: e.inner,
                        })
                        .collect_vec();

                    let message_batch = SourceMessageBatch {
                        messages,
                        non_definite_errors,
                        source_upper: extended_source_upper,
                    };
                    // Wrap in an Rc to avoid cloning when sending it on.
                    let message_batch = Rc::new(RefCell::new(Some(message_batch)));

                    let cap = cap_set.delayed(&batch_counter);
                    {
                        let mut output = output.activate();
                        let mut session = output.session(&cap);

                        session.give((message_batch, source_upper_summary));
                    }
                    if is_final {
                        cap_set.downgrade(&[batch_counter]);
                    }

                    batch_counter = batch_counter.checked_add(1).expect("exhausted counter");

                    if timer.elapsed() > YIELD_INTERVAL {
                        timer = Instant::now();
                        tokio::task::yield_now().await;
                    }
                }
            }
        },
    );

    // TODO: Roll all this into one source operator.
    let operator_name = format!("source_reader({})-demux", id);
    let mut demux_op = OperatorBuilder::new(operator_name, scope.clone());

    let mut input = demux_op.new_input(&stream, Pipeline);
    let (mut batch_output, batch_stream) = demux_op.new_output();
    let (mut summary_output, summary_stream) = demux_op.new_output();
    let (_feedback_output, feedback_stream) = demux_op.new_output();
    let summary_output_port = summary_stream.name().port;

    demux_op.build(move |_caps| {
        let mut buffer = Vec::new();

        move |_frontiers| {
            input.for_each(|cap, data| {
                data.swap(&mut buffer);

                let mut batch_output = batch_output.activate();
                let mut summary_output = summary_output.activate();

                for (message_batch, source_upper) in buffer.drain(..) {
                    let mut session = batch_output.session(&cap);
                    session.give(message_batch);

                    let summary_cap = cap.delayed_for_output(cap.time(), summary_output_port);
                    let mut session = summary_output.session(&summary_cap);
                    session.give(source_upper);
                }
            });
        }
    });

    (
        (batch_stream, summary_stream, feedback_stream),
        Some(capability),
    )
}

/// Mints new contents for the remap shard based on summaries about the source
/// upper it receives from the raw reader operators.
///
/// Only one worker will be active and write to the remap shard. All source
/// upper summaries will be exchanged to it.
fn remap_operator<G, S: 'static>(
    scope: &G,
    config: RawSourceCreationConfig,
    source_upper_summaries: timely::dataflow::Stream<G, SourceUpperSummary>,
    resume_stream: &timely::dataflow::Stream<G, ()>,
) -> (
    timely::dataflow::Stream<G, HashMap<PartitionId, Vec<(Timestamp, MzOffset)>>>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader,
{
    let RawSourceCreationConfig {
        name,
        upstream_name: _,
        id,
        num_outputs: _,
        worker_id,
        worker_count,
        timestamp_interval,
        encoding: _,
        storage_metadata,
        resume_upper,
        base_metrics: _,
        now,
        persist_clients,
    } = config;

    let chosen_worker = (id.hashed() % worker_count as u64) as usize;
    let active_worker = chosen_worker == worker_id;

    let operator_name = format!("remap({})", id);
    let mut remap_op = OperatorBuilder::new(operator_name, scope.clone());
    let (mut remap_output, remap_stream) = remap_op.new_output();

    let mut input = remap_op.new_input_connection(
        &source_upper_summaries,
        Exchange::new(move |_x| chosen_worker as u64),
        // We don't want frontier information to flow from the input to the
        // output. This operator is it's own "root source" of capabilities for
        // current reclocked, wall-clock time.
        vec![Antichain::new()],
    );

    let _resume_input = remap_op.new_input_connection(
        resume_stream,
        Pipeline,
        // We don't need this to participate in progress
        // tracking, we just need to periodically
        // introspect its frontier.
        vec![Antichain::new()],
    );

    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);

    // TODO(guswynn): figure out how to make this a native async operator without problems
    remap_op.build_async(
        scope.clone(),
        move |mut capabilities, frontiers, scheduler| async move {
            let mut buffer = Vec::new();
            let mut cap_set = if active_worker {
                CapabilitySet::from_elem(capabilities.pop().expect("missing capability"))
            } else {
                CapabilitySet::new()
            };
            // Explicitly release the unneeded capabilities!
            capabilities.clear();

            let upper_ts = resume_upper.as_option().copied().unwrap();
            let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
            let mut timestamper = match ReclockOperator::new(
                Arc::clone(&persist_clients),
                storage_metadata.clone(),
                now.clone(),
                timestamp_interval.clone(),
                as_of,
            )
            .await
            {
                Ok(t) => t,
                Err(e) => {
                    panic!("Failed to create source {} timestamper: {:#}", name, e);
                }
            };
            // The global view of the source_upper, which we track by combining
            // summaries from the raw reader operators.
            let mut global_source_upper = timestamper
                .source_upper_at_frontier(resume_upper.borrow())
                .expect("source_upper_at_frontier to be used correctly");

            if active_worker {
                let new_ts_upper = timestamper
                    .reclock_frontier(&global_source_upper)
                    .expect("compacted past upper");

                cap_set.downgrade(new_ts_upper);

                // Emit initial snapshot of the remap_shard, bootstrapping
                // downstream reclock operators.
                let remap_trace = timestamper.remap_trace();
                trace!(
                    "remap({id}) {worker_id}/{worker_count}: \
                    emitting initial remap_trace. \
                    source_upper: {:?} \
                    trace_updates: {:?}",
                    global_source_upper,
                    remap_trace
                );

                let mut remap_output = remap_output.activate();
                let cap = cap_set.delayed(cap_set.first().unwrap());
                let mut session = remap_output.session(&cap);
                session.give(remap_trace);
            }

            // The last frontier we compacted the remap shard to, starting at [0].
            let mut last_compaction_since = Antichain::from_elem(Timestamp::default());

            while scheduler.notified().await {
                if token_weak.upgrade().is_none() {
                    // Make sure we don't accidentally mint new updates when
                    // this source has been dropped. This way, we also make sure
                    // to not react to spurious frontier advancements to `[]`
                    // that happen when the input source operator is shutting
                    // down.
                    return;
                }

                if !active_worker {
                    // We simply loop because we cannot return here. Otherwise
                    // the one active worker would not get frontier upgrades
                    // anymore or other things could break.
                    //
                    // TODO: Get rid of this once the async operator wrapper is
                    // fixed.
                    continue;
                }

                // Wait until we know that we can mint new bindings. Any new
                // summaries that we read from out input would not show up in
                // the remap shard until that time anyways.
                if let Err(wait_time) = timestamper.next_mint_timestamp() {
                    tokio::time::sleep(wait_time).await;
                }

                // Every time we are woken up, we also attempt to compact the remap shard,
                // but only if it has actually made progress. Note that resumption frontier
                // progress does not drive this operator forward, only source upper updates
                // from the source_reader_operator does.
                //
                // Note that we are able to compact to JUST before the resumption frontier.
                let upper_ts = frontiers.borrow()[1].as_option().copied().unwrap();
                let compaction_since = Antichain::from_elem(upper_ts.saturating_sub(1));
                if PartialOrder::less_than(&last_compaction_since, &compaction_since) {
                    trace!(
                        "remap({id}) {worker_id}/{worker_count}: compacting remap \
                        shard to: {:?}",
                        compaction_since,
                    );
                    timestamper.compact(compaction_since.clone()).await;
                    last_compaction_since = compaction_since;
                }

                input.for_each(|_cap, data| {
                    data.swap(&mut buffer);

                    for source_upper_summary in buffer.drain(..) {
                        for (pid, offset) in source_upper_summary.source_upper.iter() {
                            let previous_offset =
                                global_source_upper.insert(pid.clone(), offset.clone());

                            // TODO(guswynn&aljoscha&petrosagg): ensure this operator does not
                            // require ordered input.
                            if let Some(previous_offset) = previous_offset {
                                assert!(previous_offset <= *offset);
                            }
                        }
                    }
                });

                let remap_trace_updates = timestamper.mint(&global_source_upper).await;

                timestamper.advance().await;
                let new_ts_upper = timestamper
                    .reclock_frontier(&global_source_upper)
                    .expect("compacted past upper");

                trace!(
                    "remap({id}) {worker_id}/{worker_count}: minted new bindings. \
                    source_upper: {:?} \
                    trace_updates: {:?} \
                    new_ts_upper: {:?}",
                    global_source_upper,
                    remap_trace_updates,
                    new_ts_upper
                );

                // Out of an abundance of caution, do not hold the output handle
                // across an await, and drop it before we downgrade the capability.
                {
                    let mut remap_output = remap_output.activate();
                    let cap = cap_set.delayed(cap_set.first().unwrap());
                    let mut session = remap_output.session(&cap);
                    session.give(remap_trace_updates);
                }

                cap_set.downgrade(new_ts_upper);

                // Make sure we do this after writing any timestamp bindings to
                // the remap shard that might be needed for the reported source
                // uppers.
                let input_frontier = &frontiers.borrow()[0];
                if input_frontier.is_empty() {
                    cap_set.downgrade(&[]);
                    return;
                }
            }
        },
    );

    (remap_stream, token)
}

/// Receives un-timestamped batches from the source reader and updates to the
/// remap trace on a second input. This operator takes the remap information,
/// reclocks incoming batches and sends them forward.
fn reclock_operator<G, S: 'static>(
    scope: &G,
    config: RawSourceCreationConfig,
    timestamper: ReclockFollower,
    batches: timely::dataflow::Stream<
        G,
        Rc<RefCell<Option<SourceMessageBatch<S::Key, S::Value, S::Diff>>>>,
    >,
    remap_trace_updates: timely::dataflow::Stream<
        G,
        HashMap<PartitionId, Vec<(Timestamp, MzOffset)>>,
    >,
) -> (
    (
        Vec<timely::dataflow::Stream<G, SourceOutput<S::Key, S::Value, S::Diff>>>,
        timely::dataflow::Stream<G, SourceError>,
    ),
    Option<SourceToken>,
)
where
    G: Scope<Timestamp = Timestamp>,
    S: SourceReader,
{
    let RawSourceCreationConfig {
        name,
        upstream_name,
        id,
        num_outputs,
        worker_id,
        worker_count,
        timestamp_interval: _,
        encoding: _,
        storage_metadata: _,
        resume_upper,
        base_metrics,
        now: _,
        persist_clients: _,
    } = config;

    let bytes_read_counter = base_metrics.bytes_read.clone();

    let operator_name = format!("reclock({})", id);
    let mut reclock_op = OperatorBuilder::new(operator_name, scope.clone());
    let (mut reclocked_output, reclocked_stream) = reclock_op.new_output();

    let mut batch_input = reclock_op.new_input_connection(
        &batches,
        Pipeline,
        // We don't want frontier information to flow from the input to the
        // output. The second input (the remap information) should be the stream
        // that drives downstream timestamps.
        vec![Antichain::new()],
    );
    // Need to broadcast the remap changes to all workers.
    let remap_trace_updates = remap_trace_updates.broadcast();
    let mut remap_input = reclock_op.new_input(&remap_trace_updates, Pipeline);

    reclock_op.build(move |mut capabilities| {
        capabilities.clear();

        let metrics_name = upstream_name.clone().unwrap_or_else(|| name.clone());
        let mut source_metrics =
            SourceMetrics::new(&base_metrics, &metrics_name, id, &worker_id.to_string());

        source_metrics
            .resume_upper
            .set(mz_persist_client::metrics::encode_ts_metric(&resume_upper));

        // Use this to retain capabilities from the remap_operator input.
        let mut cap_set = CapabilitySet::new();
        let mut batch_buffer = Vec::new();
        let mut remap_trace_buffer = Vec::new();

        // The global view of the source_upper, which we track by combining
        // summaries from the raw reader operators.
        let mut global_source_upper = OffsetAntichain::new();
        // For each partition, track the batch time and offset of the most recent
        // batch. When the `batch_input` frontier has advanced past this batch time
        // the `global_source_upper` can be advanced.
        let mut batch_times: HashMap<PartitionId, (Timestamp, MzOffset)> = HashMap::new();

        let mut untimestamped_batches = VecDeque::new();

        move |frontiers| {
            batch_input.for_each(|cap, data| {
                data.swap(&mut batch_buffer);
                for batch in batch_buffer.drain(..) {
                    let batch = batch
                        .borrow_mut()
                        .take()
                        .expect("batch already taken, but we should be the only consumer");
                    for (pid, offset) in batch.source_upper.iter() {
                        // If we have a time for the batch in `batch_times` already, we go onto
                        // checking the frontier. Otherwise, place the time and offset in the map
                        // and wait for the next empty batch to push us along.
                        if let Some((batch_ts, advance_to)) = batch_times.get(pid) {
                            trace!(
                                "reclock({id}) {worker_id}/{worker_count}: batch frontier: {:?}, \
                                processing batch at ts: {}, \
                                wanting to advance to: {:?}",
                                frontiers[0].frontier(),
                                batch_ts,
                                advance_to
                            );
                            // If the batch_input frontier has advanced past this batch time, then
                            // we can advance the `global_source_upper`, which will later downgrade
                            // `cap_set`, as we are sure we have seen all messages for this
                            // `source_upper`.
                            let should_advance =
                                if let Some(frontier_ts) = frontiers[0].frontier().as_option() {
                                    frontier_ts > batch_ts
                                } else {
                                    // The empty frontier is trivially past all times
                                    true
                                };

                            if should_advance {
                                global_source_upper.insert(pid.clone(), *advance_to);
                                // We also replace the value in `batch_times` with the new
                                // offset, associated with the frontier, to move it along
                                batch_times.insert(pid.clone(), (cap.time().clone(), *offset));
                                continue;
                            } else if offset > advance_to {
                                // Otherwise we advance to the higher offset, assuming
                                // that eventually the batch frontier will eventually
                                // go past this time.
                                batch_times.insert(pid.clone(), (cap.time().clone(), *offset));
                            }
                        } else {
                            batch_times.insert(pid.clone(), (cap.time().clone(), *offset));
                        }
                    }
                    untimestamped_batches.push_back(batch)
                }
            });

            // If this is the last set of updates, we finalize
            // the frontier.
            if frontiers[0].frontier().is_empty() {
                batch_times.drain().for_each(|(pid, (_, offset))| {
                    global_source_upper.insert(pid, offset);
                });
            }

            remap_input.for_each(|cap, data| {
                data.swap(&mut remap_trace_buffer);
                for update in remap_trace_buffer.drain(..) {
                    timestamper.push_trace_updates(update.into_iter())
                }
                cap_set.insert(cap.retain());
            });

            let remap_frontier = &frontiers[1];
            trace!(
                "reclock({id}) {worker_id}/{worker_count}: remap frontier: {:?}",
                remap_frontier.frontier()
            );
            timestamper.push_upper_update(remap_frontier.frontier().to_owned());

            // Accumulate updates to bytes_read for Prometheus metrics collection
            let mut bytes_read = 0;
            // Accumulate updates to offsets for system table metrics collection
            let mut metric_updates = HashMap::new();

            trace!(
                "reclock({id}) {worker_id}/{worker_count}: \
                untimestamped_batches.len(): {}",
                untimestamped_batches.len(),
            );

            while let Some(untimestamped_batch) = untimestamped_batches.front_mut() {
                // This scope is necessary to convince rustc that `untimestamped_batches` is unused
                // when we pop from the front at the bottom of this loop.
                {
                    let reclocked = match timestamper.reclock(&mut untimestamped_batch.messages) {
                        Ok(reclocked) => reclocked,
                        Err((pid, offset)) => panic!("failed to reclock {} @ {}", pid, offset),
                    };

                    let reclocked = match reclocked {
                        Some(reclocked) => reclocked,
                        None => {
                            trace!(
                                "reclock({id}) {worker_id}/{worker_count}: \
                                cannot yet reclock batch with source frontier {:?} \
                                reclock.source_frontier: {:?}",
                                untimestamped_batch.source_upper,
                                timestamper.source_upper()
                            );
                            // We keep batches in the order they arrive from the
                            // source. And we assume that the source frontier never
                            // regressses. So we can break now.
                            break;
                        }
                    };

                    let mut output = reclocked_output.activate();

                    reclocked.for_each(|message, ts| {
                        trace!(
                            "reclock({id}) {worker_id}/{worker_count}: \
                                handling reclocked message: {:?}:{:?} -> {}",
                            message.partition,
                            message.offset,
                            ts
                        );
                        handle_message::<S>(
                            message,
                            &mut bytes_read,
                            &cap_set,
                            &mut output,
                            &mut metric_updates,
                            ts,
                        )
                    });

                    // TODO: We should not emit the non-definite errors as
                    // DataflowErrors, which will make them end up on the persist
                    // shard for this source. Instead they should be reported to the
                    // Healthchecker. But that's future work.
                    if !untimestamped_batch.non_definite_errors.is_empty() {
                        // If there are errors, it means that someone must also have
                        // given us a capability because a batch/batch-summary was
                        // emitted to the remap operator.
                        let err_cap = cap_set.delayed(
                            cap_set
                                .first()
                                .expect("missing a capability for emitting errors"),
                        );
                        let mut session = output.session(&err_cap);
                        let errors = untimestamped_batch
                            .non_definite_errors
                            .iter()
                            .map(|e| (0, Err(e.clone())));
                        session.give_iterator(errors);
                    }
                }

                // Pop off the processed batch.
                untimestamped_batches.pop_front();
            }

            bytes_read_counter.inc_by(bytes_read as u64);
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
                    .unwrap_or(Timestamp::MAX)
                    .into(),
            );

            // It can happen that our view of the global source_upper is not yet
            // up to date with what the ReclockOperator thinks. We will
            // evantually learn about an up-to-date frontier in a future
            // invocation.
            //
            // Note that this even if the `global_source_upper` has advanced past
            // the remap input, this holds back capability downgrades until
            // they match.
            if let Ok(new_ts_upper) = timestamper.reclock_frontier(&global_source_upper) {
                let ts = new_ts_upper.as_option().cloned().unwrap_or(Timestamp::MAX);

                // TODO(aljoscha&guswynn): will these be overwritten with multi-worker
                for partition_metrics in source_metrics.partition_metrics.values_mut() {
                    partition_metrics.closed_ts.set(ts.into());
                }

                if !cap_set.is_empty() {
                    trace!(
                        "reclock({id}) {worker_id}/{worker_count}: \
                        downgrading to {:?}",
                        new_ts_upper
                    );

                    cap_set
                        .try_downgrade(new_ts_upper.iter())
                        .expect("cannot downgrade in reclock");
                }
            }
        }
    });

    let (ok_muxed_stream, err_stream) =
        reclocked_stream.map_fallible("reclock-demux-ok-err", |(output, r)| match r {
            Ok(ok) => Ok((output, ok)),
            Err(err) => Err(err),
        });

    let ok_streams = ok_muxed_stream.partition(u64::cast_from(num_outputs), |(output, data)| {
        (u64::cast_from(output), data)
    });

    ((ok_streams, err_stream), None)
}

/// Take `message` and assign it the appropriate timestamps and push it into the
/// dataflow layer, if possible.
///
/// TODO: This function is a bit of a mess rn but hopefully this function makes
/// the existing mess more obvious and points towards ways to improve it.
fn handle_message<S: SourceReader>(
    message: SourceMessage<S::Key, S::Value, S::Diff>,
    bytes_read: &mut usize,
    cap_set: &CapabilitySet<Timestamp>,
    output: &mut OutputHandle<
        Timestamp,
        (
            usize,
            Result<SourceOutput<S::Key, S::Value, S::Diff>, SourceError>,
        ),
        Tee<
            Timestamp,
            (
                usize,
                Result<SourceOutput<S::Key, S::Value, S::Diff>, SourceError>,
            ),
        >,
    >,
    metric_updates: &mut HashMap<PartitionId, (MzOffset, Timestamp, i64)>,
    ts: Timestamp,
) {
    let partition = message.partition.clone();
    let offset = message.offset;

    // Note: empty and null payload/keys are currently treated as the same
    // thing.
    let key = message.key;
    let out = message.value;
    // Entry for partition_metadata is guaranteed to exist as messages are only
    // processed after we have updated the partition_metadata for a partition
    // and created a partition queue for it.
    if let Some(len) = key.len() {
        *bytes_read += len;
    }
    if let Some(len) = out.len() {
        *bytes_read += len;
    }
    let ts_cap = cap_set.delayed(&ts);
    output.session(&ts_cap).give((
        message.output,
        Ok(SourceOutput::new(
            key,
            out,
            offset,
            message.upstream_time_millis,
            message.partition,
            message.headers,
            message.specific_diff,
        )),
    ));
    match metric_updates.entry(partition) {
        Entry::Occupied(mut entry) => {
            entry.insert((offset, ts, entry.get().2 + 1));
        }
        Entry::Vacant(entry) => {
            entry.insert((offset, ts, 1));
        }
    }
}
