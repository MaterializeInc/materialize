// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by materialized.

use std::time::Duration;

use dataflow_types::plan::make_thinning_expression;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::count::CountTotal;
use differential_dataflow::operators::Count;
use log::error;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::logging::WorkerIdentifier;

use super::{LogVariant, MaterializedLog};
use crate::activator::RcActivator;
use crate::arrangement::manager::RowSpine;
use crate::arrangement::KeysValsHandle;
use crate::replay::MzReplay;
use expr::{GlobalId, MirScalarExpr, SourceInstanceId};
use repr::adt::jsonb::Jsonb;
use repr::{Datum, DatumVec, Row, Timestamp};

/// Type alias for logging of materialized events.
pub type Logger = timely::logging_core::Logger<MaterializedEvent, WorkerIdentifier>;

/// A logged materialized event.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum MaterializedEvent {
    /// Dataflow command, true for create and false for drop.
    Dataflow(GlobalId, bool),
    /// Dataflow depends on a named source of data.
    DataflowDependency {
        /// Globally unique identifier for the dataflow.
        dataflow: GlobalId,
        /// Globally unique identifier for the source on which the dataflow depends.
        source: GlobalId,
    },
    /// Underling librdkafka statistics for a Kafka source.
    KafkaSourceStatistics {
        /// Materialize source identifier.
        source_id: SourceInstanceId,
        /// The old JSONB statistics blob to retract, if any.
        old: Option<Jsonb>,
        /// The new JSONB statistics blob to produce, if any.
        new: Option<Jsonb>,
    },
    /// Peek command, true for install and false for retire.
    Peek(Peek, bool),
    /// Tracks the source name, id, partition id, and received/ingested offsets
    SourceInfo {
        /// Name of the source
        source_name: String,
        /// Source identifier
        source_id: SourceInstanceId,
        /// Partition identifier
        partition_id: Option<String>,
        /// Difference between the previous offset and current highest offset we've seen
        offset: i64,
        /// Difference between the previous timestamp and current highest timestamp we've seen
        timestamp: i64,
    },
    /// Available frontier information for views.
    Frontier(GlobalId, Timestamp, i64),
}

/// A logged peek event.
#[derive(
    Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Peek {
    /// The identifier of the view the peek targets.
    id: GlobalId,
    /// The logical timestamp requested.
    time: Timestamp,
    /// The connection ID of the peek.
    conn_id: u32,
}

impl Peek {
    /// Create a new peek from its arguments.
    pub fn new(id: GlobalId, time: Timestamp, conn_id: u32) -> Self {
        Self { id, time, conn_id }
    }
}

/// Constructs the logging dataflow for materialized logs.
///
/// Params
/// * `worker`: The Timely worker hosting the log analysis dataflow.
/// * `config`: Logging configuration
/// * `linked`: The source to read log events from.
/// * `activator`: A handle to acknowledge activations.
///
/// Returns a map from log variant to a tuple of a trace handle and a permutation to reconstruct
/// the original rows.
pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &dataflow_types::logging::LoggingConfig,
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, MaterializedEvent)>>,
    activator: RcActivator,
) -> std::collections::HashMap<LogVariant, KeysValsHandle> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns / 1_000_000) as Timestamp;

    let traces = worker.dataflow_named("Dataflow: mz logging", move |scope| {
        let logs = Some(linked).mz_replay(
            scope,
            "materialized logs",
            Duration::from_nanos(config.granularity_ns as u64),
            activator,
        );

        let mut demux =
            OperatorBuilder::new("Materialize Logging Demux".to_string(), scope.clone());
        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&logs, Pipeline);
        let (mut dataflow_out, dataflow) = demux.new_output();
        let (mut dependency_out, dependency) = demux.new_output();
        let (mut frontier_out, frontier) = demux.new_output();
        let (mut kafka_source_statistics_out, kafka_source_statistics) = demux.new_output();
        let (mut peek_out, peek) = demux.new_output();
        let (mut peek_duration_out, peek_duration) = demux.new_output();
        let (mut source_info_out, source_info) = demux.new_output();

        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            let mut active_dataflows = std::collections::HashMap::new();
            let mut peek_stash = std::collections::HashMap::new();
            move |_frontiers| {
                let mut dataflow = dataflow_out.activate();
                let mut dependency = dependency_out.activate();
                let mut frontier = frontier_out.activate();
                let mut kafka_source_statistics = kafka_source_statistics_out.activate();
                let mut peek = peek_out.activate();
                let mut peek_duration = peek_duration_out.activate();
                let mut source_info = source_info_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut dataflow_session = dataflow.session(&time);
                    let mut dependency_session = dependency.session(&time);
                    let mut frontier_session = frontier.session(&time);
                    let mut kafka_source_statistics_session =
                        kafka_source_statistics.session(&time);
                    let mut peek_session = peek.session(&time);
                    let mut peek_duration_session = peek_duration.session(&time);
                    let mut source_info_session = source_info.session(&time);

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        let time_ms = (((time.as_millis() as Timestamp / granularity_ms) + 1)
                            * granularity_ms) as Timestamp;

                        match datum {
                            MaterializedEvent::Dataflow(id, is_create) => {
                                let diff = if is_create { 1 } else { -1 };
                                dataflow_session.give(((id, worker), time_ms, diff));

                                // For now we know that these always happen in
                                // the correct order, but it may be necessary
                                // down the line to have dataflows keep a
                                // reference to their own sources and a logger
                                // that is called on them in a `with_drop` handler
                                if is_create {
                                    active_dataflows.insert((id, worker), vec![]);
                                } else {
                                    let key = &(id, worker);
                                    match active_dataflows.remove(key) {
                                        Some(sources) => {
                                            for (source, worker) in sources {
                                                let n = key.0;
                                                dependency_session.give((
                                                    (n, source, worker),
                                                    time_ms,
                                                    -1,
                                                ));
                                            }
                                        }
                                        None => error!(
                                            "no active dataflow exists at time of drop. \
                                             name={} worker={}",
                                            key.0, worker
                                        ),
                                    }
                                }
                            }
                            MaterializedEvent::DataflowDependency { dataflow, source } => {
                                dependency_session.give(((dataflow, source, worker), time_ms, 1));
                                let key = (dataflow, worker);
                                match active_dataflows.get_mut(&key) {
                                    Some(existing_sources) => {
                                        existing_sources.push((source, worker))
                                    }
                                    None => error!(
                                        "tried to create source for dataflow that doesn't exist: \
                                         dataflow={} source={} worker={}",
                                        key.0, source, worker,
                                    ),
                                }
                            }
                            MaterializedEvent::Frontier(name, logical, delta) => {
                                frontier_session.give((
                                    Row::pack_slice(&[
                                        Datum::String(&name.to_string()),
                                        Datum::Int64(worker as i64),
                                        Datum::Int64(logical as i64),
                                    ]),
                                    time_ms,
                                    delta as isize,
                                ));
                            }
                            MaterializedEvent::KafkaSourceStatistics {
                                source_id,
                                old,
                                new,
                            } => {
                                if let Some(old) = old {
                                    kafka_source_statistics_session.give((
                                        (source_id, worker, old),
                                        time_ms,
                                        -1,
                                    ));
                                }
                                if let Some(new) = new {
                                    kafka_source_statistics_session.give((
                                        (source_id, worker, new),
                                        time_ms,
                                        1,
                                    ));
                                }
                            }
                            MaterializedEvent::Peek(peek, is_install) => {
                                let key = (worker, peek.conn_id);
                                if is_install {
                                    peek_session.give(((peek, worker), time_ms, 1));
                                    if peek_stash.contains_key(&key) {
                                        error!(
                                            "peek already registered: \
                                             worker={}, connection_id: {}",
                                            worker, key.1,
                                        );
                                    }
                                    peek_stash.insert(key, time.as_nanos());
                                } else {
                                    peek_session.give(((peek, worker), time_ms, -1));
                                    if let Some(start) = peek_stash.remove(&key) {
                                        let elapsed_ns = time.as_nanos() - start;
                                        peek_duration_session.give((
                                            (key.0, elapsed_ns.next_power_of_two()),
                                            time_ms,
                                            1isize,
                                        ));
                                    } else {
                                        error!(
                                            "peek not yet registered: \
                                             worker={}, connection_id: {}",
                                            worker, key.1,
                                        );
                                    }
                                }
                            }
                            MaterializedEvent::SourceInfo {
                                source_name,
                                source_id,
                                partition_id,
                                offset,
                                timestamp,
                            } => {
                                source_info_session.give((
                                    (source_name, source_id, partition_id),
                                    time_ms,
                                    (offset, timestamp),
                                ));
                            }
                        }
                    }
                });
            }
        });

        let dataflow_current = dataflow.as_collection().map({
            move |(name, worker)| {
                Row::pack_slice(&[
                    Datum::String(&name.to_string()),
                    Datum::Int64(worker as i64),
                ])
            }
        });

        let dependency_current = dependency.as_collection().map({
            move |(dataflow, source, worker)| {
                Row::pack_slice(&[
                    Datum::String(&dataflow.to_string()),
                    Datum::String(&source.to_string()),
                    Datum::Int64(worker as i64),
                ])
            }
        });

        let frontier_current = frontier.as_collection();

        let kafka_source_statistics_current = kafka_source_statistics.as_collection().map({
            move |(source_id, worker, stats)| {
                let mut row = Row::pack_slice(&[
                    Datum::String(&source_id.to_string()),
                    Datum::Int64(worker as i64),
                ]);
                row.extend_by_row(&stats.into_row());
                row
            }
        });

        let peek_current = peek.as_collection().map({
            move |(peek, worker)| {
                Row::pack_slice(&[
                    Datum::String(&format!("{}", peek.conn_id)),
                    Datum::Int64(worker as i64),
                    Datum::String(&peek.id.to_string()),
                    Datum::Int64(peek.time as i64),
                ])
            }
        });

        let source_info_current = source_info.as_collection().count().map({
            move |((name, id, pid), (offset, timestamp))| {
                Row::pack_slice(&[
                    Datum::String(&name),
                    Datum::String(&id.source_id.to_string()),
                    Datum::Int64(id.dataflow_id as i64),
                    Datum::from(pid.as_deref()),
                    Datum::Int64(offset),
                    Datum::Int64(timestamp),
                ])
            }
        });

        // Duration statistics derive from the non-rounded event times.
        let peek_duration = peek_duration.as_collection().count_total().map({
            move |((worker, pow), count)| {
                Row::pack_slice(&[
                    Datum::Int64(worker as i64),
                    Datum::Int64(pow as i64),
                    Datum::Int64(count as i64),
                ])
            }
        });

        let logs = vec![
            (
                LogVariant::Materialized(MaterializedLog::DataflowCurrent),
                dataflow_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::DataflowDependency),
                dependency_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::FrontierCurrent),
                frontier_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::KafkaSourceStatistics),
                kafka_source_statistics_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::PeekCurrent),
                peek_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::PeekDuration),
                peek_duration,
            ),
            (
                LogVariant::Materialized(MaterializedLog::SourceInfo),
                source_info_current,
            ),
        ];

        let mut result = std::collections::HashMap::new();
        for (variant, collection) in logs {
            if config.active_logs.contains_key(&variant) {
                let key = variant.index_by();
                let value = make_thinning_expression(
                    &key.iter()
                        .cloned()
                        .map(MirScalarExpr::Column)
                        .collect::<Vec<_>>(),
                    variant.desc().arity(),
                );
                let trace = collection
                    .map({
                        let mut row_packer = Row::default();
                        let mut datums = DatumVec::new();
                        move |row| {
                            let datums = datums.borrow_with(&row);
                            row_packer.extend(key.iter().map(|k| datums[*k]));
                            let row_key = row_packer.finish_and_reuse();
                            row_packer.extend(value.iter().map(|k| datums[*k]));
                            (row_key, row_packer.finish_and_reuse())
                        }
                    })
                    .arrange_named::<RowSpine<_, _, _, _>>(&format!("ArrangeByKey {:?}", variant))
                    .trace;
                result.insert(variant, trace);
            }
        }
        result
    });

    traces
}
