// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by materialized.

use std::time::Duration;

use differential_dataflow::operators::count::CountTotal;
use log::error;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::operators::generic::operator::Operator;
use timely::logging::WorkerIdentifier;

use super::{LogVariant, MaterializedLog};
use crate::arrangement::KeysValsHandle;
use expr::{GlobalId, SourceInstanceId};
use repr::{Datum, Timestamp};

/// Type alias for logging of materialized events.
pub type Logger = timely::logging_core::Logger<MaterializedEvent, WorkerIdentifier>;

/// A logged materialized event.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
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
    /// Tracks RTT statistics for a Kafka broker, by consumer
    /// Reference: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
    /// This structure containe splatted metrics from the rdkafka::statistics::Window struct
    /// Window cannot be used as it does not satisfy several of the traits required
    KafkaBrokerRTT {
        /// Kafka name for the consumer
        consumer_name: String,
        /// Materialize source identifier
        source_id: SourceInstanceId,
        /// The Kafka broker for these metrics (may be multiple per consumer)
        broker_name: String,
        /// Smallest value
        min: i64,
        /// Largest value
        max: i64,
        /// Average value
        avg: i64,
        /// Sum of all values
        sum: i64,
        /// Number of values samples
        cnt: i64,
        /// Standard deviation of values (based on histogram)
        stddev: i64,
        /// 50th percentile value
        p50: i64,
        /// 75th percentile value
        p75: i64,
        /// 90th percentile value
        p90: i64,
        /// 959h percentile value
        p95: i64,
        /// 99th percentile value
        p99: i64,
        /// 99.99th percentile value
        p99_99: i64,
    },
    /// Tracks statistics for a particular Kafka consumer / partition pair
    /// Reference: https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
    KafkaConsumerInfo {
        /// Kafka name for the consumer
        consumer_name: String,
        /// Materialize source identifier
        source_id: SourceInstanceId,
        /// The Kafka partition ID for these metrics (may be multiple per consumer)
        partition_id: String,
        /// Number of message sets received from Brokers
        rxmsgs: i64,
        /// Number of bytes received from Brokers
        rxbytes: i64,
        /// Number of message sets sent to Brokers
        txmsgs: i64,
        /// Number of bytes transmitted to Brokers
        txbytes: i64,
        /// Partition's low watermark offset on the broker
        lo_offset: i64,
        /// Partition's high watermark offset on the broker
        hi_offset: i64,
        /// Last stable offset on the broker
        ls_offset: i64,
        /// How far into the topic our consumer has read
        app_offset: i64,
        /// How many messages remain until our consumer reaches the (hi|lo) watermark
        consumer_lag: i64,
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
        partition_id: String,
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

/// Constructs the logging dataflows and returns a logger and trace handles.
pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &dataflow_types::logging::LoggingConfig,
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, MaterializedEvent)>>,
) -> std::collections::HashMap<LogVariant, (Vec<usize>, KeysValsHandle)> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns / 1_000_000) as Timestamp;

    let traces = worker.dataflow_named("Dataflow: mz logging", move |scope| {
        use differential_dataflow::collection::AsCollection;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        let logs = Some(linked).replay_core(
            scope,
            Some(Duration::from_nanos(config.granularity_ns as u64)),
        );

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let mut demux =
            OperatorBuilder::new("Materialize Logging Demux".to_string(), scope.clone());
        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&logs, Pipeline);
        let (mut dataflow_out, dataflow) = demux.new_output();
        let (mut dependency_out, dependency) = demux.new_output();
        let (mut frontier_out, frontier) = demux.new_output();
        let (mut kafka_broker_rtt_out, kafka_broker_rtt) = demux.new_output();
        let (mut kafka_consumer_info_out, kafka_consumer_info) = demux.new_output();
        let (mut peek_out, peek) = demux.new_output();
        let (mut source_info_out, source_info) = demux.new_output();

        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            let mut active_dataflows = std::collections::HashMap::new();
            let mut row_packer = repr::RowPacker::new();
            move |_frontiers| {
                let mut dataflow = dataflow_out.activate();
                let mut dependency = dependency_out.activate();
                let mut frontier = frontier_out.activate();
                let mut kafka_broker_rtt = kafka_broker_rtt_out.activate();
                let mut kafka_consumer_info = kafka_consumer_info_out.activate();
                let mut peek = peek_out.activate();
                let mut source_info = source_info_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut dataflow_session = dataflow.session(&time);
                    let mut dependency_session = dependency.session(&time);
                    let mut frontier_session = frontier.session(&time);
                    let mut kafka_broker_rtt_session = kafka_broker_rtt.session(&time);
                    let mut kafka_consumer_info_session = kafka_consumer_info.session(&time);
                    let mut peek_session = peek.session(&time);
                    let mut source_info_session = source_info.session(&time);

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        let time_ns = time.as_nanos() as Timestamp;
                        let time_ms = (time_ns / 1_000_000) as Timestamp;
                        let time_ms = ((time_ms / granularity_ms) + 1) * granularity_ms;
                        let time_ms = time_ms as Timestamp;

                        match datum {
                            MaterializedEvent::Dataflow(id, is_create) => {
                                dataflow_session.give((id, worker, is_create, time_ns));

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
                                                dependency_session
                                                    .give((n, source, worker, false, time_ns));
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
                                dependency_session.give((dataflow, source, worker, true, time_ns));
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
                                    row_packer.pack(&[
                                        Datum::String(&name.to_string()),
                                        Datum::Int64(worker as i64),
                                        Datum::Int64(logical as i64),
                                    ]),
                                    time_ms,
                                    delta as isize,
                                ));
                            }
                            MaterializedEvent::KafkaBrokerRTT {
                                consumer_name,
                                source_id,
                                broker_name,
                                min,
                                max,
                                avg,
                                sum,
                                cnt,
                                stddev,
                                p50,
                                p75,
                                p90,
                                p95,
                                p99,
                                p99_99,
                            } => {
                                kafka_broker_rtt_session.give((
                                    (consumer_name, source_id, broker_name),
                                    time_ms,
                                    vec![
                                        min, max, avg, sum, cnt, stddev, p50, p75, p90, p95, p99,
                                        p99_99,
                                    ],
                                ));
                            }
                            MaterializedEvent::KafkaConsumerInfo {
                                consumer_name,
                                source_id,
                                partition_id,
                                rxmsgs,
                                rxbytes,
                                txmsgs,
                                txbytes,
                                lo_offset,
                                hi_offset,
                                ls_offset,
                                app_offset,
                                consumer_lag,
                            } => {
                                kafka_consumer_info_session.give((
                                    (consumer_name, source_id, partition_id),
                                    time_ms,
                                    vec![
                                        rxmsgs,
                                        rxbytes,
                                        txmsgs,
                                        txbytes,
                                        lo_offset,
                                        hi_offset,
                                        ls_offset,
                                        app_offset,
                                        consumer_lag,
                                    ],
                                ));
                            }
                            MaterializedEvent::Peek(peek, is_install) => {
                                peek_session.give((peek, worker, is_install, time_ns))
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

        let dataflow_current = dataflow
            .map(move |(name, worker, is_create, time_ns)| {
                let time_ms = (time_ns / 1_000_000) as Timestamp;
                let time_ms = ((time_ms / granularity_ms) + 1) * granularity_ms;
                ((name, worker), time_ms, if is_create { 1 } else { -1 })
            })
            .as_collection()
            .map({
                let mut row_packer = repr::RowPacker::new();
                move |(name, worker)| {
                    row_packer.pack(&[
                        Datum::String(&name.to_string()),
                        Datum::Int64(worker as i64),
                    ])
                }
            });

        let dependency_current = dependency
            .map(move |(dataflow, source, worker, is_create, time_ns)| {
                let time_ms = (time_ns / 1_000_000) as Timestamp;
                let time_ms = ((time_ms / granularity_ms) + 1) * granularity_ms;
                let diff = if is_create { 1 } else { -1 };
                ((dataflow, source, worker), time_ms, diff)
            })
            .as_collection()
            .map({
                let mut row_packer = repr::RowPacker::new();
                move |(dataflow, source, worker)| {
                    row_packer.pack(&[
                        Datum::String(&dataflow.to_string()),
                        Datum::String(&source.to_string()),
                        Datum::Int64(worker as i64),
                    ])
                }
            });

        let frontier_current = frontier.as_collection();

        use differential_dataflow::operators::Count;
        let kafka_broker_rtt_current = kafka_broker_rtt.as_collection().count().map({
            let mut row_packer = repr::RowPacker::new();
            move |((consumer_name, source_id, broker_name), diff_vector)| {
                row_packer.pack(&[
                    Datum::String(&consumer_name),
                    Datum::String(&source_id.source_id.to_string()),
                    Datum::Int64(source_id.dataflow_id as i64),
                    Datum::String(&broker_name),
                    Datum::Int64(diff_vector[0]),
                    Datum::Int64(diff_vector[1]),
                    Datum::Int64(diff_vector[2]),
                    Datum::Int64(diff_vector[3]),
                    Datum::Int64(diff_vector[4]),
                    Datum::Int64(diff_vector[5]),
                    Datum::Int64(diff_vector[6]),
                    Datum::Int64(diff_vector[7]),
                    Datum::Int64(diff_vector[8]),
                    Datum::Int64(diff_vector[9]),
                    Datum::Int64(diff_vector[10]),
                    Datum::Int64(diff_vector[11]),
                ])
            }
        });

        let kafka_consumer_info_current = kafka_consumer_info.as_collection().count().map({
            let mut row_packer = repr::RowPacker::new();
            move |((consumer_name, source_id, partition_id), diff_vector)| {
                row_packer.pack(&[
                    Datum::String(&consumer_name),
                    Datum::String(&source_id.source_id.to_string()),
                    Datum::Int64(source_id.dataflow_id as i64),
                    Datum::String(&partition_id),
                    Datum::Int64(diff_vector[0]),
                    Datum::Int64(diff_vector[1]),
                    Datum::Int64(diff_vector[2]),
                    Datum::Int64(diff_vector[3]),
                    Datum::Int64(diff_vector[4]),
                    Datum::Int64(diff_vector[5]),
                    Datum::Int64(diff_vector[6]),
                    Datum::Int64(diff_vector[7]),
                    Datum::Int64(diff_vector[8]),
                ])
            }
        });

        let peek_current = peek
            .map(move |(name, worker, is_install, time_ns)| {
                let time_ms = (time_ns / 1_000_000) as Timestamp;
                let time_ms = ((time_ms / granularity_ms) + 1) * granularity_ms;
                let time_ms = time_ms as Timestamp;
                ((name, worker), time_ms, if is_install { 1 } else { -1 })
            })
            .as_collection()
            .map({
                let mut row_packer = repr::RowPacker::new();
                move |(peek, worker)| {
                    row_packer.pack(&[
                        Datum::String(&format!("{}", peek.conn_id)),
                        Datum::Int64(worker as i64),
                        Datum::String(&peek.id.to_string()),
                        Datum::Int64(peek.time as i64),
                    ])
                }
            });

        let source_info_current = source_info.as_collection().count().map({
            let mut row_packer = repr::RowPacker::new();
            move |((name, id, pid), (offset, timestamp))| {
                row_packer.pack(&[
                    Datum::String(&name),
                    Datum::String(&id.source_id.to_string()),
                    Datum::Int64(id.dataflow_id as i64),
                    Datum::String(&pid),
                    Datum::Int64(offset),
                    Datum::Int64(timestamp),
                ])
            }
        });

        // Duration statistics derive from the non-rounded event times.
        let peek_duration = peek
            .unary(
                timely::dataflow::channels::pact::Pipeline,
                "Peeks",
                |_, _| {
                    let mut map = std::collections::HashMap::new();
                    let mut vec = Vec::new();

                    move |input, output| {
                        input.for_each(|time, data| {
                            data.swap(&mut vec);
                            let mut session = output.session(&time);
                            // Collapsing the contained `if` makes its structure
                            // less clear.
                            #[allow(clippy::collapsible_if)]
                            for (peek, worker, is_install, time_ns) in vec.drain(..) {
                                let key = (worker, peek.conn_id);
                                if is_install {
                                    if map.contains_key(&key) {
                                        error!(
                                            "peek already registered: \
                                             worker={}, connection_id: {}",
                                            worker, peek.conn_id,
                                        );
                                    }
                                    map.insert(key, time_ns);
                                } else {
                                    if let Some(start) = map.remove(&key) {
                                        let elapsed_ns = time_ns - start;
                                        let time_ms = (time_ns / 1_000_000) as Timestamp;
                                        let time_ms =
                                            ((time_ms / granularity_ms) + 1) * granularity_ms;
                                        session.give((
                                            (key.0, elapsed_ns.next_power_of_two()),
                                            time_ms,
                                            1isize,
                                        ));
                                    } else {
                                        error!(
                                            "peek not yet registered: \
                                             worker={}, connection_id: {}",
                                            worker, peek.conn_id,
                                        );
                                    }
                                }
                            }
                        });
                    }
                },
            )
            .as_collection()
            .count_total()
            .map({
                let mut row_packer = repr::RowPacker::new();
                move |((worker, pow), count)| {
                    row_packer.pack(&[
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
                LogVariant::Materialized(MaterializedLog::KafkaBrokerRTT),
                kafka_broker_rtt_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::KafkaConsumerInfo),
                kafka_consumer_info_current,
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

        use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
        let mut result = std::collections::HashMap::new();
        for (variant, collection) in logs {
            if config.active_logs.contains_key(&variant) {
                let key = variant.index_by();
                let key_clone = key.clone();
                let trace = collection
                    .map({
                        let mut row_packer = repr::RowPacker::new();
                        move |row| {
                            let datums = row.unpack();
                            let key_row = row_packer.pack(key.iter().map(|k| datums[*k]));
                            (key_row, row)
                        }
                    })
                    .arrange_by_key()
                    .trace;
                result.insert(variant, (key_clone, trace));
            }
        }
        result
    });

    traces
}
