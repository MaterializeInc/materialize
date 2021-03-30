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
use repr::{adt::array::ArrayDimension, Datum, Timestamp};

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
    /// A batch of metrics scraped from prometheus.
    PrometheusMetrics {
        /// timestamp in millis from UNIX epoch at which these metrics were scraped.
        timestamp: u64,
        /// duration in millis when this batch of metrics gets invalidated.
        retain_for: u64,
        /// The metrics that were scraped from the registry.
        metrics: Vec<Metric>,
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

/// A prometheus value's meaning.
///
/// This is straightforward for gauges and counters (which have only one meaning), but histograms
/// and summaries can require multiple values to express their meanings correctly.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum MetricValue {
    /// A prometheus counter or gauge's current value
    Value(f64),
    /// A prometheus histogram as a set of values
    Histogram {
        /// The total sum of observed values.
        sum: f64,
        /// The total count of observed events.
        total_count: i64,
        /// The upper bounds of the histogram buckets (cumulative).
        bounds: Vec<f64>,
        /// The count of events in each histogram bucket.
        counts: Vec<i64>,
    },
}

/// The kind of a prometheus metric in a batch of metrics
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub enum MetricType {
    /// A prometheus counter
    Counter,
    /// A prometheus gauge
    Gauge,
    /// A prometheus summary
    Summary,
    /// A prometheus histogram
    Histogram,
    /// An untyped metric
    Untyped,
}

impl MetricType {
    fn as_str(&self) -> &'static str {
        use MetricType::*;
        match self {
            Counter => "counter",
            Gauge => "gauge",
            Summary => "summary",
            Histogram => "histogram",
            Untyped => "untyped",
        }
    }
}

impl From<prometheus::proto::MetricType> for MetricType {
    fn from(f: prometheus::proto::MetricType) -> Self {
        use prometheus::proto::MetricType::*;
        match f {
            COUNTER => MetricType::Counter,
            GAUGE => MetricType::Gauge,
            SUMMARY => MetricType::Summary,
            HISTOGRAM => MetricType::Histogram,

            UNTYPED => MetricType::Untyped,
        }
    }
}

/// A prometheus metric, identified by its name and its associated readings.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct Metric {
    meta: MetricMeta,
    readings: Vec<MetricReading>,
}

impl Metric {
    /// Construct a new prometheus Metric.
    pub fn new(name: String, kind: MetricType, help: String, readings: Vec<MetricReading>) -> Self {
        let meta = MetricMeta { name, kind, help };
        Self { meta, readings }
    }
}

/// Information about the prometheus metric.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct MetricMeta {
    name: String,
    kind: MetricType,
    help: String,
}

impl MetricMeta {
    fn as_packed_row(&self, packer: &mut repr::RowPacker) -> repr::Row {
        packer.pack(&[
            Datum::from(self.name.as_str()),
            Datum::from(self.kind.as_str()),
            Datum::from(self.help.as_str()),
        ])
    }
}

/// A metric reading at a time for a set of labels.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct MetricReading {
    labels: Vec<(String, String)>,
    value: MetricValue,
}

impl MetricReading {
    /// Construct a new metric reading with the given labels and a value.
    pub fn new(labels: Vec<(String, String)>, value: MetricValue) -> Self {
        Self { labels, value }
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
        let (mut kafka_consumer_info_out, kafka_consumer_info) = demux.new_output();
        let (mut metrics_out, metrics) = demux.new_output();
        let (mut metrics_histos_out, metrics_histos) = demux.new_output();
        let (mut metrics_meta_out, metrics_meta) = demux.new_output();
        let (mut peek_out, peek) = demux.new_output();
        let (mut source_info_out, source_info) = demux.new_output();

        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            let mut active_metrics = std::collections::HashMap::<MetricMeta, Timestamp>::new();
            let mut active_dataflows = std::collections::HashMap::new();
            let mut row_packer = repr::RowPacker::new();
            move |_frontiers| {
                let mut dataflow = dataflow_out.activate();
                let mut dependency = dependency_out.activate();
                let mut frontier = frontier_out.activate();
                let mut kafka_consumer_info = kafka_consumer_info_out.activate();
                let mut metrics = metrics_out.activate();
                let mut metrics_histos = metrics_histos_out.activate();
                let mut metrics_meta = metrics_meta_out.activate();
                let mut peek = peek_out.activate();
                let mut source_info = source_info_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut dataflow_session = dataflow.session(&time);
                    let mut dependency_session = dependency.session(&time);
                    let mut frontier_session = frontier.session(&time);
                    let mut kafka_consumer_info_session = kafka_consumer_info.session(&time);
                    let mut metrics_session = metrics.session(&time);
                    let mut metrics_histos_session = metrics_histos.session(&time);
                    let mut metrics_meta_session = metrics_meta.session(&time);
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
                            MaterializedEvent::PrometheusMetrics {
                                metrics,
                                timestamp,
                                retain_for,
                            } => {
                                let chrono_timestamp = chrono::NaiveDateTime::from_timestamp(0, 0)
                                    + chrono::Duration::from_std(Duration::from_millis(timestamp))
                                        .expect("Couldn't convert timestamps");
                                for metric in metrics {
                                    for reading in metric.readings {
                                        let labels = reading.labels.iter().map(|(name, value)| {
                                            (name.as_str(), Datum::from(value.as_str()))
                                        });
                                        let (row, session) = match reading.value {
                                            MetricValue::Value(v) => {
                                                row_packer
                                                    .push(Datum::from(metric.meta.name.as_str()));
                                                row_packer.push(Datum::from(chrono_timestamp));
                                                row_packer.push_dict(labels);
                                                row_packer.push(Datum::from(v));
                                                (
                                                    row_packer.finish_and_reuse(),
                                                    &mut metrics_session,
                                                )
                                            }
                                            MetricValue::Histogram {
                                                sum,
                                                total_count,
                                                bounds,
                                                counts,
                                            } => {
                                                let dims = &[ArrayDimension {
                                                    lower_bound: 0,
                                                    length: bounds.len(),
                                                }];
                                                row_packer
                                                    .push(Datum::from(metric.meta.name.as_str()));
                                                row_packer.push(Datum::from(chrono_timestamp));
                                                row_packer.push_dict(labels);
                                                row_packer.push(Datum::from(sum));
                                                row_packer.push(Datum::from(total_count));
                                                let bounds =
                                                    bounds.into_iter().map(|b| Datum::from(b));
                                                row_packer.push_array(dims, bounds).expect(
                                                    "Mismatch in histogram array dimensions",
                                                );
                                                let counts = counts.into_iter().map(Datum::Int64);
                                                row_packer.push_array(dims, counts).expect(
                                                    "Mismatch in histogram array dimensions",
                                                );
                                                (
                                                    row_packer.finish_and_reuse(),
                                                    &mut metrics_histos_session,
                                                )
                                            }
                                        };
                                        session.give((row.clone(), time_ms, 1));
                                        session.give((row, time_ms + retain_for, -1));
                                    }
                                    // Expire the metadata of a metric reading when the reading
                                    // would expire, but refresh its lifetime when we get another
                                    // reading referencing that metadata:
                                    let meta = metric.meta;
                                    let meta_expiry =
                                        active_metrics.get(&meta).copied().unwrap_or(0);
                                    if meta_expiry <= time_ms {
                                        let row = meta.as_packed_row(&mut row_packer);
                                        metrics_meta_session.give((row.clone(), time_ms, 1));
                                        metrics_meta_session.give((row, time_ms + retain_for, -1));
                                        active_metrics
                                            .insert(meta, time_ms + retain_for as Timestamp);
                                    }
                                }
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

        let metrics_current = metrics.as_collection();
        let metrics_histos_current = metrics_histos.as_collection();
        let metrics_meta_current = metrics_meta.as_collection();

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
                LogVariant::Materialized(MaterializedLog::KafkaConsumerInfo),
                kafka_consumer_info_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::MetricValues),
                metrics_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::MetricHistograms),
                metrics_histos_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::MetricsMeta),
                metrics_meta_current,
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
