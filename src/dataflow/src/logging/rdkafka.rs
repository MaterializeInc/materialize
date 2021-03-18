// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated via rdkafka statistics callbacks.

use std::time::Duration;

use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::logging::WorkerIdentifier;

use super::{LogVariant, RDKafkaLog};
use crate::arrangement::KeysValsHandle;
use expr::{GlobalId, SourceInstanceId};
use repr::{Datum, Timestamp};

/// A logged materialized event.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum RDKafkaEvent {
    /// Tracks the source name, id, partition id, and received/ingested offsets
    ConsumerInfo {
        /// Materialize name of the source
        source_name: String,
        /// Materialize source identifier
        source_id: SourceInstanceId,
        /// Kafka name for the consumer
        consumer_name: String,
        /// Number of message sets received from Brokers
        rx: i64,
        /// Number of bytes received from Brokers
        rx_bytes: i64,
        /// Number of message sets sent to Brokers
        tx: i64,
        /// Number of bytes transmitted to Brokers
        tx_bytes: i64,
    },
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
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, RDKafkaEvent)>>,
) -> std::collections::HashMap<LogVariant, (Vec<usize>, KeysValsHandle)> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns / 1_000_000) as Timestamp;

    let traces = worker.dataflow_named("Dataflow: rdkafka logging", move |scope| {
        use differential_dataflow::collection::AsCollection;
        use timely::dataflow::operators::capture::Replay;

        let logs = Some(linked).replay_core(
            scope,
            Some(Duration::from_nanos(config.granularity_ns as u64)),
        );

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let mut demux = OperatorBuilder::new("RDKafka Logging Demux".to_string(), scope.clone());
        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&logs, Pipeline);
        let (mut consumer_info_out, consumer_info) = demux.new_output();

        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            move |_frontiers| {
                let mut consumer_info = consumer_info_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut consumer_info_session = consumer_info.session(&time);

                    for (time, _, datum) in demux_buffer.drain(..) {
                        let time_ns = time.as_nanos() as Timestamp;
                        let time_ms = (time_ns / 1_000_000) as Timestamp;
                        let time_ms = ((time_ms / granularity_ms) + 1) * granularity_ms;
                        let time_ms = time_ms as Timestamp;

                        match datum {
                            RDKafkaEvent::ConsumerInfo {
                                source_name,
                                source_id,
                                consumer_name,
                                rx,
                                rx_bytes,
                                tx,
                                tx_bytes,
                            } => consumer_info_session.give((
                                (
                                    source_name,
                                    source_id,
                                    consumer_name,
                                    rx,
                                    rx_bytes,
                                    tx,
                                    tx_bytes,
                                ),
                                time_ms,
                                1,
                            )),
                        }
                    }
                });
            }
        });

        let consumer_info_current = consumer_info.as_collection().map({
            let mut row_packer = repr::RowPacker::new();
            move |(source_name, source_id, consumer_name, rx, rx_bytes, tx, tx_bytes)| {
                row_packer.pack(&[
                    Datum::String(&source_name),
                    Datum::String(&source_id.to_string()),
                    Datum::String(&consumer_name),
                    Datum::Int64(rx),
                    Datum::Int64(rx_bytes),
                    Datum::Int64(tx),
                    Datum::Int64(tx_bytes),
                ])
            }
        });

        let logs = vec![(
            LogVariant::RDKafkaLog(RDKafkaLog::ConsumerInfo),
            consumer_info_current,
        )];

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
