// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use log::error;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::operators::generic::operator::Operator;
use timely::logging::WorkerIdentifier;

use super::{LogVariant, MaterializedLog};
use crate::arrangement::KeysValsHandle;
use dataflow_types::Timestamp;
use expr::GlobalId;
use repr::{Datum, Row};

/// Type alias for logging of materialized events.
pub type Logger = timely::logging_core::Logger<MaterializedEvent, WorkerIdentifier>;

/// A logged materialized event.
#[derive(
    Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum MaterializedEvent {
    /// Avro OCF sink.
    AvroOcfSink {
        id: GlobalId,
        path: String,
        insert: bool,
    },
    /// Map from global identifiers to string name.
    Catalog(GlobalId, String, bool),
    /// Dataflow command, true for create and false for drop.
    Dataflow(GlobalId, bool),
    /// Dataflow depends on a named source of data.
    DataflowDependency {
        dataflow: GlobalId,
        source: GlobalId,
    },
    /// Kafka sink.
    KafkaSink {
        id: GlobalId,
        topic: String,
        insert: bool,
    },
    /// Peek command, true for install and false for retire.
    Peek(Peek, bool),
    /// Available frontier information for views.
    Frontier(GlobalId, Timestamp, i64),
    /// Primary key.
    PrimaryKey(GlobalId, Vec<usize>, usize),
    /// Foreign key relationship: child, parent, then pairs of child and parent columns.
    /// The final integer is used to correlate relationships, as there could be several
    /// foreign key relationships from one child relation to the same parent relation.
    ForeignKey(GlobalId, GlobalId, Vec<(usize, usize)>, usize),
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
    pub fn new(id: GlobalId, time: Timestamp, conn_id: u32) -> Self {
        Self { id, time, conn_id }
    }
}

pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &dataflow_types::logging::LoggingConfig,
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, MaterializedEvent)>>,
) -> std::collections::HashMap<LogVariant, (Vec<usize>, KeysValsHandle)> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns() / 1_000_000) as Timestamp;

    let traces = worker.dataflow(move |scope| {
        use differential_dataflow::collection::AsCollection;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        // TODO: Rewrite as one operator with multiple outputs.
        let logs = Some(linked).replay_core(
            scope,
            Some(Duration::from_nanos(config.granularity_ns() as u64)),
        );

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let mut demux =
            OperatorBuilder::new("Materialize Logging Demux".to_string(), scope.clone());
        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&logs, Pipeline);
        let (mut dataflow_out, dataflow) = demux.new_output();
        let (mut dependency_out, dependency) = demux.new_output();
        let (mut peek_out, peek) = demux.new_output();
        let (mut frontier_out, frontier) = demux.new_output();
        let (mut primary_out, primary) = demux.new_output();
        let (mut foreign_out, foreign) = demux.new_output();
        let (mut catalog_out, catalog) = demux.new_output();
        let (mut kafka_sinks_out, kafka_sinks) = demux.new_output();
        let (mut avro_ocf_sinks_out, avro_ocf_sinks) = demux.new_output();

        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            let mut active_dataflows = std::collections::HashMap::new();
            // Map from string name to pair of primary, and foreign key relationships.
            let mut view_keys =
                std::collections::HashMap::<GlobalId, (Vec<_>, Vec<(GlobalId, _, _)>)>::new();

            move |_frontiers| {
                let mut dataflow = dataflow_out.activate();
                let mut dependency = dependency_out.activate();
                let mut peek = peek_out.activate();
                let mut frontier = frontier_out.activate();
                let mut primary = primary_out.activate();
                let mut foreign = foreign_out.activate();
                let mut catalog = catalog_out.activate();
                let mut kafka_sinks = kafka_sinks_out.activate();
                let mut avro_ocf_sinks = avro_ocf_sinks_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut dataflow_session = dataflow.session(&time);
                    let mut dependency_session = dependency.session(&time);
                    let mut peek_session = peek.session(&time);
                    let mut frontier_session = frontier.session(&time);
                    let mut primary_session = primary.session(&time);
                    let mut foreign_session = foreign.session(&time);
                    let mut catalog_session = catalog.session(&time);
                    let mut kafka_sinks_session = kafka_sinks.session(&time);
                    let mut avro_ocf_sinks_session = avro_ocf_sinks.session(&time);

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        let time_ns = time.as_nanos() as Timestamp;
                        let time_ms = (time_ns / 1_000_000) as Timestamp;
                        let time_ms = ((time_ms / granularity_ms) + 1) * granularity_ms;
                        let time_ms = time_ms as Timestamp;

                        match datum {
                            MaterializedEvent::AvroOcfSink { id, path, insert } => {
                                avro_ocf_sinks_session.give((
                                    Row::pack(&[
                                        Datum::String(&id.to_string()),
                                        Datum::String(&path),
                                    ]),
                                    time_ms,
                                    if insert { 1 } else { -1 },
                                ))
                            }
                            MaterializedEvent::Catalog(id, name, insert) => {
                                catalog_session.give((
                                    (id, name),
                                    time_ms,
                                    if insert { 1 } else { -1 },
                                ));
                            }
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

                                    // Currently we respond to worker dataflow drops, of which
                                    // there can be many. Don't panic if we can't find the name.
                                    if let Some((primary, foreign)) = view_keys.remove(&id) {
                                        for (key, index) in primary.into_iter() {
                                            for k in key {
                                                primary_session.give((
                                                    Row::pack(&[
                                                        Datum::String(&id.to_string()),
                                                        Datum::Int64(k as i64),
                                                        Datum::Int64(index as i64),
                                                    ]),
                                                    time_ms,
                                                    -1,
                                                ));
                                            }
                                        }
                                        for (parent, key, number) in foreign.into_iter() {
                                            for (c, p) in key {
                                                foreign_session.give((
                                                    Row::pack(&[
                                                        Datum::String(&id.to_string()),
                                                        Datum::Int64(c as i64),
                                                        Datum::String(&parent.to_string()),
                                                        Datum::Int64(p as i64),
                                                        Datum::Int64(number as i64),
                                                    ]),
                                                    time_ms,
                                                    -1,
                                                ));
                                            }
                                        }
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
                            MaterializedEvent::KafkaSink { id, topic, insert } => {
                                kafka_sinks_session.give((
                                    Row::pack(&[
                                        Datum::String(&id.to_string()),
                                        Datum::String(&topic),
                                    ]),
                                    time_ms,
                                    if insert { 1 } else { -1 },
                                ))
                            }
                            MaterializedEvent::Peek(peek, is_install) => {
                                peek_session.give((peek, worker, is_install, time_ns))
                            }
                            MaterializedEvent::Frontier(name, logical, delta) => {
                                frontier_session.give((
                                    Row::pack(&[
                                        Datum::String(&name.to_string()),
                                        Datum::Int64(logical as i64),
                                    ]),
                                    time_ms,
                                    delta as isize,
                                ));
                            }
                            MaterializedEvent::PrimaryKey(dataflow_id, key, index) => {
                                for k in key.iter() {
                                    primary_session.give((
                                        Row::pack(&[
                                            Datum::String(&dataflow_id.to_string()),
                                            Datum::Int64(*k as i64),
                                            Datum::Int64(index as i64),
                                        ]),
                                        time_ms,
                                        1,
                                    ));
                                }
                                view_keys
                                    .entry(dataflow_id)
                                    .or_insert((Vec::new(), Vec::new()))
                                    .0
                                    .push((key, index));
                            }
                            MaterializedEvent::ForeignKey(child_id, parent_id, keys, number) => {
                                for (c, p) in keys.iter() {
                                    foreign_session.give((
                                        Row::pack(&[
                                            Datum::String(&child_id.to_string()),
                                            Datum::Int64(*c as i64),
                                            Datum::String(&parent_id.to_string()),
                                            Datum::Int64(*p as i64),
                                            Datum::Int64(number as i64),
                                        ]),
                                        time_ms,
                                        1,
                                    ));
                                }
                                view_keys
                                    .entry(child_id)
                                    .or_insert((Vec::new(), Vec::new()))
                                    .1
                                    .push((parent_id, keys, number));
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
                move |(name, worker)| {
                    Row::pack(&[
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
                move |(dataflow, source, worker)| {
                    Row::pack(&[
                        Datum::String(&dataflow.to_string()),
                        Datum::String(&source.to_string()),
                        Datum::Int64(worker as i64),
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
                move |(peek, worker)| {
                    Row::pack(&[
                        Datum::String(&format!("{}", peek.conn_id)),
                        Datum::Int64(worker as i64),
                        Datum::String(&peek.id.to_string()),
                        Datum::Int64(peek.time as i64),
                    ])
                }
            });

        let frontier_current = frontier.as_collection();
        let primary_key = primary.as_collection();
        let foreign_key = foreign.as_collection();
        let catalog = catalog.as_collection().map({
            move |(id, name)| Row::pack(&[Datum::String(&format!("{}", id)), Datum::String(&name)])
        });
        let kafka_sinks = kafka_sinks.as_collection();
        let avro_ocf_sinks = avro_ocf_sinks.as_collection();

        // Duration statistics derive from the non-rounded event times.
        use differential_dataflow::operators::reduce::Count;
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
            .count()
            .map({
                move |((worker, pow), count)| {
                    Row::pack(&[
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
                LogVariant::Materialized(MaterializedLog::PeekCurrent),
                peek_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::PeekDuration),
                peek_duration,
            ),
            (
                LogVariant::Materialized(MaterializedLog::PrimaryKeys),
                primary_key,
            ),
            (
                LogVariant::Materialized(MaterializedLog::ForeignKeys),
                foreign_key,
            ),
            (LogVariant::Materialized(MaterializedLog::Catalog), catalog),
            (
                LogVariant::Materialized(MaterializedLog::KafkaSinks),
                kafka_sinks,
            ),
            (
                LogVariant::Materialized(MaterializedLog::AvroOcfSinks),
                avro_ocf_sinks,
            ),
        ];

        use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
        let mut result = std::collections::HashMap::new();
        for (variant, collection) in logs {
            if config.active_logs().contains(&variant) {
                let key = variant.index_by();
                let key_clone = key.clone();
                let trace = collection
                    .map({
                        move |row| {
                            let datums = row.unpack();
                            let key_row = Row::pack(key.iter().map(|k| datums[*k]));
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
