// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by materialized.

use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::count::CountTotal;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::logging::WorkerIdentifier;
use tracing::error;
use uuid::Uuid;

use mz_expr::{permutation_for_arrangement, MirScalarExpr};
use mz_repr::{Datum, DatumVec, GlobalId, Row, Timestamp};
use mz_timely_util::activator::RcActivator;
use mz_timely_util::replay::MzReplay;

use crate::logging::{LogVariant, MaterializedLog};
use crate::typedefs::{KeysValsHandle, RowSpine};

/// Type alias for logging of materialized events.
pub type Logger = timely::logging_core::Logger<ComputeEvent, WorkerIdentifier>;

/// A logged materialized event.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum ComputeEvent {
    /// Dataflow command, true for create and false for drop.
    Dataflow(GlobalId, bool),
    /// Dataflow depends on a named source of data.
    DataflowDependency {
        /// Globally unique identifier for the dataflow.
        dataflow: GlobalId,
        /// Globally unique identifier for the source on which the dataflow depends.
        source: GlobalId,
    },
    /// Peek command, true for install and false for retire.
    Peek(Peek, bool),
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
    /// The ID of the peek.
    uuid: Uuid,
}

impl Peek {
    /// Create a new peek from its arguments.
    pub fn new(id: GlobalId, time: Timestamp, uuid: Uuid) -> Self {
        Self { id, time, uuid }
    }
}

/// Constructs the logging dataflow for materialized logs.
///
/// Params
/// * `worker`: The Timely worker hosting the log analysis dataflow.
/// * `config`: Logging configuration
/// * `compute`: The source to read compute log events from.
/// * `activator`: A handle to acknowledge activations.
///
/// Returns a map from log variant to a tuple of a trace handle and a permutation to reconstruct
/// the original rows.
pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &mz_compute_client::logging::LoggingConfig,
    compute: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, ComputeEvent)>>,
    activator: RcActivator,
) -> HashMap<LogVariant, (KeysValsHandle, Rc<dyn Any>)> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns / 1_000_000) as Timestamp;

    let traces = worker.dataflow_named("Dataflow: mz logging", move |scope| {
        let (compute_logs, token) = Some(compute).mz_replay(
            scope,
            "materialized logs",
            Duration::from_nanos(config.granularity_ns as u64),
            activator.clone(),
        );

        let mut demux = OperatorBuilder::new(
            "Materialize Compute Logging Demux".to_string(),
            scope.clone(),
        );
        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&compute_logs, Pipeline);
        let (mut dataflow_out, dataflow) = demux.new_output();
        let (mut dependency_out, dependency) = demux.new_output();
        let (mut frontier_out, frontier) = demux.new_output();
        let (mut peek_out, peek) = demux.new_output();
        let (mut peek_duration_out, peek_duration) = demux.new_output();

        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            let mut active_dataflows = std::collections::HashMap::new();
            let mut peek_stash = std::collections::HashMap::new();
            move |_frontiers| {
                let mut dataflow = dataflow_out.activate();
                let mut dependency = dependency_out.activate();
                let mut frontier = frontier_out.activate();
                let mut peek = peek_out.activate();
                let mut peek_duration = peek_duration_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut dataflow_session = dataflow.session(&time);
                    let mut dependency_session = dependency.session(&time);
                    let mut frontier_session = frontier.session(&time);
                    let mut peek_session = peek.session(&time);
                    let mut peek_duration_session = peek_duration.session(&time);

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        let time_ms = (((time.as_millis() as Timestamp / granularity_ms) + 1)
                            * granularity_ms) as Timestamp;

                        match datum {
                            ComputeEvent::Dataflow(id, is_create) => {
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
                            ComputeEvent::DataflowDependency { dataflow, source } => {
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
                            ComputeEvent::Frontier(name, logical, delta) => {
                                frontier_session.give((
                                    Row::pack_slice(&[
                                        Datum::String(&name.to_string()),
                                        Datum::Int64(worker as i64),
                                        Datum::Int64(logical as i64),
                                    ]),
                                    time_ms,
                                    delta,
                                ));
                            }
                            ComputeEvent::Peek(peek, is_install) => {
                                let key = (worker, peek.uuid);
                                if is_install {
                                    peek_session.give(((peek, worker), time_ms, 1));
                                    if peek_stash.contains_key(&key) {
                                        error!(
                                            "peek already registered: \
                                             worker={}, uuid: {}",
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
                                            1,
                                        ));
                                    } else {
                                        error!(
                                            "peek not yet registered: \
                                             worker={}, uuid: {}",
                                            worker, key.1,
                                        );
                                    }
                                }
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

        let peek_current = peek.as_collection().map({
            move |(peek, worker)| {
                Row::pack_slice(&[
                    Datum::Uuid(peek.uuid),
                    Datum::Int64(worker as i64),
                    Datum::String(&peek.id.to_string()),
                    Datum::Int64(peek.time as i64),
                ])
            }
        });

        // Duration statistics derive from the non-rounded event times.
        let peek_duration = peek_duration.as_collection().count_total_core().map({
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
                LogVariant::Materialized(MaterializedLog::PeekCurrent),
                peek_current,
            ),
            (
                LogVariant::Materialized(MaterializedLog::PeekDuration),
                peek_duration,
            ),
        ];

        let mut result = std::collections::HashMap::new();
        for (variant, collection) in logs {
            if config.active_logs.contains_key(&variant) {
                let key = variant.index_by();
                let (_, value) = permutation_for_arrangement::<HashMap<_, _>>(
                    &key.iter()
                        .cloned()
                        .map(MirScalarExpr::Column)
                        .collect::<Vec<_>>(),
                    variant.desc().arity(),
                );
                let trace = collection
                    .map({
                        let mut row_buf = Row::default();
                        let mut datums = DatumVec::new();
                        move |row| {
                            let datums = datums.borrow_with(&row);
                            row_buf.packer().extend(key.iter().map(|k| datums[*k]));
                            let row_key = row_buf.clone();
                            row_buf.packer().extend(value.iter().map(|k| datums[*k]));
                            let row_val = row_buf.clone();
                            (row_key, row_val)
                        }
                    })
                    .arrange_named::<RowSpine<_, _, _, _>>(&format!("ArrangeByKey {:?}", variant))
                    .trace;
                result.insert(variant, (trace, Rc::clone(&token)));
            }
        }
        result
    });

    traces
}
