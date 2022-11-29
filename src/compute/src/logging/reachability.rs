// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by timely dataflow.

use std::any::Any;
use std::convert::TryInto;
use std::rc::Rc;
use std::{collections::HashMap, time::Duration};

use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::AsCollection;
use mz_expr::{permutation_for_arrangement, MirScalarExpr};
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::operators::Filter;
use timely::logging::WorkerIdentifier;

use mz_compute_client::logging::LoggingConfig;
use mz_ore::cast::CastFrom;
use mz_ore::iter::IteratorExt;
use mz_repr::{Datum, Diff, Row, RowArena, Timestamp};
use mz_timely_util::activator::RcActivator;
use mz_timely_util::replay::MzReplay;

use crate::compute_state::ComputeState;
use crate::logging::persist::persist_sink;
use crate::logging::{ConsolidateBuffer, LogVariant, TimelyLog};
use crate::typedefs::{KeysValsHandle, RowSpine};

/// Constructs the logging dataflow for reachability logs.
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
    config: &LoggingConfig,
    compute_state: &mut ComputeState,
    linked: std::rc::Rc<
        EventLink<
            Timestamp,
            (
                Duration,
                WorkerIdentifier,
                (
                    Vec<usize>,
                    Vec<(usize, usize, bool, Option<Timestamp>, Diff)>,
                ),
            ),
        >,
    >,
    activator: RcActivator,
) -> HashMap<LogVariant, (KeysValsHandle, Rc<dyn Any>)> {
    let interval_ms = std::cmp::max(1, config.interval_ns / 1_000_000);

    // A dataflow for multiple log-derived arrangements.
    let traces = worker.dataflow_named("Dataflow: timely reachability logging", move |scope| {
        let (mut logs, token) = Some(linked).mz_replay(
            scope,
            "reachability logs",
            Duration::from_nanos(config.interval_ns as u64),
            activator,
        );

        // If logging is disabled, we still need to install the indexes, but we can leave them
        // empty. We do so by immediately filtering all logs events.
        // TODO(teskje): Remove this once we remove the arranged introspection sources.
        if !config.enable_logging {
            logs = logs.filter(|_| false);
        }

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        // Restrict results by those logs that are meant to be active.
        let logs_active = vec![LogVariant::Timely(TimelyLog::Reachability)];

        // Do we sink the common dataflow?
        let sink_common = config
            .sink_logs
            .contains_key(&LogVariant::Timely(TimelyLog::Reachability));

        // Do we need to construct the common reachability common dataflow?
        let common_needed = logs_active
            .iter()
            .any(|variant| config.index_logs.contains_key(variant))
            || sink_common;

        let mut result = std::collections::HashMap::new();
        if common_needed {
            // Construct common dataflow. Common will feed into both, active and into sinked
            let mut flatten = OperatorBuilder::new(
                "Timely Reachability Logging Flatten ".to_string(),
                scope.clone(),
            );

            use timely::dataflow::channels::pact::Pipeline;
            let mut input = flatten.new_input(&logs, Pipeline);

            let (mut updates_out, updates) = flatten.new_output();

            let mut buffer = Vec::new();
            flatten.build(move |_capability| {
                move |_frontiers| {
                    let updates = updates_out.activate();
                    let mut updates_session = ConsolidateBuffer::new(updates, 0);

                    input.for_each(|cap, data| {
                        data.swap(&mut buffer);

                        for (time, worker, (addr, massaged)) in buffer.drain(..) {
                            let time_ms = (((time.as_millis() / interval_ms) + 1) * interval_ms)
                                .try_into()
                                .expect("must fit");
                            for (source, port, update_type, ts, diff) in massaged {
                                updates_session.give(
                                    &cap,
                                    (
                                        (update_type, addr.clone(), source, port, worker, ts),
                                        time_ms,
                                        diff,
                                    ),
                                );
                            }
                        }
                    });
                }
            });

            let common = updates
                .as_collection()
                .arrange_core::<_, RowSpine<_, _, _, _>>(
                    Exchange::new(|(((_, _, _, _, w, _), ()), _, _)| *w as u64),
                    "PreArrange Timely reachability",
                );

            if sink_common {
                // Use common to create sinked, a timely collection that ouptuts a single value row.
                let (id, meta) = config
                    .sink_logs
                    .get(&LogVariant::Timely(TimelyLog::Reachability))
                    .unwrap();
                tracing::debug!("Persisting reachability to {:?}", meta);

                let mut row_buf = Row::default();
                let sinked = common.as_collection(
                    move |(update_type, addr, source, port, worker, ts), _| {
                        let row_arena = RowArena::default();
                        let update_type = if *update_type { "source" } else { "target" };
                        row_buf.packer().push_list(
                            addr.iter()
                                .chain_one(source)
                                .map(|id| Datum::UInt64(u64::cast_from(*id))),
                        );
                        let datums = &[
                            row_arena.push_unary_row(row_buf.clone()),
                            Datum::UInt64(u64::cast_from(*port)),
                            Datum::UInt64(u64::cast_from(*worker)),
                            Datum::String(update_type),
                            Datum::from(ts.clone()),
                        ];
                        row_buf.packer().extend(datums);
                        row_buf.clone()
                    },
                );
                persist_sink(scope, *id, meta, compute_state, sinked);
            }

            for variant in logs_active {
                if config.index_logs.contains_key(&variant) {
                    let key = variant.index_by();
                    let (_, value) = permutation_for_arrangement::<HashMap<_, _>>(
                        &key.iter()
                            .cloned()
                            .map(MirScalarExpr::Column)
                            .collect::<Vec<_>>(),
                        variant.desc().arity(),
                    );

                    // Use common to create an arrangement.
                    let mut row_buf = Row::default();
                    let updates = common.as_collection(
                        move |(update_type, addr, source, port, worker, ts), _| {
                            let row_arena = RowArena::default();
                            let update_type = if *update_type { "source" } else { "target" };
                            row_buf.packer().push_list(
                                addr.iter()
                                    .chain_one(source)
                                    .map(|id| Datum::UInt64(u64::cast_from(*id))),
                            );
                            let datums = &[
                                row_arena.push_unary_row(row_buf.clone()),
                                Datum::UInt64(u64::cast_from(*port)),
                                Datum::UInt64(u64::cast_from(*worker)),
                                Datum::String(update_type),
                                Datum::from(ts.clone()),
                            ];
                            row_buf.packer().extend(key.iter().map(|k| datums[*k]));
                            let key_row = row_buf.clone();
                            row_buf.packer().extend(value.iter().map(|k| datums[*k]));
                            let value_row = row_buf.clone();
                            (key_row, value_row)
                        },
                    );

                    let trace = updates
                        .arrange_named::<RowSpine<_, _, _, _>>(&format!("Arrange {:?}", variant))
                        .trace;
                    result.insert(variant.clone(), (trace, Rc::clone(&token)));
                }
            }
        }
        result
    });

    traces
}
