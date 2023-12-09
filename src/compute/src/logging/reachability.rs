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
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::rc::Rc;

use differential_dataflow::AsCollection;
use mz_compute_client::logging::LoggingConfig;
use mz_expr::{permutation_for_arrangement, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_ore::iter::IteratorExt;
use mz_repr::{Datum, Diff, RowArena, SharedRow, Timestamp};
use mz_timely_util::buffer::ConsolidateBuffer;
use mz_timely_util::replay::MzReplay;
use timely::communication::Allocate;
use timely::dataflow::operators::Filter;

use crate::extensions::arrange::MzArrange;
use crate::logging::{EventQueue, LogVariant, TimelyLog};
use crate::typedefs::{KeyValSpine, RowRowAgent, RowRowSpine};

pub(super) type ReachabilityEvent = (
    Vec<usize>,
    Vec<(usize, usize, bool, Option<Timestamp>, Diff)>,
);

/// Constructs the logging dataflow for reachability logs.
///
/// Params
/// * `worker`: The Timely worker hosting the log analysis dataflow.
/// * `config`: Logging configuration
/// * `event_queue`: The source to read log events from.
///
/// Returns a map from log variant to a tuple of a trace handle and a permutation to reconstruct
/// the original rows.
pub(super) fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
    event_queue: EventQueue<ReachabilityEvent>,
) -> BTreeMap<LogVariant, (RowRowAgent<Timestamp, Diff>, Rc<dyn Any>)> {
    let interval_ms = std::cmp::max(1, config.interval.as_millis());
    let worker_index = worker.index();

    // A dataflow for multiple log-derived arrangements.
    let traces = worker.dataflow_named("Dataflow: timely reachability logging", move |scope| {
        let (mut logs, token) = Some(event_queue.link).mz_replay(
            scope,
            "reachability logs",
            config.interval,
            event_queue.activator,
        );

        // If logging is disabled, we still need to install the indexes, but we can leave them
        // empty. We do so by immediately filtering all logs events.
        if !config.enable_logging {
            logs = logs.filter(|_| false);
        }

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        // Restrict results by those logs that are meant to be active.
        let logs_active = vec![LogVariant::Timely(TimelyLog::Reachability)];

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
                let mut updates = updates_out.activate();
                let mut updates_session = ConsolidateBuffer::new(&mut updates, 0);

                input.for_each(|cap, data| {
                    data.swap(&mut buffer);

                    for (time, _worker, (addr, massaged)) in buffer.drain(..) {
                        let time_ms = (((time.as_millis() / interval_ms) + 1) * interval_ms)
                            .try_into()
                            .expect("must fit");
                        for (source, port, update_type, ts, diff) in massaged {
                            let datum = (update_type, addr.clone(), source, port, ts);
                            updates_session.give(&cap, ((datum, ()), time_ms, diff));
                        }
                    }
                });
            }
        });

        let updates = updates
            .as_collection()
            .mz_arrange_core::<_, KeyValSpine<_, _, _, _>>(
                Pipeline,
                "PreArrange Timely reachability",
            );

        let mut result = BTreeMap::new();
        for variant in logs_active {
            if config.index_logs.contains_key(&variant) {
                let key = variant.index_by();
                let (_, value) = permutation_for_arrangement(
                    &key.iter()
                        .cloned()
                        .map(MirScalarExpr::Column)
                        .collect::<Vec<_>>(),
                    variant.desc().arity(),
                );

                let updates =
                    updates.as_collection(move |(update_type, addr, source, port, ts), _| {
                        let row_arena = RowArena::default();
                        let update_type = if *update_type { "source" } else { "target" };
                        let binding = SharedRow::get();
                        let mut row_builder = binding.borrow_mut();
                        row_builder.packer().push_list(
                            addr.iter()
                                .chain_one(source)
                                .map(|id| Datum::UInt64(u64::cast_from(*id))),
                        );
                        let datums = &[
                            row_arena.push_unary_row(row_builder.clone()),
                            Datum::UInt64(u64::cast_from(*port)),
                            Datum::UInt64(u64::cast_from(worker_index)),
                            Datum::String(update_type),
                            Datum::from(ts.clone()),
                        ];
                        row_builder.packer().extend(key.iter().map(|k| datums[*k]));
                        let key_row = row_builder.clone();
                        row_builder
                            .packer()
                            .extend(value.iter().map(|k| datums[*k]));
                        let value_row = row_builder.clone();
                        (key_row, value_row)
                    });

                let trace = updates
                    .mz_arrange::<RowRowSpine<_, _>>(&format!("Arrange {variant:?}"))
                    .trace;
                result.insert(variant.clone(), (trace, Rc::clone(&token)));
            }
        }
        result
    });

    traces
}
