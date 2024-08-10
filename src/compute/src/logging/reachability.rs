// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by timely dataflow.

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::rc::Rc;

use mz_compute_client::logging::LoggingConfig;
use mz_expr::{permutation_for_arrangement, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_ore::flatcontainer::{ItemRegion, MzIndexOptimized, MzRegionPreference, OwnedRegionOpinion};
use mz_ore::iter::IteratorExt;
use mz_repr::{Datum, Diff, RowArena, SharedRow, Timestamp};
use mz_timely_util::containers::PreallocatingCapacityContainerBuilder;
use mz_timely_util::replay::MzReplay;
use timely::communication::Allocate;
use timely::container::flatcontainer::FlatStack;
use timely::dataflow::channels::pact::Pipeline;

use crate::extensions::arrange::{MzArrange, MzArrangeCore};
use crate::logging::initialize::ReachabilityEventRegion;
use crate::logging::{EventQueue, LogCollection, LogVariant, TimelyLog};
use crate::typedefs::{FlatKeyValSpineDefault, RowRowSpine};

/// Constructs the logging dataflow for reachability logs.
///
/// Params
/// * `worker`: The Timely worker hosting the log analysis dataflow.
/// * `config`: Logging configuration
/// * `event_queue`: The source to read log events from.
pub(super) fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
    event_queue: EventQueue<FlatStack<ReachabilityEventRegion, MzIndexOptimized>>,
) -> BTreeMap<LogVariant, LogCollection> {
    let interval_ms = std::cmp::max(1, config.interval.as_millis());
    let worker_index = worker.index();
    let dataflow_index = worker.next_dataflow_index();

    // A dataflow for multiple log-derived arrangements.
    let traces = worker.dataflow_named("Dataflow: timely reachability logging", move |scope| {
        let enable_logging = config.enable_logging;
        type UpdatesKey = (
            bool,
            OwnedRegionOpinion<Vec<usize>>,
            usize,
            usize,
            Option<Timestamp>,
        );
        type UpdatesRegion =
            ItemRegion<<((UpdatesKey, ()), Timestamp, Diff) as MzRegionPreference>::Region>;

        type CB = PreallocatingCapacityContainerBuilder<FlatStack<UpdatesRegion, MzIndexOptimized>>;
        let (updates, token) = Some(event_queue.link).mz_replay::<_, CB, _>(
            scope,
            "reachability logs",
            config.interval,
            event_queue.activator,
            move |mut session, data| {
                // If logging is disabled, we still need to install the indexes, but we can leave them
                // empty. We do so by immediately filtering all logs events.
                if !enable_logging {
                    return;
                }
                for (time, _worker, (addr, massaged)) in data.iter() {
                    let time_ms = ((time.as_millis() / interval_ms) + 1) * interval_ms;
                    let time_ms: Timestamp = time_ms.try_into().expect("must fit");
                    for (source, port, update_type, ts, diff) in massaged {
                        let datum = (update_type, addr, source, port, ts);
                        session.give(((datum, ()), time_ms, diff));
                    }
                }
            },
        );

        // Restrict results by those logs that are meant to be active.
        let logs_active = vec![LogVariant::Timely(TimelyLog::Reachability)];

        let updates = updates
            .mz_arrange_core::<_, FlatKeyValSpineDefault<UpdatesKey, (), Timestamp, Diff, _>>(
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
                        let update_type = if update_type { "source" } else { "target" };
                        let binding = SharedRow::get();
                        let mut row_builder = binding.borrow_mut();
                        row_builder.packer().push_list(
                            addr.iter()
                                .copied()
                                .chain_one(source)
                                .map(|id| Datum::UInt64(u64::cast_from(id))),
                        );
                        let datums = &[
                            row_arena.push_unary_row(row_builder.clone()),
                            Datum::UInt64(u64::cast_from(port)),
                            Datum::UInt64(u64::cast_from(worker_index)),
                            Datum::String(update_type),
                            Datum::from(ts),
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
                let collection = LogCollection {
                    trace,
                    token: Rc::clone(&token),
                    dataflow_index,
                };
                result.insert(variant, collection);
            }
        }
        result
    });

    traces
}
