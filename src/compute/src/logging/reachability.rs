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
use std::time::Duration;

use columnar::{Columnar, Index};
use differential_dataflow::Hashable;
use mz_compute_client::logging::LoggingConfig;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, Row, RowRef, Timestamp};
use mz_timely_util::containers::{Col2ValBatcher, Column, ColumnBuilder};
use mz_timely_util::replay::MzReplay;
use timely::communication::Allocate;
use timely::dataflow::channels::pact::ExchangeCore;
use timely::Container;

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::initialize::ReachabilityEvent;
use crate::logging::{prepare_log_collection, EventQueue, LogCollection, LogVariant, TimelyLog};
use crate::row_spine::RowRowBuilder;
use crate::typedefs::RowRowSpine;

/// Constructs the logging dataflow for reachability logs.
///
/// Params
/// * `worker`: The Timely worker hosting the log analysis dataflow.
/// * `config`: Logging configuration
/// * `event_queue`: The source to read log events from.
pub(super) fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
    event_queue: EventQueue<Column<(Duration, ReachabilityEvent)>>,
) -> BTreeMap<LogVariant, LogCollection> {
    let interval_ms = std::cmp::max(1, config.interval.as_millis());
    let worker_index = worker.index();
    let dataflow_index = worker.next_dataflow_index();

    // A dataflow for multiple log-derived arrangements.
    let traces = worker.dataflow_named("Dataflow: timely reachability logging", move |scope| {
        let enable_logging = config.enable_logging;
        type UpdatesKey = (bool, Vec<usize>, usize, usize, Option<Timestamp>);

        type CB = ColumnBuilder<((UpdatesKey, ()), Timestamp, Diff)>;
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
                for (time, (addr, massaged)) in data.iter() {
                    let time_ms = ((time.as_millis() / interval_ms) + 1) * interval_ms;
                    let time_ms: Timestamp = time_ms.try_into().expect("must fit");
                    for (source, port, update_type, ts, diff) in massaged.into_iter() {
                        let datum = (update_type, addr, source, port, ts);
                        session.give(((datum, ()), time_ms, diff));
                    }
                }
            },
        );

        // Restrict results by those logs that are meant to be active.
        let logs_active = [LogVariant::Timely(TimelyLog::Reachability)];

        let mut addr_row = Row::default();
        let updates = prepare_log_collection(
            &updates,
            TimelyLog::Reachability,
            move |datum, (), packer| {
                let (update_type, addr, source, port, ts) = datum;
                let update_type = if update_type { "source" } else { "target" };
                addr_row.packer().push_list(
                    addr.iter()
                        .chain(std::iter::once(source))
                        .map(|id| Datum::UInt64(u64::cast_from(id))),
                );
                packer.pack_slice(&[
                    addr_row.iter().next().unwrap(),
                    Datum::UInt64(u64::cast_from(port)),
                    Datum::UInt64(u64::cast_from(worker_index)),
                    Datum::String(update_type),
                    Datum::from(<Option<Timestamp>>::into_owned(ts)),
                ]);
            }
        );

        let mut result = BTreeMap::new();
        for variant in logs_active {
            if config.index_logs.contains_key(&variant) {
                let trace = updates
                    .mz_arrange_core::<_, Col2ValBatcher<_, _, _, _>, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                        ExchangeCore::new(|((key, _val), _time, _diff): &((&RowRef, &RowRef), _, _)| key.hashed()),
                        &format!("Arrange {variant:?}"),
                    )
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
