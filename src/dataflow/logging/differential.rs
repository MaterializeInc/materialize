// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use super::{DifferentialLog, LogVariant};
use crate::arrangement::KeysOnlyHandle;
use dataflow_types::Timestamp;
use differential_dataflow::logging::DifferentialEvent;
use repr::Datum;
use std::time::Duration;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::logging::WorkerIdentifier;

pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &dataflow_types::logging::LoggingConfig,
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, DifferentialEvent)>>,
) -> std::collections::HashMap<LogVariant, KeysOnlyHandle> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns() / 1_000_000) as Timestamp;

    let traces = worker.dataflow(move |scope| {
        use differential_dataflow::collection::AsCollection;
        use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
        use differential_dataflow::operators::reduce::Count;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        // TODO: Rewrite as one operator with multiple outputs.
        let logs = Some(linked).replay_core(
            scope,
            Some(Duration::from_nanos(config.granularity_ns() as u64)),
        );

        // Duration statistics derive from the non-rounded event times.
        let arrangements = logs
            .flat_map(move |(ts, worker, event)| {
                let time_ms = ((ts.as_millis() as Timestamp / granularity_ms) + 1) * granularity_ms;
                match event {
                    DifferentialEvent::Batch(event) => {
                        let difference = differential_dataflow::difference::DiffVector::new(vec![
                            event.length as isize,
                            1,
                        ]);
                        Some(((event.operator, worker), time_ms, difference))
                    }
                    DifferentialEvent::Merge(event) => {
                        if let Some(done) = event.complete {
                            Some((
                                (event.operator, worker),
                                time_ms,
                                differential_dataflow::difference::DiffVector::new(vec![
                                    (done as isize) - ((event.length1 + event.length2) as isize),
                                    -1,
                                ]),
                            ))
                        } else {
                            None
                        }
                    }
                    DifferentialEvent::Drop(event) => {
                        let difference = differential_dataflow::difference::DiffVector::new(vec![
                            -(event.length as isize),
                            -1,
                        ]);
                        Some(((event.operator, worker), time_ms, difference))
                    }
                    DifferentialEvent::MergeShortfall(_) => None,
                    DifferentialEvent::TraceShare(_) => None,
                }
            })
            .as_collection()
            .count()
            .map(|((op, worker), count)| {
                vec![
                    Datum::Int64(op as i64),
                    Datum::Int64(worker as i64),
                    Datum::Int64(count[0] as i64),
                    Datum::Int64(count[1] as i64),
                ]
            })
            .arrange_by_self();

        // Duration statistics derive from the non-rounded event times.
        let sharing = logs
            .flat_map(move |(ts, worker, event)| {
                let time_ms = ((ts.as_millis() as Timestamp / granularity_ms) + 1) * granularity_ms;
                if let DifferentialEvent::TraceShare(event) = event {
                    Some(((event.operator, worker), time_ms, event.diff))
                } else {
                    None
                }
            })
            .as_collection()
            .count()
            .map(|((op, worker), count)| {
                vec![
                    Datum::Int64(op as i64),
                    Datum::Int64(worker as i64),
                    Datum::Int64(count as i64),
                ]
            })
            .arrange_by_self();

        vec![
            (
                LogVariant::Differential(DifferentialLog::Arrangement),
                arrangements.trace,
            ),
            (
                LogVariant::Differential(DifferentialLog::Sharing),
                sharing.trace,
            ),
        ]
        .into_iter()
        .filter(|(name, _trace)| config.active_logs().contains(name))
        .collect()
    });

    traces
}
