// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use super::BatchLogger;
use crate::dataflow::arrangement::KeysOnlyHandle;
use crate::dataflow::types::Timestamp;
use crate::repr::Datum;
use differential_dataflow::logging::DifferentialEvent;
use std::time::Duration;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::ProbeHandle;
use timely::logging::WorkerIdentifier;

pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    probe: &mut ProbeHandle<Timestamp>,
    granularity_ns: u128,
) -> (
    BatchLogger<
        DifferentialEvent,
        WorkerIdentifier,
        std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, DifferentialEvent)>>,
    >,
    [KeysOnlyHandle; 1],
) {
    // Create timely dataflow logger based on shared linked lists.
    let writer = EventLink::<Timestamp, (Duration, WorkerIdentifier, DifferentialEvent)>::new();
    let writer = std::rc::Rc::new(writer);
    let reader = writer.clone();

    // The two return values.
    let logger = BatchLogger::new(writer);

    let traces = worker.dataflow(move |scope| {
        use differential_dataflow::collection::AsCollection;
        use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
        use differential_dataflow::operators::reduce::Count;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        // TODO: Rewrite as one operator with multiple outputs.
        let logs = Some(reader).replay_into(scope);

        // Duration statistics derive from the non-rounded event times.
        let arrangements = logs
            .flat_map(move |(ts, worker, event)| {
                let time = (((ts.as_nanos() / granularity_ns) + 1) * granularity_ns) as Timestamp;
                match event {
                    DifferentialEvent::Batch(event) => {
                        let difference = differential_dataflow::difference::DiffVector::new(vec![
                            event.length as isize,
                            1,
                        ]);
                        Some(((event.operator, worker), time, difference))
                    }
                    DifferentialEvent::Merge(event) => {
                        if let Some(done) = event.complete {
                            Some((
                                (event.operator, worker),
                                time,
                                differential_dataflow::difference::DiffVector::new(vec![
                                    (done as isize) - ((event.length1 + event.length2) as isize),
                                    -1,
                                ]),
                            ))
                        } else {
                            None
                        }
                    }
                    DifferentialEvent::MergeShortfall(_) => None,
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

        arrangements.stream.probe_with(probe);

        [arrangements.trace]
    });

    (logger, traces)
}
