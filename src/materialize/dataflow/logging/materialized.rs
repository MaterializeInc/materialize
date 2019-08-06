// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

/// A logged peek event.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Peek {
    /// True for peek registration, false for peek retirement.
    registration: bool,
    /// The name of the view the peek targets.
    name: String,
    /// The logical timestamp requested.
    time: Timestamp,
    /// The UUID of the peek.
    uuid: uuid::Uuid,
}

use super::BatchLogger;
use crate::dataflow::arrangement::KeysOnlyHandle;
use crate::dataflow::types::Timestamp;
use crate::repr::Datum;
use std::time::Duration;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::ProbeHandle;
use timely::logging::WorkerIdentifier;

pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    probe: &mut ProbeHandle<Timestamp>,
    granularity_ns: u128,
) -> (
    BatchLogger<
        Peek,
        WorkerIdentifier,
        std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, Peek)>>,
    >,
    [KeysOnlyHandle; 2],
) {
    // Create timely dataflow logger based on shared linked lists.
    let writer = EventLink::<Timestamp, (Duration, WorkerIdentifier, Peek)>::new();
    let writer = std::rc::Rc::new(writer);
    let reader = writer.clone();

    // The two return values.
    let logger = BatchLogger::new(writer);

    let traces = worker.dataflow(move |scope| {
        use differential_dataflow::collection::AsCollection;
        use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        // TODO: Rewrite as one operator with multiple outputs.
        let logs = Some(reader).replay_into(scope);

        // Duration statistics derive from the non-rounded event times.
        let duration = logs.unary(
            timely::dataflow::channels::pact::Pipeline,
            "Peeks",
            |_, _| {
                let mut map = std::collections::HashMap::new();
                let mut vec = Vec::new();

                move |input, output| {
                    input.for_each(|time, data| {
                        data.swap(&mut vec);
                        let mut session = output.session(&time);
                        for (ts, worker, event) in vec.drain(..) {
                            let time_ns = ts.as_nanos();
                            let key = (worker, event.uuid);
                            if event.registration {
                                assert!(!map.contains_key(&key));
                                map.insert(key, time_ns);
                            } else {
                                assert!(map.contains_key(&key));
                                let start = map.remove(&key).expect("start event absent");
                                let elapsed = time_ns - start;
                                let time_ns = (((time_ns / granularity_ns) + 1) * granularity_ns)
                                    as Timestamp;
                                session.give((
                                    vec![
                                        Datum::String(format!("{}", key.1)),
                                        Datum::Int64(key.0 as i64),
                                        Datum::Int64(elapsed as i64),
                                    ],
                                    time_ns,
                                    1,
                                ));
                            }
                        }
                    });
                }
            },
        );

        let duration = duration.as_collection().arrange_by_self();
        duration.stream.probe_with(probe);

        let active = logs
            .map(|(ts, worker, peek)| {
                let time = ts.as_nanos() as Timestamp;
                let record = vec![
                    Datum::String(format!("{}", peek.uuid)),
                    Datum::Int64(worker as i64),
                    Datum::String(peek.name),
                    Datum::Int64(peek.time as i64),
                ];
                if peek.registration {
                    (record, time, 1isize)
                } else {
                    (record, time, -1isize)
                }
            })
            .as_collection()
            .arrange_by_self();

        active.stream.probe_with(probe);

        [duration.trace, active.trace]
    });

    (logger, traces)
}
