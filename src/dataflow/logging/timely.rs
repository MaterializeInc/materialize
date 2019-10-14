// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Clippy is wrong.
#![allow(clippy::or_fun_call)]

use super::{LogVariant, TimelyLog};
use crate::arrangement::KeysOnlyHandle;
use dataflow_types::logging::LoggingConfig;
use dataflow_types::Timestamp;
use repr::Datum;
use std::collections::HashMap;
use std::time::Duration;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::logging::{TimelyEvent, WorkerIdentifier};

// Constructs the logging dataflows and returns a logger and trace handles.
pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, TimelyEvent)>>,
) -> std::collections::HashMap<LogVariant, KeysOnlyHandle> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns() / 1_000_000) as Timestamp;

    // A dataflow for multiple log-derived arrangements.
    let traces = worker.dataflow(move |scope| {
        use differential_dataflow::collection::AsCollection;
        use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        // TODO: Rewrite as one operator with multiple outputs.
        let logs = Some(linked).replay_core(
            scope,
            Some(Duration::from_nanos(config.granularity_ns() as u64)),
        );

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let mut demux = OperatorBuilder::new("Timely Logging Demux".to_string(), scope.clone());

        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&logs, Pipeline);

        let (mut operates_out, operates) = demux.new_output();
        let (mut channels_out, channels) = demux.new_output();

        let mut demux_buffer = Vec::new();

        demux.build(move |_capability| {
            // These two maps track operator and channel information
            // so that they can be deleted when we observe the drop
            // events for the corresponding operators.
            let mut operates_data = HashMap::new();
            let mut channels_data = HashMap::new();

            move |_frontiers| {
                let mut operates = operates_out.activate();
                let mut channels = channels_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut operates_session = operates.session(&time);
                    let mut channels_session = channels.session(&time);

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        let time_ms = (((time.as_millis() as Timestamp / granularity_ms) + 1)
                            * granularity_ms) as Timestamp;

                        match datum {
                            TimelyEvent::Operates(event) => {
                                // Record operator information so that we can replay a negated
                                // version when the operator is dropped.
                                operates_data.insert(
                                    (event.id, worker),
                                    (event.addr.clone(), event.name.clone()),
                                );

                                for (addr_slot, addr_value) in event.addr.iter().enumerate() {
                                    operates_session.give((
                                        vec![
                                            Datum::Int64(event.id as i64),
                                            Datum::Int64(worker as i64),
                                            Datum::Int64(addr_slot as i64),
                                            Datum::Int64(*addr_value as i64),
                                            Datum::String(event.name.clone()),
                                        ],
                                        time_ms,
                                        1,
                                    ));
                                }
                            }
                            TimelyEvent::Channels(event) => {
                                // Record channel information so that we can replay a negated
                                // version when the host dataflow is dropped.
                                channels_data
                                    .entry(event.scope_addr[0])
                                    .or_insert(Vec::new())
                                    .push((
                                        event.id,
                                        worker,
                                        event.scope_addr.clone(),
                                        event.source,
                                        event.target,
                                    ));

                                channels_session.give((
                                    vec![
                                        Datum::Int64(event.id as i64),
                                        Datum::Int64(worker as i64),
                                        Datum::String(format!("{:?}", event.scope_addr)),
                                        Datum::Int64(event.source.0 as i64),
                                        Datum::Int64(event.source.1 as i64),
                                        Datum::Int64(event.target.0 as i64),
                                        Datum::Int64(event.target.1 as i64),
                                    ],
                                    time_ms,
                                    1,
                                ));
                            }
                            TimelyEvent::Shutdown(event) => {
                                // Dropped operators should result in a negative record for
                                // the `operates` collection, cancelling out the initial
                                // operator announcement.
                                if let Some((address, event_name)) =
                                    operates_data.remove(&(event.id, worker))
                                {
                                    for (addr_slot, addr_value) in address.iter().enumerate() {
                                        operates_session.give((
                                            vec![
                                                Datum::Int64(event.id as i64),
                                                Datum::Int64(worker as i64),
                                                Datum::Int64(addr_slot as i64),
                                                Datum::Int64(*addr_value as i64),
                                                Datum::String(event_name.clone()),
                                            ],
                                            time_ms,
                                            -1,
                                        ));
                                    }
                                    // If we are observing a dataflow shutdown, we should also
                                    // issue a deletion for channels in the dataflow.
                                    if address.len() == 1 {
                                        if let Some(channels) = channels_data.remove(&address[0]) {
                                            for (event_id, worker, scope_addr, source, target) in
                                                channels
                                            {
                                                channels_session.give((
                                                    vec![
                                                        Datum::Int64(event_id as i64),
                                                        Datum::Int64(worker as i64),
                                                        Datum::String(format!("{:?}", scope_addr)),
                                                        Datum::Int64(source.0 as i64),
                                                        Datum::Int64(source.1 as i64),
                                                        Datum::Int64(target.0 as i64),
                                                        Datum::Int64(target.1 as i64),
                                                    ],
                                                    time_ms,
                                                    -1,
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                });
            }
        });

        use differential_dataflow::operators::reduce::Count;
        use timely::dataflow::operators::generic::operator::Operator;

        // Duration statistics derive from the non-rounded event times.
        let duration = logs
            .flat_map(move |(ts, worker, x)| {
                if let TimelyEvent::Schedule(event) = x {
                    Some((ts, worker, event))
                } else {
                    None
                }
            })
            // TODO: Should probably be an exchange with the correct key ...
            .unary(
                timely::dataflow::channels::pact::Pipeline,
                "Schedules",
                |_, _| {
                    let mut map = std::collections::HashMap::new();
                    let mut vec = Vec::new();

                    move |input, output| {
                        input.for_each(|time, data| {
                            data.swap(&mut vec);
                            let mut session = output.session(&time);
                            for (ts, worker, event) in vec.drain(..) {
                                let time_ns = ts.as_nanos();
                                let key = (worker, event.id);
                                match event.start_stop {
                                    timely::logging::StartStop::Start => {
                                        assert!(!map.contains_key(&key));
                                        map.insert(key, time_ns);
                                    }
                                    timely::logging::StartStop::Stop => {
                                        assert!(map.contains_key(&key));
                                        let start = map.remove(&key).expect("start event absent");
                                        let elapsed_ns = time_ns - start;
                                        let time_ms = (time_ns / 1_000_000) as Timestamp;
                                        let time_ms =
                                            ((time_ms / granularity_ms) + 1) * granularity_ms;
                                        session.give((key.1, time_ms, elapsed_ns));
                                    }
                                }
                            }
                        });
                    }
                },
            );

        // Accumulate the durations of each operator.
        let elapsed = duration
            .map(|(op, t, d)| (op, t, d as isize))
            .as_collection()
            .count()
            .map(|(op, cnt)| vec![Datum::Int64(op as i64), Datum::Int64(cnt as i64)])
            .arrange_by_self();
        let histogram = duration
            .map(|(op, t, d)| ((op, d.next_power_of_two()), t, 1i64))
            .as_collection()
            .count()
            .map(|((op, pow), cnt)| {
                vec![
                    Datum::Int64(op as i64),
                    Datum::Int64(pow as i64),
                    Datum::Int64(cnt as i64),
                ]
            })
            .arrange_by_self();

        let operates = operates.as_collection().arrange_by_self();
        let channels = channels.as_collection().arrange_by_self();

        // Restrict results by those logs that are meant to be active.
        vec![
            (LogVariant::Timely(TimelyLog::Operates), operates.trace),
            (LogVariant::Timely(TimelyLog::Channels), channels.trace),
            (LogVariant::Timely(TimelyLog::Elapsed), elapsed.trace),
            (LogVariant::Timely(TimelyLog::Histogram), histogram.trace),
        ]
        .into_iter()
        .filter(|(name, _trace)| config.active_logs().contains(name))
        .collect()
    });

    traces
}
