// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use super::{BatchLogger, LogVariant, TimelyLog};
use crate::dataflow::arrangement::KeysOnlyHandle;
use crate::dataflow::types::Timestamp;
use crate::repr::Datum;
use std::time::Duration;
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::ProbeHandle;
use timely::logging::{TimelyEvent, WorkerIdentifier};

// Constructs the logging dataflows and returns a logger and trace handles.
pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    probe: &mut ProbeHandle<Timestamp>,
    config: &super::LoggingConfiguration,
) -> (
    BatchLogger<
        TimelyEvent,
        WorkerIdentifier,
        std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, TimelyEvent)>>,
    >,
    std::collections::HashMap<LogVariant, KeysOnlyHandle>,
) {
    // Create timely dataflow logger based on shared linked lists.
    let writer =
        timely::dataflow::operators::capture::event::link::EventLink::<Timestamp, _>::new();
    let writer = std::rc::Rc::new(writer);
    let reader = writer.clone();

    // let granularity_ns = config.granularity_ns;
    let granularity_ms = std::cmp::max(1, config.granularity_ns / 1_000_000) as Timestamp;

    // The two return values.
    let logger = BatchLogger::new(writer);

    // A dataflow for multiple log-derived arrangements.
    let traces = worker.dataflow(move |scope| {
        use differential_dataflow::collection::AsCollection;
        use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        // TODO: Rewrite as one operator with multiple outputs.
        let logs = Some(reader).replay_into(scope);

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let mut demux = OperatorBuilder::new("Timely Logging Demux".to_string(), scope.clone());

        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&logs, Pipeline);

        let (mut operates_out, operates) = demux.new_output();
        let (mut channels_out, channels) = demux.new_output();
        let (mut messages_out, messages) = demux.new_output();
        let (mut shutdown_out, shutdown) = demux.new_output();
        let (mut text_out, text) = demux.new_output();

        let mut demux_buffer = Vec::new();

        demux.build(move |_capability| {
            move |_frontiers| {
                let mut operates = operates_out.activate();
                let mut channels = channels_out.activate();
                let mut messages = messages_out.activate();
                let mut shutdown = shutdown_out.activate();
                let mut text = text_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut operates_session = operates.session(&time);
                    let mut channels_session = channels.session(&time);
                    let mut messages_session = messages.session(&time);
                    let mut shutdown_session = shutdown.session(&time);
                    let mut text_session = text.session(&time);

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        let time_ms = (((time.as_millis() as Timestamp / granularity_ms) + 1)
                            * granularity_ms) as Timestamp;

                        match datum {
                            TimelyEvent::Operates(event) => {
                                operates_session.give((
                                    vec![
                                        Datum::Int64(event.id as i64),
                                        Datum::Int64(worker as i64),
                                        Datum::String(format!("{:?}", event.addr)),
                                        Datum::String(event.name),
                                    ],
                                    time_ms,
                                    1,
                                ));
                            }
                            TimelyEvent::Channels(event) => {
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
                            TimelyEvent::Messages(messages) => messages_session.give((
                                messages.channel,
                                time_ms,
                                messages.length as isize,
                            )),
                            TimelyEvent::Shutdown(event) => {
                                shutdown_session.give((
                                    vec![
                                        Datum::Int64(event.id as i64),
                                        Datum::Int64(worker as i64),
                                    ],
                                    time_ms,
                                    1,
                                ));
                            }
                            TimelyEvent::Text(text) => {
                                text_session.give((
                                    vec![Datum::String(text), Datum::Int64(worker as i64)],
                                    time_ms,
                                    1,
                                ));
                            }
                            _ => {}
                        }
                    }
                });
            }
        });

        use differential_dataflow::operators::reduce::Count;
        use timely::dataflow::operators::generic::operator::Operator;
        use timely::dataflow::operators::probe::Probe;

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
        let messages = messages
            .as_collection()
            .count()
            .map(|(channel, count)| vec![Datum::Int64(channel as i64), Datum::Int64(count as i64)])
            .arrange_by_self();
        let shutdown = shutdown.as_collection().arrange_by_self();
        let text = text.as_collection().arrange_by_self();

        operates.stream.probe_with(probe);
        channels.stream.probe_with(probe);
        messages.stream.probe_with(probe);
        shutdown.stream.probe_with(probe);
        text.stream.probe_with(probe);
        elapsed.stream.probe_with(probe);
        histogram.stream.probe_with(probe);

        // Restrict results by those logs that are meant to be active.
        vec![
            (LogVariant::Timely(TimelyLog::Operates), operates.trace),
            (LogVariant::Timely(TimelyLog::Channels), channels.trace),
            (LogVariant::Timely(TimelyLog::Messages), messages.trace),
            (LogVariant::Timely(TimelyLog::Shutdown), shutdown.trace),
            (LogVariant::Timely(TimelyLog::Text), text.trace),
            (LogVariant::Timely(TimelyLog::Elapsed), elapsed.trace),
            (LogVariant::Timely(TimelyLog::Histogram), histogram.trace),
        ]
        .into_iter()
        .filter(|(name, _trace)| config.active_logs.contains(name))
        .collect()
    });

    (logger, traces)
}
