// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by timely dataflow.

use std::collections::HashMap;
use std::time::Duration;

use differential_dataflow::difference::{Abelian, DiffPair};
use differential_dataflow::operators::count::CountTotal;
use differential_dataflow::operators::iterate::SemigroupVariable;
use differential_dataflow::operators::join::Join;
use differential_dataflow::{Collection, Data};
use timely::communication::Allocate;
use timely::dataflow::operators::capture::EventLink;
use timely::dataflow::Scope;
use timely::logging::{ParkEvent, TimelyEvent, WorkerIdentifier};

use super::{LogVariant, TimelyLog};
use crate::arrangement::KeysValsHandle;
use dataflow_types::logging::LoggingConfig;
use repr::{Datum, Timestamp};

/// Constructs the logging dataflows and returns a logger and trace handles.
pub fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
    linked: std::rc::Rc<EventLink<Timestamp, (Duration, WorkerIdentifier, TimelyEvent)>>,
) -> std::collections::HashMap<LogVariant, (Vec<usize>, KeysValsHandle)> {
    let granularity_ms = std::cmp::max(1, config.granularity_ns / 1_000_000) as Timestamp;

    // A dataflow for multiple log-derived arrangements.
    let traces = worker.dataflow_named("Dataflow: timely logging", move |scope| {
        use differential_dataflow::collection::AsCollection;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::Map;

        let logs = Some(linked).replay_core(
            scope,
            Some(Duration::from_nanos(config.granularity_ns as u64)),
        );

        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let mut demux = OperatorBuilder::new("Timely Logging Demux".to_string(), scope.clone());

        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&logs, Pipeline);

        let (mut operates_out, operates) = demux.new_output();
        let (mut channels_out, channels) = demux.new_output();
        let (mut addresses_out, addresses) = demux.new_output();
        let (mut parks_out, parks) = demux.new_output();
        let (mut messages_out, messages) = demux.new_output();

        let mut demux_buffer = Vec::new();
        demux.build(move |_capability| {
            // These two maps track operator and channel information
            // so that they can be deleted when we observe the drop
            // events for the corresponding operators.
            let mut operates_data = HashMap::new();
            let mut channels_data = HashMap::new();
            let mut parks_data = HashMap::new();
            let mut row_packer = repr::RowPacker::new();
            move |_frontiers| {
                let mut operates = operates_out.activate();
                let mut channels = channels_out.activate();
                let mut addresses = addresses_out.activate();
                let mut parks = parks_out.activate();
                let mut messages = messages_out.activate();

                input.for_each(|time, data| {
                    data.swap(&mut demux_buffer);

                    let mut operates_session = operates.session(&time);
                    let mut channels_session = channels.session(&time);
                    let mut addresses_session = addresses.session(&time);
                    let mut parks_sesssion = parks.session(&time);
                    let mut messages_session = messages.session(&time);

                    for (time, worker, datum) in demux_buffer.drain(..) {
                        let time_ns = time.as_nanos();
                        let time_ms = (((time.as_millis() as Timestamp / granularity_ms) + 1)
                            * granularity_ms) as Timestamp;

                        match datum {
                            TimelyEvent::Operates(event) => {
                                // Record operator information so that we can replay a negated
                                // version when the operator is dropped.
                                operates_data.insert((event.id, worker), event.clone());

                                operates_session.give((
                                    ((event.id, worker), event.name),
                                    time_ms,
                                    1,
                                ));

                                for (addr_slot, addr_value) in event.addr.iter().enumerate() {
                                    addresses_session.give((
                                        row_packer.pack(&[
                                            Datum::Int64(event.id as i64),
                                            Datum::Int64(worker as i64),
                                            Datum::Int64(addr_slot as i64),
                                            Datum::Int64(*addr_value as i64),
                                        ]),
                                        time_ms,
                                        1,
                                    ));
                                }
                            }
                            TimelyEvent::Channels(event) => {
                                // Record channel information so that we can replay a negated
                                // version when the host dataflow is dropped.
                                channels_data
                                    .entry((event.scope_addr[0], worker))
                                    .or_insert_with(Vec::new)
                                    .push(event.clone());

                                // Present channel description.
                                channels_session.give((
                                    (
                                        event.id,
                                        worker,
                                        event.source.0,
                                        event.source.1,
                                        event.target.0,
                                        event.target.1,
                                    ),
                                    time_ms,
                                    1,
                                ));

                                // Enumerate the address of the scope containing the channel.
                                for (addr_slot, addr_value) in event.scope_addr.iter().enumerate() {
                                    addresses_session.give((
                                        row_packer.pack(&[
                                            Datum::Int64(event.id as i64),
                                            Datum::Int64(worker as i64),
                                            Datum::Int64(addr_slot as i64),
                                            Datum::Int64(*addr_value as i64),
                                        ]),
                                        time_ms,
                                        1,
                                    ));
                                }
                            }
                            TimelyEvent::Shutdown(event) => {
                                // Dropped operators should result in a negative record for
                                // the `operates` collection, cancelling out the initial
                                // operator announcement.
                                if let Some(event) = operates_data.remove(&(event.id, worker)) {
                                    operates_session.give((
                                        ((event.id, worker), event.name),
                                        time_ms,
                                        -1,
                                    ));

                                    for (addr_slot, addr_value) in event.addr.iter().enumerate() {
                                        addresses_session.give((
                                            row_packer.pack(&[
                                                Datum::Int64(event.id as i64),
                                                Datum::Int64(worker as i64),
                                                Datum::Int64(addr_slot as i64),
                                                Datum::Int64(*addr_value as i64),
                                            ]),
                                            time_ms,
                                            -1,
                                        ));
                                    }
                                    // If we are observing a dataflow shutdown, we should also
                                    // issue a deletion for channels in the dataflow.
                                    if event.addr.len() == 1 {
                                        let dataflow_id = event.addr[0];
                                        if let Some(events) =
                                            channels_data.remove(&(dataflow_id, worker))
                                        {
                                            for event in events {
                                                // Retract channel description.
                                                channels_session.give((
                                                    (
                                                        event.id,
                                                        worker,
                                                        event.source.0,
                                                        event.source.1,
                                                        event.target.0,
                                                        event.target.1,
                                                    ),
                                                    time_ms,
                                                    -1,
                                                ));

                                                // Enumerate the address of the scope containing the channel.
                                                for (addr_slot, addr_value) in
                                                    event.scope_addr.iter().enumerate()
                                                {
                                                    addresses_session.give((
                                                        row_packer.pack(&[
                                                            Datum::Int64(event.id as i64),
                                                            Datum::Int64(worker as i64),
                                                            Datum::Int64(addr_slot as i64),
                                                            Datum::Int64(*addr_value as i64),
                                                        ]),
                                                        time_ms,
                                                        -1,
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            TimelyEvent::Park(event) => match event {
                                ParkEvent::Park(duration) => {
                                    parks_data.insert(worker, (time_ns, duration));
                                }
                                ParkEvent::Unpark => {
                                    if let Some((start_ns, requested)) = parks_data.remove(&worker)
                                    {
                                        let duration_ns = time_ns - start_ns;
                                        parks_sesssion.give((
                                            worker,
                                            duration_ns,
                                            requested,
                                            time_ms,
                                        ));
                                    } else {
                                        panic!("Park data not found!");
                                    }
                                }
                            },

                            TimelyEvent::Messages(event) => {
                                messages_session.give((
                                    (
                                        (event.channel, event.source, event.target),
                                        event.length,
                                        event.is_send,
                                    ),
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
                                        session.give(((key.1, worker), time_ms, elapsed_ns));
                                    }
                                }
                            }
                        });
                    }
                },
            );

        let operates = operates.as_collection();

        // Feedback delay for log flushing. This should be large enough that there is not a tight
        // feedback loop that prevents high-throughput work, but not so large that we leave volumes
        // of logging data around that we do not need. Effectively, a throughput v memory trade-off.
        let delay = std::time::Duration::from_nanos(10_000_000_000);

        // Accumulate the durations of each operator.
        // The first `map` exists so that we can semijoin effectively (it requires a key-val pair).
        let mut elapsed = duration
            .map(|(op_worker, t, d)| ((op_worker, ()), t, d as isize))
            .as_collection();
        // Remove elapsed times for operators not present in `operates`.
        elapsed = thin_collection(elapsed, delay, |c| c.semijoin(&operates.map(|(k, _)| k)));
        let elapsed = elapsed.map(|(op, ())| op).count_total();

        // Accumulate histograms of execution times for each operator.
        let mut histogram = duration
            .map(|(op_worker, t, d)| ((op_worker, d.next_power_of_two()), t, 1isize))
            .as_collection();
        // Remove histogram measurements for operators not present in `operates`.
        histogram = thin_collection(histogram, delay, |c| c.semijoin(&operates.map(|(k, _)| k)));
        let histogram = histogram
            .count_total()
            .map(|((key, pow), count)| ((key), (pow, count)));

        let elapsed = elapsed.map({
            let mut row_packer = repr::RowPacker::new();
            move |((id, worker), cnt)| {
                row_packer.pack(&[
                    Datum::Int64(id as i64),
                    Datum::Int64(worker as i64),
                    Datum::Int64(cnt as i64),
                ])
            }
        });

        let histogram = histogram.map({
            let mut row_packer = repr::RowPacker::new();
            move |((id, worker), (pow, cnt))| {
                row_packer.pack(&[
                    Datum::Int64(id as i64),
                    Datum::Int64(worker as i64),
                    Datum::Int64(pow as i64),
                    Datum::Int64(cnt as i64),
                ])
            }
        });

        let operates = operates.map({
            let mut row_packer = repr::RowPacker::new();
            move |((id, worker), name)| {
                row_packer.pack(&[
                    Datum::Int64(id as i64),
                    Datum::Int64(worker as i64),
                    Datum::String(&name),
                ])
            }
        });

        let addresses = addresses.as_collection();
        let channels = channels.as_collection().map(
            |(id, worker, source_node, source_port, target_node, target_port)| {
                (
                    id,
                    (worker, source_node, source_port, target_node, target_port),
                )
            },
        );

        let parks = parks
            .map(|(w, d, r, t)| {
                (
                    (
                        w,
                        d.next_power_of_two(),
                        r.map(|r| r.as_nanos().next_power_of_two()),
                    ),
                    t,
                    1,
                )
            })
            .as_collection()
            .count_total()
            .map({
                let mut row_packer = repr::RowPacker::new();
                move |((w, d, r), c)| {
                    row_packer.pack(&[
                        Datum::Int64(w as i64),
                        Datum::Int64(d as i64),
                        r.map(|r| Datum::Int64(r as i64)).unwrap_or(Datum::Null),
                        Datum::Int64(c),
                    ])
                }
            });

        use differential_dataflow::operators::Count;
        let mut messages = messages
            .as_collection()
            .explode(|(key, count, is_send)| {
                Some((
                    key,
                    if is_send {
                        DiffPair::new(count as i64, 0)
                    } else {
                        DiffPair::new(0, count as i64)
                    },
                ))
            })
            .count()
            .map(|((channel, source, target), counts)| (channel, (source, target, counts)));
        messages = thin_collection(messages, delay, |c| {
            c.semijoin(&channels.map(|(channel, _)| channel))
        });
        let messages = messages.map({
            let mut row_packer = repr::RowPacker::new();
            move |(
                channel,
                (
                    source,
                    target,
                    DiffPair {
                        element1: sent,
                        element2: received,
                    },
                ),
            )| {
                row_packer.pack(&[
                    Datum::Int64(channel as i64),
                    Datum::Int64(source as i64),
                    Datum::Int64(target as i64),
                    Datum::Int64(sent),
                    Datum::Int64(received),
                ])
            }
        });

        let channels = channels.map({
            let mut row_packer = repr::RowPacker::new();
            move |(id, (worker, source_node, source_port, target_node, target_port))| {
                row_packer.pack(&[
                    Datum::Int64(id as i64),
                    Datum::Int64(worker as i64),
                    Datum::Int64(source_node as i64),
                    Datum::Int64(source_port as i64),
                    Datum::Int64(target_node as i64),
                    Datum::Int64(target_port as i64),
                ])
            }
        });

        use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;

        // Restrict results by those logs that are meant to be active.
        let logs = vec![
            (LogVariant::Timely(TimelyLog::Operates), operates),
            (LogVariant::Timely(TimelyLog::Channels), channels),
            (LogVariant::Timely(TimelyLog::Elapsed), elapsed),
            (LogVariant::Timely(TimelyLog::Histogram), histogram),
            (LogVariant::Timely(TimelyLog::Addresses), addresses),
            (LogVariant::Timely(TimelyLog::Parks), parks),
            (LogVariant::Timely(TimelyLog::Messages), messages),
        ];

        let mut result = std::collections::HashMap::new();
        for (variant, collection) in logs {
            if config.active_logs.contains_key(&variant) {
                let key = variant.index_by();
                let key_clone = key.clone();
                let trace = collection
                    .map({
                        let mut row_packer = repr::RowPacker::new();
                        move |row| {
                            let datums = row.unpack();
                            let key_row = row_packer.pack(key.iter().map(|k| datums[*k]));
                            (key_row, row)
                        }
                    })
                    .arrange_by_key()
                    .trace;
                result.insert(variant, (key_clone, trace));
            }
        }
        result
    });

    traces
}

/// Discard all of the records in `c` that `logic` doesn't care about (return).
fn thin_collection<G, D, R, F>(
    c: Collection<G, D, R>,
    delay: std::time::Duration,
    mut logic: F,
) -> Collection<G, D, R>
where
    G: Scope<Timestamp = Timestamp>,
    D: Data,
    R: Abelian,
    F: FnMut(Collection<G, D, R>) -> Collection<G, D, R>,
{
    // `retractions` represents the records in `c` that we no longer care about
    let retractions = SemigroupVariable::new(&mut c.scope(), delay.as_millis() as Timestamp);
    // subtract out the retractions from `c`
    let thinned = c.concat(&retractions.negate());
    // Compute the collection we still care about
    let result = logic(thinned);
    // Finally set `retractions` to be everything not part of the output.
    retractions.set(&c.concat(&result.negate()));
    result
}
