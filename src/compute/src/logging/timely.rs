// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by timely dataflow.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use mz_compute_client::logging::LoggingConfig;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, Timestamp};
use mz_timely_util::containers::{Col2ValBatcher, ColumnBuilder, ProvidedBuilder};
use mz_timely_util::replay::MzReplay;
use timely::communication::Allocate;
use timely::container::columnation::{Columnation, CopyRegion};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::pushers::{Counter, Tee};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::logging::{
    ChannelsEvent, MessagesEvent, OperatesEvent, ParkEvent, ScheduleEvent, ShutdownEvent,
    TimelyEvent,
};
use timely::Container;
use tracing::error;

use crate::extensions::arrange::MzArrange;
use crate::logging::compute::{ComputeEvent, DataflowShutdown};
use crate::logging::{consolidate_and_pack, LogCollection};
use crate::logging::{EventQueue, LogVariant, SharedLoggingState, TimelyLog};
use crate::row_spine::RowRowBuilder;
use crate::typedefs::{KeyBatcher, KeyValBatcher, RowRowSpine};

/// Constructs the logging dataflow for timely logs.
///
/// Params
/// * `worker`: The Timely worker hosting the log analysis dataflow.
/// * `config`: Logging configuration
/// * `event_queue`: The source to read log events from.
pub(super) fn construct<A: Allocate>(
    worker: &mut timely::worker::Worker<A>,
    config: &LoggingConfig,
    event_queue: EventQueue<Vec<(Duration, TimelyEvent)>>,
    shared_state: Rc<RefCell<SharedLoggingState>>,
) -> BTreeMap<LogVariant, LogCollection> {
    let logging_interval_ms = std::cmp::max(1, config.interval.as_millis());
    let worker_id = worker.index();
    let peers = worker.peers();
    let dataflow_index = worker.next_dataflow_index();

    worker.dataflow_named("Dataflow: timely logging", move |scope| {
        let enable_logging = config.enable_logging;
        let (logs, token) =
            event_queue.links.mz_replay::<_, ProvidedBuilder<_>, _>(
                scope,
                "timely logs",
                config.interval,
                event_queue.activator,
                move |mut session, mut data| {
                    // If logging is disabled, we still need to install the indexes, but we can leave them
                    // empty. We do so by immediately filtering all logs events.
                    if enable_logging {
                        session.give_container(data.to_mut())
                    }
                },
            );

        // Build a demux operator that splits the replayed event stream up into the separate
        // logging streams.
        let mut demux = OperatorBuilder::new("Timely Logging Demux".to_string(), scope.clone());
        let mut input = demux.new_input(&logs, Pipeline);
        let (mut operates_out, operates) = demux.new_output();
        let (mut channels_out, channels) = demux.new_output();
        let (mut addresses_out, addresses) = demux.new_output();
        let (mut parks_out, parks) = demux.new_output();
        let (mut messages_sent_out, messages_sent) = demux.new_output();
        let (mut messages_received_out, messages_received) = demux.new_output();
        let (mut schedules_duration_out, schedules_duration) = demux.new_output();
        let (mut schedules_histogram_out, schedules_histogram) = demux.new_output();
        let (mut batches_sent_out, batches_sent) = demux.new_output();
        let (mut batches_received_out, batches_received) = demux.new_output();

        let mut demux_state = DemuxState::default();
        demux.build(move |_capability| {
            move |_frontiers| {
                let mut operates = operates_out.activate();
                let mut channels = channels_out.activate();
                let mut addresses = addresses_out.activate();
                let mut parks = parks_out.activate();
                let mut messages_sent = messages_sent_out.activate();
                let mut messages_received = messages_received_out.activate();
                let mut batches_sent = batches_sent_out.activate();
                let mut batches_received = batches_received_out.activate();
                let mut schedules_duration = schedules_duration_out.activate();
                let mut schedules_histogram = schedules_histogram_out.activate();

                input.for_each(|cap, data| {
                    let mut output_buffers = DemuxOutput {
                        operates: operates.session_with_builder(&cap),
                        channels: channels.session_with_builder(&cap),
                        addresses: addresses.session_with_builder(&cap),
                        parks: parks.session_with_builder(&cap),
                        messages_sent: messages_sent.session_with_builder(&cap),
                        messages_received: messages_received.session_with_builder(&cap),
                        schedules_duration: schedules_duration.session_with_builder(&cap),
                        schedules_histogram: schedules_histogram.session_with_builder(&cap),
                        batches_sent: batches_sent.session_with_builder(&cap),
                        batches_received: batches_received.session_with_builder(&cap),
                    };

                    for (time, event) in data.drain(..) {
                        if let TimelyEvent::Messages(msg) = &event {
                            match msg.is_send {
                                true => assert_eq!(msg.source, worker_id),
                                false => assert_eq!(msg.target, worker_id),
                            }
                        }

                        DemuxHandler {
                            state: &mut demux_state,
                            shared_state: &mut shared_state.borrow_mut(),
                            output: &mut output_buffers,
                            logging_interval_ms,
                            peers,
                            time,
                        }
                        .handle(event);
                    }
                });
            }
        });

        // Encode the contents of each logging stream into its expected `Row` format.
        // We pre-arrange the logging streams to force a consolidation and reduce the amount of
        // updates that reach `Row` encoding.

        let operates = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &operates,
            TimelyLog::Operates,
            move |((id, name), time, diff), packer, session| {
                let data = packer.pack_slice(&[
                    Datum::UInt64(u64::cast_from(*id)),
                    Datum::UInt64(u64::cast_from(worker_id)),
                    Datum::String(name),
                ]);
                session.give((data, time, diff));
            },
        );

        let channels = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &channels,
            TimelyLog::Channels,
            move |((datum, ()), time, diff), packer, session| {
                let (source_node, source_port) = datum.source;
                let (target_node, target_port) = datum.target;
                let data = packer.pack_slice(&[
                    Datum::UInt64(u64::cast_from(datum.id)),
                    Datum::UInt64(u64::cast_from(worker_id)),
                    Datum::UInt64(u64::cast_from(source_node)),
                    Datum::UInt64(u64::cast_from(source_port)),
                    Datum::UInt64(u64::cast_from(target_node)),
                    Datum::UInt64(u64::cast_from(target_port)),
                ]);
                session.give((data, time, diff));
            },
        );

        let addresses = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &addresses,
            TimelyLog::Addresses,
            move |((id, address), time, diff), packer, session| {
                let data = packer.pack_by_index(|packer, index| match index {
                    0 => packer.push(Datum::UInt64(u64::cast_from(*id))),
                    1 => packer.push(Datum::UInt64(u64::cast_from(worker_id))),
                    2 => {
                        packer.push_list(address.iter().map(|i| Datum::UInt64(u64::cast_from(*i))))
                    }
                    _ => unreachable!("Addresses relation has three columns"),
                });
                session.give((data, time, diff));
            },
        );

        let parks = consolidate_and_pack::<_, KeyBatcher<_, _, _>, ColumnBuilder<_>, _, _>(
            &parks,
            TimelyLog::Parks,
            move |((datum, ()), time, diff), packer, session| {
                let data = packer.pack_slice(&[
                    Datum::UInt64(u64::cast_from(worker_id)),
                    Datum::UInt64(datum.duration_pow),
                    datum
                        .requested_pow
                        .map(Datum::UInt64)
                        .unwrap_or(Datum::Null),
                ]);
                session.give((data, time, diff));
            },
        );

        let batches_sent = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &batches_sent,
            TimelyLog::BatchesSent,
            move |((datum, ()), time, diff), packer, session| {
                let data = packer.pack_slice(&[
                    Datum::UInt64(u64::cast_from(datum.channel)),
                    Datum::UInt64(u64::cast_from(worker_id)),
                    Datum::UInt64(u64::cast_from(datum.worker)),
                ]);
                session.give((data, time, diff));
            },
        );

        let batches_received = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &batches_received,
            TimelyLog::BatchesReceived,
            move |((datum, ()), time, diff), packer, session| {
                let data = packer.pack_slice(&[
                    Datum::UInt64(u64::cast_from(datum.channel)),
                    Datum::UInt64(u64::cast_from(datum.worker)),
                    Datum::UInt64(u64::cast_from(worker_id)),
                ]);
                session.give((data, time, diff));
            },
        );


        let messages_sent = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &messages_sent,
            TimelyLog::MessagesSent,
            move |((datum, ()), time, diff), packer, session| {
                let data = packer.pack_slice(&[
                    Datum::UInt64(u64::cast_from(datum.channel)),
                    Datum::UInt64(u64::cast_from(worker_id)),
                    Datum::UInt64(u64::cast_from(datum.worker)),
                ]);
                session.give((data, time, diff));
            },
        );

        let messages_received = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &messages_received,
            TimelyLog::MessagesReceived,
            move |((datum, ()), time, diff), packer, session| {
                let data = packer.pack_slice(&[
                    Datum::UInt64(u64::cast_from(datum.channel)),
                    Datum::UInt64(u64::cast_from(datum.worker)),
                    Datum::UInt64(u64::cast_from(worker_id)),
                ]);
                session.give((data, time, diff));
            },
        );

        let elapsed = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &schedules_duration,
            TimelyLog::Elapsed,
            move |((operator, ()), time, diff), packer, session| {
                let data = packer.pack_slice(&[Datum::UInt64(u64::cast_from(*operator)),
                    Datum::UInt64(u64::cast_from(worker_id)),
                ]);
                session.give((data, time, diff));
            },
        );


        let histogram = consolidate_and_pack::<_, KeyValBatcher<_, _, _, _>, ColumnBuilder<_>, _, _>(
            &schedules_histogram,
            TimelyLog::Histogram,
            move |((datum, ()), time, diff), packer, session| {
                let data = packer.pack_slice(&[
                    Datum::UInt64(u64::cast_from(datum.operator)),
                    Datum::UInt64(u64::cast_from(worker_id)),
                    Datum::UInt64(datum.duration_pow),
                ]);
                session.give((data, time, diff));
            },
        );

        let logs = {
            use TimelyLog::*;
            [
                (Operates, operates),
                (Channels, channels),
                (Elapsed, elapsed),
                (Histogram, histogram),
                (Addresses, addresses),
                (Parks, parks),
                (MessagesSent, messages_sent),
                (MessagesReceived, messages_received),
                (BatchesSent, batches_sent),
                (BatchesReceived, batches_received),
            ]
        };

        // Build the output arrangements.
        let mut result = BTreeMap::new();
        for (variant, collection) in logs {
            let variant = LogVariant::Timely(variant);
            if config.index_logs.contains_key(&variant) {
                let trace = collection
                    .mz_arrange::<Col2ValBatcher<_, _, _, _>, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
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
    })
}

/// State maintained by the demux operator.
#[derive(Default)]
struct DemuxState {
    /// Information about live operators, indexed by operator ID.
    operators: BTreeMap<usize, OperatesEvent>,
    /// Maps dataflow IDs to channels in the dataflow.
    dataflow_channels: BTreeMap<usize, Vec<ChannelsEvent>>,
    /// Information about the last requested park.
    last_park: Option<Park>,
    /// Maps channel IDs to boxed slices counting the messages sent to each target worker.
    messages_sent: BTreeMap<usize, Box<[MessageCount]>>,
    /// Maps channel IDs to boxed slices counting the messages received from each source worker.
    messages_received: BTreeMap<usize, Box<[MessageCount]>>,
    /// Stores for scheduled operators the time when they were scheduled.
    schedule_starts: BTreeMap<usize, Duration>,
    /// Maps operator IDs to a vector recording the (count, elapsed_ns) values in each histogram
    /// bucket.
    schedules_data: BTreeMap<usize, Vec<(isize, i64)>>,
}

struct Park {
    /// Time when the park occurred.
    time: Duration,
    /// Requested park time.
    requested: Option<Duration>,
}

/// Organize message counts into number of batches and records.
#[derive(Default, Copy, Clone, Debug)]
struct MessageCount {
    /// The number of batches sent across a channel.
    batches: i64,
    /// The number of records sent across a channel.
    records: i64,
}

type Pusher<D> =
    Counter<Timestamp, Vec<(D, Timestamp, Diff)>, Tee<Timestamp, Vec<(D, Timestamp, Diff)>>>;
type OutputSession<'a, D> =
    Session<'a, Timestamp, ConsolidatingContainerBuilder<Vec<(D, Timestamp, Diff)>>, Pusher<D>>;

/// Bundled output buffers used by the demux operator.
//
// We use tuples rather than dedicated `*Datum` structs for `operates` and `addresses` to avoid
// having to manually implement `Columnation`. If `Columnation` could be `#[derive]`ed, that
// wouldn't be an issue.
struct DemuxOutput<'a> {
    operates: OutputSession<'a, (usize, String)>,
    channels: OutputSession<'a, (ChannelDatum, ())>,
    addresses: OutputSession<'a, (usize, Vec<usize>)>,
    parks: OutputSession<'a, (ParkDatum, ())>,
    batches_sent: OutputSession<'a, (MessageDatum, ())>,
    batches_received: OutputSession<'a, (MessageDatum, ())>,
    messages_sent: OutputSession<'a, (MessageDatum, ())>,
    messages_received: OutputSession<'a, (MessageDatum, ())>,
    schedules_duration: OutputSession<'a, (usize, ())>,
    schedules_histogram: OutputSession<'a, (ScheduleHistogramDatum, ())>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ChannelDatum {
    id: usize,
    source: (usize, usize),
    target: (usize, usize),
}

impl Columnation for ChannelDatum {
    type InnerRegion = CopyRegion<Self>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ParkDatum {
    duration_pow: u64,
    requested_pow: Option<u64>,
}

impl Columnation for ParkDatum {
    type InnerRegion = CopyRegion<Self>;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MessageDatum {
    channel: usize,
    worker: usize,
}

impl Columnation for MessageDatum {
    type InnerRegion = CopyRegion<Self>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ScheduleHistogramDatum {
    operator: usize,
    duration_pow: u64,
}

impl Columnation for ScheduleHistogramDatum {
    type InnerRegion = CopyRegion<Self>;
}

/// Event handler of the demux operator.
struct DemuxHandler<'a, 'b> {
    /// State kept by the demux operator.
    state: &'a mut DemuxState,
    /// State shared across log receivers.
    shared_state: &'a mut SharedLoggingState,
    /// Demux output buffers.
    output: &'a mut DemuxOutput<'b>,
    /// The logging interval specifying the time granularity for the updates.
    logging_interval_ms: u128,
    /// The number of timely workers.
    peers: usize,
    /// The current event time.
    time: Duration,
}

impl DemuxHandler<'_, '_> {
    /// Return the timestamp associated with the current event, based on the event time and the
    /// logging interval.
    fn ts(&self) -> Timestamp {
        let time_ms = self.time.as_millis();
        let interval = self.logging_interval_ms;
        let rounded = (time_ms / interval + 1) * interval;
        rounded.try_into().expect("must fit")
    }

    /// Handle the given timely event.
    fn handle(&mut self, event: TimelyEvent) {
        use TimelyEvent::*;

        match event {
            Operates(e) => self.handle_operates(e),
            Channels(e) => self.handle_channels(e),
            Shutdown(e) => self.handle_shutdown(e),
            Park(e) => self.handle_park(e),
            Messages(e) => self.handle_messages(e),
            Schedule(e) => self.handle_schedule(e),
            _ => (),
        }
    }

    fn handle_operates(&mut self, event: OperatesEvent) {
        let ts = self.ts();
        let datum = (event.id, event.name.clone());
        self.output.operates.give((datum, ts, 1));

        let datum = (event.id, event.addr.clone());
        self.output.addresses.give((datum, ts, 1));

        self.state.operators.insert(event.id, event);
    }

    fn handle_channels(&mut self, event: ChannelsEvent) {
        let ts = self.ts();
        let datum = ChannelDatum {
            id: event.id,
            source: event.source,
            target: event.target,
        };
        self.output.channels.give(((datum, ()), ts, 1));

        let datum = (event.id, event.scope_addr.clone());
        self.output.addresses.give((datum, ts, 1));

        let dataflow_index = event.scope_addr[0];
        self.state
            .dataflow_channels
            .entry(dataflow_index)
            .or_default()
            .push(event);
    }

    fn handle_shutdown(&mut self, event: ShutdownEvent) {
        // Dropped operators should result in a negative record for
        // the `operates` collection, cancelling out the initial
        // operator announcement.
        // Remove logging for this operator.

        let Some(operator) = self.state.operators.remove(&event.id) else {
            error!(operator_id = ?event.id, "missing operator entry at time of shutdown");
            return;
        };

        // Retract operator information.
        let ts = self.ts();
        let datum = (operator.id, operator.name);
        self.output.operates.give((datum, ts, -1));

        // Retract schedules information for the operator
        if let Some(schedules) = self.state.schedules_data.remove(&event.id) {
            for (bucket, (count, elapsed_ns)) in IntoIterator::into_iter(schedules)
                .enumerate()
                .filter(|(_, (count, _))| *count != 0)
            {
                self.output
                    .schedules_duration
                    .give(((event.id, ()), ts, -elapsed_ns));

                let datum = ScheduleHistogramDatum {
                    operator: event.id,
                    duration_pow: 1 << bucket,
                };
                let diff = Diff::cast_from(-count);
                self.output
                    .schedules_histogram
                    .give(((datum, ()), ts, diff));
            }
        }

        if operator.addr.len() == 1 {
            let dataflow_index = operator.addr[0];
            self.handle_dataflow_shutdown(dataflow_index);
        }

        let datum = (operator.id, operator.addr);
        self.output.addresses.give((datum, ts, -1));
    }

    fn handle_dataflow_shutdown(&mut self, dataflow_index: usize) {
        // Notify compute logging about the shutdown.
        if let Some(logger) = &self.shared_state.compute_logger {
            logger.log(&ComputeEvent::DataflowShutdown(DataflowShutdown {
                dataflow_index,
            }));
        }

        // When a dataflow shuts down, we need to retract all its channels.
        let Some(channels) = self.state.dataflow_channels.remove(&dataflow_index) else {
            return;
        };

        let ts = self.ts();
        for channel in channels {
            // Retract channel description.
            let datum = ChannelDatum {
                id: channel.id,
                source: channel.source,
                target: channel.target,
            };
            self.output.channels.give(((datum, ()), ts, -1));

            let datum = (channel.id, channel.scope_addr);
            self.output.addresses.give((datum, ts, -1));

            // Retract messages logged for this channel.
            if let Some(sent) = self.state.messages_sent.remove(&channel.id) {
                for (target_worker, count) in sent.iter().enumerate() {
                    let datum = MessageDatum {
                        channel: channel.id,
                        worker: target_worker,
                    };
                    self.output
                        .messages_sent
                        .give(((datum, ()), ts, -count.records));
                    self.output
                        .batches_sent
                        .give(((datum, ()), ts, -count.batches));
                }
            }
            if let Some(received) = self.state.messages_received.remove(&channel.id) {
                for (source_worker, count) in received.iter().enumerate() {
                    let datum = MessageDatum {
                        channel: channel.id,
                        worker: source_worker,
                    };
                    self.output
                        .messages_received
                        .give(((datum, ()), ts, -count.records));
                    self.output
                        .batches_received
                        .give(((datum, ()), ts, -count.batches));
                }
            }
        }
    }

    fn handle_park(&mut self, event: ParkEvent) {
        match event {
            ParkEvent::Park(requested) => {
                let park = Park {
                    time: self.time,
                    requested,
                };
                let existing = self.state.last_park.replace(park);
                if existing.is_some() {
                    error!("park without a succeeding unpark");
                }
            }
            ParkEvent::Unpark => {
                let Some(park) = self.state.last_park.take() else {
                    error!("unpark without a preceding park");
                    return;
                };

                let duration_ns = self.time.saturating_sub(park.time).as_nanos();
                let duration_pow =
                    u64::try_from(duration_ns.next_power_of_two()).expect("must fit");
                let requested_pow = park
                    .requested
                    .map(|r| u64::try_from(r.as_nanos().next_power_of_two()).expect("must fit"));

                let ts = self.ts();
                let datum = ParkDatum {
                    duration_pow,
                    requested_pow,
                };
                self.output.parks.give(((datum, ()), ts, 1));
            }
        }
    }

    fn handle_messages(&mut self, event: MessagesEvent) {
        let ts = self.ts();
        let count = Diff::try_from(event.length).expect("must fit");

        if event.is_send {
            let datum = MessageDatum {
                channel: event.channel,
                worker: event.target,
            };
            self.output.messages_sent.give(((datum, ()), ts, count));
            self.output.batches_sent.give(((datum, ()), ts, 1));

            let sent_counts = self
                .state
                .messages_sent
                .entry(event.channel)
                .or_insert_with(|| vec![Default::default(); self.peers].into_boxed_slice());
            sent_counts[event.target].records += count;
            sent_counts[event.target].batches += 1;
        } else {
            let datum = MessageDatum {
                channel: event.channel,
                worker: event.source,
            };
            self.output.messages_received.give(((datum, ()), ts, count));
            self.output.batches_received.give(((datum, ()), ts, 1));

            let received_counts = self
                .state
                .messages_received
                .entry(event.channel)
                .or_insert_with(|| vec![Default::default(); self.peers].into_boxed_slice());
            received_counts[event.source].records += count;
            received_counts[event.source].batches += 1;
        }
    }

    fn handle_schedule(&mut self, event: ScheduleEvent) {
        match event.start_stop {
            timely::logging::StartStop::Start => {
                let existing = self.state.schedule_starts.insert(event.id, self.time);
                if existing.is_some() {
                    error!(operator_id = ?event.id, "schedule start without succeeding stop");
                }
            }
            timely::logging::StartStop::Stop => {
                let Some(start_time) = self.state.schedule_starts.remove(&event.id) else {
                    error!(operator_id = ?event.id, "schedule stop without preceeding start");
                    return;
                };

                let elapsed_ns = self.time.saturating_sub(start_time).as_nanos();
                let elapsed_diff = Diff::try_from(elapsed_ns).expect("must fit");
                let elapsed_pow = u64::try_from(elapsed_ns.next_power_of_two()).expect("must fit");

                let ts = self.ts();
                let datum = event.id;
                self.output
                    .schedules_duration
                    .give(((datum, ()), ts, elapsed_diff));

                let datum = ScheduleHistogramDatum {
                    operator: event.id,
                    duration_pow: elapsed_pow,
                };
                self.output.schedules_histogram.give(((datum, ()), ts, 1));

                // Record count and elapsed time for later retraction.
                let index = usize::cast_from(elapsed_pow.trailing_zeros());
                let data = self.state.schedules_data.entry(event.id).or_default();
                grow_vec(data, index);
                let (count, duration) = &mut data[index];
                *count += 1;
                *duration += elapsed_diff;
            }
        }
    }
}

/// Grow the given vector so it fits the given index.
///
/// This does nothing if the vector is already large enough.
fn grow_vec<T>(vec: &mut Vec<T>, index: usize)
where
    T: Clone + Default,
{
    if vec.len() <= index {
        vec.resize(index + 1, Default::default());
    }
}
