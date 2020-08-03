// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// The V2 CDC representation is an unordered stream of a mix of "update" and "progress" messages.
// Each "update" statement indicates a triple `(Row, Time, Diff)` that is certain to occur.
// Each "progress" statement identifies the number of distinct update statements at each `Time` that lies between a specified `lower` and `upper` frontier.
//
// The structure of the sink is that each worker writes its received updates to a stream as updates, and transmits the number at each time to a downstream operator.
// The downstream operator integrates this information and uses its input frontier to make immutable statements about the counts of records at timestamps in intervals.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A message in the CDC V2 protocol.
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
enum Message<D, T, R> {
    /// A batch of updates that are certain to occur.
    ///
    /// Each triple is an irrevocable statement about a change that occurs.
    /// Each statement contains a datum, a time, and a difference, and asserts
    /// that the multiplicity of the datum changes at the time by the difference.
    Updates(Vec<(D, T, R)>),
    /// An irrevocable statement about the number of updates within a time interval.
    Progress(Progress<T>),
}

/// An irrevocable statement about the number of updates at times within an interval.
///
/// This statement covers all times beyond `lower` and not beyond `upper`.
/// Each element of `counts` is an irrevocable statement about the exact number of
/// distinct updates that occur at that time.
/// Times not present in `counts` have a count of zero.
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
struct Progress<T> {
    /// The lower bound of times contained in this statement.
    pub lower: Vec<T>,
    /// The upper bound of times contained in this statement.
    pub upper: Vec<T>,
    /// All non-zero counts for times beyond `lower` and not beyond `upper`.
    pub counts: Vec<(T, usize)>,
}

/// A simple source of byte slices.
pub trait BytesSource {
    /// Returns either bytes, or indicates transient unavailability.
    fn poll(&mut self) -> Option<&[u8]>;
}

/// A simple sink for byte slices.
pub trait BytesSink {
    /// Returns an amount of time to wait before retrying, or `None` for success.
    fn poll(&mut self, bytes: &[u8]) -> Option<Duration>;
    /// Indicates if the sink has committed all sent data and can be safely dropped.
    fn done(&self) -> bool;
}

/// A deduplicating, re-ordering iterator.
pub mod iterator {

    use super::{Message, Progress};
    use differential_dataflow::lattice::Lattice;
    use std::hash::Hash;
    use timely::progress::{
        frontier::{AntichainRef, MutableAntichain},
        Antichain,
    };

    /// A direct implementation of a deduplicating, re-ordering iterator.
    ///
    /// The iterator draws from a source that may have arbitrary duplication, be arbitrarily out of order,
    /// and yet produces each update once, with in-order batches. The iterator maintains a bounded memory
    /// footprint, proportional to the mismatch between the received updates and progress messages.
    struct Ordered<I, D, T, R>
    where
        I: Iterator<Item = Message<D, T, R>>,
        T: Hash + Ord + Lattice + Clone,
        D: Hash + Eq,
        T: Hash + Eq,
        R: Hash + Eq,
    {
        /// Source of potentially duplicated, out of order cdc_v2 messages.
        iterator: I,
        /// Updates that have been received, but are still beyond `reported_frontier`.
        ///
        /// These updates are retained both so that they can eventually be transmitted,
        /// but also so that they can deduplicate updates that may still be received.
        updates: std::collections::HashSet<(D, T, R)>,
        /// Frontier through which the iterator has reported updates.
        ///
        /// All updates not beyond this frontier have been reported.
        /// Any information related to times not beyond this frontier can be discarded.
        ///
        /// This frontier tracks the meet of `progress_frontier` and `messages_frontier`,
        /// our two bounds on potential uncertainty in progress and update messages.
        reported_frontier: Antichain<T>,
        /// Frontier of accepted progress statements.
        ///
        /// All progress message counts for times not beyond this frontier have been
        /// incorporated in to `messages_frontier`. This frontier also guides which
        /// received progress statements can be incorporated: those whose for which
        /// this frontier is beyond their lower bound.
        progress_frontier: Antichain<T>,
        /// Counts of outstanding messages at times.
        ///
        /// These counts track the difference between message counts at times announced
        /// by progress messages, and message counts at times received in distinct updates.
        messages_frontier: MutableAntichain<T>,
        /// Progress statements that are not yet actionable due to out-of-orderedness.
        ///
        /// A progress statement becomes actionable once the progress frontier is beyond
        /// its lower frontier. This ensures that the [0, lower) interval is already
        /// incorporated, and that we will not leave a gap by incorporating the counts
        /// and reflecting the progress statement's upper frontier.
        progress_queue: Vec<Progress<T>>,
    }

    impl<D, T, R, I> Iterator for Ordered<I, D, T, R>
    where
        I: Iterator<Item = Message<D, T, R>>,
        T: Hash + Ord + Lattice + Clone,
        D: Hash + Eq + Clone,
        R: Hash + Eq + Clone,
    {
        type Item = (Vec<(D, T, R)>, Antichain<T>);
        fn next(&mut self) -> Option<Self::Item> {
            // Each call to `next` should return some newly carved interval of time.
            // As such, we should read from our source until we find such a thing.
            //
            // An interval can be completed once our frontier of received progress
            // information and our frontier of unresolved counts have advanced.
            while let Some(message) = self.iterator.next() {
                match message {
                    Message::Updates(mut updates) => {
                        // Discard updates at reported times, or duplicates at unreported times.
                        updates.retain(|dtr| {
                            self.reported_frontier.less_equal(&dtr.1) && !self.updates.contains(dtr)
                        });
                        // Decrement our counts of accounted-for messages.
                        self.messages_frontier
                            .update_iter(updates.iter().map(|(_, t, _)| (t.clone(), -1)));
                        // Record the messages in our de-duplication collection.
                        self.updates.extend(updates.into_iter());
                    }
                    Message::Progress(progress) => {
                        // A progress statement may not be immediately actionable.
                        self.progress_queue.push(progress);
                    }
                }

                // Attempt to drain actionable progress messages.
                // A progress message is actionable if `self.progress_frontier` is greater or
                // equal to the message's lower bound.
                while let Some(position) = self.progress_queue.iter().position(|p| {
                    <_ as timely::order::PartialOrder>::less_equal(
                        &AntichainRef::new(&p.lower),
                        &self.progress_frontier.borrow(),
                    )
                }) {
                    let mut progress = self.progress_queue.remove(position);
                    // Discard counts that have already been incorporated.
                    progress
                        .counts
                        .retain(|(time, _count)| self.progress_frontier.less_equal(time));
                    // Record any new reports of expected counts.
                    self.messages_frontier
                        .update_iter(progress.counts.drain(..).map(|(t, c)| (t, c as i64)));
                    // Extend the frontier to be times greater or equal to both progress.upper and self.progress_frontier.
                    let mut new_frontier = timely::progress::Antichain::new();
                    for time1 in progress.upper {
                        for time2 in self.progress_frontier.elements() {
                            new_frontier.insert(time1.join(time2));
                        }
                    }
                    self.progress_queue.retain(|p| {
                        !<_ as timely::order::PartialOrder>::less_equal(
                            &AntichainRef::new(&p.upper),
                            &new_frontier.borrow(),
                        )
                    });
                    self.progress_frontier = new_frontier;
                }

                // Now check and see if our lower bound exceeds `self.reported_frontier`.
                let mut lower_bound = self.progress_frontier.clone();
                lower_bound.extend(self.messages_frontier.frontier().iter().cloned());
                if lower_bound != self.reported_frontier {
                    let to_publish = self
                        .updates
                        .iter()
                        .filter(|(_, t, _)| !lower_bound.less_equal(t))
                        .cloned()
                        .collect::<Vec<_>>();
                    self.updates.retain(|(_, t, _)| lower_bound.less_equal(t));
                    self.reported_frontier = lower_bound.clone();
                    return Some((to_publish, lower_bound));
                }
            }
            None
        }
    }

    impl<D, T, R, I> Ordered<I, D, T, R>
    where
        I: Iterator<Item = Message<D, T, R>>,
        T: Hash + Ord + Lattice + Clone + timely::progress::Timestamp,
        D: Hash + Eq + Clone,
        R: Hash + Eq + Clone,
    {
        pub fn new(iterator: I) -> Self {
            Self {
                iterator,
                updates: std::collections::HashSet::new(),
                reported_frontier: Antichain::from_elem(T::minimum()),
                progress_frontier: Antichain::from_elem(T::minimum()),
                messages_frontier: MutableAntichain::new(),
                progress_queue: Vec::new(),
            }
        }
    }

    #[test]
    fn test_order() {
        let mut updates = (0..10)
            .map(|i| Message::Updates(vec![((), i, 0)]))
            .collect::<Vec<_>>();
        let mut progress = (0..10)
            .map(|i| {
                Message::Progress(Progress {
                    lower: vec![i],
                    upper: vec![i + 1],
                    counts: vec![(i, 1)],
                })
            })
            .collect::<Vec<_>>();

        let mut rng = rand::thread_rng();

        let mut to_send = Vec::new();
        for _ in 0..10 {
            to_send.extend(updates.iter().cloned());
            to_send.extend(progress.iter().cloned());
        }
        use rand::seq::SliceRandom;
        (&mut to_send[..]).shuffle(&mut rng);

        let mut ordered = Ordered::new(to_send.into_iter());

        for (u, p) in ordered {
            println!("U: {:?}", u);
            println!("P: {:?}", p);
        }
    }
}

/// Methods for recovering update streams from binary bundles.
pub mod source {

    use super::{BytesSource, Message, Progress};
    use dataflow_types::Timestamp as Time;
    use expr::Diff;
    use expr::GlobalId;
    use repr::Row;
    use std::cell::RefCell;
    use std::rc::{Rc, Weak};
    use timely::dataflow::operators::Capability;
    use timely::dataflow::{Scope, Stream};

    pub fn build<G, I>(
        scope: G,
        source: I,
    ) -> (Rc<RefCell<Capability<Time>>>, Stream<G, (Row, Time, Diff)>)
    where
        G: Scope<Timestamp = Time>,
        I: Iterator<Item = Message<Row, Time, Diff>>,
    {
        // Read messages are either updates or progress messages.
        // Each may contain duplicates, and we must take care to deduplicate information before introducing it to an accumulation.
        // This includes both emitting updates, and setting expectations for update counts.

        // Updates need to be deduplicated by (data, time), and we should exchange them by such.
        // Progress needs to be deduplicated by time, and we should exchange them by such.

        // The first cut of this is a dataflow graph that looks like (flowing downward)
        //
        // 1. MESSAGES:
        //      Reads `Message` stream; maintains capabilities.
        //      Sends `Updates` to UPDATES stage by hash((data, time, diff)).
        //      Sends `Progress` to PROGRESS stage by hash(time), each with lower, upper bounds.
        //      Shares capabilities with downstream operator.
        // 2. UPDATES:
        //      Maintains and deduplicates updates.
        //      Ships updates once frontier advances.
        //      Ships counts to PROGRESS stage, by hash(time).
        // 3. PROGRESS:
        //      Maintains outstanding message counts by time. Tracks frontiers.
        //      Tracks lower bounds of messages and progress frontier. Broadcasts changes to FEEDBACK stage
        // 4. FEEDBACK:
        //      Shares capabilities with MESSAGES; downgrades to track input from PROGRESS.
        //
        // Each of these stages can be arbitrarily data-parallel, and FEEDBACK *must* have the same parallelism as RAW.
        // Limitations: MESSAGES must broadcast lower and upper bounds to PROGRESS and PROGRESS must broadcast its changes
        // to FEEDBACK. This may mean that scaling up PROGRESS could introduce quadratic problems. Though, both of these
        // broadcast things are meant to be very reduced data.

        use differential_dataflow::hashable::Hashable;
        use timely::dataflow::channels::pact::Exchange;
        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
        use timely::progress::frontier::MutableAntichain;
        use timely::progress::timestamp::Timestamp;
        use timely::progress::ChangeBatch;

        // Some message distribution logic depends on the number of workers.
        let workers = scope.peers();
        let mut shared_capability: Option<Rc<RefCell<Capability<Time>>>> = None;

        // Step 1: The MESSAGES operator.
        let mut messages_op = OperatorBuilder::new("CDCV2_Messages".to_string(), scope.clone());
        let (mut updates_out, updates) = messages_op.new_output();
        let (mut progress_out, progress) = messages_op.new_output();
        messages_op.build(move |capability| {
            // Read messages from some source; shuffle them to UPDATES and PROGRESS; share capability with FEEDBACK.
            shared_capability = Some(Rc::new(RefCell::new(capability)));
            let local_capability = Rc::downgrade(shared_capability.as_ref().unwrap());
            move |frontiers| {
                if let Some(capability) = local_capability.upgrade() {
                    let capability = &*capability.borrow();

                    let mut updates = updates_out.activate();
                    let mut progress = progress_out.activate();

                    let mut updates_session = updates.session(&capability);
                    let mut progress_session = progress.session(&capability);

                    // We presume the iterator will yield if appropriate.
                    while let Some(message) = source.next() {
                        match message {
                            Message::Updates(updates) => {
                                updates_session.give_vec(&mut updates);
                            }
                            Message::Progress(progress) => {
                                // Need to send a copy of each progress message to all workers,
                                // but can partition the counts across the workers by timestamp.
                                let to_worker = vec![Vec::new(); workers];
                                for (time, count) in progress.counts {
                                    to_worker[(time.hashed() as usize) % workers].push((time, count));
                                }
                                for (worker, counts) in to_worker.into_iter().enumerate() {
                                    progress_session.give((
                                        worker,
                                        Progress {
                                            lower: progress.lower.clone(),
                                            upper: progress.upper.clone(),
                                            counts,
                                        },
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        });

        // Step 2: The UPDATES operator.
        let mut updates_op = OperatorBuilder::new("CDCV2_Updates".to_string(), scope.clone());
        let mut input =
            updates_op.new_input(&updates, Exchange::new(|x: &(Row, Time, Diff)| x.hashed()));
        let (mut changes_out, changes) = updates_op.new_output();
        let (mut counts_out, counts) = updates_op.new_output();
        updates_op.build(move |_capability| {
            // Receive updates and deduplicate them. Ship and drop updates when input frontier advances.
            move |frontiers| {
                let mut changes = changes_out.activate();
                let mut counts = counts_out.activate();
                unimplemented!()
            }
        });

        // Step 3: The PROGRESS operator.
        let mut progress_op = OperatorBuilder::new("CDCV2_Progress".to_string(), scope.clone());
        let mut input =
            progress_op.new_input(&progress, Exchange::new(|x: &(usize, Progress<Time>)| x.0 as u64));
        let mut counts =
            progress_op.new_input(&counts, Exchange::new(|x: &(Time, usize)| (x.0).hashed()));
        let (mut frontier_out, frontier) = progress_op.new_output();
        progress_op.build(move |_capability| {
            // Receive progress statements, deduplicated counts. Track lower frontier of both and broadcast changes.
            move |frontiers| {
                let mut frontier = frontier_out.activate();
                unimplemented!()
            }
        });

        // Step 4: The FEEDBACK operator.
        let mut feedback_op = OperatorBuilder::new("CDCV2_Feedback".to_string(), scope.clone());
        let mut input =
            feedback_op.new_input(&frontier, Exchange::new(|x: &(u64, ChangeBatch<Time>)| x.0));
        feedback_op.build(move |_capability| {
            let mut resolved = MutableAntichain::new();
            resolved.update_iter(Some((Time::minimum(), workers as i64)));
            // Receive frontier changes and forcibly update capabilities shared with MESSAGES.
            move |frontiers| unimplemented!()
        });

        (shared_capability.unwrap(), changes)
    }
}

/// Methods for recording update streams to binary bundles.
pub mod sink {

    use super::{BytesSink, Message, Progress};
    use dataflow_types::Timestamp as TStamp;
    use expr::Diff;
    use expr::GlobalId;
    use repr::Row;
    use std::cell::RefCell;
    use std::rc::Weak;
    use timely::dataflow::operators::generic::operator::Operator;
    use timely::dataflow::{Scope, Stream};
    use timely::progress::Antichain;
    use timely::progress::ChangeBatch;

    pub fn build<G, BS>(
        stream: &Stream<G, (Row, TStamp, Diff)>,
        id: GlobalId,
        updates_sink: Weak<RefCell<BS>>,
        progress_sink: Weak<RefCell<BS>>,
    ) where
        G: Scope<Timestamp = TStamp>,
        BS: BytesSink + 'static,
    {
        // First we record the updates that stream in.
        // We can simply record all updates, under the presumption that the have been consolidated and so any record we see is in fact guaranteed to happen.
        let updates = stream.unary(
            timely::dataflow::channels::pact::Pipeline,
            "UpdateWriter",
            move |_cap, _info| {
                // Track the number of updates at each timestamp.
                let mut timestamps: ChangeBatch<TStamp> = timely::progress::ChangeBatch::new();
                let mut bytes_queue = std::collections::VecDeque::new();
                move |input, output| {
                    // We want to drain inputs always...
                    input.for_each(|capability, updates| {
                        // Write each update out, and record the timestamp.
                        for (_row, time, _diff) in updates.iter() {
                            timestamps.update(time.clone(), 1);
                        }

                        // Now record the update to the writer.
                        let message = Message::Updates(updates.replace(Vec::new()));
                        let bytes = bincode::serialize(&message).unwrap();
                        bytes_queue.push_back(bytes);

                        // Transmit timestamp counts downstream.
                        output
                            .session(&capability)
                            .give_iterator(timestamps.drain());
                    });

                    // Drain whatever we can from the queue of bytes to send.
                    // ... but needn't do anything more if our sink is closed.
                    if let Some(sink) = updates_sink.upgrade() {
                        let mut sink = sink.borrow_mut();
                        while let Some(bytes) = bytes_queue.front() {
                            if let Some(duration) = sink.poll(&bytes) {
                                // TODO(frank): break out of loop; reschedule in `duration` time.
                            } else {
                                bytes_queue.pop_front();
                            }
                        }
                    } else {
                        bytes_queue.clear();
                    }
                }
            },
        );

        // We now record the numbers of updates at each timestamp between lower and upper bounds.
        use differential_dataflow::hashable::Hashable;
        let sink_hash = id.hashed();
        // Track the advancing frontier, to know when to produce utterances.
        use timely::progress::timestamp::Timestamp;
        let mut frontier: Antichain<TStamp> =
            timely::progress::Antichain::from_elem(TStamp::minimum());
        // Track accumulated counts for timestamps.
        let mut timestamps = timely::progress::ChangeBatch::new();
        // Stash for serialized data yet to send.
        let mut bytes_queue = std::collections::VecDeque::new();
        let mut retain = Vec::new();
        updates.sink(
            timely::dataflow::channels::pact::Exchange::new(move |_| sink_hash),
            "ProgressWriter",
            move |input| {
                // We want to drain inputs no matter what.
                // We could do this after the next step, as we are certain these timestamps will
                // not be part of a closed frontier (as they have not yet been read). This has the
                // potential to make things speedier as we scan less and keep a smaller footprint.
                input.for_each(|_capability, counts| {
                    timestamps.extend(counts.iter().cloned());
                });

                if let Some(sink) = progress_sink.upgrade() {
                    let mut sink = sink.borrow_mut();

                    // If our frontier advances strictly, we have the opportunity to issue a progress statement.
                    if <_ as timely::order::PartialOrder>::less_than(
                        &frontier.borrow(),
                        &input.frontier.frontier(),
                    ) {
                        let new_frontier = input.frontier.frontier();

                        // Extract the timestamp counts to announce.
                        let mut announce = Vec::new();
                        for (time, count) in timestamps.drain() {
                            if !new_frontier.less_equal(&time) {
                                announce.push((time, count as usize));
                            } else {
                                retain.push((time, count));
                            }
                        }
                        timestamps.extend(retain.drain(..));

                        // Announce the lower bound, upper bound, and timestamp counts.
                        let progress = Progress {
                            lower: frontier.elements().to_vec(),
                            upper: new_frontier.to_vec(),
                            counts: announce,
                        };
                        let message = Message::<Row, _, Diff>::Progress(progress);
                        let bytes = bincode::serialize(&message).unwrap();
                        bytes_queue.push_back(bytes);

                        // Advance our frontier to track our progress utterance.
                        frontier = input.frontier.frontier().to_owned();

                        while let Some(bytes) = bytes_queue.front() {
                            if let Some(duration) = sink.poll(&bytes) {
                                // TODO(frank): break out of loop; reschedule in `duration` time.
                            } else {
                                bytes_queue.pop_front();
                            }
                        }
                    }
                } else {
                    timestamps.clear();
                    bytes_queue.clear();
                }
            },
        )
    }
}

// A sink wrapped around a Kafka producer.
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::producer::DefaultProducerContext;
use rdkafka::producer::{BaseRecord, ThreadedProducer};

pub struct KafkaSink {
    topic: String,
    producer: ThreadedProducer<DefaultProducerContext>,
}

impl KafkaSink {
    pub fn new(addr: &str, topic: &str) -> Self {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &addr);
        config.set("queue.buffering.max.kbytes", &format!("{}", 16 << 20));
        config.set("queue.buffering.max.messages", &format!("{}", 10_000_000));
        config.set("queue.buffering.max.ms", &format!("{}", 10));
        let producer = config
            .create_with_context::<_, ThreadedProducer<_>>(DefaultProducerContext)
            .expect("creating kafka producer for kafka sinks failed");
        Self {
            producer,
            topic: topic.to_string(),
        }
    }
}

impl BytesSink for KafkaSink {
    fn poll(&mut self, bytes: &[u8]) -> Option<Duration> {
        let record = BaseRecord::<[u8], _>::to(&self.topic).payload(bytes);

        self.producer.send(record).err().map(|(e, _)| {
            if let KafkaError::MessageProduction(RDKafkaError::QueueFull) = e {
                Duration::from_secs(1)
            } else {
                // TODO(frank): report this error upwards so the user knows the sink is dead.
                Duration::from_secs(1)
            }
        })
    }
    fn done(&self) -> bool {
        self.producer.in_flight_count() == 0
    }
}
