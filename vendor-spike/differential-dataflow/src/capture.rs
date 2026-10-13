//! Logic related to capture and replay of differential collections.
//!
//! This module defines a protocol for capturing and replaying differential collections
//! to streaming storage that may both duplicate and reorder messages. It records facts
//! about the collection that once true stay true, such as the exact changes data undergo
//! at each time, and the number of distinct updates at each time.
//!
//! The methods are parameterized by implementors of byte sources and byte sinks. For
//! example implementations of these traits, consult the commented text at the end of
//! this file.

use std::time::Duration;
use serde::{Deserialize, Serialize};

/// A message in the CDC V2 protocol.
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Message<D, T, R> {
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
pub struct Progress<T> {
    /// The lower bound of times contained in this statement.
    pub lower: Vec<T>,
    /// The upper bound of times contained in this statement.
    pub upper: Vec<T>,
    /// All non-zero counts for times beyond `lower` and not beyond `upper`.
    pub counts: Vec<(T, usize)>,
}

/// A simple sink for byte slices.
pub trait Writer<T> {
    /// Returns an amount of time to wait before retrying, or `None` for success.
    fn poll(&mut self, item: &T) -> Option<Duration>;
    /// Indicates if the sink has committed all sent data and can be safely dropped.
    fn done(&self) -> bool;
}

/// A deduplicating, re-ordering iterator.
pub mod iterator {

    use super::{Message, Progress};
    use crate::lattice::Lattice;
    use std::hash::Hash;
    use timely::order::PartialOrder;
    use timely::progress::{
        frontier::{AntichainRef, MutableAntichain},
        Antichain,
        Timestamp,
    };

    /// A direct implementation of a deduplicating, re-ordering iterator.
    ///
    /// The iterator draws from a source that may have arbitrary duplication, be arbitrarily out of order,
    /// and yet produces each update once, with in-order batches. The iterator maintains a bounded memory
    /// footprint, proportional to the mismatch between the received updates and progress messages.
    pub struct Iter<I, D, T, R>
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
        /// Progress statements that are not yet actionable due to out-of-Iterness.
        ///
        /// A progress statement becomes actionable once the progress frontier is beyond
        /// its lower frontier. This ensures that the [0, lower) interval is already
        /// incorporated, and that we will not leave a gap by incorporating the counts
        /// and reflecting the progress statement's upper frontier.
        progress_queue: Vec<Progress<T>>,
    }

    impl<D, T, R, I> Iterator for Iter<I, D, T, R>
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
                    <_ as PartialOrder>::less_equal(
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
                    let mut new_frontier = Antichain::new();
                    for time1 in progress.upper {
                        for time2 in self.progress_frontier.elements() {
                            new_frontier.insert(time1.join(time2));
                        }
                    }
                    self.progress_queue.retain(|p| {
                        !<_ as PartialOrder>::less_equal(
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

    impl<D, T, R, I> Iter<I, D, T, R>
    where
        I: Iterator<Item = Message<D, T, R>>,
        T: Hash + Ord + Lattice + Clone + Timestamp,
        D: Hash + Eq + Clone,
        R: Hash + Eq + Clone,
    {
        /// Construct a new re-ordering, deduplicating iterator.
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
}

/// Methods for recovering update streams from binary bundles.
pub mod source {

    use super::{Message, Progress};
    use crate::{lattice::Lattice, ExchangeData};
    use std::cell::RefCell;
    use std::hash::Hash;
    use std::rc::Rc;
    use std::marker::{Send, Sync};
    use std::sync::Arc;
    use timely::dataflow::{Scope, Stream, operators::{Capability, CapabilitySet}};
    use timely::dataflow::operators::generic::OutputBuilder;
    use timely::progress::Timestamp;
    use timely::scheduling::SyncActivator;

    // TODO(guswynn): implement this generally in timely
    struct DropActivator {
        activator: Arc<SyncActivator>,
    }

    impl Drop for DropActivator {
        fn drop(&mut self) {
            // Best effort: failure to activate
            // is ignored
            let _ = self.activator.activate();
        }
    }

    /// Constructs a stream of updates from a source of messages.
    ///
    /// The stream is built in the supplied `scope` and continues to run until
    /// the returned `Box<Any>` token is dropped. The `source_builder` argument
    /// is invoked with a `SyncActivator` that will re-activate the source.
    pub fn build<B, I, D, T, R>(
        scope: Scope<T>,
        source_builder: B,
    ) -> (Box<dyn std::any::Any + Send + Sync>, Stream<T, Vec<(D, T, R)>>)
    where
        B: FnOnce(SyncActivator) -> I,
        I: Iterator<Item = Message<D, T, R>> + 'static,
        D: ExchangeData + Hash,
        T: ExchangeData + Hash + Timestamp + Lattice,
        R: ExchangeData + Hash,
    {
        // Read messages are either updates or progress messages.
        // Each may contain duplicates, and we must take care to deduplicate information before introducing it to an accumulation.
        // This includes both emitting updates, and setting expectations for update counts.
        //
        // Updates need to be deduplicated by (data, time), and we should exchange them by such.
        // Progress needs to be deduplicated by time, and we should exchange them by such.
        //
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

        use crate::hashable::Hashable;
        use timely::dataflow::channels::pact::Exchange;
        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
        use timely::progress::frontier::MutableAntichain;
        use timely::progress::ChangeBatch;

        // Some message distribution logic depends on the number of workers.
        let workers = scope.peers();

        let mut token = None;
        // Frontier owned by the FEEDBACK operator and consulted by the MESSAGES operators.
        let mut antichain = MutableAntichain::new();
        antichain.update_iter(Some((T::minimum(), workers as i64)));
        let shared_frontier = Rc::new(RefCell::new(antichain));
        let shared_frontier2 = Rc::clone(&shared_frontier);

        // Step 1: The MESSAGES operator.
        let mut messages_op = OperatorBuilder::new("CDCV2_Messages".to_string(), scope);
        let address = messages_op.operator_info().address;
        let activator = scope.worker().sync_activator_for(address.to_vec());
        let activator2 = scope.activator_for(Rc::clone(&address));
        let drop_activator = DropActivator { activator: Arc::new(scope.worker().sync_activator_for(address.to_vec())) };
        let mut source = source_builder(activator);
        let (updates_out, updates) = messages_op.new_output();
        let mut updates_out = OutputBuilder::from(updates_out);
        let (progress_out, progress) = messages_op.new_output();
        let mut progress_out = OutputBuilder::from(progress_out);

        messages_op.build(|capabilities| {

            // A Weak that communicates whether the returned token has been dropped.
            let drop_activator_weak = Arc::downgrade(&drop_activator.activator);

            token = Some(drop_activator);

            // Read messages from some source; shuffle them to UPDATES and PROGRESS; share capability with FEEDBACK.
            let mut updates_caps = CapabilitySet::from_elem(capabilities[0].clone());
            let mut progress_caps = CapabilitySet::from_elem(capabilities[1].clone());
            // Capture the shared frontier to read out frontier updates to apply.
            let local_frontier = Rc::clone(&shared_frontier);
            //
            move |_frontiers| {
                // First check to ensure that we haven't been terminated by someone dropping our tokens.
                if drop_activator_weak.upgrade().is_none() {
                    // Give up our capabilities
                    updates_caps.downgrade(&[]);
                    progress_caps.downgrade(&[]);
                    // never continue, even if we are (erroneously) activated again.
                    return;
                }

                // Consult our shared frontier, and ensure capabilities are downgraded to it.
                let shared_frontier = local_frontier.borrow();
                updates_caps.downgrade(&shared_frontier.frontier());
                progress_caps.downgrade(&shared_frontier.frontier());

                // Next check to see if we have been terminated by the source being complete.
                if !updates_caps.is_empty() && !progress_caps.is_empty() {
                    let mut updates = updates_out.activate();
                    let mut progress = progress_out.activate();

                    // TODO(frank): this is a moment where multi-temporal capabilities need to be fixed up.
                    // Specifically, there may not be one capability valid for all updates.
                    let mut updates_session = updates.session(&updates_caps[0]);
                    let mut progress_session = progress.session(&progress_caps[0]);

                    // We presume the iterator will yield if appropriate.
                    for message in source.by_ref() {
                        match message {
                            Message::Updates(mut updates) => {
                                updates_session.give_container(&mut updates);
                            }
                            Message::Progress(progress) => {
                                // We must send a copy of each progress message to all workers,
                                // but we can partition the counts across workers by timestamp.
                                let mut to_worker = vec![Vec::new(); workers];
                                for (time, count) in progress.counts {
                                    to_worker[(time.hashed() as usize) % workers]
                                        .push((time, count));
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
        let mut updates_op = OperatorBuilder::new("CDCV2_Updates".to_string(), scope);
        let mut input = updates_op.new_input(updates, Exchange::new(|x: &(D, T, R)| x.hashed()));
        let (changes_out, changes) = updates_op.new_output();
        let mut changes_out = OutputBuilder::from(changes_out);
        let (counts_out, counts) = updates_op.new_output();
        let mut counts_out = OutputBuilder::from(counts_out);

        updates_op.build(move |_capability| {
            // Deduplicates updates, and ships novel updates and the counts for each time.
            // For simplicity, this operator ships updates as they are discovered to be new.
            // This has the defect that on load we may have two copies of the data (shipped,
            // and here for deduplication).
            //
            // Filters may be pushed ahead of this operator, but because of deduplication we
            // may not push projections ahead of this operator (at least, not without fields
            // that are known to form keys, and even then only carefully).
            let mut pending = std::collections::HashMap::new();
            let mut change_batch = ChangeBatch::<T>::new();
            move |frontiers| {
                // Thin out deduplication buffer.
                // This is the moment in a more advanced implementation where we might send
                // the data for the first time, maintaining only one copy of each update live
                // at a time in the system.
                pending.retain(|(_row, time), _diff| frontiers[0].less_equal(time));

                // Deduplicate newly received updates, sending new updates and timestamp counts.
                let mut changes = changes_out.activate();
                let mut counts = counts_out.activate();
                input.for_each(|capability, updates| {
                    let mut changes_session = changes.session(&capability);
                    let mut counts_session = counts.session(&capability);
                    for (data, time, diff) in updates.iter() {
                        if frontiers[0].less_equal(time) {
                            if let Some(prior) = pending.insert((data.clone(), time.clone()), diff.clone()) {
                                assert_eq!(&prior, diff);
                            } else {
                                change_batch.update(time.clone(), -1);
                                changes_session.give((data.clone(), time.clone(), diff.clone()));
                            }
                        }
                    }
                    if !change_batch.is_empty() {
                        counts_session.give_iterator(change_batch.drain());
                    }
                });
            }
        });

        // Step 3: The PROGRESS operator.
        let mut progress_op = OperatorBuilder::new("CDCV2_Progress".to_string(), scope);
        let mut input = progress_op.new_input(
            progress,
            Exchange::new(|x: &(usize, Progress<T>)| x.0 as u64),
        );
        let mut counts =
            progress_op.new_input(counts, Exchange::new(|x: &(T, i64)| (x.0).hashed()));
        let (frontier_out, frontier) = progress_op.new_output();
        let mut frontier_out = OutputBuilder::from(frontier_out);
        progress_op.build(move |_capability| {
            // Receive progress statements, deduplicated counts. Track lower frontier of both and broadcast changes.

            use timely::order::PartialOrder;
            use timely::progress::{frontier::AntichainRef, Antichain};

            let mut progress_queue = Vec::new();
            let mut progress_frontier = Antichain::from_elem(T::minimum());
            let mut updates_frontier = MutableAntichain::new();
            let mut reported_frontier = Antichain::from_elem(T::minimum());

            move |_frontiers| {
                let mut frontier = frontier_out.activate();

                // If the frontier changes we need a capability to express that.
                // Any capability should work; the downstream listener doesn't care.
                let mut capability: Option<Capability<T>> = None;

                // Drain all relevant update counts in to the mutable antichain tracking its frontier.
                counts.for_each(|cap, counts| {
                    updates_frontier.update_iter(counts.iter().cloned());
                    capability = Some(cap.retain(0));
                });
                // Drain all progress statements into the queue out of which we will work.
                input.for_each(|cap, progress| {
                    progress_queue.extend(progress.iter().map(|x| (x.1).clone()));
                    capability = Some(cap.retain(0));
                });

                // Extract and act on actionable progress messages.
                // A progress message is actionable if `self.progress_frontier` is beyond the message's lower bound.
                while let Some(position) = progress_queue.iter().position(|p| {
                    <_ as PartialOrder>::less_equal(
                        &AntichainRef::new(&p.lower),
                        &progress_frontier.borrow(),
                    )
                }) {
                    // Extract progress statement.
                    let mut progress = progress_queue.remove(position);
                    // Discard counts that have already been incorporated.
                    progress
                        .counts
                        .retain(|(time, _count)| progress_frontier.less_equal(time));
                    // Record any new reports of expected counts.
                    updates_frontier
                        .update_iter(progress.counts.drain(..).map(|(t, c)| (t, c as i64)));
                    // Extend self.progress_frontier by progress.upper.
                    let mut new_frontier = Antichain::new();
                    for time1 in progress.upper {
                        for time2 in progress_frontier.elements() {
                            new_frontier.insert(time1.join(time2));
                        }
                    }
                    progress_frontier = new_frontier;
                }

                // Determine if the lower bound of frontiers have advanced, and transmit updates if so.
                let mut lower_bound = progress_frontier.clone();
                lower_bound.extend(updates_frontier.frontier().iter().cloned());
                if lower_bound != reported_frontier {
                    let capability =
                        capability.expect("Changes occurred, without surfacing a capability");
                    let mut changes = ChangeBatch::new();
                    changes.extend(lower_bound.elements().iter().map(|t| (t.clone(), 1)));
                    changes.extend(reported_frontier.elements().iter().map(|t| (t.clone(), -1)));
                    let mut frontier_session = frontier.session(&capability);
                    for peer in 0..workers {
                        frontier_session.give((peer, changes.clone()));
                    }
                    reported_frontier = lower_bound.clone();
                }
            }
        });

        // Step 4: The FEEDBACK operator.
        let mut feedback_op = OperatorBuilder::new("CDCV2_Feedback".to_string(), scope);
        let mut input = feedback_op.new_input(
            frontier,
            Exchange::new(|x: &(usize, ChangeBatch<T>)| x.0 as u64),
        );
        feedback_op.build(move |_capability| {
            // Receive frontier changes and share the net result with MESSAGES.
            move |_frontiers| {
                let mut antichain = shared_frontier2.borrow_mut();
                let mut must_activate = false;
                input.for_each(|_cap, frontier_changes| {
                    for (_self, input_changes) in frontier_changes.iter() {
                        // Apply the updates, and observe if the lower bound has changed.
                        if antichain.update_iter(input_changes.unstable_internal_updates().iter().cloned()).next().is_some() {
                            must_activate = true;
                        }
                    }
                });
                // If the lower bound has changed, we must activate MESSAGES.
                if must_activate { activator2.activate(); }
            }
        });

        (Box::new(token.unwrap()), changes)
    }
}

/// Methods for recording update streams to binary bundles.
pub mod sink {

    use std::hash::Hash;
    use std::cell::RefCell;
    use std::rc::Weak;

    use serde::{Deserialize, Serialize};

    use timely::order::PartialOrder;
    use timely::progress::{Antichain, ChangeBatch, Timestamp};
    use timely::dataflow::Stream;
    use timely::dataflow::channels::pact::{Exchange, Pipeline};
    use timely::dataflow::operators::generic::{builder_rc::OperatorBuilder, OutputBuilder};

    use crate::{lattice::Lattice, ExchangeData};
    use super::{Writer, Message, Progress};

    /// Constructs a sink, for recording the updates in `stream`.
    ///
    /// It is crucial that `stream` has been consolidated before this method, which
    /// will *not* perform the consolidation on the stream's behalf. If this is not
    /// performed before calling the method, the recorded output may not be correctly
    /// reconstructed by readers.
    pub fn build<BS, D, T, R>(
        stream: Stream<T, Vec<(D, T, R)>>,
        sink_hash: u64,
        updates_sink: Weak<RefCell<BS>>,
        progress_sink: Weak<RefCell<BS>>,
    ) where
        BS: Writer<Message<D,T,R>> + 'static,
        D: ExchangeData + Hash + Serialize + for<'a> Deserialize<'a>,
        T: ExchangeData + Hash + Serialize + for<'a> Deserialize<'a> + Timestamp + Lattice,
        R: ExchangeData + Hash + Serialize + for<'a> Deserialize<'a>,
    {
        // First we record the updates that stream in.
        // We can simply record all updates, under the presumption that the have been consolidated
        // and so any record we see is in fact guaranteed to happen.
        let mut builder = OperatorBuilder::new("UpdatesWriter".to_owned(), stream.scope());
        let reactivator = stream.scope().activator_for(builder.operator_info().address);
        let mut input = builder.new_input(stream.clone(), Pipeline);
        let (updates_out, updates) = builder.new_output();
        let mut updates_out = OutputBuilder::from(updates_out);

        builder.build_reschedule(
            move |_capability| {
                let mut timestamps = <ChangeBatch<_>>::new();
                let mut send_queue = std::collections::VecDeque::new();
                move |_frontiers| {
                    let mut output = updates_out.activate();

                    // We want to drain inputs always...
                    input.for_each(|capability, updates| {
                        // Write each update out, and record the timestamp.
                        for (_data, time, _diff) in updates.iter() {
                            timestamps.update(time.clone(), 1);
                        }

                        // Now record the update to the writer.
                        send_queue.push_back(Message::Updates(std::mem::take(updates)));

                        // Transmit timestamp counts downstream.
                        output
                            .session(&capability)
                            .give_iterator(timestamps.drain());
                    });

                    // Drain whatever we can from the queue of bytes to send.
                    // ... but needn't do anything more if our sink is closed.
                    if let Some(sink) = updates_sink.upgrade() {
                        let mut sink = sink.borrow_mut();
                        while let Some(message) = send_queue.front() {
                            if let Some(duration) = sink.poll(message) {
                                // Reschedule after `duration` and then bail.
                                reactivator.activate_after(duration);
                                return true;
                            } else {
                                send_queue.pop_front();
                            }
                        }
                        // Signal incompleteness if messages remain to be sent.
                        !sink.done() || !send_queue.is_empty()
                    } else {
                        // We have been terminated, but may still receive indefinite data.
                        send_queue.clear();
                        // Signal that there are no outstanding writes.
                        false
                    }
                }
            },
        );

        // We use a lower-level builder here to get access to the operator address, for rescheduling.
        let mut builder = OperatorBuilder::new("ProgressWriter".to_owned(), stream.scope());
        let reactivator = stream.scope().activator_for(builder.operator_info().address);
        let mut input = builder.new_input(updates, Exchange::new(move |_| sink_hash));
        let should_write = stream.scope().index() == (sink_hash as usize) % stream.scope().peers();

        // We now record the numbers of updates at each timestamp between lower and upper bounds.
        // Track the advancing frontier, to know when to produce utterances.
        let mut frontier = Antichain::from_elem(T::minimum());
        // Track accumulated counts for timestamps.
        let mut timestamps = <ChangeBatch<_>>::new();
        // Stash for serialized data yet to send.
        let mut send_queue = std::collections::VecDeque::new();
        let mut retain = Vec::new();

        builder.build_reschedule(|_capabilities| {
            move |frontiers| {

                // We want to drain inputs no matter what.
                // We could do this after the next step, as we are certain these timestamps will
                // not be part of a closed frontier (as they have not yet been read). This has the
                // potential to make things speedier as we scan less and keep a smaller footprint.
                input.for_each(|_capability, counts| {
                    timestamps.extend(counts.iter().cloned());
                });

                if should_write {
                    if let Some(sink) = progress_sink.upgrade() {
                        let mut sink = sink.borrow_mut();

                        // If our frontier advances strictly, we have the opportunity to issue a progress statement.
                        if <_ as PartialOrder>::less_than(
                            &frontier.borrow(),
                            &frontiers[0].frontier(),
                        ) {
                            let new_frontier = frontiers[0].frontier();

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
                            send_queue.push_back(Message::Progress(progress));

                            // Advance our frontier to track our progress utterance.
                            frontier = frontiers[0].frontier().to_owned();

                            while let Some(message) = send_queue.front() {
                                if let Some(duration) = sink.poll(message) {
                                    // Reschedule after `duration` and then bail.
                                    reactivator.activate_after(duration);
                                    // Signal that work remains to be done.
                                    return true;
                                } else {
                                    send_queue.pop_front();
                                }
                            }
                        }
                        // Signal incompleteness if messages remain to be sent.
                        !sink.done() || !send_queue.is_empty()
                    } else {
                        timestamps.clear();
                        send_queue.clear();
                        // Signal that there are no outstanding writes.
                        false
                    }
                } else { false }
            }
        });
    }
}
