// Copyright (c) 2015 Frank McSherry
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Portions of this file are derived from `operators/arrange/agent.rs` in
// differential-dataflow. The original source code was retrieved from:
//
//     https://github.com/TimelyDataflow/differential-dataflow/blob/6074cc78884bdc169c5219af7fdbd76da7009cba/differential-dataflow/src/operators/arrange/agent.rs
//
// The crates.io release 0.26.0, which the workspace depends on, ships these sources unchanged.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.
//
// Not upstream: `crate::` paths name `differential_dataflow::`, and items
// upstream's `operators/arrange/mod.rs` defines come from `super`, which is
// `mz_timely_util::arrange_agent`. Otherwise the file is upstream's, run through rustfmt.

//! Shared read access to a trace.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::{Rc, Weak};

use timely::dataflow::Scope;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::{OperatorInfo, source};
use timely::progress::Timestamp;
use timely::progress::{Antichain, frontier::AntichainRef};

use differential_dataflow::trace::{Span, Trace, TraceReader};

use timely::scheduling::Activator;

use super::TraceReplayInstruction;
use super::{Arranged, TraceAgentQueueReader, TraceAgentQueueWriter, TraceWriter};

use differential_dataflow::trace::wrappers::frontier::{BatchFrontier, TraceFrontier};

// Not upstream: differential's public `ShutdownButton`, which upstream defines in this file.
use differential_dataflow::operators::arrange::ShutdownButton;

/// A `TraceReader` wrapper which can be imported into other dataflows.
///
/// The `TraceAgent` is the default trace type produced by `arranged`, and it can be extracted
/// from the dataflow in which it was defined, and imported into other dataflows.
pub struct TraceAgent<Tr: TraceReader> {
    trace: Rc<RefCell<trace_box::TraceBox<Tr>>>,
    queues: Weak<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>,
    logical_compaction: Antichain<Tr::Time>,
    physical_compaction: Antichain<Tr::Time>,
    temp_antichain: Antichain<Tr::Time>,

    operator: OperatorInfo,
    logging: Option<differential_dataflow::logging::Logger>,
}

impl<Tr: TraceReader> TraceReader for TraceAgent<Tr> {
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        // This method does not enforce that `frontier` is greater or equal to `self.logical_compaction`.
        // Instead, it determines the joint consequences of both guarantees and moves forward with that.
        differential_dataflow::lattice::antichain_join_into(
            &self.logical_compaction.borrow()[..],
            &frontier[..],
            &mut self.temp_antichain,
        );
        self.trace.borrow_mut().adjust_logical_compaction(
            self.logical_compaction.borrow(),
            self.temp_antichain.borrow(),
        );
        ::std::mem::swap(&mut self.logical_compaction, &mut self.temp_antichain);
        self.temp_antichain.clear();
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.logical_compaction.borrow()
    }
    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) {
        // This method does not enforce that `frontier` is greater or equal to `self.physical_compaction`.
        // Instead, it determines the joint consequences of both guarantees and moves forward with that.
        differential_dataflow::lattice::antichain_join_into(
            &self.physical_compaction.borrow()[..],
            &frontier[..],
            &mut self.temp_antichain,
        );
        self.trace.borrow_mut().adjust_physical_compaction(
            self.physical_compaction.borrow(),
            self.temp_antichain.borrow(),
        );
        ::std::mem::swap(&mut self.physical_compaction, &mut self.temp_antichain);
        self.temp_antichain.clear();
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.physical_compaction.borrow()
    }
    fn spans_through(
        &mut self,
        frontier: AntichainRef<'_, Tr::Time>,
    ) -> Option<Vec<Span<Tr::Time, Tr::Batch>>> {
        self.trace.borrow_mut().trace.spans_through(frontier)
    }
    fn map_spans<F: FnMut(&Span<Tr::Time, Tr::Batch>)>(&self, f: F) {
        self.trace.borrow().trace.map_spans(f)
    }
}

impl<Tr: TraceReader> TraceAgent<Tr> {
    /// Creates a new agent from a trace reader.
    pub fn new(
        trace: Tr,
        operator: OperatorInfo,
        logging: Option<differential_dataflow::logging::Logger>,
    ) -> (Self, TraceWriter<Tr>)
    where
        Tr: Trace,
    {
        let trace = Rc::new(RefCell::new(trace_box::TraceBox::new(trace)));
        let queues = Rc::new(RefCell::new(Vec::new()));

        if let Some(logging) = &logging {
            logging.log(differential_dataflow::logging::TraceShare {
                operator: operator.global_id,
                diff: 1,
            });
        }

        let reader = TraceAgent {
            trace: Rc::clone(&trace),
            queues: Rc::downgrade(&queues),
            logical_compaction: trace.borrow().logical_compaction.frontier().to_owned(),
            physical_compaction: trace.borrow().physical_compaction.frontier().to_owned(),
            temp_antichain: Antichain::new(),
            operator,
            logging,
        };

        let writer = TraceWriter::new(vec![Tr::Time::minimum()], Rc::downgrade(&trace), queues);

        (reader, writer)
    }

    /// Attaches a new shared queue to the trace.
    ///
    /// The queue is first populated with existing batches from the trace,
    /// The queue will be immediately populated with existing historical batches from the trace, and until the reference
    /// is dropped the queue will receive new batches as produced by the source `arrange` operator.
    pub fn new_listener(&mut self, activator: Activator) -> TraceAgentQueueReader<Tr> {
        // create a new queue for progress and batch information.
        let mut new_queue = VecDeque::new();

        // add the existing batches from the trace
        let mut upper = None;
        self.trace.borrow_mut().trace.map_spans(|span| {
            new_queue.push_back(TraceReplayInstruction::Span(
                span.clone(),
                timely::progress::Stamp::from_elem(Tr::Time::minimum()),
            ));
            upper = Some(span.upper().clone());
        });

        if let Some(upper) = upper {
            new_queue.push_back(TraceReplayInstruction::Frontier(upper));
        }

        let reference = Rc::new((activator, RefCell::new(new_queue)));

        // wraps the queue in a ref-counted ref cell and enqueue/return it.
        if let Some(queue) = self.queues.upgrade() {
            queue.borrow_mut().push(Rc::downgrade(&reference));
        }
        reference.0.activate();
        reference
    }

    /// The [OperatorInfo] of the underlying Timely operator
    pub fn operator(&self) -> &OperatorInfo {
        &self.operator
    }

    /// Obtain a reference to the inner [`trace_box::TraceBox`]. It is the caller's obligation to maintain
    /// the trace box and this trace agent's invariants. Specifically, it is undefined behavior
    /// to mutate the trace box. Keeping strong references can prevent resource reclamation.
    ///
    /// This method is subject to changes and removal and should not be considered part of a stable
    /// interface.
    pub fn trace_box_unstable(&self) -> Rc<RefCell<trace_box::TraceBox<Tr>>> {
        Rc::clone(&self.trace)
    }
}

impl<Tr: TraceReader + 'static> TraceAgent<Tr> {
    /// Copies an existing collection into the supplied scope.
    ///
    /// This method creates an `Arranged` collection that should appear indistinguishable from applying `arrange`
    /// directly to the source collection brought into the local scope. The only caveat is that the initial state
    /// of the collection is its current state, and updates occur from this point forward. The historical changes
    /// the collection experienced in the past are accumulated, and the distinctions from the initial collection
    /// are no longer evident.
    ///
    /// The current behavior is that the introduced collection accumulates updates to some times less or equal
    /// to `self.get_logical_compaction()`. There is *not* currently a guarantee that the updates are accumulated *to*
    /// the frontier, and the resulting collection history may be weirdly partial until this point. In particular,
    /// the historical collection may move through configurations that did not actually occur, even if eventually
    /// arriving at the correct collection. This is probably a bug; although we get to the right place in the end,
    /// the intermediate computation could do something that the original computation did not, like diverge.
    ///
    /// I would expect the semantics to improve to "updates are advanced to `self.get_logical_compaction()`", which
    /// means the computation will run as if starting from exactly this frontier. It is not currently clear whose
    /// responsibility this should be (the trace/batch should only reveal these times, or an operator should know
    /// to advance times before using them).
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::Config;
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
    ///
    /// ::timely::execute(Config::thread(), |worker| {
    ///
    ///     // create a first dataflow
    ///     let mut trace = worker.dataflow::<u32,_,_>(|scope| {
    ///         // create input handle and collection.
    ///         scope.new_collection_from(0 .. 10).1
    ///              .arrange_by_self()
    ///              .trace
    ///     });
    ///
    ///     // do some work.
    ///     worker.step();
    ///     worker.step();
    ///
    ///     // create a second dataflow
    ///     worker.dataflow(move |scope| {
    ///         trace.import(scope)
    ///              .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>,_,_>(
    ///                  "Reduce",
    ///                  |_key, src, dst| dst.push((*src[0].0, 1)),
    ///                  |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
    ///              )
    ///              .as_collection(|k,v| (k.clone(), v.clone()));
    ///     });
    ///
    /// }).unwrap();
    /// ```
    pub fn import<'scope>(
        &mut self,
        scope: Scope<'scope, Tr::Time>,
    ) -> Arranged<'scope, TraceAgent<Tr>> {
        self.import_named(scope, "ArrangedSource")
    }

    /// Same as `import`, but allows to name the source.
    pub fn import_named<'scope>(
        &mut self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
    ) -> Arranged<'scope, TraceAgent<Tr>> {
        // Drop ShutdownButton and return only the arrangement.
        self.import_core(scope, name).0
    }

    /// Imports an arrangement into the supplied scope.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::Config;
    /// use timely::dataflow::ProbeHandle;
    /// use timely::dataflow::operators::Probe;
    /// use differential_dataflow::input::InputSession;
    /// use differential_dataflow::trace::Trace;
    ///
    /// ::timely::execute(Config::thread(), |worker| {
    ///
    ///     let mut input = InputSession::<_,(),isize>::new();
    ///     let mut probe = ProbeHandle::new();
    ///
    ///     // create a first dataflow
    ///     let mut trace = worker.dataflow::<u32,_,_>(|scope| {
    ///         // create input handle and collection.
    ///         input.to_collection(scope)
    ///              .arrange_by_self()
    ///              .trace
    ///     });
    ///
    ///     // do some work.
    ///     worker.step();
    ///     worker.step();
    ///
    ///     // create a second dataflow
    ///     let mut shutdown = worker.dataflow(|scope| {
    ///         let (arrange, button) = trace.import_core(scope, "Import");
    ///         arrange.stream.probe_with(&mut probe);
    ///         button
    ///     });
    ///
    ///     worker.step();
    ///     worker.step();
    ///     assert!(!probe.done());
    ///
    ///     shutdown.press();
    ///
    ///     worker.step();
    ///     worker.step();
    ///     assert!(probe.done());
    ///
    /// }).unwrap();
    /// ```
    pub fn import_core<'scope>(
        &mut self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
    ) -> (
        Arranged<'scope, TraceAgent<Tr>>,
        ShutdownButton<CapabilitySet<Tr::Time>>,
    ) {
        let trace = self.clone();

        let mut shutdown_button = None;

        let stream = {
            let shutdown_button_ref = &mut shutdown_button;
            source(scope, name, move |capability, info| {
                let capabilities = Rc::new(RefCell::new(Some(CapabilitySet::new())));

                let activator = scope.activator_for(Rc::clone(&info.address));
                let queue = self.new_listener(activator);

                let activator = scope.activator_for(info.address);
                *shutdown_button_ref =
                    Some(ShutdownButton::new(Rc::clone(&capabilities), activator));

                capabilities
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .insert(capability);

                move |output| {
                    let mut capabilities = capabilities.borrow_mut();
                    if let Some(ref mut capabilities) = *capabilities {
                        let mut borrow = queue.1.borrow_mut();
                        for instruction in borrow.drain(..) {
                            match instruction {
                                TraceReplayInstruction::Frontier(frontier) => {
                                    capabilities.downgrade(&frontier.borrow()[..]);
                                }
                                TraceReplayInstruction::Span(batch, hint) => {
                                    if !hint.is_empty() && batch.has_updates() {
                                        let delayed = capabilities.delayed_stamp(&hint);
                                        output.session(&delayed).give(batch);
                                    }
                                }
                            }
                        }
                    }
                }
            })
        };

        (Arranged { stream, trace }, shutdown_button.unwrap())
    }

    /// Imports an arrangement into the supplied scope.
    ///
    /// This variant of import uses the `get_logical_compaction` to forcibly advance timestamps in updates.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::Config;
    /// use timely::progress::frontier::AntichainRef;
    /// use timely::dataflow::ProbeHandle;
    /// use timely::dataflow::operators::Probe;
    /// use timely::dataflow::operators::Inspect;
    /// use differential_dataflow::input::InputSession;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::TraceReader;
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::execute(Config::thread(), |worker| {
    ///
    ///     let mut probe = ProbeHandle::new();
    ///
    ///     // create a first dataflow
    ///     let (mut handle, mut trace) = worker.dataflow::<u32,_,_>(|scope| {
    ///         // create input handle and collection.
    ///         let (handle, stream) = scope.new_collection();
    ///         let trace = stream.arrange_by_self().trace;
    ///         (handle, trace)
    ///     });
    ///
    ///     handle.insert(0); handle.advance_to(1); handle.flush(); worker.step();
    ///     handle.remove(0); handle.advance_to(2); handle.flush(); worker.step();
    ///     handle.insert(1); handle.advance_to(3); handle.flush(); worker.step();
    ///     handle.remove(1); handle.advance_to(4); handle.flush(); worker.step();
    ///     handle.insert(0); handle.advance_to(5); handle.flush(); worker.step();
    ///
    ///     trace.set_logical_compaction(AntichainRef::new(&[5]));
    ///
    ///     // create a second dataflow
    ///     let mut shutdown = worker.dataflow(|scope| {
    ///         let (arrange, button) = trace.import_frontier(scope, "Import");
    ///         arrange
    ///             .as_collection(|k,v| (*k,*v))
    ///             .inner
    ///             .inspect(|(d,t,r)| {
    ///                 assert!(t >= &5);
    ///             })
    ///             .probe_with(&mut probe);
    ///
    ///         button
    ///     });
    ///
    ///     worker.step();
    ///     worker.step();
    ///     assert!(!probe.done());
    ///
    ///     shutdown.press();
    ///
    ///     worker.step();
    ///     worker.step();
    ///     assert!(probe.done());
    ///
    /// }).unwrap();
    /// ```
    pub fn import_frontier<'scope>(
        &mut self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
    ) -> (
        Arranged<'scope, TraceFrontier<TraceAgent<Tr>>>,
        ShutdownButton<CapabilitySet<Tr::Time>>,
    )
    where
        Tr: TraceReader,
    {
        // This frontier describes our only guarantee on the compaction frontier.
        let since = self.get_logical_compaction().to_owned();
        self.import_frontier_core(scope, name, since, Antichain::new())
    }

    /// Import a trace restricted to a specific time interval `[since, until)`.
    ///
    /// All updates present in the input trace will be first advanced to `since`, and then either emitted,
    /// or if greater or equal to `until`, suppressed. Once all times are certain to be greater or equal
    /// to `until` the operator capability will be dropped.
    ///
    /// Invoking this method with an `until` of `Antichain::new()` will perform no filtering, as the empty
    /// frontier indicates the end of times.
    pub fn import_frontier_core<'scope>(
        &mut self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
        since: Antichain<Tr::Time>,
        until: Antichain<Tr::Time>,
    ) -> (
        Arranged<'scope, TraceFrontier<TraceAgent<Tr>>>,
        ShutdownButton<CapabilitySet<Tr::Time>>,
    )
    where
        Tr: TraceReader,
    {
        let trace = self.clone();
        let trace = TraceFrontier::make_from(trace, since.borrow(), until.borrow());

        let mut shutdown_button = None;

        let stream = {
            let shutdown_button_ref = &mut shutdown_button;
            source(scope, name, move |capability, info| {
                let capabilities = Rc::new(RefCell::new(Some(CapabilitySet::new())));

                let activator = scope.activator_for(Rc::clone(&info.address));
                let queue = self.new_listener(activator);

                let activator = scope.activator_for(info.address);
                *shutdown_button_ref =
                    Some(ShutdownButton::new(Rc::clone(&capabilities), activator));

                capabilities
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .insert(capability);

                move |output| {
                    let mut capabilities = capabilities.borrow_mut();
                    if let Some(ref mut capabilities) = *capabilities {
                        let mut borrow = queue.1.borrow_mut();
                        for instruction in borrow.drain(..) {
                            // If we have dropped the capabilities due to `until`, attempt no further work.
                            // Without the capabilities, we should soon be shut down (once this loop ends).
                            if !capabilities.is_empty() {
                                match instruction {
                                    TraceReplayInstruction::Frontier(frontier) => {
                                        if timely::PartialOrder::less_equal(&until, &frontier) {
                                            // It might be nice to actively *drop* `capabilities`, but it seems
                                            // complicated logically (i.e. we'd have to break out of the loop).
                                            capabilities.downgrade(&[]);
                                        } else {
                                            capabilities.downgrade(&frontier.borrow()[..]);
                                        }
                                    }
                                    TraceReplayInstruction::Span(batch, hint) => {
                                        if !hint.is_empty() && batch.has_updates() {
                                            let delayed = capabilities.delayed_stamp(&hint);
                                            let wrapped = Span::new(
                                                batch.desc,
                                                batch.inner.map(|p| {
                                                    BatchFrontier::make_from(
                                                        p,
                                                        since.borrow(),
                                                        until.borrow(),
                                                    )
                                                }),
                                            );
                                            output.session(&delayed).give(wrapped);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })
        };

        (Arranged { stream, trace }, shutdown_button.unwrap())
    }
}

// Not upstream: `ShutdownButton` is differential's, imported above.

impl<Tr: TraceReader> Clone for TraceAgent<Tr> {
    fn clone(&self) -> Self {
        if let Some(logging) = &self.logging {
            logging.log(differential_dataflow::logging::TraceShare {
                operator: self.operator.global_id,
                diff: 1,
            });
        }

        // increase counts for wrapped `TraceBox`.
        let empty_frontier = Antichain::new();
        self.trace
            .borrow_mut()
            .adjust_logical_compaction(empty_frontier.borrow(), self.logical_compaction.borrow());
        self.trace
            .borrow_mut()
            .adjust_physical_compaction(empty_frontier.borrow(), self.physical_compaction.borrow());

        TraceAgent {
            trace: Rc::clone(&self.trace),
            queues: Weak::clone(&self.queues),
            logical_compaction: self.logical_compaction.clone(),
            physical_compaction: self.physical_compaction.clone(),
            operator: self.operator.clone(),
            logging: self.logging.clone(),
            temp_antichain: Antichain::new(),
        }
    }
}

impl<Tr: TraceReader> Drop for TraceAgent<Tr> {
    fn drop(&mut self) {
        if let Some(logging) = &self.logging {
            logging.log(differential_dataflow::logging::TraceShare {
                operator: self.operator.global_id,
                diff: -1,
            });
        }

        // decrement borrow counts to remove all holds
        let empty_frontier = Antichain::new();
        self.trace
            .borrow_mut()
            .adjust_logical_compaction(self.logical_compaction.borrow(), empty_frontier.borrow());
        self.trace
            .borrow_mut()
            .adjust_physical_compaction(self.physical_compaction.borrow(), empty_frontier.borrow());
    }
}

/// A trace wrapper suitable for use through shared reference counted ownership.
///
/// The wrapper mainly accumulates the expressed compaction constraints from many,
/// and presents their implications to the wrapped trace.
pub mod trace_box {

    use timely::progress::frontier::{AntichainRef, MutableAntichain};

    use differential_dataflow::trace::TraceReader;

    /// A wrapper around a trace which tracks the frontiers of all referees.
    ///
    /// This is an internal type, unlikely to be useful to higher-level programs, but exposed just in case.
    /// This type is equivalent to a `RefCell`, in that it wraps the mutable state that multiple referrers
    /// may influence.
    pub struct TraceBox<Tr: TraceReader> {
        /// accumulated holds on times for advancement.
        pub(crate) logical_compaction: MutableAntichain<Tr::Time>,
        /// accumulated holds on times for distinction.
        pub(crate) physical_compaction: MutableAntichain<Tr::Time>,
        /// The wrapped trace.
        pub(crate) trace: Tr,
    }

    impl<Tr: TraceReader> TraceBox<Tr> {
        /// Moves an existing trace into a shareable trace wrapper.
        ///
        /// The trace may already exist and have non-initial advance and distinguish frontiers. The boxing
        /// process will fish these out and make sure that they are used for the initial read capabilities.
        pub fn new(mut trace: Tr) -> Self {
            let mut logical_compaction = MutableAntichain::new();
            logical_compaction.update_iter(
                trace
                    .get_logical_compaction()
                    .iter()
                    .cloned()
                    .map(|t| (t, 1)),
            );
            let mut physical_compaction = MutableAntichain::new();
            physical_compaction.update_iter(
                trace
                    .get_physical_compaction()
                    .iter()
                    .cloned()
                    .map(|t| (t, 1)),
            );

            TraceBox {
                logical_compaction,
                physical_compaction,
                trace,
            }
        }
        /// Borrowed access to the underlying trace.
        ///
        /// This is used to inspect batches for purposes of resource accounting in external systems.
        pub fn trace(&self) -> &Tr {
            &self.trace
        }
        /// Replaces elements of `lower` with those of `upper`.
        #[inline]
        pub fn adjust_logical_compaction(
            &mut self,
            lower: AntichainRef<Tr::Time>,
            upper: AntichainRef<Tr::Time>,
        ) {
            self.logical_compaction
                .update_iter(upper.iter().cloned().map(|t| (t, 1)));
            self.logical_compaction
                .update_iter(lower.iter().cloned().map(|t| (t, -1)));
            self.trace
                .set_logical_compaction(self.logical_compaction.frontier());
        }
        /// Replaces elements of `lower` with those of `upper`.
        #[inline]
        pub fn adjust_physical_compaction(
            &mut self,
            lower: AntichainRef<Tr::Time>,
            upper: AntichainRef<Tr::Time>,
        ) {
            self.physical_compaction
                .update_iter(upper.iter().cloned().map(|t| (t, 1)));
            self.physical_compaction
                .update_iter(lower.iter().cloned().map(|t| (t, -1)));
            self.trace
                .set_physical_compaction(self.physical_compaction.frontier());
        }
    }
}
