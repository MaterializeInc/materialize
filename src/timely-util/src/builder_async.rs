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

//! Types to build async operators with general shapes.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker, ready};

use futures_util::Stream;
use futures_util::task::ArcWake;
use timely::communication::{Pull, Push};
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::channels::Message;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::channels::pushers::Output;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::generic::{InputHandleCore, OperatorInfo};
use timely::dataflow::operators::{Capability, CapabilitySet, InputCapability};
use timely::dataflow::{Scope, Stream as TimelyStream, StreamVec};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::{Activator, SyncActivator};
use timely::{Bincode, Container, ContainerBuilder, PartialOrder};

use crate::containers::stack::FueledBuilder;

thread_local! {
    /// INSTR: counts messages pushed to timely outputs on this worker thread.
    /// Read as a per-schedule delta to measure the descs->fabric send rate.
    static DBG_OUTPUT_PUSHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Builds async operators with generic shape.
pub struct OperatorBuilder<'scope, T: Timestamp> {
    builder: OperatorBuilderRc<'scope, T>,
    /// The operator name, retained for diagnostics.
    name: String,
    /// The activator for this operator
    activator: Activator,
    /// The waker set up to activate this timely operator when woken
    operator_waker: Arc<TimelyWaker>,
    /// The currently known upper frontier of each of the input handles.
    input_frontiers: Vec<Antichain<T>>,
    /// Input queues for each of the declared inputs of the operator.
    input_queues: Vec<Box<dyn InputQueue<T>>>,
    /// Holds type erased closures that flush an output handle when called. These handles will be
    /// automatically drained when the operator is scheduled after the logic future has been polled
    output_flushes: Vec<Box<dyn FnMut()>>,
    /// A handle to check whether all workers have pressed the shutdown button.
    shutdown_handle: ButtonHandle,
    /// A button to coordinate shutdown of this operator among workers.
    shutdown_button: Button,
}

/// A helper trait abstracting over an input handle. It facilitates keeping around type erased
/// handles for each of the operator inputs.
trait InputQueue<T: Timestamp> {
    /// Accepts all available input into local queues. Returns the number of
    /// data messages pulled from the timely channel this call.
    fn accept_input(&mut self) -> usize;

    /// Drains all available input and empties the local queue.
    fn drain_input(&mut self);

    /// Registers a frontier notification to be delivered.
    fn notify_progress(&mut self, upper: Antichain<T>);
}

impl<T, D, C, P> InputQueue<T> for InputHandleQueue<T, D, C, P>
where
    T: Timestamp,
    D: Container,
    C: InputConnection<T> + 'static,
    P: Pull<Message<T, D>> + 'static,
{
    fn accept_input(&mut self) -> usize {
        let mut queue = self.queue.borrow_mut();
        let mut pulled = 0;
        self.handle.for_each(|cap, data| {
            pulled += 1;
            let cap = self.connection.accept(cap);
            queue.push_back(Event::Data(cap, std::mem::take(data)));
        });
        if pulled > 0 {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
        pulled
    }

    fn drain_input(&mut self) {
        self.queue.borrow_mut().clear();
        self.handle.for_each(|_, _| {});
    }

    fn notify_progress(&mut self, upper: Antichain<T>) {
        let mut queue = self.queue.borrow_mut();
        // It's beneficial to consolidate two consecutive progress statements into one if the
        // operator hasn't seen the previous progress yet. This also avoids accumulation of
        // progress statements in the queue if the operator only conditionally checks this input.
        match queue.back_mut() {
            Some(&mut Event::Progress(ref mut prev_upper)) => *prev_upper = upper,
            _ => queue.push_back(Event::Progress(upper)),
        }
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

struct InputHandleQueue<
    T: Timestamp,
    D: Container,
    C: InputConnection<T>,
    P: Pull<Message<T, D>> + 'static,
> {
    queue: Rc<RefCell<VecDeque<Event<T, C::Capability, D>>>>,
    waker: Rc<Cell<Option<Waker>>>,
    connection: C,
    handle: InputHandleCore<T, D, P>,
}

/// An async Waker that activates a specific operator when woken and marks the task as ready
struct TimelyWaker {
    activator: SyncActivator,
    active: AtomicBool,
    task_ready: AtomicBool,
}

impl ArcWake for TimelyWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.task_ready.store(true, Ordering::SeqCst);
        // Only activate the timely operator if it's not already active to avoid an infinite loop
        if !arc_self.active.load(Ordering::SeqCst) {
            // We don't have any guarantees about how long the Waker will be held for and so we
            // must be prepared for the receiving end to have hung up when we finally do get woken.
            // This can happen if by the time the waker is called the receiving timely worker has
            // been shutdown. For this reason we ignore the activation error.
            let _ = arc_self.activator.activate();
        }
    }
}

/// Async handle to an operator's input stream
pub struct AsyncInputHandle<T: Timestamp, D: Container, C: InputConnection<T>> {
    queue: Rc<RefCell<VecDeque<Event<T, C::Capability, D>>>>,
    waker: Rc<Cell<Option<Waker>>>,
    /// Whether this handle has finished producing data
    done: bool,
}

impl<T: Timestamp, D: Container, C: InputConnection<T>> AsyncInputHandle<T, D, C> {
    pub fn next_sync(&mut self) -> Option<Event<T, C::Capability, D>> {
        let mut queue = self.queue.borrow_mut();
        match queue.pop_front()? {
            Event::Data(cap, data) => Some(Event::Data(cap, data)),
            Event::Progress(frontier) => {
                self.done = frontier.is_empty();
                Some(Event::Progress(frontier))
            }
        }
    }

    /// Waits for the handle to have data. After this function returns it is guaranteed that at
    /// least one call to `next_sync` will be `Some(_)`.
    pub async fn ready(&self) {
        std::future::poll_fn(|cx| self.poll_ready(cx)).await
    }

    fn poll_ready(&self, cx: &Context<'_>) -> Poll<()> {
        if self.queue.borrow().is_empty() {
            self.waker.set(Some(cx.waker().clone()));
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl<T: Timestamp, D: Container, C: InputConnection<T>> Stream for AsyncInputHandle<T, D, C> {
    type Item = Event<T, C::Capability, D>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        ready!(self.poll_ready(cx));
        Poll::Ready(self.next_sync())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.queue.borrow().len(), None)
    }
}

/// An event of an input stream
#[derive(Debug)]
pub enum Event<T: Timestamp, C, D> {
    /// A data event
    Data(C, D),
    /// A progress event
    Progress(Antichain<T>),
}

/// Shared part of an async output handle
struct AsyncOutputHandleInner<T: Timestamp, CB: ContainerBuilder> {
    /// Handle to write to the output stream.
    output: Output<T, CB::Container>,
    /// Current capability held by this output handle.
    capability: Option<Capability<T>>,
    /// Container builder to accumulate data before sending at `capability`.
    builder: CB,
}

impl<T: Timestamp, CB: ContainerBuilder> AsyncOutputHandleInner<T, CB> {
    /// Write all pending data to the output stream.
    fn flush(&mut self) {
        while let Some(container) = self.builder.finish() {
            self.output
                .give(self.capability.as_ref().expect("must exist"), container);
            DBG_OUTPUT_PUSHES.with(|c| c.set(c.get() + 1));
        }
    }

    /// Cease this output handle, flushing all pending data and releasing its capability.
    fn cease(&mut self) {
        self.flush();
        let _ = self.output.activate();
        self.capability = None;
    }

    /// Provides data at the time specified by the capability. Flushes automatically when the
    /// capability time changes.
    fn give<D>(&mut self, cap: &Capability<T>, data: D)
    where
        CB: PushInto<D>,
    {
        if let Some(capability) = &self.capability
            && cap.time() != capability.time()
        {
            self.flush();
            self.capability = None;
        }
        if self.capability.is_none() {
            self.capability = Some(cap.clone());
        }

        self.builder.push_into(data);
        while let Some(container) = self.builder.extract() {
            self.output
                .give(self.capability.as_ref().expect("must exist"), container);
            DBG_OUTPUT_PUSHES.with(|c| c.set(c.get() + 1));
        }
    }
}

pub struct AsyncOutputHandle<T: Timestamp, CB: ContainerBuilder> {
    inner: Rc<RefCell<AsyncOutputHandleInner<T, CB>>>,
    index: usize,
}

impl<T, C> AsyncOutputHandle<T, CapacityContainerBuilder<C>>
where
    T: Timestamp,
    C: Container + Clone + 'static,
{
    #[inline]
    pub fn give_container(&self, cap: &Capability<T>, container: &mut C) {
        let mut inner = self.inner.borrow_mut();
        inner.flush();
        inner.output.give(cap, container);
        DBG_OUTPUT_PUSHES.with(|c| c.set(c.get() + 1));
    }
}

impl<T, CB> AsyncOutputHandle<T, CB>
where
    T: Timestamp,
    CB: ContainerBuilder,
{
    fn new(output: Output<T, CB::Container>, index: usize) -> Self {
        let inner = AsyncOutputHandleInner {
            output,
            capability: None,
            builder: CB::default(),
        };
        Self {
            inner: Rc::new(RefCell::new(inner)),
            index,
        }
    }

    fn cease(&self) {
        self.inner.borrow_mut().cease();
    }
}

impl<T, CB> AsyncOutputHandle<T, CB>
where
    T: Timestamp,
    CB: ContainerBuilder,
{
    pub fn give<D>(&self, cap: &Capability<T>, data: D)
    where
        CB: PushInto<D>,
    {
        self.inner.borrow_mut().give(cap, data);
    }
}

impl<T, D> AsyncOutputHandle<T, FueledBuilder<CapacityContainerBuilder<Vec<D>>>>
where
    D: Clone + 'static,
    T: Timestamp,
{
    pub const MAX_OUTSTANDING_BYTES: usize = 128 * 1024 * 1024;

    /// Provides one record at the time specified by the capability and
    /// charges `size_bytes` against the builder's fuel counter. Once at least
    /// [`Self::MAX_OUTSTANDING_BYTES`] have been emitted since the last yield
    /// this method yields back to timely and resets the counter.
    pub async fn give_fueled<D2>(&self, cap: &Capability<T>, data: D2, size_bytes: usize)
    where
        FueledBuilder<CapacityContainerBuilder<Vec<D>>>: PushInto<D2>,
    {
        let should_yield = {
            let mut handle = self.inner.borrow_mut();
            handle.give(cap, data);
            let bytes = &handle.builder.bytes;
            bytes.set(bytes.get() + size_bytes);
            let should_yield = bytes.get() > Self::MAX_OUTSTANDING_BYTES;
            if should_yield {
                bytes.set(0);
            }
            should_yield
        };
        if should_yield {
            tokio::task::yield_now().await;
        }
    }
}

impl<T: Timestamp, CB: ContainerBuilder> Clone for AsyncOutputHandle<T, CB> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
            index: self.index,
        }
    }
}

/// A trait describing the connection behavior between an input of an operator and zero or more of
/// its outputs.
pub trait InputConnection<T: Timestamp> {
    /// The capability type associated with this connection behavior.
    type Capability;

    /// Generates a summary description of the connection behavior given the number of outputs.
    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>>;

    /// Accepts an input capability.
    fn accept(&self, input_cap: InputCapability<T>) -> Self::Capability;
}

/// A marker type representing a disconnected input.
pub struct Disconnected;

impl<T: Timestamp> InputConnection<T> for Disconnected {
    type Capability = T;

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        vec![Antichain::new(); outputs]
    }

    fn accept(&self, input_cap: InputCapability<T>) -> Self::Capability {
        input_cap.time().clone()
    }
}

/// A marker type representing an input connected to exactly one output.
pub struct ConnectedToOne(usize);

impl<T: Timestamp> InputConnection<T> for ConnectedToOne {
    type Capability = Capability<T>;

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        let mut summary = vec![Antichain::new(); outputs];
        summary[self.0] = Antichain::from_elem(T::Summary::default());
        summary
    }

    fn accept(&self, input_cap: InputCapability<T>) -> Self::Capability {
        input_cap.retain(self.0)
    }
}

/// A marker type representing an input connected to many outputs.
pub struct ConnectedToMany<const N: usize>([usize; N]);

impl<const N: usize, T: Timestamp> InputConnection<T> for ConnectedToMany<N> {
    type Capability = [Capability<T>; N];

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        let mut summary = vec![Antichain::new(); outputs];
        for output in self.0 {
            summary[output] = Antichain::from_elem(T::Summary::default());
        }
        summary
    }

    fn accept(&self, input_cap: InputCapability<T>) -> Self::Capability {
        self.0.map(|output| input_cap.retain(output))
    }
}

/// A helper trait abstracting over an output handle. It facilitates passing type erased
/// output handles during operator construction.
/// It is not meant to be implemented by users.
pub trait OutputIndex {
    /// The output index of this handle.
    fn index(&self) -> usize;
}

impl<T: Timestamp, CB: ContainerBuilder> OutputIndex for AsyncOutputHandle<T, CB> {
    fn index(&self) -> usize {
        self.index
    }
}

impl<'scope, T: Timestamp> OperatorBuilder<'scope, T> {
    /// Allocates a new generic async operator builder from its containing scope.
    pub fn new(name: String, scope: Scope<'scope, T>) -> Self {
        let builder = OperatorBuilderRc::new(name.clone(), scope);
        let info = builder.operator_info();
        let activator = scope.activator_for(Rc::clone(&info.address));
        let sync_activator = scope.worker().sync_activator_for(info.address.to_vec());
        let operator_waker = TimelyWaker {
            activator: sync_activator,
            active: AtomicBool::new(false),
            task_ready: AtomicBool::new(true),
        };
        let (shutdown_handle, shutdown_button) = button(scope, info.address);

        OperatorBuilder {
            builder,
            name,
            activator,
            operator_waker: Arc::new(operator_waker),
            input_frontiers: Default::default(),
            input_queues: Default::default(),
            output_flushes: Default::default(),
            shutdown_handle,
            shutdown_button,
        }
    }

    /// Adds a new input that is connected to the specified output, returning the async input handle to use.
    pub fn new_input_for<D, P>(
        &mut self,
        stream: TimelyStream<'scope, T, D>,
        pact: P,
        output: &dyn OutputIndex,
    ) -> AsyncInputHandle<T, D, ConnectedToOne>
    where
        D: Container + Clone + 'static,
        P: ParallelizationContract<T, D>,
    {
        let index = output.index();
        assert!(index < self.builder.shape().outputs());
        self.new_input_connection(stream, pact, ConnectedToOne(index))
    }

    /// Adds a new input that is connected to the specified outputs, returning the async input handle to use.
    pub fn new_input_for_many<const N: usize, D, P>(
        &mut self,
        stream: TimelyStream<'scope, T, D>,
        pact: P,
        outputs: [&dyn OutputIndex; N],
    ) -> AsyncInputHandle<T, D, ConnectedToMany<N>>
    where
        D: Container + Clone + 'static,
        P: ParallelizationContract<T, D>,
    {
        let indices = outputs.map(|output| output.index());
        for index in indices {
            assert!(index < self.builder.shape().outputs());
        }
        self.new_input_connection(stream, pact, ConnectedToMany(indices))
    }

    /// Adds a new input that is not connected to any output, returning the async input handle to use.
    pub fn new_disconnected_input<D, P>(
        &mut self,
        stream: TimelyStream<'scope, T, D>,
        pact: P,
    ) -> AsyncInputHandle<T, D, Disconnected>
    where
        D: Container + Clone + 'static,
        P: ParallelizationContract<T, D>,
    {
        self.new_input_connection(stream, pact, Disconnected)
    }

    /// Adds a new input with connection information, returning the async input handle to use.
    pub fn new_input_connection<D, P, C>(
        &mut self,
        stream: TimelyStream<'scope, T, D>,
        pact: P,
        connection: C,
    ) -> AsyncInputHandle<T, D, C>
    where
        D: Container + Clone + 'static,
        P: ParallelizationContract<T, D>,
        C: InputConnection<T> + 'static,
    {
        self.input_frontiers
            .push(Antichain::from_elem(T::minimum()));

        let outputs = self.builder.shape().outputs();
        let handle = self.builder.new_input_connection(
            stream,
            pact,
            connection.describe(outputs).into_iter().enumerate(),
        );

        let waker = Default::default();
        let queue = Default::default();
        let input_queue = InputHandleQueue {
            queue: Rc::clone(&queue),
            waker: Rc::clone(&waker),
            connection,
            handle,
        };
        self.input_queues.push(Box::new(input_queue));

        AsyncInputHandle {
            queue,
            waker,
            done: false,
        }
    }

    /// Adds a new output, returning the output handle and stream.
    pub fn new_output<CB: ContainerBuilder>(
        &mut self,
    ) -> (
        AsyncOutputHandle<T, CB>,
        TimelyStream<'scope, T, CB::Container>,
    ) {
        let index = self.builder.shape().outputs();

        let (output, stream) = self.builder.new_output_connection([]);

        let handle = AsyncOutputHandle::new(output, index);

        let flush_handle = handle.clone();
        self.output_flushes
            .push(Box::new(move || flush_handle.cease()));

        (handle, stream)
    }

    /// Creates an operator implementation from supplied logic constructor. It returns a shutdown
    /// button that when pressed it will cause the logic future to be dropped and input handles to
    /// be drained. The button can be converted into a token by using
    /// [`Button::press_on_drop`]
    pub fn build<B, L>(self, constructor: B) -> Button
    where
        B: FnOnce(Vec<Capability<T>>) -> L,
        L: Future + 'static,
    {
        let operator_waker = self.operator_waker;
        let mut input_frontiers = self.input_frontiers;
        let mut input_queues = self.input_queues;
        let mut output_flushes = self.output_flushes;
        let mut shutdown_handle = self.shutdown_handle;
        // INSTR: gate delivery-probe logging. `descs` name matches both
        // shard_source_descs and shard_source_descs_return; only the former has
        // an output, so the return operator logs nothing.
        let dbg_is_fetch = self.name.contains("shard_source_fetch");
        let dbg_is_descs = self.name.contains("shard_source_descs");
        let dbg_name = self.name.clone();
        self.builder.build_reschedule(move |caps| {
            let mut logic_fut = Some(Box::pin(constructor(caps)));
            // INSTR: cumulative schedules and messages accepted, to see whether
            // the operator is scheduled often with ~1 message each (fabric
            // dribble) or seldom with a bulk pull.
            let mut dbg_schedules: u64 = 0;
            let mut dbg_accepted_total: usize = 0;
            // INSTR (send side): messages this operator pushes to the fabric per
            // schedule. Answers whether descs flushes the whole flood in one
            // poll (fabric then buffers) or dribbles it out over many polls.
            let mut dbg_send_schedules: u64 = 0;
            let mut dbg_send_pushed_total: u64 = 0;
            move |new_frontiers| {
                operator_waker.active.store(true, Ordering::SeqCst);
                let mut dbg_accepted_now: usize = 0;
                for (i, queue) in input_queues.iter_mut().enumerate() {
                    // First, discover if there are any frontier notifications
                    let cur = &mut input_frontiers[i];
                    let new = new_frontiers[i].frontier();
                    if PartialOrder::less_than(&cur.borrow(), &new) {
                        queue.notify_progress(new.to_owned());
                        *cur = new.to_owned();
                    }
                    // Then accept all input into local queues. This step registers the received
                    // messages with progress tracking.
                    dbg_accepted_now += queue.accept_input();
                }
                operator_waker.active.store(false, Ordering::SeqCst);
                if dbg_is_fetch {
                    dbg_schedules += 1;
                    dbg_accepted_total += dbg_accepted_now;
                    // Log only when we actually pulled data. The `schedules`
                    // delta between consecutive logs is how many times the
                    // operator ran without new input, i.e. how much slack there
                    // was to pull more if the fabric had it.
                    if dbg_accepted_now > 0 {
                        tracing::info!(
                            target: "mz_persist_client::operators::shard_source",
                            "INSTR accept: op={dbg_name} pulled={dbg_accepted_now} schedules={dbg_schedules} accepted_total={dbg_accepted_total}"
                        );
                    }
                }

                // If our worker pressed the button we stop scheduling the logic future and/or
                // draining the input handles to stop producing data and frontier updates
                // downstream.
                if shutdown_handle.local_pressed() {
                    // When all workers press their buttons we drop the logic future and start
                    // draining the input handles.
                    if shutdown_handle.all_pressed() {
                        logic_fut = None;
                        for queue in input_queues.iter_mut() {
                            queue.drain_input();
                        }
                        false
                    } else {
                        true
                    }
                } else {
                    // INSTR: fabric pushes before this schedule's poll+flush.
                    let dbg_pushes_before = DBG_OUTPUT_PUSHES.with(|c| c.get());
                    // Schedule the logic future if any of the wakers above marked the task as ready
                    if let Some(fut) = logic_fut.as_mut() {
                        if operator_waker.task_ready.load(Ordering::SeqCst) {
                            let waker = futures_util::task::waker_ref(&operator_waker);
                            let mut cx = Context::from_waker(&waker);
                            operator_waker.task_ready.store(false, Ordering::SeqCst);
                            if Pin::new(fut).poll(&mut cx).is_ready() {
                                // We're done with logic so deallocate the task
                                logic_fut = None;
                            }
                            // Flush all the outputs before exiting
                            for flush in output_flushes.iter_mut() {
                                (flush)();
                            }
                        }
                    }
                    if dbg_is_descs {
                        let dbg_pushed = DBG_OUTPUT_PUSHES
                            .with(|c| c.get())
                            .wrapping_sub(dbg_pushes_before);
                        dbg_send_schedules += 1;
                        // Log when this schedule pushed anything. `pushes` per
                        // log is how many messages left the operator in one poll;
                        // if descs ever logs ~1333 the send is bulk and the fabric
                        // buffers, if it dribbles ~1 the send itself is paced.
                        if dbg_pushed > 0 {
                            dbg_send_pushed_total += dbg_pushed;
                            tracing::info!(
                                target: "mz_persist_client::operators::shard_source",
                                "INSTR send: pushes={dbg_pushed} send_schedules={dbg_send_schedules} pushed_total={dbg_send_pushed_total}"
                            );
                        }
                    }

                    // The timely operator needs to be kept alive if the task is pending
                    if logic_fut.is_some() {
                        true
                    } else {
                        // Othewise we should keep draining all inputs
                        for queue in input_queues.iter_mut() {
                            queue.drain_input();
                        }
                        false
                    }
                }
            }
        });

        self.shutdown_button
    }

    /// Creates a fallible operator implementation from supplied logic constructor. If the `Future`
    /// resolves to an error it will be emitted in the returned error stream and then the operator
    /// will wait indefinitely until the shutdown button is pressed.
    ///
    /// # Capability handling
    ///
    /// Unlike [`OperatorBuilder::build`], this method does not give owned capabilities to the
    /// constructor. All initial capabilities are wrapped in a `CapabilitySet` and a mutable
    /// reference to them is given instead. This is done to avoid storing owned capabilities in the
    /// state of the logic future which would make using the `?` operator unsafe, since the
    /// frontiers would incorrectly advance, potentially causing incorrect actions downstream.
    ///
    /// ```ignore
    /// builder.build_fallible(|caps| Box::pin(async move {
    ///     // Assert that we have the number of capabilities we expect
    ///     // `cap` will be a `&mut Option<Capability<T>>`:
    ///     let [cap_set]: &mut [_; 1] = caps.try_into().unwrap();
    ///
    ///     // Using cap to send data:
    ///     output.give(&cap_set[0], 42);
    ///
    ///     // Using cap_set to downgrade it:
    ///     cap_set.downgrade([]);
    ///
    ///     // Explicitly dropping the capability:
    ///     // Simply running `drop(cap_set)` will only drop the reference and not the capability set itself!
    ///     *cap_set = CapabilitySet::new();
    ///
    ///     // !! BIG WARNING !!:
    ///     // It is tempting to `take` the capability out of the set for convenience. This will
    ///     // move the capability into the future state, tying its lifetime to it, which will get
    ///     // dropped when an error is hit, causing incorrect progress statements.
    ///     let cap = cap_set.delayed(&Timestamp::minimum());
    ///     *cap_set = CapabilitySet::new(); // DO NOT DO THIS
    /// }));
    /// ```
    pub fn build_fallible<E: 'static, F>(
        mut self,
        constructor: F,
    ) -> (Button, StreamVec<'scope, T, Rc<E>>)
    where
        F: for<'a> FnOnce(
                &'a mut [CapabilitySet<T>],
            ) -> Pin<Box<dyn Future<Output = Result<(), E>> + 'a>>
            + 'static,
    {
        // Create a new completely disconnected output
        let (error_output, error_stream) = self.new_output::<CapacityContainerBuilder<_>>();
        let button = self.build(|mut caps| async move {
            let error_cap = caps.pop().unwrap();
            let mut caps = caps
                .into_iter()
                .map(CapabilitySet::from_elem)
                .collect::<Vec<_>>();
            if let Err(err) = constructor(&mut *caps).await {
                error_output.give(&error_cap, Rc::new(err));
                drop(error_cap);
                // IMPORTANT: wedge this operator until the button is pressed. Returning would drop
                // the capabilities and could produce incorrect progress statements.
                std::future::pending().await
            }
        });
        (button, error_stream)
    }

    /// Creates operator info for the operator.
    pub fn operator_info(&self) -> OperatorInfo {
        self.builder.operator_info()
    }

    /// Returns the activator for the operator.
    pub fn activator(&self) -> &Activator {
        &self.activator
    }
}

/// Creates a new coordinated button the worker configuration described by `scope`.
pub fn button<'scope, T: Timestamp>(
    scope: Scope<'scope, T>,
    addr: Rc<[usize]>,
) -> (ButtonHandle, Button) {
    let index = scope.worker().new_identifier();
    let (pushers, puller) = scope.worker().allocate(index, addr);

    let local_pressed = Rc::new(Cell::new(false));

    let handle = ButtonHandle {
        buttons_remaining: scope.peers(),
        local_pressed: Rc::clone(&local_pressed),
        puller,
    };

    let token = Button {
        pushers,
        local_pressed,
    };

    (handle, token)
}

/// A button that can be used to coordinate an action after all workers have pressed it.
pub struct ButtonHandle {
    /// The number of buttons still unpressed among workers.
    buttons_remaining: usize,
    /// A flag indicating whether this worker has pressed its button.
    local_pressed: Rc<Cell<bool>>,
    puller: Box<dyn Pull<Bincode<bool>>>,
}

impl ButtonHandle {
    /// Returns whether this worker has pressed its button.
    pub fn local_pressed(&self) -> bool {
        self.local_pressed.get()
    }

    /// Returns whether all workers have pressed their buttons.
    pub fn all_pressed(&mut self) -> bool {
        while self.puller.recv().is_some() {
            self.buttons_remaining -= 1;
        }
        self.buttons_remaining == 0
    }
}

pub struct Button {
    pushers: Vec<Box<dyn Push<Bincode<bool>>>>,
    local_pressed: Rc<Cell<bool>>,
}

impl Button {
    /// Presses the button. It is safe to call this function multiple times.
    pub fn press(&mut self) {
        for mut pusher in self.pushers.drain(..) {
            pusher.send(Bincode::from(true));
            pusher.done();
        }
        self.local_pressed.set(true);
    }

    /// Converts this button into a deadman's switch that will automatically press the button when
    /// dropped.
    pub fn press_on_drop(self) -> PressOnDropButton {
        PressOnDropButton(self)
    }
}

pub struct PressOnDropButton(Button);

impl Drop for PressOnDropButton {
    fn drop(&mut self) {
        self.0.press();
    }
}

#[cfg(test)]
mod test {
    use futures_util::StreamExt;
    use timely::WorkerConfig;
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::vec::ToStream;

    use super::*;

    #[mz_ore::test]
    fn async_operator() {
        let capture = timely::example(|scope| {
            let input = (0..10).to_stream(scope);

            let mut op = OperatorBuilder::new("async_passthru".to_string(), input.scope());
            let (output, output_stream) = op.new_output::<CapacityContainerBuilder<_>>();
            let mut input_handle = op.new_input_for(input, Pipeline, &output);

            op.build(move |_capabilities| async move {
                tokio::task::yield_now().await;
                while let Some(event) = input_handle.next().await {
                    match event {
                        Event::Data(cap, data) => {
                            for item in data.iter().copied() {
                                tokio::task::yield_now().await;
                                output.give(&cap, item);
                            }
                        }
                        Event::Progress(_frontier) => {}
                    }
                }
            });

            output_stream.capture()
        });
        let extracted = capture.extract();

        assert_eq!(extracted, vec![(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]);
    }

    #[mz_ore::test]
    fn gh_18837() {
        let (builders, other) = timely::CommunicationConfig::Process(2).try_build().unwrap();
        timely::execute::execute_from(builders, other, WorkerConfig::default(), |worker| {
            let index = worker.index();
            let tokens = worker.dataflow::<u64, _, _>(move |scope| {
                let mut producer = OperatorBuilder::new("producer".to_string(), scope.clone());
                let (_output, output_stream) =
                    producer.new_output::<CapacityContainerBuilder<Vec<usize>>>();
                let producer_button = producer.build(move |mut capabilities| async move {
                    let mut cap = capabilities.pop().unwrap();
                    if index != 0 {
                        return;
                    }
                    // Worker 0 downgrades to 1 and keeps the capability around forever
                    cap.downgrade(&1);
                    std::future::pending().await
                });

                let mut consumer = OperatorBuilder::new("consumer".to_string(), scope.clone());
                let mut input_handle = consumer.new_disconnected_input(output_stream, Pipeline);
                let consumer_button = consumer.build(move |_| async move {
                    while let Some(event) = input_handle.next().await {
                        if let Event::Progress(frontier) = event {
                            // We should never observe a frontier greater than [1]
                            assert!(frontier.less_equal(&1));
                        }
                    }
                });

                (
                    producer_button.press_on_drop(),
                    consumer_button.press_on_drop(),
                )
            });

            // Run dataflow until only worker 0 holds the frontier to [1]
            for _ in 0..100 {
                worker.step();
            }
            // Then drop the tokens of worker 0
            if index == 0 {
                drop(tokens)
            }
            // And step the dataflow some more to ensure consumers don't observe frontiers advancing.
            for _ in 0..100 {
                worker.step();
            }
        })
        .expect("timely panicked");
    }

    // MICROREPRO: does a flood of N records given via an Exchange pact at one
    // timestamp arrive at an async consumer in bulk (letting a bounded-concurrency
    // fetch loop ramp `in_flight` to the cap), or ~1 per schedule?
    //
    // Mirrors shard_source_fetch: producer floods N parts at one time; consumer
    // runs the same `tokio::select! { biased; in_flight.next(); input.next() }`
    // loop with a simulated fetch that stays in flight for a fixed number of
    // polls. Reports the max `in_flight` depth reached and parts-per-event.
    #[mz_ore::test]
    fn microrepro_exchange_delivery() {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures_util::stream::FuturesUnordered;
        use timely::dataflow::channels::pact::Exchange;

        const N: usize = 200;
        const MAX_CONCURRENCY: usize = 32;
        // Each simulated fetch stays in flight for this many polls (models blob
        // fetch latency in units of operator schedules).
        const FETCH_POLLS: usize = 80;

        // A future that stays Pending for `0` polls, self-waking each time so the
        // operator is rescheduled, then completes.
        struct FetchSim(usize);
        impl Future for FetchSim {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.0 == 0 {
                    Poll::Ready(())
                } else {
                    self.0 -= 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let max_parts_per_event = Arc::new(AtomicUsize::new(0));
        let schedules_with_recv = Arc::new(AtomicUsize::new(0));

        let mif = Arc::clone(&max_in_flight);
        let mppe = Arc::clone(&max_parts_per_event);
        let swr = Arc::clone(&schedules_with_recv);

        let (builders, other) = timely::CommunicationConfig::Process(2).try_build().unwrap();
        timely::execute::execute_from(builders, other, WorkerConfig::default(), move |worker| {
            let index = worker.index();
            let mif = Arc::clone(&mif);
            let mppe = Arc::clone(&mppe);
            let swr = Arc::clone(&swr);
            let tokens = worker.dataflow::<u64, _, _>(move |scope| {
                let mut producer = OperatorBuilder::new("producer".to_string(), scope.clone());
                let (output, output_stream) =
                    producer.new_output::<CapacityContainerBuilder<Vec<usize>>>();
                let producer_button = producer.build(move |mut caps| async move {
                    let cap = caps.pop().unwrap();
                    // Only worker 0 produces; all records route to worker 1.
                    if index == 0 {
                        for i in 0..N {
                            output.give(&cap, i);
                        }
                    }
                    // Returning drops `cap`, advancing the frontier to empty so
                    // the consumer observes end-of-input.
                });

                let mut consumer = OperatorBuilder::new("consumer".to_string(), scope.clone());
                // Route every record to worker 1, exercising the inter-thread
                // exchange channel exactly like `shard_source`'s pact.
                let mut input =
                    consumer.new_disconnected_input(output_stream, Exchange::new(|_: &usize| 1u64));
                let consumer_button = consumer.build(move |_caps| async move {
                    let mut pending: VecDeque<usize> = VecDeque::new();
                    let mut in_flight = FuturesUnordered::new();
                    let mut input_done = false;
                    loop {
                        while in_flight.len() < MAX_CONCURRENCY {
                            match pending.pop_front() {
                                Some(_) => in_flight.push(FetchSim(FETCH_POLLS)),
                                None => break,
                            }
                        }
                        let cur = in_flight.len();
                        mif.fetch_max(cur, Ordering::SeqCst);

                        tokio::select! {
                            biased;
                            Some(()) = in_flight.next(), if !in_flight.is_empty() => {}
                            event = input.next(), if !input_done => {
                                match event {
                                    Some(Event::Data(_cap, data)) => {
                                        let n = data.len();
                                        mppe.fetch_max(n, Ordering::SeqCst);
                                        swr.fetch_add(1, Ordering::SeqCst);
                                        for x in data {
                                            pending.push_back(x);
                                        }
                                        eprintln!(
                                            "RECV parts={n} pending_now={} in_flight={}",
                                            pending.len(),
                                            in_flight.len(),
                                        );
                                    }
                                    Some(Event::Progress(_)) => {}
                                    None => input_done = true,
                                }
                            }
                            else => break,
                        }
                    }
                });

                (
                    producer_button.press_on_drop(),
                    consumer_button.press_on_drop(),
                )
            });

            // Step until the dataflow quiesces (consumer finished draining and
            // all simulated fetches completed).
            for _ in 0..200_000 {
                worker.step();
            }
            drop(tokens);
        })
        .expect("timely panicked");

        let mif = max_in_flight.load(Ordering::SeqCst);
        let mppe = max_parts_per_event.load(Ordering::SeqCst);
        let swr = schedules_with_recv.load(Ordering::SeqCst);
        eprintln!(
            "MICROREPRO RESULT: max_in_flight={mif} max_parts_per_event={mppe} \
             recv_events={swr} (N={N}, cap={MAX_CONCURRENCY})"
        );
    }

    // MICROREPRO variant: producer emits ONE record per container (=> one timely
    // Message per record), mirroring the real shard_source observation of
    // `parts=1` per event. Tests whether N separate messages sent in one
    // producer schedule arrive at the consumer's `accept_input` in bulk (all N
    // ready next schedule) or trickle ~1 per `worker.step()`.
    #[mz_ore::test]
    fn microrepro_exchange_delivery_one_msg_per_record() {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures_util::stream::FuturesUnordered;
        use timely::dataflow::channels::pact::Exchange;

        const N: usize = 200;
        const MAX_CONCURRENCY: usize = 32;
        const FETCH_POLLS: usize = 80;

        struct FetchSim(usize);
        impl Future for FetchSim {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.0 == 0 {
                    Poll::Ready(())
                } else {
                    self.0 -= 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let max_parts_per_event = Arc::new(AtomicUsize::new(0));
        let recv_events = Arc::new(AtomicUsize::new(0));

        let mif = Arc::clone(&max_in_flight);
        let mppe = Arc::clone(&max_parts_per_event);
        let re = Arc::clone(&recv_events);

        let (builders, other) = timely::CommunicationConfig::Process(2).try_build().unwrap();
        timely::execute::execute_from(builders, other, WorkerConfig::default(), move |worker| {
            let index = worker.index();
            let mif = Arc::clone(&mif);
            let mppe = Arc::clone(&mppe);
            let re = Arc::clone(&re);
            let tokens = worker.dataflow::<u64, _, _>(move |scope| {
                let mut producer = OperatorBuilder::new("producer".to_string(), scope.clone());
                let (output, output_stream) =
                    producer.new_output::<CapacityContainerBuilder<Vec<usize>>>();
                let producer_button = producer.build(move |mut caps| async move {
                    let cap = caps.pop().unwrap();
                    if index == 0 {
                        // One record per container => one timely Message each,
                        // all at the same timestamp.
                        for i in 0..N {
                            let mut container = vec![i];
                            output.give_container(&cap, &mut container);
                        }
                    }
                });

                let mut consumer = OperatorBuilder::new("consumer".to_string(), scope.clone());
                let mut input =
                    consumer.new_disconnected_input(output_stream, Exchange::new(|_: &usize| 1u64));
                let consumer_button = consumer.build(move |_caps| async move {
                    let mut pending: VecDeque<usize> = VecDeque::new();
                    let mut in_flight = FuturesUnordered::new();
                    let mut input_done = false;
                    loop {
                        while in_flight.len() < MAX_CONCURRENCY {
                            match pending.pop_front() {
                                Some(_) => in_flight.push(FetchSim(FETCH_POLLS)),
                                None => break,
                            }
                        }
                        mif.fetch_max(in_flight.len(), Ordering::SeqCst);

                        tokio::select! {
                            biased;
                            Some(()) = in_flight.next(), if !in_flight.is_empty() => {}
                            event = input.next(), if !input_done => {
                                match event {
                                    Some(Event::Data(_cap, data)) => {
                                        let n = data.len();
                                        mppe.fetch_max(n, Ordering::SeqCst);
                                        re.fetch_add(1, Ordering::SeqCst);
                                        for x in data {
                                            pending.push_back(x);
                                        }
                                    }
                                    Some(Event::Progress(_)) => {}
                                    None => input_done = true,
                                }
                            }
                            else => break,
                        }
                    }
                });

                (
                    producer_button.press_on_drop(),
                    consumer_button.press_on_drop(),
                )
            });

            for _ in 0..200_000 {
                worker.step();
            }
            drop(tokens);
        })
        .expect("timely panicked");

        let mif = max_in_flight.load(Ordering::SeqCst);
        let mppe = max_parts_per_event.load(Ordering::SeqCst);
        let re = recv_events.load(Ordering::SeqCst);
        eprintln!(
            "MICROREPRO 1-MSG RESULT: max_in_flight={mif} max_parts_per_event={mppe} \
             recv_events={re} (N={N}, cap={MAX_CONCURRENCY})"
        );
    }

    // MICROREPRO variant: LARGE per-record payloads (like ExchangeableBatchPart),
    // to see whether timely's exchange stops coalescing above a per-message byte
    // threshold => many small receive events => does the fetch loop still ramp?
    #[mz_ore::test]
    fn microrepro_exchange_delivery_large_payload() {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures_util::stream::FuturesUnordered;
        use timely::dataflow::channels::pact::Exchange;

        const N: usize = 200;
        const MAX_CONCURRENCY: usize = 32;
        const FETCH_POLLS: usize = 80;
        // ~256 KiB per record, comparable order to a real part descriptor's
        // inline footprint being non-trivial.
        const PAYLOAD_BYTES: usize = 256 * 1024;

        struct FetchSim(usize);
        impl Future for FetchSim {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.0 == 0 {
                    Poll::Ready(())
                } else {
                    self.0 -= 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let max_parts_per_event = Arc::new(AtomicUsize::new(0));
        let recv_events = Arc::new(AtomicUsize::new(0));

        let mif = Arc::clone(&max_in_flight);
        let mppe = Arc::clone(&max_parts_per_event);
        let re = Arc::clone(&recv_events);

        let (builders, other) = timely::CommunicationConfig::Process(2).try_build().unwrap();
        timely::execute::execute_from(builders, other, WorkerConfig::default(), move |worker| {
            let index = worker.index();
            let mif = Arc::clone(&mif);
            let mppe = Arc::clone(&mppe);
            let re = Arc::clone(&re);
            let tokens = worker.dataflow::<u64, _, _>(move |scope| {
                let mut producer = OperatorBuilder::new("producer".to_string(), scope.clone());
                let (output, output_stream) =
                    producer.new_output::<CapacityContainerBuilder<Vec<Vec<u8>>>>();
                let producer_button = producer.build(move |mut caps| async move {
                    let cap = caps.pop().unwrap();
                    if index == 0 {
                        for _ in 0..N {
                            output.give(&cap, vec![0u8; PAYLOAD_BYTES]);
                        }
                    }
                });

                let mut consumer = OperatorBuilder::new("consumer".to_string(), scope.clone());
                let mut input = consumer
                    .new_disconnected_input(output_stream, Exchange::new(|_: &Vec<u8>| 1u64));
                let consumer_button = consumer.build(move |_caps| async move {
                    let mut pending: VecDeque<Vec<u8>> = VecDeque::new();
                    let mut in_flight = FuturesUnordered::new();
                    let mut input_done = false;
                    loop {
                        while in_flight.len() < MAX_CONCURRENCY {
                            match pending.pop_front() {
                                Some(_) => in_flight.push(FetchSim(FETCH_POLLS)),
                                None => break,
                            }
                        }
                        mif.fetch_max(in_flight.len(), Ordering::SeqCst);

                        tokio::select! {
                            biased;
                            Some(()) = in_flight.next(), if !in_flight.is_empty() => {}
                            event = input.next(), if !input_done => {
                                match event {
                                    Some(Event::Data(_cap, data)) => {
                                        let n = data.len();
                                        mppe.fetch_max(n, Ordering::SeqCst);
                                        re.fetch_add(1, Ordering::SeqCst);
                                        for x in data {
                                            pending.push_back(x);
                                        }
                                    }
                                    Some(Event::Progress(_)) => {}
                                    None => input_done = true,
                                }
                            }
                            else => break,
                        }
                    }
                });

                (
                    producer_button.press_on_drop(),
                    consumer_button.press_on_drop(),
                )
            });

            for _ in 0..200_000 {
                worker.step();
            }
            drop(tokens);
        })
        .expect("timely panicked");

        let mif = max_in_flight.load(Ordering::SeqCst);
        let mppe = max_parts_per_event.load(Ordering::SeqCst);
        let re = recv_events.load(Ordering::SeqCst);
        eprintln!(
            "MICROREPRO LARGE RESULT: max_in_flight={mif} max_parts_per_event={mppe} \
             recv_events={re} (N={N}, payload={PAYLOAD_BYTES}B, cap={MAX_CONCURRENCY})"
        );
    }

    // MICROREPRO variant: faithful `shard_source` TOPOLOGY. Producer (descs
    // substitute) runs in the OUTER scope and floods N parts at one time; the
    // stream `enter`s a nested scope where the consumer (fetch substitute) runs
    // with a CONNECTED input (`new_input_for_many` -> two outputs) and a
    // `completed`-fetches FEEDBACK loop back to the producer, exactly like
    // `persist_client::operators::shard_source`. The fetch is simulated as a
    // latent future. If in_flight stays ~1 here (but ramped in the flat repro),
    // the nested-scope + feedback topology is the hydration throttle.
    #[mz_ore::test]
    fn microrepro_nested_scope_feedback() {
        use std::convert::Infallible;
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures_util::stream::FuturesUnordered;
        use mz_ore::cast::CastFrom;
        use timely::dataflow::channels::pact::Exchange;
        use timely::dataflow::operators::{
            Capability, CapabilitySet, ConnectLoop, Enter, Feedback, Leave,
        };

        const N: usize = 200;
        const MAX_CONCURRENCY: usize = 32;
        const FETCH_POLLS: usize = 80;

        struct FetchSim(usize);
        impl Future for FetchSim {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.0 == 0 {
                    Poll::Ready(())
                } else {
                    self.0 -= 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let max_parts_per_event = Arc::new(AtomicUsize::new(0));
        let recv_events = Arc::new(AtomicUsize::new(0));

        let mif = Arc::clone(&max_in_flight);
        let mppe = Arc::clone(&max_parts_per_event);
        let re = Arc::clone(&recv_events);

        let (builders, other) = timely::CommunicationConfig::Process(2).try_build().unwrap();
        timely::execute::execute_from(builders, other, WorkerConfig::default(), move |worker| {
            let index = worker.index();
            let mif = Arc::clone(&mif);
            let mppe = Arc::clone(&mppe);
            let re = Arc::clone(&re);
            let tokens = worker.dataflow::<u64, _, _>(move |outer| {
                outer.clone().scoped::<u64, _, _>("inner", move |inner| {
                    // Feedback lives in the nested scope; it leaves to the outer
                    // scope to feed the producer, mirroring
                    // `completed_fetches_feedback_stream.leave(outer)`.
                    let (fb_handle, fb_stream) = inner.feedback(Default::default());

                    // Producer (descs substitute) in the OUTER scope.
                    let mut producer = OperatorBuilder::new("producer".to_string(), outer.clone());
                    let (descs_out, descs_stream) =
                        producer.new_output::<CapacityContainerBuilder<Vec<(usize, usize)>>>();
                    let mut fb_input =
                        producer.new_disconnected_input(fb_stream.leave(outer.clone()), Pipeline);
                    let producer_button = producer.build(move |caps| async move {
                        let mut cap_set =
                            CapabilitySet::from_elem(caps.into_iter().next().unwrap());
                        if index == 0 {
                            let cap = cap_set.delayed(&0u64);
                            // Route every part to worker 1 (cross-thread exchange).
                            for i in 0..N {
                                descs_out.give(&cap, (1usize, i));
                            }
                            drop(cap);
                        }
                        // Release our capability so the consumer observes end of
                        // input once it has drained.
                        let empty: [u64; 0] = [];
                        cap_set.downgrade(empty.iter());
                        // Drain the feedback (progress only) to keep the operator
                        // alive until the loop closes, like `shard_source`'s
                        // lease-returner.
                        while (fb_input.next().await).is_some() {}
                    });

                    // Fetch (consumer substitute) in the NESTED scope, reached via
                    // `enter`, with a connected input to two outputs.
                    let descs_entered = descs_stream.enter(inner);
                    let mut fetch = OperatorBuilder::new("fetch".to_string(), inner.clone());
                    let (fetched_out, _fetched_stream) =
                        fetch.new_output::<CapacityContainerBuilder<Vec<()>>>();
                    let (_completed_out, completed_stream) =
                        fetch.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();
                    let mut descs_input = fetch.new_input_for_many(
                        descs_entered,
                        Exchange::new(|&(i, _): &(usize, usize)| u64::cast_from(i)),
                        [&fetched_out, &_completed_out],
                    );
                    let fetch_button = fetch.build(move |_caps| async move {
                        let mut pending: VecDeque<[Capability<u64>; 2]> = VecDeque::new();
                        let mut in_flight = FuturesUnordered::new();
                        let mut input_done = false;
                        loop {
                            while in_flight.len() < MAX_CONCURRENCY {
                                match pending.pop_front() {
                                    Some(caps) => in_flight.push(async move {
                                        FetchSim(FETCH_POLLS).await;
                                        caps
                                    }),
                                    None => break,
                                }
                            }
                            mif.fetch_max(in_flight.len(), Ordering::SeqCst);

                            tokio::select! {
                                biased;
                                Some(caps) = in_flight.next(), if !in_flight.is_empty() => {
                                    let [c0, _c1] = caps;
                                    fetched_out.give(&c0, ());
                                }
                                event = descs_input.next(), if !input_done => {
                                    match event {
                                        Some(Event::Data(caps, data)) => {
                                            let n = data.len();
                                            mppe.fetch_max(n, Ordering::SeqCst);
                                            re.fetch_add(1, Ordering::SeqCst);
                                            for _ in data {
                                                pending.push_back(caps.clone());
                                            }
                                        }
                                        Some(Event::Progress(_)) => {}
                                        None => input_done = true,
                                    }
                                }
                                else => break,
                            }
                        }
                    });
                    completed_stream.connect_loop(fb_handle);

                    (
                        producer_button.press_on_drop(),
                        fetch_button.press_on_drop(),
                    )
                })
            });

            for _ in 0..200_000 {
                worker.step();
            }
            drop(tokens);
        })
        .expect("timely panicked");

        let mif = max_in_flight.load(Ordering::SeqCst);
        let mppe = max_parts_per_event.load(Ordering::SeqCst);
        let re = recv_events.load(Ordering::SeqCst);
        eprintln!(
            "MICROREPRO NESTED+FEEDBACK RESULT: max_in_flight={mif} \
             max_parts_per_event={mppe} recv_events={re} (N={N}, cap={MAX_CONCURRENCY})"
        );
    }

    // MICROREPRO variant: faithful topology, but the producer gives each part at
    // a DISTINCT timestamp (like the real refined part times). Distinct times
    // force the output to flush one part per message (matching the observed
    // parts=1 per event on staging). Tests whether per-part timestamps collapse
    // the fetch ramp.
    #[mz_ore::test]
    fn microrepro_distinct_times() {
        use std::convert::Infallible;
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures_util::stream::FuturesUnordered;
        use mz_ore::cast::CastFrom;
        use timely::dataflow::channels::pact::Exchange;
        use timely::dataflow::operators::{
            Capability, CapabilitySet, ConnectLoop, Enter, Feedback, Leave,
        };

        const N: usize = 200;
        const MAX_CONCURRENCY: usize = 32;
        const FETCH_POLLS: usize = 80;

        struct FetchSim(usize);
        impl Future for FetchSim {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.0 == 0 {
                    Poll::Ready(())
                } else {
                    self.0 -= 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let max_parts_per_event = Arc::new(AtomicUsize::new(0));
        let recv_events = Arc::new(AtomicUsize::new(0));

        let mif = Arc::clone(&max_in_flight);
        let mppe = Arc::clone(&max_parts_per_event);
        let re = Arc::clone(&recv_events);

        let (builders, other) = timely::CommunicationConfig::Process(2).try_build().unwrap();
        timely::execute::execute_from(builders, other, WorkerConfig::default(), move |worker| {
            let index = worker.index();
            let mif = Arc::clone(&mif);
            let mppe = Arc::clone(&mppe);
            let re = Arc::clone(&re);
            let tokens = worker.dataflow::<u64, _, _>(move |outer| {
                outer.clone().scoped::<u64, _, _>("inner", move |inner| {
                    let (fb_handle, fb_stream) = inner.feedback(Default::default());

                    let mut producer = OperatorBuilder::new("producer".to_string(), outer.clone());
                    let (descs_out, descs_stream) =
                        producer.new_output::<CapacityContainerBuilder<Vec<(usize, usize)>>>();
                    let mut fb_input =
                        producer.new_disconnected_input(fb_stream.leave(outer.clone()), Pipeline);
                    let producer_button = producer.build(move |caps| async move {
                        let mut cap_set =
                            CapabilitySet::from_elem(caps.into_iter().next().unwrap());
                        if index == 0 {
                            // Each part at a DISTINCT time => one flush (message)
                            // per part.
                            for i in 0..N {
                                let t = u64::cast_from(i);
                                descs_out.give(&cap_set.delayed(&t), (1usize, i));
                            }
                        }
                        let empty: [u64; 0] = [];
                        cap_set.downgrade(empty.iter());
                        while (fb_input.next().await).is_some() {}
                    });

                    let descs_entered = descs_stream.enter(inner);
                    let mut fetch = OperatorBuilder::new("fetch".to_string(), inner.clone());
                    let (fetched_out, _fetched_stream) =
                        fetch.new_output::<CapacityContainerBuilder<Vec<()>>>();
                    let (_completed_out, completed_stream) =
                        fetch.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();
                    let mut descs_input = fetch.new_input_for_many(
                        descs_entered,
                        Exchange::new(|&(i, _): &(usize, usize)| u64::cast_from(i)),
                        [&fetched_out, &_completed_out],
                    );
                    let fetch_button = fetch.build(move |_caps| async move {
                        let mut pending: VecDeque<[Capability<u64>; 2]> = VecDeque::new();
                        let mut in_flight = FuturesUnordered::new();
                        let mut input_done = false;
                        loop {
                            while in_flight.len() < MAX_CONCURRENCY {
                                match pending.pop_front() {
                                    Some(caps) => in_flight.push(async move {
                                        FetchSim(FETCH_POLLS).await;
                                        caps
                                    }),
                                    None => break,
                                }
                            }
                            mif.fetch_max(in_flight.len(), Ordering::SeqCst);

                            tokio::select! {
                                biased;
                                Some(caps) = in_flight.next(), if !in_flight.is_empty() => {
                                    let [c0, _c1] = caps;
                                    fetched_out.give(&c0, ());
                                }
                                event = descs_input.next(), if !input_done => {
                                    match event {
                                        Some(Event::Data(caps, data)) => {
                                            let n = data.len();
                                            mppe.fetch_max(n, Ordering::SeqCst);
                                            re.fetch_add(1, Ordering::SeqCst);
                                            for _ in data {
                                                pending.push_back(caps.clone());
                                            }
                                        }
                                        Some(Event::Progress(_)) => {}
                                        None => input_done = true,
                                    }
                                }
                                else => break,
                            }
                        }
                    });
                    completed_stream.connect_loop(fb_handle);

                    (
                        producer_button.press_on_drop(),
                        fetch_button.press_on_drop(),
                    )
                })
            });

            for _ in 0..200_000 {
                worker.step();
            }
            drop(tokens);
        })
        .expect("timely panicked");

        let mif = max_in_flight.load(Ordering::SeqCst);
        let mppe = max_parts_per_event.load(Ordering::SeqCst);
        let re = recv_events.load(Ordering::SeqCst);
        eprintln!(
            "MICROREPRO DISTINCT-TIMES RESULT: max_in_flight={mif} \
             max_parts_per_event={mppe} recv_events={re} (N={N}, cap={MAX_CONCURRENCY})"
        );
    }
}
