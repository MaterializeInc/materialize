// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types to build async operators with general shapes.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures_util::task::ArcWake;
use timely::communication::{Message, Pull, Push};
use timely::dataflow::channels::pact::ParallelizationContractCore;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::channels::BundleCore;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::generic::{
    InputHandleCore, OperatorInfo, OutputHandleCore, OutputWrapper,
};
use timely::dataflow::operators::{Capability, DowngradeError, InputCapability};
use timely::dataflow::{Scope, StreamCore};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::{Activator, SyncActivator};
use timely::{Container, Data, PartialOrder};

/// Builds async operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    builder: OperatorBuilderRc<G>,
    /// The activator for this operator
    activator: Activator,
    /// The waker set up to activate this timely operator when woken
    operator_waker: Arc<TimelyWaker>,
    /// The currently known upper frontier of each of the input handles.
    input_frontiers: Vec<Antichain<G::Timestamp>>,
    /// Input queues for each of the declared inputs of the operator.
    input_queues: Vec<Box<dyn InputQueue<G::Timestamp>>>,
    /// Holds type erased closures that flush an output handle when called. These handles will be
    /// automatically drained when the operator is scheduled after the logic future has been polled
    output_flushes: Vec<Box<dyn FnMut()>>,
    /// A list of capabilities that the operator logic dropped.
    capability_drops: Rc<RefCell<Vec<Capability<G::Timestamp>>>>,
    /// A handle to check whether all workers have pressed the shutdown button.
    shutdown_handle: ButtonHandle,
    /// A button to coordinate shutdown of this operator among workers.
    shutdown_button: Button,
}

/// A helper trait abstracting over an input handle. It facilitates keeping around type erased
/// handles for each of the operator inputs.
trait InputQueue<T: Timestamp> {
    /// Accepts all available input into local queues.
    fn accept_input(&mut self, drop_queue: &Rc<RefCell<Vec<Capability<T>>>>);

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
    P: Pull<BundleCore<T, D>> + 'static,
{
    fn accept_input(&mut self, drop_queue: &Rc<RefCell<Vec<Capability<T>>>>) {
        let mut queue = self.queue.borrow_mut();
        let mut new_data = false;
        while let Some((cap, data)) = self.handle.next() {
            new_data = true;
            let cap = self.connection.accept(cap, drop_queue);
            queue.push_back(Event::Data(cap, data.take()));
        }
        if new_data {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
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
    P: Pull<BundleCore<T, D>> + 'static,
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
    /// Produces a future that will resolve to the next event of this input stream.
    ///
    /// # Cancel safety
    ///
    /// The returned future is cancel-safe
    pub async fn next(&mut self) -> Option<Event<T, C::Capability, D>> {
        std::future::poll_fn(|cx| {
            if self.done {
                return Poll::Ready(None);
            }
            let mut queue = self.queue.borrow_mut();
            match queue.pop_front() {
                Some(event @ Event::Data(_, _)) => Poll::Ready(Some(event)),
                Some(Event::Progress(frontier)) => {
                    self.done = frontier.is_empty();
                    Poll::Ready(Some(Event::Progress(frontier)))
                }
                None => {
                    // Nothing else to produce so install the provided waker
                    self.waker.set(Some(cx.waker().clone()));
                    Poll::Pending
                }
            }
        })
        .await
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

pub struct AsyncOutputHandle<T: Timestamp, D: Container, P: Push<BundleCore<T, D>> + 'static> {
    // The field order is important here as the handle is borrowing from the wrapper. See also the
    // safety argument in the constructor
    handle: Rc<RefCell<OutputHandleCore<'static, T, D, P>>>,
    wrapper: Rc<Pin<Box<OutputWrapper<T, D, P>>>>,
    index: usize,
}

impl<T, D, P> AsyncOutputHandle<T, D, P>
where
    T: Timestamp,
    D: Container,
    P: Push<BundleCore<T, D>> + 'static,
{
    fn new(wrapper: OutputWrapper<T, D, P>, index: usize) -> Self {
        let mut wrapper = Rc::new(Box::pin(wrapper));
        // SAFETY:
        // get_unchecked_mut is safe because we are not moving the wrapper
        //
        // transmute is safe because:
        // * We're erasing the lifetime but we guarantee through field order that the handle will
        //   be dropped before the wrapper, thus manually enforcing the lifetime.
        // * We never touch wrapper again after this point
        let handle = unsafe {
            let handle = Rc::get_mut(&mut wrapper)
                .unwrap()
                .as_mut()
                .get_unchecked_mut()
                .activate();
            std::mem::transmute::<OutputHandleCore<'_, T, D, P>, OutputHandleCore<'static, T, D, P>>(
                handle,
            )
        };
        Self {
            wrapper,
            handle: Rc::new(RefCell::new(handle)),
            index,
        }
    }

    #[allow(clippy::unused_async)]
    #[inline]
    pub async fn give_container(&mut self, cap: &AsyncCapability<T>, container: &mut D) {
        let mut handle = self.handle.borrow_mut();
        handle
            .session(cap.capability.as_ref().unwrap())
            .give_container(container)
    }

    fn cease(&mut self) {
        self.handle.borrow_mut().cease()
    }
}

impl<'a, T, D, P> AsyncOutputHandle<T, Vec<D>, P>
where
    T: Timestamp,
    D: Data,
    P: Push<BundleCore<T, Vec<D>>> + 'static,
{
    #[allow(clippy::unused_async)]
    pub async fn give(&mut self, cap: &AsyncCapability<T>, data: D) {
        let mut handle = self.handle.borrow_mut();
        handle.session(cap.capability.as_ref().unwrap()).give(data)
    }
}

impl<T: Timestamp, D: Container, P: Push<BundleCore<T, D>> + 'static> Clone
    for AsyncOutputHandle<T, D, P>
{
    fn clone(&self) -> Self {
        Self {
            handle: Rc::clone(&self.handle),
            wrapper: Rc::clone(&self.wrapper),
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
    fn accept(
        &self,
        input_cap: InputCapability<T>,
        capability_drops: &Rc<RefCell<Vec<Capability<T>>>>,
    ) -> Self::Capability;
}

/// A marker type representing a disconnected input.
pub struct Disconnected;

impl<T: Timestamp> InputConnection<T> for Disconnected {
    type Capability = T;

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        vec![Antichain::new(); outputs]
    }

    fn accept(
        &self,
        input_cap: InputCapability<T>,
        _: &Rc<RefCell<Vec<Capability<T>>>>,
    ) -> Self::Capability {
        input_cap.time().clone()
    }
}

/// A marker type representing an input connected to exactly one output.
pub struct ConnectedToOne(usize);

impl<T: Timestamp> InputConnection<T> for ConnectedToOne {
    type Capability = AsyncCapability<T>;

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        let mut summary = vec![Antichain::new(); outputs];
        summary[self.0] = Antichain::from_elem(T::Summary::default());
        summary
    }

    fn accept(
        &self,
        input_cap: InputCapability<T>,
        drop_queue: &Rc<RefCell<Vec<Capability<T>>>>,
    ) -> Self::Capability {
        AsyncCapability {
            capability: Some(input_cap.retain_for_output(self.0)),
            drop_queue: Rc::clone(drop_queue),
        }
    }
}

/// A marker type representing an input connected to many outputs.
pub struct ConnectedToMany<const N: usize>([usize; N]);

impl<const N: usize, T: Timestamp> InputConnection<T> for ConnectedToMany<N> {
    type Capability = [AsyncCapability<T>; N];

    fn describe(&self, outputs: usize) -> Vec<Antichain<T::Summary>> {
        let mut summary = vec![Antichain::new(); outputs];
        for output in self.0 {
            summary[output] = Antichain::from_elem(T::Summary::default());
        }
        summary
    }

    fn accept(
        &self,
        input_cap: InputCapability<T>,
        drop_queue: &Rc<RefCell<Vec<Capability<T>>>>,
    ) -> Self::Capability {
        self.0.map(|output| AsyncCapability {
            capability: Some(input_cap.delayed_for_output(input_cap.time(), output)),
            drop_queue: Rc::clone(drop_queue),
        })
    }
}

/// A permission to send data from an async operator. This type differs from the real capability
/// type by the fact that on drop the capability is moved into a drop queue so that the effects of
/// dropping can be deferred to a later time.
#[derive(Clone, Eq, PartialEq)]
pub struct AsyncCapability<T: Timestamp> {
    capability: Option<Capability<T>>,
    drop_queue: Rc<RefCell<Vec<Capability<T>>>>,
}

impl<T: Timestamp> AsyncCapability<T> {
    pub fn time(&self) -> &T {
        self.capability.as_ref().unwrap().time()
    }

    pub fn delayed(&self, time: &T) -> AsyncCapability<T> {
        let capability = self.capability.as_ref().unwrap().delayed(time);
        Self {
            capability: Some(capability),
            drop_queue: Rc::clone(&self.drop_queue),
        }
    }

    pub fn try_delayed(&self, time: &T) -> Option<AsyncCapability<T>> {
        let capability = self.capability.as_ref().unwrap().try_delayed(time)?;
        Some(Self {
            capability: Some(capability),
            drop_queue: Rc::clone(&self.drop_queue),
        })
    }

    pub fn downgrade(&mut self, new_time: &T) {
        self.capability.as_mut().unwrap().downgrade(new_time)
    }

    pub fn try_downgrade(&mut self, new_time: &T) -> Result<(), DowngradeError> {
        self.capability.as_mut().unwrap().try_downgrade(new_time)
    }
}

impl<T: Timestamp> Deref for AsyncCapability<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.capability.as_deref().unwrap()
    }
}

impl<T: Timestamp> fmt::Debug for AsyncCapability<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncCapability")
            .field("time", self.time())
            .finish()
    }
}

impl<T: Timestamp> PartialOrder for AsyncCapability<T> {
    fn less_equal(&self, other: &Self) -> bool {
        PartialOrder::less_equal(
            self.capability.as_ref().unwrap(),
            other.capability.as_ref().unwrap(),
        )
    }
}

impl<T: Timestamp> Drop for AsyncCapability<T> {
    fn drop(&mut self) {
        self.drop_queue
            .borrow_mut()
            .push(self.capability.take().unwrap())
    }
}

/// A permission to send data from an async operator. This type differs from the real capability
/// type by the fact that on drop the capability is moved into a drop queue so that the effects of
/// dropping can be deferred to a later time.
#[derive(Debug)]
pub struct AsyncCapabilitySet<T: Timestamp> {
    elements: Antichain<AsyncCapability<T>>,
}

impl<T: Timestamp> AsyncCapabilitySet<T> {
    pub fn new() -> Self {
        Self {
            elements: Antichain::new(),
        }
    }

    pub fn from_elem(cap: AsyncCapability<T>) -> Self {
        Self {
            elements: Antichain::from_elem(cap),
        }
    }

    pub fn delayed(&self, time: &T) -> AsyncCapability<T> {
        /// Makes the panic branch cold & outlined to decrease code bloat & give
        /// the inner function the best chance possible of being inlined with
        /// minimal code bloat
        #[cold]
        #[inline(never)]
        fn delayed_panic(invalid_time: &dyn fmt::Debug) -> ! {
            // Formatting & panic machinery is relatively expensive in terms of code bloat, so
            // we outline it
            panic!(
                "failed to create a delayed capability, the current set does not \
                have an element less than or equal to {:?}",
                invalid_time,
            )
        }

        self.try_delayed(time)
            .unwrap_or_else(|| delayed_panic(time))
    }

    pub fn try_delayed(&self, time: &T) -> Option<AsyncCapability<T>> {
        self.elements
            .iter()
            .find(|capability| capability.time().less_equal(time))
            .and_then(|capability| capability.try_delayed(time))
    }

    pub fn downgrade<B, F>(&mut self, frontier: F)
    where
        B: std::borrow::Borrow<T>,
        F: IntoIterator<Item = B>,
    {
        /// Makes the panic branch cold & outlined to decrease code bloat & give
        /// the inner function the best chance possible of being inlined with
        /// minimal code bloat
        #[cold]
        #[inline(never)]
        fn downgrade_panic() -> ! {
            // Formatting & panic machinery is relatively expensive in terms of code bloat, so
            // we outline it
            panic!(
                "Attempted to downgrade a CapabilitySet with a frontier containing an element \
                that was not beyond an element within the set"
            )
        }

        self.try_downgrade(frontier)
            .unwrap_or_else(|_| downgrade_panic())
    }

    pub fn try_downgrade<B, F>(&mut self, frontier: F) -> Result<(), ()>
    where
        B: std::borrow::Borrow<T>,
        F: IntoIterator<Item = B>,
    {
        let mut new_elements = Antichain::new();
        for time in frontier.into_iter() {
            let capability = self.try_delayed(time.borrow()).ok_or(())?;
            new_elements.insert(capability);
        }
        self.elements = new_elements;

        Ok(())
    }
}

impl<T: Timestamp> Deref for AsyncCapabilitySet<T> {
    type Target = [AsyncCapability<T>];

    fn deref(&self) -> &Self::Target {
        &*self.elements
    }
}

impl<T: Timestamp> From<Vec<AsyncCapability<T>>> for AsyncCapabilitySet<T> {
    fn from(caps: Vec<AsyncCapability<T>>) -> Self {
        Self {
            elements: caps.into(),
        }
    }
}
/// A helper trait abstracting over an output handle. It facilitates passing type erased
/// output handles during operator construction.
/// It is not meant to be implemented by users.
pub trait OutputIndex {
    /// The output index of this handle.
    fn index(&self) -> usize;
}

impl<T: Timestamp, D: Container> OutputIndex for AsyncOutputHandle<T, D, TeeCore<T, D>> {
    fn index(&self) -> usize {
        self.index
    }
}

impl<G: Scope> OperatorBuilder<G> {
    /// Allocates a new generic async operator builder from its containing scope.
    pub fn new(name: String, mut scope: G) -> Self {
        let builder = OperatorBuilderRc::new(name, scope.clone());
        let info = builder.operator_info();
        let activator = scope.activator_for(&info.address);
        let sync_activator = scope.sync_activator_for(&info.address);
        let operator_waker = TimelyWaker {
            activator: sync_activator,
            active: AtomicBool::new(false),
            task_ready: AtomicBool::new(true),
        };
        let (shutdown_handle, shutdown_button) = button(&mut scope, &info.address);

        OperatorBuilder {
            builder,
            activator,
            operator_waker: Arc::new(operator_waker),
            input_frontiers: Default::default(),
            input_queues: Default::default(),
            output_flushes: Default::default(),
            capability_drops: Default::default(),
            shutdown_handle,
            shutdown_button,
        }
    }

    /// Adds a new input that is connected to the specified output, returning the async input handle to use.
    pub fn new_input_for<D: Container, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
        output: &dyn OutputIndex,
    ) -> AsyncInputHandle<G::Timestamp, D, ConnectedToOne>
    where
        P: ParallelizationContractCore<G::Timestamp, D>,
    {
        let index = output.index();
        assert!(index < self.builder.shape().outputs());
        self.new_input_connection(stream, pact, ConnectedToOne(index))
    }

    /// Adds a new input that is connected to the specified outputs, returning the async input handle to use.
    pub fn new_input_for_many<const N: usize, D: Container, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
        outputs: [&dyn OutputIndex; N],
    ) -> AsyncInputHandle<G::Timestamp, D, ConnectedToMany<N>>
    where
        P: ParallelizationContractCore<G::Timestamp, D>,
    {
        let indices = outputs.map(|output| output.index());
        for index in indices {
            assert!(index < self.builder.shape().outputs());
        }
        self.new_input_connection(stream, pact, ConnectedToMany(indices))
    }

    /// Adds a new input that is not connected to any output, returning the async input handle to use.
    pub fn new_disconnected_input<D: Container, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
    ) -> AsyncInputHandle<G::Timestamp, D, Disconnected>
    where
        P: ParallelizationContractCore<G::Timestamp, D>,
    {
        self.new_input_connection(stream, pact, Disconnected)
    }

    /// Adds a new input with connection information, returning the async input handle to use.
    pub fn new_input_connection<D: Container, P, C>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
        connection: C,
    ) -> AsyncInputHandle<G::Timestamp, D, C>
    where
        P: ParallelizationContractCore<G::Timestamp, D>,
        C: InputConnection<G::Timestamp> + 'static,
    {
        self.input_frontiers
            .push(Antichain::from_elem(G::Timestamp::minimum()));

        let outputs = self.builder.shape().outputs();
        let handle = self
            .builder
            .new_input_connection(stream, pact, connection.describe(outputs));

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
    pub fn new_output<D: Container>(
        &mut self,
    ) -> (
        AsyncOutputHandle<G::Timestamp, D, TeeCore<G::Timestamp, D>>,
        StreamCore<G, D>,
    ) {
        let index = self.builder.shape().outputs();

        let connection = vec![Antichain::new(); self.builder.shape().inputs()];
        let (wrapper, stream) = self.builder.new_output_connection(connection);

        let handle = AsyncOutputHandle::new(wrapper, index);

        let mut flush_handle = handle.clone();
        self.output_flushes
            .push(Box::new(move || flush_handle.cease()));

        (handle, stream)
    }

    /// Creates a fallible operator implementation from supplied logic constructor. If the `Future`
    /// resolves to an error it will be emitted in the returned error stream and then the operator
    /// will wait indefinitely until the shutdown button is pressed.
    pub fn build_fallible<E, B, L>(mut self, constructor: B) -> (Button, StreamCore<G, Vec<Rc<E>>>)
    where
        E: 'static,
        B: FnOnce(Vec<AsyncCapability<G::Timestamp>>) -> L,
        L: Future<Output = Result<(), E>> + 'static,
    {
        // // Create a new completely disconnected output to communicate errors
        let (mut error_output, error_stream) = self.new_output();

        let operator_waker = self.operator_waker;
        let mut input_frontiers = self.input_frontiers;
        let mut input_queues = self.input_queues;
        let mut output_flushes = self.output_flushes;
        let mut capability_drops = self.capability_drops;
        let mut shutdown_handle = self.shutdown_handle;
        self.builder.build_reschedule(move |caps| {
            let caps = caps
                .into_iter()
                .map(|cap| AsyncCapability {
                    capability: Some(cap),
                    drop_queue: Rc::clone(&capability_drops),
                })
                .collect();
            let mut logic_fut = Some(Box::pin(constructor(caps)));
            move |new_frontiers| {
                operator_waker.active.store(true, Ordering::SeqCst);
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
                    queue.accept_input(&capability_drops);
                }
                operator_waker.active.store(false, Ordering::SeqCst);

                // If our worker pressed the button we stop scheduling the logic future and/or
                // draining the input handles to stop producing data and frontier updates
                // downstream.
                if shutdown_handle.local_pressed() {
                    // When all workers press their buttons we drop the logic future and start
                    // draining the input handles.
                    if shutdown_handle.all_pressed() {
                        logic_fut = None;
                        capability_drops.borrow_mut().clear();
                        for queue in input_queues.iter_mut() {
                            queue.drain_input();
                        }
                        false
                    } else {
                        true
                    }
                } else {
                    // Schedule the logic future if any of the wakers above marked the task as ready
                    if let Some(fut) = logic_fut.as_mut() {
                        if operator_waker.task_ready.load(Ordering::SeqCst) {
                            let waker = futures_util::task::waker_ref(&operator_waker);
                            let mut cx = Context::from_waker(&waker);
                            operator_waker.task_ready.store(false, Ordering::SeqCst);
                            match Pin::new(fut).poll(&mut cx) {
                                Poll::Ready(Ok(())) => {
                                    // We're done with logic so deallocate the task
                                    logic_fut = None;
                                    capability_drops.borrow_mut().clear();
                                },
                                Poll::Ready(Err(err)) => {
                                    // An error happened so we need to keep the operator intact and
                                    // ignore all dropped capabilities until the button is pressed.
                                    // error_handle.give()
                                }
                                Poll::Pending => {
                                    capability_drops.borrow_mut().clear();
                                }
                            }
                            // Flush all the outputs before exiting
                            for flush in output_flushes.iter_mut() {
                                (flush)();
                            }
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

        (self.shutdown_button, error_stream)
    }

    /// Creates an operator implementation from supplied logic constructor. It returns a shutdown
    /// button that when pressed it will cause the logic future to be dropped and input handles to
    /// be drained. The button can be converted into a token by using
    /// [`Button::press_on_drop`]
    pub fn build<B, L>(self, constructor: B) -> Button
    where
        B: FnOnce(Vec<AsyncCapability<G::Timestamp>>) -> L,
        L: Future + 'static,
    {
        let (button, _err_stream) = self.build_fallible(move |caps| {
            let logic = constructor(caps);
            async move {
                logic.await;
                Ok::<(), ()>(())
            }
        });

        button
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
pub fn button<G: Scope>(scope: &mut G, addr: &[usize]) -> (ButtonHandle, Button) {
    let index = scope.new_identifier();
    let (pushers, puller) = scope.allocate(index, addr);

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
    puller: Box<dyn Pull<Message<bool>>>,
}

impl ButtonHandle {
    /// Returns whether this worker has pressed its button.
    pub fn local_pressed(&mut self) -> bool {
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
    pushers: Vec<Box<dyn Push<Message<bool>>>>,
    local_pressed: Rc<Cell<bool>>,
}

impl Button {
    /// Presses the button. It is safe to call this function multiple times.
    pub fn press(&mut self) {
        for mut pusher in self.pushers.drain(..) {
            pusher.send(Message::from_typed(true));
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
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, ToStream};
    use timely::WorkerConfig;

    use super::*;

    #[mz_ore::test]
    fn async_operator() {
        let capture = timely::example(|scope| {
            let input = (0..10).to_stream(scope);

            let mut op = OperatorBuilder::new("async_passthru".to_string(), input.scope());
            let (mut output, output_stream) = op.new_output();
            let mut input_handle = op.new_input_for(&input, Pipeline, &output);

            op.build(move |_capabilities| async move {
                tokio::task::yield_now().await;
                while let Some(event) = input_handle.next().await {
                    match event {
                        Event::Data(cap, data) => {
                            for item in data.iter().copied() {
                                tokio::task::yield_now().await;
                                output.give(&cap, item).await;
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
                let (_output, output_stream) = producer.new_output::<Vec<usize>>();
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
                let mut input_handle = consumer.new_disconnected_input(&output_stream, Pipeline);
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
}
