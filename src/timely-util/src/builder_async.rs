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
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures_util::task::ArcWake;
use timely::communication::{Message, Pull, Push};
use timely::container::{CapacityContainerBuilder, ContainerBuilder};
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::pushers::counter::Counter as PushCounter;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::generic::{
    InputHandleCore, OperatorInfo, OutputHandleCore, OutputWrapper,
};
use timely::dataflow::operators::{Capability, CapabilitySet, InputCapability};
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
    /// A handle to check whether all workers have pressed the shutdown button.
    shutdown_handle: ButtonHandle,
    /// A button to coordinate shutdown of this operator among workers.
    shutdown_button: Button,
}

/// A helper trait abstracting over an input handle. It facilitates keeping around type erased
/// handles for each of the operator inputs.
trait InputQueue<T: Timestamp> {
    /// Accepts all available input into local queues.
    fn accept_input(&mut self);

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
    P: Pull<Bundle<T, D>> + 'static,
{
    fn accept_input(&mut self) {
        let mut queue = self.queue.borrow_mut();
        let mut new_data = false;
        while let Some((cap, data)) = self.handle.next() {
            new_data = true;
            let cap = self.connection.accept(cap);
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
    P: Pull<Bundle<T, D>> + 'static,
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

// TODO: delete and use CapabilityTrait instead once TimelyDataflow/timely-dataflow#512 gets merged
pub trait CapabilityTrait<T: Timestamp> {
    fn session<'a, CB, P>(
        &'a self,
        handle: &'a mut OutputHandleCore<'_, T, CB, P>,
    ) -> Session<'a, T, CB, PushCounter<T, CB::Container, P>>
    where
        CB: ContainerBuilder,
        P: Push<Bundle<T, CB::Container>>;
}

impl<T: Timestamp> CapabilityTrait<T> for InputCapability<T> {
    #[inline]
    fn session<'a, CB, P>(
        &'a self,
        handle: &'a mut OutputHandleCore<'_, T, CB, P>,
    ) -> Session<'a, T, CB, PushCounter<T, CB::Container, P>>
    where
        CB: ContainerBuilder,
        P: Push<Bundle<T, CB::Container>>,
    {
        handle.session_with_builder(self)
    }
}

impl<T: Timestamp> CapabilityTrait<T> for Capability<T> {
    #[inline]
    fn session<'a, CB, P>(
        &'a self,
        handle: &'a mut OutputHandleCore<'_, T, CB, P>,
    ) -> Session<'a, T, CB, PushCounter<T, CB::Container, P>>
    where
        CB: ContainerBuilder,
        P: Push<Bundle<T, CB::Container>>,
    {
        handle.session_with_builder(self)
    }
}

pub struct AsyncOutputHandle<
    T: Timestamp,
    CB: ContainerBuilder,
    P: Push<Bundle<T, CB::Container>> + 'static,
> {
    // The field order is important here as the handle is borrowing from the wrapper. See also the
    // safety argument in the constructor
    handle: Rc<RefCell<OutputHandleCore<'static, T, CB, P>>>,
    wrapper: Rc<Pin<Box<OutputWrapper<T, CB, P>>>>,
    index: usize,
}

impl<T, C, P> AsyncOutputHandle<T, CapacityContainerBuilder<C>, P>
where
    T: Timestamp,
    C: Container,
    P: Push<Bundle<T, C>> + 'static,
{
    #[allow(clippy::unused_async)]
    #[inline]
    pub async fn give_container<Cap>(&mut self, cap: &Cap, container: &mut C)
    where
        Cap: CapabilityTrait<T>,
    {
        let mut handle = self.handle.borrow_mut();
        cap.session(&mut handle).give_container(container);
    }
}

impl<T, CB, P> AsyncOutputHandle<T, CB, P>
where
    T: Timestamp,
    CB: ContainerBuilder,
    P: Push<Bundle<T, CB::Container>> + 'static,
{
    fn new(wrapper: OutputWrapper<T, CB, P>, index: usize) -> Self {
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
            std::mem::transmute::<OutputHandleCore<'_, T, CB, P>, OutputHandleCore<'static, T, CB, P>>(
                handle,
            )
        };
        Self {
            wrapper,
            handle: Rc::new(RefCell::new(handle)),
            index,
        }
    }

    fn cease(&mut self) {
        self.handle.borrow_mut().cease()
    }
}

impl<'a, T, D, P> AsyncOutputHandle<T, CapacityContainerBuilder<Vec<D>>, P>
where
    T: Timestamp,
    D: Data,
    P: Push<Bundle<T, Vec<D>>> + 'static,
{
    #[allow(clippy::unused_async)]
    pub async fn give<C: CapabilityTrait<T>>(&mut self, cap: &C, data: D) {
        let mut handle = self.handle.borrow_mut();
        cap.session(&mut handle).give(data);
    }
}

impl<T: Timestamp, CB: ContainerBuilder, P: Push<Bundle<T, CB::Container>> + 'static> Clone
    for AsyncOutputHandle<T, CB, P>
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
        input_cap.retain_for_output(self.0)
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
        self.0
            .map(|output| input_cap.delayed_for_output(input_cap.time(), output))
    }
}

/// A helper trait abstracting over an output handle. It facilitates passing type erased
/// output handles during operator construction.
/// It is not meant to be implemented by users.
pub trait OutputIndex {
    /// The output index of this handle.
    fn index(&self) -> usize;
}

impl<T: Timestamp, CB: ContainerBuilder> OutputIndex
    for AsyncOutputHandle<T, CB, Tee<T, CB::Container>>
{
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
        P: ParallelizationContract<G::Timestamp, D>,
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
        P: ParallelizationContract<G::Timestamp, D>,
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
        P: ParallelizationContract<G::Timestamp, D>,
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
        P: ParallelizationContract<G::Timestamp, D>,
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
    pub fn new_output<CB: ContainerBuilder>(
        &mut self,
    ) -> (
        AsyncOutputHandle<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>,
        StreamCore<G, CB::Container>,
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

    /// Creates an operator implementation from supplied logic constructor. It returns a shutdown
    /// button that when pressed it will cause the logic future to be dropped and input handles to
    /// be drained. The button can be converted into a token by using
    /// [`Button::press_on_drop`]
    pub fn build<B, L>(self, constructor: B) -> Button
    where
        B: FnOnce(Vec<Capability<G::Timestamp>>) -> L,
        L: Future + 'static,
    {
        let operator_waker = self.operator_waker;
        let mut input_frontiers = self.input_frontiers;
        let mut input_queues = self.input_queues;
        let mut output_flushes = self.output_flushes;
        let mut shutdown_handle = self.shutdown_handle;
        self.builder.build_reschedule(move |caps| {
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
                    queue.accept_input();
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
    ) -> (Button, StreamCore<G, Vec<Rc<E>>>)
    where
        F: for<'a> FnOnce(
                &'a mut [CapabilitySet<G::Timestamp>],
            ) -> Pin<Box<dyn Future<Output = Result<(), E>> + 'a>>
            + 'static,
    {
        // Create a new completely disconnected output
        let (mut error_output, error_stream) = self.new_output();
        let button = self.build(|mut caps| async move {
            let error_cap = caps.pop().unwrap();
            let mut caps = caps
                .into_iter()
                .map(CapabilitySet::from_elem)
                .collect::<Vec<_>>();
            if let Err(err) = constructor(&mut *caps).await {
                error_output.give(&error_cap, Rc::new(err)).await;
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
