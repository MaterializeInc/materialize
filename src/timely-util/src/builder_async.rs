// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types to build async operators with general shapes.

use std::cell::RefCell;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use differential_dataflow::operators::arrange::agent::ShutdownButton;
use futures_util::task::ArcWake;
use futures_util::FutureExt;
use polonius_the_crab::{polonius, WithLifetime};
use timely::communication::message::RefOrMut;
use timely::communication::{Pull, Push};
use timely::dataflow::channels::pact::ParallelizationContractCore;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::pushers::counter::CounterCore as PushCounter;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::channels::BundleCore;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::generic::{
    InputHandleCore, OperatorInfo, OutputHandleCore, OutputWrapper,
};
use timely::dataflow::operators::{Capability, InputCapability};
use timely::dataflow::{Scope, StreamCore};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::{Activator, SyncActivator};
use timely::{Container, Data, PartialOrder};

/// Builds async operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    builder: OperatorBuilderRc<G>,
    /// Frontier information of input streams shared with the handles. Each frontier is paired with
    /// a flag indicating whether or not the input handle has seen the updated frontier.
    shared_frontiers: Rc<RefCell<Vec<(Antichain<G::Timestamp>, bool)>>>,
    /// Wakers registered by input handles
    registered_wakers: Rc<RefCell<Vec<Waker>>>,
    /// The activator for this operator
    activator: Activator,
    /// The waker set up to activate this timely operator when woken
    operator_waker: Arc<TimelyWaker>,
    /// Holds type erased closures that drain an input handle when called. These handles will be
    /// automatically drained when the operator is scheduled and the logic future has exited
    drain_pipe: Rc<RefCell<Vec<Box<dyn FnMut()>>>>,
    /// Holds type erased closures that flush an output handle when called. These handles will be
    /// automatically drained when the operator is scheduled after the logic future has been polled
    output_flushes: Vec<Box<dyn FnMut()>>,
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
            arc_self.activator.activate().unwrap();
        }
    }
}

/// Async handle to an operator's input stream
pub struct AsyncInputHandle<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>> + 'static> {
    state: NextFutState<T, D, P>,
    /// A scratch container used to gain mutable access to batches
    buffer: D,
}

/// This struct holds the state that is captured by the `NextFut` future. This definition is
/// mandatory for the implementation of `AsyncInputHandle::next_mut` which needs to perform a
/// disjoint capture on this state and the buffer that is about to be swapped.
struct NextFutState<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>> + 'static> {
    /// The underying synchronous input handle
    sync_handle: ManuallyDrop<InputHandleCore<T, D, P>>,
    /// Frontier information of input streams shared with the operator. Each frontier is paired
    /// with a flag indicating whether or not the handle has seen the updated frontier.
    shared_frontiers: Rc<RefCell<Vec<(Antichain<T>, bool)>>>,
    /// The index of this handle into the shared frontiers vector
    index: usize,
    /// Reference to the reactor queue of this input handle where Wakers can be registered
    reactor_registry: Weak<RefCell<Vec<Waker>>>,
    /// Holds type erased closures that should drain a handle when called. These handles will be
    /// automatically drained when the operator is scheduled and the logic future has exited
    drain_pipe: Rc<RefCell<Vec<Box<dyn FnMut()>>>>,
}

impl<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>> + 'static> AsyncInputHandle<T, D, P> {
    /// Produces a future that will resolve to the next event of this input stream with shared
    /// access to the data.
    ///
    /// # Cancel safety
    ///
    /// The returned future is cancel-safe
    pub fn next(&mut self) -> impl Future<Output = Option<Event<T, &D>>> + '_ {
        let fut = NextFut {
            state: Some(&mut self.state),
        };
        fut.map(|event| {
            Some(match event? {
                Event::Data(cap, data) => match data {
                    RefOrMut::Ref(data) => Event::Data(cap, data),
                    RefOrMut::Mut(data) => Event::Data(cap, &*data),
                },
                Event::Progress(frontier) => Event::Progress(frontier),
            })
        })
    }

    /// Produces a future that will resolve to the next event of this input stream with mutable
    /// access to the data.
    ///
    /// # Cancel safety
    ///
    /// The returned future is cancel-safe
    pub fn next_mut(&mut self) -> impl Future<Output = Option<Event<T, &mut D>>> + '_ {
        let fut = NextFut {
            state: Some(&mut self.state),
        };
        fut.map(|event| {
            Some(match event? {
                Event::Data(cap, data) => {
                    data.swap(&mut self.buffer);
                    Event::Data(cap, &mut self.buffer)
                }
                Event::Progress(frontier) => Event::Progress(frontier),
            })
        })
    }
}

impl<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>> + 'static> Drop
    for NextFutState<T, D, P>
{
    fn drop(&mut self) {
        // SAFETY: We're in a Drop impl so this runs only once
        let mut sync_handle = unsafe { ManuallyDrop::take(&mut self.sync_handle) };
        self.drain_pipe
            .borrow_mut()
            .push(Box::new(move || sync_handle.for_each(|_, _| {})));
    }
}

/// The future returned by `AsyncInputHandle::next`
struct NextFut<'handle, T: Timestamp, D: Container, P: Pull<BundleCore<T, D>> + 'static> {
    state: Option<&'handle mut NextFutState<T, D, P>>,
}

/// An event of an input stream
pub enum Event<T: Timestamp, D> {
    /// A data event
    Data(InputCapability<T>, D),
    /// A progress event
    Progress(Antichain<T>),
}

impl<'handle, T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>> Future
    for NextFut<'handle, T, D, P>
{
    type Output = Option<Event<T, RefOrMut<'handle, D>>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let state: &'handle mut NextFutState<T, D, P> =
            self.state.take().expect("future polled after completion");

        // Check for frontier notifications first, to urge operators to retire pending work
        let mut shared_frontiers = state.shared_frontiers.borrow_mut();
        let (ref frontier, ref mut pending) = shared_frontiers[state.index];
        if *pending {
            *pending = false;
            return Poll::Ready(Some(Event::Progress(frontier.clone())));
        } else if frontier.is_empty() {
            // If the frontier is empty and is not pending it means that there is no more data left
            println!("EMPTY FRONTIER");
            return Poll::Ready(None);
        };
        drop(shared_frontiers);

        // This type serves as a type-level function that "shows" the `polonius` function how to
        // create a version of the output type with a specific lifetime 'lt. It does this by
        // implementing the `WithLifetime` trait for all lifetimes 'lt and setting the associated
        // type to the output type with all lifetimes set to 'lt.
        type NextHTB<T, D> =
            dyn for<'lt> WithLifetime<'lt, T = (InputCapability<T>, RefOrMut<'lt, D>)>;

        // The polonius function encodes a safe but rejected pattern by the current borrow checker.
        // Explaining is beyond the scope of this comment but the docs have a great explanation:
        // https://docs.rs/polonius-the-crab/latest/polonius_the_crab/index.html
        match polonius::<NextHTB<T, D>, _, _, _>(state, |state| state.sync_handle.next().ok_or(()))
        {
            Ok((cap, data)) => Poll::Ready(Some(Event::Data(cap, data))),
            Err((state, ())) => {
                // Nothing else to produce so install the provided waker in the reactor
                state
                    .reactor_registry
                    .upgrade()
                    .expect("handle outlived its operator")
                    .borrow_mut()
                    .push(cx.waker().clone());
                self.state = Some(state);
                Poll::Pending
            }
        }
    }
}

// TODO: delete and use CapabilityTrait instead once TimelyDataflow/timely-dataflow#512 gets merged
pub trait CapabilityTrait<T: Timestamp> {
    fn session<'a, D, P>(
        &'a self,
        handle: &'a mut OutputHandleCore<'_, T, D, P>,
    ) -> Session<'a, T, D, PushCounter<T, D, P>>
    where
        D: Container,
        P: Push<BundleCore<T, D>>;
}

impl<T: Timestamp> CapabilityTrait<T> for InputCapability<T> {
    #[inline]
    fn session<'a, D, P>(
        &'a self,
        handle: &'a mut OutputHandleCore<'_, T, D, P>,
    ) -> Session<'a, T, D, PushCounter<T, D, P>>
    where
        D: Container,
        P: Push<BundleCore<T, D>>,
    {
        handle.session(self)
    }
}

impl<T: Timestamp> CapabilityTrait<T> for Capability<T> {
    #[inline]
    fn session<'a, D, P>(
        &'a self,
        handle: &'a mut OutputHandleCore<'_, T, D, P>,
    ) -> Session<'a, T, D, PushCounter<T, D, P>>
    where
        D: Container,
        P: Push<BundleCore<T, D>>,
    {
        handle.session(self)
    }
}

pub struct AsyncOutputHandle<T: Timestamp, D: Container, P: Push<BundleCore<T, D>> + 'static> {
    // The field order is important here as the handle is borrowing from the wrapper. See also the
    // safety argument in the constructor
    handle: Rc<RefCell<OutputHandleCore<'static, T, D, P>>>,
    wrapper: Rc<Pin<Box<OutputWrapper<T, D, P>>>>,
}

impl<T, D, P> AsyncOutputHandle<T, D, P>
where
    T: Timestamp,
    D: Container,
    P: Push<BundleCore<T, D>> + 'static,
{
    fn new(wrapper: OutputWrapper<T, D, P>) -> Self {
        let mut wrapper = Box::pin(wrapper);
        // SAFETY:
        // get_unchecked_mut is safe because we are not moving the wrapper
        //
        // transmute is safe because:
        // * We're erasing the lifetime but we guarantee through field order that the handle will
        //   be dropped before the wrapper, thus manually enforcing the lifetime.
        // * We never touch wrapper again after this point
        let handle = unsafe {
            let handle = wrapper.as_mut().get_unchecked_mut().activate();
            std::mem::transmute::<OutputHandleCore<'_, T, D, P>, OutputHandleCore<'static, T, D, P>>(
                handle,
            )
        };
        Self {
            handle: Rc::new(RefCell::new(handle)),
            wrapper: Rc::new(wrapper),
        }
    }

    #[allow(clippy::unused_async)]
    #[inline]
    pub async fn give_container<C: CapabilityTrait<T>>(&mut self, cap: &C, container: &mut D) {
        let mut handle = self.handle.borrow_mut();
        cap.session(&mut handle).give_container(container);
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
    pub async fn give<C: CapabilityTrait<T>>(&mut self, cap: &C, data: D) {
        let mut handle = self.handle.borrow_mut();
        cap.session(&mut handle).give(data);
    }
}

impl<T: Timestamp, D: Container, P: Push<BundleCore<T, D>> + 'static> Clone
    for AsyncOutputHandle<T, D, P>
{
    fn clone(&self) -> Self {
        Self {
            handle: Rc::clone(&self.handle),
            wrapper: Rc::clone(&self.wrapper),
        }
    }
}

impl<G: Scope> OperatorBuilder<G> {
    /// Allocates a new generic async operator builder from its containing scope.
    pub fn new(name: String, scope: G) -> Self {
        let builder = OperatorBuilderRc::new(name, scope.clone());
        let info = builder.operator_info();
        let activator = scope.activator_for(&info.address);
        let sync_activator = scope.sync_activator_for(&info.address);
        let operator_waker = TimelyWaker {
            activator: sync_activator,
            active: AtomicBool::new(false),
            task_ready: AtomicBool::new(true),
        };

        OperatorBuilder {
            builder,
            shared_frontiers: Default::default(),
            registered_wakers: Default::default(),
            activator,
            operator_waker: Arc::new(operator_waker),
            drain_pipe: Default::default(),
            output_flushes: Default::default(),
        }
    }

    /// Adds a new input, returning the async input handle to use.
    pub fn new_input<D: Container, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
    ) -> AsyncInputHandle<G::Timestamp, D, P::Puller>
    where
        P: ParallelizationContractCore<G::Timestamp, D>,
    {
        let connection =
            vec![Antichain::from_elem(Default::default()); self.builder.shape().outputs()];
        self.new_input_connection(stream, pact, connection)
    }

    /// Adds a new input with connection information, returning the async input handle to use.
    ///
    /// The `connection` parameter contains promises made by the operator for each of the existing
    /// *outputs*, that any timestamp appearing at the input, any output timestamp will be greater
    /// than or equal to the input timestamp subjected to a `Summary` greater or equal to some
    /// element of the corresponding antichain in `connection`.
    ///
    /// Commonly the connections are either the unit summary, indicating the same timestamp might
    /// be produced as output, or an empty antichain indicating that there is no connection from
    /// the input to the output.
    pub fn new_input_connection<D: Container, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
        connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>,
    ) -> AsyncInputHandle<G::Timestamp, D, P::Puller>
    where
        P: ParallelizationContractCore<G::Timestamp, D>,
    {
        let index = self.builder.shape().inputs();
        self.shared_frontiers
            .borrow_mut()
            .push((Antichain::from_elem(G::Timestamp::minimum()), false));

        let sync_handle = self.builder.new_input_connection(stream, pact, connection);

        AsyncInputHandle {
            state: NextFutState {
                sync_handle: ManuallyDrop::new(sync_handle),
                shared_frontiers: Rc::clone(&self.shared_frontiers),
                reactor_registry: Rc::downgrade(&self.registered_wakers),
                index,
                drain_pipe: Rc::clone(&self.drain_pipe),
            },
            buffer: D::default(),
        }
    }

    /// Adds a new output, returning the output handle and stream.
    pub fn new_output<D: Container>(
        &mut self,
    ) -> (
        AsyncOutputHandle<G::Timestamp, D, TeeCore<G::Timestamp, D>>,
        StreamCore<G, D>,
    ) {
        let connection =
            vec![Antichain::from_elem(Default::default()); self.builder.shape().inputs()];
        self.new_output_connection(connection)
    }

    /// Adds a new output with connetion information, returning the output handle and stream.
    ///
    /// The `connection` parameter contains promises made by the operator for each of the existing
    /// *inputs*, that any timestamp appearing at the input, any output timestamp will be greater
    /// than or equal to the input timestamp subjected to a `Summary` greater or equal to some
    /// element of the corresponding antichain in `connection`.
    ///
    /// Commonly the connections are either the unit summary, indicating the same timestamp might
    /// be produced as output, or an empty antichain indicating that there is no connection from
    /// the input to the output.
    pub fn new_output_connection<D: Container>(
        &mut self,
        connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>,
    ) -> (
        AsyncOutputHandle<G::Timestamp, D, TeeCore<G::Timestamp, D>>,
        StreamCore<G, D>,
    ) {
        let (wrapper, stream) = self.builder.new_output_connection(connection);

        let handle = AsyncOutputHandle::new(wrapper);

        let mut flush_handle = handle.clone();
        self.output_flushes
            .push(Box::new(move || flush_handle.cease()));

        (handle, stream)
    }

    /// Creates an operator implementation from supplied logic constructor. It returns a shutdown
    /// button that when pressed it will cause the logic future to be dropped and input handles to
    /// be drained. The button can be converted into a token by using
    /// [`ShutdownButton::press_on_drop`]
    pub fn build<B, L>(self, constructor: B) -> ShutdownButton<()>
    where
        B: FnOnce(Vec<Capability<G::Timestamp>>) -> L,
        L: Future + 'static,
    {
        let operator_waker = self.operator_waker;
        let registered_wakers = self.registered_wakers;
        let shared_frontiers = self.shared_frontiers;
        let drain_pipe = self.drain_pipe;
        let mut output_flushes = self.output_flushes;
        let token = Rc::new(RefCell::new(Some(())));
        let button = ShutdownButton::new(Rc::clone(&token), self.activator);
        self.builder.build_reschedule(move |caps| {
            let mut logic_fut = Some(Box::pin(constructor(caps)));
            move |new_frontiers| {
                // First, discover if there are any frontier notifications
                {
                    let mut old_frontiers = shared_frontiers.borrow_mut();
                    for ((old, pending), new) in old_frontiers.iter_mut().zip(new_frontiers) {
                        if PartialOrder::less_than(&old.borrow(), &new.frontier()) {
                            *old = new.frontier().to_owned();
                            *pending = true;
                        }
                    }
                }
                // Then, wake up any registered Wakers for the input streams while taking care to
                // not reactivate the timely operator since that would lead to an infinite loop.
                // This ensures that the input handles wake up properly when managed by other
                // executors, e.g a `select!` that provides its own Waker implementation.
                {
                    operator_waker.active.store(true, Ordering::SeqCst);
                    let mut registered_wakers = registered_wakers.borrow_mut();
                    for waker in registered_wakers.drain(..) {
                        waker.wake();
                    }
                    operator_waker.active.store(false, Ordering::SeqCst);
                }

                // If the shutdown button got pressed we should immediately drop the logic future
                // which will also register all the handles for drainage
                if token.borrow().is_none() {
                    logic_fut = None;
                }

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
                    // Othewise we should drain any dropped handles
                    let mut drains = drain_pipe.borrow_mut();
                    for drain in drains.iter_mut() {
                        (drain)()
                    }
                    false
                }
            }
        });

        button
    }

    /// Creates a fallible operator implementation from supplied logic constructor. If the `Future`
    /// resolves to an error it will be emitted in the returned error stream and then the operator
    /// will wait indefinitely until the shutdown button is pressed.
    ///
    /// # Capability handling
    ///
    /// Unlike [`OperatorBuilder::build`], this method does not give owned capabilities to the
    /// constructor. All initial capabilities are wrapped in an `Option` and a mutable reference to
    /// them is given instead. This is done to avoid storing owned capabilities in the state of the
    /// logic future which would make using the `?` operator unsafe, since the frontiers would
    /// incorrectly advance, potentially causing incorrect actions downstream.
    ///
    /// ```ignore
    /// builder.build_fallible(|caps| Box::pin(async move {
    ///     // Assert that we have the number of capabilities we expect
    ///     // `cap` will be a `&mut Option<Capability<T>>`:
    ///     let [cap]: &mut [_; 1] = caps.try_into().unwrap();
    ///
    ///     // Using cap to send data:
    ///     output.give(cap.as_ref().unwrap(), 42);
    ///
    ///     // Using cap to downgrade it:
    ///     cap.as_mut().unwrap().downgrade();
    ///
    ///     // Explicitly dropping the capability:
    ///     // Simply running `drop(cap)` will only drop the reference and not the capability itself!
    ///     *cap = None;
    ///
    ///     // !! BIG WARNING !!:
    ///     // It is tempting to `take` the capability out of the option for convenience. This will
    ///     // move the capability into the future state, tying its lifetime to it, which will get
    ///     // dropped when an error is hit, causing incorrect progress statements.
    ///     let cap = cap.take().unwrap(); // DO NOT DO THIS
    /// }));
    /// ```
    pub fn build_fallible<E: 'static, F>(
        mut self,
        constructor: F,
    ) -> (ShutdownButton<()>, StreamCore<G, Vec<Rc<E>>>)
    where
        F: for<'a> FnOnce(
                &'a mut [Option<Capability<G::Timestamp>>],
            ) -> Pin<Box<dyn Future<Output = Result<(), E>> + 'a>>
            + 'static,
    {
        // Create a new completely disconnected output
        let disconnected = vec![Antichain::new(); self.builder.shape().inputs()];
        let (mut error_output, error_stream) = self.new_output_connection(disconnected);
        let button = self.build(|mut caps| async move {
            let error_cap = caps.pop().unwrap();
            let mut caps = caps.into_iter().map(Some).collect::<Vec<_>>();
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, ToStream};

    use super::*;

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
    async fn async_operator() {
        // Run timely in a separate thread
        #[allow(clippy::disallowed_methods)]
        let extracted = tokio::task::spawn_blocking(|| {
            let capture = timely::example(|scope| {
                let input = (0..10).to_stream(scope);

                let mut op = OperatorBuilder::new("async_passthru".to_string(), input.scope());
                let mut input_handle = op.new_input(&input, Pipeline);
                let (mut output, output_stream) = op.new_output();

                op.build(move |_capabilities| async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    while let Some(event) = input_handle.next().await {
                        match event {
                            Event::Data(cap, data) => {
                                let cap = cap.retain();
                                for item in data.iter().copied() {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                    output.give(&cap, item).await;
                                }
                            }
                            Event::Progress(_frontier) => {}
                        }
                    }
                });

                output_stream.capture()
            });
            capture.extract()
        })
        .await
        .expect("timely panicked");

        assert_eq!(extracted, vec![(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]);
    }
}
