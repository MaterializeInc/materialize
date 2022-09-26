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
use std::mem::{transmute, transmute_copy};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures_util::task::ArcWake;
use timely::communication::{message::RefOrMut, Pull};
use timely::dataflow::channels::pact::ParallelizationContractCore;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::channels::BundleCore;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::generic::{InputHandleCore, OperatorInfo, OutputWrapper};
use timely::dataflow::operators::{Capability, CapabilityRef};
use timely::dataflow::{Scope, StreamCore};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::SyncActivator;
use timely::{Container, PartialOrder};

/// Builds async operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    builder: OperatorBuilderRc<G>,
    /// Frontier information of input streams shared with the handles. Each frontier is paired with
    /// a flag indicating whether or not the input handle has seen the updated frontier.
    shared_frontiers: Rc<RefCell<Vec<(Antichain<G::Timestamp>, bool)>>>,
    /// Wakers registered by input handles
    registered_wakers: Rc<RefCell<Vec<Waker>>>,
    /// The waker set up to activate this timely operator when woken
    operator_waker: Arc<TimelyWaker>,
}

/// An async Waker that activates a specific operator when woken and marks the task as ready
struct TimelyWaker {
    activator: SyncActivator,
    active: AtomicBool,
    task_ready: AtomicBool,
}

impl ArcWake for TimelyWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        // Only activate the timely operator if it's not already active to avoid an infinite loop
        if !arc_self.active.load(Ordering::SeqCst) {
            arc_self.activator.activate().unwrap();
        }
        arc_self.task_ready.store(true, Ordering::SeqCst);
    }
}

/// Async handle to an operator's input stream
pub struct AsyncInputHandle<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>> {
    /// The underying synchronous input handle
    sync_handle: InputHandleCore<T, D, P>,
    /// Frontier information of input streams shared with the operator. Each frontier is paired
    /// with a flag indicating whether or not the handle has seen the updated frontier.
    shared_frontiers: Rc<RefCell<Vec<(Antichain<T>, bool)>>>,
    /// The index of this handle into the shared frontiers vector
    index: usize,
    /// Reference to the reactor queue of this input handle where Wakers can be registered
    reactor_registry: Weak<RefCell<Vec<Waker>>>,
}

impl<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>> AsyncInputHandle<T, D, P> {
    /// Produces a future that will resolve to the next event of this input stream
    ///
    /// # Cancel safety
    ///
    /// The returned future is cancel-safe
    pub fn next(&mut self) -> impl Future<Output = Option<Event<'_, T, D>>> + '_ {
        NextFut { handle: Some(self) }
    }

    fn next_sync<'handle>(
        &'handle mut self,
    ) -> Option<(
        ManuallyDrop<CapabilityRef<'handle, T>>,
        RefOrMut<'handle, D>,
    )> {
        self.sync_handle
            .next()
            .map(|(cap, data)| (ManuallyDrop::new(cap), data))
    }
}

/// The future returned by `AsyncInputHandle::next`
struct NextFut<'handle, T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>> {
    handle: Option<&'handle mut AsyncInputHandle<T, D, P>>,
}

/// An event of an input stream
pub enum Event<'a, T: Timestamp, D> {
    /// A data event
    Data(CapabilityRef<'a, T>, RefOrMut<'a, D>),
    /// A progress event
    Progress(Antichain<T>),
}

impl<'handle, T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>> Future
    for NextFut<'handle, T, D, P>
{
    type Output = Option<Event<'handle, T, D>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let handle: &'handle mut AsyncInputHandle<T, D, P> =
            self.handle.take().expect("future polled after completion");
        match AsyncInputHandle::next_sync(handle) {
            Some((cap, data)) => {
                // SAFETY: This is necessary only due to a limitation in the borrow checker. Once
                // Rust starts using the polonius borrow checker the transmute can be removed.
                //
                // We have created a `&'handle mut AsyncInputHandle` value on the stack by taking
                // the handle out of the Option stored in our state. This value is then used as-is
                // to call `AsyncInputHandle::next_sync`, with no auto-deref happening, whose
                // function signature is:
                // for<'a> fn(&'a mut AsyncInputHandle) -> Option<(CapabilityRef<'a, T>, RefOrMut<'a, D>)>
                //
                // Therefore, by calling it with our `'handle` lifetimed handle we get back
                // `'handle` lifetimed capability and data.
                //
                // An additional argument for safety is that the current borrow checker is happy
                // accepting the body of this function without transmutes when either the Some or
                // None branch is replaced by a panic!(). Therefore each individual path is safe on
                // its own and in the full version we only ever call one of them, therefore the
                // composition is also safe.
                //
                // ## Safety of ManuallyDrop
                // `next_sync` wraps the returned `CapabilityRef` into a `ManuallyDrop` in order to
                // not interfere with the borrow checker that will otherwise infer that `cap` may
                // live until the end of this function and doesn't compile even when transmuting.
                //
                // ## Safety of transmute_copy
                //
                // We are forced to use `transmute_copy` and not a simple transmute because the
                // compiler cannot understand that the two types have the same size, even though
                // they clearly do, since they only differ in their lifetime. `transmute_copy` will
                // create a bit copy of the capability and at the same time remove the wrapping
                // `ManuallyDrop`. This is safe because `ManuallyDrop` is marked as
                // `#[repr(transparent)]`, therefore it has the same layout as the T it wraps.
                // Finally we don't need to care about running mem::forget() on the original cap
                // that we created a copy out because it is wrapped in `ManuallyDrop` so no
                // destructor will run.
                //
                let cap = unsafe {
                    transmute_copy::<ManuallyDrop<CapabilityRef<'_, T>>, CapabilityRef<'handle, T>>(
                        &cap,
                    )
                };
                let data = unsafe { transmute::<RefOrMut<'_, D>, RefOrMut<'handle, D>>(data) };

                Poll::Ready(Some(Event::Data(cap, data)))
            }
            None => {
                // There is no more data but there may be a pending frontier notification
                let mut shared_frontiers = handle.shared_frontiers.borrow_mut();
                let (ref frontier, ref mut pending) = shared_frontiers[handle.index];
                if *pending {
                    *pending = false;
                    Poll::Ready(Some(Event::Progress(frontier.clone())))
                } else if frontier.is_empty() {
                    // If the frontier is empty and is not pending it means that there is no more
                    // data left in this input stream
                    Poll::Ready(None)
                } else {
                    drop(shared_frontiers);
                    // Nothing else to produce so install the provided waker in the reactor
                    handle
                        .reactor_registry
                        .upgrade()
                        .expect("handle outlived its operator")
                        .borrow_mut()
                        .push(cx.waker().clone());
                    self.handle = Some(handle);
                    Poll::Pending
                }
            }
        }
    }
}

impl<G: Scope> OperatorBuilder<G> {
    /// Allocates a new generic async operator builder from its containing scope.
    pub fn new(name: String, scope: G) -> Self {
        let builder = OperatorBuilderRc::new(name, scope.clone());
        let info = builder.operator_info();
        let activator = scope.sync_activator_for(&info.address);
        let operator_waker = TimelyWaker {
            activator,
            active: AtomicBool::new(false),
            task_ready: AtomicBool::new(true),
        };

        OperatorBuilder {
            builder,
            shared_frontiers: Default::default(),
            registered_wakers: Default::default(),
            operator_waker: Arc::new(operator_waker),
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

        AsyncInputHandle {
            sync_handle: self.builder.new_input_connection(stream, pact, connection),
            shared_frontiers: Rc::clone(&self.shared_frontiers),
            reactor_registry: Rc::downgrade(&self.registered_wakers),
            index,
        }
    }

    /// Adds a new output, returning the output handle and stream.
    pub fn new_output<D: Container>(
        &mut self,
    ) -> (
        OutputWrapper<G::Timestamp, D, TeeCore<G::Timestamp, D>>,
        StreamCore<G, D>,
    ) {
        self.builder.new_output()
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
        OutputWrapper<G::Timestamp, D, TeeCore<G::Timestamp, D>>,
        StreamCore<G, D>,
    ) {
        self.builder.new_output_connection(connection)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<B, L>(self, constructor: B)
    where
        B: FnOnce(Vec<Capability<G::Timestamp>>) -> L,
        L: Future + 'static,
    {
        let operator_waker = self.operator_waker;
        let registered_wakers = self.registered_wakers;
        let shared_frontiers = self.shared_frontiers;
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
                    }
                }
                // The timely operator needs to be kept alive if the task is pending
                logic_fut.is_some()
            }
        })
    }

    /// Creates operator info for the operator.
    pub fn operator_info(&self) -> OperatorInfo {
        self.builder.operator_info()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::time::Duration;

    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, ToStream};

    #[tokio::test]
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
                                    let mut output_handle = output.activate();
                                    let mut session = output_handle.session(&cap);
                                    session.give(item);
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
