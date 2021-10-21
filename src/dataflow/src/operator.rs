// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use pin_project::pin_project;
use std::ops::Mul;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{
    operator::{self, Operator},
    InputHandle, OperatorInfo, OutputHandle, OutputWrapper,
};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::Data;

use repr::Diff;

/// Extension methods for timely [`Stream`]s.
pub trait StreamExt<G, D1>
where
    D1: Data,
    G: Scope,
{
    /// Like `timely::dataflow::operators::generic::operator::Operator::unary`,
    /// but the logic function can handle failures.
    ///
    /// Creates a new dataflow operator that partitions its input stream by a
    /// parallelization strategy `pact` and repeatedly invokes `logic`, the
    /// function returned by the function passed as `constructor`. The `logic`
    /// function can read to the input stream and write to either of two output
    /// streams, where the first output stream represents successful
    /// computations and the second output stream represents failed
    /// computations.
    fn unary_fallible<D2, E, B, P>(
        &self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
        ) -> Box<
            dyn FnMut(
                    &mut InputHandle<G::Timestamp, D1, P::Puller>,
                    &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                    &mut OutputHandle<G::Timestamp, E, Tee<G::Timestamp, E>>,
                ) + 'static,
        >,
        P: ParallelizationContract<G::Timestamp, D1>;

    fn unary_async2<D2, B, Fut, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            BundleStream<G::Timestamp, D1, P::Puller, D2, Tee<G::Timestamp, D2>>,
        ) -> Fut,
        Fut: Future<Output = std::convert::Infallible> + 'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    fn unary_async<D2, B, L, Fut, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(
                InputHandle<G::Timestamp, D1, P::Puller>,
                OutputWrapper<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
            ) -> Fut
            + 'static,
        Fut: Future<
            Output = (
                InputHandle<G::Timestamp, D1, P::Puller>,
                OutputWrapper<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
            ),
        >,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Like [`timely::dataflow::operators::map::Map::map`], but `logic`
    /// is allowed to fail. The first returned stream will contain the
    /// successful applications of `logic`, while the second returned stream
    /// will contain the failed applications.
    fn map_fallible<D2, E, L>(&self, name: &str, mut logic: L) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        L: FnMut(D1) -> Result<D2, E> + 'static,
    {
        self.flat_map_fallible(name, move |record| Some(logic(record)))
    }

    /// Like [`timely::dataflow::operators::map::Map::flat_map`], but `logic`
    /// is allowed to fail. The first returned stream will contain the
    /// successful applications of `logic`, while the second returned stream
    /// will contain the failed applications.
    fn flat_map_fallible<D2, E, I, L>(&self, name: &str, logic: L) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static;

    /// Like [`timely::dataflow::operators::filter::Filter::filter`], but `logic`
    /// is allowed to fail. The first returned stream will contain the
    /// elements where `logic` returned `Ok(true)`, and the second returned
    /// stream will contain the errors for elements where `logic` failed.
    /// Elements for which `logic` returned `Ok(false)` are not present in
    /// either stream.
    fn filter_fallible<E, L>(&self, name: &str, mut logic: L) -> (Stream<G, D1>, Stream<G, E>)
    where
        E: Data,
        L: FnMut(&D1) -> Result<bool, E> + 'static,
    {
        self.flat_map_fallible(name, move |record| match logic(&record) {
            Ok(false) => None,
            Ok(true) => Some(Ok(record)),
            Err(err) => Some(Err(err)),
        })
    }

    /// Take a Timely stream and convert it to a Differential stream, where each diff is "1"
    /// and each time is the current Timely timestamp.
    fn pass_through(&self, name: &str) -> Stream<G, (D1, G::Timestamp, Diff)>;
}

/// Extension methods for differential [`Collection`]s.
pub trait CollectionExt<G, D1, R>
where
    G: Scope,
    R: Semigroup,
{
    /// Creates a new empty collection in `scope`.
    fn empty(scope: &G) -> Collection<G, D1, R>;

    /// Like [`Collection::map`], but `logic` is allowed to fail. The first
    /// returned collection will contain successful applications of `logic`,
    /// while the second returned collection will contain the failed
    /// applications.
    fn map_fallible<D2, E, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        L: FnMut(D1) -> Result<D2, E> + 'static,
    {
        self.flat_map_fallible(name, move |record| Some(logic(record)))
    }

    /// Like [`Collection::flat_map`], but `logic` is allowed to fail. The first
    /// returned collection will contain the successful applications of `logic`,
    /// while the second returned collection will contain the failed
    /// applications.
    fn flat_map_fallible<D2, E, I, L>(
        &self,
        name: &str,
        logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static;

    /// Like [`Collection::filter`], but `logic` is allowed to fail. The first
    /// returned collection will contain the elements where `logic` returned
    /// `Ok(true)`, and the second returned collection will contain the errors
    /// for elements where `logic` failed. Elements for which `logic` returned
    /// `Ok(false)` are not present in either collection.
    fn filter_fallible<E, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Collection<G, D1, R>, Collection<G, E, R>)
    where
        D1: Data,
        E: Data,
        L: FnMut(&D1) -> Result<bool, E> + 'static,
    {
        self.flat_map_fallible(name, move |record| match logic(&record) {
            Ok(false) => None,
            Ok(true) => Some(Ok(record)),
            Err(err) => Some(Err(err)),
        })
    }

    /// Like [`Collection::flat_map`], but the first element
    /// returned from `logic` is allowed to represent a failure. The first
    /// returned collection will contain the successful applications of `logic`,
    /// while the second returned collection will contain the failed
    /// applications; in each case, with the multiplicities multiplied by the
    /// second element returned from `logic`.
    fn explode_fallible<D2, E, R2, I, L>(
        &self,
        name: &str,
        logic: L,
    ) -> (
        Collection<G, D2, <R2 as Mul<R>>::Output>,
        Collection<G, E, <R2 as Mul<R>>::Output>,
    )
    where
        D2: Data,
        E: Data,
        R2: Semigroup + Mul<R>,
        <R2 as Mul<R>>::Output: Data + Semigroup,
        I: IntoIterator<Item = (Result<D2, E>, R2)>,
        L: FnMut(D1) -> I + 'static;
}

pub struct Item<D>(Option<D>);

impl<D: Unpin> Future for Item<D> {
    type Output = D;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.0.take() {
            Some(item) => Poll::Ready(item),
            None => Poll::Pending,
        }
    }
}

pub struct BundleStream<T, D1, P1, D2, P2>
where
    T: timely::progress::Timestamp,
    P1: timely::communication::Pull<
        timely::communication::Message<timely::dataflow::channels::Message<T, D1>>,
    >,
    P2: timely::communication::Push<
        timely::communication::Message<timely::dataflow::channels::Message<T, D2>>,
    >,
{
    input: InputHandle<T, D1, P1>,
    output: OutputWrapper<T, D2, P2>,
}

impl<T, D1, P1, D2, P2> BundleStream<T, D1, P1, D2, P2>
where
    T: timely::progress::Timestamp,
    P1: timely::communication::Pull<
        timely::communication::Message<timely::dataflow::channels::Message<T, D1>>,
    >,
    P2: timely::communication::Push<
        timely::communication::Message<timely::dataflow::channels::Message<T, D2>>,
    >,
{
    pub fn next(
        &mut self,
    ) -> impl Future<Output = (&mut InputHandle<T, D1, P1>, OutputHandle<T, D2, P2>)> {
        Item(Some((&mut self.input, self.output.activate())))
    }
}

impl<G, D1> StreamExt<G, D1> for Stream<G, D1>
where
    D1: Data,
    G: Scope,
{
    fn unary_fallible<D2, E, B, P>(
        &self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
        ) -> Box<
            dyn FnMut(
                    &mut InputHandle<G::Timestamp, D1, P::Puller>,
                    &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                    &mut OutputHandle<G::Timestamp, E, Tee<G::Timestamp, E>>,
                ) + 'static,
        >,
        P: ParallelizationContract<G::Timestamp, D1>,
    {
        let mut builder = OperatorBuilder::new(name.into(), self.scope());
        builder.set_notify(false);

        let operator_info = builder.operator_info();

        let mut input = builder.new_input(self, pact);
        let (mut ok_output, ok_stream) = builder.new_output();
        let (mut err_output, err_stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |_frontiers| {
                let mut ok_output_handle = ok_output.activate();
                let mut err_output_handle = err_output.activate();
                logic(&mut input, &mut ok_output_handle, &mut err_output_handle);
            }
        });

        (ok_stream, err_stream)
    }

    fn unary_async2<D2, B, Fut, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            BundleStream<G::Timestamp, D1, P::Puller, D2, Tee<G::Timestamp, D2>>,
        ) -> Fut,
        Fut: Future<Output = std::convert::Infallible> + 'static,
        P: ParallelizationContract<G::Timestamp, D1>,
    {
        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();

        // unary operator has a single input and a single output
        let input = builder.new_input(self, pact);
        let (output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` is single-element vector since we have a single output.
            let capability = capabilities.pop().unwrap();

            // create a timely activator that will be wrapped in an async Waker/Context
            let activator = Arc::new(self.scope().sync_activator_for(&operator_info.address[..]));

            let bundle_stream = BundleStream { input, output };

            let mut logic_fut = Box::pin(constructor(capability, operator_info, bundle_stream));

            move |_frontiers| {
                let waker = futures::task::waker_ref(&activator);
                let mut cx = Context::from_waker(&waker);
                assert!(Pin::new(&mut logic_fut).poll(&mut cx).is_pending());
            }
        });

        stream
    }

    /// Like [Operator::unary] but its logic function is async.
    ///
    /// Whenever timely schedules the operator because there is more data in the input handle the
    /// supplied logic function will be called and the operator will keep polling the returned
    /// future until completion. If timely re-schedules the operator because more data is available
    /// but a future is pending, the data will be left intact and a new future will be constructed
    /// as soon as the previous resovles.
    ///
    /// Due to limitations of the Rust type-system it is impossible for the logic closure to
    /// receive mutable referneces to the handles directly like its sync counterpart. For this
    /// reason, the closure is given owned handles for input and output that must be returned back
    /// as the output of the future.
    ///
    /// ## Important ##
    ///
    /// The future returned must hit no await points if the input handle is empty. This restriction
    /// might be lifted in the future if we can learn if an input handle is empty or not.
    fn unary_async<D2, B, L, Fut, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(
                InputHandle<G::Timestamp, D1, P::Puller>,
                OutputWrapper<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
            ) -> Fut
            + 'static,
        Fut: Future<
            Output = (
                InputHandle<G::Timestamp, D1, P::Puller>,
                OutputWrapper<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
            ),
        >,
        P: ParallelizationContract<G::Timestamp, D1>,
    {
        /// The state machine of this operator
        #[pin_project(project = StateProj, project_replace = StateProjOwn)]
        enum State<A, B> {
            /// The operator is waiting for a future returned by `logic` to complete. Pinned access
            /// is required in order to poll the future.
            ///
            /// The boolean flag indicates if the future has been polled at least once.
            Waiting(bool, #[pin] A),
            /// The operator is ready to process data and will transition to the `Waiting` state as
            /// soon as timely re-schedules the operator
            Ready(B),
            /// This state is only transiently entered while transitioning from Ready -> Waiting.
            /// The operator will panic if it ever stays permanently in the Invalid state
            Invalid,
        }

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();

        // unary operator has a single input and a single output
        let input = builder.new_input(self, pact);
        let (output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` is single-element vector since we have a single output.
            let capability = capabilities.pop().unwrap();

            // create a timely activator that will be wrapped in an async Waker/Context
            let activator = Arc::new(self.scope().sync_activator_for(&operator_info.address[..]));

            let logic = constructor(capability, operator_info);

            // Allocate the operator state in the heap and pin it. By pinning the whole state we
            // can poll the future within and crucially re-use the allocation whenever we have to
            // re-create the future.
            let mut state = Box::pin(State::Ready((input, output, logic)));

            move |_frontiers| loop {
                match state.as_mut().project() {
                    // If we're in this state it means that there is data in the input handle so we
                    // need to create a future and start polling it.
                    StateProj::Ready(_) => {
                        // Temporarily replace the operator state with the `Invalid` state in order
                        // to take ownership of the input and output handles
                        match state.as_mut().project_replace(State::Invalid) {
                            StateProjOwn::Ready((input, output, mut logic)) => {
                                state
                                    .as_mut()
                                    .project_replace(State::Waiting(false, async move {
                                        // `logic` receives owned handles for input and output and
                                        // must return them back so that wen can re-use them when
                                        // we have to schedule the next future.
                                        let (input, output) = logic(input, output).await;
                                        (input, output, logic)
                                    }));
                            }
                            _ => unreachable!(),
                        }
                    }
                    // If we're in this state it means that we are waiting on a future and either
                    // the future called its waker and timely rescheduled us or there is more data
                    // in the input handle. In either case all we can do is poll the future.
                    StateProj::Waiting(polled, mut fut) => {
                        let waker = futures::task::waker_ref(&activator);
                        let mut context = Context::from_waker(&waker);

                        // Remember if the future was polled before
                        let was_polled_before = *polled;
                        match Pin::new(&mut fut).poll(&mut context) {
                            Poll::Ready(handles) => {
                                // We're done polling and we got our handles back.
                                state.as_mut().project_replace(State::Ready(handles));

                                if !was_polled_before {
                                    // The only way we can end up in Poll::Ready with
                                    // `was_polled_before` being false is if the future never
                                    // yielded (e.g if there were no await points). In this case we
                                    // don't want to re-create a future because there is nothing
                                    // do, so break.
                                    //
                                    // If however there was an await point then we don't know if
                                    // timely re-scheduled us *only* because of the waker or if
                                    // there is also data in the input handle. In this case, we'll
                                    // re-create the future just in case and rely on it not doing
                                    // any async work if the input handle is empty.
                                    //
                                    // This would be a lot simpler if there was an
                                    // InputHandle::is_empty() method.
                                    break;
                                }
                            }
                            Poll::Pending => {
                                *polled = true;
                                break;
                            }
                        }
                    }
                    StateProj::Invalid => unreachable!(),
                }
            }
        });

        stream
    }

    fn flat_map_fallible<D2, E, I, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static,
    {
        let mut storage = Vec::new();
        self.unary_fallible(Pipeline, name, move |_, _| {
            Box::new(move |input, ok_output, err_output| {
                input.for_each(|time, data| {
                    let mut ok_session = ok_output.session(&time);
                    let mut err_session = err_output.session(&time);
                    data.swap(&mut storage);
                    for r in storage.drain(..).flat_map(|d1| logic(d1)) {
                        match r {
                            Ok(d2) => ok_session.give(d2),
                            Err(e) => err_session.give(e),
                        }
                    }
                })
            })
        })
    }

    fn pass_through(
        &self,
        name: &str,
    ) -> Stream<
        G,
        (
            D1,
            G::Timestamp,
            Diff, /* Can't be generic -- Semigroup has no distinguished `1` element */
        ),
    > {
        self.unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut v = Vec::new();
                    data.swap(&mut v);
                    let mut session = output.session(&cap);
                    session.give_iterator(
                        v.into_iter()
                            .map(|payload| (payload, cap.time().clone(), 1)),
                    );
                });
            }
        })
    }
}

impl<G, D1, R> CollectionExt<G, D1, R> for Collection<G, D1, R>
where
    G: Scope,
    G::Timestamp: Data,
    D1: Data,
    R: Semigroup,
{
    fn empty(scope: &G) -> Collection<G, D1, R> {
        operator::empty(scope).as_collection()
    }

    fn flat_map_fallible<D2, E, I, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static,
    {
        let (ok_stream, err_stream) = self.inner.flat_map_fallible(name, move |(d1, t, r)| {
            logic(d1).into_iter().map(move |res| match res {
                Ok(d2) => Ok((d2, t.clone(), r.clone())),
                Err(e) => Err((e, t.clone(), r.clone())),
            })
        });
        (ok_stream.as_collection(), err_stream.as_collection())
    }

    fn explode_fallible<D2, E, R2, I, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (
        Collection<G, D2, <R2 as Mul<R>>::Output>,
        Collection<G, E, <R2 as Mul<R>>::Output>,
    )
    where
        D2: Data,
        E: Data,
        R2: Semigroup + Mul<R>,
        <R2 as Mul<R>>::Output: Data + Semigroup,
        I: IntoIterator<Item = (Result<D2, E>, R2)>,
        L: FnMut(D1) -> I + 'static,
    {
        let (ok_stream, err_stream) = self.inner.flat_map_fallible(name, move |(d1, t, r)| {
            logic(d1).into_iter().map(move |res| match res {
                (Ok(d2), r2) => Ok((d2, t.clone(), r2 * r.clone())),
                (Err(e), r2) => Err((e, t.clone(), r2 * r.clone())),
            })
        });
        (ok_stream.as_collection(), err_stream.as_collection())
    }
}
