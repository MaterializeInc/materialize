// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::Cell;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use std::ops::Mul;
use timely::communication::{Message, Pull, Push};
use timely::dataflow::channels;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{
    operator::{self, Operator},
    InputHandle, OperatorInfo, OutputHandle, OutputWrapper,
};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::progress::Timestamp;
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

    /// Like [Operator::unary] but its logic function is async.
    fn unary_async<D2, B, Fut, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            BundleStream<G::Timestamp, D1, P::Puller, D2, Tee<G::Timestamp, D2>>,
        ) -> Fut,
        Fut: Future<Output = Infallible> + 'static,
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

// The future type returned by [BundleStream::next]
pub struct Next<'a, T, D1, P1, D2, P2>
where
    T: Timestamp,
    P1: Pull<Message<channels::Message<T, D1>>>,
    P2: Push<Message<channels::Message<T, D2>>>,
{
    item: Option<(&'a mut InputHandle<T, D1, P1>, OutputHandle<'a, T, D2, P2>)>,
    data_available: &'a Cell<bool>,
}

impl<'a, T, D1, P1, D2, P2> Future for Next<'a, T, D1, P1, D2, P2>
where
    T: Timestamp,
    P1: Pull<Message<channels::Message<T, D1>>>,
    P2: Push<Message<channels::Message<T, D2>>>,
{
    type Output = (&'a mut InputHandle<T, D1, P1>, OutputHandle<'a, T, D2, P2>);

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.data_available.replace(false) {
            Poll::Ready(self.item.take().expect("polled after completion"))
        } else {
            Poll::Pending
        }
    }
}

pub struct BundleStream<T, D1, P1, D2, P2>
where
    T: Timestamp,
    P1: Pull<Message<channels::Message<T, D1>>>,
    P2: Push<Message<channels::Message<T, D2>>>,
{
    input: InputHandle<T, D1, P1>,
    output: OutputWrapper<T, D2, P2>,
    data_available: Rc<Cell<bool>>,
}

impl<T, D1, P1, D2, P2> BundleStream<T, D1, P1, D2, P2>
where
    T: Timestamp,
    P1: Pull<Message<channels::Message<T, D1>>>,
    P2: Push<Message<channels::Message<T, D2>>>,
{
    pub fn next(&mut self) -> Next<T, D1, P1, D2, P2> {
        Next {
            item: Some((&mut self.input, self.output.activate())),
            data_available: &*self.data_available,
        }
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

    /// Like [Operator::unary] but its logic function is async.
    fn unary_async<D2, B, Fut, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            BundleStream<G::Timestamp, D1, P::Puller, D2, Tee<G::Timestamp, D2>>,
        ) -> Fut,
        Fut: Future<Output = Infallible> + 'static,
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
            let waker = futures::task::waker(activator);

            let data_available = Rc::new(Cell::new(false));

            let bundle_stream = BundleStream {
                input,
                output,
                data_available: Rc::clone(&data_available),
            };

            let mut logic_fut = Box::pin(constructor(capability, operator_info, bundle_stream));

            move |_frontiers| {
                // We don't know for sure that data is available. Timely could have scheduled this
                // operator due to a future making progress so this can lead to spurious wake ups.
                // If an InputHandle::is_empty method existed we could check it before polling
                data_available.set(true);
                let mut cx = Context::from_waker(&waker);
                // The future can never resolve as its output type is Infallible
                assert!(Pin::new(&mut logic_fut).poll(&mut cx).is_pending());
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
