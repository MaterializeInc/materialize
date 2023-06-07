// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common operator transformations on timely streams and differential collections.

use std::future::Future;
use std::rc::Weak;

use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::generic::operator::{self, Operator};
use timely::dataflow::operators::generic::{InputHandle, OperatorInfo, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::{Data, ExchangeData};

use crate::buffer::ConsolidateBuffer;
use crate::builder_async::{
    AsyncInputHandle, AsyncOutputHandle, OperatorBuilder as OperatorBuilderAsync,
};

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

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy pact, and repeatedly schedules logic, the future returned by the function passed
    /// as constructor. logic can read from the input stream, and write to the output stream.
    fn unary_async<D2, P, B, BFut>(&self, pact: P, name: String, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            AsyncInputHandle<G::Timestamp, Vec<D1>, P::Puller>,
            AsyncOutputHandle<G::Timestamp, Vec<D2>, Tee<G::Timestamp, D2>>,
        ) -> BFut,
        BFut: Future + 'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy pact, and repeatedly schedules logic, the future returned by the function passed
    /// as constructor. logic can read from the input streams, and write to the output stream.
    fn binary_async<D2, D3, P1, P2, B, BFut>(
        &self,
        other: &Stream<G, D2>,
        pact1: P1,
        pact2: P2,
        name: String,
        constructor: B,
    ) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            AsyncInputHandle<G::Timestamp, Vec<D1>, P1::Puller>,
            AsyncInputHandle<G::Timestamp, Vec<D2>, P2::Puller>,
            AsyncOutputHandle<G::Timestamp, Vec<D3>, Tee<G::Timestamp, D3>>,
        ) -> BFut,
        BFut: Future + 'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy pact, and repeatedly schedules logic which can read from the input stream and
    /// inspect the frontier at the input.
    fn sink_async<P, B, BFut>(&self, pact: P, name: String, constructor: B)
    where
        B: FnOnce(OperatorInfo, AsyncInputHandle<G::Timestamp, Vec<D1>, P::Puller>) -> BFut,
        BFut: Future + 'static,
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

    /// Take a Timely stream and convert it to a Differential stream, where each diff is "1"
    /// and each time is the current Timely timestamp.
    fn pass_through<R: Data>(&self, name: &str, unit: R) -> Stream<G, (D1, G::Timestamp, R)>;

    /// Wraps the stream with an operator that passes through all received inputs as long as the
    /// provided token can be upgraded. Once the token cannot be upgraded anymore, all data flowing
    /// into the operator is dropped.
    fn with_token(&self, token: Weak<()>) -> Stream<G, D1>;

    /// Distributes the data of the stream to all workers in a round-robin fashion.
    fn distribute(&self) -> Stream<G, D1>
    where
        D1: ExchangeData;
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

    /// Replaces each record with another, with a new difference type.
    ///
    /// This method is most commonly used to take records containing aggregatable data (e.g. numbers to be summed)
    /// and move the data into the difference component. This will allow differential dataflow to update in-place.
    fn explode_one<D2, R2, L>(&self, logic: L) -> Collection<G, D2, <R2 as Multiply<R>>::Output>
    where
        D2: differential_dataflow::Data,
        R2: Semigroup + Multiply<R>,
        <R2 as Multiply<R>>::Output: Data + Semigroup,
        L: FnMut(D1) -> (D2, R2) + 'static,
        G::Timestamp: Lattice;

    /// Partitions the input into a monotonic collection and
    /// non-monotone exceptions, with respect to differences.
    ///
    /// The exceptions are transformed by `into_err`.
    fn ensure_monotonic<E, IE>(&self, into_err: IE) -> (Collection<G, D1, R>, Collection<G, E, R>)
    where
        E: Data,
        IE: Fn(D1, R) -> (E, R) + 'static,
        R: num_traits::sign::Signed;

    /// Wraps the collection with an operator that passes through all received inputs as long as
    /// the provided token can be upgraded. Once the token cannot be upgraded anymore, all data
    /// flowing into the operator is dropped.
    fn with_token(&self, token: Weak<()>) -> Collection<G, D1, R>;
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
        let mut builder = OperatorBuilderRc::new(name.into(), self.scope());
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

    fn unary_async<D2, P, B, BFut>(&self, pact: P, name: String, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            AsyncInputHandle<G::Timestamp, Vec<D1>, P::Puller>,
            AsyncOutputHandle<G::Timestamp, Vec<D2>, Tee<G::Timestamp, D2>>,
        ) -> BFut,
        BFut: Future + 'static,
        P: ParallelizationContract<G::Timestamp, D1>,
    {
        let mut builder = OperatorBuilderAsync::new(name, self.scope());
        let operator_info = builder.operator_info();

        let input = builder.new_input(self, pact);
        let (output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            constructor(capability, operator_info, input, output)
        });

        stream
    }

    fn binary_async<D2, D3, P1, P2, B, BFut>(
        &self,
        other: &Stream<G, D2>,
        pact1: P1,
        pact2: P2,
        name: String,
        constructor: B,
    ) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            AsyncInputHandle<G::Timestamp, Vec<D1>, P1::Puller>,
            AsyncInputHandle<G::Timestamp, Vec<D2>, P2::Puller>,
            AsyncOutputHandle<G::Timestamp, Vec<D3>, Tee<G::Timestamp, D3>>,
        ) -> BFut,
        BFut: Future + 'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>,
    {
        let mut builder = OperatorBuilderAsync::new(name, self.scope());
        let operator_info = builder.operator_info();

        let input1 = builder.new_input(self, pact1);
        let input2 = builder.new_input(other, pact2);
        let (output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            constructor(capability, operator_info, input1, input2, output)
        });

        stream
    }

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy pact, and repeatedly schedules logic which can read from the input stream and
    /// inspect the frontier at the input.
    fn sink_async<P, B, BFut>(&self, pact: P, name: String, constructor: B)
    where
        B: FnOnce(OperatorInfo, AsyncInputHandle<G::Timestamp, Vec<D1>, P::Puller>) -> BFut,
        BFut: Future + 'static,
        P: ParallelizationContract<G::Timestamp, D1>,
    {
        let mut builder = OperatorBuilderAsync::new(name, self.scope());
        let operator_info = builder.operator_info();

        let input = builder.new_input(self, pact);

        builder.build(move |_capabilities| constructor(operator_info, input));
    }

    // XXX(guswynn): file an minimization bug report for the logic flat_map
    // false positive here
    // TODO(guswynn): remove this after https://github.com/rust-lang/rust-clippy/issues/8098 is
    // resolved. The `logic` `FnMut` needs to be borrowed in the `flat_map` call, not moved in
    // so the simple `|d1| logic(d1)` closure is load-bearing
    #[allow(clippy::redundant_closure)]
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

    fn pass_through<R: Data>(&self, name: &str, unit: R) -> Stream<G, (D1, G::Timestamp, R)> {
        self.unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut v = Vec::new();
                    data.swap(&mut v);
                    let mut session = output.session(&cap);
                    session.give_iterator(
                        v.into_iter()
                            .map(|payload| (payload, cap.time().clone(), unit.clone())),
                    );
                });
            }
        })
    }

    fn with_token(&self, token: Weak<()>) -> Stream<G, D1> {
        self.unary(Pipeline, "WithToken", move |_cap, _info| {
            let mut vector = Default::default();
            move |input, output| {
                input.for_each(|cap, data| {
                    if token.upgrade().is_some() {
                        data.swap(&mut vector);
                        output.session(&cap).give_container(&mut vector);
                    }
                });
            }
        })
    }

    fn distribute(&self) -> Stream<G, D1>
    where
        D1: ExchangeData,
    {
        let mut vector = Vec::new();
        self.unary(crate::pact::Distribute, "Distribute", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    output.session(&time).give_vec(&mut vector);
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

    fn explode_one<D2, R2, L>(&self, mut logic: L) -> Collection<G, D2, <R2 as Multiply<R>>::Output>
    where
        D2: differential_dataflow::Data,
        R2: Semigroup + Multiply<R>,
        <R2 as Multiply<R>>::Output: Data + Semigroup,
        L: FnMut(D1) -> (D2, R2) + 'static,
        G::Timestamp: Lattice,
    {
        self.inner
            .unary(Pipeline, "ExplodeOne", move |_, _| {
                let mut buffer = Vec::new();
                move |input, output| {
                    let mut out = ConsolidateBuffer::new(output, 0);
                    input.for_each(|time, data| {
                        data.swap(&mut buffer);
                        out.give_iterator(
                            &time,
                            buffer.drain(..).map(|(x, t, d)| {
                                let (x, d2) = logic(x);
                                (x, t, d2.multiply(&d))
                            }),
                        );
                    });
                }
            })
            .as_collection()
    }

    fn ensure_monotonic<E, IE>(&self, into_err: IE) -> (Collection<G, D1, R>, Collection<G, E, R>)
    where
        E: Data,
        IE: Fn(D1, R) -> (E, R) + 'static,
        R: num_traits::sign::Signed,
    {
        let mut buffer = Vec::new();
        let (oks, errs) = self
            .inner
            .unary_fallible(Pipeline, "EnsureMonotonic", move |_, _| {
                Box::new(move |input, ok_output, err_output| {
                    input.for_each(|time, data| {
                        let mut ok_session = ok_output.session(&time);
                        let mut err_session = err_output.session(&time);
                        data.swap(&mut buffer);
                        for (x, t, d) in buffer.drain(..) {
                            if d.is_positive() {
                                ok_session.give((x, t, d))
                            } else {
                                let (e, d2) = into_err(x, d);
                                err_session.give((e, t, d2))
                            }
                        }
                    })
                })
            });
        (oks.as_collection(), errs.as_collection())
    }

    fn with_token(&self, token: Weak<()>) -> Collection<G, D1, R> {
        self.inner.with_token(token).as_collection()
    }
}

/// Creates a new async data stream source for a scope.
///
/// The source is defined by a name, and a constructor which takes a default capability and an
/// output handle to a future. The future is then repeatedly scheduled, and is expected to
/// eventually send data and downgrade and release capabilities.
pub fn source_async<G: Scope, D, B, BFut>(scope: &G, name: String, constructor: B) -> Stream<G, D>
where
    D: Data,
    B: FnOnce(
        Capability<G::Timestamp>,
        OperatorInfo,
        AsyncOutputHandle<G::Timestamp, Vec<D>, Tee<G::Timestamp, D>>,
    ) -> BFut,
    BFut: Future + 'static,
{
    let mut builder = OperatorBuilderAsync::new(name, scope.clone());
    let operator_info = builder.operator_info();

    let (output, stream) = builder.new_output();

    builder.build(move |mut capabilities| {
        // `capabilities` should be a single-element vector.
        let capability = capabilities.pop().unwrap();
        constructor(capability, operator_info, output)
    });

    stream
}
