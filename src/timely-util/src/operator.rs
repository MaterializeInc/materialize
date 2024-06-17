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
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::rc::Weak;

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::{Batcher, Builder};
use differential_dataflow::{AsCollection, Collection, Hashable};
use timely::container::columnation::{Columnation, TimelyStack};
use timely::container::{CapacityContainerBuilder, ContainerBuilder, PushInto};
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::generic::operator::{self, Operator};
use timely::dataflow::operators::generic::{InputHandle, OperatorInfo, OutputHandleCore};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream, StreamCore};
use timely::progress::{Antichain, Timestamp};
use timely::{Container, Data, ExchangeData, PartialOrder};

use crate::builder_async::{
    AsyncInputHandle, AsyncOutputHandle, ConnectedToOne, Disconnected,
    OperatorBuilder as OperatorBuilderAsync,
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
    fn unary_fallible<DCB, ECB, B, P>(
        &self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (StreamCore<G, DCB::Container>, StreamCore<G, ECB::Container>)
    where
        DCB: ContainerBuilder,
        ECB: ContainerBuilder,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
        ) -> Box<
            dyn FnMut(
                    &mut InputHandle<G::Timestamp, D1, P::Puller>,
                    &mut OutputHandleCore<G::Timestamp, DCB, Tee<G::Timestamp, DCB::Container>>,
                    &mut OutputHandleCore<G::Timestamp, ECB, Tee<G::Timestamp, ECB::Container>>,
                ) + 'static,
        >,
        P: ParallelizationContract<G::Timestamp, Vec<D1>>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy pact, and repeatedly schedules logic, the future returned by the function passed
    /// as constructor. logic can read from the input stream, and write to the output stream.
    fn unary_async<D2, P, B, BFut>(&self, pact: P, name: String, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
            AsyncInputHandle<G::Timestamp, Vec<D1>, ConnectedToOne>,
            AsyncOutputHandle<
                G::Timestamp,
                CapacityContainerBuilder<Vec<D2>>,
                Tee<G::Timestamp, Vec<D2>>,
            >,
        ) -> BFut,
        BFut: Future + 'static,
        P: ParallelizationContract<G::Timestamp, Vec<D1>>;

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
            AsyncInputHandle<G::Timestamp, Vec<D1>, ConnectedToOne>,
            AsyncInputHandle<G::Timestamp, Vec<D2>, ConnectedToOne>,
            AsyncOutputHandle<
                G::Timestamp,
                CapacityContainerBuilder<Vec<D3>>,
                Tee<G::Timestamp, Vec<D3>>,
            >,
        ) -> BFut,
        BFut: Future + 'static,
        P1: ParallelizationContract<G::Timestamp, Vec<D1>>,
        P2: ParallelizationContract<G::Timestamp, Vec<D2>>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy pact, and repeatedly schedules logic which can read from the input stream and
    /// inspect the frontier at the input.
    fn sink_async<P, B, BFut>(&self, pact: P, name: String, constructor: B)
    where
        B: FnOnce(OperatorInfo, AsyncInputHandle<G::Timestamp, Vec<D1>, Disconnected>) -> BFut,
        BFut: Future + 'static,
        P: ParallelizationContract<G::Timestamp, Vec<D1>>;

    /// Like [`timely::dataflow::operators::map::Map::map`], but `logic`
    /// is allowed to fail. The first returned stream will contain the
    /// successful applications of `logic`, while the second returned stream
    /// will contain the failed applications.
    fn map_fallible<DCB, ECB, D2, E, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (StreamCore<G, DCB::Container>, StreamCore<G, ECB::Container>)
    where
        DCB: ContainerBuilder + PushInto<D2>,
        ECB: ContainerBuilder + PushInto<E>,
        L: FnMut(D1) -> Result<D2, E> + 'static,
    {
        self.flat_map_fallible::<DCB, ECB, _, _, _, _>(name, move |record| Some(logic(record)))
    }

    /// Like [`timely::dataflow::operators::map::Map::flat_map`], but `logic`
    /// is allowed to fail. The first returned stream will contain the
    /// successful applications of `logic`, while the second returned stream
    /// will contain the failed applications.
    fn flat_map_fallible<DCB, ECB, D2, E, I, L>(
        &self,
        name: &str,
        logic: L,
    ) -> (StreamCore<G, DCB::Container>, StreamCore<G, ECB::Container>)
    where
        DCB: ContainerBuilder + PushInto<D2>,
        ECB: ContainerBuilder + PushInto<E>,
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
    ///
    /// Callers need to specify the following type parameters:
    /// * `DCB`: The container builder for the `Ok` output.
    /// * `ECB`: The container builder for the `Err` output.
    fn map_fallible<DCB, ECB, D2, E, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        DCB: ContainerBuilder<Container = Vec<(D2, G::Timestamp, R)>>
            + PushInto<(D2, G::Timestamp, R)>,
        ECB: ContainerBuilder<Container = Vec<(E, G::Timestamp, R)>>
            + PushInto<(E, G::Timestamp, R)>,
        D2: Data,
        E: Data,
        L: FnMut(D1) -> Result<D2, E> + 'static,
    {
        self.flat_map_fallible::<DCB, ECB, _, _, _, _>(name, move |record| Some(logic(record)))
    }

    /// Like [`Collection::flat_map`], but `logic` is allowed to fail. The first
    /// returned collection will contain the successful applications of `logic`,
    /// while the second returned collection will contain the failed
    /// applications.
    fn flat_map_fallible<DCB, ECB, D2, E, I, L>(
        &self,
        name: &str,
        logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        DCB: ContainerBuilder<Container = Vec<(D2, G::Timestamp, R)>>
            + PushInto<(D2, G::Timestamp, R)>,
        ECB: ContainerBuilder<Container = Vec<(E, G::Timestamp, R)>>
            + PushInto<(E, G::Timestamp, R)>,
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

    /// Consolidates the collection if `must_consolidate` is `true` and leaves it
    /// untouched otherwise.
    fn consolidate_named_if<Ba>(self, must_consolidate: bool, name: &str) -> Self
    where
        D1: differential_dataflow::ExchangeData + Hash + Columnation,
        R: Semigroup + differential_dataflow::ExchangeData + Columnation,
        G::Timestamp: Lattice + Columnation,
        Ba: Batcher<
                Input = Vec<((D1, ()), G::Timestamp, R)>,
                Output = TimelyStack<((D1, ()), G::Timestamp, R)>,
                Time = G::Timestamp,
            > + 'static;

    /// Consolidates the collection.
    fn consolidate_named<Ba>(self, name: &str) -> Self
    where
        D1: differential_dataflow::ExchangeData + Hash + Columnation,
        R: Semigroup + differential_dataflow::ExchangeData + Columnation,
        G::Timestamp: Lattice + Columnation,
        Ba: Batcher<
                Input = Vec<((D1, ()), G::Timestamp, R)>,
                Output = TimelyStack<((D1, ()), G::Timestamp, R)>,
                Time = G::Timestamp,
            > + 'static;
}

impl<G, D1> StreamExt<G, D1> for Stream<G, D1>
where
    D1: Data,
    G: Scope,
{
    fn unary_fallible<DCB, ECB, B, P>(
        &self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (StreamCore<G, DCB::Container>, StreamCore<G, ECB::Container>)
    where
        DCB: ContainerBuilder,
        ECB: ContainerBuilder,
        B: FnOnce(
            Capability<G::Timestamp>,
            OperatorInfo,
        ) -> Box<
            dyn FnMut(
                    &mut InputHandle<G::Timestamp, D1, P::Puller>,
                    &mut OutputHandleCore<G::Timestamp, DCB, Tee<G::Timestamp, DCB::Container>>,
                    &mut OutputHandleCore<G::Timestamp, ECB, Tee<G::Timestamp, ECB::Container>>,
                ) + 'static,
        >,
        P: ParallelizationContract<G::Timestamp, Vec<D1>>,
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
            AsyncInputHandle<G::Timestamp, Vec<D1>, ConnectedToOne>,
            AsyncOutputHandle<
                G::Timestamp,
                CapacityContainerBuilder<Vec<D2>>,
                Tee<G::Timestamp, Vec<D2>>,
            >,
        ) -> BFut,
        BFut: Future + 'static,
        P: ParallelizationContract<G::Timestamp, Vec<D1>>,
    {
        let mut builder = OperatorBuilderAsync::new(name, self.scope());
        let operator_info = builder.operator_info();

        let (output, stream) = builder.new_output();
        let input = builder.new_input_for(self, pact, &output);

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
            AsyncInputHandle<G::Timestamp, Vec<D1>, ConnectedToOne>,
            AsyncInputHandle<G::Timestamp, Vec<D2>, ConnectedToOne>,
            AsyncOutputHandle<
                G::Timestamp,
                CapacityContainerBuilder<Vec<D3>>,
                Tee<G::Timestamp, Vec<D3>>,
            >,
        ) -> BFut,
        BFut: Future + 'static,
        P1: ParallelizationContract<G::Timestamp, Vec<D1>>,
        P2: ParallelizationContract<G::Timestamp, Vec<D2>>,
    {
        let mut builder = OperatorBuilderAsync::new(name, self.scope());
        let operator_info = builder.operator_info();

        let (output, stream) = builder.new_output();
        let input1 = builder.new_input_for(self, pact1, &output);
        let input2 = builder.new_input_for(other, pact2, &output);

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
        B: FnOnce(OperatorInfo, AsyncInputHandle<G::Timestamp, Vec<D1>, Disconnected>) -> BFut,
        BFut: Future + 'static,
        P: ParallelizationContract<G::Timestamp, Vec<D1>>,
    {
        let mut builder = OperatorBuilderAsync::new(name, self.scope());
        let operator_info = builder.operator_info();

        let input = builder.new_disconnected_input(self, pact);

        builder.build(move |_capabilities| constructor(operator_info, input));
    }

    // XXX(guswynn): file an minimization bug report for the logic flat_map
    // false positive here
    // TODO(guswynn): remove this after https://github.com/rust-lang/rust-clippy/issues/8098 is
    // resolved. The `logic` `FnMut` needs to be borrowed in the `flat_map` call, not moved in
    // so the simple `|d1| logic(d1)` closure is load-bearing
    #[allow(clippy::redundant_closure)]
    fn flat_map_fallible<DCB, ECB, D2, E, I, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (StreamCore<G, DCB::Container>, StreamCore<G, ECB::Container>)
    where
        DCB: ContainerBuilder + PushInto<D2>,
        ECB: ContainerBuilder + PushInto<E>,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static,
    {
        let mut storage = Vec::new();
        self.unary_fallible::<DCB, ECB, _, _>(Pipeline, name, move |_, _| {
            Box::new(move |input, ok_output, err_output| {
                input.for_each(|time, data| {
                    let mut ok_session = ok_output.session_with_builder(&time);
                    let mut err_session = err_output.session_with_builder(&time);
                    data.swap(&mut storage);
                    for r in storage.drain(..).flat_map(|d1| logic(d1)) {
                        match r {
                            Ok(d2) => ok_session.push_into(d2),
                            Err(e) => err_session.push_into(e),
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
                    output.session(&time).give_container(&mut vector);
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
    R: Semigroup + 'static,
{
    fn empty(scope: &G) -> Collection<G, D1, R> {
        operator::empty(scope).as_collection()
    }

    fn flat_map_fallible<DCB, ECB, D2, E, I, L>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        DCB: ContainerBuilder<Container = Vec<(D2, G::Timestamp, R)>>
            + PushInto<(D2, G::Timestamp, R)>,
        ECB: ContainerBuilder<Container = Vec<(E, G::Timestamp, R)>>
            + PushInto<(E, G::Timestamp, R)>,
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static,
    {
        let (ok_stream, err_stream) =
            self.inner
                .flat_map_fallible::<DCB, ECB, _, _, _, _>(name, move |(d1, t, r)| {
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
            .unary::<ConsolidatingContainerBuilder<_>, _, _, _>(
                Pipeline,
                "ExplodeOne",
                move |_, _| {
                    let mut buffer = Vec::new();
                    move |input, output| {
                        input.for_each(|time, data| {
                            data.swap(&mut buffer);
                            output
                                .session_with_builder(&time)
                                .give_iterator(buffer.drain(..).map(|(x, t, d)| {
                                    let (x, d2) = logic(x);
                                    (x, t, d2.multiply(&d))
                                }));
                        });
                    }
                },
            )
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

    fn consolidate_named_if<Ba>(self, must_consolidate: bool, name: &str) -> Self
    where
        D1: differential_dataflow::ExchangeData + Hash + Columnation,
        R: Semigroup + differential_dataflow::ExchangeData + Columnation,
        G::Timestamp: Lattice + Ord + Columnation,
        Ba: Batcher<
                Input = Vec<((D1, ()), G::Timestamp, R)>,
                Output = TimelyStack<((D1, ()), G::Timestamp, R)>,
                Time = G::Timestamp,
            > + 'static,
    {
        if must_consolidate {
            // We employ AHash below instead of the default hasher in DD to obtain
            // a better distribution of data to workers. AHash claims empirically
            // both speed and high quality, according to
            // https://github.com/tkaitchuck/aHash/blob/master/compare/readme.md.
            // TODO(vmarcos): Consider here if it is worth it to spend the time to
            // implement twisted tabulation hashing as proposed in Mihai Patrascu,
            // Mikkel Thorup: Twisted Tabulation Hashing. SODA 2013: 209-228, available
            // at https://epubs.siam.org/doi/epdf/10.1137/1.9781611973105.16. The latter
            // would provide good bounds for balls-into-bins problems when the number of
            // bins is small (as is our case), so we'd have a theoretical guarantee.
            // NOTE: We fix the seeds of a RandomState instance explicity with the same
            // seeds that would be given by `AHash` via ahash::AHasher::default() so as
            // to avoid a different selection due to compile-time features being differently
            // selected in other dependencies using `AHash` vis-à-vis cargo's strategy
            // of unioning features.
            // NOTE: Depending on target features, we may end up employing the fallback
            // hasher of `AHash`, but it should be sufficient for our needs.
            let random_state = ahash::RandomState::with_seeds(
                0x243f_6a88_85a3_08d3,
                0x1319_8a2e_0370_7344,
                0xa409_3822_299f_31d0,
                0x082e_fa98_ec4e_6c89,
            );
            let exchange = Exchange::new(move |update: &((D1, _), G::Timestamp, R)| {
                let data = &(update.0).0;
                let mut h = random_state.build_hasher();
                data.hash(&mut h);
                h.finish()
            });
            // Access to `arrange_core` is OK because we specify the trace and don't hold on to it.
            consolidate_pact::<Ba, _, _, _, _, _>(&self.map(|k| (k, ())), exchange, name)
                .map(|(k, ())| k)
        } else {
            self
        }
    }

    fn consolidate_named<Ba>(self, name: &str) -> Self
    where
        D1: differential_dataflow::ExchangeData + Hash + Columnation,
        R: Semigroup + differential_dataflow::ExchangeData + Columnation,
        G::Timestamp: Lattice + Ord + Columnation,
        Ba: Batcher<
                Input = Vec<((D1, ()), G::Timestamp, R)>,
                Output = TimelyStack<((D1, ()), G::Timestamp, R)>,
                Time = G::Timestamp,
            > + 'static,
    {
        let exchange =
            Exchange::new(move |update: &((D1, ()), G::Timestamp, R)| (update.0).0.hashed().into());

        consolidate_pact::<Ba, _, _, _, _, _>(&self.map(|k| (k, ())), exchange, name)
            .map(|(k, ())| k)
    }
}

/// Creates a new async data stream source for a scope.
///
/// The source is defined by a name, and a constructor which takes a default capability and an
/// output handle to a future. The future is then repeatedly scheduled, and is expected to
/// eventually send data and downgrade and release capabilities.
pub fn source_async<G: Scope, CB, B, BFut>(
    scope: &G,
    name: String,
    constructor: B,
) -> StreamCore<G, CB::Container>
where
    CB: ContainerBuilder,
    B: FnOnce(
        Capability<G::Timestamp>,
        OperatorInfo,
        AsyncOutputHandle<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>,
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

/// Aggregates the weights of equal records into at most one record.
///
/// The data are accumulated in place, each held back until their timestamp has completed.
///
/// This serves as a low-level building-block for more user-friendly functions.
pub fn consolidate_pact<B, P, G, K, V, R>(
    collection: &Collection<G, (K, V), R>,
    pact: P,
    name: &str,
) -> Collection<G, (K, V), R>
where
    G: Scope,
    K: Data,
    V: Data,
    R: Data + Semigroup,
    B: Batcher<Input = Vec<((K, V), G::Timestamp, R)>, Time = G::Timestamp> + 'static,
    B::Output: Container,
    for<'a> Vec<((K, V), G::Timestamp, R)>: PushInto<<B::Output as Container>::Item<'a>>,
    P: ParallelizationContract<G::Timestamp, Vec<((K, V), G::Timestamp, R)>>,
{
    collection
        .inner
        .unary_frontier(pact, name, |_cap, info| {
            // Acquire a logger for arrange events.
            let logger = {
                let scope = collection.scope();
                let register = scope.log_register();
                register.get::<differential_dataflow::logging::DifferentialEvent>(
                    "differential/arrange",
                )
            };

            let mut batcher = B::new(logger, info.global_id);
            // Capabilities for the lower envelope of updates in `batcher`.
            let mut capabilities = Antichain::<Capability<G::Timestamp>>::new();
            let mut prev_frontier = Antichain::from_elem(G::Timestamp::minimum());

            move |input, output| {
                input.for_each(|cap, data| {
                    capabilities.insert(cap.retain());
                    batcher.push_container(data);
                });

                if prev_frontier.borrow() != input.frontier().frontier() {
                    if capabilities
                        .elements()
                        .iter()
                        .any(|c| !input.frontier().less_equal(c.time()))
                    {
                        let mut upper = Antichain::new(); // re-used allocation for sealing batches.

                        // For each capability not in advance of the input frontier ...
                        for (index, capability) in capabilities.elements().iter().enumerate() {
                            if !input.frontier().less_equal(capability.time()) {
                                // Assemble the upper bound on times we can commit with this capabilities.
                                // We must respect the input frontier, and *subsequent* capabilities, as
                                // we are pretending to retire the capability changes one by one.
                                upper.clear();
                                for time in input.frontier().frontier().iter() {
                                    upper.insert(time.clone());
                                }
                                for other_capability in &capabilities.elements()[(index + 1)..] {
                                    upper.insert(other_capability.time().clone());
                                }

                                // send the batch to downstream consumers, empty or not.
                                let mut session = output.session(&capabilities.elements()[index]);
                                // Extract updates not in advance of `upper`.
                                let output = batcher
                                    .seal::<ConsolidateBuilder<_, _, _, _, _>>(upper.clone());
                                for mut batch in output {
                                    session.give_container(&mut batch);
                                }
                            }
                        }

                        // Having extracted and sent batches between each capability and the input frontier,
                        // we should downgrade all capabilities to match the batcher's lower update frontier.
                        // This may involve discarding capabilities, which is fine as any new updates arrive
                        // in messages with new capabilities.

                        let mut new_capabilities = Antichain::new();
                        for time in batcher.frontier().iter() {
                            if let Some(capability) = capabilities
                                .elements()
                                .iter()
                                .find(|c| c.time().less_equal(time))
                            {
                                new_capabilities.insert(capability.delayed(time));
                            } else {
                                panic!("failed to find capability");
                            }
                        }

                        capabilities = new_capabilities;
                    }

                    prev_frontier.clear();
                    prev_frontier.extend(input.frontier().frontier().iter().cloned());
                }
            }
        })
        .as_collection()
}

/// A builder that wraps a session for direct output to a stream.
struct ConsolidateBuilder<K: Data, V: Data, T: Timestamp, R: Data, O> {
    // Session<'a, T, Vec<((K, V), T, R)>, Counter<T, ((K, V), T, R), Tee<T, ((K, V), T, R)>>>,
    buffer: Vec<Vec<((K, V), T, R)>>,
    _marker: PhantomData<O>,
}

impl<K, V, T, R, O> Builder for ConsolidateBuilder<K, V, T, R, O>
where
    K: Data,
    V: Data,
    T: Timestamp,
    R: Data,
    O: Container,
    for<'a> Vec<((K, V), T, R)>: PushInto<O::Item<'a>>,
{
    type Input = O;
    type Time = T;
    type Output = Vec<Vec<((K, V), T, R)>>;

    fn new() -> Self {
        Self {
            buffer: Vec::default(),
            _marker: PhantomData,
        }
    }

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        Self::new()
    }

    fn push(&mut self, chunk: &mut Self::Input) {
        // TODO(mh): This is less efficient than it could be because it extracts each item
        // individually and then pushes it. However, it is not a regression over the previous
        // implementation. In the future, we want to either clone many elements in one go,
        // or ensure that `Vec<Input>` == `Output`, which would avoid looking at the container
        // contents at all.
        'element: for element in chunk.drain() {
            if let Some(last) = self.buffer.last_mut() {
                if last.len() < last.capacity() {
                    last.push_into(element);
                    continue 'element;
                }
            }
            let mut new =
                Vec::with_capacity(timely::container::buffer::default_capacity::<Self::Input>());
            new.push_into(element);
            self.buffer.push(new);
        }
    }

    fn done(
        self,
        _lower: Antichain<Self::Time>,
        _upper: Antichain<Self::Time>,
        _since: Antichain<Self::Time>,
    ) -> Self::Output {
        self.buffer
    }
}
