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

//! Common operator transformations on timely streams and differential collections.

use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::rc::Weak;

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::containers::{Columnation, TimelyStack};
use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::{Batcher, Builder, Description};
use differential_dataflow::{AsCollection, Collection, Hashable};
use timely::container::{ContainerBuilder, DrainContainer, PushInto};
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::builder_rc::{
    OperatorBuilder as OperatorBuilderRc, OperatorBuilder,
};
use timely::dataflow::operators::generic::operator::{self, Operator};
use timely::dataflow::operators::generic::{InputHandleCore, OperatorInfo, OutputHandleCore};
use timely::dataflow::{Scope, ScopeParent, Stream, StreamCore};
use timely::progress::{Antichain, Timestamp};
use timely::{Container, Data, PartialOrder};

/// A session with lifetime `'a` in a scope `G` with a container builder `CB`.
///
/// This is a shorthand primarily for the reason of readability.
pub type SessionFor<'a, G, CB> = Session<
    'a,
    <G as ScopeParent>::Timestamp,
    CB,
    timely::dataflow::channels::pushers::Counter<
        <G as ScopeParent>::Timestamp,
        <CB as ContainerBuilder>::Container,
        Tee<<G as ScopeParent>::Timestamp, <CB as ContainerBuilder>::Container>,
    >,
>;

/// Extension methods for timely [`StreamCore`]s.
pub trait StreamExt<G, C1>
where
    C1: Container + DrainContainer,
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
                    &mut InputHandleCore<G::Timestamp, C1, P::Puller>,
                    &mut OutputHandleCore<G::Timestamp, DCB, Tee<G::Timestamp, DCB::Container>>,
                    &mut OutputHandleCore<G::Timestamp, ECB, Tee<G::Timestamp, ECB::Container>>,
                ) + 'static,
        >,
        P: ParallelizationContract<G::Timestamp, C1>;

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
        L: for<'a> FnMut(C1::Item<'a>) -> I + 'static;

    /// Block progress of the frontier at `expiration` time, unless the token is dropped.
    fn expire_stream_at(
        &self,
        name: &str,
        expiration: G::Timestamp,
        token: Weak<()>,
    ) -> StreamCore<G, C1>;
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
    ) -> (
        Collection<G, D2, R, DCB::Container>,
        Collection<G, E, R, ECB::Container>,
    )
    where
        DCB: ContainerBuilder + PushInto<(D2, G::Timestamp, R)>,
        ECB: ContainerBuilder + PushInto<(E, G::Timestamp, R)>,
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static;

    /// Block progress of the frontier at `expiration` time, unless the token is dropped.
    fn expire_collection_at(
        &self,
        name: &str,
        expiration: G::Timestamp,
        token: Weak<()>,
    ) -> Collection<G, D1, R>;

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

impl<G, C1> StreamExt<G, C1> for StreamCore<G, C1>
where
    C1: Container + DrainContainer,
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
                    &mut InputHandleCore<G::Timestamp, C1, P::Puller>,
                    &mut OutputHandleCore<G::Timestamp, DCB, Tee<G::Timestamp, DCB::Container>>,
                    &mut OutputHandleCore<G::Timestamp, ECB, Tee<G::Timestamp, ECB::Container>>,
                ) + 'static,
        >,
        P: ParallelizationContract<G::Timestamp, C1>,
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
        L: for<'a> FnMut(C1::Item<'a>) -> I + 'static,
    {
        self.unary_fallible::<DCB, ECB, _, _>(Pipeline, name, move |_, _| {
            Box::new(move |input, ok_output, err_output| {
                input.for_each(|time, data| {
                    let mut ok_session = ok_output.session_with_builder(&time);
                    let mut err_session = err_output.session_with_builder(&time);
                    for r in data.drain().flat_map(|d1| logic(d1)) {
                        match r {
                            Ok(d2) => ok_session.push_into(d2),
                            Err(e) => err_session.push_into(e),
                        }
                    }
                })
            })
        })
    }

    fn expire_stream_at(
        &self,
        name: &str,
        expiration: G::Timestamp,
        token: Weak<()>,
    ) -> StreamCore<G, C1> {
        let name = format!("expire_stream_at({name})");
        self.unary_frontier(Pipeline, &name.clone(), move |cap, _| {
            // Retain a capability for the expiration time, which we'll only drop if the token
            // is dropped. Else, block progress at the expiration time to prevent downstream
            // operators from making any statement about expiration time or any following time.
            let mut cap = Some(cap.delayed(&expiration));
            let mut warned = false;
            move |input, output| {
                if token.upgrade().is_none() {
                    // In shutdown, allow to propagate.
                    drop(cap.take());
                } else {
                    let frontier = input.frontier().frontier();
                    if !frontier.less_than(&expiration) && !warned {
                        // Here, we print a warning, not an error. The state is only a liveness
                        // concern, but not relevant for correctness. Additionally, a race between
                        // shutting down the dataflow and dropping the token can cause the dataflow
                        // to shut down before we drop the token.  This can happen when dropping
                        // the last remaining capability on a different worker.  We do not want to
                        // log an error every time this happens.

                        tracing::warn!(
                            name = name,
                            frontier = ?frontier,
                            expiration = ?expiration,
                            "frontier not less than expiration"
                        );
                        warned = true;
                    }
                }
                input.for_each(|time, data| {
                    let mut session = output.session(&time);
                    session.give_container(data);
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
    ) -> (
        Collection<G, D2, R, DCB::Container>,
        Collection<G, E, R, ECB::Container>,
    )
    where
        DCB: ContainerBuilder + PushInto<(D2, G::Timestamp, R)>,
        ECB: ContainerBuilder + PushInto<(E, G::Timestamp, R)>,
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

    fn expire_collection_at(
        &self,
        name: &str,
        expiration: G::Timestamp,
        token: Weak<()>,
    ) -> Collection<G, D1, R> {
        self.inner
            .expire_stream_at(name, expiration, token)
            .as_collection()
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
                    move |input, output| {
                        input.for_each(|time, data| {
                            output
                                .session_with_builder(&time)
                                .give_iterator(data.drain(..).map(|(x, t, d)| {
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
        let (oks, errs) = self
            .inner
            .unary_fallible(Pipeline, "EnsureMonotonic", move |_, _| {
                Box::new(move |input, ok_output, err_output| {
                    input.for_each(|time, data| {
                        let mut ok_session = ok_output.session(&time);
                        let mut err_session = err_output.session(&time);
                        for (x, t, d) in data.drain(..) {
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
            consolidate_pact::<Ba, _, _>(&self.map(|k| (k, ())).inner, exchange, name)
                .unary(Pipeline, "unpack consolidated", |_, _| {
                    |input, output| {
                        input.for_each(|time, data| {
                            let mut session = output.session(&time);
                            for ((k, ()), t, d) in
                                data.iter().flatten().flat_map(|chunk| chunk.iter())
                            {
                                session.give((k.clone(), t.clone(), d.clone()))
                            }
                        })
                    }
                })
                .as_collection()
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
            Exchange::new(move |update: &((D1, ()), G::Timestamp, R)| (update.0).0.hashed());

        consolidate_pact::<Ba, _, _>(&self.map(|k| (k, ())).inner, exchange, name)
            .unary(Pipeline, &format!("Unpack {name}"), |_, _| {
                |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for ((k, ()), t, d) in data.iter().flatten().flat_map(|chunk| chunk.iter())
                        {
                            session.give((k.clone(), t.clone(), d.clone()))
                        }
                    })
                }
            })
            .as_collection()
    }
}

/// Aggregates the weights of equal records into at most one record.
///
/// Produces a stream of chains of records, partitioned according to `pact`. The
/// data is sorted according to `Ba`. For each timestamp, it produces at most one chain.
///
/// The data are accumulated in place, each held back until their timestamp has completed.
pub fn consolidate_pact<Ba, P, G>(
    stream: &StreamCore<G, Ba::Input>,
    pact: P,
    name: &str,
) -> Stream<G, Vec<Ba::Output>>
where
    G: Scope,
    Ba: Batcher<Time = G::Timestamp> + 'static,
    Ba::Input: Container + Clone + 'static,
    Ba::Output: Container + Clone,
    P: ParallelizationContract<G::Timestamp, Ba::Input>,
{
    stream.unary_frontier(pact, name, |_cap, info| {
        // Acquire a logger for arrange events.
        let logger = stream
            .scope()
            .logger_for("differential/arrange")
            .map(Into::into);

        let mut batcher = Ba::new(logger, info.global_id);
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
                            let output =
                                batcher.seal::<ConsolidateBuilder<_, Ba::Output>>(upper.clone());
                            session.give(output);
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
}

/// A builder that wraps a session for direct output to a stream.
struct ConsolidateBuilder<T, I> {
    _marker: PhantomData<(T, I)>,
}

impl<T, I> Builder for ConsolidateBuilder<T, I>
where
    T: Timestamp,
    I: Container,
{
    type Input = I;
    type Time = T;
    type Output = Vec<I>;

    fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        Self::new()
    }

    fn push(&mut self, _chunk: &mut Self::Input) {
        unimplemented!("ConsolidateBuilder::push")
    }

    fn done(self, _: Description<Self::Time>) -> Self::Output {
        unimplemented!("ConsolidateBuilder::done")
    }

    fn seal(chain: &mut Vec<Self::Input>, _description: Description<Self::Time>) -> Self::Output {
        std::mem::take(chain)
    }
}

/// Merge the contents of multiple streams and combine the containers using a container builder.
pub trait ConcatenateFlatten<G: Scope, C: Container + DrainContainer> {
    /// Merge the contents of multiple streams and use the provided container builder to form
    /// output containers.
    ///
    /// # Examples
    /// ```
    /// use timely::container::CapacityContainerBuilder;
    /// use timely::dataflow::operators::{ToStream, Inspect};
    /// use mz_timely_util::operator::ConcatenateFlatten;
    ///
    /// timely::example(|scope| {
    ///
    ///     let streams = vec![(0..10).to_stream(scope),
    ///                        (0..10).to_stream(scope),
    ///                        (0..10).to_stream(scope)];
    ///
    ///     scope.concatenate_flatten::<_, CapacityContainerBuilder<Vec<_>>>(streams)
    ///          .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concatenate_flatten<I, CB>(&self, sources: I) -> StreamCore<G, CB::Container>
    where
        I: IntoIterator<Item = StreamCore<G, C>>,
        CB: ContainerBuilder + for<'a> PushInto<C::Item<'a>>;
}

impl<G, C> ConcatenateFlatten<G, C> for StreamCore<G, C>
where
    G: Scope,
    C: Container + DrainContainer,
{
    fn concatenate_flatten<I, CB>(&self, sources: I) -> StreamCore<G, CB::Container>
    where
        I: IntoIterator<Item = StreamCore<G, C>>,
        CB: ContainerBuilder + for<'a> PushInto<C::Item<'a>>,
    {
        let clone = self.clone();
        self.scope()
            .concatenate_flatten::<_, CB>(Some(clone).into_iter().chain(sources))
    }
}

impl<G, C> ConcatenateFlatten<G, C> for G
where
    G: Scope,
    C: Container + DrainContainer,
{
    fn concatenate_flatten<I, CB>(&self, sources: I) -> StreamCore<G, CB::Container>
    where
        I: IntoIterator<Item = StreamCore<G, C>>,
        CB: ContainerBuilder + for<'a> PushInto<C::Item<'a>>,
    {
        let mut builder = OperatorBuilder::new("ConcatenateFlatten".to_string(), self.clone());
        builder.set_notify(false);

        // create new input handles for each input stream.
        let mut handles = sources
            .into_iter()
            .map(|s| builder.new_input(&s, Pipeline))
            .collect::<Vec<_>>();

        // create one output handle for the concatenated results.
        let (mut output, result) = builder.new_output::<CB>();

        builder.build(move |_capability| {
            move |_frontier| {
                let mut output = output.activate();
                for handle in handles.iter_mut() {
                    handle.for_each(|time, data| {
                        output
                            .session_with_builder(&time)
                            .give_iterator(data.drain());
                    })
                }
            }
        });

        result
    }
}
