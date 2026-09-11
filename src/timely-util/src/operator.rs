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

use columnation::Columnation;
use differential_dataflow::batcher::Batcher;
use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::BatcherEvent;
use differential_dataflow::logging::Logger;
use differential_dataflow::trace::implementations::merge_batcher::Merger;
use differential_dataflow::{AsCollection, Collection, Hashable, VecCollection};
use std::hash::{BuildHasher, Hash, Hasher};
use timely::container::{DrainContainer, PushInto};
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::builder_rc::{
    OperatorBuilder as OperatorBuilderRc, OperatorBuilder,
};
use timely::dataflow::operators::generic::operator::{self, Operator};
use timely::dataflow::operators::generic::{
    InputHandleCore, OperatorInfo, OutputBuilder, OutputBuilderSession,
};
use timely::dataflow::{Scope, Stream, StreamVec};
use timely::progress::frontier::AntichainRef;
use timely::progress::operate::FrontierInterest;
use timely::progress::{Antichain, Timestamp};
use timely::{Container, ContainerBuilder, PartialOrder};

use crate::columnation::ColumnationStack;

/// Extension methods for timely [`Stream`]s.
pub trait StreamExt<'scope, T, C1>
where
    T: Timestamp,
    C1: Container + DrainContainer + Clone + 'static,
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
        self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        Stream<'scope, T, ECB::Container>,
    )
    where
        DCB: ContainerBuilder,
        ECB: ContainerBuilder,
        B: FnOnce(
            Capability<T>,
            OperatorInfo,
        ) -> Box<
            dyn FnMut(
                    &mut InputHandleCore<T, C1, P::Puller>,
                    &mut OutputBuilderSession<'_, T, DCB>,
                    &mut OutputBuilderSession<'_, T, ECB>,
                ) + 'static,
        >,
        P: ParallelizationContract<T, C1>;

    /// Like [`timely::dataflow::operators::vec::Map::flat_map`], but `logic`
    /// is allowed to fail. The first returned stream will contain the
    /// successful applications of `logic`, while the second returned stream
    /// will contain the failed applications.
    fn flat_map_fallible<DCB, ECB, D2, E, I, L>(
        self,
        name: &str,
        logic: L,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        Stream<'scope, T, ECB::Container>,
    )
    where
        DCB: ContainerBuilder + PushInto<D2>,
        ECB: ContainerBuilder + PushInto<E>,
        I: IntoIterator<Item = Result<D2, E>>,
        L: for<'a> FnMut(C1::Item<'a>) -> I + 'static;

    /// Block progress of the frontier at `expiration` time
    fn expire_stream_at(self, name: &str, expiration: T) -> Stream<'scope, T, C1>;
}

/// Extension methods for differential [`Collection`]s.
pub trait CollectionExt<'scope, T, D1, R>: Sized
where
    T: Timestamp,
    R: Semigroup,
{
    /// Creates a new empty collection in `scope`.
    fn empty(scope: Scope<'scope, T>) -> VecCollection<'scope, T, D1, R>;

    /// Like [`Collection::map`], but `logic` is allowed to fail. The first
    /// returned collection will contain successful applications of `logic`,
    /// while the second returned collection will contain the failed
    /// applications.
    ///
    /// Callers need to specify the following type parameters:
    /// * `DCB`: The container builder for the `Ok` output.
    /// * `ECB`: The container builder for the `Err` output.
    fn map_fallible<DCB, ECB, D2, E, L>(
        self,
        name: &str,
        mut logic: L,
    ) -> (
        VecCollection<'scope, T, D2, R>,
        VecCollection<'scope, T, E, R>,
    )
    where
        DCB: ContainerBuilder<Container = Vec<(D2, T, R)>> + PushInto<(D2, T, R)>,
        ECB: ContainerBuilder<Container = Vec<(E, T, R)>> + PushInto<(E, T, R)>,
        D2: Clone + 'static,
        E: Clone + 'static,
        L: FnMut(D1) -> Result<D2, E> + 'static,
    {
        self.flat_map_fallible::<DCB, ECB, _, _, _, _>(name, move |record| Some(logic(record)))
    }

    /// Like [`Collection::flat_map`], but `logic` is allowed to fail. The first
    /// returned collection will contain the successful applications of `logic`,
    /// while the second returned collection will contain the failed
    /// applications.
    fn flat_map_fallible<DCB, ECB, D2, E, I, L>(
        self,
        name: &str,
        logic: L,
    ) -> (
        Collection<'scope, T, DCB::Container>,
        Collection<'scope, T, ECB::Container>,
    )
    where
        DCB: ContainerBuilder + PushInto<(D2, T, R)>,
        ECB: ContainerBuilder + PushInto<(E, T, R)>,
        D2: Clone + 'static,
        E: Clone + 'static,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static;

    /// Block progress of the frontier at `expiration` time.
    fn expire_collection_at(self, name: &str, expiration: T) -> VecCollection<'scope, T, D1, R>;

    /// Replaces each record with another, with a new difference type.
    ///
    /// This method is most commonly used to take records containing aggregatable data (e.g. numbers to be summed)
    /// and move the data into the difference component. This will allow differential dataflow to update in-place.
    fn explode_one<D2, R2, L>(
        self,
        logic: L,
    ) -> VecCollection<'scope, T, D2, <R2 as Multiply<R>>::Output>
    where
        D2: differential_dataflow::Data,
        R2: Semigroup + Multiply<R>,
        <R2 as Multiply<R>>::Output: Clone + 'static + Semigroup,
        L: FnMut(D1) -> (D2, R2) + 'static,
        T: Lattice;

    /// Partitions the input into a monotonic collection and
    /// non-monotone exceptions, with respect to differences.
    ///
    /// The exceptions are transformed by `into_err`.
    fn ensure_monotonic<E, IE>(
        self,
        into_err: IE,
    ) -> (
        VecCollection<'scope, T, D1, R>,
        VecCollection<'scope, T, E, R>,
    )
    where
        E: Clone + 'static,
        IE: Fn(D1, R) -> (E, R) + 'static,
        R: num_traits::sign::Signed;

    /// Consolidates the collection if `must_consolidate` is `true` and leaves it
    /// untouched otherwise.
    fn consolidate_named_if<Ba>(self, must_consolidate: bool, name: &str) -> Self
    where
        D1: differential_dataflow::ExchangeData + Hash + Columnation,
        R: Semigroup + differential_dataflow::ExchangeData + Columnation,
        T: Lattice + Columnation,
        Ba: Batcher<
                Vec<((D1, ()), T, R)>,
                Time = T,
                Output = Vec<ColumnationStack<((D1, ()), T, R)>>,
            > + 'static,
        Ba: BatcherNew;

    /// Consolidates the collection.
    fn consolidate_named<Ba>(self, name: &str) -> Self
    where
        D1: differential_dataflow::ExchangeData + Hash + Columnation,
        R: Semigroup + differential_dataflow::ExchangeData + Columnation,
        T: Lattice + Columnation,
        Ba: Batcher<
                Vec<((D1, ()), T, R)>,
                Time = T,
                Output = Vec<ColumnationStack<((D1, ()), T, R)>>,
            > + 'static,
        Ba: BatcherNew;
}

impl<'scope, T, C1> StreamExt<'scope, T, C1> for Stream<'scope, T, C1>
where
    T: Timestamp,
    C1: Container + DrainContainer + Clone + 'static,
{
    fn unary_fallible<DCB, ECB, B, P>(
        self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        Stream<'scope, T, ECB::Container>,
    )
    where
        DCB: ContainerBuilder,
        ECB: ContainerBuilder,
        B: FnOnce(
            Capability<T>,
            OperatorInfo,
        ) -> Box<
            dyn FnMut(
                    &mut InputHandleCore<T, C1, P::Puller>,
                    &mut OutputBuilderSession<'_, T, DCB>,
                    &mut OutputBuilderSession<'_, T, ECB>,
                ) + 'static,
        >,
        P: ParallelizationContract<T, C1>,
    {
        let mut builder = OperatorBuilderRc::new(name.into(), self.scope());

        let operator_info = builder.operator_info();

        let mut input = builder.new_input(self.clone(), pact);
        builder.set_notify_for(0, FrontierInterest::Never);
        let (ok_output, ok_stream) = builder.new_output();
        let mut ok_output = OutputBuilder::from(ok_output);
        let (err_output, err_stream) = builder.new_output();
        let mut err_output = OutputBuilder::from(err_output);

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
        self,
        name: &str,
        mut logic: L,
    ) -> (
        Stream<'scope, T, DCB::Container>,
        Stream<'scope, T, ECB::Container>,
    )
    where
        DCB: ContainerBuilder + PushInto<D2>,
        ECB: ContainerBuilder + PushInto<E>,
        I: IntoIterator<Item = Result<D2, E>>,
        L: for<'a> FnMut(C1::Item<'a>) -> I + 'static,
    {
        self.unary_fallible::<DCB, ECB, _, _>(Pipeline, name, move |_, _| {
            Box::new(move |input, ok_output, err_output| {
                input.for_each_time(|time, data| {
                    let mut ok_session = ok_output.session_with_builder(&time);
                    let mut err_session = err_output.session_with_builder(&time);
                    for r in data
                        .flat_map(DrainContainer::drain)
                        .flat_map(|d1| logic(d1))
                    {
                        match r {
                            Ok(d2) => ok_session.give(d2),
                            Err(e) => err_session.give(e),
                        }
                    }
                })
            })
        })
    }

    fn expire_stream_at(self, name: &str, expiration: T) -> Stream<'scope, T, C1> {
        let name = format!("expire_stream_at({name})");
        self.unary_frontier(Pipeline, &name.clone(), move |cap, _| {
            // Retain a capability for the expiration time, which we'll only drop if the token
            // is dropped. Else, block progress at the expiration time to prevent downstream
            // operators from making any statement about expiration time or any following time.
            let cap = Some(cap.delayed(&expiration));
            let mut warned = false;
            move |(input, frontier), output| {
                let _ = &cap;
                let frontier = frontier.frontier();
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
                input.for_each(|time, data| {
                    let mut session = output.session(&time);
                    session.give_container(data);
                });
            }
        })
    }
}

impl<'scope, T, D1, R> CollectionExt<'scope, T, D1, R> for VecCollection<'scope, T, D1, R>
where
    T: Timestamp + Clone + 'static,
    D1: Clone + 'static,
    R: Semigroup + 'static,
{
    fn empty(scope: Scope<'scope, T>) -> VecCollection<'scope, T, D1, R> {
        operator::empty(scope).as_collection()
    }

    fn flat_map_fallible<DCB, ECB, D2, E, I, L>(
        self,
        name: &str,
        mut logic: L,
    ) -> (
        Collection<'scope, T, DCB::Container>,
        Collection<'scope, T, ECB::Container>,
    )
    where
        DCB: ContainerBuilder + PushInto<(D2, T, R)>,
        ECB: ContainerBuilder + PushInto<(E, T, R)>,
        D2: Clone + 'static,
        E: Clone + 'static,
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

    fn expire_collection_at(self, name: &str, expiration: T) -> VecCollection<'scope, T, D1, R> {
        self.inner
            .expire_stream_at(name, expiration)
            .as_collection()
    }

    fn explode_one<D2, R2, L>(
        self,
        mut logic: L,
    ) -> VecCollection<'scope, T, D2, <R2 as Multiply<R>>::Output>
    where
        D2: differential_dataflow::Data,
        R2: Semigroup + Multiply<R>,
        <R2 as Multiply<R>>::Output: Clone + 'static + Semigroup,
        L: FnMut(D1) -> (D2, R2) + 'static,
        T: Lattice,
    {
        self.inner
            .clone()
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

    fn ensure_monotonic<E, IE>(
        self,
        into_err: IE,
    ) -> (
        VecCollection<'scope, T, D1, R>,
        VecCollection<'scope, T, E, R>,
    )
    where
        E: Clone + 'static,
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
        T: Lattice + Ord + Columnation,
        Ba: Batcher<
                Vec<((D1, ()), T, R)>,
                Time = T,
                Output = Vec<ColumnationStack<((D1, ()), T, R)>>,
            > + 'static,
        Ba: BatcherNew,
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
            // The seeds are fixed for determinism across builds; see
            // [`crate::hash::fixed_state`].
            let random_state = crate::hash::fixed_state();
            let exchange = Exchange::new(move |update: &((D1, _), T, R)| {
                let data = &(update.0).0;
                let mut h = random_state.build_hasher();
                data.hash(&mut h);
                h.finish()
            });
            consolidate_pact::<Ba, _, _>(
                self.map(|k| (k, ())).inner,
                exchange,
                name,
                Ba::new_batcher,
            )
            .unary(Pipeline, "unpack consolidated", |_, _| {
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
        } else {
            self
        }
    }

    fn consolidate_named<Ba>(self, name: &str) -> Self
    where
        D1: differential_dataflow::ExchangeData + Hash + Columnation,
        R: Semigroup + differential_dataflow::ExchangeData + Columnation,
        T: Lattice + Ord + Columnation,
        Ba: Batcher<
                Vec<((D1, ()), T, R)>,
                Time = T,
                Output = Vec<ColumnationStack<((D1, ()), T, R)>>,
            > + 'static,
        Ba: BatcherNew,
    {
        let exchange = Exchange::new(move |update: &((D1, ()), T, R)| (update.0).0.hashed());

        consolidate_pact::<Ba, _, _>(self.map(|k| (k, ())).inner, exchange, name, Ba::new_batcher)
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
pub fn consolidate_pact<'scope, Ba, C, P>(
    stream: Stream<'scope, Ba::Time, C>,
    pact: P,
    name: &str,
    batcher: impl FnOnce(Option<Logger>, usize) -> Ba + 'static,
) -> StreamVec<'scope, Ba::Time, Ba::Output>
where
    Ba: Batcher<C> + 'static,
    Ba::Time: Timestamp,
    C: Container + Clone + 'static,
    Ba::Output: Default + Clone + 'static,
    P: ParallelizationContract<Ba::Time, C>,
{
    let logger = stream
        .scope()
        .worker()
        .logger_for("differential/arrange")
        .map(Into::into);
    stream.unary_frontier(pact, name, move |_cap, info| {
        let mut batcher = batcher(logger, info.global_id);
        // Capabilities for the lower envelope of updates in `batcher`.
        let mut capabilities = Antichain::<Capability<Ba::Time>>::new();
        let mut prev_frontier = Antichain::from_elem(Ba::Time::minimum());
        // `extract` reports the batcher's retained lower bound borrowed from the batcher
        // itself, so it must be copied out before the next call reborrows the batcher.
        let mut batcher_frontier = Antichain::<Ba::Time>::new();

        move |(input, frontier), output| {
            input.for_each(|cap, data| {
                // A message's stamp need not be a singleton, so retain the whole set rather
                // than asking for the one time `retain` would insist on.
                for capability in cap.retain_stamp(0).iter() {
                    capabilities.insert(capability.clone());
                }
                batcher.insert(data);
            });

            if prev_frontier.borrow() != frontier.frontier() {
                if capabilities
                    .elements()
                    .iter()
                    .any(|c| !frontier.less_equal(c.time()))
                {
                    let mut upper = Antichain::new(); // re-used allocation for sealing batches.

                    // For each capability not in advance of the input frontier ...
                    for (index, capability) in capabilities.elements().iter().enumerate() {
                        if !frontier.less_equal(capability.time()) {
                            // Assemble the upper bound on times we can commit with this capabilities.
                            // We must respect the input frontier, and *subsequent* capabilities, as
                            // we are pretending to retire the capability changes one by one.
                            upper.clear();
                            for time in frontier.frontier().iter() {
                                upper.insert(time.clone());
                            }
                            for other_capability in &capabilities.elements()[(index + 1)..] {
                                upper.insert(other_capability.time().clone());
                            }

                            // Extract updates not in advance of `upper`.
                            let (chain, retained) = batcher.extract(upper.borrow());
                            batcher_frontier.clear();
                            batcher_frontier.extend(retained.iter().cloned());

                            // send the batch to downstream consumers, empty or not.
                            let mut session = output.session(&capabilities.elements()[index]);
                            session.give(chain.unwrap_or_default());
                        }
                    }

                    // Having extracted and sent batches between each capability and the input frontier,
                    // we should downgrade all capabilities to match the batcher's lower update frontier.
                    // This may involve discarding capabilities, which is fine as any new updates arrive
                    // in messages with new capabilities.

                    let mut new_capabilities = Antichain::new();
                    for time in batcher_frontier.iter() {
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
                prev_frontier.extend(frontier.frontier().iter().cloned());
            }
        }
    })
}

/// Accumulates updates into sorted, consolidated chains, and releases what a frontier unblocks.
///
/// This is the [`Batcher`] a consolidation wants. Differential's own `MergeBatcher` seals each
/// extracted chain into a trace batch, which a caller that only needs the updates consolidated
/// would immediately have to take apart again; this one hands the chain over as it is.
///
/// `Chu` melds raw input containers into sorted, consolidated chunks, and `M` merges those
/// chunks and splits them by time. The batcher's own work is the geometric ladder of chains
/// and the carve-by-frontier, mirroring `MergeBatcher`'s.
pub struct ConsolidatingBatcher<Chu, M: Merger> {
    /// Melds input containers into sorted, consolidated chunks.
    chunker: Chu,
    /// Sorted, consolidated chains, each paired with its cached summed update count.
    ///
    /// The cached count is the chain's merge weight. A chain is immutable until merged, so
    /// the weight is computed once, at push. Go through [`Self::chain_push`] and
    /// [`Self::chain_pop`] rather than touching this directly, or the accounting drifts.
    chains: Vec<(usize, Vec<M::Chunk>)>,
    /// Stash of empty chunks, recycled through the merging process.
    stash: Vec<M::Chunk>,
    /// Merges consolidated chunks, and splits a chain at a frontier.
    merger: M,
    /// The lower-bound frontier of the data retained after the last extract.
    frontier: Antichain<M::Time>,
    /// Logger for size accounting.
    logger: Option<Logger>,
    /// Timely operator ID, which the accounting is attributed to.
    operator_id: usize,
}

impl<Chu: Default, M: Merger<Time: Timestamp>> ConsolidatingBatcher<Chu, M> {
    /// Allocates a new empty batcher.
    pub fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            chunker: Chu::default(),
            chains: Vec::new(),
            stash: Vec::new(),
            merger: M::default(),
            frontier: Antichain::new(),
            logger,
            operator_id,
        }
    }
}

impl<C, Chu, M> Batcher<C> for ConsolidatingBatcher<Chu, M>
where
    M: Merger<Time: Timestamp>,
    Chu: ContainerBuilder<Container = M::Chunk> + for<'a> PushInto<&'a mut C>,
{
    type Time = M::Time;
    type Output = Vec<M::Chunk>;

    fn insert(&mut self, container: &mut C) {
        self.chunker.push_into(container);
        while let Some(chunk) = self.chunker.extract().map(std::mem::take) {
            self.insert_chain(vec![chunk]);
        }
    }

    fn extract<'a>(
        &'a mut self,
        upper: AntichainRef<'_, M::Time>,
    ) -> (Option<Self::Output>, AntichainRef<'a, M::Time>) {
        // Flush whatever the chunker is still accumulating: a partial final chunk would
        // otherwise never reach the merge ladder.
        while let Some(chunk) = self.chunker.finish().map(std::mem::take) {
            self.insert_chain(vec![chunk]);
        }

        while self.chains.len() > 1 {
            let list1 = self.chain_pop().unwrap();
            let list2 = self.chain_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.chain_push(merged);
        }
        let merged = self.chain_pop().unwrap_or_default();

        let mut kept = Vec::new();
        let mut readied = Vec::new();
        self.frontier.clear();
        self.merger.extract(
            merged,
            upper,
            &mut self.frontier,
            &mut readied,
            &mut kept,
            &mut self.stash,
        );

        if !kept.is_empty() {
            self.chain_push(kept);
        }
        self.stash.clear();

        let readied = (!readied.is_empty()).then_some(readied);
        (readied, self.frontier.borrow())
    }
}

impl<Chu, M: Merger> ConsolidatingBatcher<Chu, M> {
    /// Insert one already sorted and consolidated chunk, bypassing the chunker.
    ///
    /// The ladder assumes each chunk it holds is sorted and consolidated, so a caller that
    /// prepared the chunk itself uses this; everything else goes through [`Batcher::insert`].
    pub fn push_chunk(&mut self, chunk: M::Chunk) {
        self.insert_chain(vec![chunk]);
    }

    /// Insert a chain and restore the ladder: chains are geometrically sized by summed
    /// updates and ordered by decreasing weight.
    fn insert_chain(&mut self, chain: Vec<M::Chunk>) {
        if !chain.is_empty() {
            self.chain_push(chain);
            while self.chains.len() > 1
                && (self.chains[self.chains.len() - 1].0
                    >= self.chains[self.chains.len() - 2].0 / 2)
            {
                let list1 = self.chain_pop().unwrap();
                let list2 = self.chain_pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.chain_push(merged);
            }
        }
    }

    fn merge_by(&mut self, list1: Vec<M::Chunk>, list2: Vec<M::Chunk>) -> Vec<M::Chunk> {
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        self.merger
            .merge(list1, list2, &mut output, &mut self.stash);
        output
    }

    fn chain_pop(&mut self) -> Option<Vec<M::Chunk>> {
        let (_weight, chain) = self.chains.pop()?;
        self.account(chain.iter().map(Self::record), -1);
        Some(chain)
    }

    fn chain_push(&mut self, chain: Vec<M::Chunk>) {
        let weight = chain.iter().map(M::len).sum();
        self.account(chain.iter().map(Self::record), 1);
        self.chains.push((weight, chain));
    }

    fn record(chunk: &M::Chunk) -> (usize, usize, usize, usize) {
        let (size, capacity, allocations) = M::allocation(chunk);
        (M::len(chunk), size, capacity, allocations)
    }

    /// Report a signed change in the resident chains, if a logger is attached.
    fn account<I: IntoIterator<Item = (usize, usize, usize, usize)>>(&self, items: I, diff: isize) {
        let Some(logger) = &self.logger else {
            return;
        };
        let (mut records, mut size, mut capacity, mut allocations) =
            (0isize, 0isize, 0isize, 0isize);
        for (records_, size_, capacity_, allocations_) in items {
            records = records.saturating_add_unsigned(records_);
            size = size.saturating_add_unsigned(size_);
            capacity = capacity.saturating_add_unsigned(capacity_);
            allocations = allocations.saturating_add_unsigned(allocations_);
        }
        logger.log(BatcherEvent {
            operator: self.operator_id,
            records_diff: records.saturating_mul(diff),
            size_diff: size.saturating_mul(diff),
            capacity_diff: capacity.saturating_mul(diff),
            allocations_diff: allocations.saturating_mul(diff),
        });
    }
}

impl<Chu, M: Merger> Drop for ConsolidatingBatcher<Chu, M> {
    fn drop(&mut self) {
        // Retract the accounting for whatever is still resident, so the per-operator
        // counters end at zero.
        while self.chain_pop().is_some() {}
    }
}

/// A batcher with differential's constructor shape, for generic code that must build one.
///
/// [`Batcher`] itself has no constructor: differential's operators take one as an `FnOnce`
/// argument, which callers can supply because they name the batcher type. A generic caller
/// that does not, such as [`CollectionExt::consolidate_named`], needs this instead.
pub trait BatcherNew {
    /// Allocates an empty batcher, reporting its footprint against `operator_id`.
    fn new_batcher(logger: Option<Logger>, operator_id: usize) -> Self;
}

impl<Chu: Default, M: Merger<Time: Timestamp>> BatcherNew for ConsolidatingBatcher<Chu, M> {
    fn new_batcher(logger: Option<Logger>, operator_id: usize) -> Self {
        Self::new(logger, operator_id)
    }
}

/// Merge the contents of multiple streams and combine the containers using a container builder.
pub trait ConcatenateFlatten<'scope, T: Timestamp, C: Container + DrainContainer> {
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
    ///     let streams: Vec<timely::dataflow::StreamVec<_, i32>> =
    ///         vec![(0..10).to_stream(scope),
    ///              (0..10).to_stream(scope),
    ///              (0..10).to_stream(scope)];
    ///
    ///     scope.concatenate_flatten::<_, CapacityContainerBuilder<Vec<i32>>>(streams)
    ///          .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn concatenate_flatten<I, CB>(&self, sources: I) -> Stream<'scope, T, CB::Container>
    where
        I: IntoIterator<Item = Stream<'scope, T, C>>,
        CB: ContainerBuilder + for<'a> PushInto<C::Item<'a>>;
}

impl<'scope, T, C> ConcatenateFlatten<'scope, T, C> for Stream<'scope, T, C>
where
    T: Timestamp,
    C: Container + DrainContainer + Clone + 'static,
{
    fn concatenate_flatten<I, CB>(&self, sources: I) -> Stream<'scope, T, CB::Container>
    where
        I: IntoIterator<Item = Stream<'scope, T, C>>,
        CB: ContainerBuilder + for<'a> PushInto<C::Item<'a>>,
    {
        self.scope()
            .concatenate_flatten::<_, CB>(Some(Clone::clone(self)).into_iter().chain(sources))
    }
}

impl<'scope, T, C> ConcatenateFlatten<'scope, T, C> for Scope<'scope, T>
where
    T: Timestamp,
    C: Container + DrainContainer,
{
    fn concatenate_flatten<I, CB>(&self, sources: I) -> Stream<'scope, T, CB::Container>
    where
        I: IntoIterator<Item = Stream<'scope, T, C>>,
        CB: ContainerBuilder + for<'a> PushInto<C::Item<'a>>,
    {
        let mut builder = OperatorBuilder::new("ConcatenateFlatten".to_string(), self.clone());

        // create new input handles for each input stream.
        let mut handles = sources
            .into_iter()
            .map(|s| builder.new_input(s, Pipeline))
            .collect::<Vec<_>>();
        for i in 0..handles.len() {
            builder.set_notify_for(i, FrontierInterest::Never);
        }

        // create one output handle for the concatenated results.
        let (output, result) = builder.new_output::<CB::Container>();
        let mut output = OutputBuilder::<_, CB>::from(output);

        builder.build(move |_capability| {
            move |_frontier| {
                let mut output = output.activate();
                for handle in handles.iter_mut() {
                    handle.for_each_time(|time, data| {
                        output
                            .session_with_builder(&time)
                            .give_iterator(data.flat_map(DrainContainer::drain));
                    })
                }
            }
        });

        result
    }
}

/// A trait for containers that can be cleared.
pub trait ClearContainer {
    /// Clear the contents of the container.
    fn clear(&mut self);
}

impl<T> ClearContainer for Vec<T> {
    fn clear(&mut self) {
        Vec::clear(self)
    }
}
