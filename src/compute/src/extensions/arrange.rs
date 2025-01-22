// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::rc::Rc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::{Batch, Batcher, Builder, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable};
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, ScopeParent, StreamCore};
use timely::progress::Timestamp;
use timely::Container;

use crate::logging::compute::{
    ArrangementHeapAllocations, ArrangementHeapCapacity, ArrangementHeapSize,
    ArrangementHeapSizeOperator, ComputeEvent, ComputeEventBuilder,
};
use crate::typedefs::{KeyAgent, KeyValAgent, RowAgent, RowRowAgent, RowValAgent};

/// Extension trait to arrange data.
pub trait MzArrange: MzArrangeCore
where
    <Self::Scope as ScopeParent>::Timestamp: Lattice,
{
    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange<Ba, Bu, Tr>(&self, name: &str) -> Arranged<Self::Scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Input = Self::Input, Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Bu: Builder<
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Input = Ba::Output,
            Output = Tr::Batch,
        >,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<Self::Scope, TraceAgent<Tr>>: ArrangementSize;
}

/// Extension trait to arrange data.
pub trait MzArrangeCore
where
    <Self::Scope as ScopeParent>::Timestamp: Lattice,
{
    /// The current scope.
    type Scope: Scope;
    /// The data input container type.
    type Input: Container + Clone + 'static;

    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`. Partitions
    /// the data according to `pact`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange_core<P, Ba, Bu, Tr>(
        &self,
        pact: P,
        name: &str,
    ) -> Arranged<Self::Scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<<Self::Scope as ScopeParent>::Timestamp, Self::Input>,
        Ba: Batcher<Input = Self::Input, Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        // Ba::Input: Container + Clone + 'static,
        Bu: Builder<
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Input = Ba::Output,
            Output = Tr::Batch,
        >,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<Self::Scope, TraceAgent<Tr>>: ArrangementSize;
}

impl<G, C> MzArrangeCore for StreamCore<G, C>
where
    G: Scope,
    G::Timestamp: Lattice,
    C: Container + Clone + 'static,
{
    type Scope = G;
    type Input = C;

    fn mz_arrange_core<P, Ba, Bu, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, Self::Input>,
        Ba: Batcher<Input = Self::Input, Time = G::Timestamp> + 'static,
        Bu: Builder<Time = G::Timestamp, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        arrange_core::<_, _, Ba, Bu, _>(self, pact, name).log_arrangement_size()
    }
}

impl<G, K, V, R> MzArrange for Collection<G, (K, V), R>
where
    G: Scope,
    G::Timestamp: Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: ExchangeData,
{
    fn mz_arrange<Ba, Bu, Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        Ba: Batcher<Input = Self::Input, Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Bu: Builder<
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Input = Ba::Output,
            Output = Tr::Batch,
        >,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        let exchange =
            Exchange::new(move |update: &((K, V), G::Timestamp, R)| (update.0).0.hashed().into());
        self.mz_arrange_core::<_, Ba, Bu, _>(exchange, name)
    }
}

impl<G, K, V, R, C> MzArrangeCore for Collection<G, (K, V), R, C>
where
    G: Scope,
    G::Timestamp: Lattice,
    C: Container + Clone + 'static,
{
    type Scope = G;
    type Input = C;

    fn mz_arrange_core<P, Ba, Bu, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, Self::Input>,
        Ba: Batcher<Input = Self::Input, Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Bu: Builder<
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Input = Ba::Output,
            Output = Tr::Batch,
        >,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        self.inner.mz_arrange_core::<_, Ba, Bu, _>(pact, name)
    }
}

/// A specialized collection where data only has a key, but no associated value.
///
/// Created by calling `collection.into()`.
pub struct KeyCollection<G: Scope, K, R = usize>(Collection<G, K, R>);

impl<G: Scope, K, R: Semigroup> From<Collection<G, K, R>> for KeyCollection<G, K, R> {
    fn from(value: Collection<G, K, R>) -> Self {
        KeyCollection(value)
    }
}

impl<G, K, R> MzArrange for KeyCollection<G, K, R>
where
    G: Scope,
    K: ExchangeData + Hashable,
    G::Timestamp: Lattice,
    R: ExchangeData,
{
    fn mz_arrange<Ba, Bu, Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        Ba: Batcher<Input = Self::Input, Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Bu: Builder<
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Input = Ba::Output,
            Output = Tr::Batch,
        >,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0.map(|d| (d, ())).mz_arrange::<Ba, Bu, _>(name)
    }
}

impl<G, K, R> MzArrangeCore for KeyCollection<G, K, R>
where
    G: Scope,
    K: Data,
    G::Timestamp: Lattice,
    R: Data,
{
    type Scope = G;
    type Input = Vec<((K, ()), G::Timestamp, R)>;

    fn mz_arrange_core<P, Ba, Bu, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, Self::Input>,
        Ba: Batcher<Input = Self::Input, Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Bu: Builder<
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Input = Ba::Output,
            Output = Tr::Batch,
        >,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0
            .map(|d| (d, ()))
            .mz_arrange_core::<_, Ba, Bu, _>(pact, name)
    }
}

/// A type that can log its heap size.
pub trait ArrangementSize {
    /// Install a logger to track the heap size of the target.
    fn log_arrangement_size(self) -> Self;
}

/// Helper for [`ArrangementSize`] to install a common operator holding on to a trace.
///
/// * `arranged`: The arrangement to inspect.
/// * `logic`: Closure that calculates the heap size/capacity/allocations for a trace. The return
///    value are size and capacity in bytes, and number of allocations, all in absolute values.
fn log_arrangement_size_inner<G, Tr, L>(
    arranged: Arranged<G, TraceAgent<Tr>>,
    mut logic: L,
) -> Arranged<G, TraceAgent<Tr>>
where
    G: Scope,
    G::Timestamp: Timestamp + Lattice + Ord,
    Tr: TraceReader + 'static,
    Tr::Time: Timestamp + Lattice + Ord + Clone + 'static,
    L: FnMut(&Tr) -> (usize, usize, usize) + 'static,
{
    let scope = arranged.stream.scope();
    let Some(logger) = scope
        .log_register()
        .get::<ComputeEventBuilder>("materialize/compute")
    else {
        return arranged;
    };
    let operator_id = arranged.trace.operator().global_id;
    let trace = Rc::downgrade(&arranged.trace.trace_box_unstable());

    let (mut old_size, mut old_capacity, mut old_allocations) = (0isize, 0isize, 0isize);

    let stream = arranged
        .stream
        .unary(Pipeline, "ArrangementSize", |_cap, info| {
            let address = info.address;
            logger.log(&ComputeEvent::ArrangementHeapSizeOperator(
                ArrangementHeapSizeOperator {
                    operator_id,
                    address: address.to_vec(),
                },
            ));
            move |input, output| {
                while let Some((time, data)) = input.next() {
                    output.session(&time).give_container(data);
                }
                let Some(trace) = trace.upgrade() else {
                    return;
                };

                let (size, capacity, allocations) = logic(&trace.borrow().trace);

                let size = size.try_into().expect("must fit");
                if size != old_size {
                    logger.log(&ComputeEvent::ArrangementHeapSize(ArrangementHeapSize {
                        operator_id,
                        delta_size: size - old_size,
                    }));
                }

                let capacity = capacity.try_into().expect("must fit");
                if capacity != old_capacity {
                    logger.log(&ComputeEvent::ArrangementHeapCapacity(
                        ArrangementHeapCapacity {
                            operator_id,
                            delta_capacity: capacity - old_capacity,
                        },
                    ));
                }

                let allocations = allocations.try_into().expect("must fit");
                if allocations != old_allocations {
                    logger.log(&ComputeEvent::ArrangementHeapAllocations(
                        ArrangementHeapAllocations {
                            operator_id,
                            delta_allocations: allocations - old_allocations,
                        },
                    ));
                }

                old_size = size;
                old_capacity = capacity;
                old_allocations = allocations;
            }
        });
    Arranged {
        trace: arranged.trace,
        stream,
    }
}

impl<G, K, V, T, R> ArrangementSize for Arranged<G, KeyValAgent<K, V, T, R>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord + Columnation,
    K: Data + Columnation,
    V: Data + Columnation,
    T: Lattice + Timestamp,
    R: Semigroup + Ord + Columnation + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            trace.map_batches(|batch| {
                batch.storage.keys.heap_size(&mut callback);
                batch.storage.keys_offs.heap_size(&mut callback);
                batch.storage.vals.heap_size(&mut callback);
                batch.storage.vals_offs.heap_size(&mut callback);
                batch.storage.times.heap_size(&mut callback);
                batch.storage.diffs.heap_size(&mut callback);
            });
            (size, capacity, allocations)
        })
    }
}

impl<G, K, T, R> ArrangementSize for Arranged<G, KeyAgent<K, T, R>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord,
    K: Data + Columnation,
    T: Lattice + Timestamp + Columnation,
    R: Semigroup + Ord + Columnation + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            trace.map_batches(|batch| {
                batch.storage.keys.heap_size(&mut callback);
                batch.storage.keys_offs.heap_size(&mut callback);
                batch.storage.times.heap_size(&mut callback);
                batch.storage.diffs.heap_size(&mut callback);
            });
            (size, capacity, allocations)
        })
    }
}

impl<G, V, T, R> ArrangementSize for Arranged<G, RowValAgent<V, T, R>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord + Columnation,
    V: Data + Columnation,
    T: Lattice + Timestamp,
    R: Semigroup + Ord + Columnation + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            trace.map_batches(|batch| {
                batch.storage.keys.heap_size(&mut callback);
                batch.storage.keys_offs.heap_size(&mut callback);
                batch.storage.vals.heap_size(&mut callback);
                batch.storage.vals_offs.heap_size(&mut callback);
                batch.storage.times.heap_size(&mut callback);
                batch.storage.diffs.heap_size(&mut callback);
            });
            (size, capacity, allocations)
        })
    }
}

impl<G, T, R> ArrangementSize for Arranged<G, RowRowAgent<T, R>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord + Columnation,
    T: Lattice + Timestamp,
    R: Semigroup + Ord + Columnation + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            trace.map_batches(|batch| {
                batch.storage.keys.heap_size(&mut callback);
                batch.storage.keys_offs.heap_size(&mut callback);
                batch.storage.vals.heap_size(&mut callback);
                batch.storage.vals_offs.heap_size(&mut callback);
                batch.storage.times.heap_size(&mut callback);
                batch.storage.diffs.heap_size(&mut callback);
            });
            (size, capacity, allocations)
        })
    }
}

impl<G, T, R> ArrangementSize for Arranged<G, RowAgent<T, R>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord + Columnation,
    T: Lattice + Timestamp,
    R: Semigroup + Ord + Columnation + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            trace.map_batches(|batch| {
                batch.storage.keys.heap_size(&mut callback);
                batch.storage.keys_offs.heap_size(&mut callback);
                batch.storage.times.heap_size(&mut callback);
                batch.storage.diffs.heap_size(&mut callback);
            });
            (size, capacity, allocations)
        })
    }
}

mod flatcontainer {
    use differential_dataflow::difference::Semigroup;
    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::operators::arrange::Arranged;
    use differential_dataflow::trace::TraceReader;
    use mz_ore::flatcontainer::MzRegionPreference;
    use timely::container::flatcontainer::{IntoOwned, Push, Region, ReserveItems};
    use timely::dataflow::Scope;
    use timely::progress::Timestamp;
    use timely::PartialOrder;

    use crate::extensions::arrange::{log_arrangement_size_inner, ArrangementSize};
    use crate::typedefs::{FlatKeyValAgent, FlatKeyValSpine};

    impl<G, K, V, T, R> ArrangementSize for Arranged<G, FlatKeyValAgent<K, V, T, R>>
    where
        Self: Clone,
        G: Scope<Timestamp = T::Owned>,
        G::Timestamp: Lattice + Ord + MzRegionPreference,
        K: Region
            + Clone
            + Push<<K as Region>::Owned>
            + for<'a> Push<<K as Region>::ReadItem<'a>>
            + for<'a> ReserveItems<<K as Region>::ReadItem<'a>>
            + 'static,
        V: Region
            + Clone
            + Push<<V as Region>::Owned>
            + for<'a> Push<<V as Region>::ReadItem<'a>>
            + for<'a> ReserveItems<<V as Region>::ReadItem<'a>>
            + 'static,
        T: Region
            + Clone
            + Push<<T as Region>::Owned>
            + for<'a> Push<<T as Region>::ReadItem<'a>>
            + for<'a> ReserveItems<<T as Region>::ReadItem<'a>>
            + 'static,
        R: Region
            + Clone
            + Push<<R as Region>::Owned>
            + for<'a> Push<&'a <R as Region>::Owned>
            + for<'a> Push<<R as Region>::ReadItem<'a>>
            + for<'a> ReserveItems<<R as Region>::ReadItem<'a>>
            + 'static,
        K::Owned: Clone + Ord,
        V::Owned: Clone + Ord,
        T::Owned: Lattice + for<'a> PartialOrder<<T as Region>::ReadItem<'a>> + Timestamp,
        R::Owned:
            Default + Ord + Semigroup + for<'a> Semigroup<<R as Region>::ReadItem<'a>> + 'static,
        for<'a> <K as Region>::ReadItem<'a>: Copy + Ord,
        for<'a> <V as Region>::ReadItem<'a>: Copy + Ord,
        for<'a> <T as Region>::ReadItem<'a>: Copy + IntoOwned<'a> + Ord + PartialOrder<T::Owned>,
        for<'a> <R as Region>::ReadItem<'a>: Copy + IntoOwned<'a, Owned = R::Owned> + Ord,
    {
        fn log_arrangement_size(self) -> Self {
            log_arrangement_size_inner::<_, FlatKeyValSpine<K, V, T, R>, _>(self, |trace| {
                let (mut size, mut capacity, mut allocations) = (0, 0, 0);
                let mut callback = |siz, cap| {
                    size += siz;
                    capacity += cap;
                    allocations += usize::from(cap > 0);
                };
                trace.map_batches(|batch| {
                    batch.storage.keys.heap_size(&mut callback);
                    batch.storage.keys_offs.heap_size(&mut callback);
                    batch.storage.vals.heap_size(&mut callback);
                    batch.storage.vals_offs.heap_size(&mut callback);
                    batch.storage.times.heap_size(&mut callback);
                    batch.storage.diffs.heap_size(&mut callback);
                });
                (size, capacity, allocations)
            })
        }
    }
}
