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
use differential_dataflow::trace::{Batch, Batcher, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable};
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, ScopeParent, StreamCore};
use timely::progress::Timestamp;
use timely::Container;

use crate::logging::compute::ComputeEvent;
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
    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<Self::Scope, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Input = Self::Input, Time = <Self::Scope as ScopeParent>::Timestamp>,
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
    type Input: Container;

    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`. Partitions
    /// the data according to `pact`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<Self::Scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<<Self::Scope as ScopeParent>::Timestamp, Self::Input>,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Input = Self::Input, Time = <Self::Scope as ScopeParent>::Timestamp>,
        Arranged<Self::Scope, TraceAgent<Tr>>: ArrangementSize;
}

impl<G, C> MzArrangeCore for StreamCore<G, C>
where
    G: Scope,
    G::Timestamp: Lattice,
    C: Container,
{
    type Scope = G;
    type Input = C;

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, Self::Input>,
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Input = Self::Input>,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        arrange_core(self, pact, name).log_arrangement_size()
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
    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Input = Self::Input>,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        let exchange =
            Exchange::new(move |update: &((K, V), G::Timestamp, R)| (update.0).0.hashed().into());
        self.mz_arrange_core(exchange, name)
    }
}

impl<G, K, V, R, C> MzArrangeCore for Collection<G, (K, V), R, C>
where
    G: Scope,
    G::Timestamp: Lattice,
    C: Container,
{
    type Scope = G;
    type Input = C;

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, Self::Input>,
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Input = Self::Input>,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        self.inner.mz_arrange_core(pact, name)
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
    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Input = Self::Input, Time = G::Timestamp>,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0.map(|d| (d, ())).mz_arrange(name)
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

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, Self::Input>,
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Input = Self::Input, Time = G::Timestamp>,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0.map(|d| (d, ())).mz_arrange_core(pact, name)
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
        .get::<ComputeEvent>("materialize/compute")
    else {
        return arranged;
    };
    let operator = arranged.trace.operator().global_id;
    let trace = Rc::downgrade(&arranged.trace.trace_box_unstable());

    let (mut old_size, mut old_capacity, mut old_allocations) = (0isize, 0isize, 0isize);

    let stream = arranged
        .stream
        .unary(Pipeline, "ArrangementSize", |_cap, info| {
            let mut buffer = Default::default();
            let address = info.address;
            logger.log(ComputeEvent::ArrangementHeapSizeOperator { operator, address });
            move |input, output| {
                while let Some((time, data)) = input.next() {
                    data.swap(&mut buffer);
                    output.session(&time).give_container(&mut buffer);
                }
                let Some(trace) = trace.upgrade() else {
                    return;
                };

                let (size, capacity, allocations) = logic(&trace.borrow().trace);

                let size = size.try_into().expect("must fit");
                if size != old_size {
                    logger.log(ComputeEvent::ArrangementHeapSize {
                        operator,
                        delta_size: size - old_size,
                    });
                }

                let capacity = capacity.try_into().expect("must fit");
                if capacity != old_capacity {
                    logger.log(ComputeEvent::ArrangementHeapCapacity {
                        operator,
                        delta_capacity: capacity - old_capacity,
                    });
                }

                let allocations = allocations.try_into().expect("must fit");
                if allocations != old_allocations {
                    logger.log(ComputeEvent::ArrangementHeapAllocations {
                        operator,
                        delta_allocations: allocations - old_allocations,
                    });
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
    use differential_dataflow::Data;
    use std::fmt::Debug;
    use timely::container::flatcontainer::{Containerized, IntoOwned, Push, Region, ReserveItems};
    use timely::dataflow::Scope;
    use timely::progress::Timestamp;
    use timely::PartialOrder;

    use crate::extensions::arrange::{log_arrangement_size_inner, ArrangementSize};
    use crate::typedefs::{FlatKeyValAgent, FlatKeyValSpine};

    impl<G, K, V, T, R, C> ArrangementSize for Arranged<G, FlatKeyValAgent<K, V, T, R, C>>
    where
        Self: Clone,
        G: Scope<Timestamp = T>,
        G::Timestamp: Lattice + Ord + Containerized,
        K: Data + Containerized,
        K::Region: Clone
            + Region<Owned = K>
            + Push<K>
            + for<'a> Push<<K::Region as Region>::ReadItem<'a>>
            + for<'a> ReserveItems<<K::Region as Region>::ReadItem<'a>>,
        for<'a> <K::Region as Region>::ReadItem<'a>: Copy + Debug + Ord,
        V: Data + Containerized,
        V::Region: Clone
            + Push<V>
            + for<'a> Push<<V::Region as Region>::ReadItem<'a>>
            + Region<Owned = V>
            + for<'a> ReserveItems<<V::Region as Region>::ReadItem<'a>>,
        for<'a> <V::Region as Region>::ReadItem<'a>: Copy + Debug + Ord,
        T: Containerized
            + Lattice
            + for<'a> PartialOrder<<T::Region as Region>::ReadItem<'a>>
            + Timestamp,
        T::Region: Clone
            + Push<T>
            + for<'a> Push<<T::Region as Region>::ReadItem<'a>>
            + Region<Owned = T>
            + for<'a> ReserveItems<<T::Region as Region>::ReadItem<'a>>,
        for<'a> <T::Region as Region>::ReadItem<'a>:
            Copy + Debug + IntoOwned<'a, Owned = T> + Ord + PartialOrder<T>,
        R: Containerized
            + Default
            + Ord
            + Semigroup
            + for<'a> Semigroup<<R::Region as Region>::ReadItem<'a>>
            + 'static,
        R::Region: Clone
            + Push<R>
            + for<'a> Push<&'a R>
            + for<'a> Push<<R::Region as Region>::ReadItem<'a>>
            + Region<Owned = R>
            + for<'a> ReserveItems<<R::Region as Region>::ReadItem<'a>>,
        for<'a> <R::Region as Region>::ReadItem<'a>: Copy + Debug + IntoOwned<'a, Owned = R> + Ord,
        C: 'static,
    {
        fn log_arrangement_size(self) -> Self {
            log_arrangement_size_inner::<_, FlatKeyValSpine<K, V, T, R, C>, _>(self, |trace| {
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
