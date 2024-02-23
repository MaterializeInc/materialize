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
use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
use differential_dataflow::trace::{Batch, Batcher, Builder, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable};
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::Timestamp;

use crate::logging::compute::ComputeEvent;
use crate::typedefs::{KeyAgent, KeyValAgent, RowAgent, RowRowAgent, RowValAgent};

/// Extension trait to arrange data.
pub trait MzArrange
where
    <Self::Scope as ScopeParent>::Timestamp: Lattice,
{
    /// The current scope.
    type Scope: Scope;
    /// The key type.
    type Key: Data;
    /// The value type.
    type Val: Data;
    /// The difference type.
    type R: Data + Semigroup;

    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<Self::Scope, TraceAgent<Tr>>
    where
        Self::Key: ExchangeData + Hashable,
        Self::Val: ExchangeData,
        Self::R: ExchangeData,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<
            Item = (
                (Self::Key, Self::Val),
                <Self::Scope as ScopeParent>::Timestamp,
                Self::R,
            ),
            Time = <Self::Scope as ScopeParent>::Timestamp,
        >,
        Tr::Builder: Builder<
            Item = (
                (Self::Key, Self::Val),
                <Self::Scope as ScopeParent>::Timestamp,
                Self::R,
            ),
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Output = Tr::Batch,
        >,
        Arranged<Self::Scope, TraceAgent<Tr>>: ArrangementSize;

    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`. Partitions
    /// the data according to `pact`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<Self::Scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<
            <Self::Scope as ScopeParent>::Timestamp,
            (
                (Self::Key, Self::Val),
                <Self::Scope as ScopeParent>::Timestamp,
                Self::R,
            ),
        >,
        Tr: Trace + TraceReader<Time = <Self::Scope as ScopeParent>::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<
            Item = (
                (Self::Key, Self::Val),
                <Self::Scope as ScopeParent>::Timestamp,
                Self::R,
            ),
            Time = <Self::Scope as ScopeParent>::Timestamp,
        >,
        Tr::Builder: Builder<
            Item = (
                (Self::Key, Self::Val),
                <Self::Scope as ScopeParent>::Timestamp,
                Self::R,
            ),
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Output = Tr::Batch,
        >,
        Arranged<Self::Scope, TraceAgent<Tr>>: ArrangementSize;
}

impl<G, K, V, R> MzArrange for Collection<G, (K, V), R>
where
    G: Scope,
    G::Timestamp: Lattice,
    K: Data + Columnation,
    V: Data + Columnation,
    R: Semigroup,
{
    type Scope = G;
    type Key = K;
    type Val = V;
    type R = R;

    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData,
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Item = ((K, V), G::Timestamp, R), Time = G::Timestamp>,
        Tr::Builder:
            Builder<Item = ((K, V), G::Timestamp, R), Time = G::Timestamp, Output = Tr::Batch>,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_named(name).log_arrangement_size()
    }

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K, V), G::Timestamp, R)>,
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<
            Item = (
                (Self::Key, Self::Val),
                <Self::Scope as ScopeParent>::Timestamp,
                Self::R,
            ),
            Time = <Self::Scope as ScopeParent>::Timestamp,
        >,
        Tr::Builder: Builder<
            Item = (
                (Self::Key, Self::Val),
                <Self::Scope as ScopeParent>::Timestamp,
                Self::R,
            ),
            Time = <Self::Scope as ScopeParent>::Timestamp,
            Output = Tr::Batch,
        >,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_core(pact, name).log_arrangement_size()
    }
}

/// A specialized collection where data only has a key, but no associated value.
///
/// Created by calling `collection.into()`.
pub struct KeyCollection<G: Scope, K, R: Semigroup = usize>(Collection<G, K, R>);

impl<G: Scope, K, R: Semigroup> From<Collection<G, K, R>> for KeyCollection<G, K, R> {
    fn from(value: Collection<G, K, R>) -> Self {
        KeyCollection(value)
    }
}

impl<G, K, R> MzArrange for KeyCollection<G, K, R>
where
    G: Scope,
    K: Data + Columnation,
    G::Timestamp: Lattice,
    R: Semigroup,
{
    type Scope = G;
    type Key = K;
    type Val = ();
    type R = R;

    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData + Hashable,
        R: ExchangeData,
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Item = ((K, ()), G::Timestamp, R), Time = G::Timestamp>,
        Tr::Builder:
            Builder<Item = ((K, ()), G::Timestamp, R), Time = G::Timestamp, Output = Tr::Batch>,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0.map(|d| (d, ())).mz_arrange(name)
    }

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K, ()), G::Timestamp, R)>,
        Tr: Trace + TraceReader<Time = G::Timestamp> + 'static,
        Tr::Batch: Batch,
        Tr::Batcher: Batcher<Item = ((K, ()), G::Timestamp, R), Time = G::Timestamp>,
        Tr::Builder:
            Builder<Item = ((K, ()), G::Timestamp, R), Time = G::Timestamp, Output = Tr::Batch>,
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
    R: Semigroup + Columnation,
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
                batch.storage.updates.heap_size(&mut callback);
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
    R: Semigroup + Columnation,
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
                batch.storage.updates.heap_size(&mut callback);
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
    R: Semigroup + Columnation,
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
                batch.storage.updates.heap_size(&mut callback);
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
    R: Semigroup + Columnation,
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
                batch.storage.updates.heap_size(&mut callback);
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
    R: Semigroup + Columnation,
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
                batch.storage.updates.heap_size(&mut callback);
            });
            (size, capacity, allocations)
        })
    }
}
