// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
use differential_dataflow::trace::{Batch, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable};
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::{Antichain, Timestamp};

use crate::logging::compute::ComputeEvent;
use crate::typedefs::{RowKeySpine, RowSpine};

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
        Tr: Trace
            + TraceReader<
                Key = Self::Key,
                Val = Self::Val,
                Time = <Self::Scope as ScopeParent>::Timestamp,
                R = Self::R,
            > + 'static,
        Tr::Batch: Batch,
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
        Tr: Trace
            + TraceReader<
                Key = Self::Key,
                Val = Self::Val,
                Time = <Self::Scope as ScopeParent>::Timestamp,
                R = Self::R,
            > + 'static,
        Tr::Batch: Batch,
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
        Tr: Trace + TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_named(name).log_arrangement_size()
    }

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K, V), G::Timestamp, R)>,
        Tr: Trace + TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
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
        Tr: Trace + TraceReader<Key = K, Val = (), Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0.map(|d| (d, ())).mz_arrange(name)
    }

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K, ()), G::Timestamp, R)>,
        Tr: Trace + TraceReader<Key = K, Val = (), Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
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

/// Helper to compute the size of a vector in memory.
///
/// The function only considers the immediate allocation of the vector, but is oblivious of any
/// pointers to owned allocations.
#[inline]
fn vec_size<T>(data: &Vec<T>, mut callback: impl FnMut(usize, usize)) {
    let size_of_t = std::mem::size_of::<T>();
    callback(data.len() * size_of_t, data.capacity() * size_of_t);
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
    L: FnMut(&TraceAgent<Tr>) -> (usize, usize, usize) + 'static,
{
    let scope = arranged.stream.scope();
    let Some(logger) = scope.log_register().get::<ComputeEvent>("materialize/compute") else {return arranged};
    let mut trace = arranged.trace.clone();
    let operator = trace.operator().global_id;

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

                // We don't want to block compaction.
                let mut upper = Antichain::new();
                trace.read_upper(&mut upper);
                trace.set_logical_compaction(upper.borrow());
                trace.set_physical_compaction(upper.borrow());

                let (size, capacity, allocations) = logic(&trace);

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

impl<G, K, V, T, R> ArrangementSize for Arranged<G, TraceAgent<RowSpine<K, V, T, R>>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord,
    K: Data + Columnation,
    V: Data + Columnation,
    T: Lattice + Timestamp,
    R: Semigroup,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                allocations += 1;
                size += siz;
                capacity += cap
            };
            trace.map_batches(|batch| {
                batch.layer.keys.heap_size(&mut callback);
                batch.layer.vals.keys.heap_size(&mut callback);
                vec_size(&batch.layer.offs, &mut callback);
                vec_size(&batch.layer.vals.offs, &mut callback);
                vec_size(&batch.layer.vals.vals.vals, &mut callback);
            });
            (size, capacity, allocations)
        })
    }
}

impl<G, K, T, R> ArrangementSize for Arranged<G, TraceAgent<RowKeySpine<K, T, R>>>
where
    G: Scope<Timestamp = T>,
    G::Timestamp: Lattice + Ord,
    K: Data + Columnation,
    T: Lattice + Timestamp,
    R: Semigroup,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |trace| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                allocations += 1;
                size += siz;
                capacity += cap
            };
            trace.map_batches(|batch| {
                batch.layer.keys.heap_size(&mut callback);
                vec_size(&batch.layer.offs, &mut callback);
                vec_size(&batch.layer.vals.vals, &mut callback);
            });
            (size, capacity, allocations)
        })
    }
}
