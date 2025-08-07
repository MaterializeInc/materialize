// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::rc::Rc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::{Batch, Batcher, Builder, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable};
use timely::Container;
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, ScopeParent, StreamCore};

use crate::logging::compute::{
    ArrangementHeapAllocations, ArrangementHeapCapacity, ArrangementHeapSize,
    ArrangementHeapSizeOperator, ComputeEvent, ComputeEventBuilder,
};
use crate::typedefs::{
    KeyAgent, KeyValAgent, MzArrangeData, MzData, MzTimestamp, RowAgent, RowRowAgent, RowValAgent,
};

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
/// * `logic`: Closure that calculates the heap size/capacity/allocations for a batch. The return
///    value are size and capacity in bytes, and number of allocations, all in absolute values.
fn log_arrangement_size_inner<G, B, L>(
    arranged: Arranged<G, TraceAgent<Spine<Rc<B>>>>,
    mut logic: L,
) -> Arranged<G, TraceAgent<Spine<Rc<B>>>>
where
    G: Scope<Timestamp: Lattice>,
    B: Batch + 'static,
    L: FnMut(&B) -> (usize, usize, usize) + 'static,
{
    let scope = arranged.stream.scope();
    let Some(logger) = scope.logger_for::<ComputeEventBuilder>("materialize/compute") else {
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

            // Weak references to batches, so we can observe batches outside the trace.
            let mut batches = BTreeMap::new();

            move |input, output| {
                while let Some((time, data)) = input.next() {
                    batches.extend(
                        data.iter()
                            .map(|batch| (Rc::as_ptr(batch), Rc::downgrade(batch))),
                    );
                    output.session(&time).give_container(data);
                }
                let Some(trace) = trace.upgrade() else {
                    return;
                };

                trace.borrow().trace.map_batches(|batch| {
                    batches.insert(Rc::as_ptr(batch), Rc::downgrade(batch));
                });

                let (mut size, mut capacity, mut allocations) = (0, 0, 0);
                batches.retain(|_, weak| {
                    if let Some(batch) = weak.upgrade() {
                        let (sz, c, a) = logic(&batch);
                        (size += sz, capacity += c, allocations += a);
                        true
                    } else {
                        false
                    }
                });

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

impl<G, K, V, R> ArrangementSize for Arranged<G, KeyValAgent<K, V, G::Timestamp, R>>
where
    G: Scope,
    G::Timestamp: MzTimestamp,
    K: Data + MzData,
    V: Data + MzData,
    R: Semigroup + Ord + MzData + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |batch| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            batch.storage.keys.heap_size(&mut callback);
            batch.storage.vals.offs.heap_size(&mut callback);
            batch.storage.vals.vals.heap_size(&mut callback);
            batch.storage.upds.offs.heap_size(&mut callback);
            batch.storage.upds.times.heap_size(&mut callback);
            batch.storage.upds.diffs.heap_size(&mut callback);
            (size, capacity, allocations)
        })
    }
}

impl<G, K, R> ArrangementSize for Arranged<G, KeyAgent<K, G::Timestamp, R>>
where
    G: Scope,
    G::Timestamp: MzTimestamp,
    K: Data + MzArrangeData,
    R: Semigroup + Ord + MzData + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |batch| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            batch.storage.keys.heap_size(&mut callback);
            batch.storage.upds.offs.heap_size(&mut callback);
            batch.storage.upds.times.heap_size(&mut callback);
            batch.storage.upds.diffs.heap_size(&mut callback);
            (size, capacity, allocations)
        })
    }
}

impl<G, V, R> ArrangementSize for Arranged<G, RowValAgent<V, G::Timestamp, R>>
where
    G: Scope,
    G::Timestamp: MzTimestamp,
    V: Data + MzArrangeData,
    R: Semigroup + Ord + MzArrangeData + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |batch| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            batch.storage.keys.heap_size(&mut callback);
            batch.storage.vals.offs.heap_size(&mut callback);
            batch.storage.vals.vals.heap_size(&mut callback);
            batch.storage.upds.offs.heap_size(&mut callback);
            batch.storage.upds.times.heap_size(&mut callback);
            batch.storage.upds.diffs.heap_size(&mut callback);
            (size, capacity, allocations)
        })
    }
}

impl<G, R> ArrangementSize for Arranged<G, RowRowAgent<G::Timestamp, R>>
where
    G: Scope,
    G::Timestamp: MzTimestamp,
    R: Semigroup + Ord + MzArrangeData + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |batch| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            batch.storage.keys.heap_size(&mut callback);
            batch.storage.vals.offs.heap_size(&mut callback);
            batch.storage.vals.vals.heap_size(&mut callback);
            batch.storage.upds.offs.heap_size(&mut callback);
            batch.storage.upds.times.heap_size(&mut callback);
            batch.storage.upds.diffs.heap_size(&mut callback);
            (size, capacity, allocations)
        })
    }
}

impl<G, R> ArrangementSize for Arranged<G, RowAgent<G::Timestamp, R>>
where
    G: Scope,
    G::Timestamp: MzTimestamp,
    R: Semigroup + Ord + MzArrangeData + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |batch| {
            let (mut size, mut capacity, mut allocations) = (0, 0, 0);
            let mut callback = |siz, cap| {
                size += siz;
                capacity += cap;
                allocations += usize::from(cap > 0);
            };
            batch.storage.keys.heap_size(&mut callback);
            batch.storage.upds.offs.heap_size(&mut callback);
            batch.storage.upds.times.heap_size(&mut callback);
            batch.storage.upds.diffs.heap_size(&mut callback);
            (size, capacity, allocations)
        })
    }
}
