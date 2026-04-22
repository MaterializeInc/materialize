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
use differential_dataflow::{Collection, Data, ExchangeData, Hashable, VecCollection};
use timely::Container;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::operators::Operator;
use timely::progress::Timestamp;

use crate::logging::compute::{
    ArrangementHeapAllocations, ArrangementHeapCapacity, ArrangementHeapSize,
    ArrangementHeapSizeOperator, ComputeEvent, ComputeEventBuilder,
};
use crate::typedefs::{
    KeyAgent, KeyValAgent, MzArrangeData, MzData, MzTimestamp, RowAgent, RowValAgent,
};

/// Extension trait to arrange data.
pub trait MzArrange<'scope>: MzArrangeCore<'scope> {
    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange<Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Input = Self::Input, Time = Self::Timestamp> + 'static,
        Bu: Builder<Time = Self::Timestamp, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = Self::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize;
}

/// Extension trait to arrange data.
pub trait MzArrangeCore<'scope> {
    /// The current scope.
    type Timestamp: Timestamp + Lattice;
    /// The data input container type.
    type Input: Container + Clone + 'static;

    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`. Partitions
    /// the data according to `pact`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange_core<P, Ba, Bu, Tr>(
        self,
        pact: P,
        name: &str,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<Self::Timestamp, Self::Input>,
        Ba: Batcher<Input = Self::Input, Time = Self::Timestamp> + 'static,
        // Ba::Input: Container + Clone + 'static,
        Bu: Builder<Time = Self::Timestamp, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = Self::Timestamp> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize;
}

impl<'scope, T, C> MzArrangeCore<'scope> for Stream<'scope, T, C>
where
    T: Timestamp + Lattice,
    C: Container + Clone + 'static,
{
    type Timestamp = T;
    type Input = C;

    fn mz_arrange_core<P, Ba, Bu, Tr>(self, pact: P, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, Self::Input>,
        Ba: Batcher<Input = Self::Input, Time = T> + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        arrange_core::<_, Ba, Bu, _>(self, pact, name).log_arrangement_size()
    }
}

impl<'scope, T, K, V, R> MzArrange<'scope> for VecCollection<'scope, T, (K, V), R>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: ExchangeData,
{
    fn mz_arrange<Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Input = Self::Input, Time = T> + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        let exchange = Exchange::new(move |update: &((K, V), T, R)| (update.0).0.hashed().into());
        self.mz_arrange_core::<_, Ba, Bu, _>(exchange, name)
    }
}

impl<'scope, T, C> MzArrangeCore<'scope> for Collection<'scope, T, C>
where
    T: Timestamp + Lattice,
    C: Container + Clone + 'static,
{
    type Timestamp = T;
    type Input = C;

    fn mz_arrange_core<P, Ba, Bu, Tr>(self, pact: P, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, Self::Input>,
        Ba: Batcher<Input = Self::Input, Time = T> + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        self.inner.mz_arrange_core::<_, Ba, Bu, _>(pact, name)
    }
}

/// A specialized collection where data only has a key, but no associated value.
///
/// Created by calling `collection.into()`.
pub struct KeyCollection<'scope, T: Timestamp, K: 'static, R: 'static = usize>(
    VecCollection<'scope, T, K, R>,
);

impl<'scope, T: Timestamp, K, R: Semigroup> From<VecCollection<'scope, T, K, R>>
    for KeyCollection<'scope, T, K, R>
{
    fn from(value: VecCollection<'scope, T, K, R>) -> Self {
        KeyCollection(value)
    }
}

impl<'scope, T, K, R> MzArrange<'scope> for KeyCollection<'scope, T, K, R>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    R: ExchangeData,
{
    fn mz_arrange<Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Input = Self::Input, Time = T> + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0.map(|d| (d, ())).mz_arrange::<Ba, Bu, _>(name)
    }
}

impl<'scope, T, K, R> MzArrangeCore<'scope> for KeyCollection<'scope, T, K, R>
where
    T: Timestamp + Lattice,
    K: Clone + 'static,
    R: Clone + 'static,
{
    type Timestamp = T;
    type Input = Vec<((K, ()), T, R)>;

    fn mz_arrange_core<P, Ba, Bu, Tr>(self, pact: P, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, Self::Input>,
        Ba: Batcher<Input = Self::Input, Time = T> + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
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
fn log_arrangement_size_inner<'scope, B, L>(
    arranged: Arranged<'scope, TraceAgent<Spine<Rc<B>>>>,
    mut logic: L,
) -> Arranged<'scope, TraceAgent<Spine<Rc<B>>>>
where
    B: Batch + 'static,
    L: FnMut(&B) -> (usize, usize, usize) + 'static,
{
    let scope = arranged.stream.scope();
    let Some(logger) = scope
        .worker()
        .logger_for::<ComputeEventBuilder>("materialize/compute")
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

            // Weak references to batches, so we can observe batches outside the trace.
            let mut batches = BTreeMap::new();

            move |input, output| {
                input.for_each(|time, data| {
                    batches.extend(
                        data.iter()
                            .map(|batch| (Rc::as_ptr(batch), Rc::downgrade(batch))),
                    );
                    output.session(&time).give_container(data);
                });
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

impl<'scope, T, K, V, R> ArrangementSize for Arranged<'scope, KeyValAgent<K, V, T, R>>
where
    T: MzTimestamp,
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

impl<'scope, T, K, R> ArrangementSize for Arranged<'scope, KeyAgent<K, T, R>>
where
    T: MzTimestamp,
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

impl<'scope, T, V, R> ArrangementSize for Arranged<'scope, RowValAgent<V, T, R>>
where
    T: MzTimestamp,
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

impl<'scope, T, R> ArrangementSize for Arranged<'scope, RowAgent<T, R>>
where
    T: MzTimestamp,
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

/// [`ArrangementSize`] impl for factorized trie-structured spines.
///
/// Uses `length_in_words * 8` on the borrowed trie to approximate heap size
/// (serialized byte count ≈ in-memory footprint for stride-backed columns).
/// Capacity matches size; allocation count is one per batch.
impl<'scope, T, K, V, R> ArrangementSize
    for Arranged<
        'scope,
        TraceAgent<Spine<Rc<mz_timely_util::columnar::factorized::batch::FactBatch<K, V, T, R>>>>,
    >
where
    T: MzTimestamp,
    K: MzData + Ord + Clone + 'static,
    V: MzData + Ord + Clone + 'static,
    R: Semigroup + Ord + Clone + MzData + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, |batch| {
            let borrowed = batch.storage.borrowed();
            let size = columnar::bytes::indexed::length_in_words(&borrowed) * 8;
            (size, size, 1)
        })
    }
}
