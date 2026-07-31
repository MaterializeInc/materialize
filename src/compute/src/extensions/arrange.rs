// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::rc::{Rc, Weak};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::implementations::ord_neu::OrdValBatch;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::{Batch, Batcher, Builder, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable, VecCollection};
use mz_repr::Row;
use mz_row_spine::RowRowLayout;
use timely::Container;
use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely::dataflow::operators::Operator;
use timely::progress::Timestamp;

use crate::logging::compute::{
    ArrangementDistinctKeys, ArrangementHeapAllocations, ArrangementHeapCapacity,
    ArrangementHeapSize, ArrangementHeapSizeOperator, ComputeEvent, ComputeEventBuilder,
};
use crate::typedefs::{
    KeyAgent, KeyValAgent, MzArrangeData, MzData, MzTimestamp, RowAgent, RowRowAgent, RowValAgent,
};

/// Extension trait to arrange data.
pub trait MzArrange<'scope>: MzArrangeCore<'scope> {
    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange<Chu, Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Time = Self::Timestamp> + 'static,
        Chu: ContainerBuilder<Container = Ba::Output>
            + for<'a> PushInto<&'a mut Self::Input>
            + 'static,
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
    fn mz_arrange_core<P, Chu, Ba, Bu, Tr>(
        self,
        pact: P,
        name: &str,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<Self::Timestamp, Self::Input>,
        Ba: Batcher<Time = Self::Timestamp> + 'static,
        Chu: ContainerBuilder<Container = Ba::Output>
            + for<'a> PushInto<&'a mut Self::Input>
            + 'static,
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

    fn mz_arrange_core<P, Chu, Ba, Bu, Tr>(
        self,
        pact: P,
        name: &str,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, Self::Input>,
        Ba: Batcher<Time = T> + 'static,
        Chu: ContainerBuilder<Container = Ba::Output>
            + for<'a> PushInto<&'a mut Self::Input>
            + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        arrange_core::<_, _, Chu, Ba, Bu, _>(self, pact, name).log_arrangement_size()
    }
}

impl<'scope, T, K, V, R> MzArrange<'scope> for VecCollection<'scope, T, (K, V), R>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: ExchangeData,
{
    fn mz_arrange<Chu, Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Time = T> + 'static,
        Chu: ContainerBuilder<Container = Ba::Output>
            + for<'a> PushInto<&'a mut Self::Input>
            + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        let exchange = Exchange::new(move |update: &((K, V), T, R)| (update.0).0.hashed().into());
        self.mz_arrange_core::<_, Chu, Ba, Bu, _>(exchange, name)
    }
}

impl<'scope, T, C> MzArrangeCore<'scope> for Collection<'scope, T, C>
where
    T: Timestamp + Lattice,
    C: Container + Clone + 'static,
{
    type Timestamp = T;
    type Input = C;

    fn mz_arrange_core<P, Chu, Ba, Bu, Tr>(
        self,
        pact: P,
        name: &str,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, Self::Input>,
        Ba: Batcher<Time = T> + 'static,
        Chu: ContainerBuilder<Container = Ba::Output>
            + for<'a> PushInto<&'a mut Self::Input>
            + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        self.inner.mz_arrange_core::<_, Chu, Ba, Bu, _>(pact, name)
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
    fn mz_arrange<Chu, Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Time = T> + 'static,
        Chu: ContainerBuilder<Container = Ba::Output>
            + for<'a> PushInto<&'a mut Self::Input>
            + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0.map(|d| (d, ())).mz_arrange::<Chu, Ba, Bu, _>(name)
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

    fn mz_arrange_core<P, Chu, Ba, Bu, Tr>(
        self,
        pact: P,
        name: &str,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, Self::Input>,
        Ba: Batcher<Time = T> + 'static,
        Chu: ContainerBuilder<Container = Ba::Output>
            + for<'a> PushInto<&'a mut Self::Input>
            + 'static,
        Bu: Builder<Time = T, Input = Ba::Output, Output = Tr::Batch>,
        Tr: Trace + TraceReader<Time = T> + 'static,
        Tr::Batch: Batch,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0
            .map(|d| (d, ()))
            .mz_arrange_core::<_, Chu, Ba, Bu, _>(pact, name)
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
/// * `logic`: Closure that calculates the heap size/capacity/allocations and the distinct key
///    count for a batch. The first three values are size and capacity in bytes, and number of
///    allocations, all in absolute values. The fourth is the number of distinct keys in the
///    batch, an exact count read directly from the key container's stored length.
fn log_arrangement_size_inner<'scope, B, L>(
    arranged: Arranged<'scope, TraceAgent<Spine<Rc<B>>>>,
    mut logic: L,
) -> Arranged<'scope, TraceAgent<Spine<Rc<B>>>>
where
    B: Batch + 'static,
    L: FnMut(&B) -> (usize, usize, usize, usize) + 'static,
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

    let (mut old_size, mut old_capacity, mut old_allocations, mut old_keys) =
        (0isize, 0isize, 0isize, 0isize);

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
            // Batches are immutable once sealed, so we compute their size exactly
            // once (when first observed) and cache it alongside the weak reference.
            // Subsequent activations only sum the cached values for live batches,
            // avoiding a repeated walk of every batch's backing regions.
            let mut batches: BTreeMap<*const B, (Weak<B>, (usize, usize, usize, usize))> =
                BTreeMap::new();

            move |input, output| {
                input.for_each(|time, data| {
                    for batch in data.iter() {
                        batches
                            .entry(Rc::as_ptr(batch))
                            .or_insert_with(|| (Rc::downgrade(batch), logic(batch)));
                    }
                    output.session(&time).give_container(data);
                });
                let Some(trace) = trace.upgrade() else {
                    return;
                };

                trace.borrow().trace().map_batches(|batch| {
                    batches
                        .entry(Rc::as_ptr(batch))
                        .or_insert_with(|| (Rc::downgrade(batch), logic(batch)));
                });

                let (mut size, mut capacity, mut allocations, mut keys) = (0, 0, 0, 0);
                batches.retain(|_, (weak, cached)| {
                    if weak.strong_count() > 0 {
                        let (sz, c, a, k) = *cached;
                        (size += sz, capacity += c, allocations += a, keys += k);
                        true
                    } else {
                        false
                    }
                });
                // `keys` sums each live batch's distinct-key count, deduplicated by the same
                // `Rc::as_ptr` cache as the heap-size figures above. A key present in more than
                // one live batch is counted once per batch, which happens normally across the
                // spine's batch pyramid and while a `MergeState::Double` merge is in progress, so
                // this sum is an upper bound on the arrangement's distinct-key count, not the
                // exact count. Over-counting is safe. Under-counting is not.

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

                let keys = keys.try_into().expect("must fit");
                if keys != old_keys {
                    logger.log(&ComputeEvent::ArrangementDistinctKeys(
                        ArrangementDistinctKeys {
                            operator_id,
                            delta_keys: keys - old_keys,
                        },
                    ));
                }

                old_size = size;
                old_capacity = capacity;
                old_allocations = allocations;
                old_keys = keys;
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
            let keys = batch.storage.keys.len();
            (size, capacity, allocations, keys)
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
            let keys = batch.storage.keys.len();
            (size, capacity, allocations, keys)
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
            let keys = batch.storage.keys.len();
            (size, capacity, allocations, keys)
        })
    }
}

impl<'scope, T, R> ArrangementSize for Arranged<'scope, RowRowAgent<T, R>>
where
    T: MzTimestamp,
    R: Semigroup + Ord + MzArrangeData + 'static,
{
    fn log_arrangement_size(self) -> Self {
        log_arrangement_size_inner(self, row_row_batch_stats::<T, R>)
    }
}

/// Heap size, capacity, allocation count, and distinct-key count for a `RowRowAgent` batch.
///
/// A free function rather than an inline closure so a unit test can call it directly on a batch
/// built from the production `DatumContainer`-backed [`RowRowLayout`], without needing a running
/// timely worker to drive [`log_arrangement_size_inner`].
fn row_row_batch_stats<T, R>(
    batch: &OrdValBatch<RowRowLayout<((Row, Row), T, R)>>,
) -> (usize, usize, usize, usize)
where
    T: MzTimestamp,
    R: Semigroup + Ord + MzArrangeData + 'static,
{
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
    let keys = batch.storage.keys.len();
    (size, capacity, allocations, keys)
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
            let keys = batch.storage.keys.len();
            (size, capacity, allocations, keys)
        })
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::{BatchReader, Builder, Description};
    use mz_repr::{Datum, Row};
    use mz_row_spine::RowRowBuilder;
    use mz_timely_util::columnation::ColumnationStack;
    use timely::progress::Antichain;

    use super::row_row_batch_stats;

    /// `row_row_batch_stats` (the per-batch closure `log_arrangement_size_inner` caches for
    /// `RowRowAgent` arrangements) must report the number of distinct *keys* in a batch, not the
    /// number of distinct `(key, value)` pairs and not the number of raw update records.
    ///
    /// The batch is built through the production [`RowRowBuilder`], so the key container under
    /// test is the real `DatumContainer` that ships in `RowRowAgent` arrangements, whose `len()`
    /// is a hand-maintained counter, rather than differential's plain `Vec`-backed container
    /// whose `len()` would be trivially correct regardless of what the read under test does. The
    /// test calls `row_row_batch_stats` itself instead of duplicating its logic, ensuring that a
    /// regression that swaps the key read for a value-count or a constant zero is caught directly.
    #[mz_ore::test]
    fn distinct_key_count_is_per_key_not_per_row() {
        let key = Row::pack_slice(&[Datum::Int64(1)]);
        let val_a = Row::pack_slice(&[Datum::Int64(10)]);
        let val_b = Row::pack_slice(&[Datum::Int64(20)]);
        let val_c = Row::pack_slice(&[Datum::Int64(30)]);

        let mut chunk: ColumnationStack<((Row, Row), u64, i64)> = Default::default();
        chunk.copy(&((key.clone(), val_a), 0, 1));
        chunk.copy(&((key.clone(), val_b.clone()), 0, 1));
        chunk.copy(&((key.clone(), val_b), 1, 1)); // same (key, value) as above, different time: bumps the record count only.
        chunk.copy(&((key, val_c), 0, 1));

        let description = Description::new(
            Antichain::from_elem(0u64),
            Antichain::from_elem(2u64),
            Antichain::from_elem(0u64),
        );
        let batch = RowRowBuilder::<u64, i64>::seal(&mut vec![chunk], description);

        assert_eq!(batch.storage.keys.len(), 1, "one distinct key");
        assert_eq!(
            batch.storage.vals.vals.len(),
            3,
            "three distinct (key, value) pairs"
        );
        assert_eq!(batch.len(), 4, "four raw update records");

        let (_, _, _, keys) = row_row_batch_stats(&batch);
        assert_eq!(
            keys, 1,
            "row_row_batch_stats must report the distinct-key count"
        );
        assert_ne!(
            keys,
            batch.storage.vals.vals.len(),
            "would fail if the key read were swapped for a (key, value)-pair count"
        );
        assert_ne!(
            keys, 0,
            "would fail if the key read were swapped for a constant zero"
        );
    }
}
