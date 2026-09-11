// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::batcher::Batcher;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::spine_fueled::{Spine, SpineBatch};
use differential_dataflow::trace::{Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable, VecCollection};
use mz_compute_types::dyncfgs::{ENABLE_COLUMN_PAGED_BATCHER, ENABLE_COLUMNAR_MERGE_BATCHER};
use mz_dyncfg::ConfigSet;
use mz_row_spine::ArcBatch;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Weak};
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
    KeyAgent, KeyValAgent, MzArrangeData, MzData, MzTimestamp, RowAgent, RowRowAgent, RowValAgent,
};

/// Which merge batcher an arrange site should instantiate.
///
/// The batcher names its own chunker and builder, so a call site spells out one
/// type per variant rather than three that have to agree.
pub enum ArrangementBatcher {
    /// `Chunker<ColumnationStack<_>>` + `Col2ValBatcher` + `RowRowBuilder`.
    /// Chains are columnation stacks.
    Columnation,
    /// `ColumnChunker` + `Col2ValColBatcher` + `RowRowColPagedBuilder`.
    /// Chains are resident `Column`s.
    Columnar,
    /// `ColumnChunker` + `Col2ValPagedBatcher` + `RowRowColPagedBuilder`.
    /// Chains are `Column`s routed through the pager, which may spill them.
    ColumnarPaged,
}

impl ArrangementBatcher {
    /// Resolve the batcher from the replica's config set.
    ///
    /// `ENABLE_COLUMN_PAGED_BATCHER` wins over
    /// `ENABLE_COLUMNAR_MERGE_BATCHER`, because it asks for the same columnar
    /// chains plus paging. Call this once per arrange site at operator
    /// construction time, so a dataflow keeps one batcher for its whole life
    /// even if the flags flip underneath it.
    pub fn from_config(config: &ConfigSet) -> Self {
        if ENABLE_COLUMN_PAGED_BATCHER.get(config) {
            Self::ColumnarPaged
        } else if ENABLE_COLUMNAR_MERGE_BATCHER.get(config) {
            Self::Columnar
        } else {
            Self::Columnation
        }
    }
}

/// Extension trait to arrange data.
pub trait MzArrange<'scope, C>: MzArrangeCore<'scope, C>
where
    C: Container + Clone + 'static,
{
    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange<Ba, Tr>(
        self,
        name: &str,
        batcher: impl FnOnce(Option<Logger>, usize) -> Ba + 'static,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<C, Time = Self::Timestamp> + 'static,
        <Ba as Batcher<C>>::Output: Into<Tr::Batch>,
        Tr: Trace<Time = Self::Timestamp> + 'static,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize;
}

/// Extension trait to arrange data.
///
/// The input container is a trait parameter rather than an associated type: every batcher bound
/// has to name it, and a projection through an associated type does not normalize inside a
/// method's where clause.
pub trait MzArrangeCore<'scope, C>
where
    C: Container + Clone + 'static,
{
    /// The current scope.
    type Timestamp: Timestamp + Lattice;

    /// Arranges a stream of `(Key, Val)` updates by `Key` into a trace of type `Tr`. Partitions
    /// the data according to `pact`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange_core<P, Ba, Tr>(
        self,
        pact: P,
        name: &str,
        batcher: impl FnOnce(Option<Logger>, usize) -> Ba + 'static,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<Self::Timestamp, C>,
        Ba: Batcher<C, Time = Self::Timestamp> + 'static,
        <Ba as Batcher<C>>::Output: Into<Tr::Batch>,
        Tr: Trace<Time = Self::Timestamp> + 'static,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize;
}

impl<'scope, T, C> MzArrangeCore<'scope, C> for Stream<'scope, T, C>
where
    T: Timestamp + Lattice,
    C: Container + Clone + 'static,
{
    type Timestamp = T;

    fn mz_arrange_core<P, Ba, Tr>(
        self,
        pact: P,
        name: &str,
        batcher: impl FnOnce(Option<Logger>, usize) -> Ba + 'static,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, C>,
        Ba: Batcher<C, Time = T> + 'static,
        <Ba as Batcher<C>>::Output: Into<Tr::Batch>,
        Tr: Trace<Time = T> + 'static,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        arrange_core::<_, _, Ba, _>(self, pact, name, batcher).log_arrangement_size()
    }
}

impl<'scope, T, K, V, R> MzArrange<'scope, Vec<((K, V), T, R)>>
    for VecCollection<'scope, T, (K, V), R>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: ExchangeData,
{
    fn mz_arrange<Ba, Tr>(
        self,
        name: &str,
        batcher: impl FnOnce(Option<Logger>, usize) -> Ba + 'static,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Vec<((K, V), T, R)>, Time = T> + 'static,
        <Ba as Batcher<Vec<((K, V), T, R)>>>::Output: Into<Tr::Batch>,
        Tr: Trace<Time = T> + 'static,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        let exchange = Exchange::new(move |update: &((K, V), T, R)| (update.0).0.hashed().into());
        self.mz_arrange_core::<_, Ba, _>(exchange, name, batcher)
    }
}

impl<'scope, T, C> MzArrangeCore<'scope, C> for Collection<'scope, T, C>
where
    T: Timestamp + Lattice,
    C: Container + Clone + 'static,
{
    type Timestamp = T;

    fn mz_arrange_core<P, Ba, Tr>(
        self,
        pact: P,
        name: &str,
        batcher: impl FnOnce(Option<Logger>, usize) -> Ba + 'static,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, C>,
        Ba: Batcher<C, Time = T> + 'static,
        <Ba as Batcher<C>>::Output: Into<Tr::Batch>,
        Tr: Trace<Time = T> + 'static,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        self.inner.mz_arrange_core::<_, Ba, _>(pact, name, batcher)
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

impl<'scope, T, K, R> MzArrange<'scope, Vec<((K, ()), T, R)>> for KeyCollection<'scope, T, K, R>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    R: ExchangeData,
{
    fn mz_arrange<Ba, Tr>(
        self,
        name: &str,
        batcher: impl FnOnce(Option<Logger>, usize) -> Ba + 'static,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Vec<((K, ()), T, R)>, Time = T> + 'static,
        <Ba as Batcher<Vec<((K, ()), T, R)>>>::Output: Into<Tr::Batch>,
        Tr: Trace<Time = T> + 'static,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0.map(|d| (d, ())).mz_arrange::<Ba, _>(name, batcher)
    }
}

impl<'scope, T, K, R> MzArrangeCore<'scope, Vec<((K, ()), T, R)>> for KeyCollection<'scope, T, K, R>
where
    T: Timestamp + Lattice,
    K: Clone + 'static,
    R: Clone + 'static,
{
    type Timestamp = T;

    fn mz_arrange_core<P, Ba, Tr>(
        self,
        pact: P,
        name: &str,
        batcher: impl FnOnce(Option<Logger>, usize) -> Ba + 'static,
    ) -> Arranged<'scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<T, Vec<((K, ()), T, R)>>,
        Ba: Batcher<Vec<((K, ()), T, R)>, Time = T> + 'static,
        <Ba as Batcher<Vec<((K, ()), T, R)>>>::Output: Into<Tr::Batch>,
        Tr: Trace<Time = T> + 'static,
        Arranged<'scope, TraceAgent<Tr>>: ArrangementSize,
    {
        self.0
            .map(|d| (d, ()))
            .mz_arrange_core::<_, Ba, _>(pact, name, batcher)
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
///
/// Batch-size logging identifies each batch by the address of its backing allocation and holds a
/// weak reference to it, so it needs the `Arc` underlying the spine's [`ArcBatch<B>`] batches;
/// `batch.0` reaches straight through the newtype to it.
fn log_arrangement_size_inner<'scope, B, L>(
    arranged: Arranged<'scope, TraceAgent<Spine<ArcBatch<B>>>>,
    mut logic: L,
) -> Arranged<'scope, TraceAgent<Spine<ArcBatch<B>>>>
where
    B: SpineBatch + 'static,
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
            // Batches are immutable once sealed, so we compute their size exactly
            // once (when first observed) and cache it alongside the weak reference.
            // Subsequent activations only sum the cached values for live batches,
            // avoiding a repeated walk of every batch's backing regions.
            let mut batches: BTreeMap<*const B, (Weak<B>, (usize, usize, usize))> = BTreeMap::new();

            move |input, output| {
                input.for_each(|time, data| {
                    for span in data.iter() {
                        // A span with no updates carries no batch, and so no allocation to weigh.
                        if let Some(batch) = &span.inner {
                            batches
                                .entry(Arc::as_ptr(&batch.0))
                                .or_insert_with(|| (Arc::downgrade(&batch.0), logic(&batch.0)));
                        }
                    }
                    output.session(&time).give_container(data);
                });
                let Some(trace) = trace.upgrade() else {
                    // Invariant: `batches` holds no entries once the trace is gone. Each entry's
                    // `Weak` keeps its batch's `ArcInner` allocation reserved, and the `retain` below
                    // that would drop it is unreachable on this path, so the entries have to go
                    // here. The upgrade cannot start succeeding again, hence clearing on every
                    // activation that takes this path also covers batches that arrive on the
                    // input afterwards.
                    batches.clear();
                    return;
                };

                trace.borrow().trace().map_spans(|span| {
                    if let Some(batch) = &span.inner {
                        batches
                            .entry(Arc::as_ptr(&batch.0))
                            .or_insert_with(|| (Arc::downgrade(&batch.0), logic(&batch.0)));
                    }
                });

                let (mut size, mut capacity, mut allocations) = (0, 0, 0);
                batches.retain(|_, (weak, cached)| {
                    if weak.strong_count() > 0 {
                        let (sz, c, a) = *cached;
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

impl<'scope, T, R> ArrangementSize for Arranged<'scope, RowRowAgent<T, R>>
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
