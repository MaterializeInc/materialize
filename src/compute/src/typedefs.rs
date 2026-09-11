// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Convience typedefs for differential types.

#![allow(dead_code, missing_docs)]

use columnar::{Container, Ref};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_repr::Diff;
use mz_row_spine::RowValBuilder;
use mz_timely_util::columnar::batcher::Chunker;
use mz_timely_util::columnation::{ColInternalMerger, ColumnationChunker, ColumnationStack};
use mz_timely_util::operator::ConsolidatingBatcher;

use crate::render::errors::DataflowErrorSer;
use crate::typedefs::spines::{ColKeyBatcher, ColKeyBuilder, ColValBatcher, ColValBuilder};

pub use crate::typedefs::spines::{ColKeySpine, ColValSpine};
pub use mz_row_spine::{RowRowSpine, RowSpine, RowValBatcher, RowValSpine};

pub(crate) mod spines {
    use columnation::Columnation;
    use differential_dataflow::trace::implementations::ord_neu::{
        OrdKeyBatch, OrdKeyBuilder, OrdValBatch, OrdValBuilder,
    };
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::implementations::{Layout, Update};
    use mz_timely_util::columnation::ColumnationStack;

    use mz_row_spine::{ArcBatch, ArcBuilder, OffsetOptimized};

    use crate::typedefs::{KeyBatcher, KeyValBatcher};

    /// A spine for generic keys and values.
    pub type ColValSpine<K, V, T, R> = Spine<ArcBatch<OrdValBatch<MzStack<((K, V), T, R)>>>>;
    pub type ColValBatcher<K, V, T, R, Chu> =
        KeyValBatcher<K, V, T, R, Chu, ColValBuilder<K, V, T, R>>;
    pub type ColValBuilder<K, V, T, R> =
        ArcBuilder<OrdValBuilder<MzStack<((K, V), T, R)>, ColumnationStack<((K, V), T, R)>>>;

    /// A spine for generic keys
    pub type ColKeySpine<K, T, R> = Spine<ArcBatch<OrdKeyBatch<MzStack<((K, ()), T, R)>>>>;
    pub type ColKeyBatcher<K, T, R, Chu> = KeyBatcher<K, T, R, Chu, ColKeyBuilder<K, T, R>>;
    pub type ColKeyBuilder<K, T, R> =
        ArcBuilder<OrdKeyBuilder<MzStack<((K, ()), T, R)>, ColumnationStack<((K, ()), T, R)>>>;

    /// A layout based on chunked timely stacks
    pub struct MzStack<U: Update> {
        phantom: std::marker::PhantomData<U>,
    }

    impl<U: Update> Layout for MzStack<U>
    where
        U::Key: Columnation + 'static,
        U::Val: Columnation + 'static,
        U::Time: Columnation,
        U::Diff: Columnation,
    {
        type KeyContainer = ColumnationStack<U::Key>;
        type ValContainer = ColumnationStack<U::Val>;
        type TimeContainer = ColumnationStack<U::Time>;
        type DiffContainer = ColumnationStack<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }
}

// Spines are data structures that collect and maintain updates.
// Agents are wrappers around spines that allow shared read access.

// Fully generic spines and agents.
pub type KeyValSpine<K, V, T, R> = ColValSpine<K, V, T, R>;
pub type KeyValAgent<K, V, T, R> = TraceAgent<KeyValSpine<K, V, T, R>>;
pub type KeyValEnter<K, V, T, R, TEnter> =
    TraceEnter<TraceFrontier<KeyValAgent<K, V, T, R>>, TEnter>;

// Fully generic key-only spines and agents
pub type KeySpine<K, T, R> = ColKeySpine<K, T, R>;
pub type KeyAgent<K, T, R> = TraceAgent<KeySpine<K, T, R>>;
pub type KeyEnter<K, T, R, TEnter> = TraceEnter<TraceFrontier<KeyAgent<K, T, R>>, TEnter>;

// Row specialized spines and agents.
pub type RowValAgent<V, T, R> = TraceAgent<RowValSpine<V, T, R>>;
pub type RowValArrangement<'scope, T, V> = Arranged<'scope, RowValAgent<V, T, Diff>>;
pub type RowValEnter<V, T, R, TEnter> = TraceEnter<TraceFrontier<RowValAgent<V, T, R>>, TEnter>;
// Row specialized spines and agents.
pub type RowRowAgent<T, R> = TraceAgent<RowRowSpine<T, R>>;
pub type RowRowArrangement<'scope, T> = Arranged<'scope, RowRowAgent<T, Diff>>;
pub type RowRowEnter<T, R, TEnter> = TraceEnter<TraceFrontier<RowRowAgent<T, R>>, TEnter>;
// Row specialized spines and agents.
pub type RowAgent<T, R> = TraceAgent<RowSpine<T, R>>;
pub type RowArrangement<'scope, T> = Arranged<'scope, RowAgent<T, Diff>>;
pub type RowEnter<T, R, TEnter> = TraceEnter<TraceFrontier<RowAgent<T, R>>, TEnter>;

// Error specialized spines and agents.
pub type ErrSpine<T, R> = ColKeySpine<DataflowErrorSer, T, R>;
pub type ErrBatcher<T, R, Chu> = ColKeyBatcher<DataflowErrorSer, T, R, Chu>;
pub type ErrBuilder<T, R> = ColKeyBuilder<DataflowErrorSer, T, R>;

pub type ErrAgent<T, R> = TraceAgent<ErrSpine<T, R>>;
pub type ErrEnter<T, TEnter> = TraceEnter<TraceFrontier<ErrAgent<T, Diff>>, TEnter>;

pub type KeyErrSpine<K, T, R> = ColValSpine<K, DataflowErrorSer, T, R>;
pub type KeyErrBatcher<K, T, R, Chu> = ColValBatcher<K, DataflowErrorSer, T, R, Chu>;
pub type KeyErrBuilder<K, T, R> = ColValBuilder<K, DataflowErrorSer, T, R>;

pub type RowErrSpine<T, R> = RowValSpine<DataflowErrorSer, T, R>;
pub type RowErrBatcher<T, R, Chu> = RowValBatcher<DataflowErrorSer, T, R, Chu>;
pub type RowErrBuilder<T, R> = RowValBuilder<DataflowErrorSer, T, R>;

// Batchers over columnation chains. `Chu` melds raw input into chunks, `Se` seals an
// extracted chain: a spine builder to arrange it, `ChainSealer` to merely consolidate it.
pub type KeyBatcher<K, T, D, Chu, Se> = KeyValBatcher<K, (), T, D, Chu, Se>;
pub type KeyValBatcher<K, V, T, D, Chu, Se> =
    MergeBatcher<Chu, ColInternalMerger<(K, V), T, D>, Se>;

/// The batcher a consolidation wants: chunks `Vec` input, hands the chain back unsealed.
pub type ConsolidateKeyValBatcher<K, V, T, D> =
    ConsolidatingBatcher<ColumnationChunker<((K, V), T, D)>, ColInternalMerger<(K, V), T, D>>;
/// [`ConsolidateKeyValBatcher`] with unit values.
pub type ConsolidateBatcher<K, T, D> = ConsolidateKeyValBatcher<K, (), T, D>;
/// [`ConsolidateKeyValBatcher`] over `Column` input rather than `Vec` input.
pub type ConsolidateColumnBatcher<K, V, T, D> = ConsolidatingBatcher<
    Chunker<ColumnationStack<((K, V), T, D)>>,
    ColInternalMerger<(K, V), T, D>,
>;

/// Timestamp trait for rendering, constraint to support [`MzData`] and [timely::progress::Timestamp].
pub trait MzTimestamp:
    MzData + timely::progress::Timestamp + differential_dataflow::lattice::Lattice + std::hash::Hash
{
}

impl<T> MzTimestamp for T
where
    T: MzData,
    T: timely::progress::Timestamp,
    T: differential_dataflow::lattice::Lattice + std::hash::Hash,
{
}

/// Trait for data types that can be used in Materialize's dataflow, supporting both columnar and
/// columnation.
pub trait MzData:
    columnation::Columnation
    + for<'a> columnar::Columnar<Container: Container<Ref<'a>: Copy + Ord> + Clone + Send>
{
}

impl<T> MzData for T
where
    T: columnation::Columnation,
    T: columnar::Columnar<Container: Clone + Send>,
    for<'a> Ref<'a, T>: Copy + Ord,
{
}

pub trait MzArrangeData: columnation::Columnation {}
impl<T> MzArrangeData for T where T: columnation::Columnation {}
