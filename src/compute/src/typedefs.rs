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
use differential_dataflow::trace::implementations::chunker::ColumnationChunker;
use differential_dataflow::trace::implementations::merge_batcher::{ColMerger, MergeBatcher};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_repr::Diff;
use mz_storage_types::errors::DataflowError;
use timely::dataflow::ScopeParent;

use crate::row_spine::RowValBuilder;
use crate::typedefs::spines::{ColKeyBatcher, ColKeyBuilder, ColValBatcher, ColValBuilder};

pub use crate::row_spine::{RowRowSpine, RowSpine, RowValBatcher, RowValSpine};
pub use crate::typedefs::spines::{ColKeySpine, ColValSpine};

pub(crate) mod spines {
    use std::rc::Rc;

    use differential_dataflow::containers::{Columnation, TimelyStack};
    use differential_dataflow::trace::implementations::ord_neu::{
        OrdKeyBatch, OrdKeyBuilder, OrdValBatch, OrdValBuilder,
    };
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::implementations::{Layout, Update};
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;

    use crate::row_spine::OffsetOptimized;
    use crate::typedefs::{KeyBatcher, KeyValBatcher};

    /// A spine for generic keys and values.
    pub type ColValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<MzStack<((K, V), T, R)>>>>;
    pub type ColValBatcher<K, V, T, R> = KeyValBatcher<K, V, T, R>;
    pub type ColValBuilder<K, V, T, R> =
        RcBuilder<OrdValBuilder<MzStack<((K, V), T, R)>, TimelyStack<((K, V), T, R)>>>;

    /// A spine for generic keys
    pub type ColKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<MzStack<((K, ()), T, R)>>>>;
    pub type ColKeyBatcher<K, T, R> = KeyBatcher<K, T, R>;
    pub type ColKeyBuilder<K, T, R> =
        RcBuilder<OrdKeyBuilder<MzStack<((K, ()), T, R)>, TimelyStack<((K, ()), T, R)>>>;

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
        type KeyContainer = TimelyStack<U::Key>;
        type ValContainer = TimelyStack<U::Val>;
        type TimeContainer = TimelyStack<U::Time>;
        type DiffContainer = TimelyStack<U::Diff>;
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
pub type RowValArrangement<S, V> = Arranged<S, RowValAgent<V, <S as ScopeParent>::Timestamp, Diff>>;
pub type RowValEnter<V, T, R, TEnter> = TraceEnter<TraceFrontier<RowValAgent<V, T, R>>, TEnter>;
// Row specialized spines and agents.
pub type RowRowAgent<T, R> = TraceAgent<RowRowSpine<T, R>>;
pub type RowRowArrangement<S> = Arranged<S, RowRowAgent<<S as ScopeParent>::Timestamp, Diff>>;
pub type RowRowEnter<T, R, TEnter> = TraceEnter<TraceFrontier<RowRowAgent<T, R>>, TEnter>;
// Row specialized spines and agents.
pub type RowAgent<T, R> = TraceAgent<RowSpine<T, R>>;
pub type RowArrangement<S> = Arranged<S, RowAgent<<S as ScopeParent>::Timestamp, Diff>>;
pub type RowEnter<T, R, TEnter> = TraceEnter<TraceFrontier<RowAgent<T, R>>, TEnter>;

// Error specialized spines and agents.
pub type ErrSpine<T, R> = ColKeySpine<DataflowError, T, R>;
pub type ErrBatcher<T, R> = ColKeyBatcher<DataflowError, T, R>;
pub type ErrBuilder<T, R> = ColKeyBuilder<DataflowError, T, R>;

pub type ErrAgent<T, R> = TraceAgent<ErrSpine<T, R>>;
pub type ErrEnter<T, TEnter> = TraceEnter<TraceFrontier<ErrAgent<T, Diff>>, TEnter>;

pub type KeyErrSpine<K, T, R> = ColValSpine<K, DataflowError, T, R>;
pub type KeyErrBatcher<K, T, R> = ColValBatcher<K, DataflowError, T, R>;
pub type KeyErrBuilder<K, T, R> = ColValBuilder<K, DataflowError, T, R>;

pub type RowErrSpine<T, R> = RowValSpine<DataflowError, T, R>;
pub type RowErrBatcher<T, R> = RowValBatcher<DataflowError, T, R>;
pub type RowErrBuilder<T, R> = RowValBuilder<DataflowError, T, R>;

// Batchers for consolidation
pub type KeyBatcher<K, T, D> = KeyValBatcher<K, (), T, D>;
pub type KeyValBatcher<K, V, T, D> =
    MergeBatcher<Vec<((K, V), T, D)>, ColumnationChunker<((K, V), T, D)>, ColMerger<(K, V), T, D>>;

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
    differential_dataflow::containers::Columnation
    + for<'a> columnar::Columnar<Container: Container<Ref<'a>: Copy + Ord> + Clone + Send>
{
}

impl<T> MzData for T
where
    T: differential_dataflow::containers::Columnation,
    T: for<'a> columnar::Columnar<Container: Clone + Send>,
    for<'a> Ref<'a, T>: Copy + Ord,
{
}

pub trait MzArrangeData: differential_dataflow::containers::Columnation {}
impl<T> MzArrangeData for T where T: differential_dataflow::containers::Columnation {}
