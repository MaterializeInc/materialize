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

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::chunker::ColumnationChunker;
use differential_dataflow::trace::implementations::merge_batcher::{ColMerger, MergeBatcher};
use differential_dataflow::trace::implementations::ord_neu::{
    FlatValBatcher, FlatValBuilder, FlatValSpine, OrdValBatch,
};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_ore::flatcontainer::MzRegionPreference;
use mz_repr::Diff;
use mz_storage_types::errors::DataflowError;
use timely::container::flatcontainer::impls::tuple::{TupleABCRegion, TupleABRegion};
use timely::dataflow::ScopeParent;

use crate::row_spine::RowValBuilder;
use crate::typedefs::spines::{
    ColKeyBatcher, ColKeyBuilder, ColValBatcher, ColValBuilder, MzFlatLayout,
};

pub use crate::row_spine::{RowRowSpine, RowSpine, RowValBatcher, RowValSpine};
pub use crate::typedefs::spines::{ColKeySpine, ColValSpine};

pub(crate) mod spines {
    use std::rc::Rc;

    use differential_dataflow::difference::Semigroup;
    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::trace::implementations::ord_neu::{
        OrdKeyBatch, OrdKeyBuilder, OrdValBatch, OrdValBuilder,
    };
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use differential_dataflow::trace::implementations::{Layout, Update};
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use mz_timely_util::containers::stack::StackWrapper;
    use timely::container::columnation::{Columnation, TimelyStack};
    use timely::container::flatcontainer::{FlatStack, Push, Region};
    use timely::progress::Timestamp;

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
        type Target = U;
        type KeyContainer = StackWrapper<U::Key>;
        type ValContainer = StackWrapper<U::Val>;
        type TimeContainer = StackWrapper<U::Time>;
        type DiffContainer = StackWrapper<U::Diff>;
        type OffsetContainer = OffsetOptimized;
    }

    /// A layout based on flat container stacks
    pub struct MzFlatLayout<K, V, T, R> {
        phantom: std::marker::PhantomData<(K, V, T, R)>,
    }

    impl<K, V, T, R> Update for MzFlatLayout<K, V, T, R>
    where
        K: Region,
        V: Region,
        T: Region,
        R: Region,
        K::Owned: Ord + Clone + 'static,
        V::Owned: Ord + Clone + 'static,
        T::Owned: Ord + Clone + Lattice + Timestamp + 'static,
        R::Owned: Ord + Semigroup + 'static,
    {
        type Key = K::Owned;
        type Val = V::Owned;
        type Time = T::Owned;
        type Diff = R::Owned;
    }

    /// Layout implementation for [`MzFlatLayout`]. Mostly equivalent to differential's
    /// flat layout but with a different opinion for the offset container. Here, we use
    /// [`OffsetOptimized`] instead of an offset list. If differential should gain access
    /// to the optimized variant, we might be able to remove this implementation.
    impl<K, V, T, R> Layout for MzFlatLayout<K, V, T, R>
    where
        K: Region
            + Push<<K as Region>::Owned>
            + for<'a> Push<<K as Region>::ReadItem<'a>>
            + 'static,
        V: Region
            + Push<<V as Region>::Owned>
            + for<'a> Push<<V as Region>::ReadItem<'a>>
            + 'static,
        T: Region
            + Push<<T as Region>::Owned>
            + for<'a> Push<<T as Region>::ReadItem<'a>>
            + 'static,
        R: Region
            + Push<<R as Region>::Owned>
            + for<'a> Push<<R as Region>::ReadItem<'a>>
            + 'static,
        K::Owned: Ord + Clone + 'static,
        V::Owned: Ord + Clone + 'static,
        T::Owned: Ord + Clone + Lattice + Timestamp + 'static,
        R::Owned: Ord + Semigroup + 'static,
        for<'a> K::ReadItem<'a>: Copy + Ord,
        for<'a> V::ReadItem<'a>: Copy + Ord,
        for<'a> T::ReadItem<'a>: Copy + Ord,
        for<'a> R::ReadItem<'a>: Copy + Ord,
    {
        type Target = Self;
        type KeyContainer = FlatStack<K>;
        type ValContainer = FlatStack<V>;
        type TimeContainer = FlatStack<T>;
        type DiffContainer = FlatStack<R>;
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

pub type FlatKeyValBatch<K, V, T, R> = OrdValBatch<MzFlatLayout<K, V, T, R>>;
pub type FlatKeyValSpine<K, V, T, R> = FlatValSpine<MzFlatLayout<K, V, T, R>>;
pub type FlatKeyValSpineDefault<K, V, T, R> = FlatKeyValSpine<
    <K as MzRegionPreference>::Region,
    <V as MzRegionPreference>::Region,
    <T as MzRegionPreference>::Region,
    <R as MzRegionPreference>::Region,
>;
pub type FlatKeyValBatcher<K, V, T, R, C> =
    FlatValBatcher<TupleABCRegion<TupleABRegion<K, V>, T, R>, C>;
pub type FlatKeyValBatcherDefault<K, V, T, R, C> = FlatKeyValBatcher<
    <K as MzRegionPreference>::Region,
    <V as MzRegionPreference>::Region,
    <T as MzRegionPreference>::Region,
    <R as MzRegionPreference>::Region,
    C,
>;
pub type FlatKeyValBuilder<K, V, T, R> =
    FlatValBuilder<MzFlatLayout<K, V, T, R>, TupleABCRegion<TupleABRegion<K, V>, T, R>>;
pub type FlatKeyValBuilderDefault<K, V, T, R> = FlatKeyValBuilder<
    <K as MzRegionPreference>::Region,
    <V as MzRegionPreference>::Region,
    <T as MzRegionPreference>::Region,
    <R as MzRegionPreference>::Region,
>;

pub type FlatKeyValAgent<K, V, T, R> = TraceAgent<FlatKeyValSpine<K, V, T, R>>;
pub type FlatKeyValEnter<K, V, T, R, TEnter> =
    TraceEnter<TraceFrontier<FlatKeyValAgent<K, V, T, R>>, TEnter>;
