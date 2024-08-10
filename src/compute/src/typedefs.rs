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

pub use crate::row_spine::{RowRowSpine, RowSpine, RowValSpine};
use crate::typedefs::spines::MzFlatLayout;
pub use crate::typedefs::spines::{ColKeySpine, ColValSpine};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::chunker::ColumnationChunker;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::merge_batcher_col::ColumnationMerger;
use differential_dataflow::trace::implementations::ord_neu::{FlatValSpine, OrdValBatch};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_ore::flatcontainer::{ItemRegion, MzIndexOptimized, MzRegionPreference};
use mz_repr::Diff;
use mz_storage_types::errors::DataflowError;
use timely::container::flatcontainer::impls::tuple::{TupleABCRegion, TupleABRegion};
use timely::dataflow::ScopeParent;

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
    use mz_ore::flatcontainer::{MzIndex, MzIndexOptimized, MzRegion};
    use mz_timely_util::containers::stack::StackWrapper;
    use timely::container::columnation::{Columnation, TimelyStack};
    use timely::container::flatcontainer::FlatStack;
    use timely::progress::Timestamp;

    use crate::row_spine::OffsetOptimized;
    use crate::typedefs::{KeyBatcher, KeyValBatcher};

    /// A spine for generic keys and values.
    pub type ColValSpine<K, V, T, R> = Spine<
        Rc<OrdValBatch<MzStack<((K, V), T, R)>>>,
        KeyValBatcher<K, V, T, R>,
        RcBuilder<OrdValBuilder<MzStack<((K, V), T, R)>, TimelyStack<((K, V), T, R)>>>,
    >;

    /// A spine for generic keys
    pub type ColKeySpine<K, T, R> = Spine<
        Rc<OrdKeyBatch<MzStack<((K, ()), T, R)>>>,
        KeyBatcher<K, T, R>,
        RcBuilder<OrdKeyBuilder<MzStack<((K, ()), T, R)>, TimelyStack<((K, ()), T, R)>>>,
    >;

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
    pub struct MzFlatLayout<KR, VR, TR, RR> {
        phantom: std::marker::PhantomData<(KR, VR, TR, RR)>,
    }

    impl<KR, VR, TR, RR> Update for MzFlatLayout<KR, VR, TR, RR>
    where
        KR: MzRegion<Index = MzIndex>,
        VR: MzRegion<Index = MzIndex>,
        TR: MzRegion<Index = MzIndex>,
        RR: MzRegion<Index = MzIndex>,
        KR::Owned: Ord + Clone + 'static,
        VR::Owned: Ord + Clone + 'static,
        TR::Owned: Ord + Clone + Lattice + Timestamp + 'static,
        RR::Owned: Ord + Semigroup + 'static,
        for<'a> KR::ReadItem<'a>: Copy + Ord,
        for<'a> VR::ReadItem<'a>: Copy + Ord,
        for<'a> TR::ReadItem<'a>: Copy + Ord,
        for<'a> RR::ReadItem<'a>: Copy + Ord,
    {
        type Key = KR::Owned;
        type Val = VR::Owned;
        type Time = TR::Owned;
        type Diff = RR::Owned;
    }

    /// Layout implementation for [`MzFlatLayout`]. Mostly equivalent to differential's
    /// flat layout but with a different opinion for the offset container. Here, we use
    /// [`OffsetOptimized`] instead of an offset list. If differential should gain access
    /// to the optimized variant, we might be able to remove this implementation.
    impl<KR, VR, TR, RR> Layout for MzFlatLayout<KR, VR, TR, RR>
    where
        KR: MzRegion<Index = MzIndex>,
        VR: MzRegion<Index = MzIndex>,
        TR: MzRegion<Index = MzIndex>,
        RR: MzRegion<Index = MzIndex>,
        KR::Owned: Ord + Clone + 'static,
        VR::Owned: Ord + Clone + 'static,
        TR::Owned: Ord + Clone + Lattice + Timestamp + 'static,
        RR::Owned: Ord + Semigroup + 'static,
        for<'a> KR::ReadItem<'a>: Copy + Ord,
        for<'a> VR::ReadItem<'a>: Copy + Ord,
        for<'a> TR::ReadItem<'a>: Copy + Ord,
        for<'a> RR::ReadItem<'a>: Copy + Ord,
    {
        type Target = Self;
        type KeyContainer = FlatStack<KR, MzIndexOptimized>;
        type ValContainer = FlatStack<VR, MzIndexOptimized>;
        type TimeContainer = FlatStack<TR, MzIndexOptimized>;
        type DiffContainer = FlatStack<RR, MzIndexOptimized>;
        type OffsetContainer = OffsetOptimized;
    }
}

// Spines are data structures that collect and maintain updates.
// Agents are wrappers around spines that allow shared read access.

// Fully generic spines and agents.
pub type KeyValSpine<K, V, T, R, C> = FlatKeyValSpineDefault<K, V, T, R, C>;
pub type KeyValAgent<K, V, T, R, C> = TraceAgent<KeyValSpine<K, V, T, R, C>>;
pub type KeyValEnter<K, V, T, R, C, TEnter> =
    TraceEnter<TraceFrontier<KeyValAgent<K, V, T, R, C>>, TEnter>;

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
pub type ErrAgent<T, R> = TraceAgent<ErrSpine<T, R>>;
pub type ErrEnter<T, TEnter> = TraceEnter<TraceFrontier<ErrAgent<T, Diff>>, TEnter>;
pub type KeyErrSpine<K, T, R> = ColValSpine<K, DataflowError, T, R>;
pub type RowErrSpine<T, R> = RowValSpine<DataflowError, T, R>;

// Batchers for consolidation
pub type KeyBatcher<K, T, D> = KeyValBatcher<K, (), T, D>;
pub type KeyValBatcher<K, V, T, D> = MergeBatcher<
    Vec<((K, V), T, D)>,
    ColumnationChunker<((K, V), T, D)>,
    ColumnationMerger<((K, V), T, D)>,
    T,
>;

pub type FlatKeyValBatch<K, V, T, R> = OrdValBatch<MzFlatLayout<K, V, T, R>>;
pub type FlatKeyValSpine<K, V, T, R, C> = FlatValSpine<
    MzFlatLayout<K, V, T, R>,
    ItemRegion<TupleABCRegion<TupleABRegion<K, V>, T, R>>,
    C,
    MzIndexOptimized,
>;
pub type FlatKeyValSpineDefault<K, V, T, R, C> = FlatKeyValSpine<
    ItemRegion<<K as MzRegionPreference>::Region>,
    ItemRegion<<V as MzRegionPreference>::Region>,
    ItemRegion<<T as MzRegionPreference>::Region>,
    ItemRegion<<R as MzRegionPreference>::Region>,
    C,
>;
pub type FlatKeyValAgent<K, V, T, R, C> = TraceAgent<FlatKeyValSpine<K, V, T, R, C>>;
pub type FlatKeyValEnter<K, V, T, R, C, TEnter> =
    TraceEnter<TraceFrontier<FlatKeyValAgent<K, V, T, R, C>>, TEnter>;
