// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Convience typedefs for differential types.

#![allow(missing_docs)]
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord_neu::{ColKeySpine, ColValSpine};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use timely::dataflow::ScopeParent;

use mz_repr::Diff;
use mz_storage_types::errors::DataflowError;

// Spines are data structures that collect and maintain updates.
// Agents are wrappers around spines that allow shared read access.

// Fully generic spines and agents.
pub type KeyValSpine<K, V, T, R> = ColValSpine<K, V, T, R>;
pub type KeyValAgent<K, V, T, R> = TraceAgent<KeyValSpine<K, V, T, R>>;
pub type KeyValArrangement<S, K, V> =
    Arranged<S, KeyValAgent<K, V, <S as ScopeParent>::Timestamp, Diff>>;
pub type KeyValImport<K, V, T, R, TEnter> =
    TraceEnter<TraceFrontier<KeyValAgent<K, V, T, R>>, TEnter>;
pub type KeyValArrangementImport<S, K, V, T> =
    Arranged<S, KeyValImport<K, V, T, Diff, <S as ScopeParent>::Timestamp>>;

// Fully generic key-only spines and agents
pub type KeySpine<K, T, R> = ColKeySpine<K, T, R>;
pub type KeyAgent<K, T, R> = TraceAgent<KeySpine<K, T, R>>;
pub type KeyArrangement<S, K> = Arranged<S, KeyAgent<K, <S as ScopeParent>::Timestamp, Diff>>;
pub type KeyImport<K, T, R, TEnter> = TraceEnter<TraceFrontier<KeyAgent<K, T, R>>, TEnter>;
pub type KeyArrangementImport<S, K, T> =
    Arranged<S, KeyImport<K, T, Diff, <S as ScopeParent>::Timestamp>>;

// Error specialized spines and agents.
pub type ErrSpine<T, R> = ColKeySpine<DataflowError, T, R>;
pub type ErrAgent<T, R> = TraceAgent<ErrSpine<T, R>>;
pub type ErrImport<T, TEnter> = TraceEnter<TraceFrontier<ErrAgent<T, Diff>>, TEnter>;
pub type ErrArrangementImport<S, T> = Arranged<S, ErrImport<T, <S as ScopeParent>::Timestamp>>;
pub type ErrArrangement<S> = Arranged<S, ErrAgent<<S as ScopeParent>::Timestamp, Diff>>;
pub type KeyErrSpine<K, T, R> = ColValSpine<K, DataflowError, T, R>;
