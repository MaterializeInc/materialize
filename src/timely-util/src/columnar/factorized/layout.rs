// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Layout wiring for factorized columnar batches.
//!
//! [`FactLayout`] implements differential-dataflow's [`Layout`] trait, mapping
//! `(K, V, T, R)` types to [`Coltainer`]-backed containers. The layout is used
//! purely for type-level wiring — actual storage is the [`KVUpdates`](super::KVUpdates)
//! trie inside [`FactBatch`](super::batch::FactBatch).

use columnar::Columnar;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::{Layout, OffsetList};

use super::container::Coltainer;

/// Layout for factorized columnar batches.
///
/// Maps `(K, V, T, R)` types to [`Coltainer`]-backed containers. The offset
/// container uses [`OffsetList`] for compatibility with the DD infrastructure,
/// though factorized batches use their own trie bounds internally.
pub struct FactLayout<K, V, T, R> {
    phantom: std::marker::PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R> Layout for FactLayout<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    V: Columnar + Ord + Clone + 'static,
    T: Columnar + Ord + Clone + Lattice + timely::progress::Timestamp + 'static,
    R: Columnar + Ord + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, K>: Ord + Copy,
    for<'a> columnar::Ref<'a, V>: Ord + Copy,
    for<'a> columnar::Ref<'a, T>: Ord + Copy,
    for<'a> columnar::Ref<'a, R>: Ord + Copy,
{
    type KeyContainer = Coltainer<K>;
    type ValContainer = Coltainer<V>;
    type TimeContainer = Coltainer<T>;
    type DiffContainer = Coltainer<R>;
    type OffsetContainer = OffsetList;
}
