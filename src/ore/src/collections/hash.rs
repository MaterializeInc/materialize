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

//! Stable wrappers around the stdlib `Hash` collections.
//!
//! The stdlib `Hash` collections (i.e., [`std::collections::HashMap`] and
//! [`std::collections::HashSet`]) don't guarantee a stable iteration order. That is, the order in
//! which elements are returned by methods like `iter` or `keys`, or in which elements are visited
//! by methods like `retain`, is undefined and can differ between program executions. This property
//! leads to non-determinism, which in turn can be the cause of bugs in logic that relies on
//! deterministic execution.
//!
//! Since bugs caused by unstable iteration order are hard to spot, and it is not always easy to
//! determine which parts of our code require determinism, we resort to banning the use of the
//! stdlib `Hash` collections in general. Alternatives are available:
//!
//!  1. In most cases, you can simply switch to using the equivalent `BTree` collection type (i.e.,
//!     [`std::collections::BTreeMap`] or [`std::collections::BTreeSet`]) as a drop-in replacement.
//!     The `BTree` collections guarantee a stable iteration order, based on the `Ord` relation.
//!  2. If the types you want to store don't (and shouldn't) implement `Ord`, or if profiling
//!     reveals that the `BTree` collections are not suitable, use the wrappers provided in this
//!     module instead.
//!
//! The `Hash` collection wrappers provided in this module only re-export methods that don't expose
//! iteration order, and are therefore safe to use in code that relies on determinism.
//!
//! There are cases where the above mentioned alternatives are not sufficient, for example, if a
//! third-party API requires you to pass a `HashMap` value. In this case, you can disable the
//! clippy lint for the affected piece of code. Note that the onus is on you then to verify that
//! doing so is sound and does not introduce bugs.

// Allow the use of `HashMap` and `HashSet`, so we can define wrappers.
#![allow(clippy::disallowed_types)]

use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap as StdMap, HashSet as StdSet, TryReserveError};
use std::hash::Hash;
use std::ops::{BitAnd, BitOr, BitXor, Index, Sub};

/// A wrapper around [`std::collections::HashMap`] that hides methods that expose unstable
/// iteration order.
///
/// See the module documentation for a rationale.
#[derive(Clone, Debug, Default)]
#[repr(transparent)]
pub struct HashMap<K, V>(StdMap<K, V>);

impl<K, V> HashMap<K, V> {
    /// See [`std::collections::HashMap::new`].
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self(StdMap::new())
    }

    /// See [`std::collections::HashMap::with_capacity`].
    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self(StdMap::with_capacity(capacity))
    }

    /// See [`std::collections::HashMap::capacity`].
    #[inline]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// See [`std::collections::HashMap::len`].
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// See [`std::collections::HashMap::is_empty`].
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// See [`std::collections::HashMap::clear`].
    #[inline]
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl<K, V> HashMap<K, V>
where
    K: Eq + Hash,
{
    /// See [`std::collections::HashMap::reserve`].
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// See [`std::collections::HashMap::try_reserve`].
    #[inline]
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.0.try_reserve(additional)
    }

    /// See [`std::collections::HashMap::shrink_to_fit`].
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }

    /// See [`std::collections::HashMap::shrink_to`].
    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.0.shrink_to(min_capacity);
    }

    /// See [`std::collections::HashMap::entry`].
    #[inline]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        self.0.entry(key)
    }

    /// See [`std::collections::HashMap::get`].
    #[inline]
    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.get(k)
    }

    /// See [`std::collections::HashMap::get_key_value`].
    #[inline]
    pub fn get_key_value<Q: ?Sized>(&self, k: &Q) -> Option<(&K, &V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.get_key_value(k)
    }

    /// See [`std::collections::HashMap::contains_key`].
    #[inline]
    pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.contains_key(k)
    }

    /// See [`std::collections::HashMap::get_mut`].
    #[inline]
    pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.get_mut(k)
    }

    /// See [`std::collections::HashMap::insert`].
    #[inline]
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.0.insert(k, v)
    }

    /// See [`std::collections::HashMap::remove`].
    #[inline]
    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.remove(k)
    }

    /// See [`std::collections::HashMap::remove_entry`].
    #[inline]
    pub fn remove_entry<Q: ?Sized>(&mut self, k: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.remove_entry(k)
    }
}

impl<K, V> PartialEq for HashMap<K, V>
where
    K: Eq + Hash,
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<K, V> Eq for HashMap<K, V>
where
    K: Eq + Hash,
    V: Eq,
{
}

impl<K, Q: ?Sized, V> Index<&Q> for HashMap<K, V>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash,
{
    type Output = V;

    #[inline]
    fn index(&self, key: &Q) -> &V {
        self.0.index(key)
    }
}

impl<K, V> FromIterator<(K, V)> for HashMap<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self(StdMap::from_iter(iter))
    }
}

impl<K, V> From<StdMap<K, V>> for HashMap<K, V>
where
    K: Eq + Hash,
{
    fn from(map: StdMap<K, V>) -> Self {
        Self(map)
    }
}

impl<K, V, const N: usize> From<[(K, V); N]> for HashMap<K, V>
where
    K: Eq + Hash,
{
    fn from(arr: [(K, V); N]) -> Self {
        Self(StdMap::from(arr))
    }
}

impl<K, V> Extend<(K, V)> for HashMap<K, V>
where
    K: Eq + Hash,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}

impl<'a, K, V> Extend<(&'a K, &'a V)> for HashMap<K, V>
where
    K: Eq + Hash + Copy,
    V: Copy,
{
    fn extend<T: IntoIterator<Item = (&'a K, &'a V)>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}

/// A wrapper around [`std::collections::HashSet`] that hides methods that expose unstable
/// iteration order.
///
/// See the module documentation for a rationale.
#[derive(Clone, Debug, Default)]
#[repr(transparent)]
pub struct HashSet<T>(StdSet<T>);

impl<T> HashSet<T> {
    /// See [`std::collections::HashSet::new`].
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self(StdSet::new())
    }

    /// See [`std::collections::HashSet::with_capacity`].
    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self(StdSet::with_capacity(capacity))
    }

    /// See [`std::collections::HashSet::capacity`].
    #[inline]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// See [`std::collections::HashSet::len`].
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// See [`std::collections::HashSet::is_empty`].
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// See [`std::collections::HashSet::clear`].
    #[inline]
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl<T> HashSet<T>
where
    T: Eq + Hash,
{
    /// See [`std::collections::HashSet::reserve`].
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// See [`std::collections::HashSet::try_reserve`].
    #[inline]
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.0.try_reserve(additional)
    }

    /// See [`std::collections::HashSet::shrink_to_fit`].
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }

    /// See [`std::collections::HashSet::shrink_to`].
    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.0.shrink_to(min_capacity);
    }

    /// See [`std::collections::HashSet::contains`].
    #[inline]
    pub fn contains<Q: ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.contains(value)
    }

    /// See [`std::collections::HashSet::get`].
    #[inline]
    pub fn get<Q: ?Sized>(&self, value: &Q) -> Option<&T>
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.get(value)
    }

    /// See [`std::collections::HashSet::is_disjoint`].
    #[inline]
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.0.is_disjoint(&other.0)
    }

    /// See [`std::collections::HashSet::is_subset`].
    #[inline]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    /// See [`std::collections::HashSet::is_superset`].
    #[inline]
    pub fn is_superset(&self, other: &Self) -> bool {
        self.0.is_superset(&other.0)
    }

    /// See [`std::collections::HashSet::insert`].
    #[inline]
    pub fn insert(&mut self, value: T) -> bool {
        self.0.insert(value)
    }

    /// See [`std::collections::HashSet::replace`].
    #[inline]
    pub fn replace(&mut self, value: T) -> Option<T> {
        self.0.replace(value)
    }

    /// See [`std::collections::HashSet::remove`].
    #[inline]
    pub fn remove<Q: ?Sized>(&mut self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.remove(value)
    }

    /// See [`std::collections::HashSet::take`].
    #[inline]
    pub fn take<Q: ?Sized>(&mut self, value: &Q) -> Option<T>
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.0.take(value)
    }
}

impl<T> PartialEq for HashSet<T>
where
    T: Eq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T> Eq for HashSet<T> where T: Eq + Hash {}

impl<T> FromIterator<T> for HashSet<T>
where
    T: Eq + Hash,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> HashSet<T> {
        Self(StdSet::from_iter(iter))
    }
}

impl<T> From<StdSet<T>> for HashSet<T>
where
    T: Eq + Hash,
{
    fn from(set: StdSet<T>) -> Self {
        Self(set)
    }
}

impl<T, const N: usize> From<[T; N]> for HashSet<T>
where
    T: Eq + Hash,
{
    fn from(arr: [T; N]) -> Self {
        Self(StdSet::from(arr))
    }
}

impl<T> Extend<T> for HashSet<T>
where
    T: Eq + Hash,
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.0.extend(iter);
    }
}

impl<'a, T> Extend<&'a T> for HashSet<T>
where
    T: 'a + Eq + Hash + Copy,
{
    fn extend<I: IntoIterator<Item = &'a T>>(&mut self, iter: I) {
        self.0.extend(iter);
    }
}

impl<T> BitOr<&HashSet<T>> for &HashSet<T>
where
    T: Eq + Hash + Clone,
{
    type Output = HashSet<T>;

    fn bitor(self, rhs: &HashSet<T>) -> HashSet<T> {
        HashSet(self.0.bitor(&rhs.0))
    }
}

impl<T> BitAnd<&HashSet<T>> for &HashSet<T>
where
    T: Eq + Hash + Clone,
{
    type Output = HashSet<T>;

    fn bitand(self, rhs: &HashSet<T>) -> HashSet<T> {
        HashSet(self.0.bitand(&rhs.0))
    }
}

impl<T> BitXor<&HashSet<T>> for &HashSet<T>
where
    T: Eq + Hash + Clone,
{
    type Output = HashSet<T>;

    fn bitxor(self, rhs: &HashSet<T>) -> HashSet<T> {
        HashSet(self.0.bitxor(&rhs.0))
    }
}

impl<T> Sub<&HashSet<T>> for &HashSet<T>
where
    T: Eq + Hash + Clone,
{
    type Output = HashSet<T>;

    fn sub(self, rhs: &HashSet<T>) -> HashSet<T> {
        HashSet(self.0.sub(&rhs.0))
    }
}
