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

//! Columnar batcher storage types for key-val and key-only update collections.
//!
//! Implements trie-shaped columnar storage for sorted, consolidated update collections,
//! suitable for use with the differential-dataflow chainless batcher.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::Range;

use columnar::{Borrow, Columnar, Container, ContainerOf, Index, Len, Push, Vecs};
use columnation::Columnation;
use differential_dataflow::containers::TimelyStack;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::logging::Logger;
use differential_dataflow::trace::implementations::chainless_batcher::BatcherStorage;
use differential_dataflow::trace::implementations::ord_neu::key_batch::{
    OrdKeyBatch, OrdKeyStorage,
};
use differential_dataflow::trace::implementations::ord_neu::val_batch::{
    OrdValBatch, OrdValStorage,
};
use differential_dataflow::trace::implementations::ord_neu::{Upds, Vals};
use differential_dataflow::trace::implementations::{BatchContainer, Layout};
use differential_dataflow::trace::{Builder, Description};
use mz_ore::cast::CastFrom;
use timely::container::PushInto;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::columnar::Column;

/// Cast a `usize` length to `u64` using infallible `CastFrom`.
#[inline(always)]
fn len_to_u64(len: usize) -> u64 {
    u64::cast_from(len)
}

/// Cast a `u64` to `usize` using infallible `CastFrom`.
#[inline(always)]
fn idx(val: u64) -> usize {
    usize::cast_from(val)
}

// ======================== ValStorage ========================

/// Trie-shaped columnar storage for key-val update collections.
///
/// Keys are stored in a flat columnar container. For each key, a list of values is stored
/// in a `Vecs`. For each value, a list of `(time, diff)` updates is stored in another `Vecs`.
pub struct ValStorage<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// An ordered list of keys.
    pub keys: ContainerOf<K>,
    /// For each key in `keys`, a list of values.
    pub vals: Vecs<ContainerOf<V>>,
    /// For each val in `vals`, a list of (time, diff) updates.
    pub upds: Vecs<(ContainerOf<T>, ContainerOf<R>)>,
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> Default for ValStorage<K, V, T, R> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            vals: Default::default(),
            upds: Default::default(),
        }
    }
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> Clone for ValStorage<K, V, T, R>
where
    ContainerOf<K>: Clone,
    Vecs<ContainerOf<V>>: Clone,
    Vecs<(ContainerOf<T>, ContainerOf<R>)>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            vals: self.vals.clone(),
            upds: self.upds.clone(),
        }
    }
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> ValStorage<K, V, T, R> {
    /// Forms a `ValStorage` from a sorted iterator of `((K, V), T, R)` columnar refs.
    ///
    /// The input must be sorted by `(key, val, time)`. Updates with the same key and value
    /// are grouped together; no consolidation is performed.
    ///
    /// Refs are converted to owned values before pushing into the columnar containers.
    pub fn form<'a>(mut sorted: impl Iterator<Item = columnar::Ref<'a, ((K, V), T, R)>>) -> Self
    where
        K: 'a,
        V: 'a,
        T: 'a,
        R: 'a,
        ContainerOf<K>: Push<K>,
        ContainerOf<V>: Push<V>,
        ContainerOf<T>: Push<T>,
        ContainerOf<R>: Push<R>,
        for<'b> columnar::Ref<'b, K>: Copy + Ord,
        for<'b> columnar::Ref<'b, V>: Copy + Ord,
    {
        let mut output = Self::default();

        if let Some(((key, val), time, diff)) = sorted.next() {
            output.keys.push(K::into_owned(key));
            output.vals.values.push(V::into_owned(val));
            output.upds.values.0.push(T::into_owned(time));
            output.upds.values.1.push(R::into_owned(diff));

            for ((key, val), time, diff) in sorted {
                let mut differs = false;
                // Check if key changed.
                let keys_len = output.keys.len();
                differs |=
                    ContainerOf::<K>::reborrow_ref(key) != output.keys.borrow().get(keys_len - 1);
                if differs {
                    output.keys.push(K::into_owned(key));
                }
                // Check if val changed (or key changed).
                let vals_len = output.vals.values.len();
                if differs {
                    output.vals.bounds.push(u64::cast_from(vals_len));
                }
                differs |= ContainerOf::<V>::reborrow_ref(val)
                    != output.vals.values.borrow().get(vals_len - 1);
                if differs {
                    output.vals.values.push(V::into_owned(val));
                }
                // Always push updates.
                let upds_len = output.upds.values.0.len();
                if differs {
                    output.upds.bounds.push(u64::cast_from(upds_len));
                }
                output.upds.values.0.push(T::into_owned(time));
                output.upds.values.1.push(R::into_owned(diff));
            }

            output
                .vals
                .bounds
                .push(len_to_u64(output.vals.values.len()));
            output
                .upds
                .bounds
                .push(len_to_u64(output.upds.values.0.len()));
        }

        debug_assert_eq!(output.keys.len(), output.vals.len());
        debug_assert_eq!(output.vals.values.len(), output.upds.len());

        output
    }

    /// Returns the val index range for a range of key indices.
    pub fn vals_bounds(&self, range: Range<usize>) -> Range<usize> {
        if !range.is_empty() {
            let lower = if range.start == 0 {
                0
            } else {
                idx(Index::get(self.vals.bounds.borrow(), range.start - 1))
            };
            let upper = idx(Index::get(self.vals.bounds.borrow(), range.end - 1));
            lower..upper
        } else {
            range
        }
    }

    /// Returns the update index range for a range of val indices.
    pub fn upds_bounds(&self, range: Range<usize>) -> Range<usize> {
        if !range.is_empty() {
            let lower = if range.start == 0 {
                0
            } else {
                idx(Index::get(self.upds.bounds.borrow(), range.start - 1))
            };
            let upper = idx(Index::get(self.upds.bounds.borrow(), range.end - 1));
            lower..upper
        } else {
            range
        }
    }

    /// Copies `other[range]` into self, including all vals and updates for the given keys.
    pub fn extend_from_keys(&mut self, other: &Self, range: Range<usize>) {
        self.keys
            .extend_from_self(other.keys.borrow(), range.clone());
        self.vals
            .extend_from_self(other.vals.borrow(), range.clone());
        self.upds
            .extend_from_self(other.upds.borrow(), other.vals_bounds(range));
    }

    /// Copies vals and their updates from `other[range]` into self.
    pub fn extend_from_vals(&mut self, other: &Self, range: Range<usize>) {
        self.vals
            .values
            .extend_from_self(other.vals.values.borrow(), range.clone());
        self.upds.extend_from_self(other.upds.borrow(), range);
    }
}

impl<K: Columnar, V: Columnar, T: Columnar, R: Columnar> timely::Accountable
    for ValStorage<K, V, T, R>
{
    #[inline]
    fn record_count(&self) -> i64 {
        self.upds.values.0.len().try_into().expect("must fit")
    }
}

// ======================== KeyStorage ========================

/// Trie-shaped columnar storage for key-only update collections.
///
/// Keys are stored in a flat columnar container. For each key, a list of `(time, diff)`
/// updates is stored in a `Vecs`.
pub struct KeyStorage<K: Columnar, T: Columnar, R: Columnar> {
    /// An ordered list of keys.
    pub keys: ContainerOf<K>,
    /// For each key in `keys`, a list of (time, diff) updates.
    pub upds: Vecs<(ContainerOf<T>, ContainerOf<R>)>,
}

impl<K: Columnar, T: Columnar, R: Columnar> Default for KeyStorage<K, T, R> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            upds: Default::default(),
        }
    }
}

impl<K: Columnar, T: Columnar, R: Columnar> Clone for KeyStorage<K, T, R>
where
    ContainerOf<K>: Clone,
    Vecs<(ContainerOf<T>, ContainerOf<R>)>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            upds: self.upds.clone(),
        }
    }
}

impl<K: Columnar, T: Columnar, R: Columnar> KeyStorage<K, T, R> {
    /// Forms a `KeyStorage` from a sorted iterator of `((K, ()), T, R)` columnar refs.
    pub fn form<'a>(mut sorted: impl Iterator<Item = columnar::Ref<'a, ((K, ()), T, R)>>) -> Self
    where
        K: 'a,
        T: 'a,
        R: 'a,
        ContainerOf<K>: Push<K>,
        ContainerOf<T>: Push<T>,
        ContainerOf<R>: Push<R>,
        for<'b> columnar::Ref<'b, K>: Copy + Ord,
    {
        let mut output = Self::default();

        if let Some(((key, ()), time, diff)) = sorted.next() {
            output.keys.push(K::into_owned(key));
            output.upds.values.0.push(T::into_owned(time));
            output.upds.values.1.push(R::into_owned(diff));

            for ((key, ()), time, diff) in sorted {
                let mut differs = false;
                let keys_len = output.keys.len();
                differs |=
                    ContainerOf::<K>::reborrow_ref(key) != output.keys.borrow().get(keys_len - 1);
                if differs {
                    output.keys.push(K::into_owned(key));
                }
                let upds_len = output.upds.values.0.len();
                if differs {
                    output.upds.bounds.push(u64::cast_from(upds_len));
                }
                output.upds.values.0.push(T::into_owned(time));
                output.upds.values.1.push(R::into_owned(diff));
            }

            output
                .upds
                .bounds
                .push(len_to_u64(output.upds.values.0.len()));
        }

        debug_assert_eq!(output.keys.len(), output.upds.len());

        output
    }

    /// Returns the update index range for a range of key indices.
    pub fn upds_bounds(&self, range: Range<usize>) -> Range<usize> {
        if !range.is_empty() {
            let lower = if range.start == 0 {
                0
            } else {
                idx(Index::get(self.upds.bounds.borrow(), range.start - 1))
            };
            let upper = idx(Index::get(self.upds.bounds.borrow(), range.end - 1));
            lower..upper
        } else {
            range
        }
    }

    /// Copies `other[range]` into self, including all updates for the given keys.
    pub fn extend_from_keys(&mut self, other: &Self, range: Range<usize>) {
        self.keys
            .extend_from_self(other.keys.borrow(), range.clone());
        self.upds.extend_from_self(other.upds.borrow(), range);
    }
}

impl<K: Columnar, T: Columnar, R: Columnar> timely::Accountable for KeyStorage<K, T, R> {
    #[inline]
    fn record_count(&self) -> i64 {
        self.upds.values.0.len().try_into().expect("must fit")
    }
}

// ======================== BatcherStorage impls ========================

impl<K, V, T, R> BatcherStorage<T> for ValStorage<K, V, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    V: Columnar + Ord + Clone + 'static,
    T: Timestamp + Columnar + Ord + Default + Clone + 'static,
    R: Columnar + Ord + Default + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    for<'a> columnar::Ref<'a, V>: Copy + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    for<'a> columnar::Ref<'a, R>: Copy + Ord,
    ContainerOf<K>: Container + Push<K> + Clone + Send,
    ContainerOf<V>: Container + Push<V> + Clone + Send,
    ContainerOf<T>: Container + Push<T> + Clone + Send,
    ContainerOf<R>: Container + Push<R> + Clone + Send,
{
    fn len(&self) -> usize {
        self.upds.values.0.len()
    }

    #[inline(never)]
    fn merge(self, other: Self) -> Self {
        let mut this_sum = R::default();
        let mut that_sum = R::default();

        let mut merged = Self::default();
        let this = self;
        let that = other;
        let this_keys = this.keys.borrow();
        let that_keys = that.keys.borrow();
        let mut this_key_range = 0..this_keys.len();
        let mut that_key_range = 0..that_keys.len();

        while !this_key_range.is_empty() && !that_key_range.is_empty() {
            let this_key = this_keys.get(this_key_range.start);
            let that_key = that_keys.get(that_key_range.start);
            match this_key.cmp(&that_key) {
                std::cmp::Ordering::Less => {
                    let lower = this_key_range.start;
                    gallop(this_keys, &mut this_key_range, |x| x < that_key);
                    merged.extend_from_keys(&this, lower..this_key_range.start);
                }
                std::cmp::Ordering::Equal => {
                    // Keys are equal; must merge vals.
                    let values_len = merged.vals.values.len();
                    let mut this_val_range =
                        this.vals_bounds(this_key_range.start..this_key_range.start + 1);
                    let mut that_val_range =
                        that.vals_bounds(that_key_range.start..that_key_range.start + 1);
                    while !this_val_range.is_empty() && !that_val_range.is_empty() {
                        let this_val = this.vals.values.borrow().get(this_val_range.start);
                        let that_val = that.vals.values.borrow().get(that_val_range.start);
                        match this_val.cmp(&that_val) {
                            std::cmp::Ordering::Less => {
                                let lower = this_val_range.start;
                                gallop(this.vals.values.borrow(), &mut this_val_range, |x| {
                                    x < that_val
                                });
                                merged.extend_from_vals(&this, lower..this_val_range.start);
                            }
                            std::cmp::Ordering::Equal => {
                                // Vals are equal; must merge updates.
                                let updates_len = merged.upds.values.0.len();
                                let mut this_upd_range = this
                                    .upds_bounds(this_val_range.start..this_val_range.start + 1);
                                let mut that_upd_range = that
                                    .upds_bounds(that_val_range.start..that_val_range.start + 1);
                                while !this_upd_range.is_empty() && !that_upd_range.is_empty() {
                                    let this_time =
                                        this.upds.values.0.borrow().get(this_upd_range.start);
                                    let that_time =
                                        that.upds.values.0.borrow().get(that_upd_range.start);
                                    match this_time.cmp(&that_time) {
                                        std::cmp::Ordering::Less => {
                                            let lower = this_upd_range.start;
                                            gallop(
                                                this.upds.values.0.borrow(),
                                                &mut this_upd_range,
                                                |x| x < that_time,
                                            );
                                            merged.upds.values.0.extend_from_self(
                                                this.upds.values.0.borrow(),
                                                lower..this_upd_range.start,
                                            );
                                            merged.upds.values.1.extend_from_self(
                                                this.upds.values.1.borrow(),
                                                lower..this_upd_range.start,
                                            );
                                        }
                                        std::cmp::Ordering::Equal => {
                                            // Times are equal; consolidate diffs.
                                            let this_diff = this
                                                .upds
                                                .values
                                                .1
                                                .borrow()
                                                .get(this_upd_range.start);
                                            let that_diff = that
                                                .upds
                                                .values
                                                .1
                                                .borrow()
                                                .get(that_upd_range.start);
                                            R::copy_from(&mut this_sum, this_diff);
                                            R::copy_from(&mut that_sum, that_diff);
                                            this_sum.plus_equals(&that_sum);
                                            if !this_sum.is_zero() {
                                                merged.upds.values.0.push(T::into_owned(this_time));
                                                merged.upds.values.1.push(this_sum.clone());
                                            }
                                            this_upd_range.start += 1;
                                            that_upd_range.start += 1;
                                        }
                                        std::cmp::Ordering::Greater => {
                                            let lower = that_upd_range.start;
                                            gallop(
                                                that.upds.values.0.borrow(),
                                                &mut that_upd_range,
                                                |x| x < this_time,
                                            );
                                            merged.upds.values.0.extend_from_self(
                                                that.upds.values.0.borrow(),
                                                lower..that_upd_range.start,
                                            );
                                            merged.upds.values.1.extend_from_self(
                                                that.upds.values.1.borrow(),
                                                lower..that_upd_range.start,
                                            );
                                        }
                                    }
                                }
                                // Remaining updates.
                                merged.upds.values.0.extend_from_self(
                                    this.upds.values.0.borrow(),
                                    this_upd_range.clone(),
                                );
                                merged
                                    .upds
                                    .values
                                    .1
                                    .extend_from_self(this.upds.values.1.borrow(), this_upd_range);
                                merged.upds.values.0.extend_from_self(
                                    that.upds.values.0.borrow(),
                                    that_upd_range.clone(),
                                );
                                merged
                                    .upds
                                    .values
                                    .1
                                    .extend_from_self(that.upds.values.1.borrow(), that_upd_range);
                                // Seal updates and push val if any updates remain.
                                if merged.upds.values.0.len() > updates_len {
                                    merged
                                        .upds
                                        .bounds
                                        .push(len_to_u64(merged.upds.values.0.len()));
                                    merged.vals.values.extend_from_self(
                                        this.vals.values.borrow(),
                                        this_val_range.start..this_val_range.start + 1,
                                    );
                                }
                                this_val_range.start += 1;
                                that_val_range.start += 1;
                            }
                            std::cmp::Ordering::Greater => {
                                let lower = that_val_range.start;
                                gallop(that.vals.values.borrow(), &mut that_val_range, |x| {
                                    x < this_val
                                });
                                merged.extend_from_vals(&that, lower..that_val_range.start);
                            }
                        }
                    }
                    // Remaining vals.
                    merged.extend_from_vals(&this, this_val_range);
                    merged.extend_from_vals(&that, that_val_range);
                    // Seal vals and push key if any vals remain.
                    if merged.vals.values.len() > values_len {
                        merged
                            .vals
                            .bounds
                            .push(len_to_u64(merged.vals.values.len()));
                        merged.keys.extend_from_self(
                            this.keys.borrow(),
                            this_key_range.start..this_key_range.start + 1,
                        );
                    }
                    this_key_range.start += 1;
                    that_key_range.start += 1;
                }
                std::cmp::Ordering::Greater => {
                    let lower = that_key_range.start;
                    gallop(that_keys, &mut that_key_range, |x| x < this_key);
                    merged.extend_from_keys(&that, lower..that_key_range.start);
                }
            }
        }
        // Remaining keys.
        merged.extend_from_keys(&this, this_key_range);
        merged.extend_from_keys(&that, that_key_range);

        merged
    }

    #[inline(never)]
    fn split(&mut self, frontier: AntichainRef<T>) -> Self {
        let mut ship = Self::default();
        let mut keep = Self::default();
        let mut time = T::default();

        for key_idx in 0..self.keys.len() {
            let keep_vals_len = keep.vals.values.len();
            let ship_vals_len = ship.vals.values.len();
            for val_idx in self.vals_bounds(key_idx..key_idx + 1) {
                let keep_upds_len = keep.upds.values.0.len();
                let ship_upds_len = ship.upds.values.0.len();
                for upd_idx in self.upds_bounds(val_idx..val_idx + 1) {
                    let t = self.upds.values.0.borrow().get(upd_idx);
                    T::copy_from(&mut time, t);
                    if frontier.less_equal(&time) {
                        keep.upds
                            .values
                            .0
                            .extend_from_self(self.upds.values.0.borrow(), upd_idx..upd_idx + 1);
                        keep.upds
                            .values
                            .1
                            .extend_from_self(self.upds.values.1.borrow(), upd_idx..upd_idx + 1);
                    } else {
                        ship.upds
                            .values
                            .0
                            .extend_from_self(self.upds.values.0.borrow(), upd_idx..upd_idx + 1);
                        ship.upds
                            .values
                            .1
                            .extend_from_self(self.upds.values.1.borrow(), upd_idx..upd_idx + 1);
                    }
                }
                if keep.upds.values.0.len() > keep_upds_len {
                    keep.upds.bounds.push(len_to_u64(keep.upds.values.0.len()));
                    keep.vals
                        .values
                        .extend_from_self(self.vals.values.borrow(), val_idx..val_idx + 1);
                }
                if ship.upds.values.0.len() > ship_upds_len {
                    ship.upds.bounds.push(len_to_u64(ship.upds.values.0.len()));
                    ship.vals
                        .values
                        .extend_from_self(self.vals.values.borrow(), val_idx..val_idx + 1);
                }
            }
            if keep.vals.values.len() > keep_vals_len {
                keep.vals.bounds.push(len_to_u64(keep.vals.values.len()));
                keep.keys
                    .extend_from_self(self.keys.borrow(), key_idx..key_idx + 1);
            }
            if ship.vals.values.len() > ship_vals_len {
                ship.vals.bounds.push(len_to_u64(ship.vals.values.len()));
                ship.keys
                    .extend_from_self(self.keys.borrow(), key_idx..key_idx + 1);
            }
        }

        *self = keep;
        ship
    }

    fn lower(&self, frontier: &mut Antichain<T>) {
        let mut times = self.upds.values.0.borrow().into_index_iter();
        if let Some(time_ref) = times.next() {
            let mut time = T::into_owned(time_ref);
            frontier.insert_ref(&time);
            for time_ref in times {
                T::copy_from(&mut time, time_ref);
                frontier.insert_ref(&time);
            }
        }
    }
}

impl<K, T, R> BatcherStorage<T> for KeyStorage<K, T, R>
where
    K: Columnar + Ord + Clone + 'static,
    T: Timestamp + Columnar + Ord + Default + Clone + 'static,
    R: Columnar + Ord + Default + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    for<'a> columnar::Ref<'a, R>: Copy + Ord,
    ContainerOf<K>: Container + Push<K> + Clone + Send,
    ContainerOf<T>: Container + Push<T> + Clone + Send,
    ContainerOf<R>: Container + Push<R> + Clone + Send,
{
    fn len(&self) -> usize {
        self.upds.values.0.len()
    }

    #[inline(never)]
    fn merge(self, other: Self) -> Self {
        let mut this_sum = R::default();
        let mut that_sum = R::default();

        let mut merged = Self::default();
        let this = self;
        let that = other;
        let this_keys = this.keys.borrow();
        let that_keys = that.keys.borrow();
        let mut this_key_range = 0..this_keys.len();
        let mut that_key_range = 0..that_keys.len();

        while !this_key_range.is_empty() && !that_key_range.is_empty() {
            let this_key = this_keys.get(this_key_range.start);
            let that_key = that_keys.get(that_key_range.start);
            match this_key.cmp(&that_key) {
                std::cmp::Ordering::Less => {
                    let lower = this_key_range.start;
                    gallop(this_keys, &mut this_key_range, |x| x < that_key);
                    merged.extend_from_keys(&this, lower..this_key_range.start);
                }
                std::cmp::Ordering::Equal => {
                    let updates_len = merged.upds.values.0.len();
                    let mut this_upd_range =
                        this.upds_bounds(this_key_range.start..this_key_range.start + 1);
                    let mut that_upd_range =
                        that.upds_bounds(that_key_range.start..that_key_range.start + 1);

                    while !this_upd_range.is_empty() && !that_upd_range.is_empty() {
                        let this_time = this.upds.values.0.borrow().get(this_upd_range.start);
                        let that_time = that.upds.values.0.borrow().get(that_upd_range.start);
                        match this_time.cmp(&that_time) {
                            std::cmp::Ordering::Less => {
                                let lower = this_upd_range.start;
                                gallop(this.upds.values.0.borrow(), &mut this_upd_range, |x| {
                                    x < that_time
                                });
                                merged.upds.values.0.extend_from_self(
                                    this.upds.values.0.borrow(),
                                    lower..this_upd_range.start,
                                );
                                merged.upds.values.1.extend_from_self(
                                    this.upds.values.1.borrow(),
                                    lower..this_upd_range.start,
                                );
                            }
                            std::cmp::Ordering::Equal => {
                                let this_diff =
                                    this.upds.values.1.borrow().get(this_upd_range.start);
                                let that_diff =
                                    that.upds.values.1.borrow().get(that_upd_range.start);
                                R::copy_from(&mut this_sum, this_diff);
                                R::copy_from(&mut that_sum, that_diff);
                                this_sum.plus_equals(&that_sum);
                                if !this_sum.is_zero() {
                                    merged.upds.values.0.push(T::into_owned(this_time));
                                    merged.upds.values.1.push(this_sum.clone());
                                }
                                this_upd_range.start += 1;
                                that_upd_range.start += 1;
                            }
                            std::cmp::Ordering::Greater => {
                                let lower = that_upd_range.start;
                                gallop(that.upds.values.0.borrow(), &mut that_upd_range, |x| {
                                    x < this_time
                                });
                                merged.upds.values.0.extend_from_self(
                                    that.upds.values.0.borrow(),
                                    lower..that_upd_range.start,
                                );
                                merged.upds.values.1.extend_from_self(
                                    that.upds.values.1.borrow(),
                                    lower..that_upd_range.start,
                                );
                            }
                        }
                    }
                    // Remaining updates.
                    merged
                        .upds
                        .values
                        .0
                        .extend_from_self(this.upds.values.0.borrow(), this_upd_range.clone());
                    merged
                        .upds
                        .values
                        .1
                        .extend_from_self(this.upds.values.1.borrow(), this_upd_range);
                    merged
                        .upds
                        .values
                        .0
                        .extend_from_self(that.upds.values.0.borrow(), that_upd_range.clone());
                    merged
                        .upds
                        .values
                        .1
                        .extend_from_self(that.upds.values.1.borrow(), that_upd_range);
                    // Seal updates and push key.
                    if merged.upds.values.0.len() > updates_len {
                        merged
                            .upds
                            .bounds
                            .push(len_to_u64(merged.upds.values.0.len()));
                        merged.keys.extend_from_self(
                            this.keys.borrow(),
                            this_key_range.start..this_key_range.start + 1,
                        );
                    }
                    this_key_range.start += 1;
                    that_key_range.start += 1;
                }
                std::cmp::Ordering::Greater => {
                    let lower = that_key_range.start;
                    gallop(that_keys, &mut that_key_range, |x| x < this_key);
                    merged.extend_from_keys(&that, lower..that_key_range.start);
                }
            }
        }
        // Remaining keys.
        merged.extend_from_keys(&this, this_key_range);
        merged.extend_from_keys(&that, that_key_range);

        merged
    }

    #[inline(never)]
    fn split(&mut self, frontier: AntichainRef<T>) -> Self {
        let mut ship = Self::default();
        let mut keep = Self::default();
        let mut time = T::default();

        for key_idx in 0..self.keys.len() {
            let keep_upds_len = keep.upds.values.0.len();
            let ship_upds_len = ship.upds.values.0.len();
            for upd_idx in self.upds_bounds(key_idx..key_idx + 1) {
                let t = self.upds.values.0.borrow().get(upd_idx);
                T::copy_from(&mut time, t);
                if frontier.less_equal(&time) {
                    keep.upds
                        .values
                        .0
                        .extend_from_self(self.upds.values.0.borrow(), upd_idx..upd_idx + 1);
                    keep.upds
                        .values
                        .1
                        .extend_from_self(self.upds.values.1.borrow(), upd_idx..upd_idx + 1);
                } else {
                    ship.upds
                        .values
                        .0
                        .extend_from_self(self.upds.values.0.borrow(), upd_idx..upd_idx + 1);
                    ship.upds
                        .values
                        .1
                        .extend_from_self(self.upds.values.1.borrow(), upd_idx..upd_idx + 1);
                }
            }
            if keep.upds.values.0.len() > keep_upds_len {
                keep.upds.bounds.push(len_to_u64(keep.upds.values.0.len()));
                keep.keys
                    .extend_from_self(self.keys.borrow(), key_idx..key_idx + 1);
            }
            if ship.upds.values.0.len() > ship_upds_len {
                ship.upds.bounds.push(len_to_u64(ship.upds.values.0.len()));
                ship.keys
                    .extend_from_self(self.keys.borrow(), key_idx..key_idx + 1);
            }
        }

        *self = keep;
        ship
    }

    fn lower(&self, frontier: &mut Antichain<T>) {
        let mut times = self.upds.values.0.borrow().into_index_iter();
        if let Some(time_ref) = times.next() {
            let mut time = T::into_owned(time_ref);
            frontier.insert_ref(&time);
            for time_ref in times {
                T::copy_from(&mut time, time_ref);
                frontier.insert_ref(&time);
            }
        }
    }
}

// ======================== FormStorage trait ========================

/// Trait for converting input containers into `ValStorage`.
pub trait FormValStorage<K: Columnar, V: Columnar, T: Columnar, R: Columnar> {
    /// Converts the input into a `ValStorage`, consuming the input. Returns `None` if empty.
    fn form_val_storage(&mut self) -> Option<ValStorage<K, V, T, R>>;
}

/// Trait for converting input containers into `KeyStorage`.
pub trait FormKeyStorage<K: Columnar, T: Columnar, R: Columnar> {
    /// Converts the input into a `KeyStorage`, consuming the input. Returns `None` if empty.
    fn form_key_storage(&mut self) -> Option<KeyStorage<K, T, R>>;
}

impl<K, V, T, R> FormValStorage<K, V, T, R> for Vec<((K, V), T, R)>
where
    K: Columnar + Ord + Clone + 'static,
    V: Columnar + Ord + Clone + 'static,
    T: Columnar + Ord + Clone + 'static,
    R: Columnar + Ord + Clone + 'static,
    ((K, V), T, R): Columnar,
    ContainerOf<((K, V), T, R)>: Push<((K, V), T, R)>,
    for<'a> columnar::Ref<'a, ((K, V), T, R)>: Copy + Ord,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    for<'a> columnar::Ref<'a, V>: Copy + Ord,
    ContainerOf<K>: Push<K>,
    ContainerOf<V>: Push<V>,
    ContainerOf<T>: Push<T>,
    ContainerOf<R>: Push<R>,
{
    fn form_val_storage(&mut self) -> Option<ValStorage<K, V, T, R>> {
        if self.is_empty() {
            return None;
        }
        // Push into a columnar container, then sort columnar refs.
        let mut container: ContainerOf<((K, V), T, R)> = Default::default();
        for item in self.drain(..) {
            container.push(item);
        }
        let borrowed = container.borrow();
        let mut permutation: Vec<_> = borrowed.into_index_iter().collect();
        permutation.sort_unstable();
        Some(ValStorage::form(permutation.into_iter()))
    }
}

impl<K, V, T, R> FormValStorage<K, V, T, R> for Column<((K, V), T, R)>
where
    K: Columnar + Ord + Clone + 'static,
    V: Columnar + Ord + Clone + 'static,
    T: Columnar + Ord + Clone + 'static,
    R: Columnar + Ord + Clone + 'static,
    ((K, V), T, R): Columnar,
    for<'a> columnar::Ref<'a, ((K, V), T, R)>: Copy + Ord,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    for<'a> columnar::Ref<'a, V>: Copy + Ord,
    ContainerOf<K>: Push<K>,
    ContainerOf<V>: Push<V>,
    ContainerOf<T>: Push<T>,
    ContainerOf<R>: Push<R>,
{
    fn form_val_storage(&mut self) -> Option<ValStorage<K, V, T, R>> {
        let borrowed = self.borrow();
        if borrowed.len() == 0 {
            return None;
        }
        let mut permutation: Vec<_> = borrowed.into_index_iter().collect();
        permutation.sort_unstable();
        Some(ValStorage::form(permutation.into_iter()))
    }
}

impl<K, T, R> FormKeyStorage<K, T, R> for Vec<((K, ()), T, R)>
where
    K: Columnar + Ord + Clone + 'static,
    T: Columnar + Ord + Clone + 'static,
    R: Columnar + Ord + Clone + 'static,
    ((K, ()), T, R): Columnar,
    ContainerOf<((K, ()), T, R)>: Push<((K, ()), T, R)>,
    for<'a> columnar::Ref<'a, ((K, ()), T, R)>: Copy + Ord,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    ContainerOf<K>: Push<K>,
    ContainerOf<T>: Push<T>,
    ContainerOf<R>: Push<R>,
{
    fn form_key_storage(&mut self) -> Option<KeyStorage<K, T, R>> {
        if self.is_empty() {
            return None;
        }
        let mut container: ContainerOf<((K, ()), T, R)> = Default::default();
        for item in self.drain(..) {
            container.push(item);
        }
        let borrowed = container.borrow();
        let mut permutation: Vec<_> = borrowed.into_index_iter().collect();
        permutation.sort_unstable();
        Some(KeyStorage::form(permutation.into_iter()))
    }
}

impl<K, T, R> FormKeyStorage<K, T, R> for Column<((K, ()), T, R)>
where
    K: Columnar + Ord + Clone + 'static,
    T: Columnar + Ord + Clone + 'static,
    R: Columnar + Ord + Clone + 'static,
    ((K, ()), T, R): Columnar,
    for<'a> columnar::Ref<'a, ((K, ()), T, R)>: Copy + Ord,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    ContainerOf<K>: Push<K>,
    ContainerOf<T>: Push<T>,
    ContainerOf<R>: Push<R>,
{
    fn form_key_storage(&mut self) -> Option<KeyStorage<K, T, R>> {
        let borrowed = self.borrow();
        if borrowed.len() == 0 {
            return None;
        }
        let mut permutation: Vec<_> = borrowed.into_index_iter().collect();
        permutation.sort_unstable();
        Some(KeyStorage::form(permutation.into_iter()))
    }
}

// ======================== Batcher types ========================

/// A columnar batcher for key-val update collections.
///
/// Accepts input containers of type `I` (either `Vec<((K,V),T,R)>` or `Column<((K,V),T,R)>`),
/// converts them to trie-shaped `ValStorage`, and maintains a geometrically-sized merge tree.
pub struct ColumnarValBatcher<K: Columnar, V: Columnar, T: Timestamp + Columnar, R: Columnar, I> {
    storages: Vec<ValStorage<K, V, T, R>>,
    lower: Antichain<T>,
    prior: Antichain<T>,
    _marker: PhantomData<fn(I)>,
}

impl<K, V, T, R, I> ColumnarValBatcher<K, V, T, R, I>
where
    K: Columnar,
    V: Columnar,
    T: Timestamp + Columnar,
    R: Columnar,
    ValStorage<K, V, T, R>: BatcherStorage<T>,
{
    fn tidy(&mut self) {
        self.storages.retain(|x| x.len() > 0);
        self.storages.sort_by_key(|x| x.len());
        self.storages.reverse();
        while let Some(pos) = (1..self.storages.len())
            .position(|i| self.storages[i - 1].len() < 2 * self.storages[i].len())
        {
            while self.storages.len() > pos + 1 {
                let x = self.storages.pop().unwrap();
                let y = self.storages.pop().unwrap();
                self.storages.push(x.merge(y));
                self.storages.sort_by_key(|x| x.len());
                self.storages.reverse();
            }
        }
    }
}

impl<K, V, T, R, I> differential_dataflow::trace::Batcher for ColumnarValBatcher<K, V, T, R, I>
where
    K: Columnar + Ord + Clone + 'static,
    V: Columnar + Ord + Clone + 'static,
    T: Timestamp + Columnar + Ord + Default + Clone + 'static,
    R: Columnar + Ord + Default + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    for<'a> columnar::Ref<'a, V>: Copy + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    for<'a> columnar::Ref<'a, R>: Copy + Ord,
    ContainerOf<K>: Container + Push<K> + Clone + Send,
    ContainerOf<V>: Container + Push<V> + Clone + Send,
    ContainerOf<T>: Container + Push<T> + Clone + Send,
    ContainerOf<R>: Container + Push<R> + Clone + Send,
    I: timely::Container + Clone + 'static + FormValStorage<K, V, T, R>,
    ValStorage<K, V, T, R>: BatcherStorage<T>,
{
    type Time = T;
    type Input = I;
    type Output = ValStorage<K, V, T, R>;

    fn new(_logger: Option<Logger>, _operator_id: usize) -> Self {
        Self {
            storages: Vec::new(),
            lower: Default::default(),
            prior: Antichain::from_elem(T::minimum()),
            _marker: PhantomData,
        }
    }

    fn push_container(&mut self, batch: &mut Self::Input) {
        if let Some(storage) = batch.form_val_storage() {
            storage.lower(&mut self.lower);
            self.storages.push(storage);
            self.tidy();
        }
    }

    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
        &mut self,
        upper: Antichain<Self::Time>,
    ) -> B::Output {
        let description = Description::new(self.prior.clone(), upper.clone(), Antichain::new());
        self.prior = upper.clone();
        if let Some(mut store) = self.storages.pop() {
            self.lower.clear();
            let mut ship = store.split(upper.borrow());
            let mut keep = store;
            while let Some(mut store) = self.storages.pop() {
                let split = store.split(upper.borrow());
                ship = ship.merge(split);
                keep = keep.merge(store);
            }
            keep.lower(&mut self.lower);
            self.storages.push(keep);
            B::seal(&mut vec![ship], description)
        } else {
            B::seal(&mut vec![], description)
        }
    }

    fn frontier(&mut self) -> AntichainRef<'_, Self::Time> {
        self.lower.borrow()
    }
}

/// A columnar batcher for key-only update collections.
pub struct ColumnarKeyBatcher<K: Columnar, T: Timestamp + Columnar, R: Columnar, I> {
    storages: Vec<KeyStorage<K, T, R>>,
    lower: Antichain<T>,
    prior: Antichain<T>,
    _marker: PhantomData<fn(I)>,
}

impl<K, T, R, I> ColumnarKeyBatcher<K, T, R, I>
where
    K: Columnar,
    T: Timestamp + Columnar,
    R: Columnar,
    KeyStorage<K, T, R>: BatcherStorage<T>,
{
    fn tidy(&mut self) {
        self.storages.retain(|x| x.len() > 0);
        self.storages.sort_by_key(|x| x.len());
        self.storages.reverse();
        while let Some(pos) = (1..self.storages.len())
            .position(|i| self.storages[i - 1].len() < 2 * self.storages[i].len())
        {
            while self.storages.len() > pos + 1 {
                let x = self.storages.pop().unwrap();
                let y = self.storages.pop().unwrap();
                self.storages.push(x.merge(y));
                self.storages.sort_by_key(|x| x.len());
                self.storages.reverse();
            }
        }
    }
}

impl<K, T, R, I> differential_dataflow::trace::Batcher for ColumnarKeyBatcher<K, T, R, I>
where
    K: Columnar + Ord + Clone + 'static,
    T: Timestamp + Columnar + Ord + Default + Clone + 'static,
    R: Columnar + Ord + Default + Clone + Semigroup + 'static,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    for<'a> columnar::Ref<'a, R>: Copy + Ord,
    ContainerOf<K>: Container + Push<K> + Clone + Send,
    ContainerOf<T>: Container + Push<T> + Clone + Send,
    ContainerOf<R>: Container + Push<R> + Clone + Send,
    I: timely::Container + Clone + 'static + FormKeyStorage<K, T, R>,
    KeyStorage<K, T, R>: BatcherStorage<T>,
{
    type Time = T;
    type Input = I;
    type Output = KeyStorage<K, T, R>;

    fn new(_logger: Option<Logger>, _operator_id: usize) -> Self {
        Self {
            storages: Vec::new(),
            lower: Default::default(),
            prior: Antichain::from_elem(T::minimum()),
            _marker: PhantomData,
        }
    }

    fn push_container(&mut self, batch: &mut Self::Input) {
        if let Some(storage) = batch.form_key_storage() {
            storage.lower(&mut self.lower);
            self.storages.push(storage);
            self.tidy();
        }
    }

    fn seal<B: Builder<Input = Self::Output, Time = Self::Time>>(
        &mut self,
        upper: Antichain<Self::Time>,
    ) -> B::Output {
        let description = Description::new(self.prior.clone(), upper.clone(), Antichain::new());
        self.prior = upper.clone();
        if let Some(mut store) = self.storages.pop() {
            self.lower.clear();
            let mut ship = store.split(upper.borrow());
            let mut keep = store;
            while let Some(mut store) = self.storages.pop() {
                let split = store.split(upper.borrow());
                ship = ship.merge(split);
                keep = keep.merge(store);
            }
            keep.lower(&mut self.lower);
            self.storages.push(keep);
            B::seal(&mut vec![ship], description)
        } else {
            B::seal(&mut vec![], description)
        }
    }

    fn frontier(&mut self) -> AntichainRef<'_, Self::Time> {
        self.lower.borrow()
    }
}

// ======================== Seal builders ========================

/// A builder that converts `ValStorage` into `OrdValBatch<L>`.
///
/// Iterates the trie-shaped columnar storage and pushes owned values into the
/// layout's batch containers.
pub struct ValSealBuilder<K, V, T, R, L: Layout> {
    _marker: PhantomData<(K, V, T, R, L)>,
}

impl<K, V, T, R, L> Builder for ValSealBuilder<K, V, T, R, L>
where
    K: Columnar + Ord + Clone + 'static,
    V: Columnar + Ord + Clone + 'static,
    T: Timestamp + Columnar + Ord + Default + Clone + 'static,
    R: Columnar + Ord + Default + Clone + 'static,
    L: Layout,
    L::KeyContainer: BatchContainer<Owned = K> + Default,
    L::ValContainer: BatchContainer<Owned = V> + Default,
    L::TimeContainer: BatchContainer<Owned = T> + Default,
    L::DiffContainer: BatchContainer<Owned = R> + Default,
    L::OffsetContainer: for<'a> BatchContainer<ReadItem<'a> = usize> + Default,
    for<'a> columnar::Ref<'a, K>: Copy,
    for<'a> columnar::Ref<'a, V>: Copy,
    for<'a> columnar::Ref<'a, T>: Copy,
    for<'a> columnar::Ref<'a, R>: Copy,
{
    type Time = T;
    type Input = ValStorage<K, V, T, R>;
    type Output = OrdValBatch<L>;

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    fn push(&mut self, _chunk: &mut Self::Input) {
        unimplemented!("ValSealBuilder only supports seal, not push")
    }

    fn done(self, _description: Description<Self::Time>) -> Self::Output {
        unimplemented!("ValSealBuilder only supports seal, not done")
    }

    fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
        if chain.is_empty() {
            OrdValBatch {
                storage: OrdValStorage {
                    keys: Default::default(),
                    vals: Vals::<L::OffsetContainer, L::ValContainer>::default(),
                    upds: Upds::<L::OffsetContainer, L::TimeContainer, L::DiffContainer>::default(),
                },
                description,
                updates: 0,
            }
        } else {
            assert_eq!(chain.len(), 1, "ValSealBuilder expects at most one storage");
            let storage = chain.pop().unwrap();
            let updates = storage.upds.values.0.len();

            // Build OrdValStorage by iterating the trie.
            let mut keys = L::KeyContainer::with_capacity(storage.keys.len());
            let mut vals = Vals::<L::OffsetContainer, L::ValContainer>::with_capacity(
                storage.keys.len(),
                storage.vals.values.len(),
            );
            let mut upds =
                Upds::<L::OffsetContainer, L::TimeContainer, L::DiffContainer>::with_capacity(
                    storage.vals.values.len(),
                    storage.upds.values.0.len(),
                );

            for key_idx in 0..storage.keys.len() {
                let key_ref = storage.keys.borrow().get(key_idx);
                let key_owned = K::into_owned(key_ref);
                keys.push_own(&key_owned);

                for val_idx in storage.vals_bounds(key_idx..key_idx + 1) {
                    let val_ref = storage.vals.values.borrow().get(val_idx);
                    let val_owned = V::into_owned(val_ref);
                    vals.vals.push_own(&val_owned);

                    for upd_idx in storage.upds_bounds(val_idx..val_idx + 1) {
                        let time_ref = storage.upds.values.0.borrow().get(upd_idx);
                        let diff_ref = storage.upds.values.1.borrow().get(upd_idx);
                        let time_owned = T::into_owned(time_ref);
                        let diff_owned = R::into_owned(diff_ref);
                        upds.times.push_own(&time_owned);
                        upds.diffs.push_own(&diff_owned);
                    }
                    upds.offs.push_ref(upds.times.len());
                }
                vals.offs.push_ref(vals.vals.len());
            }

            OrdValBatch {
                storage: OrdValStorage { keys, vals, upds },
                description,
                updates,
            }
        }
    }
}

/// A builder that converts `KeyStorage` into `OrdKeyBatch<L>`.
pub struct KeySealBuilder<K, T, R, L: Layout> {
    _marker: PhantomData<(K, T, R, L)>,
}

impl<K, T, R, L> Builder for KeySealBuilder<K, T, R, L>
where
    K: Columnar + Ord + Clone + 'static,
    T: Timestamp + Columnar + Ord + Default + Clone + 'static,
    R: Columnar + Ord + Default + Clone + 'static,
    L: Layout,
    L::KeyContainer: BatchContainer<Owned = K> + Default,
    L::ValContainer: Default,
    L::TimeContainer: BatchContainer<Owned = T> + Default,
    L::DiffContainer: BatchContainer<Owned = R> + Default,
    L::OffsetContainer: for<'a> BatchContainer<ReadItem<'a> = usize> + Default,
    for<'a> columnar::Ref<'a, K>: Copy,
    for<'a> columnar::Ref<'a, T>: Copy,
    for<'a> columnar::Ref<'a, R>: Copy,
{
    type Time = T;
    type Input = KeyStorage<K, T, R>;
    type Output = OrdKeyBatch<L>;

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    fn push(&mut self, _chunk: &mut Self::Input) {
        unimplemented!("KeySealBuilder only supports seal, not push")
    }

    fn done(self, _description: Description<Self::Time>) -> Self::Output {
        unimplemented!("KeySealBuilder only supports seal, not done")
    }

    fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
        if chain.is_empty() {
            OrdKeyBatch {
                storage: OrdKeyStorage {
                    keys: Default::default(),
                    upds: Upds::<L::OffsetContainer, L::TimeContainer, L::DiffContainer>::default(),
                },
                description,
                updates: 0,
                value: Default::default(),
            }
        } else {
            assert_eq!(chain.len(), 1, "KeySealBuilder expects at most one storage");
            let storage = chain.pop().unwrap();
            let updates = storage.upds.values.0.len();

            let mut keys = L::KeyContainer::with_capacity(storage.keys.len());
            let mut upds =
                Upds::<L::OffsetContainer, L::TimeContainer, L::DiffContainer>::with_capacity(
                    storage.keys.len(),
                    storage.upds.values.0.len(),
                );

            for key_idx in 0..storage.keys.len() {
                let key_ref = storage.keys.borrow().get(key_idx);
                let key_owned = K::into_owned(key_ref);
                keys.push_own(&key_owned);

                for upd_idx in storage.upds_bounds(key_idx..key_idx + 1) {
                    let time_ref = storage.upds.values.0.borrow().get(upd_idx);
                    let diff_ref = storage.upds.values.1.borrow().get(upd_idx);
                    let time_owned = T::into_owned(time_ref);
                    let diff_owned = R::into_owned(diff_ref);
                    upds.times.push_own(&time_owned);
                    upds.diffs.push_own(&diff_owned);
                }
                upds.offs.push_ref(upds.times.len());
            }

            OrdKeyBatch {
                storage: OrdKeyStorage { keys, upds },
                description,
                updates,
                value: Default::default(),
            }
        }
    }
}

// ======================== Helpers ========================

/// Galloping search: advances `range.start` past all elements satisfying `cmp`.
#[inline(always)]
fn gallop<TC: Index>(
    input: TC,
    range: &mut Range<usize>,
    mut cmp: impl FnMut(<TC as Index>::Ref) -> bool,
) {
    if !Range::<usize>::is_empty(range) && cmp(input.get(range.start)) {
        let mut step = 1;
        while range.start + step < range.end && cmp(input.get(range.start + step)) {
            range.start += step;
            step <<= 1;
        }

        step >>= 1;
        while step > 0 {
            if range.start + step < range.end && cmp(input.get(range.start + step)) {
                range.start += step;
            }
            step >>= 1;
        }

        range.start += 1;
    }
}

// ======================== Legacy Chunker (kept for backward compatibility) ========================

/// A chunker to transform input data into sorted columns.
#[derive(Default)]
pub struct Chunker<C> {
    /// Buffer into which we'll consolidate.
    ///
    /// Also the buffer where we'll stage responses to `extract` and `finish`.
    /// When these calls return, the buffer is available for reuse.
    target: C,
    /// Consolidated buffers ready to go.
    ready: VecDeque<C>,
}

impl<C: timely::Container + Clone + 'static> timely::container::ContainerBuilder for Chunker<C> {
    type Container = C;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.target = ready;
            Some(&mut self.target)
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.extract()
    }
}

impl<'a, D, T, R> PushInto<&'a mut Column<(D, T, R)>> for Chunker<TimelyStack<(D, T, R)>>
where
    D: Columnar + Columnation,
    for<'b> columnar::Ref<'b, D>: Ord + Copy,
    T: Columnar + Columnation,
    for<'b> columnar::Ref<'b, T>: Ord + Copy,
    R: Columnar + Columnation + Semigroup + for<'b> Semigroup<columnar::Ref<'b, R>>,
    for<'b> columnar::Ref<'b, R>: Ord,
{
    fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
        // Sort input data
        let borrowed = container.borrow();
        let mut permutation = Vec::with_capacity(borrowed.len());
        Extend::extend(&mut permutation, borrowed.into_index_iter());
        permutation.sort();

        self.target.clear();
        // Iterate over the data, accumulating diffs for like keys.
        let mut iter = permutation.drain(..);
        if let Some((data, time, diff)) = iter.next() {
            let mut owned_data = D::into_owned(data);
            let mut owned_time = T::into_owned(time);

            let mut prev_data = data;
            let mut prev_time = time;
            let mut prev_diff = <R as Columnar>::into_owned(diff);

            for (data, time, diff) in iter {
                if (&prev_data, &prev_time) == (&data, &time) {
                    prev_diff.plus_equals(&diff);
                } else {
                    if !prev_diff.is_zero() {
                        D::copy_from(&mut owned_data, prev_data);
                        T::copy_from(&mut owned_time, prev_time);
                        let tuple = (owned_data, owned_time, prev_diff);
                        self.target.push_into(&tuple);
                        (owned_data, owned_time, prev_diff) = tuple;
                    }
                    prev_data = data;
                    prev_time = time;
                    R::copy_from(&mut prev_diff, diff);
                }
            }

            if !prev_diff.is_zero() {
                D::copy_from(&mut owned_data, prev_data);
                T::copy_from(&mut owned_time, prev_time);
                let tuple = (owned_data, owned_time, prev_diff);
                self.target.push_into(&tuple);
            }
        }

        if !self.target.is_empty() {
            self.ready.push_back(std::mem::take(&mut self.target));
        }
    }
}
