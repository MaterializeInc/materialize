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

//! ID generation utilities.

use hibitset::BitSet;
use roaring::RoaringBitmap;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{AddAssign, Sub};
use std::sync::{Arc, Mutex};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::cast::CastFrom;
use crate::collections::HashSet;

/// Manages the allocation of unique IDs.
#[derive(Debug, Default, Clone)]
pub struct Gen<Id: From<u64> + Default> {
    id: u64,
    phantom: PhantomData<Id>,
}

impl<Id: From<u64> + Default> Gen<Id> {
    /// Allocates a new identifier of type `Id` and advances the generator.
    pub fn allocate_id(&mut self) -> Id {
        let id = self.id;
        self.id += 1;
        id.into()
    }
}

/// A generator of u64-bit IDs.
pub type IdGen = Gen<u64>;

/// IdAllocator common traits.
pub trait IdGenerator:
    From<u8>
    + AddAssign
    + Sub
    + PartialOrd
    + Copy
    + Eq
    + Hash
    + Ord
    + rand::distributions::uniform::SampleUniform
    + serde::Serialize
    + fmt::Display
{
}

impl<T> IdGenerator for T where
    T: From<u8>
        + AddAssign
        + Sub
        + PartialOrd
        + Copy
        + Eq
        + Hash
        + Ord
        + rand::distributions::uniform::SampleUniform
        + serde::Serialize
        + fmt::Display
{
}

/// Manages allocation of numeric IDs.
#[derive(Debug, Clone)]
pub struct IdAllocator<A: IdAllocatorInner>(Arc<Mutex<A>>);

/// Common trait for id allocators.
pub trait IdAllocatorInner: std::fmt::Debug + Send {
    /// Name of the allocator.
    const NAME: &'static str;
    /// Constructor.
    fn new(min: u32, max: u32) -> Self;
    /// Allocate a new id.
    fn alloc(&mut self) -> Option<u32>;
    /// Deallocate a used id, making it available for reuse.
    fn remove(&mut self, id: u32);
}

/// IdAllocator using a sparse hibitset.
#[derive(Debug)]
pub struct IdAllocatorInnerSparseBitSet {
    next: StdRng,
    min: u32,
    max: u32,
    used: hi_sparse_bitset::BitSet<hi_sparse_bitset::config::_256bit>,
}

impl IdAllocatorInner for IdAllocatorInnerSparseBitSet {
    const NAME: &'static str = "sparse_bit_set";

    fn alloc(&mut self) -> Option<u32> {
        let range = self.min..=self.max;
        let init = self.next.gen_range(range);
        let mut next = init;
        loop {
            // Because bitset has a hard maximum of 64**4 (~16 million), subtract the min in case
            // max is above that. This is safe because we already asserted above that `max - min <
            // 64**4`.
            let stored = usize::cast_from(next - self.min);
            if !self.used.contains(stored) {
                self.used.insert(stored);
                return Some(next);
            }
            // Existing value, increment and try again. Wrap once we hit max back to min.
            next = if next == self.max { self.min } else { next + 1 };
            // We fully wrapped around. BitSet doesn't have a rank or count method, so we can't
            // compute this early.
            if next == init {
                return None;
            }
        }
    }

    fn remove(&mut self, id: u32) {
        let stored = id - self.min;
        self.used.remove(usize::cast_from(stored));
    }

    fn new(min: u32, max: u32) -> Self {
        let total = usize::cast_from(max - min);
        assert!(total < BitSet::BITS_PER_USIZE.pow(4));
        IdAllocatorInnerSparseBitSet {
            next: StdRng::from_entropy(),
            min,
            max,
            used: hi_sparse_bitset::BitSet::new(),
        }
    }
}

/// IdAllocator using a HiBitSet.
#[derive(Debug, Clone)]
pub struct IdAllocatorInnerBitSet {
    next: StdRng,
    min: u32,
    max: u32,
    used: BitSet,
}

impl IdAllocatorInner for IdAllocatorInnerBitSet {
    const NAME: &'static str = "hibitset";

    fn new(min: u32, max: u32) -> Self {
        let total = usize::cast_from(max - min);
        assert!(total < BitSet::BITS_PER_USIZE.pow(4));
        IdAllocatorInnerBitSet {
            next: StdRng::from_entropy(),
            min,
            max,
            used: BitSet::new(),
        }
    }

    fn alloc(&mut self) -> Option<u32> {
        let range = self.min..=self.max;
        let init = self.next.gen_range(range);
        let mut next = init;
        loop {
            // Because hibitset has a hard maximum of 64**4 (~16 million), subtract the min in case
            // max is above that. This is safe because we already asserted above that `max - min <
            // 64**4`.
            let stored = next - self.min;
            if !self.used.add(stored) {
                return Some(next);
            }
            // Existing value, increment and try again. Wrap once we hit max back to min.
            next = if next == self.max { self.min } else { next + 1 };
            // We fully wrapped around. BitSet doesn't have a rank or count method, so we can't
            // compute this early.
            if next == init {
                return None;
            }
        }
    }

    fn remove(&mut self, id: u32) {
        let stored = id - self.min;
        self.used.remove(stored);
    }
}

/// IdAllocator using a RoaringBitmap.
#[derive(Debug)]
pub struct IdAllocatorInnerRoaring {
    next: StdRng,
    min: u32,
    max: u32,
    len: u32,
    used: RoaringBitmap,
}

impl IdAllocatorInner for IdAllocatorInnerRoaring {
    const NAME: &'static str = "roaring_bitmap";

    fn new(min: u32, max: u32) -> Self {
        let mut used = RoaringBitmap::new();
        used.insert_range(min..=max);
        IdAllocatorInnerRoaring {
            next: StdRng::from_entropy(),
            min,
            max,
            len: 0,
            used,
        }
    }

    fn alloc(&mut self) -> Option<u32> {
        let range = self.min..=(self.max - self.len);
        if range.is_empty() {
            return None;
        }
        let init = self.next.gen_range(range);
        let select = init - self.min;
        let chosen = self
            .used
            .select(select)
            .expect("must exist; verified above");
        assert!(self.used.remove(chosen));
        self.len += 1;
        Some(chosen)
    }

    fn remove(&mut self, id: u32) {
        assert!(self.used.insert(id));
        self.len -= 1;
    }
}

/// IdAllocator using a RoaringBitmap.
#[derive(Debug)]
pub struct IdAllocatorInnerRoaringLoop {
    next: StdRng,
    min: u32,
    max: u32,
    used: RoaringBitmap,
}

impl IdAllocatorInner for IdAllocatorInnerRoaringLoop {
    const NAME: &'static str = "roaring_bitmap_loop";

    fn new(min: u32, max: u32) -> Self {
        let used = RoaringBitmap::new();
        IdAllocatorInnerRoaringLoop {
            next: StdRng::from_entropy(),
            min,
            max,
            used,
        }
    }

    fn alloc(&mut self) -> Option<u32> {
        let range = self.min..=self.max;
        let init = self.next.gen_range(range);
        let mut next = init;
        loop {
            let stored = next - self.min;
            if self.used.insert(stored) {
                return Some(next);
            }
            next = if next == self.max { self.min } else { next + 1 };
            if next == init {
                return None;
            }
        }
    }

    fn remove(&mut self, id: u32) {
        let stored = id - self.min;
        self.used.remove(stored);
    }
}

/// IdAllocator using a naive HashSet.
#[derive(Debug)]
pub struct IdAllocatorInnerHashSet {
    next: StdRng,
    min: u32,
    max: u32,
    used: HashSet<u32>,
}

impl IdAllocatorInner for IdAllocatorInnerHashSet {
    const NAME: &'static str = "hashset";

    fn new(min: u32, max: u32) -> Self {
        IdAllocatorInnerHashSet {
            next: StdRng::from_entropy(),
            min,
            max,
            used: HashSet::new(),
        }
    }

    fn alloc(&mut self) -> Option<u32> {
        let range = self.min..=self.max;
        let mut next = self.next.gen_range(range);
        loop {
            if self.used.insert(next) {
                return Some(next);
            }
            if self.used.len() == usize::cast_from(self.max - self.min + 1) {
                return None;
            }
            // Existing value, increment and try again. Wrap once we hit max back to min.
            next = if next == self.max { self.min } else { next + 1 };
        }
    }

    fn remove(&mut self, id: u32) {
        self.used.remove(&id);
    }
}

impl<A: IdAllocatorInner> IdAllocator<A> {
    /// Creates a new `IdAllocator` that will assign IDs between `min` and
    /// `max`, both inclusive.
    pub fn new(min: u32, max: u32) -> IdAllocator<A> {
        assert!(min <= max);
        let inner = A::new(min, max);
        IdAllocator(Arc::new(Mutex::new(inner)))
    }

    /// Allocates a new ID randomly distributed between min and max.
    ///
    /// Returns `None` if the allocator is exhausted.
    ///
    /// The ID associated with the [`IdHandle`] will be freed when all of the
    /// outstanding [`IdHandle`]s have been dropped.
    pub fn alloc(&self) -> Option<IdHandle<u32, A>> {
        let inner = Arc::new(internal::IdHandleInner::new(self)?);
        Some(IdHandle::Dynamic(inner))
    }

    // Attempt to allocate a new ID. We want the ID to be randomly distributed in the range. To do
    // this, choose a random candidate. Check if it's already in the used set, and increment in a
    // loop until an unused slot is found. Ideally we could ask used set what its next used or
    // unused id is after some given X, but that's not part of the API. This means that when the
    // range is large and the used set is near full, we will spend a lot of cycles looking for an
    // open slot. However, we limit the number of connections to the thousands, and connection IDs
    // have 20 bits (~1 million) of space, so it is not currently possible to enter that state.
    fn alloc_internal(&self) -> Option<u32> {
        let mut inner = self.0.lock().expect("lock poisoned");
        inner.alloc()
    }

    fn free_internal(&self, id: u32) {
        let mut inner = self.0.lock().expect("lock poisoned");
        inner.remove(id);
    }
}

/// A clone-able owned reference to an ID.
///
/// Once all of the [`IdHandle`]s referencing an ID have been dropped, we will then free the ID
/// for later re-use.
#[derive(Debug)]
pub enum IdHandle<T, A: IdAllocatorInner> {
    /// An ID "allocated" at compile time.
    ///
    /// Note: It is *entirely* up to the caller to make sure the provided ID is
    /// not used by a dynamic ID allocator.
    Static(T),
    /// An ID allocated at runtime, gets freed once all handles have been dropped.
    Dynamic(Arc<internal::IdHandleInner<T, A>>),
}

impl<T: Clone, A: IdAllocatorInner> Clone for IdHandle<T, A> {
    fn clone(&self) -> Self {
        match self {
            IdHandle::Static(t) => IdHandle::Static(t.clone()),
            IdHandle::Dynamic(handle) => IdHandle::Dynamic(Arc::clone(handle)),
        }
    }
}

impl<T: IdGenerator, A: IdAllocatorInner> IdHandle<T, A> {
    /// Returns the raw ID inside of this handle.
    ///
    /// Use with caution! It is easy for a raw ID to outlive the handle from
    /// which it came. You are responsible for ensuring that your use of the raw
    /// ID does not lead to ID reuse bugs.
    pub fn unhandled(&self) -> T {
        *self.borrow()
    }
}

impl<T: IdGenerator, A: IdAllocatorInner> PartialEq for IdHandle<T, A> {
    fn eq(&self, other: &Self) -> bool {
        self.unhandled() == other.unhandled()
    }
}
impl<T: IdGenerator, A: IdAllocatorInner> Eq for IdHandle<T, A> {}

impl<T: IdGenerator, A: IdAllocatorInner> PartialOrd for IdHandle<T, A> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: IdGenerator, A: IdAllocatorInner> Ord for IdHandle<T, A> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.unhandled().cmp(&other.unhandled())
    }
}

impl<T, A: IdAllocatorInner> Borrow<T> for IdHandle<T, A> {
    fn borrow(&self) -> &T {
        match self {
            IdHandle::Static(id) => id,
            IdHandle::Dynamic(inner) => &inner.id,
        }
    }
}

impl<T: IdGenerator, A: IdAllocatorInner> fmt::Display for IdHandle<T, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.unhandled().fmt(f)
    }
}

impl<T: IdGenerator, A: IdAllocatorInner> serde::Serialize for IdHandle<T, A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.unhandled().serialize(serializer)
    }
}

mod internal {
    use std::sync::Arc;

    use crate::cast::CastFrom;
    use crate::id_gen::{IdAllocator, IdAllocatorInner};

    #[derive(Debug)]
    pub struct IdHandleInner<T, A: IdAllocatorInner> {
        /// A handle to the [`IdAllocator`] used to allocated the provided id.
        pub(super) allocator: IdAllocator<A>,
        /// The actual ID that was allocated.
        pub(super) id: T,
        stored: u32,
    }

    impl<T, A: IdAllocatorInner> IdHandleInner<T, A>
    where
        T: CastFrom<u32>,
    {
        pub fn new(allocator: &IdAllocator<A>) -> Option<Self> {
            let stored = allocator.alloc_internal()?;
            Some(IdHandleInner {
                allocator: IdAllocator(Arc::clone(&allocator.0)),
                id: T::cast_from(stored),
                stored,
            })
        }
    }

    impl<T, A: IdAllocatorInner> Drop for IdHandleInner<T, A> {
        fn drop(&mut self) {
            // Release our ID for later re-use.
            self.allocator.free_internal(self.stored);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[mz_test_macro::test]
    fn test_id_gen() {
        test_ad_allocator::<IdAllocatorInnerHashSet>();
        test_ad_allocator::<IdAllocatorInnerBitSet>();
        test_ad_allocator::<IdAllocatorInnerRoaring>();
        test_ad_allocator::<IdAllocatorInnerRoaringLoop>();
        test_ad_allocator::<IdAllocatorInnerSparseBitSet>();
    }

    fn test_ad_allocator<A: IdAllocatorInner>() {
        test_id_alloc::<A>();
        test_static_id_sorting::<A>();
        test_id_reuse::<A>();
        test_display::<A>();
        test_map_lookup::<A>();
        test_serialization::<A>();
    }

    fn test_id_alloc<A: IdAllocatorInner>() {
        let ida = IdAllocator::<A>::new(3, 5);
        let id3 = ida.alloc().unwrap();
        let id4 = ida.alloc().unwrap();
        let id5 = ida.alloc().unwrap();
        assert_ne!(id3, id4);
        assert_ne!(id3, id5);
        assert_ne!(id4, id5);
        drop(id4);
        let _id4 = ida.alloc().unwrap();
        drop(id5);
        drop(id3);
        let _id5 = ida.alloc().unwrap();
        let _id3 = ida.alloc().unwrap();
        match ida.alloc() {
            Some(id) => panic!(
                "id allocator returned {}, not expected id exhaustion error",
                id
            ),
            None => (),
        }
    }

    fn test_static_id_sorting<A: IdAllocatorInner>() {
        let ida = IdAllocator::<A>::new(0, 0);
        let id0 = ida.alloc().unwrap();
        let id1 = IdHandle::Static(1);
        assert!(id0 < id1);

        let ida = IdAllocator::<A>::new(1, 1);
        let id0 = IdHandle::Static(0);
        let id1 = ida.alloc().unwrap();
        assert!(id0 < id1);
    }

    fn test_id_reuse<A: IdAllocatorInner>() {
        let allocator = IdAllocator::<A>::new(10, 11);

        let id_a = allocator.alloc().unwrap();
        let a = id_a.unhandled();
        let id_a_clone = id_a.clone();
        // a should not get freed.
        drop(id_a);

        // There are only two slots, so trying to allocate 2 more should fail the second time.
        let _id_b = allocator.alloc().unwrap();
        assert!(allocator.alloc().is_none());

        // a should get freed since all outstanding references have been dropped.
        drop(id_a_clone);

        // We should re-use a.
        let id_c = allocator.alloc().unwrap();
        assert_eq!(id_c.unhandled(), a);
    }

    fn test_display<A: IdAllocatorInner>() {
        let allocator = IdAllocator::<A>::new(65_000, 65_000);

        let id_a = allocator.alloc().unwrap();
        assert_eq!(id_a.unhandled(), 65_000);

        // An IdHandle should use the inner type's Display impl.
        let id_display = format!("{id_a}");
        let val_display = format!("{}", id_a.unhandled());

        assert_eq!(id_display, val_display);
    }

    fn test_map_lookup<A: IdAllocatorInner>() {
        let allocator = IdAllocator::<A>::new(99, 101);

        let id_a = allocator.alloc().unwrap();
        let a = id_a.unhandled();

        let mut btree = BTreeMap::new();
        btree.insert(id_a, "hello world");

        // We should be able to lookup an IdHandle, based on just the value.
        let entry = btree.remove(&a).unwrap();
        assert_eq!(entry, "hello world");

        assert!(btree.is_empty());
    }

    fn test_serialization<A: IdAllocatorInner>() {
        let allocator = IdAllocator::<A>::new(42, 42);

        let id_a = allocator.alloc().unwrap();
        assert_eq!(id_a.unhandled(), 42);

        // An IdHandle should serialize the same as the inner value.
        let id_json = serde_json::to_string(&id_a).unwrap();
        let val_json = serde_json::to_string(&id_a.unhandled()).unwrap();

        assert_eq!(id_json, val_json);
    }
}
