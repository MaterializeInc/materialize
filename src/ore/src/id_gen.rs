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
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::cast::CastFrom;

/// Manages the allocation of unique IDs.
#[derive(Debug, Clone)]
pub struct Gen<Id> {
    id: u64,
    phantom: PhantomData<Id>,
}

impl<Id> Default for Gen<Id> {
    fn default() -> Self {
        Self {
            id: 0,
            phantom: PhantomData,
        }
    }
}

impl<Id: From<u64>> Gen<Id> {
    /// Allocates a new identifier of type `Id` and advances the generator.
    pub fn allocate_id(&mut self) -> Id {
        let id = self.id;
        self.id += 1;
        id.into()
    }
}

/// A generator of u64-bit IDs.
pub type IdGen = Gen<u64>;

/// Manages the allocation of unique IDs.
///
/// Atomic version of `Gen`, for sharing between threads.
#[derive(Debug)]
pub struct AtomicGen<Id> {
    id: AtomicU64,
    phantom: PhantomData<Id>,
}

impl<Id> Default for AtomicGen<Id> {
    fn default() -> Self {
        Self {
            id: AtomicU64::new(0),
            phantom: PhantomData,
        }
    }
}

impl<Id: From<u64> + Default> AtomicGen<Id> {
    /// Allocates a new identifier of type `Id` and advances the generator.
    pub fn allocate_id(&self) -> Id {
        // The only purpose of the atomic here is to ensure every caller receives a distinct ID,
        // there are no requirements on the order in which IDs are produced and there is no other
        // state protected by this atomic. `Relaxed` ordering is therefore sufficient.
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        id.into()
    }
}

/// A generator of u64-bit IDs.
///
/// Atomic version of `IdGen`, for sharing between threads.
pub type AtomicIdGen = AtomicGen<u64>;

/// IdAllocator common traits.
pub trait IdGenerator:
    From<u8> + AddAssign + Sub + PartialOrd + Copy + Eq + Hash + Ord + serde::Serialize + fmt::Display
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
        + serde::Serialize
        + fmt::Display
{
}

/// Manages allocation of numeric IDs.
#[derive(Debug, Clone)]
pub struct IdAllocator<A: IdAllocatorInner>(pub Arc<Mutex<A>>);

/// Common trait for id allocators.
pub trait IdAllocatorInner: std::fmt::Debug + Send {
    /// Name of the allocator.
    const NAME: &'static str;
    /// Construct an allocator with the given range. Returned ids will be OR'd with `mask`. `mask`
    /// must not have any bits that could be set by a number <= `max`.
    fn new(min: u32, max: u32, mask: u32) -> Self;
    /// Allocate a new id.
    fn alloc(&mut self) -> Option<u32>;
    /// Deallocate a used id, making it available for reuse.
    fn remove(&mut self, id: u32);
}

/// IdAllocator using a HiBitSet.
#[derive(Debug, Clone)]
pub struct IdAllocatorInnerBitSet {
    next: StdRng,
    min: u32,
    max: u32,
    mask: u32,
    used: BitSet,
}

impl IdAllocatorInner for IdAllocatorInnerBitSet {
    const NAME: &'static str = "hibitset";

    fn new(min: u32, max: u32, mask: u32) -> Self {
        let total = usize::cast_from(max - min);
        assert!(total < BitSet::BITS_PER_USIZE.pow(4));
        IdAllocatorInnerBitSet {
            next: StdRng::from_entropy(),
            min,
            max,
            mask,
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
                assert!(
                    next & self.mask == 0,
                    "chosen ID must not intersect with mask:\n{:#034b}\n{:#034b}",
                    next,
                    self.mask
                );
                return Some(next | self.mask);
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
        let id = (!self.mask) & id;
        let stored = id - self.min;
        self.used.remove(stored);
    }
}

impl<A: IdAllocatorInner> IdAllocator<A> {
    /// Creates a new `IdAllocator` that will assign IDs between `min` and
    /// `max`, both inclusive.
    pub fn new(min: u32, max: u32, mask: u32) -> IdAllocator<A> {
        assert!(min <= max);
        if mask != 0 && max > 0 {
            // mask_check is all 1s in any bit set by all numbers >= max. Assert that the mask
            // doesn't share any bits with those.
            let mask_check = (1 << (max.ilog2() + 1)) - 1;
            assert_eq!(mask & mask_check, 0, "max and mask share bits");
        }
        let inner = A::new(min, max, mask);
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
    use std::fmt::Debug;
    use std::sync::Arc;

    use crate::cast::CastFrom;
    use crate::id_gen::{IdAllocator, IdAllocatorInner};

    pub struct IdHandleInner<T, A: IdAllocatorInner> {
        /// A handle to the [`IdAllocator`] used to allocated the provided id.
        pub(super) allocator: IdAllocator<A>,
        /// The actual ID that was allocated.
        pub(super) id: T,
        stored: u32,
    }

    impl<T: Debug, A: IdAllocatorInner> Debug for IdHandleInner<T, A> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("IdHandleInner")
                .field("id", &self.id)
                .field("stored", &self.stored)
                .finish_non_exhaustive()
        }
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

/// Number of bits the org id is offset into a connection id.
pub const ORG_ID_OFFSET: usize = 19;

/// Max (inclusive) connection id that can be produced.
pub const MAX_ORG_ID: u32 = (1 << ORG_ID_OFFSET) - 1;

/// Extracts the lower 12 bits from an org id. These are later used as the [31, 20] bits of a
/// connection id to help route cancellation requests.
pub fn org_id_conn_bits(uuid: &Uuid) -> u32 {
    let lower = uuid.as_u128();
    let lower = (lower & 0xFFF) << ORG_ID_OFFSET;
    let lower: u32 = lower.try_into().expect("must fit");
    lower
}

/// Returns the portion of the org's UUID present in connection id.
pub fn conn_id_org_uuid(conn_id: u32) -> String {
    const UPPER: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
    ];

    // Extract UUID from conn_id: upper 12 bits excluding the first.
    let orgid = usize::try_from((conn_id >> ORG_ID_OFFSET) & 0xFFF).expect("must cast");
    // Convert the bits into a 3 char string and inject into the resolver template.
    let mut dst = String::with_capacity(3);
    dst.push(UPPER[(orgid >> 8) & 0xf]);
    dst.push(UPPER[(orgid >> 4) & 0xf]);
    dst.push(UPPER[orgid & 0xf]);
    dst
}

/// Generate a random temporary ID.
///
/// Concretely we generate a UUIDv4 and return the last 12 characters for maximum uniqueness.
///
/// Note: the reason we use the last 12 characters is because the bits 6, 7, and 12 - 15
/// are all hard coded <https://www.rfc-editor.org/rfc/rfc4122#section-4.4>.
/// ```
/// use mz_ore::id_gen::temp_id;
///
/// let temp = temp_id();
/// assert_eq!(temp.len(), 12);
/// assert!(temp.is_ascii());
/// ```
pub fn temp_id() -> String {
    let temp_uuid = uuid::Uuid::new_v4().as_hyphenated().to_string();
    temp_uuid.chars().rev().take_while(|c| *c != '-').collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::assert_none;

    use super::*;

    #[crate::test]
    fn test_conn_org() {
        let uuid = Uuid::parse_str("9e37ec59-56f4-450a-acbd-18ff14f10ca8").unwrap();
        let lower = org_id_conn_bits(&uuid);
        let org_lower_uuid = conn_id_org_uuid(lower);
        assert_eq!(org_lower_uuid, "CA8");
    }

    #[crate::test]
    fn test_id_gen() {
        test_ad_allocator::<IdAllocatorInnerBitSet>();
    }

    // Test masks and maxs that intersect panic.
    #[crate::test]
    #[should_panic]
    fn test_mask_intersect<A: IdAllocatorInner>() {
        let env_lower = org_id_conn_bits(&uuid::Uuid::from_u128(u128::MAX));
        let ida = IdAllocator::<IdAllocatorInnerBitSet>::new(
            1 << ORG_ID_OFFSET,
            1 << ORG_ID_OFFSET,
            env_lower,
        );
        let id = ida.alloc().unwrap();
        assert_eq!(id.unhandled(), (0xfff << ORG_ID_OFFSET) | MAX_ORG_ID);
    }

    fn test_ad_allocator<A: IdAllocatorInner>() {
        test_id_alloc::<A>();
        test_static_id_sorting::<A>();
        test_id_reuse::<A>();
        test_display::<A>();
        test_map_lookup::<A>();
        test_serialization::<A>();
        test_mask::<A>();
        test_mask_envd::<A>();
    }

    fn test_mask<A: IdAllocatorInner>() {
        let ida = IdAllocator::<A>::new(1, 1, 0xfff << 20);
        let id = ida.alloc().unwrap();
        assert_eq!(id.unhandled(), (0xfff << 20) | 1);
    }

    // Test that the random conn id and and uuid each with all bits set don't intersect.
    fn test_mask_envd<A: IdAllocatorInner>() {
        let env_lower = org_id_conn_bits(&uuid::Uuid::from_u128(u128::MAX));
        let ida = IdAllocator::<A>::new(MAX_ORG_ID, MAX_ORG_ID, env_lower);
        let id = ida.alloc().unwrap();
        assert_eq!(id.unhandled(), (0xfff << ORG_ID_OFFSET) | MAX_ORG_ID);
    }

    fn test_id_alloc<A: IdAllocatorInner>() {
        let ida = IdAllocator::<A>::new(3, 5, 0);
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
        let ida = IdAllocator::<A>::new(0, 0, 0);
        let id0 = ida.alloc().unwrap();
        let id1 = IdHandle::Static(1);
        assert!(id0 < id1);

        let ida = IdAllocator::<A>::new(1, 1, 0);
        let id0 = IdHandle::Static(0);
        let id1 = ida.alloc().unwrap();
        assert!(id0 < id1);
    }

    fn test_id_reuse<A: IdAllocatorInner>() {
        let allocator = IdAllocator::<A>::new(10, 11, 0);

        let id_a = allocator.alloc().unwrap();
        let a = id_a.unhandled();
        let id_a_clone = id_a.clone();
        // a should not get freed.
        drop(id_a);

        // There are only two slots, so trying to allocate 2 more should fail the second time.
        let _id_b = allocator.alloc().unwrap();
        assert_none!(allocator.alloc());

        // a should get freed since all outstanding references have been dropped.
        drop(id_a_clone);

        // We should re-use a.
        let id_c = allocator.alloc().unwrap();
        assert_eq!(id_c.unhandled(), a);
    }

    fn test_display<A: IdAllocatorInner>() {
        let allocator = IdAllocator::<A>::new(65_000, 65_000, 0);

        let id_a = allocator.alloc().unwrap();
        assert_eq!(id_a.unhandled(), 65_000);

        // An IdHandle should use the inner type's Display impl.
        let id_display = format!("{id_a}");
        let val_display = format!("{}", id_a.unhandled());

        assert_eq!(id_display, val_display);
    }

    fn test_map_lookup<A: IdAllocatorInner>() {
        let allocator = IdAllocator::<A>::new(99, 101, 0);

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
        let allocator = IdAllocator::<A>::new(42, 42, 0);

        let id_a = allocator.alloc().unwrap();
        assert_eq!(id_a.unhandled(), 42);

        // An IdHandle should serialize the same as the inner value.
        let id_json = serde_json::to_string(&id_a).unwrap();
        let val_json = serde_json::to_string(&id_a.unhandled()).unwrap();

        assert_eq!(id_json, val_json);
    }
}
