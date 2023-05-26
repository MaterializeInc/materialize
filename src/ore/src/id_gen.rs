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

use std::borrow::Borrow;
use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
use std::ops::AddAssign;
use std::sync::{Arc, Mutex};

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

/// Manages allocation of numeric IDs.
///
/// Note that the current implementation wastes memory. It would be far more
/// efficient to use a compressed bitmap, like <https://roaringbitmap.org> or
/// the hibitset crate, but neither presently supports a fast "find first zero"
/// operation.
#[derive(Debug, Clone)]
pub struct IdAllocator<T>(Arc<Mutex<IdAllocatorInner<T>>>);

#[derive(Debug)]
struct IdAllocatorInner<T> {
    next: T,
    max: T,
    free: VecDeque<T>,
}

impl<T> IdAllocator<T>
where
    T: From<u8> + AddAssign + PartialOrd + Copy,
{
    /// Creates a new `IdAllocator` that will assign IDs between `min` and
    /// `max`, both inclusive.
    pub fn new(min: T, max: T) -> IdAllocator<T> {
        let inner = IdAllocatorInner {
            next: min,
            max,
            free: VecDeque::new(),
        };
        IdAllocator(Arc::new(Mutex::new(inner)))
    }

    /// Allocates a new ID.
    ///
    /// Returns `None` if the allocator is exhausted.
    pub fn alloc(&self) -> Option<T> {
        let mut inner = self.0.lock().expect("lock poisoned");
        if let Some(id) = inner.free.pop_front() {
            Some(id)
        } else {
            let id = inner.next;
            if id > inner.max {
                None
            } else {
                inner.next += 1.into();
                Some(id)
            }
        }
    }

    /// Allocates a new _owned_ ID.
    ///
    /// The ID associated with the [`IdHandle`] will be freed when all of the referencing
    /// [`IdHandle`]s have been dropped.
    pub fn alloc_owned(&self) -> Option<IdHandle<T>> {
        IdHandle::new(self)
    }

    /// Releases a new ID back to the pool.
    ///
    /// It is undefined behavior to free an ID twice, or to free an ID that was
    /// not allocated by this allocator.
    pub fn free(&self, id: T) {
        let mut inner = self.0.lock().expect("lock poisoned");
        inner.free.push_back(id);
    }
}

/// A clone-able owned reference to an ID.
///
/// Once all of the [`IdHandle`]s referencing an ID have been dropped, we will then free the ID
/// for later re-use.
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub enum IdHandle<T: From<u8> + AddAssign + PartialOrd + Copy> {
    /// An ID "allocated" a compile time.
    Static(T),
    /// An ID allocated at runtime, get's freed once all handles have been dropped.
    Dynamic(Arc<internal::IdHandleInner<T>>),
}

impl<T> IdHandle<T>
where
    T: From<u8> + AddAssign + PartialOrd + Copy,
{
    /// Creates a new [`IdHandle`] with the provided ID.
    ///
    /// Note: It is __entirely__ up to the caller to make sure the provided ID is not already being
    /// used.
    pub const fn new_static(id: T) -> Self {
        IdHandle::Static(id)
    }

    /// Returns the ID associated with this [`IdHandle`].
    pub fn val(&self) -> T {
        match self {
            IdHandle::Static(id) => *id,
            IdHandle::Dynamic(inner) => *inner.id(),
        }
    }

    /// Allocates a new ID and returns an owned [`IdHandle`] that can be cloned.
    fn new(allocator: &IdAllocator<T>) -> Option<Self> {
        let inner = Arc::new(internal::IdHandleInner::new(allocator)?);
        Some(IdHandle::Dynamic(inner))
    }
}

impl<T> Borrow<T> for IdHandle<T>
where
    T: From<u8> + AddAssign + PartialOrd + Copy,
{
    fn borrow(&self) -> &T {
        match self {
            IdHandle::Static(id) => id,
            IdHandle::Dynamic(inner) => inner.id(),
        }
    }
}

impl<T> fmt::Display for IdHandle<T>
where
    T: fmt::Display + From<u8> + AddAssign + PartialOrd + Copy,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.val().fmt(f)
    }
}

#[cfg(feature = "serde")]
impl<T> serde::Serialize for IdHandle<T>
where
    T: serde::Serialize + From<u8> + AddAssign + PartialOrd + Copy,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.val().serialize(serializer)
    }
}

mod internal {
    use std::ops::AddAssign;

    use crate::id_gen::IdAllocator;

    #[derive(Debug)]
    pub struct IdHandleInner<T>
    where
        T: From<u8> + AddAssign + PartialOrd + Copy,
    {
        /// A handle to the [`IdAllocator`] used to allocated the provided id.
        allocator: IdAllocator<T>,
        /// The actual ID that was allocated.
        id: T,
    }

    impl<T> IdHandleInner<T>
    where
        T: From<u8> + AddAssign + PartialOrd + Copy,
    {
        pub fn new(allocator: &IdAllocator<T>) -> Option<Self> {
            let id = allocator.alloc()?;
            Some(IdHandleInner {
                allocator: allocator.clone(),
                id,
            })
        }

        pub fn id(&self) -> &T {
            &self.id
        }
    }

    impl<T> PartialEq for IdHandleInner<T>
    where
        T: PartialEq + From<u8> + AddAssign + PartialOrd + Copy,
    {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }
    impl<T> Eq for IdHandleInner<T> where T: PartialEq + From<u8> + AddAssign + PartialOrd + Copy {}

    impl<T> PartialOrd for IdHandleInner<T>
    where
        T: PartialOrd + From<u8> + AddAssign + Copy,
    {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.id.partial_cmp(&other.id)
        }
    }

    impl<T> Ord for IdHandleInner<T>
    where
        T: Ord + From<u8> + AddAssign + PartialOrd + Copy,
    {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.id.cmp(&other.id)
        }
    }

    impl<T> Drop for IdHandleInner<T>
    where
        T: From<u8> + AddAssign + PartialOrd + Copy,
    {
        fn drop(&mut self) {
            // Release our ID for later re-use.
            self.allocator.free(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_id_alloc() {
        let ida = IdAllocator::new(3, 5);
        assert_eq!(ida.alloc().unwrap(), 3);
        assert_eq!(ida.alloc().unwrap(), 4);
        assert_eq!(ida.alloc().unwrap(), 5);
        ida.free(4);
        assert_eq!(ida.alloc().unwrap(), 4);
        ida.free(5);
        ida.free(3);
        assert_eq!(ida.alloc().unwrap(), 5);
        assert_eq!(ida.alloc().unwrap(), 3);
        match ida.alloc() {
            Some(id) => panic!(
                "id allocator returned {}, not expected id exhaustion error",
                id
            ),
            None => (),
        }
    }

    #[test]
    fn test_id_reuse() {
        let allocator = IdAllocator::new(10, 13);

        let id_a = allocator.alloc_owned().unwrap();
        assert_eq!(id_a.val(), 10);

        let id_a_clone = id_a.clone();
        assert_eq!(id_a_clone.val(), 10);

        // 10 should not get freed.
        drop(id_a);

        let id_b = allocator.alloc_owned().unwrap();
        assert_eq!(id_b.val(), 11);

        // 10 should get freed since all outstanding references have been dropped.
        drop(id_a_clone);

        // We should re-use 10.
        let id_c = allocator.alloc_owned().unwrap();
        assert_eq!(id_c.val(), 10);
    }

    #[test]
    fn test_display() {
        let allocator = IdAllocator::<u32>::new(65_000, 65_101);

        let id_a = allocator.alloc_owned().unwrap();
        assert_eq!(id_a.val(), 65_000);

        // An IdHandle should use the inner type's Display impl.
        let id_display = format!("{id_a}");
        let val_display = format!("{}", id_a.val());

        assert_eq!(id_display, val_display);
    }

    #[test]
    fn test_map_lookup() {
        let allocator = IdAllocator::<u32>::new(99, 101);

        let id_a = allocator.alloc_owned().unwrap();
        assert_eq!(id_a.val(), 99);

        let mut btree = BTreeMap::new();
        btree.insert(id_a, "hello world");

        // We should be able to lookup an IdHandle, based on just the value.
        let entry = btree.remove(&99).unwrap();
        assert_eq!(entry, "hello world");

        assert!(btree.is_empty());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serialization() {
        let allocator = IdAllocator::<u32>::new(42, 43);

        let id_a = allocator.alloc_owned().unwrap();
        assert_eq!(id_a.val(), 42);

        // An IdHandle should serialize the same as the inner value.
        let id_json = serde_json::to_string(&id_a).unwrap();
        let val_json = serde_json::to_string(&id_a.val()).unwrap();

        assert_eq!(id_json, val_json);
    }
}
