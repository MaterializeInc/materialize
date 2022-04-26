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

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::AddAssign;
use std::sync::{Mutex, MutexGuard};

/// Manages the allocation of unique IDs.
#[derive(Debug, Default)]
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
#[derive(Debug)]
pub struct IdAllocator<T>(Mutex<IdAllocatorInner<T>>);

#[derive(Debug)]
struct IdAllocatorInner<T> {
    next: T,
    max: T,
    free: VecDeque<T>,
    allocated: HashSet<T>,
}

impl<T> IdAllocator<T>
where
    T: From<u8> + AddAssign + PartialOrd + Copy + Eq + Hash,
{
    /// Creates a new `IdAllocator` that will assign IDs between `min` and
    /// `max`, both inclusive.
    pub fn new(min: T, max: T) -> IdAllocator<T> {
        IdAllocator(Mutex::new(IdAllocatorInner {
            next: min,
            max,
            free: VecDeque::new(),
            allocated: HashSet::new(),
        }))
    }

    /// Allocates a new ID.
    ///
    /// Returns `None` if the allocator is exhausted.
    pub fn alloc(&self) -> Option<T> {
        let mut inner = self.0.lock().expect("lock poisoned");
        while let Some(id) = self.alloc_inner(&mut inner) {
            if !inner.allocated.contains(&id) {
                return Some(id);
            }
        }

        None
    }

    fn alloc_inner(&self, inner: &mut MutexGuard<IdAllocatorInner<T>>) -> Option<T> {
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

    /// Releases a new ID back to the pool.
    ///
    /// It is undefined behavior to free an ID twice, or to free an ID that was
    /// not allocated by this allocator.
    pub fn free(&self, id: T) {
        let mut inner = self.0.lock().expect("lock poisoned");
        inner.free.push_back(id);
        let _ = inner.allocated.remove(&id);
    }

    /// Marks an ID as allocated and prevents `IdAllocator` from allocating it
    /// in the future.
    pub fn mark_allocated(&self, id: T) {
        let mut inner = self.0.lock().expect("lock poisoned");
        inner.allocated.insert(id);
    }
}

#[cfg(test)]
mod tests {
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
    fn test_mark_allocated() {
        let ida = IdAllocator::new(1, 10);
        ida.mark_allocated(4);

        while let Some(id) = ida.alloc() {
            assert_ne!(4, id);
        }

        ida.free(3);
        assert_eq!(3, ida.alloc().unwrap());
        assert!(ida.alloc().is_none());

        ida.free(4);
        assert_eq!(4, ida.alloc().unwrap());
        assert!(ida.alloc().is_none());
    }
}
