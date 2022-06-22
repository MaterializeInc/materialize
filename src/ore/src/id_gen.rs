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

use std::collections::{BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::ops::AddAssign;
use std::sync::Mutex;

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
}

impl<T> IdAllocator<T>
where
    T: From<u8> + AddAssign + PartialOrd + Copy,
{
    /// Creates a new `IdAllocator` that will assign IDs between `min` and
    /// `max`, both inclusive.
    pub fn new(min: T, max: T) -> IdAllocator<T> {
        IdAllocator(Mutex::new(IdAllocatorInner {
            next: min,
            max,
            free: VecDeque::new(),
        }))
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

    /// Releases a new ID back to the pool.
    ///
    /// It is undefined behavior to free an ID twice, or to free an ID that was
    /// not allocated by this allocator.
    pub fn free(&self, id: T) {
        let mut inner = self.0.lock().expect("lock poisoned");
        inner.free.push_back(id);
    }
}

/// Manages allocation of process ports. Similar to `IdAllocator` but specific to
/// the allocation of ports.
///
/// Note that the current implementation is fairly memory inefficient.
#[derive(Debug)]
pub struct PortAllocator(Mutex<BTreeSet<u16>>);

impl PortAllocator {
    /// Creates a new `PortAllocator` that will assign ports between `min` and
    /// `max`, both inclusive.
    pub fn new(min: u16, max: u16) -> PortAllocator {
        PortAllocator(Mutex::new((min..=max).collect()))
    }

    /// Creates a new `PortAllocator` that will assign ports between `min` and
    /// `max`, both inclusive.
    ///
    /// The ports listed in `banned` will not be assigned.
    pub fn new_with_filter(min: u16, max: u16, banned: &[u16]) -> PortAllocator {
        PortAllocator(Mutex::new(
            (min..=max).filter(|p| !banned.contains(p)).collect(),
        ))
    }

    /// Allocates a new port.
    ///
    /// Returns `None` if the allocator is exhausted.
    pub fn alloc(&self) -> Option<u16> {
        let mut inner = self.0.lock().expect("lock poisoned");
        let port = inner.iter().next().cloned();
        if let Some(port) = port {
            assert!(inner.remove(&port));
        }
        port
    }

    /// Releases a new port back to the pool.
    ///
    /// It is undefined behavior to free an port twice, or to free an port that was
    /// not allocated by this allocator.
    pub fn free(&self, id: u16) {
        let mut inner = self.0.lock().expect("lock poisoned");
        let _ = inner.insert(id);
    }

    /// Marks a port as already allocated.
    ///
    /// Returns false if port was previously allocated and true otherwise.
    pub fn mark_allocated(&self, port: u16) -> bool {
        let mut inner = self.0.lock().expect("lock poisoned");
        inner.remove(&port)
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
    fn test_port_allocator() {
        let ida = PortAllocator::new(1, 10);
        assert!(ida.mark_allocated(4));

        while let Some(id) = ida.alloc() {
            assert_ne!(4, id);
        }

        ida.free(3);
        assert_eq!(3, ida.alloc().unwrap());
        assert!(ida.alloc().is_none());

        ida.free(4);
        assert_eq!(4, ida.alloc().unwrap());
        assert!(ida.alloc().is_none());

        assert!(!ida.mark_allocated(9));
    }
}
