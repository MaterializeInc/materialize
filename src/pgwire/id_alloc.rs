// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Manages allocation of u32 IDs.
///
/// Note that the current implementation wastes memory. It would be far more
/// efficient to use a compressed bitmap, like https://roaringbitmap.org or
/// the hibitset crate, but neither presently supports a fast "find first zero"
/// operation.
#[derive(Debug, Clone)]
pub struct IdAllocator(Arc<Mutex<IdAllocatorInner>>);

#[derive(Debug, Clone)]
struct IdAllocatorInner {
    next: u32,
    max: u32,
    free: VecDeque<u32>,
}

impl IdAllocator {
    /// Creates a new `IdAllocator` that will assign IDs between `min` and
    /// `max`, both inclusive.
    pub fn new(min: u32, max: u32) -> IdAllocator {
        IdAllocator(Arc::new(Mutex::new(IdAllocatorInner {
            next: min,
            max,
            free: VecDeque::new(),
        })))
    }

    /// Allocates a new ID.
    pub fn alloc(&self) -> Result<u32, IdExhaustionError> {
        let mut inner = self.0.lock().expect("lock poisoned");
        if let Some(id) = inner.free.pop_front() {
            Ok(id)
        } else {
            let id = inner.next;
            if id > inner.max {
                Err(IdExhaustionError)
            } else {
                inner.next += 1;
                Ok(id)
            }
        }
    }

    /// Releases a new ID back to the pool.
    ///
    /// It is undefined behavior to free an ID twice, or to free an ID that was
    /// not allocated by this allocator.
    pub fn free(&self, id: u32) {
        let mut inner = self.0.lock().expect("lock poisoned");
        inner.free.push_back(id);
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct IdExhaustionError;

impl fmt::Display for IdExhaustionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("id allocator exhausted all valid ids")
    }
}

impl Error for IdExhaustionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_alloc() {
        let ida = IdAllocator::new(3, 5);
        assert_eq!(ida.alloc(), Ok(3));
        assert_eq!(ida.alloc(), Ok(4));
        assert_eq!(ida.alloc(), Ok(5));
        ida.free(4);
        assert_eq!(ida.alloc(), Ok(4));
        ida.free(5);
        ida.free(3);
        assert_eq!(ida.alloc(), Ok(5));
        assert_eq!(ida.alloc(), Ok(3));
        match ida.alloc() {
            Ok(id) => panic!(
                "id allocator returned {}, not expected id exhaution error",
                id
            ),
            Err(IdExhaustionError) => (),
        }
    }
}
