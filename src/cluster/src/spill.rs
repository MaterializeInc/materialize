// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! lgalloc-backed spill strategy for the timely communication merge queue.
//!
//! Builds a [`SpillPolicyFn`] that produces matched writer/reader policy pairs.
//! "Spilling" copies the queued [`Bytes`] out of the zero-copy slab into a
//! fresh lgalloc 0.7 allocation (with a heap fallback), releasing the slab
//! back to its pool. The reader side prefetches up to a configured budget back
//! into the in-memory queue.
//!
//! lgalloc 0.7 is intentionally separate from the lgalloc 0.6 used by the
//! slab refill in `crate::client`, so the two callers can evolve independently.

use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use timely::bytes::arc::{Bytes, BytesMut};
use timely::communication::allocator::zero_copy::spill::{
    BytesFetch, BytesSpill, PrefetchPolicy, SpillPolicy, SpillPolicyFn, Threshold,
};

/// Configuration for lgalloc-backed merge queue spilling.
#[derive(Clone, Debug)]
pub struct SpillConfig {
    /// Per-queue byte threshold above which the writer spills out of the slab.
    pub threshold_bytes: usize,
    /// Bytes kept in memory at the head / prefetch budget on the reader side.
    pub head_reserve_bytes: usize,
}

impl SpillConfig {
    /// Build a [`SpillPolicyFn`] that produces a matched writer/reader policy pair per queue.
    pub fn into_policy_fn(self) -> SpillPolicyFn {
        let threshold_bytes = self.threshold_bytes;
        let head_reserve_bytes = self.head_reserve_bytes;
        Arc::new(move || {
            let strategy: Box<dyn BytesSpill> = Box::new(LgallocSpillStrategy);
            let mut tp = Threshold::new(strategy);
            tp.threshold_bytes = threshold_bytes;
            tp.head_reserve_bytes = head_reserve_bytes;
            let writer: Box<dyn SpillPolicy> = Box::new(tp);
            let reader: Box<dyn SpillPolicy> = Box::new(PrefetchPolicy::new(head_reserve_bytes));
            (writer, reader)
        })
    }
}

/// lgalloc 0.7-backed [`BytesSpill`] implementation.
///
/// Each spilled chunk is copied into its own lgalloc allocation (with a heap
/// fallback if lgalloc cannot service the size), releasing the originating
/// slab. The fetch handle hands the chunk back as a fresh [`Bytes`].
struct LgallocSpillStrategy;

impl BytesSpill for LgallocSpillStrategy {
    fn spill(&mut self, chunks: &mut Vec<Bytes>, handles: &mut Vec<Box<dyn BytesFetch>>) {
        if chunks.is_empty() {
            return;
        }
        handles.reserve(chunks.len());
        for chunk in chunks.drain(..) {
            let chunk_len = chunk.len();
            let mut buf = Lgalloc07Handle::with_capacity(chunk_len);
            buf[..chunk_len].copy_from_slice(&chunk[..]);
            // Wrap the (possibly larger) lgalloc buffer and split off exactly the chunk bytes.
            let mut bm = BytesMut::from(buf);
            let bytes = bm.extract_to(chunk_len);
            handles.push(Box::new(LgallocHandleEntry {
                bytes: Mutex::new(Some(bytes)),
            }));
        }
    }
}

struct LgallocHandleEntry {
    bytes: Mutex<Option<Bytes>>,
}

impl BytesFetch for LgallocHandleEntry {
    fn fetch(self: Box<Self>) -> Result<Vec<Bytes>, Box<dyn BytesFetch>> {
        let bytes = self
            .bytes
            .lock()
            .expect("spill handle poisoned")
            .take()
            .expect("spill handle fetched twice");
        Ok(vec![bytes])
    }
}

/// A handle to memory allocated by lgalloc 0.7, with a heap fallback.
///
/// Mirrors the lgalloc-0.6 `LgallocHandle` in `crate::client::alloc`, but uses
/// the separately-versioned lgalloc crate so the two callers don't share state.
struct Lgalloc07Handle {
    /// lgalloc handle, set if the memory was allocated by lgalloc.
    handle: Option<lgalloc_07::Handle>,
    /// Pointer to the allocated memory. Always well-aligned, but can be dangling.
    pointer: NonNull<u8>,
    /// Capacity of the allocated memory in bytes.
    capacity: usize,
}

// SAFETY: `Lgalloc07Handle` owns its allocation; the raw pointer is never
// shared and is freed in `Drop`. lgalloc handles are themselves `Send`.
unsafe impl Send for Lgalloc07Handle {}

impl Lgalloc07Handle {
    fn with_capacity(size: usize) -> Self {
        match lgalloc_07::allocate::<u8>(size) {
            Ok((pointer, capacity, handle)) => Self {
                handle: Some(handle),
                pointer,
                capacity,
            },
            Err(_) => {
                let mut alloc = vec![0_u8; size];
                alloc.shrink_to_fit();
                let pointer = NonNull::new(alloc.as_mut_ptr()).expect("non-null");
                let capacity = alloc.capacity();
                std::mem::forget(alloc);
                Self {
                    handle: None,
                    pointer,
                    capacity,
                }
            }
        }
    }
}

impl Deref for Lgalloc07Handle {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.capacity) }
    }
}

impl DerefMut for Lgalloc07Handle {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.pointer.as_ptr(), self.capacity) }
    }
}

impl Drop for Lgalloc07Handle {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            lgalloc_07::deallocate(handle);
        } else {
            unsafe { Vec::from_raw_parts(self.pointer.as_ptr(), 0, self.capacity) };
        }
        self.pointer = NonNull::dangling();
        self.capacity = 0;
    }
}
