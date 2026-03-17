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

//! Wrappers around [`lgalloc`] that add application-level memory profiling.
//!
//! All lgalloc allocations and deallocations should go through this module
//! instead of calling the `lgalloc` crate directly. This allows us to track
//! how much memory the application currently holds via lgalloc, including
//! stack traces at each allocation site for heap profiling.

use std::collections::BTreeMap;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub use lgalloc::AllocError;

/// Total bytes currently allocated via lgalloc.
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
/// Number of live lgalloc allocations.
static LIVE_COUNT: AtomicUsize = AtomicUsize::new(0);
/// Monotonically increasing allocation ID.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);
/// Live allocations with their stack traces for heap profiling.
static LIVE_ALLOCATIONS: Mutex<BTreeMap<u64, AllocationRecord>> = Mutex::new(BTreeMap::new());

/// A record of a live lgalloc allocation.
struct AllocationRecord {
    /// Stack trace at the time of allocation (instruction pointer addresses).
    backtrace: Vec<usize>,
    /// Size of the allocation in bytes.
    capacity_bytes: usize,
}

/// Application-level lgalloc memory stats, tracking live allocations made
/// through this module's [`allocate`] and [`deallocate`] functions.
#[derive(Debug, Clone, Copy)]
pub struct LgAllocStats {
    /// Total bytes currently allocated via lgalloc.
    pub live_bytes: usize,
    /// Number of live lgalloc allocations.
    pub live_count: usize,
}

/// Returns application-level lgalloc memory stats.
pub fn stats() -> LgAllocStats {
    LgAllocStats {
        live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
        live_count: LIVE_COUNT.load(Ordering::Relaxed),
    }
}

/// Returns the current lgalloc heap profile as aggregated (stack_addresses, total_bytes) pairs.
///
/// Allocations sharing the same stack trace are grouped together with their byte counts summed.
pub fn heap_profile() -> Vec<(Vec<usize>, usize)> {
    let allocs = LIVE_ALLOCATIONS.lock().expect("poisoned");
    let mut by_stack: BTreeMap<&[usize], usize> = BTreeMap::new();
    for record in allocs.values() {
        *by_stack.entry(&record.backtrace).or_default() += record.capacity_bytes;
    }
    by_stack
        .into_iter()
        .map(|(addrs, bytes)| (addrs.to_vec(), bytes))
        .collect()
}

/// An lgalloc handle that tracks its allocation for profiling.
///
/// Must be passed to [`deallocate`] to free the memory and update
/// the global profiling counters.
pub struct Handle {
    inner: lgalloc::Handle,
    id: u64,
    capacity_bytes: usize,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("id", &self.id)
            .field("capacity_bytes", &self.capacity_bytes)
            .finish_non_exhaustive()
    }
}

/// Allocate memory via lgalloc, tracking the allocation in global profiling counters.
///
/// Captures a backtrace at the call site for heap profiling.
// TODO: Add a feature flag to enable/disable backtrace capture at runtime,
// so that the profiling overhead can be avoided when not needed.
pub fn allocate<T>(capacity: usize) -> Result<(NonNull<T>, usize, Handle), AllocError> {
    #[allow(clippy::disallowed_methods)]
    let (ptr, actual_capacity, handle) = lgalloc::allocate::<T>(capacity)?;
    let bytes = actual_capacity * std::mem::size_of::<T>();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    // Capture backtrace for heap profiling.
    let mut addrs = Vec::new();
    backtrace::trace(|frame| {
        addrs.push(frame.ip().addr());
        true
    });

    LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed);
    LIVE_COUNT.fetch_add(1, Ordering::Relaxed);
    LIVE_ALLOCATIONS.lock().expect("poisoned").insert(
        id,
        AllocationRecord {
            backtrace: addrs,
            capacity_bytes: bytes,
        },
    );

    Ok((
        ptr,
        actual_capacity,
        Handle {
            inner: handle,
            id,
            capacity_bytes: bytes,
        },
    ))
}

/// Deallocate lgalloc memory and update profiling counters.
pub fn deallocate(handle: Handle) {
    LIVE_BYTES.fetch_sub(handle.capacity_bytes, Ordering::Relaxed);
    LIVE_COUNT.fetch_sub(1, Ordering::Relaxed);
    LIVE_ALLOCATIONS
        .lock()
        .expect("poisoned")
        .remove(&handle.id);
    #[allow(clippy::disallowed_methods)]
    lgalloc::deallocate(handle.inner);
}
