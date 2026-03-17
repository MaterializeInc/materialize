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
//!
//! Stack traces are interned: allocations from the same call site share a
//! single copy of the trace, identified by a `StackId`.

use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use std::collections::BTreeMap;

pub use lgalloc::AllocError;

/// Total bytes currently allocated via lgalloc.
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
/// Number of live lgalloc allocations.
static LIVE_COUNT: AtomicUsize = AtomicUsize::new(0);
/// Monotonically increasing allocation ID.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// All profiling state, protected by a single mutex.
static STATE: Mutex<ProfileState> = Mutex::new(ProfileState::new());

/// Interned stack trace identifier.
type StackId = u64;

struct ProfileState {
    /// Interned stack traces: stack ID -> (addresses, refcount).
    stacks: BTreeMap<StackId, InternedStack>,
    /// Map from stack trace content to its ID, for deduplication.
    stack_index: BTreeMap<Vec<usize>, StackId>,
    /// Live allocations: allocation ID -> (stack ID, capacity bytes).
    allocations: BTreeMap<u64, (StackId, usize)>,
    /// Next stack ID.
    next_stack_id: StackId,
}

struct InternedStack {
    addrs: Vec<usize>,
    refcount: usize,
}

impl ProfileState {
    const fn new() -> Self {
        Self {
            stacks: BTreeMap::new(),
            stack_index: BTreeMap::new(),
            allocations: BTreeMap::new(),
            next_stack_id: 0,
        }
    }

    /// Intern a stack trace, returning its ID. Increments the refcount if
    /// already interned.
    fn intern_stack(&mut self, addrs: Vec<usize>) -> StackId {
        if let Some(&id) = self.stack_index.get(&addrs) {
            self.stacks.get_mut(&id).expect("consistent").refcount += 1;
            return id;
        }
        let id = self.next_stack_id;
        self.next_stack_id += 1;
        self.stack_index.insert(addrs.clone(), id);
        self.stacks.insert(
            id,
            InternedStack {
                addrs,
                refcount: 1,
            },
        );
        id
    }

    /// Decrement refcount for a stack trace, removing it if no longer referenced.
    fn release_stack(&mut self, stack_id: StackId) {
        let entry = self.stacks.get_mut(&stack_id).expect("consistent");
        entry.refcount -= 1;
        if entry.refcount == 0 {
            let removed = self.stacks.remove(&stack_id).expect("consistent");
            self.stack_index.remove(&removed.addrs);
        }
    }

    fn insert(&mut self, alloc_id: u64, addrs: Vec<usize>, capacity_bytes: usize) {
        let stack_id = self.intern_stack(addrs);
        self.allocations.insert(alloc_id, (stack_id, capacity_bytes));
    }

    fn remove(&mut self, alloc_id: u64) {
        if let Some((stack_id, _)) = self.allocations.remove(&alloc_id) {
            self.release_stack(stack_id);
        }
    }
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
    let state = STATE.lock().expect("poisoned");
    // Aggregate bytes per stack ID.
    let mut by_stack: BTreeMap<StackId, usize> = BTreeMap::new();
    for &(stack_id, capacity_bytes) in state.allocations.values() {
        *by_stack.entry(stack_id).or_default() += capacity_bytes;
    }
    // Resolve stack IDs to addresses.
    by_stack
        .into_iter()
        .map(|(stack_id, bytes)| {
            let addrs = &state.stacks.get(&stack_id).expect("consistent").addrs;
            (addrs.clone(), bytes)
        })
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
    STATE.lock().expect("poisoned").insert(id, addrs, bytes);

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
    STATE.lock().expect("poisoned").remove(handle.id);
    #[allow(clippy::disallowed_methods)]
    lgalloc::deallocate(handle.inner);
}
