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

//! Sampling allocation tracker, independent of the underlying allocator.
//!
//! [`TrackingAlloc`] wraps any [`GlobalAlloc`] and reports heap events to
//! [`on_alloc`] and [`on_free`]. The buffer pool ([`crate::pool`]) reports
//! its slot and extent allocations through the same hooks, so one
//! [`snapshot`] attributes heap and pool memory to call stacks.
//!
//! Allocations are sampled as a Poisson process over allocated bytes with
//! mean [`sample_interval`] (default 512 KiB, jemalloc's
//! `lg_prof_sample:19`): an allocation of `s` bytes is sampled with
//! probability `1 - exp(-s / interval)`, and each sample is weighted by the
//! inverse of that probability, so reported counts and bytes are unbiased
//! estimates. An unsampled allocation costs two relaxed loads and a
//! thread-local decrement. The per-free cost is two
//! dependent loads into a sparse address filter (see `filter`), which only
//! sends frees of addresses near a live sample to the locked registry.
//!
//! With [`set_track_frees`] enabled, freeing a sampled allocation also
//! captures the freeing stack, and [`Snapshot::freed`] aggregates freed
//! bytes and lifetimes by (allocation site, free site) pair.
//!
//! NOTE: The tracker allocates its own metadata through the global
//! allocator. A thread-local reentrancy flag routes those allocations past
//! the sampler and their frees past the registry, which is what keeps the
//! registry's locks from being reentered. The flag is sound only because the
//! tracker never frees memory it did not allocate while the flag was set.

mod filter;
mod registry;
#[cfg(test)]
mod tests;

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::cast::{CastFrom, CastLossy};

pub use registry::{FreeSite, Site, Snapshot, reset_history, snapshot};

/// The kind of memory an event concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Space {
    /// Allocations through the global allocator.
    Heap,
    /// Buffer pool slots, sized by their payload.
    PoolSlot,
    /// Buffer pool extent arena slots, sized by their stored length.
    PoolExtent,
}

impl Space {
    /// A short stable name, for use as a profile annotation.
    pub fn name(&self) -> &'static str {
        match self {
            Space::Heap => "heap",
            Space::PoolSlot => "pool_slot",
            Space::PoolExtent => "pool_extent",
        }
    }
}

/// How a sampled allocation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FreeKind {
    /// A deallocation.
    Dealloc,
    /// A reallocation, which ends the old allocation whether or not the
    /// address moved.
    Realloc,
}

/// The default mean sampling interval in bytes.
pub const DEFAULT_SAMPLE_INTERVAL: u64 = 1 << 19;

static ACTIVE: AtomicBool = AtomicBool::new(true);
static TRACK_FREES: AtomicBool = AtomicBool::new(false);
static SAMPLE_INTERVAL: AtomicU64 = AtomicU64::new(DEFAULT_SAMPLE_INTERVAL);

/// Enables or disables sampling of new allocations. Frees of already
/// sampled allocations are tracked regardless.
pub fn set_active(active: bool) {
    ACTIVE.store(active, Ordering::Relaxed);
}

/// Whether new allocations are sampled.
pub fn is_active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// Enables or disables capturing the freeing stack of sampled allocations.
pub fn set_track_frees(track: bool) {
    TRACK_FREES.store(track, Ordering::Relaxed);
}

/// Whether the freeing stacks of sampled allocations are captured.
pub fn track_frees() -> bool {
    TRACK_FREES.load(Ordering::Relaxed)
}

/// Sets the mean sampling interval in bytes, clamped to at least 1. Threads
/// pick up the new interval at their next sample.
pub fn set_sample_interval(bytes: u64) {
    SAMPLE_INTERVAL.store(bytes.max(1), Ordering::Relaxed);
}

/// The mean sampling interval in bytes.
pub fn sample_interval() -> u64 {
    SAMPLE_INTERVAL.load(Ordering::Relaxed)
}

/// Introspection entry points of the underlying allocator, registered by
/// whoever installs it.
#[derive(Debug, Clone, Copy)]
pub struct AllocatorHooks {
    /// The allocator's name.
    pub name: &'static str,
    /// Renders the allocator's own statistics as text.
    pub stats: fn() -> String,
    /// Returns cached free memory to the OS where the allocator supports it.
    pub collect: fn(),
}

static ALLOCATOR: std::sync::OnceLock<AllocatorHooks> = std::sync::OnceLock::new();

/// Registers the underlying allocator's hooks. Later registrations are
/// ignored.
pub fn register_allocator(hooks: AllocatorHooks) {
    let _ = ALLOCATOR.set(hooks);
}

/// The registered allocator hooks, if any.
pub fn allocator() -> Option<&'static AllocatorHooks> {
    ALLOCATOR.get()
}

/// The inverse probability that an allocation of `size` bytes is sampled at
/// mean interval `interval`: the weight that makes a sample's count unbiased.
fn sample_scale(size: usize, interval: u64) -> f64 {
    let p = -(-f64::cast_lossy(size) / f64::cast_lossy(interval)).exp_m1();
    if p > 0.0 { 1.0 / p } else { 1.0 }
}

/// Per-thread byte countdown to the next sample.
#[derive(Debug, Clone, Copy)]
struct Sampler {
    /// Bytes left until the next sample point.
    countdown: u64,
    /// xorshift64* state. Zero means unseeded.
    rng: u64,
}

impl Sampler {
    const UNSEEDED: Sampler = Sampler {
        countdown: 0,
        rng: 0,
    };

    fn seeded(seed: u64) -> Sampler {
        let mut sampler = Sampler {
            countdown: 0,
            rng: splitmix64(seed) | 1,
        };
        sampler.countdown = sampler.draw(sample_interval());
        sampler
    }

    /// Draws an exponentially distributed gap with mean `interval`, at
    /// least 1.
    fn draw(&mut self, interval: u64) -> u64 {
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        let bits = x.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11;
        // In (0, 1], so the logarithm is finite.
        let u = 1.0 - f64::cast_lossy(bits) / f64::cast_lossy(1u64 << 53);
        u64::cast_lossy(-u.ln() * f64::cast_lossy(interval)).max(1)
    }

    /// Advances past an allocation of `size` bytes and reports whether a
    /// sample point fell inside it. The countdown restarts after a sample
    /// with a fresh draw, which the memorylessness of the exponential makes
    /// equivalent to continuing the process.
    #[inline]
    fn step(&mut self, size: usize, interval: u64) -> bool {
        let size = u64::cast_from(size);
        if size < self.countdown {
            self.countdown -= size;
            false
        } else {
            self.countdown = self.draw(interval);
            true
        }
    }
}

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

struct ThreadState {
    /// The [`Sampler`] countdown, kept apart so the hot path touches nothing
    /// else.
    countdown: Cell<u64>,
    /// The [`Sampler`] rng state.
    rng: Cell<u64>,
    /// Set while this thread runs tracker code. See the module docs.
    busy: Cell<bool>,
    /// The registry's stack shard for this thread, `u32::MAX` until the
    /// first sample assigns one.
    stack_shard: Cell<u32>,
}

thread_local! {
    // NOTE: Const-initialized and without `Drop`, so access never
    // allocates, never registers a destructor, and stays valid during
    // thread teardown. The global allocator relies on all three.
    static THREAD: ThreadState = const {
        ThreadState {
            countdown: Cell::new(Sampler::UNSEEDED.countdown),
            rng: Cell::new(Sampler::UNSEEDED.rng),
            busy: Cell::new(false),
            stack_shard: Cell::new(u32::MAX),
        }
    };
}

/// Runs `f` with this thread's reentrancy flag set, or returns `None` if
/// the flag is already set.
fn with_busy<R>(f: impl FnOnce() -> R) -> Option<R> {
    struct Reset<'a>(&'a Cell<bool>);
    impl Drop for Reset<'_> {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }
    THREAD.with(|t| {
        if t.busy.replace(true) {
            return None;
        }
        let _reset = Reset(&t.busy);
        Some(f())
    })
}

/// Reports a new allocation of `size` bytes at `ptr`.
///
/// The caller must report the matching [`on_free`] before the address can
/// be handed out again, and must not report `ptr` again before then.
#[inline]
pub fn on_alloc(ptr: *mut u8, size: usize, space: Space) {
    if !ACTIVE.load(Ordering::Relaxed) {
        return;
    }
    THREAD.with(|t| {
        let size64 = u64::cast_from(size);
        let left = t.countdown.get();
        if size64 < left {
            t.countdown.set(left - size64);
        } else {
            sample_slow(t, ptr, size, space);
        }
    })
}

/// Handles an allocation that reached the countdown, which is also how an
/// unseeded thread's first allocation arrives.
#[cold]
#[inline(never)]
fn sample_slow(t: &ThreadState, ptr: *mut u8, size: usize, space: Space) {
    let interval = sample_interval();
    let mut sampler = Sampler {
        countdown: t.countdown.get(),
        rng: t.rng.get(),
    };
    if sampler.rng == 0 {
        sampler = Sampler::seeded(u64::cast_from(std::ptr::from_ref(t).addr()) ^ now_nanos());
    }
    let sampled = sampler.step(size, interval);
    t.countdown.set(sampler.countdown);
    t.rng.set(sampler.rng);
    if sampled {
        with_busy(|| registry::record_alloc(ptr.addr(), size, space, interval));
    }
}

/// Reports that the allocation at `ptr` ends. Must precede the release of
/// the memory to its allocator, else a concurrent reuse of the address could
/// be attributed this free.
#[inline]
pub fn on_free(ptr: *mut u8, kind: FreeKind) {
    if filter::maybe_contains(ptr.addr()) {
        free_slow(ptr, kind);
    }
}

#[cold]
#[inline(never)]
fn free_slow(ptr: *mut u8, kind: FreeKind) {
    with_busy(|| registry::record_free(ptr.addr(), kind));
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::cast_lossy(d.as_secs_f64() * 1e9))
}

/// A [`GlobalAlloc`] that forwards to `A` and reports every event to the
/// tracker.
#[derive(Debug, Default)]
pub struct TrackingAlloc<A>(pub A);

// SAFETY: every method forwards to `A` with unchanged arguments and returns
// its result unchanged. The tracker hooks neither touch the allocated memory
// nor unwind, and reentrant allocations they make reach `A` through the
// global allocator like any other.
unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackingAlloc<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded under the caller's contract.
        let ptr = unsafe { self.0.alloc(layout) };
        if !ptr.is_null() {
            on_alloc(ptr, layout.size(), Space::Heap);
        }
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded under the caller's contract.
        let ptr = unsafe { self.0.alloc_zeroed(layout) };
        if !ptr.is_null() {
            on_alloc(ptr, layout.size(), Space::Heap);
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        on_free(ptr, FreeKind::Dealloc);
        // SAFETY: forwarded under the caller's contract.
        unsafe { self.0.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Ending the old allocation first is required by `on_free`'s
        // contract. If the reallocation fails, the old allocation survives
        // untracked, which loses one sample and nothing else.
        on_free(ptr, FreeKind::Realloc);
        // SAFETY: forwarded under the caller's contract.
        let new = unsafe { self.0.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            on_alloc(new, new_size, Space::Heap);
        }
        new
    }
}
