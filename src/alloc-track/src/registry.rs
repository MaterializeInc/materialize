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

//! The sample registry: live samples, interned stacks, and history.
//!
//! All functions here run with the thread's reentrancy flag set. Locks are
//! never nested: a sample interns its stack and then inserts into its live
//! shard, and a free removes from its live shard and then interns the free
//! stack.
//!
//! Live records and history entries own their stacks through [`Arc`]s, and
//! the intern tables hold only [`Weak`] references, so a stack is freed with
//! its last live sample or history entry. Memory for stacks therefore
//! follows the live samples while history is off. Each stack exists once:
//! the shared [`TABLE`] is the source of truth, and per-thread-shard
//! [`CACHES`] answer repeated lookups without its lock. With a single
//! locked stack table, a 16-thread benchmark spent about 30% of its cycles
//! contending on that lock, and a cache hit costs only an atomic increment.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use crate::cast::{CastFrom, CastLossy};
use crate::{FreeKind, Space, THREAD, TRACK_HISTORY, filter, sample_scale};

const SHARD_COUNT: usize = 64;
const STACK_SHARD_COUNT: usize = 64;
/// The deepest stack captured, matching jemalloc's profiler. Frames beyond it
/// from the leaf are dropped.
const MAX_FRAMES: usize = 128;

/// An interned stack, as return addresses, root first.
pub(super) struct Stack {
    pub(super) frames: Box<[usize]>,
}

static STACK_COUNT: AtomicUsize = AtomicUsize::new(0);
static STACK_BYTES: AtomicUsize = AtomicUsize::new(0);

impl Stack {
    fn new(frames: &[usize]) -> Stack {
        STACK_COUNT.fetch_add(1, Ordering::Relaxed);
        STACK_BYTES.fetch_add(Self::bytes(frames.len()), Ordering::Relaxed);
        Stack {
            frames: frames.into(),
        }
    }

    /// The heap footprint of a stack of `frames` frames: the frames, and the
    /// `Arc` allocation holding the stack and its two reference counts.
    fn bytes(frames: usize) -> usize {
        std::mem::size_of::<usize>() * (frames + 2) + std::mem::size_of::<Stack>()
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        STACK_COUNT.fetch_sub(1, Ordering::Relaxed);
        STACK_BYTES.fetch_sub(Self::bytes(self.frames.len()), Ordering::Relaxed);
    }
}

fn hash_frames(frames: &[usize]) -> u64 {
    frames.iter().fold(0x243f_6a88_85a3_08d3, |h, &f| {
        (h ^ u64::cast_from(f))
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .rotate_left(29)
    })
}

/// Every live stack by hash. Entries with dead references are pruned when
/// their bucket is next written and by [`snapshot`].
static TABLE: Mutex<BTreeMap<u64, Vec<Weak<Stack>>>> = Mutex::new(BTreeMap::new());

/// Recently interned stacks by hash, per stack shard.
static CACHES: [Mutex<BTreeMap<u64, Weak<Stack>>>; STACK_SHARD_COUNT] =
    [const { Mutex::new(BTreeMap::new()) }; STACK_SHARD_COUNT];

/// Stacks created since the last [`prune`].
static CREATED_SINCE_PRUNE: AtomicUsize = AtomicUsize::new(0);
/// [`intern`] prunes once this many stacks were created since the last
/// prune: the live stack count at that prune, but at least
/// [`MIN_PRUNE_AFTER`]. Dead entries then stay proportional to the live
/// stacks, even when nothing takes snapshots.
static PRUNE_AFTER: AtomicUsize = AtomicUsize::new(MIN_PRUNE_AFTER);
const MIN_PRUNE_AFTER: usize = 1024;

/// Returns the interned stack for `frames`, creating it if no live stack
/// has these frames.
pub(super) fn intern(shard: usize, frames: &[usize]) -> Arc<Stack> {
    let hash = hash_frames(frames);
    let cached = lock(&CACHES[shard]).get(&hash).and_then(Weak::upgrade);
    if let Some(stack) = cached {
        if *stack.frames == *frames {
            return stack;
        }
    }
    let (stack, created) = {
        let mut table = lock(&TABLE);
        let bucket = table.entry(hash).or_default();
        bucket.retain(|w| w.strong_count() > 0);
        match bucket
            .iter()
            .filter_map(Weak::upgrade)
            .find(|s| *s.frames == *frames)
        {
            Some(stack) => (stack, false),
            None => {
                let stack = Arc::new(Stack::new(frames));
                bucket.push(Arc::downgrade(&stack));
                (stack, true)
            }
        }
    };
    lock(&CACHES[shard]).insert(hash, Arc::downgrade(&stack));
    if created
        && CREATED_SINCE_PRUNE.fetch_add(1, Ordering::Relaxed) + 1
            >= PRUNE_AFTER.load(Ordering::Relaxed)
    {
        // Threads racing past the check each prune, which only repeats work.
        CREATED_SINCE_PRUNE.store(0, Ordering::Relaxed);
        prune();
        PRUNE_AFTER.store(
            STACK_COUNT.load(Ordering::Relaxed).max(MIN_PRUNE_AFTER),
            Ordering::Relaxed,
        );
    }
    stack
}

/// The number of hashes in the intern table, live or not.
#[cfg(test)]
pub(super) fn table_len() -> usize {
    lock(&TABLE).len()
}

/// Drops intern table entries whose stacks are gone.
fn prune() {
    lock(&TABLE).retain(|_, bucket| {
        bucket.retain(|w| w.strong_count() > 0);
        !bucket.is_empty()
    });
    for cache in &CACHES {
        lock(cache).retain(|_, w| w.strong_count() > 0);
    }
}

#[derive(Clone)]
struct Live {
    stack: Arc<Stack>,
    size: usize,
    space: Space,
    /// Inverse sampling probability at sample time.
    scale: f64,
    born: Instant,
}

static SHARDS: [Mutex<BTreeMap<usize, Live>>; SHARD_COUNT] =
    [const { Mutex::new(BTreeMap::new()) }; SHARD_COUNT];

#[derive(Debug, Clone, Copy, Default)]
struct Totals {
    count: f64,
    bytes: f64,
}

impl Totals {
    fn add(&mut self, scale: f64, size: usize) {
        self.count += scale;
        self.bytes += scale * f64::cast_lossy(size);
    }

    fn merge(&mut self, other: &Totals) {
        self.count += other.count;
        self.bytes += other.bytes;
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct FreeTotals {
    totals: Totals,
    /// Sum of lifetimes in seconds, weighted like `totals.count`.
    lifetime_secs: f64,
}

impl FreeTotals {
    fn merge(&mut self, other: &FreeTotals) {
        self.totals.merge(&other.totals);
        self.lifetime_secs += other.lifetime_secs;
    }
}

/// Identifies a stack by the address of its allocation, which is stable
/// while the map holding the key also holds the stack.
fn stack_key(stack: &Arc<Stack>) -> usize {
    Arc::as_ptr(stack).addr()
}

/// One shard of the history, written by the threads of one stack shard.
#[derive(Default)]
struct History {
    /// Cumulative sampled allocations by (space, stack).
    allocated: BTreeMap<(Space, usize), (Arc<Stack>, Totals)>,
    /// Cumulative sampled frees by (space, kind, allocating stack, freeing
    /// stack).
    freed: BTreeMap<(Space, FreeKind, usize, usize), (Arc<Stack>, Arc<Stack>, FreeTotals)>,
}

static HISTORY: [Mutex<History>; STACK_SHARD_COUNT] = [const {
    Mutex::new(History {
        allocated: BTreeMap::new(),
        freed: BTreeMap::new(),
    })
}; STACK_SHARD_COUNT];

/// Locks `mutex`, ignoring poison: a panic while holding a registry lock
/// leaves at worst a partially updated aggregate, and the allocator must not
/// panic.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn shard(addr: usize) -> &'static Mutex<BTreeMap<usize, Live>> {
    let hash = (u64::cast_from(addr) >> 4).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 58;
    &SHARDS[usize::cast_from(hash)]
}

/// Returns the calling thread's stack shard. Threads take shards round
/// robin on their first sample, so threads share a shard only once more
/// than [`STACK_SHARD_COUNT`] of them have sampled.
fn stack_shard() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    THREAD.with(|t| {
        let mut shard = t.stack_shard.get();
        if shard == usize::MAX {
            shard = NEXT.fetch_add(1, Ordering::Relaxed) % STACK_SHARD_COUNT;
            t.stack_shard.set(shard);
        }
        shard
    })
}

/// Active [`suspend_unwinding`] calls.
static UNWIND_SUSPENSIONS: AtomicUsize = AtomicUsize::new(0);
/// Threads inside [`capture`] that passed the suspension check.
static UNWINDING: AtomicUsize = AtomicUsize::new(0);

pub(super) fn suspend_unwinding() {
    // Pairs with the increment and check in `capture`. With sequentially
    // consistent accesses on both sides, either `capture` sees the
    // suspension and skips, or this loop sees its increment and waits for
    // the unwind to finish.
    UNWIND_SUSPENSIONS.fetch_add(1, Ordering::SeqCst);
    while UNWINDING.load(Ordering::SeqCst) != 0 {
        std::thread::yield_now();
    }
}

pub(super) fn resume_unwinding() {
    UNWIND_SUSPENSIONS.fetch_sub(1, Ordering::SeqCst);
}

/// Captures the calling stack, root first, into `buf`, returning the frame
/// count, or `None` while unwinding is suspended.
fn capture(buf: &mut [usize; MAX_FRAMES]) -> Option<usize> {
    UNWINDING.fetch_add(1, Ordering::SeqCst);
    if UNWIND_SUSPENSIONS.load(Ordering::SeqCst) != 0 {
        UNWINDING.fetch_sub(1, Ordering::SeqCst);
        return None;
    }
    let mut n = 0;
    // SAFETY: `trace_unsynchronized` is unsafe because some backends
    // (Windows dbghelp) are not thread safe. On unix it walks the stack with
    // the system unwinder's `_Unwind_Backtrace`, which is.
    unsafe {
        backtrace::trace_unsynchronized(|frame| {
            // The unwinder can end with a null frame. Profile conversion
            // subtracts one from every address, which underflows on it.
            let ip = frame.ip().addr();
            if ip != 0 {
                buf[n] = ip;
                n += 1;
            }
            n < MAX_FRAMES
        });
    }
    UNWINDING.fetch_sub(1, Ordering::SeqCst);
    buf[..n].reverse();
    Some(n)
}

pub(super) fn record_alloc(addr: usize, size: usize, space: Space, interval: u64) {
    let mut buf = [0; MAX_FRAMES];
    let Some(n) = capture(&mut buf) else {
        return;
    };
    let scale = sample_scale(size, interval);
    if !filter::insert(addr) {
        return;
    }
    let shard_index = stack_shard();
    let stack = intern(shard_index, &buf[..n]);
    if TRACK_HISTORY.load(Ordering::Relaxed) {
        lock(&HISTORY[shard_index])
            .allocated
            .entry((space, stack_key(&stack)))
            .or_insert_with(|| (Arc::clone(&stack), Totals::default()))
            .1
            .add(scale, size);
    }
    let live = Live {
        stack,
        size,
        space,
        scale,
        born: Instant::now(),
    };
    if lock(shard(addr)).insert(addr, live).is_some() {
        // A stale record means a free went unreported. The new record
        // replaces it, so give back the stale record's filter count.
        filter::remove(addr);
    }
}

pub(super) fn record_free(addr: usize, kind: FreeKind) {
    let Some(live) = lock(shard(addr)).remove(&addr) else {
        // A filter false positive.
        return;
    };
    filter::remove(addr);
    if !TRACK_HISTORY.load(Ordering::Relaxed) {
        return;
    }
    let lifetime = live.born.elapsed().as_secs_f64();
    let mut buf = [0; MAX_FRAMES];
    let Some(n) = capture(&mut buf) else {
        return;
    };
    let shard_index = stack_shard();
    let free_stack = intern(shard_index, &buf[..n]);
    let key = (
        live.space,
        kind,
        stack_key(&live.stack),
        stack_key(&free_stack),
    );
    let mut history = lock(&HISTORY[shard_index]);
    let entry = &mut history
        .freed
        .entry(key)
        .or_insert_with(|| (live.stack, free_stack, FreeTotals::default()))
        .2;
    entry.totals.add(live.scale, live.size);
    entry.lifetime_secs += live.scale * lifetime;
}

/// Estimated allocations attributed to one stack.
#[derive(Debug, Clone, PartialEq)]
pub struct Site {
    /// The memory kind.
    pub space: Space,
    /// Index into [`Snapshot::stacks`].
    pub stack: u32,
    /// Estimated number of allocations.
    pub count: f64,
    /// Estimated bytes.
    pub bytes: f64,
}

/// Estimated frees attributed to one (allocation stack, free stack) pair.
#[derive(Debug, Clone, PartialEq)]
pub struct FreeSite {
    /// The memory kind.
    pub space: Space,
    /// How the allocations ended.
    pub kind: FreeKind,
    /// Index into [`Snapshot::stacks`] of the allocating stack.
    pub alloc_stack: u32,
    /// Index into [`Snapshot::stacks`] of the freeing stack.
    pub free_stack: u32,
    /// Estimated number of freed allocations.
    pub count: f64,
    /// Estimated freed bytes.
    pub bytes: f64,
    /// Mean lifetime of the freed allocations.
    pub mean_lifetime: Duration,
}

/// A point-in-time copy of the tracker's state.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// The number of live sampled allocations.
    pub live_samples: usize,
    /// Stacks referenced by the sites below, as return addresses, root first.
    pub stacks: Vec<Box<[usize]>>,
    /// Live allocations by site.
    pub live: Vec<Site>,
    /// Allocations since history was enabled or last reset, by site. Empty
    /// while history is off, see [`set_track_history`](super::set_track_history).
    pub allocated: Vec<Site>,
    /// Frees since history was enabled or last reset, by site pair. Empty
    /// while history is off.
    pub freed: Vec<FreeSite>,
    /// The number of stacks the tracker holds, including those of history.
    pub tracker_stacks: usize,
    /// The approximate heap bytes of the tracker's stacks.
    pub tracker_stack_bytes: usize,
}

/// Copies the tracker's state. Concurrent events may or may not be included.
pub fn snapshot() -> Snapshot {
    super::with_busy(snapshot_inner).unwrap_or_default()
}

/// Assigns dense indexes to stacks. Keying by stack identity is keying by
/// frames, because [`intern`] keeps at most one live stack per frames.
#[derive(Default)]
struct Numbering {
    ids: BTreeMap<usize, u32>,
    stacks: Vec<Box<[usize]>>,
}

impl Numbering {
    fn id(&mut self, stack: &Arc<Stack>) -> u32 {
        let stacks = &mut self.stacks;
        *self.ids.entry(stack_key(stack)).or_insert_with(|| {
            stacks.push(stack.frames.clone());
            u32::try_from(stacks.len() - 1).expect("fewer than 2^32 distinct stacks")
        })
    }
}

fn snapshot_inner() -> Snapshot {
    // Cloning the records keeps their stacks alive, so stack keys stay unique
    // until the snapshot is assembled.
    let mut live_samples = 0;
    let mut live_records = Vec::new();
    for shard in &SHARDS {
        let shard = lock(shard);
        live_samples += shard.len();
        live_records.extend(shard.values().cloned());
    }
    let mut allocated_records = Vec::new();
    let mut freed_records = Vec::new();
    for shard in &HISTORY {
        let shard = lock(shard);
        allocated_records.extend(
            shard
                .allocated
                .iter()
                .map(|(&(space, _), (stack, t))| (space, Arc::clone(stack), *t)),
        );
        freed_records.extend(shard.freed.iter().map(|(&(space, kind, _, _), (a, f, t))| {
            (space, kind, Arc::clone(a), Arc::clone(f), *t)
        }));
    }

    let mut numbering = Numbering::default();
    let mut live: BTreeMap<(Space, u32), Totals> = BTreeMap::new();
    for l in &live_records {
        live.entry((l.space, numbering.id(&l.stack)))
            .or_default()
            .add(l.scale, l.size);
    }
    let mut allocated: BTreeMap<(Space, u32), Totals> = BTreeMap::new();
    for (space, stack, t) in &allocated_records {
        allocated
            .entry((*space, numbering.id(stack)))
            .or_default()
            .merge(t);
    }
    let mut freed: BTreeMap<(Space, FreeKind, u32, u32), FreeTotals> = BTreeMap::new();
    for (space, kind, alloc_stack, free_stack, t) in &freed_records {
        let key = (
            *space,
            *kind,
            numbering.id(alloc_stack),
            numbering.id(free_stack),
        );
        freed.entry(key).or_default().merge(t);
    }
    drop((live_records, allocated_records, freed_records));
    prune();

    let site = |((space, stack), t): ((Space, u32), Totals)| Site {
        space,
        stack,
        count: t.count,
        bytes: t.bytes,
    };
    Snapshot {
        live_samples,
        stacks: numbering.stacks,
        live: live.into_iter().map(site).collect(),
        allocated: allocated.into_iter().map(site).collect(),
        freed: freed
            .into_iter()
            .map(|((space, kind, alloc_stack, free_stack), f)| FreeSite {
                space,
                kind,
                alloc_stack,
                free_stack,
                count: f.totals.count,
                bytes: f.totals.bytes,
                mean_lifetime: Duration::from_secs_f64(if f.totals.count > 0.0 {
                    f.lifetime_secs / f.totals.count
                } else {
                    0.0
                }),
            })
            .collect(),
        tracker_stacks: STACK_COUNT.load(Ordering::Relaxed),
        tracker_stack_bytes: STACK_BYTES.load(Ordering::Relaxed),
    }
}

/// Clears the history. Live samples are kept.
pub fn reset_history() {
    super::with_busy(|| {
        for shard in &HISTORY {
            // Swap the maps out first, so their stacks drop after the lock
            // is released.
            let history = std::mem::take(&mut *lock(shard));
            drop(history);
        }
    });
}
