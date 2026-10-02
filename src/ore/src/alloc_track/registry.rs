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

//! The sample registry: live samples, interned stacks, and aggregates.
//!
//! All functions here run with the thread's reentrancy flag set. Locks are
//! never nested: a sample interns its stack and then inserts into its live
//! shard, and a free removes from its live shard and then interns the free
//! stack.
//!
//! Live records are sharded by address. Stacks and the cumulative
//! aggregates are sharded by thread, because a hot stack sampled on many
//! threads otherwise serializes them on one lock: with a single stack
//! table, a 16-thread benchmark spent about 30% of its cycles contending on
//! that table's lock. A stack
//! sampled on several threads is therefore interned once per shard, and
//! [`snapshot`] merges the shards and renumbers stacks densely.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::alloc_track::{FreeKind, Space, THREAD, TRACK_FREES, filter, sample_scale};
use crate::cast::{CastFrom, CastLossy};

const SHARD_COUNT: usize = 64;
const STACK_SHARD_BITS: u32 = 6;
const STACK_SHARD_COUNT: u32 = 1 << STACK_SHARD_BITS;
const MAX_FRAMES: usize = 64;

#[derive(Debug, Clone, Copy)]
struct Live {
    /// A stack id as described on [`Stacks`].
    stack: u32,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FreeKey {
    space: Space,
    kind: FreeKind,
    alloc_stack: u32,
    free_stack: u32,
}

#[derive(Debug, Clone, Copy, Default)]
struct FreeTotals {
    totals: Totals,
    /// Sum of lifetimes in seconds, weighted like `totals.count`.
    lifetime_secs: f64,
}

/// One stack shard. A stack id encodes its shard in the low
/// [`STACK_SHARD_BITS`] bits and its index into that shard's `frames` in
/// the bits above.
struct Stacks {
    ids: BTreeMap<Box<[usize]>, u32>,
    frames: Vec<Box<[usize]>>,
    /// Cumulative sampled allocations by (space, stack).
    allocated: BTreeMap<(Space, u32), Totals>,
    /// Cumulative sampled frees, recorded in the freeing thread's shard.
    freed: BTreeMap<FreeKey, FreeTotals>,
}

static STACKS: [Mutex<Stacks>; 1 << STACK_SHARD_BITS] = [const {
    Mutex::new(Stacks {
        ids: BTreeMap::new(),
        frames: Vec::new(),
        allocated: BTreeMap::new(),
        freed: BTreeMap::new(),
    })
}; 1 << STACK_SHARD_BITS];

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
fn stack_shard() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    THREAD.with(|t| {
        let mut shard = t.stack_shard.get();
        if shard == u32::MAX {
            shard = NEXT.fetch_add(1, Ordering::Relaxed) % STACK_SHARD_COUNT;
            t.stack_shard.set(shard);
        }
        shard
    })
}

/// Captures the calling stack, root first, into `buf`, returning the frame
/// count. Frames deeper than [`MAX_FRAMES`] from the leaf are dropped.
fn capture(buf: &mut [usize; MAX_FRAMES]) -> usize {
    let mut n = 0;
    // SAFETY: `trace_unsynchronized` is unsafe because some backends
    // (Windows dbghelp) are not thread safe. On unix it walks the stack with
    // the system unwinder's `_Unwind_Backtrace`, which is.
    unsafe {
        backtrace::trace_unsynchronized(|frame| {
            buf[n] = frame.ip().addr();
            n += 1;
            n < MAX_FRAMES
        });
    }
    buf[..n].reverse();
    n
}

impl Stacks {
    /// Interns `frames` into this shard, numbered `shard`.
    fn intern(&mut self, shard: u32, frames: &[usize]) -> u32 {
        if let Some(&id) = self.ids.get(frames) {
            return id;
        }
        let index = u32::try_from(self.frames.len())
            .ok()
            .filter(|&index| index < 1 << (32 - STACK_SHARD_BITS))
            .expect("fewer than 2^26 distinct stacks per shard");
        let id = (index << STACK_SHARD_BITS) | shard;
        self.frames.push(frames.into());
        self.ids.insert(frames.into(), id);
        id
    }
}

pub(super) fn record_alloc(addr: usize, size: usize, space: Space, interval: u64) {
    let mut buf = [0; MAX_FRAMES];
    let n = capture(&mut buf);
    let scale = sample_scale(size, interval);
    if !filter::insert(addr) {
        return;
    }
    let stack = {
        let shard = stack_shard();
        let mut stacks = lock(&STACKS[usize::cast_from(shard)]);
        let id = stacks.intern(shard, &buf[..n]);
        stacks
            .allocated
            .entry((space, id))
            .or_default()
            .add(scale, size);
        id
    };
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
    if !TRACK_FREES.load(Ordering::Relaxed) {
        return;
    }
    let lifetime = live.born.elapsed().as_secs_f64();
    let mut buf = [0; MAX_FRAMES];
    let n = capture(&mut buf);
    let shard = stack_shard();
    let mut stacks = lock(&STACKS[usize::cast_from(shard)]);
    let free_stack = stacks.intern(shard, &buf[..n]);
    let key = FreeKey {
        space: live.space,
        kind,
        alloc_stack: live.stack,
        free_stack,
    };
    let entry = stacks.freed.entry(key).or_default();
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
    /// The sampling interval at snapshot time.
    pub sample_interval: u64,
    /// The number of live sampled allocations.
    pub live_samples: usize,
    /// Distinct stacks as return addresses, root first.
    pub stacks: Vec<Box<[usize]>>,
    /// Live allocations by site.
    pub live: Vec<Site>,
    /// Allocations since the last [`reset_history`], by site.
    pub allocated: Vec<Site>,
    /// Frees since the last [`reset_history`], by site pair. Populated only
    /// while [`set_track_frees`](super::set_track_frees) is enabled.
    pub freed: Vec<FreeSite>,
}

/// Copies the tracker's state. Concurrent events may or may not be included.
pub fn snapshot() -> Snapshot {
    super::with_busy(snapshot_inner).unwrap_or_default()
}

fn snapshot_inner() -> Snapshot {
    // Live shards are read before stack shards: every stack id a live record
    // or a free aggregate holds was interned before that record or
    // aggregate was written, so the stack shards copied afterwards resolve
    // it.
    let mut live_samples = 0;
    let mut live_raw: BTreeMap<(Space, u32), Totals> = BTreeMap::new();
    for shard in &SHARDS {
        let shard = lock(shard);
        live_samples += shard.len();
        for l in shard.values() {
            live_raw
                .entry((l.space, l.stack))
                .or_default()
                .add(l.scale, l.size);
        }
    }
    let copies: Vec<_> = STACKS
        .iter()
        .map(|shard| {
            let shard = lock(shard);
            (
                shard.frames.clone(),
                shard.allocated.clone(),
                shard.freed.clone(),
            )
        })
        .collect();

    // Renumber stacks densely, merging stacks interned in several shards.
    let mut stacks = Vec::new();
    let mut dense_ids: BTreeMap<&[usize], u32> = BTreeMap::new();
    let mut remap: Vec<Vec<u32>> = Vec::with_capacity(copies.len());
    for (frames, _, _) in &copies {
        let mut shard_remap = Vec::with_capacity(frames.len());
        for f in frames {
            let id = *dense_ids.entry(&**f).or_insert_with(|| {
                stacks.push(f.clone());
                u32::try_from(stacks.len() - 1).expect("fewer than 2^32 distinct stacks")
            });
            shard_remap.push(id);
        }
        remap.push(shard_remap);
    }
    let dense = |id: u32| {
        let shard = usize::cast_from(id & (STACK_SHARD_COUNT - 1));
        remap[shard][usize::cast_from(id >> STACK_SHARD_BITS)]
    };

    let mut live: BTreeMap<(Space, u32), Totals> = BTreeMap::new();
    for ((space, stack), t) in &live_raw {
        live.entry((*space, dense(*stack))).or_default().merge(t);
    }
    let mut allocated: BTreeMap<(Space, u32), Totals> = BTreeMap::new();
    let mut freed: BTreeMap<FreeKey, FreeTotals> = BTreeMap::new();
    for (_, shard_allocated, shard_freed) in &copies {
        for ((space, stack), t) in shard_allocated {
            allocated
                .entry((*space, dense(*stack)))
                .or_default()
                .merge(t);
        }
        for (k, f) in shard_freed {
            let key = FreeKey {
                alloc_stack: dense(k.alloc_stack),
                free_stack: dense(k.free_stack),
                ..*k
            };
            let entry = freed.entry(key).or_default();
            entry.totals.merge(&f.totals);
            entry.lifetime_secs += f.lifetime_secs;
        }
    }

    let site = |((space, stack), t): ((Space, u32), Totals)| Site {
        space,
        stack,
        count: t.count,
        bytes: t.bytes,
    };
    Snapshot {
        sample_interval: super::sample_interval(),
        live_samples,
        stacks,
        live: live.into_iter().map(site).collect(),
        allocated: allocated.into_iter().map(site).collect(),
        freed: freed
            .into_iter()
            .map(|(k, f)| FreeSite {
                space: k.space,
                kind: k.kind,
                alloc_stack: k.alloc_stack,
                free_stack: k.free_stack,
                count: f.totals.count,
                bytes: f.totals.bytes,
                mean_lifetime: Duration::from_secs_f64(if f.totals.count > 0.0 {
                    f.lifetime_secs / f.totals.count
                } else {
                    0.0
                }),
            })
            .collect(),
    }
}

/// Clears the cumulative allocation and free aggregates. Live samples and
/// interned stacks are kept.
pub fn reset_history() {
    super::with_busy(|| {
        for shard in &STACKS {
            let mut shard = lock(shard);
            shard.allocated.clear();
            shard.freed.clear();
        }
    });
}
