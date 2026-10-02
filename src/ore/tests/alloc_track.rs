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

//! End-to-end tests of the allocation tracker installed as the global
//! allocator. Tests share the process-wide tracker, so each identifies its
//! own sites by symbolizing stacks for a uniquely named function.

use std::alloc::System;

use mz_ore::alloc_track::{self, FreeKind, Snapshot, Space, TrackingAlloc};
use mz_ore::cast::CastLossy;
use mz_ore::pool::{ChunkHints, IDENTITY_CODEC, Pool};

#[global_allocator]
static ALLOC: TrackingAlloc<System> = TrackingAlloc(System);

const MIB: usize = 1 << 20;

/// Whether any frame of `stack` symbolizes to a name containing `needle`.
fn stack_mentions(stack: &[usize], needle: &str) -> bool {
    stack.iter().any(|&ip| {
        let mut found = false;
        backtrace::resolve(std::ptr::without_provenance_mut(ip), |sym| {
            if let Some(name) = sym.name() {
                found |= name.to_string().contains(needle);
            }
        });
        found
    })
}

fn live_bytes(snapshot: &Snapshot, space: Space, needle: &str) -> f64 {
    snapshot
        .live
        .iter()
        .filter(|s| s.space == space)
        .filter(|s| stack_mentions(&snapshot.stacks[usize::try_from(s.stack).unwrap()], needle))
        .map(|s| s.bytes)
        .sum()
}

fn freed_bytes(snapshot: &Snapshot, space: Space, alloc_needle: &str, free_needle: &str) -> f64 {
    snapshot
        .freed
        .iter()
        .filter(|s| s.space == space && s.kind == FreeKind::Dealloc)
        .filter(|s| {
            stack_mentions(
                &snapshot.stacks[usize::try_from(s.alloc_stack).unwrap()],
                alloc_needle,
            ) && stack_mentions(
                &snapshot.stacks[usize::try_from(s.free_stack).unwrap()],
                free_needle,
            )
        })
        .map(|s| s.bytes)
        .sum()
}

fn assert_close(actual: f64, expected: usize, what: &str) {
    let expected = f64::cast_lossy(expected);
    assert!(
        (actual - expected).abs() <= expected * 0.01,
        "{what}: {actual} bytes, expected about {expected}",
    );
}

#[inline(never)]
fn heap_alloc_site(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|_| vec![1u8; MIB]).collect()
}

#[inline(never)]
fn heap_free_site(bufs: Vec<Vec<u8>>) {
    drop(std::hint::black_box(bufs));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
fn heap_live_and_free_sites() {
    alloc_track::set_track_frees(true);
    // At this interval every 1 MiB buffer is sampled with weight 1, which
    // makes the estimates exact up to the small outer vector.
    alloc_track::set_sample_interval(4096);
    let bufs = heap_alloc_site(64);
    let snapshot = alloc_track::snapshot();
    assert_close(
        live_bytes(&snapshot, Space::Heap, "heap_alloc_site"),
        64 * MIB,
        "live",
    );
    heap_free_site(bufs);
    let snapshot = alloc_track::snapshot();
    assert!(live_bytes(&snapshot, Space::Heap, "heap_alloc_site") < f64::cast_lossy(MIB));
    assert_close(
        freed_bytes(&snapshot, Space::Heap, "heap_alloc_site", "heap_free_site"),
        64 * MIB,
        "freed",
    );
}

#[inline(never)]
fn pool_insert_site(pool: &Pool, n: usize) -> Vec<mz_ore::pool::ChunkHandle> {
    (0..n)
        .map(|_| {
            pool.insert_with(MIB / 8, ChunkHints::default(), &IDENTITY_CODEC, |dst| {
                dst.fill(7)
            })
        })
        .collect()
}

#[inline(never)]
fn pool_free_site(chunks: Vec<mz_ore::pool::ChunkHandle>) {
    drop(std::hint::black_box(chunks));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
fn pool_slots_are_tracked() {
    alloc_track::set_track_frees(true);
    alloc_track::set_sample_interval(4096);
    let pool = Pool::new().expect("pool reservation");
    let chunks = pool_insert_site(&pool, 16);
    let snapshot = alloc_track::snapshot();
    assert_close(
        live_bytes(&snapshot, Space::PoolSlot, "pool_insert_site"),
        16 * MIB,
        "live slots",
    );
    pool_free_site(chunks);
    let snapshot = alloc_track::snapshot();
    assert!(live_bytes(&snapshot, Space::PoolSlot, "pool_insert_site") < 1.0);
    assert_close(
        freed_bytes(
            &snapshot,
            Space::PoolSlot,
            "pool_insert_site",
            "pool_free_site",
        ),
        16 * MIB,
        "freed slots",
    );
}
