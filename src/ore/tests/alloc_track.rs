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

//! End-to-end test of the buffer pool's reports to the allocation tracker,
//! installed as the global allocator. The test identifies its own sites by
//! symbolizing stacks for uniquely named functions.

use std::alloc::System;

use mz_alloc_track::testing::{assert_close, freed_bytes, live_bytes, sample_every_mib};
use mz_alloc_track::{self as alloc_track, Space, TrackingAlloc};
use mz_ore::pool::{ChunkHints, IDENTITY_CODEC, Pool};

#[global_allocator]
static ALLOC: TrackingAlloc<System> = TrackingAlloc(System);

const MIB: usize = 1 << 20;

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
    alloc_track::set_track_history(true);
    sample_every_mib();
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
