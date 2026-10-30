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
//! allocator.

use std::alloc::System;

use mz_alloc_track::testing::{
    assert_close, freed_bytes, live_bytes, sample_every_mib, stack_mentions,
};
use mz_alloc_track::{self as alloc_track, Space, TrackingAlloc};

#[global_allocator]
static ALLOC: TrackingAlloc<System> = TrackingAlloc(System);

const MIB: usize = 1 << 20;
const MIB_F64: f64 = 1048576.0;

#[inline(never)]
fn heap_alloc_site(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|_| vec![1u8; MIB]).collect()
}

#[inline(never)]
fn heap_free_site(bufs: Vec<Vec<u8>>) {
    drop(std::hint::black_box(bufs));
}

#[test] // allow(test-attribute): this crate cannot depend on mz-ore, which depends on it
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
fn heap_live_and_free_sites() {
    alloc_track::set_track_history(true);
    sample_every_mib();
    let bufs = heap_alloc_site(64);
    let snapshot = alloc_track::snapshot();
    assert_close(
        live_bytes(&snapshot, Space::Heap, "heap_alloc_site"),
        64 * MIB,
        "live",
    );
    heap_free_site(bufs);
    let snapshot = alloc_track::snapshot();
    assert!(live_bytes(&snapshot, Space::Heap, "heap_alloc_site") < MIB_F64);
    assert_close(
        freed_bytes(&snapshot, Space::Heap, "heap_alloc_site", "heap_free_site"),
        64 * MIB,
        "freed",
    );
}

#[inline(never)]
fn thread_alloc_site(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|_| vec![1u8; MIB]).collect()
}

#[test] // allow(test-attribute): this crate cannot depend on mz-ore, which depends on it
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
fn threads_sampling_one_stack_merge_into_one_site() {
    sample_every_mib();
    let bufs: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..4).map(|_| s.spawn(|| thread_alloc_site(8))).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let snapshot = alloc_track::snapshot();
    let mut stacks = snapshot.stacks.clone();
    stacks.sort();
    stacks.dedup();
    assert_eq!(stacks.len(), snapshot.stacks.len(), "stacks are distinct");
    let sites: Vec<_> = snapshot
        .live
        .iter()
        // Skips the outer vectors, which the sampler may also catch.
        .filter(|s| s.space == Space::Heap && s.bytes >= MIB_F64)
        // NOTE: The linker may fold `thread_alloc_site`'s closure with
        // identical ones of other tests, so the needle is this test's name.
        .filter(|s| {
            stack_mentions(
                &snapshot.stacks[usize::try_from(s.stack).unwrap()],
                "threads_sampling_one_stack_merge_into_one_site",
            )
        })
        .collect();
    assert_eq!(
        sites.len(),
        1,
        "one site for one stack sampled on 4 threads"
    );
    assert_close(sites[0].bytes, 32 * MIB, "live");
    drop(bufs);
}
