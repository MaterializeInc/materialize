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

//! Overhead of [`TrackingAlloc`] over plain mimalloc, called directly rather
//! than installed as the global allocator. Each iteration allocates and then
//! frees a batch, with a retained set of tracked allocations keeping live
//! samples (and so populated filter pages) spread across the heap.

use std::alloc::{GlobalAlloc, Layout};
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mimalloc::MiMalloc;
use mz_ore::alloc_track::{self, TrackingAlloc};

const BATCH: usize = 1024;

fn churn<A: GlobalAlloc>(alloc: &A, layout: Layout, ptrs: &mut Vec<*mut u8>) {
    for _ in 0..BATCH {
        // SAFETY: `layout` has nonzero size.
        ptrs.push(unsafe { alloc.alloc(layout) });
    }
    for ptr in ptrs.drain(..) {
        // SAFETY: allocated above with `layout`.
        unsafe { alloc.dealloc(black_box(ptr), layout) };
    }
}

fn bench(c: &mut Criterion) {
    static TRACKED: TrackingAlloc<MiMalloc> = TrackingAlloc(MiMalloc);
    let retained_layout = Layout::from_size_align(64, 8).unwrap();
    let retained: Vec<*mut u8> = (0..1 << 20)
        // SAFETY: nonzero size.
        .map(|_| unsafe { TRACKED.alloc(retained_layout) })
        .collect();

    let mut group = c.benchmark_group("alloc_track");
    group.throughput(Throughput::Elements(u64::try_from(BATCH).unwrap()));
    let mut ptrs = Vec::with_capacity(BATCH);
    for size in [16usize, 256, 4096, 65536] {
        let layout = Layout::from_size_align(size, 8).unwrap();
        group.bench_with_input(BenchmarkId::new("mimalloc", size), &layout, |b, &l| {
            b.iter(|| churn(&MiMalloc, l, &mut ptrs))
        });
        alloc_track::set_active(false);
        group.bench_with_input(
            BenchmarkId::new("tracked_inactive", size),
            &layout,
            |b, &l| b.iter(|| churn(&TRACKED, l, &mut ptrs)),
        );
        alloc_track::set_active(true);
        alloc_track::set_track_frees(false);
        group.bench_with_input(BenchmarkId::new("tracked", size), &layout, |b, &l| {
            b.iter(|| churn(&TRACKED, l, &mut ptrs))
        });
        alloc_track::set_track_frees(true);
        group.bench_with_input(BenchmarkId::new("tracked_frees", size), &layout, |b, &l| {
            b.iter(|| churn(&TRACKED, l, &mut ptrs))
        });
        alloc_track::set_track_frees(false);
    }
    group.finish();

    for ptr in retained {
        // SAFETY: allocated above with `retained_layout`.
        unsafe { TRACKED.dealloc(ptr, retained_layout) };
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
