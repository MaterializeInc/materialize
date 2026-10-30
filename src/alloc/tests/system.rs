// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `MZ_ALLOCATOR=system` selects the system allocator, and allocations stay
//! tracked exactly once: Rust allocations as Rust allocations, C allocations
//! as C allocations. The allocator is decided on the first allocation, so the
//! test reruns its own executable with the variable set.
//!
//! A Rust allocation that also passed through the exported `malloc` would be
//! reported twice for one address. The second report replaces the first, so
//! the double report shows only as twice the sampling probability, which
//! doubles the estimate for allocations much smaller than the interval.

#![cfg(all(target_os = "linux", target_env = "gnu"))]

use std::ffi::c_void;

use mz_alloc_track::Space;
use mz_alloc_track::testing::{live_bytes, sample_every_mib};

const MIB: usize = 1 << 20;

#[inline(never)]
fn rust_alloc_site() -> Vec<Vec<u8>> {
    (0..8).map(|_| vec![1u8; MIB]).collect()
}

#[inline(never)]
fn small_rust_alloc_site() -> Vec<Vec<u8>> {
    (0..SMALL_COUNT).map(|_| vec![1u8; SMALL_SIZE]).collect()
}

const SMALL_SIZE: usize = 4096;
const SMALL_COUNT: usize = 20_000;

#[inline(never)]
fn c_malloc_site() -> Vec<*mut c_void> {
    // SAFETY: `malloc` has no preconditions.
    (0..8).map(|_| unsafe { libc::malloc(MIB) }).collect()
}

fn check_system_allocator() {
    assert_eq!(mz_alloc::allocator(), None);
    sample_every_mib();

    let rust = rust_alloc_site();
    let c = c_malloc_site();
    let snapshot = mz_alloc_track::snapshot();
    let expected = 8.0 * 1048576.0;
    let rust_heap = live_bytes(&snapshot, Space::Heap, "rust_alloc_site");
    let c_heap = live_bytes(&snapshot, Space::CHeap, "c_malloc_site");
    assert!(rust_heap >= expected, "Rust allocations: {rust_heap} bytes");
    assert!(c_heap >= expected, "C allocations: {c_heap} bytes");

    drop(rust);
    for ptr in c {
        // SAFETY: allocated by `malloc` above.
        unsafe { libc::free(ptr) };
    }
    let snapshot = mz_alloc_track::snapshot();
    assert!(live_bytes(&snapshot, Space::Heap, "rust_alloc_site") < 1048576.0);
    assert!(live_bytes(&snapshot, Space::CHeap, "c_malloc_site") < 1048576.0);

    // About 80 samples, so the estimate's relative standard deviation is
    // about 11%, and the bounds are more than four deviations away.
    mz_alloc_track::set_sample_interval(1 << 20);
    drop(std::hint::black_box(vec![0u8; 64 * MIB]));
    let small = small_rust_alloc_site();
    let snapshot = mz_alloc_track::snapshot();
    let actual = 4096.0 * 20_000.0;
    let estimate = live_bytes(&snapshot, Space::Heap, "small_rust_alloc_site")
        + live_bytes(&snapshot, Space::CHeap, "small_rust_alloc_site");
    assert!(
        (0.5 * actual..1.5 * actual).contains(&estimate),
        "small Rust allocations: estimated {estimate} bytes of {actual}",
    );
    drop(small);
}

#[mz_ore::test]
fn system_allocator_is_tracked_once() {
    if std::env::var_os("MZ_ALLOCATOR").is_some_and(|v| v == "system") {
        check_system_allocator();
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "system_allocator_is_tracked_once", "--nocapture"])
        .env("MZ_ALLOCATOR", "system")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "the rerun with MZ_ALLOCATOR=system failed"
    );
}
