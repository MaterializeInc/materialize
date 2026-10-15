// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! C allocations reach the allocation tracker, both direct calls and calls
//! from inside glibc.

#![cfg(all(target_os = "linux", target_env = "gnu"))]

use std::ffi::{CString, c_void};

use mz_alloc as _;
use mz_alloc_track::{Snapshot, Space};

const MIB: usize = 1 << 20;

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

fn live_c_bytes(snapshot: &Snapshot, needle: &str) -> f64 {
    snapshot
        .live
        .iter()
        .filter(|s| s.space == Space::CHeap)
        .filter(|s| stack_mentions(&snapshot.stacks[usize::try_from(s.stack).unwrap()], needle))
        .map(|s| s.bytes)
        .sum()
}

/// Samples every allocation of at least 1 MiB on this thread.
fn sample_every_mib() {
    mz_alloc_track::set_sample_interval(4096);
    // The thread picks up the interval at its next sample, and a countdown
    // drawn at the default interval can skip a 1 MiB allocation. One 64 MiB
    // allocation consumes any such countdown.
    drop(std::hint::black_box(vec![0u8; 64 * MIB]));
}

#[inline(never)]
fn c_malloc_site() -> Vec<*mut c_void> {
    // SAFETY: `malloc` has no preconditions.
    (0..8).map(|_| unsafe { libc::malloc(MIB) }).collect()
}

#[inline(never)]
fn glibc_strdup_site(s: &CString) -> Vec<*mut libc::c_char> {
    // SAFETY: `s` is NUL-terminated.
    (0..8)
        .map(|_| unsafe { libc::strdup(s.as_ptr()) })
        .collect()
}

#[mz_ore::test]
fn c_allocations_are_tracked() {
    sample_every_mib();
    let direct = c_malloc_site();
    let s = CString::new(vec![b'x'; MIB]).unwrap();
    let inside_glibc = glibc_strdup_site(&s);
    let snapshot = mz_alloc_track::snapshot();
    let expected = 8.0 * 1048576.0;
    let direct_bytes = live_c_bytes(&snapshot, "c_malloc_site");
    assert!(
        direct_bytes >= expected,
        "direct malloc: {direct_bytes} bytes, expected at least {expected}",
    );
    let strdup_bytes = live_c_bytes(&snapshot, "glibc_strdup_site");
    assert!(
        strdup_bytes >= expected,
        "strdup: {strdup_bytes} bytes, expected at least {expected}",
    );
    for ptr in direct {
        // SAFETY: allocated by `malloc` above.
        unsafe { libc::free(ptr) };
    }
    for ptr in inside_glibc {
        // SAFETY: allocated by `strdup` above.
        unsafe { libc::free(ptr.cast()) };
    }
    let snapshot = mz_alloc_track::snapshot();
    assert!(live_c_bytes(&snapshot, "c_malloc_site") < 1048576.0);
    assert!(live_c_bytes(&snapshot, "glibc_strdup_site") < 1048576.0);
}
