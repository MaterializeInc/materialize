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
use mz_alloc_track::Space;
use mz_alloc_track::testing::{live_bytes, sample_every_mib};

const MIB: usize = 1 << 20;

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
    let direct_bytes = live_bytes(&snapshot, Space::CHeap, "c_malloc_site");
    assert!(
        direct_bytes >= expected,
        "direct malloc: {direct_bytes} bytes, expected at least {expected}",
    );
    let strdup_bytes = live_bytes(&snapshot, Space::CHeap, "glibc_strdup_site");
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
    assert!(live_bytes(&snapshot, Space::CHeap, "c_malloc_site") < 1048576.0);
    assert!(live_bytes(&snapshot, Space::CHeap, "glibc_strdup_site") < 1048576.0);
}
