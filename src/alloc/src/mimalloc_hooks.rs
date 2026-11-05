// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection hooks for mimalloc.

use std::ffi::{CStr, c_char, c_void};

use mz_alloc_track::AllocatorHooks;

pub(crate) const HOOKS: AllocatorHooks = AllocatorHooks {
    name: "mimalloc",
    stats,
    collect,
};

/// # Safety
///
/// `msg` must be a NUL-terminated string and `arg` must point to a live
/// `String` with no other references.
unsafe extern "C" fn append(msg: *const c_char, arg: *mut c_void) {
    // SAFETY: the function contract.
    let (out, msg) = unsafe { (&mut *arg.cast::<String>(), CStr::from_ptr(msg)) };
    out.push_str(&msg.to_string_lossy());
}

fn stats() -> String {
    let mut out = String::new();
    // SAFETY: `append` upholds the output function contract, and `out`
    // outlives the call, which invokes `append` only synchronously.
    unsafe { libmimalloc_sys::mi_stats_print_out(Some(append), (&raw mut out).cast()) };
    out
}

fn collect() {
    // SAFETY: no preconditions.
    unsafe { libmimalloc_sys::mi_collect(true) };
}
