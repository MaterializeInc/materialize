// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection hooks for jemalloc.

use mz_ore::alloc_track::AllocatorHooks;

pub(crate) const HOOKS: AllocatorHooks = AllocatorHooks {
    name: "jemalloc",
    stats,
    collect,
};

fn stats() -> String {
    let mut buf = Vec::new();
    match tikv_jemalloc_ctl::stats_print::stats_print(&mut buf, Default::default()) {
        Ok(()) => String::from_utf8_lossy(&buf).into_owned(),
        Err(e) => format!("jemalloc stats unavailable: {e}"),
    }
}

fn collect() {
    // 4096 is `MALLCTL_ARENAS_ALL`, the pseudo-index that addresses every
    // arena.
    const NAME: &[u8] = b"arena.4096.purge\0";
    // SAFETY: `arena.<i>.purge` takes neither an old nor a new value, and
    // `NAME` is NUL-terminated.
    let _ = unsafe {
        tikv_jemalloc_sys::mallctl(
            NAME.as_ptr().cast(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
}
