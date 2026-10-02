// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Chooses a global memory allocator based on Cargo features and, when both
//! jemalloc and mimalloc are compiled in, on the process name.
//!
//! Either allocator is wrapped in the sampling allocation tracker
//! ([`mz_ore::alloc_track`]). With both compiled in, `environmentd` and
//! `balancerd` use jemalloc and every other process uses mimalloc. The
//! choice is per process because one `materialized` executable runs as
//! either `environmentd` or `clusterd`, depending on the name it is invoked
//! by. environmentd allocates and frees at a high rate across many tokio
//! threads, which leaves mimalloc's per-thread pages partly empty: feature
//! benchmarks measured 130 to 290 MB more memory in the `materialized`
//! container than with jemalloc. clusterd uses less memory with mimalloc.

#[cfg(all(feature = "jemalloc", not(miri)))]
mod jemalloc_hooks;
#[cfg(all(feature = "mimalloc", not(miri)))]
mod mimalloc_hooks;

use mz_ore::metrics::MetricsRegistry;

/// An allocator the global allocator can delegate to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Allocator {
    /// jemalloc.
    Jemalloc,
    /// mimalloc.
    Mimalloc,
}

/// The allocator this process delegates to, or `None` if it uses the system
/// allocator.
#[cfg(any(miri, not(any(feature = "jemalloc", feature = "mimalloc"))))]
pub fn allocator() -> Option<Allocator> {
    None
}

/// The allocator this process delegates to, or `None` if it uses the system
/// allocator.
#[cfg(all(not(miri), feature = "jemalloc", not(feature = "mimalloc")))]
#[inline]
pub fn allocator() -> Option<Allocator> {
    Some(Allocator::Jemalloc)
}

/// The allocator this process delegates to, or `None` if it uses the system
/// allocator.
#[cfg(all(not(miri), feature = "mimalloc", not(feature = "jemalloc")))]
#[inline]
pub fn allocator() -> Option<Allocator> {
    Some(Allocator::Mimalloc)
}

/// The allocator this process delegates to, or `None` if it uses the system
/// allocator.
#[cfg(all(not(miri), feature = "jemalloc", feature = "mimalloc"))]
#[inline]
pub fn allocator() -> Option<Allocator> {
    Some(selected::get())
}

#[cfg(all(not(miri), feature = "jemalloc", feature = "mimalloc"))]
mod selected {
    use std::sync::atomic::{AtomicU8, Ordering};

    use crate::Allocator;

    const UNDECIDED: u8 = 0;
    const JEMALLOC: u8 = 1;
    const MIMALLOC: u8 = 2;

    static SELECTED: AtomicU8 = AtomicU8::new(UNDECIDED);

    /// The allocator for this process. The first call decides, and must not
    /// allocate, because it runs inside the first allocation. Racing first
    /// calls decide identically.
    #[inline]
    pub(crate) fn get() -> Allocator {
        match SELECTED.load(Ordering::Relaxed) {
            JEMALLOC => Allocator::Jemalloc,
            MIMALLOC => Allocator::Mimalloc,
            _ => decide(),
        }
    }

    #[cold]
    fn decide() -> Allocator {
        let allocator = if uses_jemalloc(process_name()) {
            Allocator::Jemalloc
        } else {
            Allocator::Mimalloc
        };
        let value = match allocator {
            Allocator::Jemalloc => JEMALLOC,
            Allocator::Mimalloc => MIMALLOC,
        };
        SELECTED.store(value, Ordering::Relaxed);
        allocator
    }

    fn uses_jemalloc(name: &[u8]) -> bool {
        name.starts_with(b"environmentd") || name.starts_with(b"balancerd")
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn process_name() -> &'static [u8] {
        unsafe extern "C" {
            static program_invocation_short_name: *const std::ffi::c_char;
        }
        // SAFETY: glibc sets `program_invocation_short_name` to the basename
        // of `argv[0]` before any user code runs, as a NUL-terminated string
        // that lives as long as the process. Reading it does not allocate.
        unsafe { std::ffi::CStr::from_ptr(program_invocation_short_name).to_bytes() }
    }

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    fn process_name() -> &'static [u8] {
        b""
    }
}

/// Delegates to the allocator [`allocator`] selects.
#[cfg(all(not(miri), any(feature = "jemalloc", feature = "mimalloc")))]
#[derive(Debug)]
pub struct Selected;

#[cfg(all(not(miri), any(feature = "jemalloc", feature = "mimalloc")))]
macro_rules! delegate {
    ($method:ident($($arg:expr),*)) => {
        match allocator() {
            #[cfg(feature = "jemalloc")]
            Some(Allocator::Jemalloc) => tikv_jemallocator::Jemalloc.$method($($arg),*),
            #[cfg(feature = "mimalloc")]
            Some(Allocator::Mimalloc) => mimalloc::MiMalloc.$method($($arg),*),
            _ => unreachable!("an allocator is compiled in"),
        }
    };
}

// SAFETY: every method forwards to the same allocator for the whole process
// lifetime, because `allocator` is fixed once decided, so memory is always
// returned to the allocator that produced it.
#[cfg(all(not(miri), any(feature = "jemalloc", feature = "mimalloc")))]
unsafe impl std::alloc::GlobalAlloc for Selected {
    #[inline]
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        // SAFETY: forwarded under the caller's contract.
        unsafe { delegate!(alloc(layout)) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        // SAFETY: forwarded under the caller's contract.
        unsafe { delegate!(alloc_zeroed(layout)) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: forwarded under the caller's contract.
        unsafe { delegate!(dealloc(ptr, layout)) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded under the caller's contract.
        unsafe { delegate!(realloc(ptr, layout, new_size)) }
    }
}

// NOTE: The workspace builds mimalloc with `no_thp`, so it never advises its
// arenas `MADV_HUGEPAGE`. With the advice, on hosts with transparent huge pages
// in `madvise` mode, untouched tails of huge pages count towards RSS: feature
// benchmarks measured clusterd at 2.4x jemalloc's memory. `MIMALLOC_ALLOW_THP=0`
// is no substitute, because mimalloc implements it with
// `prctl(PR_SET_THP_DISABLE)`, which also disables the huge pages the buffer
// pool requests. Swap splits huge pages, which return as base pages on swap-in.
#[cfg(all(not(miri), any(feature = "jemalloc", feature = "mimalloc")))]
#[global_allocator]
static ALLOC: mz_ore::alloc_track::TrackingAlloc<Selected> =
    mz_ore::alloc_track::TrackingAlloc(Selected);

/// Registers metrics for the global allocator into the provided registry,
/// and its introspection hooks with [`mz_ore::alloc_track`].
///
/// What metrics are registered varies by platform. Not all platforms use
/// allocators that support metrics.
#[allow(clippy::unused_async)]
pub async fn register_metrics_into(registry: &MetricsRegistry) {
    match allocator() {
        #[cfg(all(feature = "jemalloc", not(miri)))]
        Some(Allocator::Jemalloc) => {
            mz_ore::alloc_track::register_allocator(jemalloc_hooks::HOOKS);
            mz_prof::jemalloc::JemallocMetrics::register_into(registry).await;
        }
        #[cfg(all(feature = "mimalloc", not(miri)))]
        Some(Allocator::Mimalloc) => {
            mz_ore::alloc_track::register_allocator(mimalloc_hooks::HOOKS);
        }
        _ => {}
    }
    let _ = registry;
}
