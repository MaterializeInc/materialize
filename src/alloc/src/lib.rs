// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Chooses a global memory allocator based on Cargo features.

use mz_ore::metrics::MetricsRegistry;

#[cfg(all(feature = "jemalloc", not(feature = "mimalloc"), not(miri)))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// NOTE: The workspace builds mimalloc with `no_thp`, so it never advises its
// arenas `MADV_HUGEPAGE`. With the advice, on hosts with transparent huge pages
// in `madvise` mode, untouched tails of huge pages count towards RSS: feature
// benchmarks measured clusterd at 2.4x jemalloc's memory. `MIMALLOC_ALLOW_THP=0`
// is no substitute, because mimalloc implements it with
// `prctl(PR_SET_THP_DISABLE)`, which also disables the huge pages the buffer
// pool requests. Swap splits huge pages, which return as base pages on swap-in.
#[cfg(all(feature = "mimalloc", not(miri)))]
#[global_allocator]
static ALLOC: mz_ore::alloc_track::TrackingAlloc<mimalloc::MiMalloc> =
    mz_ore::alloc_track::TrackingAlloc(mimalloc::MiMalloc);

#[cfg(all(feature = "mimalloc", not(miri)))]
mod mimalloc_hooks {
    use std::ffi::{CStr, c_char, c_void};

    use mz_ore::alloc_track::AllocatorHooks;

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

    /// Per block-size bucket: pages, pages with no used block, page bytes,
    /// and used block bytes.
    type Buckets = [(usize, usize, usize, usize); BUCKET_BOUNDS.len()];
    const BUCKET_BOUNDS: [usize; 7] = [
        64,
        1 << 10,
        8 << 10,
        64 << 10,
        512 << 10,
        8 << 20,
        usize::MAX,
    ];

    struct Walk {
        buckets: Buckets,
        /// Block area start, capacity bytes, and reserved bytes per page.
        /// Preallocated: the visitor must not allocate while mimalloc walks
        /// its pages.
        ranges: Vec<(usize, usize, usize)>,
    }

    /// # Safety
    ///
    /// `area` must point to a valid area and `arg` to a live `Walk` with no
    /// other references.
    unsafe extern "C" fn visit_area(
        _heap: *const libmimalloc_sys::mi_heap_t,
        area: *const libmimalloc_sys::mi_heap_area_t,
        _block: *mut c_void,
        _block_size: usize,
        arg: *mut c_void,
    ) -> bool {
        // SAFETY: the function contract.
        let (area, walk) = unsafe { (&*area, &mut *arg.cast::<Walk>()) };
        let i = BUCKET_BOUNDS
            .iter()
            .position(|&b| area.block_size <= b)
            .unwrap_or(BUCKET_BOUNDS.len() - 1);
        let b = &mut walk.buckets[i];
        b.0 += 1;
        b.1 += usize::from(area.used == 0);
        b.2 += area.committed;
        b.3 += area.used * area.full_block_size;
        if walk.ranges.len() < walk.ranges.capacity() {
            walk.ranges
                .push((area.blocks.addr(), area.committed, area.reserved));
        }
        true
    }

    /// Resident bytes in `[start, start + len)`, rounded out to pages.
    fn resident(start: usize, len: usize) -> usize {
        const PAGE: usize = 4096;
        if len == 0 {
            return 0;
        }
        let lo = start & !(PAGE - 1);
        let hi = (start + len).next_multiple_of(PAGE);
        let mut vec = vec![0u8; (hi - lo) / PAGE];
        // SAFETY: `vec` holds one byte per page of the range, and mincore
        // only reads the page tables of the range.
        let rc = unsafe {
            libc::mincore(
                std::ptr::without_provenance_mut(lo),
                hi - lo,
                vec.as_mut_ptr(),
            )
        };
        if rc != 0 {
            return 0;
        }
        vec.iter().filter(|&&v| v & 1 != 0).count() * PAGE
    }

    fn stats() -> String {
        use std::fmt::Write;
        let mut out = String::new();
        // SAFETY: `append` upholds the output function contract, and `out`
        // outlives the call, which invokes `append` only synchronously.
        unsafe { libmimalloc_sys::mi_stats_print_out(Some(append), (&raw mut out).cast()) };
        // NOTE: mimalloc walks the pages assuming no concurrent mutation.
        // Without visiting blocks it only reads page headers, so a racing
        // thread makes the counts approximate, which is all this is for.
        let mut walk = Walk {
            buckets: Default::default(),
            ranges: Vec::with_capacity(1 << 20),
        };
        // SAFETY: `visit_area` upholds the visitor contract, and `walk`
        // outlives the synchronous walk. A null heap selects the main heap.
        unsafe {
            libmimalloc_sys::mi_heap_visit_blocks(
                std::ptr::null_mut(),
                false,
                Some(visit_area),
                (&raw mut walk).cast(),
            );
        }
        writeln!(
            out,
            "\npage walk by block size: pages, empty pages, page MiB, used MiB"
        )
        .unwrap();
        let mib = |b: usize| f64::from(u32::try_from(b >> 10).unwrap_or(u32::MAX)) / 1024.0;
        let mut total = (0, 0, 0, 0);
        for (i, b) in walk.buckets.into_iter().enumerate() {
            let bound = BUCKET_BOUNDS[i];
            writeln!(
                out,
                "  <= {bound:>20}: {:6} {:6} {:9.1} {:9.1}",
                b.0,
                b.1,
                mib(b.2),
                mib(b.3)
            )
            .unwrap();
            total = (total.0 + b.0, total.1 + b.1, total.2 + b.2, total.3 + b.3);
        }
        writeln!(
            out,
            "  total                  : {:6} {:6} {:9.1} {:9.1}",
            total.0,
            total.1,
            mib(total.2),
            mib(total.3)
        )
        .unwrap();
        let (mut in_capacity, mut beyond_capacity, mut reserved) = (0, 0, 0);
        for &(start, committed, res) in &walk.ranges {
            in_capacity += resident(start, committed);
            beyond_capacity += resident(start + committed, res.saturating_sub(committed));
            reserved += res;
        }
        writeln!(
            out,
            "page residency: {:.1} MiB within capacity, {:.1} MiB beyond capacity, {:.1} MiB reserved",
            mib(in_capacity),
            mib(beyond_capacity),
            mib(reserved),
        )
        .unwrap();
        out
    }

    fn collect() {
        // SAFETY: no preconditions.
        unsafe { libmimalloc_sys::mi_collect(true) };
    }
}

/// Registers metrics for the global allocator into the provided registry,
/// and its introspection hooks with [`mz_ore::alloc_track`].
///
/// What metrics are registered varies by platform. Not all platforms use
/// allocators that support metrics.
#[allow(clippy::unused_async)]
pub async fn register_metrics_into(registry: &MetricsRegistry) {
    #[cfg(all(feature = "mimalloc", not(miri)))]
    mz_ore::alloc_track::register_allocator(mimalloc_hooks::HOOKS);
    #[cfg(all(feature = "jemalloc", not(miri)))]
    mz_prof::jemalloc::JemallocMetrics::register_into(registry).await;
    let _ = registry;
}
