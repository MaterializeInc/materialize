// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Throughput benchmark for [`mz_timely_util::column_pager`].
//!
//! Two shapes:
//!
//! * `rt`: round-trip cost of `page` immediately followed by `take` on a
//!   freshly built column. Measures the full encode/decode path including
//!   pager I/O and (for the lz4 axis) compression and decompression.
//! * `loop_`: operator-loop cost — refill an existing column then `page` it,
//!   without `take`. Mirrors how a spill operator amortizes allocations and
//!   measures the cheaper write-only side of the cycle.
//!
//! Axes:
//!
//! * Column size, in target uncompressed bytes: 4 KiB, 256 KiB, 4 MiB.
//! * Pager backend: `Swap`, `File`.
//! * Codec: uncompressed, lz4.
//!
//! ## Caveat: swap backend numbers are the warm fast path
//!
//! The pager's swap backend keeps the body `Vec<u64>` resident and hints
//! `MADV_COLD` to the kernel. This bench round-trips one column at a time
//! and never accumulates enough working set to exceed system RAM, so the
//! kernel never actually evicts. Swap-backend results therefore measure
//! `pageout = move-Vec-into-handle` and `take = move-Vec-out` plus
//! bookkeeping — essentially memcpy at the configured size — not the real
//! cost of a page-in from disk under memory pressure.
//!
//! To distinguish the cases, swap-backend results are labelled
//! `swap-warm` rather than `swap`. A separate `column_pager_pressure`
//! bench (TODO) will hold many paged handles alive under a constrained
//! cgroup (`systemd-run --user --scope -p MemoryMax=...`) so the kernel
//! is forced to evict, and time `take` on a cold handle.
//!
//! Run with:
//!
//!     cargo bench -p mz-timely-util --bench column_pager

use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use mz_ore::pager::{self, Backend};
use mz_timely_util::column_pager::{
    Codec, ColumnPager, PageDecision, PageEvent, PageHint, PagedColumn, PagingPolicy,
};
use mz_timely_util::columnar::Column;
use timely::container::PushInto;
use timely::dataflow::channels::ContainerBytes;

/// Stub policy that always returns the configured decision. Records nothing.
struct AlwaysPage {
    backend: Backend,
    codec: Option<Codec>,
}

impl PagingPolicy for AlwaysPage {
    fn decide(&self, _hint: PageHint) -> PageDecision {
        PageDecision::Page {
            backend: self.backend,
            codec: self.codec,
        }
    }
    fn record(&self, _event: PageEvent) {}
}

/// Builds a `Column<i64>` whose serialized byte size is approximately
/// `target_bytes`. The actual size is reported by [`ContainerBytes::length_in_bytes`]
/// and used for throughput accounting.
fn build_column(target_bytes: usize) -> Column<i64> {
    // i64 typed columns serialize to roughly 8 bytes per element plus header
    // overhead. Aim a touch high and trust `length_in_bytes` for accounting.
    let n = i64::try_from((target_bytes / 8).max(1)).expect("fits in i64");
    let mut c: Column<i64> = Default::default();
    for v in 0..n {
        c.push_into(v);
    }
    c
}

fn label(prefix: &str, target: usize, backend: Backend, codec: Option<Codec>) -> String {
    let size = match target {
        n if n >= 1 << 20 => format!("{}MiB", n >> 20),
        n if n >= 1 << 10 => format!("{}KiB", n >> 10),
        n => format!("{n}B"),
    };
    let codec = match codec {
        None => "raw",
        Some(Codec::Lz4) => "lz4",
    };
    // `swap-warm` flags that this measures the in-memory fast path: the
    // bench never builds enough working set to push the system into actual
    // swap eviction, so swap-backend numbers reflect pageout/pagein as
    // memcpy + bookkeeping, not kernel paging cost. See module docs.
    let backend = match backend {
        Backend::Swap => "swap-warm",
        Backend::File => "file",
    };
    format!("{prefix}/{size}/{backend}/{codec}")
}

fn bench_round_trip(c: &mut Criterion, target: usize, backend: Backend, codec: Option<Codec>) {
    let policy: Arc<dyn PagingPolicy> = Arc::new(AlwaysPage { backend, codec });
    let cp = ColumnPager::new(policy);
    let prototype = build_column(target);
    let actual_bytes = prototype.length_in_bytes();

    let mut group = c.benchmark_group("column_pager");
    group.throughput(Throughput::Bytes(u64::try_from(actual_bytes).unwrap()));
    group.bench_function(label("rt", target, backend, codec), |b| {
        b.iter_batched(
            || build_column(target),
            |mut col| {
                let p = cp.page(&mut col);
                let _ = cp.take(p);
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_loop(c: &mut Criterion, target: usize, backend: Backend, codec: Option<Codec>) {
    let policy: Arc<dyn PagingPolicy> = Arc::new(AlwaysPage { backend, codec });
    let cp = ColumnPager::new(policy);
    let prototype = build_column(target);
    let actual_bytes = prototype.length_in_bytes();

    let mut group = c.benchmark_group("column_pager");
    group.throughput(Throughput::Bytes(u64::try_from(actual_bytes).unwrap()));
    group.bench_function(label("loop", target, backend, codec), |b| {
        let mut col = build_column(target);
        b.iter(|| {
            // Operator loop: refill the column, then page it. Drop the paged
            // result without `take`, simulating a write-only spill operator.
            if col.length_in_bytes() == 0 {
                col = build_column(target);
            }
            let paged: PagedColumn<i64> = cp.page(&mut col);
            // Refill before next iteration so the column carries data again.
            col = build_column(target);
            std::mem::drop(paged);
        });
    });
    group.finish();
}

fn benches(c: &mut Criterion) {
    // The File backend writes to a scratch directory chosen at process
    // startup; tests do this via `tempfile`. For the bench we use the
    // platform default, which `pager::file` will create under
    // `/tmp/<pid>/...` if no override is set.
    let scratch = std::env::temp_dir().join(format!("column-pager-bench-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&scratch);
    pager::set_scratch_dir(scratch);

    let sizes = [4 * 1024, 256 * 1024, 4 * 1024 * 1024];
    let backends = [Backend::Swap, Backend::File];
    let codecs = [None, Some(Codec::Lz4)];

    for &size in &sizes {
        for &backend in &backends {
            for &codec in &codecs {
                bench_round_trip(c, size, backend, codec);
                bench_loop(c, size, backend, codec);
            }
        }
    }
}

criterion_group!(column_pager_benches, benches);
criterion_main!(column_pager_benches);
