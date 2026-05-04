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

#![cfg(feature = "pager")]

use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mz_ore::cast::CastFrom;
use mz_ore::pager::{self, Backend, Handle};

const PAGE_BYTES: usize = 4096;
const PAGE_U64S: usize = PAGE_BYTES / 8;

fn ensure_scratch() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let dir: PathBuf = std::env::var_os("MZ_PAGER_SCRATCH")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        pager::set_scratch_dir(dir);
    });
}

fn fill_payload(len_u64s: usize) -> Vec<u64> {
    (0..u64::cast_from(len_u64s)).collect()
}

/// Reads one `u64` from each page of `buf` to force the kernel to fault them in.
/// Returns a side-effecting sum so the compiler cannot elide the loads.
fn touch_every_page(buf: &[u64]) -> u64 {
    let mut s: u64 = 0;
    let mut i = 0;
    while i < buf.len() {
        s = s.wrapping_add(buf[i]);
        i += PAGE_U64S;
    }
    s
}

/// Round-trip a single-chunk payload through the pager and touch every page on
/// readback. Reuses the buffer between iterations so allocation/page-fault tax
/// is paid once at setup, not measured.
fn round_trip_single(c: &mut Criterion) {
    ensure_scratch();
    let mut group = c.benchmark_group("pager/round_trip_touch/single");
    group.measurement_time(Duration::from_secs(5));
    for size_kib in [4usize, 64, 1024, 2048, 16384] {
        let len = (size_kib * 1024) / 8;
        for backend in [Backend::Swap, Backend::File] {
            pager::set_backend(backend);
            group.throughput(Throughput::Bytes(u64::cast_from(size_kib * 1024)));
            group.bench_function(BenchmarkId::new(format!("{backend:?}"), size_kib), |b| {
                b.iter_custom(|iters| {
                    let mut payload = fill_payload(len);
                    let mut tmp: Vec<u64> = Vec::with_capacity(len);
                    let start = Instant::now();
                    for _ in 0..iters {
                        let mut chunks = [std::mem::take(&mut payload)];
                        let h: Handle = pager::pageout(&mut chunks);
                        pager::take(h, &mut tmp);
                        black_box(touch_every_page(&tmp));
                        payload = std::mem::take(&mut tmp);
                        tmp = Vec::with_capacity(len);
                    }
                    start.elapsed()
                });
            });
        }
    }
    group.finish();
}

/// Round-trip a scatter-input (multiple chunks forming one logical 2 MiB block).
/// Measures the same touch-every-page readback pattern as `round_trip_single`.
fn round_trip_scatter_2mib(c: &mut Criterion) {
    ensure_scratch();
    let mut group = c.benchmark_group("pager/round_trip_touch/scatter_2MiB");
    group.measurement_time(Duration::from_secs(5));
    let total_bytes: usize = 2 * 1024 * 1024;
    for chunk_count in [1usize, 2, 8, 64] {
        let chunk_bytes = total_bytes / chunk_count;
        let chunk_len_u64s = chunk_bytes / 8;
        for backend in [Backend::Swap, Backend::File] {
            pager::set_backend(backend);
            group.throughput(Throughput::Bytes(u64::cast_from(total_bytes)));
            group.bench_function(BenchmarkId::new(format!("{backend:?}"), chunk_count), |b| {
                b.iter_custom(|iters| {
                    let mut payload: Vec<Vec<u64>> = (0..chunk_count)
                        .map(|_| fill_payload(chunk_len_u64s))
                        .collect();
                    let mut tmp: Vec<u64> = Vec::with_capacity(total_bytes / 8);
                    let start = Instant::now();
                    for _ in 0..iters {
                        let h: Handle = pager::pageout(payload.as_mut_slice());
                        pager::take(h, &mut tmp);
                        black_box(touch_every_page(&tmp));
                        // Rebuild the input from `tmp` for the next iteration:
                        // swap the consolidated buffer back into chunk 0, leave
                        // the other chunks empty (they were drained by the
                        // swap backend's `mem::take`). The file backend already
                        // preserved their capacity, so this still amortizes its
                        // allocation cost.
                        payload[0] = std::mem::take(&mut tmp);
                        tmp = Vec::with_capacity(total_bytes / 8);
                        // For chunk_count > 1, refill the trailing chunks by
                        // splitting payload[0] back into the original shape.
                        if chunk_count > 1 {
                            let mut head = std::mem::take(&mut payload[0]);
                            for i in 1..chunk_count {
                                let take_len = std::cmp::min(chunk_len_u64s, head.len());
                                let tail = head.split_off(head.len() - take_len);
                                payload[i] = tail;
                            }
                            payload[0] = head;
                        }
                    }
                    start.elapsed()
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, round_trip_single, round_trip_scatter_2mib);
criterion_main!(benches);
