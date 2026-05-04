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
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mz_ore::cast::CastFrom;
use mz_ore::pager::{self, Backend};

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

fn bench_pageout_single(c: &mut Criterion) {
    ensure_scratch();
    let mut group = c.benchmark_group("pager/pageout/single_chunk");
    group.measurement_time(Duration::from_secs(2));
    for size_kib in [4usize, 64, 1024, 16384] {
        let len = (size_kib * 1024) / 8;
        for backend in [Backend::Swap, Backend::File] {
            pager::set_backend(backend);
            let total_bytes = u64::cast_from(size_kib * 1024);
            group.throughput(Throughput::Bytes(total_bytes));
            group.bench_function(BenchmarkId::new(format!("{backend:?}"), size_kib), |b| {
                b.iter_batched(
                    || [fill_payload(len)],
                    |mut chunks| {
                        let h = pager::pageout(&mut chunks);
                        black_box(h);
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

fn bench_pageout_scatter(c: &mut Criterion) {
    ensure_scratch();
    let mut group = c.benchmark_group("pager/pageout/scatter_2MiB");
    group.measurement_time(Duration::from_secs(2));
    let total_bytes: usize = 2 * 1024 * 1024;
    for chunk_count in [1usize, 2, 64] {
        let chunk_bytes = total_bytes / chunk_count;
        let chunk_len_u64s = chunk_bytes / 8;
        for backend in [Backend::Swap, Backend::File] {
            pager::set_backend(backend);
            let total_bytes_u64 = u64::cast_from(total_bytes);
            group.throughput(Throughput::Bytes(total_bytes_u64));
            group.bench_function(BenchmarkId::new(format!("{backend:?}"), chunk_count), |b| {
                b.iter_batched(
                    || {
                        (0..chunk_count)
                            .map(|_| fill_payload(chunk_len_u64s))
                            .collect::<Vec<_>>()
                    },
                    |mut chunks| {
                        let h = pager::pageout(chunks.as_mut_slice());
                        black_box(h);
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

fn bench_round_trip(c: &mut Criterion) {
    ensure_scratch();
    let mut group = c.benchmark_group("pager/round_trip");
    group.measurement_time(Duration::from_secs(3));
    let len = (256 * 1024) / 8;
    for backend in [Backend::Swap, Backend::File] {
        pager::set_backend(backend);
        group.throughput(Throughput::Bytes(256 * 1024));
        group.bench_function(BenchmarkId::new(format!("{backend:?}"), 256), |b| {
            b.iter_batched(
                || [fill_payload(len)],
                |mut chunks| {
                    let h = pager::pageout(&mut chunks);
                    let mut dst = Vec::new();
                    pager::take(h, &mut dst);
                    black_box(dst);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_pageout_single,
    bench_pageout_scatter,
    bench_round_trip
);
criterion_main!(benches);
