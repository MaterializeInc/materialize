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

//! Benchmarks for factorized columnar storage.

use columnar::Len;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mz_timely_util::columnar::factorized::FactorizedColumns;

/// Generate sorted data with controllable repetition.
fn generate_sorted_data(n: usize, distinct_a: usize, distinct_b: usize) -> Vec<(u64, u64, i64)> {
    let mut data: Vec<(u64, u64, i64)> = Vec::with_capacity(n);
    for i in 0..n {
        data.push(((i % distinct_a) as u64, (i % distinct_b) as u64, i as i64));
    }
    data.sort();
    data
}

fn bench_push_flat(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/push_flat");
    for n in [10_000, 100_000] {
        let data = generate_sorted_data(n, n, n); // all distinct
        group.bench_with_input(BenchmarkId::new("n", n), &data, |b, data| {
            b.iter(|| {
                let mut fc: FactorizedColumns<u64, u64, i64> = Default::default();
                for (a, bb, cc) in data {
                    fc.push_flat(a, bb, cc);
                }
                fc
            });
        });
    }
    group.finish();
}

fn bench_form(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/form");
    for (n, da, db) in [(100_000, 100, 1_000), (100_000, 10, 100), (100_000, 1, 1)] {
        let data = generate_sorted_data(n, da, db);

        // Pre-build flat structure so we benchmark only form().
        let mut flat: FactorizedColumns<u64, u64, i64> = Default::default();
        for (a, b, cc) in &data {
            flat.push_flat(a, b, cc);
        }

        let label = format!("n={n}/da={da}/db={db}");
        group.bench_function(BenchmarkId::new("from_flat", &label), |b| {
            b.iter(|| {
                let refs: Vec<_> = flat.iter().collect();
                FactorizedColumns::<u64, u64, i64>::form(refs.into_iter())
            });
        });
    }
    group.finish();
}

fn bench_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/iter");
    for (n, da, db) in [(100_000, 100, 1_000), (100_000, 1, 1)] {
        let data = generate_sorted_data(n, da, db);
        let mut flat: FactorizedColumns<u64, u64, i64> = Default::default();
        for (a, b, cc) in &data {
            flat.push_flat(a, b, cc);
        }
        let refs: Vec<_> = flat.iter().collect();
        let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());

        let label = format!("n={n}/da={da}/db={db}");

        group.bench_function(BenchmarkId::new("flat", &label), |b| {
            b.iter(|| flat.iter().count())
        });

        group.bench_function(BenchmarkId::new("formed", &label), |b| {
            b.iter(|| fc.iter().count())
        });
    }
    group.finish();
}

fn bench_dedup_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/dedup_ratio");
    // Vary repetition level: from no dedup to extreme dedup.
    for (da, db) in [(100_000, 100_000), (1_000, 10_000), (100, 1_000), (10, 100)] {
        let n = 100_000;
        let data = generate_sorted_data(n, da, db);
        let mut flat: FactorizedColumns<u64, u64, i64> = Default::default();
        for (a, b, cc) in &data {
            flat.push_flat(a, b, cc);
        }

        let label = format!("da={da}/db={db}");
        group.bench_function(BenchmarkId::new("form", &label), |b| {
            b.iter(|| {
                let refs: Vec<_> = flat.iter().collect();
                let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());
                // Return lengths to prevent dead-code elimination.
                (
                    Len::len(&fc.lists.values),
                    Len::len(&fc.rest.lists.values),
                    fc.len(),
                )
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_push_flat,
    bench_form,
    bench_iter,
    bench_dedup_ratio,
);
criterion_main!(benches);
