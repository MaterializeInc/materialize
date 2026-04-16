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

use columnar::bytes::indexed;
use columnar::{Borrow, Len};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mz_timely_util::columnar::factorized::{FactorizedColumns, KVUpdates, KVUpdatesRepeats};

/// Generate sorted data with controllable repetition.
fn generate_sorted_data(n: usize, distinct_a: usize, distinct_b: usize) -> Vec<(u64, u64, i64)> {
    let mut data: Vec<(u64, u64, i64)> = Vec::with_capacity(n);
    for i in 0..n {
        data.push(((i % distinct_a) as u64, (i % distinct_b) as u64, i as i64));
    }
    data.sort();
    data
}

/// Compute total serialized size of a `FactorizedColumns` in u64 words.
fn total_words(fc: &FactorizedColumns<u64, u64, i64>) -> usize {
    indexed::length_in_words(&fc.lists.borrow())
        + indexed::length_in_words(&fc.rest.lists.borrow())
        + indexed::length_in_words(&fc.rest.rest.borrow())
}

fn bench_push_flat(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/push_flat");
    for n in [10_000, 100_000] {
        let data = generate_sorted_data(n, n, n); // all distinct
        group.throughput(Throughput::Elements(n as u64));
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

        // Build formed version to measure sizes.
        let refs: Vec<_> = flat.iter().collect();
        let formed = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());
        let flat_words = total_words(&flat);
        let formed_words = total_words(&formed);

        group.throughput(Throughput::Elements(n as u64));
        let label = format!("n={n}/da={da}/db={db}");
        group.bench_function(BenchmarkId::new("from_flat", &label), |b| {
            b.iter(|| {
                let refs: Vec<_> = flat.iter().collect();
                FactorizedColumns::<u64, u64, i64>::form(refs.into_iter())
            });
        });

        eprintln!(
            "  [{label}] flat: {flat_words} words ({} bytes), formed: {formed_words} words ({} bytes), ratio: {:.1}x",
            flat_words * 8,
            formed_words * 8,
            flat_words as f64 / formed_words as f64,
        );
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

        let flat_words = total_words(&flat);
        let formed_words = total_words(&fc);

        let label = format!("n={n}/da={da}/db={db}");

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("flat", &label), |b| {
            b.iter(|| flat.iter().count())
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("formed", &label), |b| {
            b.iter(|| fc.iter().count())
        });

        eprintln!(
            "  [{label}] flat: {flat_words} words, formed: {formed_words} words, A={}, B={}, C={}",
            Len::len(&fc.lists.values),
            Len::len(&fc.rest.lists.values),
            Len::len(&fc.rest.rest.values),
        );
    }
    group.finish();
}

fn bench_dedup_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/dedup_ratio");
    // Vary repetition level: from no dedup to extreme dedup.
    for (da, db) in [(100_000, 100_000), (1_000, 10_000), (100, 1_000), (10, 100)] {
        let n = 100_000usize;
        let data = generate_sorted_data(n, da, db);
        let mut flat: FactorizedColumns<u64, u64, i64> = Default::default();
        for (a, b, cc) in &data {
            flat.push_flat(a, b, cc);
        }

        // Build formed to measure sizes.
        let refs: Vec<_> = flat.iter().collect();
        let formed = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());
        let flat_words = total_words(&flat);
        let formed_words = total_words(&formed);

        group.throughput(Throughput::Elements(n as u64));
        let label = format!("da={da}/db={db}");
        group.bench_function(BenchmarkId::new("form", &label), |b| {
            b.iter(|| {
                let refs: Vec<_> = flat.iter().collect();
                let fc = FactorizedColumns::<u64, u64, i64>::form(refs.into_iter());
                (
                    Len::len(&fc.lists.values),
                    Len::len(&fc.rest.lists.values),
                    fc.len(),
                )
            });
        });

        eprintln!(
            "  [{label}] flat: {flat_words} words ({} bytes), formed: {formed_words} words ({} bytes), ratio: {:.1}x, A={}, B={}, C={}",
            flat_words * 8,
            formed_words * 8,
            flat_words as f64 / formed_words as f64,
            Len::len(&formed.lists.values),
            Len::len(&formed.rest.lists.values),
            formed.len(),
        );
    }
    group.finish();
}

/// Generate sorted K → V → (Time, Diff) data simulating real update patterns.
fn generate_kv_data(
    n: usize,
    n_keys: usize,
    n_vals: usize,
    n_times: usize,
) -> Vec<(u64, u64, (u64, i64))> {
    let mut data: Vec<(u64, u64, (u64, i64))> = Vec::with_capacity(n);
    for i in 0..n {
        data.push((
            (i % n_keys) as u64,
            (i % n_vals) as u64,
            ((i % n_times) as u64, 1i64),
        ));
    }
    data.sort();
    data
}

/// Compute total words for a KVUpdates structure.
fn kv_total_words<CC: Borrow + columnar::Container>(
    fc: &mz_timely_util::columnar::factorized::Level<
        u64,
        mz_timely_util::columnar::factorized::Level<
            u64,
            columnar::Vecs<CC, columnar::primitive::offsets::Strides>,
        >,
    >,
) -> usize
where
    for<'a> <CC as Borrow>::Borrowed<'a>: columnar::AsBytes<'a>,
{
    indexed::length_in_words(&fc.lists.borrow())
        + indexed::length_in_words(&fc.rest.lists.borrow())
        + indexed::length_in_words(&fc.rest.rest.borrow())
}

fn bench_kv_form(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/kv_form");
    // Realistic: 100 keys, 1000 vals, 5 distinct times, all +1 diffs.
    for (n, nk, nv, nt) in [
        (100_000, 100, 1_000, 5),
        (100_000, 100, 1_000, 100),
        (100_000, 10, 100, 5),
    ] {
        let data = generate_kv_data(n, nk, nv, nt);

        // Build plain flat.
        let mut plain_flat: KVUpdates<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            plain_flat.push_flat(k, v, (&td.0, &td.1));
        }
        // Build repeats flat.
        let mut repeat_flat: KVUpdatesRepeats<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            repeat_flat.push_flat(k, v, (&td.0, &td.1));
        }

        // Form both.
        let plain_refs: Vec<_> = plain_flat.iter().collect();
        let plain = KVUpdates::<u64, u64, u64, i64>::form(plain_refs.into_iter());
        let repeat_refs: Vec<_> = repeat_flat.iter().collect();
        let repeat = KVUpdatesRepeats::<u64, u64, u64, i64>::form(repeat_refs.into_iter());

        let plain_words = kv_total_words(&plain);
        let repeat_words = kv_total_words(&repeat);

        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("plain", &label), |b| {
            b.iter(|| {
                let refs: Vec<_> = plain_flat.iter().collect();
                KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter())
            });
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("repeats", &label), |b| {
            b.iter(|| {
                let refs: Vec<_> = repeat_flat.iter().collect();
                KVUpdatesRepeats::<u64, u64, u64, i64>::form(refs.into_iter())
            });
        });

        eprintln!(
            "  [{label}] plain: {plain_words} words ({} bytes), repeats: {repeat_words} words ({} bytes), ratio: {:.1}x",
            plain_words * 8,
            repeat_words * 8,
            plain_words as f64 / repeat_words as f64,
        );
    }
    group.finish();
}

fn bench_kv_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/kv_iter");
    for (n, nk, nv, nt) in [(100_000, 100, 1_000, 5), (100_000, 10, 100, 5)] {
        let data = generate_kv_data(n, nk, nv, nt);

        let mut plain_flat: KVUpdates<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            plain_flat.push_flat(k, v, (&td.0, &td.1));
        }
        let refs: Vec<_> = plain_flat.iter().collect();
        let plain = KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());

        let mut repeat_flat: KVUpdatesRepeats<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            repeat_flat.push_flat(k, v, (&td.0, &td.1));
        }
        let refs: Vec<_> = repeat_flat.iter().collect();
        let repeat = KVUpdatesRepeats::<u64, u64, u64, i64>::form(refs.into_iter());

        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("plain/iter", &label), |b| {
            b.iter(|| plain.iter().count())
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("plain/cursor", &label), |b| {
            b.iter(|| {
                let mut count = 0usize;
                plain.for_each_cursor(|_, _, _| count += 1);
                count
            })
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("repeats/iter", &label), |b| {
            b.iter(|| repeat.iter().count())
        });

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("repeats/cursor", &label), |b| {
            b.iter(|| {
                let mut count = 0usize;
                repeat.for_each_cursor(|_, _, _| count += 1);
                count
            })
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
    bench_kv_form,
    bench_kv_iter,
);
criterion_main!(benches);
