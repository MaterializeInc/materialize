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
use columnar::{Borrow, FromBytes, Index, Len};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mz_timely_util::columnar::factorized::{
    FactorizedColumns, KVUpdates, KVUpdatesRepeats, Lists, child_range,
};

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
    let b = fc.borrowed();
    indexed::length_in_words(&b.lists)
        + indexed::length_in_words(&b.rest.lists)
        + indexed::length_in_words(&b.rest.rest)
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
fn kv_total_words<
    KV: columnar::ContainerBytes,
    VV: columnar::ContainerBytes,
    CC: columnar::ContainerBytes,
>(
    fc: &mz_timely_util::columnar::factorized::Level<
        columnar::Vecs<KV, columnar::primitive::offsets::Strides>,
        mz_timely_util::columnar::factorized::Level<
            columnar::Vecs<VV, columnar::primitive::offsets::Strides>,
            columnar::Vecs<CC, columnar::primitive::offsets::Strides>,
        >,
    >,
) -> usize {
    let b = fc.borrowed();
    indexed::length_in_words(&b.lists)
        + indexed::length_in_words(&b.rest.lists)
        + indexed::length_in_words(&b.rest.rest)
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

/// Serialize a `Lists<CC>` into a `Vec<u64>` store.
fn serialize_lists<CC: columnar::ContainerBytes>(lists: &Lists<CC>) -> Vec<u64> {
    let mut store = Vec::new();
    indexed::encode(&mut store, &lists.borrow());
    store
}

/// Helper: decode a serialized `Lists` store into its borrowed form and iterate with cursor.
macro_rules! for_each_serialized {
    ($a_store:expr, $b_store:expr, $c_store:expr, $at:ty, $bt:ty, $ct:ty, $f:expr) => {{
        type BorrowedStrides<'a> = columnar::primitive::offsets::Strides<&'a [u64], &'a [u64]>;
        let a_ds = indexed::DecodedStore::new($a_store);
        let b_ds = indexed::DecodedStore::new($b_store);
        let c_ds = indexed::DecodedStore::new($c_store);
        let a_lists: columnar::Vecs<$at, BorrowedStrides<'_>> =
            FromBytes::from_store(&a_ds, &mut 0);
        let b_lists: columnar::Vecs<$bt, BorrowedStrides<'_>> =
            FromBytes::from_store(&b_ds, &mut 0);
        let c_lists: columnar::Vecs<$ct, BorrowedStrides<'_>> =
            FromBytes::from_store(&c_ds, &mut 0);
        for_each_on_borrowed(a_lists, b_lists, c_lists, $f);
    }};
}

/// Iterate three borrowed `Vecs` levels using cursor-based leaf traversal.
fn for_each_on_borrowed<AL, BL, CL>(
    a_lists: columnar::Vecs<AL, columnar::primitive::offsets::Strides<&[u64], &[u64]>>,
    b_lists: columnar::Vecs<BL, columnar::primitive::offsets::Strides<&[u64], &[u64]>>,
    c_lists: columnar::Vecs<CL, columnar::primitive::offsets::Strides<&[u64], &[u64]>>,
    mut f: impl FnMut(AL::Ref, BL::Ref, CL::Ref),
) where
    AL: Index + Copy,
    BL: Index + Copy,
    CL: Index + Copy,
    AL::Ref: Copy,
    BL::Ref: Copy,
{
    for outer in 0..Len::len(&a_lists) {
        for a_idx in child_range(a_lists.bounds, outer) {
            let a_val = a_lists.values.get(a_idx);
            for b_idx in child_range(b_lists.bounds, a_idx) {
                let b_val = b_lists.values.get(b_idx);
                let range = child_range(c_lists.bounds, b_idx);
                for c_val in c_lists.values.cursor(range) {
                    f(a_val, b_val, c_val);
                }
            }
        }
    }
}

fn bench_kv_serialized(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/kv_serialized");
    for (n, nk, nv, nt) in [(100_000, 100, 1_000, 5), (100_000, 10, 100, 5)] {
        let data = generate_kv_data(n, nk, nv, nt);

        // Build and form plain.
        let mut plain_flat: KVUpdates<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            plain_flat.push_flat(k, v, (&td.0, &td.1));
        }
        let refs: Vec<_> = plain_flat.iter().collect();
        let plain = KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());

        // Build and form repeats.
        let mut repeat_flat: KVUpdatesRepeats<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            repeat_flat.push_flat(k, v, (&td.0, &td.1));
        }
        let refs: Vec<_> = repeat_flat.iter().collect();
        let repeat = KVUpdatesRepeats::<u64, u64, u64, i64>::form(refs.into_iter());

        // Serialize each level.
        let plain_a = serialize_lists(&plain.lists);
        let plain_b = serialize_lists(&plain.rest.lists);
        let plain_c = serialize_lists(&plain.rest.rest);
        let repeat_a = serialize_lists(&repeat.lists);
        let repeat_b = serialize_lists(&repeat.rest.lists);
        let repeat_c = serialize_lists(&repeat.rest.rest);

        let plain_bytes = (plain_a.len() + plain_b.len() + plain_c.len()) * 8;
        let repeat_bytes = (repeat_a.len() + repeat_b.len() + repeat_c.len()) * 8;

        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        // Typed cursor (baseline).
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("plain/typed", &label), |b| {
            b.iter(|| {
                let mut count = 0usize;
                plain.for_each_cursor(|_, _, _| count += 1);
                count
            })
        });

        // Serialized cursor plain.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("plain/serialized", &label), |b| {
            b.iter(|| {
                let mut count = 0usize;
                for_each_serialized!(
                    &plain_a,
                    &plain_b,
                    &plain_c,
                    &[u64],
                    &[u64],
                    (&[u64], &[i64]),
                    |_: &u64, _: &u64, _: (&u64, &i64)| count += 1
                );
                count
            })
        });

        // Typed cursor repeats.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("repeats/typed", &label), |b| {
            b.iter(|| {
                let mut count = 0usize;
                repeat.for_each_cursor(|_, _, _| count += 1);
                count
            })
        });

        // Serialized cursor repeats.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("repeats/serialized", &label), |b| {
            b.iter(|| {
                let mut count = 0usize;
                type BorrowedRepeatsU64<'a> =
                    columnar::Repeats<&'a [u64], &'a [u64], &'a [u64], &'a [u64]>;
                type BorrowedRepeatsI64<'a> =
                    columnar::Repeats<&'a [i64], &'a [u64], &'a [u64], &'a [u64]>;
                for_each_serialized!(
                    &repeat_a,
                    &repeat_b,
                    &repeat_c,
                    &[u64],
                    &[u64],
                    (BorrowedRepeatsU64<'_>, BorrowedRepeatsI64<'_>),
                    |_: &u64, _: &u64, _: (&u64, &i64)| count += 1
                );
                count
            })
        });

        eprintln!(
            "  [{label}] plain: {plain_bytes} bytes, repeats: {repeat_bytes} bytes, ratio: {:.1}x",
            plain_bytes as f64 / repeat_bytes as f64,
        );
    }
    group.finish();
}

/// Benchmark 1: Sort cost in isolation.
///
/// Measures the cost of collecting refs from a flat FactorizedColumns and sorting them.
/// This is the new overhead the builder pays that the current ColumnBuilder does not.
fn bench_sort_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/sort_cost");
    for (n, nk, nv, nt) in [(100_000, 100, 1_000, 5), (100_000, 10, 100, 5)] {
        let data = generate_kv_data(n, nk, nv, nt);

        // Build flat (unsorted — simulate random arrival order).
        let mut shuffled = data.clone();
        // Deterministic unshuffle: reverse to break sortedness.
        shuffled.reverse();

        let mut flat: KVUpdates<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &shuffled {
            flat.push_flat(k, v, (&td.0, &td.1));
        }

        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        // Just the sort (collect + sort, no form).
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("sort_refs", &label), |b| {
            b.iter(|| {
                let mut refs: Vec<_> = flat.iter().collect();
                refs.sort();
                refs.len()
            })
        });

        // Sort + form (the full builder path minus serialization).
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("sort_and_form", &label), |b| {
            b.iter(|| {
                let mut refs: Vec<_> = flat.iter().collect();
                refs.sort();
                KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter())
            })
        });
    }
    group.finish();
}

/// Benchmark 2: Full builder pipeline comparison.
///
/// Compares the proposed factorized path (push → sort → form → serialize) against
/// the current flat path (push → serialize without sort). Measures wall time and
/// reports output sizes.
fn bench_builder_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/builder_pipeline");
    for (n, nk, nv, nt) in [(100_000, 100, 1_000, 5), (100_000, 10, 100, 5)] {
        let data = generate_kv_data(n, nk, nv, nt);

        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        // Current path: push flat → serialize (no sort, no form).
        // Simulates what ColumnBuilder does today.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("flat_serialize", &label), |b| {
            b.iter(|| {
                let mut flat: KVUpdates<u64, u64, u64, i64> = Default::default();
                for (k, v, td) in &data {
                    flat.push_flat(k, v, (&td.0, &td.1));
                }
                let borrowed = flat.borrowed();
                let mut store = Vec::new();
                indexed::encode(&mut store, &borrowed);
                store.len()
            })
        });

        // Proposed path: push flat → sort → form → serialize.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("sort_form_serialize", &label), |b| {
            b.iter(|| {
                let mut flat: KVUpdates<u64, u64, u64, i64> = Default::default();
                for (k, v, td) in &data {
                    flat.push_flat(k, v, (&td.0, &td.1));
                }
                let mut refs: Vec<_> = flat.iter().collect();
                refs.sort();
                let formed = KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());
                let borrowed = formed.borrowed();
                let mut store = Vec::new();
                indexed::encode(&mut store, &borrowed);
                store.len()
            })
        });

        // Measure output sizes for comparison.
        let mut flat: KVUpdates<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &data {
            flat.push_flat(k, v, (&td.0, &td.1));
        }
        let mut refs: Vec<_> = flat.iter().collect();
        refs.sort();
        let formed = KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());

        let flat_words = kv_total_words(&flat);
        let formed_words = kv_total_words(&formed);

        eprintln!(
            "  [{label}] flat output: {} bytes, formed output: {} bytes, ratio: {:.1}x",
            flat_words * 8,
            formed_words * 8,
            flat_words as f64 / formed_words as f64,
        );
    }
    group.finish();
}

/// Single-pass counting sort by one byte of a u64 key.
/// Partitions `src` into `dst` by the byte at `shift`, returns number of non-empty buckets.
fn counting_sort_pass<T: Copy>(src: &[T], dst: &mut Vec<T>, key: &impl Fn(&T) -> u64, shift: u32) {
    dst.clear();
    dst.resize_with(src.len(), || src[0]); // safe fill, no unsafe

    let mut counts = [0u32; 256];
    for item in src.iter() {
        counts[((key(item) >> shift) & 0xFF) as usize] += 1;
    }

    let mut offsets = [0u32; 256];
    for i in 1..256 {
        offsets[i] = offsets[i - 1] + counts[i - 1];
    }

    for item in src.iter() {
        let byte = ((key(item) >> shift) & 0xFF) as usize;
        let pos = offsets[byte] as usize;
        offsets[byte] += 1;
        dst[pos] = *item;
    }
}

/// Radix sort on first-column prefix, then comparison sort within runs.
///
/// For `(K, V, (T, D))` tuples: radix sort by K (u64), then std sort
/// each run of equal K by (V, T, D). For variable-length keys (Row),
/// the prefix would be the first 8 bytes.
/// Single MSB byte pass to bucket by first column prefix, then comparison sort each bucket.
fn radix_then_comparison_sort<T: Ord + Copy>(data: &mut Vec<T>, prefix_key: impl Fn(&T) -> u64) {
    if data.len() <= 1 {
        return;
    }
    let mut buf = Vec::new();
    // Sort by top byte of the prefix key (MSB pass).
    counting_sort_pass(data, &mut buf, &prefix_key, 56);
    std::mem::swap(data, &mut buf);

    // Identify runs of equal top byte and comparison sort each.
    let mut start = 0;
    while start < data.len() {
        let top_byte = (prefix_key(&data[start]) >> 56) & 0xFF;
        let mut end = start + 1;
        while end < data.len() && ((prefix_key(&data[end]) >> 56) & 0xFF) == top_byte {
            end += 1;
        }
        if end - start > 1 {
            data[start..end].sort();
        }
        start = end;
    }
}

/// Full LSB radix sort (all 8 bytes) on first column, then comparison sort within runs.
fn full_radix_then_comparison_sort<T: Ord + Copy>(
    data: &mut Vec<T>,
    prefix_key: impl Fn(&T) -> u64,
) {
    if data.len() <= 1 {
        return;
    }
    // Count bytes at all 8 positions in one scan (datatoad approach).
    let mut counts = [[0u32; 256]; 8];
    for item in data.iter() {
        let k = prefix_key(item);
        for pass in 0..8u32 {
            counts[pass as usize][((k >> (pass * 8)) & 0xFF) as usize] += 1;
        }
    }

    // Only do passes where >1 distinct byte value exists.
    let active: Vec<u32> = (0..8u32)
        .filter(|&pass| counts[pass as usize].iter().filter(|&&c| c > 0).count() > 1)
        .collect();

    let mut buf = Vec::new();
    let mut swapped = false;
    for &pass in &active {
        counting_sort_pass(data, &mut buf, &prefix_key, pass * 8);
        std::mem::swap(data, &mut buf);
        swapped = !swapped;
    }
    // If odd number of swaps, result is in the wrong vec.
    if swapped {
        data.clear();
        data.append(&mut buf);
    }

    // Within runs of equal prefix, sort remaining columns.
    let mut start = 0;
    while start < data.len() {
        let prefix = prefix_key(&data[start]);
        let mut end = start + 1;
        while end < data.len() && prefix_key(&data[end]) == prefix {
            end += 1;
        }
        if end - start > 1 {
            data[start..end].sort();
        }
        start = end;
    }
}

fn bench_radix_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/radix_sort");
    for (n, nk, nv, nt) in [(100_000, 100, 1_000, 5), (100_000, 10, 100, 5)] {
        let data = generate_kv_data(n, nk, nv, nt);

        // Build flat in reversed order (unsorted).
        let mut shuffled = data.clone();
        shuffled.reverse();
        let mut flat: KVUpdates<u64, u64, u64, i64> = Default::default();
        for (k, v, td) in &shuffled {
            flat.push_flat(k, v, (&td.0, &td.1));
        }

        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        // Baseline: std sort.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("std_sort", &label), |b| {
            b.iter(|| {
                let mut refs: Vec<_> = flat.iter().collect();
                refs.sort();
                refs.len()
            })
        });

        // 1-pass MSB radix (top byte of K) + comparison sort within buckets.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("msb1_then_cmp", &label), |b| {
            b.iter(|| {
                let mut refs: Vec<_> = flat.iter().collect();
                radix_then_comparison_sort(&mut refs, |&(k, _, _)| *k);
                refs.len()
            })
        });

        // Smart radix: skip uniform bytes (datatoad approach) + comparison within runs.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("smart_radix_then_cmp", &label), |b| {
            b.iter(|| {
                let mut refs: Vec<_> = flat.iter().collect();
                full_radix_then_comparison_sort(&mut refs, |&(k, _, _)| *k);
                refs.len()
            })
        });

        // MSB1 radix + form.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("radix_k_then_cmp_form", &label), |b| {
            b.iter(|| {
                let mut refs: Vec<_> = flat.iter().collect();
                radix_then_comparison_sort(&mut refs, |&(k, _, _)| *k);
                KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter())
            })
        });
    }
    group.finish();
}

// --- Arrangement stack benchmarks ---

use differential_dataflow::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
use mz_timely_util::columnar::factorized::batch::{FactBatch, FactBuilder};
use mz_timely_util::columnar::factorized::column::FactColumn;
use timely::container::{DrainContainer, PushInto};
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

/// Generate sorted `((K,V),T,R)` data for arrangement benchmarks.
fn generate_kv_updates(
    n: usize,
    n_keys: usize,
    n_vals: usize,
    n_times: usize,
) -> Vec<((u64, u64), u64, i64)> {
    let mut data: Vec<((u64, u64), u64, i64)> = Vec::with_capacity(n);
    for i in 0..n {
        data.push((
            ((i % n_keys) as u64, (i % n_vals) as u64),
            (i % n_times) as u64,
            1i64,
        ));
    }
    data.sort();
    data
}

fn build_fact_batch(
    data: &[((u64, u64), u64, i64)],
    lower: u64,
    upper: u64,
) -> FactBatch<u64, u64, u64, i64> {
    let mut chunk: Vec<_> = data.to_vec();
    let mut builder = FactBuilder::with_capacity(0, 0, 0);
    builder.push(&mut chunk);
    builder.done(Description::new(
        Antichain::from_elem(lower),
        Antichain::from_elem(upper),
        Antichain::from_elem(0u64),
    ))
}

fn bench_arrangement_builder(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/arrangement/builder");
    for (n, nk, nv, nt) in [
        (100_000, 100, 1_000, 5),
        (100_000, 10, 100, 5),
        (1_000_000, 1_000, 10_000, 10),
        (1_000_000, 100, 1_000, 5),
    ] {
        let data = generate_kv_updates(n, nk, nv, nt);
        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("fact_builder", &label), |b| {
            b.iter(|| {
                let mut chunk = data.clone();
                let mut builder = FactBuilder::<u64, u64, u64, i64>::with_capacity(0, 0, 0);
                builder.push(&mut chunk);
                builder.done(Description::new(
                    Antichain::from_elem(0u64),
                    Antichain::from_elem(1000u64),
                    Antichain::from_elem(0u64),
                ))
            })
        });
    }
    group.finish();
}

fn bench_arrangement_cursor(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/arrangement/cursor");
    for (n, nk, nv, nt) in [
        (100_000, 100, 1_000, 5),
        (100_000, 10, 100, 5),
        (1_000_000, 1_000, 10_000, 10),
        (1_000_000, 100, 1_000, 5),
    ] {
        let data = generate_kv_updates(n, nk, nv, nt);
        let batch = build_fact_batch(&data, 0, 1000);
        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        // Full cursor traversal.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("traverse", &label), |b| {
            b.iter(|| {
                let mut count = 0usize;
                let mut cursor = batch.cursor();
                while cursor.key_valid(&batch) {
                    while cursor.val_valid(&batch) {
                        cursor.map_times(&batch, |_, _| count += 1);
                        cursor.step_val(&batch);
                    }
                    cursor.step_key(&batch);
                }
                count
            })
        });

        // Seek every 10th key.
        group.throughput(Throughput::Elements(nk as u64 / 10));
        group.bench_function(BenchmarkId::new("seek_key", &label), |b| {
            b.iter(|| {
                let mut cursor = batch.cursor();
                let mut found = 0usize;
                for target in (0..nk as u64).step_by(10) {
                    cursor.rewind_keys(&batch);
                    cursor.seek_key(&batch, &target);
                    if cursor.key_valid(&batch) {
                        found += 1;
                    }
                }
                found
            })
        });
    }
    group.finish();
}

fn bench_arrangement_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/arrangement/merge");
    for (n, nk, nv, nt) in [
        (50_000, 100, 1_000, 5),
        (50_000, 10, 100, 5),
        (500_000, 1_000, 10_000, 10),
        (500_000, 100, 1_000, 5),
    ] {
        let data1 = generate_kv_updates(n, nk, nv, nt);
        let mut data2: Vec<_> = (0..n)
            .map(|i| {
                (
                    ((i % nk) as u64, (i % nv) as u64),
                    (nt + i % nt) as u64,
                    1i64,
                )
            })
            .collect();
        data2.sort();

        let batch1 = build_fact_batch(&data1, 0, 500);
        let batch2 = build_fact_batch(&data2, 500, 1000);
        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        // Merge without compaction.
        group.throughput(Throughput::Elements(2 * n as u64));
        group.bench_function(BenchmarkId::new("no_compaction", &label), |b| {
            b.iter(|| {
                let mut merger = batch1.begin_merge(&batch2, AntichainRef::new(&[]));
                merger.work(&batch1, &batch2, &mut 10_000_000);
                merger.done()
            })
        });

        // Merge with compaction.
        let frontier = Antichain::from_elem(500u64);
        group.throughput(Throughput::Elements(2 * n as u64));
        group.bench_function(BenchmarkId::new("with_compaction", &label), |b| {
            b.iter(|| {
                let mut merger = batch1.begin_merge(&batch2, frontier.borrow());
                merger.work(&batch1, &batch2, &mut 10_000_000);
                merger.done()
            })
        });
    }
    group.finish();
}

fn bench_fact_column_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("factorized/arrangement/container_bytes");
    for (n, nk, nv, nt) in [(100_000, 100, 1_000, 5), (1_000_000, 1_000, 10_000, 10)] {
        let data = generate_kv_updates(n, nk, nv, nt);
        let label = format!("n={n}/k={nk}/v={nv}/t={nt}");

        // Build a FactColumn.
        let mut col = FactColumn::<u64, u64, u64, i64>::default();
        for item in &data {
            col.push_into(item.clone());
        }

        let byte_len = col.length_in_bytes();
        eprintln!("  [{label}] FactColumn serialized size: {byte_len} bytes ({n} updates)");

        // Serialize.
        group.throughput(Throughput::Bytes(byte_len as u64));
        group.bench_function(BenchmarkId::new("serialize", &label), |b| {
            b.iter(|| {
                let mut buf = Vec::with_capacity(byte_len);
                col.into_bytes(&mut buf);
                buf.len()
            })
        });

        // Serialize once for deserialization benchmark.
        let mut buf = Vec::with_capacity(byte_len);
        col.into_bytes(&mut buf);
        let bytes = timely::bytes::arc::BytesMut::from(buf).freeze();

        // Deserialize + drain.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("deserialize_drain", &label), |b| {
            b.iter(|| {
                let mut col2 = FactColumn::<u64, u64, u64, i64>::from_bytes(bytes.clone());
                col2.drain().count()
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
    bench_kv_serialized,
    bench_sort_cost,
    bench_builder_pipeline,
    bench_radix_sort,
    bench_arrangement_builder,
    bench_arrangement_cursor,
    bench_arrangement_merge,
    bench_fact_column_serialization,
);
criterion_main!(benches);
