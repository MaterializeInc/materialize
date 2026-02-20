// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Benchmarks for ReductionMonoid-style Min/Max semigroup operations:
//! - Old: always calls unpack_first() for both lhs and rhs (full datum decoding)
//! - New: peeks at tag byte via first_datum_is_null() to short-circuit Null identity cases

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mz_repr::{Datum, Row};

/// Old approach: always unpack_first() for both sides.
#[inline(never)]
fn min_plus_equals_old(lhs: &mut Row, rhs: &Row) {
    let swap = {
        let lhs_val = lhs.unpack_first();
        let rhs_val = rhs.unpack_first();
        match (lhs_val, rhs_val) {
            (_, Datum::Null) => false,
            (Datum::Null, _) => true,
            (lhs, rhs) => rhs < lhs,
        }
    };
    if swap {
        lhs.clone_from(rhs);
    }
}

/// New approach: peek at tag byte for Null, only decode when needed.
#[inline(never)]
fn min_plus_equals_new(lhs: &mut Row, rhs: &Row) {
    let rhs_ref = rhs.as_row_ref();
    if rhs_ref.first_datum_is_null() {
        // rhs is Null (identity) → lhs unchanged
    } else if lhs.as_row_ref().first_datum_is_null() {
        // lhs is Null (identity) → take rhs
        lhs.clone_from(rhs);
    } else {
        // Both non-null: full datum comparison
        let lhs_val = lhs.unpack_first();
        let rhs_val = rhs_ref.unpack_first();
        if rhs_val < lhs_val {
            lhs.clone_from(rhs);
        }
    }
}

/// Max variant - old approach.
#[inline(never)]
fn max_plus_equals_old(lhs: &mut Row, rhs: &Row) {
    let swap = {
        let lhs_val = lhs.unpack_first();
        let rhs_val = rhs.unpack_first();
        match (lhs_val, rhs_val) {
            (_, Datum::Null) => false,
            (Datum::Null, _) => true,
            (lhs, rhs) => rhs > lhs,
        }
    };
    if swap {
        lhs.clone_from(rhs);
    }
}

/// Max variant - new approach.
#[inline(never)]
fn max_plus_equals_new(lhs: &mut Row, rhs: &Row) {
    let rhs_ref = rhs.as_row_ref();
    if rhs_ref.first_datum_is_null() {
        // rhs is Null (identity) → lhs unchanged
    } else if lhs.as_row_ref().first_datum_is_null() {
        // lhs is Null (identity) → take rhs
        lhs.clone_from(rhs);
    } else {
        // Both non-null: full datum comparison
        let lhs_val = lhs.unpack_first();
        let rhs_val = rhs_ref.unpack_first();
        if rhs_val > lhs_val {
            lhs.clone_from(rhs);
        }
    }
}

fn make_null_row() -> Row {
    Row::pack_slice(&[Datum::Null])
}

fn make_int64_row(v: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(v)])
}

fn make_timestamp_row(secs: i64) -> Row {
    use chrono::NaiveDateTime;
    use mz_repr::adt::timestamp::CheckedTimestamp;
    let ndt = NaiveDateTime::from_timestamp_opt(secs, 0).unwrap();
    let ct = CheckedTimestamp::from_timestamplike(ndt).unwrap();
    Row::pack_slice(&[Datum::Timestamp(ct)])
}

fn make_string_row(s: &str) -> Row {
    Row::pack_slice(&[Datum::String(s)])
}

fn make_numeric_row(s: &str) -> Row {
    use mz_repr::adt::numeric::Numeric;
    let n: Numeric = s.parse().unwrap();
    Row::pack_slice(&[Datum::from(n)])
}

fn bench_per_call(c: &mut Criterion) {
    let mut group = c.benchmark_group("reduction_monoid_per_call");

    let null = make_null_row();
    let int42 = make_int64_row(42);
    let int100 = make_int64_row(100);
    let ts1 = make_timestamp_row(1_700_000_000);
    let ts2 = make_timestamp_row(1_700_100_000);
    let str1 = make_string_row("hello world");
    let str2 = make_string_row("hello xorld");
    let num1 = make_numeric_row("123.456789");
    let num2 = make_numeric_row("987.654321");

    // Case 1: rhs is Null (identity) — ~95% of real calls
    group.bench_function("min_rhs_null_old_int", |b| {
        b.iter(|| {
            let mut lhs = int42.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&null));
        });
    });
    group.bench_function("min_rhs_null_new_int", |b| {
        b.iter(|| {
            let mut lhs = int42.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&null));
        });
    });

    group.bench_function("min_rhs_null_old_ts", |b| {
        b.iter(|| {
            let mut lhs = ts1.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&null));
        });
    });
    group.bench_function("min_rhs_null_new_ts", |b| {
        b.iter(|| {
            let mut lhs = ts1.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&null));
        });
    });

    group.bench_function("min_rhs_null_old_str", |b| {
        b.iter(|| {
            let mut lhs = str1.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&null));
        });
    });
    group.bench_function("min_rhs_null_new_str", |b| {
        b.iter(|| {
            let mut lhs = str1.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&null));
        });
    });

    group.bench_function("min_rhs_null_old_numeric", |b| {
        b.iter(|| {
            let mut lhs = num1.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&null));
        });
    });
    group.bench_function("min_rhs_null_new_numeric", |b| {
        b.iter(|| {
            let mut lhs = num1.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&null));
        });
    });

    // Case 2: lhs is Null, rhs is non-Null (swap needed)
    group.bench_function("min_lhs_null_old_int", |b| {
        b.iter(|| {
            let mut lhs = null.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&int42));
        });
    });
    group.bench_function("min_lhs_null_new_int", |b| {
        b.iter(|| {
            let mut lhs = null.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&int42));
        });
    });

    group.bench_function("min_lhs_null_old_ts", |b| {
        b.iter(|| {
            let mut lhs = null.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&ts1));
        });
    });
    group.bench_function("min_lhs_null_new_ts", |b| {
        b.iter(|| {
            let mut lhs = null.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&ts1));
        });
    });

    // Case 3: Both non-null, no swap (rhs >= lhs for Min)
    group.bench_function("min_no_swap_old_int", |b| {
        b.iter(|| {
            let mut lhs = int42.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&int100));
        });
    });
    group.bench_function("min_no_swap_new_int", |b| {
        b.iter(|| {
            let mut lhs = int42.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&int100));
        });
    });

    group.bench_function("min_no_swap_old_ts", |b| {
        b.iter(|| {
            let mut lhs = ts1.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&ts2));
        });
    });
    group.bench_function("min_no_swap_new_ts", |b| {
        b.iter(|| {
            let mut lhs = ts1.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&ts2));
        });
    });

    // Case 4: Both non-null, swap needed (rhs < lhs for Min)
    group.bench_function("min_swap_old_int", |b| {
        b.iter(|| {
            let mut lhs = int100.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&int42));
        });
    });
    group.bench_function("min_swap_new_int", |b| {
        b.iter(|| {
            let mut lhs = int100.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&int42));
        });
    });

    // Case 5: Both Null
    group.bench_function("min_both_null_old", |b| {
        b.iter(|| {
            let mut lhs = null.clone();
            min_plus_equals_old(black_box(&mut lhs), black_box(&null));
        });
    });
    group.bench_function("min_both_null_new", |b| {
        b.iter(|| {
            let mut lhs = null.clone();
            min_plus_equals_new(black_box(&mut lhs), black_box(&null));
        });
    });

    group.finish();
}

fn bench_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("reduction_monoid_batch");

    let null = make_null_row();
    let ts_base = make_timestamp_row(1_700_000_000);
    let num_base = make_numeric_row("12345.6789");

    // Simulate realistic workload: 10k plus_equals calls with ~95% Null rhs.
    // This models consolidation of a Min/Max reduction where most diffs are identity.
    let n = 10_000;
    let rhs_rows_int: Vec<Row> = (0..n)
        .map(|i| {
            if i % 20 == 0 {
                make_int64_row(i as i64 * 7 + 42)
            } else {
                null.clone()
            }
        })
        .collect();

    let rhs_rows_ts: Vec<Row> = (0..n)
        .map(|i| {
            if i % 20 == 0 {
                make_timestamp_row(1_700_000_000 + i as i64 * 100)
            } else {
                null.clone()
            }
        })
        .collect();

    let rhs_rows_numeric: Vec<Row> = (0..n)
        .map(|i| {
            if i % 20 == 0 {
                make_numeric_row(&format!("{}.{}", i * 100, i % 100))
            } else {
                null.clone()
            }
        })
        .collect();

    // 10k calls, 95% Null rhs (Int64)
    group.bench_function("10k_95pct_null_old_int", |b| {
        b.iter(|| {
            let mut acc = make_int64_row(1000);
            for rhs in &rhs_rows_int {
                min_plus_equals_old(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });
    group.bench_function("10k_95pct_null_new_int", |b| {
        b.iter(|| {
            let mut acc = make_int64_row(1000);
            for rhs in &rhs_rows_int {
                min_plus_equals_new(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });

    // 10k calls, 95% Null rhs (Timestamp)
    group.bench_function("10k_95pct_null_old_ts", |b| {
        b.iter(|| {
            let mut acc = ts_base.clone();
            for rhs in &rhs_rows_ts {
                min_plus_equals_old(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });
    group.bench_function("10k_95pct_null_new_ts", |b| {
        b.iter(|| {
            let mut acc = ts_base.clone();
            for rhs in &rhs_rows_ts {
                min_plus_equals_new(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });

    // 10k calls, 95% Null rhs (Numeric)
    group.bench_function("10k_95pct_null_old_numeric", |b| {
        b.iter(|| {
            let mut acc = num_base.clone();
            for rhs in &rhs_rows_numeric {
                min_plus_equals_old(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });
    group.bench_function("10k_95pct_null_new_numeric", |b| {
        b.iter(|| {
            let mut acc = num_base.clone();
            for rhs in &rhs_rows_numeric {
                min_plus_equals_new(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });

    // 10k calls, 100% Null rhs (pure identity case)
    let all_null: Vec<Row> = (0..n).map(|_| null.clone()).collect();
    group.bench_function("10k_all_null_old_int", |b| {
        b.iter(|| {
            let mut acc = make_int64_row(42);
            for rhs in &all_null {
                min_plus_equals_old(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });
    group.bench_function("10k_all_null_new_int", |b| {
        b.iter(|| {
            let mut acc = make_int64_row(42);
            for rhs in &all_null {
                min_plus_equals_new(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });

    // 10k calls, 0% Null rhs (all comparisons, worst case for new approach)
    let all_int: Vec<Row> = (0..n).map(|i| make_int64_row(i as i64 * 7)).collect();
    group.bench_function("10k_no_null_old_int", |b| {
        b.iter(|| {
            let mut acc = make_int64_row(i64::MAX);
            for rhs in &all_int {
                min_plus_equals_old(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });
    group.bench_function("10k_no_null_new_int", |b| {
        b.iter(|| {
            let mut acc = make_int64_row(i64::MAX);
            for rhs in &all_int {
                min_plus_equals_new(black_box(&mut acc), black_box(rhs));
            }
            acc
        });
    });

    group.finish();
}

criterion_group!(benches, bench_per_call, bench_batch);
criterion_main!(benches);
