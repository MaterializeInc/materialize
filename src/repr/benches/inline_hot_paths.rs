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

//! Benchmark for cross-crate hot path functions that benefit from #[inline].
//!
//! This exercises Row::clone, Row::hash, Row::cmp, RowRef::eq, RowRef::hash,
//! DatumListIter::next, DatumVec::borrow_with, and Row::packer - all of which
//! are called billions of times in production and benefit from #[inline]
//! annotations when LTO is off (as in the `optimized` profile).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_ore::cast::CastFrom;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, DatumVec, Row, RowRef};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn make_int_row(n: i64) -> Row {
    Row::pack(vec![
        Datum::Int64(n),
        Datum::Int64(n + 1),
        Datum::Int64(n + 2),
        Datum::Int64(n + 3),
        Datum::Int64(n + 4),
    ])
}

fn make_mixed_row(n: i64) -> Row {
    Row::pack(vec![
        Datum::Int64(n),
        Datum::String("hello world"),
        Datum::Float64(3.14.into()),
        Datum::True,
        Datum::Int32(42),
        Datum::String("test data"),
    ])
}

fn make_rows(count: usize) -> Vec<Row> {
    (0..count).map(|i| make_int_row(i as i64)).collect()
}

fn make_mixed_rows(count: usize) -> Vec<Row> {
    (0..count).map(|i| make_mixed_row(i as i64)).collect()
}

fn bench_row_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_clone");
    let rows = make_rows(10_000);
    let mixed = make_mixed_rows(10_000);

    group.bench_function("int5_10k", |b| {
        b.iter(|| {
            let mut cloned = Vec::with_capacity(rows.len());
            for row in &rows {
                cloned.push(black_box(row).clone());
            }
            black_box(cloned);
        });
    });

    group.bench_function("mixed6_10k", |b| {
        b.iter(|| {
            let mut cloned = Vec::with_capacity(mixed.len());
            for row in &mixed {
                cloned.push(black_box(row).clone());
            }
            black_box(cloned);
        });
    });

    group.finish();
}

fn bench_row_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_hash");
    let rows = make_rows(10_000);
    let mixed = make_mixed_rows(10_000);

    group.bench_function("int5_10k", |b| {
        b.iter(|| {
            let mut total: u64 = 0;
            for row in &rows {
                let mut hasher = DefaultHasher::new();
                black_box(row).hash(&mut hasher);
                total = total.wrapping_add(hasher.finish());
            }
            black_box(total)
        });
    });

    group.bench_function("mixed6_10k", |b| {
        b.iter(|| {
            let mut total: u64 = 0;
            for row in &mixed {
                let mut hasher = DefaultHasher::new();
                black_box(row).hash(&mut hasher);
                total = total.wrapping_add(hasher.finish());
            }
            black_box(total)
        });
    });

    group.finish();
}

fn bench_row_cmp(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_cmp");
    let rows = make_rows(10_000);
    let mixed = make_mixed_rows(10_000);

    group.bench_function("int5_10k_sequential", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for pair in rows.windows(2) {
                if black_box(&pair[0]).cmp(black_box(&pair[1])) == std::cmp::Ordering::Less {
                    count += 1;
                }
            }
            black_box(count)
        });
    });

    group.bench_function("mixed6_10k_sequential", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for pair in mixed.windows(2) {
                if black_box(&pair[0]).cmp(black_box(&pair[1])) == std::cmp::Ordering::Less {
                    count += 1;
                }
            }
            black_box(count)
        });
    });

    group.finish();
}

fn bench_row_eq(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_eq");
    let rows = make_rows(10_000);
    // Make pairs of equal rows for the equality check
    let rows2: Vec<Row> = rows.iter().cloned().collect();

    group.bench_function("int5_10k_equal", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for (a, b) in rows.iter().zip(rows2.iter()) {
                if black_box(a) == black_box(b) {
                    count += 1;
                }
            }
            black_box(count)
        });
    });

    group.finish();
}

fn bench_datum_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum_iter");
    let rows = make_rows(10_000);
    let mixed = make_mixed_rows(10_000);

    group.bench_function("int5_10k_iterate", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for row in &rows {
                for datum in black_box(row).iter() {
                    black_box(datum);
                    count += 1;
                }
            }
            black_box(count)
        });
    });

    group.bench_function("mixed6_10k_iterate", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for row in &mixed {
                for datum in black_box(row).iter() {
                    black_box(datum);
                    count += 1;
                }
            }
            black_box(count)
        });
    });

    group.finish();
}

fn bench_datum_vec_borrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("datum_vec_borrow");
    let rows = make_rows(10_000);
    let mixed = make_mixed_rows(10_000);

    group.bench_function("int5_10k_borrow_with", |b| {
        let mut datum_vec = DatumVec::new();
        b.iter(|| {
            let mut count = 0usize;
            for row in &rows {
                let borrow = datum_vec.borrow_with(black_box(row));
                count += borrow.len();
                drop(borrow);
            }
            black_box(count)
        });
    });

    group.bench_function("mixed6_10k_borrow_with", |b| {
        let mut datum_vec = DatumVec::new();
        b.iter(|| {
            let mut count = 0usize;
            for row in &mixed {
                let borrow = datum_vec.borrow_with(black_box(row));
                count += borrow.len();
                drop(borrow);
            }
            black_box(count)
        });
    });

    group.finish();
}

fn bench_row_packer(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_packer");

    group.bench_function("int5_10k_pack", |b| {
        let mut row = Row::default();
        b.iter(|| {
            for i in 0..10_000i64 {
                let mut packer = row.packer();
                packer.push(Datum::Int64(i));
                packer.push(Datum::Int64(i + 1));
                packer.push(Datum::Int64(i + 2));
                packer.push(Datum::Int64(i + 3));
                packer.push(Datum::Int64(i + 4));
            }
            black_box(&row);
        });
    });

    group.bench_function("mixed6_10k_pack", |b| {
        let mut row = Row::default();
        b.iter(|| {
            for i in 0..10_000i64 {
                let mut packer = row.packer();
                packer.push(Datum::Int64(i));
                packer.push(Datum::String("hello world"));
                packer.push(Datum::Float64(3.14.into()));
                packer.push(Datum::True);
                packer.push(Datum::Int32(42));
                packer.push(Datum::String("test data"));
            }
            black_box(&row);
        });
    });

    group.finish();
}

fn bench_cast_from(c: &mut Criterion) {
    let mut group = c.benchmark_group("cast_from");

    group.bench_function("u64_to_usize_10k", |b| {
        b.iter(|| {
            let mut total: usize = 0;
            for i in 0..10_000u64 {
                total = total.wrapping_add(usize::cast_from(black_box(i)));
            }
            black_box(total)
        });
    });

    group.bench_function("usize_to_u64_10k", |b| {
        b.iter(|| {
            let mut total: u64 = 0;
            for i in 0..10_000usize {
                total = total.wrapping_add(u64::cast_from(black_box(i)));
            }
            black_box(total)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_row_clone,
    bench_row_hash,
    bench_row_cmp,
    bench_row_eq,
    bench_datum_iter,
    bench_datum_vec_borrow,
    bench_row_packer,
    bench_cast_from,
);
criterion_main!(benches);
