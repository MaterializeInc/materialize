// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Row-keyed merge-batcher microbench.
//!
//! Mirror of [`mz-timely-util/benches/columnar_merger.rs`] with
//! `Data = Row`. Lives in `mz-repr` because [`mz_repr::Row`]'s columnar
//! container `Rows` lives in a private module here, and `mz-repr` already
//! depends on `mz-timely-util` (which would create a dev-dep cycle if the
//! bench tried to live the other way around).
//!
//! The thesis this answers: where does `Column` win against
//! `ColumnationStack` once records carry a variable-length payload? The
//! sister primitive bench shows `Column` structurally pays a per-leaf push
//! tax on 24-byte records; here each record carries an `Int64` plus a
//! variable-length `String`, so the per-leaf overhead amortizes against
//! per-row work and the heap-spill path actually runs (columnation copies
//! into its `LgAllocRegion`; column packs into its `Rows` value buffer).
//!
//! Two axes match the primitive bench:
//!
//! - **regime** — *mixed* / *collisions* / *disjoint*.
//! - **size** — per-side heap-resident payload bytes; element count is
//!   derived from the per-row payload estimate. The labels are
//!   conservative — actual heap footprints (Row overhead, container
//!   metadata, output buffers) land roughly 3× higher.

use std::mem::size_of;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use differential_dataflow::trace::implementations::merge_batcher::Merger;
use mz_repr::{Datum, Row};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::ColumnMerger;
use mz_timely_util::columnation::{ColInternalMerger, ColumnationStack};
use rand::{Rng, SeedableRng, rngs::StdRng};
use timely::container::PushInto;

type Data = Row;
type Time = u64;
type Diff = i64;
type Tuple = (Data, Time, Diff);

/// Per-side payload-byte targets. Counts the encoded Row body only; ignores
/// `Row` / `CompactBytes` overhead and the (Time, Diff) suffix. The sweep
/// brackets the cache hierarchy: 32 KiB ≈ L1d, 512 KiB ≈ L2-resident,
/// 8 MiB ≈ DRAM-bound. 128 MiB is omitted because Row-heavy setup costs
/// would dominate the wall-clock for a regime that's already DRAM-bound at
/// 8 MiB.
const SIZES: &[(&str, usize)] = &[
    ("32K", 32 * 1024),
    ("512K", 512 * 1024),
    ("8M", 8 * 1024 * 1024),
];

/// Encoded Row payload size estimate used to derive element counts from
/// byte targets. Each row carries `Datum::Int64(key)` plus a 24-char
/// hex-string suffix; the columnar encoding is not byte-exact but lands
/// near this number for both representations.
const ROW_PAYLOAD_BYTES: usize = 32;

/// Build a deterministic Row from a key. Same key always yields the same
/// Row, so `key_range` in [`make`] drives collision density the same way it
/// does in the primitive bench.
///
/// The 24-character string overflows `CompactBytes`'s inline budget, so
/// every row spills to heap — this exercises columnation's `LgAllocRegion`
/// copy path (without spill it just clones inline bytes) and column's
/// variable-length values buffer.
fn make_row(key: u64) -> Row {
    let s = format!("k{:024x}", key);
    let mut row = Row::default();
    row.packer().extend([Datum::Int64(key as i64), Datum::String(&s)]);
    row
}

/// Generate a sorted+consolidated `Vec<Tuple>` of approximately `n`
/// records, with row-keys drawn uniformly from `[0, key_range)` and times
/// from `[0, time_range)`. Length will typically be smaller than `n`
/// because the sort+sum-and-drop-zero pass collapses duplicates.
fn make(seed: u64, n: usize, key_range: u64, time_range: u64) -> Vec<Tuple> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut raw: Vec<Tuple> = (0..n)
        .map(|_| {
            let k = rng.random_range(0..key_range);
            (
                make_row(k),
                rng.random_range(0..time_range),
                rng.random_range(-3i64..=3),
            )
        })
        .collect();
    raw.sort();
    let mut out: Vec<Tuple> = Vec::new();
    for (d, t, r) in raw {
        if let Some(last) = out.last_mut() {
            if last.0 == d && last.1 == t {
                last.2 += r;
                continue;
            }
        }
        out.push((d, t, r));
    }
    out.retain(|x| x.2 != 0);
    out
}

fn build_columnation(data: &[Tuple]) -> ColumnationStack<Tuple> {
    let mut stack: ColumnationStack<Tuple> = ColumnationStack::with_capacity(data.len());
    for tup in data {
        // ColumnationStack accepts owned tuples; clone to keep `data` valid
        // for the parallel `build_column` call.
        stack.push_into(tup.clone());
    }
    stack
}

fn build_column(data: &[Tuple]) -> Column<Tuple> {
    let mut col: Column<Tuple> = Default::default();
    for tup in data {
        // `Rows: Push<&Row>` only, so push by reference. Tuple-leaf
        // dispatch wants `&(Row, u64, i64)` to drive each leaf's
        // `Push<&D>`, `Push<&u64>`, `Push<&i64>` impl.
        col.push_into(tup);
    }
    col
}

/// Build the three regime input pairs at the given per-side element count.
/// Key/time ranges scale with `n` so regime properties hold across sizes.
fn configs(n: usize) -> [(&'static str, Vec<Tuple>, Vec<Tuple>); 3] {
    [
        // Same wide key range on both sides → records interleave.
        (
            "mixed",
            make(1, n, 2 * n as u64, 4),
            make(2, n, 2 * n as u64, 4),
        ),
        // Tight key + time ranges → many records map to the same `(d, t)`,
        // exercising the equal-key diff-consolidation branch.
        (
            "collisions",
            make(3, n, (n / 4) as u64, 2),
            make(4, n, (n / 4) as u64, 2),
        ),
        // Left in `[0, n)`, right in `[n, 2n)` → no overlap. Each
        // `Less`/`Greater` run extends to the end of its side; column's
        // gallop bulk-copy should win here.
        (
            "disjoint",
            make(5, n, n as u64, 4),
            make(6, n, n as u64, 4)
                .into_iter()
                .map(|(d, t, r)| {
                    // Re-key right side into a disjoint range. Decode the
                    // existing Int64 and shift; this keeps `make_row`'s
                    // string-suffix invariant intact.
                    let datums: Vec<Datum> = d.iter().collect();
                    let key = match datums[0] {
                        Datum::Int64(k) => k as u64 + n as u64,
                        _ => unreachable!(),
                    };
                    (make_row(key), t, r)
                })
                .collect(),
        ),
    ]
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_two_sorted_row");

    for (size_label, payload_bytes_per_side) in SIZES {
        let n = payload_bytes_per_side / ROW_PAYLOAD_BYTES;
        let cfgs = configs(n);

        for (regime, a, b) in &cfgs {
            // Throughput in elements (records merged) — bytes is awkward
            // for variable-length data because each merge step's "bytes"
            // depends on the row picked. Element-count is the apples-to-
            // apples unit.
            let elems = u64::try_from(a.len() + b.len()).unwrap();
            group.throughput(Throughput::Elements(elems));

            let id = format!("{regime}/{size_label}");

            // Build the columnar containers once, outside the timed
            // closure. `iter_batched` clones them each iteration; clone of
            // a `ColumnationStack` / `Column` is a Vec / slice clone, far
            // cheaper than re-running `build_*` on heap-spilling Rows.
            let ca = build_columnation(a);
            let cb = build_columnation(b);
            group.bench_with_input(
                BenchmarkId::new("columnation", &id),
                &(),
                |bencher, _| {
                    bencher.iter_batched(
                        || (ca.clone(), cb.clone()),
                        |(ca, cb)| {
                            let mut merger: ColInternalMerger<Data, Time, Diff> =
                                Default::default();
                            let mut output = Vec::new();
                            let mut stash = Vec::new();
                            merger.merge(vec![ca], vec![cb], &mut output, &mut stash);
                            output
                        },
                        BatchSize::LargeInput,
                    );
                },
            );

            let ka = build_column(a);
            let kb = build_column(b);
            group.bench_with_input(BenchmarkId::new("column", &id), &(), |bencher, _| {
                bencher.iter_batched(
                    || (ka.clone(), kb.clone()),
                    |(ka, kb)| {
                        let mut merger: ColumnMerger<Data, Time, Diff> = Default::default();
                        let mut output = Vec::new();
                        let mut stash = Vec::new();
                        merger.merge(vec![ka], vec![kb], &mut output, &mut stash);
                        output
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }

    // Touch `size_of::<Tuple>()` so a Tuple shape change that breaks the
    // `ROW_PAYLOAD_BYTES` estimate doesn't go unnoticed.
    let _ = size_of::<Tuple>();

    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
