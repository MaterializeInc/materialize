// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Microbenchmark comparing the legacy column-backed `ColumnMerger` against
//! the new pageable `ColumnMergeBatcher` driver on the merge-batcher's hot
//! path.
//!
//! Each iteration drives a single 2-input merge — either via `Merger::merge`
//! (legacy) or via `merge_chains` (new, driven through a `ColumnPager`).
//!
//! Three pager configurations sweep the cost of the new path:
//!
//! - **`paged-disabled`** — `ColumnPager::disabled`; chunks stay `Resident`
//!   throughout. Compared to `column`, this isolates the pager-wrapping
//!   overhead (extra `Resident(_, ticket)` enum dispatch and the
//!   `FetchIter`-shaped driver).
//! - **`paged-swap`** — every chunk routes through the Swap backend
//!   uncompressed. Measures the cost of byte-level serialization + buffered
//!   allocation moves with no codec work.
//! - **`paged-lz4`** — same as `paged-swap` but with lz4 frame compression.
//!   Adds codec CPU cost to the swap baseline.
//!
//! Two axes match the sister bench `columnar_merger.rs`:
//! regime × size. See that file for axis rationale.

use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use differential_dataflow::trace::implementations::merge_batcher::Merger;
use itertools::Itertools;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::pager::Backend;
use mz_timely_util::column_pager::{
    Codec, ColumnPager, PageDecision, PageEvent, PageHint, PagedColumn, PagingPolicy,
};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::ColumnMerger;
use mz_timely_util::columnar::merge_batcher::{FetchIter, merge_chains};
use rand::{Rng, SeedableRng, rngs::StdRng};
use timely::container::PushInto;

type Data = (u64, u64);
type Time = u64;
type Diff = i64;
type Tuple = (Data, Time, Diff);

/// Per-side heap footprints to sweep across. Same tiers as
/// `columnar_merger.rs`.
const SIZES: &[(&str, usize)] = &[
    ("32K", 32 * 1024),
    ("512K", 512 * 1024),
    ("8M", 8 * 1024 * 1024),
    ("128M", 128 * 1024 * 1024),
];

/// Always-page policy parameterized over backend + codec. Used to force the
/// merger through the byte-shaped path even when chunks are tiny.
struct ForcePage {
    backend: Backend,
    codec: Option<Codec>,
}
impl PagingPolicy for ForcePage {
    fn decide(&self, _hint: PageHint) -> PageDecision {
        PageDecision::Page {
            backend: self.backend,
            codec: self.codec,
        }
    }
    fn record(&self, _event: PageEvent) {}
}

fn pager_disabled() -> ColumnPager {
    ColumnPager::disabled()
}

fn pager_force(backend: Backend, codec: Option<Codec>) -> ColumnPager {
    ColumnPager::new(Arc::new(ForcePage { backend, codec }))
}

/// Wrap a single resident `Column` as a one-entry chain.
fn one_chain(mut c: Column<Tuple>, pager: &ColumnPager) -> VecDeque<PagedColumn<Tuple>> {
    let paged = pager.page(&mut c);
    VecDeque::from([paged])
}

fn make(seed: u64, n: usize, key_range: u64, time_range: u64) -> Vec<Tuple> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut raw: Vec<Tuple> = (0..n)
        .map(|_| {
            (
                (
                    rng.random_range(0..key_range),
                    rng.random_range(0..key_range),
                ),
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

fn build_column(data: &[Tuple]) -> Column<Tuple> {
    let mut col: Column<Tuple> = Default::default();
    for &tup in data {
        col.push_into(tup);
    }
    col
}

fn configs(n: usize) -> [(&'static str, Vec<Tuple>, Vec<Tuple>); 3] {
    let n_u64 = u64::cast_from(n);
    [
        ("mixed", make(1, n, 2 * n_u64, 4), make(2, n, 2 * n_u64, 4)),
        (
            "collisions",
            make(3, n, u64::cast_from(n / 4), 2),
            make(4, n, u64::cast_from(n / 4), 2),
        ),
        (
            "disjoint",
            make(5, n, n_u64, 4),
            make(6, n, n_u64, 4)
                .into_iter()
                .map(|((k1, k2), t, r)| ((k1 + n_u64, k2 + n_u64), t, r))
                .collect(),
        ),
    ]
}

/// One row of the throughput summary — bytes-per-iter, plus the four variant
/// labels we'll look up in `target/criterion/<group>/<variant>/<id>/...`.
const VARIANTS: &[&str] = &["column", "paged-disabled", "paged-swap", "paged-lz4"];

fn bench_merge_batcher(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_batcher_two_sorted");

    let bytes_per_record = size_of::<Tuple>();
    let mut summary: Vec<(String, u64)> = Vec::new();

    for (size_label, bytes_per_side) in SIZES {
        let n = bytes_per_side / bytes_per_record;
        let cfgs = configs(n);

        for (regime, a, b) in &cfgs {
            let bytes = u64::try_from((a.len() + b.len()) * bytes_per_record).unwrap();
            group.throughput(Throughput::Bytes(bytes));

            let id = format!("{regime}/{size_label}");
            summary.push((id.clone(), bytes));

            // Variant 1: legacy `ColumnMerger::merge`. Baseline.
            group.bench_with_input(BenchmarkId::new("column", &id), &(), |bencher, _| {
                bencher.iter_batched(
                    || (build_column(a), build_column(b)),
                    |(ca, cb)| {
                        let mut merger: ColumnMerger<Data, Time, Diff> = Default::default();
                        let mut output = Vec::new();
                        let mut stash = Vec::new();
                        merger.merge(vec![ca], vec![cb], &mut output, &mut stash);
                        output
                    },
                    BatchSize::LargeInput,
                );
            });

            // Variant 2: new path, disabled pager. Isolates wrapping cost.
            group.bench_with_input(
                BenchmarkId::new("paged-disabled", &id),
                &(),
                |bencher, _| {
                    let pager = pager_disabled();
                    bencher.iter_batched(
                        || {
                            (
                                one_chain(build_column(a), &pager),
                                one_chain(build_column(b), &pager),
                            )
                        },
                        |(q1, q2)| {
                            let mut output: Vec<PagedColumn<Tuple>> = Vec::new();
                            let mut stash: Vec<Column<Tuple>> = Vec::new();
                            merge_chains(
                                FetchIter::new(q1, &pager),
                                FetchIter::new(q2, &pager),
                                |p| output.push(p),
                                &mut stash,
                            );
                            output
                        },
                        BatchSize::LargeInput,
                    );
                },
            );

            // Variant 3: force-page to Swap, no codec.
            group.bench_with_input(BenchmarkId::new("paged-swap", &id), &(), |bencher, _| {
                let pager = pager_force(Backend::Swap, None);
                bencher.iter_batched(
                    || {
                        (
                            one_chain(build_column(a), &pager),
                            one_chain(build_column(b), &pager),
                        )
                    },
                    |(q1, q2)| {
                        let mut output: Vec<PagedColumn<Tuple>> = Vec::new();
                        let mut stash: Vec<Column<Tuple>> = Vec::new();
                        merge_chains(
                            FetchIter::new(q1, &pager),
                            FetchIter::new(q2, &pager),
                            |p| output.push(p),
                            &mut stash,
                        );
                        output
                    },
                    BatchSize::LargeInput,
                );
            });

            // Variant 4: force-page to Swap with lz4. Codec cost.
            group.bench_with_input(BenchmarkId::new("paged-lz4", &id), &(), |bencher, _| {
                let pager = pager_force(Backend::Swap, Some(Codec::Lz4));
                bencher.iter_batched(
                    || {
                        (
                            one_chain(build_column(a), &pager),
                            one_chain(build_column(b), &pager),
                        )
                    },
                    |(q1, q2)| {
                        let mut output: Vec<PagedColumn<Tuple>> = Vec::new();
                        let mut stash: Vec<Column<Tuple>> = Vec::new();
                        merge_chains(
                            FetchIter::new(q1, &pager),
                            FetchIter::new(q2, &pager),
                            |p| output.push(p),
                            &mut stash,
                        );
                        output
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();

    print_throughput_table(
        "Throughput summary — primitive ((u64, u64), u64, i64):",
        "merge_batcher_two_sorted",
        &summary,
    );
}

// ===========================================================================
// Throughput summary helpers
//
// Same shape as `columnar_merger.rs` but widened for our four variants. The
// helpers are duplicated rather than shared because bench files don't have
// an easy way to import each other.
// ===========================================================================

fn criterion_dir() -> std::path::PathBuf {
    let mut cur = std::env::current_dir().unwrap_or_default();
    loop {
        let candidate = cur.join("target").join("criterion");
        if candidate.is_dir() {
            return candidate;
        }
        if !cur.pop() {
            return std::path::PathBuf::from("target/criterion");
        }
    }
}

fn read_criterion_median_ns(group: &str, bench_id: &str) -> Option<f64> {
    let path = criterion_dir()
        .join(group)
        .join(bench_id)
        .join("new")
        .join("estimates.json");
    let json = std::fs::read_to_string(&path).ok()?;
    let median_idx = json.find("\"median\"")?;
    let after = &json[median_idx..];
    let pe_marker = "\"point_estimate\"";
    let pe_idx = after.find(pe_marker)?;
    let rest = after[pe_idx + pe_marker.len()..].trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let end = rest.find(|c: char| c == ',' || c == '}')?;
    rest[..end].trim().parse::<f64>().ok()
}

fn fmt_throughput(bytes: u64, ns: f64) -> String {
    if !ns.is_finite() || ns <= 0.0 {
        return "—".to_string();
    }
    let bytes_per_sec = f64::cast_lossy(bytes) * 1e9 / ns;
    let gibs = bytes_per_sec / f64::cast_lossy(1u64 << 30);
    if gibs >= 1.0 {
        format!("{gibs:.2} GiB/s")
    } else {
        let mibs = bytes_per_sec / f64::cast_lossy(1u64 << 20);
        format!("{mibs:.0} MiB/s")
    }
}

fn fmt_time(ns: f64) -> String {
    if !ns.is_finite() {
        "—".to_string()
    } else if ns < 1e3 {
        format!("{:.0} ns", ns)
    } else if ns < 1e6 {
        format!("{:.1} µs", ns / 1e3)
    } else if ns < 1e9 {
        format!("{:.2} ms", ns / 1e6)
    } else {
        format!("{:.2} s", ns / 1e9)
    }
}

fn fmt_ratio(num_ns: f64, den_ns: f64) -> String {
    if !(num_ns.is_finite() && den_ns.is_finite()) || den_ns <= 0.0 {
        return "—".to_string();
    }
    let r = num_ns / den_ns;
    if (r - 1.0).abs() < 0.01 {
        "≈ 1.00×".to_string()
    } else if r < 1.0 {
        format!("{:.2}× faster", 1.0 / r)
    } else {
        format!("{:.2}× slower", r)
    }
}

fn print_throughput_table(title: &str, group: &str, rows: &[(String, u64)]) {
    // Columns: Config | column | paged-disabled | paged-swap | paged-lz4 |
    // disabled vs column.
    let mut cells: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for (label, bytes) in rows {
        let ns: Vec<f64> = VARIANTS
            .iter()
            .map(|v| {
                let bench_id = format!("{}/{}", v, label.replace('/', "_"));
                read_criterion_median_ns(group, &bench_id).unwrap_or(f64::NAN)
            })
            .collect();
        let column_ns = ns[0];
        let disabled_ns = ns[1];

        let mut row = vec![label.clone()];
        for (variant_ns, _variant) in ns.iter().zip_eq(VARIANTS.iter()) {
            row.push(format!(
                "{} ({})",
                fmt_throughput(*bytes, *variant_ns),
                fmt_time(*variant_ns)
            ));
        }
        row.push(fmt_ratio(disabled_ns, column_ns));
        cells.push(row);
    }

    let headers = [
        "Config",
        "column",
        "paged-disabled",
        "paged-swap",
        "paged-lz4",
        "disabled vs column",
    ];
    let max_chars = |i: usize| -> usize {
        cells
            .iter()
            .map(|c| c[i].chars().count())
            .max()
            .unwrap_or(0)
            .max(headers[i].chars().count())
    };
    let widths: Vec<usize> = (0..headers.len()).map(max_chars).collect();

    let bar = |l: char, m: char, r: char| -> String {
        let mut s = String::new();
        s.push(l);
        for (i, &w) in widths.iter().enumerate() {
            for _ in 0..w + 2 {
                s.push('─');
            }
            s.push(if i + 1 < widths.len() { m } else { r });
        }
        s
    };

    println!();
    println!("{title}");
    println!();
    println!("{}", bar('┌', '┬', '┐'));
    let header_row = headers
        .iter()
        .zip_eq(widths.iter())
        .map(|(h, w)| format!(" {:^w$} ", h, w = w))
        .collect::<Vec<_>>()
        .join("│");
    println!("│{header_row}│");
    println!("{}", bar('├', '┼', '┤'));
    for (i, row) in cells.iter().enumerate() {
        if i > 0 {
            println!("{}", bar('├', '┼', '┤'));
        }
        let line = row
            .iter()
            .zip_eq(widths.iter())
            .enumerate()
            .map(|(idx, (cell, w))| {
                if idx == 0 {
                    format!(" {:<w$} ", cell, w = w)
                } else if idx + 1 == row.len() {
                    format!(" {:>w$} ", cell, w = w)
                } else {
                    format!(" {:<w$} ", cell, w = w)
                }
            })
            .collect::<Vec<_>>()
            .join("│");
        println!("│{line}│");
    }
    println!("{}", bar('└', '┴', '┘'));
}

criterion_group!(benches, bench_merge_batcher);
criterion_main!(benches);
