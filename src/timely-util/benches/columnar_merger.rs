// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Microbenchmark comparing the columnation-backed `ColInternalMerger`
//! against the column-backed `ColumnMerger` on the merge-batcher's hot path.
//!
//! Each iteration drives a single 2-input merge via `Merger::merge`, the
//! same trait method the merge-batcher framework calls during chain
//! compaction.
//!
//! Two axes:
//!
//! - **regime** — the input distribution shape:
//!   - *mixed*: identical key range; left and right interleave throughout.
//!   - *collisions*: small key range; lots of equal-key consolidation.
//!   - *disjoint*: non-overlapping key ranges; one side dominates locally,
//!     so the column merger's galloping should pay off.
//!
//! - **size** — the per-side heap footprint, swept across targets that
//!   bracket the cache hierarchy. Each merger's curve crosses the cache
//!   tiers at its own size, and a single fixed `n` would only show one
//!   point on each curve. Stating size in bytes-per-side (rather than
//!   element count) keeps the comparison meaningful when the record type
//!   changes.
//!
//! See `mz-compute/benches/columnar_merger_row.rs` for the `Row`-keyed
//! companion bench (variable-length payload regime).

use std::mem::size_of;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use differential_dataflow::trace::implementations::merge_batcher::Merger;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::ColumnMerger;
use mz_timely_util::columnation::{ColInternalMerger, ColumnationStack};
use rand::{Rng, SeedableRng, rngs::StdRng};
use timely::container::PushInto;

type Data = (u64, u64);
type Time = u64;
type Diff = i64;
type Tuple = (Data, Time, Diff);

/// Per-side heap footprints to sweep across. Targets one point inside each
/// cache tier on Apple Silicon (L1d ≈ 64–128 KiB per core, shared L2 a few
/// MiB, then DRAM); on x86 the same picks straddle similar boundaries.
/// Element counts are derived in `bench_merge` from `size_of::<Tuple>()`.
const SIZES: &[(&str, usize)] = &[
    ("32K", 32 * 1024),
    ("512K", 512 * 1024),
    ("8M", 8 * 1024 * 1024),
    ("128M", 128 * 1024 * 1024),
];

/// Generate a sorted+consolidated `Vec<Tuple>` of approximately `n` records,
/// with keys drawn uniformly from `[0, key_range)` and times from
/// `[0, time_range)`. The exact length will typically be smaller than `n` due
/// to consolidation.
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

fn build_columnation(data: &[Tuple]) -> ColumnationStack<Tuple> {
    let mut stack: ColumnationStack<Tuple> = ColumnationStack::with_capacity(data.len());
    for &tup in data {
        stack.push_into(tup);
    }
    stack
}

fn build_column(data: &[Tuple]) -> Column<Tuple> {
    let mut col: Column<Tuple> = Default::default();
    for &tup in data {
        col.push_into(tup);
    }
    col
}

/// Build the three regime input pairs at the given per-side element count.
/// Key/time ranges scale with `n` so the regime properties (interleaving,
/// collision density, disjoint-ness) hold across sizes.
fn configs(n: usize) -> [(&'static str, Vec<Tuple>, Vec<Tuple>); 3] {
    let n_u64 = u64::cast_from(n);
    [
        // Same wide key range on both sides → records interleave.
        ("mixed", make(1, n, 2 * n_u64, 4), make(2, n, 2 * n_u64, 4)),
        // Tight key + time ranges → many records map to the same `(d, t)`,
        // exercising the equal-key diff-consolidation branch.
        (
            "collisions",
            make(3, n, u64::cast_from(n / 4), 2),
            make(4, n, u64::cast_from(n / 4), 2),
        ),
        // Left in `[0, n)`, right in `[n, 2n)` → no overlap. Each
        // `Less`/`Greater` run extends to the end of its side; galloping
        // should win here.
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

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_two_sorted");

    let bytes_per_record = size_of::<Tuple>();

    // Accumulator for the post-bench throughput summary; one entry per
    // criterion bench (paired across mergers).
    let mut summary: Vec<(String, u64)> = Vec::new();

    for (size_label, bytes_per_side) in SIZES {
        let n = bytes_per_side / bytes_per_record;
        let cfgs = configs(n);

        for (regime, a, b) in &cfgs {
            // Throughput in bytes; criterion converts to elements/s when we
            // divide by record size at report time. Bytes is the more
            // meaningful unit when comparing across record types.
            let bytes = u64::try_from((a.len() + b.len()) * bytes_per_record).unwrap();
            group.throughput(Throughput::Bytes(bytes));

            let id = format!("{regime}/{size_label}");
            summary.push((id.clone(), bytes));

            group.bench_with_input(BenchmarkId::new("columnation", &id), &(), |bencher, _| {
                bencher.iter_batched(
                    || (build_columnation(a), build_columnation(b)),
                    |(ca, cb)| {
                        let mut merger: ColInternalMerger<Data, Time, Diff> = Default::default();
                        let mut output = Vec::new();
                        let mut stash = Vec::new();
                        merger.merge(vec![ca], vec![cb], &mut output, &mut stash);
                        output
                    },
                    BatchSize::LargeInput,
                );
            });

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
        }
    }

    group.finish();

    print_throughput_table(
        "Throughput summary — primitive ((u64, u64), u64, i64):",
        "merge_two_sorted",
        &summary,
    );
}

// === Throughput summary helpers ===
//
// Criterion's stdout reporting interleaves with the rest of the output and
// buries throughput inside per-bench paragraphs. Once the group finishes we
// pull the median time out of each bench's `estimates.json`, divide by the
// bytes-per-iter we already configured, and emit a single side-by-side
// table. The same helpers live (duplicated) in the Row-keyed sister
// benches under `mz-compute/benches/`; bench files don't share a library
// easily so we accept the copy.

/// Locate the `target/criterion` directory by walking up from cwd. Needed
/// because `cargo bench -p <pkg>` runs the bench binary with cwd set to
/// the package dir (e.g. `src/timely-util`), not the workspace root.
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

/// Reads the median point estimate (ns) out of criterion's
/// `estimates.json`, with a small string scanner instead of pulling in
/// `serde_json` as a dev-dep just for this.
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

fn print_throughput_table(title: &str, group: &str, rows: &[(String, u64)]) {
    let cells: Vec<(String, String, String, String)> = rows
        .iter()
        .map(|(label, bytes)| {
            // Criterion replaces '/' in BenchmarkId parameters with '_'.
            let cmn_id = format!("columnation/{}", label.replace('/', "_"));
            let col_id = format!("column/{}", label.replace('/', "_"));
            let cmn_ns = read_criterion_median_ns(group, &cmn_id).unwrap_or(f64::NAN);
            let col_ns = read_criterion_median_ns(group, &col_id).unwrap_or(f64::NAN);
            let cmn_str = format!("{} ({})", fmt_throughput(*bytes, cmn_ns), fmt_time(cmn_ns));
            let col_str = format!("{} ({})", fmt_throughput(*bytes, col_ns), fmt_time(col_ns));
            let ratio = col_ns / cmn_ns;
            let ratio_str = if !ratio.is_finite() {
                "—".to_string()
            } else if ratio < 1.0 {
                format!("{:.2}× faster", 1.0 / ratio)
            } else {
                format!("{:.2}× slower", ratio)
            };
            (label.clone(), cmn_str, col_str, ratio_str)
        })
        .collect();

    let max_chars = |i: usize, header: &str| -> usize {
        cells
            .iter()
            .map(|c| {
                let s = match i {
                    0 => &c.0,
                    1 => &c.1,
                    2 => &c.2,
                    3 => &c.3,
                    _ => unreachable!(),
                };
                s.chars().count()
            })
            .max()
            .unwrap_or(0)
            .max(header.chars().count())
    };

    let w = [
        max_chars(0, "Config"),
        max_chars(1, "columnation"),
        max_chars(2, "column"),
        max_chars(3, "column vs columnation"),
    ];

    let bar = |l: char, m: char, r: char| -> String {
        let mut s = String::new();
        s.push(l);
        for (i, &width) in w.iter().enumerate() {
            for _ in 0..width + 2 {
                s.push('─');
            }
            s.push(if i + 1 < w.len() { m } else { r });
        }
        s
    };

    println!();
    println!("{title}");
    println!();
    println!("{}", bar('┌', '┬', '┐'));
    println!(
        "│ {:^w0$} │ {:^w1$} │ {:^w2$} │ {:^w3$} │",
        "Config",
        "columnation",
        "column",
        "column vs columnation",
        w0 = w[0],
        w1 = w[1],
        w2 = w[2],
        w3 = w[3],
    );
    println!("{}", bar('├', '┼', '┤'));
    for (i, (cfg, cmn, col, rat)) in cells.iter().enumerate() {
        if i > 0 {
            println!("{}", bar('├', '┼', '┤'));
        }
        println!(
            "│ {:<w0$} │ {:<w1$} │ {:<w2$} │ {:>w3$} │",
            cfg,
            cmn,
            col,
            rat,
            w0 = w[0],
            w1 = w[1],
            w2 = w[2],
            w3 = w[3],
        );
    }
    println!("{}", bar('└', '┴', '┘'));
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
