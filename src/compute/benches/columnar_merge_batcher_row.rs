// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! End-to-end Row-keyed [`MergeBatcher`] microbench.
//!
//! Companion to [`columnar_merger_row`](`columnar_merger_row.rs`) which
//! exercises just `Merger::merge`. This bench drives the full
//! push-rounds + seal cycle so we cover:
//! - the chunker path (`push_container` → `chunker.extract`),
//! - chain compaction (`Merger::merge` during `insert_chain` + final
//!   merge in `seal`),
//! - extraction (`Merger::extract` during `seal`).
//!
//! Two batchers are compared head-to-head:
//! - **columnation**: `Chunker<ColumnationStack>` + `ColInternalMerger`
//!   (the legacy / production path used by compute's `KeyValBatcher`).
//! - **column**: `ColumnChunker` + `ColumnMerger` (this PR's
//!   all-`Column` path).
//!
//! Sealing is funneled through a no-op [`Builder`] so the bench measures
//! the batcher itself rather than batch construction. Both batchers
//! consume the same pre-built [`Column<Tuple>`] inputs so the chunker
//! sees identical input shape.

use std::mem::size_of;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use differential_dataflow::trace::Batcher;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use mz_ore::cast::{CastFrom, CastLossy, ReinterpretCast};
use mz_repr::{Datum, Row};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::{Chunker, ColumnChunker, ColumnMerger};
use mz_timely_util::columnation::{ColInternalMerger, ColumnationStack};
use rand::{Rng, SeedableRng, rngs::StdRng};
use timely::container::ContainerBuilder;
use timely::container::PushInto;
use timely::progress::Antichain;

type Data = Row;
type Time = u64;
type Diff = i64;
type Tuple = (Data, Time, Diff);

/// Legacy path: input is `Column<Tuple>`, chunker produces
/// `ColumnationStack<Tuple>` chunks, merger operates on those.
type ColumnationBatcher = MergeBatcher<ColInternalMerger<Data, Time, Diff>>;
/// Chunker feeding [`ColumnationBatcher`].
type ColumnationBatcherChunker = Chunker<ColumnationStack<Tuple>>;

/// All-`Column` path: input is `Column<Tuple>`, chunker produces
/// `Column<Tuple>` chunks, merger operates on those.
type ColumnBatcher = MergeBatcher<ColumnMerger<Data, Time, Diff>>;
/// Chunker feeding [`ColumnBatcher`].
type ColumnBatcherChunker = ColumnChunker<Tuple>;

/// Per-side payload-byte targets. Element counts are derived from
/// [`ROW_PAYLOAD_BYTES`]. Same shape as [`columnar_merger_row`] so
/// numbers can be cross-referenced.
const SIZES: &[(&str, usize)] = &[
    ("32K", 32 * 1024),
    ("512K", 512 * 1024),
    ("8M", 8 * 1024 * 1024),
];

/// Encoded Row payload size estimate.
const ROW_PAYLOAD_BYTES: usize = 32;

/// Per-round update count. A "round" is one `push_container` call. A
/// realistic workload pushes many small batches over time, so we split
/// the per-config record budget across many rounds rather than one huge
/// push. Round size is bounded so every config triggers at least one
/// internal `merge_by` chain compaction.
const PER_ROUND: usize = 4 * 1024;

/// Build a deterministic `Row` from `key`. 24-character hex string suffix
/// overflows `CompactBytes`'s inline budget so columnation actually
/// exercises its `LgAllocRegion` copy path and column packs into its
/// `Rows` value buffer.
fn make_row(key: u64) -> Row {
    let s = format!("k{:024x}", key);
    let mut row = Row::default();
    row.packer()
        .extend([Datum::Int64(i64::reinterpret_cast(key)), Datum::String(&s)]);
    row
}

/// Generate `n_rounds` rounds of `per_round` records each. Each round is
/// pushed to the batcher via `push_container` to simulate a steady-state
/// stream of input batches.
fn make_rounds(
    seed: u64,
    n_rounds: usize,
    per_round: usize,
    key_range: u64,
    time_range: u64,
) -> Vec<Vec<Tuple>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n_rounds)
        .map(|_| {
            (0..per_round)
                .map(|_| {
                    let k = rng.random_range(0..key_range);
                    (
                        make_row(k),
                        rng.random_range(0..time_range),
                        rng.random_range(-3i64..=3),
                    )
                })
                .collect()
        })
        .collect()
}

/// Generate `n_rounds` rounds approximating the persist-hydration shape:
/// one globally sorted+consolidated dataset of ~`n_rounds * per_round`
/// records, sliced into contiguous rounds. Each round is sorted
/// internally and its key range is non-overlapping with adjacent rounds.
///
/// This matches what compute sees when reading a sorted snapshot from
/// persist: each on-the-wire batch is a tile of the same global sort
/// order. The chunker still re-sorts on push (it has no
/// already-sorted fast path), but the merger should fly — within a
/// chain, every subsequent chunk is sortable-after the previous one,
/// so `Merger::merge`'s whole-chunk passthrough fires on every chain
/// compaction.
fn make_sorted_rounds(
    seed: u64,
    n_rounds: usize,
    per_round: usize,
    key_range: u64,
    time_range: u64,
) -> Vec<Vec<Tuple>> {
    let n_total = n_rounds * per_round;
    let mut rng = StdRng::seed_from_u64(seed);

    // Build the full dataset, sort, and consolidate.
    let mut raw: Vec<Tuple> = (0..n_total)
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
    let mut consolidated: Vec<Tuple> = Vec::with_capacity(raw.len());
    for (d, t, r) in raw {
        if let Some(last) = consolidated.last_mut() {
            if last.0 == d && last.1 == t {
                last.2 += r;
                continue;
            }
        }
        consolidated.push((d, t, r));
    }
    consolidated.retain(|x| x.2 != 0);

    // Slice into contiguous rounds. Final round picks up any remainder
    // from consolidation; keeps every record in some round.
    let stride = consolidated.len().div_ceil(n_rounds.max(1));
    let mut rounds: Vec<Vec<Tuple>> = Vec::with_capacity(n_rounds);
    for chunk in consolidated.chunks(stride.max(1)) {
        rounds.push(chunk.to_vec());
    }
    while rounds.len() < n_rounds {
        rounds.push(Vec::new());
    }
    rounds
}

/// Convert each round's `Vec<Tuple>` into a `Column<Tuple>` so both
/// batchers can read the same input shape. Done outside the timed
/// closure so building the per-round columns isn't on the hot path.
fn rounds_to_columns(rounds: &[Vec<Tuple>]) -> Vec<Column<Tuple>> {
    rounds
        .iter()
        .map(|round| {
            let mut col: Column<Tuple> = Default::default();
            for tup in round {
                col.push_into(tup);
            }
            col
        })
        .collect()
}

/// Run a fresh `B` over a clone of `prebuilt_rounds`, pushing each round
/// then sealing at `+inf` so all data flows through extract. Generic over
/// `B::Output` because the columnation path produces `ColumnationStack`
/// chunks while the column path produces `Column` chunks; the no-op
/// builder accommodates either.
fn drive_batcher<B, Chu>(prebuilt_rounds: &[Column<Tuple>])
where
    B: Batcher<Time = Time>,
    Chu: ContainerBuilder<Container = B::Output> + for<'a> PushInto<&'a mut Column<Tuple>>,
    B::Output: 'static,
{
    let mut batcher = B::new(None, 0);
    let mut chunker = Chu::default();
    for round in prebuilt_rounds {
        let mut col = round.clone();
        chunker.push_into(&mut col);
        while let Some(chunk) = chunker.extract() {
            batcher.push_into(std::mem::take(chunk));
        }
    }
    while let Some(chunk) = chunker.finish() {
        batcher.push_into(std::mem::take(chunk));
    }
    let upper = Antichain::from_elem(Time::MAX);
    let _ = batcher.seal(upper);
}

fn bench_batcher(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_batcher_row");

    let bytes_per_record = ROW_PAYLOAD_BYTES + size_of::<Time>() + size_of::<Diff>();

    for (size_label, payload_bytes_total) in SIZES {
        let n_total = payload_bytes_total / ROW_PAYLOAD_BYTES;
        let n_rounds = n_total.div_ceil(PER_ROUND);
        let n_total_u64 = u64::cast_from(n_total);

        let configs: [(&str, Vec<Vec<Tuple>>); 4] = [
            (
                "mixed",
                make_rounds(1, n_rounds, PER_ROUND, 2 * n_total_u64, 4),
            ),
            (
                "collisions",
                make_rounds(2, n_rounds, PER_ROUND, u64::cast_from(n_total / 4), 2),
            ),
            (
                "disjoint",
                make_rounds(3, n_rounds, PER_ROUND, n_total_u64, 4),
            ),
            // Persist-hydration shape: one globally sorted dataset
            // sliced into contiguous rounds. Adjacent rounds have
            // non-overlapping key ranges, so chain compaction in
            // `Merger::merge` should hit the whole-chunk passthrough
            // fast path on every step.
            (
                "sorted",
                make_sorted_rounds(4, n_rounds, PER_ROUND, 2 * n_total_u64, 4),
            ),
        ];

        for (regime, rounds) in &configs {
            let total_records = rounds.iter().map(|r| r.len()).sum::<usize>();
            let bytes = u64::try_from(total_records * bytes_per_record).unwrap();
            group.throughput(Throughput::Bytes(bytes));

            let prebuilt = rounds_to_columns(rounds);
            let id = format!("{regime}/{size_label}");

            group.bench_with_input(BenchmarkId::new("columnation", &id), &(), |bencher, _| {
                bencher.iter_batched(
                    || prebuilt.clone(),
                    |rounds| {
                        drive_batcher::<ColumnationBatcher, ColumnationBatcherChunker>(&rounds)
                    },
                    BatchSize::LargeInput,
                );
            });

            group.bench_with_input(BenchmarkId::new("column", &id), &(), |bencher, _| {
                bencher.iter_batched(
                    || prebuilt.clone(),
                    |rounds| drive_batcher::<ColumnBatcher, ColumnBatcherChunker>(&rounds),
                    BatchSize::LargeInput,
                );
            });
        }
    }

    group.finish();

    print_throughput_table(
        "Throughput summary — full MergeBatcher, Row (Int64 + 24-char string):",
        "merge_batcher_row",
    );
}

// === Throughput summary helpers ===
//
// Same shape as the helpers in
// `mz-timely-util/benches/columnar_merger.rs` and the sister
// `columnar_merger_row.rs` in this directory. Pulls medians from
// criterion's `estimates.json` so we don't add a `serde_json` dev-dep.

/// Locate the `target/criterion` directory by walking up from cwd. Cargo
/// runs benches with cwd at the package dir, so a relative
/// `target/criterion` won't resolve.
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

/// Re-derive the per-config bytes (in case `print_throughput_table` is
/// called fresh — mirrors the values fed to `Throughput::Bytes`).
fn config_bytes(regime: &str, size_label: &str) -> Option<u64> {
    let payload_bytes_total = SIZES
        .iter()
        .find(|(label, _)| *label == size_label)
        .map(|(_, bytes)| *bytes)?;
    let n_total = payload_bytes_total / ROW_PAYLOAD_BYTES;
    let n_rounds = n_total.div_ceil(PER_ROUND);
    let total_records = n_rounds * PER_ROUND;
    let bytes_per_record = ROW_PAYLOAD_BYTES + size_of::<Time>() + size_of::<Diff>();
    let _ = regime; // record count is the same across regimes by construction
    u64::try_from(total_records * bytes_per_record).ok()
}

fn print_throughput_table(title: &str, group: &str) {
    let mut rows: Vec<(String, u64)> = Vec::new();
    for (size_label, _) in SIZES {
        for regime in ["mixed", "collisions", "disjoint", "sorted"] {
            let id = format!("{regime}/{size_label}");
            if let Some(b) = config_bytes(regime, size_label) {
                rows.push((id, b));
            }
        }
    }

    let cells: Vec<(String, String, String, String)> = rows
        .iter()
        .map(|(label, bytes)| {
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

criterion_group!(benches, bench_batcher);
criterion_main!(benches);
