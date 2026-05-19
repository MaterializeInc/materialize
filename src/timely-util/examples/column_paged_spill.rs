// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! End-to-end spill demo for the column-paged merge batcher.
//!
//! Drives a real timely dataflow (`arrange_core` over multiple workers)
//! against a cancellation workload: each `(k, v, t, +d)` is followed by
//! `(k, v, t, -d)` at the same logical time, so the *spine* stays empty
//! and all memory pressure lives in the merge-batcher's transient state.
//! That's the regime where the paged batcher should obviously win over
//! the no-spill baseline.
//!
//! Cancellation pattern, RSS sampler thread, and key-scrambling
//! (`mix()` so post-sort columnar bytes look incompressible) all
//! mirror `differential-dataflow/examples/columnar_spill.rs`. The
//! pager indirection swaps DD's `Spill`/`SpillPolicy`/`Fetch`/`Threshold`
//! plumbing for our existing `ColumnPager` + `TieredPolicy`.
//!
//! ```text
//! cargo run --release --example column_paged_spill -- --help
//! cargo run --release --example column_paged_spill -- --mode both --workers 4 \
//!     --times 64 --keys 24000000 --per-worker 33554432 --shared 536870912 \
//!     --sample-secs 30
//! ```

use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use differential_dataflow::operators::arrange::arrangement::arrange_core;
use mz_ore::cast::{CastFrom, CastLossy, ReinterpretCast};
use differential_dataflow::trace::implementations::Vector;
use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
use mz_ore::pager::{self, Backend};
use mz_timely_util::column_pager::policy::TieredPolicy;
use mz_timely_util::column_pager::{ColumnPager, set_global_pager};
use mz_timely_util::columnar::Col2ValPagedBatcher;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::builder::ColumnBuilder;
use timely::dataflow::InputHandle;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Input;
use timely::dataflow::operators::probe::{Handle as ProbeHandle, Probe};

type Update = ((u64, u64), u64, i64);

type MyBatcher = Col2ValPagedBatcher<u64, u64, u64, i64>;
type MyBuilder = RcBuilder<OrdValBuilder<Vector<Update>, Column<Update>>>;
type MySpine = Spine<Rc<OrdValBatch<Vector<Update>>>>;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    Both,
    Spill,
    Baseline,
}

struct Config {
    times: u64,
    keys_per_time: u64,
    per_worker_bytes: usize,
    shared_bytes: usize,
    workers: usize,
    sample_secs: u64,
    mode: Mode,
}

fn install_pager(spill: bool, budget: usize) {
    if spill {
        // Each process keeps a single `mz-pager-{pid}-{nonce}` subdir under
        // this root; reused across `set_global_pager` reinstalls.
        pager::set_scratch_dir(std::env::temp_dir());
        let policy = Arc::new(TieredPolicy::new(budget, Backend::File, None));
        set_global_pager(ColumnPager::new(policy));
    } else {
        set_global_pager(ColumnPager::disabled());
    }
}

fn run_dataflow(cfg: &Config, label: &str) -> Duration {
    let stop = Arc::new(AtomicBool::new(false));

    // RSS sampler thread. `ps -o rss=` is portable across Linux + macOS
    // and doesn't add a dep just to read /proc/self/status.
    let sampler = if cfg.sample_secs > 0 {
        let stop = Arc::clone(&stop);
        let label_owned = label.to_string();
        let interval = Duration::from_secs(cfg.sample_secs);
        let start = Instant::now();
        Some(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Some(rss) = rss_kb() {
                    println!(
                        "  [{}] +{:>5.0}s   RSS {:>9} kB",
                        label_owned,
                        start.elapsed().as_secs_f64(),
                        rss,
                    );
                }
                std::thread::sleep(interval);
            }
        }))
    } else {
        None
    };

    let times = cfg.times;
    let keys_per_time = cfg.keys_per_time;
    let timer = Instant::now();

    timely::execute(timely::Config::process(cfg.workers), move |worker| {
        let index = worker.index();
        let peers = worker.peers();

        let mut input = <InputHandle<u64, ColumnBuilder<Update>>>::new_with_builder();
        let probe: ProbeHandle<u64> = ProbeHandle::new();

        worker.dataflow::<u64, _, _>(|scope| {
            let stream = scope.input_from(&mut input);
            // Demo wires the raw DD operator; production paths go through
            // `MzArrange::mz_arrange_core` in `mz-compute`.
            #[allow(clippy::disallowed_methods)]
            let arranged = arrange_core::<_, MyBatcher, MyBuilder, MySpine>(
                stream,
                Pipeline,
                "ColumnPagedSpillArrange",
            );
            arranged.stream.probe_with(&probe);
        });

        // Push positives then negatives at the same logical time so they
        // cancel inside the merger rather than producing two giant sealed
        // batches that cancel only at the spine. `mix` scrambles the keys
        // so the post-sort columnar bytes look incompressible — without
        // this macOS' page compressor crushes the sequential-u64 pattern
        // and skews the comparison toward baseline.
        const STEP_EVERY: usize = 1 << 16;
        let mut sent_since_step = 0usize;
        for sign in [1i64, -1] {
            for t in 0..times {
                let mut k = u64::cast_from(index);
                while k < keys_per_time {
                    let kh = mix(k);
                    let d = (i64::reinterpret_cast(kh) >> 1) | 1;
                    input.send(((kh, kh & 0x3), t, sign * d));
                    k += u64::cast_from(peers);
                    sent_since_step += 1;
                    if sent_since_step >= STEP_EVERY {
                        worker.step();
                        sent_since_step = 0;
                    }
                }
            }
        }
        input.advance_to(1);
        input.flush();

        while probe.less_than(input.time()) {
            worker.step();
        }
    })
    .expect("timely::execute failed");

    let elapsed = timer.elapsed();
    stop.store(true, Ordering::Relaxed);
    if let Some(s) = sampler {
        let _ = s.join();
    }
    elapsed
}

/// Reversible bijection that destroys spatial locality of sequential keys.
/// `xorshift*` mixing — output is determined by `k` so cancellation still
/// pairs the same `(k, v, t, +d)` with `(k, v, t, -d)`.
fn mix(k: u64) -> u64 {
    let x = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 32)
}

fn rss_kb() -> Option<usize> {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let s = std::str::from_utf8(&output.stdout).ok()?;
    s.trim().parse::<usize>().ok()
}

fn main() {
    let cfg = match parse_args() {
        Some(cfg) => cfg,
        None => return,
    };

    let total_records = usize::cast_from(cfg.times * cfg.keys_per_time) * 2;
    let bytes_per_record = std::mem::size_of::<Update>();
    let raw_gb = f64::cast_lossy(total_records * bytes_per_record) / f64::cast_lossy(1u64 << 30);
    println!(
        "config: times={} keys={} workers={} per_worker={} shared={} mode={:?} sample_secs={}",
        cfg.times,
        cfg.keys_per_time,
        cfg.workers,
        cfg.per_worker_bytes,
        cfg.shared_bytes,
        cfg.mode,
        cfg.sample_secs,
    );
    println!(
        "workload: {} records ({:.2} GB raw, {} bytes/record) — cancellation, spine stays empty",
        total_records, raw_gb, bytes_per_record,
    );

    if cfg.mode != Mode::Baseline {
        install_pager(true, cfg.shared_bytes);
        let elapsed = run_dataflow(&cfg, "spill");
        println!(
            "spill:    {:.2}s | {:.2} M records/s | {:.2} GB/s",
            elapsed.as_secs_f64(),
            f64::cast_lossy(total_records) / elapsed.as_secs_f64() / 1e6,
            raw_gb / elapsed.as_secs_f64(),
        );
    }

    if cfg.mode != Mode::Spill {
        install_pager(false, 0);
        let elapsed = run_dataflow(&cfg, "baseline");
        println!(
            "baseline: {:.2}s | {:.2} M records/s | {:.2} GB/s",
            elapsed.as_secs_f64(),
            f64::cast_lossy(total_records) / elapsed.as_secs_f64() / 1e6,
            raw_gb / elapsed.as_secs_f64(),
        );
    }
}

fn parse_args() -> Option<Config> {
    let mut cfg = Config {
        times: 8,
        keys_per_time: 500_000,
        per_worker_bytes: 32 * 1024 * 1024,
        shared_bytes: 512 * 1024 * 1024,
        workers: 1,
        sample_secs: 0,
        mode: Mode::Both,
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let take = |it: &mut dyn Iterator<Item = String>, name: &str| -> String {
            it.next().unwrap_or_else(|| {
                print_usage();
                panic!("--{} requires a value", name)
            })
        };
        match a.as_str() {
            "-h" | "--help" => {
                print_usage();
                return None;
            }
            "--times" => cfg.times = take(&mut it, "times").parse().expect("times: u64"),
            "--keys" => {
                cfg.keys_per_time = take(&mut it, "keys").parse().expect("keys: u64");
            }
            "--per-worker" => {
                cfg.per_worker_bytes = take(&mut it, "per-worker")
                    .parse()
                    .expect("per-worker: usize");
            }
            "--shared" => {
                cfg.shared_bytes = take(&mut it, "shared").parse().expect("shared: usize");
            }
            "--workers" => {
                cfg.workers = take(&mut it, "workers").parse().expect("workers: usize");
            }
            "--sample-secs" => {
                cfg.sample_secs = take(&mut it, "sample-secs")
                    .parse()
                    .expect("sample-secs: u64");
            }
            "--mode" => {
                cfg.mode = match take(&mut it, "mode").as_str() {
                    "both" => Mode::Both,
                    "spill" => Mode::Spill,
                    "baseline" => Mode::Baseline,
                    other => {
                        print_usage();
                        panic!("unknown mode: {other}");
                    }
                };
            }
            other => {
                print_usage();
                panic!("unknown arg: {other}");
            }
        }
    }
    Some(cfg)
}

fn print_usage() {
    eprintln!("Usage: column_paged_spill [OPTIONS]");
    eprintln!();
    eprintln!("  --times N           distinct data timestamps           (default 8)");
    eprintln!("  --keys N            keys per timestamp                 (default 500000)");
    eprintln!("  --per-worker BYTES  TieredPolicy per-worker budget     (default 32 MiB)");
    eprintln!("  --shared BYTES      TieredPolicy shared budget         (default 512 MiB)");
    eprintln!("  --workers N         timely worker threads              (default 1)");
    eprintln!("  --sample-secs N     print RSS every N seconds          (default 0 = off)");
    eprintln!("  --mode MODE         spill | baseline | both            (default both)");
    eprintln!();
    eprintln!("Total records pushed = 2 * times * keys (positives + negatives that cancel).");
    eprintln!("Records partitioned across workers by `k % workers` after `mix()` scramble.");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  # quick smoke — 8M records, both modes, 1 worker");
    eprintln!("  column_paged_spill");
    eprintln!();
    eprintln!("  # 100 GB workload on 4 workers, RSS every 30s, spill-only");
    eprintln!("  column_paged_spill --mode spill --workers 4 \\");
    eprintln!("    --times 64 --keys 24000000 --sample-secs 30");
}
