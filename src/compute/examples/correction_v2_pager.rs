// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Memory/throughput harness for the `CorrectionV2` pager integration.
//!
//! Drives a single `CorrectionV2` buffer through a hydration-style `drain_stepwise`
//! workload (insert across many timestamps, then `updates_before` + `insert_negated`
//! feedback + `advance_since` per timestamp) under one of several pager configurations,
//! and reports wall time plus peak/final RSS. The point is to compare the resident
//! footprint of holding the correction buffer in memory against spilling it through the
//! column pager.
//!
//! Configurations (`--config`):
//!  * `baseline`  — global pager disabled; every chunk stays resident (today's behavior).
//!  * `nospill`   — `TieredPolicy` with a budget far above the working set, so every chunk
//!                  is kept resident by policy. Isolates the pager's per-chunk bookkeeping
//!                  cost from any actual I/O.
//!  * `spill`     — `TieredPolicy` with a small budget, raw (uncompressed) pageout.
//!  * `spill-lz4` — like `spill`, but lz4-compressed pageout.
//!
//! Backends (`--backend swap|file`) apply to the two spill configs. The `file` backend
//! needs `--scratch DIR`. The `swap` backend only reduces RSS under real memory pressure
//! (`MADV_COLD` defers reclaim to the kernel), so run it under a memory cap, e.g.:
//!
//! ```text
//! cargo build --release -p mz-compute --features bench --example correction_v2_pager
//! systemd-run --user --scope -p MemoryMax=512M -p MemorySwapMax=8G -- \
//!   target/release/examples/correction_v2_pager --config spill --backend swap --num-ts 200000
//! ```
//!
//! The `file` backend drops RSS without a cap because finished chunks are written out and
//! their allocations freed.
//!
//! ## Fair backing store
//!
//! A meaningful swap-vs-file comparison needs *both* backends on real disk. Two common
//! dev-box traps make every result RAM-backed and incomparable:
//!
//!  * **`/tmp` is often tmpfs** (RAM). Point `--scratch` at a directory on a real disk
//!    filesystem (`df -T <dir>` must not say `tmpfs`), otherwise the file backend just writes
//!    to RAM.
//!  * **swap is often zram/zswap** (compressed RAM). Check `/proc/swaps`: if the highest-
//!    priority device is `/dev/zram*`, the swap backend pages into compressed RAM, never disk.
//!    For a disk-swap measurement, `swapoff` the zram device (needs root) so a real partition
//!    is the active swap.
//!
//! The write *volume* and amplification figures are backing-store-independent (they're a
//! property of the merge schedule), but throughput and wall time are only meaningful against
//! the storage the production cluster actually uses.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use mz_compute::sink::correction_v2::CorrectionV2;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::pager::{self, Backend};
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::{Metrics, SinkMetrics};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_timely_util::column_pager::policy::TieredPolicy;
use mz_timely_util::column_pager::{
    self, Codec, ColumnPager, PageDecision, PageEvent, PageHint, PagingPolicy,
};
use timely::progress::Antichain;

/// Default value of `compute_correction_v2_chain_proportionality`.
const CHAIN_PROPORTIONALITY: f64 = 3.0;
/// Default value of `compute_correction_v2_chunk_size`.
const CHUNK_SIZE: usize = 8 * 1024;
/// Number of updates inserted per distinct timestamp.
const UPDATES_PER_TS: u64 = 16;

/// The pager configuration under test.
#[derive(Clone, Copy, Debug)]
enum Config {
    /// Global pager disabled; every chunk resident.
    Baseline,
    /// Budget far above the working set; resident by policy.
    NoSpill,
    /// Small budget, raw pageout.
    Spill,
    /// Small budget, lz4-compressed pageout.
    SpillLz4,
}

impl Config {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "baseline" => Ok(Self::Baseline),
            "nospill" => Ok(Self::NoSpill),
            "spill" => Ok(Self::Spill),
            "spill-lz4" => Ok(Self::SpillLz4),
            other => Err(format!("unknown config {other:?}")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::NoSpill => "nospill",
            Self::Spill => "spill",
            Self::SpillLz4 => "spill-lz4",
        }
    }
}

/// The shape of the update stream fed into the correction buffer.
#[derive(Clone, Copy, Debug)]
enum Pattern {
    /// Every timestamp appends new, distinct rows. Nothing consolidates away, so the
    /// at-rest working set grows monotonically — the case that benefits most from spilling.
    Append,
    /// Every timestamp updates the same set of keys: an addition plus a retraction of the
    /// previous value. Retraction-heavy, consolidates down to a small set.
    Upsert,
}

impl Pattern {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "append" => Ok(Self::Append),
            "upsert" => Ok(Self::Upsert),
            other => Err(format!("unknown pattern {other:?}")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Upsert => "upsert",
        }
    }
}

/// Parsed command-line arguments.
struct Args {
    config: Config,
    backend: Backend,
    num_ts: u64,
    pattern: Pattern,
    budget_mib: usize,
    scratch: Option<PathBuf>,
    /// `chain_proportionality`: the size ratio the chain invariant maintains between adjacent
    /// chains. Controls the merge depth, hence the pager write amplification (~log_p(N/M)).
    proportionality: f64,
    /// Whether to run the (slow, superlinear) stepwise drain after filling. The fill phase
    /// alone establishes the resident footprint we compare; the drain adds throughput data at
    /// a large wall-time cost. Defaults to on.
    drain: bool,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut config = Config::Baseline;
        let mut backend = Backend::Swap;
        let mut num_ts = 100_000_u64;
        let mut pattern = Pattern::Append;
        let mut budget_mib = 64_usize;
        let mut scratch = None;
        let mut proportionality = CHAIN_PROPORTIONALITY;
        let mut drain = true;

        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            let mut value = || {
                args.next()
                    .ok_or_else(|| format!("missing value for {flag}"))
            };
            match flag.as_str() {
                "--config" => config = Config::parse(&value()?)?,
                "--backend" => {
                    backend = match value()?.as_str() {
                        "swap" => Backend::Swap,
                        "file" => Backend::File,
                        other => return Err(format!("unknown backend {other:?}")),
                    }
                }
                "--num-ts" => num_ts = value()?.parse().map_err(|e| format!("--num-ts: {e}"))?,
                "--pattern" => pattern = Pattern::parse(&value()?)?,
                "--budget-mib" => {
                    budget_mib = value()?.parse().map_err(|e| format!("--budget-mib: {e}"))?
                }
                "--scratch" => scratch = Some(PathBuf::from(value()?)),
                "--proportionality" => {
                    proportionality = value()?
                        .parse()
                        .map_err(|e| format!("--proportionality: {e}"))?
                }
                "--no-drain" => drain = false,
                "--help" | "-h" => return Err(HELP.to_string()),
                other => return Err(format!("unknown flag {other:?}\n{HELP}")),
            }
        }

        if matches!(backend, Backend::File)
            && matches!(config, Config::Spill | Config::SpillLz4)
            && scratch.is_none()
        {
            return Err("--backend file requires --scratch DIR".to_string());
        }

        Ok(Self {
            config,
            backend,
            num_ts,
            pattern,
            budget_mib,
            scratch,
            proportionality,
            drain,
        })
    }
}

const HELP: &str = "\
usage: correction_v2_pager [options]
  --config   baseline|nospill|spill|spill-lz4   (default baseline)
  --backend  swap|file                          (default swap)
  --num-ts   N                                  (default 100000)
  --pattern  append|upsert                      (default append)
  --budget-mib M                                (default 64; spill budget)
  --scratch  DIR                                (required for --backend file with spill)
  --proportionality P                           (default 3.0; chain size ratio)
  --no-drain                                    (fill only; skip the slow stepwise drain)";

/// Wraps a [`TieredPolicy`] to count the bytes the pager moves.
///
/// `decide` defers entirely to the inner policy (so the budget logic and backend/codec choice
/// are unchanged); `record` tallies pageout/pagein traffic before forwarding to the inner
/// policy (which credits the resident budget on ticket drops). The byte counters give the
/// I/O volume — and, divided by wall time, the sustained backend throughput the workload
/// demands.
///
/// Note: `bytes_out` is the on-storage payload after codec/padding. For the file backend that
/// is the actual write volume; for the swap backend it is the logical volume marked cold
/// (`MADV_COLD`), not bytes the kernel necessarily wrote to swap.
struct CountingPolicy {
    inner: TieredPolicy,
    pageouts: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    pageins: AtomicU64,
    pagein_bytes: AtomicU64,
}

impl CountingPolicy {
    fn new(budget: usize, backend: Backend, codec: Option<Codec>) -> Self {
        Self {
            inner: TieredPolicy::new(budget, backend, codec),
            pageouts: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            pageins: AtomicU64::new(0),
            pagein_bytes: AtomicU64::new(0),
        }
    }
}

impl PagingPolicy for CountingPolicy {
    fn decide(&self, hint: PageHint) -> PageDecision {
        self.inner.decide(hint)
    }

    fn record(&self, event: PageEvent) {
        match &event {
            PageEvent::PagedOut {
                bytes_in,
                bytes_out,
                ..
            } => {
                self.pageouts.fetch_add(1, Ordering::Relaxed);
                self.bytes_in
                    .fetch_add(u64::cast_from(*bytes_in), Ordering::Relaxed);
                self.bytes_out
                    .fetch_add(u64::cast_from(*bytes_out), Ordering::Relaxed);
            }
            PageEvent::PagedIn { bytes } => {
                self.pageins.fetch_add(1, Ordering::Relaxed);
                self.pagein_bytes
                    .fetch_add(u64::cast_from(*bytes), Ordering::Relaxed);
            }
            PageEvent::ResidentReleased { .. } | PageEvent::Failed { .. } => {}
        }
        self.inner.record(event);
    }
}

/// Install the global pager for `config`/`backend`, returning the counting policy (if any) so
/// the caller can read the I/O tallies afterward. `Baseline` installs the disabled pager and
/// returns `None`.
fn configure_pager(args: &Args) -> Option<Arc<CountingPolicy>> {
    if let Some(scratch) = &args.scratch {
        // Idempotent; honors only the first path. Set before any file-backed pageout.
        pager::set_scratch_dir(scratch.clone());
    }

    const MIB: usize = 1024 * 1024;
    // A budget that dwarfs any working set this harness builds: every `decide` answers `Skip`,
    // so chunks stay resident but still pay the ticket-accounting path.
    let (budget, codec) = match args.config {
        Config::Baseline => {
            column_pager::set_global_pager(ColumnPager::disabled());
            return None;
        }
        Config::NoSpill => (usize::MAX / 2, None),
        Config::Spill => (args.budget_mib * MIB, None),
        Config::SpillLz4 => (args.budget_mib * MIB, Some(Codec::Lz4)),
    };

    let policy = Arc::new(CountingPolicy::new(budget, args.backend, codec));
    #[expect(
        clippy::clone_on_ref_ptr,
        reason = "unsize coercion to Arc<dyn PagingPolicy>"
    )]
    let dyn_policy: Arc<dyn PagingPolicy> = policy.clone();
    column_pager::set_global_pager(ColumnPager::new(dyn_policy));
    Some(policy)
}

fn sink_metrics() -> SinkMetrics {
    let registry = MetricsRegistry::new();
    let metrics = Metrics::new(&PersistConfig::new_for_tests(), &registry);
    metrics.sink.clone()
}

fn make_correction(metrics: &SinkMetrics, proportionality: f64) -> CorrectionV2<Row> {
    CorrectionV2::new(
        metrics.clone(),
        metrics.for_worker(0),
        None,
        proportionality,
        CHUNK_SIZE,
    )
}

fn row(key: u64, value: u64) -> Row {
    let payload = format!("payload-{value:016}");
    Row::pack_slice(&[Datum::UInt64(key), Datum::String(&payload)])
}

/// Generate the batch of updates for a single timestamp `t`.
///
/// Generated on the fly during the fill loop and dropped after insertion, so the only large
/// resident structure is the correction buffer itself — otherwise a retained `Vec` of all
/// batches would dominate RSS and mask the buffer's footprint.
fn gen_batch(t: u64, pattern: Pattern) -> Vec<(Row, Timestamp, Diff)> {
    let time = Timestamp::from(t);
    match pattern {
        Pattern::Append => (0..UPDATES_PER_TS)
            .map(|i| (row(t * UPDATES_PER_TS + i, t), time, Diff::ONE))
            .collect(),
        Pattern::Upsert => (0..UPDATES_PER_TS / 2)
            .flat_map(|key| {
                let addition = (row(key, t), time, Diff::ONE);
                let retraction = t
                    .checked_sub(1)
                    .map(|prev| (row(key, prev), time, -Diff::ONE));
                std::iter::once(addition).chain(retraction)
            })
            .collect(),
    }
}

/// Drain the correction buffer one timestamp at a time, mirroring the `write_batches`
/// operator: read updates before each `upper`, feed back their negations (persist
/// feedback), and advance the since.
fn drain_stepwise(correction: &mut CorrectionV2<Row>, num_ts: u64) {
    for t in 0..num_ts {
        let upper = Antichain::from_elem(Timestamp::from(t + 1));
        let mut written: Vec<_> = correction.updates_before(&upper).collect();
        correction.insert_negated(&mut written);
        correction.advance_since(upper);
    }
}

/// Read a `/proc/self/status` field in KiB (e.g. `VmHWM`, `VmRSS`). Returns 0 off Linux.
fn proc_status_kib(field: &str) -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field).and_then(|r| r.strip_prefix(':')) {
            return rest
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}

fn main() {
    let args = match Args::parse() {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };

    let policy = configure_pager(&args);

    let metrics = sink_metrics();

    // Fill: insert one freshly generated batch per timestamp. Each batch is dropped after
    // `insert` drains it, so the resident set reflects the correction buffer, not the input.
    let fill_start = Instant::now();
    let mut correction = make_correction(&metrics, args.proportionality);
    for t in 0..args.num_ts {
        let mut batch = gen_batch(t, args.pattern);
        correction.insert(&mut batch);
    }
    let fill_ms = u64::try_from(fill_start.elapsed().as_millis()).unwrap_or(u64::MAX);
    // Resident set with the whole working set buffered: VmRSS is the live footprint, VmHWM the
    // peak reached so far. For the spill configs the buffer is paged out, so VmRSS should drop
    // below baseline (file always; swap only under a memory cap).
    let rss_fill_kib = proc_status_kib("VmRSS");
    let rss_fill_peak_kib = proc_status_kib("VmHWM");

    let drain_ms = if args.drain {
        let drain_start = Instant::now();
        drain_stepwise(&mut correction, args.num_ts);
        u64::try_from(drain_start.elapsed().as_millis()).unwrap_or(u64::MAX)
    } else {
        0
    };
    drop(correction);

    let rss_end_kib = proc_status_kib("VmRSS");

    // Pager I/O tallies (0 for baseline, which has no counting policy). `bytes_out` is the
    // backend write volume; over the whole run, `bytes_out / total_ms` is the sustained
    // throughput the backend had to sustain.
    // `out_mib` is the on-storage write volume (after codec/padding); `out_raw_mib` is the
    // uncompressed pageout volume, so `out_raw_mib / out_mib` is the compression ratio.
    // `pagein_mib` is the uncompressed volume delivered by page-ins (read-back traffic).
    let (pageouts, out_mib, out_raw_mib, pageins, pagein_mib) = match &policy {
        Some(p) => {
            const MIB: f64 = 1024.0 * 1024.0;
            let mib = |bytes: u64| f64::cast_lossy(bytes) / MIB;
            (
                p.pageouts.load(Ordering::Relaxed),
                mib(p.bytes_out.load(Ordering::Relaxed)),
                mib(p.bytes_in.load(Ordering::Relaxed)),
                p.pageins.load(Ordering::Relaxed),
                mib(p.pagein_bytes.load(Ordering::Relaxed)),
            )
        }
        None => (0, 0.0, 0.0, 0, 0.0),
    };
    // Sustained backend write throughput over fill + drain (MiB/s); 0 if no time elapsed.
    let total_ms = fill_ms + drain_ms;
    let write_mibps = if total_ms > 0 {
        out_mib / (f64::cast_lossy(total_ms) / 1000.0)
    } else {
        0.0
    };

    // One CSV row.
    let backend = match args.backend {
        Backend::Swap => "swap",
        Backend::File => "file",
    };
    println!(
        "config,backend,num_ts,pattern,prop,budget_mib,fill_ms,drain_ms,rss_fill_kib,rss_fill_peak_kib,rss_end_kib,pageouts,out_mib,out_raw_mib,pageins,pagein_mib,write_mibps"
    );
    println!(
        "{},{},{},{},{:.1},{},{},{},{},{},{},{},{:.1},{:.1},{},{:.1},{:.1}",
        args.config.name(),
        backend,
        args.num_ts,
        args.pattern.name(),
        args.proportionality,
        args.budget_mib,
        fill_ms,
        drain_ms,
        rss_fill_kib,
        rss_fill_peak_kib,
        rss_end_kib,
        pageouts,
        out_mib,
        out_raw_mib,
        pageins,
        pagein_mib,
        write_mibps,
    );
}
