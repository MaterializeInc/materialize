// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Peak-memory harness for the MV sink correction buffers.
//!
//! Criterion measures CPU time across many iterations, which makes peak RSS meaningless: setup,
//! measured closure, and drop of the previous iteration overlap. This harness runs exactly one
//! fill (and optionally one stepwise drain) in a fresh process and reports wall time next to
//! `VmHWM`, so the number can be compared across implementations and read under a `memcap` cap.
//!
//! ```text
//! cargo run --release -p mz-compute --features bench --example correction_mem -- \
//!     --version v2 --num-ts 262144 --phase drain
//! ```

use std::time::Instant;

use mz_compute::sink::correction::CorrectionV1;
use mz_compute::sink::correction_v2::CorrectionV2;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::pager::Backend;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::{Metrics, SinkMetrics};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_timely_util::column_pager::{self, Codec};
use timely::progress::Antichain;

enum Corr {
    V1(CorrectionV1<Row>),
    V2(CorrectionV2<Row>),
}

impl Corr {
    fn insert(&mut self, updates: &mut Vec<(Row, Timestamp, Diff)>) {
        match self {
            Self::V1(c) => c.insert(updates),
            Self::V2(c) => c.insert(updates),
        }
    }

    fn insert_negated(&mut self, updates: &mut Vec<(Row, Timestamp, Diff)>) {
        match self {
            Self::V1(c) => c.insert_negated(updates),
            Self::V2(c) => c.insert_negated(updates),
        }
    }

    fn updates_before(
        &mut self,
        upper: &Antichain<Timestamp>,
    ) -> Box<dyn Iterator<Item = (Row, Timestamp, Diff)> + '_> {
        match self {
            Self::V1(c) => Box::new(c.updates_before(upper)),
            Self::V2(c) => Box::new(c.updates_before(upper)),
        }
    }

    fn advance_since(&mut self, since: Antichain<Timestamp>) {
        match self {
            Self::V1(c) => c.advance_since(since),
            Self::V2(c) => c.advance_since(since),
        }
    }
}

fn row(key: u64, value: u64) -> Row {
    // Written by hand rather than with `format!`: integer formatting runs inside the timed loop
    // and shows up at over 10% of an insert profile, which would be charged to the buffer. The
    // payload keeps its 24-byte width so peak-RSS numbers stay comparable.
    let mut payload = *b"payload-0000000000000000";
    let mut rest = value;
    for slot in payload[8..].iter_mut().rev() {
        *slot = b'0' + u8::try_from(rest % 10).expect("single digit");
        rest /= 10;
    }
    let payload = std::str::from_utf8(&payload).expect("ASCII");
    Row::pack_slice(&[Datum::UInt64(key), Datum::String(payload)])
}

/// Reads a `/proc/self/status` size field, in MiB.
fn status_mib(field: &str) -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let kb: f64 = status
        .lines()
        .find_map(|l| l.strip_prefix(field))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("{field} present"));
    kb / 1024.0
}

/// Peak resident set size in MiB.
///
/// `VmHWM` is a high-water mark that never falls, so a pager that reclaims after the peak looks
/// identical to one that never reclaims. Read [`live_rss_mib`] alongside it.
fn peak_rss_mib() -> f64 {
    status_mib("VmHWM:")
}

/// Current resident set size in MiB.
fn live_rss_mib() -> f64 {
    status_mib("VmRSS:")
}

fn arg(args: &[String], name: &str, default: &str) -> String {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let version = arg(&args, "--version", "v2");
    let num_ts: u64 = arg(&args, "--num-ts", "262144").parse().expect("--num-ts");
    let chunk_size: usize = arg(&args, "--chunk-size", "8192")
        .parse()
        .expect("--chunk-size");
    let updates_per_ts: u64 = arg(&args, "--updates-per-ts", "16")
        .parse()
        .expect("--updates-per-ts");
    let pager = arg(&args, "--pager", "off");
    let phase = arg(&args, "--phase", "insert");
    // Resident-byte budget for the tiered policy. The presets below pin it to 0 (page
    // everything) or effectively unbounded; a real deployment sizes it against the replica's
    // memory limit, so it is tunable here.
    let budget: Option<usize> = args
        .iter()
        .position(|a| a == "--budget-mib")
        .and_then(|i| args.get(i + 1))
        .map(|v| v.parse::<usize>().expect("--budget-mib") * 1024 * 1024);

    mz_ore::pager::set_scratch_dir(std::env::temp_dir());

    // Budget the process buffer pool the correction buffer spills through. A zero budget
    // installs no pool, which leaves `active_pool` `None` and every chunk resident.
    let pool_budget: usize = arg(&args, "--pool-budget-mib", "0")
        .parse::<usize>()
        .expect("--pool-budget-mib")
        * 1024
        * 1024;
    let pool_rss: usize = arg(&args, "--pool-rss-mib", "0")
        .parse::<usize>()
        .expect("--pool-rss-mib")
        * 1024
        * 1024;
    if pool_budget > 0 {
        let ok = mz_timely_util::pool_config::apply_pool_config(
            mz_timely_util::pool_config::PoolPagerConfig {
                budget_bytes: pool_budget,
                spill_threads: 2,
                eager_backing: false,
                rss_target_bytes: pool_rss,
            },
        );
        assert!(ok, "pool reservation failed");
        // Spilling is gated per subsystem on top of the installed pool.
        mz_timely_util::columnar::chunk::set_compute_spill_enabled(true);
    }
    let (enabled, budget_default, backend, codec, pageout) = match pager.as_str() {
        "off" => (false, 0, Backend::Swap, None, false),
        "resident" => (true, usize::MAX / 2, Backend::Swap, None, false),
        "swap_cold" => (true, 0, Backend::Swap, None, false),
        "swap_pageout" => (true, 0, Backend::Swap, None, true),
        "swap_lz4" => (true, 0, Backend::Swap, Some(Codec::Lz4), false),
        "file" => (true, 0, Backend::File, None, false),
        "file_lz4" => (true, 0, Backend::File, Some(Codec::Lz4), false),
        other => panic!("unknown --pager {other}"),
    };
    let budget = budget.unwrap_or(budget_default);
    column_pager::apply_tiered_config(enabled, budget, backend, codec, pageout);

    let registry = MetricsRegistry::new();
    let metrics: SinkMetrics = Metrics::new(&PersistConfig::new_for_tests(), &registry)
        .sink
        .clone();
    let worker_metrics = metrics.for_worker(0);
    let mut correction = match version.as_str() {
        "v1" => Corr::V1(CorrectionV1::new(metrics.clone(), worker_metrics, 1)),
        "v2" => Corr::V2(CorrectionV2::new(
            metrics.clone(),
            worker_metrics,
            None,
            3.0,
            chunk_size,
        )),
        other => panic!("unknown --version {other}"),
    };

    // Batches are generated per timestamp rather than up front: holding the whole input resident
    // would dominate the peak we are trying to attribute to the buffer.
    let start = Instant::now();
    for t in 0..num_ts {
        let time = Timestamp::from(t);
        let mut batch: Vec<_> = (0..updates_per_ts)
            .map(|i| (row(t * updates_per_ts + i, t), time, Diff::ONE))
            .collect();
        correction.insert(&mut batch);
    }
    let insert_ms = start.elapsed().as_secs_f64() * 1e3;
    let insert_peak = peak_rss_mib();

    let mut drain_ms = f64::NAN;
    if phase == "drain" {
        let start = Instant::now();
        for t in 0..num_ts {
            let upper = Antichain::from_elem(Timestamp::from(t + 1));
            let mut written: Vec<_> = correction.updates_before(&upper).collect();
            correction.insert_negated(&mut written);
            correction.advance_since(upper);
        }
        drain_ms = start.elapsed().as_secs_f64() * 1e3;
    }

    let updates = num_ts * updates_per_ts;
    let pool_budget_mib = pool_budget / 1024 / 1024;
    println!(
        "version={version} pager={pager} chunk_size={chunk_size} num_ts={num_ts} updates={updates} \
         insert_ms={insert_ms:.1} insert_peak_mib={insert_peak:.1} drain_ms={drain_ms:.1} \
         pool_budget_mib={pool_budget_mib} peak_mib={:.1} live_mib={:.1}",
        peak_rss_mib(),
        live_rss_mib(),
    );
}
