// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Periodic INFO-level stats line.
//!
//! Each interval, snapshots a handful of committer counters and histograms
//! and logs a single line so that bottleneck attribution is possible without
//! a Prometheus scrape. Targeted at operators reading raw envd logs in CI
//! and during incident response.

use std::time::Duration;

use mz_ore::cast::CastLossy;
use mz_ore::task::JoinHandle;
use tracing::info;

use crate::metrics::CommitterMetrics;

/// The ops the heartbeat reports on. Kept in sync with the labels used at
/// metric call sites.
const OPS: &[&str] = &["head", "scan", "cas", "truncate", "list_keys"];

/// Per-op counter / histogram snapshot taken at one tick.
#[derive(Clone, Debug, Default)]
struct OpSnapshot {
    ok_total: u64,
    err_det_total: u64,
    err_indet_total: u64,
    cas_committed_total: u64,
    cas_mismatch_total: u64,
    cache_hits: u64,
    cache_misses: u64,
    rpc_count: u64,
    rpc_sum: f64,
    backing_count: u64,
    backing_sum: f64,
}

#[derive(Clone, Debug, Default)]
struct Snapshot {
    ops: Vec<(&'static str, OpSnapshot)>,
    inflight_total: i64,
    inflight_by_op: Vec<(&'static str, i64)>,
    cached_shards: i64,
    refresh_lag_count: u64,
    refresh_lag_sum: f64,
    coalesce_batch_count: u64,
    coalesce_batch_sum: f64,
}

fn snapshot(metrics: &CommitterMetrics) -> Snapshot {
    let mut ops = Vec::with_capacity(OPS.len());
    let mut inflight_by_op = Vec::with_capacity(OPS.len());
    for op in OPS {
        let ok_total = metrics.rpc_total.with_label_values(&[op, "ok"]).get();
        let err_det_total = metrics
            .rpc_total
            .with_label_values(&[op, "err_determinate"])
            .get();
        let err_indet_total = metrics
            .rpc_total
            .with_label_values(&[op, "err_indeterminate"])
            .get();
        let cas_committed_total = if *op == "cas" {
            metrics
                .rpc_total
                .with_label_values(&[op, "committed"])
                .get()
        } else {
            0
        };
        let cas_mismatch_total = if *op == "cas" {
            metrics.rpc_total.with_label_values(&[op, "mismatch"]).get()
        } else {
            0
        };
        let cache_hits = metrics.cache_hits_total.with_label_values(&[op]).get();
        let cache_misses = metrics.cache_misses_total.with_label_values(&[op]).get();
        let rpc_hist = metrics.rpc_duration_seconds.with_label_values(&[op]);
        let backing_hist = metrics.backing_duration_seconds.with_label_values(&[op]);
        ops.push((
            *op,
            OpSnapshot {
                ok_total,
                err_det_total,
                err_indet_total,
                cas_committed_total,
                cas_mismatch_total,
                cache_hits,
                cache_misses,
                rpc_count: rpc_hist.get_sample_count(),
                rpc_sum: rpc_hist.get_sample_sum(),
                backing_count: backing_hist.get_sample_count(),
                backing_sum: backing_hist.get_sample_sum(),
            },
        ));
        inflight_by_op.push((
            *op,
            metrics.inflight_rpcs_by_op.with_label_values(&[op]).get(),
        ));
    }
    Snapshot {
        ops,
        inflight_total: metrics.inflight_rpcs.get(),
        inflight_by_op,
        cached_shards: metrics.cached_shards.get(),
        refresh_lag_count: metrics.cas_refresh_lag_seconds.get_sample_count(),
        refresh_lag_sum: metrics.cas_refresh_lag_seconds.get_sample_sum(),
        coalesce_batch_count: metrics.coalesce_batch_size.get_sample_count(),
        coalesce_batch_sum: metrics.coalesce_batch_size.get_sample_sum(),
    }
}

fn mean_ms(delta_count: u64, delta_sum: f64) -> f64 {
    if delta_count == 0 {
        0.0
    } else {
        (delta_sum / f64::cast_lossy(delta_count)) * 1_000.0
    }
}

fn emit(prev: &Snapshot, curr: &Snapshot, interval: Duration) {
    let mut per_op = String::new();
    // `prev.ops` and `curr.ops` are built from the same `OPS` slice in
    // `snapshot`, so they share length and ordering. Iterate by index to
    // avoid pulling in itertools for `zip_eq`.
    assert_eq!(prev.ops.len(), curr.ops.len());
    for i in 0..prev.ops.len() {
        let (op, p) = &prev.ops[i];
        let (_, c) = &curr.ops[i];
        let total_count = c.rpc_count.saturating_sub(p.rpc_count);
        let backing_count = c.backing_count.saturating_sub(p.backing_count);
        let total_mean = mean_ms(total_count, c.rpc_sum - p.rpc_sum);
        let backing_mean = mean_ms(backing_count, c.backing_sum - p.backing_sum);
        let hits = c.cache_hits.saturating_sub(p.cache_hits);
        let misses = c.cache_misses.saturating_sub(p.cache_misses);
        let hit_rate = if hits + misses == 0 {
            0.0
        } else {
            f64::cast_lossy(hits) / f64::cast_lossy(hits + misses)
        };
        let detail = if *op == "cas" {
            format!(
                "committed={} mismatch={} err_det={} err_indet={}",
                c.cas_committed_total.saturating_sub(p.cas_committed_total),
                c.cas_mismatch_total.saturating_sub(p.cas_mismatch_total),
                c.err_det_total.saturating_sub(p.err_det_total),
                c.err_indet_total.saturating_sub(p.err_indet_total),
            )
        } else {
            format!(
                "ok={} err_det={} err_indet={}",
                c.ok_total.saturating_sub(p.ok_total),
                c.err_det_total.saturating_sub(p.err_det_total),
                c.err_indet_total.saturating_sub(p.err_indet_total),
            )
        };
        per_op.push_str(&format!(
            " {op}={{n={total_count} mean_ms={total_mean:.2} backing_mean_ms={backing_mean:.2} \
             cache_hit_rate={hit_rate:.2} {detail}}}"
        ));
    }
    let inflight_str = curr
        .inflight_by_op
        .iter()
        .map(|(op, v)| format!("{op}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    let refresh_count = curr
        .refresh_lag_count
        .saturating_sub(prev.refresh_lag_count);
    let refresh_mean = mean_ms(refresh_count, curr.refresh_lag_sum - prev.refresh_lag_sum);
    // Mean number of CaS requests folded into one backing statement over the
    // interval. 0 batches means coalescing is disabled (or saw no traffic);
    // a mean stuck at 1.00 means it is enabled but found no concurrency.
    let coalesce_batches = curr
        .coalesce_batch_count
        .saturating_sub(prev.coalesce_batch_count);
    let coalesce_mean = if coalesce_batches == 0 {
        0.0
    } else {
        (curr.coalesce_batch_sum - prev.coalesce_batch_sum) / f64::cast_lossy(coalesce_batches)
    };
    info!(
        interval_s = interval.as_secs(),
        inflight_total = curr.inflight_total,
        inflight_by_op = inflight_str,
        cached_shards = curr.cached_shards,
        refresh_lag_count = refresh_count,
        refresh_lag_mean_ms = refresh_mean,
        coalesce_batches,
        coalesce_batch_mean = coalesce_mean,
        "persist committer stats:{per_op}",
    );
}

/// Spawn the heartbeat task. Logs one INFO line per `interval`. Returns a
/// `JoinHandle` whose `abort_on_drop` keeps the task lifecycle tied to the
/// `CommitterHandle`.
pub fn spawn_heartbeat(metrics: CommitterMetrics, interval: Duration) -> JoinHandle<()> {
    mz_ore::task::spawn(|| "persist_committer::heartbeat", async move {
        if interval.is_zero() {
            return;
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Discard the immediately-fired first tick so the first emitted line
        // covers a real interval rather than the boot moment.
        ticker.tick().await;
        let mut prev = snapshot(&metrics);
        loop {
            ticker.tick().await;
            let curr = snapshot(&metrics);
            emit(&prev, &curr, interval);
            prev = curr;
        }
    })
}
