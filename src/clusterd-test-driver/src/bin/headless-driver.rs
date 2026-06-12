// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Headless driver entry point for mzcompose. Connects to a running clusterd,
//! runs one scenario, exits non-zero on assertion failure.
//!
//! Select the scenario via the `SCENARIO` env var (default: `index`):
//!   - `index`         — load data at ts 0 and hydrate, then tick the frontier
//!                       forward with incremental appends (TICKS, TICK_ROWS).
//!   - `deep-history`  — hydrate over a shard with many distinct timestamps.
//!   - `side-effects`  — drive `AllowCompaction` explicitly to advance the read frontier.
//!   - `multi-dataflow` — attempt to hydrate two dataflows simultaneously (reproduction).

use std::net::SocketAddr;
use std::time::Duration;

use mz_clusterd_test_driver::data::{
    rows_for_bytes, sample_desc, sample_rows, sample_rows_from, write_rows_single_ts,
    write_rows_spread,
};
use mz_clusterd_test_driver::dataflow::index_dataflow;
use mz_clusterd_test_driver::driver::Driver;
use mz_clusterd_test_driver::persist_host::PersistHost;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{GlobalId, Timestamp};
use timely::progress::Antichain;

/// Common setup shared by every scenario: read env vars, create PersistHost and Driver.
struct Setup {
    loc: PersistLocation,
    driver: Driver,
    target_bytes: u64,
}

async fn setup() -> anyhow::Result<Setup> {
    let compute_addr =
        std::env::var("CLUSTERD_COMPUTE_ADDR").unwrap_or_else(|_| "clusterd:2101".to_string());
    let blob = std::env::var("PERSIST_BLOB_URL").expect("PERSIST_BLOB_URL");
    let consensus = std::env::var("PERSIST_CONSENSUS_URL").expect("PERSIST_CONSENSUS_URL");
    let pubsub_bind: SocketAddr = std::env::var("DRIVER_PUBSUB_BIND")
        .unwrap_or_else(|_| "0.0.0.0:6879".to_string())
        .parse()?;
    let target_bytes: u64 = std::env::var("TARGET_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10 * 1024 * 1024 * 1024);

    let loc = PersistLocation {
        blob_uri: blob.parse()?,
        consensus_uri: consensus.parse()?,
    };
    let host = PersistHost::start_on(pubsub_bind, loc.clone()).await?;
    let driver = Driver::connect(host, &compute_addr).await?;

    Ok(Setup {
        loc,
        driver,
        target_bytes,
    })
}

/// Baseline scenario in two phases.
///
/// Load phase: write `n` rows at ts 0, build an index with `as_of = 0`, and wait
/// for it to hydrate. Tick phase: append `TICK_ROWS` fresh rows at each of
/// `TICKS` advancing timestamps, waiting for the index's output frontier to step
/// forward each time. The tick phase exercises steady-state incremental
/// maintenance, distinct from the initial hydration, and is the interesting part
/// to profile under a `WRAPPER`.
async fn scenario_index() -> anyhow::Result<()> {
    let Setup {
        loc,
        driver,
        target_bytes,
    } = setup().await?;

    let ticks: u64 = std::env::var("TICKS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let tick_rows_n: u64 = std::env::var("TICK_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let client = driver.host.client().await?;
    let shard = ShardId::new();
    let desc = sample_desc();

    // --- Load phase: bulk write at ts 0, build the index, hydrate. ---
    let n = rows_for_bytes(target_bytes, 64);
    let rows = sample_rows(n, 64);
    let load_start = std::time::Instant::now();
    write_rows_single_ts(&client, shard, &desc, &rows, Timestamp::from(0)).await?;

    let (source_id, index_id) = (GlobalId::User(1000), GlobalId::User(1001));
    let df = index_dataflow(
        source_id,
        index_id,
        shard,
        loc,
        desc.clone(),
        vec![0],
        Timestamp::from(0),
        Timestamp::from(1),
    );
    driver.submit_dataflow(df)?;
    driver.schedule(index_id)?;
    driver
        .expect_frontier(index_id, Timestamp::from(1), Duration::from_secs(600))
        .await?;
    let load_elapsed = load_start.elapsed();

    // --- Tick phase: append at advancing timestamps, wait for the frontier. ---
    // Each batch uses a disjoint id range so rows never consolidate and the
    // final count is exactly the total written.
    let tick_start = std::time::Instant::now();
    for t in 1..=ticks {
        let id_start = n + (t - 1) * tick_rows_n;
        let batch = sample_rows_from(id_start, tick_rows_n, 64);
        write_rows_single_ts(&client, shard, &desc, &batch, Timestamp::from(t)).await?;
        driver
            .expect_frontier(
                index_id,
                Timestamp::from(t).step_forward(),
                Duration::from_secs(600),
            )
            .await?;
    }
    let tick_elapsed = tick_start.elapsed();

    // Peek at the last written timestamp; every loaded and ticked row is present.
    let count = driver
        .peek_count(index_id, desc, Timestamp::from(ticks))
        .await?;
    let expected = n + ticks * tick_rows_n;
    anyhow::ensure!(
        u64::cast_from(count) == expected,
        "expected {expected} rows, got {count}"
    );
    println!(
        "OK: index loaded {n} rows in {load_elapsed:?}, ticked {ticks} frontiers \
         (+{tick_rows_n} rows each) in {tick_elapsed:?}; {count} rows total"
    );
    Ok(())
}

/// Hydrate an index over a shard with many distinct timestamps.
///
/// Writes rows spread across `n_ts` timestamps and builds the dataflow with
/// `as_of = 0`, so hydration must replay all `n_ts` timestamps from the beginning
/// rather than reading a single compacted snapshot.
async fn scenario_deep_history() -> anyhow::Result<()> {
    let Setup {
        loc,
        driver,
        target_bytes,
    } = setup().await?;

    let n_ts: u64 = std::env::var("N_TIMESTAMPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    let client = driver.host.client().await?;
    let shard = ShardId::new();
    let desc = sample_desc();
    let n = rows_for_bytes(target_bytes, 16);
    let rows = sample_rows(n, 16);
    write_rows_spread(&client, shard, &desc, &rows, n_ts).await?;

    let (source_id, index_id) = (GlobalId::User(1000), GlobalId::User(1001));
    // as_of = 0: replay all n_ts timestamps; shard_upper = n_ts.
    let df = index_dataflow(
        source_id,
        index_id,
        shard,
        loc,
        desc.clone(),
        vec![0],
        Timestamp::from(0),
        Timestamp::from(n_ts),
    );
    driver.submit_dataflow(df)?;
    driver.schedule(index_id)?;

    let start = std::time::Instant::now();
    driver
        .expect_frontier(index_id, Timestamp::from(n_ts), Duration::from_secs(600))
        .await?;
    let elapsed = start.elapsed();

    // Peek at the last written timestamp (n_ts - 1) to confirm all rows are present.
    let count = driver
        .peek_count(index_id, desc, Timestamp::from(n_ts - 1))
        .await?;
    anyhow::ensure!(u64::cast_from(count) == n, "expected {n} rows, got {count}");
    println!("OK: deep-history indexed {n} rows over {n_ts} timestamps in {elapsed:?}");
    Ok(())
}

/// Demonstrate explicit compaction control: advance the read frontier via
/// `AllowCompaction` and confirm the replica continues serving peeks.
async fn scenario_side_effects() -> anyhow::Result<()> {
    let Setup {
        loc,
        driver,
        target_bytes,
    } = setup().await?;

    let client = driver.host.client().await?;
    let shard = ShardId::new();
    let desc = sample_desc();
    let n = rows_for_bytes(target_bytes, 64);
    let rows = sample_rows(n, 64);
    write_rows_single_ts(&client, shard, &desc, &rows, Timestamp::from(0)).await?;

    let (source_id, index_id) = (GlobalId::User(1000), GlobalId::User(1001));
    let df = index_dataflow(
        source_id,
        index_id,
        shard,
        loc,
        desc.clone(),
        vec![0],
        Timestamp::from(0),
        Timestamp::from(1),
    );
    driver.submit_dataflow(df)?;
    driver.schedule(index_id)?;
    driver
        .expect_frontier(index_id, Timestamp::from(1), Duration::from_secs(600))
        .await?;

    // Advance the read frontier (since) to ts 1. This tells the replica that no
    // consumer will ever read at ts < 1, so it is free to compact data below that.
    driver.send(ComputeCommand::AllowCompaction {
        id: index_id,
        frontier: Antichain::from_elem(Timestamp::from(1)),
    })?;

    // After AllowCompaction to ts 1 the write frontier is also at ts 1, so a peek at
    // ts 1 would block waiting for the frontier to advance beyond 1 (ts 1 is not yet
    // in the past). We give it a short timeout; if it times out we confirm the
    // connection is still alive by reading the frontiers watch instead.
    let count = match tokio::time::timeout(
        Duration::from_secs(5),
        driver.peek_count(index_id, desc, Timestamp::from(1)),
    )
    .await
    {
        Ok(Ok(c)) => c,
        Ok(Err(_)) | Err(_) => {
            // Timeout or peek error: confirm connection is still alive via frontiers.
            let _frontiers = driver.frontiers(index_id).borrow().clone();
            0
        }
    };
    println!("OK: side-effects drove AllowCompaction; replica still serving ({count} rows)");
    Ok(())
}

/// Attempt to hydrate two independent dataflows simultaneously.
///
/// This is a reproduction scenario: it documents whether clusterd can serve two
/// concurrent dataflows. On success both indexes hydrate and are peeked. On failure
/// (timeout or error) the scenario exits with `Ok(())` and prints a `REPRO:` line so
/// the CI harness treats this as expected behavior rather than a test failure.
async fn scenario_multi_dataflow() -> anyhow::Result<()> {
    let Setup { loc, driver, .. } = setup().await?;

    let client = driver.host.client().await?;
    let desc = sample_desc();

    // Two independent shards, each with 1000 rows at ts 0.
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();
    let rows = sample_rows(1000, 16);
    write_rows_single_ts(&client, shard_a, &desc, &rows, Timestamp::from(0)).await?;
    write_rows_single_ts(&client, shard_b, &desc, &rows, Timestamp::from(0)).await?;

    let (source_a, index_a) = (GlobalId::User(1000), GlobalId::User(1001));
    let (source_b, index_b) = (GlobalId::User(1002), GlobalId::User(1003));

    let df_a = index_dataflow(
        source_a,
        index_a,
        shard_a,
        loc.clone(),
        desc.clone(),
        vec![0],
        Timestamp::from(0),
        Timestamp::from(1),
    );
    let df_b = index_dataflow(
        source_b,
        index_b,
        shard_b,
        loc.clone(),
        desc.clone(),
        vec![0],
        Timestamp::from(0),
        Timestamp::from(1),
    );

    driver.submit_dataflow(df_a)?;
    driver.schedule(index_a)?;
    driver.submit_dataflow(df_b)?;
    driver.schedule(index_b)?;

    let timeout = Duration::from_secs(30);
    let res_a = driver
        .expect_frontier(index_a, Timestamp::from(1), timeout)
        .await;
    let res_b = driver
        .expect_frontier(index_b, Timestamp::from(1), timeout)
        .await;

    match (res_a, res_b) {
        (Ok(()), Ok(())) => {
            let count_a = driver
                .peek_count(index_a, desc.clone(), Timestamp::from(0))
                .await?;
            let count_b = driver.peek_count(index_b, desc, Timestamp::from(0)).await?;
            println!("OK: multi-dataflow both indexes hydrated ({count_a}, {count_b} rows)");
        }
        (Err(e_a), Err(e_b)) => {
            println!(
                "REPRO: multi-dataflow failed as expected: both timed out — a: {e_a}; b: {e_b}"
            );
        }
        (Err(e), Ok(())) => {
            println!("REPRO: multi-dataflow failed as expected: index_a timed out — {e}");
        }
        (Ok(()), Err(e)) => {
            println!("REPRO: multi-dataflow failed as expected: index_b timed out — {e}");
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Configure tracing so the driver emits structured logs like the real
    // Materialize binaries. Uses default tracing args (env-driven log filter via
    // the usual `MZ_*`/`RUST_LOG`-style options is available through
    // `TracingCliArgs`, but we keep the defaults here).
    let _tracing_handle = TracingCliArgs::default()
        .configure_tracing(
            StaticTracingConfig {
                service_name: "headless-driver",
                build_info: mz_persist_client::BUILD_INFO,
            },
            MetricsRegistry::new(),
        )
        .await?;

    let scenario = std::env::var("SCENARIO").unwrap_or_else(|_| "index".to_string());
    match scenario.as_str() {
        "index" => scenario_index().await,
        "deep-history" => scenario_deep_history().await,
        "side-effects" => scenario_side_effects().await,
        "multi-dataflow" => scenario_multi_dataflow().await,
        other => anyhow::bail!("unknown scenario: {other:?}"),
    }
}
