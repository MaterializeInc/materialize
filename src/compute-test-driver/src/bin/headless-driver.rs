//! Headless driver entry point for mzcompose. Connects to a running clusterd,
//! runs one scenario, exits non-zero on assertion failure.

use std::net::SocketAddr;
use std::time::Duration;

use mz_compute_test_driver::data::{
    rows_for_bytes, sample_desc, sample_rows, write_rows_single_ts,
};
use mz_compute_test_driver::dataflow::index_dataflow;
use mz_compute_test_driver::driver::Driver;
use mz_compute_test_driver::persist_host::PersistHost;
use mz_ore::cast::CastFrom;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{GlobalId, Timestamp};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    let client = host.client().await?;

    let shard = ShardId::new();
    let desc = sample_desc();
    let n = rows_for_bytes(target_bytes, 64);
    let rows = sample_rows(n, 64);
    write_rows_single_ts(&client, shard, &desc, &rows, Timestamp::from(0)).await?;

    let driver = Driver::connect(host, &compute_addr).await?;
    let (source_id, index_id) = (GlobalId::User(1000), GlobalId::User(1001));
    let df = index_dataflow(
        source_id,
        index_id,
        shard,
        loc,
        desc.clone(),
        vec![0],
        Timestamp::from(0),
    );
    driver.submit_dataflow(df)?;
    driver.schedule(index_id)?;
    driver
        .expect_frontier(index_id, Timestamp::from(1), Duration::from_secs(600))
        .await?;
    let count = driver
        .peek_count(index_id, desc, Timestamp::from(0))
        .await?;
    anyhow::ensure!(u64::cast_from(count) == n, "expected {n} rows, got {count}");
    println!("OK: indexed {count} rows");
    Ok(())
}
