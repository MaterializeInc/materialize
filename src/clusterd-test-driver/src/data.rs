// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Synthetic data generation and direct persist writes. This supports the
//! direct-write *use case*; the mechanism does not depend on it.

use std::sync::Arc;

use mz_ore::cast::CastFrom;
use mz_persist_client::Diagnostics;
use mz_persist_client::PersistClient;
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, RelationDesc, Row, SqlColumnType, SqlScalarType, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;

/// A two-column `(bigint, text)` schema used by the simple generators.
pub fn sample_desc() -> RelationDesc {
    RelationDesc::builder()
        .with_column(
            "id",
            SqlColumnType {
                scalar_type: SqlScalarType::Int64,
                nullable: false,
            },
        )
        .with_column(
            "payload",
            SqlColumnType {
                scalar_type: SqlScalarType::String,
                nullable: false,
            },
        )
        .finish()
}

/// Builds `n` rows; `payload` is `pad` bytes wide so callers can target a byte
/// budget (≈ `n * (pad + overhead)`).
pub fn sample_rows(n: u64, pad: usize) -> Vec<Row> {
    sample_rows_from(0, n, pad)
}

/// Like [`sample_rows`], but ids run `start..start + n`. Successive batches with
/// disjoint id ranges produce distinct rows that never consolidate with each
/// other, so a downstream count equals the total rows written.
pub fn sample_rows_from(start: u64, n: u64, pad: usize) -> Vec<Row> {
    (start..start + n)
        .map(|i| {
            let mut row = Row::default();
            let mut packer = row.packer();
            packer.push(Datum::Int64(i64::try_from(i).expect("fits")));
            let s = format!("{:0>width$}", i, width = pad);
            packer.push(Datum::String(&s));
            row
        })
        .collect()
}

/// Writes `rows` to `shard` at `ts`, advancing `upper` to `ts+1`. All rows are
/// inserted with diff `+1`. Returns once the append succeeds.
pub async fn write_rows_single_ts(
    client: &PersistClient,
    shard: ShardId,
    desc: &RelationDesc,
    rows: &[Row],
    ts: Timestamp,
) -> anyhow::Result<()> {
    let mut writer = client
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            Arc::new(desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics {
                shard_name: "driver-data".to_string(),
                handle_purpose: "headless driver write".to_string(),
            },
        )
        .await?;

    let updates: Vec<_> = rows
        .iter()
        .map(|r| ((SourceData(Ok(r.clone())), ()), ts, 1i64))
        .collect();
    let lower = Antichain::from_elem(ts);
    let upper = Antichain::from_elem(ts.step_forward());
    writer
        .compare_and_append(&updates, lower, upper)
        .await?
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Writes `rows` spread across timestamps `0..n_ts` (row `i` at time `i % n_ts`)
/// in a single append that seals `[0, n_ts)`. All rows are inserted with diff
/// `+1`. This is one `compare_and_append` regardless of `n_ts` — persist accepts
/// updates at any timestamp within the sealed range — so it stays fast even for
/// very large `n_ts` (a per-timestamp append would be `n_ts` consensus
/// round-trips).
pub async fn write_rows_spread(
    client: &PersistClient,
    shard: ShardId,
    desc: &RelationDesc,
    rows: &[Row],
    n_ts: u64,
) -> anyhow::Result<()> {
    assert!(n_ts > 0, "n_ts must be positive");
    let mut writer = client
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            Arc::new(desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics {
                shard_name: "driver-data".to_string(),
                handle_purpose: "headless driver spread write".to_string(),
            },
        )
        .await?;
    let updates: Vec<_> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let t = u64::cast_from(i) % n_ts;
            ((SourceData(Ok(r.clone())), ()), Timestamp::from(t), 1i64)
        })
        .collect();
    let lower = Antichain::from_elem(Timestamp::from(0));
    let upper = Antichain::from_elem(Timestamp::from(n_ts));
    writer
        .compare_and_append(&updates, lower, upper)
        .await?
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

/// Number of rows needed to roughly hit `target_bytes` given `pad`-wide
/// payloads. Overhead per row is approximate; coarse sizing, not exact.
pub fn rows_for_bytes(target_bytes: u64, pad: usize) -> u64 {
    let per_row = u64::cast_from(pad) + 24;
    (target_bytes / per_row).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist_host::PersistHost;
    use mz_persist_types::PersistLocation;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn write_then_snapshot_counts() {
        let host = PersistHost::start(PersistLocation::new_in_mem())
            .await
            .expect("host");
        let client = host.client().await.expect("client");
        let shard = ShardId::new();
        let desc = sample_desc();
        let rows = sample_rows(1000, 16);
        write_rows_single_ts(&client, shard, &desc, &rows, Timestamp::from(0))
            .await
            .expect("write");

        let mut reader = client
            .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("snapshot"),
                true,
            )
            .await
            .expect("reader");
        let as_of = Antichain::from_elem(Timestamp::from(0));
        let contents = reader.snapshot_and_fetch(as_of).await.expect("snapshot");
        let count: i64 = contents.iter().map(|(_, _, d)| *d).sum();
        assert_eq!(count, 1000);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn spread_write_snapshot_counts() {
        use crate::persist_host::PersistHost;
        use mz_persist_types::PersistLocation;

        let host = PersistHost::start(PersistLocation::new_in_mem())
            .await
            .expect("host");
        let client = host.client().await.expect("client");
        let shard = ShardId::new();
        let desc = sample_desc();
        let rows = sample_rows(1000, 16);
        write_rows_spread(&client, shard, &desc, &rows, 8)
            .await
            .expect("spread write");

        let mut reader = client
            .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("snapshot"),
                true,
            )
            .await
            .expect("reader");
        // Snapshot at the last written timestamp (7); all 1000 rows must be present.
        let as_of = Antichain::from_elem(Timestamp::from(7));
        let contents = reader.snapshot_and_fetch(as_of).await.expect("snapshot");
        let count: i64 = contents.iter().map(|(_, _, d)| *d).sum();
        assert_eq!(count, 1000);
    }

    #[mz_ore::test]
    fn rows_for_bytes_basic() {
        assert_eq!(rows_for_bytes(0, 16), 1); // always at least 1
        assert!(rows_for_bytes(1_000_000, 64) > 0);
    }
}
