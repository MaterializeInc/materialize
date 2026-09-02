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

/// An owned scalar value, packable into a [`Row`].
///
/// Bridges synthetic generation and explicit script-provided values: both need
/// owned storage because [`Datum`] borrows its `String` and `Bytes` payloads. The
/// supported set is intentionally small; floats, numerics, and temporal types can
/// be added alongside their synthetic/parse rules when a scenario needs them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    /// SQL `NULL` (only valid in a nullable column).
    Null,
    /// `smallint`.
    Int16(i16),
    /// `integer`.
    Int32(i32),
    /// `bigint`.
    Int64(i64),
    /// `boolean`.
    Bool(bool),
    /// `text`.
    Str(String),
    /// `bytea`.
    Bytes(Vec<u8>),
}

impl Cell {
    /// Borrow this cell as a [`Datum`] for packing.
    pub fn datum(&self) -> Datum<'_> {
        match self {
            Cell::Null => Datum::Null,
            Cell::Int16(v) => Datum::from(*v),
            Cell::Int32(v) => Datum::from(*v),
            Cell::Int64(v) => Datum::from(*v),
            Cell::Bool(v) => Datum::from(*v),
            Cell::Str(v) => Datum::String(v),
            Cell::Bytes(v) => Datum::Bytes(v),
        }
    }
}

/// Pack one row from owned cells, in column order.
pub fn pack_cells(cells: &[Cell]) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    for cell in cells {
        packer.push(cell.datum());
    }
    row
}

/// A two-column `(bigint, text)` schema used as the default for the generators
/// and the script reader.
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

/// A deterministic synthetic value for a column of `scalar_type` at row index `i`.
///
/// `pad` widens `text` columns so callers can target a byte budget. Each value is
/// a function of `i`, so a row is distinct per `i` as long as the schema has at
/// least one wide-enough column (int, text, or bytes); an all-`bool` schema would
/// collide. Unsupported types panic — schema construction rejects them first.
pub fn synth_cell(scalar_type: &SqlScalarType, i: u64, pad: usize) -> Cell {
    match scalar_type {
        // Narrow ints wrap within their non-negative range; use a wide column as
        // the id for large row counts. The modulus keeps the value in range, so
        // `try_from` never fails.
        SqlScalarType::Int16 => Cell::Int16(i16::try_from(i % 0x8000).expect("fits i16")),
        SqlScalarType::Int32 => Cell::Int32(i32::try_from(i % 0x8000_0000).expect("fits i32")),
        SqlScalarType::Int64 => Cell::Int64(i64::try_from(i).expect("row index fits i64")),
        SqlScalarType::Bool => Cell::Bool(i % 2 == 0),
        SqlScalarType::String => Cell::Str(format!("{:0>width$}", i, width = pad)),
        SqlScalarType::Bytes => Cell::Bytes(format!("{:0>width$}", i, width = pad).into_bytes()),
        other => panic!("synth_cell: unsupported scalar type {other:?}"),
    }
}

/// Generate `n` synthetic rows for `desc`, with row indices running
/// `start..start + n`.
///
/// Successive batches over disjoint index ranges produce distinct rows that never
/// consolidate, so a downstream count equals the total rows written (provided the
/// schema carries a wide-enough column; see [`synth_cell`]).
pub fn synth_rows(desc: &RelationDesc, start: u64, n: u64, pad: usize) -> Vec<Row> {
    let types: Vec<SqlScalarType> = desc.iter_types().map(|c| c.scalar_type.clone()).collect();
    (start..start + n)
        .map(|i| {
            let cells: Vec<Cell> = types.iter().map(|t| synth_cell(t, i, pad)).collect();
            pack_cells(&cells)
        })
        .collect()
}

/// Builds `n` rows of the [`sample_desc`] schema; `payload` is `pad` bytes wide so
/// callers can target a byte budget (≈ `n * (pad + overhead)`).
pub fn sample_rows(n: u64, pad: usize) -> Vec<Row> {
    sample_rows_from(0, n, pad)
}

/// Like [`sample_rows`], but ids run `start..start + n`. Successive batches with
/// disjoint id ranges produce distinct rows that never consolidate with each
/// other, so a downstream count equals the total rows written.
pub fn sample_rows_from(start: u64, n: u64, pad: usize) -> Vec<Row> {
    synth_rows(&sample_desc(), start, n, pad)
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
