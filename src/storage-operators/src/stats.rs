// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits that connect up our mz-repr types with the stats that persist maintains.

use mz_persist_client::metrics::Metrics;
use mz_persist_client::read::{Cursor, LazyPartStats, ReadHandle, Since};
use mz_repr::{Diff, RelationDesc, Row, Timestamp};
use mz_storage_types::controller::TxnsCodecRow;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_storage_types::stats::RelationPartStats;
use mz_txn_wal::txn_cache::TxnsCache;
use timely::progress::Antichain;

/// This is a streaming-consolidating cursor type specialized to `RelationDesc`.
///
/// Internally this maintains two separate cursors: one for errors and one for data.
/// This is necessary so that errors are presented before data, which matches our usual
/// lookup semantics. To avoid being ludicrously inefficient, this pushes down a filter
/// on the stats. (In particular, in the common case of no errors, we don't do any extra
/// fetching.)
pub struct StatsCursor {
    errors: Cursor<SourceData, (), Timestamp, Diff>,
    data: Cursor<SourceData, (), Timestamp, Diff>,
}

impl StatsCursor {
    pub async fn new(
        handle: &mut ReadHandle<SourceData, (), Timestamp, Diff>,
        // If and only if we are using txn-wal to manage this shard, then
        // this must be Some. This is because the upper might be advanced lazily
        // and we have to go through txn-wal for reads.
        txns_read: Option<&mut TxnsCache<Timestamp, TxnsCodecRow>>,
        metrics: &Metrics,
        desc: &RelationDesc,
        as_of: Antichain<Timestamp>,
    ) -> Result<StatsCursor, Since<Timestamp>> {
        let should_fetch = |name: &'static str, count: fn(&RelationPartStats) -> Option<usize>| {
            move |stats: Option<&LazyPartStats>| {
                let Some(stats) = stats else { return true };
                let stats = stats.decode();
                let metrics = &metrics.pushdown.part_stats;
                let relation_stats = RelationPartStats::new(name, metrics, desc, &stats);
                count(&relation_stats).map_or(true, |n| n > 0)
            }
        };
        let (errors, data) = match txns_read {
            None => {
                let errors = handle
                    .snapshot_cursor(as_of.clone(), should_fetch("errors", |s| s.err_count()))
                    .await?;
                let data = handle
                    .snapshot_cursor(as_of.clone(), should_fetch("data", |s| s.ok_count()))
                    .await?;
                (errors, data)
            }
            Some(txns_read) => {
                let as_of = as_of
                    .as_option()
                    .expect("reads as_of empty antichain block forever")
                    .clone();
                let _ = txns_read.update_gt(&as_of).await;
                let data_snapshot = txns_read.data_snapshot(handle.shard_id(), as_of);
                let errors: Cursor<SourceData, (), Timestamp, i64> = data_snapshot
                    .snapshot_cursor(handle, should_fetch("errors", |s| s.err_count()))
                    .await?;
                let data = data_snapshot
                    .snapshot_cursor(handle, should_fetch("data", |s| s.ok_count()))
                    .await?;
                (errors, data)
            }
        };

        Ok(StatsCursor { errors, data })
    }

    pub async fn next(
        &mut self,
    ) -> Option<impl Iterator<Item = (Result<Row, DataflowError>, Timestamp, Diff)> + '_> {
        fn expect_decode(
            raw: impl Iterator<
                Item = (
                    (Result<SourceData, String>, Result<(), String>),
                    Timestamp,
                    Diff,
                ),
            >,
            is_err: bool,
        ) -> impl Iterator<Item = (Result<Row, DataflowError>, Timestamp, Diff)> {
            raw.map(|((k, v), t, d)| {
                // NB: this matches the decode behaviour in sources
                let SourceData(row) = k.expect("decode error");
                let () = v.expect("decode error");
                (row, t, d)
            })
            .filter(move |(r, _, _)| if is_err { r.is_err() } else { r.is_ok() })
        }

        if let Some(errors) = self.errors.next().await {
            Some(expect_decode(errors, true))
        } else if let Some(data) = self.data.next().await {
            Some(expect_decode(data, false))
        } else {
            None
        }
    }
}
