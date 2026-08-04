// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Discovers boundaries that split a table's string primary key space into
//! ranges of roughly equal row counts, for parallel snapshot reads.
//!
//! All key matching and ordering happens server-side under the key column's
//! own collation. Rust never orders the returned prefixes.

use mz_ore::cast::CastLossy;

use crate::{KeyProber, MySqlError, QualifiedTableRef};

/// When partitioning the data, how many ranges per worker should we break the
/// keyspace into. This helps avoid underestimates for large row counts resulting
/// in severe skew.
const TARGET_RANGES_PER_WORKER: f64 = 8.0;

/// Computes up to `num_workers - 1` partition boundaries that divide the primary key space
/// into `num_workers` roughly even partitions. The boundaries are ordered according to MySQL's
/// internal collation rules. This should only be used against a single CHAR(N) or VARCHAR
/// primary key column.
///
/// NOTE: Run this inside a REPEATABLE READ transaction. The underlying probes
/// read separate snapshots otherwise, and concurrent inserts can make the
/// prefix walk see the same prefix repeatedly instead of advancing.
pub async fn partition_table_by_pk_prefix(
    conn: &mut mysql_async::Conn,
    table: QualifiedTableRef<'_>,
    pk_col: &str,
    num_workers: usize,
    estimated_row_count: u64,
    min_rows_per_worker: u64,
) -> Result<Vec<String>, MySqlError> {
    let (schema_name, table_name) = (table.schema_name, table.table_name);
    let mut db = KeyProber::new(conn, table, pk_col);
    let boundaries = partition(
        &mut db,
        num_workers,
        estimated_row_count,
        min_rows_per_worker,
    )
    .await?;
    tracing::trace!(
        schema = schema_name,
        table = table_name,
        ?boundaries,
        "partitioned table by pk prefix"
    );
    Ok(boundaries)
}

#[derive(Debug)]
struct Range {
    /// Exclusive start, `None` at the beginning of the key space.
    start: Option<String>,
    /// Exclusive end, `None` for the final open range.
    end: Option<String>,
    /// Row estimate for the range, at least 1.
    estimated_rows: f64,
    /// Prefix length this range was split at.
    depth: usize,
}

async fn partition<D: PartitionDb>(
    db: &mut D,
    workers: usize,
    estimated_row_count: u64,
    min_rows_per_worker: u64,
) -> Result<Vec<String>, MySqlError> {
    if workers <= 1 {
        return Ok(Vec::new());
    }
    let estimated_row_count = f64::cast_lossy(estimated_row_count.max(1));
    let min_rows_per_worker = f64::cast_lossy(min_rows_per_worker.max(1));
    let target_max_rows_per_range =
        get_target_max_rows_per_range(workers, estimated_row_count, min_rows_per_worker);
    // Should be many more ranges than workers unless the overall row count of the table is quite small.
    let ranges = split_into_ranges(db, estimated_row_count, target_max_rows_per_range).await?;
    Ok(assign_boundaries(&ranges, workers))
}

fn get_target_max_rows_per_range(
    workers: usize,
    estimated_row_count: f64,
    min_rows_per_worker: f64,
) -> f64 {
    // Break up the key space into smaller ranges to more accurately rebuild the per-worker ranges later with less skew.
    let estimated_rows_per_worker =
        (estimated_row_count / f64::cast_lossy(workers)).max(min_rows_per_worker);
    // Respect min_rows_per_worker as a lower bound for the granularity with which we attempt to break up the table.
    // No need to add this overhead for small tables. Estimate accuracy isn't super clear for small numbers.
    let target_rows_per_range =
        (estimated_rows_per_worker / TARGET_RANGES_PER_WORKER).max(min_rows_per_worker);

    tracing::debug!(
        estimated_row_count,
        workers,
        estimated_rows_per_worker,
        target_rows_per_range,
        "partitioning key space"
    );
    target_rows_per_range
}

async fn split_into_ranges<D: PartitionDb>(
    db: &mut D,
    estimated_row_count: f64,
    target_rows_per_bucket: f64,
) -> Result<Vec<Range>, MySqlError> {
    let mut ranges = vec![Range {
        start: None,
        end: None,
        estimated_rows: estimated_row_count,
        depth: 0,
    }];
    loop {
        let mut split_any = false;
        let mut next: Vec<Range> = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.estimated_rows > target_rows_per_bucket {
                split_any = true;
                next.extend(split_range(db, &range, target_rows_per_bucket).await?);
            } else {
                next.push(range);
            }
        }
        ranges = next;
        if !split_any {
            break;
        }
    }
    Ok(ranges)
}

/// Accumulates `ranges` (in key order) into `workers` buckets of roughly
/// equal estimated rows and returns the bucket edges as boundaries.
fn assign_boundaries(ranges: &[Range], workers: usize) -> Vec<String> {
    // Emit a boundary each time the cumulative estimated rows pass the next
    // worker's share.
    let total: f64 = ranges.iter().map(|r| r.estimated_rows).sum();
    let per_worker = total / f64::cast_lossy(workers);
    tracing::debug!(
        ranges = ranges.len(),
        total_estimated_rows = total,
        per_worker,
        "assigning prefix ranges to workers"
    );
    let mut boundaries: Vec<String> = Vec::with_capacity(workers - 1);
    let mut rows_seen = 0.0;
    for range in ranges {
        if boundaries.len() == workers - 1 {
            break;
        }
        rows_seen += range.estimated_rows;
        if rows_seen >= f64::cast_lossy(boundaries.len() + 1) * per_worker {
            // The final range's end is None (open), it can never be a boundary.
            if let Some(end) = &range.end {
                boundaries.push(end.clone());
            }
        }
    }
    boundaries
}

/// Splits `parent` at every distinct key prefix one character longer than the
/// prefix `parent`, i.e. "a" depth: 1 for a table with keys "a", "aa", "aaa", "ab" would be split to
/// "a" depth: 2 estimate 1, "aa" depth: 2 estimate 2, "ab" depth 2, estimate 1.
async fn split_range<D: PartitionDb>(
    db: &mut D,
    parent: &Range,
    target_rows: f64,
) -> Result<Vec<Range>, MySqlError> {
    let depth = parent.depth + 1;
    let mut children = Vec::new();

    let Some(mut cur) = db
        .first_prefix(parent.start.as_deref(), parent.end.as_deref(), depth)
        .await?
    else {
        return Ok(children);
    };
    // The first child inherits the parent's start.
    let mut start = parent.start.clone();

    loop {
        // Small optimization to return early if the remaining rows past the current start prefix
        // fits within the threshold we're looking for.
        let remaining = db
            .estimate_range_rows(start.as_deref(), parent.end.as_deref())
            .await?;
        let remaining = f64::cast_lossy(remaining).max(1.0);
        if remaining <= target_rows {
            children.push(Range {
                start,
                end: parent.end.clone(),
                estimated_rows: remaining,
                depth,
            });
            return Ok(children);
        }
        // When `cur` is an exact key shorter than `depth`, every key extending
        // it matches `cur`, so next_prefix exhausts even though those
        // extensions still need visiting. The first key past `cur` exposes
        // them, so the walk can keep splitting instead of retrying the whole
        // range at greater depths forever.
        let next = match db.next_prefix(&cur, parent.end.as_deref(), depth).await? {
            Some(next) => Some(next),
            None => {
                db.first_prefix(Some(&cur), parent.end.as_deref(), depth)
                    .await?
            }
        };
        // A prefix equal to `cur` cannot advance the walk (only a misbehaving
        // server produces one), stop splitting here.
        match next.filter(|next| next != &cur) {
            Some(next) => {
                let estimated_rows = db
                    .estimate_range_rows(start.as_deref(), Some(&next))
                    .await?;
                children.push(Range {
                    start: start.clone(),
                    end: Some(next.clone()),
                    estimated_rows: f64::cast_lossy(estimated_rows).max(1.0),
                    depth,
                });
                start = Some(next.clone());
                cur = next;
            }
            None => {
                children.push(Range {
                    start,
                    end: parent.end.clone(),
                    estimated_rows: remaining,
                    depth,
                });
                return Ok(children);
            }
        }
    }
}

/// Wrapper around KeyProber for testing purposes.
trait PartitionDb {
    async fn estimate_range_rows(
        &mut self,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<u64, MySqlError>;

    /// A `start` of `None` means the beginning of the key space.
    async fn first_prefix(
        &mut self,
        start: Option<&str>,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError>;

    /// Every key extending `cur` is covered by the match, including `cur`
    /// itself as an exact key.
    async fn next_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError>;
}

impl<'a> PartitionDb for KeyProber<'a> {
    async fn estimate_range_rows(
        &mut self,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<u64, MySqlError> {
        // A missing optimizer estimate reads as an empty range, which the
        // splitting loop drops or leaves unsplit.
        Ok(KeyProber::estimate_range_rows(self, start, end)
            .await?
            .unwrap_or(0))
    }

    async fn first_prefix(
        &mut self,
        start: Option<&str>,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        KeyProber::prefix_of_first_key_in_range(self, start, end, len).await
    }

    async fn next_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        KeyProber::prefix_of_first_row_not_matching_prefix(self, cur, end, len).await
    }
}
