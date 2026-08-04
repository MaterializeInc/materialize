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
//! Ranges are split on character prefixes: a range the optimizer estimates
//! too large is subdivided at each distinct key prefix one character longer
//! than the prefix it was last split at, recursively, until every range is
//! small. Adjacent small ranges are then accumulated back into one bucket per
//! worker. Row estimates come from `EXPLAIN`, so probing a range costs an
//! index dive instead of a scan, and inaccurate estimates skew bucket sizes
//! but never correctness: any ordered boundary list partitions the key space.
//!
//! All key matching and ordering happens server-side under the key column's
//! own collation. Rust never orders the returned prefixes, it only checks
//! them for byte equality as a termination guard.

use mz_ore::cast::CastLossy;

use crate::{KeyProber, MySqlError, QualifiedTableRef};

/// Longest key prefix to split on, bounds the refinement loop.
const MAX_DEPTH: usize = 16;
/// Hard cap on children from a single split, bounds the walk even if the
/// server's ordering misbehaves.
const MAX_CHILDREN_PER_SPLIT: usize = 4096;

/// Computes up to `workers - 1` exclusive upper bounds, in key order, that
/// split `table` into per-worker key ranges of roughly equal estimated row
/// counts. Worker `i` reads keys in `[boundaries[i - 1], boundaries[i])`,
/// with the first and last range open ended. Returns an empty list when the
/// table is too small to be worth splitting.
///
/// `pk_col` is the raw (unquoted) name of the key column. It must be a string
/// column, prefixes of a numeric column do not order consistently with its
/// values, and it should be the leading column of an index or every probe
/// becomes a full table scan. `estimated_row_count` seeds the bucket sizing
/// and may be approximate. No range is split below `min_bucket_rows` rows.
pub async fn partition_table_by_pk_prefix(
    conn: &mut mysql_async::Conn,
    table: QualifiedTableRef<'_>,
    pk_col: &str,
    workers: usize,
    estimated_row_count: u64,
    min_bucket_rows: u64,
) -> Result<Vec<String>, MySqlError> {
    let (schema_name, table_name) = (table.schema_name, table.table_name);
    let mut db = KeyProber::new(conn, table, pk_col);
    let boundaries = partition(&mut db, workers, estimated_row_count, min_bucket_rows).await?;
    tracing::trace!(
        schema = schema_name,
        table = table_name,
        ?boundaries,
        "partitioned table by pk prefix"
    );
    Ok(boundaries)
}

/// What the partitioner needs from the database. Keys are treated as strings
/// and split by prefix, so implementations must order prefixes consistently
/// with the full keys they abbreviate.
///
/// Bounds are exclusive on both sides and optional: a `start` of `None`
/// means the beginning of the key space, an `end` of `None` means unbounded.
/// A key exactly equal to a bound or prefix argument is skipped, its
/// extensions still surface through the exclusive bound.
trait PartitionDb {
    /// Row count estimate for keys in `(start, end)`. May be arbitrarily
    /// inaccurate.
    async fn estimate_range_rows(
        &mut self,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<u64, MySqlError>;

    /// The prefix of up to `len` characters of the first key in
    /// `(start, end)`, or `None` if the range holds no rows.
    async fn first_prefix(
        &mut self,
        start: Option<&str>,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError>;

    /// The prefix of up to `len` characters of the first key past the last
    /// key matching `cur`, taken from keys below `end`. `None` if no such
    /// key exists. Every key extending `cur` is covered by the match,
    /// including `cur` itself as an exact key.
    async fn next_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError>;
}

/// A half open key range `[start, end)` tracked by the splitting loop.
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
    min_bucket_rows: u64,
) -> Result<Vec<String>, MySqlError> {
    if workers <= 1 {
        return Ok(Vec::new());
    }
    let estimated_row_count = f64::cast_lossy(estimated_row_count.max(1));
    let min_bucket_rows = f64::cast_lossy(min_bucket_rows.max(1));

    // Aim for more buckets than workers. With exactly one bucket per worker
    // an under-estimated table size could make the whole table look too small
    // to split. Small buckets are recombined when boundaries are assigned.
    let mut buckets = workers;
    while buckets < 8 {
        buckets *= 2;
    }
    let target_bucket_rows = (estimated_row_count / f64::cast_lossy(buckets)).max(min_bucket_rows);
    // Split ranges down to well below the bucket size so that accumulated
    // bucket boundaries can land close to their targets.
    let target_split_rows = (target_bucket_rows / 8.0).max(min_bucket_rows);
    tracing::debug!(
        estimated_row_count,
        buckets,
        target_bucket_rows,
        target_split_rows,
        "partitioning key space by prefix"
    );

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
            if range.estimated_rows > target_split_rows && range.depth < MAX_DEPTH {
                split_any = true;
                // An empty child list means the range holds no rows (a
                // phantom estimate) and is dropped. A single child spanning
                // the whole parent comes back with a greater depth, so
                // retrying it splits on a longer prefix and MAX_DEPTH bounds
                // the loop.
                next.extend(split_range(db, &range, target_split_rows).await?);
            } else {
                next.push(range);
            }
        }
        ranges = next;
        if !split_any {
            break;
        }
    }

    // Ranges are in key order. Emit a boundary each time the cumulative
    // estimated rows pass the next worker's share.
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
    for range in &ranges {
        if boundaries.len() == workers - 1 {
            break;
        }
        rows_seen += range.estimated_rows;
        if rows_seen >= f64::cast_lossy(boundaries.len() + 1) * per_worker {
            // The final range's end is None (open), it can never be a boundary.
            if let Some(end) = &range.end {
                // Only a misbehaving server can repeat an end (the walk stops
                // on non-advancing prefixes). Skip it, a duplicate boundary
                // would fail the strict monotonicity check downstream.
                if boundaries.last() != Some(end) {
                    boundaries.push(end.clone());
                }
            }
        }
    }
    Ok(boundaries)
}

/// Splits `parent` at every distinct key prefix one character longer than the
/// prefix `parent` was split at, except that a tail whose estimate already
/// fits `target_rows` stays a single child.
async fn split_range<D: PartitionDb>(
    db: &mut D,
    parent: &Range,
    target_rows: f64,
) -> Result<Vec<Range>, MySqlError> {
    let len = parent.depth + 1;
    let mut children = Vec::new();

    let Some(mut cur) = db
        .first_prefix(parent.start.as_deref(), parent.end.as_deref(), len)
        .await?
    else {
        return Ok(children);
    };
    // The first child inherits the parent's start.
    let mut start = parent.start.clone();

    loop {
        // If the remaining rows fit the target, emit them as one child and
        // stop. The children cap likewise closes out the split with whatever
        // remains.
        let remaining = db
            .estimate_range_rows(start.as_deref(), parent.end.as_deref())
            .await?;
        let remaining = f64::cast_lossy(remaining).max(1.0);
        if remaining <= target_rows || children.len() + 1 >= MAX_CHILDREN_PER_SPLIT {
            children.push(Range {
                start,
                end: parent.end.clone(),
                estimated_rows: remaining,
                depth: len,
            });
            return Ok(children);
        }
        // When `cur` is an exact key shorter than `len`, every key extending
        // it matches `cur`, so next_prefix exhausts even though those
        // extensions still need visiting. The first key past `cur` exposes
        // them, so the walk can keep splitting instead of retrying the whole
        // range at greater depths forever.
        let next = match db.next_prefix(&cur, parent.end.as_deref(), len).await? {
            Some(next) => Some(next),
            None => {
                db.first_prefix(Some(&cur), parent.end.as_deref(), len)
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
                    depth: len,
                });
                start = Some(next.clone());
                cur = next;
            }
            None => {
                children.push(Range {
                    start,
                    end: parent.end.clone(),
                    estimated_rows: remaining,
                    depth: len,
                });
                return Ok(children);
            }
        }
    }
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
