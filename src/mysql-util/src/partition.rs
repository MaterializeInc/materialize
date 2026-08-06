// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::cast::CastLossy;
use mz_ore::str::redact;

use crate::{KeyProber, MySqlError, QualifiedTableRef};

/// Computes up to `num_workers - 1` partition boundaries that divide the primary key space
/// into `num_workers` roughly even partitions.
/// This should be run in a repeatable read transaction against a primary key varchar/char column
/// with collation that compares character by character (no contractions, expansions or ignorable
/// characters).
pub async fn partition_table(
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
        // The boundaries are user data, redacted outside of CI.
        boundaries = ?redact(&boundaries),
        "partitioned table by pk prefix"
    );
    Ok(boundaries)
}

#[derive(Debug)]
struct Prefix {
    /// Empty for the beginning of the key space.
    prefix: String,
    /// Exclusive end, `None` for the final open prefix.
    end: Option<String>,
    /// Row estimate for the prefix, at least 1.
    estimated_rows: u64,
    /// Length this prefix was split at.
    depth: usize,
    /// Use the position within each parent as a surrogate sort key to maintain the sort ordering
    /// specified by MySQL.
    surrogate_sort_key: Vec<usize>,
}

async fn partition(
    db: &mut KeyProber<'_>,
    workers: usize,
    estimated_row_count: u64,
    min_rows_per_worker: u64,
) -> Result<Vec<String>, MySqlError> {
    if workers <= 1 {
        return Ok(Vec::new());
    }
    let estimated_row_count = estimated_row_count.max(1);

    // Estimates vary wildly especially near the full table size (see `KeyProber::estimate_range_rows` for more details).
    // Estimates tend to get more useful as smaller chunks, so break up the table into at least 1/8ths before selecting partitions.
    // Prefixes estimate at least one row, so a target below one never
    // converges.
    let target_max_rows_per_prefix = (f64::cast_lossy(estimated_row_count)
        / f64::cast_lossy(workers.max(8)))
    .max(f64::cast_lossy(min_rows_per_worker))
    .max(1.0);

    compute_boundaries(db, workers, estimated_row_count, target_max_rows_per_prefix).await
}

async fn compute_boundaries(
    db: &mut KeyProber<'_>,
    workers: usize,
    estimated_row_count: u64,
    target_rows_per_prefix: f64,
) -> Result<Vec<String>, MySqlError> {
    // BFS of prefixes, splitting until estimates fall under the target.
    let mut final_prefixes: Vec<Prefix> = vec![];
    let mut pending_prefixes = vec![Prefix {
        prefix: String::new(),
        end: None,
        estimated_rows: estimated_row_count,
        depth: 0,
        surrogate_sort_key: Vec::new(),
    }];

    while !pending_prefixes.is_empty() {
        let mut next: Vec<Prefix> = Vec::with_capacity(pending_prefixes.len());
        for prefix in pending_prefixes {
            for child in children_prefixes(db, &prefix).await? {
                if f64::cast_lossy(child.estimated_rows) > target_rows_per_prefix {
                    next.push(child);
                } else {
                    final_prefixes.push(child);
                }
            }
        }
        pending_prefixes = next;
    }
    final_prefixes.sort_unstable_by(|a, b| a.surrogate_sort_key.cmp(&b.surrogate_sort_key));

    // Recompute the total after partitioning the table to get more even splits because the actual row count and the
    // granularly estimated row count can diverge from the original top level estimate.
    let total: f64 = final_prefixes
        .iter()
        .map(|r| f64::cast_lossy(r.estimated_rows))
        .sum();
    let per_worker = total / f64::cast_lossy(workers);
    tracing::debug!(
        prefixes = final_prefixes.len(),
        total_estimated_rows = total,
        per_worker,
        "assigning prefixes to workers"
    );
    let mut boundaries: Vec<String> = Vec::with_capacity(workers - 1);
    let mut rows_seen = 0.0;
    for prefix in &final_prefixes {
        if boundaries.len() == workers - 1 {
            break;
        }
        rows_seen += f64::cast_lossy(prefix.estimated_rows);
        if rows_seen >= f64::cast_lossy(boundaries.len() + 1) * per_worker {
            // The final prefix's end is None (open), it can never be a boundary.
            if let Some(end) = &prefix.end {
                boundaries.push(end.clone());
            }
        }
    }
    Ok(boundaries)
}

/// Splits `parent` into prefixes one character longer. i.e. prefix "a", upper bound "b" in table
/// with pks: ["a", "ab", "abc", "abd", "af", "bb"] will return: ["ab", "af"].
///
/// Note: This will drop the key "a" on the floor. We accept this because we only lose
/// at max one row per prefix we step deeper into.
async fn children_prefixes(
    db: &mut KeyProber<'_>,
    parent: &Prefix,
) -> Result<Vec<Prefix>, MySqlError> {
    let depth = parent.depth + 1;
    let mut children = Vec::new();

    // Guaranteed to return None or a key longer than the current prefix assuming the upper
    // bound correctly caps keys to the current prefix and we're in a transaction where
    // new keys with a shorter length can't be inserted. Note that this only holds for
    // collations that sort character-by-character.
    let Some(mut cur) = db
        .prefix_of_first_key_in_range(&parent.prefix, parent.end.as_deref(), depth)
        .await?
    else {
        return Ok(children);
    };

    loop {
        let next = db
            .prefix_of_first_row_not_matching_prefix(&cur, parent.end.as_deref(), depth)
            .await?;
        let end = next.clone().or_else(|| parent.end.clone());
        let estimated_rows = db.estimate_range_rows(&cur, end.as_deref()).await?;
        let mut surrogate_sort_key = parent.surrogate_sort_key.clone();
        surrogate_sort_key.push(children.len());
        children.push(Prefix {
            prefix: cur,
            end: end.clone(),
            estimated_rows: estimated_rows.max(1),
            depth,
            surrogate_sort_key,
        });
        match next {
            Some(next) => cur = next,
            None => return Ok(children),
        }
    }
}
