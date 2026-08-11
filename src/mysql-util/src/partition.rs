// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::cast::CastFrom;
use mz_ore::str::redact;

use crate::{KeyProber, MySqlError, QualifiedTableRef};

/// Computes up to `num_workers - 1` partition boundaries that divide the primary key space
/// into `num_workers` roughly even partitions.
/// This should be run in a repeatable read transaction against a primary key varchar/char column
/// with the `utf8mb4_bin` collation.
pub async fn partition_table(
    conn: &mut mysql_async::Conn,
    table: QualifiedTableRef<'_>,
    pk_col: &str,
    num_workers: usize,
    estimated_row_count: u64,
    min_split_threshold: u64,
) -> Result<Vec<String>, MySqlError> {
    let (schema_name, table_name) = (table.schema_name, table.table_name);
    let mut db = KeyProber::new(conn, table, pk_col);
    let boundaries = partition(
        &mut db,
        num_workers,
        estimated_row_count,
        min_split_threshold,
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
}

async fn partition(
    db: &mut KeyProber<'_>,
    workers: usize,
    estimated_row_count: u64,
    min_split_threshold: u64,
) -> Result<Vec<String>, MySqlError> {
    if workers <= 1 {
        return Ok(Vec::new());
    }
    let estimated_row_count = estimated_row_count.max(1);

    // Estimates vary wildly especially near the full table size (see `KeyProber::estimate_range_rows` for more details).
    // Estimates tend to get more useful as smaller chunks, so break up the table into at least 1/8ths (2 workers * 4)
    // before selecting partitions. Breaking down to smaller partitions results in more accurate splits, so we keep the
    // 4x multiple of the worker count for > 2 workers.
    let target_max_rows_per_prefix = (estimated_row_count / u64::cast_from(workers * 4))
        .max(min_split_threshold)
        .max(1);

    compute_boundaries(db, workers, estimated_row_count, target_max_rows_per_prefix).await
}

async fn compute_boundaries(
    db: &mut KeyProber<'_>,
    workers: usize,
    estimated_row_count: u64,
    target_rows_per_prefix: u64,
) -> Result<Vec<String>, MySqlError> {
    // BFS of prefixes, splitting until estimates fall under the target.
    let mut ordered_prefixes = vec![Prefix {
        prefix: String::new(),
        end: None,
        estimated_rows: estimated_row_count,
        depth: 0,
    }];

    loop {
        let mut next_ordered_prefixes: Vec<Prefix> = vec![];
        let mut split_any = false;
        for prefix in ordered_prefixes {
            if prefix.estimated_rows > target_rows_per_prefix {
                split_any = true;
                // Partitioning children can drop some rows from the parent prefix range. This
                // is acceptable given the approximate nature of the algorithm.
                let children = children_prefixes(db, &prefix).await?;
                next_ordered_prefixes.extend(children);
            } else {
                next_ordered_prefixes.push(prefix);
            }
        }
        ordered_prefixes = next_ordered_prefixes;
        if !split_any {
            break;
        }
    }

    // Recompute the total after partitioning the table to get more even splits because the actual row count and the
    // granularly estimated row count can diverge from the original top level estimate.
    let total: u64 = ordered_prefixes.iter().map(|r| r.estimated_rows).sum();
    let per_worker = total / u64::cast_from(workers);
    tracing::debug!(
        prefixes = ordered_prefixes.len(),
        total_estimated_rows = total,
        per_worker,
        "assigning prefixes to workers"
    );
    let mut boundaries: Vec<String> = Vec::with_capacity(workers - 1);
    let mut rows_seen = 0;
    for prefix in &ordered_prefixes {
        if boundaries.len() == workers - 1 {
            break;
        }
        rows_seen += prefix.estimated_rows;
        if rows_seen >= u64::cast_from(boundaries.len() + 1) * per_worker {
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
/// Note: This will drop the key "a" on the floor, along with any keys
/// sorting below their own prefix (below-space characters at this depth).
async fn children_prefixes(
    db: &mut KeyProber<'_>,
    parent: &Prefix,
) -> Result<Vec<Prefix>, MySqlError> {
    let depth = parent.depth + 1;
    let mut children = Vec::new();

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
        children.push(Prefix {
            prefix: cur,
            end: end.clone(),
            estimated_rows: estimated_rows.max(1),
            depth,
        });
        match next {
            Some(next) => cur = next,
            None => return Ok(children),
        }
    }
}
