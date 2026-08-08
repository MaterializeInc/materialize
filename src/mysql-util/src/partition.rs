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

async fn partition<D: PrimaryKeyProber>(
    db: &mut D,
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

async fn compute_boundaries<D: PrimaryKeyProber>(
    db: &mut D,
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
async fn children_prefixes<D: PrimaryKeyProber>(
    db: &mut D,
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

/// Wrapper around KeyProber for testing purposes.
trait PrimaryKeyProber {
    async fn estimate_range_rows(
        &mut self,
        start: &str,
        end: Option<&str>,
    ) -> Result<u64, MySqlError>;

    async fn prefix_of_first_key_in_range(
        &mut self,
        start: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError>;

    async fn prefix_of_first_row_not_matching_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError>;
}

impl<'a> PrimaryKeyProber for KeyProber<'a> {
    async fn estimate_range_rows(
        &mut self,
        start: &str,
        end: Option<&str>,
    ) -> Result<u64, MySqlError> {
        KeyProber::estimate_range_rows(self, start, end).await
    }

    async fn prefix_of_first_key_in_range(
        &mut self,
        start: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        KeyProber::prefix_of_first_key_in_range(self, start, end, len).await
    }

    async fn prefix_of_first_row_not_matching_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        KeyProber::prefix_of_first_row_not_matching_prefix(self, cur, end, len).await
    }
}

#[cfg(test)]
mod tests {
    use mysql_async::prelude::Queryable;
    use mysql_async::{Params, Value};
    use mz_ore::cast::CastFrom;

    use super::*;

    /// In-memory [`PrimaryKeyProber`] over a sorted key list with exact
    /// "estimates". Byte order stands in for the collation.
    struct MockDb {
        keys: Vec<String>,
    }

    impl MockDb {
        fn bounds(&self, start: &str, end: Option<&str>) -> (usize, usize) {
            // The lower bound is exclusive, a key equal to `start` is skipped.
            let lo = self.keys.partition_point(|k| k.as_str() <= start);
            let hi = match end {
                Some(e) => self.keys.partition_point(|k| k.as_str() < e),
                None => self.keys.len(),
            };
            (lo, hi.max(lo))
        }
    }

    impl PrimaryKeyProber for MockDb {
        async fn estimate_range_rows(
            &mut self,
            start: &str,
            end: Option<&str>,
        ) -> Result<u64, MySqlError> {
            let (lo, hi) = self.bounds(start, end);
            Ok(u64::cast_from(hi - lo))
        }

        async fn prefix_of_first_key_in_range(
            &mut self,
            start: &str,
            end: Option<&str>,
            len: usize,
        ) -> Result<Option<String>, MySqlError> {
            let (lo, hi) = self.bounds(start, end);
            if lo >= hi {
                return Ok(None);
            }
            Ok(Some(self.keys[lo].chars().take(len).collect()))
        }

        async fn prefix_of_first_row_not_matching_prefix(
            &mut self,
            cur: &str,
            end: Option<&str>,
            len: usize,
        ) -> Result<Option<String>, MySqlError> {
            let (_, hi) = self.bounds("", end);
            // Find the last key matching `cur`, byte prefixes stand in for
            // the collation's LIKE matching.
            let Some(last_match) = self.keys[..hi].iter().rposition(|k| k.starts_with(cur)) else {
                return Ok(None);
            };
            Ok(self.keys[last_match + 1..hi]
                .first()
                .map(|k| k.chars().take(len).collect()))
        }
    }

    fn keys(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("{i:06}")).collect()
    }

    const MIN_ROWS_PER_WORKER: u64 = 50_000;

    #[mz_ore::test(tokio::test)]
    async fn single_worker_gets_no_boundaries() -> Result<(), MySqlError> {
        let mut db = MockDb { keys: keys(1000) };
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 1, count, MIN_ROWS_PER_WORKER).await?;
        assert!(boundaries.is_empty());
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn small_table_gets_no_boundaries() -> Result<(), MySqlError> {
        // All keys share one depth-1 prefix and fit under `min_rows_per_worker`,
        // so the single open-ended range yields no boundary.
        let mut db = MockDb { keys: keys(10_000) };
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 4, count, MIN_ROWS_PER_WORKER).await?;
        assert!(boundaries.is_empty());
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn empty_table_gets_no_boundaries() -> Result<(), MySqlError> {
        let mut db = MockDb { keys: vec![] };
        let boundaries = partition(&mut db, 4, 0, MIN_ROWS_PER_WORKER).await?;
        assert!(boundaries.is_empty());
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn splits_evenly_across_workers() -> Result<(), MySqlError> {
        let mut db = MockDb {
            keys: keys(200_000),
        };
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 4, count, MIN_ROWS_PER_WORKER).await?;
        assert_eq!(boundaries.len(), 3);
        // Boundaries must be sorted and split the keys into ~50k chunks.
        let mut prev = 0;
        for b in &boundaries {
            let idx = db.keys.partition_point(|k| k.as_str() < b.as_str());
            let share = idx - prev;
            assert!(
                (40_000..=60_000).contains(&share),
                "uneven share {share} at boundary {b:?} (all: {boundaries:?})",
            );
            prev = idx;
        }
        assert!((40_000..=60_000).contains(&(db.keys.len() - prev)));
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn low_min_bucket_rows_splits_small_tables() -> Result<(), MySqlError> {
        let mut db = MockDb { keys: keys(1000) };
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 4, count, 10).await?;
        assert_eq!(boundaries.len(), 3);
        let mut prev = 0;
        for b in &boundaries {
            let idx = db.keys.partition_point(|k| k.as_str() < b.as_str());
            let share = idx - prev;
            assert!(
                (150..=350).contains(&share),
                "uneven share {share} at boundary {b:?} (all: {boundaries:?})",
            );
            prev = idx;
        }
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn short_key_does_not_block_splitting() -> Result<(), MySqlError> {
        // One key is a bare "U" and every other key extends it. The walk
        // skips the exact key (exclusive lower bounds) and must keep
        // splitting inside the extensions at greater depths instead of
        // stalling on the all-encompassing "U" prefix.
        let mut all_keys = vec!["U".to_string()];
        all_keys.extend((0..1000).map(|i| format!("U{i:06}")));
        let mut db = MockDb { keys: all_keys };
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 4, count, 10).await?;
        assert_eq!(boundaries.len(), 3);
        for b in &boundaries {
            assert!(
                b.starts_with('U') && b.len() > 1,
                "boundary {b:?} does not subdivide the extensions (all: {boundaries:?})"
            );
        }
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn empty_string_key_is_skipped() -> Result<(), MySqlError> {
        // The exclusive root bound (id > '') drops the empty key like any
        // other exact-match key, so no empty prefix is ever produced.
        let mut all_keys = vec![String::new()];
        all_keys.extend(keys(1000));
        let mut db = MockDb { keys: all_keys };
        let root = Prefix {
            prefix: String::new(),
            end: None,
            estimated_rows: 1001,
            depth: 0,
            surrogate_sort_key: Vec::new(),
        };
        let children = children_prefixes(&mut db, &root).await?;
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].prefix, "0");
        assert_eq!(children[0].end, None);

        let boundaries = partition(&mut db, 4, 1001, 10).await?;
        assert_eq!(boundaries, vec!["0003", "0005", "0008"]);
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn fractional_target_still_terminates() -> Result<(), MySqlError> {
        // count / max(workers, 8) is fractional and the minimum is zero, so
        // the target floors at one row instead of splitting forever.
        let mut db = MockDb { keys: keys(3) };
        let boundaries = partition(&mut db, 4, 3, 0).await?;
        assert_eq!(boundaries, vec!["000001", "000002"]);
        Ok(())
    }

    /// Exercises the partitioner against a live MySQL server, covering what
    /// the mock cannot see: `EXPLAIN` estimates over prepared statements,
    /// `LIKE` pattern semantics, and the nested next-prefix query.
    ///
    /// Skipped unless `MZ_TEST_MYSQL_URL` points at a server this test may
    /// scribble on, e.g. `mysql://root:p%40ssw0rd@127.0.0.1:13306`.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a network connection
    async fn test_live_mysql() -> Result<(), anyhow::Error> {
        let Ok(url) = std::env::var("MZ_TEST_MYSQL_URL") else {
            if mz_ore::env::is_var_truthy("CI") {
                panic!("CI is supposed to run this test but something has gone wrong!");
            }
            tracing::info!("MZ_TEST_MYSQL_URL not set: skipping live MySQL test");
            return Ok(());
        };
        let mut conn = mysql_async::Conn::new(mysql_async::Opts::from_url(&url)?).await?;

        // Static DDL strings, nothing to parameterize.
        #[allow(clippy::disallowed_methods)]
        {
            conn.query_drop("DROP DATABASE IF EXISTS mz_partition_test")
                .await?;
            conn.query_drop("CREATE DATABASE mz_partition_test").await?;
            conn.query_drop(
                "CREATE TABLE mz_partition_test.t (id VARCHAR(32) PRIMARY KEY NOT NULL)",
            )
            .await?;
            // 900 keys under 'a', 100 under 'b', plus LIKE metacharacter keys
            // and a bare 'a' that every 'a...' key extends.
            conn.query_drop(
                "INSERT INTO mz_partition_test.t \
                 WITH RECURSIVE n AS (SELECT 1 x UNION ALL SELECT x+1 FROM n WHERE x < 900) \
                 SELECT CONCAT('a', LPAD(x, 5, '0')) FROM n",
            )
            .await?;
            conn.query_drop("INSERT INTO mz_partition_test.t VALUES ('a')")
                .await?;
            conn.query_drop(
                "INSERT INTO mz_partition_test.t \
                 WITH RECURSIVE n AS (SELECT 1 x UNION ALL SELECT x+1 FROM n WHERE x < 100) \
                 SELECT CONCAT('b', LPAD(x, 5, '0')) FROM n",
            )
            .await?;
            conn.query_drop("INSERT INTO mz_partition_test.t VALUES ('c_1'), ('c%2'), ('c\\\\3')")
                .await?;
            // Refresh optimizer statistics so EXPLAIN estimates see the rows.
            conn.query_drop("ANALYZE TABLE mz_partition_test.t").await?;
        }

        let table = QualifiedTableRef {
            schema_name: "mz_partition_test",
            table_name: "t",
        };

        // Even with a minimum above the table size the root splits once, so
        // coarse boundaries may exist but stay within the worker count.
        let boundaries = partition_table(&mut conn, table.clone(), "id", 4, 1004, 50_000).await?;
        assert!(boundaries.len() <= 3, "{boundaries:?}");

        // A low minimum splits the table, and MySQL agrees the boundaries are
        // strictly increasing under the column collation.
        let boundaries = partition_table(&mut conn, table.clone(), "id", 4, 1004, 10).await?;
        assert!(
            !boundaries.is_empty() && boundaries.len() <= 3,
            "{boundaries:?}"
        );
        // Most of the rows extend the bare key 'a', so splitting must reach
        // inside those extensions rather than stopping at the exact key.
        assert!(
            boundaries.iter().any(|b| b.starts_with('a') && b.len() > 1),
            "{boundaries:?}"
        );
        for pair in boundaries.windows(2) {
            let increasing: Option<i64> = conn
                .exec_first("SELECT ? < ?", (&pair[0], &pair[1]))
                .await?;
            assert_eq!(increasing, Some(1), "{boundaries:?}");
        }
        // Ranges partition the table: per-range counts sum to the total.
        let mut total = 0u64;
        let mut lower: Option<String> = None;
        for upper in boundaries.iter().map(Some).chain([None]) {
            let (clause, params) = match (&lower, upper) {
                (None, Some(hi)) => ("id < ?".to_string(), vec![Value::from(hi)]),
                (Some(lo), Some(hi)) => (
                    "id >= ? AND id < ?".to_string(),
                    vec![Value::from(lo), Value::from(hi)],
                ),
                (Some(lo), None) => ("id >= ?".to_string(), vec![Value::from(lo)]),
                (None, None) => unreachable!("at least one boundary exists"),
            };
            let count: Option<u64> = conn
                .exec_first(
                    format!("SELECT COUNT(*) FROM mz_partition_test.t WHERE {clause}"),
                    Params::Positional(params),
                )
                .await?;
            total += count.expect("count returns a row");
            lower = upper.cloned();
        }
        assert_eq!(total, 1004);

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_partition_test").await?;
        conn.disconnect().await?;
        Ok(())
    }
}
