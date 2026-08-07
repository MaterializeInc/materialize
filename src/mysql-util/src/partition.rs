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
/// with the `utf8mb4_bin` collation.
///
/// At most `max_requests` probes are issued against the server. When the
/// budget runs out, remaining prefixes stay unsplit, which skews partition
/// sizes but never correctness.
pub async fn partition_table(
    conn: &mut mysql_async::Conn,
    table: QualifiedTableRef<'_>,
    pk_col: &str,
    num_workers: usize,
    estimated_row_count: u64,
    min_rows_per_worker: u64,
    max_requests: u64,
) -> Result<Vec<String>, MySqlError> {
    let (schema_name, table_name) = (table.schema_name, table.table_name);
    let mut db = KeyProber::new(conn, table, pk_col);
    let boundaries = partition(
        &mut db,
        num_workers,
        estimated_row_count,
        min_rows_per_worker,
        max_requests,
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

async fn partition<D: PrimaryKeyProber>(
    db: &mut D,
    workers: usize,
    estimated_row_count: u64,
    min_rows_per_worker: u64,
    max_requests: u64,
) -> Result<Vec<String>, MySqlError> {
    if workers <= 1 {
        return Ok(Vec::new());
    }
    let estimated_row_count = estimated_row_count.max(1);

    // Estimates vary wildly especially near the full table size (see `KeyProber::estimate_range_rows` for more details).
    // Estimates tend to get more useful as smaller chunks, so break up the table into at least 1/8ths (2 workers * 4)
    // before selecting partitions. Breaking down to smaller partitions results in more accurate splits, so we keep the
    // 4x multiple of the worker count for > 2 workers.
    let target_max_rows_per_prefix = (f64::cast_lossy(estimated_row_count)
        / f64::cast_lossy(workers * 4))
    .max(f64::cast_lossy(min_rows_per_worker))
    .max(1.0);

    compute_boundaries(
        db,
        workers,
        estimated_row_count,
        target_max_rows_per_prefix,
        max_requests,
    )
    .await
}

async fn compute_boundaries<D: PrimaryKeyProber>(
    db: &mut D,
    workers: usize,
    estimated_row_count: u64,
    target_rows_per_prefix: f64,
    max_requests: u64,
) -> Result<Vec<String>, MySqlError> {
    let mut budget = max_requests;
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
            // Entering a split costs one probe for the first prefix plus two
            // for its first walk step, a smaller budget keeps the prefix as a
            // leaf.
            if budget >= 3 && f64::cast_lossy(prefix.estimated_rows) > target_rows_per_prefix {
                split_any = true;
                // Partitioning children can drop some rows from the parent prefix range. This
                // is acceptable given the approximate nature of the algorithm.
                let children = children_prefixes(db, &prefix, &mut budget).await?;
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
    tracing::debug!(
        prefixes = ordered_prefixes.len(),
        requests_spent = max_requests - budget,
        "split key space into prefixes"
    );

    // Recompute the total after partitioning the table to get more even splits because the actual row count and the
    // granularly estimated row count can diverge from the original top level estimate.
    let total: f64 = ordered_prefixes
        .iter()
        .map(|r| f64::cast_lossy(r.estimated_rows))
        .sum();
    let per_worker = total / f64::cast_lossy(workers);
    tracing::debug!(
        prefixes = ordered_prefixes.len(),
        total_estimated_rows = total,
        per_worker,
        "assigning prefixes to workers"
    );
    let mut boundaries: Vec<String> = Vec::with_capacity(workers - 1);
    let mut rows_seen = 0.0;
    for prefix in &ordered_prefixes {
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
/// Note: This will drop the key "a" on the floor, along with any keys
/// sorting below their own prefix (below-space characters at this depth).
///
/// `budget` is decremented once per probe. The caller must provide at least
/// 3, one for the first prefix and two for a walk step. The walk closes out
/// with a tail child once it cannot afford another step.
async fn children_prefixes<D: PrimaryKeyProber>(
    db: &mut D,
    parent: &Prefix,
    budget: &mut u64,
) -> Result<Vec<Prefix>, MySqlError> {
    let depth = parent.depth + 1;
    let mut children = Vec::new();

    *budget -= 1;
    let Some(mut cur) = db
        .prefix_of_first_key_in_range(&parent.prefix, parent.end.as_deref(), depth)
        .await?
    else {
        return Ok(children);
    };

    loop {
        // A walk step costs two probes. When they are unaffordable, close out
        // with a tail child so the parent's key space stays covered,
        // estimated as the parent's unconsumed mass.
        if *budget < 2 {
            let consumed: u64 = children.iter().map(|c| c.estimated_rows).sum();
            children.push(Prefix {
                prefix: cur,
                end: parent.end.clone(),
                estimated_rows: parent.estimated_rows.saturating_sub(consumed).max(1),
                depth,
            });
            return Ok(children);
        }
        *budget -= 1;
        let next = db
            .prefix_of_first_row_not_matching_prefix(&cur, parent.end.as_deref(), depth)
            .await?;
        let end = next.clone().or_else(|| parent.end.clone());
        *budget -= 1;
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

/// Wrapper around [KeyProber] for testing purposes. See [KeyProber] for more details.
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
    use mz_ore::cast::CastFrom;

    use super::*;
    use crate::probe::tests::{connect, drop_db, setup_table};

    /// In-memory [`PrimaryKeyProber`] over a sorted key list with exact
    /// "estimates". Byte order stands in for the collation.
    struct MockDb {
        keys: Vec<String>,
        /// Probes served, for asserting on the request budget.
        requests: u64,
    }

    impl MockDb {
        fn new(keys: Vec<String>) -> Self {
            MockDb { keys, requests: 0 }
        }

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
            self.requests += 1;
            let (lo, hi) = self.bounds(start, end);
            Ok(u64::cast_from(hi - lo))
        }

        async fn prefix_of_first_key_in_range(
            &mut self,
            start: &str,
            end: Option<&str>,
            len: usize,
        ) -> Result<Option<String>, MySqlError> {
            self.requests += 1;
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
            self.requests += 1;
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
        let mut db = MockDb::new(keys(1000));
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 1, count, MIN_ROWS_PER_WORKER, u64::MAX).await?;
        assert!(boundaries.is_empty());
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn small_table_gets_no_boundaries() -> Result<(), MySqlError> {
        // All keys share one depth-1 prefix and fit under `min_rows_per_worker`,
        // so the single open-ended range yields no boundary.
        let mut db = MockDb::new(keys(10_000));
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 4, count, MIN_ROWS_PER_WORKER, u64::MAX).await?;
        assert!(boundaries.is_empty());
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn empty_table_gets_no_boundaries() -> Result<(), MySqlError> {
        let mut db = MockDb::new(vec![]);
        let boundaries = partition(&mut db, 4, 0, MIN_ROWS_PER_WORKER, u64::MAX).await?;
        assert!(boundaries.is_empty());
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn splits_evenly_across_workers() -> Result<(), MySqlError> {
        let mut db = MockDb::new(keys(200_000));
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 4, count, MIN_ROWS_PER_WORKER, u64::MAX).await?;
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
        let mut db = MockDb::new(keys(1000));
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 4, count, 10, u64::MAX).await?;
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
        let mut db = MockDb::new(all_keys);
        let count = u64::cast_from(db.keys.len());
        let boundaries = partition(&mut db, 4, count, 10, u64::MAX).await?;
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
    async fn fractional_target_still_terminates() -> Result<(), MySqlError> {
        // count / (workers * 4) is fractional and the minimum is zero, so
        // the target floors at one row instead of splitting forever.
        let mut db = MockDb::new(keys(3));
        let boundaries = partition(&mut db, 4, 3, 0, u64::MAX).await?;
        assert_eq!(boundaries, vec!["000001", "000002"]);
        Ok(())
    }

    // Live tests against MySQL (when available) for more realistic results.

    /// Splitting must reach inside the extensions of the bare key 'a' and
    /// yield boundaries MySQL agrees are strictly increasing.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn skewed_partitions_with_wildcards_and_short_keys() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };

        // A bare key 'a' that 900 keys extend, 100 keys under 'b', and LIKE
        // metacharacters.
        let mut all_keys = vec![
            "a".to_string(),
            "c_1".to_string(),
            "c%2".to_string(),
            "c\\3".to_string(),
        ];
        all_keys.extend((0..900).map(|i| format!("a{i:05}")));
        all_keys.extend((0..100).map(|i| format!("b{i:05}")));

        const DB: &str = "mz_partition_test";
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &all_keys).await?;
        let total = u64::cast_from(all_keys.len());

        // A minimum above the table size yields no boundaries at all.
        let bounds =
            partition_table(&mut conn, table.clone(), "id", 4, total, 50_000, u64::MAX).await?;
        assert!(bounds.is_empty(), "{bounds:?}");

        // A low minimum splits inside the 'a' extensions rather than stopping
        // at the exact key.
        let bounds = partition_table(&mut conn, table, "id", 4, total, 10, u64::MAX).await?;
        assert!(bounds.len() == 3, "{bounds:?}");

        // MySQL agrees the boundaries are strictly increasing.
        for pair in bounds.windows(2) {
            let increasing: Option<i64> = conn
                .exec_first("SELECT ? < ?", (&pair[0], &pair[1]))
                .await?;
            assert_eq!(increasing, Some(1), "{bounds:?}");
        }
        let counts = partition_counts(&mut conn, DB, &bounds, total).await?;
        // ~1/4 of the table is 250 so 100 leaves lots of room for error.
        assert!(counts.iter().all(|&c| c > 100), "{counts:?}");

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn skew_empty_string_and_below_space_characters_inaccuracy() -> Result<(), anyhow::Error>
    {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };

        // ~10k keys in total, about as many under "c" as the rest combined.
        // Tabs and empty strings will be dropped, which will show up in the resulting skew.
        let mut all_keys = vec![String::new()];
        add_1k_keys(&mut all_keys, "\t");
        add_1k_keys(&mut all_keys, "a");
        add_1k_keys(&mut all_keys, "b");
        add_1k_keys(&mut all_keys, "b\t");
        add_1k_keys(&mut all_keys, "c");
        add_1k_keys(&mut all_keys, "ca");
        add_1k_keys(&mut all_keys, "cb");
        add_1k_keys(&mut all_keys, "cc");
        add_1k_keys(&mut all_keys, "cd");
        add_1k_keys(&mut all_keys, "d");

        const DB: &str = "mz_partition_live_mixed_test";
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &all_keys).await?;
        let total = u64::cast_from(all_keys.len());

        // Partition for 4 workers with a minimum split size around 250.
        let bounds = partition_table(&mut conn, table, "id", 4, total, 250, u64::MAX).await?;
        assert_eq!(bounds.len(), 3);
        let counts = partition_counts(&mut conn, DB, &bounds, total).await?;
        // ~8k are visible, so each count should have at least 2k for perfect partitioning and the ranges are
        // cleanly partitionable except for the hidden tab prefixes, so we should be reliably able to assert
        // that each count is greater than 1600 -- this makes room for single partitions being misallocated (~250)
        // and some inaccuracy on top of that (~150).
        assert!(counts.iter().all(|&c| c > 1600), "{counts:?}");

        // Each hidden group piles into the partition left of the next visible
        // boundary, here all of them ('', tabs, b-tabs) land in the first.
        // This is a performance degradation edge case, not a correctness
        // issue.
        assert!(counts[0] > 2600, "{counts:?}");

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    fn add_1k_keys(all_keys: &mut Vec<String>, prefix: &str) {
        all_keys.extend((0..1000).map(|i| format!("{prefix}{i:03}")));
    }

    /// Rows per snapshot partition of `bounds`, i.e. the half-open ranges
    /// `[..b0), [b0, b1), .., [bn, ..)`. The server counts, so the
    /// comparisons happen under the column's collation.
    async fn partition_counts(
        conn: &mut mysql_async::Conn,
        db: &str,
        bounds: &[String],
        total: u64,
    ) -> Result<Vec<u64>, anyhow::Error> {
        let mut counts = Vec::with_capacity(bounds.len() + 1);
        let mut below = 0;
        for bound in bounds {
            let cumulative: Option<u64> = conn
                .exec_first(
                    format!("SELECT COUNT(*) FROM {db}.t WHERE id < ?"),
                    (bound.as_str(),),
                )
                .await?;
            let cumulative = cumulative.expect("COUNT returns a row");
            counts.push(cumulative - below);
            below = cumulative;
        }
        counts.push(total - below);
        Ok(counts)
    }

    async fn live_conn() -> Result<Option<mysql_async::Conn>, anyhow::Error> {
        let Ok(url) = std::env::var("MZ_TEST_MYSQL_URL") else {
            if mz_ore::env::is_var_truthy("CI") {
                panic!("CI is supposed to run this test but something has gone wrong!");
            }
            return Ok(None);
        };
        Ok(Some(
            mysql_async::Conn::new(mysql_async::Opts::from_url(&url)?).await?,
        ))
    }

    /// Creates `{db}.t` with an `id` key column and one block of keys per
    /// `(sql_prefix_expr, count)`.
    #[allow(clippy::disallowed_methods)]
    async fn setup_blocks(
        conn: &mut mysql_async::Conn,
        db: &str,
        collate: &str,
        blocks: &[(&str, usize)],
    ) -> Result<(), anyhow::Error> {
        conn.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
            .await?;
        conn.query_drop(format!("CREATE DATABASE {db}")).await?;
        conn.query_drop(format!(
            "CREATE TABLE {db}.t (id VARCHAR(32) {collate} PRIMARY KEY NOT NULL)"
        ))
        .await?;
        for (prefix, n) in blocks {
            conn.query_drop(format!(
                "INSERT INTO {db}.t \
                 WITH RECURSIVE n AS (SELECT 1 x UNION ALL SELECT x+1 FROM n WHERE x < {n}) \
                 SELECT CONCAT({prefix}, LPAD(x, 5, '0')) FROM n",
            ))
            .await?;
        }
        conn.query_drop(format!("ANALYZE TABLE {db}.t")).await?;
        Ok(())
    }

    /// The walk must build an inverted range and fail with
    /// [`MySqlError::MissingRowEstimate`].
    async fn expect_missing_estimate(
        conn: &mut mysql_async::Conn,
        db: &'static str,
        rows: u64,
    ) -> Result<(), anyhow::Error> {
        let table = QualifiedTableRef {
            schema_name: db,
            table_name: "t",
        };
        match partition_table(conn, table, "id", 4, rows, 10, 10_000).await {
            Err(MySqlError::MissingRowEstimate {
                qualified_table_name,
                ..
            }) => assert_eq!(qualified_table_name, format!("{db}.t")),
            other => panic!("expected MissingRowEstimate, got {other:?}"),
        }
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!("DROP DATABASE {db}")).await?;
        Ok(())
    }

    /// Czech `ch` collates as one letter between `h` and `i`, so a key
    /// starting with `ch` truncates to a `c` prefix that sorts below the
    /// range it was found in.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a network connection
    async fn test_live_contraction_breaks_walk() -> Result<(), anyhow::Error> {
        let Some(mut conn) = live_conn().await? else {
            return Ok(());
        };
        let facts: Option<(i64, i64, i64)> = conn
            .exec_first(
                "SELECT _utf8mb4'ch' COLLATE utf8mb4_cs_0900_ai_ci > 'h',
                        _utf8mb4'c'  COLLATE utf8mb4_cs_0900_ai_ci < 'h',
                        _utf8mb4'chleba' COLLATE utf8mb4_cs_0900_ai_ci LIKE 'c%'",
                (),
            )
            .await?;
        assert_eq!(facts, Some((1, 1, 1)));

        setup_blocks(
            &mut conn,
            "mz_contraction_test",
            "COLLATE utf8mb4_cs_0900_ai_ci",
            &[
                ("'duha'", 300),
                ("'hora'", 300),
                ("'chleba'", 600),
                ("'ibis'", 300),
            ],
        )
        .await?;
        expect_missing_estimate(&mut conn, "mz_contraction_test", 1500).await?;
        conn.disconnect().await?;
        Ok(())
    }

    /// `ß` expands to two collation elements (`ß` = `ss`) under the stock
    /// default collation, so truncations of `aß...` and `asz...` keys sort
    /// opposite to the keys themselves.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a network connection
    async fn test_live_expansion_breaks_walk() -> Result<(), anyhow::Error> {
        let Some(mut conn) = live_conn().await? else {
            return Ok(());
        };
        // Literal comparisons use the connection collation, pin the column's.
        let facts: Option<(i64, i64, i64)> = conn
            .exec_first(
                "SELECT 'ß'  COLLATE utf8mb4_0900_ai_ci = 'ss',
                        'as' COLLATE utf8mb4_0900_ai_ci < 'aß',
                        'assx' COLLATE utf8mb4_0900_ai_ci LIKE 'aß%'",
                (),
            )
            .await?;
        assert_eq!(facts, Some((1, 1, 0)));

        setup_blocks(
            &mut conn,
            "mz_expansion_test",
            "",
            &[("'aaa'", 300), ("'aßx'", 600), ("'asz'", 300)],
        )
        .await?;
        expect_missing_estimate(&mut conn, "mz_expansion_test", 1200).await?;
        conn.disconnect().await?;
        Ok(())
    }

    /// NUL carries zero collation elements in any position under the default
    /// collation: a NUL key collates equal to its NUL-free twin, and a
    /// leading-NUL key truncates to a prefix that sorts below everything.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a network connection
    async fn test_live_nul_breaks_walk() -> Result<(), anyhow::Error> {
        let Some(mut conn) = live_conn().await? else {
            return Ok(());
        };
        let facts: Option<(i64, i64, i64, i64)> = conn
            .exec_first(
                "SELECT CONCAT('a', CHAR(0 USING utf8mb4)) COLLATE utf8mb4_0900_ai_ci = 'a',
                        CONCAT('a', CHAR(0 USING utf8mb4), 'b') COLLATE utf8mb4_0900_ai_ci = 'ab',
                        CONCAT(CHAR(0 USING utf8mb4), 'ab') COLLATE utf8mb4_0900_ai_ci = 'ab',
                        LENGTH(WEIGHT_STRING(CHAR(0 USING utf8mb4) COLLATE utf8mb4_0900_ai_ci))",
                (),
            )
            .await?;
        assert_eq!(facts, Some((1, 1, 1, 0)));

        setup_blocks(
            &mut conn,
            "mz_nul_test",
            "",
            &[
                ("'aa'", 300),
                ("'mm'", 300),
                ("CONCAT(CHAR(0 USING utf8mb4), 'zz')", 600),
            ],
        )
        .await?;

        // NUL twins collate equal, the unique PK rejects the second.
        conn.exec_drop("INSERT INTO mz_nul_test.t VALUES (?)", ("ab",))
            .await?;
        match conn
            .exec_drop("INSERT INTO mz_nul_test.t VALUES (?)", ("a\0b",))
            .await
        {
            Err(mysql_async::Error::Server(err)) => assert_eq!(err.code, 1062, "{err}"),
            other => panic!("expected duplicate-key error, got {other:?}"),
        }
        conn.exec_drop("DELETE FROM mz_nul_test.t WHERE id = ?", ("ab",))
            .await?;

        expect_missing_estimate(&mut conn, "mz_nul_test", 1200).await?;
        conn.disconnect().await?;
        Ok(())
    }

    /// Under the PAD SPACE collations that snapshot integration splits,
    /// a truncation cut after an intermediate space (`'a '`) compares equal
    /// to its space-free prefix. The walk must still produce ordered
    /// boundaries.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a network connection
    async fn test_live_pad_space_cuts_split_cleanly() -> Result<(), anyhow::Error> {
        let Some(mut conn) = live_conn().await? else {
            return Ok(());
        };
        let facts: Option<(i64, i64, i64)> = conn
            .exec_first(
                "SELECT 'ab ' COLLATE utf8mb4_general_ci = 'ab',
                        'ab ' COLLATE utf8mb4_bin = 'ab',
                        'a b' COLLATE utf8mb4_general_ci < 'aa'",
                (),
            )
            .await?;
        assert_eq!(facts, Some((1, 1, 1)));

        for collation in ["utf8mb4_general_ci", "utf8mb4_bin"] {
            setup_blocks(
                &mut conn,
                "mz_pad_test",
                &format!("COLLATE {collation}"),
                &[("'a b'", 600), ("'aa'", 300), ("'ax'", 300)],
            )
            .await?;

            // Space twins collate equal, the unique PK rejects the second.
            conn.exec_drop("INSERT INTO mz_pad_test.t VALUES (?)", ("zz",))
                .await?;
            match conn
                .exec_drop("INSERT INTO mz_pad_test.t VALUES (?)", ("zz ",))
                .await
            {
                Err(mysql_async::Error::Server(err)) => assert_eq!(err.code, 1062, "{err}"),
                other => panic!("expected duplicate-key error, got {other:?}"),
            }
            conn.exec_drop("DELETE FROM mz_pad_test.t WHERE id = ?", ("zz",))
                .await?;

            let table = QualifiedTableRef {
                schema_name: "mz_pad_test",
                table_name: "t",
            };
            let boundaries = partition_table(&mut conn, table, "id", 4, 1200, 10, 10_000).await?;
            assert!(
                !boundaries.is_empty() && boundaries.len() <= 3,
                "{collation}: {boundaries:?}"
            );
            for pair in boundaries.windows(2) {
                let increasing: Option<i64> = conn
                    .exec_first(
                        format!("SELECT ? COLLATE {collation} < ?"),
                        (&pair[0], &pair[1]),
                    )
                    .await?;
                assert_eq!(increasing, Some(1), "{collation}: {boundaries:?}");
            }
        }

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_pad_test").await?;
        conn.disconnect().await?;
        Ok(())
    }
}
