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
///
/// Nothing here validates the setup: the caller must abide by these
/// constraints or undefined/untested behavior could occur, e.g. boundaries
/// that fail to partition the key space or a walk that does not converge.
/// * `pk_col` is the table's single-column primary key.
/// * The column type is CHAR or VARCHAR with a declared length of at most
///   [`crate::probe::MAX_KEY_LENGTH`] characters.
/// * The column collation is `utf8mb4_bin`.
/// * The connection is inside a REPEATABLE READ transaction, so the probes
///   (several queries each) all see one snapshot of the table.
///
/// `min_split_threshold` is the smallest estimated row count granularity partitioning will
/// target, which means if the algorithm processes a prefix estimated to cover less
/// than min_split_threshold rows it won't bother splitting it up further. This is useful to
/// avoid unnecessary work for smaller tables limiting the overhead of partitioning.
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

async fn partition<D: PrimaryKeyProber>(
    db: &mut D,
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
    // before selecting partitions. 1/8th was selected by feel due to a couple of observed inaccuracies:
    // 1. Large estimates were observed as capped at 1/2 the estimated table size when the estimates were big.
    // 2. Medium or approaching 1/2 estimated table size estimates were observed as large overestimates (~2x)
    // So, that's a potential 4x swing and then a 2x safety factor to not push too close to the edge.
    //
    // Breaking down to smaller partitions results in more accurate splits, so we keep the
    // 4x multiple of the worker count for > 2 workers. Initial testing was with an 8x multiplier, selected
    // arbitrarily. From first principles, you can expect that if a prefix containing ~target_max_rows_per_prefix rows
    // lands right on a boundary (i.e. the worker was 99% full for its range) the worker will get a slot worth
    // 99% + 1/multiplier (in this case 25%) of the normal worker share resulting in skew with ~124% of the rows it
    // should own.
    let target_max_rows_per_prefix = (estimated_row_count / u64::cast_from(workers * 4))
        .max(min_split_threshold)
        .max(1);

    compute_boundaries(db, workers, estimated_row_count, target_max_rows_per_prefix).await
}

async fn compute_boundaries<D: PrimaryKeyProber>(
    db: &mut D,
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
async fn children_prefixes<D: PrimaryKeyProber>(
    db: &mut D,
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
            end,
            estimated_rows: estimated_rows.max(1),
            depth,
        });
        match next {
            Some(next) => cur = next,
            None => return Ok(children),
        }
    }
}

/// Probing operations of [`KeyProber`], as a trait so tests can substitute
/// an in-memory implementation. See [`KeyProber`]'s methods for each
/// operation's contract.
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
    /// "estimates". Byte order stands in for the collation, so the PAD SPACE
    /// below-space cases are deliberately out of scope here, the live tests
    /// cover them.
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
    async fn low_min_rows_per_worker_splits_small_tables() -> Result<(), MySqlError> {
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
    async fn fractional_target_still_terminates() -> Result<(), MySqlError> {
        // count / (workers * 4) is fractional and the minimum is zero, so
        // the target floors at one row instead of splitting forever.
        let mut db = MockDb { keys: keys(3) };
        let boundaries = partition(&mut db, 4, 3, 0).await?;
        assert_eq!(boundaries, vec!["000001", "000002"]);
        Ok(())
    }

    // Live tests against MySQL (when available) for more realistic results.

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn basic_partitioning() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };

        // 10k 4-digit incrementing integer numbers as strings 0000-9999
        let mut all_keys = vec![];
        all_keys.extend((0..10000).map(|i| format!("{i:04}")));

        const DB: &str = "mz_partition_basic_test";
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &all_keys).await?;
        let total = u64::cast_from(all_keys.len());

        let bounds = partition_table(&mut conn, table.clone(), "id", 4, total, 100).await?;
        assert_eq!(bounds.len(), 3, "{bounds:?}");
        assert_bounds_increasing(&mut conn, &bounds, "utf8mb4_bin").await?;
        let counts = partition_counts(&mut conn, DB, &bounds, total).await?;
        // ~1/4 of the table is 2500, so 2000 for some wiggle room
        assert!(counts.iter().all(|&c| c > 2000), "{counts:?}");

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

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
            "c|4".to_string(),
        ];
        all_keys.extend((0..900).map(|i| format!("a{i:05}")));
        all_keys.extend((0..100).map(|i| format!("b{i:05}")));

        const DB: &str = "mz_partition_test";
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &all_keys).await?;
        let total = u64::cast_from(all_keys.len());

        // A minimum above the table size yields no boundaries at all.
        let bounds = partition_table(&mut conn, table.clone(), "id", 4, total, 50_000).await?;
        assert!(bounds.is_empty(), "{bounds:?}");

        // A low minimum splits inside the 'a' extensions rather than stopping
        // at the exact key.
        let bounds = partition_table(&mut conn, table, "id", 4, total, 10).await?;
        assert_eq!(bounds.len(), 3, "{bounds:?}");

        assert_bounds_increasing(&mut conn, &bounds, "utf8mb4_bin").await?;
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
        let bounds = partition_table(&mut conn, table, "id", 4, total, 250).await?;
        assert_eq!(bounds.len(), 3);
        let counts = partition_counts(&mut conn, DB, &bounds, total).await?;
        // ~8k keys are visible, so each count gets at least 2k under perfect
        // partitioning, and the ranges partition cleanly except for the
        // hidden tab prefixes. Asserting each count above 1600 makes room
        // for single partitions being misallocated (~250) and some
        // inaccuracy on top of that (~150).
        assert!(counts.iter().all(|&c| c > 1600), "{counts:?}");

        // Each hidden group piles into the partition left of the next visible
        // boundary, here all of them ('', tabs, b-tabs) land in the first. Keep
        // the assertion low to ensure there's room for estimate variability.
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
    /// Asserts `bounds` are strictly increasing when MySQL compares them under
    /// `collation`, the same comparison the column's range predicates use. Bare
    /// `?` parameters would compare under the session collation instead.
    async fn assert_bounds_increasing(
        conn: &mut mysql_async::Conn,
        bounds: &[String],
        collation: &str,
    ) -> Result<(), anyhow::Error> {
        let charset = collation.split('_').next().expect("nonempty collation");
        let term = format!("CONVERT(? USING {charset}) COLLATE {collation}");
        for pair in bounds.windows(2) {
            let increasing: Option<i64> = conn
                .exec_first(format!("SELECT {term} < {term}"), (&pair[0], &pair[1]))
                .await?;
            assert_eq!(increasing, Some(1), "{bounds:?}");
        }
        Ok(())
    }

    /// Rows per snapshot partition of `bounds`, i.e. the half-open ranges
    /// `[..b0), [b0, b1), .., [bn, ..)`. The server counts, so the
    /// comparisons happen under the column's collation. Panics unless `bounds`
    /// is strictly increasing under it.
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
            // Checked because `ci` builds have overflow checks off, where a
            // wrapped count would clear every lower bound the tests assert.
            counts.push(cumulative.checked_sub(below).expect("increasing bounds"));
            below = cumulative;
        }
        counts.push(total.checked_sub(below).expect("increasing bounds"));
        Ok(counts)
    }
}
