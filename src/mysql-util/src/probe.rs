// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mysql_async::prelude::Queryable;
use mysql_async::{Params, Value};
use mz_ore::cast::CastFrom;

use crate::{MySqlError, QualifiedTableRef, quote_identifier};

pub struct KeyProber<'a, Q> {
    conn: &'a mut Q,
    table: String,
    col: String,
}

impl<'a, Q: Queryable> KeyProber<'a, Q> {
    pub fn new(conn: &'a mut Q, table: QualifiedTableRef<'_>, key_col: &str) -> Self {
        Self {
            conn,
            table: format!(
                "{}.{}",
                quote_identifier(table.schema_name),
                quote_identifier(table.table_name)
            ),
            col: quote_identifier(key_col),
        }
    }

    /// Estimates the row count for the given range. These estimates can vary pretty widely. They
    /// will generally never be more than half the size of the full row count reported by
    /// `information_schema.tables`. In some tests these have been over-estimates in practice,
    /// where the sum of all table ranges has been ~2x as large as the estimate or table size.
    pub async fn estimate_range_rows(
        &mut self,
        start: &str,
        end: Option<&str>,
    ) -> Result<u64, MySqlError> {
        let (clause, params) = self.range_filter(start, end);
        let select = format!(
            "SELECT {col} FROM {table} WHERE {clause}",
            col = self.col,
            table = self.table,
        );
        let estimate =
            explain_row_estimate(&mut *self.conn, &select, Params::Positional(params)).await?;
        Ok(estimate.unwrap_or(0))
    }

    /// Grabs a prefix of length `len` for the first row in the given range. If the string is
    /// shorter than `len`, it will return that shorter value.
    ///
    /// The query will generally look something like:
    ///
    /// ```sql
    /// SELECT LEFT(pk_col, 3) FROM table
    /// WHERE pk_col >= 'ab' AND pk_col < 'ac'
    /// ORDER BY pk_col
    /// LIMIT 1
    /// ```
    pub async fn first_prefix(
        &mut self,
        start: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        let (clause, mut params) = self.range_filter(start, end);
        let sql = format!(
            "SELECT LEFT({col}, ?) FROM {table} WHERE {clause} ORDER BY {col} LIMIT 1",
            col = self.col,
            table = self.table,
        );
        params.insert(0, u64::cast_from(len).into());
        self.query_string(sql, params).await
    }

    /// Grabs the next prefix of length `len` after the prefix `cur`, from keys below `end`.
    ///
    /// The query will generally look something like:
    ///
    /// ```sql
    /// SELECT LEFT(pk_col, 3) FROM table
    /// WHERE pk_col > (
    ///     SELECT pk_col FROM table
    ///     WHERE pk_col LIKE 'abc%' AND pk_col < 'ac'
    ///     ORDER BY pk_col DESC
    ///     LIMIT 1
    /// ) AND pk_col < 'ac'
    /// ORDER BY pk_col
    /// LIMIT 1
    /// ```
    ///
    /// In this case it would likely return something like `abd` or `abe`, but not `aca`. It's
    /// useful for finding the next prefix of a given primary key at the provided depth. If the
    /// next key is shorter than the given prefix, it will return a shorter key.
    pub async fn next_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        let col = &self.col;
        let table = &self.table;
        let mut sql = format!(
            "SELECT LEFT({col}, ?) FROM {table} \
             WHERE {col} > (SELECT {col} FROM {table} WHERE {col} LIKE ?"
        );
        let mut params: Vec<Value> =
            vec![u64::cast_from(len).into(), like_prefix_pattern(cur).into()];
        if let Some(end) = end {
            sql.push_str(&format!(" AND {col} < ?"));
            params.push(end.into());
        }
        sql.push_str(&format!(" ORDER BY {col} DESC LIMIT 1)"));
        if let Some(end) = end {
            sql.push_str(&format!(" AND {col} < ?"));
            params.push(end.into());
        }
        sql.push_str(&format!(" ORDER BY {col} LIMIT 1"));
        self.query_string(sql, params).await
    }

    fn range_filter(&self, start: &str, end: Option<&str>) -> (String, Vec<Value>) {
        let col = &self.col;
        let mut clause = format!("{col} >= ?");
        let mut params: Vec<Value> = vec![start.into()];
        if let Some(end) = end {
            clause.push_str(&format!(" AND {col} < ?"));
            params.push(end.into());
        }
        (clause, params)
    }

    async fn query_string(
        &mut self,
        sql: String,
        params: Vec<Value>,
    ) -> Result<Option<String>, MySqlError> {
        let row: Option<mysql_async::Row> = self
            .conn
            .exec_first(sql, Params::Positional(params))
            .await?;
        Ok(row.and_then(|mut row| row.take_opt::<String, _>(0).and_then(Result::ok)))
    }
}

pub fn like_prefix_pattern(prefix: &str) -> String {
    let mut pattern = String::with_capacity(prefix.len() + 1);
    for c in prefix.chars() {
        if matches!(c, '\\' | '%' | '_') {
            pattern.push('\\');
        }
        pattern.push(c);
    }
    pattern.push('%');
    pattern
}

pub async fn explain_row_estimate<Q, P>(
    conn: &mut Q,
    select: &str,
    params: P,
) -> Result<Option<u64>, MySqlError>
where
    Q: Queryable,
    P: Into<Params> + Send,
{
    let plan: Option<mysql_async::Row> = conn
        .exec_first(format!("EXPLAIN FORMAT=TRADITIONAL {select}"), params)
        .await?;
    let estimate = plan.and_then(|row| {
        row.get_opt::<Option<u64>, _>("rows")
            .and_then(Result::ok)
            .flatten()
    });
    Ok(estimate)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[mz_ore::test]
    fn test_like_prefix_pattern() {
        assert_eq!(like_prefix_pattern("abc"), "abc%");
        assert_eq!(like_prefix_pattern(""), "%");
        assert_eq!(like_prefix_pattern("a_b"), "a\\_b%");
        assert_eq!(like_prefix_pattern("50%"), "50\\%%");
        assert_eq!(like_prefix_pattern("a\\b"), "a\\\\b%");
        assert_eq!(like_prefix_pattern("héllo"), "héllo%");
    }

    /// Connects to the server named by `MZ_TEST_MYSQL_URL`, or `None` to skip
    /// the test when it is unset. Skipping is a local-only convenience, CI
    /// must always provide the URL.
    async fn connect() -> Result<Option<mysql_async::Conn>, anyhow::Error> {
        let Ok(url) = std::env::var("MZ_TEST_MYSQL_URL") else {
            if mz_ore::env::is_var_truthy("CI") {
                panic!("CI is supposed to run this test but something has gone wrong!");
            }
            tracing::info!("MZ_TEST_MYSQL_URL not set: skipping live MySQL test");
            return Ok(None);
        };
        Ok(Some(
            mysql_async::Conn::new(mysql_async::Opts::from_url(&url)?).await?,
        ))
    }

    /// Drops and recreates the scratch database `db`. Each test must use its
    /// own database name, tests on the shared server run concurrently.
    async fn recreate_db(conn: &mut mysql_async::Conn, db: &str) -> Result<(), anyhow::Error> {
        #[allow(clippy::disallowed_methods)]
        {
            conn.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
                .await?;
            conn.query_drop(format!("CREATE DATABASE {db}")).await?;
        }
        Ok(())
    }

    /// Walks the whole key space at prefix length `len`, panicking if the
    /// walk revisits a prefix.
    async fn walk_prefixes(
        prober: &mut KeyProber<'_, mysql_async::Conn>,
        len: usize,
    ) -> Result<Vec<String>, anyhow::Error> {
        let mut walked = Vec::new();
        let Some(mut cur) = prober.first_prefix("", None, len).await? else {
            return Ok(walked);
        };
        loop {
            assert!(
                !walked.contains(&cur),
                "prefix repeated: {cur:?} (walked: {walked:?})"
            );
            walked.push(cur.clone());
            match prober.next_prefix(&cur, None, len).await? {
                Some(next) => cur = next,
                None => break,
            }
        }
        Ok(walked)
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };

        #[allow(clippy::disallowed_methods)]
        {
            conn.query_drop("DROP DATABASE IF EXISTS mz_probe_test")
                .await?;
            conn.query_drop("CREATE DATABASE mz_probe_test").await?;
            conn.query_drop("CREATE TABLE mz_probe_test.t (id VARCHAR(32) PRIMARY KEY NOT NULL)")
                .await?;
            conn.query_drop(
                "INSERT INTO mz_probe_test.t \
                 WITH RECURSIVE n AS (SELECT 1 x UNION ALL SELECT x+1 FROM n WHERE x < 900) \
                 SELECT CONCAT('a', LPAD(x, 5, '0')) FROM n",
            )
            .await?;
            conn.query_drop(
                "INSERT INTO mz_probe_test.t \
                 WITH RECURSIVE n AS (SELECT 1 x UNION ALL SELECT x+1 FROM n WHERE x < 100) \
                 SELECT CONCAT('b', LPAD(x, 5, '0')) FROM n",
            )
            .await?;
            conn.query_drop("INSERT INTO mz_probe_test.t VALUES ('c_1'), ('c%2'), ('c\\\\3')")
                .await?;
            conn.query_drop("ANALYZE TABLE mz_probe_test.t").await?;
        }

        let table = QualifiedTableRef {
            schema_name: "mz_probe_test",
            table_name: "t",
        };
        let mut prober = KeyProber::new(&mut conn, table, "id");

        assert_eq!(
            prober.first_prefix("", None, 1).await?.as_deref(),
            Some("a")
        );
        assert_eq!(
            prober.next_prefix("a", None, 1).await?.as_deref(),
            Some("b")
        );
        assert_eq!(
            prober.next_prefix("b", None, 1).await?.as_deref(),
            Some("c")
        );
        assert_eq!(prober.next_prefix("c", None, 1).await?, None);

        assert_eq!(
            prober
                .first_prefix("b", Some("b00002"), 6)
                .await?
                .as_deref(),
            Some("b00001")
        );
        assert_eq!(prober.next_prefix("a", Some("b"), 1).await?, None);
        assert_eq!(prober.first_prefix("zzz", None, 1).await?, None);

        let mut walked = BTreeSet::new();
        let mut cur = prober
            .first_prefix("c", None, 3)
            .await?
            .expect("c keys exist");
        loop {
            assert!(walked.insert(cur.clone()), "prefix repeated: {cur:?}");
            match prober.next_prefix(&cur, None, 3).await? {
                Some(next) => cur = next,
                None => break,
            }
        }
        let expected: BTreeSet<String> = ["c_1", "c%2", "c\\3"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(walked, expected);

        let all = prober.estimate_range_rows("", None).await?;
        let b_range = prober.estimate_range_rows("b", Some("c")).await?;
        assert!(all > b_range, "all={all} b_range={b_range}");
        assert!(b_range > 0);

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_probe_test").await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_ulid_pk() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        recreate_db(&mut conn, "mz_probe_ulid_test").await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop("CREATE TABLE mz_probe_ulid_test.t (id VARCHAR(32) PRIMARY KEY NOT NULL)")
            .await?;

        // ULIDs minted around the same time share a long timestamp prefix,
        // here chars 11 and 12 are the first that differ.
        const CROCKFORD: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        let ids: Vec<String> = (0..1000)
            .map(|i| {
                format!(
                    "01J8ZXABCD{}{}00000000000000",
                    char::from(CROCKFORD[i / 32]),
                    char::from(CROCKFORD[i % 32]),
                )
            })
            .collect();
        conn.exec_batch(
            "INSERT INTO mz_probe_ulid_test.t VALUES (?)",
            ids.iter().map(|id| (id.as_str(),)),
        )
        .await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop("ANALYZE TABLE mz_probe_ulid_test.t")
            .await?;

        let table = QualifiedTableRef {
            schema_name: "mz_probe_ulid_test",
            table_name: "t",
        };
        let mut prober = KeyProber::new(&mut conn, table, "id");

        // Every key shares the timestamp prefix, so short prefixes cannot
        // split the key space at all.
        assert_eq!(
            prober.first_prefix("", None, 1).await?.as_deref(),
            Some("0")
        );
        assert_eq!(prober.next_prefix("0", None, 1).await?, None);
        assert_eq!(prober.next_prefix("01J8ZXABCD", None, 10).await?, None);

        // One character past the shared prefix distinguishes the keys.
        let walked = walk_prefixes(&mut prober, 11).await?;
        let expected: Vec<String> = (0..32)
            .map(|i| format!("01J8ZXABCD{}", char::from(CROCKFORD[i])))
            .collect();
        assert_eq!(walked, expected);

        let all = prober.estimate_range_rows("", None).await?;
        assert!(all > 0, "all={all}");

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_probe_ulid_test").await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_uuid_pk() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        recreate_db(&mut conn, "mz_probe_uuid_test").await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop("CREATE TABLE mz_probe_uuid_test.t (id VARCHAR(36) PRIMARY KEY NOT NULL)")
            .await?;

        // Hyphenated lowercase v4-shaped UUIDs, unique via the last group,
        // with the leading group scattered like random UUIDs.
        let ids: Vec<String> = (0..1000u64)
            .map(|i| {
                let h = i.wrapping_mul(2654435761) % 0x1_0000_0000;
                format!("{h:08x}-0000-4000-8000-{i:012x}")
            })
            .collect();
        conn.exec_batch(
            "INSERT INTO mz_probe_uuid_test.t VALUES (?)",
            ids.iter().map(|id| (id.as_str(),)),
        )
        .await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop("ANALYZE TABLE mz_probe_uuid_test.t")
            .await?;

        let table = QualifiedTableRef {
            schema_name: "mz_probe_uuid_test",
            table_name: "t",
        };
        let mut prober = KeyProber::new(&mut conn, table, "id");

        // Lowercase hex order matches byte order under the default collation.
        assert_eq!(
            prober.first_prefix("", None, 36).await?.as_deref(),
            ids.iter().min().map(String::as_str)
        );

        let walked = walk_prefixes(&mut prober, 1).await?;
        let expected: BTreeSet<String> = ids.iter().map(|id| id[..1].to_string()).collect();
        assert_eq!(walked.iter().cloned().collect::<BTreeSet<_>>(), expected);

        let all = prober.estimate_range_rows("", None).await?;
        assert!(all > 0, "all={all}");

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_probe_uuid_test").await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_like_metacharacters() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        recreate_db(&mut conn, "mz_probe_like_test").await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop("CREATE TABLE mz_probe_like_test.t (id VARCHAR(32) PRIMARY KEY NOT NULL)")
            .await?;

        // Every next_prefix step anchors on `LIKE '<prefix>%'`, so keys whose
        // prefixes are LIKE metacharacters exercise the escaping.
        let ids = [
            "%",
            "%%",
            "_",
            "__",
            "\\",
            "\\\\",
            "a%",
            "a%b",
            "a_",
            "a_b",
            "a\\",
            "a\\b",
            "ab",
            "a b",
            "100%",
            "50%off",
            "under_score",
            "back\\slash",
        ];
        conn.exec_batch(
            "INSERT INTO mz_probe_like_test.t VALUES (?)",
            ids.iter().map(|id| (*id,)),
        )
        .await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop("ANALYZE TABLE mz_probe_like_test.t")
            .await?;

        let table = QualifiedTableRef {
            schema_name: "mz_probe_like_test",
            table_name: "t",
        };
        let mut prober = KeyProber::new(&mut conn, table, "id");

        // A key shorter than `len` yields a short prefix that subsumes longer
        // keys sharing it (the walk skips past everything `LIKE '<prefix>%'`),
        // so instead of comparing exact prefixes, assert the walk partitions
        // the keys: every key starts with exactly one walked prefix.
        for len in [1, 2] {
            let walked = walk_prefixes(&mut prober, len).await?;
            for id in ids {
                let covering: Vec<_> = walked
                    .iter()
                    .filter(|p| id.starts_with(p.as_str()))
                    .collect();
                assert_eq!(
                    covering.len(),
                    1,
                    "id={id:?} len={len} covering={covering:?} walked={walked:?}"
                );
            }
        }

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_probe_like_test").await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_collations() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        recreate_db(&mut conn, "mz_probe_collation_test").await?;
        #[allow(clippy::disallowed_methods)]
        {
            conn.query_drop(
                "CREATE TABLE mz_probe_collation_test.t_ci \
                 (id VARCHAR(32) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY NOT NULL)",
            )
            .await?;
            conn.query_drop(
                "CREATE TABLE mz_probe_collation_test.t_bin \
                 (id VARCHAR(32) COLLATE utf8mb4_bin PRIMARY KEY NOT NULL)",
            )
            .await?;
        }
        // Case-insensitive collation: case variants of one key collide, so
        // keys differ by letter, in mixed case.
        conn.exec_batch(
            "INSERT INTO mz_probe_collation_test.t_ci VALUES (?)",
            ["Apple", "apricot", "banana", "Cherry"].map(|id| (id,)),
        )
        .await?;
        // Binary collation: case variants coexist and order by byte value.
        conn.exec_batch(
            "INSERT INTO mz_probe_collation_test.t_bin VALUES (?)",
            ["ABC", "ABD", "abc", "abd"].map(|id| (id,)),
        )
        .await?;
        #[allow(clippy::disallowed_methods)]
        {
            conn.query_drop("ANALYZE TABLE mz_probe_collation_test.t_ci")
                .await?;
            conn.query_drop("ANALYZE TABLE mz_probe_collation_test.t_bin")
                .await?;
        }

        let t_ci = QualifiedTableRef {
            schema_name: "mz_probe_collation_test",
            table_name: "t_ci",
        };
        let mut prober = KeyProber::new(&mut conn, t_ci, "id");
        // 'A' anchors past 'apricot' too: LIKE is case-insensitive here, so a
        // returned prefix covers every case variant of it.
        assert_eq!(walk_prefixes(&mut prober, 1).await?, ["A", "b", "C"]);
        assert_eq!(
            walk_prefixes(&mut prober, 32).await?,
            ["Apple", "apricot", "banana", "Cherry"]
        );

        let t_bin = QualifiedTableRef {
            schema_name: "mz_probe_collation_test",
            table_name: "t_bin",
        };
        let mut prober = KeyProber::new(&mut conn, t_bin, "id");
        // Uppercase sorts before lowercase in byte order, and case variants
        // are distinct prefixes.
        assert_eq!(walk_prefixes(&mut prober, 1).await?, ["A", "a"]);
        assert_eq!(
            walk_prefixes(&mut prober, 32).await?,
            ["ABC", "ABD", "abc", "abd"]
        );

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_probe_collation_test")
            .await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_stale_statistics() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        recreate_db(&mut conn, "mz_probe_stale_test").await?;
        // STATS_AUTO_RECALC=0 plus an ANALYZE while empty pins the persisted
        // statistics at zero rows, no matter what is inserted afterwards.
        #[allow(clippy::disallowed_methods)]
        {
            conn.query_drop(
                "CREATE TABLE mz_probe_stale_test.t \
                 (id VARCHAR(32) PRIMARY KEY NOT NULL) \
                 STATS_AUTO_RECALC=0, STATS_PERSISTENT=1",
            )
            .await?;
            conn.query_drop("ANALYZE TABLE mz_probe_stale_test.t")
                .await?;
        }
        let ids: Vec<String> = (0..1000).map(|i| format!("a{i:05}")).collect();
        conn.exec_batch(
            "INSERT INTO mz_probe_stale_test.t VALUES (?)",
            ids.iter().map(|id| (id.as_str(),)),
        )
        .await?;

        // The staleness this test is about: table_rows reports 0.
        let table_rows: Option<u64> = conn
            .exec_first(
                "SELECT table_rows FROM information_schema.tables \
                 WHERE table_schema = 'mz_probe_stale_test' AND table_name = 't'",
                (),
            )
            .await?;
        assert_eq!(table_rows, Some(0));

        let table = QualifiedTableRef {
            schema_name: "mz_probe_stale_test",
            table_name: "t",
        };
        let mut prober = KeyProber::new(&mut conn, table, "id");

        // Range estimates come from index dives on the real B-tree, not the
        // stale table statistics, so they still reflect the actual data.
        let all = prober.estimate_range_rows("", None).await?;
        assert!((500..=2000).contains(&all), "all={all}");
        let range = prober.estimate_range_rows("a00100", Some("a00200")).await?;
        assert!((50..=200).contains(&range), "range={range}");

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_probe_stale_test").await?;
        conn.disconnect().await?;
        Ok(())
    }
}
