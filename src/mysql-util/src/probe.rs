// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Server-side probes over a string key column of a MySQL table, for
//! discovering how rows distribute across the key space: row count estimates
//! for half-open key ranges, and walks over the distinct key prefixes present
//! in a range. Useful for planning how to divide a table into key ranges of
//! roughly equal size, e.g. for parallel reads.
//!
//! Row estimates come from `EXPLAIN`, so probing a range costs an index dive
//! instead of a scan. All key matching and ordering happens server-side under
//! the key column's own collation, callers must not re-order the returned
//! prefixes in Rust and can only rely on byte equality.

use mysql_async::prelude::Queryable;
use mysql_async::{Params, Value};
use mz_ore::cast::CastFrom;

use crate::{MySqlError, QualifiedTableRef, quote_identifier};

/// Probes over the values of a string key column. Key values bind as prepared
/// statement parameters, identifiers are quoted at construction.
///
/// Ranges are half open: `start` is an inclusive lower bound with `""`
/// meaning the beginning of the key space, `end` is an exclusive upper bound
/// with `None` meaning unbounded.
///
/// The column must be a string column, and it should be the leading column of
/// an index or every probe becomes a full table scan.
pub struct KeyProber<'a, Q> {
    conn: &'a mut Q,
    /// Quoted `` `schema`.`table` ``.
    table: String,
    /// Quoted key column.
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

    /// Row count estimate for keys in `[start, end)`, from the optimizer.
    /// May be arbitrarily inaccurate, callers must tolerate that.
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

    /// The length-`len` prefix of the first key in `[start, end)`, or `None`
    /// if the range holds no rows.
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

    /// The smallest length-`len` key prefix strictly greater than `cur`,
    /// taken from keys below `end`. `None` if no such key exists.
    pub async fn next_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        // Anchor on the last key sharing the current prefix, then take the
        // prefix of the first key past the anchor. The range's upper bound
        // applies inside the subquery too: a key sharing the prefix but at or
        // past `end` must not become the anchor, or the outer scan would
        // start beyond the range. An empty subquery result compares as NULL
        // and the outer query returns no row, i.e. no next prefix.
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

    /// WHERE clause and params selecting keys in `[start, end)`.
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

    /// Runs a query returning at most one string value.
    ///
    /// A value that fails to decode as UTF-8 maps to `None`, i.e. "no key
    /// found". For prefix walks that ends the walk early, which callers must
    /// treat as correct-but-coarser knowledge of the key space, it beats
    /// failing outright over an unrepresentable prefix.
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

/// Escapes `prefix` for use as a `LIKE` pattern and appends the `%` wildcard,
/// so the pattern matches exactly the values starting with `prefix`.
///
/// The result is meant to be bound as a prepared statement parameter, it is
/// not quoted as a SQL string literal.
///
/// NOTE: This uses the default `LIKE` escape character `\`. Sessions with the
/// `NO_BACKSLASH_ESCAPES` SQL mode disable that default, under which wildcard
/// characters inside `prefix` would match as wildcards instead of literally.
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

/// Runs `EXPLAIN` on `select` and returns the optimizer's estimate of rows
/// examined, from the `rows` column of the plan.
///
/// The caller must pass a single-table `SELECT`. Joins and subqueries produce
/// multiple plan rows whose estimates do not combine additively, only the
/// first row is read. Returns `None` when the optimizer reports no estimate.
/// The estimate can be arbitrarily stale, callers must tolerate inaccuracy.
pub async fn explain_row_estimate<Q, P>(
    conn: &mut Q,
    select: &str,
    params: P,
) -> Result<Option<u64>, MySqlError>
where
    Q: Queryable,
    P: Into<Params> + Send,
{
    // `select` comes from this module's probes (or a caller-validated query),
    // values bind through `params`, so the interpolation is safe.
    // NOTE: The format must be pinned because newer MySQL versions default
    // `explain_format` to `TREE`, which has no `rows` column.
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
        // LIKE metacharacters in the prefix match literally.
        assert_eq!(like_prefix_pattern("a_b"), "a\\_b%");
        assert_eq!(like_prefix_pattern("50%"), "50\\%%");
        assert_eq!(like_prefix_pattern("a\\b"), "a\\\\b%");
        // Multi-byte characters pass through untouched.
        assert_eq!(like_prefix_pattern("héllo"), "héllo%");
    }

    /// Exercises the probes against a live MySQL server, covering what unit
    /// tests cannot see: `EXPLAIN` estimates over prepared statements, `LIKE`
    /// pattern semantics, and the nested next-prefix query.
    ///
    /// Skipped unless `MZ_TEST_MYSQL_URL` points at a server this test may
    /// scribble on, e.g. `mysql://root:p%40ssw0rd@127.0.0.1:13306`.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // needs a network connection
    async fn test_live_mysql() -> Result<(), anyhow::Error> {
        let Ok(url) = std::env::var("MZ_TEST_MYSQL_URL") else {
            tracing::info!("MZ_TEST_MYSQL_URL not set: skipping live MySQL test");
            return Ok(());
        };
        let mut conn = mysql_async::Conn::new(mysql_async::Opts::from_url(&url)?).await?;

        // Static DDL strings, nothing to parameterize.
        #[allow(clippy::disallowed_methods)]
        {
            conn.query_drop("DROP DATABASE IF EXISTS mz_probe_test")
                .await?;
            conn.query_drop("CREATE DATABASE mz_probe_test").await?;
            conn.query_drop("CREATE TABLE mz_probe_test.t (id VARCHAR(32) PRIMARY KEY NOT NULL)")
                .await?;
            // 900 keys under 'a', 100 under 'b', plus LIKE metacharacter keys.
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
            // Refresh optimizer statistics so EXPLAIN estimates see the rows.
            conn.query_drop("ANALYZE TABLE mz_probe_test.t").await?;
        }

        let table = QualifiedTableRef {
            schema_name: "mz_probe_test",
            table_name: "t",
        };
        let mut prober = KeyProber::new(&mut conn, table, "id");

        // Walking length-1 prefixes visits each leading character once.
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

        // The exclusive upper bound applies to both probes.
        assert_eq!(
            prober
                .first_prefix("b", Some("b00002"), 6)
                .await?
                .as_deref(),
            Some("b00001")
        );
        assert_eq!(prober.next_prefix("a", Some("b"), 1).await?, None);
        assert_eq!(prober.first_prefix("zzz", None, 1).await?, None);

        // Walking the 'c' keys at full length exercises LIKE escaping: each
        // step anchors on a key containing a LIKE metacharacter. The server's
        // collation dictates the visit order, so only compare as sets.
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

        // Estimates are approximate, only sanity-check magnitudes: the whole
        // key space dwarfs the 'b' range, and both are non-empty.
        let all = prober.estimate_range_rows("", None).await?;
        let b_range = prober.estimate_range_rows("b", Some("c")).await?;
        assert!(all > b_range, "all={all} b_range={b_range}");
        assert!(b_range > 0);

        #[allow(clippy::disallowed_methods)]
        conn.query_drop("DROP DATABASE mz_probe_test").await?;
        conn.disconnect().await?;
        Ok(())
    }
}
