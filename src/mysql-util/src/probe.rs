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

/// The escape character for `LIKE` patterns built by [`like_prefix_pattern`].
const LIKE_ESCAPE: char = '|';

pub struct KeyProber<'a, Q> {
    conn: &'a mut Q,
    table: String,
    col: String,
}

impl<'a, Q: Queryable> KeyProber<'a, Q> {
    /// NOTE: `conn` is assumed to use a utf8mb4 connection character set (the
    /// driver's handshake default), so key values arrive converted to UTF-8.
    /// [`KeyProber::next_prefix`] relies on that for character counting.
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

    /// Estimates the row count for the given range. Estimates vary widely. On a static table
    /// with 2.2B rows we observed estimates that should be near 2B report exactly half the
    /// `TABLE_ROWS` reported by `information_schema.tables`. The sum of the row estimates
    /// from this function were around 4B for the same test case, or about a 2x overcount relative
    /// to the 2.05B reported by `TABLE_ROWS` reported by `information_schema.tables` and the 2.2B
    /// actual rows. The underlying estimates are computed by sampling a small number of pages
    /// after traversing the index (assuming this is a primary key being filtered on), so extrapolated
    /// row counts can be inaccurate but appear to eventually converge towards more accurate estimates
    /// as the sampled range shrinks on a static table.
    pub async fn estimate_range_rows(
        &mut self,
        start: &str,
        end: Option<&str>,
    ) -> Result<Option<u64>, MySqlError> {
        let (clause, params) = self.range_filter(start, true, end);
        let select = format!(
            "SELECT {col} FROM {table} WHERE {clause}",
            col = self.col,
            table = self.table,
        );
        explain_row_estimate(&mut *self.conn, &select, Params::Positional(params)).await
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
        let (clause, mut params) = self.range_filter(start, true, end);
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
    /// SELECT LEFT(pk_col, /* len */ 3) FROM table
    /// WHERE pk_col > (
    ///     SELECT pk_col FROM table
    ///     WHERE pk_col LIKE /* cur% */ 'abc%'  AND pk_col < /* end */ 'ac'
    ///     ORDER BY pk_col DESC
    ///     LIMIT 1
    /// ) AND pk_col < /* end */ 'ac'
    /// ORDER BY pk_col
    /// LIMIT 1
    /// ```
    ///
    /// In this case it would likely return something like `abd` or `abe`, but not `aca`. It's
    /// useful for finding the next prefix of a given length. If the next key is shorter than the given
    /// length it will return the shorter key. In MySQL shorter keys are ordered before keys with the same
    /// prefix, and this holds for all collations.
    pub async fn next_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        // chars().count() counts Unicode code points. The column can use any
        // character set, but the driver handshakes the connection character
        // set as utf8mb4, so values reach us converted to UTF-8, and every
        // MySQL character converts to exactly one code point. The count here
        // therefore agrees with how LEFT counted characters when it produced
        // the prefix.
        if cur.chars().count() < len {
            // When `cur` has fewer than `len` characters it likely names an exact key, i.e. imagine strings
            // "a", "aa", "aaa". If we get "a" with depth 2 we want the next key to be "aa". Without this block,
            // we'd skip past all of these strings because they match the prefix "a".
            let (clause, mut params) = self.range_filter(cur, false, end);
            let sql = format!(
                "SELECT LEFT({col}, ?) FROM {table} WHERE {clause} ORDER BY {col} LIMIT 1",
                col = self.col,
                table = self.table,
            );
            params.insert(0, u64::cast_from(len).into());
            return self.query_string(sql, params).await;
        }
        let col = &self.col;
        let table = &self.table;
        let mut sql = format!(
            "SELECT LEFT({col}, ?) FROM {table} \
             WHERE {col} > (SELECT {col} FROM {table} WHERE {col} LIKE ? ESCAPE '{LIKE_ESCAPE}'"
        );
        let mut params: Vec<Value> =
            vec![u64::cast_from(len).into(), like_prefix_pattern(cur).into()];
        self.less_than_end(&mut sql, &mut params, end);
        sql.push_str(&format!(" ORDER BY {col} DESC LIMIT 1)"));
        self.less_than_end(&mut sql, &mut params, end);
        sql.push_str(&format!(" ORDER BY {col} LIMIT 1"));
        self.query_string(sql, params).await
    }

    fn range_filter(
        &self,
        start: &str,
        inclusive: bool,
        end: Option<&str>,
    ) -> (String, Vec<Value>) {
        let col = &self.col;
        let op = if inclusive { ">=" } else { ">" };
        let mut clause = format!("{col} {op} ?");
        let mut params: Vec<Value> = vec![start.into()];
        self.less_than_end(&mut clause, &mut params, end);
        (clause, params)
    }

    fn less_than_end(&self, sql: &mut String, params: &mut Vec<Value>, end: Option<&str>) {
        if let Some(end) = end {
            let col = &self.col;
            sql.push_str(&format!(" AND {col} < ?"));
            params.push(end.into());
        }
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

/// LIKE uses % and _ as wildcard characters. By default MySQL uses a backslash as an escape character
/// but that can be disabled via config, so we instead specify a specific escape character.
/// LIKE operator: https://dev.mysql.com/doc/refman/8.0/en/string-comparison-functions.html#operator_like
/// Backslash escapes: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_backslash_escapes
fn like_prefix_pattern(prefix: &str) -> String {
    let mut pattern = String::with_capacity(prefix.len() + 1);
    for c in prefix.chars() {
        if c == LIKE_ESCAPE || matches!(c, '%' | '_') {
            pattern.push(LIKE_ESCAPE);
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
    use super::*;

    #[mz_ore::test]
    fn test_like_prefix_pattern() {
        assert_eq!(like_prefix_pattern("abc"), "abc%");
        assert_eq!(like_prefix_pattern(""), "%");
        assert_eq!(like_prefix_pattern("a_b"), "a|_b%");
        assert_eq!(like_prefix_pattern("50%"), "50|%%");
        assert_eq!(like_prefix_pattern("a|b"), "a||b%");
        // Backslash has no special meaning under an explicit ESCAPE '|'.
        assert_eq!(like_prefix_pattern("a\\b"), "a\\b%");
        assert_eq!(like_prefix_pattern("héllo"), "héllo%");
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_basic_prefix_traversal() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_basic";
        let keys = ["aa", "ab", "b", "bb", "bbb", "c"];
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        assert_eq!(first(p, "", None, 1).await, some("a"));
        assert_eq!(next(p, "a", None, 1).await, some("b"));
        assert_eq!(next(p, "b", None, 1).await, some("c"));
        assert_eq!(next(p, "c", None, 1).await, None);

        assert_eq!(first(p, "a", Some("b"), 2).await, some("aa"));
        assert_eq!(next(p, "aa", Some("b"), 2).await, some("ab"));
        assert_eq!(next(p, "ab", Some("b"), 2).await, None);

        // The key "b" is shorter than the prefix length, so the walk steps
        // past it as an exact key: bb becomes its own prefix, which then
        // subsumes bbb.
        assert_eq!(first(p, "b", Some("c"), 2).await, some("b"));
        assert_eq!(next(p, "b", Some("c"), 2).await, some("bb"));
        assert_eq!(next(p, "bb", Some("c"), 2).await, None);

        assert_eq!(first(p, "c", None, 2).await, some("c"));
        assert_eq!(next(p, "c", None, 2).await, None);

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_explain_row_estimate_sizing() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_explain_test";
        let ids: Vec<String> = (0..1000).map(|i| format!("a{i:05}")).collect();
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &ids).await?;
        let mut p = KeyProber::new(&mut conn, table, "id");

        // Estimates are index dives, near reality but never exact by
        // contract, so the bounds are deliberately loose.
        let all = p.estimate_range_rows("", None).await?.expect("estimate");
        assert!((500..=2000).contains(&all), "all={all}");
        let half = p
            .estimate_range_rows("a00500", None)
            .await?
            .expect("estimate");
        assert!((250..=1000).contains(&half), "half={half}");
        let none = p.estimate_range_rows("zzz", None).await?.expect("estimate");
        assert!(none <= 5, "none={none}");

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    // Test helpers.

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

    /// Recreates scratch database `db` holding one table `t` whose string
    /// primary key `id` is pinned to the given utf8mb4 `collation`, containing
    /// `keys`, with fresh statistics. Returns a ref for [`KeyProber::new`].
    async fn setup_table<'a>(
        conn: &mut mysql_async::Conn,
        db: &'a str,
        collation: &str,
        keys: &[impl AsRef<str> + Sync],
    ) -> Result<QualifiedTableRef<'a>, anyhow::Error> {
        recreate_db(conn, db).await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!(
            "CREATE TABLE {db}.t (id VARCHAR(36) CHARACTER SET utf8mb4 \
             COLLATE {collation} PRIMARY KEY NOT NULL)"
        ))
        .await?;
        conn.exec_batch(
            format!("INSERT INTO {db}.t VALUES (?)"),
            keys.iter().map(|id| (id.as_ref(),)),
        )
        .await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!("ANALYZE TABLE {db}.t")).await?;
        Ok(QualifiedTableRef {
            schema_name: db,
            table_name: "t",
        })
    }

    /// Drops the scratch database `db`.
    async fn drop_db(conn: &mut mysql_async::Conn, db: &str) -> Result<(), anyhow::Error> {
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!("DROP DATABASE {db}")).await?;
        Ok(())
    }

    /// [`KeyProber::first_prefix`], unwrapped so assertions stay one-liners.
    async fn first(
        prober: &mut KeyProber<'_, mysql_async::Conn>,
        start: &str,
        end: Option<&str>,
        len: usize,
    ) -> Option<String> {
        prober
            .first_prefix(start, end, len)
            .await
            .expect("first_prefix failed")
    }

    /// [`KeyProber::next_prefix`], unwrapped so assertions stay one-liners.
    async fn next(
        prober: &mut KeyProber<'_, mysql_async::Conn>,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Option<String> {
        prober
            .next_prefix(cur, end, len)
            .await
            .expect("next_prefix failed")
    }

    /// `Some` for comparing against [`first`]/[`next`] results without
    /// `as_deref` noise at every assertion.
    fn some(s: &str) -> Option<String> {
        Some(s.into())
    }
}
