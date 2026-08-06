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

use crate::{MySqlError, QualifiedTableRef, quote_identifier};

/// The escape character for `LIKE` patterns built by [`like_prefix_pattern`].
const LIKE_ESCAPE: char = '|';

pub struct KeyProber<'a> {
    conn: &'a mut mysql_async::Conn,
    /// Quoted `` `schema`.`table` `` for SQL interpolation.
    table: String,
    /// Quoted key column for SQL interpolation.
    col: String,
    /// Unquoted `schema.table` for error reporting.
    table_name: String,
    /// Unquoted key column for error reporting.
    col_name: String,
}

impl<'a> KeyProber<'a> {
    /// NOTE: `conn` is assumed to use a utf8mb4 connection character set (the
    /// driver's handshake default), so key values arrive converted to UTF-8.
    pub fn new(
        conn: &'a mut mysql_async::Conn,
        table: QualifiedTableRef<'_>,
        key_col: &str,
    ) -> Self {
        Self {
            conn,
            table: format!(
                "{}.{}",
                quote_identifier(table.schema_name),
                quote_identifier(table.table_name)
            ),
            col: quote_identifier(key_col),
            table_name: format!("{}.{}", table.schema_name, table.table_name),
            col_name: key_col.to_string(),
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
        lower_bound_exclusive: Option<&str>,
        upper_bound_exclusive: Option<&str>,
    ) -> Result<Option<u64>, MySqlError> {
        let (clause, params) = self.range_filter(lower_bound_exclusive, upper_bound_exclusive);
        let select = format!(
            "SELECT {col} FROM {table} WHERE {clause}",
            col = self.col,
            table = self.table,
        );
        explain_row_estimate(&mut *self.conn, &select, Params::Positional(params)).await
    }

    /// Grabs a prefix of up to `max_prefix_length` characters for the first
    /// key in the given range. If the key is shorter than `max_prefix_length`,
    /// it returns that shorter value.
    ///
    /// The query will generally look something like:
    ///
    /// ```sql
    /// SELECT LEFT(pk_col, 3) FROM table
    /// WHERE pk_col > 'ab' AND pk_col < 'ac'
    /// ORDER BY pk_col
    /// LIMIT 1
    /// ```
    pub async fn prefix_of_first_key_in_range(
        &mut self,
        lower_bound_exclusive: Option<&str>,
        upper_bound_exclusive: Option<&str>,
        max_prefix_length: usize,
    ) -> Result<Option<String>, MySqlError> {
        let (clause, params) = self.range_filter(lower_bound_exclusive, upper_bound_exclusive);
        let sql = format!(
            "SELECT LEFT({col}, {max_prefix_length}) FROM {table} WHERE {clause} ORDER BY {col} LIMIT 1",
            col = self.col,
            table = self.table,
        );
        self.query_string(sql, params).await
    }

    /// Returns the prefix of up to `max_prefix_length` characters of the first key after `prefix`,
    /// but below `upper_bound_exclusive`. Returns None if no key matching these conditions exists.
    ///
    /// Note: this should be run inside a REPEATABLE READ transaction because
    /// it issues two queries sequentially.
    pub async fn prefix_of_first_row_not_matching_prefix(
        &mut self,
        prefix: &str,
        upper_bound_exclusive: Option<&str>,
        max_prefix_length: usize,
    ) -> Result<Option<String>, MySqlError> {
        let Some(max_key) = self
            .max_key_with_prefix(prefix, upper_bound_exclusive)
            .await?
        else {
            return Ok(None);
        };
        self.prefix_of_first_key_in_range(Some(&max_key), upper_bound_exclusive, max_prefix_length)
            .await
    }

    /// Quick way to grab the maximum key matching the prefix below the
    /// exclusive upper bound.
    ///
    /// The query will generally look something like:
    ///
    /// ```sql
    ///     SELECT pk_col FROM table
    ///     WHERE pk_col LIKE /* prefix% */ 'abc%'  AND pk_col < /* upper_bound_exclusive */ 'ac'
    ///     ORDER BY pk_col DESC
    ///     LIMIT 1
    /// ```
    async fn max_key_with_prefix(
        &mut self,
        prefix: &str,
        upper_bound_exclusive: Option<&str>,
    ) -> Result<Option<String>, MySqlError> {
        let (range_clause, range_params) = self.range_filter(None, upper_bound_exclusive);
        let sql = format!(
            "SELECT {col} FROM {table} WHERE {col} LIKE ? ESCAPE '{LIKE_ESCAPE}' \
             AND {range_clause} ORDER BY {col} DESC LIMIT 1",
            col = self.col,
            table = self.table,
        );
        let mut params: Vec<Value> = vec![like_prefix_pattern(prefix).into()];
        params.extend(range_params);
        self.query_string(sql, params).await
    }

    /// Returns clause with upper and lower bounds enforced if present.
    /// If both are None returns TRUE so this can plug in cleanly after a
    /// leading "WHERE" or "AND".
    fn range_filter(
        &self,
        lower_bound_exclusive: Option<&str>,
        upper_bound_exclusive: Option<&str>,
    ) -> (String, Vec<Value>) {
        let col = &self.col;
        let mut conditions = Vec::new();
        let mut params: Vec<Value> = Vec::new();
        if let Some(lower) = lower_bound_exclusive {
            conditions.push(format!("{col} > ?"));
            params.push(lower.into());
        }
        if let Some(upper) = upper_bound_exclusive {
            conditions.push(format!("{col} < ?"));
            params.push(upper.into());
        }
        if conditions.is_empty() {
            ("TRUE".to_string(), params)
        } else {
            (conditions.join(" AND "), params)
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
        match row.and_then(|mut row| row.take_opt::<String, _>(0)) {
            None => Ok(None),
            Some(Ok(value)) => Ok(Some(value)),
            Some(Err(err)) => Err(MySqlError::NonUtf8KeyValue {
                qualified_table_name: self.table_name.clone(),
                column_name: self.col_name.clone(),
                error: err.to_string(),
            }),
        }
    }
}

/// LIKE uses % and _ as wildcard characters. By default MySQL uses a backslash as an escape character
/// but that can be disabled via config, so we instead specify a specific escape character.
/// LIKE operator: <https://dev.mysql.com/doc/refman/8.0/en/string-comparison-functions.html#operator_like>
/// Backslash escapes: <https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_backslash_escapes>
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

async fn explain_row_estimate<P>(
    conn: &mut mysql_async::Conn,
    select: &str,
    params: P,
) -> Result<Option<u64>, MySqlError>
where
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
        assert_eq!(
            prefix_of_first_key_in_range(p, None, None, 1).await,
            some("a")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a", None, 1).await,
            some("b")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "b", None, 1).await,
            some("c")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "c", None, 1).await,
            None
        );

        assert_eq!(
            prefix_of_first_key_in_range(p, Some("a"), Some("b"), 2).await,
            some("aa")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "aa", Some("b"), 2).await,
            some("ab")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "ab", Some("b"), 2).await,
            None
        );

        // Bounds are exclusive: the exact key "b" is skipped as a split
        // point, and its extensions surface as their own prefixes.
        assert_eq!(
            prefix_of_first_key_in_range(p, Some("b"), Some("c"), 2).await,
            some("bb")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "bb", Some("c"), 2).await,
            None
        );
        assert_eq!(
            prefix_of_first_key_in_range(p, Some("c"), None, 2).await,
            None
        );

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
        let all = p.estimate_range_rows(None, None).await?.expect("estimate");
        assert!((500..=2000).contains(&all), "all={all}");
        let half = p
            .estimate_range_rows(Some("a00500"), None)
            .await?
            .expect("estimate");
        assert!((250..=1000).contains(&half), "half={half}");
        let none = p
            .estimate_range_rows(Some("zzz"), None)
            .await?
            .expect("estimate");
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

    // Wrapped to limit boilerplate
    async fn prefix_of_first_key_in_range(
        prober: &mut KeyProber<'_>,
        lower_bound_exclusive: Option<&str>,
        upper_bound_exclusive: Option<&str>,
        max_prefix_length: usize,
    ) -> Option<String> {
        prober
            .prefix_of_first_key_in_range(
                lower_bound_exclusive,
                upper_bound_exclusive,
                max_prefix_length,
            )
            .await
            .expect("prefix_of_first_key_in_range failed")
    }

    // Wrapped to limit boilerplate
    async fn prefix_of_first_row_not_matching_prefix(
        prober: &mut KeyProber<'_>,
        prefix: &str,
        upper_bound_exclusive: Option<&str>,
        max_prefix_length: usize,
    ) -> Option<String> {
        prober
            .prefix_of_first_row_not_matching_prefix(
                prefix,
                upper_bound_exclusive,
                max_prefix_length,
            )
            .await
            .expect("prefix_of_first_row_not_matching_prefix failed")
    }

    /// `Some` for comparing against [`prefix_of_first_key_in_range`] and
    /// [`prefix_of_first_row_not_matching_prefix`] results without `as_deref` noise at
    /// every assertion.
    fn some(s: &str) -> Option<String> {
        Some(s.into())
    }
}
