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

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql() -> Result<(), anyhow::Error> {
        let Ok(url) = std::env::var("MZ_TEST_MYSQL_URL") else {
            if mz_ore::env::is_var_truthy("CI") {
                panic!("CI is supposed to run this test but something has gone wrong!");
            }
            tracing::info!("MZ_TEST_MYSQL_URL not set: skipping live MySQL test");
            return Ok(());
        };
        let mut conn = mysql_async::Conn::new(mysql_async::Opts::from_url(&url)?).await?;

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
}
