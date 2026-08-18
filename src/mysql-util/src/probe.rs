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
use mz_ore::str::redact;

use crate::{MySqlError, QualifiedTableRef, quote_identifier};

/// The escape character for `LIKE` patterns built by [`like_prefix_pattern`].
const LIKE_ESCAPE: char = '|';

/// The longest key the probe bounds cover, in characters.
/// <https://dev.mysql.com/doc/refman/8.4/en/innodb-limits.html> caps an index
/// key at 3072 bytes, or 768 utf8mb4 characters. Longer keys (possible
/// through prefix indexes or narrower charsets) are not supported.
pub const MAX_KEY_LENGTH: u32 = 768;

/// Probes a string primary key column. Only supports `utf8mb4_bin` against CHAR/VARCHAR
/// columns up to 768 characters. Enforcement is deferred to the caller. There may be
/// other collations we can support, but we should do more validation.
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
        lower_bound_exclusive: &str,
        upper_bound_exclusive: Option<&str>,
    ) -> Result<u64, MySqlError> {
        let (clause, params) =
            self.range_filter(Some(lower_bound_exclusive), upper_bound_exclusive);
        let select = format!(
            "SELECT {col} FROM {table} WHERE {clause}",
            col = self.col,
            table = self.table,
        );
        explain_row_estimate(&mut *self.conn, &select, Params::Positional(params))
            .await?
            .ok_or_else(|| MySqlError::MissingRowEstimate {
                qualified_table_name: self.table_name.clone(),
                // The bounds are column values, redact them so the error
                // stays loggable outside of CI.
                lower_bound: format!("{:?}", redact(&lower_bound_exclusive)),
                upper_bound: format!("{:?}", redact(&upper_bound_exclusive)),
            })
    }

    /// Grabs a prefix of up to `max_prefix_length` characters for the first
    /// key in the given range. If the key is shorter than `max_prefix_length`,
    /// it returns that shorter value.
    ///
    /// The query will generally look something like:
    ///
    /// ```sql
    /// SELECT LEFT(pk_col, 3) FROM table
    /// WHERE pk_col > 'ab' AND pk_col < RPAD('ac', 768, CHAR(0))
    /// ORDER BY pk_col
    /// LIMIT 1
    /// ```
    pub async fn prefix_of_first_key_in_range(
        &mut self,
        lower_bound_exclusive: &str,
        upper_bound_exclusive: Option<&str>,
        max_prefix_length: usize,
    ) -> Result<Option<String>, MySqlError> {
        let (clause, params) =
            self.range_filter(Some(lower_bound_exclusive), upper_bound_exclusive);
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
        self.prefix_of_first_key_in_range(&max_key, upper_bound_exclusive, max_prefix_length)
            .await
    }

    /// Quick way to grab the maximum key matching the prefix below the
    /// exclusive upper bound.
    ///
    /// The query will generally look something like:
    ///
    /// ```sql
    ///     SELECT pk_col FROM table
    ///     WHERE pk_col LIKE /* prefix% */ 'abc%' AND pk_col < /* upper_bound_exclusive */ RPAD('ac', 768, CHAR(0))
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
    ///
    /// The upper bound is padded with NUL characters so that no key it
    /// prefixes falls inside the range. Under PAD SPACE collations like
    /// `utf8mb4_bin` "ab" is ordered as equivalent to "ab        " (however
    /// many spaces are needed to fill remaining char/varchar length), so "ab\0"
    /// sorts before either of those (because NUL is below all other characters
    /// in `utf8mb4_bin`). Padding to [`MAX_KEY_LENGTH`] bounds every key an
    /// utf8mb4 primary key column can hold.
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
            conditions.push(format!(
                "{col} < RPAD(?, {MAX_KEY_LENGTH}, CHAR(0 USING utf8mb4))"
            ));
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

/// The live MySQL harness here is shared with [`crate::partition`]'s tests.
#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeSet;

    use mz_ore::cast::CastFrom;

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
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        assert_eq!(
            prefix_of_first_key_in_range(p, "", None, 1).await,
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
            prefix_of_first_key_in_range(p, "a", Some("b"), 2).await,
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
            prefix_of_first_key_in_range(p, "b", Some("c"), 2).await,
            some("bb")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "bb", Some("c"), 2).await,
            None
        );
        assert_eq!(prefix_of_first_key_in_range(p, "c", None, 2).await, None);

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
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &ids).await?;
        let mut p = KeyProber::new(&mut conn, table, "id");

        // Estimates are index dives, near reality but never exact by
        // contract, so the bounds are deliberately loose.
        let all = p.estimate_range_rows("", None).await?;
        assert!((500..=2000).contains(&all), "all={all}");
        let half = p.estimate_range_rows("a00500", None).await?;
        assert!((250..=1000).contains(&half), "half={half}");
        let none = p.estimate_range_rows("zzz", None).await?;
        assert!(none <= 5, "none={none}");

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_case_insensitive_prefix_traversal() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_case_insensitive";
        let keys = ["Aa", "ab", "b", "Bb", "bbb", "C"];
        let table = setup_table(&mut conn, DB, "utf8mb4_general_ci", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        // Sorting is case-insensitive but returned prefixes are the stored
        // bytes: "A", "b", "C".
        // Grab the initial prefix.
        assert_eq!(
            prefix_of_first_key_in_range(p, "", None, 1).await,
            some("A")
        );
        // Traverse through sibling prefixes at depth 1.
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "A", None, 1).await,
            some("b")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "b", None, 1).await,
            some("C")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "C", None, 1).await,
            None
        );

        // Same traversal at depth 2, bounded by the depth-1 prefixes.
        assert_eq!(
            prefix_of_first_key_in_range(p, "A", Some("b"), 2).await,
            some("Aa")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "Aa", Some("b"), 2).await,
            some("ab")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "ab", Some("b"), 2).await,
            None
        );

        // The exclusive bound skips the exact key "b", and its extensions
        // surface as their own prefixes under this case-insensitive
        // collation.
        assert_eq!(
            prefix_of_first_key_in_range(p, "b", Some("C"), 2).await,
            some("Bb")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "Bb", Some("C"), 2).await,
            None
        );
        // Every key matching 'b%' is covered by the prefix match.
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "b", Some("C"), 2).await,
            None
        );

        assert_eq!(prefix_of_first_key_in_range(p, "C", None, 2).await, None);

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_wildcard_char_in_data() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_wildcard_test";
        // Keys are a_1, a\2, a\3, a%4, a|5, covering the LIKE wildcards
        // and the escape character itself. utf8mb4_bin orders them by byte:
        // a%4 < a\2 < a\3 < a_1 < a|5.
        let keys = ["a_1", "a\\2", "a\\3", "a%4", "a|5"];
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        assert_eq!(
            prefix_of_first_key_in_range(p, "", None, 1).await,
            some("a")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a", None, 1).await,
            None
        );
        assert_eq!(
            prefix_of_first_key_in_range(p, "a", None, 2).await,
            some("a%")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a%", None, 2).await,
            some("a\\")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a\\", None, 2).await,
            some("a_")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a_", None, 2).await,
            some("a|")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a|", None, 2).await,
            None
        );

        // Range bounds that are themselves wildcard characters.
        assert_eq!(
            prefix_of_first_key_in_range(p, "a%", Some("a\\"), 3).await,
            some("a%4")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a%4", Some("a\\"), 3).await,
            None
        );
        assert_eq!(
            prefix_of_first_key_in_range(p, "a\\", Some("a_"), 3).await,
            some("a\\2")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a\\2", Some("a_"), 3).await,
            some("a\\3")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a\\3", Some("a_"), 3).await,
            None
        );
        assert_eq!(
            prefix_of_first_key_in_range(p, "a_", Some("a|"), 3).await,
            some("a_1")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a_1", None, 3).await,
            some("a|5")
        );
        assert_eq!(
            prefix_of_first_key_in_range(p, "a|", None, 3).await,
            some("a|5")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a|5", None, 3).await,
            None
        );

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_multibyte_chars_in_data() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_multibyte_test";
        // utf8mb4_general_ci gives every supplementary character one shared
        // weight, so emoji sort last: a < a😀 < 日本 < 日本語 < 😀 < 😀a < 😀😀.
        let keys = ["a", "a😀", "日本", "日本語", "😀", "😀a", "😀😀"];
        let table = setup_table(&mut conn, DB, "utf8mb4_general_ci", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        // Prefix lengths count characters, not bytes: a one-char prefix of a
        // four-byte emoji is the whole emoji, never a broken fragment.
        assert_eq!(
            prefix_of_first_key_in_range(p, "", None, 1).await,
            some("a")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a", None, 1).await,
            some("日")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "日", None, 1).await,
            some("😀")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "😀", None, 1).await,
            None
        );

        // Depth 2 walk for each prefix from depth 1.
        assert_eq!(
            prefix_of_first_key_in_range(p, "a", Some("日"), 2).await,
            some("a😀")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a😀", Some("日"), 2).await,
            None
        );
        assert_eq!(
            prefix_of_first_key_in_range(p, "日", Some("😀"), 2).await,
            some("日本")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "日本", Some("😀"), 2).await,
            None
        );
        assert_eq!(
            prefix_of_first_key_in_range(p, "😀", None, 2).await,
            some("😀a")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "😀a", None, 2).await,
            some("😀😀")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "😀😀", None, 2).await,
            None
        );

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_uuid_pk() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_uuid_test";
        // Hyphenated lowercase v4-shaped UUIDs, unique via the last group,
        // with the leading group scattered like random UUIDs.
        let ids: Vec<String> = (0..1000u64)
            .map(|i| {
                let h = i.wrapping_mul(2654435761) % 0x1_0000_0000;
                format!("{h:08x}-0000-4000-8000-{i:012x}")
            })
            .collect();
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &ids).await?;
        let mut p = KeyProber::new(&mut conn, table, "id");

        // Lowercase hex order matches byte order under this collation.
        assert_eq!(
            prefix_of_first_key_in_range(&mut p, "", None, 36).await,
            ids.iter().min().cloned()
        );

        let walked = walk_prefixes(&mut p, 1).await?;
        let expected: Vec<String> = ids
            .iter()
            .map(|id| id[..1].to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        assert_eq!(walked, expected);

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_like_metacharacters() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_like_test";
        // Every walk step matches on `LIKE '<prefix>%'`, so keys whose
        // prefixes are LIKE metacharacters exercise the escaping.
        let ids = [
            "%a",
            "%%",
            "_a",
            "__",
            "\\a",
            "\\\\",
            "a%",
            "a%b",
            "a_",
            "a_b",
            "a\\",
            "a\\b",
            "ab",
            "a b",
            "|a",
            "||",
            "a|",
            "a|b",
            "100%",
            "50%off",
            "under_score",
            "back\\slash",
        ];
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &ids).await?;

        // Assert that walking the prefixes gives range boundaries that
        // partition the table and every key falls into exactly one range.
        for len in [1, 2] {
            let walked =
                walk_prefixes(&mut KeyProber::new(&mut conn, table.clone(), "id"), len).await?;
            let mut total = 0;
            for (i, lo) in walked.iter().enumerate() {
                let (n, prefixed) = count_range(&mut conn, DB, lo, walked.get(i + 1)).await?;
                assert!(
                    n > 0,
                    "empty interval: len={len} lo={lo:?} walked={walked:?}"
                );
                assert_eq!(
                    prefixed, n,
                    "keys outside prefix: len={len} lo={lo:?} walked={walked:?}"
                );
                total += n;
            }
            assert_eq!(
                total,
                u64::cast_from(ids.len()),
                "len={len} walked={walked:?}"
            );
        }

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_collations() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const CI_DB: &str = "mz_probe_collation_ci_test";
        const BIN_DB: &str = "mz_probe_collation_bin_test";

        // Case-insensitive collation: case variants of one key collide, so
        // keys differ by letter, in mixed case.
        let ci_keys = ["Apple", "apricot", "banana", "Cherry"];
        let t_ci = setup_table(&mut conn, CI_DB, "utf8mb4_general_ci", &ci_keys).await?;
        let mut prober = KeyProber::new(&mut conn, t_ci, "id");
        // 'A' covers 'apricot' too: LIKE is case-insensitive here, so a
        // returned prefix covers every case variant of it.
        assert_eq!(walk_prefixes(&mut prober, 1).await?, ["A", "b", "C"]);

        // Binary collation: case variants coexist and order by byte value.
        let bin_keys = ["ABC", "ABD", "abc", "abd"];
        let t_bin = setup_table(&mut conn, BIN_DB, "utf8mb4_bin", &bin_keys).await?;
        let mut prober = KeyProber::new(&mut conn, t_bin, "id");
        // Uppercase sorts before lowercase in byte order, and case variants
        // are distinct prefixes.
        assert_eq!(walk_prefixes(&mut prober, 1).await?, ["A", "a"]);
        assert_eq!(walk_prefixes(&mut prober, 3).await?, bin_keys);

        drop_db(&mut conn, CI_DB).await?;
        drop_db(&mut conn, BIN_DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_invalid_utf8_keys() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_binary_test";
        recreate_db(&mut conn, DB).await?;
        // A binary key column passes bytes through unconverted, so this is
        // the one way invalid UTF-8 can reach the client. The snapshot
        // operator will only support char and varchar columns, so this
        // shouldn't happen in practice.
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!(
            "CREATE TABLE {DB}.t (id VARBINARY(36) PRIMARY KEY NOT NULL)"
        ))
        .await?;
        let keys: Vec<Vec<u8>> = vec![b"a1".to_vec(), b"a2".to_vec(), vec![0xff, 0xfe, 0x31]];
        conn.exec_batch(
            format!("INSERT INTO {DB}.t VALUES (?)"),
            keys.iter().map(|k| (Value::Bytes(k.clone()),)),
        )
        .await?;
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!("ANALYZE TABLE {DB}.t")).await?;
        let table = QualifiedTableRef {
            schema_name: DB,
            table_name: "t",
        };
        let mut p = KeyProber::new(&mut conn, table, "id");

        // Estimates never decode key values, they keep working.
        assert!(p.estimate_range_rows("", None).await.is_ok());

        // ASCII keys order before the 0xff key and decode fine.
        assert_eq!(
            prefix_of_first_key_in_range(&mut p, "", None, 2).await,
            some("a1")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(&mut p, "a1", None, 2).await,
            some("a2")
        );
        // The next key is invalid UTF-8. The probe reports it as a named
        // error so callers can log it and fall back.
        let err = p
            .prefix_of_first_row_not_matching_prefix("a2", None, 2)
            .await
            .unwrap_err();
        assert!(matches!(err, MySqlError::NonUtf8KeyValue { .. }), "{err:?}");

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_stale_statistics() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_stale_test";
        recreate_db(&mut conn, DB).await?;
        // This setup stays bespoke: STATS_AUTO_RECALC=0 plus an ANALYZE while
        // empty pins the persisted statistics at zero rows, no matter what is
        // inserted afterwards.
        #[allow(clippy::disallowed_methods)]
        {
            conn.query_drop(format!(
                "CREATE TABLE {DB}.t (id VARCHAR(36) CHARACTER SET utf8mb4 \
                 COLLATE utf8mb4_bin PRIMARY KEY NOT NULL) \
                 STATS_AUTO_RECALC=0, STATS_PERSISTENT=1"
            ))
            .await?;
            conn.query_drop(format!("ANALYZE TABLE {DB}.t")).await?;
        }
        let ids: Vec<String> = (0..1000).map(|i| format!("a{i:05}")).collect();
        conn.exec_batch(
            format!("INSERT INTO {DB}.t VALUES (?)"),
            ids.iter().map(|id| (id.as_str(),)),
        )
        .await?;

        // The staleness this test is about: table_rows reports 0.
        let table_rows: Option<u64> = conn
            .exec_first(
                "SELECT table_rows FROM information_schema.tables \
                 WHERE table_schema = ? AND table_name = 't'",
                (DB,),
            )
            .await?;
        assert_eq!(table_rows, Some(0));

        let table = QualifiedTableRef {
            schema_name: DB,
            table_name: "t",
        };
        let mut prober = KeyProber::new(&mut conn, table, "id");

        // Range estimates come from index dives on the real B-tree, not the
        // stale table statistics, so they still reflect the actual data.
        let all = prober.estimate_range_rows("", None).await?;
        assert!((500..=2000).contains(&all), "all={all}");
        let range = prober.estimate_range_rows("a00100", Some("a00200")).await?;
        assert!((50..=200).contains(&range), "range={range}");

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_probe_sargability() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_sargable_test";
        let ids: Vec<String> = (0..1000).map(|i| format!("a{i:05}")).collect();
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &ids).await?;

        // Prove the methodology first: a deliberately non-sargable predicate
        // reads every row, and the session handler counters see it.
        let before = handler_reads(&mut conn).await?;
        let _: Option<u64> = conn
            .exec_first(
                format!("SELECT COUNT(*) FROM {DB}.t WHERE LEFT(id, 2) = 'a0'"),
                (),
            )
            .await?;
        let scan_reads = handler_reads(&mut conn).await? - before;
        assert!(scan_reads >= 1000, "scan_reads={scan_reads}");

        // Every probe must stay a handful of index operations. A regression
        // to a scan costs >= 1000 reads, far past the generous bound.
        let before = handler_reads(&mut conn).await?;
        let got = prefix_of_first_key_in_range(
            &mut KeyProber::new(&mut conn, table.clone(), "id"),
            "a00500",
            None,
            6,
        )
        .await;
        let reads = handler_reads(&mut conn).await? - before;
        // The exclusive bound skips the exact key a00500.
        assert_eq!(got, some("a00501"));
        assert!(reads < 50, "prefix_of_first_key_in_range reads={reads}");

        let before = handler_reads(&mut conn).await?;
        let got = prefix_of_first_row_not_matching_prefix(
            &mut KeyProber::new(&mut conn, table.clone(), "id"),
            "a00500",
            None,
            6,
        )
        .await;
        let reads = handler_reads(&mut conn).await? - before;
        assert_eq!(got, some("a00501"));
        assert!(reads < 50, "max_key probe reads={reads}");

        let before = handler_reads(&mut conn).await?;
        let got = prefix_of_first_row_not_matching_prefix(
            &mut KeyProber::new(&mut conn, table.clone(), "id"),
            "a0",
            None,
            6,
        )
        .await;
        let reads = handler_reads(&mut conn).await? - before;
        // Every key matches 'a0%', so the prefix match covers the whole table and
        // there is no next prefix, at the cost of two dives rather than a
        // scan.
        assert_eq!(got, None);
        assert!(reads < 50, "whole-table match reads={reads}");

        let before = handler_reads(&mut conn).await?;
        let got = prefix_of_first_key_in_range(
            &mut KeyProber::new(&mut conn, table.clone(), "id"),
            "a00500",
            Some("a00501"),
            6,
        )
        .await;
        let reads = handler_reads(&mut conn).await? - before;
        assert_eq!(got, None);
        assert!(reads < 50, "empty bounded range reads={reads}");

        let bounded = KeyProber::new(&mut conn, table.clone(), "id")
            .estimate_range_rows("a00100", Some("a00200"))
            .await?;
        assert!((50..=300).contains(&bounded), "bounded estimate={bounded}");

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    /// `utf8mb4_bin` has no contractions or expansions: Czech `ch` stays an
    /// ordinary `c` extension and `ß` an ordinary character, so the walk
    /// visits every prefix. This would not work with the standard default collation.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_bin_no_contraction_or_expansion() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_bin_no_hazards";
        let keys = [
            "aaa", "asz", "aßx", "cesta", "chleba", "duha", "hora", "ibis",
        ];
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        assert_eq!(walk_prefixes(p, 1).await?, ["a", "c", "d", "h", "i"]);
        assert_eq!(
            walk_prefixes(p, 2).await?,
            ["aa", "as", "aß", "ce", "ch", "du", "ho", "ib"]
        );

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    /// `utf8mb4_bin` compares character by character but is PAD SPACE, so
    /// keys starting below space sort below the empty string. A walk seeded
    /// with the empty string drops them, they land in the snapshot range
    /// left of the first boundary. This means keys starting below space
    /// will just be included in the first open range, which will be fine
    /// for our partitioning, just a little unbalanced.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_keys_below_empty_string() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_below_empty_test";
        let keys = ["\0a", "\u{1}a", "\u{9}b", "a1", "a1\u{1}x", "b1"];
        let table = setup_table(&mut conn, DB, "utf8mb4_bin", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        assert_eq!(
            prefix_of_first_key_in_range(p, "", None, 2).await,
            some("a1")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "a1", None, 2).await,
            some("b1")
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "b1", None, 2).await,
            None
        );

        assert_eq!(
            prefix_of_first_key_in_range(p, "", Some("a1"), 2).await,
            None
        );
        assert_eq!(
            prefix_of_first_row_not_matching_prefix(p, "\u{9}", Some("a1"), 2).await,
            None
        );
        assert_eq!(
            prefix_of_first_key_in_range(p, "", Some("b1"), 2).await,
            some("a1")
        );

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    // Test helpers.

    /// Connects to the server named by `MZ_TEST_MYSQL_URL`, or `None` to skip
    /// the test when it is unset. Skipping is a local-only convenience, CI
    /// must always provide the URL.
    pub(crate) async fn connect() -> Result<Option<mysql_async::Conn>, anyhow::Error> {
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
    /// primary key `id` is pinned to the given `collation`, containing
    /// `keys`, with fresh statistics. Returns a ref for [`KeyProber::new`].
    pub(crate) async fn setup_table<'a>(
        conn: &mut mysql_async::Conn,
        db: &'a str,
        collation: &str,
        keys: &[impl AsRef<str> + Sync],
    ) -> Result<QualifiedTableRef<'a>, anyhow::Error> {
        recreate_db(conn, db).await?;
        // MySQL collation names start with their character set's name, so
        // the charset is pinned explicitly without a second parameter.
        let charset = collation.split('_').next().expect("nonempty collation");
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!(
            "CREATE TABLE {db}.t (id VARCHAR(36) CHARACTER SET {charset} \
             COLLATE {collation} PRIMARY KEY NOT NULL)"
        ))
        .await?;

        for chunk in keys.chunks(1000) {
            conn.exec_drop(
                format!(
                    "INSERT INTO {db}.t VALUES {}",
                    vec!["(?)"; chunk.len()].join(",")
                ),
                chunk
                    .iter()
                    .map(|id| id.as_ref().into())
                    .collect::<Vec<mysql_async::Value>>(),
            )
            .await?;
        }
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!("ANALYZE TABLE {db}.t")).await?;
        Ok(QualifiedTableRef {
            schema_name: db,
            table_name: "t",
        })
    }

    /// Drops the scratch database `db`.
    pub(crate) async fn drop_db(
        conn: &mut mysql_async::Conn,
        db: &str,
    ) -> Result<(), anyhow::Error> {
        #[allow(clippy::disallowed_methods)]
        conn.query_drop(format!("DROP DATABASE {db}")).await?;
        Ok(())
    }

    /// Keys in `[lo, hi)` of `db`'s table: the total, and how many have `lo`
    /// as a prefix, counted by the server so the comparisons happen under the
    /// column's collation.
    async fn count_range(
        conn: &mut mysql_async::Conn,
        db: &str,
        lo: &str,
        hi: Option<&String>,
    ) -> Result<(u64, u64), anyhow::Error> {
        let mut clause = "id >= ?".to_string();
        let mut params: Vec<Value> = vec![lo.into(), lo.into(), lo.into()];
        if let Some(hi) = hi {
            clause.push_str(" AND id < ?");
            params.push(hi.as_str().into());
        }
        let row: Option<(u64, Option<u64>)> = conn
            .exec_first(
                format!(
                    "SELECT COUNT(*), SUM(LEFT(id, CHAR_LENGTH(?)) = ?) FROM {db}.t WHERE {clause}"
                ),
                Params::Positional(params),
            )
            .await?;
        let (total, prefixed) = row.expect("COUNT returns a row");
        Ok((total, prefixed.unwrap_or(0)))
    }

    /// Sum of this session's `Handler_read_*` counters: how many index or row
    /// read operations the connection has performed so far.
    async fn handler_reads(conn: &mut mysql_async::Conn) -> Result<u64, anyhow::Error> {
        let rows: Vec<(String, String)> = conn
            .exec("SHOW SESSION STATUS LIKE 'Handler_read%'", ())
            .await?;
        Ok(rows.into_iter().map(|(_, v)| v.parse().unwrap_or(0)).sum())
    }

    // Wrapped to limit boilerplate
    async fn prefix_of_first_key_in_range(
        prober: &mut KeyProber<'_>,
        lower_bound_exclusive: &str,
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

    /// Test helper to walk prefixes at a consistent depth. Only works when
    /// all keys have length >= len.
    async fn walk_prefixes(
        prober: &mut KeyProber<'_>,
        len: usize,
    ) -> Result<Vec<String>, anyhow::Error> {
        let mut walked = Vec::new();
        let Some(mut cur) = prober.prefix_of_first_key_in_range("", None, len).await? else {
            return Ok(walked);
        };
        loop {
            assert!(
                !walked.contains(&cur),
                "prefix repeated: {cur:?} (walked: {walked:?})"
            );
            walked.push(cur.clone());
            match prober
                .prefix_of_first_row_not_matching_prefix(&cur, None, len)
                .await?
            {
                Some(next) => cur = next,
                None => break,
            }
        }
        Ok(walked)
    }
}
