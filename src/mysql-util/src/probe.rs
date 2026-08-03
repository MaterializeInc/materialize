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
        let (clause, params) = self.range_filter(start, true, end);
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
    /// useful for finding the next prefix of a given length. If the next key is shorter than the given
    /// length it will return the shorter key. In MySQL shorter keys are ordered before keys with the same
    /// prefix, and this holds for all collations.
    pub async fn next_prefix(
        &mut self,
        cur: &str,
        end: Option<&str>,
        len: usize,
    ) -> Result<Option<String>, MySqlError> {
        // chars().count() counts Unicode code points, which is also what one
        // "character" means to LEFT and CHAR_LENGTH for utf8mb4 data, so the
        // short-key check agrees with how the prefix was produced. Data from
        // MySQL is already decoded as Unicode -- anything not matching should have
        // caused a failure. We could move towards explicitly querying/storing
        // MySQL's reported length if it diverges or we fear it may diverge.
        if cur.chars().count() < len {
            // When `cur` has fewer than `len` characters it names an exact key
            // rather than a truncation, and the anchor would skip every key extending
            // it. The walk instead steps just past that one key, so extensions of
            // `cur` become prefixes of their own.
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
    async fn test_case_insensitive_prefix_traversal() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_case_insensitive";
        let keys = ["Aa", "ab", "b", "Bb", "bbb", "C"];
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        // Although the sorting is case-insensitive the values returned by mysql are not normalized, so
        // we need to use the correct character representation here to get the tests to pass.
        assert_eq!(first(p, "", None, 1).await, some("A"));
        assert_eq!(next(p, "A", None, 1).await, some("b"));
        assert_eq!(next(p, "b", None, 1).await, some("C"));
        assert_eq!(next(p, "C", None, 1).await, None);

        assert_eq!(first(p, "A", Some("b"), 2).await, some("Aa"));
        assert_eq!(next(p, "Aa", Some("b"), 2).await, some("ab"));
        assert_eq!(next(p, "ab", Some("b"), 2).await, None);

        // Stepping past the exact key "b" lands on Bb, whose prefix then
        // subsumes bbb under this case-insensitive collation.
        assert_eq!(first(p, "b", Some("C"), 2).await, some("b"));
        assert_eq!(next(p, "b", Some("C"), 2).await, some("Bb"));
        assert_eq!(next(p, "Bb", Some("C"), 2).await, None);

        assert_eq!(first(p, "C", None, 2).await, some("C"));
        assert_eq!(next(p, "C", None, 2).await, None);

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_wild_card_char_in_data() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_wildcard_test";
        // Keys are a_1, a\2, a\3, a%4. The utf8mb4_0900_ai_ci collation
        // orders them a_1 < a\2 < a\3 < a%4.
        let keys = ["a_1", "a\\2", "a\\3", "a%4"];
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        assert_eq!(first(p, "", None, 1).await, some("a"));
        assert_eq!(next(p, "a", None, 1).await, None);
        assert_eq!(first(p, "a", None, 2).await, some("a_"));
        assert_eq!(next(p, "a_", None, 2).await, some("a\\"));
        assert_eq!(next(p, "a\\", None, 2).await, some("a%"));
        assert_eq!(next(p, "a%", None, 2).await, None);

        // Range bounds that are themselves wildcard characters.
        assert_eq!(first(p, "a_", Some("a\\"), 3).await, some("a_1"));
        assert_eq!(next(p, "a_1", Some("a\\"), 3).await, None);
        assert_eq!(first(p, "a\\", Some("a%"), 3).await, some("a\\2"));
        assert_eq!(next(p, "a\\2", Some("a%"), 3).await, some("a\\3"));
        assert_eq!(next(p, "a\\3", Some("a%"), 3).await, None);
        assert_eq!(first(p, "a%", None, 3).await, some("a%4"));
        assert_eq!(next(p, "a%4", None, 3).await, None);

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
        // utf8mb4_0900_ai_ci orders symbols before letters and Han after
        // Latin: 😀 < 😀😀 < 😀a < a < a😀 < 日本 < 日本語.
        let keys = ["a", "a😀", "😀", "😀a", "😀😀", "日本", "日本語"];
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &keys).await?;

        let p = &mut KeyProber::new(&mut conn, table, "id");
        // Prefix lengths count characters, not bytes: a one-char prefix of a
        // four-byte emoji is the whole emoji, never a broken fragment.
        assert_eq!(first(p, "", None, 1).await, some("😀"));
        assert_eq!(next(p, "😀", None, 1).await, some("a"));
        assert_eq!(next(p, "a", None, 1).await, some("日"));
        assert_eq!(next(p, "日", None, 1).await, None);

        // Both walk modes work mid-multibyte: the exact-key step past 😀 and
        // 日本, and the anchor past everything sharing 😀😀.
        assert_eq!(next(p, "😀", None, 2).await, some("😀😀"));
        assert_eq!(next(p, "😀😀", None, 2).await, some("😀a"));
        assert_eq!(next(p, "日本", None, 3).await, some("日本語"));

        drop_db(&mut conn, DB).await?;
        conn.disconnect().await?;
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_live_mysql_ulid_pk() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_ulid_test";
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
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &ids).await?;
        let mut prober = KeyProber::new(&mut conn, table, "id");

        // Every key shares the timestamp prefix, so short prefixes cannot
        // split the key space at all.
        assert_eq!(first(&mut prober, "", None, 1).await, some("0"));
        assert_eq!(next(&mut prober, "0", None, 1).await, None);
        assert_eq!(next(&mut prober, "01J8ZXABCD", None, 10).await, None);

        // One character past the shared prefix distinguishes the keys.
        let walked = walk_prefixes(&mut prober, 11).await?;
        let expected: Vec<String> = (0..32)
            .map(|i| format!("01J8ZXABCD{}", char::from(CROCKFORD[i])))
            .collect();
        assert_eq!(walked, expected);

        let all = prober.estimate_range_rows("", None).await?;
        assert!(all > 0, "all={all}");

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
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &ids).await?;
        let mut p = KeyProber::new(&mut conn, table, "id");

        // Lowercase hex order matches byte order under this collation.
        assert_eq!(first(&mut p, "", None, 36).await, ids.iter().min().cloned());

        let walked = walk_prefixes(&mut p, 1).await?;
        let expected: BTreeSet<String> = ids.iter().map(|id| id[..1].to_string()).collect();
        assert_eq!(walked.iter().cloned().collect::<BTreeSet<_>>(), expected);

        let all = p.estimate_range_rows("", None).await?;
        assert!(all > 0, "all={all}");

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
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &ids).await?;

        // The collation dictates both the visit order and how short keys
        // interleave with their extensions, so assert the property that
        // matters instead of exact prefixes: the walked prefixes are range
        // boundaries that partition the table, every key falls in exactly
        // one interval. The server does the interval counting, under the
        // column's own collation.
        for len in [1, 2] {
            let walked =
                walk_prefixes(&mut KeyProber::new(&mut conn, table.clone(), "id"), len).await?;
            let mut total = 0;
            for (i, lo) in walked.iter().enumerate() {
                total += count_range(&mut conn, DB, lo, walked.get(i + 1)).await?;
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
        let t_ci = setup_table(&mut conn, CI_DB, "utf8mb4_0900_ai_ci", &ci_keys).await?;
        let mut prober = KeyProber::new(&mut conn, t_ci, "id");
        // 'A' anchors past 'apricot' too: LIKE is case-insensitive here, so a
        // returned prefix covers every case variant of it.
        assert_eq!(walk_prefixes(&mut prober, 1).await?, ["A", "b", "C"]);
        assert_eq!(walk_prefixes(&mut prober, 32).await?, ci_keys);

        // Binary collation: case variants coexist and order by byte value.
        let bin_keys = ["ABC", "ABD", "abc", "abd"];
        let t_bin = setup_table(&mut conn, BIN_DB, "utf8mb4_bin", &bin_keys).await?;
        let mut prober = KeyProber::new(&mut conn, t_bin, "id");
        // Uppercase sorts before lowercase in byte order, and case variants
        // are distinct prefixes.
        assert_eq!(walk_prefixes(&mut prober, 1).await?, ["A", "a"]);
        assert_eq!(walk_prefixes(&mut prober, 32).await?, bin_keys);

        drop_db(&mut conn, CI_DB).await?;
        drop_db(&mut conn, BIN_DB).await?;
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
                 COLLATE utf8mb4_0900_ai_ci PRIMARY KEY NOT NULL) \
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
    async fn test_probe_sargability() -> Result<(), anyhow::Error> {
        let Some(mut conn) = connect().await? else {
            return Ok(());
        };
        const DB: &str = "mz_probe_sargable_test";
        let ids: Vec<String> = (0..1000).map(|i| format!("a{i:05}")).collect();
        let table = setup_table(&mut conn, DB, "utf8mb4_0900_ai_ci", &ids).await?;

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
        let got = first(
            &mut KeyProber::new(&mut conn, table.clone(), "id"),
            "a00500",
            None,
            6,
        )
        .await;
        let reads = handler_reads(&mut conn).await? - before;
        assert_eq!(got, some("a00500"));
        assert!(reads < 50, "first_prefix reads={reads}");

        let before = handler_reads(&mut conn).await?;
        let got = next(
            &mut KeyProber::new(&mut conn, table.clone(), "id"),
            "a00500",
            None,
            6,
        )
        .await;
        let reads = handler_reads(&mut conn).await? - before;
        assert_eq!(got, some("a00501"));
        assert!(reads < 50, "next_prefix anchor reads={reads}");

        let before = handler_reads(&mut conn).await?;
        let got = next(
            &mut KeyProber::new(&mut conn, table.clone(), "id"),
            "a0",
            None,
            6,
        )
        .await;
        let reads = handler_reads(&mut conn).await? - before;
        assert_eq!(got, some("a00000"));
        assert!(reads < 50, "next_prefix exact-key step reads={reads}");

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

    /// Number of keys in `[lo, hi)` of `db`'s table, counted by the server so
    /// the comparison happens under the column's collation.
    async fn count_range(
        conn: &mut mysql_async::Conn,
        db: &str,
        lo: &str,
        hi: Option<&String>,
    ) -> Result<u64, anyhow::Error> {
        let mut clause = "id >= ?".to_string();
        let mut params: Vec<Value> = vec![lo.into()];
        if let Some(hi) = hi {
            clause.push_str(" AND id < ?");
            params.push(hi.as_str().into());
        }
        let count: Option<u64> = conn
            .exec_first(
                format!("SELECT COUNT(*) FROM {db}.t WHERE {clause}"),
                Params::Positional(params),
            )
            .await?;
        Ok(count.expect("COUNT returns a row"))
    }

    /// Sum of this session's `Handler_read_*` counters: how many index or row
    /// read operations the connection has performed so far.
    async fn handler_reads(conn: &mut mysql_async::Conn) -> Result<u64, anyhow::Error> {
        let rows: Vec<(String, String)> = conn
            .exec("SHOW SESSION STATUS LIKE 'Handler_read%'", ())
            .await?;
        Ok(rows.into_iter().map(|(_, v)| v.parse().unwrap_or(0)).sum())
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
}
