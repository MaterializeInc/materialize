// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::disallowed_methods)]

use std::borrow::Cow;
use std::fmt::{self, Write};
use std::ops::{Add, AddAssign};

use mz_sql_parser::ast::display::{AstDisplay, escaped_string_literal};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, GenericClient, Row, SimpleQueryMessage, SimpleQueryRow, Statement};

use crate::PostgresError;

/// A composable SQL query string.
///
/// Use [`crate::sql!`] for static SQL fragments and [`Sql::ident`]/[`Sql::literal`] for
/// dynamic values. This mirrors psycopg's split between trusted SQL text and escaped
/// values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sql(Cow<'static, str>);

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlTemplateError {
    InvalidOpenBrace,
    InvalidCloseBrace,
}

#[doc(hidden)]
pub const fn sql_template_placeholder_count(template: &str) -> Result<usize, SqlTemplateError> {
    let bytes = template.as_bytes();
    let mut i = 0;
    let mut count = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'{' => {
                if i + 1 >= bytes.len() {
                    return Err(SqlTemplateError::InvalidOpenBrace);
                }
                match bytes[i + 1] {
                    b'{' => i += 2,
                    b'}' => {
                        count += 1;
                        i += 2;
                    }
                    _ => return Err(SqlTemplateError::InvalidOpenBrace),
                }
            }
            b'}' => {
                if i + 1 >= bytes.len() {
                    return Err(SqlTemplateError::InvalidCloseBrace);
                }
                match bytes[i + 1] {
                    b'}' => i += 2,
                    _ => return Err(SqlTemplateError::InvalidCloseBrace),
                }
            }
            _ => i += 1,
        }
    }

    Ok(count)
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlFormatError {
    #[error("SQL format string contains an invalid '{{' sequence")]
    InvalidOpenBrace,
    #[error("SQL format string contains an invalid '}}' sequence")]
    InvalidCloseBrace,
    #[error("SQL format string expected more arguments")]
    MissingArgument,
    #[error("SQL format string received too many arguments")]
    ExtraArgument,
}

impl Sql {
    /// Creates a SQL fragment from a static SQL string.
    pub fn new(sql: &'static str) -> Self {
        Self(Cow::Borrowed(sql))
    }

    /// Creates a SQL fragment by escaping a SQL identifier.
    pub fn ident(ident: &str) -> Self {
        // PostgreSQL identifiers are escaped by surrounding with double quotes
        // and doubling any embedded double quotes.
        let mut out = String::with_capacity(ident.len() + 2);
        out.push('"');
        for ch in ident.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
        Self(Cow::Owned(out))
    }

    /// Creates a SQL fragment by escaping a SQL literal.
    pub fn literal(literal: &str) -> Self {
        Self(Cow::Owned(
            escaped_string_literal(literal).to_ast_string_simple(),
        ))
    }

    /// Creates a SQL fragment for a PostgreSQL positional parameter (e.g. `$1`).
    pub fn param(index: usize) -> Self {
        let mut out = String::new();
        out.push('$');
        let _ = write!(out, "{index}");
        Self(Cow::Owned(out))
    }

    /// Joins SQL fragments with a static separator.
    pub fn join(parts: impl IntoIterator<Item = Sql>, separator: &'static str) -> Self {
        let mut iter = parts.into_iter();
        let Some(first) = iter.next() else {
            return Self(Cow::Borrowed(""));
        };

        let mut out = first.0;
        for part in iter {
            out.to_mut().push_str(separator);
            out.to_mut().push_str(part.as_str());
        }
        Self(out)
    }

    /// Formats this SQL fragment by replacing each `{}` with the next SQL argument.
    ///
    /// Use `{{` and `}}` to escape literal braces.
    pub fn format(self, args: impl IntoIterator<Item = Sql>) -> Result<Self, SqlFormatError> {
        let mut args = args.into_iter();
        let mut out = String::with_capacity(self.0.len());
        let mut chars = self.0.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '{' => match chars.peek() {
                    Some('{') => {
                        chars.next();
                        out.push('{');
                    }
                    Some('}') => {
                        chars.next();
                        let arg = args.next().ok_or(SqlFormatError::MissingArgument)?;
                        out.push_str(arg.as_str());
                    }
                    _ => return Err(SqlFormatError::InvalidOpenBrace),
                },
                '}' => match chars.peek() {
                    Some('}') => {
                        chars.next();
                        out.push('}');
                    }
                    _ => return Err(SqlFormatError::InvalidCloseBrace),
                },
                _ => out.push(ch),
            }
        }

        if args.next().is_some() {
            return Err(SqlFormatError::ExtraArgument);
        }
        Ok(Sql(Cow::Owned(out)))
    }

    #[doc(hidden)]
    pub fn format_unchecked(self, args: impl IntoIterator<Item = Sql>) -> Self {
        let mut args = args.into_iter();
        let mut out = String::with_capacity(self.0.len());
        let mut chars = self.0.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '{' => match chars.next().expect("validated in sql! macro") {
                    '{' => out.push('{'),
                    '}' => {
                        let arg = args.next().expect("validated in sql! macro");
                        out.push_str(arg.as_str());
                    }
                    _ => unreachable!("validated in sql! macro"),
                },
                '}' => match chars.next().expect("validated in sql! macro") {
                    '}' => out.push('}'),
                    _ => unreachable!("validated in sql! macro"),
                },
                _ => out.push(ch),
            }
        }
        debug_assert!(args.next().is_none(), "validated in sql! macro");
        Sql(Cow::Owned(out))
    }

    /// Returns the underlying SQL string.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    /// Consumes this value and returns the SQL string.
    pub fn into_string(self) -> String {
        self.0.into_owned()
    }
}

impl fmt::Display for Sql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Add for Sql {
    type Output = Sql;

    fn add(mut self, rhs: Sql) -> Self::Output {
        self += rhs;
        self
    }
}

impl AddAssign for Sql {
    fn add_assign(&mut self, rhs: Sql) {
        self.0.to_mut().push_str(rhs.as_str());
    }
}

macro_rules! impl_from_integer_for_sql {
    ($($t:ty),+ $(,)?) => {
        $(
            impl From<$t> for Sql {
                fn from(value: $t) -> Self {
                    Sql(Cow::Owned(value.to_string()))
                }
            }
        )+
    };
}

impl_from_integer_for_sql!(i16, i32, i64, isize, u16, u32, u64, usize);

impl From<tokio_postgres::types::PgLsn> for Sql {
    fn from(value: tokio_postgres::types::PgLsn) -> Self {
        Sql(Cow::Owned(value.to_string()))
    }
}

#[macro_export]
macro_rules! sql {
    ($template:literal $(,)?) => {
        $crate::Sql::new($template)
    };
    ($template:literal, $($arg:expr),+ $(,)?) => {{
        const __SQL_FORMAT_ARG_COUNT: usize = <[()]>::len(&[$($crate::sql!(@unit $arg)),*]);
        const __SQL_FORMAT_PLACEHOLDER_COUNT: usize =
            match $crate::query::sql_template_placeholder_count($template) {
                Ok(n) => n,
                Err($crate::query::SqlTemplateError::InvalidOpenBrace) => {
                    panic!("sql!: invalid '{{' in SQL template")
                }
                Err($crate::query::SqlTemplateError::InvalidCloseBrace) => {
                    panic!("sql!: invalid '}}' in SQL template")
                }
            };
        const _: () = {
            if __SQL_FORMAT_ARG_COUNT != __SQL_FORMAT_PLACEHOLDER_COUNT {
                panic!("sql!: placeholder count does not match arguments");
            }
        };

        $crate::Sql::new($template).format_unchecked([$($crate::Sql::from($arg)),*])
    }};
    (@unit $_arg:expr) => { () };
}

/// Runs the given query using the client and expects at most a single row to be returned.
pub async fn simple_query_opt(
    client: &Client,
    query: Sql,
) -> Result<Option<SimpleQueryRow>, PostgresError> {
    let result = simple_query(client, query).await?;
    let mut rows = result.into_iter().filter_map(|msg| match msg {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    });
    match (rows.next(), rows.next()) {
        (Some(row), None) => Ok(Some(row)),
        (None, None) => Ok(None),
        _ => Err(PostgresError::UnexpectedRow),
    }
}

/// Runs a simple query and returns all protocol messages.
pub async fn simple_query(
    client: &Client,
    query: Sql,
) -> Result<Vec<SimpleQueryMessage>, PostgresError> {
    Ok(client.simple_query(query.as_str()).await?)
}

/// Runs a query and returns all resulting rows.
pub async fn query<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, PostgresError> {
    Ok(client.query(query.as_str(), params).await?)
}

/// Runs a prepared query and returns all resulting rows.
pub async fn query_prepared<C: GenericClient + Sync>(
    client: &C,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, PostgresError> {
    Ok(client.query(statement, params).await?)
}

/// Runs a query and returns exactly one row.
pub async fn query_one<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Row, PostgresError> {
    Ok(client.query_one(query.as_str(), params).await?)
}

/// Runs a prepared query and returns exactly one row.
pub async fn query_one_prepared<C: GenericClient + Sync>(
    client: &C,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Row, PostgresError> {
    Ok(client.query_one(statement, params).await?)
}

/// Runs a query and returns at most one row.
pub async fn query_opt<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, PostgresError> {
    Ok(client.query_opt(query.as_str(), params).await?)
}

/// Runs a prepared query and returns at most one row.
pub async fn query_opt_prepared<C: GenericClient + Sync>(
    client: &C,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, PostgresError> {
    Ok(client.query_opt(statement, params).await?)
}

/// Runs a query and returns the number of affected rows.
pub async fn execute<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, PostgresError> {
    Ok(client.execute(query.as_str(), params).await?)
}

/// Runs a prepared query and returns the number of affected rows.
pub async fn execute_prepared<C: GenericClient + Sync>(
    client: &C,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, PostgresError> {
    Ok(client.execute(statement, params).await?)
}

/// Runs one or more SQL statements with no returned rows.
pub async fn batch_execute<C: GenericClient + Sync>(
    client: &C,
    query: Sql,
) -> Result<(), PostgresError> {
    Ok(client.batch_execute(query.as_str()).await?)
}

#[cfg(test)]
mod tests {
    use super::{Sql, SqlFormatError};

    #[mz_ore::test]
    fn sql_identifier_escaping() {
        assert_eq!(Sql::ident("a").as_str(), "\"a\"");
        assert_eq!(Sql::ident("a\"b").as_str(), "\"a\"\"b\"");
    }

    #[mz_ore::test]
    fn sql_literal_escaping() {
        assert_eq!(Sql::literal("a").as_str(), "'a'");
        assert_eq!(Sql::literal("a'b").as_str(), "'a''b'");
    }

    #[mz_ore::test]
    fn sql_format_composes_fragments() {
        let query = Sql::new("SELECT * FROM {} WHERE col = {}")
            .format([Sql::ident("my_table"), Sql::literal("v")])
            .expect("valid template");
        assert_eq!(query.as_str(), "SELECT * FROM \"my_table\" WHERE col = 'v'");
    }

    #[mz_ore::test]
    fn sql_format_errors_on_invalid_placeholders() {
        let err = Sql::new("SELECT {x}")
            .format([Sql::ident("t")])
            .expect_err("invalid format");
        assert_eq!(err, SqlFormatError::InvalidOpenBrace);
    }

    #[mz_ore::test]
    fn sql_macro_composes_fragments() {
        let query = crate::sql!(
            "SELECT * FROM {} WHERE col = {}",
            Sql::ident("my_table"),
            Sql::literal("v")
        );
        assert_eq!(query.as_str(), "SELECT * FROM \"my_table\" WHERE col = 'v'");
    }

    #[mz_ore::test]
    fn sql_macro_escaped_braces() {
        let query = crate::sql!("SELECT '{{}}' AS braces, {} AS t", Sql::ident("col"));
        assert_eq!(query.as_str(), "SELECT '{}' AS braces, \"col\" AS t");
    }

    #[mz_ore::test]
    fn sql_macro_static_literal() {
        let query = crate::sql!("SELECT '{not_a_placeholder}'");
        assert_eq!(query.as_str(), "SELECT '{not_a_placeholder}'");
    }
}
