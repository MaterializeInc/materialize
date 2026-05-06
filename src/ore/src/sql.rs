// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Composable, escape-aware SQL fragment building.
//!
//! Use [`crate::sql!`]/[`Sql::new`] for trusted SQL text and
//! [`Sql::ident`]/[`Sql::literal`] to escape values from untrusted sources.
//! The escape rules are PostgreSQL's.

use std::borrow::Cow;
use std::fmt::{self, Write};
use std::ops::{Add, AddAssign};

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

/// Errors produced by [`Sql::format`].
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SqlFormatError {
    /// The template contained a `{` that was not part of a `{}` placeholder
    /// or `{{` escape.
    #[error("SQL format string contains an invalid '{{' sequence")]
    InvalidOpenBrace,
    /// The template contained a `}` that was not part of a `{}` placeholder
    /// or `}}` escape.
    #[error("SQL format string contains an invalid '}}' sequence")]
    InvalidCloseBrace,
    /// The template had more `{}` placeholders than supplied arguments.
    #[error("SQL format string expected more arguments")]
    MissingArgument,
    /// The template had fewer `{}` placeholders than supplied arguments.
    #[error("SQL format string received too many arguments")]
    ExtraArgument,
}

impl Sql {
    /// Creates a SQL fragment from a static SQL string.
    pub fn new(sql: &'static str) -> Self {
        Self(Cow::Borrowed(sql))
    }

    /// Creates a SQL fragment from an arbitrary owned string, trusting the caller
    /// that it is safe SQL. Prefer [`Sql::ident`] or [`Sql::literal`] when handling
    /// untrusted input.
    pub fn raw_unchecked(sql: String) -> Self {
        Self(Cow::Owned(sql))
    }

    /// Wraps the body of an entire SQL request received over the external HTTP
    /// or WebSocket SQL API (`/api/sql`).
    ///
    /// **Do not use this constructor anywhere else.** It exists solely so that
    /// the [`serde::Deserialize`] impl on [`Sql`] — used to decode JSON-encoded
    /// HTTP/WebSocket SQL requests where the API explicitly accepts arbitrary
    /// caller-supplied SQL — has a path to reach the unsafe construction.
    /// Internal code must build SQL via [`Sql::new`], [`Sql::ident`],
    /// [`Sql::literal`], the [`crate::sql!`] macro, or — when the SQL is
    /// already trusted by other means — [`Sql::raw_unchecked`].
    pub fn trusted_external_request(sql: String) -> Self {
        Self(Cow::Owned(sql))
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
        // PostgreSQL string literals are escaped by surrounding with single
        // quotes and doubling any embedded single quotes.
        let mut out = String::with_capacity(literal.len() + 2);
        out.push('\'');
        for ch in literal.chars() {
            if ch == '\'' {
                out.push('\'');
            }
            out.push(ch);
        }
        out.push('\'');
        Self(Cow::Owned(out))
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
        if cfg!(debug_assertions) {
            assert!(args.next().is_none(), "validated in sql! macro");
        }
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

impl serde::Serialize for Sql {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Sql {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Deserialization is the trust boundary for SQL arriving over the
        // external `/api/sql` endpoint. Any other path that produces a `Sql`
        // value should use the safe constructors (`Sql::new`, `Sql::ident`,
        // `Sql::literal`, the `sql!` macro) or `Sql::raw_unchecked`.
        Ok(Sql::trusted_external_request(String::deserialize(
            deserializer,
        )?))
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

/// Builds a [`Sql`] fragment from a static template and SQL arguments.
///
/// The template uses `{}` placeholders for SQL arguments and `{{`/`}}` to
/// escape literal braces. Argument count is verified at compile time.
#[macro_export]
macro_rules! sql {
    ($template:literal $(,)?) => {
        $crate::sql::Sql::new($template)
    };
    ($template:literal, $($arg:expr),+ $(,)?) => {{
        const __SQL_FORMAT_ARG_COUNT: usize = <[()]>::len(&[$($crate::sql!(@unit $arg)),*]);
        const __SQL_FORMAT_PLACEHOLDER_COUNT: usize =
            match $crate::sql::sql_template_placeholder_count($template) {
                Ok(n) => n,
                Err($crate::sql::SqlTemplateError::InvalidOpenBrace) => {
                    panic!("sql!: invalid '{{' in SQL template")
                }
                Err($crate::sql::SqlTemplateError::InvalidCloseBrace) => {
                    panic!("sql!: invalid '}}' in SQL template")
                }
            };
        const _: () = {
            if __SQL_FORMAT_ARG_COUNT != __SQL_FORMAT_PLACEHOLDER_COUNT {
                panic!("sql!: placeholder count does not match arguments");
            }
        };

        $crate::sql::Sql::new($template).format_unchecked([$($crate::sql::Sql::from($arg)),*])
    }};
    (@unit $_arg:expr) => { () };
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
