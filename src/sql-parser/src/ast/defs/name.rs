// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
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

use mz_ore::str::StrExt;
use mz_sql_lexer::keywords::Keyword;
use mz_sql_lexer::lexer::{IdentString, MAX_IDENTIFIER_LENGTH};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{AstInfo, QualifiedReplica};

/// An identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Ident(pub(crate) String);

impl Ident {
    /// Maximum length of an identifier in Materialize.
    pub const MAX_LENGTH: usize = MAX_IDENTIFIER_LENGTH;

    /// Create a new [`Ident`] with the given value, checking our invariants.
    ///
    /// # Examples
    ///
    /// ```
    /// use mz_sql_parser::ast::Ident;
    ///
    /// let id = Ident::new("hello_world").unwrap();
    /// assert_eq!(id.as_str(), "hello_world");
    ///
    /// let too_long = "I am a very long identifier that is more than 255 bytes long which is the max length for idents.\
    /// 😊😁😅😂😬🍻😮‍💨😮🗽🛰️🌈😊😁😅😂😬🍻😮‍💨😮🗽🛰️🌈😊😁😅😂😬🍻😮‍💨😮🗽🛰️🌈";
    /// assert_eq!(too_long.len(), 258);
    ///
    /// let too_long_id = Ident::new(too_long);
    /// assert!(too_long_id.is_err());
    ///
    /// let invalid_name_dot = Ident::new(".");
    /// assert!(invalid_name_dot.is_err());
    ///
    /// let invalid_name_dot_dot = Ident::new("..");
    /// assert!(invalid_name_dot_dot.is_err());
    /// ```
    ///
    pub fn new<S>(s: S) -> Result<Self, IdentError>
    where
        S: TryInto<IdentString>,
        <S as TryInto<IdentString>>::Error: fmt::Display,
    {
        let s = s
            .try_into()
            .map_err(|e| IdentError::TooLong(e.to_string()))?;

        if &*s == "." || &*s == ".." {
            return Err(IdentError::Invalid(s.into_inner()));
        }

        Ok(Ident(s.into_inner()))
    }

    /// Create a new [`Ident`] modifying the given value as necessary to meet our invariants.
    ///
    /// # Examples
    ///
    /// ```
    /// use mz_sql_parser::ast::Ident;
    ///
    /// let too_long = "🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\
    /// 🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵\
    /// 🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴";
    ///
    /// let id = Ident::new_lossy(too_long);
    ///
    /// // `new_lossy` will truncate the provided string, since it's too long. Note the missing
    /// // `🔴` characters.
    /// assert_eq!(id.as_str(), "🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵");
    /// ```
    pub fn new_lossy<S: Into<String>>(value: S) -> Self {
        let s: String = value.into();
        if s.len() <= Self::MAX_LENGTH {
            return Ident(s);
        }

        let mut byte_length = 0;
        let s_truncated = s
            .chars()
            .take_while(|c| {
                byte_length += c.len_utf8();
                byte_length <= Self::MAX_LENGTH
            })
            .collect();

        Ident(s_truncated)
    }

    /// Create a new [`Ident`] _without checking any of our invariants_.
    ///
    /// NOTE: Generally you __should not use this function__! If you're trying to create an
    /// [`Ident`] from a `&'static str` you know is valid, use the [`ident!`] macro. For all other
    /// use cases, see [`Ident::new`] which correctly checks our invariants.
    ///
    /// [`ident!`]: [`mz_sql_parser::ident`]
    pub fn new_unchecked<S: Into<String>>(value: S) -> Self {
        let s = value.into();
        mz_ore::soft_assert_no_log!(s.len() <= Self::MAX_LENGTH);

        Ident(s)
    }

    /// Generate a valid [`Ident`] with the provided `prefix` and `suffix`.
    ///
    /// # Examples
    ///
    /// ```
    /// use mz_sql_parser::ast::{Ident, IdentError};
    ///
    /// let good_id =
    ///   Ident::try_generate_name("hello", "_world", |_| Ok::<_, IdentError>(true)).unwrap();
    /// assert_eq!(good_id.as_str(), "hello_world");
    ///
    /// // Return invalid once.
    /// let mut attempts = 0;
    /// let one_failure = Ident::try_generate_name("hello", "_world", |_candidate| {
    ///     if attempts == 0 {
    ///         attempts += 1;
    ///         Ok::<_, IdentError>(false)
    ///     } else {
    ///         Ok(true)
    ///     }
    /// })
    /// .unwrap();
    ///
    /// // We "hello_world" was invalid, so we appended "_1".
    /// assert_eq!(one_failure.as_str(), "hello_world_1");
    /// ```
    pub fn try_generate_name<P, S, F, E>(prefix: P, suffix: S, mut is_valid: F) -> Result<Self, E>
    where
        P: Into<String>,
        S: Into<String>,
        E: From<IdentError>,
        F: FnMut(&Ident) -> Result<bool, E>,
    {
        const MAX_ATTEMPTS: usize = 1000;

        let prefix: String = prefix.into();
        let suffix: String = suffix.into();

        // First just append the prefix and suffix.
        let mut candidate = Ident(prefix.clone());
        candidate.append_lossy(suffix.clone());
        if is_valid(&candidate)? {
            return Ok(candidate);
        }

        // Otherwise, append a number to the back.
        for i in 1..MAX_ATTEMPTS {
            let mut candidate = Ident(prefix.clone());
            candidate.append_lossy(format!("{suffix}_{i}"));

            if is_valid(&candidate)? {
                return Ok(candidate);
            }
        }

        // Couldn't find any valid name!
        Err(E::from(IdentError::FailedToGenerate {
            prefix,
            suffix,
            attempts: MAX_ATTEMPTS,
        }))
    }

    /// Append the provided `suffix`, truncating `self` as necessary to satisfy our invariants.
    ///
    /// Note: We soft-assert that the provided `suffix` is not too long, if it is, we'll
    /// truncate it.
    ///
    /// # Examples
    ///
    /// ```
    /// use mz_sql_parser::{
    ///     ident,
    ///     ast::Ident,
    /// };
    ///
    /// let mut id = ident!("🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵🔵");
    /// id.append_lossy("🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴");
    ///
    /// // We truncated the original ident, removing all '🔵' chars.
    /// assert_eq!(id.as_str(), "🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴");
    /// ```
    ///
    /// ### Too long suffix
    /// If the provided suffix is too long, we'll also truncate that.
    ///
    /// ```
    /// # mz_ore::assert::SOFT_ASSERTIONS.store(false, std::sync::atomic::Ordering::Relaxed);
    /// use mz_sql_parser::{
    ///     ident,
    ///     ast::Ident,
    /// };
    ///
    /// let mut stem = ident!("hello___world");
    ///
    /// let too_long_suffix = "\
    /// 🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\
    /// 🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\
    /// 🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\
    /// 🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🔵🔵\
    /// ";
    ///
    /// stem.append_lossy(too_long_suffix);
    ///
    /// // Notice the "hello___world" stem got truncated, as did the "🔵🔵" characters from the suffix.
    /// let result = "hello___wor\
    /// 🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\
    /// 🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\
    /// 🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\
    /// 🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢\
    /// ";
    /// assert_eq!(stem.as_str(), result);
    /// ```
    pub fn append_lossy<S: Into<String>>(&mut self, suffix: S) {
        // Make sure our suffix at least leaves a bit of room for the original ident.
        const MAX_SUFFIX_LENGTH: usize = Ident::MAX_LENGTH - 8;

        let mut suffix: String = suffix.into();
        mz_ore::soft_assert_or_log!(suffix.len() <= MAX_SUFFIX_LENGTH, "suffix too long");

        // Truncate the suffix as necessary.
        if suffix.len() > MAX_SUFFIX_LENGTH {
            let mut byte_length = 0;
            suffix = suffix
                .chars()
                .take_while(|c| {
                    byte_length += c.len_utf8();
                    byte_length <= MAX_SUFFIX_LENGTH
                })
                .collect();
        }

        // Truncate ourselves as necessary.
        let available_length = Ident::MAX_LENGTH - suffix.len();
        if self.0.len() > available_length {
            let mut byte_length = 0;
            self.0 = self
                .0
                .chars()
                .take_while(|c| {
                    byte_length += c.len_utf8();
                    byte_length <= available_length
                })
                .collect();
        }

        // Append the suffix.
        self.0.push_str(&suffix);
    }

    /// An identifier can be printed in bare mode if
    ///  * it matches the regex `[a-z_][a-z0-9_]*` and
    ///  * it is not a "reserved keyword."
    pub fn can_be_printed_bare(&self) -> bool {
        let mut chars = self.0.chars();
        chars
            .next()
            .map(|ch| matches!(ch, 'a'..='z' | '_'))
            .unwrap_or(false)
            && chars.all(|ch| matches!(ch, 'a'..='z' | '0'..='9' | '_'))
            && !self
                .as_keyword()
                .map(Keyword::is_sometimes_reserved)
                .unwrap_or(false)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_keyword(&self) -> Option<Keyword> {
        self.0.parse().ok()
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// More-or-less a direct translation of the Postgres function for doing the same thing:
///
///   <https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/ruleutils.c#L10730-L10812>
///
/// Quotation is forced when printing in Stable mode.
impl AstDisplay for Ident {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        if self.can_be_printed_bare() && !f.stable() {
            f.write_str(&self.0);
        } else {
            f.write_str("\"");
            for ch in self.0.chars() {
                // Double up on double-quotes.
                if ch == '"' {
                    f.write_str("\"");
                }
                f.write_str(ch);
            }
            f.write_str("\"");
        }
    }
}
impl_display!(Ident);

#[derive(Clone, Debug, thiserror::Error)]
pub enum IdentError {
    #[error("identifier too long (len: {}, max: {}, value: {})", .0.len(), Ident::MAX_LENGTH, .0.quoted())]
    TooLong(String),
    #[error(
        "failed to generate identifier with prefix '{prefix}' and suffix '{suffix}' after {attempts} attempts"
    )]
    FailedToGenerate {
        prefix: String,
        suffix: String,
        attempts: usize,
    },

    #[error("invalid identifier: {}", .0.quoted())]
    Invalid(String),
}

/// A name of a table, view, custom type, etc. that lives in a schema, possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UnresolvedItemName(pub Vec<Ident>);

pub enum CatalogName {
    ItemName(Vec<Ident>),
    FuncName(Vec<Ident>),
}

impl UnresolvedItemName {
    /// Creates an `ItemName` with a single [`Ident`], i.e. it appears as
    /// "unqualified".
    pub fn unqualified(ident: Ident) -> UnresolvedItemName {
        UnresolvedItemName(vec![ident])
    }

    /// Creates an `ItemName` with an [`Ident`] for each element of `n`.
    ///
    /// Panics if passed an in ineligible `&[&str]` whose length is 0 or greater
    /// than 3.
    pub fn qualified(n: &[Ident]) -> UnresolvedItemName {
        assert!(n.len() <= 3 && n.len() > 0);
        UnresolvedItemName(n.iter().cloned().collect::<Vec<_>>())
    }
}

impl AstDisplay for UnresolvedItemName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        display::separated(&self.0, ".").fmt(f);
    }
}
impl_display!(UnresolvedItemName);

impl AstDisplay for &UnresolvedItemName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        display::separated(&self.0, ".").fmt(f);
    }
}

/// A name of a schema
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnresolvedSchemaName(pub Vec<Ident>);

impl AstDisplay for UnresolvedSchemaName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        display::separated(&self.0, ".").fmt(f);
    }
}
impl_display!(UnresolvedSchemaName);

/// A name of a database
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnresolvedDatabaseName(pub Ident);

impl AstDisplay for UnresolvedDatabaseName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_node(&self.0);
    }
}
impl_display!(UnresolvedDatabaseName);

// The name of an item not yet created during name resolution, which should be
// resolveable as an item name later.
#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub enum DeferredItemName<T: AstInfo> {
    Named(T::ItemName),
    Deferred(UnresolvedItemName),
}

impl<T: AstInfo> AstDisplay for DeferredItemName<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            DeferredItemName::Named(o) => f.write_node(o),
            DeferredItemName::Deferred(o) => f.write_node(o),
        }
    }
}
impl_display_t!(DeferredItemName);

#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub enum UnresolvedObjectName {
    Cluster(Ident),
    ClusterReplica(QualifiedReplica),
    Database(UnresolvedDatabaseName),
    Schema(UnresolvedSchemaName),
    Role(Ident),
    Item(UnresolvedItemName),
    NetworkPolicy(Ident),
}

impl AstDisplay for UnresolvedObjectName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            UnresolvedObjectName::Cluster(n) => f.write_node(n),
            UnresolvedObjectName::ClusterReplica(n) => f.write_node(n),
            UnresolvedObjectName::Database(n) => f.write_node(n),
            UnresolvedObjectName::Schema(n) => f.write_node(n),
            UnresolvedObjectName::Role(n) => f.write_node(n),
            UnresolvedObjectName::Item(n) => f.write_node(n),
            UnresolvedObjectName::NetworkPolicy(n) => f.write_node(n),
        }
    }
}
impl_display!(UnresolvedObjectName);
