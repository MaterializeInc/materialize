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

use std::fmt;

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::ast::{AstInfo, QualifiedReplica};
use crate::keywords::Keyword;

/// An identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ident(pub(crate) String);

impl Ident {
    /// Create a new identifier with the given value.
    pub fn new<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident(value.into())
    }

    /// An identifier can be printed in bare mode if
    ///  * it matches the regex [a-z_][a-z0-9_]* and
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

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident(value.to_string())
    }
}

impl From<String> for Ident {
    fn from(value: String) -> Self {
        Ident(value)
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

/// A name of a table, view, custom type, etc. that lives in a schema, possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnresolvedItemName(pub Vec<Ident>);

pub enum CatalogName {
    ItemName(Vec<Ident>),
    FuncName(Vec<Ident>),
}

impl UnresolvedItemName {
    /// Creates an `ItemName` with a single [`Ident`], i.e. it appears as
    /// "unqualified".
    pub fn unqualified(n: &str) -> UnresolvedItemName {
        UnresolvedItemName(vec![Ident::new(n)])
    }

    /// Creates an `ItemName` with an [`Ident`] for each element of `n`.
    ///
    /// Panics if passed an in ineligible `&[&str]` whose length is 0 or greater
    /// than 3.
    pub fn qualified(n: &[&str]) -> UnresolvedItemName {
        assert!(n.len() <= 3 && n.len() > 0);
        UnresolvedItemName(n.iter().map(|n| (*n).into()).collect::<Vec<_>>())
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
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
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
        }
    }
}
impl_display!(UnresolvedObjectName);
