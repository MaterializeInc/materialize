// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
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

use crate::ast::display::{self, AstDisplay, AstFormatter};
use crate::keywords::Keyword;

/// An identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident(String);

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
            .map(|ch| (ch >= 'a' && ch <= 'z') || (ch == '_'))
            .unwrap_or(false)
            && chars.all(|ch| (ch >= 'a' && ch <= 'z') || (ch == '_') || (ch >= '0' && ch <= '9'))
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

/// More-or-less a direct translation of the Postgres function for doing the same thing:
///
///   https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/ruleutils.c#L10730-L10812
///
/// Quotation is forced when printing in Stable mode.
impl AstDisplay for Ident {
    fn fmt(&self, f: &mut AstFormatter) {
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

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName(pub Vec<Ident>);

impl ObjectName {
    /// Creates an `ObjectName` with a single [`Ident`], i.e. it appears as
    /// "unqualified".
    pub fn unqualified(n: &str) -> ObjectName {
        ObjectName(vec![Ident::new(n)])
    }
}

impl AstDisplay for ObjectName {
    fn fmt(&self, f: &mut AstFormatter) {
        display::separated(&self.0, ".").fmt(f);
    }
}
impl_display!(ObjectName);

impl AstDisplay for &ObjectName {
    fn fmt(&self, f: &mut AstFormatter) {
        display::separated(&self.0, ".").fmt(f);
    }
}
