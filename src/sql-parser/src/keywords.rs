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
use std::str::FromStr;

use ore::ascii::UncasedStr;

use crate::ast::Ident;

// The `Keyword` type and the keyword constants are automatically generated from
// the list in keywords.txt by the crate's build script.
//
// We go to the trouble of code generation primarily to create a "perfect hash
// function" at compile time via the phf crate, which enables very fast,
// case-insensitive keyword parsing. From there it's easy to generate a few
// more convenience functions and accessors.
//
// If the only keywords were `Insert` and `Select`, we'd generate the following
// code:
//
//     pub enum Keyword {
//         Insert,
//         Select,
//     }
//
//     pub const INSERT: Keyword = Keyword::Insert;
//     pub const SELECT: Keyword = Keyword::Select;
//
//     impl Keyword {
//         pub fn as_str(&self) -> &'static str {
//             match self {
//                 Keyword::Insert => "INSERT",
//                 Keyword::Select => "SELECT",
//             }
//         }
//         pub fn reserve_type(&self) -> ReserveType {
//             match self {
//                 Keyword::Insert => ReserveType::Unreserved,
//                 Keyword::Select => ReserveType::Reserved,
//             }
//         }
//     }
//
//     static KEYWORDS: phf::Map<&'static UncasedStr, Keyword> = { /* ... */ };
//
include!(concat!(env!("OUT_DIR"), "/keywords.rs"));

pub enum ReserveType {
    Reserved,
    ColumnName,
    TableAlias,
    Unreserved,
}

impl Keyword {
    pub fn into_ident(self) -> Ident {
        Ident::new(self.as_str().to_lowercase())
    }

    /// Reports whether this keyword requires quoting when used as an
    /// identifier in any context.
    ///
    /// The only exception to the rule is when the keyword follows `AS` in a
    /// column or table alias.
    pub fn is_reserved(self) -> bool {
        matches!(self.reserve_type(), ReserveType::Reserved)
    }

    /// Reports whether this keyword requires quoting when used as a table
    /// alias.
    ///
    /// Note that this rule is only applies when the table alias is "bare";
    /// i.e., when the table alias is not preceded by `AS`.
    ///
    /// Ensures that `FROM <table_name> <table_alias>` can be parsed
    /// unambiguously.
    pub fn is_reserved_in_table_alias(self) -> bool {
        matches!(
            self.reserve_type(),
            ReserveType::TableAlias | ReserveType::Reserved
        )
    }

    /// Reports whether this keyword requires quoting when used as a column
    /// alias.
    ///
    ///
    /// Note that this rule is only applies when the column alias is "bare";
    /// i.e., when the column alias is not preceded by `AS`.
    ///
    /// Ensures that `SELECT <column_name> <column_alias>` can be parsed
    /// unambiguously.
    pub fn is_reserved_in_column_alias(self) -> bool {
        matches!(
            self.reserve_type(),
            ReserveType::ColumnName | ReserveType::Reserved
        )
    }

    /// Reports whether a keyword is considered reserved in any context:
    /// either in table aliases, column aliases, or in all contexts.
    pub fn is_sometimes_reserved(self) -> bool {
        !matches!(self.reserve_type(), ReserveType::Unreserved)
    }
}

impl FromStr for Keyword {
    type Err = ();

    fn from_str(s: &str) -> Result<Keyword, ()> {
        match KEYWORDS.get(UncasedStr::new(s)) {
            Some(kw) => Ok(*kw),
            None => Err(()),
        }
    }
}

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
