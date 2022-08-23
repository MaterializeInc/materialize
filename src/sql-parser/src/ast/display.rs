// Copyright 2020 sqlparser-rs contributors. All rights reserved.
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

pub struct DisplaySeparated<'a, T>
where
    T: AstDisplay,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> AstDisplay for DisplaySeparated<'a, T>
where
    T: AstDisplay,
{
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        let mut delim = "";
        for t in self.slice {
            f.write_str(delim);
            delim = self.sep;
            t.fmt(f);
        }
    }
}

impl<'a, T> std::fmt::Display for DisplaySeparated<'a, T>
where
    T: AstDisplay,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        AstFormatter::new(f, FormatMode::Simple).write_node(self);
        Ok(())
    }
}

pub fn separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: AstDisplay,
{
    DisplaySeparated { slice, sep }
}

pub fn comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: AstDisplay,
{
    DisplaySeparated { slice, sep: ", " }
}

/// Describes the context in which to print an AST.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FormatMode {
    // Simple is the normal way of printing for human consumption. Identifiers are quoted only if
    // necessary.
    Simple,
    // Stable prints out the AST in a form more suitable for persistence. All identifiers are
    // quoted, even if not necessary. This mode is used when persisting table information to the
    // catalog.
    Stable,
}

#[derive(Debug)]
pub struct AstFormatter<W> {
    buf: W,
    mode: FormatMode,
}

impl<W> AstFormatter<W>
where
    W: fmt::Write,
{
    pub fn write_node<T: AstDisplay>(&mut self, s: &T) {
        s.fmt(self);
    }

    // TODO(justin): make this only accept a &str so that we don't accidentally pass an AstDisplay
    // to it.
    pub fn write_str<T: fmt::Display>(&mut self, s: T) {
        write!(self.buf, "{}", s).expect("unexpected error in fmt::Display implementation");
    }

    // Whether the AST should be optimized for persistence.
    pub fn stable(&self) -> bool {
        self.mode == FormatMode::Stable
    }

    pub fn new(buf: W, mode: FormatMode) -> Self {
        AstFormatter { buf, mode }
    }
}

// AstDisplay is an alternative to fmt::Display to be used for formatting ASTs. It permits
// configuration global to a printing of a given AST.
pub trait AstDisplay {
    fn fmt<W>(&self, f: &mut AstFormatter<W>)
    where
        W: fmt::Write;

    fn to_ast_string(&self) -> String {
        let mut buf = String::new();
        let mut f = AstFormatter::new(&mut buf, FormatMode::Simple);
        self.fmt(&mut f);
        buf
    }

    fn to_ast_string_stable(&self) -> String {
        let mut buf = String::new();
        let mut f = AstFormatter::new(&mut buf, FormatMode::Stable);
        self.fmt(&mut f);
        buf
    }
}

// Derive a fmt::Display implementation for types implementing AstDisplay.
#[macro_export]
macro_rules! impl_display {
    ($name:ident) => {
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                use $crate::ast::display::{AstFormatter, FormatMode};
                AstFormatter::new(f, FormatMode::Simple).write_node(self);
                Ok(())
            }
        }
    };
}

macro_rules! impl_display_t {
    ($name:ident) => {
        impl<T: AstInfo> std::fmt::Display for $name<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                use crate::ast::display::{AstFormatter, FormatMode};
                AstFormatter::new(f, FormatMode::Simple).write_node(self);
                Ok(())
            }
        }
    };
}

impl<T: AstDisplay> AstDisplay for &Box<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        (*self).fmt(f);
    }
}

impl<T: AstDisplay> AstDisplay for Box<T> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        (**self).fmt(f);
    }
}

// u64 used directly to represent, e.g., type modifiers
impl AstDisplay for u64 {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(self);
    }
}

impl AstDisplay for i64 {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(self);
    }
}

pub struct EscapeSingleQuoteString<'a>(&'a str);

impl<'a> AstDisplay for EscapeSingleQuoteString<'a> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        for c in self.0.chars() {
            if c == '\'' {
                f.write_str("\'\'");
            } else {
                f.write_str(c);
            }
        }
    }
}
impl<'a> fmt::Display for EscapeSingleQuoteString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.to_ast_string())
    }
}

pub fn escape_single_quote_string(s: &str) -> EscapeSingleQuoteString<'_> {
    EscapeSingleQuoteString(s)
}
