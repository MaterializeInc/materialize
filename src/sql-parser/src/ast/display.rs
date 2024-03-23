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
    /// Simple is the normal way of printing for human consumption. Identifiers are quoted only if
    /// necessary and sensative information is redacted.
    Simple,
    /// SimpleRedacted is like Simple, but strips out string and number literals.
    /// This makes SQL queries be "usage data", rather than "customer data" according to our
    /// data management policy, allowing us to introspect it.
    SimpleRedacted,
    /// Stable prints out the AST in a form more suitable for persistence. All identifiers are
    /// quoted, even if not necessary. This mode is used when persisting table information to the
    /// catalog.
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

    /// Whether the AST should be printed out in a more human readable format.
    pub fn simple(&self) -> bool {
        matches!(self.mode, FormatMode::Simple | FormatMode::SimpleRedacted)
    }

    /// Whether the AST should be printed in redacted form
    pub fn redacted(&self) -> bool {
        self.mode == FormatMode::SimpleRedacted
    }

    /// Sets the current mode to a compatible version that does not redact
    /// values; returns the current mode, which should be reset when the
    /// unredacted printing is complete using [`Self::set_mode`].
    ///
    /// Note that this is the simplest means of unredacting values opt-out
    /// rather than opt-in. We must monitor usage of this API carefully to
    /// ensure we don't end up leaking values.
    pub fn unredact(&mut self) -> FormatMode {
        match self.mode {
            FormatMode::Simple => FormatMode::Simple,
            FormatMode::SimpleRedacted => {
                self.mode = FormatMode::Simple;
                FormatMode::SimpleRedacted
            }
            FormatMode::Stable => FormatMode::Stable,
        }
    }

    pub fn set_mode(&mut self, mode: FormatMode) {
        self.mode = mode;
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

    fn to_ast_string_redacted(&self) -> String {
        let mut buf = String::new();
        let mut f = AstFormatter::new(&mut buf, FormatMode::SimpleRedacted);
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

/// Functions that generalize to AST nodes representing the "name" of a `WITH`
/// option.
pub trait WithOptionName {
    /// Expresses whether or not values should be redacted based on the option
    /// name (i.e. the option's "key").
    ///
    /// # WARNING
    ///
    /// Whenever implementing this trait consider very carefully whether or not
    /// this value could contain sensitive user data.
    ///
    /// # Context
    /// Many statements in MZ use the format `WITH (<options>...)` to modify the
    /// resulting behavior of the statement. Most often these are modeled in the
    /// AST as a struct with two fields: an option name and a value.
    ///
    /// We do not type check the values of the types until planning, so most
    /// values represent arbitrary user input. To prevent leaking any PII in
    /// that data, we default to replacing values with the string `<REDACTED>`.
    ///
    /// However, in some cases, the values do not need to be redacted. For our
    /// `WITH` options, knowing which option we're dealing with should be
    /// sufficient to understand if a value needs redaction––so this trait
    /// controls redaction on a per-option basis.
    ///
    /// ## Genericizing `WITH` options
    /// It would be nice to force every AST node we consider a `WITH` option to
    /// conform to a particular structure––however, we have a proc macro that
    /// generates visitors over all of our nodes that inhibits our ability to do
    /// this easily. This means, unfortunately, that we cannot rely on
    /// compilation guarantees for this and instead must use the honor system.
    ///
    /// ## Nothing is ever redacted...
    ///
    /// In the initial implementation of this trait, no option requires its
    /// values to be redacted (except for the one test case). That doesn't mean
    /// there won't be in the future. When in doubt, take the more conservative
    /// approach.
    fn redact_value(&self) -> bool {
        // We conservatively assume that all values should be redacted.
        true
    }
}

/// To allow `WITH` option AST nodes to be printed without redaction, you should
/// use this macro's implementation of `AstDisplay`. For more details, consult
/// the doc strings on the functions used on its implementation.
macro_rules! impl_display_for_with_option {
    ($name:ident) => {
        impl<T: AstInfo> AstDisplay for $name<T> {
            fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
                f.write_node(&self.name);
                if let Some(v) = &self.value {
                    f.write_str(" = ");

                    // If the formatter is redacted, but the name does not
                    // require setting the value to be redacted, allow the value
                    // to be printed without redaction.
                    if f.redacted() && !self.name.redact_value() {
                        let mode = f.unredact();
                        f.write_node(v);
                        f.set_mode(mode);
                    } else {
                        f.write_node(v);
                    }
                }
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

// u32 used directly to represent, e.g., oids
impl AstDisplay for u32 {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(self);
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

pub struct EscapedStringLiteral<'a>(&'a str);

impl<'a> AstDisplay for EscapedStringLiteral<'a> {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str("'");
        f.write_node(&escape_single_quote_string(self.0));
        f.write_str("'");
    }
}

impl<'a> fmt::Display for EscapedStringLiteral<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.to_ast_string())
    }
}

pub fn escaped_string_literal(s: &str) -> EscapedStringLiteral<'_> {
    EscapedStringLiteral(s)
}
