// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt::{self, Debug, Write};

use crate::targets::RustTarget;

pub mod args;
pub mod context;
pub mod header;
pub mod metadata;
pub mod rules;
pub mod targets;

/// Global configuration for generating `BUILD` files.
#[derive(Debug, Clone)]
pub struct Config {
    ignored_crates: Vec<Cow<'static, str>>,
    proto_build_crates: Vec<Cow<'static, str>>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            ignored_crates: vec!["workspace-hack".into()],
            proto_build_crates: vec!["prost_build".into(), "tonic_build".into()],
        }
    }
}

impl Config {
    /// Returns `true` if the named dependency should be included, `false` if it should be ignored.
    pub fn include_dep(&self, name: &str) -> bool {
        !self.ignored_crates.contains(&Cow::Borrowed(name))
    }
}

/// An entire `BUILD.bazel` file.
///
/// This includes an auto-generated header, `load(...)` statements, and all
/// Bazel targets.
pub struct BazelBuildFile<'a> {
    pub header: header::BazelHeader,
    pub targets: Vec<&'a dyn RustTarget>,
}

impl<'a> fmt::Display for BazelBuildFile<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.header.format(f)?;
        writeln!(f)?;

        for target in &self.targets {
            target.format(f)?;
            writeln!(f)?;
        }
        Ok(())
    }
}

/// Formatting trait for converting a type to its `BUILD.bazel` representation.
pub trait ToBazelDefinition: Debug {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error>;

    fn to_bazel_definition(&self) -> String {
        let mut buf = String::new();
        self.format(&mut buf).expect("failed to write into string");
        buf
    }
}

impl<T: ToBazelDefinition> ToBazelDefinition for Option<T> {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        match self {
            Some(val) => val.format(writer),
            None => Ok(()),
        }
    }
}

/// Wrapper around a [`std::fmt::Write`] that helps write at the correct level of indentation.
struct AutoIndentingWriter<'w> {
    level: usize,
    should_indent: bool,
    writer: &'w mut dyn fmt::Write,
}

impl<'w> AutoIndentingWriter<'w> {
    fn new(writer: &'w mut dyn fmt::Write) -> Self {
        AutoIndentingWriter {
            level: 0,
            should_indent: true,
            writer,
        }
    }

    fn indent(&mut self) -> AutoIndentingWriter<'_> {
        AutoIndentingWriter {
            level: self.level + 1,
            should_indent: self.should_indent,
            writer: self.writer,
        }
    }
}

impl<'w> fmt::Write for AutoIndentingWriter<'w> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let lines = s.split_inclusive('\n');

        for line in lines {
            if self.should_indent {
                for _ in 0..self.level {
                    self.writer.write_char('\t')?;
                }
            }

            self.writer.write_str(line)?;
            self.should_indent = line.ends_with('\n');
        }

        Ok(())
    }
}

/// A [`String`] that when formatted for Bazel is quoted.
///
/// ```
/// use cargo_gazelle::{QuotedString, ToBazelDefinition};
///
/// let deps = QuotedString::new("json");
/// assert_eq!(deps.to_bazel_definition(), "\"json\"");
/// ```
#[derive(Debug, Clone)]
pub struct QuotedString(String);

impl QuotedString {
    pub fn new(s: impl Into<String>) -> Self {
        QuotedString(s.into())
    }

    /// Returns the inner value of the string, unquoted.
    pub fn unquoted(&self) -> &str {
        &self.0
    }
}

impl ToBazelDefinition for QuotedString {
    fn format(&self, writer: &mut dyn Write) -> Result<(), fmt::Error> {
        write!(writer, "\"{}\"", self.0)?;
        Ok(())
    }
}

impl From<String> for QuotedString {
    fn from(value: String) -> Self {
        QuotedString(value)
    }
}

impl<'a> From<&'a str> for QuotedString {
    fn from(value: &'a str) -> Self {
        QuotedString(value.to_string())
    }
}

/// A field within a Build rule, e.g. `name = "foo"`.
///
/// ```
/// use cargo_gazelle::{Field, List, QuotedString, ToBazelDefinition};
///
/// let deps = Field::new("crate_features", List::new(vec![QuotedString::new("json")]));
/// assert_eq!(deps.to_bazel_definition(), "crate_features = [\"json\"],\n");
/// ```
#[derive(Debug, Clone)]
pub struct Field<T> {
    name: String,
    value: T,
}

impl<T> Field<T> {
    pub fn new(name: impl Into<String>, value: T) -> Self {
        Field {
            name: name.into(),
            value,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T: ToBazelDefinition> ToBazelDefinition for Field<T> {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        write!(writer, "{} = ", self.name)?;
        self.value.format(writer)?;
        writeln!(writer, ",")?;

        Ok(())
    }
}

/// Helper for formatting a list of items.
///
/// ```
/// use cargo_gazelle::{List, QuotedString, ToBazelDefinition};
///
/// let deps = List::new(vec![QuotedString::new("tokio")]);
/// assert_eq!(deps.to_bazel_definition(), "[\"tokio\"]");
/// ```
#[derive(Debug)]
pub struct List<T> {
    items: Vec<T>,
    objects: Vec<Box<dyn ToBazelDefinition>>,
}

impl<T> List<T> {
    pub fn new(items: impl IntoIterator<Item = T>) -> Self {
        List {
            items: items.into_iter().collect(),
            objects: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        List::new(VecDeque::new())
    }

    /// Concatenate another Bazel object to this list.
    ///
    /// Concretely this will result in a generated Bazel list like `[ ... ] + <concat>`.
    ///
    /// TODO(parkmcar): This feels a bit off, maybe the API should be something like
    /// `LinkedList`?
    pub fn concat_other(mut self, other: impl ToBazelDefinition + 'static) -> Self {
        self.objects.push(Box::new(other));
        self
    }

    /// Push a value of `T` to the front of the list.
    pub fn push_front(&mut self, val: T) {
        self.items.insert(0, val)
    }

    /// Push a value of `T` to the back of the list.
    pub fn push_back(&mut self, val: T) {
        self.items.push(val)
    }

    /// Extend `self` with the values from `vals`.
    pub fn extend(&mut self, vals: impl IntoIterator<Item = T>) {
        self.items.extend(vals)
    }
}

impl<A> FromIterator<A> for List<A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        List {
            items: iter.into_iter().collect(),
            objects: Vec::new(),
        }
    }
}

impl<T: ToBazelDefinition> ToBazelDefinition for List<T> {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        match &self.items[..] {
            [] => write!(w, "[]")?,
            [one] => write!(w, "[{}]", one.to_bazel_definition())?,
            multiple => {
                write!(w, "[")?;
                for item in multiple {
                    let mut w = w.indent();
                    writeln!(w)?;
                    write!(w, "{},", item.to_bazel_definition())?;
                }
                write!(w, "\n]")?;
            }
        }

        for o in &self.objects {
            write!(w, " + ")?;
            o.format(&mut w)?;
        }

        Ok(())
    }
}

/// A Bazel [`filegroup`](https://bazel.build/reference/be/general#filegroup).
#[derive(Debug)]
pub struct FileGroup {
    name: Field<QuotedString>,
    files: Field<List<QuotedString>>,
}

impl FileGroup {
    pub fn new<S: Into<String>>(
        name: impl Into<String>,
        files: impl IntoIterator<Item = S>,
    ) -> Self {
        let name = Field::new("name", QuotedString::new(name.into()));
        let files = Field::new(
            "srcs",
            files.into_iter().map(|f| QuotedString::new(f)).collect(),
        );

        FileGroup { name, files }
    }
}

impl ToBazelDefinition for FileGroup {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        writeln!(w, "filegroup(")?;
        {
            let mut w = w.indent();
            self.name.format(&mut w)?;
            self.files.format(&mut w)?;
        }
        writeln!(w, ")")?;

        Ok(())
    }
}
