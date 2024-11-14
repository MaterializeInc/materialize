// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Debug, Write};
use std::rc::Rc;

use crate::platforms::PlatformVariant;
use crate::targets::RustTarget;

pub mod args;
pub mod config;
pub mod context;
pub mod header;
pub mod platforms;
pub mod rules;
pub mod targets;

/// An entire `BUILD.bazel` file.
///
/// This includes an auto-generated header, `load(...)` statements, and all
/// Bazel targets.
pub struct BazelBuildFile {
    pub header: header::BazelHeader,
    pub targets: Vec<Box<dyn RustTarget>>,
}

impl fmt::Display for BazelBuildFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.header.format(f)?;
        for target in &self.targets {
            writeln!(f)?;
            target.format(f)?;
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

impl ToBazelDefinition for bool {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let bazel_str = if *self { "True" } else { "False" };
        writer.write_str(bazel_str)
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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

impl<T: ToString> From<T> for QuotedString {
    fn from(value: T) -> Self {
        QuotedString(value.to_string())
    }
}

/// A field within a Build rule, e.g. `name = "foo"`.
///
/// ```
/// use cargo_gazelle::{Field, List, QuotedString, ToBazelDefinition};
///
/// let deps: Field<List<QuotedString>> = Field::new(
///     "crate_features",
///     List::new(vec![QuotedString::new("json")],
/// ));
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
/// let deps: List<QuotedString> = List::new(vec![QuotedString::new("tokio")]);
/// assert_eq!(deps.to_bazel_definition(), "[\"tokio\"]");
/// ```
#[derive(Debug, Clone)]
pub struct List<T> {
    items: Vec<T>,
    objects: Vec<Rc<dyn ToBazelDefinition>>,
}

impl<T> List<T> {
    pub fn new<E: Into<T>, I: IntoIterator<Item = E>>(items: I) -> Self {
        List {
            items: items.into_iter().map(Into::into).collect(),
            objects: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        List::new(VecDeque::<T>::new())
    }

    /// Concatenate another Bazel object to this list.
    ///
    /// Concretely this will result in a generated Bazel list like `[ ... ] + <concat>`.
    ///
    /// TODO(parkmcar): This feels a bit off, maybe the API should be something like
    /// `LinkedList`?
    pub fn concat_other(mut self, other: impl ToBazelDefinition + 'static) -> Self {
        self.objects.push(Rc::new(other));
        self
    }

    /// Push a value of `T` to the front of the list.
    pub fn push_front<E: Into<T>>(&mut self, val: E) {
        self.items.insert(0, val.into())
    }

    /// Push a value of `T` to the back of the list.
    pub fn push_back<E: Into<T>>(&mut self, val: E) {
        self.items.push(val.into())
    }

    /// Extend `self` with the values from `vals`.
    pub fn extend<E: Into<T>, I: IntoIterator<Item = E>>(&mut self, vals: I) {
        self.items.extend(vals.into_iter().map(Into::into))
    }

    /// Returns an iterator over all of the `items`.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }

    /// Returns if this [`List`] is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty() && self.objects.is_empty()
    }
}

impl<A, B, T> From<T> for List<A>
where
    B: Into<A>,
    T: IntoIterator<Item = B>,
{
    fn from(value: T) -> Self {
        List::from_iter(value.into_iter().map(|x| x.into()))
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
            let def = o.to_bazel_definition();
            if !def.is_empty() {
                write!(w, " + {def}")?;
            }
        }

        Ok(())
    }
}

/// Helper for formatting a dictionary.
///
/// ```
/// use cargo_gazelle::{Dict, QuotedString, ToBazelDefinition};
///
/// let entry = (QuotedString::new("RUST_LOG"), QuotedString::new("INFO"));
/// let deps: Dict<QuotedString, QuotedString> = Dict::new(vec![entry]);
/// assert_eq!(deps.to_bazel_definition(), "{ \"RUST_LOG\": \"INFO\" }");
/// ```
#[derive(Debug)]
pub struct Dict<K, V> {
    items: BTreeMap<K, V>,
}

impl<K, V> Dict<K, V> {
    pub fn new<M, N, I>(vals: I) -> Self
    where
        M: Into<K>,
        N: Into<V>,
        I: IntoIterator<Item = (M, N)>,
        K: Ord,
    {
        Dict {
            items: vals
                .into_iter()
                .map(|(m, n)| (m.into(), n.into()))
                .collect(),
        }
    }

    pub fn empty() -> Self {
        Dict {
            items: BTreeMap::default(),
        }
    }

    pub fn insert<M, N>(&mut self, key: M, val: N)
    where
        M: Into<K>,
        N: Into<V>,
        K: Ord,
    {
        self.items.insert(key.into(), val.into());
    }

    pub fn extend<M, N, I>(&mut self, vals: I)
    where
        M: Into<K>,
        N: Into<V>,
        I: IntoIterator<Item = (M, N)>,
        K: Ord,
    {
        self.items
            .extend(vals.into_iter().map(|(k, v)| (k.into(), v.into())));
    }
}

impl<K: ToBazelDefinition, V: ToBazelDefinition> ToBazelDefinition for Dict<K, V> {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        match self.items.len() {
            0 => write!(w, "{{}}")?,
            1 => {
                let (key, val) = self.items.iter().next().expect("checked length");
                write!(
                    w,
                    "{{ {}: {} }}",
                    key.to_bazel_definition(),
                    val.to_bazel_definition()
                )?;
            }
            _ => {
                write!(w, "{{")?;
                for (key, val) in &self.items {
                    let mut w = w.indent();
                    writeln!(w)?;
                    write!(
                        w,
                        "{{ {}: {} }}",
                        key.to_bazel_definition(),
                        val.to_bazel_definition()
                    )?;
                }
                write!(w, "\n}}")?;
            }
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

/// A Bazel [`alias`](https://bazel.build/reference/be/general#alias)
#[derive(Debug)]
pub struct Alias {
    name: Field<QuotedString>,
    actual: Field<QuotedString>,
}

impl Alias {
    pub fn new<N: Into<String>, A: Into<String>>(name: N, actual: A) -> Self {
        let name = Field::new("name", QuotedString::new(name.into()));
        let actual = Field::new("actual", QuotedString::new(actual.into()));

        Alias { name, actual }
    }
}

impl ToBazelDefinition for Alias {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        writeln!(w, "alias(")?;
        {
            let mut w = w.indent();
            self.name.format(&mut w)?;
            self.actual.format(&mut w)?;
        }
        writeln!(w, ")")?;

        Ok(())
    }
}

/// A Bazel [`glob`](https://bazel.build/reference/be/functions#glob)
///
/// TODO(parkmcar): Support `excludes`.
#[derive(Debug)]
pub struct Glob {
    includes: List<QuotedString>,
}

impl Glob {
    pub fn new<E, I>(globs: I) -> Glob
    where
        E: Into<QuotedString>,
        I: IntoIterator<Item = E>,
    {
        Glob {
            includes: List::new(globs),
        }
    }
}

impl ToBazelDefinition for Glob {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        write!(writer, "glob(")?;
        self.includes.format(writer)?;
        write!(writer, ")")?;

        Ok(())
    }
}

/// Helper for formatting a [`select({ ... })`](https://bazel.build/reference/be/functions#select) function.
///
/// ```
/// use cargo_gazelle::{
///     List,
///     QuotedString,
///     Select,
///     ToBazelDefinition,
/// };
/// use cargo_gazelle::platforms::PlatformVariant;
///
/// let features = List::new(["json"]);
/// let features = [(PlatformVariant::Aarch64MacOS, features)];
///
/// let select: Select<List<QuotedString>> = Select::new(features, List::empty());
/// assert_eq!(select.to_bazel_definition(), "select({\n\t\"@//misc/bazel/platforms:macos_arm\": [\"json\"],\n\t\"//conditions:default\": [],\n})");
/// ```
#[derive(Debug)]
pub struct Select<T> {
    entries: BTreeMap<PlatformVariant, T>,
    default: T,
}

impl<T> Select<T> {
    pub fn new<E, I>(entires: I, default: E) -> Select<T>
    where
        E: Into<T>,
        I: IntoIterator<Item = (PlatformVariant, E)>,
    {
        Select {
            entries: entires
                .into_iter()
                .map(|(variant, entry)| (variant, entry.into()))
                .collect(),
            default: default.into(),
        }
    }
}

impl<T: ToBazelDefinition> ToBazelDefinition for Select<T> {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        writeln!(w, "select({{")?;
        {
            let mut w = w.indent();
            for (variant, entry) in &self.entries {
                variant.format(&mut w)?;
                write!(w, ": ")?;
                entry.format(&mut w)?;
                writeln!(w, ",")?;
            }

            write!(w, "\"//conditions:default\": ")?;
            self.default.format(&mut w)?;
            writeln!(w, ",")?;
        }
        write!(w, "}})")?;

        Ok(())
    }
}
