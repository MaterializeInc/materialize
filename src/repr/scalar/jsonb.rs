// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! JSON representation.
//!
//! This module provides a [`serde_json`]-like API that is backed by the native
//! Materialize data format, i.e., [`Row`] and [`Datum`]. It supports
//! efficiently parsing and serializing JSON strings and byte slices with
//! minimal allocations. It also provides seamless interop with APIs that
//! require [`serde_json::Value`], though at a small performance cost.
//!
//! There are two core types in the module:
//!
//!   * [`Jsonb`] represents owned JSON data. This type houses the
//!     deserialization functions.
//!
//!   * [`JsonbRef`] is a borrowed view of JSON data. This type houses the
//!     serialization functions.
//!
//! The name "jsonb" is based on the PostgreSQL data type of the same name.
//! Various sources claim this stands for "JSON better", as compared to the
//! less-efficient `json` data type in PostgreSQL.
//!
//! ## Constructing JSON objects
//!
//! To parse JSON from a string, use the [`FromStr`] implementation. Once
//! parsed, the underlying [`Row`] can be extracted with [`Jsonb::into_row`].
//!
//! ```
//! # use repr::jsonb::Jsonb;
//! let jsonb: Jsonb = r#"{"a": 1, "b": 2}"#.parse()?;
//! let row = jsonb.into_row();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! If the source JSON is in bytes, use [`Jsonb::from_slice`] instead:
//!
//! ```
//! # use repr::jsonb::Jsonb;
//! let jsonb = Jsonb::from_slice(br#"{"a": 1, "b": 2}"#);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Serializing JSON objects
//!
//! To write a JSON object to a string, use the [`fmt::Display`] implementation.
//! The alternate format produces pretty output.
//!
//! ```
//! # use repr::jsonb::Jsonb;
//! # let jsonb: Jsonb = "null".parse().unwrap();
//! format!("compressed: {}", jsonb);
//! format!("pretty: {:#}", jsonb);
//! ```
//!
//! ## Direct JSON deserialization
//!
//! You can skip [`Jsonb`] entirely and deserialize JSON directly into an
//! existing [`RowPacker`] with [`JsonbPacker`]. This saves an allocation and a
//! copy.
//!
//! ```rust
//! # use repr::jsonb::JsonbPacker;
//! # use repr::{Datum, RowPacker};
//! let mut packer = RowPacker::new();
//! packer.push(Datum::Int32(42));
//! packer = JsonbPacker::new(packer).pack_str("[1, 2]")?;
//! let row = packer.finish();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::borrow::Cow;
use std::fmt;
use std::io;
use std::str::{self, FromStr};

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

use crate::{Datum, Row, RowPacker};

/// An owned JSON value backed by a [`Row`].
///
/// Similar to [`serde_json::Value`], but the conversion to [`Row`] is free.
///
/// All numbers are represented as [`f64`]s. It is not possible to construct a
/// `Jsonb` from a JSON object that contains integers that cannot be represented
/// exactly as `f64`s.
#[derive(Debug)]
pub struct Jsonb {
    row: Row,
}

impl Jsonb {
    /// Constructs a new `Jsonb` from a [`serde_json::Value`].
    ///
    /// Errors if any of the contained integers cannot be represented exactly as
    /// an [`f64`].
    pub fn from_serde_json(val: serde_json::Value) -> Result<Self, failure::Error> {
        let packer = JsonbPacker::new(RowPacker::new()).pack_serde_json(val)?;
        Ok(Jsonb {
            row: packer.finish(),
        })
    }

    /// Parses a `Jsonb` from a byte slice `buf`.
    ///
    /// Errors if the slice is not valid JSON or if any of the contained
    /// integers cannot be represented exactly as an [`f64`].
    pub fn from_slice(buf: &[u8]) -> Result<Jsonb, failure::Error> {
        let packer = JsonbPacker::new(RowPacker::new()).pack_slice(buf)?;
        Ok(Jsonb {
            row: packer.finish(),
        })
    }

    /// Constructs a [`JsonbRef`] that references the JSON in this `Jsonb`.
    pub fn as_ref(&self) -> JsonbRef {
        JsonbRef {
            datum: self.row.unpack_first(),
        }
    }

    /// Consumes this `Jsonb` and returns the underlying [`Row`].
    pub fn into_row(self) -> Row {
        self.row
    }
}

impl FromStr for Jsonb {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let packer = JsonbPacker::new(RowPacker::new()).pack_str(s)?;
        Ok(Jsonb {
            row: packer.finish(),
        })
    }
}

impl fmt::Display for Jsonb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

/// A borrowed JSON value.
///
/// `JsonbRef` is to [`Jsonb`] as [`&str`](str) is to [`String`].
#[derive(Debug)]
pub struct JsonbRef<'a> {
    datum: Datum<'a>,
}

impl JsonbRef<'_> {
    /// Constructs a `JsonbRef` from a [`Datum`].
    ///
    /// Note that `datum` is not checked for validity. Not all `Datum`s are
    /// valid JSON.
    pub fn from_datum(datum: Datum) -> JsonbRef {
        JsonbRef { datum }
    }

    /// Constructs an owned [`Jsonb`] from this `JsonbRef`.
    pub fn to_owned(&self) -> Jsonb {
        Jsonb {
            row: Row::pack(&[self.datum]),
        }
    }

    /// Serializes the JSON value into the given IO stream.
    pub fn to_writer<W>(&self, writer: W) -> Result<(), failure::Error>
    where
        W: io::Write,
    {
        serde_json::to_writer(writer, &JsonbDatum(self.datum))?;
        Ok(())
    }

    /// Constructs an owned [`serde_json::Value`] from this `JsonbRef`.
    pub fn to_serde_json(&self) -> serde_json::Value {
        serde_json::to_value(&JsonbDatum(self.datum))
            .expect("conversion to serde_json::Value known to be valid")
    }
}

impl fmt::Display for JsonbRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let alternate = f.alternate();
        let mut w = WriterFormatter { inner: f };
        if alternate {
            serde_json::to_writer_pretty(&mut w, &JsonbDatum(self.datum)).map_err(|_| fmt::Error)
        } else {
            serde_json::to_writer(&mut w, &JsonbDatum(self.datum)).map_err(|_| fmt::Error)
        }
    }
}

/// A JSON deserializer that decodes directly into an existing [`RowPacker`].
///
/// The `JsonbPacker` takes ownership of the `RowPacker` and returns ownership
/// after successfully packing one JSON object. Packing multiple JSON objects in
/// sequence requires constructing multiple `JsonbPacker`s. This somewhat
/// irritating API is required to preserve the safety properties of the
/// `RowPacker`, which require that no one observe the state of the `RowPacker`
/// after a decoding error.
#[derive(Debug)]
pub struct JsonbPacker {
    packer: RowPacker,
}

impl JsonbPacker {
    /// Constructs a new `JsonbPacker` that will pack into `packer`.
    pub fn new(packer: RowPacker) -> JsonbPacker {
        JsonbPacker { packer }
    }

    /// Packs a [`serde_json::Value`].
    ///
    /// Errors if any of the contained integers cannot be represented exactly as
    /// an [`f64`].
    pub fn pack_serde_json(mut self, val: serde_json::Value) -> Result<RowPacker, failure::Error> {
        let mut commands = vec![];
        Collector(&mut commands).deserialize(val)?;
        pack_value(&commands, &mut self.packer);
        Ok(self.packer)
    }

    /// Parses and packs a JSON-formatted byte slice.
    ///
    /// Errors if the slice is not valid JSON or if any of the contained
    /// integers cannot be represented exactly as an [`f64`].
    pub fn pack_slice(mut self, buf: &[u8]) -> Result<RowPacker, failure::Error> {
        let mut commands = vec![];
        let mut deserializer = serde_json::Deserializer::from_slice(buf);
        Collector(&mut commands).deserialize(&mut deserializer)?;
        deserializer.end()?;
        pack_value(&commands, &mut self.packer);
        Ok(self.packer)
    }

    /// Parses and packs a JSON-formatted string.
    ///
    /// Errors if the string is not valid or JSON or if any of the contained
    /// integers cannot be represented exactly as an [`f64`].
    pub fn pack_str(mut self, s: &str) -> Result<RowPacker, failure::Error> {
        let mut commands = vec![];
        let mut deserializer = serde_json::Deserializer::from_str(s);
        Collector(&mut commands).deserialize(&mut deserializer)?;
        deserializer.end()?;
        pack_value(&commands, &mut self.packer);
        Ok(self.packer)
    }
}

#[derive(Debug)]
enum Command<'de> {
    Null,
    Bool(bool),
    Number(f64),
    String(Cow<'de, str>),
    Array(usize), // further commands
    Map(usize),   // further commands
}

struct Collector<'a, 'de>(&'a mut Vec<Command<'de>>);

impl<'a, 'de> DeserializeSeed<'de> for Collector<'a, 'de> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'a, 'de> Visitor<'de> for Collector<'a, 'de> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JSON datum")
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<(), E> {
        self.0.push(Command::Null);
        Ok(())
    }

    #[inline]
    fn visit_bool<E>(self, value: bool) -> Result<(), E> {
        self.0.push(Command::Bool(value));
        Ok(())
    }

    #[inline]
    fn visit_i64<E>(self, value: i64) -> Result<(), E>
    where
        E: de::Error,
    {
        if value > (1 << 53) || value < (-1 << 53) {
            return Err(de::Error::custom(format!(
                "{} is out of range for a jsonb number",
                value
            )));
        }
        self.0.push(Command::Number(value as f64));
        Ok(())
    }

    #[inline]
    fn visit_u64<E>(self, value: u64) -> Result<(), E>
    where
        E: de::Error,
    {
        if value > (1 << 53) {
            return Err(de::Error::custom(format!(
                "{} is out of range for a jsonb number",
                value
            )));
        }
        self.0.push(Command::Number(value as f64));
        Ok(())
    }

    #[inline]
    fn visit_f64<E>(self, value: f64) -> Result<(), E> {
        self.0.push(Command::Number(value));
        Ok(())
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<(), E> {
        self.0.push(Command::String(Cow::Owned(value.to_owned())));
        Ok(())
    }

    #[inline]
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<(), E> {
        self.0.push(Command::String(Cow::Borrowed(value)));
        Ok(())
    }

    #[inline]
    fn visit_seq<V>(self, mut visitor: V) -> Result<(), V::Error>
    where
        V: SeqAccess<'de>,
    {
        self.0.push(Command::Array(0));
        let start = self.0.len();
        while visitor.next_element_seed(Collector(self.0))?.is_some() {}
        self.0[start - 1] = Command::Array(self.0.len() - start);
        Ok(())
    }

    #[inline]
    fn visit_map<V>(self, mut visitor: V) -> Result<(), V::Error>
    where
        V: MapAccess<'de>,
    {
        self.0.push(Command::Map(0));
        let start = self.0.len();
        while visitor.next_key_seed(Collector(self.0))?.is_some() {
            visitor.next_value_seed(Collector(self.0))?;
        }
        self.0[start - 1] = Command::Map(self.0.len() - start);
        Ok(())
    }
}

#[inline]
fn pack_value<'de>(value: &[Command<'de>], packer: &mut RowPacker) {
    match &value[0] {
        Command::Null => packer.push(Datum::JsonNull),
        Command::Bool(b) => packer.push(if *b { Datum::True } else { Datum::False }),
        Command::Number(n) => packer.push(Datum::Float64((*n).into())),
        Command::String(s) => packer.push(Datum::String(s)),
        Command::Array(further) => {
            let range = &value[1..][..*further];
            pack_list(range, packer);
        }
        Command::Map(further) => {
            let range = &value[1..][..*further];
            pack_dict(range, packer);
        }
    }
}

/// Packs a sequence of values as an ordered list.
fn pack_list<'de>(mut values: &[Command<'de>], packer: &mut RowPacker) {
    let start = unsafe { packer.start_list() };
    while !values.is_empty() {
        let value = extract_value(&mut values);
        pack_value(value, packer);
    }
    unsafe { packer.finish_list(start) }
}

/// Packs a sequence of (key, val) pairs as an ordered dictionary.
///
/// The keys are required to be `Command::String` variants, and
/// the entries in the dictionary are sorted by these strings.
/// Multiple values for the same key are detected and only
/// the last value is kept.
fn pack_dict<'de>(mut entries: &[Command<'de>], packer: &mut RowPacker) {
    struct Entry<'a> {
        key: &'a str,
        val: &'a [Command<'a>],
    }

    // TODO: It is a bit annoying to have this allocation, but
    // avoiding it seems to require mocking up the recursive
    // function evaluation as a stack machine, which is also
    // annoying.
    let mut sorts = Vec::new();
    while !entries.is_empty() {
        if let Command::String(key) = &entries[0] {
            entries = &entries[1..];
            let val = extract_value(&mut entries);
            sorts.push(Entry { key, val });
        } else {
            unreachable!("JSON decoding produced invalid command sequence");
        }
    }

    // Keys must be written in ascending order, per the requirements of our
    // `Row` representation. If keys are duplicated, we must keep only the last
    // value for the key, as ordered by appearance in the source JSON, per
    // PostgreSQL's implementation.
    sorts.sort_by_key(|entry| entry.key);
    let start = unsafe { packer.start_dict() };
    for i in 0..sorts.len() {
        if i == sorts.len() - 1 || sorts[i].key != sorts[i + 1].key {
            packer.push(Datum::String(sorts[i].key));
            pack_value(sorts[i].val, packer);
        }
    }
    unsafe { packer.finish_dict(start) }
}

/// Extracts a self-contained slice of commands for the next parse node.
fn extract_value<'de, 'a>(values: &mut &'a [Command<'de>]) -> &'a [Command<'de>] {
    let result = match values[0] {
        Command::Array(further) => &values[..further + 1],
        Command::Map(further) => &values[..further + 1],
        _ => &values[0..1],
    };
    *values = &values[result.len()..];
    result
}

/// A wrapper for [`Datum`] that implements [`Serialize`].
///
/// This is a separate type from `JsonbRef` because the `Serialize`
/// implementation is only valid for use with serde_json. If we implemented it
/// on `JsonbRef` directly, it would be possible to serialize a `JsonbRef` into
/// any serde format, like YAML, but that transformation would not be tested
/// and would likely be invalid.
struct JsonbDatum<'a>(Datum<'a>);

impl Serialize for JsonbDatum<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Datum::JsonNull => serializer.serialize_none(),
            Datum::True => serializer.serialize_bool(true),
            Datum::False => serializer.serialize_bool(false),
            Datum::Float64(f) => serializer.serialize_f64(*f),
            Datum::String(s) => serializer.serialize_str(s),
            Datum::List(list) => {
                let mut seq = serializer.serialize_seq(None)?;
                for e in list.iter() {
                    seq.serialize_element(&JsonbDatum(e))?;
                }
                seq.end()
            }
            Datum::Dict(dict) => {
                let mut map = serializer.serialize_map(None)?;
                for (k, v) in dict.iter() {
                    map.serialize_entry(k, &JsonbDatum(v))?;
                }
                map.end()
            }
            d => unreachable!("not a json-compatible datum: {:?}", d),
        }
    }
}

/// Implements `io::Write` for `fmt::Formatter`.
struct WriterFormatter<'a, 'b: 'a> {
    inner: &'a mut fmt::Formatter<'b>,
}

impl<'a, 'b> io::Write for WriterFormatter<'a, 'b> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        fn io_error<E>(_: E) -> io::Error {
            // Error value is dropped by `fmt::Display` implementations above.
            io::Error::new(io::ErrorKind::Other, "fmt error")
        }
        let s = str::from_utf8(buf).map_err(io_error)?;
        self.inner.write_str(s).map_err(io_error)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
