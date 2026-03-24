// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! RDF term representation.
//!
//! This module provides a [`serde_json`]-like API for RDF terms, backed by
//! the native Materialize data format ([`Row`] and [`Datum`]). It follows
//! the same pattern as [`crate::adt::jsonb`]: no new `Datum` variants are
//! added — instead, existing variants are reinterpreted through wrapper types.
//!
//! There are two core types:
//!
//!   * [`Rdf`] represents an owned RDF term (wraps a [`Row`]).
//!   * [`RdfRef`] is a borrowed view of an RDF term (wraps a [`Datum`]).
//!
//! ## Encoding
//!
//! Common XSD types use native `Datum` variants directly (zero overhead):
//!
//! | XSD type | Datum variant |
//! |----------|---------------|
//! | `xsd:string` | `Datum::String` |
//! | `xsd:integer` | `Datum::Int64` |
//! | `xsd:decimal` | `Datum::Numeric` |
//! | `xsd:float` | `Datum::Float32` |
//! | `xsd:double` | `Datum::Float64` |
//! | `xsd:boolean` | `Datum::True` / `Datum::False` |
//! | `xsd:dateTime` | `Datum::TimestampTz` |
//! | `xsd:date` | `Datum::Date` |
//! | `xsd:time` | `Datum::Time` |
//! | `xsd:duration` | `Datum::Interval` |
//!
//! Types that need annotation use `Datum::List` with a small integer tag:
//!
//! | RDF kind | Encoding | Tag |
//! |----------|----------|-----|
//! | IRI | `List[Int32(0), String]` | 0 |
//! | Blank node | `List[Int32(1), String]` | 1 |
//! | Language-tagged string | `List[Int32(2), String, String]` | 2 |
//! | Other typed literal | `List[Int32(3), String, String]` | 3 |

use std::fmt;
use std::str::FromStr;

use dec::OrderedDecimal;

use crate::adt::numeric::Numeric;
use crate::{Datum, Row, RowPacker};

/// Tags for compound RDF terms encoded as `Datum::List`.
/// These are small sequential integers for compact variable-width encoding.
const TAG_IRI: i32 = 0;
const TAG_BLANK_NODE: i32 = 1;
const TAG_LANG_STRING: i32 = 2;
const TAG_OTHER_TYPED: i32 = 3;

/// An owned RDF term backed by a [`Row`].
///
/// Similar to [`crate::adt::jsonb::Jsonb`], but for RDF terms.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct Rdf {
    row: Row,
}

impl Rdf {
    /// Constructs an `Rdf` from a [`Row`] containing a single datum.
    pub fn from_row(row: Row) -> Rdf {
        Rdf { row }
    }

    /// Consumes the `Rdf` and returns the underlying [`Row`].
    pub fn into_row(self) -> Row {
        self.row
    }

    /// Returns a reference to the underlying [`Row`].
    pub fn row(&self) -> &Row {
        &self.row
    }

    /// Returns a borrowed view of this RDF term.
    pub fn as_ref(&self) -> RdfRef<'_> {
        RdfRef {
            datum: self.row.unpack_first(),
        }
    }
}

impl fmt::Display for Rdf {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl FromStr for Rdf {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse N-Triples term format.
        let mut row = Row::default();
        let mut packer = row.packer();
        RdfPacker::new(&mut packer).pack_term(s)?;
        Ok(Rdf { row })
    }
}

/// A borrowed RDF term.
///
/// `RdfRef` is to [`Rdf`] as [`&str`](prim@str) is to [`String`].
#[derive(Debug, Copy, Clone)]
pub struct RdfRef<'a> {
    datum: Datum<'a>,
}

impl<'a> RdfRef<'a> {
    /// Constructs an `RdfRef` from a [`Datum`].
    ///
    /// The datum is not validated. Not all datums are valid RDF terms.
    pub fn from_datum(datum: Datum<'a>) -> RdfRef<'a> {
        RdfRef { datum }
    }

    /// Returns the underlying [`Datum`].
    pub fn into_datum(self) -> Datum<'a> {
        self.datum
    }

    /// Converts this borrowed RDF term into an owned [`Rdf`].
    pub fn to_owned(&self) -> Rdf {
        Rdf {
            row: Row::pack_slice(&[self.datum]),
        }
    }

    /// Determines the kind of this RDF term by dispatching on the Datum variant.
    pub fn kind(&self) -> RdfKind<'a> {
        match self.datum {
            // Native types — XSD type implicit from Datum variant.
            Datum::Int64(v) => RdfKind::Integer(v),
            Datum::Float32(v) => RdfKind::Float(v),
            Datum::Float64(v) => RdfKind::Double(v),
            Datum::Numeric(v) => RdfKind::Decimal(v),
            Datum::True => RdfKind::Boolean(true),
            Datum::False => RdfKind::Boolean(false),
            Datum::Date(v) => RdfKind::Date(v),
            Datum::TimestampTz(v) => RdfKind::DateTime(v),
            Datum::Time(v) => RdfKind::Time(v),
            Datum::Interval(v) => RdfKind::Duration(v),
            // Plain string = xsd:string (most common literal type).
            Datum::String(s) => RdfKind::XsdString(s),
            // Tagged compound types — read the discriminant from the list.
            Datum::List(list) => {
                let mut iter = list.iter();
                let tag = match iter.next() {
                    Some(Datum::Int32(t)) => t,
                    _ => return RdfKind::Invalid,
                };
                match tag {
                    TAG_IRI => match iter.next() {
                        Some(Datum::String(s)) => RdfKind::Iri(s),
                        _ => RdfKind::Invalid,
                    },
                    TAG_BLANK_NODE => match iter.next() {
                        Some(Datum::String(s)) => RdfKind::BlankNode(s),
                        _ => RdfKind::Invalid,
                    },
                    TAG_LANG_STRING => {
                        let value = match iter.next() {
                            Some(Datum::String(s)) => s,
                            _ => return RdfKind::Invalid,
                        };
                        let lang = match iter.next() {
                            Some(Datum::String(s)) => s,
                            _ => return RdfKind::Invalid,
                        };
                        RdfKind::LangString { value, lang }
                    }
                    TAG_OTHER_TYPED => {
                        let value = match iter.next() {
                            Some(Datum::String(s)) => s,
                            _ => return RdfKind::Invalid,
                        };
                        let datatype = match iter.next() {
                            Some(Datum::String(s)) => s,
                            _ => return RdfKind::Invalid,
                        };
                        RdfKind::OtherTyped { value, datatype }
                    }
                    _ => RdfKind::Invalid,
                }
            }
            Datum::Null => RdfKind::Null,
            _ => RdfKind::Invalid,
        }
    }
}

impl fmt::Display for RdfRef<'_> {
    /// Formats the RDF term in N-Triples syntax.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind() {
            RdfKind::Iri(iri) => write!(f, "<{iri}>"),
            RdfKind::BlankNode(label) => write!(f, "_:{label}"),
            RdfKind::XsdString(s) => write!(f, "\"{s}\""),
            RdfKind::LangString { value, lang } => write!(f, "\"{value}\"@{lang}"),
            RdfKind::Integer(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#integer>")
            }
            RdfKind::Decimal(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#decimal>")
            }
            RdfKind::Float(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#float>")
            }
            RdfKind::Double(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#double>")
            }
            RdfKind::Boolean(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#boolean>")
            }
            RdfKind::Date(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#date>")
            }
            RdfKind::DateTime(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>")
            }
            RdfKind::Time(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#time>")
            }
            RdfKind::Duration(v) => {
                write!(f, "\"{v}\"^^<http://www.w3.org/2001/XMLSchema#duration>")
            }
            RdfKind::OtherTyped { value, datatype } => {
                write!(f, "\"{value}\"^^<{datatype}>")
            }
            RdfKind::Null => write!(f, "NULL"),
            RdfKind::Invalid => write!(f, "<INVALID>"),
        }
    }
}

/// The kind of an RDF term, extracted from its Datum encoding.
#[derive(Debug, Clone, PartialEq)]
pub enum RdfKind<'a> {
    // --- Resource types (compound encoded) ---
    /// An IRI reference.
    Iri(&'a str),
    /// A blank node.
    BlankNode(&'a str),

    // --- Literal types (native Datum, zero overhead) ---
    /// `xsd:string` — plain string literal.
    XsdString(&'a str),
    /// `xsd:integer` (and all integer subtypes).
    Integer(i64),
    /// `xsd:decimal`.
    Decimal(OrderedDecimal<Numeric>),
    /// `xsd:float`.
    Float(ordered_float::OrderedFloat<f32>),
    /// `xsd:double`.
    Double(ordered_float::OrderedFloat<f64>),
    /// `xsd:boolean`.
    Boolean(bool),
    /// `xsd:date`.
    Date(crate::adt::date::Date),
    /// `xsd:dateTime`.
    DateTime(crate::adt::timestamp::CheckedTimestamp<chrono::DateTime<chrono::Utc>>),
    /// `xsd:time`.
    Time(chrono::NaiveTime),
    /// `xsd:duration` (approximated by Interval).
    Duration(crate::adt::interval::Interval),

    // --- Compound literal types (List encoded) ---
    /// `rdf:langString` — a string with a language tag.
    LangString { value: &'a str, lang: &'a str },
    /// A typed literal with a custom/unknown datatype IRI.
    OtherTyped { value: &'a str, datatype: &'a str },

    // --- Special ---
    /// SQL NULL.
    Null,
    /// Invalid encoding (shouldn't occur in well-formed data).
    Invalid,
}

/// Packs RDF terms into a [`RowPacker`].
///
/// Analogous to [`crate::adt::jsonb::JsonbPacker`]. Encodes RDF terms
/// directly into Row format without intermediate allocations.
#[derive(Debug)]
pub struct RdfPacker<'a, 'row> {
    packer: &'a mut RowPacker<'row>,
}

impl<'a, 'row> RdfPacker<'a, 'row> {
    /// Constructs a new packer that writes into the given [`RowPacker`].
    pub fn new(packer: &'a mut RowPacker<'row>) -> RdfPacker<'a, 'row> {
        RdfPacker { packer }
    }

    /// Packs an IRI.
    pub fn pack_iri(self, iri: &str) {
        self.packer.push_list_with(|packer| {
            packer.push(Datum::Int32(TAG_IRI));
            packer.push(Datum::String(iri));
        });
    }

    /// Packs a blank node.
    pub fn pack_blank_node(self, label: &str) {
        self.packer.push_list_with(|packer| {
            packer.push(Datum::Int32(TAG_BLANK_NODE));
            packer.push(Datum::String(label));
        });
    }

    /// Packs an `xsd:string` literal (bare string, zero overhead).
    pub fn pack_string(self, value: &str) {
        self.packer.push(Datum::String(value));
    }

    /// Packs a language-tagged string (`rdf:langString`).
    pub fn pack_lang_string(self, value: &str, lang: &str) {
        self.packer.push_list_with(|packer| {
            packer.push(Datum::Int32(TAG_LANG_STRING));
            packer.push(Datum::String(value));
            packer.push(Datum::String(lang));
        });
    }

    /// Packs an `xsd:integer` literal.
    pub fn pack_integer(self, value: i64) {
        self.packer.push(Datum::Int64(value));
    }

    /// Packs an `xsd:double` literal.
    pub fn pack_double(self, value: f64) {
        self.packer
            .push(Datum::Float64(ordered_float::OrderedFloat(value)));
    }

    /// Packs an `xsd:float` literal.
    pub fn pack_float(self, value: f32) {
        self.packer
            .push(Datum::Float32(ordered_float::OrderedFloat(value)));
    }

    /// Packs an `xsd:boolean` literal.
    pub fn pack_boolean(self, value: bool) {
        self.packer
            .push(if value { Datum::True } else { Datum::False });
    }

    /// Packs a typed literal with a custom/unknown datatype IRI.
    pub fn pack_other_typed(self, value: &str, datatype: &str) {
        self.packer.push_list_with(|packer| {
            packer.push(Datum::Int32(TAG_OTHER_TYPED));
            packer.push(Datum::String(value));
            packer.push(Datum::String(datatype));
        });
    }

    /// Packs an RDF term from an N-Triples string representation.
    ///
    /// Recognizes:
    /// - `<iri>` → IRI
    /// - `_:label` → blank node
    /// - `"value"` → xsd:string
    /// - `"value"@lang` → language-tagged string
    /// - `"value"^^<type>` → typed literal (parsed to native type if known)
    pub fn pack_term(self, s: &str) -> Result<(), anyhow::Error> {
        let s = s.trim();
        if s.starts_with('<') && s.ends_with('>') {
            // IRI
            self.pack_iri(&s[1..s.len() - 1]);
        } else if s.starts_with("_:") {
            // Blank node
            self.pack_blank_node(&s[2..]);
        } else if s.starts_with('"') {
            // Literal — find the closing quote
            let close_quote = s[1..]
                .find('"')
                .map(|i| i + 1)
                .ok_or_else(|| anyhow::anyhow!("unclosed quote in RDF term: {s}"))?;
            let value = &s[1..close_quote];
            let rest = &s[close_quote + 1..];
            if rest.starts_with("@") {
                // Language-tagged string
                self.pack_lang_string(value, &rest[1..]);
            } else if rest.starts_with("^^<") && rest.ends_with('>') {
                // Typed literal
                let datatype = &rest[3..rest.len() - 1];
                self.pack_typed_literal(value, datatype);
            } else if rest.is_empty() {
                // Plain string
                self.pack_string(value);
            } else {
                anyhow::bail!("invalid RDF term suffix: {rest}");
            }
        } else {
            anyhow::bail!("unrecognized RDF term: {s}");
        }
        Ok(())
    }

    /// Packs a typed literal, converting known XSD types to native Datums.
    fn pack_typed_literal(self, value: &str, datatype: &str) {
        match datatype {
            "http://www.w3.org/2001/XMLSchema#integer"
            | "http://www.w3.org/2001/XMLSchema#long"
            | "http://www.w3.org/2001/XMLSchema#int"
            | "http://www.w3.org/2001/XMLSchema#short"
            | "http://www.w3.org/2001/XMLSchema#byte"
            | "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
            | "http://www.w3.org/2001/XMLSchema#positiveInteger"
            | "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"
            | "http://www.w3.org/2001/XMLSchema#negativeInteger"
            | "http://www.w3.org/2001/XMLSchema#unsignedLong"
            | "http://www.w3.org/2001/XMLSchema#unsignedInt"
            | "http://www.w3.org/2001/XMLSchema#unsignedShort"
            | "http://www.w3.org/2001/XMLSchema#unsignedByte" => {
                if let Ok(v) = value.parse::<i64>() {
                    self.pack_integer(v);
                } else {
                    self.pack_other_typed(value, datatype);
                }
            }
            "http://www.w3.org/2001/XMLSchema#double" => {
                if let Ok(v) = value.parse::<f64>() {
                    self.pack_double(v);
                } else {
                    self.pack_other_typed(value, datatype);
                }
            }
            "http://www.w3.org/2001/XMLSchema#float" => {
                if let Ok(v) = value.parse::<f32>() {
                    self.pack_float(v);
                } else {
                    self.pack_other_typed(value, datatype);
                }
            }
            "http://www.w3.org/2001/XMLSchema#boolean" => match value {
                "true" | "1" => self.pack_boolean(true),
                "false" | "0" => self.pack_boolean(false),
                _ => self.pack_other_typed(value, datatype),
            },
            "http://www.w3.org/2001/XMLSchema#string" => {
                self.pack_string(value);
            }
            _ => {
                // Unknown datatype — store as-is with type annotation.
                self.pack_other_typed(value, datatype);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pack_and_read(f: impl FnOnce(RdfPacker<'_, '_>)) -> Rdf {
        let mut row = Row::default();
        let mut packer = row.packer();
        f(RdfPacker::new(&mut packer));
        Rdf::from_row(row)
    }

    #[mz_ore::test]
    fn test_iri_round_trip() {
        let rdf = pack_and_read(|p| p.pack_iri("http://example.org/alice"));
        let kind = rdf.as_ref().kind();
        assert_eq!(kind, RdfKind::Iri("http://example.org/alice"));
        assert_eq!(rdf.to_string(), "<http://example.org/alice>");
    }

    #[mz_ore::test]
    fn test_blank_node_round_trip() {
        let rdf = pack_and_read(|p| p.pack_blank_node("b0"));
        let kind = rdf.as_ref().kind();
        assert_eq!(kind, RdfKind::BlankNode("b0"));
        assert_eq!(rdf.to_string(), "_:b0");
    }

    #[mz_ore::test]
    fn test_xsd_string_round_trip() {
        let rdf = pack_and_read(|p| p.pack_string("hello world"));
        let kind = rdf.as_ref().kind();
        assert_eq!(kind, RdfKind::XsdString("hello world"));
        assert_eq!(rdf.to_string(), "\"hello world\"");
    }

    #[mz_ore::test]
    fn test_empty_string() {
        let rdf = pack_and_read(|p| p.pack_string(""));
        assert_eq!(rdf.as_ref().kind(), RdfKind::XsdString(""));
        assert_eq!(rdf.to_string(), "\"\"");
    }

    #[mz_ore::test]
    fn test_lang_string_round_trip() {
        let rdf = pack_and_read(|p| p.pack_lang_string("hello", "en"));
        let kind = rdf.as_ref().kind();
        assert_eq!(
            kind,
            RdfKind::LangString {
                value: "hello",
                lang: "en"
            }
        );
        assert_eq!(rdf.to_string(), "\"hello\"@en");
    }

    #[mz_ore::test]
    fn test_integer_round_trip() {
        let rdf = pack_and_read(|p| p.pack_integer(42));
        assert_eq!(rdf.as_ref().kind(), RdfKind::Integer(42));
    }

    #[mz_ore::test]
    fn test_negative_integer() {
        let rdf = pack_and_read(|p| p.pack_integer(-100));
        assert_eq!(rdf.as_ref().kind(), RdfKind::Integer(-100));
    }

    #[mz_ore::test]
    fn test_double_round_trip() {
        let rdf = pack_and_read(|p| p.pack_double(3.14));
        match rdf.as_ref().kind() {
            RdfKind::Double(v) => assert!((v.into_inner() - 3.14).abs() < f64::EPSILON),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn test_double_nan() {
        let rdf = pack_and_read(|p| p.pack_double(f64::NAN));
        match rdf.as_ref().kind() {
            RdfKind::Double(v) => assert!(v.into_inner().is_nan()),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn test_float_round_trip() {
        let rdf = pack_and_read(|p| p.pack_float(2.5));
        match rdf.as_ref().kind() {
            RdfKind::Float(v) => assert!((v.into_inner() - 2.5).abs() < f32::EPSILON),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn test_boolean_round_trip() {
        let rdf_true = pack_and_read(|p| p.pack_boolean(true));
        assert_eq!(rdf_true.as_ref().kind(), RdfKind::Boolean(true));

        let rdf_false = pack_and_read(|p| p.pack_boolean(false));
        assert_eq!(rdf_false.as_ref().kind(), RdfKind::Boolean(false));
    }

    #[mz_ore::test]
    fn test_other_typed_round_trip() {
        let rdf = pack_and_read(|p| {
            p.pack_other_typed(
                "POINT(1 2)",
                "http://www.opengis.net/ont/geosparql#wktLiteral",
            )
        });
        assert_eq!(
            rdf.as_ref().kind(),
            RdfKind::OtherTyped {
                value: "POINT(1 2)",
                datatype: "http://www.opengis.net/ont/geosparql#wktLiteral"
            }
        );
    }

    #[mz_ore::test]
    fn test_pack_term_iri() {
        let rdf: Rdf = "<http://example.org/foo>".parse().unwrap();
        assert_eq!(rdf.as_ref().kind(), RdfKind::Iri("http://example.org/foo"));
    }

    #[mz_ore::test]
    fn test_pack_term_blank_node() {
        let rdf: Rdf = "_:b42".parse().unwrap();
        assert_eq!(rdf.as_ref().kind(), RdfKind::BlankNode("b42"));
    }

    #[mz_ore::test]
    fn test_pack_term_plain_string() {
        let rdf: Rdf = "\"hello\"".parse().unwrap();
        assert_eq!(rdf.as_ref().kind(), RdfKind::XsdString("hello"));
    }

    #[mz_ore::test]
    fn test_pack_term_lang_string() {
        let rdf: Rdf = "\"bonjour\"@fr".parse().unwrap();
        assert_eq!(
            rdf.as_ref().kind(),
            RdfKind::LangString {
                value: "bonjour",
                lang: "fr"
            }
        );
    }

    #[mz_ore::test]
    fn test_pack_term_typed_integer() {
        let rdf: Rdf = "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            .parse()
            .unwrap();
        assert_eq!(rdf.as_ref().kind(), RdfKind::Integer(42));
    }

    #[mz_ore::test]
    fn test_pack_term_typed_boolean() {
        let rdf: Rdf = "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"
            .parse()
            .unwrap();
        assert_eq!(rdf.as_ref().kind(), RdfKind::Boolean(true));
    }

    #[mz_ore::test]
    fn test_pack_term_typed_double() {
        let rdf: Rdf = "\"3.14\"^^<http://www.w3.org/2001/XMLSchema#double>"
            .parse()
            .unwrap();
        match rdf.as_ref().kind() {
            RdfKind::Double(v) => assert!((v.into_inner() - 3.14).abs() < f64::EPSILON),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn test_pack_term_typed_string() {
        let rdf: Rdf = "\"hello\"^^<http://www.w3.org/2001/XMLSchema#string>"
            .parse()
            .unwrap();
        assert_eq!(rdf.as_ref().kind(), RdfKind::XsdString("hello"));
    }

    #[mz_ore::test]
    fn test_pack_term_unknown_type() {
        let rdf: Rdf = "\"data\"^^<http://example.org/custom>".parse().unwrap();
        assert_eq!(
            rdf.as_ref().kind(),
            RdfKind::OtherTyped {
                value: "data",
                datatype: "http://example.org/custom"
            }
        );
    }

    #[mz_ore::test]
    fn test_long_iri() {
        let long_iri = "http://example.org/".to_string() + &"a".repeat(10000);
        let rdf = pack_and_read(|p| p.pack_iri(&long_iri));
        assert_eq!(rdf.as_ref().kind(), RdfKind::Iri(long_iri.as_str()));
    }

    #[mz_ore::test]
    fn test_multibyte_lang_tag() {
        // BCP47 tags are ASCII, but test robustness with edge case
        let rdf = pack_and_read(|p| p.pack_lang_string("value", "zh-Hant-TW"));
        assert_eq!(
            rdf.as_ref().kind(),
            RdfKind::LangString {
                value: "value",
                lang: "zh-Hant-TW"
            }
        );
    }

    #[mz_ore::test]
    fn test_negative_zero_double() {
        let rdf = pack_and_read(|p| p.pack_double(-0.0));
        match rdf.as_ref().kind() {
            RdfKind::Double(v) => {
                assert!(v.into_inner().is_sign_negative());
                assert_eq!(v.into_inner(), 0.0);
            }
            other => panic!("expected Double, got {other:?}"),
        }
    }
}
