// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serialization of query results in W3C SPARQL result formats.
//!
//! Implements the following output formats:
//! - **SPARQL Query Results JSON** (`application/sparql-results+json`):
//!   [W3C Recommendation](https://www.w3.org/TR/sparql11-results-json/)
//! - **SPARQL Query Results XML** (`application/sparql-results+xml`):
//!   [W3C Recommendation](https://www.w3.org/TR/rdf-sparql-XMLres/)
//! - **N-Triples** (`application/n-triples`):
//!   [W3C Recommendation](https://www.w3.org/TR/n-triples/)
//! - **Turtle** (`text/turtle`):
//!   [W3C Recommendation](https://www.w3.org/TR/turtle/)
//! - **JSON-LD** (`application/ld+json`):
//!   [W3C Recommendation](https://www.w3.org/TR/json-ld11/)
//!
//! These formats are used with `COPY (SPARQL $$...$$) TO STDOUT WITH (FORMAT ...)`.
//! The default pgwire tabular format is unchanged for normal query execution.

use std::fmt::Write;

use mz_repr::RowRef;

/// Classifies an RDF term encoded as a string.
///
/// SPARQL results in Materialize store all terms as TEXT. We infer the term
/// type from its string encoding:
/// - `<...>` → IRI (uri)
/// - `_:...` → blank node (bnode)
/// - `"..."^^<...>` → typed literal
/// - `"..."@...` → language-tagged literal
/// - anything else → plain literal
enum RdfTermKind<'a> {
    Uri(&'a str),
    Bnode(&'a str),
    TypedLiteral { value: &'a str, datatype: &'a str },
    LangLiteral { value: &'a str, lang: &'a str },
    PlainLiteral(&'a str),
}

fn classify_term(s: &str) -> RdfTermKind<'_> {
    if s.starts_with('<') && s.ends_with('>') {
        RdfTermKind::Uri(&s[1..s.len() - 1])
    } else if s.starts_with("_:") {
        RdfTermKind::Bnode(&s[2..])
    } else if s.starts_with('"') {
        // Check for typed literal: "value"^^<datatype>
        if let Some(pos) = s.rfind("\"^^<") {
            let value = &s[1..pos];
            let datatype = &s[pos + 4..s.len() - 1]; // strip trailing >
            RdfTermKind::TypedLiteral { value, datatype }
        }
        // Check for language-tagged literal: "value"@lang
        else if let Some(pos) = s.rfind("\"@") {
            let value = &s[1..pos];
            let lang = &s[pos + 2..];
            RdfTermKind::LangLiteral { value, lang }
        }
        // Plain quoted literal: "value"
        else if s.ends_with('"') {
            RdfTermKind::PlainLiteral(&s[1..s.len() - 1])
        } else {
            RdfTermKind::PlainLiteral(s)
        }
    } else {
        RdfTermKind::PlainLiteral(s)
    }
}

// ---------------------------------------------------------------------------
// SPARQL Query Results JSON (application/sparql-results+json)
// ---------------------------------------------------------------------------

/// Emit the JSON header including `head.vars` and opening of `results.bindings`.
pub fn sparql_json_header(vars: &[&str], out: &mut Vec<u8>) {
    let mut s = String::with_capacity(128);
    s.push_str("{\"head\":{\"vars\":[");
    for (i, var) in vars.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        write!(s, "\"{}\"", json_escape(var)).unwrap();
    }
    s.push_str("]},\"results\":{\"bindings\":[\n");
    out.extend_from_slice(s.as_bytes());
}

/// Emit a single result binding row as a JSON object.
///
/// `row_index` is used to emit commas between rows (comma before all rows
/// except the first).
pub fn sparql_json_row(row: &RowRef, vars: &[&str], row_index: usize, out: &mut Vec<u8>) {
    let mut s = String::with_capacity(256);
    if row_index > 0 {
        s.push(',');
    }
    s.push('{');
    let mut first = true;
    let mut iter = row.iter();
    for var in vars {
        let datum = iter.next();
        let datum = match datum {
            Some(mz_repr::Datum::Null) | None => continue,
            Some(d) => d,
        };
        let text = match datum {
            mz_repr::Datum::String(s) => s,
            _ => continue,
        };
        if !first {
            s.push(',');
        }
        first = false;
        write!(s, "\"{}\":", json_escape(var)).unwrap();
        match classify_term(text) {
            RdfTermKind::Uri(uri) => {
                write!(s, "{{\"type\":\"uri\",\"value\":\"{}\"}}", json_escape(uri)).unwrap();
            }
            RdfTermKind::Bnode(id) => {
                write!(
                    s,
                    "{{\"type\":\"bnode\",\"value\":\"{}\"}}",
                    json_escape(id)
                )
                .unwrap();
            }
            RdfTermKind::TypedLiteral { value, datatype } => {
                write!(
                    s,
                    "{{\"type\":\"typed-literal\",\"value\":\"{}\",\"datatype\":\"{}\"}}",
                    json_escape(value),
                    json_escape(datatype)
                )
                .unwrap();
            }
            RdfTermKind::LangLiteral { value, lang } => {
                write!(
                    s,
                    "{{\"type\":\"literal\",\"value\":\"{}\",\"xml:lang\":\"{}\"}}",
                    json_escape(value),
                    json_escape(lang)
                )
                .unwrap();
            }
            RdfTermKind::PlainLiteral(value) => {
                write!(
                    s,
                    "{{\"type\":\"literal\",\"value\":\"{}\"}}",
                    json_escape(value)
                )
                .unwrap();
            }
        }
    }
    s.push_str("}\n");
    out.extend_from_slice(s.as_bytes());
}

/// Emit the closing brackets for the JSON results structure.
pub fn sparql_json_footer(out: &mut Vec<u8>) {
    out.extend_from_slice(b"]}}\n");
}

// ---------------------------------------------------------------------------
// SPARQL Query Results XML (application/sparql-results+xml)
// ---------------------------------------------------------------------------

/// Emit the XML header including variable declarations and opening `<results>`.
pub fn sparql_xml_header(vars: &[&str], out: &mut Vec<u8>) {
    let mut s = String::with_capacity(256);
    s.push_str("<?xml version=\"1.0\"?>\n");
    s.push_str("<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n");
    s.push_str("  <head>\n");
    for var in vars {
        write!(s, "    <variable name=\"{}\"/>\n", xml_escape(var)).unwrap();
    }
    s.push_str("  </head>\n");
    s.push_str("  <results>\n");
    out.extend_from_slice(s.as_bytes());
}

/// Emit a single result row as an XML `<result>` element.
pub fn sparql_xml_row(row: &RowRef, vars: &[&str], out: &mut Vec<u8>) {
    let mut s = String::with_capacity(256);
    s.push_str("    <result>\n");
    let mut iter = row.iter();
    for var in vars {
        let datum = iter.next();
        let datum = match datum {
            Some(mz_repr::Datum::Null) | None => continue,
            Some(d) => d,
        };
        let text = match datum {
            mz_repr::Datum::String(s) => s,
            _ => continue,
        };
        write!(s, "      <binding name=\"{}\">", xml_escape(var)).unwrap();
        match classify_term(text) {
            RdfTermKind::Uri(uri) => {
                write!(s, "<uri>{}</uri>", xml_escape(uri)).unwrap();
            }
            RdfTermKind::Bnode(id) => {
                write!(s, "<bnode>{}</bnode>", xml_escape(id)).unwrap();
            }
            RdfTermKind::TypedLiteral { value, datatype } => {
                write!(
                    s,
                    "<literal datatype=\"{}\">{}</literal>",
                    xml_escape(datatype),
                    xml_escape(value)
                )
                .unwrap();
            }
            RdfTermKind::LangLiteral { value, lang } => {
                write!(
                    s,
                    "<literal xml:lang=\"{}\">{}</literal>",
                    xml_escape(lang),
                    xml_escape(value)
                )
                .unwrap();
            }
            RdfTermKind::PlainLiteral(value) => {
                write!(s, "<literal>{}</literal>", xml_escape(value)).unwrap();
            }
        }
        s.push_str("</binding>\n");
    }
    s.push_str("    </result>\n");
    out.extend_from_slice(s.as_bytes());
}

/// Emit the closing tags for the XML structure.
pub fn sparql_xml_footer(out: &mut Vec<u8>) {
    out.extend_from_slice(b"  </results>\n</sparql>\n");
}

// ---------------------------------------------------------------------------
// N-Triples (application/n-triples)
// ---------------------------------------------------------------------------

/// Emit a single triple as an N-Triples line.
///
/// Expects the row to have exactly 3 columns: (subject, predicate, object).
/// Each term is already in its N-Triples-compatible encoding (IRIs in `<>`,
/// literals in `""`).
pub fn ntriples_row(row: &RowRef, out: &mut Vec<u8>) {
    let mut iter = row.iter();
    let subject = iter.next().and_then(datum_to_str).unwrap_or("");
    let predicate = iter.next().and_then(datum_to_str).unwrap_or("");
    let object = iter.next().and_then(datum_to_str).unwrap_or("");

    // N-Triples: each line is `subject predicate object .`
    let mut s = String::with_capacity(subject.len() + predicate.len() + object.len() + 8);
    write!(
        s,
        "{} {} {} .\n",
        ntriples_term(subject),
        ntriples_term(predicate),
        ntriples_term(object)
    )
    .unwrap();
    out.extend_from_slice(s.as_bytes());
}

// ---------------------------------------------------------------------------
// Turtle (text/turtle)
// ---------------------------------------------------------------------------

/// Emit a single triple in Turtle format.
///
/// This is a simplified Turtle serializer that emits one triple per line
/// without subject grouping (which would require buffering). The result is
/// valid Turtle but not maximally compact.
pub fn turtle_row(row: &RowRef, out: &mut Vec<u8>) {
    let mut iter = row.iter();
    let subject = iter.next().and_then(datum_to_str).unwrap_or("");
    let predicate = iter.next().and_then(datum_to_str).unwrap_or("");
    let object = iter.next().and_then(datum_to_str).unwrap_or("");

    let mut s = String::with_capacity(subject.len() + predicate.len() + object.len() + 8);
    write!(
        s,
        "{} {} {} .\n",
        turtle_term(subject),
        turtle_term(predicate),
        turtle_term(object)
    )
    .unwrap();
    out.extend_from_slice(s.as_bytes());
}

// ---------------------------------------------------------------------------
// JSON-LD (application/ld+json)
// ---------------------------------------------------------------------------

/// Emit the opening bracket for a JSON-LD array.
pub fn jsonld_header(out: &mut Vec<u8>) {
    out.extend_from_slice(b"[\n");
}

/// Emit a single triple as a JSON-LD node.
///
/// Each triple becomes a minimal JSON-LD node:
/// ```json
/// {"@id": "subject", "predicate": [{"@value": "object"}]}
/// ```
///
/// For typed literals, `@type` is included. For language-tagged literals,
/// `@language` is included.
pub fn jsonld_row(row: &RowRef, row_index: usize, out: &mut Vec<u8>) {
    let mut iter = row.iter();
    let subject = iter.next().and_then(datum_to_str).unwrap_or("");
    let predicate = iter.next().and_then(datum_to_str).unwrap_or("");
    let object = iter.next().and_then(datum_to_str).unwrap_or("");

    let mut s = String::with_capacity(256);
    if row_index > 0 {
        s.push(',');
    }

    // Extract the subject IRI (strip angle brackets if present).
    let subject_id = strip_angles(subject);
    let predicate_id = strip_angles(predicate);

    write!(
        s,
        "{{\"@id\":\"{}\",\"{}\":[",
        json_escape(subject_id),
        json_escape(predicate_id)
    )
    .unwrap();

    match classify_term(object) {
        RdfTermKind::Uri(uri) => {
            write!(s, "{{\"@id\":\"{}\"}}", json_escape(uri)).unwrap();
        }
        RdfTermKind::Bnode(id) => {
            write!(s, "{{\"@id\":\"_:{}\"}}", json_escape(id)).unwrap();
        }
        RdfTermKind::TypedLiteral { value, datatype } => {
            write!(
                s,
                "{{\"@value\":\"{}\",\"@type\":\"{}\"}}",
                json_escape(value),
                json_escape(datatype)
            )
            .unwrap();
        }
        RdfTermKind::LangLiteral { value, lang } => {
            write!(
                s,
                "{{\"@value\":\"{}\",\"@language\":\"{}\"}}",
                json_escape(value),
                json_escape(lang)
            )
            .unwrap();
        }
        RdfTermKind::PlainLiteral(value) => {
            write!(s, "{{\"@value\":\"{}\"}}", json_escape(value)).unwrap();
        }
    }

    s.push_str("]}\n");
    out.extend_from_slice(s.as_bytes());
}

/// Emit the closing bracket for a JSON-LD array.
pub fn jsonld_footer(out: &mut Vec<u8>) {
    out.extend_from_slice(b"]\n");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn datum_to_str(d: mz_repr::Datum<'_>) -> Option<&str> {
    match d {
        mz_repr::Datum::String(s) => Some(s),
        _ => None,
    }
}

/// Strip angle brackets from an IRI term like `<http://example.org/>`.
fn strip_angles(s: &str) -> &str {
    if s.starts_with('<') && s.ends_with('>') {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Format a term for N-Triples output. IRIs are kept in angle brackets,
/// blank nodes as `_:id`, and everything else is treated as a literal.
fn ntriples_term(s: &str) -> String {
    if s.starts_with('<') && s.ends_with('>') {
        // Already an IRI in angle brackets
        s.to_string()
    } else if s.starts_with("_:") {
        // Blank node
        s.to_string()
    } else if s.starts_with('"') {
        // Already a quoted literal (possibly with datatype or lang tag)
        s.to_string()
    } else {
        // Unquoted value — wrap as xsd:string literal
        format!("\"{}\"", ntriples_escape(s))
    }
}

/// Format a term for Turtle output. Same as N-Triples for this simple
/// serializer (we don't abbreviate with prefixes).
fn turtle_term(s: &str) -> String {
    ntriples_term(s)
}

/// Escape special characters for JSON string values.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < '\x20' => write!(out, "\\u{:04x}", c as u32).unwrap(),
            c => out.push(c),
        }
    }
    out
}

/// Escape special characters for XML content.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// Escape special characters for N-Triples string literals.
fn ntriples_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::{Datum, Row};

    fn make_row(values: &[&str]) -> Row {
        let mut row = Row::default();
        let mut packer = row.packer();
        for v in values {
            packer.push(Datum::String(v));
        }
        row
    }

    fn make_row_with_null(values: &[Option<&str>]) -> Row {
        let mut row = Row::default();
        let mut packer = row.packer();
        for v in values {
            match v {
                Some(s) => packer.push(Datum::String(s)),
                None => packer.push(Datum::Null),
            }
        }
        row
    }

    #[test]
    fn test_classify_uri() {
        match classify_term("<http://example.org/foo>") {
            RdfTermKind::Uri(uri) => assert_eq!(uri, "http://example.org/foo"),
            _ => panic!("expected Uri"),
        }
    }

    #[test]
    fn test_classify_bnode() {
        match classify_term("_:b0") {
            RdfTermKind::Bnode(id) => assert_eq!(id, "b0"),
            _ => panic!("expected Bnode"),
        }
    }

    #[test]
    fn test_classify_typed_literal() {
        match classify_term("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>") {
            RdfTermKind::TypedLiteral { value, datatype } => {
                assert_eq!(value, "42");
                assert_eq!(datatype, "http://www.w3.org/2001/XMLSchema#integer");
            }
            _ => panic!("expected TypedLiteral"),
        }
    }

    #[test]
    fn test_classify_lang_literal() {
        match classify_term("\"hello\"@en") {
            RdfTermKind::LangLiteral { value, lang } => {
                assert_eq!(value, "hello");
                assert_eq!(lang, "en");
            }
            _ => panic!("expected LangLiteral"),
        }
    }

    #[test]
    fn test_classify_plain_literal() {
        match classify_term("\"hello\"") {
            RdfTermKind::PlainLiteral(v) => assert_eq!(v, "hello"),
            _ => panic!("expected PlainLiteral"),
        }
    }

    #[test]
    fn test_classify_unquoted_literal() {
        match classify_term("hello") {
            RdfTermKind::PlainLiteral(v) => assert_eq!(v, "hello"),
            _ => panic!("expected PlainLiteral"),
        }
    }

    #[test]
    fn test_sparql_json_select() {
        let vars = vec!["s", "p"];
        let mut out = Vec::new();
        sparql_json_header(&vars, &mut out);

        let row = make_row(&["<http://ex.org/Alice>", "<http://ex.org/knows>"]);
        sparql_json_row(&row, &vars, 0, &mut out);

        let row2 = make_row(&["<http://ex.org/Bob>", "\"hello\"@en"]);
        sparql_json_row(&row2, &vars, 1, &mut out);

        sparql_json_footer(&mut out);
        let result = String::from_utf8(out).unwrap();

        assert!(result.contains("\"head\":{\"vars\":[\"s\",\"p\"]}"));
        assert!(result.contains("\"type\":\"uri\""));
        assert!(result.contains("\"type\":\"literal\""));
        assert!(result.contains("\"xml:lang\":\"en\""));
        assert!(result.ends_with("}}\n"));
    }

    #[test]
    fn test_sparql_json_null_binding() {
        let vars = vec!["s", "o"];
        let mut out = Vec::new();
        sparql_json_header(&vars, &mut out);

        let row = make_row_with_null(&[Some("<http://ex.org/Alice>"), None]);
        sparql_json_row(&row, &vars, 0, &mut out);

        sparql_json_footer(&mut out);
        let result = String::from_utf8(out).unwrap();
        // Null bindings should be omitted per the W3C spec
        assert!(!result.contains("\"o\""));
    }

    #[test]
    fn test_sparql_xml_select() {
        let vars = vec!["s", "p"];
        let mut out = Vec::new();
        sparql_xml_header(&vars, &mut out);

        let row = make_row(&[
            "<http://ex.org/Alice>",
            "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        ]);
        sparql_xml_row(&row, &vars, &mut out);

        sparql_xml_footer(&mut out);
        let result = String::from_utf8(out).unwrap();

        assert!(result.contains("<?xml version=\"1.0\"?>"));
        assert!(result.contains("<variable name=\"s\"/>"));
        assert!(result.contains("<uri>http://ex.org/Alice</uri>"));
        assert!(result.contains("datatype=\"http://www.w3.org/2001/XMLSchema#integer\""));
        assert!(result.contains("</sparql>"));
    }

    #[test]
    fn test_ntriples_row() {
        let row = make_row(&[
            "<http://ex.org/Alice>",
            "<http://ex.org/knows>",
            "<http://ex.org/Bob>",
        ]);
        let mut out = Vec::new();
        ntriples_row(&row, &mut out);
        let result = String::from_utf8(out).unwrap();
        assert_eq!(
            result,
            "<http://ex.org/Alice> <http://ex.org/knows> <http://ex.org/Bob> .\n"
        );
    }

    #[test]
    fn test_ntriples_literal_object() {
        let row = make_row(&["<http://ex.org/Alice>", "<http://ex.org/name>", "\"Alice\""]);
        let mut out = Vec::new();
        ntriples_row(&row, &mut out);
        let result = String::from_utf8(out).unwrap();
        assert_eq!(
            result,
            "<http://ex.org/Alice> <http://ex.org/name> \"Alice\" .\n"
        );
    }

    #[test]
    fn test_ntriples_unquoted_literal() {
        let row = make_row(&["<http://ex.org/Alice>", "<http://ex.org/name>", "Alice"]);
        let mut out = Vec::new();
        ntriples_row(&row, &mut out);
        let result = String::from_utf8(out).unwrap();
        assert_eq!(
            result,
            "<http://ex.org/Alice> <http://ex.org/name> \"Alice\" .\n"
        );
    }

    #[test]
    fn test_turtle_row() {
        let row = make_row(&[
            "<http://ex.org/Alice>",
            "<http://ex.org/knows>",
            "<http://ex.org/Bob>",
        ]);
        let mut out = Vec::new();
        turtle_row(&row, &mut out);
        let result = String::from_utf8(out).unwrap();
        assert_eq!(
            result,
            "<http://ex.org/Alice> <http://ex.org/knows> <http://ex.org/Bob> .\n"
        );
    }

    #[test]
    fn test_jsonld_output() {
        let mut out = Vec::new();
        jsonld_header(&mut out);

        let row = make_row(&[
            "<http://ex.org/Alice>",
            "<http://ex.org/knows>",
            "<http://ex.org/Bob>",
        ]);
        jsonld_row(&row, 0, &mut out);

        let row2 = make_row(&[
            "<http://ex.org/Alice>",
            "<http://ex.org/name>",
            "\"Alice\"@en",
        ]);
        jsonld_row(&row2, 1, &mut out);

        jsonld_footer(&mut out);
        let result = String::from_utf8(out).unwrap();

        assert!(result.starts_with("[\n"));
        assert!(result.contains("\"@id\":\"http://ex.org/Alice\""));
        assert!(result.contains("\"@id\":\"http://ex.org/Bob\""));
        assert!(result.contains("\"@language\":\"en\""));
        assert!(result.ends_with("]\n"));
    }

    #[test]
    fn test_json_escape() {
        assert_eq!(json_escape("hello"), "hello");
        assert_eq!(json_escape("he\"llo"), "he\\\"llo");
        assert_eq!(json_escape("a\\b"), "a\\\\b");
        assert_eq!(json_escape("line\nnew"), "line\\nnew");
    }

    #[test]
    fn test_xml_escape() {
        assert_eq!(xml_escape("hello"), "hello");
        assert_eq!(xml_escape("<b>hi</b>"), "&lt;b&gt;hi&lt;/b&gt;");
        assert_eq!(xml_escape("a&b"), "a&amp;b");
        assert_eq!(xml_escape("say \"hi\""), "say &quot;hi&quot;");
    }

    #[test]
    fn test_ntriples_escape() {
        assert_eq!(ntriples_escape("hello"), "hello");
        assert_eq!(ntriples_escape("he\"llo"), "he\\\"llo");
        assert_eq!(ntriples_escape("a\nb"), "a\\nb");
    }
}
