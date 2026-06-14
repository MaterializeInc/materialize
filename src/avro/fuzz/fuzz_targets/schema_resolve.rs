// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `resolve_schemas` reconciles a writer schema with a reader
//! schema (the core of decoding Kafka Avro data whose writer schema came from
//! an external, possibly hostile, schema registry). It walks both schemas in
//! lock-step doing type promotion, default substitution, and union matching, so
//! a panic here is an availability bug for source ingestion.
//!
//! Random bytes almost never parse as an Avro schema, so we generate *valid*
//! schema JSON from the fuzz input. Earlier this target drew two *independent*
//! schemas — but two independently-named random schemas almost never line up,
//! so resolution failed at the very first node (a record/enum/fixed name
//! mismatch) and the interesting resolve branches stayed dead.
//!
//! Instead we generate one structured shape (`Shape`) and emit *two* JSON
//! renderings of it: a writer rendering and a reader rendering that share all
//! record/enum/fixed names but deliberately differ in ways the resolver is
//! supposed to handle, so we actually reach its non-trivial paths:
//!   * primitive promotion — a writer `int` rendered as reader `long`/`float`/
//!     `double`, `long`→`float`/`double`, `float`→`double` (the `ResolveIntLong`
//!     / `ResolveFloatDouble` / … machinery);
//!   * default substitution — the reader record sometimes carries an *extra*
//!     trailing field (absent from the writer) with a JSON `default`, driving
//!     the "reader field not in writer, use default" branch in `resolve_named`;
//!   * union matching — multi-variant unions whose variants the resolver must
//!     match up by type/name across writer and reader;
//!   * enums with a `default` symbol.
//! We resolve writer-against-itself (identity), and both cross-directions. None
//! of it may panic.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_avro::Schema;
use mz_avro::schema::resolve_schemas;

/// One of the primitive Avro types, ordered by promotability so the reader
/// rendering can pick a "wider" target. `int` ⊑ `long` ⊑ `float` ⊑ `double`.
const PROMO_CHAIN: &[&str] = &["int", "long", "float", "double"];
const OTHER_PRIMS: &[&str] = &["null", "boolean", "bytes", "string"];

/// A structured schema shape. Generated once, then rendered twice (writer /
/// reader) with controlled per-rendering variation. `names` are stable across
/// both renderings so named types line up during resolution.
enum Shape {
    /// A primitive on the promotion chain (index into `PROMO_CHAIN`).
    Promotable(usize),
    /// A primitive that has no promotion (rendered identically on both sides).
    OtherPrim(&'static str),
    /// `[..]` union with N>=1 variants.
    Union(Vec<Shape>),
    Array(Box<Shape>),
    Map(Box<Shape>),
    Record {
        name: u32,
        fields: Vec<Shape>,
        /// Whether the reader rendering appends an extra defaulted field.
        reader_extra_default: bool,
    },
    Enum {
        name: u32,
        /// Whether the reader rendering gives the enum a `default` symbol.
        reader_default: bool,
    },
    Fixed {
        name: u32,
        size: u8,
    },
}

fn gen_shape(u: &mut Unstructured, counter: &mut u32, depth: u32) -> arbitrary::Result<Shape> {
    let choice = if depth == 0 || u.is_empty() {
        u.int_in_range(0u8..=1)?
    } else {
        u.int_in_range(0u8..=8)?
    };
    Ok(match choice {
        0 => Shape::Promotable(usize::from(u.int_in_range(0u8..=3)?)),
        1 => Shape::OtherPrim(u.choose(OTHER_PRIMS)?),
        2 => {
            // Multi-variant union. We draw distinct primitive-ish variants so
            // the union stays valid (Avro forbids duplicate non-named types).
            let n = u.int_in_range(1u8..=3)?;
            let mut variants = Vec::with_capacity(n.into());
            // First variant is often null (the common nullable shape).
            if u.int_in_range(0u8..=1)? == 0 {
                variants.push(Shape::OtherPrim("null"));
            }
            // Then a small set of distinct promotable primitives.
            let mut used = [false; 4];
            for _ in 0..n {
                let idx = usize::from(u.int_in_range(0u8..=3)?);
                if !used[idx] {
                    used[idx] = true;
                    variants.push(Shape::Promotable(idx));
                }
            }
            if variants.is_empty() {
                variants.push(Shape::Promotable(0));
            }
            Shape::Union(variants)
        }
        3 => Shape::Array(Box::new(gen_shape(u, counter, depth - 1)?)),
        4 => Shape::Map(Box::new(gen_shape(u, counter, depth - 1)?)),
        5 | 6 => {
            *counter += 1;
            let name = *counter;
            let n = u.int_in_range(0u8..=3)?;
            let mut fields = Vec::with_capacity(n.into());
            for _ in 0..n {
                fields.push(gen_shape(u, counter, depth - 1)?);
            }
            Shape::Record {
                name,
                fields,
                reader_extra_default: u.int_in_range(0u8..=1)? == 0,
            }
        }
        7 => {
            *counter += 1;
            Shape::Enum {
                name: *counter,
                reader_default: u.int_in_range(0u8..=1)? == 0,
            }
        }
        _ => {
            *counter += 1;
            Shape::Fixed {
                name: *counter,
                size: u.int_in_range(1u8..=16)?,
            }
        }
    })
}

/// Render the *writer* version of `shape` to schema JSON: promotable
/// primitives use their base type, records carry only their real fields, and
/// enums have no default.
fn render_writer(shape: &Shape, out: &mut String) {
    match shape {
        Shape::Promotable(idx) => {
            out.push('"');
            out.push_str(PROMO_CHAIN[*idx]);
            out.push('"');
        }
        Shape::OtherPrim(p) => {
            out.push('"');
            out.push_str(p);
            out.push('"');
        }
        Shape::Union(variants) => {
            out.push('[');
            for (i, v) in variants.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                render_writer(v, out);
            }
            out.push(']');
        }
        Shape::Array(item) => {
            out.push_str("{\"type\":\"array\",\"items\":");
            render_writer(item, out);
            out.push('}');
        }
        Shape::Map(values) => {
            out.push_str("{\"type\":\"map\",\"values\":");
            render_writer(values, out);
            out.push('}');
        }
        Shape::Record { name, fields, .. } => {
            out.push_str(&format!("{{\"type\":\"record\",\"name\":\"N{name}\",\"fields\":["));
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&format!("{{\"name\":\"f{i}\",\"type\":"));
                render_writer(f, out);
                out.push('}');
            }
            out.push_str("]}");
        }
        Shape::Enum { name, .. } => {
            out.push_str(&format!(
                "{{\"type\":\"enum\",\"name\":\"N{name}\",\"symbols\":[\"A\",\"B\",\"C\"]}}"
            ));
        }
        Shape::Fixed { name, size } => {
            out.push_str(&format!(
                "{{\"type\":\"fixed\",\"name\":\"N{name}\",\"size\":{size}}}"
            ));
        }
    }
}

/// Render the *reader* version of `shape`: widens each promotable primitive to
/// a (fuzz-chosen) wider type on the promotion chain, appends a defaulted
/// `extra` record field, and gives enums a `default` symbol — all the
/// schema-evolution shapes `resolve_schemas` handles.
fn render_reader_promoted(
    u: &mut Unstructured,
    shape: &Shape,
    out: &mut String,
) -> arbitrary::Result<()> {
    match shape {
        Shape::Promotable(idx) => {
            // Choose a target at or after `idx` on the chain — a valid
            // promotion the resolver should accept.
            let target = u.int_in_range(*idx..=PROMO_CHAIN.len() - 1)?;
            out.push('"');
            out.push_str(PROMO_CHAIN[target]);
            out.push('"');
        }
        Shape::OtherPrim(p) => {
            out.push('"');
            out.push_str(p);
            out.push('"');
        }
        Shape::Union(variants) => {
            out.push('[');
            for (i, v) in variants.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                render_reader_promoted(u, v, out)?;
            }
            out.push(']');
        }
        Shape::Array(item) => {
            out.push_str("{\"type\":\"array\",\"items\":");
            render_reader_promoted(u, item, out)?;
            out.push('}');
        }
        Shape::Map(values) => {
            out.push_str("{\"type\":\"map\",\"values\":");
            render_reader_promoted(u, values, out)?;
            out.push('}');
        }
        Shape::Record {
            name,
            fields,
            reader_extra_default,
        } => {
            out.push_str(&format!("{{\"type\":\"record\",\"name\":\"N{name}\",\"fields\":["));
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&format!("{{\"name\":\"f{i}\",\"type\":"));
                render_reader_promoted(u, f, out)?;
                out.push('}');
            }
            if *reader_extra_default {
                if !fields.is_empty() {
                    out.push(',');
                }
                out.push_str("{\"name\":\"extra\",\"type\":\"long\",\"default\":7}");
            }
            out.push_str("]}");
        }
        Shape::Enum { name, reader_default } => {
            out.push_str(&format!(
                "{{\"type\":\"enum\",\"name\":\"N{name}\",\"symbols\":[\"A\",\"B\",\"C\"]"
            ));
            if *reader_default {
                out.push_str(",\"default\":\"A\"");
            }
            out.push_str("}");
        }
        Shape::Fixed { name, size } => {
            out.push_str(&format!(
                "{{\"type\":\"fixed\",\"name\":\"N{name}\",\"size\":{size}}}"
            ));
        }
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let mut counter = 0u32;
    // The top level of an OCF/registry schema is virtually always a record.
    counter += 1;
    let top_name = counter;
    let n = u.int_in_range(0u8..=4)?;
    let mut fields = Vec::with_capacity(n.into());
    for _ in 0..n {
        fields.push(gen_shape(&mut u, &mut counter, 3)?);
    }
    let shape = Shape::Record {
        name: top_name,
        fields,
        reader_extra_default: u.int_in_range(0u8..=1)? == 0,
    };

    let mut writer_json = String::new();
    render_writer(&shape, &mut writer_json);
    let mut reader_json = String::new();
    render_reader_promoted(&mut u, &shape, &mut reader_json)?;

    let Ok(writer) = writer_json.parse::<Schema>() else {
        return Ok(());
    };
    // Identity resolution must succeed without panicking.
    let _ = resolve_schemas(&writer, &writer);
    if let Ok(reader) = reader_json.parse::<Schema>() {
        // The evolved direction (writer→reader) should hit promotion / default
        // / union-match branches; the reverse exercises the failure paths.
        let _ = resolve_schemas(&writer, &reader);
        let _ = resolve_schemas(&reader, &writer);
        let _ = resolve_schemas(&reader, &reader);
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
