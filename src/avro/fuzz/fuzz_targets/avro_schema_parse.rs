// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `mz_avro::Schema::from_str` parses an Avro schema from JSON —
//! the schema that arrives from an external, possibly hostile, schema registry
//! (or straight from a user's `CREATE SOURCE … USING SCHEMA`). It is a
//! hand-written recursive descent over the parsed JSON, with named-type
//! definition/reference resolution, aliases, logical types, and a
//! `MAX_SCHEMA_DEPTH` guard against stack overflow on deeply nested types.
//!
//! `schema_resolve` only parses the narrow set of schemas it generates (no
//! logical types, no named back-references, shallow), and the decode targets
//! cap nesting low. This one stresses the parser itself: it generates schema
//! JSON that can nest *past* the depth limit (so the guard must fire cleanly
//! rather than overflow the stack) and that re-references already-defined names
//! (recursive definitions — a distinct resolution path).
//!
//! It also drives the parser's *naming* and *validation* paths, which fixed
//! shallow shapes never touched:
//!   * `namespace` fields and dotted/`a.b.C` names — the `FullName::from_parts`
//!     split logic, including the documented edge case where a name has dots
//!     *and* a `namespace` is also given (they may disagree);
//!   * `aliases` arrays on named types, sometimes deliberately colliding with
//!     another defined name or with the type's own name;
//!   * structurally-valid-but-semantically-invalid schemas the parser is meant
//!     to *reject* (not panic on): duplicate enum symbols, duplicate record
//!     field names, decimal `scale > precision`, and `fixed` `size: 0`.
//! Parsing must never panic; it returns `Ok`/`Err`.

#![no_main]

use std::str::FromStr;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_avro::Schema;

const PRIMITIVES: &[&str] = &[
    "null", "boolean", "int", "long", "float", "double", "bytes", "string",
];

/// Generate the optional `name`-affecting JSON attributes for a named type:
/// the `name` itself (sometimes dotted, i.e. namespace-qualified inline), an
/// optional separate `namespace`, and an optional `aliases` array (sometimes
/// colliding with another defined name or with the type's own name). The
/// returned `name` is the bare leaf name to record in `defined` for later
/// back-references. Exercises `Name::parse` / `FullName::from_parts`, including
/// the dotted-name-plus-`namespace` edge case the source flags as `[XXX]`.
fn gen_named_attrs(
    u: &mut Unstructured,
    leaf: &str,
    defined: &[String],
) -> arbitrary::Result<String> {
    let mut attrs = String::new();

    // The `name`: usually bare, sometimes dotted (carries an inline namespace),
    // which drives the rfind('.') split in `FullName::from_parts`.
    match u.int_in_range(0u8..=3)? {
        0 => attrs.push_str(&format!("\"name\":\"{leaf}\"")),
        1 => attrs.push_str(&format!("\"name\":\"ns.{leaf}\"")),
        2 => attrs.push_str(&format!("\"name\":\"a.b.{leaf}\"")),
        // Leading dot → empty namespace component.
        _ => attrs.push_str(&format!("\"name\":\".{leaf}\"")),
    }

    // Optional separate `namespace`. Combined with a dotted name above, this
    // hits the case where the computed and provided namespaces may disagree.
    match u.int_in_range(0u8..=2)? {
        0 => {}
        1 => attrs.push_str(",\"namespace\":\"ns\""),
        _ => attrs.push_str(",\"namespace\":\"other\""),
    }

    // Optional `aliases` array.
    match u.int_in_range(0u8..=4)? {
        0 | 1 => {}
        2 => attrs.push_str(",\"aliases\":[\"AnAlias\"]"),
        // Alias colliding with the type's own (leaf) name.
        3 => attrs.push_str(&format!(",\"aliases\":[\"{leaf}\"]")),
        // Alias colliding with some other already-defined name, if any.
        _ => {
            if let Some(other) = defined.last() {
                attrs.push_str(&format!(",\"aliases\":[\"{other}\"]"));
            } else {
                attrs.push_str(",\"aliases\":[\"AnAlias\"]");
            }
        }
    }

    Ok(attrs)
}

/// Generate a schema that is *structurally* valid JSON for a named type but
/// *semantically* invalid per the Avro spec — the parser must reject it with an
/// error rather than panic.
fn gen_invalid(u: &mut Unstructured, counter: &mut u32) -> arbitrary::Result<String> {
    *counter += 1;
    Ok(match u.int_in_range(0u8..=4)? {
        // Enum with duplicate symbols.
        0 => format!(
            "{{\"type\":\"enum\",\"name\":\"E{counter}\",\"symbols\":[\"A\",\"B\",\"A\"]}}"
        ),
        // Record with duplicate field names.
        1 => format!(
            "{{\"type\":\"record\",\"name\":\"R{counter}\",\"fields\":[\
             {{\"name\":\"dup\",\"type\":\"int\"}},\
             {{\"name\":\"dup\",\"type\":\"string\"}}]}}"
        ),
        // Decimal with scale > precision.
        2 => "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":2,\"scale\":9}"
            .to_string(),
        // Fixed with size 0.
        3 => format!("{{\"type\":\"fixed\",\"name\":\"F{counter}\",\"size\":0}}"),
        // Enum default not among the symbols.
        _ => format!(
            "{{\"type\":\"enum\",\"name\":\"E{counter}\",\"symbols\":[\"A\",\"B\"],\"default\":\"Z\"}}"
        ),
    })
}

/// A leaf type: a primitive, a logical type, or a reference to an
/// already-defined named type (which makes the schema recursive).
fn gen_leaf(u: &mut Unstructured, defined: &[String]) -> arbitrary::Result<String> {
    Ok(match u.int_in_range(0u8..=7)? {
        0 => format!("\"{}\"", u.choose(PRIMITIVES)?),
        1 => "{\"type\":\"int\",\"logicalType\":\"date\"}".to_string(),
        2 => "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}".to_string(),
        3 => "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}".to_string(),
        4 => "{\"type\":\"string\",\"logicalType\":\"uuid\"}".to_string(),
        5 => {
            let precision = u.int_in_range(1u32..=38)?;
            let scale = u.int_in_range(0u32..=precision)?;
            format!(
                "{{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":{precision},\"scale\":{scale}}}"
            )
        }
        // Reference a previously-defined name when one exists (recursive
        // definition); otherwise fall back to a primitive.
        _ if !defined.is_empty() => format!("\"{}\"", u.choose(defined)?),
        _ => format!("\"{}\"", u.choose(PRIMITIVES)?),
    })
}

fn gen_type(
    u: &mut Unstructured,
    counter: &mut u32,
    defined: &mut Vec<String>,
    depth: u32,
) -> arbitrary::Result<String> {
    if depth == 0 || u.is_empty() {
        return gen_leaf(u, defined);
    }
    Ok(match u.int_in_range(0u8..=11)? {
        0 | 1 => gen_leaf(u, defined)?,
        // Array / map / union — the cheap recursive wrappers that let the
        // fuzzer drive nesting depth (potentially past MAX_SCHEMA_DEPTH).
        2 => format!(
            "{{\"type\":\"array\",\"items\":{}}}",
            gen_type(u, counter, defined, depth - 1)?
        ),
        3 => format!(
            "{{\"type\":\"map\",\"values\":{}}}",
            gen_type(u, counter, defined, depth - 1)?
        ),
        4 => format!("[{}]", gen_type(u, counter, defined, depth - 1)?),
        5 => format!("[\"null\",{}]", gen_type(u, counter, defined, depth - 1)?),
        // Record — defines a new name (recorded so later leaves can reference
        // it). The name may be dotted/namespaced and may carry `aliases`.
        6 | 7 => {
            *counter += 1;
            let leaf = format!("R{counter}");
            let attrs = gen_named_attrs(u, &leaf, defined)?;
            defined.push(leaf);
            let n = u.int_in_range(0u8..=3)?;
            let mut fields = Vec::with_capacity(n.into());
            for i in 0..n {
                let ty = gen_type(u, counter, defined, depth - 1)?;
                fields.push(format!("{{\"name\":\"f{i}\",\"type\":{ty}}}"));
            }
            format!(
                "{{\"type\":\"record\",{attrs},\"fields\":[{}]}}",
                fields.join(",")
            )
        }
        // Enum — also name-attribute-bearing; sometimes with a `default` symbol.
        8 => {
            *counter += 1;
            let leaf = format!("E{counter}");
            let attrs = gen_named_attrs(u, &leaf, defined)?;
            defined.push(leaf);
            let default = if u.int_in_range(0u8..=1)? == 0 {
                ",\"default\":\"B\""
            } else {
                ""
            };
            format!("{{\"type\":\"enum\",{attrs},\"symbols\":[\"A\",\"B\",\"C\"]{default}}}")
        }
        // Fixed.
        9 => {
            *counter += 1;
            let leaf = format!("F{counter}");
            let attrs = gen_named_attrs(u, &leaf, defined)?;
            defined.push(leaf);
            let size = u.int_in_range(0u32..=32)?;
            format!("{{\"type\":\"fixed\",{attrs},\"size\":{size}}}")
        }
        // A structurally-valid-but-semantically-invalid named type: the parser
        // must reject it cleanly rather than panic.
        _ => gen_invalid(u, counter)?,
    })
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let mut counter = 0u32;
    let mut defined = Vec::new();
    // Start deeper than MAX_SCHEMA_DEPTH (128) so the fuzzer can drive nesting
    // past the limit and exercise the depth guard, not just shallow schemas.
    let depth = u.int_in_range(1u32..=200)?;
    let schema = gen_type(&mut u, &mut counter, &mut defined, depth)?;
    let _ = Schema::from_str(&schema);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
