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
//! schema JSON from the fuzz input. Two independently-named random schemas
//! almost never line up, so resolution would fail at the very first node (a
//! record/enum/fixed name mismatch) and the interesting resolve branches would
//! stay dead.
//!
//! Instead we generate one structured shape (`Shape`) and emit *two* JSON
//! renderings of it: a writer rendering and a reader rendering that share all
//! record/enum/fixed names but deliberately differ in ways the resolver is
//! supposed to handle, so we actually reach its non-trivial paths:
//!   * primitive promotion. A writer `int` rendered as reader `long`/`float`/
//!     `double`, `long`→`float`/`double`, `float`→`double` (the `ResolveIntLong`
//!     / `ResolveFloatDouble` / … machinery).
//!   * default substitution. The reader record sometimes carries an *extra*
//!     trailing field (absent from the writer) with a JSON `default`, driving
//!     the "reader field not in writer, use default" branch in `resolve_named`.
//!   * union matching. Multi-variant unions whose variants the resolver must
//!     match up across writer and reader, by type for the primitive variants
//!     and by *name* for the record/enum/fixed ones. The reader rendering also
//!     sometimes collapses a union down to the one variant the encoder
//!     expresses (`ResolveUnionConcrete`) and sometimes wraps a concrete node
//!     in a union (`ResolveConcreteUnion`), so all three union arms of
//!     `SchemaResolver::resolve` are reachable, not just union-against-union.
//!   * enums with a `default` symbol, whose reader rendering also drops
//!     trailing symbols. A dropped symbol is what makes the `default`
//!     load-bearing: it becomes an `Err` entry in the resolved enum that decode
//!     substitutes the default for.
//! We resolve writer-against-itself (identity), and both cross-directions.
//!
//! A panic is not the only failure mode, though. `resolve_schemas` can return
//! `Ok` while *deferring* a match failure into the resolved schema. It might
//! store an `Err` inside a `ResolveUnionUnion` permutation, say, which then
//! re-raises only when a record actually expresses that branch at decode time.
//! That is exactly the shape of <https://github.com/MaterializeInc/materialize/pull/37087>:
//! an `int`→`double` promotion inside a `["null", T]` union resolved to `Ok`
//! but failed to decode. A "doesn't panic" oracle that discards the `Result`
//! sees nothing wrong. So beyond requiring no panic, we add a *decode* oracle:
//! the reader rendering only ever widens the writer, so every node, every
//! union branch included, has a valid reader target, and decoding a
//! writer-encoded value through the writer→reader resolved schema MUST succeed
//! and MUST consume every byte the writer wrote. A deferred mismatch turns that
//! decode into an error, which this target treats as a finding.
//!
//! Because the reader rendering only widens, both renderings are valid schema
//! JSON, identity resolution succeeds, and writer→reader resolution succeeds,
//! all by construction. Those are `expect`s rather than `if let Ok(..)` guards
//! on purpose. A promotion that regresses at a *non-union* position (a record
//! field, an array item, a map value) surfaces as a top-level `Err` from
//! `resolve_schemas`, not as a deferred per-variant `Err`, so a guard there
//! would switch the decode oracle off for exactly the inputs that found a bug.
//! Only the narrowing reader→writer direction may legitimately fail.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_avro::schema::resolve_schemas;
use mz_avro::{Schema, from_avro_datum};

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
    Union {
        variants: Vec<Shape>,
        /// The variant the encoder expresses on the wire. Drawn from the fuzz
        /// input: decoding a `ResolveUnionUnion` only ever consults the
        /// `permutation` entry for the encoded index, so a branch fixed by the
        /// generator would leave every other entry's deferred `Err` unobserved.
        branch: usize,
        /// Whether the reader rendering drops the union down to `branch` alone.
        reader_collapse: bool,
    },
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
        /// How many trailing symbols the reader rendering omits. Only ever
        /// nonzero when `reader_default` is set: dropping a symbol the writer
        /// can express is a widening exactly because the default catches it.
        reader_dropped: u8,
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
            // Named variants get distinct names, and a union's duplicate check
            // keys named variants by name, so any number of them is valid even
            // when several are the same kind of named type. They are the only
            // way to reach the name-keyed side of the resolver's variant
            // matching.
            for _ in 0..u.int_in_range(0u8..=2)? {
                variants.push(gen_named(u, counter, depth - 1)?);
            }
            let branch = u.int_in_range(0..=variants.len() - 1)?;
            Shape::Union {
                variants,
                branch,
                // A starved `Unstructured` answers every `int_in_range` with the
                // low end, so testing against the high end keeps collapsing
                // rare rather than universal once the input runs out.
                reader_collapse: u.int_in_range(0u8..=3)? == 3,
            }
        }
        3 => Shape::Array(Box::new(gen_shape(u, counter, depth - 1)?)),
        4 => Shape::Map(Box::new(gen_shape(u, counter, depth - 1)?)),
        _ => gen_named(u, counter, depth - 1)?,
    })
}

/// Generate one *named* shape. Named types are what the resolver matches up by
/// name rather than by type, both at ordinary positions and as union variants.
fn gen_named(u: &mut Unstructured, counter: &mut u32, depth: u32) -> arbitrary::Result<Shape> {
    let kind = u.int_in_range(0u8..=3)?;
    *counter += 1;
    let name = *counter;
    Ok(match kind {
        0 | 1 => {
            let n = u.int_in_range(0u8..=3)?;
            let mut fields = Vec::with_capacity(n.into());
            for _ in 0..n {
                fields.push(gen_shape(u, counter, depth)?);
            }
            Shape::Record {
                name,
                fields,
                reader_extra_default: u.int_in_range(0u8..=1)? == 0,
            }
        }
        2 => {
            let reader_default = u.int_in_range(0u8..=1)? == 0;
            Shape::Enum {
                name,
                reader_default,
                reader_dropped: if reader_default {
                    u.int_in_range(0u8..=2)?
                } else {
                    0
                },
            }
        }
        _ => Shape::Fixed {
            name,
            size: u.int_in_range(1u8..=16)?,
        },
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
        Shape::Union { variants, .. } => {
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
            out.push_str(&format!(
                "{{\"type\":\"record\",\"name\":\"N{name}\",\"fields\":["
            ));
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
/// `extra` record field, drops trailing enum symbols in favour of a `default`,
/// and rewrites unions into and out of concrete types. These are all the
/// schema-evolution shapes `resolve_schemas` handles.
///
/// `in_union` says whether `shape` sits directly inside a union, which forbids
/// wrapping it in another one.
fn render_reader_promoted(
    u: &mut Unstructured,
    shape: &Shape,
    in_union: bool,
    out: &mut String,
) -> arbitrary::Result<()> {
    // Reading a concrete writer node as a union that contains a match for it is
    // a widening, and the only way to reach `ResolveConcreteUnion`. Avro rejects
    // a union directly inside a union, and `["null","null"]` is a duplicate
    // variant, hence the two exclusions. Testing against the high end of the
    // range keeps wrapping rare once the input is exhausted rather than
    // wrapping every single node.
    if !in_union
        && !matches!(shape, Shape::Union { .. } | Shape::OtherPrim("null"))
        && u.int_in_range(0u8..=3)? == 3
    {
        out.push_str("[\"null\",");
        render_reader_promoted(u, shape, true, out)?;
        out.push(']');
        return Ok(());
    }
    match shape {
        Shape::Promotable(idx) => {
            // Choose a target at or after `idx` on the chain, a valid
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
        Shape::Union {
            variants,
            branch,
            reader_collapse,
        } if *reader_collapse => {
            // Drop the union down to the single variant the encoder expresses,
            // so a union writer resolves against a concrete reader
            // (`ResolveUnionConcrete`). That variant is rendered in its *writer*
            // form on purpose. Resolution records the one writer variant that
            // matches the reader's concrete type, and decoding then rejects any
            // other encoded index. Since a union's variants are unique per type
            // and per name, an exact rendering is what guarantees the match lands
            // on `branch`. A widened one can select a different variant, turning
            // a correct decode into a wrong-index error: `["int","long"]` with
            // the `long` branch encoded, read as `"float"`, matches `int`, which
            // comes first and promotes to `float` just as well.
            render_writer(&variants[*branch], out);
        }
        Shape::Union { variants, .. } => {
            // Widen the promotable variants while keeping their types distinct.
            // Widening each independently could collapse two variants onto the
            // same type (`[int, long]` becoming `[long, long]`), which Avro
            // rejects as a duplicate union type, and an unparseable reader
            // rendering fails the parse assertion in `run`. We emit the
            // non-promotable variants (`null` and the named ones) first, then
            // the promotables in ascending source order with strictly
            // increasing targets. Reserving one chain slot for every later
            // variant keeps a valid assignment reachable, since PROMO_CHAIN has
            // one slot per possible source index.
            out.push('[');
            let mut written = 0;
            for v in variants
                .iter()
                .filter(|v| !matches!(v, Shape::Promotable(_)))
            {
                if written > 0 {
                    out.push(',');
                }
                render_reader_promoted(u, v, true, out)?;
                written += 1;
            }
            let mut promo: Vec<usize> = variants
                .iter()
                .filter_map(|v| match v {
                    Shape::Promotable(idx) => Some(*idx),
                    _ => None,
                })
                .collect();
            promo.sort_unstable();
            let m = promo.len();
            let mut min_target = 0;
            for (k, idx) in promo.into_iter().enumerate() {
                if written > 0 {
                    out.push(',');
                }
                let lo = idx.max(min_target);
                let hi = (PROMO_CHAIN.len() - 1) - (m - 1 - k);
                let target = u.int_in_range(lo..=hi)?;
                min_target = target + 1;
                out.push('"');
                out.push_str(PROMO_CHAIN[target]);
                out.push('"');
                written += 1;
            }
            out.push(']');
        }
        Shape::Array(item) => {
            out.push_str("{\"type\":\"array\",\"items\":");
            render_reader_promoted(u, item, false, out)?;
            out.push('}');
        }
        Shape::Map(values) => {
            out.push_str("{\"type\":\"map\",\"values\":");
            render_reader_promoted(u, values, false, out)?;
            out.push('}');
        }
        Shape::Record {
            name,
            fields,
            reader_extra_default,
        } => {
            out.push_str(&format!(
                "{{\"type\":\"record\",\"name\":\"N{name}\",\"fields\":["
            ));
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&format!("{{\"name\":\"f{i}\",\"type\":"));
                render_reader_promoted(u, f, false, out)?;
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
        Shape::Enum {
            name,
            reader_default,
            reader_dropped,
        } => {
            // Symbols are dropped from the tail, which keeps `A` around as the
            // default. A writer symbol the reader lacks becomes an `Err` entry
            // in the resolved enum, and decoding it substitutes the default.
            let symbols = match *reader_dropped {
                0 => "\"A\",\"B\",\"C\"",
                1 => "\"A\",\"B\"",
                _ => "\"A\"",
            };
            out.push_str(&format!(
                "{{\"type\":\"enum\",\"name\":\"N{name}\",\"symbols\":[{symbols}]"
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

/// Avro encodes `int`/`long` (and union branch indices, array/map block
/// counts, and blob lengths) as zig-zag varints.
fn encode_long(n: i64, out: &mut Vec<u8>) {
    let mut z = ((n << 1) ^ (n >> 63)) as u64;
    loop {
        if z & !0x7f == 0 {
            out.push(z as u8);
            return;
        }
        out.push(((z & 0x7f) | 0x80) as u8);
        z >>= 7;
    }
}

/// Length-prefixed bytes, the wire form of `string`, `bytes`, and each map key.
fn encode_blob(bytes: &[u8], out: &mut Vec<u8>) {
    encode_long(bytes.len() as i64, out);
    out.extend_from_slice(bytes);
}

/// Avro-binary-encode one value matching the *writer* rendering of `shape`
/// (i.e. `render_writer`'s wire format), so it can be decoded back through a
/// resolved schema. A union is encoded at its `Shape::Union::branch`, which the
/// generator drew from the fuzz input: decoding consults only the resolved
/// permutation entry for the encoded index, so a deferred `Err` parked in any
/// other entry, the failure mode of #37087, is invisible unless the branch
/// varies.
fn encode_writer_value(
    u: &mut Unstructured,
    shape: &Shape,
    out: &mut Vec<u8>,
) -> arbitrary::Result<()> {
    match shape {
        Shape::Promotable(idx) => match PROMO_CHAIN[*idx] {
            "int" => encode_long(i64::from(u.arbitrary::<i32>()?), out),
            "long" => encode_long(u.arbitrary::<i64>()?, out),
            "float" => out.extend_from_slice(&u.arbitrary::<f32>()?.to_le_bytes()),
            // "double"
            _ => out.extend_from_slice(&u.arbitrary::<f64>()?.to_le_bytes()),
        },
        Shape::OtherPrim(p) => match *p {
            "null" => {}
            "boolean" => out.push(u.int_in_range(0u8..=1)?),
            // "bytes" | "string": both length-prefixed on the wire.
            _ => {
                let n = u.int_in_range(0usize..=8)?;
                let mut b = Vec::with_capacity(n);
                for _ in 0..n {
                    b.push(u.int_in_range(0x20u8..=0x7e)?);
                }
                encode_blob(&b, out);
            }
        },
        Shape::Union {
            variants, branch, ..
        } => {
            encode_long(*branch as i64, out);
            encode_writer_value(u, &variants[*branch], out)?;
        }
        Shape::Array(item) => {
            let n = u.int_in_range(0i64..=3)?;
            if n > 0 {
                encode_long(n, out);
                for _ in 0..n {
                    encode_writer_value(u, item, out)?;
                }
            }
            encode_long(0, out); // end-of-array block marker
        }
        Shape::Map(values) => {
            let n = u.int_in_range(0i64..=3)?;
            if n > 0 {
                encode_long(n, out);
                for _ in 0..n {
                    let k = u.int_in_range(0usize..=4)?;
                    let mut key = Vec::with_capacity(k);
                    for _ in 0..k {
                        key.push(u.int_in_range(0x61u8..=0x7a)?);
                    }
                    encode_blob(&key, out);
                    encode_writer_value(u, values, out)?;
                }
            }
            encode_long(0, out); // end-of-map block marker
        }
        Shape::Record { fields, .. } => {
            for f in fields {
                encode_writer_value(u, f, out)?;
            }
        }
        // The writer enum's symbols are always `["A","B","C"]`.
        Shape::Enum { .. } => encode_long(u.int_in_range(0i64..=2)?, out),
        Shape::Fixed { size, .. } => {
            for _ in 0..*size {
                out.push(u.arbitrary::<u8>()?);
            }
        }
    }
    Ok(())
}

/// Decode `value` through `resolved`, requiring both that the decode succeeds
/// and that it consumes every byte the writer wrote. Leftover bytes mean the
/// resolved schema drove the decoder over less than the writer's value, for
/// instance a promotion of the wrong width or a field the resolver dropped
/// without skipping its bytes, which the tail of a record would otherwise hide.
fn decode_fully(resolved: &Schema, value: &[u8], what: &str) {
    let mut cursor = value;
    match from_avro_datum(resolved, &mut cursor) {
        Ok(_) => assert!(
            cursor.is_empty(),
            "decoding the writer's bytes through the {what} left {} of {} bytes unconsumed",
            cursor.len(),
            value.len(),
        ),
        Err(e) => panic!("decoding the writer's bytes through the {what} failed: {e}"),
    }
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
    render_reader_promoted(&mut u, &shape, false, &mut reader_json)?;

    // Both renderings are valid schema JSON by construction, so a parse failure
    // is a generator bug. Reporting it beats skipping the input, which would
    // turn the decode oracle below off without a trace.
    let writer = writer_json
        .parse::<Schema>()
        .unwrap_or_else(|e| panic!("writer rendering {writer_json} must parse: {e}"));
    let reader = reader_json
        .parse::<Schema>()
        .unwrap_or_else(|e| panic!("reader rendering {reader_json} must parse: {e}"));

    // Encode one value against the writer rendering. We decode it back through
    // the resolved schemas below. The writer only ever widens into the reader,
    // so every node, every union branch included, has a valid reader target
    // and decoding the writer's own bytes through the resolved schema must
    // succeed. A `resolve_schemas` that returned `Ok` but deferred a fixable
    // mismatch (see the module comment / #37087) surfaces it here as a decode
    // error rather than slipping past a panic-only oracle.
    let mut writer_value = Vec::new();
    encode_writer_value(&mut u, &shape, &mut writer_value)?;

    // Identity resolution is the control: a schema always resolves against
    // itself, and the resolved form always decodes what the writer wrote.
    let resolved = resolve_schemas(&writer, &writer)
        .unwrap_or_else(|e| panic!("identity resolution of {writer_json} must succeed: {e}"));
    decode_fully(
        &resolved,
        &writer_value,
        &format!("identity resolution of {writer_json}"),
    );

    // Writer→reader is the *widening* direction: it hits the promotion /
    // default / union-match branches, must resolve, and the resolved schema must
    // decode the writer's bytes. A promotion the resolver rejects outright fails
    // the `resolve_schemas` assertion, and one it accepts but defers into the
    // resolved schema fails the decode.
    let resolved = resolve_schemas(&writer, &reader).unwrap_or_else(|e| {
        panic!("the reader rendering only widens, so resolving {writer_json} against {reader_json} must succeed: {e}")
    });
    decode_fully(
        &resolved,
        &writer_value,
        &format!("resolution of {writer_json} against {reader_json}"),
    );

    // The reverse (reader→writer) *narrows*, so its resolution may legitimately
    // fail or defer a genuine mismatch. We only require that it does not panic.
    // It is also the direction that reaches the no-match error paths of the
    // asymmetric union arms: a node the reader wrapped and widened, say an `int`
    // read as `["null","double"]`, has no match in reverse, because `double`
    // does not narrow back to `int`.
    let _ = resolve_schemas(&reader, &writer);
    let _ = resolve_schemas(&reader, &reader);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
