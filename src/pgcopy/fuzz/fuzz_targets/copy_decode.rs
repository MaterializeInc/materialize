// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `mz_pgcopy::decode_copy_format` parses untrusted `COPY … FROM`
//! data (text and CSV) into rows. COPY data comes straight from a client, and
//! the field splitting plus per-type value decoding are hand-written parsers at
//! the trust boundary, so any panic is an availability bug.
//!
//! The field splitter accepts almost any bytes, but the per-type value decoders
//! (int/float/date/jsonb/uuid/bytea) reject random field text immediately — so
//! raw bytes exercise the line/field framing but barely reach the value
//! decoders. We instead consume the byte stream as grammar choices and emit a
//! valid row set for the fixed 9-column schema so each value decoder runs
//! through to its success path. Generation is tailored per format:
//!
//!   * Text: per-type values delimiter-joined with `\N` NULL tokens, and the
//!     text column sometimes carries real COPY-text escape sequences (`a\tb`,
//!     `\x41`, `\052`, embedded `\n`, `\b\f\v\r`) so the `consume_raw_value`
//!     backslash/hex/octal escape decoder runs end to end (its decoded bytes
//!     stay valid UTF-8 so the Text decode still succeeds).
//!   * CSV: the format params themselves are fuzzed the way the product's
//!     `impl Arbitrary for CopyCsvFormatParams` does it (quote derived to differ
//!     from the delimiter, an optional distinct escape, optional header,
//!     optional non-empty NULL token). Every field that could contain the
//!     active delimiter/quote/escape (notably the text column and the NULL
//!     token) is properly quoted and escaped so it survives the csv-core framing
//!     layer and reaches the value decoder instead of erroring early. When a
//!     header is configured we additionally emit a leading header row so the
//!     header-skip path is exercised alongside data rows.
//!
//! A quarter of inputs feed the raw bytes through both formats so the
//! framing/error paths stay covered.

#![no_main]

use std::borrow::Cow;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_pgcopy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams, decode_copy_format};
use mz_pgrepr::Type;

/// A fixed, type-diverse schema exercising many per-field value decoders.
const COLS: [Type; 9] = [
    Type::Int4,
    Type::Text,
    Type::Bool,
    Type::Float8,
    Type::Bytea,
    Type::Date,
    Type::Int8,
    Type::Jsonb,
    Type::Uuid,
];

const HEX: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];

/// Emit the *logical* (already-unescaped) text representation of column `col`.
/// Callers escape the result as needed for the target format. `text_format`
/// only affects bytea, which uses a literal backslash prefix that the text
/// escaper will later double.
fn push_value(
    u: &mut Unstructured,
    col: usize,
    text_format: bool,
    out: &mut String,
) -> arbitrary::Result<()> {
    match col {
        0 => out.push_str(&u.arbitrary::<i32>()?.to_string()),
        1 => {
            let n = u.int_in_range(0usize..=6)?;
            for _ in 0..n {
                out.push(*u.choose(&['a', 'b', 'Z', '0', '9', ' '])?);
            }
        }
        2 => out.push_str(u.choose(&["t", "f", "true", "false"])?),
        3 => out.push_str(u.choose(&[
            "0", "-1.5", "3.14", "1e10", "-2.5e-3", "Infinity", "-Infinity", "NaN",
        ])?),
        4 => {
            // bytea hex: `\x<hex>`; in COPY text the backslash must be doubled.
            out.push_str(if text_format { "\\\\x" } else { "\\x" });
            for _ in 0..u.int_in_range(0usize..=6)? {
                out.push(*u.choose(HEX)?);
            }
        }
        5 => {
            let y = u.int_in_range(2000u32..=2099)?;
            let m = u.int_in_range(1u32..=12)?;
            let d = u.int_in_range(1u32..=28)?;
            out.push_str(&format!("{y:04}-{m:02}-{d:02}"));
        }
        6 => out.push_str(&u.arbitrary::<i64>()?.to_string()),
        // Comma/tab/backslash-free JSON so the same value is valid in both formats.
        7 => out.push_str(u.choose(&["1", "true", "null", "\"s\"", "{\"a\":1}", "[1]"])?),
        _ => {
            let b: [u8; 16] = u.arbitrary()?;
            for (i, byte) in b.iter().enumerate() {
                if matches!(i, 4 | 6 | 8 | 10) {
                    out.push('-');
                }
                out.push_str(&format!("{byte:02x}"));
            }
        }
    }
    Ok(())
}

/// Append the text column (col 1) directly as COPY-text escape sequences, so
/// the `consume_raw_value` backslash/hex/octal decoder runs through its escape
/// branches. The chosen escapes all decode to valid-UTF-8 bytes that are not a
/// delimiter or newline, so the field stays single, decodes as a `Text` value,
/// and does not disturb the line framing.
fn push_text_escapes(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    let n = u.int_in_range(0usize..=4)?;
    for _ in 0..n {
        out.push_str(u.choose(&[
            // Literal chars (decode to themselves).
            "a", "b", "Z", "0",
            // Recognized C-style escapes (decode to control bytes).
            "\\b", "\\f", "\\n", "\\r", "\\t", "\\v",
            // Hex escapes in the printable ASCII range.
            "\\x41", "\\x7e", "\\x2c",
            // Octal escapes in the printable ASCII range.
            "\\101", "\\052", "\\176",
            // A backslash before a non-escape char drops the backslash.
            "\\q", "\\\\",
        ])?);
    }
    Ok(())
}

/// Append `field` to a CSV record, quoting+escaping it when it could otherwise
/// confuse the framing layer: when it contains the delimiter, quote, escape,
/// `\r`, or `\n`, when it is empty (so it is never mistaken for the empty-string
/// NULL marker), or when it equals the active NULL token (so a data value that
/// happens to match the NULL token is preserved as data rather than read as
/// SQL NULL).
fn push_csv_field(field: &str, params: &CopyCsvFormatParams, out: &mut String) {
    let q = params.quote as char;
    let esc = params.escape as char;
    let delim = params.delimiter as char;
    let needs_quote = field.is_empty()
        || *field == *params.null
        || field
            .chars()
            .any(|c| c == delim || c == q || c == esc || c == '\r' || c == '\n');
    if needs_quote {
        out.push(q);
        for c in field.chars() {
            if c == q || c == esc {
                out.push(esc);
            }
            out.push(c);
        }
        out.push(q);
    } else {
        out.push_str(field);
    }
}

fn decode_text(data: &[u8]) {
    let _ = decode_copy_format(
        data,
        &COLS,
        CopyFormatParams::Text(CopyTextFormatParams {
            null: Cow::Borrowed("\\N"),
            delimiter: b'\t',
        }),
    );
}

fn decode_csv(data: &[u8], params: CopyCsvFormatParams<'static>) {
    let _ = decode_copy_format(data, &COLS, CopyFormatParams::Csv(params));
}

/// Build CSV params the way `impl Arbitrary for CopyCsvFormatParams` does:
/// derive the quote so it differs from the delimiter, optionally a distinct
/// escape, an optional header, and an optional non-empty NULL token. The result
/// always satisfies the `try_new` invariant (quote != delimiter).
fn arbitrary_csv_params(u: &mut Unstructured) -> arbitrary::Result<CopyCsvFormatParams<'static>> {
    let delimiter: u8 = u.arbitrary()?;
    // Mirror the proptest strategy: pick a non-zero difference and wrap-add it
    // to the delimiter, guaranteeing quote != delimiter.
    let diff = u.arbitrary::<u8>()?.saturating_sub(1).max(1);
    let quote = delimiter.wrapping_add(diff);
    // Half the time use a distinct escape (the non-double-quote csv-core path).
    let escape = if u.arbitrary()? {
        let ediff = u.arbitrary::<u8>()?.saturating_sub(1).max(1);
        quote.wrapping_add(ediff)
    } else {
        quote
    };
    let header: bool = u.arbitrary()?;
    let null = if u.arbitrary()? {
        // A non-empty NULL token (e.g. the conventional `NULL` / `\N`).
        u.choose(&["NULL", "\\N", "null", "NA", "-"])?.to_string()
    } else {
        String::new()
    };
    CopyCsvFormatParams::try_new(
        Some(delimiter),
        Some(quote),
        Some(escape),
        Some(header),
        Some(null),
    )
    .map_err(|_| arbitrary::Error::IncorrectFormat)
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, feed the raw bytes through both formats: keeps the
    // field/line framing and error paths covered.
    if u.int_in_range(0u8..=3)? == 0 {
        let rest = u.take_rest();
        decode_text(rest);
        decode_csv(rest, CopyCsvFormatParams::default());
        return Ok(());
    }

    let text_format = u.int_in_range(0u8..=1)? == 0;

    if text_format {
        let mut s = String::new();
        let rows = u.int_in_range(1usize..=4)?;
        for _ in 0..rows {
            for col in 0..COLS.len() {
                if col > 0 {
                    s.push('\t');
                }
                // 1-in-8 NULL via the `\N` token.
                if u.int_in_range(0u8..=7)? == 0 {
                    s.push_str("\\N");
                } else if col == 1 && u.arbitrary()? {
                    // Drive the escape decoder through the text column.
                    push_text_escapes(&mut u, &mut s)?;
                } else {
                    push_value(&mut u, col, true, &mut s)?;
                }
            }
            s.push('\n');
        }
        decode_text(s.as_bytes());
        return Ok(());
    }

    // CSV: fuzz the format params, then emit data the params can actually frame.
    let params = arbitrary_csv_params(&mut u)?;
    let delim = params.delimiter as char;
    let mut s = String::new();

    // A configured header means the first record is column names that the
    // decoder skips; emit a plausible one so the header-skip path runs.
    if params.header {
        for col in 0..COLS.len() {
            if col > 0 {
                s.push(delim);
            }
            push_csv_field(&format!("col{col}"), &params, &mut s);
        }
        s.push('\n');
    }

    let rows = u.int_in_range(1usize..=4)?;
    for _ in 0..rows {
        for col in 0..COLS.len() {
            if col > 0 {
                s.push(delim);
            }
            // 1-in-8 NULL: emit the unquoted NULL token (empty field for the
            // default empty marker), which the decoder reads as SQL NULL.
            if u.int_in_range(0u8..=7)? == 0 {
                s.push_str(&params.null);
            } else {
                let mut field = String::new();
                push_value(&mut u, col, false, &mut field)?;
                push_csv_field(&field, &params, &mut s);
            }
        }
        s.push('\n');
    }

    decode_csv(s.as_bytes(), params);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
