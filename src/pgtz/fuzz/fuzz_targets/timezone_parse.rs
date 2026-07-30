// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `mz_pgtz::timezone::Timezone::parse` parses untrusted time-zone
//! strings (the `AT TIME ZONE` / `SET timezone` value) with a hand-written
//! tokenizer + offset builder, in both ISO and POSIX modes. Any panic is an
//! availability bug.
//!
//! The interesting surface is the *offset tokenizer*. On the first ASCII
//! alphabetic character `tokenize_timezone` pushes the entire remainder of the
//! string as a single `TzName` and returns, so everything after a letter is
//! folded into the name rather than tokenized. Two consequences shape this
//! generator: fuzzing the POSIX DST-rule grammar is dead weight, and only
//! inputs whose letters come *last* get both halves of the string tokenized.
//! Everything else flows through `parse_num`, which splits long all-digit runs
//! into `[..hhhh]mm` chunks unless a `:` is present, plus the
//! punctuation-as-`Delim` handling and the `z`/`Z`-only-at-end rule.
//! `build_timezone_offset_second` then matches the token stream against a table
//! of `±H[H][:M[M][:S[S]]]` / `±HH H` / `TzName` / `Zulu` shapes and enforces
//! the `hour<=15`, `min<60`, `sec<60` bounds.
//!
//! So we generate inputs that stress exactly that math: long all-digit runs
//! (`+00000100`, `+0000001:000001`), runs long enough to overflow the `u64`
//! parse, the hour/min/sec boundaries (`+15:59:59`, `+16`, `+0:60`), the
//! colon-vs-no-colon `split_nums` toggle, *interior* punctuation, a `z`/`Z`
//! after digits, abbreviations from `TIMEZONE_ABBREVS` placed after an offset
//! so both halves tokenize, and case-mangled IANA names. A quarter of inputs
//! stay the raw bytes so the tokenizer reject paths keep their coverage.
//!
//! NOTE: two of the twelve entries in that format table, `[±, Num, Num, Num]`,
//! are unreachable. `parse_num` is the only thing that pushes `Num` and it
//! pushes at most two per digit run, while every other tokenizer arm pushes a
//! separator, `Zulu`, or `TzName` in between, so no input yields three
//! consecutive `Num` tokens. The widest all-digit offset (`+00000100`) matches
//! the three-token `[Plus, Num, Num]` shape instead.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_pgtz::timezone::{Timezone, TimezoneSpec};

/// IANA names exercising fractional-hour offsets and DST, in canonical casing.
/// `gen_named` may re-case them to hit the case-insensitive lookup path.
/// `posixrules` is not a chrono-tz zone, so it covers the reject path.
const NAMED: &[&str] = &[
    "UTC",
    "GMT",
    "America/New_York",
    "Europe/London",
    "Asia/Kolkata",        // :30 offset
    "Australia/Lord_Howe", // :30 offset with DST
    "Pacific/Chatham",     // :45 offset
    "America/Argentina/Buenos_Aires",
    "Etc/GMT+12",
    "posixrules",
];

/// A spread of abbreviations from `TIMEZONE_ABBREVS`: fixed-offset ones, DST
/// ones, and ones that alias to a `Tz`, so the abbrev lookup and its fallback
/// to `Tz::from_str_insensitive` both run. `WEST` is the one entry absent from
/// `src/pgtz/tznames/Default`, so it drives the miss-then-fallback-then-reject
/// path.
const ABBREVS: &[&str] = &[
    "EST", "EDT", "PST", "PDT", "CST", "CDT", "MST", "MDT", "CET", "CEST", "EET", "EEST", "BST",
    "IST", "JST", "ACDT", "ACST", "AEST", "AEDT", "NZST", "NZDT", "CHADT", "CHAST", "HKT", "WET",
    "WEST", "UCT", "ZULU", "GMT", "UTC",
];

/// ASCII whitespace and punctuation, which the tokenizer trims at the edges of
/// the string and turns into a `Delim` in the interior. Deliberately excludes
/// `+`/`-`, the two characters the trimming closure spares.
const JUNK: &[char] = &[' ', '!', '?', '.', ',', '*', '/', '#', '~', '\t'];

/// Emit a numeric UTC offset, biased toward the tokenizer/builder boundaries:
/// `z`/`Z`, `±HH`, `±HH:MM`, `±HH:MM:SS`, long all-digit runs that `parse_num`
/// must chunk or fail to parse, and the exact `hour<=15` / `min<60` / `sec<60`
/// edges.
///
/// Every shape but the bare `z`/`Z` ends in a digit. Callers that append more
/// text must check for that, since a trailing letter would make the tokenizer
/// fold the whole string into one `TzName`.
fn gen_offset(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=9)? {
        // Bare Zulu (only valid at end-of-string).
        0 => {
            out.push(zulu(u)?);
            return Ok(());
        }
        // Hour at/around the `<= 15` boundary.
        1 => {
            out.push(sign(u)?);
            out.push_str(&format!("{:02}", u.int_in_range(13u32..=17)?));
        }
        // `±HH:MM` with minute at/around the `< 60` boundary.
        2 => {
            out.push(sign(u)?);
            out.push_str(&format!(
                "{:02}:{:02}",
                u.int_in_range(0u32..=15)?,
                u.int_in_range(57u32..=61)?
            ));
        }
        // `±HH:MM:SS` with second at/around the `< 60` boundary, e.g. `+15:59:59`.
        3 => {
            out.push(sign(u)?);
            out.push_str(&format!(
                "{:02}:{:02}:{:02}",
                u.int_in_range(0u32..=15)?,
                u.int_in_range(0u32..=59)?,
                u.int_in_range(57u32..=61)?
            ));
        }
        // Long all-digit run (no colon): exercises the `split_nums` `[..hh]mm`
        // chunking and leading-zero handling, e.g. `+00000100`, `+0000005`.
        4 => {
            out.push(sign(u)?);
            let zeros = u.int_in_range(0u32..=8)?;
            for _ in 0..zeros {
                out.push('0');
            }
            out.push_str(&u.int_in_range(0u32..=999)?.to_string());
        }
        // Colon-delimited long all-digit runs (a colon disables `split_nums`),
        // e.g. `+0000001:000001:000001`. At least two parts: a single part emits
        // no colon at all and degenerates into the arm above.
        5 => {
            out.push(sign(u)?);
            let parts = u.int_in_range(2u8..=3)?;
            for p in 0..parts {
                if p > 0 {
                    out.push(':');
                }
                let zeros = u.int_in_range(0u32..=7)?;
                for _ in 0..zeros {
                    out.push('0');
                }
                out.push_str(&u.int_in_range(0u32..=99)?.to_string());
            }
        }
        // A digit run long enough to overflow the `u64` parse in `parse_num`.
        // The run must start nonzero: leading zeros accumulate to zero and never
        // overflow, however long the padding. `parse_num` parses the whole run
        // when a colon is present (`split_nums` off, overflowing past 20 digits)
        // and the run minus its last two digits otherwise, so this range
        // straddles both thresholds.
        6 => {
            out.push(sign(u)?);
            let digits = u.int_in_range(18u32..=24)?;
            for _ in 0..digits {
                out.push(*u.choose(&['1', '8', '9'])?);
            }
            if u.ratio(1, 2)? {
                out.push_str(":00");
            }
        }
        // Ordinary `±HH[:MM[:SS]]` across the full valid range.
        _ => {
            out.push(sign(u)?);
            out.push_str(&format!("{:02}", u.int_in_range(0u32..=15)?));
            match u.int_in_range(0u8..=2)? {
                0 => {}
                1 => out.push_str(&format!(":{:02}", *u.choose(&[0u32, 15, 30, 45])?)),
                _ => out.push_str(&format!(
                    ":{:02}:{:02}",
                    *u.choose(&[0u32, 30, 45])?,
                    u.int_in_range(0u32..=59)?
                )),
            }
        }
    }
    Ok(())
}

fn sign(u: &mut Unstructured) -> arbitrary::Result<char> {
    Ok(if u.ratio(1, 2)? { '+' } else { '-' })
}

fn zulu(u: &mut Unstructured) -> arbitrary::Result<char> {
    Ok(if u.ratio(1, 2)? { 'z' } else { 'Z' })
}

/// Emit an IANA name, sometimes case-mangled to hit `from_str_insensitive`.
fn gen_named(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    let name = *u.choose(NAMED)?;
    match u.int_in_range(0u8..=3)? {
        0 => out.push_str(&name.to_lowercase()),
        1 => out.push_str(&name.to_uppercase()),
        2 => {
            // Alternate-case mangling.
            for (i, c) in name.chars().enumerate() {
                if i % 2 == 0 {
                    out.extend(c.to_lowercase());
                } else {
                    out.extend(c.to_uppercase());
                }
            }
        }
        _ => out.push_str(name),
    }
    Ok(())
}

/// Emit an offset whose components are separated by *interior* punctuation,
/// e.g. `+05!30` or `-12.30`.
///
/// Interior placement is what makes this arm distinct: the tokenizer trims
/// leading and trailing whitespace and punctuation, so an offset merely
/// *bracketed* in junk is byte-identical to the bare offset by the time it is
/// tokenized, and yields no `Delim` at all. Empty components put two separators
/// back to back, producing the odd token streams that match no format at all
/// (`-5::15` gives `[Dash, Num, Colon, Colon, Num]`), which drives the
/// mismatch/reset arm of `build_timezone_offset_second`.
fn gen_punct_delimited(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    // Leading junk is trimmed away, so keep it rare, just enough to cover the
    // trimming closure itself.
    if u.ratio(1, 4)? {
        out.push(*u.choose(JUNK)?);
    }
    out.push(sign(u)?);
    let parts = u.int_in_range(2u8..=4)?;
    for p in 0..parts {
        if p > 0 {
            // `:` keeps `split_nums` disabled, JUNK yields a `Delim`.
            if u.ratio(1, 3)? {
                out.push(':');
            } else {
                out.push(*u.choose(JUNK)?);
            }
        }
        if u.ratio(7, 8)? {
            out.push_str(&u.int_in_range(0u32..=99)?.to_string());
        }
    }
    Ok(())
}

fn gen_tz(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=9)? {
        0 => gen_named(u, out)?,
        1 => out.push_str(u.choose(ABBREVS)?),
        2 | 3 | 4 => {
            gen_offset(u, out)?;
            // A `z`/`Z` after digits is the only shape that reaches the
            // `parse_num` call in the tokenizer's Zulu arm with digits pending,
            // e.g. `+05:30z` -> `[Plus, Num, Colon, Num, Zulu]`. Appending it to
            // a bare `z` offset would instead just build a two-letter `TzName`.
            if out.ends_with(|c: char| c.is_ascii_digit()) && u.ratio(1, 4)? {
                out.push(zulu(u)?);
            }
        }
        5 | 6 => gen_punct_delimited(u, out)?,
        // An offset, a delimiter, then an abbreviation, e.g. `+05:30 EST` ->
        // `[Plus, Num, Colon, Num, Delim, TzName("EST")]`. The letters have to
        // come last for both halves to tokenize, and this reaches a `Delim`, a
        // short `TzName`, and a six-token stream in one input.
        7 | 8 => {
            gen_offset(u, out)?;
            if out.ends_with(|c: char| c.is_ascii_digit()) {
                out.push(*u.choose(JUNK)?);
                out.push_str(u.choose(ABBREVS)?);
            }
        }
        // Letters first, so the alphabetic branch takes the whole remaining
        // string and the numeric tail is never tokenized: the input collapses
        // into one long `TzName` that misses both the abbrev table and
        // `Tz::from_str_insensitive`. Low weight, because its only marginal
        // coverage over a bare abbreviation is a longer lookup miss.
        _ => {
            if u.ratio(1, 2)? {
                out.push_str(u.choose(ABBREVS)?);
            } else {
                out.push(zulu(u)?);
            }
            gen_offset(u, out)?;
        }
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, the raw bytes: keeps the tokenizer reject paths
    // covered.
    let spec = if u.int_in_range(0u8..=3)? == 0 {
        String::from_utf8_lossy(u.take_rest()).into_owned()
    } else {
        let mut s = String::new();
        gen_tz(&mut u, &mut s)?;
        s
    };
    let _ = Timezone::parse(&spec, TimezoneSpec::Iso);
    let _ = Timezone::parse(&spec, TimezoneSpec::Posix);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
