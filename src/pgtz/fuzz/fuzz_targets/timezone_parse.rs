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
//! The interesting surface is the *offset tokenizer*: `tokenize_timezone`
//! grabs the first alphabetic run as a single `TzName` and returns immediately
//! (so any POSIX DST-rule tail is silently discarded, making fuzzing that
//! grammar dead weight), while everything else flows through `parse_num`, which
//! splits long all-digit runs into `[..hhhh]mm` chunks unless a `:` is present,
//! plus the punctuation-as-delimiter trimming and the `z`/`Z`-only-at-end rule.
//! `build_timezone_offset_second` then matches the token stream against twelve
//! fixed `±H[H][:M[M][:S[S]]]` / `±HHH` / `TzName` / `Zulu` shapes and enforces
//! the `hour<=15`, `min<60`, `sec<60` bounds. So we generate inputs that stress
//! exactly that math: long all-digit runs (`+00000100`, `+0000001:000001`),
//! the hour/min/sec boundaries (`+15:59:59`, `+16`, `+0:60`), the colon-vs-no-
//! colon `split_nums` toggle, punctuation-delimited junk around a real offset,
//! bare `z`/`Z` placed mid-string vs at the end, abbreviations drawn from
//! `TIMEZONE_ABBREVS`, and case-mangled IANA names. A quarter of inputs stay
//! the raw bytes so the tokenizer reject paths keep their coverage.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_pgtz::timezone::{Timezone, TimezoneSpec};

/// IANA names exercising fractional-hour offsets and DST, in canonical casing.
/// `gen_named` may re-case them to hit the case-insensitive lookup path.
const NAMED: &[&str] = &[
    "UTC",
    "GMT",
    "America/New_York",
    "Europe/London",
    "Asia/Kolkata",          // :30 offset
    "Australia/Lord_Howe",   // :30 offset with DST
    "Pacific/Chatham",       // :45 offset
    "America/Argentina/Buenos_Aires",
    "Etc/GMT+12",
    "posixrules",
];

/// A spread of abbreviations from `TIMEZONE_ABBREVS`: fixed-offset ones, DST
/// ones, and ones that alias to a `Tz`, so the abbrev lookup + fallback to
/// `Tz::from_str_insensitive` both run. `EST`/`PST`/... also double as the
/// leading `std` name of a POSIX-looking string (whose offset tail is what the
/// tokenizer actually keeps).
const ABBREVS: &[&str] = &[
    "EST", "EDT", "PST", "PDT", "CST", "CDT", "MST", "MDT", "CET", "CEST", "EET",
    "EEST", "BST", "IST", "JST", "ACDT", "ACST", "AEST", "AEDT", "NZST", "NZDT",
    "CHADT", "CHAST", "HKT", "WET", "WEST", "UCT", "ZULU", "GMT", "UTC",
];

/// Emit a numeric UTC offset, biased toward the tokenizer/builder boundaries:
/// `z`/`Z`, `±HH`, `±HH:MM`, `±HH:MM:SS`, long all-digit runs that `parse_num`
/// must chunk, and the exact `hour<=15` / `min<60` / `sec<60` edges.
fn gen_offset(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=8)? {
        // Bare Zulu (only valid at end-of-string).
        0 => {
            out.push(if u.ratio(1, 2)? { 'z' } else { 'Z' });
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
        // Colon-delimited long all-digit runs (colon disables `split_nums`),
        // e.g. `+0000001:000001:000001`.
        5 => {
            out.push(sign(u)?);
            let parts = u.int_in_range(1u8..=3)?;
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

/// Wrap an inner spec in leading/trailing whitespace and ASCII punctuation,
/// which the tokenizer trims (except `+`/`-`) or treats as `Delim`. This keeps
/// the `" ! ? ! - 5:15 ? ! ? "`-style paths covered.
fn gen_punct_wrapped(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    const JUNK: &[char] = &[' ', '!', '?', '.', ',', '*', '/', '#', '~', '\t'];
    let lead = u.int_in_range(0u8..=3)?;
    for _ in 0..lead {
        out.push(*u.choose(JUNK)?);
    }
    gen_offset(u, out)?;
    let trail = u.int_in_range(0u8..=3)?;
    for _ in 0..trail {
        out.push(*u.choose(JUNK)?);
    }
    Ok(())
}

fn gen_tz(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=6)? {
        0 => gen_named(u, out)?,
        1 => out.push_str(u.choose(ABBREVS)?),
        2 | 3 => gen_offset(u, out)?,
        4 => gen_punct_wrapped(u, out)?,
        // An abbreviation immediately followed by an offset: the tokenizer keeps
        // the abbrev as a `TzName` and returns, so the offset tail is ignored,
        // but this still stresses the "first alpha wins" early return.
        5 => {
            out.push_str(u.choose(ABBREVS)?);
            gen_offset(u, out)?;
        }
        // A bare `z`/`Z` placed *before* more text, so it is NOT at end-of-string
        // and must be tokenized as a `TzName`, not `Zulu`.
        _ => {
            out.push(if u.ratio(1, 2)? { 'z' } else { 'Z' });
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
