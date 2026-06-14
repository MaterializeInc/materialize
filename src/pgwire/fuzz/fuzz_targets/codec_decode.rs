// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: drive `mz_pgwire`'s frontend-message `Codec` over arbitrary
//! bytes. The decoder lives at the trust boundary between SQL clients and
//! environmentd; any panic/SEGV reachable from the wire is a real
//! availability bug.
//!
//! A frame is `[type:1][len:4 BE][body:len-4]`. Random bytes rarely have a
//! length field that matches the bytes that follow, so the decoder bails in the
//! header before reaching the per-message body parsers (Query/Parse/Bind/
//! Describe/Execute/…) — and once one frame errors, the streaming decoder stops,
//! so later frames never decode either. So we consume the byte stream as grammar
//! choices and emit correctly-framed messages: a valid type tag, the right
//! length, and (usually) a valid body for that type, concatenating several so
//! the decoder walks frame after frame. A quarter of inputs are still the raw
//! bytes, and a quarter of frames carry an arbitrary body, so the header
//! validation and per-message error paths stay covered.
//!
//! Beyond well-formed frames we deliberately stress two thin spots:
//!
//! * **Count-driven loops.** The body parsers for Parse and Bind read an `i16`
//!   element count (param-type / format-code / parameter counts) and then loop
//!   that many times reading from the body, and a Bind parameter declares its
//!   own `i32` byte length. We sometimes emit a huge count or length (up to
//!   `i16::MAX` / a large positive `i32`) backed by a body far too short to
//!   satisfy it, so the loops read off the end of the cursor and must error out
//!   gracefully rather than over-read, over-allocate, or panic. Long cstrings
//!   feed the same idea on the string side.
//!
//! * **Streaming / partial-frame reassembly.** The codec is a `tokio_util`
//!   `Decoder`: it advances `Head -> Data -> Head` across calls and returns
//!   `Ok(None)` whenever the body promised by the length field hasn't fully
//!   arrived yet. Feeding the whole stream in one `BytesMut` never lands mid
//!   frame, so we (a) sometimes hand frames a length field that overstates the
//!   real body, and (b) drip the byte stream into the decoder in arbitrary
//!   chunks, so it parks in the `Data` await-more-bytes state and resumes when
//!   the rest shows up.
//!
//! Errors are expected; what we assert is the absence of panics and
//! memory-safety violations.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_pgwire::fuzz_exports::Codec;
use tokio_util::codec::Decoder;

/// Frontend message type tags the codec dispatches on.
const TAGS: &[u8] = &[
    b'Q', b'P', b'D', b'B', b'E', b'H', b'S', b'C', b'X', b'p', b'f', b'd', b'c',
];

fn push_cstr(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    let n = u.int_in_range(0usize..=8)?;
    for _ in 0..n {
        // Printable, non-NUL.
        out.push(u.int_in_range(0x20u8..=0x7e)?);
    }
    out.push(0);
    Ok(())
}

/// Append a long (but bounded) printable, NUL-terminated string. Stresses the
/// `read_cstr` scan and downstream allocations without blowing past
/// `MAX_REQUEST_SIZE`.
fn push_long_cstr(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    let n = u.int_in_range(64usize..=4096)?;
    let fill = u.int_in_range(0x20u8..=0x7e)?;
    out.resize(out.len() + n, fill);
    out.push(0);
    Ok(())
}

fn be16(out: &mut Vec<u8>, v: i16) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn be32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// Pick an element count for a count-driven loop. Usually small and matched by
/// the body that follows, but sometimes a large value the body can't satisfy so
/// the loop reads off the end of the cursor and must error rather than over-read
/// or over-allocate. Returns `(declared_count, honest_count)`: `declared_count`
/// is written to the wire, `honest_count` is how many elements we actually emit.
fn count(u: &mut Unstructured) -> arbitrary::Result<(i16, i16)> {
    match u.int_in_range(0u8..=7)? {
        // Mostly: a small, honest count fully backed by the body.
        0..=4 => {
            let n = u.int_in_range(0i16..=3)?;
            Ok((n, n))
        }
        // A large declared count with no/too-few backing elements: the loop
        // should run out of buffer and bail.
        5 => Ok((u.int_in_range(1i16..=i16::MAX)?, 0)),
        6 => Ok((i16::MAX, u.int_in_range(0i16..=2)?)),
        // A negative count: the `for _ in 0..n` loop runs zero times, so the
        // remaining body is interpreted as the next field/frame.
        7 => Ok((u.int_in_range(i16::MIN..=-1)?, 0)),
        _ => unreachable!(),
    }
}

/// Build a valid body for message `tag`.
fn gen_body(u: &mut Unstructured, tag: u8, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    match tag {
        // Empty-body messages.
        b'X' | b'S' | b'H' | b'c' => {}
        // Simple query / copy-fail: a single cstring.
        b'Q' | b'f' => maybe_long_cstr(u, out)?,
        // Password / generic auth: a cstring is a plausible password message.
        b'p' => maybe_long_cstr(u, out)?,
        // CopyData: arbitrary payload.
        b'd' => {
            for _ in 0..u.int_in_range(0usize..=16)? {
                out.push(u.arbitrary::<u8>()?);
            }
        }
        // Describe / Close: a 'S'tatement|'P'ortal byte then a name cstring.
        b'D' | b'C' => {
            out.push(if u.int_in_range(0u8..=1)? == 0 { b'S' } else { b'P' });
            maybe_long_cstr(u, out)?;
        }
        // Execute: portal cstring + max-rows i32.
        b'E' => {
            maybe_long_cstr(u, out)?;
            be32(out, u.arbitrary::<i32>()?);
        }
        // Parse: name + query cstrings + param-type oids.
        b'P' => {
            maybe_long_cstr(u, out)?;
            maybe_long_cstr(u, out)?;
            let (declared, honest) = count(u)?;
            be16(out, declared);
            for _ in 0..honest {
                be32(out, u.arbitrary::<i32>()?);
            }
        }
        // Bind: portal + stmt cstrings, format codes, parameters, result formats.
        b'B' => {
            maybe_long_cstr(u, out)?;
            maybe_long_cstr(u, out)?;
            let (declared, honest) = count(u)?;
            be16(out, declared);
            for _ in 0..honest {
                be16(out, u.int_in_range(0i16..=1)?);
            }
            let (declared, honest) = count(u)?;
            be16(out, declared);
            for _ in 0..honest {
                match u.int_in_range(0u8..=4)? {
                    0 => be32(out, -1), // NULL parameter
                    // A large declared length with a short (or empty) value: the
                    // per-byte read loop should run out of buffer and bail.
                    1 => {
                        be32(out, u.int_in_range(1i32..=i32::MAX)?);
                        for _ in 0..u.int_in_range(0usize..=2)? {
                            out.push(u.arbitrary::<u8>()?);
                        }
                    }
                    _ => {
                        let len = u.int_in_range(0usize..=4)?;
                        be32(out, len as i32);
                        for _ in 0..len {
                            out.push(u.arbitrary::<u8>()?);
                        }
                    }
                }
            }
            let (declared, honest) = count(u)?;
            be16(out, declared);
            for _ in 0..honest {
                be16(out, u.int_in_range(0i16..=1)?);
            }
        }
        _ => {}
    }
    Ok(())
}

/// A cstring that is usually short but occasionally long, to stress the scan
/// and downstream string allocations.
fn maybe_long_cstr(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    if u.int_in_range(0u8..=7)? == 0 {
        push_long_cstr(u, out)
    } else {
        push_cstr(u, out)
    }
}

fn push_frame(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    let tag = *u.choose(TAGS)?;
    let mut body = Vec::new();
    // A quarter of frames carry an arbitrary body so the per-message parsers'
    // error handling stays covered; the rest are valid for their type.
    if u.int_in_range(0u8..=3)? == 0 {
        for _ in 0..u.int_in_range(0usize..=16)? {
            body.push(u.arbitrary::<u8>()?);
        }
    } else {
        gen_body(u, tag, &mut body)?;
    }
    out.push(tag);
    // Length field counts itself (4 bytes) plus the body, but not the tag.
    // Usually honest, but occasionally we overstate it so the streaming decoder
    // parks in the `Data` await-more-bytes state expecting bytes that may or may
    // not arrive (a later frame's bytes get reinterpreted as this body, or the
    // stream simply ends mid-frame).
    let honest = (body.len() as u32) + 4;
    let declared = if u.int_in_range(0u8..=7)? == 0 {
        // Overstate by a bounded amount; `parse_frame_len` rejects anything over
        // MAX_FRAME_SIZE (64 MiB), so keep the claim well under that.
        honest.saturating_add(u.int_in_range(1u32..=4096)?)
    } else {
        honest
    };
    out.extend_from_slice(&declared.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(())
}

/// Feed `data` to the codec. When `chunked`, drip it in arbitrary-sized slices
/// so the decoder repeatedly parks in its partial-frame (`Ok(None)`) state and
/// resumes when more bytes land — exercising the streaming reassembly path that
/// a single all-at-once `BytesMut` never reaches mid-frame.
fn pump(u: &mut Unstructured, data: &[u8], chunked: bool) -> arbitrary::Result<()> {
    let mut codec = Codec::new();
    let mut buf = BytesMut::new();

    let mut feed_and_drain = |buf: &mut BytesMut| {
        // The codec is a streaming decoder; pump it until it stops returning
        // complete messages or errors out. Errors are expected; what we care
        // about is the absence of panics and memory-safety violations.
        loop {
            match codec.decode(buf) {
                Ok(Some(_msg)) => continue,
                Ok(None) => break,
                Err(_) => break,
            }
        }
    };

    if chunked {
        let mut rest = data;
        while !rest.is_empty() {
            let take = u.int_in_range(1usize..=rest.len())?.min(rest.len());
            buf.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            feed_and_drain(&mut buf);
        }
        // A final drain in case the last chunk completed a frame.
        feed_and_drain(&mut buf);
    } else {
        buf.extend_from_slice(data);
        feed_and_drain(&mut buf);
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, the raw bytes: keeps the header-framing and
    // unknown-tag error paths covered.
    if u.int_in_range(0u8..=3)? == 0 {
        let chunked = u.int_in_range(0u8..=1)? == 0;
        let rest = u.take_rest();
        return pump(&mut Unstructured::new(rest), rest, chunked);
    }
    let mut out = Vec::new();
    let frames = u.int_in_range(1usize..=5)?;
    for _ in 0..frames {
        push_frame(&mut u, &mut out)?;
    }
    // Half the time, drip the assembled stream into the decoder in chunks to
    // exercise the partial-frame await-more-bytes logic mid-stream.
    let chunked = u.int_in_range(0u8..=1)? == 0;
    pump(&mut u, &out, chunked)
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
