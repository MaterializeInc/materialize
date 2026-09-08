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
//! environmentd, so any panic/SEGV reachable from the wire is a real
//! availability bug.
//!
//! A frame is `[type:1][len:4 BE][body:len-4]`. Random bytes rarely have a
//! length field that matches the bytes that follow, so the decoder bails in the
//! header before reaching the per-message body parsers (Query/Parse/Bind/
//! Describe/Execute/…). So we consume the byte stream as grammar choices and emit
//! correctly-framed messages: a valid type tag, the right length, and (usually) a
//! valid body for that type, concatenating several so the decoder walks frame
//! after frame. A quarter of inputs are still the raw bytes, and a quarter of
//! frames carry an arbitrary body, so the header validation and per-message error
//! paths stay covered.
//!
//! Beyond well-formed frames we deliberately stress three thin spots:
//!
//! * **Count-driven loops.** The body parsers for Parse and Bind read an `i16`
//!   element count (param-type / format-code / parameter counts) and then loop
//!   that many times reading from the body, and a Bind parameter declares its
//!   own `i32` byte length. We sometimes emit a huge count or length (up to
//!   `i16::MAX` / a large positive `i32`) backed by a body far too short to
//!   satisfy it, so the loops read off the end of the cursor and must error out
//!   gracefully rather than over-read or panic. Long cstrings feed the same idea
//!   on the string side.
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
//! * **The pre-auth SASL/password grammars.** `Codec::decode` does not parse
//!   auth payloads. Its `b'p'` arm copies the body verbatim into
//!   `RawAuthentication`, and `protocol` later picks a parser based on where the
//!   handshake is. Those parsers are hand-rolled, byte-at-a-time, and run before
//!   the client has authenticated, so `feed_and_drain` runs all three on every
//!   payload and `gen_auth_body` generates the shapes they accept.
//!
//! Errors are expected. What we assert is the absence of panics and
//! memory-safety violations.
//!
//! Note that allocation amplification is *not* in scope. The only speculative
//! `reserve` is on the declared frame length, which `parse_frame_len` caps at
//! `MAX_FRAME_SIZE` (64 MiB), far under the runner's `-rss_limit_mb`. The
//! count-driven loops push one element per successful cursor read, so they are
//! bounded by the bytes actually present. No oracle here can catch an
//! over-allocation, so don't read one into the target.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_pgwire::fuzz_exports::{
    Codec, Cursor, FrontendMessage, decode_password, decode_sasl_initial_response,
    decode_sasl_response,
};
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
        // Auth: shapes for the sub-parsers `feed_and_drain` runs on the payload.
        b'p' => gen_auth_body(u, out)?,
        // CopyData: arbitrary payload.
        b'd' => {
            for _ in 0..u.int_in_range(0usize..=16)? {
                out.push(u.arbitrary::<u8>()?);
            }
        }
        // Describe / Close: a 'S'tatement|'P'ortal byte then a name cstring.
        b'D' | b'C' => {
            out.push(if u.int_in_range(0u8..=1)? == 0 {
                b'S'
            } else {
                b'P'
            });
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

/// Append a comma-free printable run. Every SASL field is comma-delimited and
/// goes through `String::from_utf8`, so restricting tokens to `0x2d..=0x7e`
/// (printable ASCII above `,`) is what lets the parser advance past a field
/// instead of stopping short or erroring on invalid UTF-8.
fn push_token(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    for _ in 0..u.int_in_range(0usize..=8)? {
        out.push(u.int_in_range(0x2du8..=0x7e)?);
    }
    Ok(())
}

/// Build an auth-message payload targeting the three sub-parsers: a cleartext
/// password, a SASL initial response, or a SASL client-final message.
fn gen_auth_body(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=2)? {
        // `decode_password` reads a single cstring.
        0 => maybe_long_cstr(u, out)?,
        // `decode_sasl_initial_response`: mechanism cstring, a declared response
        // length it only rejects when negative, then a client-first-message
        // parsed out of whatever is left.
        1 => {
            match u.int_in_range(0u8..=2)? {
                0 => out.extend_from_slice(b"SCRAM-SHA-256\0"),
                1 => out.extend_from_slice(b"SCRAM-SHA-256-PLUS\0"),
                _ => push_cstr(u, out)?,
            }
            // The parser rejects a negative declared length and then ignores the
            // value entirely, parsing whatever follows regardless. Mostly
            // declare a non-negative one so the client-first grammar is reached,
            // occasionally go negative to cover the rejection.
            let declared = if u.int_in_range(0u8..=7)? == 0 {
                u.int_in_range(i32::MIN..=-1)?
            } else {
                u.int_in_range(0i32..=i32::MAX)?
            };
            be32(out, declared);
            gen_sasl_client_first(u, out)?;
        }
        _ => gen_sasl_client_final(u, out)?,
    }
    Ok(())
}

/// A SCRAM `client-first-message` (RFC 5802): `gs2-cbind-flag "," [authzid] ","
/// ["m=" mext ","] "n=" user "," "r=" nonce ["," ext]*`. Kept well-formed
/// because the parser aborts on the first unexpected byte, so an approximation
/// of the grammar would never reach the later fields.
fn gen_sasl_client_first(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=2)? {
        0 => out.push(b'n'),
        1 => out.push(b'y'),
        // Channel binding required: "p=" carries the channel name.
        _ => {
            out.extend_from_slice(b"p=");
            push_token(u, out)?;
        }
    }
    out.push(b',');
    if u.int_in_range(0u8..=1)? == 0 {
        out.extend_from_slice(b"a=");
        push_token(u, out)?;
    }
    out.push(b',');
    if u.int_in_range(0u8..=3)? == 0 {
        out.extend_from_slice(b"m=");
        push_token(u, out)?;
        out.push(b',');
    }
    out.extend_from_slice(b"n=");
    push_token(u, out)?;
    out.extend_from_slice(b",r=");
    push_token(u, out)?;
    for _ in 0..u.int_in_range(0usize..=2)? {
        out.push(b',');
        push_token(u, out)?;
    }
    Ok(())
}

/// A SCRAM `client-final-message` (RFC 5802): `"c=" cbind "," "r=" nonce
/// ["," ext]* "," "p=" proof`. The proof is mandatory and last.
fn gen_sasl_client_final(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    out.extend_from_slice(b"c=");
    push_token(u, out)?;
    out.extend_from_slice(b",r=");
    push_token(u, out)?;
    for _ in 0..u.int_in_range(0usize..=2)? {
        out.push(b',');
        push_token(u, out)?;
    }
    out.extend_from_slice(b",p=");
    push_token(u, out)?;
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
    // error handling stays covered. The rest are valid for their type.
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
        // Overstate by a bounded amount. `parse_frame_len` rejects anything over
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
/// resumes when more bytes land, exercising the streaming reassembly path that
/// a single all-at-once `BytesMut` never reaches mid-frame.
fn pump(u: &mut Unstructured, data: &[u8], chunked: bool) -> arbitrary::Result<()> {
    let mut codec = Codec::new();
    let mut buf = BytesMut::new();

    let mut feed_and_drain = |buf: &mut BytesMut| {
        // The codec is a streaming decoder, so pump it until it stops returning
        // complete messages or runs out of forward progress. Errors are
        // expected. What we care about is the absence of panics and
        // memory-safety violations.
        loop {
            let before = buf.len();
            match codec.decode(buf) {
                Ok(Some(msg)) => {
                    if std::env::var_os("MZ_FUZZ_TRACE").is_some() {
                        eprintln!("TRACE decoded {}", msg.name());
                        if let FrontendMessage::RawAuthentication(d) = &msg {
                            eprintln!(
                                "TRACE   password={:?} sasl_init={:?} sasl_resp={:?}",
                                decode_password(Cursor::new(d)).is_ok(),
                                decode_sasl_initial_response(Cursor::new(d)).is_ok(),
                                decode_sasl_response(Cursor::new(d)).is_ok(),
                            );
                        }
                    }
                    // `decode_auth` copies the payload verbatim without parsing
                    // it. The real parsers run in `protocol`, which picks one by
                    // handshake state: the two SASL parsers during SCRAM,
                    // `decode_password` for cleartext. This target has no
                    // connection state, so run all three on every payload. That
                    // is a superset of what a single connection reaches, but each
                    // is reachable pre-auth, so a panic in any of them is a real
                    // pre-auth availability bug.
                    if let FrontendMessage::RawAuthentication(data) = msg {
                        let _ = decode_password(Cursor::new(&data));
                        let _ = decode_sasl_initial_response(Cursor::new(&data));
                        let _ = decode_sasl_response(Cursor::new(&data));
                    }
                    continue;
                }
                Ok(None) => break,
                Err(_) => {
                    // Production `FramedConn` tears the connection down on the
                    // first error, so nothing ever resumes mid-frame. Match that
                    // by starting over rather than leaving `decode_state` stuck
                    // in `Data(stale_tag, stale_len)`, which would shred every
                    // later frame at the stale length and re-parse it under the
                    // stale tag instead of its own.
                    codec = Codec::new();
                    // A body-parse error has already split the frame off, so the
                    // buffer shrank and the next frame is aligned. A header
                    // rejection (`parse_frame_len`, or the aggregate size cap)
                    // consumes nothing, so continuing would spin on the same
                    // bytes forever.
                    if buf.len() == before {
                        break;
                    }
                }
            }
        }
    };

    if chunked {
        let mut rest = data;
        while !rest.is_empty() {
            let take = u.int_in_range(1usize..=rest.len())?;
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
    // unknown-tag error paths covered, and is the only path on which a
    // hand-written wire capture reaches the decoder as written.
    //
    // NOTE: `int_in_range` consumes from the *front*, one byte per decision, so
    // this prefix is a wire format the corpus depends on: `data[0] % 4 == 0`
    // selects this branch, `data[1] % 2 == 0` selects chunked, and `data[2..]` is
    // what the decoder sees byte for byte. `prepare-corpus.sh` prepends
    // `\x00\x00` to every seed to land here. Reordering these two decisions, or
    // adding a third ahead of them, silently repurposes every seed's leading
    // bytes and strands the corpus.
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
