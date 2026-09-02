// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: exercises `ProtoRollup` decoding and the `Rollup<u64>`
//! `from_proto` conversion. A rollup is a full state snapshot read from blob on
//! load, so a decoder panic on a corrupted/crafted blob makes the shard
//! unrecoverable. NOTE: an `Err` is not a *good* outcome on that path either.
//! `UntypedState::decode` `.expect()`s the conversion, so a rejected rollup
//! still aborts the process. What this target buys is a decoder that fails
//! predictably and boundedly (no hang, no OOM, no wild slicing) plus a working
//! error path for the CLI/inspect callers that do surface it.
//!
//! Decoding *random* bytes as a protobuf almost never yields a `ProtoRollup`
//! that survives `into_rust`: the conversion needs a well-formed shard id
//! (`s` + UUID), a present `trace`, and, most interestingly, inlined `diffs`
//! whose `lower`/`upper` bounds line up with the latest rollup's seqno and the
//! state's seqno. Random input plateaus long before reaching those cross-field
//! invariant checks, so this target hand-builds a *structurally valid*
//! `ProtoRollup` on the protobuf wire from fuzzer-chosen parameters. The first
//! byte selects a mode:
//!
//! * mode 0: feed the remaining bytes straight to `ProtoRollup::decode` (the
//!   robustness arm: must never panic, and any value that converts must be
//!   *stable* under a further round trip). Arbitrary bytes can be non-canonical,
//!   so this arm can only assert stability, not losslessness. Mode 1 covers that.
//! * mode 1: synthesize a valid rollup *with* inlined diffs whose bounds
//!   satisfy the invariants, then require that it converts and that the
//!   conversion is *lossless*: the hand-built message is canonical, so
//!   re-encoding the converted value must reproduce it field for field.
//! * mode 2: synthesize a valid rollup, then *perturb a single invariant
//!   field* (drop the rollups map, or shift `diffs.lower`/`diffs.upper` off
//!   the expected seqno). `from_proto` must reject it with an `Err` that names
//!   the perturbed field, never panic. Matching the message matters: a bare
//!   `is_err()` would keep passing if the rollup started being rejected for
//!   some unrelated reason, hiding that the mutation stopped being tested.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_persist_client::fuzz_exports::{ProtoRollup, Rollup};
use mz_proto::{ProtoType, RustType};
use prost::Message;

// --- Minimal protobuf wire-format writer ---------------------------------
//
// We only need the few wire types used by `ProtoRollup` and its (private,
// not-re-exported) nested messages. Building the bytes by hand lets us feed
// the *real* `ProtoRollup::decode` a structurally valid message without naming
// types that the `fuzz` feature does not export.

const WIRE_VARINT: u32 = 0;
const WIRE_LEN: u32 = 2;

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if v == 0 {
            break;
        }
    }
}

fn put_key(buf: &mut Vec<u8>, tag: u32, wire: u32) {
    put_varint(buf, ((tag as u64) << 3) | (wire as u64));
}

/// Writes a varint (`uint64`/`int64`/`enum`) field.
fn put_uint(buf: &mut Vec<u8>, tag: u32, v: u64) {
    put_key(buf, tag, WIRE_VARINT);
    put_varint(buf, v);
}

/// Writes a length-delimited field (string/bytes/sub-message).
fn put_bytes(buf: &mut Vec<u8>, tag: u32, payload: &[u8]) {
    put_key(buf, tag, WIRE_LEN);
    put_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

// --- Pull-style reader over the fuzzer's parameter bytes -----------------

struct Unstructured<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Unstructured<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Unstructured { bytes, pos: 0 }
    }

    fn u8(&mut self) -> u8 {
        let b = self.bytes.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        b
    }

    fn u64(&mut self) -> u64 {
        let mut v = 0u64;
        for _ in 0..8 {
            v = (v << 8) | u64::from(self.u8());
        }
        v
    }

    /// A small count in `[0, max]`, biased toward the low end.
    fn count(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        usize::from(self.u8()) % (max + 1)
    }
}

// --- Sub-message builders -------------------------------------------------

/// `ProtoU64Antichain { repeated int64 elements = 1; }`
fn antichain(elements: &[u64]) -> Vec<u8> {
    let mut buf = Vec::new();
    for e in elements {
        put_uint(&mut buf, 1, *e);
    }
    buf
}

/// `ProtoTrace { ProtoU64Antichain since = 1; ... }`. An empty trace (no
/// batches) with just a `since` antichain, which unflattens cleanly.
fn empty_trace(since: &[u64]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_bytes(&mut buf, 1, &antichain(since));
    buf
}

/// `ProtoHollowRollup { string key = 1; optional uint64 encoded_size_bytes = 2; }`
fn hollow_rollup(key: &str, encoded_size_bytes: Option<u64>) -> Vec<u8> {
    let mut buf = Vec::new();
    put_bytes(&mut buf, 1, key.as_bytes());
    if let Some(sz) = encoded_size_bytes {
        put_uint(&mut buf, 2, sz);
    }
    buf
}

/// One entry of `map<uint64, ProtoHollowRollup> rollups = 16;`
fn rollups_entry(seqno: u64, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_uint(&mut buf, 1, seqno);
    put_bytes(&mut buf, 2, value);
    buf
}

/// `ProtoVersionedData { uint64 seqno = 1; bytes data = 2; }`
fn versioned_data(seqno: u64, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_uint(&mut buf, 1, seqno);
    put_bytes(&mut buf, 2, data);
    buf
}

/// `ProtoInlinedDiffs { uint64 lower = 1; uint64 upper = 2; repeated ProtoVersionedData diffs = 3; }`
fn inlined_diffs(lower: u64, upper: u64, diffs: &[(u64, Vec<u8>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_uint(&mut buf, 1, lower);
    put_uint(&mut buf, 2, upper);
    for (seqno, data) in diffs {
        put_bytes(&mut buf, 3, &versioned_data(*seqno, data));
    }
    buf
}

/// Which invariant to break in mode 2.
enum Mutation {
    DropRollups,
    ShiftLower,
    ShiftUpper,
}

/// Builds a structurally valid `ProtoRollup` wire encoding driven by `u`.
///
/// When `mutate` is `Some`, the named invariant is intentionally broken so the
/// caller can assert `from_proto` rejects it.
fn build_rollup(u: &mut Unstructured, mutate: Option<Mutation>) -> Vec<u8> {
    // A fixed, parseable shard id. The interesting surface is the diff bounds,
    // not shard-id parsing (covered by the raw-bytes arm).
    const SHARD_ID: &str = "s00000000-0000-0000-0000-000000000000";

    let mut buf = Vec::new();

    // shard_id = 1
    put_bytes(&mut buf, 1, SHARD_ID.as_bytes());
    // key_codec = 2, val_codec = 3, ts_codec = 4, diff_codec = 5 (free-form
    // strings on the rollup path, so pick fixed values).
    put_bytes(&mut buf, 2, b"()");
    put_bytes(&mut buf, 3, b"()");
    put_bytes(&mut buf, 4, b"u64");
    put_bytes(&mut buf, 5, b"i64");

    // Choose rollup seqnos. `latest_rollup` is the max key in the map, and the
    // state seqno must be >= it so the diff range is non-empty/well-formed.
    let num_rollups = u.count(3).max(1);
    let mut rollup_seqnos: Vec<u64> = Vec::with_capacity(num_rollups);
    let mut next = 1 + (u.u64() % 1000);
    for _ in 0..num_rollups {
        rollup_seqnos.push(next);
        next += 1 + (u.u64() % 1000);
    }
    let latest_rollup_seqno = *rollup_seqnos.iter().max().expect("non-empty");
    // state.seqno >= latest rollup seqno.
    let state_seqno = latest_rollup_seqno + (u.u64() % 1000);

    let drop_rollups = matches!(mutate, Some(Mutation::DropRollups));
    if !drop_rollups {
        for (i, seqno) in rollup_seqnos.iter().enumerate() {
            let key = format!("rollup-key-{i}");
            let sz = if u.u8() & 1 == 0 {
                Some(u.u64() % 1_000_000)
            } else {
                None
            };
            put_bytes(
                &mut buf,
                16,
                &rollups_entry(*seqno, &hollow_rollup(&key, sz)),
            );
        }
    }

    // seqno = 6
    put_uint(&mut buf, 6, state_seqno);
    // trace = 7 (required: `into_rust_if_some("trace")`).
    put_bytes(&mut buf, 7, &empty_trace(&[u.u64() % 1000]));
    // last_gc_req = 10
    put_uint(&mut buf, 10, u.u64() % 1000);
    // applier_version = 11 (parseable semver, or empty for the "infinitely old"
    // backward-compat path).
    if u.u8() & 1 == 0 {
        put_bytes(&mut buf, 11, b"0.1.2");
    }
    // hostname = 14
    put_bytes(&mut buf, 14, b"fuzz-host");
    // walltime_ms = 15
    put_uint(&mut buf, 15, u.u64());

    // Optionally attach inlined diffs with invariant-satisfying bounds.
    //
    // The decoder requires:
    //   diffs.lower == latest_rollup_seqno + 1
    //   diffs.upper == state_seqno + 1
    // (`SeqNo::next` is `+ 1`). Each inlined `VersionedData` must sit within
    // `[lower, upper)`. Bounds become degenerate when `state_seqno ==
    // latest_rollup_seqno`, in which case there is no valid diff seqno.
    let want_diffs = mutate.is_some() || (u.u8() & 1 == 0);
    if want_diffs {
        let mut lower = latest_rollup_seqno + 1;
        let mut upper = state_seqno + 1;
        match &mutate {
            Some(Mutation::ShiftLower) => lower = lower.wrapping_add(1 + (u.u64() % 5)),
            Some(Mutation::ShiftUpper) => upper = upper.wrapping_add(1 + (u.u64() % 5)),
            _ => {}
        }

        // Diff seqnos start at `lower`, which is the shifted one for
        // `ShiftLower`, so the mutated cases can place them outside the true
        // range. That is harmless: `from_proto` checks only the `lower`/`upper`
        // bounds and never the individual diff seqnos, so this is about
        // producing realistic content rather than satisfying an invariant.
        let mut diffs: Vec<(u64, Vec<u8>)> = Vec::new();
        if state_seqno > latest_rollup_seqno {
            let span = upper.saturating_sub(lower).min(4);
            let n = if span == 0 { 0 } else { u.count(span as usize) };
            for i in 0..n {
                let seqno = lower + (i as u64);
                diffs.push((seqno, vec![u.u8(), u.u8(), u.u8()]));
            }
        }
        put_bytes(&mut buf, 17, &inlined_diffs(lower, upper, &diffs));
    }

    buf
}

/// Round trip whatever converts, tolerating inputs that don't.
///
/// Only for mode 0, where arbitrary bytes legitimately fail to convert. A mode
/// that builds a rollup it *knows* is valid must require the conversion instead,
/// or a newly-rejected shape silently turns the arm into a no-op.
fn roundtrip_lenient(proto: ProtoRollup) {
    let orig: Rollup<u64> = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };
    roundtrip_from(orig.into_proto());
}

/// Asserts a canonical proto (already an `into_proto` output) is stable under a
/// further encode / decode / convert / re-encode.
///
/// Both sides of the comparison are post-`into_proto`, so this only proves
/// `into_proto ∘ from_proto` is idempotent. Losslessness of the *first* pass is
/// mode 1's job, since only a hand-built canonical input can assert it.
fn roundtrip_from(proto2: ProtoRollup) {
    let bytes2 = proto2.encode_to_vec();
    let proto3 =
        ProtoRollup::decode(bytes2.as_slice()).expect("re-encode of valid Rollup must decode");
    let round: Rollup<u64> = proto3
        .into_rust()
        .expect("re-encoded Rollup must convert back to Rust");
    let proto4: ProtoRollup = round.into_proto();

    assert_eq!(proto2, proto4, "Rollup changed across proto roundtrip");
}

fuzz_target!(|data: &[u8]| {
    let (mode, rest) = match data.split_first() {
        Some((m, r)) => (*m, r),
        None => return,
    };

    match mode % 3 {
        0 => {
            // Robustness arm: arbitrary bytes must never panic the decoder, and
            // anything that converts must round-trip.
            let Ok(proto) = ProtoRollup::decode(rest) else {
                return;
            };
            roundtrip_lenient(proto);
        }
        1 => {
            // Valid-rollup arm: the synthesized message satisfies the diff
            // invariants, so it must convert, and convert losslessly.
            let mut u = Unstructured::new(rest);
            let bytes = build_rollup(&mut u, None);
            let proto =
                ProtoRollup::decode(bytes.as_slice()).expect("hand-built ProtoRollup must decode");
            // Every shape `build_rollup` emits is valid, so rejection here is a
            // find, not a reason to skip the arm.
            let orig: Rollup<u64> = proto
                .clone()
                .into_rust()
                .expect("synthesized rollup must convert");
            let proto2: ProtoRollup = orig.into_proto();
            // The strong oracle: the input is already canonical, so a field the
            // conversion drops or defaults shows up right here. Comparing two
            // `into_proto` outputs (as `roundtrip_from` does) cannot see that,
            // and `Rollup::into_proto` reaches through `self.state.state...`
            // rather than destructuring, so a field added to `State` and wired
            // into `from_proto` but forgotten in `into_proto` compiles fine and
            // is then silently lost from every rollup written.
            //
            // The one legitimate normalization: an absent `applier_version`
            // decodes as "infinitely old" and re-encodes as 0.0.0.
            let mut expected = proto;
            if expected.applier_version.is_empty() {
                expected.applier_version = "0.0.0".into();
            }
            assert_eq!(
                expected, proto2,
                "field lost across ProtoRollup -> Rollup -> ProtoRollup"
            );
            roundtrip_from(proto2);
        }
        _ => {
            // Invariant-violation arm: break exactly one invariant and require
            // `from_proto` to reject it, naming that invariant (not panic, and
            // not fail for some unrelated reason).
            let mut u = Unstructured::new(rest);
            let (mutation, expected_err) = match u.u8() % 3 {
                0 => (Mutation::DropRollups, "no rollups"),
                1 => (Mutation::ShiftLower, "diffs lower"),
                _ => (Mutation::ShiftUpper, "diffs upper"),
            };
            let bytes = build_rollup(&mut u, Some(mutation));
            let proto =
                ProtoRollup::decode(bytes.as_slice()).expect("hand-built ProtoRollup must decode");
            let result: Result<Rollup<u64>, _> = proto.into_rust();
            let err = result.err().expect(
                "Rollup with a broken diff-bounds invariant must be rejected by from_proto",
            );
            let msg = err.to_string();
            assert!(
                msg.contains(expected_err),
                "expected a rejection mentioning {expected_err:?}, got: {msg}"
            );
        }
    }
});
