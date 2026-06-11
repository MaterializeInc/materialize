// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: exercises `ProtoStateDiff` decoding and the `StateDiff<u64>`
//! `from_proto` conversion. State diffs are read from consensus on every state
//! update, so a decoder panic on a corrupted/crafted blob poisons the shard.
//!
//! The interesting surface is `ProtoStateDiff::field_diffs`, a *columnar*
//! encoding (`ProtoStateFieldDiffs`): parallel `fields`/`diff_types` arrays plus
//! a `data_lens`/`data_bytes` blob holding the per-diff key and value slices.
//! `from_proto` first `validate()`s that the slice counts and byte lengths are
//! self-consistent, then iterates, slicing `data_bytes` and decoding each slice
//! as the sub-proto for that field. Decoding *random* bytes as a protobuf
//! essentially never produces a `field_diffs` that survives `validate()` (let
//! alone one whose slices decode), so the columnar reader is never exercised.
//!
//! This target therefore hand-builds the columnar encoding on the protobuf wire
//! from fuzzer-chosen parameters. The first byte selects a mode:
//!
//! * mode 0: feed the remaining bytes straight to `ProtoStateDiff::decode`
//!   (the original robustness arm — must never panic, and any value that
//!   converts must survive a proto re-encode round trip losslessly).
//! * mode 1: synthesize a *valid* columnar diff over a mix of fields and
//!   insert/update/delete diff types, with self-consistent slice counts and
//!   lengths, then decode + round-trip it.
//! * mode 2: synthesize a diff whose columnar bookkeeping is *inconsistent*
//!   (slice count or byte length mismatch, or an unknown field/diff-type enum).
//!   `from_proto`'s `validate()`/iter must reject it with `Err`, never panic.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_persist_client::fuzz_exports::{ProtoStateDiff, StateDiff};
use mz_proto::{ProtoType, RustType};
use prost::Message;

// --- Minimal protobuf wire-format writer ---------------------------------
//
// Building the bytes by hand lets us feed the *real* `ProtoStateDiff::decode` a
// well-formed columnar message without naming the nested proto types that the
// `fuzz` feature does not re-export.

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

    /// A small count in `[lo, hi]`, biased toward the low end.
    fn range(&mut self, lo: usize, hi: usize) -> usize {
        if hi <= lo {
            return lo;
        }
        lo + (usize::from(self.u8()) % (hi - lo + 1))
    }
}

// --- Trivial-message encoders --------------------------------------------
//
// prost encodes scalar "trivial messages" with the value at field tag 1
// (`u64`/`i64` as a varint, `String`/`bytes` as length-delimited). The default
// value (0 / empty) encodes to *zero* bytes, so an empty slice is a valid
// encoding of every key/value type the diff reader decodes.

/// Encoded slice for a `u64` trivial message.
fn enc_u64(v: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    if v != 0 {
        put_uint(&mut buf, 1, v);
    }
    buf
}

/// Encoded slice for a `String` trivial message.
fn enc_string(s: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    if !s.is_empty() {
        put_bytes(&mut buf, 1, s.as_bytes());
    }
    buf
}

/// Encoded slice for `ProtoU64Antichain { repeated int64 elements = 1; }`.
fn enc_antichain(elements: &[u64]) -> Vec<u8> {
    let mut buf = Vec::new();
    for e in elements {
        put_uint(&mut buf, 1, *e);
    }
    buf
}

/// Encoded slice for `ProtoHollowRollup { string key = 1; optional uint64 encoded_size_bytes = 2; }`.
fn enc_hollow_rollup(key: &str, encoded_size_bytes: Option<u64>) -> Vec<u8> {
    let mut buf = Vec::new();
    if !key.is_empty() {
        put_bytes(&mut buf, 1, key.as_bytes());
    }
    if let Some(sz) = encoded_size_bytes {
        put_uint(&mut buf, 2, sz);
    }
    buf
}

/// Encoded slice for `ProtoActiveRollup`/`ProtoActiveGC { uint64 seqno = 1; uint64 start_ms = 2; }`.
fn enc_active(seqno: u64, start_ms: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    if seqno != 0 {
        put_uint(&mut buf, 1, seqno);
    }
    if start_ms != 0 {
        put_uint(&mut buf, 2, start_ms);
    }
    buf
}

// ProtoStateField enum values (see diff.proto).
const FIELD_LAST_GC_REQ: u64 = 1;
const FIELD_SINCE: u64 = 4;
const FIELD_HOSTNAME: u64 = 7;
const FIELD_ROLLUPS: u64 = 8;
const FIELD_ACTIVE_ROLLUP: u64 = 13;
const FIELD_ACTIVE_GC: u64 = 14;

// ProtoStateFieldDiffType enum values.
const DIFF_INSERT: u64 = 0;
const DIFF_UPDATE: u64 = 1;
const DIFF_DELETE: u64 = 2;

/// A single columnar diff: the field, the diff type, and the already-encoded
/// key/value slices (1 key + 1 or 2 vals, matching the diff type).
struct ColumnarDiff {
    field: u64,
    diff_type: u64,
    /// key slice followed by 1 (insert/delete) or 2 (update) value slices.
    slices: Vec<Vec<u8>>,
}

/// Picks a field + diff type + correctly-shaped key/value slices.
///
/// All fields here decode losslessly from the encoded slices (scalar/simple
/// sub-protos with no required nested fields), so the un-mutated diff survives
/// a full proto round trip. Batch/reader/writer/schema fields are intentionally
/// avoided: their sub-protos have required nested messages that would not
/// decode from an empty/default slice.
fn gen_diff(u: &mut Unstructured) -> ColumnarDiff {
    let diff_type = match u.u8() % 3 {
        0 => DIFF_INSERT,
        1 => DIFF_UPDATE,
        _ => DIFF_DELETE,
    };
    let num_vals = if diff_type == DIFF_UPDATE { 2 } else { 1 };

    // (field, key slice, value-slice generator).
    let (field, key): (u64, Vec<u8>) = match u.u8() % 6 {
        0 => (FIELD_HOSTNAME, Vec::new()),    // key ()
        1 => (FIELD_LAST_GC_REQ, Vec::new()), // key ()
        2 => (FIELD_SINCE, Vec::new()),       // key ()
        3 => (FIELD_ROLLUPS, enc_u64(u.u64() % 10_000)), // key u64 (SeqNo)
        4 => (FIELD_ACTIVE_ROLLUP, Vec::new()), // key ()
        _ => (FIELD_ACTIVE_GC, Vec::new()),   // key ()
    };

    let mut slices = Vec::with_capacity(1 + num_vals);
    slices.push(key);
    for _ in 0..num_vals {
        let val = match field {
            FIELD_HOSTNAME => enc_string(&format!("host-{}", u.u8())),
            FIELD_LAST_GC_REQ => enc_u64(u.u64() % 10_000),
            FIELD_SINCE => {
                let n = u.range(0, 2);
                let elems: Vec<u64> = (0..n).map(|_| u.u64() % 10_000).collect();
                enc_antichain(&elems)
            }
            FIELD_ROLLUPS => {
                let sz = if u.u8() & 1 == 0 {
                    Some(u.u64() % 1_000_000)
                } else {
                    None
                };
                enc_hollow_rollup(&format!("rollup-{}", u.u8()), sz)
            }
            FIELD_ACTIVE_ROLLUP | FIELD_ACTIVE_GC => {
                enc_active(u.u64() % 10_000, u.u64() % 1_000_000)
            }
            _ => Vec::new(),
        };
        slices.push(val);
    }

    ColumnarDiff {
        field,
        diff_type,
        slices,
    }
}

/// Serializes `ProtoStateFieldDiffs { fields, diff_types, data_lens, data_bytes }`
/// from a list of columnar diffs.
fn encode_field_diffs(diffs: &[ColumnarDiff]) -> Vec<u8> {
    let mut fields = Vec::new();
    let mut diff_types = Vec::new();
    let mut data_lens = Vec::new();
    let mut data_bytes: Vec<u8> = Vec::new();

    for d in diffs {
        put_uint(&mut fields, 1, d.field);
        put_uint(&mut diff_types, 2, d.diff_type);
        for slice in &d.slices {
            put_uint(&mut data_lens, 3, slice.len() as u64);
            data_bytes.extend_from_slice(slice);
        }
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(&fields);
    buf.extend_from_slice(&diff_types);
    buf.extend_from_slice(&data_lens);
    if !data_bytes.is_empty() {
        put_bytes(&mut buf, 4, &data_bytes);
    }
    buf
}

/// Wraps a `field_diffs` payload into a full `ProtoStateDiff`.
fn encode_state_diff(u: &mut Unstructured, field_diffs: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    // applier_version = 1 (parseable semver, or empty for the backward-compat path).
    if u.u8() & 1 == 0 {
        put_bytes(&mut buf, 1, b"0.1.2");
    }
    // seqno_from = 2, seqno_to = 3
    put_uint(&mut buf, 2, u.u64() % 10_000);
    put_uint(&mut buf, 3, u.u64() % 10_000);
    // latest_rollup_key = 4 (free-form string)
    put_bytes(&mut buf, 4, b"rollup-key");
    // field_diffs = 5
    put_bytes(&mut buf, 5, field_diffs);
    // walltime_ms = 6
    put_uint(&mut buf, 6, u.u64());
    buf
}

fn roundtrip(proto: ProtoStateDiff) {
    let orig: StateDiff<u64> = match proto.into_rust() {
        Ok(v) => v,
        Err(_) => return,
    };

    let proto2: ProtoStateDiff = orig.into_proto();
    let bytes2 = proto2.encode_to_vec();
    let proto3 = ProtoStateDiff::decode(bytes2.as_slice())
        .expect("re-encode of valid StateDiff must decode");
    let round: StateDiff<u64> = proto3
        .into_rust()
        .expect("re-encoded StateDiff must convert back to Rust");
    let proto4: ProtoStateDiff = round.into_proto();

    assert_eq!(proto2, proto4, "StateDiff changed across proto roundtrip");
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
            let Ok(proto) = ProtoStateDiff::decode(rest) else {
                return;
            };
            roundtrip(proto);
        }
        1 => {
            // Valid columnar arm: a self-consistent set of field diffs that must
            // decode + round-trip.
            let mut u = Unstructured::new(rest);
            let num_diffs = u.range(0, 6);
            let diffs: Vec<ColumnarDiff> = (0..num_diffs).map(|_| gen_diff(&mut u)).collect();
            let field_diffs = encode_field_diffs(&diffs);
            let bytes = encode_state_diff(&mut u, &field_diffs);
            let proto = ProtoStateDiff::decode(bytes.as_slice())
                .expect("hand-built ProtoStateDiff must decode");
            roundtrip(proto);
        }
        _ => {
            // Inconsistent-columnar arm: build a valid set, then corrupt the
            // bookkeeping so `validate()`/iter must reject it (not panic).
            let mut u = Unstructured::new(rest);
            let num_diffs = u.range(1, 6);
            let mut diffs: Vec<ColumnarDiff> =
                (0..num_diffs).map(|_| gen_diff(&mut u)).collect();

            match u.u8() % 4 {
                0 => {
                    // Drop a value slice: data_lens count no longer matches the
                    // count implied by diff_types.
                    if let Some(last) = diffs.last_mut() {
                        last.slices.pop();
                    }
                }
                1 => {
                    // Append a spurious data slice with no corresponding diff.
                    if let Some(last) = diffs.last_mut() {
                        last.slices.push(vec![0xff; u.range(1, 4)]);
                    }
                }
                2 => {
                    // Unknown field enum value.
                    if let Some(last) = diffs.last_mut() {
                        last.field = 9999;
                    }
                }
                _ => {
                    // Unknown diff-type enum value.
                    if let Some(last) = diffs.last_mut() {
                        last.diff_type = 9999;
                    }
                }
            }

            let field_diffs = encode_field_diffs(&diffs);
            let bytes = encode_state_diff(&mut u, &field_diffs);
            let proto = ProtoStateDiff::decode(bytes.as_slice())
                .expect("hand-built ProtoStateDiff must decode");
            let result: Result<StateDiff<u64>, _> = proto.into_rust();
            assert!(
                result.is_err(),
                "StateDiff with inconsistent columnar field_diffs must be rejected by from_proto"
            );
        }
    }
});
