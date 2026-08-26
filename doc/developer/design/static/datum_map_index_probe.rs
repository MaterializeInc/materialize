// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Index layout probe for `DatumMap`, supporting
//! `doc/developer/design/20260826_datum_map_index_alternatives.md`.
//!
//! Standalone by design: most of the layouts it compares do not exist in
//! `mz-repr`, and implementing them there in order to benchmark them would be
//! the change the design document is trying to decide on. It models the
//! `DatumMap` payload byte shape instead (tag, length, key, value encoding, the
//! {1, 2, 4}-byte offset widths, the trailing count word) and cross-checks that
//! every layout resolves every probe identically before reporting a timing.
//!
//! Measures, per layout and map size:
//!   pack     build the payload from key-sorted entries (every decode path)
//!   get      single-key lookup: `->`, `->>`, `#>`, `?`, `?&`, `?|`, `@>`
//!   miss     the same, for a key that is absent
//!   encode   emit entries in key order: Arrow/persist write, pgwire text,
//!            `jsonb_stringify`, `jsonb_object_keys`
//!   rebuild  scan and repack minus one key: `jsonb_delete_string`, `||`
//!   iter     walk every entry in storage order
//!
//! No dependencies, so it builds with rustc directly:
//!
//! ```text
//! rustc -O --edition 2021 datum_map_index_probe.rs -o /tmp/probe && /tmp/probe
//! ```
//!
//! Takes an optional corpus size in bytes (default 16 MiB).
//! `MAPBENCH_SMOKE=1` runs a fast pass over fewer sizes.
//! `MAPBENCH_CHECK=1` runs the whole size and key-style matrix with one round
//! and a tiny corpus, for the cross-check rather than the timings. Use it after
//! touching a layout: the cross-check is what catches a search that misses a
//! key, which a timing run would otherwise report as a fast wrong answer.


mod hash {
//! A fixed-seed, portable, deterministic hash. Any hash used in a `Row` payload
//! must be all three: `Row` equality is byte equality, so the hash becomes part
//! of the in-memory format contract. That rules out `ahash` (per-process random
//! seeds) and `DefaultHasher` (unspecified, may change between Rust releases).

#[inline(always)]
pub fn hash_seeded(bytes: &[u8], seed: u64) -> u64 {
    const K: u64 = 0x517c_c1b7_2722_0a95;
    let mut h = (0x9e37_79b9_7f4a_7c15u64 ^ seed).wrapping_mul(K) ^ (bytes.len() as u64);
    let mut chunks = bytes.chunks_exact(8);
    for c in &mut chunks {
        h = (h ^ u64::from_le_bytes(c.try_into().unwrap()))
            .wrapping_mul(K)
            .rotate_left(29);
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut b = [0u8; 8];
        b[..rem.len()].copy_from_slice(rem);
        h = (h ^ u64::from_le_bytes(b)).wrapping_mul(K).rotate_left(29);
    }
    h ^ (h >> 32)
}

#[inline(always)]
pub fn hash_key(bytes: &[u8]) -> u64 {
    hash_seeded(bytes, 0)
}

/// Fingerprints come from the high bits, which the bucket index does not use.
#[inline(always)]
pub fn fingerprint(h: u64) -> u8 {
    (h >> 56) as u8
}

#[inline(always)]
pub fn fingerprint16(h: u64) -> u16 {
    (h >> 48) as u16
}

/// The 32-bit hash `E` sorts by and stores. Truncation must happen *before* the
/// sort: sorting by the full 64-bit hash would not leave the stored 32-bit
/// array sorted, which is the kind of bug a binary search hides as a miss.
#[inline(always)]
pub fn hash32(bytes: &[u8]) -> u32 {
    (hash_key(bytes) >> 32) as u32
}

}

mod layout {
//! Payload layouts for the `DatumMap` in-map index design space.
//!
//! Every layout models the real `DatumMap` payload shape: a self-describing
//! sequence of `(key, value)` datum pairs followed by an index *suffix* and a
//! trailing count word. Reader-recoverable quantities (`n`, `width`,
//! `entries_len`) are cached in `Map` exactly as a reader recovers them from the
//! count word, so no layout is charged for header parsing the others avoid.

use crate::hash::{fingerprint, fingerprint16, hash32, hash_key, hash_seeded};

pub const TAG_STR_TINY: u8 = 1;
pub const TAG_STR_SHORT: u8 = 2;
pub const TAG_INT64: u8 = 3;
pub const TAG_STR_VAL: u8 = 4;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Layout {
    /// Linear scan. What `main` does today.
    L,
    /// Key-sorted entries + offsets[1..n]; binary search with string compares.
    /// This is PR #37085.
    A,
    /// Open-addressed hash table of offsets, load factor 0.5.
    B,
    /// `A` + a 1-byte hash fingerprint per key, SWAR-scanned 8 at a time.
    C,
    /// Minimal perfect hash into exactly `n` slots, found by seed search.
    D,
    /// Entries sorted by (hash, key) + offsets + a `u32` hash array; binary
    /// search over integers. Changes iteration order.
    E,
    /// Key-sorted entries + a hash-sorted side index of `(h16, entry_index)`;
    /// binary search over integers, iteration order preserved.
    F,
    /// `A` + a 4-byte big-endian key prefix per entry. Big-endian byte order is
    /// monotone with respect to lexicographic order, so the prefix array is
    /// sorted exactly when the keys are: binary search compares integers and
    /// reads a key only when prefixes tie.
    G,
    /// `A` + 2-byte fingerprints, SWAR-scanned 4 at a time.
    I,
    /// `G` with (prefix, offset) interleaved so one probe touches one cache line.
    K,
    /// Sparse prefix index: one 4-byte prefix per group of 8 keys, then a linear
    /// scan within the group.
    M,
    /// `K`, but the map's longest common key prefix is stripped before the
    /// 4-byte prefix is taken, so the stored bytes are the ones that actually
    /// discriminate. Since every key shares the LCP, stripping it preserves
    /// order. Keys are sorted, so the LCP of the whole map is the LCP of the
    /// first and last key: O(1) to find at pack time.
    N,
    /// `N` with a 2-byte prefix, to see how far the discriminating bytes can be
    /// trimmed.
    P,
    /// `N` with a 1-byte prefix: the cheapest possible discriminator.
    T,
    /// `N`'s array searched by interpolation rather than bisection. After LCP
    /// stripping the discriminating bytes of a machine-generated key set are
    /// close to uniform, which is the case interpolation search is built for.
    Q,
    /// `N`'s prefixes in a contiguous (non-interleaved) array, located by a
    /// branchless rank: count how many prefixes sort below the probe. No
    /// mispredicted branches, and the count vectorizes.
    V,
    /// The steelman for hash ordering: entries stored in hash order for the
    /// integer binary search of `E`, plus a key-order permutation so `iter()`
    /// still yields ascending keys. Semantics preserved, at the cost of the
    /// permutation's bytes and an indirection on every scan.
    H,
    /// `N` with the prefix width chosen from the data: the narrowest of
    /// {1, 2, 4} bytes that tells every LCP-stripped key apart, falling back to
    /// 4. Composes with the offset-width class the PR already packs into the
    /// count word, so a narrow key set pays one byte per entry.
    X,
    /// `V`'s contiguous prefix array, located by a branchless bisection: always
    /// ceil(log2 n) integer compares, no data-dependent branches, then one
    /// forward run over equal prefixes to verify.
    Y,
    /// `X`'s adaptive-width discriminator in a contiguous array, scanned
    /// branch-free with SWAR rather than bisected. The width rule makes the
    /// prefixes strictly increasing, hence unique, so the scan has at most one
    /// candidate to verify.
    Z,
    /// `Z`'s layout with the search chosen from `n` at read time: the SWAR scan
    /// for small maps, the branch-free bisection for large ones. Same bytes as
    /// `Z`, so the choice is not part of the format.
    R,
    /// Keys and values in separate regions, the way PostgreSQL's `jsonb`
    /// container stores them, with an offset array for each. A key scan or a
    /// binary search then never touches a value byte. Costs a second offset
    /// array, and forces the packer to buffer values separately.
    O,
    /// `O`'s layout with a linear scan over the key region: what today's `main`
    /// would cost if the values were out of the way.
    U,
    /// `Z`'s bytes with no scan at all: always the branch-free bisection. One
    /// code path, no crossover to tune.
    J,
    /// Speed of light: the caller already knows the key's slot (a shape-shared
    /// dictionary computed once per map shape, not per row). One offset read and
    /// one key verify.
    S,
}

/// How a layout must be walked to emit entries in ascending key order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Scan {
    /// Entries are already in key order: walk them.
    Sequential,
    /// Entries are in some other order: collect and sort.
    SortAfterScan,
    /// Entries are in some other order, but a stored permutation gives key
    /// order: walk the permutation and jump.
    Permuted,
    /// Keys and values live in separate regions: walk both with one cursor
    /// each.
    Split,
}

pub use Layout::*;

pub const ALL: [Layout; 25] = [L, A, B, C, D, E, F, G, I, K, M, N, P, T, Q, V, H, X, Y, Z, R, J, O, U, S];

impl Layout {
    pub fn name(self) -> &'static str {
        match self {
            L => "L lin",
            A => "A pr",
            B => "B htbl",
            C => "C fp8",
            D => "D mph",
            E => "E hsort",
            F => "F hidx",
            G => "G pfx",
            I => "I fp16",
            K => "K ilv",
            M => "M sparse",
            N => "N lcp4",
            P => "P lcp2",
            T => "T lcp1",
            Q => "Q interp",
            V => "V rank",
            H => "H hs+prm",
            X => "X adapt",
            Y => "Y brless",
            Z => "Z swar",
            R => "R hybrid",
            J => "J bisect",
            O => "O split",
            U => "U splitln",
            S => "S shape",
        }
    }

    /// How a key-ordered scan (the Arrow/persist write, pgwire text,
    /// `jsonb_stringify`, `jsonb_object_keys`) reaches the entries.
    pub fn scan(self) -> Scan {
        match self {
            E => Scan::SortAfterScan,
            H => Scan::Permuted,
            O | U => Scan::Split,
            _ => Scan::Sequential,
        }
    }
}

// ------------------------------------------------------------------ encoding

pub fn push_key(buf: &mut Vec<u8>, k: &str) {
    if k.len() < 256 {
        buf.push(TAG_STR_TINY);
        buf.push(k.len() as u8);
    } else {
        buf.push(TAG_STR_SHORT);
        buf.extend_from_slice(&(k.len() as u16).to_le_bytes());
    }
    buf.extend_from_slice(k.as_bytes());
}

pub fn push_value(buf: &mut Vec<u8>, value_len: usize, i: usize) {
    if value_len == 0 {
        buf.push(TAG_INT64);
        buf.extend_from_slice(&(i as i64).to_le_bytes());
    } else {
        buf.push(TAG_STR_VAL);
        buf.push(value_len as u8);
        buf.extend(std::iter::repeat(b'x').take(value_len));
    }
}

/// Mirrors `read_datum` for a string key.
#[inline(always)]
pub fn read_key<'a>(cursor: &mut &'a [u8]) -> &'a str {
    let (len, hdr) = match cursor[0] {
        TAG_STR_TINY => (cursor[1] as usize, 2),
        _ => (u16::from_le_bytes([cursor[1], cursor[2]]) as usize, 3),
    };
    let s = unsafe { std::str::from_utf8_unchecked(&cursor[hdr..hdr + len]) };
    *cursor = &cursor[hdr + len..];
    s
}

#[inline(always)]
pub fn skip_value(cursor: &[u8]) -> &[u8] {
    match cursor[0] {
        TAG_INT64 => &cursor[9..],
        _ => &cursor[2 + cursor[1] as usize..],
    }
}

pub fn offset_width(entries_len: usize) -> usize {
    if entries_len <= 0x100 {
        1
    } else if entries_len <= 0x1_0000 {
        2
    } else {
        4
    }
}

#[inline(always)]
pub fn put_off(buf: &mut Vec<u8>, off: usize, w: usize) {
    match w {
        1 => buf.push(off as u8),
        2 => buf.extend_from_slice(&(off as u16).to_le_bytes()),
        _ => buf.extend_from_slice(&(off as u32).to_le_bytes()),
    }
}

#[inline(always)]
pub fn get_off(data: &[u8], at: usize, w: usize) -> usize {
    match w {
        1 => data[at] as usize,
        2 => u16::from_le_bytes(data[at..at + 2].try_into().unwrap()) as usize,
        _ => u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as usize,
    }
}

/// Length of the longest common prefix of two strings, in bytes.
#[inline(always)]
pub fn lcp_len(a: &str, b: &str) -> usize {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// The 4-byte big-endian zero-padded prefix of `k` *after* dropping `skip`
/// bytes. A probe shorter than `skip` yields 0, which steers the search left
/// and ends in a full-key compare that fails, so a short probe is a miss
/// without a special case.
#[inline(always)]
pub fn key_prefix_from(k: &str, skip: usize) -> u32 {
    let b = k.as_bytes();
    let rest = if skip < b.len() { &b[skip..] } else { &[][..] };
    let mut p = [0u8; 4];
    let take = rest.len().min(4);
    p[..take].copy_from_slice(&rest[..take]);
    u32::from_be_bytes(p)
}

/// The narrowest prefix width in {1, 2, 4} that separates every key once the
/// map's common prefix is dropped, or 4 if none does. Keys arrive sorted, so
/// adjacent pairs are the only ones that can collide.
pub fn adaptive_prefix_width(keys: &[&str], lcp: usize) -> usize {
    let distinct_at = |w: usize| -> bool {
        keys.windows(2).all(|p| match w {
            1 => key_prefix8_from(p[0], lcp) != key_prefix8_from(p[1], lcp),
            2 => key_prefix16_from(p[0], lcp) != key_prefix16_from(p[1], lcp),
            _ => key_prefix_from(p[0], lcp) != key_prefix_from(p[1], lcp),
        })
    };
    if distinct_at(1) {
        1
    } else if distinct_at(2) {
        2
    } else {
        4
    }
}

#[inline(always)]
pub fn key_prefix8_from(k: &str, skip: usize) -> u8 {
    let b = k.as_bytes();
    if skip < b.len() { b[skip] } else { 0 }
}

#[inline(always)]
pub fn key_prefix16_from(k: &str, skip: usize) -> u16 {
    let b = k.as_bytes();
    let rest = if skip < b.len() { &b[skip..] } else { &[][..] };
    let mut p = [0u8; 2];
    let take = rest.len().min(2);
    p[..take].copy_from_slice(&rest[..take]);
    u16::from_be_bytes(p)
}

/// The 4-byte big-endian zero-padded prefix of `k`. Weakly monotone with
/// respect to lexicographic order on `k`, which is what lets `G`/`K`/`M` binary
/// search it: keys that tie on the prefix fall through to a full compare.
#[inline(always)]
pub fn key_prefix(k: &str) -> u32 {
    let b = k.as_bytes();
    let mut p = [0u8; 4];
    let take = b.len().min(4);
    p[..take].copy_from_slice(&b[..take]);
    u32::from_be_bytes(p)
}

// ------------------------------------------------------------------ the map

#[derive(Clone)]
pub struct Map {
    pub payload: Vec<u8>,
    pub entries_len: usize,
    pub n: usize,
    pub width: usize,
    /// D: seed-search attempts, 0 if no seed was found within the cap.
    pub tries: u32,
}

pub const D_MAX_TRIES: u32 = 100_000;

/// Writes the entries in the order given, recording each one's offset. This is
/// work the packer already does; `A` gets its offsets from here for free, which
/// is what `push_indexed_dict_with` captures.
#[inline(always)]
pub fn write_entries(buf: &mut Vec<u8>, keys: &[&str], value_len: usize, offs: &mut Vec<usize>) {
    offs.clear();
    let base = buf.len();
    for (i, k) in keys.iter().enumerate() {
        offs.push(buf.len() - base);
        push_key(buf, k);
        push_value(buf, value_len, i);
    }
}

/// Builds the payload for `layout` from `keys`, which arrive **sorted by key**,
/// as every decode path in Materialize provides them.
///
/// `scratch` is reused across calls so the measurement is index-build cost, not
/// allocator cost.
pub struct Scratch {
    /// Values buffered while the keys are written, for the split layouts.
    pub values: Vec<u8>,
    pub voffs: Vec<usize>,
    pub offs: Vec<usize>,
    pub order: Vec<u32>,
    pub hashes: Vec<u64>,
    pub slots: Vec<usize>,
    pub pairs: Vec<(u16, u32)>,
    pub buckets: Vec<usize>,
}

impl Scratch {
    pub fn new() -> Self {
        Scratch {
            values: Vec::with_capacity(1 << 14),
            voffs: Vec::with_capacity(1024),
            offs: Vec::with_capacity(1024),
            order: Vec::with_capacity(1024),
            hashes: Vec::with_capacity(1024),
            slots: Vec::with_capacity(2048),
            pairs: Vec::with_capacity(1024),
            buckets: Vec::with_capacity(2048),
        }
    }
}

pub fn pack(
    layout: Layout,
    keys: &[&str],
    value_len: usize,
    buf: &mut Vec<u8>,
    s: &mut Scratch,
) -> (usize, usize, u32) {
    buf.clear();
    let n = keys.len();

    // `E` and `H` are the layouts that cannot write the entries in the order
    // they were handed: they must hash every key and reorder first.
    if layout == E || layout == H {
        s.hashes.clear();
        s.order.clear();
        for (i, k) in keys.iter().enumerate() {
            s.hashes.push(u64::from(hash32(k.as_bytes())));
            s.order.push(i as u32);
        }
        let hashes = &s.hashes;
        s.order
            .sort_unstable_by(|&a, &b| {
                hashes[a as usize]
                    .cmp(&hashes[b as usize])
                    .then_with(|| keys[a as usize].cmp(keys[b as usize]))
            });
        let mut reordered: Vec<&str> = Vec::with_capacity(n);
        for &i in &s.order {
            reordered.push(keys[i as usize]);
        }
        write_entries(buf, &reordered, value_len, &mut s.offs);
        let entries_len = buf.len();
        let w = offset_width(entries_len);
        for i in 1..n {
            put_off(buf, s.offs[i], w);
        }
        for &i in &s.order {
            buf.extend_from_slice(&(hashes[i as usize] as u32).to_le_bytes());
        }
        if layout == H {
            // Key-order permutation: `perm[j]` is where the j-th key in
            // ascending key order sits among the hash-ordered entries. `keys`
            // arrives key-sorted, so inverting `order` gives it directly.
            let iw = if n <= 255 { 1 } else { 2 };
            let mut inverse = vec![0u32; n];
            for (pos, &orig) in s.order.iter().enumerate() {
                inverse[orig as usize] = pos as u32;
            }
            for j in 0..n {
                if iw == 1 {
                    buf.push(inverse[j] as u8);
                } else {
                    buf.extend_from_slice(&(inverse[j] as u16).to_le_bytes());
                }
            }
        }
        buf.extend_from_slice(&(n as u32).to_le_bytes());
        return (entries_len, w, 0);
    }

    if layout == O || layout == U {
        // Keys first, values buffered alongside and appended after, which is the
        // cost a one-buffer packer pays to split the regions.
        s.offs.clear();
        s.voffs.clear();
        s.values.clear();
        for (i, k) in keys.iter().enumerate() {
            s.offs.push(buf.len());
            push_key(buf, k);
            s.voffs.push(s.values.len());
            push_value(&mut s.values, value_len, i);
        }
        let keys_len = buf.len();
        buf.extend_from_slice(&s.values);
        let entries_len = buf.len();
        let values_len = entries_len - keys_len;
        let wk = offset_width(keys_len);
        let wv = offset_width(values_len);
        for i in 1..n {
            put_off(buf, s.offs[i], wk);
        }
        for i in 1..n {
            put_off(buf, s.voffs[i], wv);
        }
        buf.extend_from_slice(&(keys_len as u32).to_le_bytes());
        buf.extend_from_slice(&(n as u32).to_le_bytes());
        return (entries_len, wk, 0);
    }

    write_entries(buf, keys, value_len, &mut s.offs);
    let entries_len = buf.len();
    let mut width = 1;
    let mut tries = 0;

    match layout {
        L => {}
        A | S => {
            width = offset_width(entries_len);
            for i in 1..n {
                put_off(buf, s.offs[i], width);
            }
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        B => {
            width = offset_width(entries_len + 1);
            let nb = (n * 2).next_power_of_two();
            let mask = nb - 1;
            s.buckets.clear();
            s.buckets.resize(nb, 0);
            for (i, k) in keys.iter().enumerate() {
                let mut b = (hash_key(k.as_bytes()) as usize) & mask;
                while s.buckets[b] != 0 {
                    b = (b + 1) & mask;
                }
                s.buckets[b] = s.offs[i] + 1;
            }
            for i in 0..nb {
                put_off(buf, s.buckets[i], width);
            }
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        C => {
            width = offset_width(entries_len);
            for i in 1..n {
                put_off(buf, s.offs[i], width);
            }
            for k in keys {
                buf.push(fingerprint(hash_key(k.as_bytes())));
            }
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        I => {
            width = offset_width(entries_len);
            for i in 1..n {
                put_off(buf, s.offs[i], width);
            }
            for k in keys {
                buf.extend_from_slice(&fingerprint16(hash_key(k.as_bytes())).to_le_bytes());
            }
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        D => {
            width = offset_width(entries_len + 1);
            s.slots.clear();
            s.slots.resize(n.max(1), usize::MAX);
            let mut found = 0u32;
            if n > 0 {
                'seeds: for seed in 1..=D_MAX_TRIES {
                    s.slots.iter_mut().for_each(|x| *x = usize::MAX);
                    for (i, k) in keys.iter().enumerate() {
                        let slot =
                            (hash_seeded(k.as_bytes(), seed as u64) % (n as u64)) as usize;
                        if s.slots[slot] != usize::MAX {
                            continue 'seeds;
                        }
                        s.slots[slot] = s.offs[i];
                    }
                    found = seed;
                    break;
                }
            }
            tries = found;
            if found == 0 {
                // No seed within the cap. Fall back to A so the map is still
                // readable; `tries == 0` marks the layout non-viable at this n.
                width = offset_width(entries_len);
                for i in 1..n {
                    put_off(buf, s.offs[i], width);
                }
                buf.extend_from_slice(&(n as u32).to_le_bytes());
            } else {
                for i in 0..n {
                    put_off(buf, s.slots[i], width);
                }
                buf.extend_from_slice(&found.to_le_bytes());
                buf.extend_from_slice(&(n as u32).to_le_bytes());
            }
        }
        F => {
            width = offset_width(entries_len);
            for i in 1..n {
                put_off(buf, s.offs[i], width);
            }
            s.pairs.clear();
            for (i, k) in keys.iter().enumerate() {
                s.pairs
                    .push((fingerprint16(hash_key(k.as_bytes())), i as u32));
            }
            s.pairs.sort_unstable();
            let iw = if n <= 255 { 1 } else { 2 };
            for &(h, i) in &s.pairs {
                buf.extend_from_slice(&h.to_le_bytes());
                if iw == 1 {
                    buf.push(i as u8);
                } else {
                    buf.extend_from_slice(&(i as u16).to_le_bytes());
                }
            }
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        G => {
            width = offset_width(entries_len);
            for i in 1..n {
                put_off(buf, s.offs[i], width);
            }
            for k in keys {
                buf.extend_from_slice(&key_prefix(k).to_le_bytes());
            }
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        K => {
            width = offset_width(entries_len);
            // Interleaved (prefix, offset) so a probe touches one cache line.
            for i in 0..n {
                buf.extend_from_slice(&key_prefix(keys[i]).to_le_bytes());
                put_off(buf, s.offs[i], width);
            }
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        M => {
            width = offset_width(entries_len);
            for i in 1..n {
                put_off(buf, s.offs[i], width);
            }
            // One prefix per group of 8: the first key of each group.
            let groups = n.div_ceil(8);
            for g in 0..groups {
                buf.extend_from_slice(&key_prefix(keys[g * 8]).to_le_bytes());
            }
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        N | P | T | Q => {
            width = offset_width(entries_len);
            // Sorted keys, so the whole map's LCP is the LCP of the extremes.
            let lcp = if n > 1 { lcp_len(keys[0], keys[n - 1]) } else { 0 };
            let lcp = lcp.min(255);
            for i in 0..n {
                match layout {
                    P => buf.extend_from_slice(&key_prefix16_from(keys[i], lcp).to_le_bytes()),
                    T => buf.push(key_prefix8_from(keys[i], lcp)),
                    _ => buf.extend_from_slice(&key_prefix_from(keys[i], lcp).to_le_bytes()),
                }
                put_off(buf, s.offs[i], width);
            }
            buf.push(lcp as u8);
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        X => {
            width = offset_width(entries_len);
            let lcp = if n > 1 { lcp_len(keys[0], keys[n - 1]) } else { 0 };
            let lcp = lcp.min(255);
            let pw = adaptive_prefix_width(keys, lcp);
            for i in 0..n {
                match pw {
                    1 => buf.push(key_prefix8_from(keys[i], lcp)),
                    2 => buf.extend_from_slice(&key_prefix16_from(keys[i], lcp).to_le_bytes()),
                    _ => buf.extend_from_slice(&key_prefix_from(keys[i], lcp).to_le_bytes()),
                }
                put_off(buf, s.offs[i], width);
            }
            buf.push(lcp as u8);
            buf.push(pw as u8);
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        Z | R | J => {
            width = offset_width(entries_len);
            let lcp = if n > 1 { lcp_len(keys[0], keys[n - 1]) } else { 0 };
            let lcp = lcp.min(255);
            let pw = adaptive_prefix_width(keys, lcp);
            // Prefixes contiguous so the scan can read whole words.
            for i in 0..n {
                match pw {
                    1 => buf.push(key_prefix8_from(keys[i], lcp)),
                    2 => buf.extend_from_slice(&key_prefix16_from(keys[i], lcp).to_le_bytes()),
                    _ => buf.extend_from_slice(&key_prefix_from(keys[i], lcp).to_le_bytes()),
                }
            }
            for i in 1..n {
                put_off(buf, s.offs[i], width);
            }
            buf.push(lcp as u8);
            buf.push(pw as u8);
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        V | Y => {
            width = offset_width(entries_len);
            let lcp = if n > 1 { lcp_len(keys[0], keys[n - 1]) } else { 0 };
            let lcp = lcp.min(255);
            // Prefixes contiguous, so the rank count can vectorize; offsets
            // after them.
            for i in 0..n {
                buf.extend_from_slice(&key_prefix_from(keys[i], lcp).to_le_bytes());
            }
            for i in 1..n {
                put_off(buf, s.offs[i], width);
            }
            buf.push(lcp as u8);
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        E | H | O | U => unreachable!("handled above"),
    }

    (entries_len, width, tries)
}

}

mod get {
//! Single-key lookup, one implementation per layout.

use crate::hash::{fingerprint, fingerprint16, hash_key, hash_seeded};
use crate::layout::*;

#[inline(always)]
fn entry_key<'a>(m: &'a Map, off: usize) -> &'a str {
    let mut cursor = &m.payload[off..m.entries_len];
    read_key(&mut cursor)
}

/// Offset of entry `i`, with entry 0 implicit at 0 (the PR's convention).
#[inline(always)]
fn off_at(m: &Map, i: usize) -> usize {
    if i == 0 {
        0
    } else {
        get_off(&m.payload, m.entries_len + m.width * (i - 1), m.width)
    }
}

pub fn get_l(m: &Map, key: &str, _hint: usize) -> bool {
    let mut cursor = &m.payload[..m.entries_len];
    while !cursor.is_empty() {
        if read_key(&mut cursor) == key {
            return true;
        }
        cursor = skip_value(cursor);
    }
    false
}

pub fn get_a(m: &Map, key: &str, _hint: usize) -> bool {
    let mut lo = 0;
    let mut hi = m.n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match key.cmp(entry_key(m, off_at(m, mid))) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => hi = mid,
            std::cmp::Ordering::Greater => lo = mid + 1,
        }
    }
    false
}

pub fn get_b(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let mask: usize = (n * 2).next_power_of_two() - 1;
    let mut b = (hash_key(key.as_bytes()) as usize) & mask;
    loop {
        let slot = get_off(&m.payload, el + w * b, w);
        if slot == 0 {
            return false;
        }
        if entry_key(m, slot - 1) == key {
            return true;
        }
        b = (b + 1) & mask;
    }
}

pub fn get_c(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let fp = fingerprint(hash_key(key.as_bytes()));
    let at = el + w * (n - 1);
    let fps = &m.payload[at..at + n];

    let target = u64::from_ne_bytes([fp; 8]);
    let mut i = 0;
    while i + 8 <= n {
        let word = u64::from_ne_bytes(fps[i..i + 8].try_into().unwrap());
        let x = word ^ target;
        let mut mask = x.wrapping_sub(0x0101_0101_0101_0101) & !x & 0x8080_8080_8080_8080;
        while mask != 0 {
            let j = i + (mask.trailing_zeros() as usize) / 8;
            if entry_key(m, off_at(m, j)) == key {
                return true;
            }
            mask &= mask - 1;
        }
        i += 8;
    }
    while i < n {
        if fps[i] == fp && entry_key(m, off_at(m, i)) == key {
            return true;
        }
        i += 1;
    }
    false
}

pub fn get_i(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let fp = fingerprint16(hash_key(key.as_bytes()));
    let at = el + w * (n - 1);
    let fps = &m.payload[at..at + 2 * n];

    let target = u64::from(fp) * 0x0001_0001_0001_0001;
    let mut i = 0;
    // Four u16 lanes per word, via the 16-bit generalization of haszero.
    while i + 4 <= n {
        let word = u64::from_le_bytes(fps[2 * i..2 * i + 8].try_into().unwrap());
        let x = word ^ target;
        let mut mask =
            x.wrapping_sub(0x0001_0001_0001_0001) & !x & 0x8000_8000_8000_8000;
        while mask != 0 {
            let j = i + (mask.trailing_zeros() as usize) / 16;
            if entry_key(m, off_at(m, j)) == key {
                return true;
            }
            mask &= mask - 1;
        }
        i += 4;
    }
    while i < n {
        let h = u16::from_le_bytes(fps[2 * i..2 * i + 2].try_into().unwrap());
        if h == fp && entry_key(m, off_at(m, i)) == key {
            return true;
        }
        i += 1;
    }
    false
}

pub fn get_d(m: &Map, key: &str, _hint: usize) -> bool {
    if m.tries == 0 {
        return get_a(m, key, _hint);
    }
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let seed = u32::from_le_bytes(m.payload[el + w * n..el + w * n + 4].try_into().unwrap()) as u64;
    let slot = (hash_seeded(key.as_bytes(), seed) % (n as u64)) as usize;
    let off = get_off(&m.payload, el + w * slot, w);
    entry_key(m, off) == key
}

/// Entries are in (hash, key) order; the suffix carries a `u32` hash per entry
/// in the same order, so the search compares integers out of a contiguous array
/// and never reads a key until the hash matches.
pub fn get_e(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let target = crate::hash::hash32(key.as_bytes());
    let at = el + w * (n - 1);
    let hs = &m.payload[at..at + 4 * n];

    // Lower bound over the hash array.
    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let h = u32::from_le_bytes(hs[4 * mid..4 * mid + 4].try_into().unwrap());
        if h < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    // Walk the run of equal hashes, verifying keys.
    while lo < n {
        let h = u32::from_le_bytes(hs[4 * lo..4 * lo + 4].try_into().unwrap());
        if h != target {
            return false;
        }
        if entry_key(m, off_at(m, lo)) == key {
            return true;
        }
        lo += 1;
    }
    false
}

/// Key-sorted entries with a hash-sorted side index of `(h16, entry_index)`:
/// integer binary search, iteration order untouched.
pub fn get_f(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let target = fingerprint16(hash_key(key.as_bytes()));
    let iw = if n <= 255 { 1usize } else { 2 };
    let stride = 2 + iw;
    let at = el + w * (n - 1);
    let idx = &m.payload[at..at + stride * n];

    let read_h = |i: usize| u16::from_le_bytes(idx[stride * i..stride * i + 2].try_into().unwrap());
    let read_i = |i: usize| -> usize {
        if iw == 1 {
            idx[stride * i + 2] as usize
        } else {
            u16::from_le_bytes(idx[stride * i + 2..stride * i + 4].try_into().unwrap()) as usize
        }
    };

    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if read_h(mid) < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    while lo < n && read_h(lo) == target {
        if entry_key(m, off_at(m, read_i(lo))) == key {
            return true;
        }
        lo += 1;
    }
    false
}

/// Binary search over a 4-byte big-endian key-prefix array. The prefix order
/// agrees with key order, so a full key read happens only on a prefix tie.
pub fn get_g(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let target = key_prefix(key);
    let at = el + w * (n - 1);
    let pfx = &m.payload[at..at + 4 * n];

    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let p = u32::from_le_bytes(pfx[4 * mid..4 * mid + 4].try_into().unwrap());
        if p < target {
            lo = mid + 1;
        } else if p > target {
            hi = mid;
        } else {
            match key.cmp(entry_key(m, off_at(m, mid))) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
    }
    false
}

/// `G` with the prefix and offset interleaved: one probe, one cache line.
pub fn get_k(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let target = key_prefix(key);
    let stride = 4 + w;

    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let base = el + stride * mid;
        let p = u32::from_le_bytes(m.payload[base..base + 4].try_into().unwrap());
        if p < target {
            lo = mid + 1;
        } else if p > target {
            hi = mid;
        } else {
            let off = get_off(&m.payload, base + 4, w);
            match key.cmp(entry_key(m, off)) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
    }
    false
}

/// Sparse prefix index: binary search one prefix per group of 8, then scan the
/// group. Costs 0.5 bytes per key.
pub fn get_m(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let groups: usize = n.div_ceil(8);
    let target = key_prefix(key);
    let at = el + w * (n - 1);
    let pfx = &m.payload[at..at + 4 * groups];

    // Last group whose first key is <= `key`.
    let mut lo = 0usize;
    let mut hi = groups;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let p = u32::from_le_bytes(pfx[4 * mid..4 * mid + 4].try_into().unwrap());
        let less_or_eq = if p < target {
            true
        } else if p > target {
            false
        } else {
            entry_key(m, off_at(m, mid * 8)) <= key
        };
        if less_or_eq {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        return false;
    }
    let g = lo - 1;
    let start = g * 8;
    let end = (start + 8).min(n);
    let mut cursor = &m.payload[off_at(m, start)..el];
    for _ in start..end {
        let k = read_key(&mut cursor);
        if k == key {
            return true;
        }
        if k > key {
            return false;
        }
        cursor = skip_value(cursor);
    }
    false
}

/// The floor: the slot is already known from a per-shape dictionary, so the
/// per-row cost is one offset read plus one key verify.
/// `K` over an LCP-stripped prefix. Correctness does not need the probe to be
/// checked against the map's LCP: a `true` is only ever returned after a full
/// key compare, so a probe that does not share the LCP can steer the search
/// anywhere and still lands on a miss.
pub fn get_n(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let stride = 4 + w;
    let lcp = usize::from(m.payload[el + stride * n]);
    let target = key_prefix_from(key, lcp);

    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let base = el + stride * mid;
        let p = u32::from_le_bytes(m.payload[base..base + 4].try_into().unwrap());
        if p < target {
            lo = mid + 1;
        } else if p > target {
            hi = mid;
        } else {
            let off = get_off(&m.payload, base + 4, w);
            match key.cmp(entry_key(m, off)) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
    }
    false
}

pub fn get_p(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let stride = 2 + w;
    let lcp = usize::from(m.payload[el + stride * n]);
    let target = key_prefix16_from(key, lcp);

    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let base = el + stride * mid;
        let p = u16::from_le_bytes(m.payload[base..base + 2].try_into().unwrap());
        if p < target {
            lo = mid + 1;
        } else if p > target {
            hi = mid;
        } else {
            let off = get_off(&m.payload, base + 2, w);
            match key.cmp(entry_key(m, off)) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
    }
    false
}

pub fn get_t(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let stride = 1 + w;
    let lcp = usize::from(m.payload[el + stride * n]);
    let target = key_prefix8_from(key, lcp);

    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let base = el + stride * mid;
        let p = m.payload[base];
        if p < target {
            lo = mid + 1;
        } else if p > target {
            hi = mid;
        } else {
            let off = get_off(&m.payload, base + 1, w);
            match key.cmp(entry_key(m, off)) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
    }
    false
}

/// Interpolation search over `N`'s prefix array, falling back to bisection once
/// the range is small or the estimate stops making progress. The prefixes are
/// only weakly monotone, so ties still end in a full key compare.
pub fn get_q(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let stride = 4 + w;
    let lcp = usize::from(m.payload[el + stride * n]);
    let target = key_prefix_from(key, lcp);
    let pfx = |i: usize| -> u32 {
        let base = el + stride * i;
        u32::from_le_bytes(m.payload[base..base + 4].try_into().unwrap())
    };
    let verify = |i: usize| -> std::cmp::Ordering {
        let off = get_off(&m.payload, el + stride * i + 4, w);
        key.cmp(entry_key(m, off))
    };

    let mut lo = 0usize;
    let mut hi = n;
    // Interpolate while the window is wide; each step must shrink it.
    let mut steps = 0;
    while hi - lo > 8 && steps < 4 {
        let (plo, phi) = (pfx(lo), pfx(hi - 1));
        if target < plo || target > phi {
            break;
        }
        let span = u64::from(phi - plo);
        let mid = if span == 0 {
            lo + (hi - lo) / 2
        } else {
            let frac = u64::from(target - plo) * ((hi - lo - 1) as u64) / span;
            lo + frac as usize
        };
        let p = pfx(mid);
        if p < target {
            lo = mid + 1;
        } else if p > target {
            hi = mid;
        } else {
            match verify(mid) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
        steps += 1;
    }
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let p = pfx(mid);
        if p < target {
            lo = mid + 1;
        } else if p > target {
            hi = mid;
        } else {
            match verify(mid) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
    }
    false
}

/// Branchless rank over a contiguous LCP-stripped prefix array: the number of
/// prefixes strictly below the probe is the first candidate index. No
/// mispredicted branches, and the count vectorizes.
pub fn get_v(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let lcp = usize::from(m.payload[el + 4 * n + w * (n - 1)]);
    let target = key_prefix_from(key, lcp);
    let pfx = &m.payload[el..el + 4 * n];

    let mut rank = 0usize;
    for i in 0..n {
        let p = u32::from_le_bytes(pfx[4 * i..4 * i + 4].try_into().unwrap());
        rank += usize::from(p < target);
    }
    // Offsets live after the prefixes; entry 0 is implicit at 0.
    let off_at_v = |i: usize| -> usize {
        if i == 0 {
            0
        } else {
            get_off(&m.payload, el + 4 * n + w * (i - 1), w)
        }
    };
    let mut i = rank;
    while i < n {
        let p = u32::from_le_bytes(pfx[4 * i..4 * i + 4].try_into().unwrap());
        if p != target {
            return false;
        }
        if entry_key(m, off_at_v(i)) == key {
            return true;
        }
        i += 1;
    }
    false
}

/// `N` with the prefix width read from the payload.
pub fn get_x(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    // Trailer is [lcp: u8][prefix_width: u8]; the entries' stride follows from
    // the width, so read the width first from the end of the suffix.
    // Suffix length is n * (pw + w) + 2, so probe each candidate width.
    let total = m.payload.len() - 4 - el;
    let pw = if total == n * (1 + w) + 2 {
        1
    } else if total == n * (2 + w) + 2 {
        2
    } else {
        4
    };
    let stride = pw + w;
    let lcp = usize::from(m.payload[el + stride * n]);

    let read_p = |i: usize| -> u32 {
        let base = el + stride * i;
        match pw {
            1 => u32::from(m.payload[base]),
            2 => u32::from(u16::from_le_bytes(
                m.payload[base..base + 2].try_into().unwrap(),
            )),
            _ => u32::from_le_bytes(m.payload[base..base + 4].try_into().unwrap()),
        }
    };
    let target = match pw {
        1 => u32::from(key_prefix8_from(key, lcp)),
        2 => u32::from(key_prefix16_from(key, lcp)),
        _ => key_prefix_from(key, lcp),
    };

    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let p = read_p(mid);
        if p < target {
            lo = mid + 1;
        } else if p > target {
            hi = mid;
        } else {
            let off = get_off(&m.payload, el + stride * mid + pw, w);
            match key.cmp(entry_key(m, off)) {
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
    }
    false
}

/// Branchless bisection over `V`'s contiguous prefix array. The search phase
/// has no data-dependent branch: it always runs the same number of integer
/// compares and folds the outcome in arithmetically.
pub fn get_y(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let lcp = usize::from(m.payload[el + 4 * n + w * (n - 1)]);
    let target = key_prefix_from(key, lcp);
    let pfx = &m.payload[el..el + 4 * n];
    let read_p = |i: usize| u32::from_le_bytes(pfx[4 * i..4 * i + 4].try_into().unwrap());

    // partition_point: first index whose prefix is >= target.
    let mut lo = 0usize;
    let mut len = n;
    while len > 0 {
        let half = len / 2;
        let take = usize::from(read_p(lo + half) < target);
        lo += take * (half + 1);
        len = take * (len - half - 1) + (1 - take) * half;
    }

    let off_at_y = |i: usize| -> usize {
        if i == 0 {
            0
        } else {
            get_off(&m.payload, el + 4 * n + w * (i - 1), w)
        }
    };
    let mut i = lo;
    while i < n && read_p(i) == target {
        if entry_key(m, off_at_y(i)) == key {
            return true;
        }
        i += 1;
    }
    false
}

/// Branch-free SWAR scan over `X`'s adaptive-width discriminator.
///
/// The width rule separates adjacent keys at widths 1 and 2, but width 4 is a
/// fallback that can still collide, so a candidate whose full key does not match
/// must not end the scan. The SWAR mask can also report the lane above a match,
/// since the borrow out of a zeroed lane propagates, which is another reason
/// every candidate is verified rather than trusted.
pub fn get_z(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    // Trailer is [lcp: u8][prefix_width: u8]; recover the width from the
    // suffix length, as a reader would from the count word's spare bits.
    let total = m.payload.len() - 4 - el;
    let pw = if total == n + w * (n - 1) + 2 {
        1
    } else if total == 2 * n + w * (n - 1) + 2 {
        2
    } else {
        4
    };
    let lcp = usize::from(m.payload[el + pw * n + w * (n - 1)]);
    let pfx = &m.payload[el..el + pw * n];

    let off_at_z = |i: usize| -> usize {
        if i == 0 {
            0
        } else {
            get_off(&m.payload, el + pw * n + w * (i - 1), w)
        }
    };
    let verify = |i: usize| entry_key(m, off_at_z(i)) == key;

    match pw {
        1 => {
            let fp = key_prefix8_from(key, lcp);
            let target = u64::from(fp) * 0x0101_0101_0101_0101;
            let mut i = 0;
            while i + 8 <= n {
                let word = u64::from_le_bytes(pfx[i..i + 8].try_into().unwrap());
                let x = word ^ target;
                let mut mask =
                    x.wrapping_sub(0x0101_0101_0101_0101) & !x & 0x8080_8080_8080_8080;
                while mask != 0 {
                    let j = i + (mask.trailing_zeros() as usize) / 8;
                    if pfx[j] == fp && verify(j) {
                        return true;
                    }
                    mask &= mask - 1;
                }
                i += 8;
            }
            while i < n {
                if pfx[i] == fp && verify(i) {
                    return true;
                }
                i += 1;
            }
            false
        }
        2 => {
            let fp = key_prefix16_from(key, lcp);
            let target = u64::from(fp) * 0x0001_0001_0001_0001;
            let read = |i: usize| {
                u16::from_le_bytes(pfx[2 * i..2 * i + 2].try_into().unwrap())
            };
            let mut i = 0;
            while i + 4 <= n {
                let word = u64::from_le_bytes(pfx[2 * i..2 * i + 8].try_into().unwrap());
                let x = word ^ target;
                let mut mask =
                    x.wrapping_sub(0x0001_0001_0001_0001) & !x & 0x8000_8000_8000_8000;
                while mask != 0 {
                    let j = i + (mask.trailing_zeros() as usize) / 16;
                    if read(j) == fp && verify(j) {
                        return true;
                    }
                    mask &= mask - 1;
                }
                i += 4;
            }
            while i < n {
                if read(i) == fp && verify(i) {
                    return true;
                }
                i += 1;
            }
            false
        }
        _ => {
            let fp = key_prefix_from(key, lcp);
            let target = u64::from(fp) * 0x0000_0001_0000_0001;
            let read = |i: usize| {
                u32::from_le_bytes(pfx[4 * i..4 * i + 4].try_into().unwrap())
            };
            let mut i = 0;
            while i + 2 <= n {
                let word = u64::from_le_bytes(pfx[4 * i..4 * i + 8].try_into().unwrap());
                let x = word ^ target;
                let mut mask =
                    x.wrapping_sub(0x0000_0001_0000_0001) & !x & 0x8000_0000_8000_0000;
                while mask != 0 {
                    let j = i + (mask.trailing_zeros() as usize) / 32;
                    if read(j) == fp && verify(j) {
                        return true;
                    }
                    mask &= mask - 1;
                }
                i += 2;
            }
            while i < n {
                if read(i) == fp && verify(i) {
                    return true;
                }
                i += 1;
            }
            false
        }
    }
}

/// The crossover at which the scan stops beating the bisection. Measured, not
/// derived: below it the scan reads one or two words with no mispredicted
/// branch, above it the bisection's `log2 n` compares are fewer.
const SCAN_MAX: usize = 8;

/// `Z`'s bytes, searched by scan or bisection depending on `n`. The two agree
/// on every probe, so the choice is a read-time detail rather than part of the
/// encoding.
pub fn get_r(m: &Map, key: &str, hint: usize) -> bool {
    if m.n <= SCAN_MAX {
        return get_z(m, key, hint);
    }
    get_j(m, key, hint)
}

/// `Z`'s bytes searched by a branch-free bisection at every size. The search
/// phase has no data-dependent branch, so it costs the same `ceil(log2 n)`
/// integer compares whatever the probe is, and there is no crossover to tune.
///
/// The width rule leaves duplicate prefixes possible at width 4, so the run of
/// equal prefixes after the lower bound is walked rather than assumed to hold
/// one entry.
pub fn get_j(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, w, el) = (m.n, m.width, m.entries_len);
    let total = m.payload.len() - 4 - el;
    let pw = if total == n + w * (n - 1) + 2 {
        1
    } else if total == 2 * n + w * (n - 1) + 2 {
        2
    } else {
        4
    };
    let lcp = usize::from(m.payload[el + pw * n + w * (n - 1)]);
    let pfx = &m.payload[el..el + pw * n];
    let read_p = |i: usize| -> u32 {
        match pw {
            1 => u32::from(pfx[i]),
            2 => u32::from(u16::from_le_bytes(pfx[2 * i..2 * i + 2].try_into().unwrap())),
            _ => u32::from_le_bytes(pfx[4 * i..4 * i + 4].try_into().unwrap()),
        }
    };
    let target = match pw {
        1 => u32::from(key_prefix8_from(key, lcp)),
        2 => u32::from(key_prefix16_from(key, lcp)),
        _ => key_prefix_from(key, lcp),
    };

    let mut lo = 0usize;
    let mut len = n;
    while len > 0 {
        let half = len / 2;
        let take = usize::from(read_p(lo + half) < target);
        lo += take * (half + 1);
        len = take * (len - half - 1) + (1 - take) * half;
    }
    while lo < n && read_p(lo) == target {
        let off = if lo == 0 {
            0
        } else {
            get_off(&m.payload, el + pw * n + w * (lo - 1), w)
        };
        if entry_key(m, off) == key {
            return true;
        }
        lo += 1;
    }
    false
}

/// Hash-ordered entries with a key-order permutation: the lookup is `E`'s.
pub fn get_h(m: &Map, key: &str, hint: usize) -> bool {
    get_e(m, key, hint)
}

/// Recovers the split layout's geometry: the key region length and the two
/// offset widths.
#[inline(always)]
fn split_geom(m: &Map) -> (usize, usize, usize) {
    let len = m.payload.len();
    let keys_len =
        u32::from_le_bytes(m.payload[len - 8..len - 4].try_into().unwrap()) as usize;
    let wk = offset_width(keys_len);
    let wv = offset_width(m.entries_len - keys_len);
    (keys_len, wk, wv)
}

pub fn get_o(m: &Map, key: &str, _hint: usize) -> bool {
    let (n, el) = (m.n, m.entries_len);
    let (keys_len, wk, _wv) = split_geom(m);
    let koff = el;

    let mut lo = 0;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = if mid == 0 {
            0
        } else {
            get_off(&m.payload, koff + wk * (mid - 1), wk)
        };
        let mut cursor = &m.payload[off..keys_len];
        match key.cmp(read_key(&mut cursor)) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => hi = mid,
            std::cmp::Ordering::Greater => lo = mid + 1,
        }
    }
    false
}

pub fn get_u(m: &Map, key: &str, _hint: usize) -> bool {
    let (keys_len, _wk, _wv) = split_geom(m);
    let mut cursor = &m.payload[..keys_len];
    while !cursor.is_empty() {
        if read_key(&mut cursor) == key {
            return true;
        }
    }
    false
}

/// The floor. `hint` is the entry's slot, resolved once per map *shape* by a
/// dictionary the caller keeps, not once per row; `usize::MAX` means the shape
/// dictionary already knows the key is absent.
pub fn get_s(m: &Map, key: &str, hint: usize) -> bool {
    if hint == usize::MAX {
        return false;
    }
    entry_key(m, off_at(m, hint)) == key
}

/// Walks every entry in *storage* order, imposing no ordering requirement.
/// This is what a consumer that only needs the entries, not a particular order,
/// pays. Split layouts walk two cursors.
pub fn scan_storage(m: &Map, scan: Scan) -> usize {
    let mut acc = 0usize;
    if scan == Scan::Split {
        let (keys_len, _wk, _wv) = split_geom(m);
        let mut kc = &m.payload[..keys_len];
        let mut vc = &m.payload[keys_len..m.entries_len];
        while !kc.is_empty() {
            acc += read_key(&mut kc).len();
            vc = skip_value(vc);
        }
        std::hint::black_box(vc.len());
    } else {
        let mut cursor = &m.payload[..m.entries_len];
        while !cursor.is_empty() {
            acc += read_key(&mut cursor).len();
            cursor = skip_value(cursor);
        }
    }
    acc
}

/// Accumulates key lengths in ascending key order, the way every encode path
/// walks a map. `Scan::Permuted` layouts pay an indirection per entry.
pub fn scan_keyorder<'a>(m: &'a Map, scan: Scan, sortbuf: &mut Vec<&'a str>) -> usize {
    let mut acc = 0usize;
    match scan {
        Scan::Sequential => {
            let mut cursor = &m.payload[..m.entries_len];
            while !cursor.is_empty() {
                acc += read_key(&mut cursor).len();
                cursor = skip_value(cursor);
            }
        }
        Scan::SortAfterScan => {
            sortbuf.clear();
            let mut cursor = &m.payload[..m.entries_len];
            while !cursor.is_empty() {
                sortbuf.push(read_key(&mut cursor));
                cursor = skip_value(cursor);
            }
            sortbuf.sort_unstable();
            for k in sortbuf.iter() {
                acc += k.len();
            }
        }
        Scan::Permuted => {
            let (n, w, el) = (m.n, m.width, m.entries_len);
            let iw = if n <= 255 { 1usize } else { 2 };
            let at = el + w * (n - 1) + 4 * n;
            for j in 0..n {
                let pos = if iw == 1 {
                    usize::from(m.payload[at + j])
                } else {
                    usize::from(u16::from_le_bytes(
                        m.payload[at + 2 * j..at + 2 * j + 2].try_into().unwrap(),
                    ))
                };
                acc += entry_key(m, off_at(m, pos)).len();
            }
        }
        Scan::Split => {
            let (keys_len, _wk, _wv) = split_geom(m);
            let mut kc = &m.payload[..keys_len];
            let mut vc = &m.payload[keys_len..m.entries_len];
            while !kc.is_empty() {
                acc += read_key(&mut kc).len();
                vc = skip_value(vc);
            }
            std::hint::black_box(vc.len());
        }
    }
    acc
}

/// Collects the keys in ascending key order, minus `drop_key`.
pub fn collect_keyorder<'a>(
    m: &'a Map,
    scan: Scan,
    drop_key: &str,
    out: &mut Vec<&'a str>,
) {
    out.clear();
    match scan {
        Scan::Sequential => {
            let mut cursor = &m.payload[..m.entries_len];
            while !cursor.is_empty() {
                let k = read_key(&mut cursor);
                if k != drop_key {
                    out.push(k);
                }
                cursor = skip_value(cursor);
            }
        }
        Scan::SortAfterScan => {
            let mut cursor = &m.payload[..m.entries_len];
            while !cursor.is_empty() {
                let k = read_key(&mut cursor);
                if k != drop_key {
                    out.push(k);
                }
                cursor = skip_value(cursor);
            }
            out.sort_unstable();
        }
        Scan::Permuted => {
            let (n, w, el) = (m.n, m.width, m.entries_len);
            let iw = if n <= 255 { 1usize } else { 2 };
            let at = el + w * (n - 1) + 4 * n;
            for j in 0..n {
                let pos = if iw == 1 {
                    usize::from(m.payload[at + j])
                } else {
                    usize::from(u16::from_le_bytes(
                        m.payload[at + 2 * j..at + 2 * j + 2].try_into().unwrap(),
                    ))
                };
                let k = entry_key(m, off_at(m, pos));
                if k != drop_key {
                    out.push(k);
                }
            }
        }
        Scan::Split => {
            let (keys_len, _wk, _wv) = split_geom(m);
            let mut kc = &m.payload[..keys_len];
            while !kc.is_empty() {
                let k = read_key(&mut kc);
                if k != drop_key {
                    out.push(k);
                }
            }
        }
    }
}

pub type Get = fn(&Map, &str, usize) -> bool;

pub fn getter(layout: Layout) -> Get {
    match layout {
        L => get_l,
        A => get_a,
        B => get_b,
        C => get_c,
        D => get_d,
        E => get_e,
        F => get_f,
        G => get_g,
        I => get_i,
        K => get_k,
        M => get_m,
        N => get_n,
        P => get_p,
        T => get_t,
        Q => get_q,
        V => get_v,
        H => get_h,
        X => get_x,
        Y => get_y,
        Z => get_z,
        R => get_r,
        J => get_j,
        O => get_o,
        U => get_u,
        S => get_s,
    }
}

}

use std::time::Instant;

use get::{Get, collect_keyorder, getter, scan_keyorder, scan_storage};
use layout::*;

const ROUNDS: usize = 7;

fn timed(units: usize, mut f: impl FnMut()) -> f64 {
    let rounds = if std::env::var("MAPBENCH_CHECK").is_ok() { 1 } else { ROUNDS };
    let mut best = f64::MAX;
    for _ in 0..rounds {
        let t = Instant::now();
        f();
        let per = t.elapsed().as_nanos() as f64 / units as f64;
        if per < best {
            best = per;
        }
    }
    best
}

/// Deterministic pseudo-random key names with distinct prefixes.
fn distinct_keys(n: usize) -> Vec<String> {
    const SYL: [&str; 16] = [
        "ax", "be", "cor", "dyn", "ep", "fu", "gra", "hy", "id", "jo", "ka", "lum", "mi", "nu",
        "ob", "pra",
    ];
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut out = Vec::with_capacity(n);
    let mut seen = std::collections::BTreeSet::new();
    while out.len() < n {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut s = String::new();
        for j in 0..3 {
            s.push_str(SYL[((state >> (8 * j)) & 0xf) as usize]);
        }
        s.push_str(&format!("{}", (state >> 40) % 100));
        if seen.insert(s.clone()) {
            out.push(s);
        }
    }
    out
}

fn keys_for(n: usize, style: &str) -> Vec<String> {
    let mut v: Vec<String> = match style {
        "short" => (0..n).map(|i| format!("k{i:03}")).collect(),
        "typical" => (0..n).map(|i| format!("field_name_{i:03}")).collect(),
        "prefixed" => (0..n)
            .map(|i| format!("com.example.service.metrics.dimension_{i:03}"))
            .collect(),
        "distinct" => distinct_keys(n),
        _ => unreachable!(),
    };
    v.sort();
    v
}

struct Cell {
    vals: Vec<f64>,
    viable: Vec<bool>,
}

fn print_table(title: &str, unit: &str, ns: &[usize], rows: &[Cell]) {
    println!("  {title}  [{unit}]");
    print!("  {:>5}", "n");
    for l in ALL {
        print!(" {:>8}", l.name());
    }
    println!();
    for (r, &n) in rows.iter().zip(ns) {
        print!("  {n:>5}");
        for (i, _) in ALL.iter().enumerate() {
            if r.viable[i] {
                print!(" {:>8.1}", r.vals[i]);
            } else {
                print!(" {:>8}", "n/a");
            }
        }
        println!();
    }
    println!();
}

fn print_bytes_table(ns: &[usize], rows: &[Vec<Option<usize>>]) {
    println!("  index bytes (payload minus entries)  [bytes, and bytes/entry]");
    print!("  {:>5}", "n");
    for l in ALL {
        print!(" {:>10}", l.name());
    }
    println!();
    for (r, &n) in rows.iter().zip(ns) {
        print!("  {n:>5}");
        for v in r {
            match v {
                Some(b) => print!(" {:>4} {:>5.2}", b, *b as f64 / n as f64),
                None => print!(" {:>10}", "n/a"),
            }
        }
        println!();
    }
    println!();
}

struct Summary {
    /// Geometric-mean get time relative to A, over n <= 32.
    small: Vec<f64>,
    /// Same, over n >= 100.
    large: Vec<f64>,
    /// Mean index bytes per entry.
    bpe: Vec<f64>,
    /// Pack time relative to A, over n <= 32.
    pack_small: Vec<f64>,
    count_small: usize,
    count_large: usize,
}

fn main() {
    let target: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(16 << 20);
    let smoke = std::env::var("MAPBENCH_SMOKE").is_ok();
    // Correctness sweep: every size and key style, one round, tiny corpus. The
    // cross-check between layouts is the point, not the timings.
    let check = std::env::var("MAPBENCH_CHECK").is_ok();
    let ns: Vec<usize> = if smoke && !check {
        vec![3, 8, 16, 50, 500]
    } else {
        vec![3, 5, 8, 12, 16, 24, 32, 50, 100, 250, 500]
    };
    let styles: &[&str] = if smoke && !check {
        &["typical", "distinct"]
    } else {
        &["short", "typical", "prefixed", "distinct"]
    };
    println!(
        "DatumMap index layout probe. corpus ~{} MiB per layout, best-of-{ROUNDS}, \
         D seed cap {D_MAX_TRIES}",
        target >> 20
    );
    println!("layouts: {}\n", ALL.map(|l| l.name()).join("  "));

    let mut sum = Summary {
        small: vec![1.0; ALL.len()],
        large: vec![1.0; ALL.len()],
        bpe: vec![0.0; ALL.len()],
        pack_small: vec![1.0; ALL.len()],
        count_small: 0,
        count_large: 0,
    };
    let mut bpe_count = 0usize;

    for &style in styles {
        for value_len in [0usize, 60] {
            let vdesc = if value_len == 0 {
                "int64 values".to_string()
            } else {
                format!("{value_len}B string values")
            };
            println!("=============== keys={style}, {vdesc} ===============\n");

            let mut t_pack = Vec::new();
            let mut t_get = Vec::new();
            let mut t_iter = Vec::new();
            let mut t_encode = Vec::new();
            let mut t_rebuild = Vec::new();
            let mut t_miss = Vec::new();
            let mut bytes_rows = Vec::new();

            for &n in &ns {
                let owned = keys_for(n, style);
                let keys: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();

                // A "JSON to columns" projection: four present keys spread over
                // the map, plus one miss.
                let probe_idx = [0usize, n / 3, n * 2 / 3, n - 1];
                let mut probes: Vec<(&str, usize)> =
                    probe_idx.iter().map(|&i| (keys[i], i)).collect();
                probes.push(("not_a_present_key_at_all", usize::MAX));

                let misses: Vec<(&str, usize)> = vec![
                    ("!before_everything", usize::MAX),
                    ("zzz_after_everything", usize::MAX),
                    ("field_name_XXX", usize::MAX),
                    ("com.example.service.metrics.dimension_XXX", usize::MAX),
                    ("k999", usize::MAX),
                ];

                let mut buf = Vec::with_capacity(1 << 17);
                let mut scratch = Scratch::new();
                let protos: Vec<Map> = ALL
                    .iter()
                    .map(|&l| {
                        let (entries_len, width, tries) =
                            pack(l, &keys, value_len, &mut buf, &mut scratch);
                        Map {
                            payload: buf.clone(),
                            entries_len,
                            n,
                            width,
                            tries,

                        }
                    })
                    .collect();
                let base_len = protos[0].payload.len();
                let rows = (target / protos[1].payload.len().max(1)).clamp(64, 200_000);

                let mut pack_c = Cell { vals: vec![0.0; ALL.len()], viable: vec![true; ALL.len()] };
                let mut get_c = Cell { vals: vec![0.0; ALL.len()], viable: vec![true; ALL.len()] };
                let mut iter_c = Cell { vals: vec![0.0; ALL.len()], viable: vec![true; ALL.len()] };
                let mut enc_c = Cell { vals: vec![0.0; ALL.len()], viable: vec![true; ALL.len()] };
                let mut reb_c = Cell { vals: vec![0.0; ALL.len()], viable: vec![true; ALL.len()] };
                let mut miss_c = Cell { vals: vec![0.0; ALL.len()], viable: vec![true; ALL.len()] };
                let mut byte_row: Vec<Option<usize>> = Vec::new();
                let mut hit_counts: Vec<usize> = Vec::new();

                for (li, &lay) in ALL.iter().enumerate() {
                    let viable = lay != D || protos[li].tries > 0;
                    for c in [
                        &mut pack_c,
                        &mut get_c,
                        &mut iter_c,
                        &mut enc_c,
                        &mut reb_c,
                        &mut miss_c,
                    ] {
                        c.viable[li] = viable;
                    }
                    byte_row.push(if viable {
                        Some(protos[li].payload.len() - base_len)
                    } else {
                        None
                    });
                    if !viable {
                        hit_counts.push(usize::MAX);
                        continue;
                    }

                    let corpus: Vec<Map> = (0..rows).map(|_| protos[li].clone()).collect();
                    let f: Get = getter(lay);

                    // pack. D's seed search costs O(tries * n) hashes per map,
                    // orders of magnitude above the others, so scale its
                    // iteration count down rather than let it dominate.
                    let packs = if check {
                        5
                    } else if smoke {
                        200
                    } else if lay == D {
                        (50_000 / (protos[li].tries as usize + 1)).clamp(20, 5_000)
                    } else {
                        5_000
                    };
                    let mut pb = Vec::with_capacity(1 << 17);
                    let mut ps = Scratch::new();
                    pack_c.vals[li] = timed(packs, || {
                        for _ in 0..packs {
                            let r = pack(lay, &keys, value_len, &mut pb, &mut ps);
                            std::hint::black_box(r);
                        }
                    });

                    // get
                    let mut hits = 0usize;
                    let units = corpus.len() * probes.len();
                    get_c.vals[li] = timed(units, || {
                        for m in &corpus {
                            for &(p, slot) in &probes {
                                if std::hint::black_box(f(std::hint::black_box(m), p, slot)) {
                                    hits += 1;
                                }
                            }
                        }
                    });
                    hit_counts.push(hits);

                    // miss: probes that are all absent
                    let mut mhits = 0usize;
                    let munits = corpus.len() * misses.len();
                    miss_c.vals[li] = timed(munits, || {
                        for m in &corpus {
                            for &(p, slot) in &misses {
                                if std::hint::black_box(f(std::hint::black_box(m), p, slot)) {
                                    mhits += 1;
                                }
                            }
                        }
                    });
                    assert_eq!(mhits, 0, "layout {} found an absent key", lay.name());

                    // iter: walk every entry, storage order
                    let mut acc = 0usize;
                    let scan0 = lay.scan();
                    iter_c.vals[li] = timed(corpus.len(), || {
                        for m in &corpus {
                            acc += scan_storage(m, scan0);
                        }
                    });
                    std::hint::black_box(acc);

                    // encode: emit entries in key order
                    let mut eacc = 0usize;
                    let scan = lay.scan();
                    let mut sortbuf: Vec<&str> = Vec::with_capacity(n);
                    enc_c.vals[li] = timed(corpus.len(), || {
                        for m in &corpus {
                            eacc += scan_keyorder(m, scan, &mut sortbuf);
                        }
                    });
                    std::hint::black_box(eacc);

                    // rebuild: scan + repack minus one key
                    let drop_key = keys[n / 2];
                    let rebuilds = if check { 5 } else if smoke { 200 } else { 5_000 }.min(corpus.len());
                    let mut rb = Vec::with_capacity(1 << 17);
                    let mut rs = Scratch::new();
                    let mut kept: Vec<&str> = Vec::with_capacity(n);
                    reb_c.vals[li] = timed(rebuilds, || {
                        for m in corpus.iter().take(rebuilds) {
                            collect_keyorder(m, scan, drop_key, &mut kept);
                            let r = pack(lay, &kept, value_len, &mut rb, &mut rs);
                            std::hint::black_box(r);
                        }
                    });
                }

                // Every viable layout must resolve the same set of probes.
                let reference = hit_counts
                    .iter()
                    .copied()
                    .find(|&h| h != usize::MAX)
                    .unwrap();
                for (i, &h) in hit_counts.iter().enumerate() {
                    if h != usize::MAX {
                        assert_eq!(
                            h, reference,
                            "layout {} disagrees on lookups at n={n}, style={style}",
                            ALL[i].name()
                        );
                    }
                }

                // Accumulate the decision summary.
                let a_get = get_c.vals[1];
                let a_pack = pack_c.vals[1];
                if n <= 32 {
                    sum.count_small += 1;
                    for i in 0..ALL.len() {
                        if get_c.viable[i] {
                            sum.small[i] *= get_c.vals[i] / a_get;
                            sum.pack_small[i] *= pack_c.vals[i] / a_pack;
                        }
                    }
                } else if n >= 100 {
                    sum.count_large += 1;
                    for i in 0..ALL.len() {
                        if get_c.viable[i] {
                            sum.large[i] *= get_c.vals[i] / a_get;
                        }
                    }
                }
                bpe_count += 1;
                for i in 0..ALL.len() {
                    if let Some(b) = byte_row[i] {
                        sum.bpe[i] += b as f64 / n as f64;
                    }
                }

                t_pack.push(pack_c);
                t_get.push(get_c);
                t_iter.push(iter_c);
                t_encode.push(enc_c);
                t_rebuild.push(reb_c);
                t_miss.push(miss_c);
                bytes_rows.push(byte_row);
            }

            print_table("get: single-key lookup", "ns/lookup", &ns, &t_get);
            print_table("miss: lookup of an absent key", "ns/lookup", &ns, &t_miss);
            print_table("pack: build payload from key-sorted entries", "ns/map", &ns, &t_pack);
            print_table("encode: emit entries in key order", "ns/map", &ns, &t_encode);
            print_table("rebuild: scan + repack minus one key", "ns/map", &ns, &t_rebuild);
            print_table("iter: raw sequential scan", "ns/map", &ns, &t_iter);
            print_bytes_table(&ns, &bytes_rows);
        }
    }

    println!("=============== summary ===============\n");
    println!(
        "  {:>8} {:>14} {:>14} {:>12} {:>14}",
        "layout", "get n<=32 /A", "get n>=100 /A", "bytes/entry", "pack n<=32 /A"
    );
    for (i, l) in ALL.iter().enumerate() {
        let g1 = sum.small[i].powf(1.0 / sum.count_small as f64);
        let g2 = sum.large[i].powf(1.0 / sum.count_large as f64);
        let p1 = sum.pack_small[i].powf(1.0 / sum.count_small as f64);
        println!(
            "  {:>8} {:>14.2} {:>14.2} {:>12.2} {:>14.2}",
            l.name(),
            g1,
            g2,
            sum.bpe[i] / bpe_count as f64,
            p1
        );
    }
    println!("\n  (geometric means; D's columns cover only the n where a seed was found)");
}
