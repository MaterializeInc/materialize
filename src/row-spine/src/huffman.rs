// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Canonical Huffman coding over a byte alphabet, seeded from batch-wide
//! frequencies.
//!
//! This is the entropy layer for row-spine batches: a `HuffmanCode` is built
//! once per batch (per column) from the frequencies of the bytes it will
//! compress, and the same code encodes every row's bytes for that column.
//!
//! A code is described entirely by its per-symbol bit lengths; the canonical
//! codes (and the decode tables) are derived from the lengths alone, so storing
//! a code costs only the 256 lengths.
//!
//! Decoding is forward-streaming: codes are read MSB-first and symbols emitted
//! one at a time with O(1) state, so a comparison can decode its operands
//! without materializing them into a buffer.

use std::sync::OnceLock;

use mz_ore::cast::CastFrom;

/// Maximum code length we will emit. Optimal Huffman codes over 256 symbols can
/// in pathological (Fibonacci-like) frequency distributions reach lengths up to
/// 255, which neither fits a `u32` code word nor is worth the decode cost. When
/// the optimal code exceeds this bound we decline to build a code (the caller
/// leaves the data uncompressed); real column data stays far below it.
const MAX_BITS: usize = 24;

/// Index width of the single-level decode table: it maps the next `DECODE_TABLE_BITS` bits to
/// `(length << 8) | symbol` for codes that fit in that many bits. Longer codes are left absent
/// and fall back to the canonical decode. 8 keeps the table at a cache-friendly 256 entries
/// (512 B): a Huffman code's *frequent* symbols have short codes that land in it, and only the
/// rare, long-coded symbols fall back. (Measured a touch faster than a 12-bit table on real
/// text, and 16x smaller, since the peek is already cheap with the buffered reader.)
const DECODE_TABLE_BITS: u8 = 8;

/// A canonical Huffman code over the 256 byte values.
#[derive(Debug)]
pub struct HuffmanCode {
    /// Code length in bits per byte; `0` means the byte does not appear in the
    /// model and must never be encoded with this code.
    lengths: [u8; 256],
    /// Canonical code per byte, right-aligned in the low `lengths[b]` bits,
    /// MSB-first. Only meaningful where `lengths[b] > 0`.
    codes: [u32; 256],
    /// `count[l]` is the number of symbols whose code length is `l`.
    count: [u16; MAX_BITS + 1],
    /// Symbols ordered by `(length, value)` — the canonical decode order.
    sorted: Vec<u8>,
    max_len: u8,
    /// Lazily-built decode-acceleration table (see [`DECODE_TABLE_BITS`]). Derived from
    /// `lengths`/`codes` on first decode and excluded from the code's size estimate, so
    /// building it does not penalize codec selection; reset (rebuilt lazily) on clone.
    decode_table: OnceLock<Box<[u16]>>,
}

impl Clone for HuffmanCode {
    fn clone(&self) -> Self {
        HuffmanCode {
            lengths: self.lengths,
            codes: self.codes,
            count: self.count,
            sorted: self.sorted.clone(),
            max_len: self.max_len,
            decode_table: OnceLock::new(),
        }
    }
}

impl HuffmanCode {
    /// Build a code from per-byte frequencies.
    ///
    /// Returns `None` only when no byte appears (empty input) or when the optimal code would
    /// exceed [`MAX_BITS`]. A single distinct byte still yields a code (1 bit per byte), so the
    /// stream has positive length per symbol; callers that find that unprofitable compare its
    /// estimated cost against the alternatives rather than relying on `None` here.
    pub fn from_frequencies(freq: &[u64; 256]) -> Option<HuffmanCode> {
        let lengths = code_lengths(freq)?;
        Some(Self::from_lengths(lengths))
    }

    /// Build a code from its per-byte lengths (e.g. when reloading a stored
    /// model). Lengths must form a valid prefix code; `0` marks absent symbols.
    pub fn from_lengths(lengths: [u8; 256]) -> HuffmanCode {
        let mut count = [0u16; MAX_BITS + 1];
        let mut max_len = 0u8;
        for &l in &lengths {
            if l > 0 {
                count[usize::from(l)] += 1;
                max_len = max_len.max(l);
            }
        }

        // Symbols in canonical order: ascending length, then ascending value.
        let mut sorted = Vec::new();
        for l in 1..=usize::from(max_len) {
            for b in 0..256 {
                if usize::from(lengths[b]) == l {
                    sorted.push(u8::try_from(b).expect("b < 256"));
                }
            }
        }

        // Assign canonical codes. `next[l]` walks the codes of length `l`,
        // starting from the first canonical code of that length.
        let mut next = [0u32; MAX_BITS + 1];
        let mut code = 0u32;
        for l in 1..=usize::from(max_len) {
            code = (code + u32::from(count[l - 1])) << 1;
            next[l] = code;
        }
        let mut codes = [0u32; 256];
        for &b in &sorted {
            let l = usize::from(lengths[usize::from(b)]);
            codes[usize::from(b)] = next[l];
            next[l] += 1;
        }

        HuffmanCode {
            lengths,
            codes,
            count,
            sorted,
            max_len,
            decode_table: OnceLock::new(),
        }
    }

    /// The per-byte code lengths, sufficient to reconstruct this code.
    pub fn lengths(&self) -> &[u8; 256] {
        &self.lengths
    }

    /// Visit the heap allocations backing this code (the fixed-size arrays live
    /// inline and are not reported).
    pub fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
        callback(self.sorted.len(), self.sorted.capacity());
        if let Some(table) = self.decode_table.get() {
            let bytes = std::mem::size_of_val(&table[..]);
            callback(bytes, bytes);
        }
    }

    /// Encode `bytes`, appending the bits to `out`. Every byte must be present
    /// in the model (`lengths[b] > 0`); this holds when the model was built from
    /// a superset of `bytes`, as it is for batch-wide column models.
    pub fn encode(&self, bytes: &[u8], out: &mut BitWriter) {
        for &b in bytes {
            let len = self.lengths[usize::from(b)];
            debug_assert!(len > 0, "byte {b} absent from Huffman model");
            out.write(self.codes[usize::from(b)], len);
        }
    }

    /// Decode `n` bytes from `reader`, appending them to `out`.
    pub fn decode_into(&self, reader: &mut BitReader<'_>, n: usize, out: &mut Vec<u8>) {
        out.reserve(n);
        for _ in 0..n {
            out.push(self.decode_one(reader));
        }
    }

    /// Decode a single symbol from `reader`.
    ///
    /// Fast path: peek `DECODE_TABLE_BITS` bits and look the symbol up directly — short
    /// (frequent) codes resolve in O(1). Codes too long for the table fall back to the
    /// canonical decode.
    #[inline]
    pub fn decode_one(&self, reader: &mut BitReader<'_>) -> u8 {
        let table = self.decode_table.get_or_init(|| self.build_decode_table());
        let entry = table[usize::cast_from(reader.peek(DECODE_TABLE_BITS))];
        let len = u8::try_from(entry >> 8).expect("length byte");
        if len > 0 {
            reader.advance(len);
            u8::try_from(entry & 0xFF).expect("symbol byte")
        } else {
            self.decode_one_canonical(reader)
        }
    }

    /// Canonical decode: read bits MSB-first until the accumulated code falls within the range
    /// of codes of the current length. Used for codes too long for the decode table.
    fn decode_one_canonical(&self, reader: &mut BitReader<'_>) -> u8 {
        let mut code = 0u32;
        let mut first = 0u32;
        let mut index = 0u32;
        for len in 1..=usize::from(self.max_len) {
            code = (code << 1) | reader.read_bit();
            let count = u32::from(self.count[len]);
            if code.wrapping_sub(first) < count {
                return self.sorted[usize::cast_from(index + (code - first))];
            }
            index += count;
            first = (first + count) << 1;
        }
        // A well-formed bitstream produced by `encode` always resolves within
        // `max_len` bits; reaching here means the input was not produced by this
        // code.
        panic!("invalid Huffman bitstream")
    }

    /// Build the single-level decode table: for every symbol whose code fits in
    /// `DECODE_TABLE_BITS`, fill all index entries whose top bits are that code with
    /// `(length << 8) | symbol`. Entries for longer codes stay `0` (length 0 = "fall back").
    fn build_decode_table(&self) -> Box<[u16]> {
        let mut table = vec![0u16; 1usize << DECODE_TABLE_BITS];
        for &b in &self.sorted {
            let len = self.lengths[usize::from(b)];
            if len == 0 || len > DECODE_TABLE_BITS {
                continue;
            }
            // The code occupies the top `len` bits of the index; every index with those top
            // bits decodes to this symbol.
            let shift = DECODE_TABLE_BITS - len;
            let start = usize::cast_from(self.codes[usize::from(b)]) << shift;
            let end = start + (1usize << shift);
            let entry = (u16::from(len) << 8) | u16::from(b);
            for slot in &mut table[start..end] {
                *slot = entry;
            }
        }
        table.into_boxed_slice()
    }
}

/// Accumulate per-byte frequencies to seed a [`HuffmanCode`].
#[derive(Clone, Debug)]
pub struct FrequencyCounter {
    freq: [u64; 256],
}

impl Default for FrequencyCounter {
    fn default() -> Self {
        FrequencyCounter { freq: [0; 256] }
    }
}

impl FrequencyCounter {
    pub fn observe(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.freq[usize::from(b)] += 1;
        }
    }

    /// Add another counter's per-byte counts into this one. Merging two batches'
    /// counters this way yields a counter for their union, from which a fresh
    /// Huffman code valid for the merged data can be built.
    pub fn add(&mut self, other: &FrequencyCounter) {
        for b in 0..256 {
            self.freq[b] += other.freq[b];
        }
    }

    pub fn frequencies(&self) -> &[u64; 256] {
        &self.freq
    }

    pub fn build(&self) -> Option<HuffmanCode> {
        HuffmanCode::from_frequencies(&self.freq)
    }

    /// Build a code that can encode *any* byte value, by treating every unseen
    /// byte as if it occurred once. Used for mid-formation install, where the
    /// codec is fixed before all rows are seen: a code built only over observed
    /// bytes could not encode a byte that arrives later. Flooring lengthens the
    /// observed bytes' codes slightly (256 symbols instead of the seen subset),
    /// the price of soundness against unseen bytes. `None` only if the floored
    /// distribution still needs codes longer than [`MAX_BITS`] (degenerate).
    pub fn build_floored(&self) -> Option<HuffmanCode> {
        let mut freq = self.freq;
        for slot in freq.iter_mut() {
            *slot = (*slot).max(1);
        }
        HuffmanCode::from_frequencies(&freq)
    }
}

/// Compute optimal Huffman code lengths for the given frequencies, or `None` if
/// fewer than two symbols are present or the optimal lengths exceed [`MAX_BITS`].
fn code_lengths(freq: &[u64; 256]) -> Option<[u8; 256]> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let present: Vec<usize> = (0..256).filter(|&b| freq[b] > 0).collect();
    let mut lengths = [0u8; 256];
    match present.len() {
        0 => return None,
        1 => {
            // A single symbol still needs one bit so the stream has positive
            // length per symbol.
            lengths[present[0]] = 1;
            return Some(lengths);
        }
        _ => {}
    }

    // Each node tracks its weight and parent; leaves come first, internal nodes
    // are appended as the tree is built bottom-up. `seq` breaks weight ties for
    // deterministic construction.
    // `NONE` marks a node with no parent (the root); no real node index reaches
    // `usize::MAX`. Using a sentinel rather than `Option`/`i32` keeps the parent
    // walk free of conversions.
    const NONE: usize = usize::MAX;
    let mut weight: Vec<u64> = present.iter().map(|&b| freq[b]).collect();
    let mut parent: Vec<usize> = vec![NONE; present.len()];
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = present
        .iter()
        .enumerate()
        .map(|(i, &b)| Reverse((freq[b], i)))
        .collect();

    while heap.len() > 1 {
        let Reverse((wa, a)) = heap.pop().unwrap();
        let Reverse((wb, b)) = heap.pop().unwrap();
        let node = weight.len();
        weight.push(wa + wb);
        parent.push(NONE);
        parent[a] = node;
        parent[b] = node;
        heap.push(Reverse((wa + wb, node)));
    }

    // Code length of each leaf is its depth: walk parent pointers to the root.
    for (i, &b) in present.iter().enumerate() {
        let mut depth = 0u32;
        let mut node = i;
        while parent[node] != NONE {
            depth += 1;
            node = parent[node];
        }
        if usize::cast_from(depth) > MAX_BITS {
            return None;
        }
        lengths[b] = u8::try_from(depth).expect("depth <= MAX_BITS, fits u8");
    }
    Some(lengths)
}

/// Writes bits MSB-first into a growable byte buffer.
#[derive(Debug, Default)]
pub struct BitWriter {
    out: Vec<u8>,
    /// Pending bits, left-aligned within the low `nbits` bits of `acc`.
    acc: u64,
    nbits: u32,
}

impl BitWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append the low `len` bits of `code` (MSB-first).
    #[inline]
    pub fn write(&mut self, code: u32, len: u8) {
        let len = u32::from(len);
        self.acc = (self.acc << len) | u64::from(code);
        self.nbits += len;
        while self.nbits >= 8 {
            self.nbits -= 8;
            // Mask to the byte we are flushing; `acc` keeps already-flushed bits
            // in higher positions, which we discard here.
            let byte = (self.acc >> self.nbits) & 0xFF;
            self.out
                .push(u8::try_from(byte).expect("masked to one byte"));
        }
    }

    /// Flush any partial final byte (zero-padded) and return the bytes.
    pub fn finish(mut self) -> Vec<u8> {
        if self.nbits > 0 {
            let byte = (self.acc << (8 - self.nbits)) & 0xFF;
            self.out
                .push(u8::try_from(byte).expect("masked to one byte"));
        }
        self.out
    }
}

/// Reads bits MSB-first from a byte slice. Reading past the end yields zero
/// bits, which never happens for a well-formed stream decoded for its known
/// length.
#[derive(Debug)]
/// Reads a byte buffer as an MSB-first bit stream, buffering bits in a 64-bit accumulator
/// so a read/peek is register arithmetic rather than a per-bit slice access.
pub struct BitReader<'a> {
    data: &'a [u8],
    /// Next byte to load into `acc`.
    byte: usize,
    /// Bit accumulator, left-aligned: the next bit to read is bit 63, and the `valid` bits
    /// below it. Bits below `valid` are always 0, so reads past the end return 0 padding.
    acc: u64,
    /// Number of valid bits currently in `acc` (counted from bit 63 downward).
    valid: u32,
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        BitReader {
            data,
            byte: 0,
            acc: 0,
            valid: 0,
        }
    }

    /// Load whole bytes into the top of `acc` until it holds more than 56 bits (so up to 8
    /// more can't overflow) or the input is exhausted.
    #[inline]
    fn refill(&mut self) {
        while self.valid <= 56 {
            let Some(&b) = self.data.get(self.byte) else {
                break;
            };
            self.acc |= u64::from(b) << (56 - self.valid);
            self.valid += 8;
            self.byte += 1;
        }
    }

    #[inline]
    pub fn read_bit(&mut self) -> u32 {
        if self.valid == 0 {
            self.refill();
            if self.valid == 0 {
                return 0; // past the end: padding bit
            }
        }
        let bit = u32::try_from(self.acc >> 63).expect("single bit");
        self.acc <<= 1;
        self.valid -= 1;
        bit
    }

    /// Peek the next `n` bits (`1 <= n <= 32`) MSB-first as an integer, without consuming.
    /// Bits past the end of the input read as 0.
    #[inline]
    pub fn peek(&mut self, n: u8) -> u32 {
        if self.valid < u32::from(n) {
            self.refill();
        }
        u32::try_from(self.acc >> (64 - u32::from(n))).expect("n <= 32 bits")
    }

    /// Advance the read position by `n` bits (call after a `peek(n)`, or to skip end padding).
    #[inline]
    pub fn advance(&mut self, n: u8) {
        self.acc <<= n;
        self.valid = self.valid.saturating_sub(u32::from(n));
    }

    /// Number of whole bytes consumed so far, rounding up a partially-read byte.
    /// Used to advance past a byte-aligned bitstream embedded in a larger buffer.
    pub fn bytes_consumed(&self) -> usize {
        // Bits loaded into `acc` (`byte * 8`) minus those still buffered (`valid`).
        let consumed_bits = self.byte * 8 - usize::cast_from(self.valid);
        consumed_bits.div_ceil(8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(data: &[u8]) {
        let mut counter = FrequencyCounter::default();
        counter.observe(data);
        let Some(code) = counter.build() else {
            // Fewer than two distinct symbols: nothing to check here.
            return;
        };

        // Lengths fully determine the code.
        let rebuilt = HuffmanCode::from_lengths(*code.lengths());
        assert_eq!(code.lengths(), rebuilt.lengths());

        let mut writer = BitWriter::new();
        code.encode(data, &mut writer);
        let bits = writer.finish();

        let mut reader = BitReader::new(&bits);
        let mut out = Vec::new();
        rebuilt.decode_into(&mut reader, data.len(), &mut out);
        assert_eq!(out, data);
    }

    #[mz_ore::test]
    fn round_trip_empty() {
        round_trip(&[]);
    }

    #[mz_ore::test]
    fn round_trip_single_symbol() {
        round_trip(&[7; 1000]);
    }

    #[mz_ore::test]
    fn round_trip_two_symbols() {
        let data: Vec<u8> = (0..1000).map(|i| if i % 3 == 0 { 1 } else { 0 }).collect();
        round_trip(&data);
    }

    #[mz_ore::test]
    fn round_trip_skewed() {
        // Heavily skewed distribution: byte 0 dominates, a long tail of rares.
        let mut data = vec![0u8; 5000];
        for b in 1..=200u8 {
            data.extend(std::iter::repeat(b).take(usize::from(b) / 10 + 1));
        }
        round_trip(&data);
    }

    #[mz_ore::test]
    fn round_trip_uniform() {
        let data: Vec<u8> = (0u8..=255).cycle().take(8192).collect();
        round_trip(&data);
        // Uniform over 256 symbols: every code is exactly 8 bits, so the encoded
        // size matches the input (no compression, no expansion).
        let mut counter = FrequencyCounter::default();
        counter.observe(&data);
        let code = counter.build().unwrap();
        for b in 0..=255u8 {
            assert_eq!(code.lengths()[usize::from(b)], 8);
        }
    }

    #[mz_ore::test]
    fn round_trip_pseudo_random() {
        // A cheap LCG so the test stays deterministic without rng deps.
        let mut state = 0x1234_5678_9abc_def0u64;
        let data: Vec<u8> = (0..10_000)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                u8::try_from((state >> 33) & 0xFF).expect("masked to one byte")
            })
            .collect();
        round_trip(&data);
    }

    #[mz_ore::test]
    fn declines_when_optimal_code_exceeds_max_bits() {
        // Fibonacci frequencies force a maximally unbalanced ("caterpillar") tree whose
        // longest codeword length grows with the symbol count. With enough symbols it
        // exceeds MAX_BITS, and `from_frequencies` must decline rather than build a code
        // longer than the decoder supports.
        let mut freq = [0u64; 256];
        let (mut a, mut b) = (1u64, 1u64);
        for slot in freq.iter_mut().take(MAX_BITS + 6) {
            *slot = a;
            let next = a + b;
            a = b;
            b = next;
        }
        assert!(
            HuffmanCode::from_frequencies(&freq).is_none(),
            "a code exceeding MAX_BITS must decline"
        );
    }

    #[mz_ore::test]
    fn decode_table_matches_canonical() {
        // The table fast path (with its short-code entries + long-code fallback) must decode
        // identically to the canonical decoder. Use a skewed-but-long-tailed distribution so
        // some codes exceed DECODE_TABLE_BITS and exercise the fallback.
        let mut freq = [0u64; 256];
        for (b, slot) in freq.iter_mut().enumerate() {
            let rank = 256 - u64::cast_from(b);
            *slot = 1 + rank * rank;
        }
        let code = HuffmanCode::from_frequencies(&freq).expect("code");
        let data: Vec<u8> = (0..5000u32)
            .map(|i| u8::try_from(i.wrapping_mul(2_654_435_761) >> 24).unwrap())
            .collect();
        let mut w = BitWriter::new();
        code.encode(&data, &mut w);
        let bits = w.finish();
        let mut out = Vec::new();
        code.decode_into(&mut BitReader::new(&bits), data.len(), &mut out);
        assert_eq!(out, data, "table decode must match the input");
    }
}
