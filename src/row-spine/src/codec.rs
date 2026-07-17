// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-column compression for row-spine batches.
//!
//! Each column of a batch is compressed independently with a [`ColumnCodec`]
//! chosen from batch-wide statistics, so a column of ids, a column of names, and
//! a column of flags each get a scheme that fits. A [`RowCodec`] bundles one
//! codec per column and encodes/decodes whole rows row-major: a row's columns
//! are encoded back-to-back into the row's stored bytes.
//!
//! Codecs are **self-delimiting** in the encoded stream — each one consumes
//! exactly its own bytes on decode — so they compose without a central
//! length-prefix block: decoding a row just walks the column codecs in order.
//!
//! New codecs slot in by adding a [`ColumnCodec`] variant plus its size estimate
//! in [`ColumnStats::choose`]; the selector then adopts it wherever it wins. The
//! variants here are the row-major-compatible ones (each value encoded from a
//! batch-wide stat); cross-row schemes (delta, RLE) would need a column-major
//! layout and are out of scope.

use std::collections::BTreeMap;

use mz_repr::read_datum;

use crate::huffman::{BitReader, BitWriter, FrequencyCounter, HuffmanCode};
use crate::row_codec::{MisraGries, SAFE_TAG_BASE};

/// Number of dictionary entries, and the leading-byte values reserved for their
/// references. A dictionary reference is a single byte `>= SAFE_TAG_BASE`, a region
/// no datum's first byte ever occupies (pinned by `row_codec`'s `test_safe_tag_base`),
/// so a decoder can tell a reference from a raw datum by that byte alone. This both
/// bounds the table to `256 - SAFE_TAG_BASE` (= 134) entries and keeps every code a
/// single byte — the same capacity as `row_codec`'s dictionary. Popular values that
/// do not fit fall through to raw; the bounded heavy-hitter summary keeps the *most*
/// popular ones in the table as cardinality grows.
// `as` in a const: `From<u8>`/`usize::from` are not const-callable here.
#[allow(clippy::as_conversions)]
const DICT_TAGS: usize = 256 - SAFE_TAG_BASE as usize;

/// A codec for a single column, selected per column at `seal()`.
#[derive(Clone, Debug)]
pub enum ColumnCodec {
    /// Store the datum's bytes verbatim. Self-delimited by the row encoding
    /// (`read_datum` knows where a datum ends).
    Raw,
    /// Every value in the column is identical batch-wide; nothing is stored per
    /// row, and the value is reproduced on decode.
    Constant(Box<[u8]>),
    /// Huffman-code the datum's bytes: `uvarint(decoded_len) ++ byte-aligned bits`.
    /// Boxed so the enum stays pointer-sized — a `Raw`/`Constant` column then
    /// costs only a discriminant + pointer in the per-column `Vec`, not the
    /// ~1.3 KB of a `HuffmanCode`'s inline tables.
    Huffman(Box<HuffmanCode>),
    /// Frame-of-reference for fixed-width (<= 8 byte) values: interpret the bytes
    /// as a big-endian integer, store `value - min` in the fewest bytes that hold
    /// `max - min`. Reconstructs the exact original bytes (it is pure arithmetic
    /// on the byte pattern, so no datum-type knowledge is needed); it simply does
    /// not get *chosen* when the byte order makes residuals large.
    For(Box<ForCodec>),
    /// Map the column's *popular* values to a single spare-tag byte (`>= SAFE_TAG_BASE`,
    /// a region no datum first-byte occupies, so decode tells a reference from a raw
    /// datum by that byte); any value not in the table falls through to its raw datum.
    /// Built from a bounded heavy-hitter summary, so it stays useful as cardinality
    /// grows and is sound to install before all data is seen (unseen values just fall
    /// through to raw). Bounded to `DICT_TAGS` entries — the popular values that fit.
    Dictionary(Box<DictCodec>),
}

/// Frame-of-reference codec state.
#[derive(Clone, Debug)]
pub struct ForCodec {
    /// Original (fixed) byte length of every value.
    len: u8,
    /// Minimum value (big-endian interpretation of the bytes).
    min: u64,
    /// Bytes used to store each residual (`value - min`).
    width: u8,
}

/// Per-column dictionary codec state. Holds the popular (heavy-hitter) values
/// only; values absent from the table are stored raw on decode/encode's
/// fall-through path.
#[derive(Clone, Debug)]
pub struct DictCodec {
    /// Popular values, indexed by their code (`0..DICT_TAGS`).
    values: Vec<Box<[u8]>>,
    /// Value -> code, for encoding (used on every push to map a value to its
    /// single reference byte; absent values fall through to raw).
    index: BTreeMap<Box<[u8]>, u8>,
}

impl ColumnCodec {
    /// Append the encoding of one column value to `out`.
    fn encode(&self, value: &[u8], out: &mut Vec<u8>) {
        match self {
            ColumnCodec::Raw => out.extend_from_slice(value),
            ColumnCodec::Constant(_) => {}
            ColumnCodec::Huffman(code) => {
                put_uvarint(value.len(), out);
                let mut writer = BitWriter::new();
                code.encode(value, &mut writer);
                out.extend_from_slice(&writer.finish());
            }
            ColumnCodec::For(c) => {
                let residual = be_uint(value) - c.min;
                put_be(residual, c.width, out);
            }
            ColumnCodec::Dictionary(c) => {
                if let Some(&code) = c.index.get(value) {
                    // Heavy hitter: a single reference byte in the reserved region.
                    out.push(SAFE_TAG_BASE + code);
                } else {
                    // Not in the dictionary: store the datum raw. Sound because the
                    // row format never produces a first byte `>= SAFE_TAG_BASE`, so
                    // decode can tell this raw datum from a reference. (A dictionary
                    // value is itself a raw datum, hence also `< SAFE_TAG_BASE`.)
                    debug_assert!(
                        value.first().is_some_and(|&b| b < SAFE_TAG_BASE),
                        "raw fall-through datum first-byte must be < SAFE_TAG_BASE",
                    );
                    out.extend_from_slice(value);
                }
            }
        }
    }

    /// Consume one column value from `cursor` (advancing it past exactly this
    /// value's encoded bytes), appending the original value bytes to `out`.
    fn decode(&self, cursor: &mut &[u8], out: &mut Vec<u8>) {
        match self {
            ColumnCodec::Raw => {
                let before = *cursor;
                // SAFETY: stored bytes are valid row-encoded datums.
                unsafe { read_datum(cursor) };
                let consumed = before.len() - cursor.len();
                out.extend_from_slice(&before[..consumed]);
            }
            ColumnCodec::Constant(bytes) => out.extend_from_slice(bytes),
            ColumnCodec::Huffman(code) => {
                let (len, rest) = get_uvarint(cursor);
                let mut reader = BitReader::new(rest);
                code.decode_into(&mut reader, len, out);
                *cursor = &rest[reader.bytes_consumed()..];
            }
            ColumnCodec::For(c) => {
                let residual = take_be(cursor, c.width);
                let value = c.min + residual;
                put_be(value, c.len, out);
            }
            ColumnCodec::Dictionary(c) => {
                // The leading byte disambiguates: `>= SAFE_TAG_BASE` is a reference
                // (the row format never starts a datum that high), otherwise a raw
                // datum stored on the fall-through path.
                if cursor[0] >= SAFE_TAG_BASE {
                    let code = cursor[0] - SAFE_TAG_BASE;
                    *cursor = &cursor[1..];
                    out.extend_from_slice(&c.values[usize::from(code)]);
                } else {
                    let before = *cursor;
                    // SAFETY: stored bytes are valid row-encoded datums.
                    unsafe { read_datum(cursor) };
                    let consumed = before.len() - cursor.len();
                    out.extend_from_slice(&before[..consumed]);
                }
            }
        }
    }

    fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
        match self {
            ColumnCodec::Raw => {}
            ColumnCodec::Constant(bytes) => callback(bytes.len(), bytes.len()),
            ColumnCodec::Huffman(code) => {
                // The boxed `HuffmanCode`'s inline tables, plus its own heap.
                let inline = std::mem::size_of::<HuffmanCode>();
                callback(inline, inline);
                code.heap_size(callback);
            }
            ColumnCodec::For(_) => {
                let inline = std::mem::size_of::<ForCodec>();
                callback(inline, inline);
            }
            ColumnCodec::Dictionary(c) => {
                let inline = std::mem::size_of::<DictCodec>();
                callback(inline, inline);
                for v in &c.values {
                    callback(v.len(), v.len());
                }
                // The encode index holds the same keys again, plus map overhead.
                let entry = std::mem::size_of::<(Box<[u8]>, u32)>();
                callback(c.index.len() * entry, c.index.len() * entry);
                for k in c.index.keys() {
                    callback(k.len(), k.len());
                }
            }
        }
    }
}

/// Interpret `bytes` (length <= 8) as a big-endian unsigned integer.
fn be_uint(bytes: &[u8]) -> u64 {
    let mut value = 0u64;
    for &b in bytes {
        value = (value << 8) | u64::from(b);
    }
    value
}

/// Append the low `width` bytes of `value`, big-endian.
fn put_be(value: u64, width: u8, out: &mut Vec<u8>) {
    for shift in (0..width).rev() {
        let byte = (value >> (u32::from(shift) * 8)) & 0xFF;
        out.push(u8::try_from(byte).expect("masked to a byte"));
    }
}

/// Read `width` big-endian bytes from the front of `cursor`, advancing it.
fn take_be(cursor: &mut &[u8], width: u8) -> u64 {
    let width = usize::from(width);
    let value = be_uint(&cursor[..width]);
    *cursor = &cursor[width..];
    value
}

/// One codec per column; encodes and decodes whole rows.
///
/// The codec retains the [`RowStats`] it was built from. Those stats are
/// mergeable (per-column frequencies add, min/max widen, dictionaries union),
/// so when two compressed batches compact we can combine their stats and
/// re-derive a codec valid for the union via [`RowCodec::new_from`] — the same
/// shape the dictionary codec uses, so per-column compression survives merges
/// rather than being dropped to raw.
#[derive(Clone, Debug)]
pub struct RowCodec {
    columns: Vec<ColumnCodec>,
    /// The stats this codec was built from, retained so a later merge can
    /// combine them and rebuild (rather than trying to combine the lossy
    /// codecs themselves).
    stats: RowStats,
}

impl RowCodec {
    /// The number of columns this codec models.
    pub fn arity(&self) -> usize {
        self.columns.len()
    }

    /// Build a codec valid for the union of the supplied codecs' data by merging
    /// their retained stats and re-selecting per-column codecs. Returns `None`
    /// on the same conditions as [`RowStats::build`] (no columns, mixed arity, or
    /// nothing worth compressing). Used by the merge path so per-column
    /// compression survives compaction.
    pub fn new_from<'a>(codecs: impl IntoIterator<Item = &'a RowCodec>) -> Option<RowCodec> {
        let mut merged: Option<RowStats> = None;
        for codec in codecs {
            match &mut merged {
                None => merged = Some(codec.stats.clone()),
                Some(acc) => acc.merge(&codec.stats),
            }
        }
        merged?.build()
    }

    /// Encode a row's column byte slices (in order), appending to `out`.
    pub fn encode<'a, I>(&self, columns: I, out: &mut Vec<u8>)
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut count = 0;
        for (index, bytes) in columns.into_iter().enumerate() {
            self.columns[index].encode(bytes, out);
            count += 1;
        }
        debug_assert_eq!(count, self.columns.len(), "row arity != codec arity");
    }

    /// Record a row's columns in the retained stats without encoding. The encode
    /// path calls this for every row it stores, so the retained stats track the
    /// rows currently held under this codec (consolidation drops some on merge);
    /// the next merge rebuilds from these via [`RowCodec::new_from`]. This is the
    /// "harvest the surviving counts for the next merge" half of the design.
    pub fn observe<'a, I>(&mut self, columns: I)
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        self.stats.observe(columns);
    }

    /// Decode a row (the format produced by [`Self::encode`]), appending the
    /// concatenated original column bytes to `out`.
    pub fn decode_into(&self, record: &[u8], out: &mut Vec<u8>) {
        let mut cursor = record;
        for column in &self.columns {
            column.decode(&mut cursor, out);
        }
    }

    pub fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
        let elem = std::mem::size_of::<ColumnCodec>();
        callback(self.columns.len() * elem, self.columns.capacity() * elem);
        for column in &self.columns {
            column.heap_size(callback);
        }
        // The retained stats (kept so merges can rebuild) hold heap allocations
        // of their own: the per-column dictionaries and constant values.
        self.stats.heap_size(callback);
    }
}

/// Per-column statistics gathered over a batch to select a [`ColumnCodec`].
#[derive(Clone, Debug, Default)]
struct ColumnStats {
    freq: FrequencyCounter,
    /// Total bytes of all values (the size of the `Raw` encoding).
    raw_bytes: usize,
    /// Number of values observed.
    count: usize,
    /// The single value seen so far, while all values have been identical.
    constant: Option<Box<[u8]>>,
    /// Whether all values observed have been identical.
    is_constant: bool,
    /// Common byte length while every value shares one (for frame-of-reference).
    len: Option<usize>,
    /// Whether all values have had the same byte length.
    fixed_len: bool,
    /// Min/max of the big-endian integer interpretation (valid when
    /// `fixed_len` and `len <= 8`).
    min_int: u64,
    max_int: u64,
    /// Heavy-hitter summary of the column's values, bounded regardless of true
    /// cardinality. The dictionary is built from these popular values; everything
    /// else falls through to raw. Unlike an enumerate-all map, this never "gives
    /// up" as distinct values accumulate — dictionary compression stays available
    /// for the popular values no matter how long the rare tail grows.
    heavy: MisraGries<Vec<u8>>,
}

impl ColumnStats {
    fn observe(&mut self, bytes: &[u8]) {
        self.freq.observe(bytes);
        self.raw_bytes += bytes.len();
        if self.count == 0 {
            self.constant = Some(bytes.into());
            self.is_constant = true;
            self.len = Some(bytes.len());
            self.fixed_len = true;
            self.min_int = u64::MAX;
            self.max_int = 0;
        } else {
            if self.is_constant && self.constant.as_deref() != Some(bytes) {
                self.is_constant = false;
                self.constant = None;
            }
            if self.len != Some(bytes.len()) {
                self.fixed_len = false;
            }
        }
        if bytes.len() <= 8 {
            let v = be_uint(bytes);
            self.min_int = self.min_int.min(v);
            self.max_int = self.max_int.max(v);
        }
        // Track popularity for the dictionary. Bounded: never holds more than the
        // summary's capacity, no matter how many distinct values arrive.
        self.heavy.insert_ref(bytes);
        self.count += 1;
    }

    /// Choose the smallest-estimated codec for this column.
    fn choose(&self) -> ColumnCodec {
        // `Constant` stores nothing per row — unbeatable when it applies.
        if self.is_constant {
            if let Some(value) = &self.constant {
                return ColumnCodec::Constant(value.clone());
            }
        }

        let mut best = ColumnCodec::Raw;
        let mut best_bytes = self.raw_bytes;

        if let Some(code) = self.freq.build() {
            // Estimated Huffman payload: sum of code-length(byte) over all bytes,
            // rounded to bytes, plus per-row overhead (a length varint and up to
            // one padding byte from byte-aligning each value's bitstream).
            let lengths = code.lengths();
            let freq = self.freq.frequencies();
            let mut bits = 0u64;
            for b in 0..256 {
                bits += freq[b] * u64::from(lengths[b]);
            }
            let data_bytes = usize::try_from(bits.div_ceil(8)).unwrap_or(usize::MAX);
            let overhead = self.count * 2; // ~1 byte varint + ~1 byte alignment per row
            // The model itself costs storage (its inline tables + sorted symbols),
            // counted once for the whole column; only adopt Huffman when the
            // per-row savings outweigh it. This keeps Huffman off small columns,
            // where the table would dwarf the data.
            let model_bytes = std::mem::size_of::<HuffmanCode>() + code.lengths().len();
            let huffman_bytes = data_bytes
                .saturating_add(overhead)
                .saturating_add(model_bytes);
            if huffman_bytes < best_bytes {
                best = ColumnCodec::Huffman(Box::new(code));
                best_bytes = huffman_bytes;
            }
        }

        // Frame-of-reference: fixed-width (<= 8 byte) numeric-ish columns.
        if self.fixed_len && self.len.is_some_and(|l| l <= 8) && self.max_int > self.min_int {
            let width = byte_width(self.max_int - self.min_int);
            let for_bytes = self
                .count
                .saturating_mul(usize::from(width))
                .saturating_add(std::mem::size_of::<ForCodec>());
            if for_bytes < best_bytes {
                best = ColumnCodec::For(Box::new(ForCodec {
                    len: u8::try_from(self.len.unwrap()).expect("len <= 8"),
                    min: self.min_int,
                    width,
                }));
                best_bytes = for_bytes;
            }
        }

        // Dictionary: map the popular values to compact references; values not in
        // the table fall through to raw. Robust as cardinality grows — the popular
        // values stay compressed however long the rare tail gets.
        if let Some((codec, dict_bytes)) = self.dict_candidate() {
            if dict_bytes < best_bytes {
                best = codec;
                best_bytes = dict_bytes;
            }
        }

        // Every arm updates `best_bytes` for symmetry, so the last candidate's update (the
        // Dictionary arm above) is never read. Acknowledge it rather than special-casing the
        // final arm, which would break if another candidate is appended later.
        let _ = best_bytes;
        best
    }

    /// Build the heavy-hitter dictionary candidate for this column, with its
    /// estimated encoded size, or `None` if no dictionary is worth building.
    ///
    /// Sound on *every* path — seal, merge, and mid-formation — because values
    /// absent from the table fall through to raw, so it never depends on having
    /// observed the whole column. That is why both [`ColumnStats::choose`] and the
    /// mid-formation [`ColumnStats::choose_safe`] use it, unlike `For`/`Constant`.
    fn dict_candidate(&self) -> Option<(ColumnCodec, usize)> {
        // Heavy hitters, most popular first. Keep only values worth a table slot
        // (seen at least twice), capped at the one-byte reference capacity.
        let mut hitters = self.heavy.clone().done();
        hitters.retain(|(_value, count)| *count >= 2);
        hitters.truncate(DICT_TAGS);
        if hitters.len() < 2 {
            return None;
        }
        let table_bytes: usize = hitters.iter().map(|(v, _)| v.len()).sum::<usize>()
            + hitters.len() * std::mem::size_of::<Box<[u8]>>();
        // One byte per hit row; the rest of the column's raw bytes are stored verbatim
        // on the fall-through path. (Heavy-hitter counts are lower bounds, so this
        // estimate is mildly conservative.)
        let hit_count: usize = hitters.iter().map(|(_, c)| *c).sum();
        let hit_value_bytes: usize = hitters.iter().map(|(v, c)| v.len() * c).sum();
        let miss_bytes = self.raw_bytes.saturating_sub(hit_value_bytes);
        let dict_bytes = hit_count
            .saturating_add(table_bytes)
            .saturating_add(miss_bytes)
            .saturating_add(std::mem::size_of::<DictCodec>());
        let mut values: Vec<Box<[u8]>> = Vec::with_capacity(hitters.len());
        let mut index = BTreeMap::new();
        for (code, (value, _count)) in hitters.into_iter().enumerate() {
            let code = u8::try_from(code).expect("bounded by DICT_TAGS (<= 134)");
            let value: Box<[u8]> = value.into_boxed_slice();
            index.insert(value.clone(), code);
            values.push(value);
        }
        let codec = ColumnCodec::Dictionary(Box::new(DictCodec { values, index }));
        Some((codec, dict_bytes))
    }

    /// Choose a codec that stays sound as more rows arrive: per-column Huffman over
    /// all 256 bytes (via [`FrequencyCounter::build_floored`]) or the heavy-hitter
    /// [`Self::dict_candidate`] (misses fall through to raw), whichever is smaller,
    /// else `Raw`. Excludes `For`/`Constant`, whose soundness needs the full batch.
    /// Used by the mid-formation install; see [`RowStats::build_safe`].
    fn choose_safe(&self) -> ColumnCodec {
        let mut best = ColumnCodec::Raw;
        let mut best_bytes = self.raw_bytes;
        if let Some(code) = self.freq.build_floored() {
            let lengths = code.lengths();
            let freq = self.freq.frequencies();
            let mut bits = 0u64;
            for b in 0..256 {
                bits += freq[b] * u64::from(lengths[b]);
            }
            let data_bytes = usize::try_from(bits.div_ceil(8)).unwrap_or(usize::MAX);
            let overhead = self.count * 2;
            let model_bytes = std::mem::size_of::<HuffmanCode>() + lengths.len();
            let huffman_bytes = data_bytes
                .saturating_add(overhead)
                .saturating_add(model_bytes);
            if huffman_bytes < best_bytes {
                best = ColumnCodec::Huffman(Box::new(code));
                best_bytes = huffman_bytes;
            }
        }
        // Dictionary is sound mid-formation (misses fall through to raw), so the
        // incremental path gets it too — this is where the big low-cardinality wins
        // on reduce/upsert/CDC outputs come from, not just byte-Huffman.
        if let Some((codec, dict_bytes)) = self.dict_candidate() {
            if dict_bytes < best_bytes {
                best = codec;
                best_bytes = dict_bytes;
            }
        }
        let _ = best_bytes;
        best
    }

    /// Fold another column's stats into this one, yielding stats for the union of
    /// the two columns' values. Each field merges in the way that keeps it a valid
    /// summary of the combined data: frequencies add, the constant/fixed-length
    /// predicates stay true only if both sides agree, min/max widen, and the
    /// heavy-hitter summaries combine (popularity adds).
    fn merge(&mut self, other: &ColumnStats) {
        self.freq.add(&other.freq);
        self.raw_bytes += other.raw_bytes;
        // Constant only if both sides are constant *and* share the one value.
        if !(self.is_constant && other.is_constant && self.constant == other.constant) {
            self.is_constant = false;
            self.constant = None;
        }
        // Fixed-length only if both sides are fixed and share the same length.
        if !(self.fixed_len && other.fixed_len && self.len == other.len) {
            self.fixed_len = false;
            self.len = None;
        }
        self.min_int = self.min_int.min(other.min_int);
        self.max_int = self.max_int.max(other.max_int);
        // Combine the heavy-hitter summaries: popularity adds, so the merged summary
        // reflects the union's popular values (and `choose` re-derives the dictionary
        // from it). Bounded on both sides, so the merge stays bounded too.
        self.heavy += other.heavy.clone();
        self.count += other.count;
    }

    /// Visit heap allocations held by these stats (the constant value and the
    /// dictionary of distinct values); the inline frequency table is counted by
    /// the caller via `size_of::<ColumnStats>()`.
    fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
        if let Some(value) = &self.constant {
            callback(value.len(), value.len());
        }
        self.heavy.heap_size(callback);
    }
}

/// Number of bytes needed to hold the unsigned value `max` (at least 1).
fn byte_width(max: u64) -> u8 {
    let bits = 64 - max.leading_zeros();
    u8::try_from(bits.div_ceil(8).max(1)).expect("width <= 8")
}

/// Gathers per-column statistics across a batch to build a [`RowCodec`].
#[derive(Clone, Debug, Default)]
pub struct RowStats {
    columns: Vec<ColumnStats>,
    /// Arity of the first observed row. A [`RowCodec`] models a fixed number of
    /// columns and `decode_into` walks all of them, so rows of differing arity
    /// cannot share one codec.
    arity: Option<usize>,
    /// Set once any observed row's arity differs from [`RowStats::arity`].
    mixed_arity: bool,
}

impl RowStats {
    /// Observe one row's column byte slices (in order).
    pub fn observe<'a, I>(&mut self, columns: I)
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut count = 0;
        for (index, bytes) in columns.into_iter().enumerate() {
            if self.columns.len() <= index {
                self.columns.push(ColumnStats::default());
            }
            self.columns[index].observe(bytes);
            count = index + 1;
        }
        // Track arity consistency. An empty (arity-0) row is a real arity signal here,
        // not something to skip: a codec built from non-empty rows would walk its columns
        // off the end of an empty record at decode time.
        match self.arity {
            None => self.arity = Some(count),
            Some(arity) if arity != count => self.mixed_arity = true,
            Some(_) => {}
        }
    }

    /// Fold another batch's stats into these, yielding stats for the union of the
    /// two batches' rows. Used by [`RowCodec::new_from`] on the merge path.
    pub fn merge(&mut self, other: &RowStats) {
        // Arity must agree for the per-column model to be sound; if it does not,
        // mark mixed so `build` declines (the merged container stays raw).
        match (self.arity, other.arity) {
            (Some(a), Some(b)) if a == b => {}
            _ => self.mixed_arity = true,
        }
        if other.mixed_arity {
            self.mixed_arity = true;
        }
        for (index, column) in other.columns.iter().enumerate() {
            if self.columns.len() <= index {
                self.columns.push(column.clone());
            } else {
                self.columns[index].merge(column);
            }
        }
    }

    /// Build a per-column codec, or `None` if no columns were observed, the
    /// observed rows have differing arity, or the best choice for every column
    /// is `Raw` (nothing to gain — store rows raw and skip the codec entirely).
    ///
    /// Consumes the stats: the resulting [`RowCodec`] retains them so a later
    /// merge can combine them and rebuild.
    pub fn build(self) -> Option<RowCodec> {
        // Decline on mixed arity: `decode_into` walks every modeled column, so a row whose
        // arity differs from the codec's would read past its record — a panic or silent
        // corruption in release. A uniform-arity container (the common case) is unaffected.
        if self.columns.is_empty() || self.mixed_arity {
            return None;
        }
        let columns: Vec<ColumnCodec> = self.columns.iter().map(ColumnStats::choose).collect();
        if columns.iter().all(|c| matches!(c, ColumnCodec::Raw)) {
            return None;
        }
        // The codec starts with *empty* retained stats: the chosen-from stats are
        // consumed here, and the codec re-accumulates them by observing the rows
        // it actually encodes (the seal/merge push, see `RowCodec::observe`). This
        // is what lets a merged codec's stats reflect surviving rows rather than
        // the ever-growing union of everything ever merged.
        Some(RowCodec {
            columns,
            stats: RowStats::default(),
        })
    }

    /// Build a *mid-formation-safe* codec from these stats: only codecs that stay
    /// correct as more rows (with as-yet-unseen byte values) arrive after the
    /// codec is fixed. That is per-column Huffman built over all 256 bytes (so any
    /// future byte still encodes) or `Raw` — never `For`/`Dictionary`/`Constant`,
    /// whose soundness needs the whole batch (a later value can exceed the FoR
    /// range, miss the dictionary, or break constancy). Used by the
    /// `STATS_THRESHOLD` install on a container still being formed; the full
    /// selector runs later at the next seal/merge, which see all the data.
    pub fn build_safe(self) -> Option<RowCodec> {
        if self.columns.is_empty() || self.mixed_arity {
            return None;
        }
        let columns: Vec<ColumnCodec> = self.columns.iter().map(ColumnStats::choose_safe).collect();
        if columns.iter().all(|c| matches!(c, ColumnCodec::Raw)) {
            return None;
        }
        Some(RowCodec {
            columns,
            stats: RowStats::default(),
        })
    }

    /// Visit heap allocations held by these stats, so a retaining [`RowCodec`]
    /// can account for them in its own `heap_size`.
    pub fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
        let elem = std::mem::size_of::<ColumnStats>();
        callback(self.columns.len() * elem, self.columns.capacity() * elem);
        for column in &self.columns {
            column.heap_size(callback);
        }
    }
}

/// Append `value` as an unsigned LEB128 varint.
fn put_uvarint(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let low = u8::try_from(value & 0x7f).expect("masked to 7 bits");
        value >>= 7;
        if value == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

/// Read an unsigned LEB128 varint from the front of `data`, returning the value
/// and the remaining bytes.
fn get_uvarint(data: &[u8]) -> (usize, &[u8]) {
    let mut value = 0usize;
    let mut shift = 0u32;
    let mut consumed = 0;
    for &byte in data {
        consumed += 1;
        value |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    (value, &data[consumed..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::{Datum, Row};

    /// Encode each row's columns, then decode and confirm the concatenated
    /// original column bytes are reproduced.
    fn round_trip(rows: &[Row]) -> Option<RowCodec> {
        let mut stats = RowStats::default();
        for row in rows {
            stats.observe(row.iter_bytes());
        }
        let codec = stats.build()?;
        for row in rows {
            let mut buf = Vec::new();
            codec.encode(row.iter_bytes(), &mut buf);
            let mut out = Vec::new();
            codec.decode_into(&buf, &mut out);
            let expected: Vec<u8> = row.iter_bytes().flatten().copied().collect();
            assert_eq!(out, expected, "row did not round-trip");
        }
        Some(codec)
    }

    /// The column byte slices of a row, via the `read_datum` split.
    trait IterBytes {
        fn iter_bytes(&self) -> ColIter<'_>;
    }
    impl IterBytes for Row {
        fn iter_bytes(&self) -> ColIter<'_> {
            ColIter { data: self.data() }
        }
    }
    #[derive(Clone)]
    struct ColIter<'a> {
        data: &'a [u8],
    }
    impl<'a> Iterator for ColIter<'a> {
        type Item = &'a [u8];
        fn next(&mut self) -> Option<&'a [u8]> {
            if self.data.is_empty() {
                return None;
            }
            let before = self.data;
            unsafe { read_datum(&mut self.data) };
            Some(&before[..before.len() - self.data.len()])
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn round_trip_mixed() {
        let rows: Vec<Row> = (0..500i64)
            .map(|i| {
                Row::pack_slice(&[
                    Datum::Int64(i % 13),                           // numeric, repetitive
                    Datum::String("a shared longish string value"), // constant text
                    Datum::Int32(i32::try_from(i).unwrap()),        // varying
                ])
            })
            .collect();
        let codec = round_trip(&rows).expect("a codec is chosen");
        assert_eq!(codec.arity(), 3);
        // The constant column 1 should be encoded as `Constant`.
        assert!(matches!(codec.columns[1], ColumnCodec::Constant(_)));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn round_trip_text_heavy() {
        // Enough rows that Huffman's savings outweigh its (storage-charged) table,
        // so the selector actually adopts it and we exercise the Huffman decode.
        let rows: Vec<Row> = (0..5000i64)
            .map(|i| {
                Row::pack_slice(&[Datum::String(match i % 4 {
                    0 => "mz_catalog",
                    1 => "mz_internal",
                    2 => "information_schema",
                    _ => "pg_catalog",
                })])
            })
            .collect();
        let codec = round_trip(&rows).expect("a codec is chosen");
        // Four distinct values -> a 1-byte dictionary code beats Huffman.
        assert!(matches!(codec.columns[0], ColumnCodec::Dictionary(_)));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn round_trip_for_numeric() {
        // A fixed-width numeric column with values in a narrow range -> FoR.
        let rows: Vec<Row> = (0..4000i64)
            .map(|i| Row::pack_slice(&[Datum::Int64(1_000_000 + (i % 500))]))
            .collect();
        let codec = round_trip(&rows).expect("a codec is chosen");
        assert!(matches!(codec.columns[0], ColumnCodec::For(_)));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn round_trip_dictionary_lowcard() {
        // A handful of distinct longer strings -> dictionary.
        let opts = ["alpha-value", "beta-value", "gamma-value"];
        let rows: Vec<Row> = (0..3000usize)
            .map(|i| Row::pack_slice(&[Datum::String(opts[i % opts.len()])]))
            .collect();
        let codec = round_trip(&rows).expect("a codec is chosen");
        assert!(matches!(codec.columns[0], ColumnCodec::Dictionary(_)));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn all_raw_returns_none() {
        // Two short, high-entropy distinct values per column: Huffman can't beat
        // raw once per-row overhead is counted, so no codec is built.
        let rows: Vec<Row> = vec![
            Row::pack_slice(&[Datum::Int16(1)]),
            Row::pack_slice(&[Datum::Int16(2)]),
        ];
        let mut stats = RowStats::default();
        for row in &rows {
            stats.observe(row.iter_bytes());
        }
        assert!(
            stats.build().is_none(),
            "expected no codec for tiny raw column"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn mixed_arity_returns_none() {
        // Rows of differing arity can't share a codec (decode walks a fixed column
        // count), so the builder must decline rather than risk reading off the end.
        let wide = Row::pack_slice(&[Datum::Int64(1), Datum::String("x")]);
        let narrow = Row::pack_slice(&[Datum::Int64(2)]);
        let mut stats = RowStats::default();
        stats.observe(wide.iter_bytes());
        stats.observe(narrow.iter_bytes());
        assert!(stats.build().is_none(), "mixed arity must decline");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn empty_row_is_an_arity_signal() {
        // An empty (arity-0) row among otherwise-compressible rows is a mixed-arity
        // signal, not something to skip: a codec built without it would later walk an
        // empty record's columns off the end.
        let full: Vec<Row> = (0..500i64)
            .map(|i| Row::pack_slice(&[Datum::Int64(i % 7), Datum::String("constant")]))
            .collect();
        let empty = Row::default();
        let mut stats = RowStats::default();
        for row in &full {
            stats.observe(row.iter_bytes());
        }
        stats.observe(empty.iter_bytes());
        assert!(
            stats.build().is_none(),
            "empty row among non-empty rows must decline"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn huffman_selected_for_skewed_high_cardinality() {
        // Long, all-distinct values (each appears exactly once): Dictionary finds no
        // value worth a table slot (needs a value seen >= 2x), FoR is out (variable
        // length), and Huffman's per-byte savings on the shared "aaa..." prefix beat
        // Raw -- so the selector lands on Huffman. This is the only test that drives
        // the Huffman arm through `RowCodec` end to end.
        let prefix = "a".repeat(40);
        let values: Vec<String> = (0..20_000).map(|i| format!("{prefix}{i}")).collect();
        let rows: Vec<Row> = values
            .iter()
            .map(|s| Row::pack_slice(&[Datum::String(s)]))
            .collect();
        let mut stats = RowStats::default();
        for row in &rows {
            stats.observe(row.iter_bytes());
        }
        let codec = stats.build().expect("a codec is chosen");
        assert!(
            matches!(codec.columns[0], ColumnCodec::Huffman(_)),
            "expected Huffman, got {:?}",
            codec.columns[0]
        );
        // Spot-check the Huffman round-trip on a sample (a full sweep is needlessly slow).
        for row in rows.iter().step_by(4096) {
            let mut buf = Vec::new();
            codec.encode(row.iter_bytes(), &mut buf);
            let mut out = Vec::new();
            codec.decode_into(&buf, &mut out);
            let expected: Vec<u8> = row.iter_bytes().flatten().copied().collect();
            assert_eq!(out, expected, "Huffman row did not round-trip");
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn new_from_merges_and_round_trips() {
        // Two batches with overlapping-but-different low-cardinality values. The
        // codec rebuilt from their merged stats (as a compaction does) must
        // round-trip rows from BOTH batches — i.e. per-column compression
        // survives the merge instead of being dropped to raw.
        let pick = |options: &[&'static str], i: i64| -> &'static str {
            options[usize::try_from(i).unwrap() % options.len()]
        };
        let batch1: Vec<Row> = (0..400i64)
            .map(|i| {
                Row::pack_slice(&[
                    Datum::Int64(i % 7),
                    Datum::String(pick(&["red", "green", "blue"], i)),
                ])
            })
            .collect();
        let batch2: Vec<Row> = (0..400i64)
            .map(|i| {
                Row::pack_slice(&[
                    Datum::Int64(i % 11),
                    Datum::String(pick(&["green", "blue", "cyan", "magenta"], i)),
                ])
            })
            .collect();
        let build = |rows: &[Row]| {
            let mut stats = RowStats::default();
            for row in rows {
                stats.observe(row.iter_bytes());
            }
            let mut codec = stats.build().expect("a codec is chosen");
            // `build` leaves the codec's retained stats empty; the encode path
            // normally repopulates them by observing each stored row. Simulate
            // that here so `new_from` has stats to merge.
            for row in rows {
                codec.observe(row.iter_bytes());
            }
            codec
        };
        let c1 = build(&batch1);
        let c2 = build(&batch2);
        let merged = RowCodec::new_from([&c1, &c2]).expect("merged codec is chosen");
        assert_eq!(merged.arity(), 2);
        // The merged codec must encode/decode every row of both batches, including
        // values (e.g. Int64 7..=10, "cyan"/"magenta") absent from batch1.
        for row in batch1.iter().chain(batch2.iter()) {
            let mut buf = Vec::new();
            merged.encode(row.iter_bytes(), &mut buf);
            let mut out = Vec::new();
            merged.decode_into(&buf, &mut out);
            let expected: Vec<u8> = row.iter_bytes().flatten().copied().collect();
            assert_eq!(out, expected, "merged codec round-trips a union row");
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn build_safe_uses_dictionary() {
        // Mid-formation (build_safe) must now pick the heavy-hitter Dictionary for a
        // low-cardinality column, not just byte-Huffman — this is the win for
        // reduce/upsert/CDC outputs. Round-trips through the dictionary's encode/decode.
        let opts = [
            "DELIVER IN PERSON",
            "COLLECT COD",
            "NONE",
            "TAKE BACK RETURN",
        ];
        let rows: Vec<Row> = (0..5_000usize)
            .map(|i| Row::pack_slice(&[Datum::String(opts[i % opts.len()])]))
            .collect();
        let mut stats = RowStats::default();
        for row in &rows {
            stats.observe(row.iter_bytes());
        }
        let codec = stats.build_safe().expect("a safe codec is chosen");
        assert!(
            matches!(codec.columns[0], ColumnCodec::Dictionary(_)),
            "mid-formation should pick Dictionary for low-cardinality data, got {:?}",
            codec.columns[0]
        );
        for row in &rows {
            let mut buf = Vec::new();
            codec.encode(row.iter_bytes(), &mut buf);
            let mut out = Vec::new();
            codec.decode_into(&buf, &mut out);
            let expected: Vec<u8> = row.iter_bytes().flatten().copied().collect();
            assert_eq!(
                out, expected,
                "mid-formation dictionary row must round-trip"
            );
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn dictionary_robust_to_rare_tail() {
        // A few popular values plus a long tail of distinct one-off values. The old
        // enumerate-all dictionary would "give up" once the tail crossed a cardinality
        // cap, dropping compression entirely. The heavy-hitter dictionary must instead
        // keep compressing the popular values, route the tail to raw, and round-trip
        // every row -- robustness as cardinality grows, exercising the fall-through.
        let popular = ["the most common value", "the second most common value"];
        let rows: Vec<Row> = (0..40_000i64)
            .map(|i| {
                // ~80% popular, ~20% unique tail values (well past any old cap over a
                // long run, though this batch stays modest to keep the test fast).
                if i % 5 == 0 {
                    Row::pack_slice(&[Datum::String(&format!("rare-tail-value-{i}"))])
                } else {
                    Row::pack_slice(&[Datum::String(popular[usize::try_from(i).unwrap() % 2])])
                }
            })
            .collect();

        let mut stats = RowStats::default();
        for row in &rows {
            stats.observe(row.iter_bytes());
        }
        let codec = stats.build().expect("a codec is chosen");
        assert!(
            matches!(codec.columns[0], ColumnCodec::Dictionary(_)),
            "popular values must still get a dictionary despite the rare tail, got {:?}",
            codec.columns[0]
        );
        // Every row round-trips: dictionary hits for the popular values, raw
        // fall-through for the unique tail.
        for row in &rows {
            let mut buf = Vec::new();
            codec.encode(row.iter_bytes(), &mut buf);
            let mut out = Vec::new();
            codec.decode_into(&buf, &mut out);
            let expected: Vec<u8> = row.iter_bytes().flatten().copied().collect();
            assert_eq!(
                out, expected,
                "row with rare-tail fall-through must round-trip"
            );
        }
    }
}
