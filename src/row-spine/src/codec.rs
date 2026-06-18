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
    /// Map each distinct value to a fixed-width index into a value table.
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

/// Per-column dictionary codec state.
#[derive(Clone, Debug)]
pub struct DictCodec {
    /// Distinct values, indexed by their code.
    values: Vec<Box<[u8]>>,
    /// Value -> code, for encoding (dead weight after `seal`, like the row
    /// dictionary's encode map).
    index: BTreeMap<Box<[u8]>, u32>,
    /// Bytes used to store each code.
    width: u8,
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
                let code = self_code(&c.index, value);
                put_be(u64::from(code), c.width, out);
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
                let code = take_be(cursor, c.width);
                out.extend_from_slice(&c.values[usize::try_from(code).expect("code fits usize")]);
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

/// Look up a value's dictionary code, panicking if absent (the model was built
/// from a superset of the encoded values).
fn self_code(index: &BTreeMap<Box<[u8]>, u32>, value: &[u8]) -> u32 {
    *index.get(value).expect("value present in dictionary")
}

/// One codec per column; encodes and decodes whole rows.
#[derive(Clone, Debug)]
pub struct RowCodec {
    columns: Vec<ColumnCodec>,
}

impl RowCodec {
    /// The number of columns this codec models.
    pub fn arity(&self) -> usize {
        self.columns.len()
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
    }
}

/// Max distinct values for which a dictionary is considered (keeps the code
/// width <= 2 bytes and bounds the stats map).
const DICT_MAX_CARDINALITY: usize = 1 << 16;

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
    /// Distinct values -> code, while cardinality stays within the dictionary
    /// bound; `None` once it is exceeded (dictionary no longer viable).
    distinct: Option<BTreeMap<Box<[u8]>, u32>>,
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
            self.distinct = Some(BTreeMap::new());
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
        if let Some(distinct) = &mut self.distinct {
            if !distinct.contains_key(bytes) {
                if distinct.len() >= DICT_MAX_CARDINALITY {
                    self.distinct = None;
                } else {
                    // Codes are assigned in first-seen (insertion) order, NOT sorted order.
                    // Decode is a table lookup so order is irrelevant to correctness, but it
                    // means dictionary codes are not order-preserving — a comparison fast path
                    // could not compare codes directly without sorting the table first.
                    let code = u32::try_from(distinct.len()).expect("cardinality bounded");
                    distinct.insert(bytes.into(), code);
                }
            }
        }
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

        // Dictionary: low-cardinality columns -> fixed-width code into a table.
        if let Some(distinct) = &self.distinct {
            if distinct.len() >= 2 {
                let width = byte_width(u64::try_from(distinct.len() - 1).unwrap_or(u64::MAX));
                let table_bytes: usize = distinct.keys().map(|k| k.len()).sum::<usize>()
                    + distinct.len() * std::mem::size_of::<Box<[u8]>>();
                let dict_bytes = self
                    .count
                    .saturating_mul(usize::from(width))
                    .saturating_add(table_bytes)
                    .saturating_add(std::mem::size_of::<DictCodec>());
                if dict_bytes < best_bytes {
                    let mut values: Vec<Box<[u8]>> = vec![Box::default(); distinct.len()];
                    for (value, &code) in distinct {
                        values[usize::try_from(code).expect("code fits usize")] = value.clone();
                    }
                    best = ColumnCodec::Dictionary(Box::new(DictCodec {
                        values,
                        index: distinct.clone(),
                        width,
                    }));
                    best_bytes = dict_bytes;
                }
            }
        }

        // Every arm updates `best_bytes` for symmetry, so the last candidate's update (the
        // Dictionary arm above) is never read. Acknowledge it rather than special-casing the
        // final arm, which would break if another candidate is appended later.
        let _ = best_bytes;
        best
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

    /// Build a per-column codec, or `None` if no columns were observed, the
    /// observed rows have differing arity, or the best choice for every column
    /// is `Raw` (nothing to gain — store rows raw and skip the codec entirely).
    pub fn build(&self) -> Option<RowCodec> {
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
        Some(RowCodec { columns })
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
        // Long, heavily-skewed, all-distinct values, more than DICT_MAX_CARDINALITY of
        // them: Dictionary is disqualified by cardinality, FoR by variable length, and
        // Huffman's per-byte savings beat Raw -- so the selector lands on Huffman. This
        // is the only test that drives the Huffman arm through `RowCodec` end to end.
        let prefix = "a".repeat(40);
        let values: Vec<String> = (0..DICT_MAX_CARDINALITY + 2000)
            .map(|i| format!("{prefix}{i}"))
            .collect();
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
    fn dictionary_cardinality_boundary() {
        // The distinct-value table is retained at exactly DICT_MAX_CARDINALITY entries
        // (codes still fit two bytes) and dropped the instant a further distinct value
        // arrives, disqualifying Dictionary.
        let mut at_cap = ColumnStats::default();
        for i in 0..u64::try_from(DICT_MAX_CARDINALITY).unwrap() {
            at_cap.observe(&i.to_be_bytes());
        }
        let distinct = at_cap.distinct.as_ref().expect("retained at the cap");
        assert_eq!(distinct.len(), DICT_MAX_CARDINALITY);
        assert_eq!(byte_width(u64::try_from(distinct.len() - 1).unwrap()), 2);

        let mut over_cap = at_cap.clone();
        over_cap.observe(&u64::MAX.to_be_bytes());
        assert!(
            over_cap.distinct.is_none(),
            "one past the cap disqualifies Dictionary"
        );
    }
}
