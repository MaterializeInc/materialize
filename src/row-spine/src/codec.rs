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
    Huffman(HuffmanCode),
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
        }
    }

    fn heap_size(&self, callback: &mut impl FnMut(usize, usize)) {
        match self {
            ColumnCodec::Raw => {}
            ColumnCodec::Constant(bytes) => callback(bytes.len(), bytes.len()),
            ColumnCodec::Huffman(code) => code.heap_size(callback),
        }
    }
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
}

impl ColumnStats {
    fn observe(&mut self, bytes: &[u8]) {
        self.freq.observe(bytes);
        self.raw_bytes += bytes.len();
        if self.count == 0 {
            self.constant = Some(bytes.into());
            self.is_constant = true;
        } else if self.is_constant && self.constant.as_deref() != Some(bytes) {
            self.is_constant = false;
            self.constant = None;
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
            let huffman_bytes = data_bytes.saturating_add(overhead);
            if huffman_bytes < best_bytes {
                best = ColumnCodec::Huffman(code);
                best_bytes = huffman_bytes;
            }
        }

        let _ = best_bytes;
        best
    }
}

/// Gathers per-column statistics across a batch to build a [`RowCodec`].
#[derive(Clone, Debug, Default)]
pub struct RowStats {
    columns: Vec<ColumnStats>,
}

impl RowStats {
    /// Observe one row's column byte slices (in order).
    pub fn observe<'a, I>(&mut self, columns: I)
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        for (index, bytes) in columns.into_iter().enumerate() {
            if self.columns.len() <= index {
                self.columns.push(ColumnStats::default());
            }
            self.columns[index].observe(bytes);
        }
    }

    /// Build a per-column codec, or `None` if no columns were observed or the
    /// best choice for every column is `Raw` (nothing to gain — store rows raw
    /// and skip the codec entirely).
    pub fn build(&self) -> Option<RowCodec> {
        if self.columns.is_empty() {
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
        let rows: Vec<Row> = (0..300i64)
            .map(|i| {
                Row::pack_slice(&[Datum::String(match i % 4 {
                    0 => "mz_catalog",
                    1 => "mz_internal",
                    2 => "information_schema",
                    _ => "pg_catalog",
                })])
            })
            .collect();
        round_trip(&rows).expect("a codec is chosen");
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
}
