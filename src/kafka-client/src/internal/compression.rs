// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Record-set compression codecs and the codec error type.
//!
//! Compression writes what librdkafka writes for RecordBatch v2:
//!
//! * gzip: a single gzip member.
//! * snappy: one raw snappy block, without the xerial (snappy-java) framing.
//! * lz4: one LZ4 frame with independent 64 KiB blocks, no block or content
//!   checksums and no content size.
//! * zstd: one zstd frame that records the content size.
//!
//! Decompression accepts what real producers and brokers write: gzip or zlib
//! streams, raw or xerial-framed snappy (the Java producer frames snappy), LZ4
//! frames in any block size and block mode with optional checksums, and zstd
//! frames with or without a content size. Every decoder stops with
//! [`CodecError::TooLarge`] before producing more than `max_output` bytes, and
//! no allocation is sized by an untrusted length larger than `max_output`.

use std::fmt;
use std::io::{self, Read, Write};
use std::str::FromStr;

use mz_ore::cast::CastFrom;

/// A Kafka compression codec, as stored in the low three bits of the batch
/// (or legacy message) attributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Compression {
    const ATTRIBUTE_MASK: i16 = 0x07;

    /// Decodes the codec from batch or message attributes.
    pub(crate) fn from_attributes(attributes: i16) -> Result<Self, CodecError> {
        match attributes & Self::ATTRIBUTE_MASK {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Gzip),
            2 => Ok(Compression::Snappy),
            3 => Ok(Compression::Lz4),
            4 => Ok(Compression::Zstd),
            other => Err(CodecError::UnsupportedCompression(other)),
        }
    }

    /// The attribute bits that select this codec.
    pub(crate) fn attribute_bits(self) -> i16 {
        match self {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
        }
    }

    /// The librdkafka name of the codec.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Snappy => "snappy",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Parses a value of the global librdkafka `compression.codec` property (alias
/// `compression.type`).
///
/// Like librdkafka, names match case-insensitively. NOTE: `inherit` is only
/// valid for the topic-level property, where it means "use the global codec".
/// librdkafka rejects it for the global property, the only level this client
/// exposes, so it is rejected here too.
impl FromStr for Compression {
    type Err = CodecError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ]
        .into_iter()
        .find(|c| c.name().eq_ignore_ascii_case(s))
        .ok_or_else(|| CodecError::InvalidCompressionName(s.to_owned()))
    }
}

/// An error encoding or decoding Kafka record data.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CodecError {
    /// The attributes name a codec that does not exist.
    #[error("unsupported compression codec {0}")]
    UnsupportedCompression(i16),
    /// The entry at `offset` has a magic byte other than 0, 1 or 2. Its
    /// framing is intact, so consumers can skip it.
    #[error("unsupported message magic byte {magic} at offset {offset}")]
    UnsupportedMagic { magic: i8, offset: i64 },
    /// The data is structurally invalid.
    #[error("malformed record data: {0}")]
    Malformed(String),
    /// Compressed data could not be decompressed.
    #[error("failed to decompress {codec} data: {reason}")]
    Decompress { codec: Compression, reason: String },
    /// Data could not be compressed.
    #[error("failed to compress {codec} data: {reason}")]
    Compress { codec: Compression, reason: String },
    /// Decompressed data would exceed the caller's limit.
    #[error("decompressed data exceeds the limit of {limit} bytes")]
    TooLarge { limit: usize },
    /// The stored checksum does not match the data.
    #[error("checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },
    /// A batch must contain at least one record.
    #[error("cannot build an empty record batch")]
    EmptyBatch,
    /// The text matches librdkafka's error for this property.
    #[error("Invalid value for configuration property \"compression.codec\"")]
    InvalidCompressionName(String),
    /// The text matches librdkafka's error for this property.
    #[error("Invalid value for configuration property \"partitioner\": {0}")]
    InvalidPartitionerName(String),
}

impl CodecError {
    pub(crate) fn malformed(reason: impl Into<String>) -> Self {
        CodecError::Malformed(reason.into())
    }

    fn decompress(codec: Compression, reason: impl fmt::Display) -> Self {
        CodecError::Decompress {
            codec,
            reason: reason.to_string(),
        }
    }

    fn compress(codec: Compression, reason: impl fmt::Display) -> Self {
        CodecError::Compress {
            codec,
            reason: reason.to_string(),
        }
    }
}

/// Compresses `input` with `c` and appends the result to `out`. On error, `out`
/// may hold partial output.
///
/// `level` is librdkafka's `compression.level`: `None` or `-1` selects the
/// codec's default (gzip 6, zstd 3), larger values are clamped to the codec's
/// maximum (gzip 9, zstd 22). Snappy has no levels. NOTE: librdkafka switches
/// to LZ4HC for LZ4 levels 3 and above. lz4_flex has no HC mode, so LZ4 always
/// uses the fast compressor. The frame format is identical, only the ratio
/// differs.
pub(crate) fn compress(
    c: Compression,
    level: Option<i32>,
    input: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), CodecError> {
    let level = level.filter(|l| *l != -1);
    match c {
        Compression::None => {
            out.extend_from_slice(input);
            Ok(())
        }
        Compression::Gzip => {
            let level = level.map_or(6, |l| l.clamp(0, 9));
            let level = u32::try_from(level).expect("clamped to 0..=9");
            let mut encoder = flate2::write::GzEncoder::new(out, flate2::Compression::new(level));
            encoder
                .write_all(input)
                .and_then(|()| encoder.try_finish())
                .map_err(|e| CodecError::compress(c, e))
        }
        Compression::Snappy => {
            let max_len = snap::raw::max_compress_len(input.len());
            if max_len == 0 {
                return Err(CodecError::compress(c, "input too large"));
            }
            let start = out.len();
            out.resize(start + max_len, 0);
            match snap::raw::Encoder::new().compress(input, &mut out[start..]) {
                Ok(written) => {
                    out.truncate(start + written);
                    Ok(())
                }
                Err(e) => {
                    out.truncate(start);
                    Err(CodecError::compress(c, e))
                }
            }
        }
        Compression::Lz4 => lz4_compress(input, out),
        Compression::Zstd => {
            let level = level.map_or(3, |l| l.min(22));
            let start = out.len();
            out.reserve(zstd::zstd_safe::compress_bound(input.len()));
            let mut cursor = io::Cursor::new(&mut *out);
            cursor.set_position(u64::cast_from(start));
            zstd::bulk::Compressor::new(level)
                .and_then(|mut z| z.compress_to_buffer(input, &mut cursor))
                .map_err(|e| CodecError::compress(c, e))?;
            Ok(())
        }
    }
}

/// Decompresses a record set (RecordBatch v2 records, or the inner message
/// set of a magic 1 wrapper message) compressed with `c`.
///
/// Fails with [`CodecError::TooLarge`] if the output would exceed
/// `max_output` bytes.
pub(crate) fn decompress(
    c: Compression,
    input: &[u8],
    max_output: usize,
) -> Result<Vec<u8>, CodecError> {
    decompress_inner(c, input, max_output, Lz4HeaderChecksum::Proper)
}

/// Like [`decompress`], for the payload of a magic 0 wrapper message.
///
/// Kafka before 0.10 computed the LZ4 frame header checksum over the magic
/// number too (KAFKA-3160). Such frames are accepted here, as are frames with
/// a correct checksum.
pub(crate) fn decompress_magic0(
    c: Compression,
    input: &[u8],
    max_output: usize,
) -> Result<Vec<u8>, CodecError> {
    decompress_inner(c, input, max_output, Lz4HeaderChecksum::AllowLegacy)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lz4HeaderChecksum {
    Proper,
    AllowLegacy,
}

fn decompress_inner(
    c: Compression,
    input: &[u8],
    max_output: usize,
    lz4_hc: Lz4HeaderChecksum,
) -> Result<Vec<u8>, CodecError> {
    match c {
        Compression::None => {
            if input.len() > max_output {
                return Err(CodecError::TooLarge { limit: max_output });
            }
            Ok(input.to_vec())
        }
        Compression::Gzip => {
            // Like librdkafka (zlib `inflateInit2(15 + 32)`), accept zlib
            // streams as well as gzip. Only the first gzip member is read.
            let hint = initial_capacity(input.len(), max_output);
            if input.starts_with(&[0x1f, 0x8b]) {
                read_limited(c, flate2::bufread::GzDecoder::new(input), hint, max_output)
            } else {
                read_limited(
                    c,
                    flate2::bufread::ZlibDecoder::new(input),
                    hint,
                    max_output,
                )
            }
        }
        Compression::Snappy => snappy_decompress(input, max_output),
        Compression::Lz4 => lz4_decompress(input, max_output, lz4_hc),
        Compression::Zstd => {
            let hint = match zstd::zstd_safe::get_frame_content_size(input) {
                Ok(Some(size)) => match usize::try_from(size) {
                    Ok(size) if size <= max_output => size,
                    _ => return Err(CodecError::TooLarge { limit: max_output }),
                },
                Ok(None) => initial_capacity(input.len(), max_output),
                Err(_) => return Err(CodecError::decompress(c, "invalid zstd frame header")),
            };
            let decoder = zstd::stream::read::Decoder::with_buffer(input)
                .map_err(|e| CodecError::decompress(c, e))?;
            read_limited(c, decoder, hint, max_output)
        }
    }
}

/// A capacity guess for decompressed data of unknown size.
fn initial_capacity(input_len: usize, max_output: usize) -> usize {
    input_len.saturating_mul(4).min(max_output)
}

/// Reads `reader` to the end, failing once more than `max_output` bytes come
/// out of it.
fn read_limited(
    c: Compression,
    reader: impl Read,
    capacity: usize,
    max_output: usize,
) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(capacity);
    let limit = u64::cast_from(max_output).saturating_add(1);
    reader
        .take(limit)
        .read_to_end(&mut out)
        .map_err(|e| CodecError::decompress(c, e))?;
    if out.len() > max_output {
        return Err(CodecError::TooLarge { limit: max_output });
    }
    Ok(out)
}

/// The header snappy-java's `SnappyOutputStream` writes: magic, then a
/// big-endian version and minimum compatible version.
const XERIAL_MAGIC: &[u8; 8] = b"\x82SNAPPY\x00";
const XERIAL_HEADER_LEN: usize = 16;

fn snappy_decompress(input: &[u8], max_output: usize) -> Result<Vec<u8>, CodecError> {
    let c = Compression::Snappy;
    let mut decoder = snap::raw::Decoder::new();
    if !input.starts_with(XERIAL_MAGIC) || input.len() < XERIAL_HEADER_LEN {
        let len = snap::raw::decompress_len(input).map_err(|e| CodecError::decompress(c, e))?;
        if len > max_output {
            return Err(CodecError::TooLarge { limit: max_output });
        }
        let mut out = vec![0; len];
        decoder
            .decompress(input, &mut out)
            .map_err(|e| CodecError::decompress(c, e))?;
        return Ok(out);
    }

    // Xerial framing: after the header, a sequence of big-endian u32 length
    // prefixed raw snappy blocks. Size the output from the block headers
    // first so that it is allocated once.
    let chunks = || XerialChunks {
        rest: &input[XERIAL_HEADER_LEN..],
    };
    let mut total: usize = 0;
    for chunk in chunks() {
        let len = snap::raw::decompress_len(chunk?).map_err(|e| CodecError::decompress(c, e))?;
        total = total.saturating_add(len);
        if total > max_output {
            return Err(CodecError::TooLarge { limit: max_output });
        }
    }
    let mut out = vec![0; total];
    let mut pos = 0;
    for chunk in chunks() {
        pos += decoder
            .decompress(chunk?, &mut out[pos..])
            .map_err(|e| CodecError::decompress(c, e))?;
    }
    Ok(out)
}

struct XerialChunks<'a> {
    rest: &'a [u8],
}

impl<'a> Iterator for XerialChunks<'a> {
    type Item = Result<&'a [u8], CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        let Some((len, rest)) = self.rest.split_first_chunk::<4>() else {
            self.rest = &[];
            return Some(Err(CodecError::decompress(
                Compression::Snappy,
                "truncated snappy-java chunk length",
            )));
        };
        let len = usize::cast_from(u32::from_be_bytes(*len));
        if len > rest.len() {
            self.rest = &[];
            return Some(Err(CodecError::decompress(
                Compression::Snappy,
                format!(
                    "invalid snappy-java chunk length {len} > {} available bytes",
                    rest.len()
                ),
            )));
        }
        let (chunk, rest) = rest.split_at(len);
        self.rest = rest;
        Some(Ok(chunk))
    }
}

const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4d, 0x18];
/// The LZ4 block window: how far back a match in linked block mode may reach.
const LZ4_WINDOW: usize = 64 * 1024;

/// The frame descriptor this client writes: version 1 with independent
/// blocks and no checksums (`FLG`), 64 KiB blocks (`BD`), as librdkafka.
const LZ4_DESCRIPTOR: [u8; 2] = [0b0110_0000, 0b0100_0000];
const LZ4_BLOCK_SIZE: usize = 64 * 1024;
/// The high bit of a block size marks a block stored uncompressed.
const LZ4_UNCOMPRESSED_BLOCK: u32 = 1 << 31;

/// Writes `input` as one LZ4 frame to `out`, compressing each block straight
/// into `out`.
///
/// NOTE: lz4_flex's `FrameEncoder` would allocate a 64 KiB input buffer, an
/// output buffer of the same size and its hash table for every batch, on the
/// sender's thread (see the producer's `BufferPool` for why that matters).
fn lz4_compress(input: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
    out.extend_from_slice(&LZ4_FRAME_MAGIC);
    out.extend_from_slice(&LZ4_DESCRIPTOR);
    out.push(lz4_header_checksum(&LZ4_DESCRIPTOR));
    for block in input.chunks(LZ4_BLOCK_SIZE) {
        let at = out.len();
        let len = u32::try_from(block.len()).expect("blocks are 64 KiB");
        out.resize(
            at + 4 + lz4_flex::block::get_maximum_output_size(block.len()),
            0,
        );
        let compressed = lz4_flex::block::compress_into(block, &mut out[at + 4..])
            .map_err(|e| CodecError::compress(Compression::Lz4, e))?;
        if compressed < block.len() {
            let n = u32::try_from(compressed).expect("smaller than the block");
            out[at..at + 4].copy_from_slice(&n.to_le_bytes());
            out.truncate(at + 4 + compressed);
        } else {
            out.truncate(at);
            out.extend_from_slice(&(len | LZ4_UNCOMPRESSED_BLOCK).to_le_bytes());
            out.extend_from_slice(block);
        }
    }
    // End mark.
    out.extend_from_slice(&0u32.to_le_bytes());
    Ok(())
}

/// Decodes one LZ4 frame (lz4 frame format v1.6) straight into the output
/// buffer.
///
/// Like librdkafka, a single frame is expected and trailing bytes after it are
/// an error. Dictionary IDs are not supported (librdkafka and Kafka never set
/// them).
fn lz4_decompress(
    input: &[u8],
    max_output: usize,
    header_checksum: Lz4HeaderChecksum,
) -> Result<Vec<u8>, CodecError> {
    let c = Compression::Lz4;
    let err = |reason: &str| CodecError::decompress(c, reason);
    if input.len() < 7 || input[..4] != LZ4_FRAME_MAGIC {
        return Err(err("invalid LZ4 frame magic"));
    }
    let flg = input[4];
    let bd = input[5];
    if flg >> 6 != 0b01 {
        return Err(err("unsupported LZ4 frame version"));
    }
    let linked_blocks = flg & 0x20 == 0;
    let block_checksums = flg & 0x10 != 0;
    let has_content_size = flg & 0x08 != 0;
    let content_checksum = flg & 0x04 != 0;
    if flg & 0x02 != 0 || bd & 0x8f != 0 {
        return Err(err("reserved LZ4 frame header bits set"));
    }
    if flg & 0x01 != 0 {
        return Err(err("LZ4 frame dictionaries are not supported"));
    }
    let block_max = match (bd >> 4) & 0x07 {
        4 => 64 << 10,
        5 => 256 << 10,
        6 => 1 << 20,
        7 => 4 << 20,
        _ => return Err(err("invalid LZ4 block maximum size")),
    };
    let header_len = 6 + if has_content_size { 8 } else { 0 };
    let Some(&hc) = input.get(header_len) else {
        return Err(err("truncated LZ4 frame header"));
    };
    let proper_hc = lz4_header_checksum(&input[4..header_len]);
    let legacy_ok = header_checksum == Lz4HeaderChecksum::AllowLegacy
        && hc == lz4_header_checksum(&input[..header_len]);
    if hc != proper_hc && !legacy_ok {
        return Err(err("LZ4 frame header checksum mismatch"));
    }
    let content_size = if has_content_size {
        let size = u64::from_le_bytes(input[6..14].try_into().expect("8 bytes"));
        match usize::try_from(size) {
            Ok(size) if size <= max_output => Some(size),
            _ => return Err(CodecError::TooLarge { limit: max_output }),
        }
    } else {
        None
    };

    let mut out = Vec::with_capacity(
        content_size.unwrap_or_else(|| initial_capacity(input.len(), max_output)),
    );
    let mut rest = &input[header_len + 1..];
    loop {
        let Some((block_header, tail)) = rest.split_first_chunk::<4>() else {
            return Err(err("truncated LZ4 block header"));
        };
        let block_header = u32::from_le_bytes(*block_header);
        rest = tail;
        if block_header == 0 {
            break;
        }
        let uncompressed = block_header & 0x8000_0000 != 0;
        let len = usize::cast_from(block_header & 0x7fff_ffff);
        if len > block_max {
            return Err(err("LZ4 block larger than the frame's maximum block size"));
        }
        if len > rest.len() {
            return Err(err("truncated LZ4 block"));
        }
        let (block, tail) = rest.split_at(len);
        rest = tail;
        if block_checksums {
            let Some((checksum, tail)) = rest.split_first_chunk::<4>() else {
                return Err(err("truncated LZ4 block checksum"));
            };
            rest = tail;
            if twox_hash::XxHash32::oneshot(0, block) != u32::from_le_bytes(*checksum) {
                return Err(err("LZ4 block checksum mismatch"));
            }
        }
        let start = out.len();
        if uncompressed {
            if start + len > max_output {
                return Err(CodecError::TooLarge { limit: max_output });
            }
            out.extend_from_slice(block);
            continue;
        }
        // A block expands to at most `block_max` bytes. Give the decoder that
        // much room, then check the limit against what it actually produced.
        out.resize(start + block_max, 0);
        let (prev, dst) = out.split_at_mut(start);
        let written = if linked_blocks {
            let dict = &prev[prev.len().saturating_sub(LZ4_WINDOW)..];
            lz4_flex::block::decompress_into_with_dict(block, dst, dict)
        } else {
            lz4_flex::block::decompress_into(block, dst)
        }
        .map_err(|e| CodecError::decompress(c, e))?;
        out.truncate(start + written);
        if out.len() > max_output {
            return Err(CodecError::TooLarge { limit: max_output });
        }
    }
    if content_checksum {
        let Some((checksum, tail)) = rest.split_first_chunk::<4>() else {
            return Err(err("truncated LZ4 content checksum"));
        };
        rest = tail;
        if twox_hash::XxHash32::oneshot(0, &out) != u32::from_le_bytes(*checksum) {
            return Err(err("LZ4 content checksum mismatch"));
        }
    }
    if content_size.is_some_and(|size| size != out.len()) {
        return Err(err("LZ4 content size mismatch"));
    }
    if !rest.is_empty() {
        return Err(err("trailing bytes after LZ4 frame"));
    }
    Ok(out)
}

/// The LZ4 frame header checksum: the second byte of the xxHash32 of the frame
/// descriptor.
fn lz4_header_checksum(descriptor: &[u8]) -> u8 {
    let [_, hc, _, _] = twox_hash::XxHash32::oneshot(0, descriptor).to_le_bytes();
    hc
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [Compression; 5] = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    fn sample(len: usize) -> Vec<u8> {
        // Compressible but not trivially so.
        (0..len)
            .map(|i| u8::try_from((i * 7 + i / 13) % 251).expect("< 251"))
            .collect()
    }

    fn compressed(c: Compression, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        compress(c, None, input, &mut out).expect("compress");
        out
    }

    #[mz_ore::test]
    fn attributes_round_trip() {
        for c in ALL {
            assert_eq!(Compression::from_attributes(c.attribute_bits()), Ok(c));
            // Higher attribute bits are ignored.
            assert_eq!(
                Compression::from_attributes(c.attribute_bits() | 0x30),
                Ok(c)
            );
        }
        for bits in 5..8 {
            assert_eq!(
                Compression::from_attributes(bits),
                Err(CodecError::UnsupportedCompression(bits))
            );
        }
    }

    #[mz_ore::test]
    fn parse_names() {
        for c in ALL {
            assert_eq!(c.name().parse::<Compression>(), Ok(c));
            assert_eq!(c.name().to_uppercase().parse::<Compression>(), Ok(c));
        }
        let err = "inherit".parse::<Compression>().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid value for configuration property \"compression.codec\""
        );
        assert!("lz5".parse::<Compression>().is_err());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn round_trip_all_codecs() {
        for len in [0, 1, 100, 65535, 65536, 65537, 300_000] {
            let input = sample(len);
            for c in ALL {
                for level in [None, Some(-1), Some(0), Some(1), Some(9), Some(12)] {
                    let mut out = b"prefix".to_vec();
                    compress(c, level, &input, &mut out).expect("compress");
                    assert!(out.starts_with(b"prefix"), "{c} must append");
                    let decoded = decompress(c, &out[6..], len).expect("decompress");
                    assert!(decoded == input, "{c} level {level:?} len {len}");
                    if len > 0 {
                        assert_eq!(
                            decompress(c, &out[6..], len - 1),
                            Err(CodecError::TooLarge { limit: len - 1 }),
                            "{c} len {len}"
                        );
                    }
                }
            }
        }
    }

    #[mz_ore::test]
    fn writes_librdkafka_formats() {
        let input = sample(1000);
        // Raw snappy, no xerial header.
        assert!(!compressed(Compression::Snappy, &input).starts_with(XERIAL_MAGIC));
        // LZ4: FLG = version 01, independent blocks, no checksums or content
        // size. BD = 64 KiB blocks.
        let lz4 = compressed(Compression::Lz4, &input);
        assert_eq!(lz4[..6], [0x04, 0x22, 0x4d, 0x18, 0x60, 0x40]);
        // zstd records the content size.
        let zstd = compressed(Compression::Zstd, &input);
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&zstd).ok(),
            Some(Some(1000))
        );
        // gzip magic.
        assert!(compressed(Compression::Gzip, &input).starts_with(&[0x1f, 0x8b]));
    }

    #[mz_ore::test]
    fn gzip_accepts_zlib() {
        let input = sample(5000);
        let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&input).unwrap();
        let zlib = enc.finish().unwrap();
        assert_eq!(decompress(Compression::Gzip, &zlib, 5000).unwrap(), input);
    }

    fn xerial(input: &[u8], chunk_size: usize) -> Vec<u8> {
        let mut out = XERIAL_MAGIC.to_vec();
        out.extend_from_slice(&1u32.to_be_bytes());
        out.extend_from_slice(&1u32.to_be_bytes());
        for chunk in input.chunks(chunk_size) {
            let block = snap::raw::Encoder::new().compress_vec(chunk).unwrap();
            out.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
            out.extend_from_slice(&block);
        }
        out
    }

    #[mz_ore::test]
    fn snappy_xerial_framing() {
        let input = sample(100_000);
        let framed = xerial(&input, 32 * 1024);
        assert_eq!(
            decompress(Compression::Snappy, &framed, 100_000).unwrap(),
            input
        );
        assert_eq!(
            decompress(Compression::Snappy, &framed, 99_999),
            Err(CodecError::TooLarge { limit: 99_999 })
        );
        // Header only: no chunks.
        assert_eq!(
            decompress(Compression::Snappy, &xerial(&[], 1), 10).unwrap(),
            Vec::<u8>::new()
        );
        // Truncated chunk.
        assert!(decompress(Compression::Snappy, &framed[..framed.len() - 1], 100_000).is_err());
        // Truncated chunk length.
        let mut bad = xerial(&input[..10], 32);
        bad.extend_from_slice(&[0, 0]);
        assert!(decompress(Compression::Snappy, &bad, 100_000).is_err());
    }

    #[mz_ore::test]
    fn snappy_raw_length_is_checked_before_allocating() {
        // A raw snappy block claiming 2^32 - 1 bytes of output.
        let bogus = [0xff, 0xff, 0xff, 0xff, 0x0f, 0x00];
        assert_eq!(
            decompress(Compression::Snappy, &bogus, 1 << 20),
            Err(CodecError::TooLarge { limit: 1 << 20 })
        );
    }

    /// Rewrites the header checksum the way Kafka < 0.10 computed it.
    fn break_lz4_header_checksum(frame: &mut [u8]) {
        let header_len = 6 + if frame[4] & 0x08 != 0 { 8 } else { 0 };
        frame[header_len] = lz4_header_checksum(&frame[..header_len]);
    }

    #[mz_ore::test]
    fn lz4_legacy_header_checksum() {
        let input = sample(10_000);
        let mut frame = compressed(Compression::Lz4, &input);
        assert_eq!(
            decompress_magic0(Compression::Lz4, &frame, 10_000).unwrap(),
            input
        );
        break_lz4_header_checksum(&mut frame);
        assert!(decompress(Compression::Lz4, &frame, 10_000).is_err());
        assert_eq!(
            decompress_magic0(Compression::Lz4, &frame, 10_000).unwrap(),
            input
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn lz4_frame_variants() {
        use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
        let input = sample(600_000);
        for block_size in [
            BlockSize::Max64KB,
            BlockSize::Max256KB,
            BlockSize::Max1MB,
            BlockSize::Max4MB,
        ] {
            for block_mode in [BlockMode::Independent, BlockMode::Linked] {
                for checksums in [false, true] {
                    for content_size in [None, Some(600_000)] {
                        let info = FrameInfo::new()
                            .block_size(block_size)
                            .block_mode(block_mode)
                            .block_checksums(checksums)
                            .content_checksum(checksums)
                            .content_size(content_size);
                        let mut enc = FrameEncoder::with_frame_info(info, Vec::new());
                        enc.write_all(&input).unwrap();
                        let mut frame = enc.finish().unwrap();
                        let ctx =
                            format!("{block_size:?} {block_mode:?} {checksums} {content_size:?}");
                        assert!(
                            decompress(Compression::Lz4, &frame, 600_000).unwrap() == input,
                            "{ctx}"
                        );
                        assert_eq!(
                            decompress(Compression::Lz4, &frame, 599_999),
                            Err(CodecError::TooLarge { limit: 599_999 }),
                            "{ctx}"
                        );
                        if checksums {
                            // Flip a byte in the last block's payload.
                            let at = frame.len() - 12;
                            frame[at] ^= 0x55;
                            assert!(
                                decompress(Compression::Lz4, &frame, 600_000).is_err(),
                                "{ctx}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn lz4_frames_decode_with_lz4_flex() {
        use lz4_flex::frame::{BlockMode, BlockSize, FrameDecoder, FrameEncoder, FrameInfo};
        // Incompressible data is stored in uncompressed blocks.
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let random: Vec<u8> = (0..150_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_le_bytes()[0]
            })
            .collect();
        for input in [
            Vec::new(),
            b"hello".to_vec(),
            sample(LZ4_BLOCK_SIZE),
            sample(200_001),
            random,
        ] {
            let frame = compressed(Compression::Lz4, &input);
            let mut decoded = Vec::new();
            FrameDecoder::new(&frame[..])
                .read_to_end(&mut decoded)
                .unwrap();
            assert!(decoded == input, "{}", input.len());
            assert!(decompress(Compression::Lz4, &frame, input.len()).unwrap() == input);
            // The frame header is lz4_flex's for the same settings.
            let info = FrameInfo::new()
                .block_size(BlockSize::Max64KB)
                .block_mode(BlockMode::Independent);
            let mut enc = FrameEncoder::with_frame_info(info, Vec::new());
            enc.write_all(&input).unwrap();
            assert_eq!(frame[..7], enc.finish().unwrap()[..7]);
        }
    }

    #[mz_ore::test]
    fn lz4_trailing_bytes_rejected() {
        let mut frame = compressed(Compression::Lz4, b"hello");
        frame.push(0);
        assert!(decompress(Compression::Lz4, &frame, 100).is_err());
    }

    #[mz_ore::test]
    fn zstd_without_content_size() {
        let input = sample(200_000);
        let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        enc.include_contentsize(false).unwrap();
        enc.write_all(&input).unwrap();
        let frame = enc.finish().unwrap();
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&frame).ok(),
            Some(None)
        );
        assert_eq!(
            decompress(Compression::Zstd, &frame, 200_000).unwrap(),
            input
        );
        assert_eq!(
            decompress(Compression::Zstd, &frame, 199_999),
            Err(CodecError::TooLarge { limit: 199_999 })
        );
    }

    #[mz_ore::test]
    fn zstd_content_size_is_checked_before_allocating() {
        // A frame header claiming a huge content size.
        let frame = compressed(Compression::Zstd, &sample(1000));
        assert_eq!(
            decompress(Compression::Zstd, &frame, 999),
            Err(CodecError::TooLarge { limit: 999 })
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn garbage_never_panics() {
        use rand::{RngExt, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let input = sample(3000);
        for c in ALL {
            let valid = compressed(c, &input);
            for _ in 0..2000 {
                let mut data = valid.clone();
                match rng.random_range(0..3) {
                    0 => {
                        let n = rng.random_range(1..4);
                        for _ in 0..n {
                            let i = rng.random_range(0..data.len());
                            data[i] = rng.random();
                        }
                    }
                    1 => data.truncate(rng.random_range(0..data.len())),
                    _ => {
                        let len = rng.random_range(0..64);
                        data = (0..len).map(|_| rng.random()).collect();
                    }
                }
                let _ = decompress(c, &data, 4000);
                let _ = decompress_magic0(c, &data, 4000);
            }
        }
    }
}
