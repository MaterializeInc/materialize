// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Legacy message sets: message formats v0 and v1 ("magic 0" and "magic 1").
//!
//! Brokers return stored data verbatim, so topics written by pre-0.11
//! producers still contain these. Each entry is one message, big-endian:
//!
//! ```text
//!  0 offset       int64
//!  8 messageSize  int32   bytes after this field
//! 12 crc          uint32  CRC-32 of everything from magic on
//! 16 magic        int8
//! 17 attributes   int8    bits 0-2 codec, bit 3 log append time (magic 1)
//! 18 timestamp    int64   magic 1 only
//!    key          int32 length (negative for null), bytes
//!    value        int32 length (negative for null), bytes
//! ```
//!
//! A compressed "wrapper" message carries a compressed message set as its
//! value, and its offset is that of the last inner message. Inner offsets are
//! absolute for magic 0 and relative (0, 1, ...) for magic 1.

use std::ops::Range;

use bytes::Bytes;
use mz_ore::cast::ReinterpretCast;

use crate::internal::compression::{self, CodecError, Compression};
use crate::internal::records::{
    BatchHeader, FetchedBatch, LOG_OVERHEAD, Record, RecordHeaders, TimestampType, get_i32, get_i64,
};

const CRC: usize = 12;
const MAGIC: usize = 16;
const ATTRIBUTES: usize = 17;
const TIMESTAMP: usize = 18;

const ATTR_COMPRESSION_MASK: u8 = 0x07;
const ATTR_LOG_APPEND_TIME: u8 = 0x08;

/// A parsed message. `key` and `value` index into the parsed bytes.
struct Message {
    offset: i64,
    magic: i8,
    attributes: u8,
    /// -1 for magic 0.
    timestamp: i64,
    key: Option<Range<usize>>,
    value: Option<Range<usize>>,
}

/// Parses a message whose bytes are exactly `data`.
fn parse(data: &[u8]) -> Result<Message, CodecError> {
    if data.len() <= ATTRIBUTES {
        return Err(CodecError::malformed("truncated message header"));
    }
    let offset = get_i64(data, 0);
    let magic = i8::reinterpret_cast(data[MAGIC]);
    let header_len = match magic {
        0 => TIMESTAMP,
        1 => TIMESTAMP + 8,
        magic => return Err(CodecError::UnsupportedMagic { magic, offset }),
    };
    // The key and value lengths follow the header.
    if data.len() < header_len + 8 {
        return Err(CodecError::malformed(format!(
            "message at offset {offset} is shorter than its header"
        )));
    }
    let timestamp = if magic == 1 {
        get_i64(data, TIMESTAMP)
    } else {
        -1
    };
    let mut pos = header_len;
    let key = read_bytes(data, &mut pos)?;
    let value = read_bytes(data, &mut pos)?;
    if pos != data.len() {
        return Err(CodecError::malformed(format!(
            "message at offset {offset} has a size that does not match its contents"
        )));
    }
    Ok(Message {
        offset,
        magic,
        attributes: data[ATTRIBUTES],
        timestamp,
        key,
        value,
    })
}

/// Reads an int32 length and that many bytes. Like the Java client, any
/// negative length means null.
fn read_bytes(data: &[u8], pos: &mut usize) -> Result<Option<Range<usize>>, CodecError> {
    let Some(len) = data.get(*pos..*pos + 4) else {
        return Err(CodecError::malformed("truncated message"));
    };
    let len = i32::from_be_bytes(len.try_into().expect("4 bytes"));
    *pos += 4;
    let Ok(len) = usize::try_from(len) else {
        return Ok(None);
    };
    let start = *pos;
    let end = start
        .checked_add(len)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| CodecError::malformed("message key or value extends past its end"))?;
    *pos = end;
    Ok(Some(start..end))
}

/// Parses the header of a legacy entry spanning all of `data`.
pub(super) fn parse_header(data: Bytes) -> Result<FetchedBatch, CodecError> {
    let message = parse(&data)?;
    let compression = Compression::from_attributes(i16::from(message.attributes))?;
    let timestamp_type = if message.magic == 0 {
        TimestampType::NotAvailable
    } else if message.attributes & ATTR_LOG_APPEND_TIME != 0 {
        TimestampType::LogAppendTime
    } else {
        TimestampType::CreateTime
    };
    let header = BatchHeader {
        base_offset: message.offset,
        last_offset: message.offset,
        partition_leader_epoch: -1,
        magic: message.magic,
        compression,
        timestamp_type,
        is_transactional: false,
        is_control: false,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        first_timestamp: message.timestamp,
        max_timestamp: message.timestamp,
        record_count: if compression == Compression::None {
            1
        } else {
            -1
        },
        size: data.len(),
    };
    Ok(FetchedBatch { header, data })
}

/// Decodes the records of a legacy entry, decompressing a wrapper message.
pub(super) fn records(data: &Bytes, max_decompressed: usize) -> Result<Vec<Record>, CodecError> {
    let wrapper = parse(data)?;
    let compression = Compression::from_attributes(i16::from(wrapper.attributes))?;
    if compression == Compression::None {
        let (timestamp, timestamp_type) = message_timestamp(&wrapper);
        return Ok(vec![record(
            data,
            0,
            &wrapper,
            wrapper.offset,
            timestamp,
            timestamp_type,
        )]);
    }

    let compressed = wrapper.value.clone().map_or(&[][..], |v| &data[v]);
    let decompress = if wrapper.magic == 0 {
        compression::decompress_magic0
    } else {
        compression::decompress
    };
    let inner = Bytes::from(decompress(compression, compressed, max_decompressed)?);

    let mut messages = Vec::new();
    let mut pos = 0;
    while pos < inner.len() {
        let rest = &inner[pos..];
        if rest.len() < LOG_OVERHEAD {
            return Err(CodecError::malformed(
                "truncated message in compressed message set",
            ));
        }
        let size = usize::try_from(get_i32(rest, 8))
            .ok()
            .map(|size| LOG_OVERHEAD + size)
            .filter(|size| *size <= rest.len())
            .ok_or_else(|| {
                CodecError::malformed("invalid message size in compressed message set")
            })?;
        let message = parse(&rest[..size])?;
        // Like the Java client, reject nested compression.
        if message.attributes & ATTR_COMPRESSION_MASK != 0 {
            return Err(CodecError::malformed(
                "compressed message inside a compressed message",
            ));
        }
        messages.push((pos, message));
        pos += size;
    }

    // Magic 1 inner offsets are relative, and the wrapper's offset is the
    // last inner message's absolute offset.
    let offset_base = match messages.last() {
        Some((_, last)) if wrapper.magic >= 1 => wrapper
            .offset
            .checked_sub(last.offset)
            .ok_or_else(|| CodecError::malformed("invalid relative offsets"))?,
        _ => 0,
    };
    let log_append_time = wrapper.magic >= 1 && wrapper.attributes & ATTR_LOG_APPEND_TIME != 0;
    messages
        .iter()
        .map(|(pos, message)| {
            let offset = offset_base
                .checked_add(message.offset)
                .ok_or_else(|| CodecError::malformed("invalid relative offsets"))?;
            let (timestamp, timestamp_type) = if log_append_time {
                (wrapper.timestamp, TimestampType::LogAppendTime)
            } else {
                message_timestamp(message)
            };
            Ok(record(
                &inner,
                *pos,
                message,
                offset,
                timestamp,
                timestamp_type,
            ))
        })
        .collect()
}

/// The timestamp of an uncompressed message. NOTE: like librdkafka, a magic 1
/// timestamp of 0 counts as not available.
fn message_timestamp(message: &Message) -> (i64, TimestampType) {
    if message.magic == 0 || message.timestamp == 0 {
        (-1, TimestampType::NotAvailable)
    } else if message.attributes & ATTR_LOG_APPEND_TIME != 0 {
        (message.timestamp, TimestampType::LogAppendTime)
    } else {
        (message.timestamp, TimestampType::CreateTime)
    }
}

/// Builds a record for `message`, which was parsed from `buf[base..]`.
fn record(
    buf: &Bytes,
    base: usize,
    message: &Message,
    offset: i64,
    timestamp: i64,
    timestamp_type: TimestampType,
) -> Record {
    let slice =
        |r: &Option<Range<usize>>| r.as_ref().map(|r| buf.slice(base + r.start..base + r.end));
    Record {
        offset,
        timestamp,
        timestamp_type,
        key: slice(&message.key),
        value: slice(&message.value),
        headers: RecordHeaders::default(),
    }
}

/// The stored and computed CRC-32 of a message.
pub(super) fn crc(data: &[u8]) -> (u32, u32) {
    (
        u32::reinterpret_cast(get_i32(data, CRC)),
        crc32fast::hash(&data[MAGIC..]),
    )
}
