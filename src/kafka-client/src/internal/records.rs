// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kafka record batches (message format v2) and legacy message sets (message
//! formats v0 and v1).
//!
//! Encoding ([`BatchBuilder`]) writes each record straight into the batch
//! buffer, behind a placeholder for the batch header that
//! [`BatchBuilder::build_reusing`] fills in. Uncompressed batches are never
//! copied.
//!
//! Decoding is zero-copy: [`parse_batches`] splits a fetched record set into
//! entries by reading only their headers, and [`FetchedBatch::records`]
//! decodes records on demand from one buffer (the fetched bytes, or one
//! decompressed allocation per batch). Keys, values and headers are `Bytes`
//! slices of that buffer.
//!
//! RecordBatch v2 layout, big-endian, with byte offsets:
//!
//! ```text
//!  0 baseOffset            int64
//!  8 batchLength           int32   bytes after this field
//! 12 partitionLeaderEpoch  int32
//! 16 magic                 int8    2
//! 17 crc                   uint32  CRC-32C of everything from attributes on
//! 21 attributes            int16   bits 0-2 codec, 3 log append time,
//!                                  4 transactional, 5 control
//! 23 lastOffsetDelta       int32
//! 27 baseTimestamp         int64
//! 35 maxTimestamp          int64
//! 43 producerId            int64
//! 51 producerEpoch         int16
//! 53 baseSequence          int32
//! 57 recordCount           int32
//! 61 records, possibly compressed as a whole
//! ```
//!
//! Each record is a zigzag varint length followed by: attributes (int8, unused),
//! timestampDelta (varlong), offsetDelta (varint), key length (varint, -1 for
//! null) and key, value length and value, header count (varint), and headers
//! (key length and key, value length (-1 for null) and value).

mod legacy;
mod varint;

#[cfg(test)]
mod tests;

use bytes::{Bytes, BytesMut};
use mz_ore::cast::ReinterpretCast;

use crate::internal::compression::{self, CodecError, Compression};
use crate::internal::records::varint::{put_varlong, read_varint, read_varlong, varlong_size};

const BASE_OFFSET: usize = 0;
const BATCH_LENGTH: usize = 8;
const PARTITION_LEADER_EPOCH: usize = 12;
const MAGIC: usize = 16;
const CRC: usize = 17;
const ATTRIBUTES: usize = 21;
const LAST_OFFSET_DELTA: usize = 23;
const BASE_TIMESTAMP: usize = 27;
const MAX_TIMESTAMP: usize = 35;
const PRODUCER_ID: usize = 43;
const PRODUCER_EPOCH: usize = 51;
const BASE_SEQUENCE: usize = 53;
const RECORD_COUNT: usize = 57;

/// The size of the RecordBatch v2 header.
pub(crate) const BATCH_HEADER_LEN: usize = 61;
/// The bytes of every entry (batch or legacy message) not counted by its
/// length field: the base offset and the length itself.
const LOG_OVERHEAD: usize = 12;

const ATTR_LOG_APPEND_TIME: i16 = 0x08;
const ATTR_TRANSACTIONAL: i16 = 0x10;
const ATTR_CONTROL: i16 = 0x20;

/// The decompression limit for control batches, which hold one small record.
const CONTROL_BATCH_MAX_DECOMPRESSED: usize = 1 << 20;

/// A record header to encode.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RecordHeader<'a> {
    pub(crate) key: &'a str,
    /// `None` encodes a null value, distinct from an empty one.
    pub(crate) value: Option<&'a [u8]>,
}

/// The producer id and epoch stamped on a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProducerIdentity {
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
}

impl ProducerIdentity {
    /// The identity of a producer that is neither idempotent nor
    /// transactional.
    pub(crate) const NONE: ProducerIdentity = ProducerIdentity {
        producer_id: -1,
        producer_epoch: -1,
    };
}

/// Accumulates records into a RecordBatch v2.
///
/// Records get offset deltas 0, 1, ... in append order, and timestamp deltas
/// relative to the first record's timestamp.
#[derive(Debug)]
pub(crate) struct BatchBuilder {
    compression: Compression,
    level: Option<i32>,
    transactional: bool,
    /// A placeholder for the batch header, followed by the encoded records.
    buf: Vec<u8>,
    record_count: i32,
    first_timestamp: i64,
    max_timestamp: i64,
}

impl BatchBuilder {
    #[cfg(test)]
    pub(crate) fn new(compression: Compression, level: Option<i32>, transactional: bool) -> Self {
        Self::with_capacity(compression, level, transactional, 0)
    }

    #[cfg(test)]
    pub(crate) fn with_capacity(
        compression: Compression,
        level: Option<i32>,
        transactional: bool,
        capacity: usize,
    ) -> Self {
        Self::with_buffer(compression, level, transactional, Vec::new(), capacity)
    }

    /// A builder for a batch compressed with `compression` at `level` (see
    /// [`compression::compress`]), encoding into `buf`, whose contents are
    /// discarded, with room for `capacity` bytes of encoded records.
    /// `transactional` sets the batch's transactional attribute.
    pub(crate) fn with_buffer(
        compression: Compression,
        level: Option<i32>,
        transactional: bool,
        mut buf: Vec<u8>,
        capacity: usize,
    ) -> Self {
        buf.clear();
        buf.reserve(BATCH_HEADER_LEN + capacity);
        buf.resize(BATCH_HEADER_LEN, 0);
        BatchBuilder {
            compression,
            level,
            transactional,
            buf,
            record_count: 0,
            first_timestamp: 0,
            max_timestamp: i64::MIN,
        }
    }

    /// The number of bytes [`BatchBuilder::append`] would add for this record.
    #[cfg(test)]
    pub(crate) fn encoded_record_size(
        &self,
        timestamp_ms: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader<'_>],
    ) -> usize {
        let body = self.body_size(timestamp_ms, key, value, headers);
        varlong_size(len_i64(body)) + body
    }

    /// Appends a record and returns its encoded size.
    ///
    /// `None` key, value or header value encode as null, distinct from empty.
    /// NOTE: a key, value or header larger than `i32::MAX` bytes produces a
    /// batch that brokers reject. Callers enforce `message.max.bytes` first.
    pub(crate) fn append(
        &mut self,
        timestamp_ms: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader<'_>],
    ) -> usize {
        let body = self.body_size(timestamp_ms, key, value, headers);
        let timestamp_delta = self.timestamp_delta(timestamp_ms);
        if self.record_count == 0 {
            self.first_timestamp = timestamp_ms;
        }
        self.max_timestamp = self.max_timestamp.max(timestamp_ms);

        let start = self.buf.len();
        let body_len = len_i64(body);
        self.buf.reserve(varlong_size(body_len) + body);
        put_varlong(&mut self.buf, body_len);
        // Record attributes are unused.
        self.buf.push(0);
        put_varlong(&mut self.buf, timestamp_delta);
        put_varlong(&mut self.buf, i64::from(self.record_count));
        put_bytes(&mut self.buf, key);
        put_bytes(&mut self.buf, value);
        put_varlong(&mut self.buf, len_i64(headers.len()));
        for header in headers {
            put_bytes(&mut self.buf, Some(header.key.as_bytes()));
            put_bytes(&mut self.buf, header.value);
        }
        self.record_count += 1;
        self.buf.len() - start
    }

    #[cfg(test)]
    pub(crate) fn record_count(&self) -> i32 {
        self.record_count
    }

    /// The size of the batch if it were built uncompressed now, including the
    /// batch header.
    pub(crate) fn uncompressed_size(&self) -> usize {
        self.buf.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// The bytes that can be appended without reallocating the buffer.
    pub(crate) fn spare_capacity(&self) -> usize {
        self.buf.capacity() - self.buf.len()
    }

    /// Moves the batch into `buf`, whose contents are discarded, and returns
    /// the previous buffer for reuse.
    pub(crate) fn move_to(&mut self, mut buf: Vec<u8>) -> Vec<u8> {
        buf.clear();
        buf.extend_from_slice(&self.buf);
        std::mem::replace(&mut self.buf, buf)
    }

    /// Decodes the records appended so far, with offsets from 0. Copies the
    /// encoded records once.
    pub(crate) fn records(&self) -> RecordIter {
        RecordIter(RecordIterInner::V2(V2Records {
            buf: Bytes::copy_from_slice(&self.buf[BATCH_HEADER_LEN..]),
            pos: 0,
            remaining: usize::try_from(self.record_count).expect("non-negative count"),
            base_offset: 0,
            base_timestamp: self.first_timestamp,
            log_append_time: false,
            max_timestamp: self.max_timestamp,
        }))
    }

    #[cfg(test)]
    pub(crate) fn build(
        self,
        ident: ProducerIdentity,
        base_sequence: i32,
    ) -> Result<EncodedBatch, CodecError> {
        self.build_reusing(ident, base_sequence, None)
            .map(|(batch, _)| batch)
    }

    /// Finishes the batch: compresses the records and fills in the header.
    ///
    /// The base offset is 0 and the partition leader epoch -1, which brokers
    /// overwrite. Like librdkafka, the batch is sent uncompressed if compression
    /// makes it larger. Fails for an empty batch.
    ///
    /// Compresses into `scratch` (whose contents are discarded) if given, and
    /// returns the buffer the batch does not use, for reuse: the uncompressed
    /// records, or the scratch buffer if the batch is not compressed.
    pub(crate) fn build_reusing(
        self,
        ident: ProducerIdentity,
        base_sequence: i32,
        scratch: Option<Vec<u8>>,
    ) -> Result<(EncodedBatch, Option<Vec<u8>>), CodecError> {
        if self.record_count == 0 {
            return Err(CodecError::EmptyBatch);
        }
        let mut buf = self.buf;
        let mut spare = scratch;
        let mut compression = self.compression;
        if compression != Compression::None {
            let records = &buf[BATCH_HEADER_LEN..];
            let mut compressed = spare.take().unwrap_or_default();
            compressed.clear();
            compressed.reserve(BATCH_HEADER_LEN + records.len() / 2);
            compressed.resize(BATCH_HEADER_LEN, 0);
            compression::compress(compression, self.level, records, &mut compressed)?;
            if compressed.len() > buf.len() {
                compression = Compression::None;
                spare = Some(compressed);
            } else {
                spare = Some(std::mem::replace(&mut buf, compressed));
            }
        }
        let batch_length = i32::try_from(buf.len() - LOG_OVERHEAD)
            .map_err(|_| CodecError::malformed("record batch larger than 2 GiB"))?;
        let mut attributes = compression.attribute_bits();
        if self.transactional {
            attributes |= ATTR_TRANSACTIONAL;
        }
        put_i64(&mut buf, BASE_OFFSET, 0);
        put_i32(&mut buf, BATCH_LENGTH, batch_length);
        put_i32(&mut buf, PARTITION_LEADER_EPOCH, -1);
        buf[MAGIC] = 2;
        put_i16(&mut buf, ATTRIBUTES, attributes);
        put_i32(&mut buf, LAST_OFFSET_DELTA, self.record_count - 1);
        put_i64(&mut buf, BASE_TIMESTAMP, self.first_timestamp);
        put_i64(&mut buf, MAX_TIMESTAMP, self.max_timestamp);
        put_i32(&mut buf, RECORD_COUNT, self.record_count);
        stamp(&mut buf, ident, base_sequence);
        let batch = EncodedBatch {
            data: Bytes::from(buf),
        };
        Ok((batch, spare))
    }

    fn timestamp_delta(&self, timestamp_ms: i64) -> i64 {
        if self.record_count == 0 {
            0
        } else {
            timestamp_ms.wrapping_sub(self.first_timestamp)
        }
    }

    /// The size of a record after its length prefix.
    fn body_size(
        &self,
        timestamp_ms: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader<'_>],
    ) -> usize {
        let headers_size: usize = headers
            .iter()
            .map(|h| bytes_size(Some(h.key.as_bytes())) + bytes_size(h.value))
            .sum();
        1 + varlong_size(self.timestamp_delta(timestamp_ms))
            + varlong_size(i64::from(self.record_count))
            + bytes_size(key)
            + bytes_size(value)
            + varlong_size(len_i64(headers.len()))
            + headers_size
    }
}

/// A length as a varlong value. Slices never exceed `isize::MAX` bytes.
fn len_i64(len: usize) -> i64 {
    i64::try_from(len).expect("slice lengths fit in i64")
}

fn bytes_size(b: Option<&[u8]>) -> usize {
    match b {
        None => varlong_size(-1),
        Some(b) => varlong_size(len_i64(b.len())) + b.len(),
    }
}

fn put_bytes(buf: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        None => put_varlong(buf, -1),
        Some(b) => {
            put_varlong(buf, len_i64(b.len()));
            buf.extend_from_slice(b);
        }
    }
}

/// Writes the producer fields and the CRC that covers them.
fn stamp(buf: &mut [u8], ident: ProducerIdentity, base_sequence: i32) {
    put_i64(buf, PRODUCER_ID, ident.producer_id);
    put_i16(buf, PRODUCER_EPOCH, ident.producer_epoch);
    put_i32(buf, BASE_SEQUENCE, base_sequence);
    let crc = crc32c::crc32c(&buf[ATTRIBUTES..]);
    buf[CRC..CRC + 4].copy_from_slice(&crc.to_be_bytes());
}

/// A finished RecordBatch v2, ready to be sent in a ProduceRequest.
#[derive(Clone, Debug)]
pub(crate) struct EncodedBatch {
    data: Bytes,
}

impl EncodedBatch {
    /// The encoded batch. Cloning `Bytes` is cheap.
    pub(crate) fn bytes(&self) -> Bytes {
        self.data.clone()
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    #[cfg(test)]
    pub(crate) fn record_count(&self) -> i32 {
        get_i32(&self.data, RECORD_COUNT)
    }

    /// The codec actually used, which is `None` if compression did not help.
    #[cfg(test)]
    pub(crate) fn compression(&self) -> Compression {
        Compression::from_attributes(get_i16(&self.data, ATTRIBUTES)).expect("valid codec")
    }

    pub(crate) fn base_sequence(&self) -> i32 {
        get_i32(&self.data, BASE_SEQUENCE)
    }

    /// The sequence number of the last record, wrapping like Kafka's.
    #[cfg(test)]
    pub(crate) fn last_sequence(&self) -> i32 {
        increment_sequence(self.base_sequence(), self.record_count() - 1)
    }

    pub(crate) fn producer_identity(&self) -> ProducerIdentity {
        ProducerIdentity {
            producer_id: get_i64(&self.data, PRODUCER_ID),
            producer_epoch: get_i16(&self.data, PRODUCER_EPOCH),
        }
    }

    /// The batch's buffer, for reuse, unless `Bytes` previously returned by
    /// [`EncodedBatch::bytes`] are still alive.
    pub(crate) fn into_buffer(self) -> Option<Vec<u8>> {
        self.data.try_into_mut().ok().map(Vec::from)
    }

    /// Rewrites the producer id, epoch and base sequence, and the CRC,
    /// without re-encoding the records.
    ///
    /// `Bytes` previously returned by [`EncodedBatch::bytes`] keep the old
    /// contents: if any are still alive, the batch is copied first.
    pub(crate) fn restamp(&mut self, ident: ProducerIdentity, base_sequence: i32) {
        let data = std::mem::take(&mut self.data);
        let mut buf = data
            .try_into_mut()
            .unwrap_or_else(|shared| BytesMut::from(&shared[..]));
        stamp(&mut buf, ident, base_sequence);
        self.data = buf.freeze();
    }
}

/// Adds `delta` to a producer sequence number. Sequence numbers wrap from
/// `i32::MAX` to 0, as in Kafka's `DefaultRecordBatch.incrementSequence`.
pub(crate) fn increment_sequence(sequence: i32, delta: i32) -> i32 {
    if sequence > i32::MAX - delta {
        delta - (i32::MAX - sequence) - 1
    } else {
        sequence + delta
    }
}

/// How a batch's timestamps were assigned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TimestampType {
    CreateTime,
    LogAppendTime,
    NotAvailable,
}

/// The header of a fetched batch (or legacy message), parsed without
/// decompressing anything.
///
/// For a legacy (magic 0 or 1) entry: the producer fields are -1, the flags
/// false, and the timestamps the message's (-1 for magic 0). A compressed
/// legacy wrapper message only reveals its last offset (its own offset), so
/// its `base_offset` equals `last_offset` and its `record_count` is -1.
/// [`FetchedBatch::records`] yields exact offsets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BatchHeader {
    pub(crate) base_offset: i64,
    /// The offset after which the next fetch continues. NOTE: for compacted
    /// batches this can be beyond the offset of the last remaining record, and
    /// a batch can have no records at all.
    pub(crate) last_offset: i64,
    pub(crate) partition_leader_epoch: i32,
    pub(crate) magic: i8,
    pub(crate) compression: Compression,
    pub(crate) timestamp_type: TimestampType,
    pub(crate) is_transactional: bool,
    pub(crate) is_control: bool,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) base_sequence: i32,
    /// The base timestamp. NOTE: when the log cleaner sets a delete horizon
    /// (KIP-534, attribute bit 6) this holds the horizon, not a record's
    /// timestamp. Record timestamps are unaffected.
    pub(crate) first_timestamp: i64,
    pub(crate) max_timestamp: i64,
    pub(crate) record_count: i32,
    /// The size of the entry on the wire, including base offset and length.
    pub(crate) size: usize,
}

/// One entry of a fetched record set: a RecordBatch v2 or a legacy message.
#[derive(Clone, Debug)]
pub(crate) struct FetchedBatch {
    header: BatchHeader,
    /// The whole entry, starting at its base offset.
    data: Bytes,
}

impl FetchedBatch {
    pub(crate) fn header(&self) -> &BatchHeader {
        &self.header
    }

    /// The entry's bytes as fetched.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> &Bytes {
        &self.data
    }

    /// Decompresses the entry, if needed, and returns an iterator over its
    /// records.
    ///
    /// Decompression fails with [`CodecError::TooLarge`] beyond
    /// `max_decompressed` bytes. Records come in the batch's order and may
    /// have offsets below the offset the fetch asked for: consumers skip
    /// those. Control batches yield their control records.
    pub(crate) fn records(&self, max_decompressed: usize) -> Result<RecordIter, CodecError> {
        if self.header.magic < 2 {
            let records = legacy::records(&self.data, max_decompressed)?;
            return Ok(RecordIter(RecordIterInner::Legacy(records.into_iter())));
        }
        let payload = self.data.slice(BATCH_HEADER_LEN..);
        let buf = match self.header.compression {
            Compression::None => payload,
            c => Bytes::from(compression::decompress(c, &payload, max_decompressed)?),
        };
        let remaining = usize::try_from(self.header.record_count)
            .map_err(|_| CodecError::malformed("negative record count"))?;
        Ok(RecordIter(RecordIterInner::V2(V2Records {
            buf,
            pos: 0,
            remaining,
            base_offset: self.header.base_offset,
            base_timestamp: self.header.first_timestamp,
            log_append_time: self.header.timestamp_type == TimestampType::LogAppendTime,
            max_timestamp: self.header.max_timestamp,
        })))
    }

    /// The control record of a control batch, or `None` for a data batch (or
    /// a control batch emptied by compaction).
    pub(crate) fn control_record(&self) -> Result<Option<ControlRecord>, CodecError> {
        if !self.header.is_control {
            return Ok(None);
        }
        let Some(record) = self.records(CONTROL_BATCH_MAX_DECOMPRESSED)?.next() else {
            return Ok(None);
        };
        let key = record?
            .key
            .ok_or_else(|| CodecError::malformed("control record without key"))?;
        if key.len() < 4 {
            return Err(CodecError::malformed(
                "control record key shorter than 4 bytes",
            ));
        }
        let version = get_i16(&key, 0);
        let kind = get_i16(&key, 2);
        // Like the Java client (and unlike librdkafka, which ignores records
        // with a version other than 0), the type is read for any non-negative
        // version: later versions only append fields.
        if version < 0 {
            return Err(CodecError::malformed("negative control record version"));
        }
        let kind = match kind {
            0 => ControlRecordKind::Abort,
            1 => ControlRecordKind::Commit,
            other => ControlRecordKind::Unknown(other),
        };
        Ok(Some(ControlRecord { version, kind }))
    }

    /// Checks the entry's stored checksum: CRC-32C for RecordBatch v2, CRC-32
    /// for a legacy message.
    ///
    /// For a compressed legacy wrapper this checks the wrapper, whose checksum
    /// covers the compressed inner messages.
    pub(crate) fn verify_crc(&self) -> Result<(), CodecError> {
        let (stored, computed) = if self.header.magic < 2 {
            legacy::crc(&self.data)
        } else {
            (
                u32::reinterpret_cast(get_i32(&self.data, CRC)),
                crc32c::crc32c(&self.data[ATTRIBUTES..]),
            )
        };
        if stored == computed {
            Ok(())
        } else {
            Err(CodecError::CrcMismatch { stored, computed })
        }
    }
}

/// The marker a transaction coordinator writes to end a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ControlRecord {
    pub(crate) version: i16,
    pub(crate) kind: ControlRecordKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlRecordKind {
    Abort,
    Commit,
    Unknown(i16),
}

/// Splits a fetched record set into its entries, reading only their headers.
///
/// Iteration ends at the first incomplete entry: brokers may cut off the last
/// batch of a fetch response. An entry with an unsupported magic byte or
/// invalid header yields an error and iteration continues after it, unless
/// its length field is negative, which ends iteration.
pub(crate) fn parse_batches(data: Bytes) -> BatchIter {
    BatchIter {
        data,
        pos: 0,
        done: false,
    }
}

/// The iterator returned by [`parse_batches`].
#[derive(Debug)]
pub(crate) struct BatchIter {
    data: Bytes,
    pos: usize,
    done: bool,
}

impl BatchIter {
    /// The number of trailing bytes that do not form a complete entry, once
    /// the iterator is exhausted. Nonzero with no entry yielded means a single
    /// batch was larger than the fetch size.
    pub(crate) fn truncated_bytes(&self) -> usize {
        if self.done {
            self.data.len() - self.pos
        } else {
            0
        }
    }
}

impl Iterator for BatchIter {
    type Item = Result<FetchedBatch, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let rest = &self.data[self.pos..];
        // The magic byte sits at the same position in all message formats.
        if rest.len() <= MAGIC {
            self.done = true;
            return None;
        }
        let Ok(length) = usize::try_from(get_i32(rest, BATCH_LENGTH)) else {
            self.done = true;
            return Some(Err(CodecError::malformed(format!(
                "negative record batch length at offset {}",
                get_i64(rest, BASE_OFFSET)
            ))));
        };
        let size = LOG_OVERHEAD + length;
        if size > rest.len() {
            self.done = true;
            return None;
        }
        let magic = i8::reinterpret_cast(rest[MAGIC]);
        let entry = self.data.slice(self.pos..self.pos + size);
        self.pos += size;
        Some(match magic {
            2 => parse_v2_header(entry),
            0 | 1 => legacy::parse_header(entry),
            magic => Err(CodecError::UnsupportedMagic {
                magic,
                offset: get_i64(&entry, BASE_OFFSET),
            }),
        })
    }
}

fn parse_v2_header(data: Bytes) -> Result<FetchedBatch, CodecError> {
    let base_offset = get_i64(&data, BASE_OFFSET);
    if data.len() < BATCH_HEADER_LEN {
        return Err(CodecError::malformed(format!(
            "record batch at offset {base_offset} is shorter than its header"
        )));
    }
    let attributes = get_i16(&data, ATTRIBUTES);
    let last_offset = base_offset
        .checked_add(i64::from(get_i32(&data, LAST_OFFSET_DELTA)))
        .ok_or_else(|| CodecError::malformed("last offset out of range"))?;
    let timestamp_type = if attributes & ATTR_LOG_APPEND_TIME != 0 {
        TimestampType::LogAppendTime
    } else {
        TimestampType::CreateTime
    };
    let header = BatchHeader {
        base_offset,
        last_offset,
        partition_leader_epoch: get_i32(&data, PARTITION_LEADER_EPOCH),
        magic: 2,
        compression: Compression::from_attributes(attributes)?,
        timestamp_type,
        is_transactional: attributes & ATTR_TRANSACTIONAL != 0,
        is_control: attributes & ATTR_CONTROL != 0,
        producer_id: get_i64(&data, PRODUCER_ID),
        producer_epoch: get_i16(&data, PRODUCER_EPOCH),
        base_sequence: get_i32(&data, BASE_SEQUENCE),
        first_timestamp: get_i64(&data, BASE_TIMESTAMP),
        max_timestamp: get_i64(&data, MAX_TIMESTAMP),
        record_count: get_i32(&data, RECORD_COUNT),
        size: data.len(),
    };
    Ok(FetchedBatch { header, data })
}

/// A decoded record. Keys, values and headers share the batch's buffer.
#[derive(Clone, Debug)]
pub(crate) struct Record {
    pub(crate) offset: i64,
    /// For log append time batches, the batch's max timestamp, as in Kafka.
    /// -1 when the type is [`TimestampType::NotAvailable`].
    pub(crate) timestamp: i64,
    pub(crate) timestamp_type: TimestampType,
    pub(crate) key: Option<Bytes>,
    pub(crate) value: Option<Bytes>,
    pub(crate) headers: RecordHeaders,
}

/// A record's headers, parsed only when iterated.
#[derive(Clone, Debug, Default)]
pub(crate) struct RecordHeaders {
    count: usize,
    data: Bytes,
}

impl RecordHeaders {
    /// The header count the record declares.
    pub(crate) fn len(&self) -> usize {
        self.count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The declared header count and the encoded headers that follow it,
    /// sharing the batch's buffer.
    pub(crate) fn into_parts(self) -> (usize, Bytes) {
        (self.count, self.data)
    }

    /// Iterates over `(key, value)` pairs, with `None` for a null value.
    /// Malformed header data yields one error, then the iterator ends.
    pub(crate) fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            data: &self.data,
            pos: 0,
            remaining: self.count,
        }
    }
}

/// The iterator returned by [`RecordHeaders::iter`].
#[derive(Debug)]
pub(crate) struct HeaderIter<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> HeaderIter<'a> {
    fn parse(&mut self) -> Result<(&'a [u8], Option<&'a [u8]>), CodecError> {
        let data = self.data;
        let key_len = usize::try_from(read_varint(data, &mut self.pos)?)
            .map_err(|_| CodecError::malformed("negative header key length"))?;
        let key = take(data, &mut self.pos, key_len)?;
        let value_len = read_varint(data, &mut self.pos)?;
        let value = match usize::try_from(value_len) {
            Ok(len) => Some(take(data, &mut self.pos, len)?),
            Err(_) => None,
        };
        Ok((key, value))
    }
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = Result<(&'a [u8], Option<&'a [u8]>), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            if self.pos < self.data.len() {
                self.pos = self.data.len();
                return Some(Err(CodecError::malformed(
                    "bytes after the record's last header",
                )));
            }
            return None;
        }
        self.remaining -= 1;
        let result = self.parse();
        if result.is_err() {
            self.remaining = 0;
            self.pos = self.data.len();
        }
        Some(result)
    }
}

/// The iterator returned by [`FetchedBatch::records`].
///
/// Malformed record data yields one error, then the iterator ends.
#[derive(Debug)]
pub(crate) struct RecordIter(RecordIterInner);

#[derive(Debug)]
enum RecordIterInner {
    V2(V2Records),
    Legacy(std::vec::IntoIter<Record>),
}

impl Iterator for RecordIter {
    type Item = Result<Record, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            RecordIterInner::V2(records) => records.next(),
            RecordIterInner::Legacy(records) => records.next().map(Ok),
        }
    }
}

#[derive(Debug)]
struct V2Records {
    /// The batch's (decompressed) records.
    buf: Bytes,
    pos: usize,
    remaining: usize,
    base_offset: i64,
    base_timestamp: i64,
    log_append_time: bool,
    max_timestamp: i64,
}

impl V2Records {
    fn next(&mut self) -> Option<Result<Record, CodecError>> {
        if self.remaining == 0 {
            // Like the Java client, reject a batch whose count does not
            // account for all its bytes.
            if self.pos < self.buf.len() {
                self.pos = self.buf.len();
                return Some(Err(CodecError::malformed(
                    "bytes after the batch's last record",
                )));
            }
            return None;
        }
        self.remaining -= 1;
        let result = self.parse();
        if result.is_err() {
            self.remaining = 0;
            self.pos = self.buf.len();
        }
        Some(result)
    }

    fn parse(&mut self) -> Result<Record, CodecError> {
        let mut pos = self.pos;
        let len = usize::try_from(read_varint(&self.buf, &mut pos)?)
            .map_err(|_| CodecError::malformed("negative record length"))?;
        let end = pos
            .checked_add(len)
            .filter(|end| *end <= self.buf.len())
            .ok_or_else(|| CodecError::malformed("record extends past the end of its batch"))?;
        // Reads below are confined to this record.
        let body = &self.buf[..end];
        let Some(&attributes) = body.get(pos) else {
            return Err(CodecError::malformed("truncated record"));
        };
        pos += 1;
        let timestamp_delta = read_varlong(body, &mut pos)?;
        let offset_delta = read_varint(body, &mut pos)?;
        let key = read_nullable_bytes(&self.buf, end, &mut pos)?;
        let value = read_nullable_bytes(&self.buf, end, &mut pos)?;
        let header_count = usize::try_from(read_varint(body, &mut pos)?)
            .map_err(|_| CodecError::malformed("negative header count"))?;
        let headers = if header_count == 0 {
            if pos != end {
                return Err(CodecError::malformed("bytes after the record's value"));
            }
            RecordHeaders::default()
        } else {
            RecordHeaders {
                count: header_count,
                data: self.buf.slice(pos..end),
            }
        };
        self.pos = end;

        let offset = self
            .base_offset
            .checked_add(i64::from(offset_delta))
            .ok_or_else(|| CodecError::malformed("record offset out of range"))?;
        // librdkafka also honors the (otherwise unused) log append time bit
        // in the record attributes.
        let (timestamp, timestamp_type) =
            if self.log_append_time || i16::from(attributes) & ATTR_LOG_APPEND_TIME != 0 {
                (self.max_timestamp, TimestampType::LogAppendTime)
            } else {
                (
                    self.base_timestamp.wrapping_add(timestamp_delta),
                    TimestampType::CreateTime,
                )
            };
        Ok(Record {
            offset,
            timestamp,
            timestamp_type,
            key,
            value,
            headers,
        })
    }
}

/// Reads a varint length and that many bytes from `buf[..end]`. Like the Java
/// client, any negative length means null.
fn read_nullable_bytes(
    buf: &Bytes,
    end: usize,
    pos: &mut usize,
) -> Result<Option<Bytes>, CodecError> {
    let Ok(len) = usize::try_from(read_varint(&buf[..end], pos)?) else {
        return Ok(None);
    };
    let start = *pos;
    take(&buf[..end], pos, len)?;
    Ok(Some(buf.slice(start..*pos)))
}

/// Returns the next `len` bytes of `data` at `*pos` and advances past them.
fn take<'a>(data: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8], CodecError> {
    let start = *pos;
    let end = start
        .checked_add(len)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| CodecError::malformed("length extends past the end of the record"))?;
    *pos = end;
    Ok(&data[start..end])
}

// Fixed-width big-endian accessors. Callers check that `at` is in bounds.

fn get_i16(b: &[u8], at: usize) -> i16 {
    i16::from_be_bytes(b[at..at + 2].try_into().expect("2 bytes"))
}

fn get_i32(b: &[u8], at: usize) -> i32 {
    i32::from_be_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

fn get_i64(b: &[u8], at: usize) -> i64 {
    i64::from_be_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

fn put_i16(b: &mut [u8], at: usize, v: i16) {
    b[at..at + 2].copy_from_slice(&v.to_be_bytes());
}

fn put_i32(b: &mut [u8], at: usize, v: i32) {
    b[at..at + 4].copy_from_slice(&v.to_be_bytes());
}

fn put_i64(b: &mut [u8], at: usize, v: i64) {
    b[at..at + 8].copy_from_slice(&v.to_be_bytes());
}
