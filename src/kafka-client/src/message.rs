// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Store and manipulate Kafka messages.

use std::fmt;
use std::marker::PhantomData;
use std::ops::Range;
use std::str;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::KafkaError;
use crate::util::millis_to_epoch;

/// Timestamp of a Kafka message.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Timestamp {
    /// Timestamp not available.
    NotAvailable,
    /// Message creation time.
    CreateTime(i64),
    /// Log append time.
    LogAppendTime(i64),
}

impl Timestamp {
    /// Converts the timestamp to milliseconds since the epoch, or `None` if not
    /// available. A value of `-1` is "not available" too.
    pub fn to_millis(self) -> Option<i64> {
        match self {
            Timestamp::NotAvailable | Timestamp::CreateTime(-1) | Timestamp::LogAppendTime(-1) => {
                None
            }
            Timestamp::CreateTime(t) | Timestamp::LogAppendTime(t) => Some(t),
        }
    }

    /// Creates a new `Timestamp::CreateTime` representing the current time.
    pub fn now() -> Timestamp {
        Timestamp::from(SystemTime::now())
    }
}

impl From<i64> for Timestamp {
    fn from(system_time: i64) -> Timestamp {
        Timestamp::CreateTime(system_time)
    }
}

impl From<SystemTime> for Timestamp {
    fn from(system_time: SystemTime) -> Timestamp {
        Timestamp::CreateTime(millis_to_epoch(system_time))
    }
}

/// A generic representation of Kafka message headers.
pub trait Headers {
    /// Returns the number of contained headers.
    fn count(&self) -> usize;

    /// Gets the specified header, where the first header corresponds to
    /// index 0.
    ///
    /// Panics if the index is out of bounds.
    fn get(&self, idx: usize) -> Header<'_, &[u8]> {
        self.try_get(idx).unwrap_or_else(|| {
            panic!(
                "headers index out of bounds: the count is {} but the index is {}",
                self.count(),
                idx,
            )
        })
    }

    /// Like [`Headers::get`], but the value of the header will be converted
    /// to the specified type.
    ///
    /// Panics if the index is out of bounds.
    fn get_as<V>(&self, idx: usize) -> Result<Header<'_, &V>, V::Error>
    where
        V: FromBytes + ?Sized,
    {
        self.try_get_as(idx).unwrap_or_else(|| {
            panic!(
                "headers index out of bounds: the count is {} but the index is {}",
                self.count(),
                idx,
            )
        })
    }

    /// Like [`Headers::get`], but returns an option if the header is out of
    /// bounds rather than panicking.
    fn try_get(&self, idx: usize) -> Option<Header<'_, &[u8]>>;

    /// Like [`Headers::get_as`], but returns an option if the header is out
    /// of bounds rather than panicking.
    fn try_get_as<V>(&self, idx: usize) -> Option<Result<Header<'_, &V>, V::Error>>
    where
        V: FromBytes + ?Sized,
    {
        self.try_get(idx).map(|header| header.parse())
    }

    /// Iterates over all headers in order.
    fn iter(&self) -> HeadersIter<'_, Self>
    where
        Self: Sized,
    {
        HeadersIter {
            headers: self,
            index: 0,
        }
    }
}

/// A Kafka message header.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Header<'a, V> {
    /// The header's key.
    pub key: &'a str,
    /// The header's value. `None` is a null value, distinct from an empty one.
    pub value: Option<V>,
}

impl<'a> Header<'a, &'a [u8]> {
    fn parse<V>(&self) -> Result<Header<'a, &'a V>, V::Error>
    where
        V: FromBytes + ?Sized,
    {
        Ok(Header {
            key: self.key,
            value: self.value.map(V::from_bytes).transpose()?,
        })
    }
}

/// An iterator over [`Headers`].
pub struct HeadersIter<'a, H> {
    headers: &'a H,
    index: usize,
}

impl<'a, H> Iterator for HeadersIter<'a, H>
where
    H: Headers,
{
    type Item = Header<'a, &'a [u8]>;

    fn next(&mut self) -> Option<Header<'a, &'a [u8]>> {
        let item = self.headers.try_get(self.index)?;
        self.index += 1;
        Some(item)
    }
}

/// A generic representation of a Kafka message.
///
/// Only read-only methods are provided by this trait, as the underlying storage
/// might not allow modification.
pub trait Message {
    /// The type of headers that this message contains.
    type Headers: Headers;

    /// Returns the key of the message, or `None` if there is no key.
    fn key(&self) -> Option<&[u8]>;

    /// Returns the payload of the message, or `None` if there is no payload.
    fn payload(&self) -> Option<&[u8]>;

    /// Returns the source topic of the message.
    fn topic(&self) -> &str;

    /// Returns the partition number where the message is stored.
    fn partition(&self) -> i32;

    /// Returns the offset of the message within the partition.
    fn offset(&self) -> i64;

    /// Returns the message timestamp.
    fn timestamp(&self) -> Timestamp;

    /// Converts the raw bytes of the payload to a reference of the specified
    /// type, that points to the same data inside the message and without
    /// performing any memory allocation.
    fn payload_view<P: ?Sized + FromBytes>(&self) -> Option<Result<&P, P::Error>> {
        self.payload().map(P::from_bytes)
    }

    /// Converts the raw bytes of the key to a reference of the specified type,
    /// that points to the same data inside the message and without performing
    /// any memory allocation.
    fn key_view<K: ?Sized + FromBytes>(&self) -> Option<Result<&K, K::Error>> {
        self.key().map(K::from_bytes)
    }

    /// Returns the headers of the message, or `None` if there are no headers.
    fn headers(&self) -> Option<&Self::Headers>;
}

/// Location of one header inside a [`BorrowedHeaders`] buffer.
#[derive(Debug, Clone)]
struct HeaderSpan {
    key: Range<usize>,
    value: Option<Range<usize>>,
}

/// The headers of a [`BorrowedMessage`].
///
/// A view over the header section of a Kafka v2 record (a varint count followed
/// by `varint key length, key, varint value length, value` entries). It shares
/// the fetched buffer and is only indexed when first accessed, so a message
/// whose headers are never read costs no allocation.
pub struct BorrowedHeaders {
    raw: Bytes,
    count: usize,
    index: OnceLock<Box<[HeaderSpan]>>,
}

/// Why a record's header section could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeaderDecodeError {
    /// The section ended in the middle of a header.
    Truncated,
    /// A count or length is negative (other than a null value) or too large.
    InvalidLength,
    /// A header key is not valid UTF-8.
    InvalidKey,
    /// Bytes remain after the last header.
    TrailingBytes,
}

impl BorrowedHeaders {
    /// Validates a record's header section and wraps it without copying.
    ///
    /// `raw` must hold exactly the header section, starting with the header
    /// count. Returns `None` when the record has zero headers, which is what
    /// [`Message::headers`] reports for such records.
    pub(crate) fn from_record_bytes(
        raw: Bytes,
    ) -> Result<Option<BorrowedHeaders>, HeaderDecodeError> {
        let mut count = 0;
        walk_headers(&raw, |_| count += 1)?;
        if count == 0 {
            return Ok(None);
        }
        Ok(Some(BorrowedHeaders {
            raw,
            count,
            index: OnceLock::new(),
        }))
    }

    /// Encodes `headers` in record format.
    ///
    /// For messages that originate from this process, such as delivery reports.
    pub(crate) fn from_owned(headers: &OwnedHeaders) -> Option<BorrowedHeaders> {
        if headers.count() == 0 {
            return None;
        }
        let mut buf = BytesMut::new();
        put_varint(
            &mut buf,
            i64::try_from(headers.count()).expect("count fits i64"),
        );
        for header in headers.iter() {
            put_varint(
                &mut buf,
                i64::try_from(header.key.len()).expect("len fits i64"),
            );
            buf.put_slice(header.key.as_bytes());
            match header.value {
                None => put_varint(&mut buf, -1),
                Some(value) => {
                    put_varint(&mut buf, i64::try_from(value.len()).expect("len fits i64"));
                    buf.put_slice(value);
                }
            }
        }
        BorrowedHeaders::from_record_bytes(buf.freeze()).expect("encoded headers are well formed")
    }

    fn spans(&self) -> &[HeaderSpan] {
        self.index.get_or_init(|| {
            let mut spans = Vec::with_capacity(self.count);
            walk_headers(&self.raw, |span| spans.push(span))
                .expect("headers are validated on construction");
            spans.into_boxed_slice()
        })
    }

    /// Clones the content of `BorrowedHeaders` and returns an [`OwnedHeaders`]
    /// that can outlive the consumer.
    pub fn detach(&self) -> OwnedHeaders {
        let mut owned = OwnedHeaders::new_with_capacity(self.count);
        for header in self.iter() {
            owned = owned.insert(header);
        }
        owned
    }
}

impl Headers for BorrowedHeaders {
    fn count(&self) -> usize {
        self.count
    }

    fn try_get(&self, idx: usize) -> Option<Header<'_, &[u8]>> {
        let span = self.spans().get(idx)?;
        let key = str::from_utf8(&self.raw[span.key.clone()])
            .expect("header keys are validated as UTF-8 on construction");
        Some(Header {
            key,
            value: span.value.clone().map(|range| &self.raw[range]),
        })
    }
}

impl fmt::Debug for BorrowedHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// Validates a record header section, calling `f` for every header.
fn walk_headers(raw: &[u8], mut f: impl FnMut(HeaderSpan)) -> Result<(), HeaderDecodeError> {
    let mut pos = 0;
    let count = read_varint(raw, &mut pos)?;
    let count = usize::try_from(count).map_err(|_| HeaderDecodeError::InvalidLength)?;
    for _ in 0..count {
        let key_len = read_varint(raw, &mut pos)?;
        let key_len = usize::try_from(key_len).map_err(|_| HeaderDecodeError::InvalidLength)?;
        let key = take(raw, &mut pos, key_len)?;
        if str::from_utf8(&raw[key.clone()]).is_err() {
            return Err(HeaderDecodeError::InvalidKey);
        }
        let value_len = read_varint(raw, &mut pos)?;
        let value = if value_len == -1 {
            None
        } else {
            let value_len =
                usize::try_from(value_len).map_err(|_| HeaderDecodeError::InvalidLength)?;
            Some(take(raw, &mut pos, value_len)?)
        };
        f(HeaderSpan { key, value });
    }
    if pos != raw.len() {
        return Err(HeaderDecodeError::TrailingBytes);
    }
    Ok(())
}

fn take(raw: &[u8], pos: &mut usize, len: usize) -> Result<Range<usize>, HeaderDecodeError> {
    let end = pos
        .checked_add(len)
        .filter(|end| *end <= raw.len())
        .ok_or(HeaderDecodeError::Truncated)?;
    let range = *pos..end;
    *pos = end;
    Ok(range)
}

/// Reads a zigzag-encoded varint as used by Kafka v2 records.
fn read_varint(raw: &[u8], pos: &mut usize) -> Result<i64, HeaderDecodeError> {
    let mut value: u64 = 0;
    for shift in (0..64).step_by(7) {
        let byte = *raw.get(*pos).ok_or(HeaderDecodeError::Truncated)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            let magnitude = i64::try_from(value >> 1).expect("u64 >> 1 fits i64");
            return Ok(if value & 1 == 0 {
                magnitude
            } else {
                -magnitude - 1
            });
        }
    }
    Err(HeaderDecodeError::InvalidLength)
}

fn put_varint(buf: &mut BytesMut, value: i64) {
    let zigzag = (value << 1) ^ (value >> 63);
    let mut v = u64::from_ne_bytes(zigzag.to_ne_bytes());
    while v >= 0x80 {
        buf.put_u8(u8::try_from(v & 0x7f).expect("masked to 7 bits") | 0x80);
        v >>= 7;
    }
    buf.put_u8(u8::try_from(v).expect("less than 0x80"));
}

/// A zero-copy Kafka message.
///
/// The content of the message is shared with the buffer it was fetched into.
/// The lifetime ties the message to the consumer that returned it, as in
/// rust-rdkafka, although the message keeps its buffer alive on its own.
pub struct BorrowedMessage<'a> {
    topic: Arc<str>,
    partition: i32,
    offset: i64,
    timestamp: Timestamp,
    key: Option<Bytes>,
    payload: Option<Bytes>,
    headers: Option<BorrowedHeaders>,
    _owner: PhantomData<&'a ()>,
}

impl<'a> BorrowedMessage<'a> {
    /// Creates a message from decoded record parts.
    ///
    /// `None` key or payload is a Kafka null, distinct from an empty value.
    /// `headers` must be `None` when the record has no headers.
    pub(crate) fn new(
        topic: Arc<str>,
        partition: i32,
        offset: i64,
        timestamp: Timestamp,
        key: Option<Bytes>,
        payload: Option<Bytes>,
        headers: Option<BorrowedHeaders>,
    ) -> BorrowedMessage<'a> {
        BorrowedMessage {
            topic,
            partition,
            offset,
            timestamp,
            key,
            payload,
            headers,
            _owner: PhantomData,
        }
    }

    /// Returns the length of the key field of the message.
    pub fn key_len(&self) -> usize {
        self.key.as_ref().map_or(0, Bytes::len)
    }

    /// Returns the length of the payload field of the message.
    pub fn payload_len(&self) -> usize {
        self.payload.as_ref().map_or(0, Bytes::len)
    }

    /// Clones the content of the `BorrowedMessage` and returns an
    /// [`OwnedMessage`] that can outlive the consumer.
    pub fn detach(&self) -> OwnedMessage {
        OwnedMessage {
            key: self.key().map(|k| k.to_vec()),
            payload: self.payload().map(|p| p.to_vec()),
            topic: self.topic().to_owned(),
            timestamp: self.timestamp(),
            partition: self.partition(),
            offset: self.offset(),
            headers: self.headers().map(BorrowedHeaders::detach),
        }
    }
}

impl<'a> fmt::Debug for BorrowedMessage<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedMessage")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("timestamp", &self.timestamp)
            .field("key_len", &self.key_len())
            .field("payload_len", &self.payload_len())
            .finish_non_exhaustive()
    }
}

impl<'a> Message for BorrowedMessage<'a> {
    type Headers = BorrowedHeaders;

    fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn partition(&self) -> i32 {
        self.partition
    }

    fn offset(&self) -> i64 {
        self.offset
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn headers(&self) -> Option<&BorrowedHeaders> {
        self.headers.as_ref()
    }
}

/// Location of one header inside an [`OwnedHeaders`].
#[derive(Debug, Clone)]
struct OwnedHeaderEntry {
    key: Range<usize>,
    value: Option<Range<usize>>,
}

/// A collection of Kafka message headers that owns its backing data.
///
/// Kafka supports multiple headers with the same key, so this is a list, not a
/// map.
#[derive(Clone, Default)]
pub struct OwnedHeaders {
    keys: String,
    values: Vec<u8>,
    entries: Vec<OwnedHeaderEntry>,
}

impl OwnedHeaders {
    /// Creates a new `OwnedHeaders` struct with initial capacity 5.
    pub fn new() -> OwnedHeaders {
        OwnedHeaders::new_with_capacity(5)
    }

    /// Creates a new `OwnedHeaders` struct with the desired initial capacity.
    /// The structure is automatically resized as more headers are added.
    pub fn new_with_capacity(initial_capacity: usize) -> OwnedHeaders {
        OwnedHeaders {
            keys: String::new(),
            values: Vec::new(),
            entries: Vec::with_capacity(initial_capacity),
        }
    }

    /// Inserts a new header.
    pub fn insert<V>(mut self, header: Header<'_, &V>) -> OwnedHeaders
    where
        V: ToBytes + ?Sized,
    {
        let key_start = self.keys.len();
        self.keys.push_str(header.key);
        let value = header.value.map(|value| {
            let value_start = self.values.len();
            self.values.extend_from_slice(value.to_bytes());
            value_start..self.values.len()
        });
        self.entries.push(OwnedHeaderEntry {
            key: key_start..self.keys.len(),
            value,
        });
        self
    }
}

impl Headers for OwnedHeaders {
    fn count(&self) -> usize {
        self.entries.len()
    }

    fn try_get(&self, idx: usize) -> Option<Header<'_, &[u8]>> {
        let entry = self.entries.get(idx)?;
        Some(Header {
            key: &self.keys[entry.key.clone()],
            value: entry.value.clone().map(|range| &self.values[range]),
        })
    }
}

impl fmt::Debug for OwnedHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// A Kafka message that owns its backing data.
#[derive(Debug, Clone)]
pub struct OwnedMessage {
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    topic: String,
    timestamp: Timestamp,
    partition: i32,
    offset: i64,
    headers: Option<OwnedHeaders>,
}

impl OwnedMessage {
    /// Creates a new message with the specified content.
    pub fn new(
        payload: Option<Vec<u8>>,
        key: Option<Vec<u8>>,
        topic: String,
        timestamp: Timestamp,
        partition: i32,
        offset: i64,
        headers: Option<OwnedHeaders>,
    ) -> OwnedMessage {
        OwnedMessage {
            payload,
            key,
            topic,
            timestamp,
            partition,
            offset,
            headers,
        }
    }

    /// Detaches the [`OwnedHeaders`] from this `OwnedMessage`.
    pub fn detach_headers(&mut self) -> Option<OwnedHeaders> {
        self.headers.take()
    }
}

impl Message for OwnedMessage {
    type Headers = OwnedHeaders;

    fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn partition(&self) -> i32 {
        self.partition
    }

    fn offset(&self) -> i64 {
        self.offset
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn headers(&self) -> Option<&OwnedHeaders> {
        self.headers.as_ref()
    }
}

/// The result of a message production.
///
/// If message production is successful `DeliveryResult` will contain the sent
/// message, which can be used to find which partition and offset the message
/// was sent to. If message production is not successful, the `DeliveryResult`
/// will contain an error and the message that failed to be sent.
pub type DeliveryResult<'a> = Result<BorrowedMessage<'a>, (KafkaError, BorrowedMessage<'a>)>;

/// A cheap conversion from a byte slice to typed data.
pub trait FromBytes {
    /// The error type that will be returned if the conversion fails.
    type Error;
    /// Tries to convert the provided byte slice into a different type.
    fn from_bytes(_: &[u8]) -> Result<&Self, Self::Error>;
}

impl FromBytes for [u8] {
    type Error = ();
    fn from_bytes(bytes: &[u8]) -> Result<&Self, Self::Error> {
        Ok(bytes)
    }
}

impl FromBytes for str {
    type Error = str::Utf8Error;
    fn from_bytes(bytes: &[u8]) -> Result<&Self, Self::Error> {
        str::from_utf8(bytes)
    }
}

/// A cheap conversion from typed data to a byte slice.
pub trait ToBytes {
    /// Converts the provided data to bytes.
    fn to_bytes(&self) -> &[u8];
}

impl ToBytes for [u8] {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

impl ToBytes for str {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl ToBytes for String {
    fn to_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a, T: ToBytes> ToBytes for &'a T {
    fn to_bytes(&self) -> &[u8] {
        (*self).to_bytes()
    }
}

impl ToBytes for () {
    fn to_bytes(&self) -> &[u8] {
        &[]
    }
}

impl<const N: usize> ToBytes for [u8; N] {
    fn to_bytes(&self) -> &[u8] {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    #[mz_ore::test]
    fn timestamp() {
        assert_eq!(Timestamp::CreateTime(100).to_millis(), Some(100));
        assert_eq!(Timestamp::LogAppendTime(100).to_millis(), Some(100));
        assert_eq!(Timestamp::CreateTime(-1).to_millis(), None);
        assert_eq!(Timestamp::LogAppendTime(-1).to_millis(), None);
        assert_eq!(Timestamp::NotAvailable.to_millis(), None);
        assert_eq!(Timestamp::from(100), Timestamp::CreateTime(100));
        assert_eq!(
            Timestamp::from(UNIX_EPOCH + Duration::from_millis(7)),
            Timestamp::CreateTime(7)
        );
        assert!(Timestamp::now().to_millis().unwrap() > 0);
    }

    fn sample_headers() -> OwnedHeaders {
        OwnedHeaders::new()
            .insert(Header {
                key: "key1",
                value: Some("value1"),
            })
            .insert(Header {
                key: "key2",
                value: Some(&b""[..]),
            })
            .insert(Header {
                key: "key1",
                value: None::<&[u8]>,
            })
            .insert(Header {
                key: "",
                value: Some(&vec![1u8, 2, 3]),
            })
    }

    fn assert_sample<H: Headers>(headers: &H) {
        assert_eq!(headers.count(), 4);
        let all: Vec<_> = headers.iter().collect();
        assert_eq!(
            all,
            vec![
                Header {
                    key: "key1",
                    value: Some(&b"value1"[..]),
                },
                Header {
                    key: "key2",
                    value: Some(&b""[..]),
                },
                Header {
                    key: "key1",
                    value: None,
                },
                Header {
                    key: "",
                    value: Some(&[1u8, 2, 3][..]),
                },
            ]
        );
        assert!(headers.try_get(4).is_none());
        assert_eq!(
            headers.get_as::<str>(0).unwrap(),
            Header {
                key: "key1",
                value: Some("value1"),
            }
        );
    }

    #[mz_ore::test]
    fn owned_headers() {
        let headers = sample_headers();
        assert_sample(&headers);
        assert_sample(&headers.clone());
        assert_eq!(OwnedHeaders::new().count(), 0);
        assert_eq!(OwnedHeaders::default().iter().count(), 0);
    }

    #[mz_ore::test]
    fn borrowed_headers_round_trip() {
        let owned = sample_headers();
        let borrowed = BorrowedHeaders::from_owned(&owned).unwrap();
        assert_sample(&borrowed);
        assert_sample(&borrowed.detach());
        assert!(BorrowedHeaders::from_owned(&OwnedHeaders::new()).is_none());
    }

    #[mz_ore::test]
    fn borrowed_headers_decode_errors() {
        // Zero headers: varint 0.
        assert_eq!(
            BorrowedHeaders::from_record_bytes(Bytes::from_static(&[0])).map(|h| h.is_none()),
            Ok(true)
        );
        // Empty input.
        assert_eq!(
            BorrowedHeaders::from_record_bytes(Bytes::new()).err(),
            Some(HeaderDecodeError::Truncated)
        );
        // Negative count (-1 zigzag encodes to 1).
        assert_eq!(
            BorrowedHeaders::from_record_bytes(Bytes::from_static(&[1])).err(),
            Some(HeaderDecodeError::InvalidLength)
        );
        // One header, key length 2 but only one byte follows.
        assert_eq!(
            BorrowedHeaders::from_record_bytes(Bytes::from_static(&[2, 4, b'a'])).err(),
            Some(HeaderDecodeError::Truncated)
        );
        // One header with an invalid UTF-8 key and a null value.
        assert_eq!(
            BorrowedHeaders::from_record_bytes(Bytes::from_static(&[2, 2, 0xff, 1])).err(),
            Some(HeaderDecodeError::InvalidKey)
        );
        // Trailing garbage.
        assert_eq!(
            BorrowedHeaders::from_record_bytes(Bytes::from_static(&[2, 2, b'k', 1, 0])).err(),
            Some(HeaderDecodeError::TrailingBytes)
        );
        // One header "k" with a null value.
        let h = BorrowedHeaders::from_record_bytes(Bytes::from_static(&[2, 2, b'k', 1]))
            .unwrap()
            .unwrap();
        assert_eq!(
            h.get(0),
            Header {
                key: "k",
                value: None
            }
        );
    }

    #[mz_ore::test]
    fn varint() {
        for v in [
            0,
            1,
            -1,
            63,
            -64,
            64,
            300,
            -300,
            i64::from(i32::MAX),
            i64::MIN,
            i64::MAX,
        ] {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, v);
            let mut pos = 0;
            assert_eq!(read_varint(&buf, &mut pos), Ok(v));
            assert_eq!(pos, buf.len());
        }
    }

    #[mz_ore::test]
    fn borrowed_message() {
        let headers = BorrowedHeaders::from_owned(&sample_headers());
        let msg = BorrowedMessage::new(
            Arc::from("topic"),
            3,
            42,
            Timestamp::LogAppendTime(1000),
            None,
            Some(Bytes::from_static(b"")),
            headers,
        );
        assert_eq!(msg.topic(), "topic");
        assert_eq!(msg.partition(), 3);
        assert_eq!(msg.offset(), 42);
        assert_eq!(msg.timestamp().to_millis(), Some(1000));
        assert_eq!(msg.key(), None);
        assert_eq!(msg.payload(), Some(&b""[..]));
        assert_eq!(msg.key_len(), 0);
        assert_eq!(msg.payload_view::<str>(), Some(Ok("")));
        assert_sample(msg.headers().unwrap());

        let owned = msg.detach();
        assert_eq!(owned.topic(), "topic");
        assert_eq!(owned.key(), None);
        assert_eq!(owned.payload(), Some(&b""[..]));
        assert_eq!(owned.offset(), 42);
        assert_sample(owned.headers().unwrap());

        let no_headers = BorrowedMessage::new(
            Arc::from("t"),
            0,
            0,
            Timestamp::NotAvailable,
            Some(Bytes::from_static(b"k")),
            None,
            None,
        );
        assert!(no_headers.headers().is_none());
        assert!(no_headers.detach().headers().is_none());
    }

    #[mz_ore::test]
    fn to_bytes() {
        assert_eq!("ab".to_bytes(), b"ab");
        assert_eq!(String::from("ab").to_bytes(), b"ab");
        assert_eq!(vec![1u8].to_bytes(), &[1]);
        assert_eq!([1u8, 2].to_bytes(), &[1, 2]);
        let empty: &[u8] = &[];
        assert_eq!(().to_bytes(), empty);
        let by_ref = &vec![3u8];
        assert_eq!(ToBytes::to_bytes(&by_ref), &[3]);
    }
}
