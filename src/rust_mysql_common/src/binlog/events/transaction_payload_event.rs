// Copyright (c) 2023 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    borrow::Cow,
    cmp::min,
    convert::TryFrom,
    fmt,
    io::{self, BufRead, BufReader},
};

use saturating::Saturating as S;

#[allow(unused)]
use crate::binlog::EventStreamReader;

use super::BinlogEventHeader;
use crate::{
    binlog::{
        consts::{
            BinlogVersion, EventType, TransactionPayloadCompressionType, TransactionPayloadFields,
        },
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::{BufMutExt, ParseBuf, ReadMysqlExt},
    misc::raw::{bytes::EofBytes, int::*, RawBytes},
    proto::{MyDeserialize, MySerialize},
};

/// This structure implements [`io::BufRead`] and represents
/// the payload of a [`TransactionPayloadEvent`].
#[derive(Debug)]
pub struct TransactionPayloadReader<'a> {
    inner: TransactionPayloadInner<'a>,
}

impl<'a> TransactionPayloadReader<'a> {
    /// Creates new instance (`data` is not compressed).
    pub fn new_uncompressed(data: &'a [u8]) -> Self {
        Self {
            inner: TransactionPayloadInner::Uncompressed(data),
        }
    }

    /// Creates new instance (`data` is ZSTD-compressed).
    pub fn new_zstd(data: &'a [u8]) -> io::Result<Self> {
        let decoder = zstd::Decoder::with_buffer(data)?;
        Ok(Self {
            inner: TransactionPayloadInner::ZstdCompressed(BufReader::new(decoder)),
        })
    }

    /// Returns `false` if the reader is exhausted.
    ///
    /// Some io might be necessary to check for data,
    /// so this functions returns `Result<bool>`, not `bool`.
    pub fn has_data_left(&mut self) -> io::Result<bool> {
        self.inner.has_data_left()
    }
}

impl io::Read for TransactionPayloadReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl io::BufRead for TransactionPayloadReader<'_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt)
    }
}

/// Wraps the data providing [`io::BufRead`] implementaion.
enum TransactionPayloadInner<'a> {
    Uncompressed(&'a [u8]),
    ZstdCompressed(BufReader<zstd::Decoder<'a, &'a [u8]>>),
}

impl TransactionPayloadInner<'_> {
    fn has_data_left(&mut self) -> io::Result<bool> {
        match self {
            TransactionPayloadInner::Uncompressed(x) => Ok(!x.is_empty()),
            TransactionPayloadInner::ZstdCompressed(ref mut x) => {
                x.fill_buf().map(|b| !b.is_empty())
            }
        }
    }
}

impl io::BufRead for TransactionPayloadInner<'_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            TransactionPayloadInner::Uncompressed(ref mut x) => x.fill_buf(),
            TransactionPayloadInner::ZstdCompressed(ref mut x) => x.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            TransactionPayloadInner::Uncompressed(ref mut x) => x.consume(amt),
            TransactionPayloadInner::ZstdCompressed(ref mut x) => x.consume(amt),
        }
    }
}

impl io::Read for TransactionPayloadInner<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            TransactionPayloadInner::Uncompressed(ref mut x) => x.read(buf),
            TransactionPayloadInner::ZstdCompressed(ref mut x) => x.read(buf),
        }
    }
}

impl fmt::Debug for TransactionPayloadInner<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uncompressed(arg0) => f.debug_tuple("Uncompressed").field(arg0).finish(),
            Self::ZstdCompressed(_) => f.debug_tuple("ZstdCompressed").field(&"..").finish(),
        }
    }
}

/// Event that encloses all the events of a transaction.
///
/// It is used for carrying compressed payloads, and contains
/// compression metadata.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TransactionPayloadEvent<'a> {
    /// payload size
    payload_size: RawInt<LeU64>,

    /// compression algorithm
    algorithm: TransactionPayloadCompressionType,

    /// uncompressed size
    uncompressed_size: RawInt<LeU64>,

    /// payload to be decompressed
    payload: RawBytes<'a, EofBytes>,

    /// size of parsed header
    header_size: usize,
}

impl<'a> TransactionPayloadEvent<'a> {
    pub fn new(
        payload_size: u64,
        algorithm: TransactionPayloadCompressionType,
        uncompressed_size: u64,
        payload: impl Into<Cow<'a, [u8]>>,
    ) -> Self {
        Self {
            payload_size: RawInt::new(payload_size),
            algorithm,
            uncompressed_size: RawInt::new(uncompressed_size),
            payload: RawBytes::new(payload),
            header_size: 0,
        }
    }

    /// Sets the `payload_size` field value.
    pub fn with_payload_size(mut self, payload_size: u64) -> Self {
        self.payload_size = RawInt::new(payload_size);
        self
    }
    /// Sets the `algorithm` field value.
    pub fn with_algorithm(mut self, algorithm: TransactionPayloadCompressionType) -> Self {
        self.algorithm = algorithm;
        self
    }
    /// Sets the `uncompressed_size` field value.
    pub fn with_uncompressed_size(mut self, uncompressed_size: u64) -> Self {
        self.uncompressed_size = RawInt::new(uncompressed_size);
        self
    }

    /// Sets the `payload` field value.
    pub fn with_payload(mut self, payload: impl Into<Cow<'a, [u8]>>) -> Self {
        self.payload = RawBytes::new(payload);
        self
    }

    /// Returns the payload_size.
    pub fn payload_size(&self) -> u64 {
        self.payload_size.0
    }

    /// Returns raw payload of the binlog event.
    pub fn payload_raw(&'a self) -> &'a [u8] {
        self.payload.as_bytes()
    }

    /// Returns decompressed payload in form of a struct that implements [`io::BufRead`].
    ///
    /// See [`EventStreamReader::read_decompressed`].
    pub fn decompressed(&self) -> io::Result<TransactionPayloadReader<'_>> {
        if self.algorithm == TransactionPayloadCompressionType::NONE {
            return Ok(TransactionPayloadReader::new_uncompressed(
                self.payload_raw(),
            ));
        }

        return TransactionPayloadReader::new_zstd(self.payload_raw());
    }

    /// Decompress the whole payload.
    ///
    /// # Danger
    ///
    /// This function may allocate a huge buffer and cause OOM.
    /// Consider using [`TransactionPayloadEvent::decompressed`] instead.
    pub fn danger_decompress(self) -> Vec<u8> {
        if self.algorithm == TransactionPayloadCompressionType::NONE {
            return self.payload_raw().to_vec();
        }
        let mut decode_buf = vec![0_u8; self.uncompressed_size.0 as usize];
        match zstd::stream::copy_decode(self.payload.as_bytes(), &mut decode_buf[..]) {
            Ok(_) => {}
            Err(_) => {
                return Vec::new();
            }
        };
        decode_buf
    }

    /// Returns the algorithm.
    pub fn algorithm(&self) -> TransactionPayloadCompressionType {
        self.algorithm
    }

    /// Returns the uncompressed_size.
    pub fn uncompressed_size(&self) -> u64 {
        self.uncompressed_size.0
    }

    pub fn into_owned(self) -> TransactionPayloadEvent<'static> {
        TransactionPayloadEvent {
            payload_size: self.payload_size,
            algorithm: self.algorithm,
            uncompressed_size: self.uncompressed_size,
            payload: self.payload.into_owned(),
            header_size: self.header_size,
        }
    }
}

impl<'de> MyDeserialize<'de> for TransactionPayloadEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;
    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut ob = Self {
            payload_size: RawInt::new(0),
            algorithm: TransactionPayloadCompressionType::NONE,
            uncompressed_size: RawInt::new(0),
            payload: RawBytes::from("".as_bytes()),
            header_size: 0,
        };
        let mut have_payload_size = false;
        let mut have_compression_type = false;
        let original_buf_size = buf.len();
        while !buf.is_empty() {
            /* read the type of the field. */
            let field_type = buf.read_lenenc_int()?;
            match TransactionPayloadFields::try_from(field_type) {
                // we have reached the end of the header
                Ok(TransactionPayloadFields::OTW_PAYLOAD_HEADER_END_MARK) => {
                    if !have_payload_size || !have_compression_type {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Missing field in payload header",
                        ))?;
                    }
                    if ob.payload_size.0 as usize > buf.len() {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Payload size is bigger than the remaining buffer: {} > {}",
                                ob.payload_size.0,
                                buf.len()
                            ),
                        ))?;
                    }
                    ob.header_size = original_buf_size - ob.payload_size.0 as usize;
                    let mut payload_buf: ParseBuf = buf.parse(ob.payload_size.0 as usize)?;
                    ob.payload = RawBytes::from(payload_buf.eat_all());
                    break;
                }

                Ok(TransactionPayloadFields::OTW_PAYLOAD_SIZE_FIELD) => {
                    let _length = buf.read_lenenc_int()?;
                    let val = buf.read_lenenc_int()?;
                    ob.payload_size = RawInt::new(val);
                    have_payload_size = true;
                    continue;
                }
                Ok(TransactionPayloadFields::OTW_PAYLOAD_COMPRESSION_TYPE_FIELD) => {
                    let _length = buf.read_lenenc_int()?;
                    let val = buf.read_lenenc_int()?;
                    ob.algorithm = TransactionPayloadCompressionType::try_from(val).unwrap();
                    have_compression_type = true;
                    continue;
                }
                Ok(TransactionPayloadFields::OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD) => {
                    let _length = buf.read_lenenc_int()?;
                    let val = buf.read_lenenc_int()?;
                    ob.uncompressed_size = RawInt::new(val);
                    continue;
                }
                Err(_) => {
                    let length = buf.eat_lenenc_int();
                    buf.skip(length as usize);
                    continue;
                }
            };
        }

        Ok(ob)
    }
}

impl MySerialize for TransactionPayloadEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_lenenc_int(TransactionPayloadFields::OTW_PAYLOAD_COMPRESSION_TYPE_FIELD as u64);
        buf.put_lenenc_int(crate::misc::lenenc_int_len(self.algorithm as u64));
        buf.put_lenenc_int(self.algorithm as u64);

        if self.algorithm != TransactionPayloadCompressionType::NONE {
            buf.put_lenenc_int(
                TransactionPayloadFields::OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD as u64,
            );
            buf.put_lenenc_int(crate::misc::lenenc_int_len(self.uncompressed_size.0));
            buf.put_lenenc_int(self.uncompressed_size.0);
        }

        buf.put_lenenc_int(TransactionPayloadFields::OTW_PAYLOAD_SIZE_FIELD as u64);
        buf.put_lenenc_int(crate::misc::lenenc_int_len(self.payload_size.0));
        buf.put_lenenc_int(self.payload_size.0);

        buf.put_lenenc_int(TransactionPayloadFields::OTW_PAYLOAD_HEADER_END_MARK as u64);

        self.payload.serialize(&mut *buf);
    }
}

impl<'a> BinlogEvent<'a> for TransactionPayloadEvent<'a> {
    const EVENT_TYPE: EventType = EventType::TRANSACTION_PAYLOAD_EVENT;
}

impl<'a> BinlogStruct<'a> for TransactionPayloadEvent<'a> {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(self.header_size);

        len += S(self.payload.0.len());

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}
