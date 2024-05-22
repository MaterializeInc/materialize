// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! MySql protocol codec implementation.

pub use flate2::Compression;

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, BytesMut};
use flate2::read::{ZlibDecoder, ZlibEncoder};

use std::{
    cmp::{max, min},
    io::Read,
    mem,
    num::NonZeroUsize,
    ptr::slice_from_raw_parts_mut,
};

use self::error::PacketCodecError;
use crate::constants::{DEFAULT_MAX_ALLOWED_PACKET, MAX_PAYLOAD_LEN, MIN_COMPRESS_LENGTH};

pub mod error;

/// Will split given `packet` to MySql packet chunks and write into `dst`.
///
/// Chunk ids will start with given `seq_id`.
///
/// Resulting sequence id will be returned.
pub fn packet_to_chunks<T: Buf>(mut seq_id: u8, packet: &mut T, dst: &mut BytesMut) -> u8 {
    let extra_packet = packet.remaining() % MAX_PAYLOAD_LEN == 0;
    dst.reserve(packet.remaining() + (packet.remaining() / MAX_PAYLOAD_LEN) * 4 + 4);

    while packet.has_remaining() {
        let mut chunk_len = min(packet.remaining(), MAX_PAYLOAD_LEN);
        dst.put_u32_le(chunk_len as u32 | (u32::from(seq_id) << 24));
        while chunk_len > 0 {
            let chunk = packet.chunk();
            let count = min(chunk.len(), chunk_len);
            dst.put(&chunk[..count]);
            chunk_len -= count;
            packet.advance(count);
        }
        seq_id = seq_id.wrapping_add(1);
    }

    if extra_packet {
        dst.put_u32_le(u32::from(seq_id) << 24);
        seq_id = seq_id.wrapping_add(1);
    }

    seq_id
}

/// Will compress all data from `src` to `dst`.
///
/// Compressed packets will start with given `seq_id`. Resulting sequence id will be returned.
pub fn compress(
    mut seq_id: u8,
    compression: Compression,
    max_allowed_packet: usize,
    src: &mut BytesMut,
    dst: &mut BytesMut,
) -> Result<u8, PacketCodecError> {
    if src.is_empty() {
        return Ok(0);
    }

    for chunk in src.chunks(min(MAX_PAYLOAD_LEN, max_allowed_packet)) {
        dst.reserve(7 + chunk.len());

        if compression != Compression::none() && chunk.len() >= MIN_COMPRESS_LENGTH {
            unsafe {
                let mut encoder = ZlibEncoder::new(chunk, compression);
                let mut read = 0;
                loop {
                    dst.reserve(max(chunk.len().saturating_sub(read), 1));
                    let dst_buf = &mut dst.chunk_mut()[7 + read..];
                    match encoder.read(&mut *slice_from_raw_parts_mut(
                        dst_buf.as_mut_ptr(),
                        dst_buf.len(),
                    ))? {
                        0 => break,
                        count => read += count,
                    }
                }

                dst.put_uint_le(read as u64, 3);
                dst.put_u8(seq_id);
                dst.put_uint_le(chunk.len() as u64, 3);
                dst.advance_mut(read);
            }
        } else {
            dst.put_uint_le(chunk.len() as u64, 3);
            dst.put_u8(seq_id);
            dst.put_uint_le(0, 3);
            dst.put_slice(chunk);
        }

        seq_id = seq_id.wrapping_add(1);
    }

    src.clear();

    Ok(seq_id)
}

/// Chunk info.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ChunkInfo {
    /// A packet chunk with given sequence id that isn't last in a packet.
    ///
    /// Only makes sense for plain MySql protocol.
    Middle(u8),
    /// Last chunk in a packet. Stores chunk sequence id.
    ///
    /// The only variant that `CompDecoder` will return.
    Last(u8),
}

impl ChunkInfo {
    fn seq_id(self) -> u8 {
        match self {
            ChunkInfo::Middle(x) | ChunkInfo::Last(x) => x,
        }
    }
}

/// Decoder for MySql protocol chunk.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum ChunkDecoder {
    /// Decoder is waiting for the first or subsequent packet chunk.
    ///
    /// It'll need at least 4 bytes to start decoding a chunk.
    #[default]
    Idle,
    /// Chunk is being decoded.
    Chunk {
        /// Sequence id of chunk being decoded.
        seq_id: u8,
        /// Number of bytes needed to finish this chunk.
        needed: NonZeroUsize,
    },
}

impl ChunkDecoder {
    /// Will try to decode MySql packet chunk from `src` to `dst`.
    ///
    /// If chunk is decoded, then `ChunkInfo` is returned.
    ///
    /// If the `dst` buffer isn't empty then it is expected that it contains previous chunks
    /// of the same packet, or this function may erroneously report
    /// [`PacketCodecError::PacketTooLarge`] error.
    pub fn decode<T>(
        &mut self,
        src: &mut BytesMut,
        dst: &mut T,
        max_allowed_packet: usize,
    ) -> Result<Option<ChunkInfo>, PacketCodecError>
    where
        T: AsRef<[u8]>,
        T: BufMut,
    {
        match *self {
            ChunkDecoder::Idle => {
                if src.len() < 4 {
                    // We need at least 4 bytes to read chunk length and sequence id.
                    Ok(None)
                } else {
                    let raw_chunk_len = LittleEndian::read_u24(&*src) as usize;
                    let seq_id = src[3];

                    match NonZeroUsize::new(raw_chunk_len) {
                        Some(chunk_len) => {
                            if dst.as_ref().len() + chunk_len.get() > max_allowed_packet {
                                return Err(PacketCodecError::PacketTooLarge);
                            }

                            *self = ChunkDecoder::Chunk {
                                seq_id,
                                needed: chunk_len,
                            };

                            if src.len() > 4 {
                                self.decode(src, dst, max_allowed_packet)
                            } else {
                                Ok(None)
                            }
                        }
                        None => {
                            src.advance(4);
                            Ok(Some(ChunkInfo::Last(seq_id)))
                        }
                    }
                }
            }
            ChunkDecoder::Chunk { seq_id, needed } => {
                if src.len() >= 4 + needed.get() {
                    src.advance(4);

                    dst.put_slice(&src[..needed.get()]);
                    src.advance(needed.get());

                    *self = ChunkDecoder::Idle;

                    if dst.as_ref().len() % MAX_PAYLOAD_LEN == 0 {
                        Ok(Some(ChunkInfo::Middle(seq_id)))
                    } else {
                        Ok(Some(ChunkInfo::Last(seq_id)))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// Stores information about compressed packet being decoded.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CompData {
    /// Compressed(&lt;needed&gt;, &lt;uncompressed len&gt;)
    Compressed(NonZeroUsize, NonZeroUsize),
    /// Uncompressed(&lt;needed&gt;)
    Uncompressed(NonZeroUsize),
}

impl CompData {
    /// Creates new `CompData` if given arguments are valid.
    fn new(
        compressed_len: usize,
        uncompressed_len: usize,
        max_allowed_packet: usize,
    ) -> Result<Option<Self>, PacketCodecError> {
        // max_allowed_packet will be an upper boundary
        if max(compressed_len, uncompressed_len) > max_allowed_packet {
            return Err(PacketCodecError::PacketTooLarge);
        }

        let compressed_len = NonZeroUsize::new(compressed_len);
        let uncompressed_len = NonZeroUsize::new(uncompressed_len);

        match (compressed_len, uncompressed_len) {
            (Some(needed), Some(plain_len)) => Ok(Some(CompData::Compressed(needed, plain_len))),
            (Some(needed), None) => Ok(Some(CompData::Uncompressed(needed))),
            (None, Some(_)) => {
                // Zero bytes of compressed data that stores
                // non-zero bytes of plain data? Absurd.
                Err(PacketCodecError::BadCompressedPacketHeader)
            }
            (None, None) => Ok(None),
        }
    }

    /// Returns number of bytes needed to decode packet.
    fn needed(&self) -> usize {
        match *self {
            CompData::Compressed(needed, _) | CompData::Uncompressed(needed) => needed.get(),
        }
    }
}

/// Decoder for MySql compressed packet.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CompDecoder {
    /// Decoder is waiting for compressed packet header.
    Idle,
    /// Decoder is decoding a packet.
    Packet {
        /// Compressed packet sequence id.
        seq_id: u8,
        /// Compressed packet size information.
        needed: CompData,
    },
}

impl CompDecoder {
    /// Will try to decode compressed packet from `src` into `dst`.
    ///
    /// If packet is decoded, then `ChunkInfo::Last` is returned.
    pub fn decode(
        &mut self,
        src: &mut BytesMut,
        dst: &mut BytesMut,
        max_allowed_packet: usize,
    ) -> Result<Option<ChunkInfo>, PacketCodecError> {
        match *self {
            CompDecoder::Idle => {
                if src.len() < 7 {
                    // We need at least 7 bytes to read compressed packet header.
                    Ok(None)
                } else {
                    let compressed_len = LittleEndian::read_u24(&*src) as usize;
                    let seq_id = src[3];
                    let uncompressed_len = LittleEndian::read_u24(&src[4..]) as usize;

                    match CompData::new(compressed_len, uncompressed_len, max_allowed_packet)? {
                        Some(needed) => {
                            *self = CompDecoder::Packet { seq_id, needed };
                            self.decode(src, dst, max_allowed_packet)
                        }
                        None => {
                            src.advance(7);
                            Ok(Some(ChunkInfo::Last(seq_id)))
                        }
                    }
                }
            }
            CompDecoder::Packet { seq_id, needed } => {
                if src.len() >= 7 + needed.needed() {
                    src.advance(7);
                    match needed {
                        CompData::Uncompressed(needed) => {
                            dst.extend_from_slice(&src[..needed.get()]);
                        }
                        CompData::Compressed(needed, plain_len) => {
                            dst.reserve(plain_len.get());
                            unsafe {
                                let mut decoder = ZlibDecoder::new(&src[..needed.get()]);
                                let dst_buf = &mut dst.chunk_mut()[..plain_len.get()];
                                decoder.read_exact(&mut *slice_from_raw_parts_mut(
                                    dst_buf.as_mut_ptr(),
                                    dst_buf.len(),
                                ))?;
                                dst.advance_mut(plain_len.get());
                            }
                        }
                    }
                    src.advance(needed.needed());
                    *self = CompDecoder::Idle;
                    Ok(Some(ChunkInfo::Last(seq_id)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// Codec for MySql protocol packets.
///
/// Codec supports both plain and compressed protocols.
#[derive(Debug)]
pub struct PacketCodec {
    /// Maximum size of a packet for this codec.
    pub max_allowed_packet: usize,
    /// Actual implementation.
    inner: PacketCodecInner,
}

impl PacketCodec {
    /// Sets sequence id to `0`.
    pub fn reset_seq_id(&mut self) {
        self.inner.reset_seq_id();
    }

    /// Overwrites plain sequence id with compressed sequence id.
    pub fn sync_seq_id(&mut self) {
        self.inner.sync_seq_id();
    }

    /// Turns compression on.
    pub fn compress(&mut self, level: Compression) {
        self.inner.compress(level);
    }

    /// Will try to decode a packet from `src` into `dst`.
    ///
    /// Returns
    ///
    /// * `true` - decoded packet was written into the `dst`,
    /// * `false` - `src` did not contain a full packet.
    pub fn decode<T>(&mut self, src: &mut BytesMut, dst: &mut T) -> Result<bool, PacketCodecError>
    where
        T: AsRef<[u8]>,
        T: BufMut,
    {
        self.inner.decode(src, dst, self.max_allowed_packet)
    }

    /// Will encode packets into `dst`.
    pub fn encode<T: Buf>(
        &mut self,
        src: &mut T,
        dst: &mut BytesMut,
    ) -> Result<(), PacketCodecError> {
        self.inner.encode(src, dst, self.max_allowed_packet)
    }
}

impl Default for PacketCodec {
    fn default() -> Self {
        Self {
            max_allowed_packet: DEFAULT_MAX_ALLOWED_PACKET,
            inner: Default::default(),
        }
    }
}

/// Packet codec implementation.
#[derive(Debug)]
enum PacketCodecInner {
    /// Plain packet codec.
    Plain(PlainPacketCodec),
    /// Compressed packet codec.
    Comp(CompPacketCodec),
}

impl PacketCodecInner {
    /// Sets sequence id to `0`.
    fn reset_seq_id(&mut self) {
        match self {
            PacketCodecInner::Plain(c) => c.reset_seq_id(),
            PacketCodecInner::Comp(c) => c.reset_seq_id(),
        }
    }

    /// Overwrites plain sequence id with compressed sequence id.
    fn sync_seq_id(&mut self) {
        match self {
            PacketCodecInner::Plain(_) => (),
            PacketCodecInner::Comp(c) => c.sync_seq_id(),
        }
    }

    /// Turns compression on.
    fn compress(&mut self, level: Compression) {
        match self {
            PacketCodecInner::Plain(c) => {
                *self = PacketCodecInner::Comp(CompPacketCodec {
                    level,
                    comp_seq_id: 0,
                    in_buf: BytesMut::with_capacity(DEFAULT_MAX_ALLOWED_PACKET),
                    out_buf: BytesMut::with_capacity(DEFAULT_MAX_ALLOWED_PACKET),
                    comp_decoder: CompDecoder::Idle,
                    plain_codec: mem::take(c),
                })
            }
            PacketCodecInner::Comp(c) => c.level = level,
        }
    }

    /// Will try to decode packet from `src` into `dst`.
    ///
    /// If `true` is returned then `dst` contains full packet.
    fn decode<T>(
        &mut self,
        src: &mut BytesMut,
        dst: &mut T,
        max_allowed_packet: usize,
    ) -> Result<bool, PacketCodecError>
    where
        T: AsRef<[u8]>,
        T: BufMut,
    {
        match self {
            PacketCodecInner::Plain(codec) => codec.decode(src, dst, max_allowed_packet, None),
            PacketCodecInner::Comp(codec) => codec.decode(src, dst, max_allowed_packet),
        }
    }

    /// Will try to encode packets into `dst`.
    fn encode<T: Buf>(
        &mut self,
        packet: &mut T,
        dst: &mut BytesMut,
        max_allowed_packet: usize,
    ) -> Result<(), PacketCodecError> {
        match self {
            PacketCodecInner::Plain(codec) => codec.encode(packet, dst, max_allowed_packet),
            PacketCodecInner::Comp(codec) => codec.encode(packet, dst, max_allowed_packet),
        }
    }
}

impl Default for PacketCodecInner {
    fn default() -> Self {
        PacketCodecInner::Plain(Default::default())
    }
}

/// Codec for plain MySql protocol.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
struct PlainPacketCodec {
    /// Chunk sequence id.
    pub seq_id: u8,
    /// Chunk decoder.
    chunk_decoder: ChunkDecoder,
}

impl PlainPacketCodec {
    /// Sets sequence id to `0`.
    fn reset_seq_id(&mut self) {
        self.seq_id = 0;
    }

    /// Will try to decode packet from `src` into `dst`.
    ///
    /// * `comp_seq_id` - is the sequence id of the last compressed packet (if any).
    ///
    /// If `true` is returned then `dst` contains full packet.
    fn decode<T>(
        &mut self,
        src: &mut BytesMut,
        dst: &mut T,
        max_allowed_packet: usize,
        comp_seq_id: Option<u8>,
    ) -> Result<bool, PacketCodecError>
    where
        T: AsRef<[u8]>,
        T: BufMut,
    {
        match self.chunk_decoder.decode(src, dst, max_allowed_packet)? {
            Some(chunk_info) => {
                if self.seq_id != chunk_info.seq_id() {
                    match comp_seq_id {
                        Some(seq_id) if seq_id == chunk_info.seq_id() => {
                            // server syncronized pkt_nr (in `net_flush`)
                            self.seq_id = seq_id;
                        }
                        _ => {
                            return Err(PacketCodecError::PacketsOutOfSync);
                        }
                    }
                }

                self.seq_id = self.seq_id.wrapping_add(1);

                match chunk_info {
                    ChunkInfo::Middle(_) => {
                        if !src.is_empty() {
                            self.decode(src, dst, max_allowed_packet, comp_seq_id)
                        } else {
                            Ok(false)
                        }
                    }
                    ChunkInfo::Last(_) => Ok(true),
                }
            }
            None => Ok(false),
        }
    }

    /// Will try to encode packets into `dst`.
    fn encode<T: Buf>(
        &mut self,
        packet: &mut T,
        dst: &mut BytesMut,
        max_allowed_packet: usize,
    ) -> Result<(), PacketCodecError> {
        if packet.remaining() > max_allowed_packet {
            return Err(PacketCodecError::PacketTooLarge);
        }

        self.seq_id = packet_to_chunks(self.seq_id, packet, dst);

        Ok(())
    }
}

/// Codec for compressed MySql protocol.
#[derive(Debug)]
struct CompPacketCodec {
    /// Compression level for this codec.
    level: Compression,
    /// Compressed packet sequence id.
    comp_seq_id: u8,
    /// Buffer for decompressed input data.
    in_buf: BytesMut,
    /// Buffer for compressed output data.
    out_buf: BytesMut,
    /// Compressed packet decoder.
    comp_decoder: CompDecoder,
    /// Wrapped codec for plain MySql protocol.
    plain_codec: PlainPacketCodec,
}

impl CompPacketCodec {
    /// Sets sequence id to `0`.
    fn reset_seq_id(&mut self) {
        self.comp_seq_id = 0;
        self.plain_codec.reset_seq_id();
    }

    /// Overwrites plain sequence id with compressed sequence id
    /// if on compressed packet boundary.
    fn sync_seq_id(&mut self) {
        if self.in_buf.is_empty() {
            self.plain_codec.seq_id = self.comp_seq_id;
        }
    }

    /// Will try to decode packet from `src` into `dst`.
    ///
    /// If `true` is returned then `dst` contains full packet.
    fn decode<T>(
        &mut self,
        src: &mut BytesMut,
        dst: &mut T,
        max_allowed_packet: usize,
    ) -> Result<bool, PacketCodecError>
    where
        T: AsRef<[u8]>,
        T: BufMut,
    {
        if !self.in_buf.is_empty()
            && self.plain_codec.decode(
                &mut self.in_buf,
                dst,
                max_allowed_packet,
                // the server could sync the sequence id of the plain packet
                // with the id of the last compressed packet
                Some(self.comp_seq_id.wrapping_sub(1)),
            )?
        {
            return Ok(true);
        }

        match self
            .comp_decoder
            .decode(src, &mut self.in_buf, max_allowed_packet)?
        {
            Some(chunk_info) => {
                if self.comp_seq_id != chunk_info.seq_id() {
                    return Err(PacketCodecError::PacketsOutOfSync);
                }

                self.comp_seq_id = self.comp_seq_id.wrapping_add(1);

                self.decode(src, dst, max_allowed_packet)
            }
            None => Ok(false),
        }
    }

    /// Will try to encode packets into `dst`.
    fn encode<T: Buf>(
        &mut self,
        packet: &mut T,
        dst: &mut BytesMut,
        max_allowed_packet: usize,
    ) -> Result<(), PacketCodecError> {
        self.plain_codec
            .encode(packet, &mut self.out_buf, max_allowed_packet)?;

        self.comp_seq_id = compress(
            self.comp_seq_id,
            self.level,
            max_allowed_packet,
            &mut self.out_buf,
            dst,
        )?;

        /* Sync packet number if using compression (see net_serv.cc) */
        self.plain_codec.seq_id = self.comp_seq_id;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPRESSED: &[u8] = &[
        0x22, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x78, 0x9c, 0xd3, 0x63, 0x60, 0x60, 0x60, 0x2e,
        0x4e, 0xcd, 0x49, 0x4d, 0x2e, 0x51, 0x50, 0x32, 0x30, 0x34, 0x32, 0x36, 0x31, 0x35, 0x33,
        0xb7, 0xb0, 0xc4, 0xcd, 0x52, 0x02, 0x00, 0x0c, 0xd1, 0x0a, 0x6c,
    ];

    const PLAIN: [u8; 46] = [
        0x03, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x22, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35,
        0x36, 0x37, 0x38, 0x39, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30,
        0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35,
        0x22,
    ];

    #[test]
    fn zero_len_packet() -> Result<(), error::PacketCodecError> {
        let mut encoder = PacketCodec::default();
        let mut empty: &[u8] = &[];
        let mut src = BytesMut::new();
        encoder.encode(&mut empty, &mut src)?;

        let mut dst = vec![];
        let mut decoder = PacketCodec::default();
        let result = decoder.decode(&mut src, &mut dst)?;
        assert!(result);
        assert_eq!(dst, vec![0_u8; 0]);

        Ok(())
    }

    #[test]
    fn regular_packet() -> Result<(), error::PacketCodecError> {
        let mut encoder = PacketCodec::default();
        let mut src = BytesMut::new();
        encoder.encode(&mut &[0x31_u8, 0x32, 0x33][..], &mut src)?;

        let mut dst = vec![];
        let mut decoder = PacketCodec::default();
        let result = decoder.decode(&mut src, &mut dst)?;
        assert!(result);
        assert_eq!(dst, vec![0x31, 0x32, 0x33]);

        Ok(())
    }

    #[test]
    fn packet_sequence() -> Result<(), error::PacketCodecError> {
        let mut encoder = PacketCodec::default();
        let mut decoder = PacketCodec::default();
        let mut src = BytesMut::new();

        for i in 0..1024_usize {
            encoder.encode(&mut &*vec![0; i], &mut src)?;
            let mut dst = vec![];
            let result = decoder.decode(&mut src, &mut dst)?;
            assert!(result);
            assert_eq!(dst, vec![0; i]);
        }

        Ok(())
    }

    #[test]
    fn large_packets() -> Result<(), error::PacketCodecError> {
        let lengths = vec![MAX_PAYLOAD_LEN, MAX_PAYLOAD_LEN + 1, MAX_PAYLOAD_LEN * 2];
        let mut encoder = PacketCodec::default();
        let mut decoder = PacketCodec::default();
        let mut src = BytesMut::new();

        decoder.max_allowed_packet = *lengths.iter().max().unwrap();
        encoder.max_allowed_packet = *lengths.iter().max().unwrap();

        for &len in &lengths {
            encoder.encode(&mut &*vec![0x42_u8; len], &mut src)?;
        }

        for &len in &lengths {
            let mut dst = vec![];
            let result = decoder.decode(&mut src, &mut dst)?;
            assert!(result);
            assert_eq!(dst, vec![0x42; len]);
        }

        Ok(())
    }

    #[test]
    fn compressed_roundtrip() {
        let mut encoder = PacketCodec::default();
        let mut decoder = PacketCodec::default();
        let mut src = BytesMut::from(COMPRESSED);

        encoder.compress(Compression::best());
        decoder.compress(Compression::best());

        let mut dst = vec![];
        let result = decoder.decode(&mut src, &mut dst).unwrap();
        assert!(result);
        assert_eq!(&*dst, PLAIN);
        encoder.encode(&mut &*dst, &mut src).unwrap();

        let mut dst = vec![];
        decoder.reset_seq_id();
        let result = decoder.decode(&mut src, &mut dst).unwrap();
        assert!(result);
        assert_eq!(&*dst, PLAIN);
    }

    #[test]
    fn compression_none() {
        let mut encoder = PacketCodec::default();
        let mut decoder = PacketCodec::default();
        let mut src = BytesMut::new();

        encoder.compress(Compression::none());
        decoder.compress(Compression::none());

        encoder.encode(&mut (&PLAIN[..]), &mut src).unwrap();
        let mut dst = vec![];
        let result = decoder.decode(&mut src, &mut dst).unwrap();
        assert!(result);
        assert_eq!(&*dst, PLAIN);
    }

    #[test]
    #[should_panic(expected = "PacketsOutOfSync")]
    fn out_of_sync() {
        let mut src = BytesMut::from(&b"\x00\x00\x00\x01"[..]);
        let mut codec = PacketCodec::default();
        let mut dst = vec![];
        codec.decode(&mut src, &mut dst).unwrap();
    }

    #[test]
    #[should_panic(expected = "PacketTooLarge")]
    fn packet_too_large() {
        let mut encoder = PacketCodec::default();
        let mut decoder = PacketCodec::default();
        let mut src = BytesMut::new();

        encoder
            .encode(&mut &*vec![0; encoder.max_allowed_packet + 1], &mut src)
            .unwrap();
        let mut dst = vec![];
        decoder.decode(&mut src, &mut dst).unwrap();
    }
}
