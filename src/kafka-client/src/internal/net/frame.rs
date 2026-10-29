// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Length-prefixed response frames.

use std::io;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Decoder;

use crate::internal::pool::{BufferPool, LIMIT_DECAY};
use crate::internal::protocol::FRAME_LEN_SIZE;

/// A frame read error.
#[derive(Debug)]
pub(crate) enum FrameError {
    Io(io::Error),
    /// A frame whose declared size is below 4 (the correlation id) or above
    /// `receive.message.max.bytes`.
    Size(i32),
}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> FrameError {
        FrameError::Io(e)
    }
}

/// Decodes response frames: an `i32` length followed by that many bytes.
/// Yields the bytes after the length.
pub(crate) struct FrameCodec {
    max_size: usize,
}

impl FrameCodec {
    pub(crate) fn new(max_size: usize) -> FrameCodec {
        FrameCodec { max_size }
    }
}

/// The length prefix and the correlation id.
const RESPONSE_HEADER_SIZE: usize = FRAME_LEN_SIZE + 4;

/// Validates a declared frame size like librdkafka: at least the correlation
/// id, at most `receive.message.max.bytes`.
fn check_size(size: i32, max_size: usize) -> Result<usize, FrameError> {
    match usize::try_from(size) {
        Ok(n) if (4..=max_size).contains(&n) => Ok(n),
        _ => Err(FrameError::Size(size)),
    }
}

impl Decoder for FrameCodec {
    type Item = Bytes;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Bytes>, FrameError> {
        // Like librdkafka, validate the size only once the whole response
        // header (size and correlation id) has arrived. A peer that sends
        // fewer bytes and closes (e.g. a TLS alert to a plaintext client) is
        // a disconnect, not an invalid size.
        if src.len() < RESPONSE_HEADER_SIZE {
            return Ok(None);
        }
        let size = i32::from_be_bytes(src[..FRAME_LEN_SIZE].try_into().expect("4 bytes"));
        let size = check_size(size, self.max_size)?;
        if src.len() < FRAME_LEN_SIZE + size {
            src.reserve(FRAME_LEN_SIZE + size - src.len());
            return Ok(None);
        }
        src.advance(FRAME_LEN_SIZE);
        // A copy, so that `src` stays the only owner of its allocation and
        // keeps reading into it. A frame split off it would pin the
        // allocation until the response is handled, so that the next read
        // would allocate a new read-ahead buffer, typically on another
        // worker's jemalloc arena.
        let frame = Bytes::copy_from_slice(&src[..size]);
        src.advance(size);
        Ok(Some(frame))
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Bytes>, FrameError> {
        match self.decode(src)? {
            Some(frame) => Ok(Some(frame)),
            // A connection closed in the middle of a frame is a disconnect
            // like any other.
            None if !src.is_empty() => Err(FrameError::Io(io::ErrorKind::UnexpectedEof.into())),
            None => Ok(None),
        }
    }
}

/// Frames larger than this are read into a buffer of their own.
const LARGE_FRAME: usize = 64 << 10;

/// How much [`FrameReader`] reads ahead for headers and small frames.
const READ_AHEAD: usize = 64 << 10;

/// The least capacity of a large frame's buffer: a Fetch response of the
/// consumer's 8 MiB cap plus some batches over it (KIP-74). Responses that
/// grow while consumption ramps up then reuse one buffer, instead of each
/// replacing a smaller one whose freed pages the allocator keeps resident.
/// Capacity that is never written takes address space, not memory.
const LARGE_FRAME_CAPACITY: usize = 10 << 20;

/// After how long without a frame a connection frees its kept frame
/// buffer, e.g. while a consumer whose queues are full sends no Fetch.
const IDLE_RELEASE: Duration = LIMIT_DECAY;

/// A large frame being read.
struct LargeFrame {
    buf: Vec<u8>,
    size: usize,
}

/// Reads response frames from a stream, like `FramedRead` with
/// [`FrameCodec`], except that a frame larger than [`LARGE_FRAME`] (Fetch
/// responses, mostly) is read into a pooled buffer of its own.
///
/// NOTE: `FramedRead` reads ahead into its buffer right after splitting a
/// frame off it, while the frame still shares that buffer, so every large
/// frame lands in a new allocation of its size. Consuming at a high rate,
/// those allocations, coming from whichever thread polls the reader, kept
/// several times the live memory resident in the allocator (see
/// [`crate::internal::pool`]). One pooled frame buffer is reused while large
/// frames keep coming, and freed once only small frames or none arrived for
/// a while. A frame much smaller than the kept buffer gets a buffer of its
/// own, so one oversized frame does not pin its buffer for every later one.
pub(crate) struct FrameReader<R> {
    io: R,
    codec: FrameCodec,
    /// Read-ahead for headers and small frames.
    buf: BytesMut,
    large: Option<LargeFrame>,
    pool: BufferPool,

    eof: bool,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub(crate) fn new(io: R, max_size: usize) -> FrameReader<R> {
        FrameReader {
            io,
            codec: FrameCodec::new(max_size),
            buf: BytesMut::new(),
            large: None,
            pool: BufferPool::with_fit(2),
            eof: false,
        }
    }

    /// Reads the next frame, without its length prefix. `None` when the
    /// stream ended at a frame boundary; a stream that ends inside a frame
    /// is an `UnexpectedEof` error.
    ///
    /// Cancel safe: if the future is dropped, no data is lost, and the next
    /// call continues where this one stopped.
    pub(crate) async fn next(&mut self) -> Option<Result<Bytes, FrameError>> {
        loop {
            if let Some(large) = &mut self.large {
                let remaining = large.size - large.buf.len();
                if remaining == 0 {
                    let large = self.large.take().expect("checked");
                    return Some(Ok(self.pool.freeze(large.buf)));
                }
                // Read at most the rest of this frame into its buffer.
                match self
                    .io
                    .read_buf(&mut (&mut large.buf).limit(remaining))
                    .await
                {
                    Ok(0) => {
                        return Some(Err(FrameError::Io(io::ErrorKind::UnexpectedEof.into())));
                    }
                    Ok(_) => continue,
                    Err(e) => return Some(Err(e.into())),
                }
            }
            match self.decode() {
                Err(e) => return Some(Err(e)),
                Ok(Some(frame)) => return Some(Ok(frame)),
                Ok(None) if self.large.is_some() => continue,
                Ok(None) => {}
            }
            if self.eof {
                return self.codec.decode_eof(&mut self.buf).transpose();
            }
            self.buf.reserve(READ_AHEAD);
            let read = self.io.read_buf(&mut self.buf);
            let result = if self.pool.pooled() > 0 {
                tokio::select! {
                    result = read => result,
                    _ = tokio::time::sleep(IDLE_RELEASE) => {
                        self.pool.decay_limit(0);
                        continue;
                    }
                }
            } else {
                read.await
            };
            match result {
                Ok(0) => self.eof = true,
                Ok(_) => {}
                Err(e) => return Some(Err(e.into())),
            }
        }
    }

    /// Decodes a small frame from the read-ahead buffer, or starts reading a
    /// large one.
    fn decode(&mut self) -> Result<Option<Bytes>, FrameError> {
        if self.buf.len() >= RESPONSE_HEADER_SIZE {
            let size = i32::from_be_bytes(self.buf[..FRAME_LEN_SIZE].try_into().expect("4 bytes"));
            let size = check_size(size, self.codec.max_size)?;
            if size > LARGE_FRAME {
                self.buf.advance(FRAME_LEN_SIZE);
                let have = self.buf.len().min(size);
                let mut buf = self.pool.take(size.max(LARGE_FRAME_CAPACITY));
                buf.extend_from_slice(&self.buf[..have]);
                self.buf.advance(have);
                // Keep a buffer for the next large frame, as long as large
                // frames keep coming.
                self.pool.decay_limit(buf.capacity());
                self.large = Some(LargeFrame { buf, size });
                return Ok(None);
            }
            self.pool.decay_limit(0);
        }
        self.codec.decode(&mut self.buf)
    }
}

/// Reads one frame from `io`, for the sequential connection setup phase.
pub(crate) async fn read_frame<S: AsyncRead + Unpin>(
    io: &mut S,
    max_size: usize,
) -> Result<Bytes, FrameError> {
    let mut header = [0u8; RESPONSE_HEADER_SIZE];
    io.read_exact(&mut header).await?;
    let len: [u8; FRAME_LEN_SIZE] = header[..FRAME_LEN_SIZE].try_into().expect("4 bytes");
    let size = check_size(i32::from_be_bytes(len), max_size)?;
    let mut buf = vec![0u8; size];
    buf[..4].copy_from_slice(&header[FRAME_LEN_SIZE..]);
    io.read_exact(&mut buf[4..]).await?;
    Ok(Bytes::from(buf))
}

/// Reads one raw size-prefixed SASL token (SaslHandshake v0), which may be
/// empty.
pub(crate) async fn read_raw_frame<S: AsyncRead + Unpin>(
    io: &mut S,
    max_size: usize,
) -> Result<Bytes, FrameError> {
    let mut len = [0u8; FRAME_LEN_SIZE];
    io.read_exact(&mut len).await?;
    let size = i32::from_be_bytes(len);
    let size = match usize::try_from(size) {
        Ok(n) if n <= max_size => n,
        _ => return Err(FrameError::Size(size)),
    };
    let mut buf = vec![0u8; size];
    io.read_exact(&mut buf).await?;
    Ok(Bytes::from(buf))
}

/// Writes an already framed request.
pub(crate) async fn write_frame<S: AsyncWrite + Unpin>(io: &mut S, frame: &[u8]) -> io::Result<()> {
    io.write_all(frame).await?;
    io.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes `frames` to a reader in pieces of `chunk` bytes and collects
    /// what [`FrameReader`] yields.
    async fn read_all(
        frames: &[Vec<u8>],
        chunk: usize,
        close_early: bool,
    ) -> Vec<Result<Bytes, String>> {
        let mut wire = Vec::new();
        for f in frames {
            wire.extend_from_slice(&i32::try_from(f.len()).unwrap().to_be_bytes());
            wire.extend_from_slice(f);
        }
        if close_early {
            wire.truncate(wire.len() - 1);
        }
        let (mut tx, rx) = tokio::io::duplex(chunk);
        let writer = mz_ore::task::spawn(|| "frame-reader-test-writer", async move {
            for piece in wire.chunks(chunk) {
                tx.write_all(piece).await.unwrap();
            }
        });
        let mut reader = FrameReader::new(rx, 1 << 20);
        let mut out = Vec::new();
        while let Some(r) = reader.next().await {
            let failed = r.is_err();
            out.push(r.map_err(|e| format!("{e:?}")));
            if failed {
                break;
            }
        }
        writer.await;
        out
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn frame_reader() {
        let small = vec![1u8, 2, 3, 4, 5];
        let large: Vec<u8> = (0..200_000u32)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let frames = vec![
            small.clone(),
            large.clone(),
            small.clone(),
            large.clone(),
            large.clone(),
        ];
        for chunk in [7, 1000, 70_000, 1 << 20] {
            let got = read_all(&frames, chunk, false).await;
            let got: Vec<Vec<u8>> = got.into_iter().map(|r| r.unwrap().to_vec()).collect();
            assert_eq!(got, frames, "chunk {chunk}");
            let got = read_all(&frames, chunk, true).await;
            assert_eq!(got.len(), 5, "chunk {chunk}");
            assert!(
                got[4].as_ref().unwrap_err().contains("UnexpectedEof"),
                "{:?}",
                got[4]
            );
        }
        // An invalid size is reported once the header is complete.
        let (mut tx, rx) = tokio::io::duplex(64);
        tx.write_all(&[0, 0, 0, 3, 0, 0, 0, 0]).await.unwrap();
        let mut reader = FrameReader::new(rx, 1 << 20);
        assert!(matches!(
            reader.next().await,
            Some(Err(FrameError::Size(3)))
        ));
        // A stream that ends at a frame boundary ends the reader.
        drop(tx);
        let (tx, rx) = tokio::io::duplex(64);
        drop(tx);
        let mut reader = FrameReader::new(rx, 1 << 20);
        assert!(reader.next().await.is_none());
    }

    /// One oversized frame does not pin its buffer: smaller large frames get
    /// their own, and the kept buffer is freed once no frame arrives.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn frame_reader_releases_buffers() {
        let frame = |len: usize| {
            let mut out = i32::try_from(len).unwrap().to_be_bytes().to_vec();
            out.resize(4 + len, 7);
            out
        };
        let (mut tx, rx) = tokio::io::duplex(1 << 20);
        let mut reader = FrameReader::new(rx, 64 << 20);
        let writer = mz_ore::task::spawn(|| "frame-writer", async move {
            tx.write_all(&frame(30 << 20)).await.unwrap();
            tx.write_all(&frame(100 << 10)).await.unwrap();
            tx
        });
        let huge = reader.next().await.unwrap().unwrap();
        assert_eq!(huge.len(), 30 << 20);
        drop(huge);
        let kept = reader.pool.pooled();
        assert!(kept >= 30 << 20, "{kept}");
        let small = reader.next().await.unwrap().unwrap();
        assert_eq!(small.len(), 100 << 10);
        assert_eq!(reader.pool.pooled(), kept, "the huge buffer was not reused");
        drop(small);
        let _tx = writer.await;
        // Nothing arrives: the kept buffers are freed while the reader waits.
        let idle = tokio::time::timeout(IDLE_RELEASE * 3, reader.next()).await;
        assert!(idle.is_err());
        assert_eq!(reader.pool.pooled(), 0);
    }

    #[mz_ore::test]
    fn decodes_frames() {
        let mut codec = FrameCodec::new(10);
        let mut buf = BytesMut::from(&[0u8, 0, 0, 5, 1, 2, 3][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&[4, 5, 0, 0]);
        assert_eq!(
            &codec.decode(&mut buf).unwrap().unwrap()[..],
            &[1, 2, 3, 4, 5]
        );
        assert_eq!(&buf[..], &[0, 0]);
        let mut big = BytesMut::from(&[0u8, 0, 0, 11, 0, 0, 0, 0][..]);
        assert!(matches!(codec.decode(&mut big), Err(FrameError::Size(11))));
        let mut neg = BytesMut::from(&[0xffu8, 0xff, 0xff, 0xff, 0, 0, 0, 0][..]);
        assert!(matches!(codec.decode(&mut neg), Err(FrameError::Size(-1))));
        let mut small = BytesMut::from(&[0u8, 0, 0, 3, 0, 0, 0, 0][..]);
        assert!(matches!(codec.decode(&mut small), Err(FrameError::Size(3))));
        // A TLS alert record (7 bytes) followed by EOF is a disconnect.
        let mut alert = BytesMut::from(&[0x15u8, 3, 3, 0, 2, 2, 70][..]);
        assert!(codec.decode(&mut alert).unwrap().is_none());
        assert!(matches!(
            codec.decode_eof(&mut alert),
            Err(FrameError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof
        ));
    }
}
