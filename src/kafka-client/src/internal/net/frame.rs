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

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Decoder;

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
        if src.len() < FRAME_LEN_SIZE {
            return Ok(None);
        }
        let size = i32::from_be_bytes(src[..FRAME_LEN_SIZE].try_into().expect("4 bytes"));
        let size = check_size(size, self.max_size)?;
        if src.len() < FRAME_LEN_SIZE + size {
            src.reserve(FRAME_LEN_SIZE + size - src.len());
            return Ok(None);
        }
        src.advance(FRAME_LEN_SIZE);
        Ok(Some(src.split_to(size).freeze()))
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

/// Reads one frame from `io`, for the sequential connection setup phase.
pub(crate) async fn read_frame<S: AsyncRead + Unpin>(
    io: &mut S,
    max_size: usize,
) -> Result<Bytes, FrameError> {
    let mut len = [0u8; FRAME_LEN_SIZE];
    io.read_exact(&mut len).await?;
    let size = check_size(i32::from_be_bytes(len), max_size)?;
    let mut buf = vec![0u8; size];
    io.read_exact(&mut buf).await?;
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
        let mut big = BytesMut::from(&[0u8, 0, 0, 11][..]);
        assert!(matches!(codec.decode(&mut big), Err(FrameError::Size(11))));
        let mut neg = BytesMut::from(&[0xffu8, 0xff, 0xff, 0xff][..]);
        assert!(matches!(codec.decode(&mut neg), Err(FrameError::Size(-1))));
        let mut small = BytesMut::from(&[0u8, 0, 0, 3][..]);
        assert!(matches!(codec.decode(&mut small), Err(FrameError::Size(3))));
    }
}
