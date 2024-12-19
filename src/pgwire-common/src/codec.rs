// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Encoding/decoding of messages in pgwire. See "[Frontend/Backend Protocol:
//! Message Formats][1]" in the PostgreSQL reference for the specification.
//!
//! See the [crate docs](crate) for higher level concerns.
//!
//! [1]: https://www.postgresql.org/docs/11/protocol-message-formats.html

use std::collections::BTreeMap;
use std::error::Error;
use std::{fmt, str};

use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, BytesMut};
use mz_ore::cast::{u64_to_usize, CastFrom};
use mz_ore::netio::{self};
use tokio::io::{self, AsyncRead, AsyncReadExt};

use crate::format::Format;
use crate::message::{FrontendStartupMessage, VERSION_CANCEL, VERSION_GSSENC, VERSION_SSL};
use crate::FrontendMessage;

pub const REJECT_ENCRYPTION: u8 = b'N';
pub const ACCEPT_SSL_ENCRYPTION: u8 = b'S';

/// Maximum allowed size for a request.
pub const MAX_REQUEST_SIZE: usize = u64_to_usize(2 * bytesize::MB);

#[derive(Debug)]
pub enum CodecError {
    StringNoTerminator,
}

impl Error for CodecError {}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            CodecError::StringNoTerminator => "The string does not have a terminator",
        })
    }
}

pub trait Pgbuf: BufMut {
    fn put_string(&mut self, s: &str);
    fn put_length_i16(&mut self, len: usize) -> Result<(), io::Error>;
    fn put_format_i8(&mut self, format: Format);
    fn put_format_i16(&mut self, format: Format);
}

impl<B: BufMut> Pgbuf for B {
    fn put_string(&mut self, s: &str) {
        self.put(s.as_bytes());
        self.put_u8(b'\0');
    }

    fn put_length_i16(&mut self, len: usize) -> Result<(), io::Error> {
        let len = i16::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "length does not fit in an i16"))?;
        self.put_i16(len);
        Ok(())
    }

    fn put_format_i8(&mut self, format: Format) {
        self.put_i8(format.into())
    }

    fn put_format_i16(&mut self, format: Format) {
        self.put_i8(0);
        self.put_format_i8(format);
    }
}

pub async fn decode_startup<A>(mut conn: A) -> Result<Option<FrontendStartupMessage>, io::Error>
where
    A: AsyncRead + Unpin,
{
    let mut frame_len = [0; 4];
    let nread = netio::read_exact_or_eof(&mut conn, &mut frame_len).await?;
    match nread {
        // Complete frame length. Continue.
        4 => (),
        // Connection closed cleanly. Indicate that the startup sequence has
        // been terminated by the client.
        0 => return Ok(None),
        // Partial frame length. Likely a client bug or network glitch, so
        // surface the unexpected EOF.
        _ => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "early eof")),
    };
    let frame_len = parse_frame_len(&frame_len)?;

    let mut buf = BytesMut::new();
    buf.resize(frame_len, b'0');
    conn.read_exact(&mut buf).await?;

    let mut buf = Cursor::new(&buf);
    let version = buf.read_i32()?;
    let message = match version {
        VERSION_CANCEL => FrontendStartupMessage::CancelRequest {
            conn_id: buf.read_u32()?,
            secret_key: buf.read_u32()?,
        },
        VERSION_SSL => FrontendStartupMessage::SslRequest,
        VERSION_GSSENC => FrontendStartupMessage::GssEncRequest,
        _ => {
            let mut params = BTreeMap::new();
            while buf.peek_byte()? != 0 {
                let name = buf.read_cstr()?.to_owned();
                let value = buf.read_cstr()?.to_owned();
                params.insert(name, value);
            }
            FrontendStartupMessage::Startup { version, params }
        }
    };
    Ok(Some(message))
}

impl FrontendStartupMessage {
    /// Encodes self into dst.
    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), io::Error> {
        // Write message length placeholder. The true length is filled in later.
        let base = dst.len();
        dst.put_u32(0);

        // Write message contents.
        match self {
            FrontendStartupMessage::Startup { version, params } => {
                dst.put_i32(*version);
                for (k, v) in params {
                    dst.put_string(k);
                    dst.put_string(v);
                }
                dst.put_i8(0);
            }
            FrontendStartupMessage::CancelRequest {
                conn_id,
                secret_key,
            } => {
                dst.put_i32(VERSION_CANCEL);
                dst.put_u32(*conn_id);
                dst.put_u32(*secret_key);
            }
            FrontendStartupMessage::SslRequest {} => dst.put_i32(VERSION_SSL),
            _ => panic!("unsupported"),
        }

        let len = dst.len() - base;

        // Overwrite length placeholder with true length.
        let len = i32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "length of encoded message does not fit into an i32",
            )
        })?;
        dst[base..base + 4].copy_from_slice(&len.to_be_bytes());

        Ok(())
    }
}

impl FrontendMessage {
    /// Encodes self into dst.
    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), io::Error> {
        // Write type byte.
        let byte = match self {
            FrontendMessage::Password { .. } => b'p',
            _ => panic!("unsupported"),
        };
        dst.put_u8(byte);

        // Write message length placeholder. The true length is filled in later.
        let base = dst.len();
        dst.put_u32(0);

        // Write message contents.
        match self {
            FrontendMessage::Password { password } => {
                dst.put_string(password);
            }
            _ => panic!("unsupported"),
        }

        let len = dst.len() - base;

        // Overwrite length placeholder with true length.
        let len = i32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "length of encoded message does not fit into an i32",
            )
        })?;
        dst[base..base + 4].copy_from_slice(&len.to_be_bytes());

        Ok(())
    }
}

#[derive(Debug)]
pub enum DecodeState {
    Head,
    Data(u8, usize),
}

pub fn parse_frame_len(src: &[u8]) -> Result<usize, io::Error> {
    let n = usize::cast_from(NetworkEndian::read_u32(src));
    if n > netio::MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            netio::FrameTooBig,
        ));
    } else if n < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid frame length",
        ));
    }
    Ok(n - 4)
}

/// Decodes data within pgwire messages.
///
/// The API provided is very similar to [`bytes::Buf`], but operations return
/// errors rather than panicking. This is important for safety, as we don't want
/// to crash if the user sends us malformed pgwire messages.
///
/// There are also some special-purpose methods, like [`Cursor::read_cstr`],
/// that are specific to pgwire messages.
#[derive(Debug)]
pub struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    /// Constructs a new `Cursor` from a byte slice. The cursor will begin
    /// decoding from the beginning of the slice.
    pub fn new(buf: &'a [u8]) -> Cursor<'a> {
        Cursor { buf }
    }

    /// Returns the next byte without advancing the cursor.
    pub fn peek_byte(&self) -> Result<u8, io::Error> {
        self.buf
            .get(0)
            .copied()
            .ok_or_else(|| input_err("No byte to read"))
    }

    /// Returns the next byte, advancing the cursor by one byte.
    pub fn read_byte(&mut self) -> Result<u8, io::Error> {
        let byte = self.peek_byte()?;
        self.advance(1);
        Ok(byte)
    }

    /// Returns the next null-terminated string. The null character is not
    /// included the returned string. The cursor is advanced past the null-
    /// terminated string.
    ///
    /// If there is no null byte remaining in the string, returns
    /// `CodecError::StringNoTerminator`. If the string is not valid UTF-8,
    /// returns an `io::Error` with an error kind of
    /// `io::ErrorKind::InvalidInput`.
    ///
    /// NOTE(benesch): it is possible that returning a string here is wrong, and
    /// we should be returning bytes, so that we can support messages that are
    /// not UTF-8 encoded. At the moment, we've not discovered a need for this,
    /// though, and using proper strings is convenient.
    pub fn read_cstr(&mut self) -> Result<&'a str, io::Error> {
        if let Some(pos) = self.buf.iter().position(|b| *b == 0) {
            let val = std::str::from_utf8(&self.buf[..pos]).map_err(input_err)?;
            self.advance(pos + 1);
            Ok(val)
        } else {
            Err(input_err(CodecError::StringNoTerminator))
        }
    }

    /// Reads the next 16-bit signed integer, advancing the cursor by two
    /// bytes.
    pub fn read_i16(&mut self) -> Result<i16, io::Error> {
        if self.buf.len() < 2 {
            return Err(input_err("not enough buffer for an Int16"));
        }
        let val = NetworkEndian::read_i16(self.buf);
        self.advance(2);
        Ok(val)
    }

    /// Reads the next 32-bit signed integer, advancing the cursor by four
    /// bytes.
    pub fn read_i32(&mut self) -> Result<i32, io::Error> {
        if self.buf.len() < 4 {
            return Err(input_err("not enough buffer for an Int32"));
        }
        let val = NetworkEndian::read_i32(self.buf);
        self.advance(4);
        Ok(val)
    }

    /// Reads the next 32-bit unsigned integer, advancing the cursor by four
    /// bytes.
    pub fn read_u32(&mut self) -> Result<u32, io::Error> {
        if self.buf.len() < 4 {
            return Err(input_err("not enough buffer for an Int32"));
        }
        let val = NetworkEndian::read_u32(self.buf);
        self.advance(4);
        Ok(val)
    }

    /// Reads the next 16-bit format code, advancing the cursor by two bytes.
    pub fn read_format(&mut self) -> Result<Format, io::Error> {
        Format::try_from(self.read_i16()?)
    }

    /// Advances the cursor by `n` bytes.
    pub fn advance(&mut self, n: usize) {
        self.buf = &self.buf[n..]
    }
}

/// Constructs an error indicating that the client has violated the pgwire
/// protocol.
pub fn input_err(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, source.into())
}
