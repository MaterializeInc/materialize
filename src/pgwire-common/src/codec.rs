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
use mz_ore::cast::{CastFrom, u64_to_usize};
use mz_ore::netio::{self};
use tokio::io::{self, AsyncRead, AsyncReadExt};

use crate::FrontendMessage;
use crate::format::Format;
use crate::message::{FrontendStartupMessage, VERSION_CANCEL, VERSION_GSSENC, VERSION_SSL};

pub const REJECT_ENCRYPTION: u8 = b'N';
pub const ACCEPT_SSL_ENCRYPTION: u8 = b'S';

/// Maximum allowed size for a request.
pub const MAX_REQUEST_SIZE: usize = u64_to_usize(2 * bytesize::MB);

/// Maximum size of a startup frame accepted directly from a client.
///
/// Matches PostgreSQL's `MAX_STARTUP_PACKET_LENGTH`, so any client that can
/// complete a PostgreSQL handshake can complete this one. A startup frame
/// carries only the protocol version and the connection parameters, of which
/// `options` is the only one a client can make large.
pub const MAX_STARTUP_FRAME_SIZE: usize = 10_000;

/// Startup budget allowed on top of [`MAX_STARTUP_FRAME_SIZE`] for parameters a
/// balancer appends while forwarding.
///
/// A balancer adds [`CONN_UUID_KEY`] and [`MZ_FORWARDED_FOR_KEY`] to the
/// parameters before forwarding startup, so a frame that just fits the client
/// budget arrives downstream larger than the client sent it. Without this
/// allowance such a connection is accepted by the balancer and then rejected
/// behind it, which surfaces as a proxy error with no obvious cause.
///
/// The two parameters cost at most 119 bytes: an 18-byte key with a 36-byte
/// UUID, a 16-byte key with an address of up to 45 bytes, and a NUL after each
/// of the four strings.
///
/// [`CONN_UUID_KEY`]: crate::CONN_UUID_KEY
/// [`MZ_FORWARDED_FOR_KEY`]: crate::MZ_FORWARDED_FOR_KEY
pub const FORWARDED_STARTUP_PARAM_ALLOWANCE: usize = 128;

/// Maximum size of a startup frame accepted from a client that may be behind a
/// balancer.
pub const MAX_FORWARDED_STARTUP_FRAME_SIZE: usize =
    MAX_STARTUP_FRAME_SIZE + FORWARDED_STARTUP_PARAM_ALLOWANCE;

/// Maximum frame size accepted from a client that has not yet authenticated.
///
/// The only frames a client legitimately sends before authenticating are
/// credentials: a password, or one leg of a SASL exchange. This sits far above
/// any of those while leaving room for a bearer token, which can run to several
/// kilobytes.
pub const MAX_PREAUTH_FRAME_SIZE: usize = u64_to_usize(16 * bytesize::KIB);

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
    fn put_length_u16(&mut self, len: usize) -> Result<(), io::Error>;
    fn put_format_i8(&mut self, format: Format);
    fn put_format_i16(&mut self, format: Format);
}

impl<B: BufMut> Pgbuf for B {
    fn put_string(&mut self, s: &str) {
        self.put(s.as_bytes());
        self.put_u8(b'\0');
    }

    fn put_length_i16(&mut self, len: usize) -> Result<(), io::Error> {
        let len = i16::try_from(len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "length does not fit in an i16")
        })?;
        self.put_i16(len);
        Ok(())
    }

    /// Writes a count field as unsigned, so it may exceed 32767. The protocol
    /// calls these fields `Int16`, but PostgreSQL clients decode them as
    /// unsigned.
    fn put_length_u16(&mut self, len: usize) -> Result<(), io::Error> {
        let len = u16::try_from(len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "length does not fit in a u16")
        })?;
        self.put_u16(len);
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

/// Reads and decodes one startup message from the client.
///
/// `max_frame_len` bounds the frame the client may declare, including its own
/// four-byte length field. It is checked before any buffer is sized from it, so
/// an unauthenticated peer cannot make the server commit memory by declaring a
/// large frame it never sends. Callers on a plaintext, pre-authentication
/// socket should pass [`MAX_STARTUP_FRAME_SIZE`], or
/// [`MAX_FORWARDED_STARTUP_FRAME_SIZE`] if a balancer may have appended
/// parameters in transit.
pub async fn decode_startup<A>(
    mut conn: A,
    max_frame_len: usize,
) -> Result<Option<FrontendStartupMessage>, io::Error>
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
    let frame_len = parse_frame_len(&frame_len, max_frame_len)?;

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
            FrontendStartupMessage::GssEncRequest => panic!("unsupported"),
        }

        let len = dst.len() - base;

        // Overwrite length placeholder with true length.
        let len = i32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
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
                io::ErrorKind::InvalidData,
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

/// Parses a frame length header, rejecting frames larger than `max_frame_len`.
///
/// The ceiling is the caller's rather than a single global one, because the
/// paths that parse frame lengths have requirements that differ by orders of
/// magnitude: a startup frame is well under 10 KB, a pre-authentication
/// credential exchange needs a few kilobytes, and only post-authentication
/// query traffic needs room for bulk data. Stating it per call site keeps a
/// buffer from ever being sized against a bound that belongs to a different
/// path.
///
/// `max_frame_len` counts the frame including its own four-byte length field,
/// matching the number the client declares. [`netio::MAX_FRAME_SIZE`] is the
/// protocol ceiling; callers pass that or less.
pub fn parse_frame_len(src: &[u8], max_frame_len: usize) -> Result<usize, io::Error> {
    let n = usize::cast_from(NetworkEndian::read_u32(src));
    if n > max_frame_len {
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

    /// Reads the next 16-bit unsigned integer, advancing the cursor by two
    /// bytes.
    pub fn read_u16(&mut self) -> Result<u16, io::Error> {
        if self.buf.len() < 2 {
            return Err(input_err("not enough buffer for an Int16"));
        }
        let val = NetworkEndian::read_u16(self.buf);
        self.advance(2);
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
        Format::try_from(self.read_u16()?)
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

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use crate::conn::{CONN_UUID_KEY, MZ_FORWARDED_FOR_KEY};
    use crate::message::VERSION_3;
    use tokio::io::ReadBuf;

    use super::*;

    /// The buffer the server offered the client to fill.
    ///
    /// The server can only hand out a slice it has already sized, so an offer is
    /// evidence that the declared length was trusted before the body arrived,
    /// and its absence is evidence that the frame was rejected first.
    #[derive(Debug)]
    struct Offer {
        len: usize,
    }

    /// A client that writes a fixed prefix and then goes silent forever.
    ///
    /// Going silent means returning `Poll::Pending` without registering a waker,
    /// so nothing can ever resume the read. Any stall is therefore a property of
    /// the protocol handling, not a scheduling artifact.
    struct SilentClient {
        prefix: Vec<u8>,
        sent: usize,
        offer: Option<Offer>,
    }

    impl AsyncRead for SilentClient {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let unsent = self.prefix.len() - self.sent;
            if unsent > 0 {
                let n = std::cmp::min(unsent, buf.remaining());
                let start = self.sent;
                buf.put_slice(&self.prefix[start..start + n]);
                self.sent += n;
                return Poll::Ready(Ok(()));
            }
            self.offer = Some(Offer {
                len: buf.remaining(),
            });
            Poll::Pending
        }
    }

    struct Attempt {
        offer: Option<Offer>,
        /// `None` if `decode_startup` was still waiting an hour on.
        result: Option<Result<Option<FrontendStartupMessage>, io::Error>>,
    }

    /// Feeds `frame` to `decode_startup` under `max_frame_len`, then goes silent
    /// and lets an hour of (virtual) time pass.
    async fn attempt(frame: &[u8], max_frame_len: usize) -> Attempt {
        let mut client = SilentClient {
            prefix: frame.to_vec(),
            sent: 0,
            offer: None,
        };
        // The runtime has nothing to run once the client goes silent, so the
        // paused clock jumps straight to the deadline and the hour is free.
        let result = tokio::time::timeout(
            Duration::from_secs(60 * 60),
            decode_startup(&mut client, max_frame_len),
        )
        .await;
        Attempt {
            offer: client.offer,
            result: result.ok(),
        }
    }

    /// Startup parameters whose encoded frame is exactly `frame_len` bytes.
    fn params_sized_to(frame_len: usize) -> BTreeMap<String, String> {
        // Length, version, the key and its NUL, the value's NUL, terminator.
        let overhead = 4 + 4 + "options".len() + 1 + 1 + 1;
        BTreeMap::from([("options".to_string(), "x".repeat(frame_len - overhead))])
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn test_startup_frame_over_budget_is_rejected_before_allocating() {
        for declared in [MAX_STARTUP_FRAME_SIZE + 1, 1 << 20, netio::MAX_FRAME_SIZE] {
            let header = u32::try_from(declared)
                .expect("fits in a frame-length field")
                .to_be_bytes();
            let attempt = attempt(&header, MAX_STARTUP_FRAME_SIZE).await;

            let err = attempt
                .result
                .expect("decode_startup stalled instead of rejecting the frame")
                .expect_err("oversized startup frame was accepted");
            assert_eq!(
                err.kind(),
                io::ErrorKind::InvalidData,
                "declared {declared}"
            );
            assert_eq!(
                attempt.offer.as_ref().map(|offer| offer.len),
                None,
                "declared {declared}: a buffer was sized before the frame was rejected",
            );
        }
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn test_startup_frame_within_budget_is_accepted() {
        let params = params_sized_to(MAX_STARTUP_FRAME_SIZE);
        let mut frame = BytesMut::new();
        FrontendStartupMessage::Startup {
            version: VERSION_3,
            params: params.clone(),
        }
        .encode(&mut frame)
        .expect("encodes");
        assert_eq!(frame.len(), MAX_STARTUP_FRAME_SIZE);

        let message = attempt(&frame, MAX_STARTUP_FRAME_SIZE)
            .await
            .result
            .expect("decode_startup stalled on a complete frame")
            .expect("a startup frame at the budget was rejected");
        match message {
            Some(FrontendStartupMessage::Startup {
                version,
                params: decoded,
            }) => {
                assert_eq!(version, VERSION_3);
                assert_eq!(decoded, params);
            }
            other => panic!("expected a startup message, got {other:?}"),
        }
    }

    /// A balancer appends two parameters while forwarding, so a frame that just
    /// fits the client budget grows in transit. The downstream bound has to
    /// cover the difference, or the connection is accepted by the balancer and
    /// rejected behind it.
    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn test_forwarded_startup_params_fit_the_allowance() {
        // Widest values the two parameters can carry: a hyphenated UUID, and an
        // IPv4-mapped IPv6 address in its longest textual form.
        const WIDEST_UUID: &str = "00000000-0000-0000-0000-000000000000";
        const WIDEST_ADDR: &str = "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255";

        let mut params = params_sized_to(MAX_STARTUP_FRAME_SIZE);
        params.insert(CONN_UUID_KEY.to_string(), WIDEST_UUID.to_string());
        params.insert(MZ_FORWARDED_FOR_KEY.to_string(), WIDEST_ADDR.to_string());

        let mut forwarded = BytesMut::new();
        FrontendStartupMessage::Startup {
            version: VERSION_3,
            params,
        }
        .encode(&mut forwarded)
        .expect("encodes");

        assert!(
            forwarded.len() <= MAX_FORWARDED_STARTUP_FRAME_SIZE,
            "FORWARDED_STARTUP_PARAM_ALLOWANCE is too small: {} bytes forwarded              against a {MAX_FORWARDED_STARTUP_FRAME_SIZE} byte bound",
            forwarded.len(),
        );
        assert!(
            attempt(&forwarded, MAX_FORWARDED_STARTUP_FRAME_SIZE)
                .await
                .result
                .expect("decode_startup stalled on a complete frame")
                .is_ok(),
        );
    }

    /// The startup path still has no deadline, so a client that declares a frame
    /// within budget and then stops sending holds the connection, and whatever
    /// was sized for it, indefinitely. Bounding the frame caps what one such
    /// connection costs, not how many of them one peer may hold.
    ///
    /// TODO(CLO-272): assert a deadline here once the pre-startup limit lands.
    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn test_startup_body_wait_has_no_deadline() {
        let header = u32::try_from(MAX_STARTUP_FRAME_SIZE)
            .expect("fits in a frame-length field")
            .to_be_bytes();
        let attempt = attempt(&header, MAX_STARTUP_FRAME_SIZE).await;

        assert!(
            attempt.result.is_none(),
            "an hour on, decode_startup is still waiting for a body that never arrives",
        );
        assert_eq!(
            attempt.offer.as_ref().map(|offer| offer.len),
            Some(MAX_STARTUP_FRAME_SIZE - 4),
            "the wait is now bounded by the startup budget rather than MAX_FRAME_SIZE",
        );
    }
}
