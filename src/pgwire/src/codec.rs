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

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::str;

use async_trait::async_trait;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, BytesMut};
use futures::{sink, SinkExt, TryStreamExt};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, Interest, Ready};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::trace;

use mz_ore::cast::CastFrom;
use mz_ore::future::OreSinkExt;
use mz_ore::netio::{self, AsyncReady};

use crate::message::{
    BackendMessage, ErrorResponse, FrontendMessage, FrontendStartupMessage, TransactionStatus,
    VERSION_CANCEL, VERSION_GSSENC, VERSION_SSL,
};
use crate::server::Conn;

pub const REJECT_ENCRYPTION: u8 = b'N';
pub const ACCEPT_SSL_ENCRYPTION: u8 = b'S';

#[derive(Debug)]
enum CodecError {
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

/// A connection that manages the encoding and decoding of pgwire frames.
pub struct FramedConn<A> {
    conn_id: u32,
    inner: sink::Buffer<Framed<Conn<A>, Codec>, BackendMessage>,
}

impl<A> FramedConn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    /// Constructs a new framed connection.
    ///
    /// The underlying connection, `inner`, is expected to be something like a
    /// TCP stream. Anything that implements [`AsyncRead`] and [`AsyncWrite`]
    /// will do.
    ///
    /// The supplied `conn_id` is used to identify the connection in logging
    /// messages.
    pub fn new(conn_id: u32, inner: Conn<A>) -> FramedConn<A> {
        FramedConn {
            conn_id,
            inner: Framed::new(inner, Codec::new()).buffer(32),
        }
    }

    /// Returns the ID of this connection.
    pub fn id(&self) -> u32 {
        self.conn_id
    }

    /// Reads and decodes one frontend message from the client.
    ///
    /// Blocks until the client sends a complete message. If the client
    /// terminates the stream, returns `None`. Returns an error if the client
    /// sends a malformed message or if the connection underlying is broken.
    pub async fn recv(&mut self) -> Result<Option<FrontendMessage>, io::Error> {
        let message = self.inner.try_next().await?;
        match &message {
            Some(message) => trace!("cid={} recv={:?}", self.conn_id, message),
            None => trace!("cid={} recv=<eof>", self.conn_id),
        }
        Ok(message)
    }

    /// Encodes and sends one backend message to the client.
    ///
    /// Note that the connection is not flushed after calling this method. You
    /// must call [`FramedConn::flush`] explicitly. Returns an error if the
    /// underlying connection is broken.
    ///
    /// Please use `StateMachine::send` instead if calling from `StateMachine`,
    /// as it applies session-based filters before calling this method.
    pub async fn send<M>(&mut self, message: M) -> Result<(), io::Error>
    where
        M: Into<BackendMessage>,
    {
        let message = message.into();
        trace!("cid={} send={:?}", self.conn_id, message);
        self.inner.enqueue(message).await
    }

    /// Encodes and sends the backend messages in the `messages` iterator to the
    /// client.
    ///
    /// As with [`FramedConn::send`], the connection is not flushed after
    /// calling this method. You must call [`FramedConn::flush`] explicitly.
    /// Returns an error if the underlying connection is broken.
    pub async fn send_all(
        &mut self,
        messages: impl IntoIterator<Item = BackendMessage>,
    ) -> Result<(), io::Error> {
        // N.B. we intentionally don't use `self.conn.send_all` here to avoid
        // flushing the sink unnecessarily.
        for m in messages {
            self.send(m).await?;
        }
        Ok(())
    }

    /// Flushes all outstanding messages.
    pub async fn flush(&mut self) -> Result<(), io::Error> {
        self.inner.flush().await
    }

    /// Injects state that affects how certain backend messages are encoded.
    ///
    /// Specifically, the encoding of `BackendMessage::DataRow` depends upon the
    /// types of the datums in the row. To avoid including the same type
    /// information in each message, we use this side channel to install the
    /// type information in the codec before sending any data row messages. This
    /// violates the abstraction boundary a bit but results in much better
    /// performance.
    pub fn set_encode_state(&mut self, encode_state: Vec<(mz_pgrepr::Type, mz_pgrepr::Format)>) {
        self.inner.get_mut().codec_mut().encode_state = encode_state;
    }
}

impl<A> FramedConn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    pub fn inner(&self) -> &Conn<A> {
        self.inner.get_ref().get_ref()
    }
}

#[async_trait]
impl<A> AsyncReady for FramedConn<A>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
{
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.inner.get_ref().get_ref().ready(interest).await
    }
}

struct Codec {
    decode_state: DecodeState,
    encode_state: Vec<(mz_pgrepr::Type, mz_pgrepr::Format)>,
}

impl Codec {
    /// Creates a new `Codec`.
    pub fn new() -> Codec {
        Codec {
            decode_state: DecodeState::Head,
            encode_state: vec![],
        }
    }
}

impl Default for Codec {
    fn default() -> Codec {
        Codec::new()
    }
}

impl Encoder<BackendMessage> for Codec {
    type Error = io::Error;

    fn encode(&mut self, msg: BackendMessage, dst: &mut BytesMut) -> Result<(), io::Error> {
        // Write type byte.
        let byte = match &msg {
            BackendMessage::AuthenticationOk => b'R',
            BackendMessage::AuthenticationCleartextPassword => b'R',
            BackendMessage::RowDescription(_) => b'T',
            BackendMessage::DataRow(_) => b'D',
            BackendMessage::CommandComplete { .. } => b'C',
            BackendMessage::EmptyQueryResponse => b'I',
            BackendMessage::ReadyForQuery(_) => b'Z',
            BackendMessage::NoData => b'n',
            BackendMessage::ParameterStatus(_, _) => b'S',
            BackendMessage::PortalSuspended => b's',
            BackendMessage::BackendKeyData { .. } => b'K',
            BackendMessage::ParameterDescription(_) => b't',
            BackendMessage::ParseComplete => b'1',
            BackendMessage::BindComplete => b'2',
            BackendMessage::CloseComplete => b'3',
            BackendMessage::ErrorResponse(r) => {
                if r.severity.is_error() {
                    b'E'
                } else {
                    b'N'
                }
            }
            BackendMessage::CopyInResponse { .. } => b'G',
            BackendMessage::CopyOutResponse { .. } => b'H',
            BackendMessage::CopyData(_) => b'd',
            BackendMessage::CopyDone => b'c',
        };
        dst.put_u8(byte);

        // Write message length placeholder. The true length is filled in later.
        let base = dst.len();
        dst.put_u32(0);

        // Write message contents.
        match msg {
            BackendMessage::CopyInResponse {
                overall_format,
                column_formats,
            }
            | BackendMessage::CopyOutResponse {
                overall_format,
                column_formats,
            } => {
                dst.put_format_i8(overall_format);
                dst.put_length_i16(column_formats.len())?;
                for format in column_formats {
                    dst.put_format_i16(format);
                }
            }
            BackendMessage::CopyData(data) => {
                dst.put_slice(&data);
            }
            BackendMessage::CopyDone => (),
            BackendMessage::AuthenticationOk => {
                dst.put_u32(0);
            }
            BackendMessage::AuthenticationCleartextPassword => {
                dst.put_u32(3);
            }
            BackendMessage::RowDescription(fields) => {
                dst.put_length_i16(fields.len())?;
                for f in &fields {
                    dst.put_string(&f.name.to_string());
                    dst.put_u32(f.table_id);
                    dst.put_u16(f.column_id);
                    dst.put_u32(f.type_oid);
                    dst.put_i16(f.type_len);
                    dst.put_i32(f.type_mod);
                    // TODO: make the format correct
                    dst.put_format_i16(f.format);
                }
            }
            BackendMessage::DataRow(fields) => {
                dst.put_length_i16(fields.len())?;
                for (f, (ty, format)) in fields.iter().zip(&self.encode_state) {
                    if let Some(f) = f {
                        let base = dst.len();
                        dst.put_u32(0);
                        f.encode(ty, *format, dst)?;
                        let len = dst.len() - base - 4;
                        let len = i32::try_from(len).map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "length of encoded data row field does not fit into an i32",
                            )
                        })?;
                        dst[base..base + 4].copy_from_slice(&len.to_be_bytes());
                    } else {
                        dst.put_i32(-1);
                    }
                }
            }
            BackendMessage::CommandComplete { tag } => {
                dst.put_string(&tag);
            }
            BackendMessage::ParseComplete => (),
            BackendMessage::BindComplete => (),
            BackendMessage::CloseComplete => (),
            BackendMessage::EmptyQueryResponse => (),
            BackendMessage::ReadyForQuery(status) => {
                dst.put_u8(match status {
                    TransactionStatus::Idle => b'I',
                    TransactionStatus::InTransaction => b'T',
                    TransactionStatus::Failed => b'E',
                });
            }
            BackendMessage::ParameterStatus(name, value) => {
                dst.put_string(name);
                dst.put_string(&value);
            }
            BackendMessage::PortalSuspended => (),
            BackendMessage::NoData => (),
            BackendMessage::BackendKeyData {
                conn_id,
                secret_key,
            } => {
                dst.put_u32(conn_id);
                dst.put_u32(secret_key);
            }
            BackendMessage::ParameterDescription(params) => {
                dst.put_length_i16(params.len())?;
                for param in params {
                    dst.put_u32(param.oid());
                }
            }
            BackendMessage::ErrorResponse(ErrorResponse {
                severity,
                code,
                message,
                detail,
                hint,
                position,
            }) => {
                dst.put_u8(b'S');
                dst.put_string(severity.as_str());
                dst.put_u8(b'C');
                dst.put_string(code.code());
                dst.put_u8(b'M');
                dst.put_string(&message);
                if let Some(detail) = &detail {
                    dst.put_u8(b'D');
                    dst.put_string(detail);
                }
                if let Some(hint) = &hint {
                    dst.put_u8(b'H');
                    dst.put_string(hint);
                }
                if let Some(position) = &position {
                    dst.put_u8(b'P');
                    dst.put_string(&position.to_string());
                }
                dst.put_u8(b'\0');
            }
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

trait Pgbuf: BufMut {
    fn put_string(&mut self, s: &str);
    fn put_length_i16(&mut self, len: usize) -> Result<(), io::Error>;
    fn put_format_i8(&mut self, format: mz_pgrepr::Format);
    fn put_format_i16(&mut self, format: mz_pgrepr::Format);
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

    fn put_format_i8(&mut self, format: mz_pgrepr::Format) {
        self.put_i8(match format {
            mz_pgrepr::Format::Text => 0,
            mz_pgrepr::Format::Binary => 1,
        })
    }

    fn put_format_i16(&mut self, format: mz_pgrepr::Format) {
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
            let mut params = HashMap::new();
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

#[derive(Debug)]
enum DecodeState {
    Head,
    Data(u8, usize),
}

fn parse_frame_len(src: &[u8]) -> Result<usize, io::Error> {
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

impl Decoder for Codec {
    type Item = FrontendMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<FrontendMessage>, io::Error> {
        loop {
            match self.decode_state {
                DecodeState::Head => {
                    if src.len() < 5 {
                        return Ok(None);
                    }
                    let msg_type = src[0];
                    let frame_len = parse_frame_len(&src[1..])?;
                    src.advance(5);
                    src.reserve(frame_len);
                    self.decode_state = DecodeState::Data(msg_type, frame_len);
                }

                DecodeState::Data(msg_type, frame_len) => {
                    if src.len() < frame_len {
                        return Ok(None);
                    }
                    let buf = src.split_to(frame_len).freeze();
                    let buf = Cursor::new(&buf);
                    let msg = match msg_type {
                        // Simple query flow.
                        b'Q' => decode_query(buf)?,

                        // Extended query flow.
                        b'P' => decode_parse(buf)?,
                        b'D' => decode_describe(buf)?,
                        b'B' => decode_bind(buf)?,
                        b'E' => decode_execute(buf)?,
                        b'H' => decode_flush(buf)?,
                        b'S' => decode_sync(buf)?,
                        b'C' => decode_close(buf)?,

                        // Termination.
                        b'X' => decode_terminate(buf)?,

                        // Authentication.
                        b'p' => decode_password(buf)?,

                        // Copy from flow.
                        b'f' => decode_copy_fail(buf)?,
                        b'd' => decode_copy_data(buf, frame_len)?,
                        b'c' => decode_copy_done(buf)?,

                        // Invalid.
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("unknown message type {}", msg_type),
                            ));
                        }
                    };
                    src.reserve(5);
                    self.decode_state = DecodeState::Head;
                    return Ok(Some(msg));
                }
            }
        }
    }
}

fn decode_terminate(mut _buf: Cursor) -> Result<FrontendMessage, io::Error> {
    // Nothing more to decode.
    Ok(FrontendMessage::Terminate)
}

fn decode_password(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    Ok(FrontendMessage::Password {
        password: buf.read_cstr()?.to_owned(),
    })
}

fn decode_query(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    Ok(FrontendMessage::Query {
        sql: buf.read_cstr()?.to_string(),
    })
}

fn decode_parse(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let name = buf.read_cstr()?;
    let sql = buf.read_cstr()?;

    let mut param_types = vec![];
    for _ in 0..buf.read_i16()? {
        param_types.push(buf.read_u32()?);
    }

    Ok(FrontendMessage::Parse {
        name: name.into(),
        sql: sql.into(),
        param_types,
    })
}

fn decode_close(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    match buf.read_byte()? {
        b'S' => Ok(FrontendMessage::CloseStatement {
            name: buf.read_cstr()?.to_owned(),
        }),
        b'P' => Ok(FrontendMessage::ClosePortal {
            name: buf.read_cstr()?.to_owned(),
        }),
        b => Err(input_err(format!(
            "invalid type byte in close message: {}",
            b
        ))),
    }
}

fn decode_describe(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let first_char = buf.read_byte()?;
    let name = buf.read_cstr()?.to_string();
    match first_char {
        b'S' => Ok(FrontendMessage::DescribeStatement { name }),
        b'P' => Ok(FrontendMessage::DescribePortal { name }),
        other => Err(input_err(format!("Invalid describe type: {:#x?}", other))),
    }
}

fn decode_bind(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let portal_name = buf.read_cstr()?.to_string();
    let statement_name = buf.read_cstr()?.to_string();

    let mut param_formats = Vec::new();
    for _ in 0..buf.read_i16()? {
        param_formats.push(buf.read_format()?);
    }

    let mut raw_params = Vec::new();
    for _ in 0..buf.read_i16()? {
        let len = buf.read_i32()?;
        if len == -1 {
            raw_params.push(None); // NULL
        } else {
            // TODO(benesch): this should use bytes::Bytes to avoid the copy.
            let mut value = Vec::new();
            for _ in 0..len {
                value.push(buf.read_byte()?);
            }
            raw_params.push(Some(value));
        }
    }

    let mut result_formats = Vec::new();
    for _ in 0..buf.read_i16()? {
        result_formats.push(buf.read_format()?);
    }

    Ok(FrontendMessage::Bind {
        portal_name,
        statement_name,
        param_formats,
        raw_params,
        result_formats,
    })
}

fn decode_execute(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let portal_name = buf.read_cstr()?.to_string();
    let max_rows = buf.read_i32()?;
    Ok(FrontendMessage::Execute {
        portal_name,
        max_rows,
    })
}

fn decode_flush(mut _buf: Cursor) -> Result<FrontendMessage, io::Error> {
    // Nothing more to decode.
    Ok(FrontendMessage::Flush)
}

fn decode_sync(mut _buf: Cursor) -> Result<FrontendMessage, io::Error> {
    // Nothing more to decode.
    Ok(FrontendMessage::Sync)
}

fn decode_copy_data(mut buf: Cursor, frame_len: usize) -> Result<FrontendMessage, io::Error> {
    let mut data = Vec::with_capacity(frame_len);
    for _ in 0..frame_len {
        data.push(buf.read_byte()?);
    }
    Ok(FrontendMessage::CopyData(data))
}

fn decode_copy_done(mut _buf: Cursor) -> Result<FrontendMessage, io::Error> {
    // Nothing more to decode.
    Ok(FrontendMessage::CopyDone)
}

fn decode_copy_fail(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    Ok(FrontendMessage::CopyFail(buf.read_cstr()?.to_string()))
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
struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    /// Constructs a new `Cursor` from a byte slice. The cursor will begin
    /// decoding from the beginning of the slice.
    fn new(buf: &'a [u8]) -> Cursor {
        Cursor { buf }
    }

    /// Returns the next byte without advancing the cursor.
    fn peek_byte(&self) -> Result<u8, io::Error> {
        self.buf
            .get(0)
            .copied()
            .ok_or_else(|| input_err("No byte to read"))
    }

    /// Returns the next byte, advancing the cursor by one byte.
    fn read_byte(&mut self) -> Result<u8, io::Error> {
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
    fn read_cstr(&mut self) -> Result<&'a str, io::Error> {
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
    fn read_i16(&mut self) -> Result<i16, io::Error> {
        if self.buf.len() < 2 {
            return Err(input_err("not enough buffer for an Int16"));
        }
        let val = NetworkEndian::read_i16(self.buf);
        self.advance(2);
        Ok(val)
    }

    /// Reads the next 32-bit signed integer, advancing the cursor by four
    /// bytes.
    fn read_i32(&mut self) -> Result<i32, io::Error> {
        if self.buf.len() < 4 {
            return Err(input_err("not enough buffer for an Int32"));
        }
        let val = NetworkEndian::read_i32(self.buf);
        self.advance(4);
        Ok(val)
    }

    /// Reads the next 32-bit unsigned integer, advancing the cursor by four
    /// bytes.
    fn read_u32(&mut self) -> Result<u32, io::Error> {
        if self.buf.len() < 4 {
            return Err(input_err("not enough buffer for an Int32"));
        }
        let val = NetworkEndian::read_u32(self.buf);
        self.advance(4);
        Ok(val)
    }

    /// Reads the next 16-bit format code, advancing the cursor by two bytes.
    fn read_format(&mut self) -> Result<mz_pgrepr::Format, io::Error> {
        match self.read_i16()? {
            0 => Ok(mz_pgrepr::Format::Text),
            1 => Ok(mz_pgrepr::Format::Binary),
            n => Err(input_err(format!("unknown format code: {}", n))),
        }
    }

    /// Advances the cursor by `n` bytes.
    fn advance(&mut self, n: usize) {
        self.buf = &self.buf[n..]
    }
}

/// Constructs an error indicating that the client has violated the pgwire
/// protocol.
fn input_err(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, source.into())
}
