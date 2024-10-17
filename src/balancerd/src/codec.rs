// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use bytesize::ByteSize;
use futures::{sink, SinkExt, TryStreamExt};
use mz_ore::cast::CastFrom;
use mz_ore::future::OreSinkExt;
use mz_ore::netio::AsyncReady;
use mz_pgwire_common::{
    parse_frame_len, Conn, Cursor, DecodeState, ErrorResponse, FrontendMessage, Pgbuf,
    MAX_REQUEST_SIZE,
};
use tokio::io::{self, AsyncRead, AsyncWrite, Interest, Ready};
use tokio_util::codec::{Decoder, Encoder, Framed};

/// Internal representation of a backend [message].
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationCleartextPassword,
    ErrorResponse(ErrorResponse),
}

impl From<ErrorResponse> for BackendMessage {
    fn from(err: ErrorResponse) -> BackendMessage {
        BackendMessage::ErrorResponse(err)
    }
}

/// A connection that manages the encoding and decoding of pgwire frames.
pub struct FramedConn<A> {
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
    pub fn new(inner: Conn<A>) -> FramedConn<A> {
        FramedConn {
            inner: Framed::new(inner, Codec::new()).buffer(32),
        }
    }

    /// Reads and decodes one frontend message from the client.
    ///
    /// Blocks until the client sends a complete message. If the client
    /// terminates the stream, returns `None`. Returns an error if the client
    /// sends a malformed message or if the connection underlying is broken.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. The returned future only holds onto a
    /// reference to thea underlying stream, so dropping it will never lose a
    /// value.
    ///
    /// <https://docs.rs/tokio-stream/latest/tokio_stream/trait.StreamExt.html#cancel-safety-1>
    pub async fn recv(&mut self) -> Result<Option<FrontendMessage>, io::Error> {
        let message = self.inner.try_next().await?;
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
        self.inner.enqueue(message).await
    }

    /// Flushes all outstanding messages.
    pub async fn flush(&mut self) -> Result<(), io::Error> {
        self.inner.flush().await
    }
}

impl<A> FramedConn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    pub fn inner(&self) -> &Conn<A> {
        self.inner.get_ref().get_ref()
    }
    pub fn inner_mut(&mut self) -> &mut Conn<A> {
        self.inner.get_mut().get_mut()
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
}

impl Codec {
    /// Creates a new `Codec`.
    pub fn new() -> Codec {
        Codec {
            decode_state: DecodeState::Head,
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
            BackendMessage::AuthenticationCleartextPassword => b'R',
            BackendMessage::ErrorResponse(r) => {
                if r.severity.is_error() {
                    b'E'
                } else {
                    b'N'
                }
            }
        };
        dst.put_u8(byte);

        // Write message length placeholder. The true length is filled in later.
        let base = dst.len();
        dst.put_u32(0);

        // Write message contents.
        match msg {
            BackendMessage::AuthenticationCleartextPassword => {
                dst.put_u32(3);
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

impl Decoder for Codec {
    type Item = FrontendMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<FrontendMessage>, io::Error> {
        if src.len() > MAX_REQUEST_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "request larger than {}",
                    ByteSize::b(u64::cast_from(MAX_REQUEST_SIZE))
                ),
            ));
        }
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
                        // Termination.
                        b'X' => decode_terminate(buf)?,

                        // Authentication.
                        b'p' => decode_password(buf)?,

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
