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

use std::net::IpAddr;

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use bytesize::ByteSize;
use futures::{SinkExt, TryStreamExt, sink};
use itertools::Itertools;
use mz_adapter_types::connection::ConnectionId;
use mz_ore::cast::CastFrom;
use mz_ore::future::OreSinkExt;
use mz_ore::netio::AsyncReady;
use mz_pgwire_common::{
    ChannelBinding, Conn, Cursor, DecodeState, ErrorResponse, FrontendMessage, GS2Header,
    MAX_REQUEST_SIZE, Pgbuf, SASLClientFinalResponse, SASLInitialResponse, input_err,
    parse_frame_len,
};
use tokio::io::{self, AsyncRead, AsyncWrite, Interest, Ready};
use tokio::time::{self, Duration};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::trace;

use crate::message::{BackendMessage, BackendMessageKind, SASLServerFinalMessageKinds};

/// A connection that manages the encoding and decoding of pgwire frames.
pub struct FramedConn<A> {
    conn_id: ConnectionId,
    peer_addr: Option<IpAddr>,
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
    pub fn new(conn_id: ConnectionId, peer_addr: Option<IpAddr>, inner: Conn<A>) -> FramedConn<A> {
        FramedConn {
            conn_id,
            peer_addr,
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
        match &message {
            Some(message) => trace!("cid={} recv_name={}", self.conn_id, message.name()),
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
        trace!(
            "cid={} send={:?}",
            self.conn_id,
            BackendMessageKind::from(&message)
        );
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
    pub fn set_encode_state(
        &mut self,
        encode_state: Vec<(mz_pgrepr::Type, mz_pgwire_common::Format)>,
    ) {
        self.inner.get_mut().codec_mut().encode_state = encode_state;
    }

    /// Waits for the connection to be closed.
    ///
    /// Returns a "connection closed" error when the connection is closed. If
    /// another error occurs before the connection is closed, that error is
    /// returned instead.
    ///
    /// Use this method when you have an unbounded stream of data to forward to
    /// the connection and the protocol does not require the client to
    /// periodically acknowledge receipt. If you don't call this method to
    /// periodically check if the connection has closed, you may not notice that
    /// the client has gone away for an unboundedly long amount of time; usually
    /// not until the stream of data produces its next message and you attempt
    /// to write the data to the connection.
    pub async fn wait_closed(&self) -> io::Error
    where
        A: AsyncReady + Send + Sync,
    {
        loop {
            time::sleep(Duration::from_secs(1)).await;

            match self.ready(Interest::READABLE | Interest::WRITABLE).await {
                Ok(ready) if ready.is_read_closed() || ready.is_write_closed() => {
                    return io::Error::new(io::ErrorKind::Other, "connection closed");
                }
                Ok(_) => (),
                Err(err) => return err,
            }
        }
    }

    /// Returns the ID associated with this connection.
    pub fn conn_id(&self) -> &ConnectionId {
        &self.conn_id
    }

    /// Returns the peer address of the connection.
    pub fn peer_addr(&self) -> &Option<IpAddr> {
        &self.peer_addr
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
    encode_state: Vec<(mz_pgrepr::Type, mz_pgwire_common::Format)>,
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
            BackendMessage::AuthenticationCleartextPassword
            | BackendMessage::AuthenticationSASL
            | BackendMessage::AuthenticationSASLContinue(_)
            | BackendMessage::AuthenticationSASLFinal(_) => b'R',
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
            BackendMessage::AuthenticationSASL => {
                dst.put_u32(10);
                dst.put_string("SCRAM-SHA-256");
                dst.put_u8(b'\0');
            }
            BackendMessage::AuthenticationSASLContinue(data) => {
                dst.put_u32(11);
                let data = format!(
                    "r={},s={},i={}",
                    data.nonce, data.salt, data.iteration_count
                );
                dst.put_slice(data.as_bytes());
            }
            BackendMessage::AuthenticationSASLFinal(data) => {
                dst.put_u32(12);
                let res = match data.kind {
                    SASLServerFinalMessageKinds::Verifier(verifier) => {
                        format!("v={}", verifier)
                    }
                };
                dst.put_slice(res.as_bytes());
                if !data.extensions.is_empty() {
                    dst.put_slice(b",");
                    dst.put_slice(data.extensions.join(",").as_bytes());
                }
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
                for (f, (ty, format)) in fields.iter().zip_eq(&self.encode_state) {
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
                dst.put_u8(status.into());
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
                        b'p' => decode_auth(buf)?,

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

fn decode_auth(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let mut value = Vec::new();
    while let Ok(b) = buf.read_byte() {
        value.push(b);
    }
    Ok(FrontendMessage::RawAuthentication(value))
}

fn expect(buf: &mut Cursor, expected: &[u8]) -> Result<(), io::Error> {
    for i in 0..expected.len() {
        if buf.read_byte()? != expected[i] {
            return Err(input_err(format!(
                "Invalid SASL initial response: expected '{}'",
                std::str::from_utf8(expected).unwrap_or("invalid UTF-8")
            )));
        }
    }
    Ok(())
}

fn read_until_comma(buf: &mut Cursor) -> Result<Vec<u8>, io::Error> {
    let mut v = Vec::new();
    while let Ok(b) = buf.peek_byte() {
        if b == b',' {
            break;
        }
        v.push(buf.read_byte()?);
    }
    Ok(v)
}

// All SASL parsing is based on RFC 5802, [section 7](https://datatracker.ietf.org/doc/html/rfc5802#section-7)

//   extensions = attr-val *("," attr-val)
//                     ;; All extensions are optional,
//                     ;; i.e., unrecognized attributes
//                     ;; not defined in this document
//                     ;; MUST be ignored.
//   reserved-mext  = "m=" 1*(value-char)
//                     ;; Reserved for signaling mandatory extensions.
//                     ;; The exact syntax will be defined in
//                     ;; the future.
//   gs2-cbind-flag  = ("p=" cb-name) / "n" / "y"
//                     ;; "n" -> client doesn't support channel binding.
//                     ;; "y" -> client does support channel binding
//                     ;;        but thinks the server does not.
//                     ;; "p" -> client requires channel binding.
//                     ;; The selected channel binding follows "p=".
//
//   gs2-header      = gs2-cbind-flag "," [ authzid ] ","
//                     ;; GS2 header for SCRAM
//                     ;; (the actual GS2 header includes an optional
//                     ;; flag to indicate that the GSS mechanism is not
//                     ;; "standard", but since SCRAM is "standard", we
//                     ;; don't include that flag).
//   client-first-message-bare =
//                     [reserved-mext ","]
//                     username "," nonce ["," extensions]
//
//   client-first-message =
//                     gs2-header client-first-message-bare
pub fn decode_sasl_client_first_message(mut buf: Cursor) -> Result<SASLInitialResponse, io::Error> {
    // 1) GS2 cbind flag
    let cbind_flag = match buf.read_byte()? {
        b'n' => ChannelBinding::None,
        b'y' => ChannelBinding::ClientSupported,
        b'p' => {
            // must be "p=" then cbname up to next comma
            expect(&mut buf, b"=")?;
            let cbname = String::from_utf8(read_until_comma(&mut buf)?)
                .map_err(|_| input_err("invalid cbname utf8"))?;
            ChannelBinding::Required(cbname)
        }
        other => {
            return Err(input_err(format!(
                "Invalid channel binding flag: {}",
                other
            )));
        }
    };
    expect(&mut buf, b",")?;

    // 2) Optional authzid: either empty, or "a=" up to next comma
    let mut authzid = None;
    if buf.peek_byte()? == b'a' {
        expect(&mut buf, b"a=")?;
        let a = String::from_utf8(read_until_comma(&mut buf)?)
            .map_err(|_| input_err("invalid authzid utf8"))?;
        authzid = Some(a);
    }
    expect(&mut buf, b",")?;

    let mut client_first_message_bare_raw = String::new();

    // 3) Optional reserved "m=" extension before n=
    let mut reserved_mext = None;
    if buf.peek_byte()? == b'm' {
        expect(&mut buf, b"m=")?;
        let mext_val = String::from_utf8(read_until_comma(&mut buf)?)
            .map_err(|_| input_err("invalid m ext utf8"))?;
        client_first_message_bare_raw.push_str(&format!("m={},", mext_val));
        reserved_mext = Some(mext_val);
        expect(&mut buf, b",")?;
    }

    // 4) Username: must be "n=" then saslname
    expect(&mut buf, b"n=")?;
    // Postgres doesn't use the username here, so we just consume
    let username = String::from_utf8(read_until_comma(&mut buf)?)
        .map_err(|_| input_err("invalid username utf8"))?;
    expect(&mut buf, b",")?;
    client_first_message_bare_raw.push_str(&format!("n={},", username));

    // 5) Nonce: must be "r=" then value up to next comma or end
    expect(&mut buf, b"r=")?;
    let nonce = String::from_utf8(read_until_comma(&mut buf)?)
        .map_err(|_| input_err("invalid nonce utf8"))?;
    client_first_message_bare_raw.push_str(&format!("r={}", nonce));

    // 6) Optional extensions: "," key=value chunks
    let mut extensions = Vec::new();
    while let Ok(b',') = buf.peek_byte().map(|b| b) {
        expect(&mut buf, b",")?;
        let ext = String::from_utf8(read_until_comma(&mut buf)?)
            .map_err(|_| input_err("invalid ext utf8"))?;
        if !ext.is_empty() {
            client_first_message_bare_raw.push_str(&format!(",{}", ext));
            extensions.push(ext);
        }
    }

    Ok(SASLInitialResponse {
        gs2_header: GS2Header {
            cbind_flag,
            authzid,
        },
        nonce,
        extensions,
        reserved_mext,
        client_first_message_bare_raw,
    })
}

pub fn decode_sasl_initial_response(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let mechanism = buf.read_cstr()?;
    let initial_resp_len = buf.read_i32()?;
    if initial_resp_len < 0 {
        // -1 means no response? We bail here
        return Err(input_err("No initial response"));
    }

    let initial_response = decode_sasl_client_first_message(buf)?;
    Ok(FrontendMessage::SASLInitialResponse {
        gs2_header: initial_response.gs2_header.clone(),
        mechanism: mechanism.to_owned(),
        initial_response,
    })
}

//   proof           = "p=" base64
//
//   channel-binding = "c=" base64
//                     ;; base64 encoding of cbind-input.
//   client-final-message-without-proof =
//                     channel-binding "," nonce [","
//                     extensions]
//
//   client-final-message =
//                     client-final-message-without-proof "," proof
pub fn decode_sasl_response(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    // --- client-final-message-without-proof ---
    let mut client_final_message_bare_raw = String::new();
    // channel-binding: "c=" <base64>, up to the next comma
    expect(&mut buf, b"c=")?;
    let channel_binding = String::from_utf8(read_until_comma(&mut buf)?)
        .map_err(|_| input_err("invalid channel-binding utf8"))?;
    expect(&mut buf, b",")?;
    client_final_message_bare_raw.push_str(&format!("c={},", channel_binding));

    // nonce: "r=" <printable>, up to the next comma
    expect(&mut buf, b"r=")?;
    let nonce = String::from_utf8(read_until_comma(&mut buf)?)
        .map_err(|_| input_err("invalid nonce utf8"))?;
    client_final_message_bare_raw.push_str(&format!("r={}", nonce));

    // after reading channel-binding and nonce
    let mut extensions = Vec::new();

    // Keep reading ",<token>" until we see ",p="
    while buf.peek_byte()? == b',' {
        expect(&mut buf, b",")?;
        if buf.peek_byte()? == b'p' {
            break;
        }
        let ext = String::from_utf8(read_until_comma(&mut buf)?)
            .map_err(|_| input_err("invalid extension utf8"))?;
        if !ext.is_empty() {
            client_final_message_bare_raw.push_str(&format!(",{}", ext));
            extensions.push(ext);
        }
    }

    // Proof is mandatory and last
    expect(&mut buf, b"p=")?;
    let proof = String::from_utf8(read_until_comma(&mut buf)?)
        .map_err(|_| input_err("invalid proof utf8"))?;

    Ok(FrontendMessage::SASLResponse(SASLClientFinalResponse {
        channel_binding,
        nonce,
        extensions,
        proof,
        client_final_message_bare_raw,
    }))
}

pub fn decode_password(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
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
