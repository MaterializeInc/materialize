// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, BytesMut, IntoBuf};
use std::borrow::Cow;
use tokio::codec::{Decoder, Encoder};
use tokio::io;

use crate::pgwire::message::{BackendMessage, FieldValue, FrontendMessage};
use ore::netio;

/// A Tokio codec to encode and decode pgwire frames.
///
/// Use a `Codec` by wrapping it in a [`tokio::codec::Framed`]:
///
/// ```
/// use futures::{Future, Stream};
/// use materialize::pgwire::Codec;
/// use tokio::io;
/// use tokio::net::TcpStream;
/// use tokio::codec::Framed;
///
/// fn handle_connection(rw: TcpStream) -> impl Future<Item = (), Error = io::Error> {
///     let rw = Framed::new(rw, Codec::new());
///     rw.for_each(|msg| Ok(println!("{:#?}", msg)))
/// }
/// ```
pub struct Codec {
    decode_state: DecodeState,
}

impl Codec {
    /// Creates a new `Codec`.
    pub fn new() -> Codec {
        Codec {
            decode_state: DecodeState::Startup,
        }
    }
}

impl Default for Codec {
    fn default() -> Codec {
        Codec::new()
    }
}

impl Encoder for Codec {
    type Item = BackendMessage;
    type Error = io::Error;

    fn encode(&mut self, msg: BackendMessage, dst: &mut BytesMut) -> Result<(), io::Error> {
        // TODO(benesch): do we need to be smarter about avoiding allocations?
        // At the very least, we won't need a separate buffer when BytesMut
        // automatically grows its capacity (carllerche/bytes#170).
        let mut buf = Vec::new();

        // Write type byte.
        buf.put(match msg {
            BackendMessage::AuthenticationOk => b'R',
            BackendMessage::RowDescription(_) => b'T',
            BackendMessage::DataRow(_) => b'D',
            BackendMessage::CommandComplete { .. } => b'C',
            BackendMessage::EmptyQueryResponse => b'I',
            BackendMessage::ReadyForQuery => b'Z',
            BackendMessage::ParameterStatus(_, _) => b'S',
            BackendMessage::ErrorResponse { .. } => b'E',
            BackendMessage::CopyOutResponse => b'H',
            BackendMessage::CopyData(_) => b'd',
        });

        // Write message length placeholder. The true length is filled in later.
        let start_len = buf.len();
        buf.put_u32_be(0);

        // Write message contents.
        match msg {
            // psql doesn't actually care about the number of columns.
            // It should be saved in the message if we ever need to care about it; until then,
            // 0 is fine.
            BackendMessage::CopyOutResponse/*(n_cols)*/ => {
                buf.put_u8(0); // textual format
                buf.put_i16_be(0/*n_cols*/);
                /*
                for _ in 0..n_cols {
                    buf.put_i16_be(0); // textual format for this column
                }
                */
            }
            BackendMessage::CopyData(mut data) => {
                buf.append(&mut data);
            }
            BackendMessage::AuthenticationOk => {
                buf.put_u32_be(0);
            }
            BackendMessage::RowDescription(fields) => {
                buf.put_u16_be(fields.len() as u16);
                for f in &fields {
                    buf.put_string(&f.name);
                    buf.put_u32_be(f.table_id);
                    buf.put_u16_be(f.column_id);
                    buf.put_u32_be(f.type_oid);
                    buf.put_i16_be(f.type_len);
                    buf.put_i32_be(f.type_mod);
                    buf.put_u16_be(f.format as u16);
                }
            }
            BackendMessage::DataRow(fields) => {
                buf.put_u16_be(fields.len() as u16);
                for f in fields {
                    if let Some(f) = f {
                        let s: Cow<[u8]> = match f {
                            FieldValue::Bool(false) => b"f"[..].into(),
                            FieldValue::Bool(true) => b"t"[..].into(),
                            FieldValue::Bytea(b) => b.into(),
                            FieldValue::Int4(i) => format!("{}", i).into_bytes().into(),
                            FieldValue::Int8(i) => format!("{}", i).into_bytes().into(),
                            FieldValue::Float4(f) => format!("{}", f).into_bytes().into(),
                            FieldValue::Float8(f) => format!("{}", f).into_bytes().into(),
                            FieldValue::Numeric(n) => format!("{}", n).into_bytes().into(),
                            FieldValue::Text(ref s) => s.as_bytes().into(),
                        };
                        buf.put_u32_be(s.len() as u32);
                        buf.put(&*s);
                    } else {
                        buf.put_i32_be(-1)
                    }
                }
            }
            BackendMessage::CommandComplete { tag } => {
                buf.put_string(tag);
            }
            BackendMessage::EmptyQueryResponse => (),
            BackendMessage::ReadyForQuery => {
                buf.put(b'I'); // transaction indicator
            }
            BackendMessage::ParameterStatus(name, value) => {
                buf.put_string(name);
                buf.put_string(value);
            }
            BackendMessage::ErrorResponse {
                severity,
                code,
                message,
                detail,
            } => {
                buf.put(b'S');
                buf.put_string(severity.string());
                buf.put(b'C');
                buf.put_string(code);
                buf.put(b'M');
                buf.put_string(message);
                if let Some(ref detail) = detail {
                    buf.put(b'D');
                    buf.put_string(detail);
                }
                buf.put(b'\0');
            }
        }

        // Overwrite length placeholder with true length.
        let len = buf.len() - start_len;
        NetworkEndian::write_u32(&mut buf[start_len..start_len + 4], len as u32);

        dst.extend(buf);
        Ok(())
    }
}

#[derive(Debug)]
enum DecodeState {
    Startup,
    Head,
    Data(u8, usize),
}

const MAX_FRAME_SIZE: usize = 8 << 10;

fn parse_frame_len(src: &[u8]) -> Result<usize, io::Error> {
    let n = cast::usize(NetworkEndian::read_u32(src));
    if n > MAX_FRAME_SIZE {
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
                DecodeState::Startup => {
                    if src.len() < 4 {
                        return Ok(None);
                    }
                    let frame_len = parse_frame_len(&src)?;
                    src.advance(4);
                    src.reserve(frame_len);
                    self.decode_state = DecodeState::Data(b's', frame_len);
                }

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
                    let buf = src.take().freeze();
                    let msg = match msg_type {
                        b's' => {
                            let version = NetworkEndian::read_u32(&buf[..4]);
                            FrontendMessage::Startup { version }
                        }
                        b'Q' => FrontendMessage::Query {
                            query: buf.slice_to(frame_len - 1),
                        },
                        b'X' => FrontendMessage::Terminate,
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

trait Pgbuf: BufMut {
    fn put_string<T: IntoBuf>(&mut self, s: T);
}

impl<B: BufMut> Pgbuf for B {
    fn put_string<T: IntoBuf>(&mut self, s: T) {
        self.put(s);
        self.put(b'\0');
    }
}
