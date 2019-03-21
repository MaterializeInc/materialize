// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, BytesMut, IntoBuf};
use tokio::codec::{Decoder, Encoder};
use tokio::io;

use crate::dataflow::Scalar;
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

impl Encoder for Codec {
    type Item = BackendMessage;
    type Error = io::Error;

    fn encode(&mut self, msg: BackendMessage, dst: &mut BytesMut) -> Result<(), io::Error> {
        match msg {
            BackendMessage::AuthenticationOk => {
                let len = 8;
                dst.reserve(len);
                dst.put(b'R');
                dst.put_u32_be(len as u32); // XXX cast
                dst.put_u32_be(0);
            }
            BackendMessage::RowDescription(fields) => {
                let mut len = 4 + 2 + (4 + 2 + 4 + 2 + 4 + 2) * fields.len();
                for f in &fields {
                    len += f.name.len() + 1;
                }
                dst.reserve(len);
                dst.put(b'T');
                dst.put_u32_be(len as u32);
                dst.put_u16_be(fields.len() as u16);
                for f in &fields {
                    dst.put_string(&f.name);
                    dst.put_u32_be(f.table_id);
                    dst.put_u16_be(f.column_id);
                    dst.put_u32_be(f.type_oid);
                    dst.put_i16_be(f.type_len);
                    dst.put_i32_be(f.type_mod);
                    dst.put_u16_be(f.format as u16);
                }
            }
            BackendMessage::DataRow(fields) => {
                dst.put(b'D');
                let startlen = dst.len();
                dst.put_u32_be(0); // len to be filled in later
                dst.put_u16_be(fields.len() as u16);
                for f in fields {
                    match f {
                        FieldValue::Null => {
                            dst.put_i32_be(-1);
                        }
                        FieldValue::Scalar(s) => {
                            let s = match s {
                                Scalar::Int(i) => format!("{}", i),
                                Scalar::String(s) => s,
                            };
                            dst.put_u32_be(s.len() as u32);
                            dst.put(s);
                        }
                    }
                }
                let len = dst.len() - startlen;
                NetworkEndian::write_u32(&mut dst[startlen..startlen + 4], len as u32);
            }
            BackendMessage::CommandComplete { tag } => {
                let len = 4 + tag.len() + 1;
                dst.reserve(len);
                dst.put(b'C');
                dst.put_u32_be(len as u32); // XXX cast
                dst.put_string(tag);
            }
            BackendMessage::EmptyQueryResponse => panic!("unimplemented"),
            BackendMessage::ReadyForQuery => {
                let len = 5;
                dst.reserve(len);
                dst.put(b'Z');
                dst.put_u32_be(len as u32); // XXX cast
                dst.put(b'I'); // transaction indicator
            }
            BackendMessage::ErrorResponse {
                severity,
                code,
                message,
                detail,
            } => {
                let mut len =
                    4 + severity.string().len() + 2 + code.len() + 2 + message.len() + 2 + 1;
                if let Some(ref detail) = detail {
                    len += detail.len() + 2;
                }
                dst.put(b'E');
                dst.put_u32_be(len as u32); // XXX cast
                dst.put(b'S');
                dst.put_string(severity.string());
                dst.put(b'C');
                dst.put_string(code);
                dst.put(b'M');
                dst.put_string(message);
                if let Some(ref detail) = detail {
                    dst.put(b'D');
                    dst.put_string(detail);
                }
                dst.put(b'\0');
            }
        }
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
                            FrontendMessage::Startup { version: version }
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

impl Pgbuf for BytesMut {
    fn put_string<T: IntoBuf>(&mut self, s: T) {
        self.put(s);
        self.put(b'\0');
    }
}
