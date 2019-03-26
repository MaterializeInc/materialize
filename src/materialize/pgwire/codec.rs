// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, BytesMut, IntoBuf};
use tokio::codec::{Decoder, Encoder};
use tokio::io;

use crate::pgwire::message::{BackendMessage, FieldValue, FrontendMessage};
use crate::repr::Datum;
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
        // Write type byte.
        dst.put(match msg {
            BackendMessage::AuthenticationOk => b'R',
            BackendMessage::RowDescription(_) => b'T',
            BackendMessage::DataRow(_) => b'D',
            BackendMessage::CommandComplete { .. } => b'C',
            BackendMessage::EmptyQueryResponse => panic!("unimplemented"),
            BackendMessage::ReadyForQuery => b'Z',
            BackendMessage::ErrorResponse { .. } => b'E',
        });

        // Write message length placeholder. The true length is filled in later.
        let start_len = dst.len();
        dst.put_u32_be(0);

        // Write message contents.
        match msg {
            BackendMessage::AuthenticationOk => {
                dst.put_u32_be(0);
            }
            BackendMessage::RowDescription(fields) => {
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
                dst.put_u16_be(fields.len() as u16);
                for f in fields {
                    match f {
                        FieldValue::Null => {
                            dst.put_i32_be(-1);
                        }
                        FieldValue::Datum(d) => {
                            let d = match d {
                                Datum::Int32(i) => format!("{}", i),
                                Datum::Int64(i) => format!("{}", i),
                                Datum::String(s) => s,
                                _ => unimplemented!(),
                            };
                            dst.put_u32_be(d.len() as u32);
                            dst.put(d);
                        }
                    }
                }
            }
            BackendMessage::CommandComplete { tag } => {
                dst.put_string(tag);
            }
            BackendMessage::EmptyQueryResponse => panic!("unimplemented"),
            BackendMessage::ReadyForQuery => {
                dst.put(b'I'); // transaction indicator
            }
            BackendMessage::ErrorResponse {
                severity,
                code,
                message,
                detail,
            } => {
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

        // Overwrite length placeholder with true length.
        let len = dst.len() - start_len;
        NetworkEndian::write_u32(&mut dst[start_len..start_len + 4], len as u32);

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
