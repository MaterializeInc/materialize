// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Encoding/decoding of messages in pgwire. See "[Frontend/Backend Protocol:
//! Message Formats][1]" in the PostgreSQL reference for the specification.
//!
//! See the parent [`pgwire`] module docs for higher level concerns.
//!
//! [1]: https://www.postgresql.org/docs/11/protocol-message-formats.html

use std::borrow::Cow;
use std::cell::RefCell;
use std::convert::TryFrom;

use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, BytesMut, IntoBuf};
use tokio::codec::{Decoder, Encoder};
use tokio::io;

use crate::pgwire::message::{BackendMessage, FieldFormat, FrontendMessage};
use ore::netio;

#[derive(Debug)]
enum CodecError {
    StringNoTerminator,
}

impl std::error::Error for CodecError {}
impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            CodecError::StringNoTerminator => "The string does not have a terminator",
        })
    }
}

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
            BackendMessage::DataRow(_, _) => b'D',
            BackendMessage::CommandComplete { .. } => b'C',
            BackendMessage::EmptyQueryResponse => b'I',
            BackendMessage::ReadyForQuery => b'Z',
            BackendMessage::ParameterStatus(_, _) => b'S',
            BackendMessage::ParameterDescription => b't',
            BackendMessage::ParseComplete => b'1',
            BackendMessage::BindComplete => b'2',
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
                    // TODO: make the format correct
                    buf.put_u16_be(f.format as u16);
                }
            }
            BackendMessage::DataRow(fields, formats) => {
                buf.put_u16_be(fields.len() as u16);
                for (f, ff) in fields.iter().zip(formats) {
                    if let Some(f) = f {
                        let s: Cow<[u8]> = match ff {
                            FieldFormat::Text => f.to_text(),
                            FieldFormat::Binary => f.to_binary().map_err(|e| {
                                log::error!("binary err: {}", e);
                                unsupported_err(e)
                            })?,
                        };
                        buf.put_u32_be(s.len() as u32);
                        buf.put(&*s);
                    } else {
                        buf.put_i32_be(-1);
                    }
                }
            }
            BackendMessage::CommandComplete { tag } => {
                buf.put_string(tag);
            }
            BackendMessage::ParseComplete => {}
            BackendMessage::BindComplete => {}
            BackendMessage::EmptyQueryResponse => (),
            BackendMessage::ReadyForQuery => {
                buf.put(b'I'); // transaction indicator
            }
            BackendMessage::ParameterStatus(name, value) => {
                buf.put_string(name);
                buf.put_string(value);
            }
            BackendMessage::ParameterDescription => {
                // 7 bytes: b't', u32, 0 parameters
                buf.put_u32_be(7);
                buf.put_u16_be(0);
            }
            BackendMessage::ErrorResponse {
                severity,
                code,
                message,
                detail,
            } => {
                log::warn!("error for client: {:?}->{}", severity, message);

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
                    let buf = src.split_to(frame_len).freeze();
                    let buf = Cursor::new(&buf);
                    let msg = match msg_type {
                        // initialization
                        b's' => {
                            let version = buf.read_u32()?;
                            FrontendMessage::Startup { version }
                        }
                        // Simple query
                        b'Q' => FrontendMessage::Query {
                            sql: buf.read_cstr()?.to_string(),
                        },
                        // Extended query flow
                        b'P' => parse_parse_msg(buf)?,
                        b'D' => parse_describe(buf)?,
                        b'B' => parse_bind(buf)?,
                        b'E' => parse_execute(buf)?,
                        b'S' => FrontendMessage::Sync,

                        // end
                        b'X' => FrontendMessage::Terminate,
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "unknown message type {:?}",
                                    bytes::Bytes::from(&[msg_type][..])
                                ),
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

fn parse_parse_msg(buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let name = buf.read_cstr()?;
    let sql = buf.read_cstr()?;

    // A parameter data type can be left unspecified by setting it to zero, or by making
    // the array of parameter type OIDs shorter than the number of parameter symbols ($n)
    // used in the query string. Another special case is that a parameter's type can be
    // specified as void (that is, the OID of the void pseudo-type). This is meant to
    // allow parameter symbols to be used for function parameters that are actually OUT
    // parameters. Ordinarily there is no context in which a void parameter could be
    // used, but if such a parameter symbol appears in a function's parameter list, it is
    // effectively ignored. For example, a function call such as foo($1,$2,$3,$4) could
    // match a function with two IN and two OUT arguments, if $3 and $4 are specified as
    // having type void.
    //
    // Oh god
    let parameter_data_type_count = buf.read_u16()?;
    let mut param_dts = vec![];
    for _ in 0..parameter_data_type_count {
        param_dts.push(buf.read_u32()?);
    }

    let msg = FrontendMessage::Parse {
        name: name.into(),
        sql: sql.into(),
        parameter_data_type_count,
        parameter_data_types: param_dts,
    };

    Ok(msg)
}

fn parse_describe(buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let first_char = buf.read_byte()?;
    let name = buf.read_cstr()?.to_string();
    match first_char {
        b'S' => Ok(FrontendMessage::DescribeStatement { name }),
        b'P' => Ok(FrontendMessage::DescribePortal { name }),
        // Err(unsupported_err("Cannot handle Describe Portal")),
        other => Err(input_err(format!("Invalid describe type: {:#x?}", other))),
    }
}

/// Parse a `Byte1('B')`
fn parse_bind(buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let portal_name = buf.read_cstr()?.to_string();
    let statement_name = buf.read_cstr()?.to_string();

    let parameter_format_code_count = buf.read_u16()?;
    if parameter_format_code_count > 0 {
        // Verify that we can skip parsing parameter format codes (C=Int16, Int16[C]),
        return Err(unsupported_err("parameter format codes is not supported"));
    }
    if buf.read_u16()? > 0 {
        return Err(unsupported_err("binding parameters is not supported"));
    }

    let return_format_code_count = buf.read_u16()?;
    let mut fmt_codes = Vec::with_capacity(usize::from(return_format_code_count));
    for _ in 0..return_format_code_count {
        fmt_codes.push(FieldFormat::try_from(buf.read_u16()?).map_err(input_err)?);
    }

    Ok(FrontendMessage::Bind {
        portal_name,
        statement_name,
        return_field_formats: fmt_codes,
    })
}

fn parse_execute(buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let portal_name = buf.read_cstr()?.to_string();
    let max_rows = buf.read_u32()?;
    if max_rows > 0 {
        log::warn!(
            "Ignoring maximum_rows={} for portal={:?}",
            max_rows,
            portal_name
        );
    }
    Ok(FrontendMessage::Execute { portal_name })
}

/// Read postgres-formatted items from the network
#[derive(Debug)]
struct Cursor<'a> {
    buf: &'a [u8],
    offset: RefCell<usize>,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Cursor {
        Cursor {
            buf,
            offset: RefCell::new(0),
        }
    }

    fn read_byte(&self) -> Result<u8, io::Error> {
        let byte = self
            .cur_buf()
            .get(0)
            .ok_or_else(|| input_err("No byte to read"))?;
        *self.offset.borrow_mut() += 1;
        Ok(*byte)
    }

    /// Read the first full null-terminated string in `self`
    fn read_cstr(&self) -> Result<&str, io::Error> {
        if let Some(pos) = self.cur_buf().iter().position(|b| *b == 0) {
            let val = std::str::from_utf8(&self.cur_buf()[..pos]).map_err(input_err)?;
            *self.offset.borrow_mut() += pos + 1;
            Ok(val)
        } else {
            Err(input_err(CodecError::StringNoTerminator))
        }
    }

    fn read_u16(&self) -> Result<u16, io::Error> {
        if self.cur_buf().len() < 2 {
            return Err(input_err("not enough buffer for an Int16"));
        }
        let val = NetworkEndian::read_u16(self.cur_buf());
        *self.offset.borrow_mut() += 2;
        Ok(val)
    }

    fn read_u32(&self) -> Result<u32, io::Error> {
        if self.cur_buf().len() < 4 {
            return Err(input_err("not enough buffer for an Int32"));
        }
        let val = NetworkEndian::read_u32(self.cur_buf());
        *self.offset.borrow_mut() += 4;
        Ok(val)
    }

    /// Get the current buffer
    fn cur_buf(&self) -> &[u8] {
        &self.buf[*self.offset.borrow()..]
    }
}

fn unsupported_err(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, source.into())
}

/// An actual error in the input
fn input_err(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, source.into())
}
