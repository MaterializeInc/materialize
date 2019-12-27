// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Encoding/decoding of messages in pgwire. See "[Frontend/Backend Protocol:
//! Message Formats][1]" in the PostgreSQL reference for the specification.
//!
//! See the [crate docs](crate) for higher level concerns.
//!
//! [1]: https://www.postgresql.org/docs/11/protocol-message-formats.html

use std::convert::TryFrom;
use std::str;

use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, BytesMut};
use ordered_float::OrderedFloat;
use tokio::io;
use tokio_util::codec::{Decoder, Encoder};

use crate::message::{
    BackendMessage, EncryptionType, FrontendMessage, TransactionStatus, VERSION_CANCEL,
    VERSION_GSSENC, VERSION_SSL,
};
use ore::netio;
use repr::{Datum, ScalarType};

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
/// use futures::stream::StreamExt;
/// use pgwire::Codec;
/// use tokio::io;
/// use tokio::net::TcpStream;
/// use tokio_util::codec::Framed;
///
/// async fn handle_connection(rw: TcpStream) {
///     let mut rw = Framed::new(rw, Codec::new());
///     while let Some(msg) = rw.next().await {
///         println!("{:#?}", msg);
///     }
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

    /// Instructs the codec to expect another startup message.
    pub fn reset_decode_state(&mut self) {
        self.decode_state = DecodeState::Startup;
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
        if let BackendMessage::EncryptionResponse(typ) = msg {
            dst.put_u8(match typ {
                EncryptionType::None => b'N',
                EncryptionType::Ssl => b'S',
                EncryptionType::GssApi => b'G',
            });
            return Ok(());
        }

        // Write type byte.
        let byte = match msg {
            BackendMessage::EncryptionResponse(_) => unreachable!(),
            BackendMessage::AuthenticationOk => b'R',
            BackendMessage::RowDescription(_) => b'T',
            BackendMessage::DataRow(_, _) => b'D',
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
            BackendMessage::ErrorResponse { .. } => b'E',
            BackendMessage::CopyOutResponse { .. } => b'H',
            BackendMessage::CopyData(_) => b'd',
            BackendMessage::CopyDone => b'c',
        };
        dst.put_u8(byte);

        // Write message length placeholder. The true length is filled in later.
        let start_len = dst.len();
        dst.put_u32(0);

        // Write message contents.
        match msg {
            BackendMessage::EncryptionResponse(_) => unreachable!(),
            BackendMessage::CopyOutResponse {
                overall_format,
                column_formats,
            } => {
                dst.put_i8(overall_format as i8);
                dst.put_i16(column_formats.len() as i16);
                for format in column_formats {
                    dst.put_i16(format as i16);
                }
            }
            BackendMessage::CopyData(data) => {
                dst.put_slice(&data);
            }
            BackendMessage::CopyDone => (),
            BackendMessage::AuthenticationOk => {
                dst.put_u32(0);
            }
            BackendMessage::RowDescription(fields) => {
                dst.put_u16(fields.len() as u16);
                for f in &fields {
                    dst.put_string(&f.name.to_string());
                    dst.put_u32(f.table_id);
                    dst.put_u16(f.column_id);
                    dst.put_u32(f.type_oid);
                    dst.put_i16(f.type_len);
                    dst.put_i32(f.type_mod);
                    // TODO: make the format correct
                    dst.put_u16(f.format as u16);
                }
            }
            BackendMessage::DataRow(fields, formats) => {
                dst.put_u16(fields.len() as u16);
                for (f, ff) in fields.iter().zip(formats.iter()) {
                    if let Some(f) = f {
                        let base = dst.len();
                        dst.put_u32(0);
                        f.encode(*ff, dst);
                        let len = dst.len() - base - 4;
                        let len = (len as u32).to_be_bytes();
                        dst[base..base + 4].copy_from_slice(&len);
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
                dst.put_u16(params.len() as u16);
                for param in params {
                    dst.put_u32(param.type_oid);
                }
            }
            BackendMessage::ErrorResponse {
                severity,
                code,
                message,
                detail,
            } => {
                dst.put_u8(b'S');
                dst.put_string(severity.string());
                dst.put_u8(b'C');
                dst.put_string(code);
                dst.put_u8(b'M');
                dst.put_string(&message);
                if let Some(ref detail) = detail {
                    dst.put_u8(b'D');
                    dst.put_string(detail);
                }
                dst.put_u8(b'\0');
            }
        }

        // Overwrite length placeholder with true length.
        let len = dst.len() - start_len;
        NetworkEndian::write_u32(&mut dst[start_len..start_len + 4], len as u32);

        Ok(())
    }
}

trait Pgbuf: BufMut {
    fn put_string(&mut self, s: &str);
}

impl<B: BufMut> Pgbuf for B {
    fn put_string(&mut self, s: &str) {
        self.put(s.as_bytes());
        self.put_u8(b'\0');
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
                        // Initialization and termination.
                        b's' => decode_startup(buf)?,
                        b'X' => decode_terminate(buf)?,

                        // Simple query flow.
                        b'Q' => decode_query(buf)?,

                        // Extended query flow.
                        b'P' => decode_parse(buf)?,
                        b'D' => decode_describe(buf)?,
                        b'B' => decode_bind(buf)?,
                        b'E' => decode_execute(buf)?,
                        b'S' => decode_sync(buf)?,
                        b'C' => decode_close(buf)?,

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

fn decode_startup(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    let version = buf.read_i32()?;
    if version == VERSION_CANCEL {
        Ok(FrontendMessage::CancelRequest {
            conn_id: buf.read_u32()?,
            secret_key: buf.read_u32()?,
        })
    } else if version == VERSION_SSL {
        Ok(FrontendMessage::SslRequest)
    } else if version == VERSION_GSSENC {
        Ok(FrontendMessage::GssEncRequest)
    } else {
        Ok(FrontendMessage::Startup { version })
    }
}

fn decode_terminate(mut _buf: Cursor) -> Result<FrontendMessage, io::Error> {
    // Nothing more to decode.
    Ok(FrontendMessage::Terminate)
}

fn decode_query(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
    Ok(FrontendMessage::Query {
        sql: buf.read_cstr()?.to_string(),
    })
}

fn decode_parse(mut buf: Cursor) -> Result<FrontendMessage, io::Error> {
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
    let parameter_data_type_count = buf.read_i16()?;
    let mut param_dts = vec![];
    for _ in 0..parameter_data_type_count {
        param_dts.push(buf.read_i32()?);
    }

    let msg = FrontendMessage::Parse {
        name: name.into(),
        sql: sql.into(),
        parameter_data_type_count,
        parameter_data_types: param_dts,
    };

    Ok(msg)
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

    // Actions depending on the number of format codes provided:
    //     0 => use text for all parameters, if any exist
    //     1 => use the specified format code for all parameters
    //    >1 => use separate format code for each parameter
    let parameter_format_code_count = buf.read_i16()?;
    let mut parameter_format_codes = Vec::with_capacity(parameter_format_code_count as usize);
    if parameter_format_code_count == 0 {
        parameter_format_codes.push(pgrepr::Format::Text);
    } else {
        for _ in 0..parameter_format_code_count {
            parameter_format_codes
                .push(pgrepr::Format::try_from(buf.read_i16()?).map_err(input_err)?);
        }
    }

    let parameter_count = buf.read_i16()?;
    // If we have fewer format codes than parameters,
    // we're using the same format code for all parameters.
    // Add parameter number of that format code to
    // FrontendMessage::Bind to provide a cleaner interface.
    if parameter_format_code_count < parameter_count {
        for _ in 0..parameter_count - parameter_format_code_count {
            parameter_format_codes.push(parameter_format_codes[0]);
        }
    }

    let mut parameters = Vec::new();
    for _ in 0..parameter_count {
        let param_value_length = buf.read_i32()?;
        if param_value_length == -1 {
            // Only happens if the value is Null.
            parameters.push(None);
        } else {
            let mut value: Vec<u8> = Vec::new();
            for _ in 0..param_value_length {
                value.push(buf.read_byte()?);
            }
            parameters.push(Some(value));
        }
    }

    // Actions depending on the number of result format codes provided:
    //     0 => no result columns or all should use text
    //     1 => use the specified format code for all results
    //    >1 => use separate format code for each result
    let result_formats_count = buf.read_i16()?;
    let mut result_formats = Vec::with_capacity(result_formats_count as usize);
    for _ in 0..result_formats_count {
        result_formats.push(pgrepr::Format::try_from(buf.read_i16()?).map_err(input_err)?);
    }

    let raw_parameter_bytes = RawParameterBytes::new(parameters, parameter_format_codes);
    Ok(FrontendMessage::Bind {
        portal_name,
        statement_name,
        raw_parameter_bytes,
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

fn decode_sync(mut _buf: Cursor) -> Result<FrontendMessage, io::Error> {
    // Nothing more to decode.
    Ok(FrontendMessage::Sync)
}

/// Decodes data within pgwire messages.
///
/// The API provided is very similar to [`bytes::Buf`], but operations return
/// errors rather than panicking. This is important for safety, as we don't want
/// to crash if the user sends us malformatted pgwire messages.
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

    /// Returns the next byte, advancing the cursor by one byte.
    fn read_byte(&mut self) -> Result<u8, io::Error> {
        let byte = self
            .buf
            .get(0)
            .ok_or_else(|| input_err("No byte to read"))?;
        self.advance(1);
        Ok(*byte)
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

    /// Advances the cursor by `n` bytes.
    fn advance(&mut self, n: usize) {
        self.buf = &self.buf[n..]
    }
}

/// Stores raw bytes passed from Postgres to
/// bind to prepared statements.
#[derive(Debug)]
pub struct RawParameterBytes {
    parameters: Vec<Option<Vec<u8>>>,
    parameter_format_codes: Vec<pgrepr::Format>,
}

impl RawParameterBytes {
    pub fn new(
        parameters: Vec<Option<Vec<u8>>>,
        parameter_format_codes: Vec<pgrepr::Format>,
    ) -> RawParameterBytes {
        RawParameterBytes {
            parameters,
            parameter_format_codes,
        }
    }

    pub fn decode_parameters<'a>(
        &'a self,
        typs: &[ScalarType],
    ) -> Result<Vec<(Datum<'a>, ScalarType)>, failure::Error> {
        let mut parameters = Vec::new();
        for (i, parameter) in self.parameters.iter().enumerate() {
            let datum = match parameter {
                Some(bytes) => match self.parameter_format_codes[i] {
                    pgrepr::Format::Binary => {
                        RawParameterBytes::generate_datum_from_bytes(&bytes, &typs[i])?
                    }
                    pgrepr::Format::Text => {
                        RawParameterBytes::generate_datum_from_text(&bytes, &typs[i])?
                    }
                },
                None => Datum::Null,
            };
            parameters.push((datum, typs[i].clone()));
        }
        Ok(parameters)
    }

    fn generate_datum_from_bytes<'a>(
        bytes: &'a [u8],
        typ: &ScalarType,
    ) -> Result<Datum<'a>, failure::Error> {
        Ok(match typ {
            ScalarType::Null => Datum::Null,
            ScalarType::Bool => match bytes[0] {
                // Rust bools are 1 byte in size.
                0 => Datum::False,
                _ => Datum::True,
            },
            ScalarType::Int32 => Datum::Int32(NetworkEndian::read_i32(&bytes)),
            ScalarType::Int64 => Datum::Int64(NetworkEndian::read_i64(&bytes)),
            ScalarType::Float32 => Datum::Float32(NetworkEndian::read_f32(&bytes).into()),
            ScalarType::Float64 => Datum::Float64(NetworkEndian::read_f64(&bytes).into()),
            ScalarType::Bytes => Datum::Bytes(&bytes),
            ScalarType::String => Datum::String(str::from_utf8(bytes)?),
            _ => {
                // todo(jldlaughlin): implement Bool, Decimal, Date, Time, Timestamp, Interval
                failure::bail!(
                    "Generating datum not implemented for ScalarType: {:#?}",
                    typ
                )
            }
        })
    }

    fn generate_datum_from_text<'a>(
        bytes: &'a [u8],
        typ: &ScalarType,
    ) -> Result<Datum<'a>, failure::Error> {
        let as_str = str::from_utf8(bytes)?;
        Ok(match typ {
            ScalarType::Null => Datum::Null,
            ScalarType::Bool => match &*as_str {
                "0" => Datum::False,
                _ => Datum::True, // Note: anything non-zero is true!
            },
            ScalarType::Int32 => Datum::Int32(as_str.parse::<i32>()?),
            ScalarType::Int64 => Datum::Int64(as_str.parse::<i64>()?),
            ScalarType::Float32 => Datum::Float32(OrderedFloat::from(as_str.parse::<f32>()?)),
            ScalarType::Float64 => Datum::Float64(OrderedFloat::from(as_str.parse::<f64>()?)),
            ScalarType::Bytes => Datum::Bytes(as_str.as_bytes()),
            ScalarType::String => Datum::String(as_str),
            _ => {
                // todo(jldlaughlin): implement Decimal, Date, Time, Timestamp, Interval
                failure::bail!(
                    "Generating datum from text not implemented for ScalarType: {:#?} {:#?}",
                    typ,
                    as_str
                )
            }
        })
    }
}

/// Constructs an error indicating that the client has violated the pgwire
/// protocol.
fn input_err(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, source.into())
}
