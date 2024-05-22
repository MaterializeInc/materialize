// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::BufMut;

use std::{convert::TryFrom, fmt, io, marker::PhantomData, str::from_utf8};

use crate::{
    constants::{ColumnFlags, ColumnType},
    io::{BufMutExt, ParseBuf},
    misc::{
        lenenc_str_len,
        raw::bytes::{LenEnc, RawBytes},
        unexpected_buf_eof,
    },
    proto::{MyDeserialize, MySerialize},
    value::Value::*,
};

pub mod convert;
pub mod json;

/// Side of MySql value serialization.
pub trait SerializationSide {
    /// Null-bitmap offset of this side.
    const BIT_OFFSET: usize;
}

/// Server side serialization. Null-bitmap bit offset: `2`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ServerSide;

impl SerializationSide for ServerSide {
    const BIT_OFFSET: usize = 2;
}

/// Client side serialization. Null-bitmap bit offset: `0`.
pub struct ClientSide;

impl SerializationSide for ClientSide {
    const BIT_OFFSET: usize = 0;
}

/// Textual value representation.
pub struct TextValue;

/// Binary value representation.
pub struct BinValue;

/// Client side representation of a value of MySql column.
///
/// The `Value` is also used as a parameter to a prepared statement.
#[derive(Clone, PartialEq, PartialOrd)]
pub enum Value {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    /// year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32),
}

impl MySerialize for Value {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Self::NULL => (),
            Value::Bytes(x) => {
                buf.put_lenenc_str(x);
            }
            Value::Int(x) => {
                buf.put_i64_le(*x);
            }
            Value::UInt(x) => {
                buf.put_u64_le(*x);
            }
            Value::Float(x) => {
                buf.put_f32_le(*x);
            }
            Value::Double(x) => {
                buf.put_f64_le(*x);
            }
            Value::Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32) => {
                buf.put_u8(0);
            }
            Value::Date(year, mon, day, 0u8, 0u8, 0u8, 0u32) => {
                buf.put_u8(4);
                buf.put_u16_le(*year);
                buf.put_u8(*mon);
                buf.put_u8(*day);
            }
            Value::Date(year, mon, day, hour, min, sec, 0u32) => {
                buf.put_u8(7);
                buf.put_u16_le(*year);
                buf.put_u8(*mon);
                buf.put_u8(*day);
                buf.put_u8(*hour);
                buf.put_u8(*min);
                buf.put_u8(*sec);
            }
            Value::Date(year, mon, day, hour, min, sec, usec) => {
                buf.put_u8(11);
                buf.put_u16_le(*year);
                buf.put_u8(*mon);
                buf.put_u8(*day);
                buf.put_u8(*hour);
                buf.put_u8(*min);
                buf.put_u8(*sec);
                buf.put_u32_le(*usec);
            }
            Value::Time(_, 0u32, 0u8, 0u8, 0u8, 0u32) => {
                buf.put_u8(0);
            }
            Value::Time(neg, d, h, m, s, 0u32) => {
                buf.put_u8(8);
                buf.put_u8(if *neg { 1 } else { 0 });
                buf.put_u32_le(*d);
                buf.put_u8(*h);
                buf.put_u8(*m);
                buf.put_u8(*s);
            }
            Value::Time(neg, days, hours, mins, secs, usecs) => {
                buf.put_u8(12);
                buf.put_u8(if *neg { 1 } else { 0 });
                buf.put_u32_le(*days);
                buf.put_u8(*hours);
                buf.put_u8(*mins);
                buf.put_u8(*secs);
                buf.put_u32_le(*usecs);
            }
        }
    }
}

/// Deserializer for a MySql value.
///
/// `T` specifies the value representation (textual or binary).
#[derive(Debug, Clone, PartialEq)]
pub struct ValueDeserializer<T>(pub Value, PhantomData<T>);

impl<'de> MyDeserialize<'de> for ValueDeserializer<TextValue> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let value = Value::deserialize_text(buf)?;
        Ok(Self(value, PhantomData))
    }
}

impl<'de> MyDeserialize<'de> for ValueDeserializer<BinValue> {
    const SIZE: Option<usize> = None;
    type Ctx = (ColumnType, ColumnFlags);

    fn deserialize((col_type, col_flags): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let value = Value::deserialize_bin((col_type, col_flags), buf)?;
        Ok(Self(value, PhantomData))
    }
}

/// Will escape string for SQL depending on `no_backslash_escape` flag.
fn escaped(input: &str, no_backslash_escape: bool) -> String {
    let mut output = String::with_capacity(input.len());
    output.push('\'');
    if no_backslash_escape {
        for c in input.chars() {
            if c == '\'' {
                output.push('\'');
                output.push('\'');
            } else {
                output.push(c);
            }
        }
    } else {
        for c in input.chars() {
            if c == '\x00' {
                output.push('\\');
                output.push('0');
            } else if c == '\n' {
                output.push('\\');
                output.push('n');
            } else if c == '\r' {
                output.push('\\');
                output.push('r');
            } else if c == '\\' || c == '\'' || c == '"' {
                output.push('\\');
                output.push(c);
            } else if c == '\x1a' {
                output.push('\\');
                output.push('Z');
            } else {
                output.push(c);
            }
        }
    }
    output.push('\'');
    output
}

macro_rules! de_num {
    ($name:ident, $i:ident, $u:ident) => {
        fn $name(unsigned: bool, buf: &mut ParseBuf<'_>) -> io::Result<Self> {
            if unsigned {
                buf.$u()
                    .ok_or_else(unexpected_buf_eof)
                    .map(|x| Int(x as i64))
            } else {
                buf.$i()
                    .ok_or_else(unexpected_buf_eof)
                    .map(|x| Int(x as i64))
            }
        }
    };
}

impl Value {
    /// Returns length in binary serialized form.
    pub fn bin_len(&self) -> u64 {
        match self {
            Value::NULL => 0,
            Value::Bytes(x) => lenenc_str_len(x),
            Value::Int(_) => 8,
            Value::UInt(_) => 8,
            Value::Float(_) => 4,
            Value::Double(_) => 8,
            Value::Date(0u16, 0u8, 0u8, 0u8, 0u8, 0u8, 0u32) => 1,
            Value::Date(_, _, _, 0u8, 0u8, 0u8, 0u32) => 5,
            Value::Date(_, _, _, _, _, _, 0u32) => 8,
            Value::Date(_, _, _, _, _, _, _) => 12,
            Value::Time(_, 0u32, 0u8, 0u8, 0u8, 0u32) => 1,
            Value::Time(_, _, _, _, _, 0u32) => 9,
            Value::Time(_, _, _, _, _, _) => 13,
        }
    }

    pub fn as_sql(&self, no_backslash_escape: bool) -> String {
        match *self {
            Value::NULL => "NULL".into(),
            Value::Int(x) => format!("{}", x),
            Value::UInt(x) => format!("{}", x),
            Value::Float(x) => format!("{}", x),
            Value::Double(x) => format!("{}", x),
            Value::Date(y, m, d, 0, 0, 0, 0) => format!("'{:04}-{:02}-{:02}'", y, m, d),
            Value::Date(year, month, day, hour, minute, second, 0) => format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
                year, month, day, hour, minute, second
            ),
            Value::Date(year, month, day, hour, minute, second, micros) => format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
                year, month, day, hour, minute, second, micros
            ),
            Value::Time(neg, d, h, i, s, 0) => {
                if neg {
                    format!("'-{:03}:{:02}:{:02}'", d * 24 + u32::from(h), i, s)
                } else {
                    format!("'{:03}:{:02}:{:02}'", d * 24 + u32::from(h), i, s)
                }
            }
            Value::Time(neg, days, hours, minutes, seconds, micros) => {
                if neg {
                    format!(
                        "'-{:03}:{:02}:{:02}.{:06}'",
                        days * 24 + u32::from(hours),
                        minutes,
                        seconds,
                        micros
                    )
                } else {
                    format!(
                        "'{:03}:{:02}:{:02}.{:06}'",
                        days * 24 + u32::from(hours),
                        minutes,
                        seconds,
                        micros
                    )
                }
            }
            Value::Bytes(ref bytes) => match from_utf8(bytes) {
                Ok(string) => escaped(string, no_backslash_escape),
                Err(_) => {
                    let mut s = String::from("0x");
                    for c in bytes.iter() {
                        s.extend(format!("{:02X}", *c).chars())
                    }
                    s
                }
            },
        }
    }

    fn deserialize_text(buf: &mut ParseBuf<'_>) -> io::Result<Self> {
        if buf.is_empty() {
            return Err(unexpected_buf_eof());
        }

        match buf.0[0] {
            0xfb => {
                buf.skip(1);
                Ok(Value::NULL)
            }
            _ => {
                let bytes: RawBytes<LenEnc> = buf.parse(())?;
                Ok(Value::Bytes(bytes.0.into_owned()))
            }
        }
    }

    de_num!(deserialize_tiny, checked_eat_i8, checked_eat_u8);
    de_num!(deserialize_short, checked_eat_i16_le, checked_eat_u16_le);
    de_num!(deserialize_long, checked_eat_i32_le, checked_eat_u32_le);

    fn deserialize_longlong(unsigned: bool, buf: &mut ParseBuf<'_>) -> io::Result<Self> {
        if unsigned {
            buf.checked_eat_u64_le()
                .ok_or_else(unexpected_buf_eof)
                .map(|x| i64::try_from(x).map(Int).unwrap_or_else(|_| UInt(x)))
        } else {
            buf.checked_eat_i64_le()
                .ok_or_else(unexpected_buf_eof)
                .map(Int)
        }
    }

    fn deserialize_datetime(buf: &mut ParseBuf<'_>) -> io::Result<Self> {
        let len = buf.checked_eat_u8().ok_or_else(unexpected_buf_eof)?;

        let mut year = 0u16;
        let mut month = 0u8;
        let mut day = 0u8;
        let mut hour = 0u8;
        let mut minute = 0u8;
        let mut second = 0u8;
        let mut micro_second = 0u32;

        let mut buf = buf
            .checked_eat_buf(len as usize)
            .ok_or_else(unexpected_buf_eof)?;

        if len >= 4u8 {
            year = buf.eat_u16_le();
            month = buf.eat_u8();
            day = buf.eat_u8();
        }
        if len >= 7u8 {
            hour = buf.eat_u8();
            minute = buf.eat_u8();
            second = buf.eat_u8();
        }
        if len == 11u8 {
            micro_second = buf.eat_u32_le();
        }

        Ok(Date(year, month, day, hour, minute, second, micro_second))
    }

    fn deserialize_time(buf: &mut ParseBuf<'_>) -> io::Result<Self> {
        let len = buf.checked_eat_u8().ok_or_else(unexpected_buf_eof)?;

        let mut is_negative = false;
        let mut days = 0u32;
        let mut hours = 0u8;
        let mut minutes = 0u8;
        let mut seconds = 0u8;
        let mut micro_seconds = 0u32;

        let mut buf = buf
            .checked_eat_buf(len as usize)
            .ok_or_else(unexpected_buf_eof)?;

        if len >= 8u8 {
            is_negative = buf.eat_u8() == 1u8;
            days = buf.eat_u32_le();
            hours = buf.eat_u8();
            minutes = buf.eat_u8();
            seconds = buf.eat_u8();
        }
        if len == 12u8 {
            micro_seconds = buf.eat_u32_le();
        }

        Ok(Time(
            is_negative,
            days,
            hours,
            minutes,
            seconds,
            micro_seconds,
        ))
    }

    pub(crate) fn deserialize_bin(
        (column_type, column_flags): (ColumnType, ColumnFlags),
        buf: &mut ParseBuf<'_>,
    ) -> io::Result<Self> {
        match column_type {
            ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_BLOB
            | ColumnType::MYSQL_TYPE_TINY_BLOB
            | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
            | ColumnType::MYSQL_TYPE_LONG_BLOB
            | ColumnType::MYSQL_TYPE_SET
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_DECIMAL
            | ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_BIT
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
            | ColumnType::MYSQL_TYPE_GEOMETRY
            | ColumnType::MYSQL_TYPE_JSON => Ok(Bytes(
                buf.checked_eat_lenenc_str()
                    .ok_or_else(unexpected_buf_eof)?
                    .to_vec(),
            )),
            ColumnType::MYSQL_TYPE_TINY => {
                Self::deserialize_tiny(column_flags.contains(ColumnFlags::UNSIGNED_FLAG), buf)
            }
            ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                Self::deserialize_short(column_flags.contains(ColumnFlags::UNSIGNED_FLAG), buf)
            }
            ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                Self::deserialize_long(column_flags.contains(ColumnFlags::UNSIGNED_FLAG), buf)
            }
            ColumnType::MYSQL_TYPE_LONGLONG => {
                Self::deserialize_longlong(column_flags.contains(ColumnFlags::UNSIGNED_FLAG), buf)
            }
            ColumnType::MYSQL_TYPE_FLOAT => buf
                .checked_eat_f32_le()
                .ok_or_else(unexpected_buf_eof)
                .map(Float),
            ColumnType::MYSQL_TYPE_DOUBLE => buf
                .checked_eat_f64_le()
                .ok_or_else(unexpected_buf_eof)
                .map(Double),
            ColumnType::MYSQL_TYPE_TIMESTAMP
            | ColumnType::MYSQL_TYPE_DATE
            | ColumnType::MYSQL_TYPE_DATETIME => Self::deserialize_datetime(buf),
            ColumnType::MYSQL_TYPE_TIME => Self::deserialize_time(buf),
            ColumnType::MYSQL_TYPE_NULL => Ok(NULL),
            x => unimplemented!("Unsupported column type {:?}", x),
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Value::NULL => formatter.debug_tuple("Null").finish(),
            Value::Bytes(ref bytes) => {
                let mut debug = formatter.debug_tuple("Bytes");
                if bytes.len() <= 8 {
                    debug
                        .field(&String::from_utf8_lossy(bytes).replace('\n', "\\n"))
                        .finish()
                } else {
                    let bytes = String::from_utf8_lossy(&bytes[..8]).replace('\n', "\\n");
                    debug.field(&format!("{}..", bytes)).finish()
                }
            }
            Value::Int(ref val) => formatter.debug_tuple("Int").field(val).finish(),
            Value::UInt(ref val) => formatter.debug_tuple("UInt").field(val).finish(),
            Value::Float(ref val) => formatter.debug_tuple("Float").field(val).finish(),
            Value::Double(ref val) => formatter.debug_tuple("Double").field(val).finish(),
            Value::Date(y, m, d, 0, 0, 0, 0) => {
                let format = format!("'{:04}-{:02}-{:02}'", y, m, d);
                formatter.debug_tuple("Date").field(&format).finish()
            }
            Value::Date(year, month, day, hour, minute, second, 0) => {
                let format = format!(
                    "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
                    year, month, day, hour, minute, second
                );
                formatter.debug_tuple("Date").field(&format).finish()
            }
            Value::Date(year, month, day, hour, minute, second, micros) => {
                let format = format!(
                    "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
                    year, month, day, hour, minute, second, micros
                );
                formatter.debug_tuple("Date").field(&format).finish()
            }
            Value::Time(neg, d, h, i, s, 0) => {
                let format = if neg {
                    format!("'-{:03}:{:02}:{:02}'", d * 24 + u32::from(h), i, s)
                } else {
                    format!("'{:03}:{:02}:{:02}'", d * 24 + u32::from(h), i, s)
                };
                formatter.debug_tuple("Time").field(&format).finish()
            }
            Value::Time(neg, days, hours, minutes, seconds, micros) => {
                let format = if neg {
                    format!(
                        "'-{:03}:{:02}:{:02}.{:06}'",
                        days * 24 + u32::from(hours),
                        minutes,
                        seconds,
                        micros
                    )
                } else {
                    format!(
                        "'{:03}:{:02}:{:02}.{:06}'",
                        days * 24 + u32::from(hours),
                        minutes,
                        seconds,
                        micros
                    )
                };
                formatter.debug_tuple("Time").field(&format).finish()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use crate::{io::ParseBuf, value::Value};

    #[test]
    fn should_escape_string() {
        assert_eq!(r"'?p??\\\\?p??'", Value::from("?p??\\\\?p??").as_sql(false));
        assert_eq!(r#"'?p??\"?p??'"#, Value::from("?p??\"?p??").as_sql(false));
        assert_eq!(r"'?p??\'?p??'", Value::from("?p??'?p??").as_sql(false));
        assert_eq!(r"'?p??\n?p??'", Value::from("?p??\n?p??").as_sql(false));
        assert_eq!(r"'?p??\r?p??'", Value::from("?p??\r?p??").as_sql(false));
        assert_eq!(r"'?p??\0?p??'", Value::from("?p??\x00?p??").as_sql(false));
    }

    #[cfg(feature = "nightly")]
    mod benches {
        use std::convert::TryFrom;

        use crate::{
            constants::ColumnType,
            io::WriteMysqlExt,
            packets::{Column, ComStmtExecuteRequestBuilder, NullBitmap},
            value::{ClientSide, Value},
        };

        #[bench]
        fn bench_build_stmt_execute_request(bencher: &mut test::Bencher) {
            let values = vec![
                Value::Bytes(b"12.3456789".to_vec()),
                Value::Int(0xF0),
                Value::Int(0xF000),
                Value::Int(0xF0000000),
                Value::Float(std::f32::MAX),
                Value::Double(std::f64::MAX),
                Value::NULL,
                Value::Date(2019, 11, 27, 12, 30, 0, 123456),
                Value::UInt(0xF000000000000000),
                Value::Int(0xF00000),
                Value::Date(2019, 11, 27, 0, 0, 0, 0),
                Value::Time(true, 300, 8, 8, 8, 123456),
                Value::Date(2019, 11, 27, 12, 30, 0, 123456),
                Value::Int(2019),
                Value::Bytes(b"varchar".to_vec()),
                Value::Bytes(b"1000000110000001".to_vec()),
                Value::Bytes(br#"{"foo":"bar","baz":42345.6777}"#.to_vec()),
                Value::Bytes(b"12.3456789".to_vec()),
                Value::Bytes(b"Variant".to_vec()),
                Value::Bytes(b"Element".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_TINY_BLOB".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_MEDIUM_BLOB".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_LONG_BLOB".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_BLOB".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_VAR_STRING".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_STRING".to_vec()),
                Value::NULL,
                Value::Bytes(b"MYSQL_TYPE_GEOMETRY".to_vec()),
            ];

            let (body, _) = ComStmtExecuteRequestBuilder::new(0).build(&*values);

            bencher.bytes = body.len() as u64;
            bencher.iter(|| ComStmtExecuteRequestBuilder::new(0).build(&*values));
        }

        #[cfg(feature = "nightly")]
        #[bench]
        fn bench_parse_bin_row(bencher: &mut test::Bencher) {
            fn col(name: &str, ty: ColumnType) -> Column<'static> {
                let mut payload = b"\x00def".to_vec();
                for _ in 0..5 {
                    payload.write_lenenc_str(name.as_bytes()).unwrap();
                }
                payload.extend_from_slice(&b"_\x2d\x00\xff\xff\xff\xff"[..]);
                payload.push(ty as u8);
                payload.extend_from_slice(&b"\x00\x00\x00"[..]);
                Column::read(&payload[..]).unwrap()
            }

            let values = vec![
                Value::Bytes(b"12.3456789".to_vec()),
                Value::Int(0xF0),
                Value::Int(0xF000),
                Value::Int(0xF0000000),
                Value::Float(std::f32::MAX),
                Value::Double(std::f64::MAX),
                Value::NULL,
                Value::Date(2019, 11, 27, 12, 30, 0, 123456),
                Value::UInt(0xF000000000000000),
                Value::Int(0xF00000),
                Value::Date(2019, 11, 27, 0, 0, 0, 0),
                Value::Time(true, 300, 8, 8, 8, 123456),
                Value::Date(2019, 11, 27, 12, 30, 0, 123456),
                Value::Int(2019),
                Value::Bytes(b"varchar".to_vec()),
                Value::Bytes(b"1000000110000001".to_vec()),
                Value::Bytes(br#"{"foo":"bar","baz":42345.6777}"#.to_vec()),
                Value::Bytes(b"12.3456789".to_vec()),
                Value::Bytes(b"Variant".to_vec()),
                Value::Bytes(b"Element".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_TINY_BLOB".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_MEDIUM_BLOB".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_LONG_BLOB".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_BLOB".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_VAR_STRING".to_vec()),
                Value::Bytes(b"MYSQL_TYPE_STRING".to_vec()),
                Value::NULL,
                Value::Bytes(b"MYSQL_TYPE_GEOMETRY".to_vec()),
            ];

            let (body, _) = ComStmtExecuteRequestBuilder::new(0).build(&*values);

            let bitmap_len = NullBitmap::<ClientSide>::bitmap_len(values.len());

            let meta_offset = ComStmtExecuteRequestBuilder::NULL_BITMAP_OFFSET + bitmap_len + 1;
            let meta_len = values.len() * 2;
            let columns = body[meta_offset..(meta_offset + meta_len)]
                .chunks(2)
                .map(|meta| col("foo", ColumnType::try_from(meta[0]).unwrap()))
                .collect::<Vec<_>>();

            let mut data = vec![0x00];
            data.extend_from_slice(
                &body[ComStmtExecuteRequestBuilder::NULL_BITMAP_OFFSET
                    ..(ComStmtExecuteRequestBuilder::NULL_BITMAP_OFFSET + bitmap_len)],
            );
            data.extend_from_slice(
                &body[(ComStmtExecuteRequestBuilder::NULL_BITMAP_OFFSET
                    + bitmap_len
                    + 1
                    + 2 * values.len())..],
            );

            bencher.bytes = data.len() as u64;
            bencher.iter(|| Value::read_bin_many::<ClientSide>(&*data, &*columns).unwrap());
        }
    }

    #[test]
    fn mysql_simple_issue_284() -> io::Result<()> {
        use Value::*;

        let mut buf = ParseBuf(&[1, 49, 1, 50, 1, 51, 251, 1, 52, 1, 53, 251, 1, 55][..]);
        assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"1".to_vec()));
        assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"2".to_vec()));
        assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"3".to_vec()));
        assert_eq!(Value::deserialize_text(&mut buf)?, NULL);
        assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"4".to_vec()));
        assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"5".to_vec()));
        assert_eq!(Value::deserialize_text(&mut buf)?, NULL);
        assert_eq!(Value::deserialize_text(&mut buf)?, Bytes(b"7".to_vec()));

        Ok(())
    }
}
