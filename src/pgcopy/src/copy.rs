// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::io;

use bytes::BytesMut;
use csv::{ByteRecord, ReaderBuilder};
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::{Datum, RelationType, Row, RowArena};
use proptest::prelude::any;
use proptest::prelude::Arbitrary;
use proptest::strategy::{BoxedStrategy, Strategy, Union};
use serde::Deserialize;
use serde::Serialize;

static END_OF_COPY_MARKER: &[u8] = b"\\.";

include!(concat!(env!("OUT_DIR"), "/mz_pgcopy.copy.rs"));

pub fn encode_copy_row_binary(
    row: &Row,
    typ: &RelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    const NULL_BYTES: [u8; 4] = (-1i32).to_be_bytes();

    // 16-bit int of number of tuples.
    let count = i16::try_from(typ.column_types.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::Other,
            "column count does not fit into an i16",
        )
    })?;

    out.extend(count.to_be_bytes());
    let mut buf = BytesMut::new();
    for (field, typ) in row
        .iter()
        .zip(&typ.column_types)
        .map(|(datum, typ)| (mz_pgrepr::Value::from_datum(datum, &typ.scalar_type), typ))
    {
        match field {
            None => out.extend(NULL_BYTES),
            Some(field) => {
                buf.clear();
                field.encode_binary(&mz_pgrepr::Type::from(&typ.scalar_type), &mut buf)?;
                out.extend(
                    i32::try_from(buf.len())
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "field length does not fit into an i32",
                            )
                        })?
                        .to_be_bytes(),
                );
                out.extend(&buf);
            }
        }
    }
    Ok(())
}

pub fn encode_copy_row_text(
    CopyTextFormatParams { null, delimiter }: CopyTextFormatParams,
    row: &Row,
    typ: &RelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    let null = null.as_bytes();
    let mut buf = BytesMut::new();
    for (idx, field) in mz_pgrepr::values_from_row(row, typ).into_iter().enumerate() {
        if idx > 0 {
            out.push(delimiter);
        }
        match field {
            None => out.extend(null),
            Some(field) => {
                buf.clear();
                field.encode_text(&mut buf);
                for b in &buf {
                    match b {
                        b'\\' => out.extend(b"\\\\"),
                        b'\n' => out.extend(b"\\n"),
                        b'\r' => out.extend(b"\\r"),
                        b'\t' => out.extend(b"\\t"),
                        _ => out.push(*b),
                    }
                }
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

pub fn encode_copy_row_csv(
    CopyCsvFormatParams {
        delimiter: delim,
        quote,
        escape,
        header: _,
        null,
    }: CopyCsvFormatParams,
    row: &Row,
    typ: &RelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    let null = null.as_bytes();
    let is_special = |c: &u8| *c == delim || *c == quote || *c == b'\r' || *c == b'\n';
    let mut buf = BytesMut::new();
    for (idx, field) in mz_pgrepr::values_from_row(row, typ).into_iter().enumerate() {
        if idx > 0 {
            out.push(delim);
        }
        match field {
            None => out.extend(null),
            Some(field) => {
                buf.clear();
                field.encode_text(&mut buf);
                // A field needs quoting if:
                //   * It is the only field and the value is exactly the end
                //     of copy marker.
                //   * The field contains a special character.
                //   * The field is exactly the NULL sentinel.
                if (typ.column_types.len() == 1 && buf == END_OF_COPY_MARKER)
                    || buf.iter().any(is_special)
                    || &*buf == null
                {
                    // Quote the value by wrapping it in the quote character and
                    // emitting the escape character before any quote or escape
                    // characters within.
                    out.push(quote);
                    for b in &buf {
                        if *b == quote || *b == escape {
                            out.push(escape);
                        }
                        out.push(*b);
                    }
                    out.push(quote);
                } else {
                    // The value does not need quoting and can be emitted
                    // directly.
                    out.extend(&buf);
                }
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

pub struct CopyTextFormatParser<'a> {
    data: &'a [u8],
    position: usize,
    column_delimiter: u8,
    null_string: &'a str,
    buffer: Vec<u8>,
}

impl<'a> CopyTextFormatParser<'a> {
    pub fn new(data: &'a [u8], column_delimiter: u8, null_string: &'a str) -> Self {
        Self {
            data,
            position: 0,
            column_delimiter,
            null_string,
            buffer: Vec::new(),
        }
    }

    fn peek(&self) -> Option<u8> {
        if self.position < self.data.len() {
            Some(self.data[self.position])
        } else {
            None
        }
    }

    fn consume_n(&mut self, n: usize) {
        self.position = std::cmp::min(self.position + n, self.data.len());
    }

    pub fn is_eof(&self) -> bool {
        self.peek().is_none() || self.is_end_of_copy_marker()
    }

    pub fn is_end_of_copy_marker(&self) -> bool {
        self.check_bytes(END_OF_COPY_MARKER)
    }

    fn is_end_of_line(&self) -> bool {
        match self.peek() {
            Some(b'\n') | None => true,
            _ => false,
        }
    }

    pub fn expect_end_of_line(&mut self) -> Result<(), io::Error> {
        if self.is_end_of_line() {
            self.consume_n(1);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "extra data after last expected column",
            ))
        }
    }

    fn is_column_delimiter(&self) -> bool {
        self.check_bytes(&[self.column_delimiter])
    }

    pub fn expect_column_delimiter(&mut self) -> Result<(), io::Error> {
        if self.consume_bytes(&[self.column_delimiter]) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing data for column",
            ))
        }
    }

    fn check_bytes(&self, bytes: &[u8]) -> bool {
        let remaining_bytes = self.data.len() - self.position;
        remaining_bytes >= bytes.len()
            && self.data[self.position..]
                .iter()
                .zip(bytes.iter())
                .all(|(x, y)| x == y)
    }

    fn consume_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.check_bytes(bytes) {
            self.consume_n(bytes.len());
            true
        } else {
            false
        }
    }

    fn consume_null_string(&mut self) -> bool {
        if self.null_string.is_empty() {
            // An empty NULL marker is supported. Look ahead to ensure that is followed by
            // a column delimiter, an end of line or it is at the end of the data.
            self.is_column_delimiter()
                || self.is_end_of_line()
                || self.is_end_of_copy_marker()
                || self.is_eof()
        } else {
            self.consume_bytes(self.null_string.as_bytes())
        }
    }

    pub fn consume_raw_value(&mut self) -> Result<Option<&[u8]>, io::Error> {
        if self.consume_null_string() {
            return Ok(None);
        }

        let mut start = self.position;

        // buffer where unescaped data is accumulated
        self.buffer.clear();

        while !self.is_eof() && !self.is_end_of_copy_marker() {
            if self.is_end_of_line() || self.is_column_delimiter() {
                break;
            }
            match self.peek() {
                Some(b'\\') => {
                    // Add non-escaped data parsed so far
                    self.buffer.extend(&self.data[start..self.position]);

                    self.consume_n(1);
                    match self.peek() {
                        Some(b'b') => {
                            self.consume_n(1);
                            self.buffer.push(8);
                        }
                        Some(b'f') => {
                            self.consume_n(1);
                            self.buffer.push(12);
                        }
                        Some(b'n') => {
                            self.consume_n(1);
                            self.buffer.push(b'\n');
                        }
                        Some(b'r') => {
                            self.consume_n(1);
                            self.buffer.push(b'\r');
                        }
                        Some(b't') => {
                            self.consume_n(1);
                            self.buffer.push(b'\t');
                        }
                        Some(b'v') => {
                            self.consume_n(1);
                            self.buffer.push(11);
                        }
                        Some(b'x') => {
                            self.consume_n(1);
                            match self.peek() {
                                Some(_c @ b'0'..=b'9')
                                | Some(_c @ b'A'..=b'F')
                                | Some(_c @ b'a'..=b'f') => {
                                    let mut value: u8 = 0;
                                    let decode_nibble = |b| match b {
                                        Some(c @ b'a'..=b'f') => Some(c - b'a' + 10),
                                        Some(c @ b'A'..=b'F') => Some(c - b'A' + 10),
                                        Some(c @ b'0'..=b'9') => Some(c - b'0'),
                                        _ => None,
                                    };
                                    for _ in 0..2 {
                                        match decode_nibble(self.peek()) {
                                            Some(c) => {
                                                self.consume_n(1);
                                                value = value << 4 | c;
                                            }
                                            _ => break,
                                        }
                                    }
                                    self.buffer.push(value);
                                }
                                _ => {
                                    self.buffer.push(b'x');
                                }
                            }
                        }
                        Some(_c @ b'0'..=b'7') => {
                            let mut value: u8 = 0;
                            for _ in 0..3 {
                                match self.peek() {
                                    Some(c @ b'0'..=b'7') => {
                                        self.consume_n(1);
                                        value = value << 3 | (c - b'0');
                                    }
                                    _ => break,
                                }
                            }
                            self.buffer.push(value);
                        }
                        Some(c) => {
                            self.consume_n(1);
                            self.buffer.push(c);
                        }
                        None => {
                            self.buffer.push(b'\\');
                        }
                    }

                    start = self.position;
                }
                Some(_) => {
                    self.consume_n(1);
                }
                None => {}
            }
        }

        // Return a slice of the original buffer if no escaped characters where processed
        if self.buffer.is_empty() {
            Ok(Some(&self.data[start..self.position]))
        } else {
            // ... otherwise, add the remaining non-escaped data to the decoding buffer
            // and return a pointer to it
            self.buffer.extend(&self.data[start..self.position]);
            Ok(Some(&self.buffer[..]))
        }
    }

    /// Error if more than `num_columns` values in `parser`.
    pub fn iter_raw(self, num_columns: usize) -> RawIterator<'a> {
        RawIterator {
            parser: self,
            current_column: 0,
            num_columns,
            truncate: false,
        }
    }

    /// Return no more than `num_columns` values from `parser`.
    pub fn iter_raw_truncating(self, num_columns: usize) -> RawIterator<'a> {
        RawIterator {
            parser: self,
            current_column: 0,
            num_columns,
            truncate: true,
        }
    }
}

pub struct RawIterator<'a> {
    parser: CopyTextFormatParser<'a>,
    current_column: usize,
    num_columns: usize,
    truncate: bool,
}

impl<'a> RawIterator<'a> {
    pub fn next(&mut self) -> Option<Result<Option<&[u8]>, io::Error>> {
        if self.current_column > self.num_columns {
            return None;
        }

        if self.current_column == self.num_columns {
            if !self.truncate {
                if let Some(err) = self.parser.expect_end_of_line().err() {
                    return Some(Err(err));
                }
            }

            return None;
        }

        if self.current_column > 0 {
            if let Some(err) = self.parser.expect_column_delimiter().err() {
                return Some(Err(err));
            }
        }

        self.current_column += 1;
        Some(self.parser.consume_raw_value())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum CopyFormatParams<'a> {
    Text(CopyTextFormatParams<'a>),
    Csv(CopyCsvFormatParams<'a>),
}

impl RustType<ProtoCopyFormatParams> for CopyFormatParams<'static> {
    fn into_proto(&self) -> ProtoCopyFormatParams {
        use proto_copy_format_params::Kind;
        ProtoCopyFormatParams {
            kind: Some(match self {
                Self::Text(f) => Kind::Text(f.into_proto()),
                Self::Csv(f) => Kind::Csv(f.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoCopyFormatParams) -> Result<Self, TryFromProtoError> {
        use proto_copy_format_params::Kind;
        match proto.kind {
            Some(Kind::Text(f)) => Ok(Self::Text(f.into_rust()?)),
            Some(Kind::Csv(f)) => Ok(Self::Csv(f.into_rust()?)),
            None => Err(TryFromProtoError::missing_field(
                "ProtoCopyFormatParams::kind",
            )),
        }
    }
}

impl Arbitrary for CopyFormatParams<'static> {
    type Parameters = ();
    type Strategy = Union<BoxedStrategy<Self>>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            any::<CopyTextFormatParams>().prop_map(Self::Text).boxed(),
            any::<CopyCsvFormatParams>().prop_map(Self::Csv).boxed(),
        ])
    }
}

/// Decodes the given bytes into `Row`-s based on the given `CopyFormatParams`.
pub fn decode_copy_format<'a>(
    data: &[u8],
    column_types: &[mz_pgrepr::Type],
    params: CopyFormatParams<'a>,
) -> Result<Vec<Row>, io::Error> {
    match params {
        CopyFormatParams::Text(params) => decode_copy_format_text(data, column_types, params),
        CopyFormatParams::Csv(params) => decode_copy_format_csv(data, column_types, params),
    }
}

/// Encodes the given `Row` into bytes based on the given `CopyFormatParams`.
pub fn encode_copy_format<'a>(
    params: CopyFormatParams<'a>,
    row: &Row,
    typ: &RelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    match params {
        CopyFormatParams::Text(params) => encode_copy_row_text(params, row, typ, out),
        CopyFormatParams::Csv(params) => encode_copy_row_csv(params, row, typ, out),
        // TODO (mouli): Handle Binary format here as well?
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct CopyTextFormatParams<'a> {
    pub null: Cow<'a, str>,
    pub delimiter: u8,
}

impl<'a> Default for CopyTextFormatParams<'a> {
    fn default() -> Self {
        CopyTextFormatParams {
            delimiter: b'\t',
            null: Cow::from("\\N"),
        }
    }
}

impl RustType<ProtoCopyTextFormatParams> for CopyTextFormatParams<'static> {
    fn into_proto(&self) -> ProtoCopyTextFormatParams {
        ProtoCopyTextFormatParams {
            null: self.null.into_proto(),
            delimiter: self.delimiter.into_proto(),
        }
    }

    fn from_proto(proto: ProtoCopyTextFormatParams) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            null: Cow::Owned(proto.null.into_rust()?),
            delimiter: proto.delimiter.into_rust()?,
        })
    }
}

impl Arbitrary for CopyTextFormatParams<'static> {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (any::<String>(), any::<u8>())
            .prop_map(|(null, delimiter)| Self {
                null: Cow::Owned(null),
                delimiter,
            })
            .boxed()
    }
}

pub fn decode_copy_format_text(
    data: &[u8],
    column_types: &[mz_pgrepr::Type],
    CopyTextFormatParams { null, delimiter }: CopyTextFormatParams,
) -> Result<Vec<Row>, io::Error> {
    let mut rows = Vec::new();

    // TODO: pass the `CopyTextFormatParams` to the `new` method
    let mut parser = CopyTextFormatParser::new(data, delimiter, &null);
    while !parser.is_eof() && !parser.is_end_of_copy_marker() {
        let mut row = Vec::new();
        let buf = RowArena::new();
        for (col, typ) in column_types.iter().enumerate() {
            if col > 0 {
                parser.expect_column_delimiter()?;
            }
            let raw_value = parser.consume_raw_value()?;
            if let Some(raw_value) = raw_value {
                match mz_pgrepr::Value::decode_text(typ, raw_value) {
                    Ok(value) => row.push(value.into_datum(&buf, typ)),
                    Err(err) => {
                        let msg = format!("unable to decode column: {}", err);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                    }
                }
            } else {
                row.push(Datum::Null);
            }
        }
        parser.expect_end_of_line()?;
        rows.push(Row::pack(row));
    }
    // Note that if there is any junk data after the end of copy marker, we drop
    // it on the floor as PG does.
    Ok(rows)
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct CopyCsvFormatParams<'a> {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: u8,
    pub header: bool,
    pub null: Cow<'a, str>,
}

impl RustType<ProtoCopyCsvFormatParams> for CopyCsvFormatParams<'static> {
    fn into_proto(&self) -> ProtoCopyCsvFormatParams {
        ProtoCopyCsvFormatParams {
            delimiter: self.delimiter.into(),
            quote: self.quote.into(),
            escape: self.escape.into(),
            header: self.header,
            null: self.null.into_proto(),
        }
    }

    fn from_proto(proto: ProtoCopyCsvFormatParams) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            delimiter: proto.delimiter.into_rust()?,
            quote: proto.quote.into_rust()?,
            escape: proto.escape.into_rust()?,
            header: proto.header,
            null: Cow::Owned(proto.null.into_rust()?),
        })
    }
}

impl Arbitrary for CopyCsvFormatParams<'static> {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<u8>(),
            any::<u8>(),
            any::<u8>(),
            any::<bool>(),
            any::<String>(),
        )
            .prop_map(|(delimiter, quote, escape, header, null)| Self {
                delimiter,
                quote,
                escape,
                header,
                null: Cow::Owned(null),
            })
            .boxed()
    }
}

impl<'a> Default for CopyCsvFormatParams<'a> {
    fn default() -> Self {
        CopyCsvFormatParams {
            delimiter: b',',
            quote: b'"',
            escape: b'"',
            header: false,
            null: Cow::from(""),
        }
    }
}

impl<'a> CopyCsvFormatParams<'a> {
    pub fn try_new(
        delimiter: Option<u8>,
        quote: Option<u8>,
        escape: Option<u8>,
        header: Option<bool>,
        null: Option<String>,
    ) -> Result<CopyCsvFormatParams<'a>, String> {
        let mut params = CopyCsvFormatParams::default();

        if let Some(delimiter) = delimiter {
            params.delimiter = delimiter;
        }
        if let Some(quote) = quote {
            params.quote = quote;
            // escape defaults to the value provided for quote
            params.escape = quote;
        }
        if let Some(escape) = escape {
            params.escape = escape;
        }
        if let Some(header) = header {
            params.header = header;
        }
        if let Some(null) = null {
            params.null = Cow::from(null);
        }

        if params.quote == params.delimiter {
            return Err("COPY delimiter and quote must be different".to_string());
        }
        Ok(params)
    }
}

pub fn decode_copy_format_csv(
    data: &[u8],
    column_types: &[mz_pgrepr::Type],
    CopyCsvFormatParams {
        delimiter,
        quote,
        escape,
        null,
        header,
    }: CopyCsvFormatParams,
) -> Result<Vec<Row>, io::Error> {
    let mut rows = Vec::new();

    let (double_quote, escape) = if quote == escape {
        (true, None)
    } else {
        (false, Some(escape))
    };

    let mut rdr = ReaderBuilder::new()
        .delimiter(delimiter)
        .quote(quote)
        .has_headers(header)
        .double_quote(double_quote)
        .escape(escape)
        // Must be flexible to accept end of copy marker, which will always be 1
        // field.
        .flexible(true)
        .from_reader(data);

    let null_as_bytes = null.as_bytes();

    let mut record = ByteRecord::new();

    while rdr.read_byte_record(&mut record)? {
        if record.len() == 1 && record.iter().next() == Some(END_OF_COPY_MARKER) {
            break;
        }

        match record.len().cmp(&column_types.len()) {
            std::cmp::Ordering::Less => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing data for column",
            )),
            std::cmp::Ordering::Greater => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "extra data after last expected column",
            )),
            std::cmp::Ordering::Equal => Ok(()),
        }?;

        let mut row = Vec::new();
        let buf = RowArena::new();

        for (typ, raw_value) in column_types.iter().zip(record.iter()) {
            if raw_value == null_as_bytes {
                row.push(Datum::Null);
            } else {
                match mz_pgrepr::Value::decode_text(typ, raw_value) {
                    Ok(value) => row.push(value.into_datum(&buf, typ)),
                    Err(err) => {
                        let msg = format!("unable to decode column: {}", err);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                    }
                }
            }
        }
        rows.push(Row::pack(row));
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use mz_repr::ColumnType;

    use super::*;

    #[mz_ore::test]
    fn test_copy_format_text_parser() {
        let text = "\t\\nt e\t\\N\t\n\\x60\\xA\\x7D\\x4a\n\\44\\044\\123".as_bytes();
        let mut parser = CopyTextFormatParser::new(text, b'\t', "\\N");
        assert!(parser.is_column_delimiter());
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "\nt e".as_bytes()
        );
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        // null value
        assert!(parser
            .consume_raw_value()
            .expect("unexpected error")
            .is_none());
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        assert!(parser.is_end_of_line());
        parser.expect_end_of_line().expect("expected eol");
        // hex value
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "`\n}J".as_bytes()
        );
        parser.expect_end_of_line().expect("expected eol");
        // octal value
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "$$S".as_bytes()
        );
        assert!(parser.is_eof());
    }

    #[mz_ore::test]
    fn test_copy_format_text_empty_null_string() {
        let text = "\t\n10\t20\n30\t\n40\t".as_bytes();
        let expect = vec![
            vec![None, None],
            vec![Some("10"), Some("20")],
            vec![Some("30"), None],
            vec![Some("40"), None],
        ];
        let mut parser = CopyTextFormatParser::new(text, b'\t', "");
        for line in expect {
            for (i, value) in line.iter().enumerate() {
                if i > 0 {
                    parser
                        .expect_column_delimiter()
                        .expect("expected column delimiter");
                }
                match value {
                    Some(s) => {
                        assert!(!parser.consume_null_string());
                        assert_eq!(
                            parser
                                .consume_raw_value()
                                .expect("unexpected error")
                                .expect("unexpected empty result"),
                            s.as_bytes()
                        );
                    }
                    None => {
                        assert!(parser.consume_null_string());
                    }
                }
            }
            parser.expect_end_of_line().expect("expected eol");
        }
    }

    #[mz_ore::test]
    fn test_copy_format_text_parser_escapes() {
        struct TestCase {
            input: &'static str,
            expect: &'static [u8],
        }
        let tests = vec![
            TestCase {
                input: "simple",
                expect: b"simple",
            },
            TestCase {
                input: r#"new\nline"#,
                expect: b"new\nline",
            },
            TestCase {
                input: r#"\b\f\n\r\t\v\\"#,
                expect: b"\x08\x0c\n\r\t\x0b\\",
            },
            TestCase {
                input: r#"\0\12\123"#,
                expect: &[0, 0o12, 0o123],
            },
            TestCase {
                input: r#"\x1\xaf"#,
                expect: &[0x01, 0xaf],
            },
            TestCase {
                input: r#"T\n\07\xEV\x0fA\xb2C\1"#,
                expect: b"T\n\x07\x0eV\x0fA\xb2C\x01",
            },
            TestCase {
                input: r#"\\\""#,
                expect: b"\\\"",
            },
            TestCase {
                input: r#"\x"#,
                expect: b"x",
            },
            TestCase {
                input: r#"\xg"#,
                expect: b"xg",
            },
            TestCase {
                input: r#"\"#,
                expect: b"\\",
            },
            TestCase {
                input: r#"\8"#,
                expect: b"8",
            },
            TestCase {
                input: r#"\a"#,
                expect: b"a",
            },
            TestCase {
                input: r#"\x\xg\8\xH\x32\s\"#,
                expect: b"xxg8xH2s\\",
            },
        ];

        for test in tests {
            let mut parser = CopyTextFormatParser::new(test.input.as_bytes(), b'\t', "\\N");
            assert_eq!(
                parser
                    .consume_raw_value()
                    .expect("unexpected error")
                    .expect("unexpected empty result"),
                test.expect,
                "input: {}, expect: {:?}",
                test.input,
                std::str::from_utf8(test.expect),
            );
            assert!(parser.is_eof());
        }
    }

    #[mz_ore::test]
    fn test_copy_csv_format_params() {
        assert_eq!(
            CopyCsvFormatParams::try_new(Some(b't'), Some(b'q'), None, None, None),
            Ok(CopyCsvFormatParams {
                delimiter: b't',
                quote: b'q',
                escape: b'q',
                header: false,
                null: Cow::from(""),
            })
        );

        assert_eq!(
            CopyCsvFormatParams::try_new(
                Some(b't'),
                Some(b'q'),
                Some(b'e'),
                Some(true),
                Some("null".to_string())
            ),
            Ok(CopyCsvFormatParams {
                delimiter: b't',
                quote: b'q',
                escape: b'e',
                header: true,
                null: Cow::from("null"),
            })
        );

        assert_eq!(
            CopyCsvFormatParams::try_new(
                None,
                Some(b','),
                Some(b'e'),
                Some(true),
                Some("null".to_string())
            ),
            Err("COPY delimiter and quote must be different".to_string())
        );
    }

    #[mz_ore::test]
    fn test_copy_csv_row() -> Result<(), io::Error> {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.push(Datum::from("1,2,\"3\""));
        packer.push(Datum::Null);
        packer.push(Datum::from(1000u64));
        packer.push(Datum::from("qe")); // overridden quote and escape character in test below
        packer.push(Datum::from(""));

        let typ: RelationType = RelationType::new(vec![
            ColumnType {
                scalar_type: mz_repr::ScalarType::String,
                nullable: false,
            },
            ColumnType {
                scalar_type: mz_repr::ScalarType::String,
                nullable: true,
            },
            ColumnType {
                scalar_type: mz_repr::ScalarType::UInt64,
                nullable: false,
            },
            ColumnType {
                scalar_type: mz_repr::ScalarType::String,
                nullable: false,
            },
            ColumnType {
                scalar_type: mz_repr::ScalarType::String,
                nullable: false,
            },
        ]);

        let mut out = Vec::new();

        struct TestCase<'a> {
            params: CopyCsvFormatParams<'a>,
            expected: &'static [u8],
        }

        let tests = [
            TestCase {
                params: CopyCsvFormatParams::default(),
                expected: b"\"1,2,\"\"3\"\"\",,1000,qe,\"\"\n",
            },
            TestCase {
                params: CopyCsvFormatParams {
                    null: Cow::from("NULL"),
                    quote: b'q',
                    escape: b'e',
                    ..Default::default()
                },
                expected: b"q1,2,\"3\"q,NULL,1000,qeqeeq,\n",
            },
        ];

        for TestCase { params, expected } in tests {
            out.clear();
            let params = CopyFormatParams::Csv(params);
            let _ = encode_copy_format(params.clone(), &row, &typ, &mut out);
            let output = std::str::from_utf8(&out);
            assert_eq!(output, std::str::from_utf8(expected));
        }

        Ok(())
    }
}
