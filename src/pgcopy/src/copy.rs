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
use itertools::Itertools;
use mz_repr::{
    Datum, RelationDesc, Row, RowArena, RowRef, SharedRow, SqlColumnType, SqlRelationType,
    SqlScalarType,
};
use proptest::prelude::{Arbitrary, any};
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};

static END_OF_COPY_MARKER: &[u8] = b"\\.";

fn encode_copy_row_binary(
    row: &RowRef,
    typ: &SqlRelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    const NULL_BYTES: [u8; 4] = (-1i32).to_be_bytes();

    // 16-bit int of number of tuples.
    let count = i16::try_from(typ.column_types.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "column count does not fit into an i16",
        )
    })?;

    out.extend(count.to_be_bytes());
    let mut buf = BytesMut::new();
    for (field, typ) in row
        .iter()
        .zip_eq(&typ.column_types)
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
                                io::ErrorKind::InvalidData,
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

fn encode_copy_row_text(
    CopyTextFormatParams { null, delimiter }: &CopyTextFormatParams,
    row: &RowRef,
    typ: &SqlRelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    let null = null.as_bytes();
    let mut buf = BytesMut::new();
    for (idx, field) in mz_pgrepr::values_from_row(row, typ).into_iter().enumerate() {
        if idx > 0 {
            out.push(*delimiter);
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

fn encode_copy_row_csv(
    CopyCsvFormatParams {
        delimiter: delim,
        quote,
        escape,
        header: _,
        null,
    }: &CopyCsvFormatParams,
    row: &RowRef,
    typ: &SqlRelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    let null = null.as_bytes();
    let is_special = |c: &u8| *c == *delim || *c == *quote || *c == b'\r' || *c == b'\n';
    let mut buf = BytesMut::new();
    for (idx, field) in mz_pgrepr::values_from_row(row, typ).into_iter().enumerate() {
        if idx > 0 {
            out.push(*delim);
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
                    out.push(*quote);
                    for b in &buf {
                        if *b == *quote || *b == *escape {
                            out.push(*escape);
                        }
                        out.push(*b);
                    }
                    out.push(*quote);
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
        self.data
            .get(self.position..self.position + bytes.len())
            .map_or(false, |d| d == bytes)
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
                                                value = (value << 4) | c;
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
                                        value = (value << 3) | (c - b'0');
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
    Binary,
    Parquet,
}

impl CopyFormatParams<'static> {
    pub fn file_extension(&self) -> &str {
        match self {
            &CopyFormatParams::Text(_) => "txt",
            &CopyFormatParams::Csv(_) => "csv",
            &CopyFormatParams::Binary => "bin",
            &CopyFormatParams::Parquet => "parquet",
        }
    }

    pub fn requires_header(&self) -> bool {
        match self {
            CopyFormatParams::Text(_) => false,
            CopyFormatParams::Csv(params) => params.header,
            CopyFormatParams::Binary => false,
            CopyFormatParams::Parquet => false,
        }
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
        CopyFormatParams::Binary => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cannot decode as binary format",
        )),
        CopyFormatParams::Parquet => {
            // TODO(cf2): Support Parquet over STDIN.
            Err(io::Error::new(io::ErrorKind::Unsupported, "parquet format"))
        }
    }
}

/// Encodes the given `Row` into bytes based on the given `CopyFormatParams`.
pub fn encode_copy_format<'a>(
    params: &CopyFormatParams<'a>,
    row: &RowRef,
    typ: &SqlRelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    match params {
        CopyFormatParams::Text(params) => encode_copy_row_text(params, row, typ, out),
        CopyFormatParams::Csv(params) => encode_copy_row_csv(params, row, typ, out),
        CopyFormatParams::Binary => encode_copy_row_binary(row, typ, out),
        CopyFormatParams::Parquet => {
            // TODO(cf2): Support Parquet over STDIN.
            Err(io::Error::new(io::ErrorKind::Unsupported, "parquet format"))
        }
    }
}

pub fn encode_copy_format_header<'a>(
    params: &CopyFormatParams<'a>,
    desc: &RelationDesc,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    match params {
        CopyFormatParams::Text(_) => Ok(()),
        CopyFormatParams::Binary => Ok(()),
        CopyFormatParams::Csv(params) => {
            let mut header_row = Row::with_capacity(desc.arity());
            header_row
                .packer()
                .extend(desc.iter_names().map(|s| Datum::from(s.as_str())));
            let typ = SqlRelationType::new(vec![
                SqlColumnType {
                    scalar_type: SqlScalarType::String,
                    nullable: false,
                };
                desc.arity()
            ]);
            encode_copy_row_csv(params, &header_row, &typ, out)
        }
        CopyFormatParams::Parquet => {
            // TODO(cf2): Support Parquet over STDIN.
            Err(io::Error::new(io::ErrorKind::Unsupported, "parquet format"))
        }
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
                    Ok(value) => {
                        row.push(
                            value
                                .into_datum_decode_error(&buf, typ, "column")
                                .map_err(|msg| io::Error::new(io::ErrorKind::InvalidData, msg))?,
                        );
                    }
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

impl<'a> CopyCsvFormatParams<'a> {
    pub fn to_owned(&self) -> CopyCsvFormatParams<'static> {
        CopyCsvFormatParams {
            delimiter: self.delimiter,
            quote: self.quote,
            escape: self.escape,
            header: self.header,
            null: Cow::Owned(self.null.to_string()),
        }
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
            .prop_map(|(delimiter, diff, escape, header, null)| {
                // Delimiter and Quote need to be different.
                let diff = diff.saturating_sub(1).max(1);
                let quote = delimiter.wrapping_add(diff);

                Self::try_new(
                    Some(delimiter),
                    Some(quote),
                    Some(escape),
                    Some(header),
                    Some(null),
                )
                .expect("delimiter and quote should be different")
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
    let (double_quote, escape) = if quote == escape {
        (true, None)
    } else {
        (false, Some(escape))
    };

    let mut rdr = csv_core::ReaderBuilder::new()
        .delimiter(delimiter)
        .quote(quote)
        .escape(escape)
        .double_quote(double_quote)
        .build();

    let null_as_bytes = null.as_bytes();
    let mut rows = Vec::new();
    // We use csv-core (rather than the higher-level csv crate) so we can
    // recover per-field "was this field quoted?" information by inspecting the
    // first byte of each field's input. csv unquotes during parsing, which
    // makes a quoted empty string indistinguishable from an unquoted empty
    // field — and PostgreSQL COPY ... FORMAT CSV semantics need that
    // distinction to honor the NULL marker.
    let mut input = data;
    let mut output = vec![0u8; data.len().max(1024)];
    let mut out_pos = 0;
    let mut fields: Vec<(usize, usize, bool)> = Vec::new();
    let mut field_start = 0;
    let mut field_quoted: Option<bool> = None;
    let mut skip_header = header;
    // True at the start of a record (including the very first), where csv-core
    // may have left an orphaned terminator byte; false for fields that follow a
    // delimiter within a record.
    let mut at_record_start = true;

    loop {
        if field_quoted.is_none() {
            if at_record_start {
                // csv-core's default terminator is CRLF, and it reports a
                // record complete after consuming the `\r`, leaving the
                // trailing `\n` as the first byte of the next record's input
                // (and, after a worker chunk is split at a `\r` boundary, a
                // chunk can likewise begin with that orphan `\n`). csv-core
                // itself consumes and ignores those stray terminator bytes when
                // parsing the field, but the quote probe below must skip them
                // so it inspects the field's real first byte rather than an
                // orphan — otherwise a quoted field on any non-first CRLF record
                // is misclassified as unquoted. Only skip at a record boundary:
                // a `\r`/`\n` after a delimiter legitimately terminates an empty
                // trailing field and must not be swallowed.
                while matches!(input.first(), Some(&b'\r') | Some(&b'\n')) {
                    input = &input[1..];
                }
            }
            field_quoted = Some(input.first() == Some(&quote));
            field_start = out_pos;
        }

        let (result, nin, nout) = rdr.read_field(input, &mut output[out_pos..]);
        input = &input[nin..];
        out_pos += nout;

        match result {
            csv_core::ReadFieldResult::Field { record_end } => {
                fields.push((field_start, out_pos, field_quoted.take().unwrap()));
                // The next field begins a new record only if this field ended
                // one; otherwise it follows a delimiter mid-record.
                at_record_start = record_end;

                if record_end {
                    if skip_header {
                        skip_header = false;
                    } else if fields.len() == 1
                        && !fields[0].2
                        && &output[fields[0].0..fields[0].1] == END_OF_COPY_MARKER
                    {
                        // Bare `\.` on its own line: end-of-copy marker. A
                        // quoted `"\."` also decodes to `\.` but is data, so
                        // only an unquoted match terminates the import.
                        return Ok(rows);
                    } else {
                        match fields.len().cmp(&column_types.len()) {
                            std::cmp::Ordering::Less => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "missing data for column",
                                ));
                            }
                            std::cmp::Ordering::Greater => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "extra data after last expected column",
                                ));
                            }
                            std::cmp::Ordering::Equal => {}
                        }

                        let mut row_builder = SharedRow::get();
                        let mut row_packer = row_builder.packer();
                        for (typ, &(s, e, quoted)) in column_types.iter().zip_eq(fields.iter()) {
                            let raw_value = &output[s..e];
                            if !quoted && raw_value == null_as_bytes {
                                row_packer.push(Datum::Null);
                            } else {
                                let s = match std::str::from_utf8(raw_value) {
                                    Ok(s) => s,
                                    Err(err) => {
                                        let msg = format!("invalid utf8 data in column: {}", err);
                                        return Err(io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            msg,
                                        ));
                                    }
                                };
                                if let Err(err) =
                                    mz_pgrepr::Value::decode_text_into_row(typ, s, &mut row_packer)
                                {
                                    let msg = format!("unable to decode column: {}", err);
                                    return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                                }
                            }
                        }
                        rows.push(row_builder.clone());
                    }
                    fields.clear();
                    out_pos = 0;
                }
            }
            csv_core::ReadFieldResult::OutputFull => {
                let new_len = output.len().saturating_mul(2).max(out_pos + 1);
                output.resize(new_len, 0);
            }
            csv_core::ReadFieldResult::InputEmpty => {
                // We've consumed all input; loop again with the now-empty
                // slice so csv-core can flush any buffered trailing field.
            }
            csv_core::ReadFieldResult::End => return Ok(rows),
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::collections::CollectionExt;
    use mz_repr::SqlColumnType;
    use proptest::prelude::*;

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
        assert!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .is_none()
        );
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

        let typ: SqlRelationType = SqlRelationType::new(vec![
            SqlColumnType {
                scalar_type: mz_repr::SqlScalarType::String,
                nullable: false,
            },
            SqlColumnType {
                scalar_type: mz_repr::SqlScalarType::String,
                nullable: true,
            },
            SqlColumnType {
                scalar_type: mz_repr::SqlScalarType::UInt64,
                nullable: false,
            },
            SqlColumnType {
                scalar_type: mz_repr::SqlScalarType::String,
                nullable: false,
            },
            SqlColumnType {
                scalar_type: mz_repr::SqlScalarType::String,
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
            let _ = encode_copy_format(&params, &row, &typ, &mut out);
            let output = std::str::from_utf8(&out);
            assert_eq!(output, std::str::from_utf8(expected));
        }

        Ok(())
    }

    #[mz_ore::test]
    fn test_decode_copy_format_csv_end_marker() {
        // Bare `\.` on its own line terminates the COPY. A quoted `"\."`
        // decodes to the same bytes but must be treated as data. This must
        // hold for every line ending (LF, CRLF, CR): csv-core places a CRLF
        // record boundary between `\r` and `\n`, so a naive raw-byte check is
        // fooled by the orphaned terminator bytes on CRLF/CR input.
        let column_types = vec![mz_pgrepr::Type::from(&mz_repr::SqlScalarType::String)];

        let decode_strings = |input: &[u8]| -> Vec<String> {
            decode_copy_format_csv(input, &column_types, CopyCsvFormatParams::default())
                .expect("decode should succeed")
                .iter()
                .map(|r| match r.iter().next().unwrap() {
                    Datum::String(s) => s.to_owned(),
                    d => panic!("unexpected datum: {:?}", d),
                })
                .collect()
        };

        for eol in [&b"\n"[..], b"\r\n", b"\r"] {
            let join = |lines: &[&str]| -> Vec<u8> {
                let mut out = Vec::new();
                for line in lines {
                    out.extend_from_slice(line.as_bytes());
                    out.extend_from_slice(eol);
                }
                out
            };

            // Quoted "\." is data — all three rows are imported.
            assert_eq!(
                decode_strings(&join(&["before", "\"\\.\"", "after"])),
                vec!["before", "\\.", "after"],
                "quoted marker, eol={eol:?}"
            );

            // Bare `\.` terminates the COPY; rows after it are dropped.
            assert_eq!(
                decode_strings(&join(&["first", "\\.", "ignored"])),
                vec!["first"],
                "bare marker, eol={eol:?}"
            );
        }
    }

    #[mz_ore::test]
    fn test_decode_copy_format_csv_leading_orphan() {
        // Worker chunks after the first begin at a `\r` boundary, so for CRLF
        // input a chunk's bytes start with the orphan `\n` that csv-core left
        // behind. The decoder must still classify the first field's quote
        // state from its real first byte, not the stray `\n`. Regression test
        // for the per-chunk quote probe.
        let column_types = vec![
            mz_pgrepr::Type::from(&mz_repr::SqlScalarType::String),
            mz_pgrepr::Type::from(&mz_repr::SqlScalarType::String),
        ];

        let rows = decode_copy_format_csv(
            b"\n\"\",x\r\ny,z\r\n",
            &column_types,
            CopyCsvFormatParams::default(),
        )
        .expect("decode should succeed");
        let got: Vec<Vec<Datum>> = rows.iter().map(|r| r.iter().collect()).collect();
        assert_eq!(got.len(), 2);
        // Quoted empty first field on the leading-orphan record must decode to
        // the empty string, not SQL NULL (the bug would misread it as
        // unquoted and match the default empty NULL marker).
        assert_eq!(got[0][0], Datum::String(""));
        assert_eq!(got[0][1], Datum::String("x"));
        assert_eq!(got[1][0], Datum::String("y"));
        assert_eq!(got[1][1], Datum::String("z"));
    }

    #[mz_ore::test]
    fn test_decode_copy_format_csv_quoted_null() {
        // PG COPY ... FORMAT CSV distinguishes quoted vs unquoted NULL
        // markers: unquoted → SQL NULL, quoted → the literal string.
        let column_types = vec![
            mz_pgrepr::Type::from(&mz_repr::SqlScalarType::String),
            mz_pgrepr::Type::from(&mz_repr::SqlScalarType::String),
        ];

        // Lines as (col_a, col_b) literals, joined per-eol below. The quoted
        // vs unquoted distinction must survive CRLF/CR line endings, where
        // csv-core leaves an orphaned terminator byte at the start of every
        // non-first record.
        let cases: &[(CopyCsvFormatParams, &[(&str, &str)], &[[Option<&str>; 2]])] = &[
            // Default params: NULL marker is empty string.
            (
                CopyCsvFormatParams::default(),
                &[("a", ""), ("b", "\"\""), ("\"\"", "c")],
                &[
                    [Some("a"), None],
                    [Some("b"), Some("")],
                    [Some(""), Some("c")],
                ],
            ),
            // Custom NULL marker "NULL".
            (
                CopyCsvFormatParams {
                    null: Cow::from("NULL"),
                    ..Default::default()
                },
                &[("a", "NULL"), ("b", "\"NULL\""), ("NULL", "c")],
                &[
                    [Some("a"), None],
                    [Some("b"), Some("NULL")],
                    [None, Some("c")],
                ],
            ),
        ];

        for eol in [&b"\n"[..], b"\r\n", b"\r"] {
            for (params, lines, expected) in cases {
                let mut input = Vec::new();
                for (a, b) in *lines {
                    input.extend_from_slice(a.as_bytes());
                    input.push(b',');
                    input.extend_from_slice(b.as_bytes());
                    input.extend_from_slice(eol);
                }
                let rows = decode_copy_format_csv(&input, &column_types, params.clone())
                    .expect("decode should succeed");
                assert_eq!(rows.len(), expected.len(), "eol={eol:?}");
                for (row, want) in rows.iter().zip_eq(expected.iter()) {
                    let got: Vec<Datum> = row.iter().collect();
                    assert_eq!(got.len(), 2);
                    for (g, w) in got.iter().zip_eq(want.iter()) {
                        match (g, w) {
                            (Datum::Null, None) => {}
                            (Datum::String(s), Some(w)) => assert_eq!(s, w, "eol={eol:?}"),
                            _ => panic!("mismatch: got {g:?}, want {w:?}, eol={eol:?}"),
                        }
                    }
                }
            }
        }
    }

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn proptest_csv_roundtrips(copy_csv_params: CopyCsvFormatParams)  {
            // Given a SqlScalarType and Datum roundtrips it through the CSV COPY format.
            let try_roundtrip_datum = |scalar_type: &SqlScalarType, datum| {
                let row = Row::pack_slice(&[datum]);
                let typ = SqlRelationType::new(vec![
                    SqlColumnType {
                        scalar_type: scalar_type.clone(),
                        nullable: true,
                    }
                ]);

                let mut buf = Vec::new();
                let mut csv_params = copy_csv_params.clone();
                // TODO: Encoding never writes a header.
                csv_params.header = false;
                let params = CopyFormatParams::Csv(csv_params);

                // Roundtrip the Row through our CSV format.
                encode_copy_format(&params, &row, &typ, &mut buf)?;
                let column_types = typ
                    .column_types
                    .iter()
                    .map(|x| &x.scalar_type)
                    .map(mz_pgrepr::Type::from)
                    .collect::<Vec<mz_pgrepr::Type>>();
                let result = decode_copy_format(&buf, &column_types, params);

                match result {
                    Ok(rows) => {
                        let out_str = std::str::from_utf8(&buf[..]);

                        prop_assert_eq!(
                            rows.len(),
                            1,
                            "unexpected number of rows! {:?}, csv string: {:?}", rows, out_str
                        );
                        let output = rows.into_element();

                        prop_assert_eq!(
                            row,
                            output,
                            "csv string: {:?}, scalar_type: {:?}", out_str, scalar_type
                        );
                    }
                    _ => {
                        // ignoring decoding failures
                    }
                }

                Ok(())
            };

            // Try roundtripping all of our interesting Datums.
            for scalar_type in SqlScalarType::enumerate() {
                for datum in scalar_type.interesting_datums() {
                    // TODO: The decoder cannot differentiate between empty string and null.
                    if let Some(value) = mz_pgrepr::Value::from_datum(datum, scalar_type) {
                        let mut buf = bytes::BytesMut::new();
                        value.encode_text(&mut buf);

                        if let Ok(datum_str) = std::str::from_utf8(&buf[..]) {
                            if datum_str == copy_csv_params.null {
                                continue;
                            }
                        }
                    }

                    let updated_datum = match datum {
                        // TODO: Fix roundtrip decoding of these types.
                        Datum::Timestamp(_) | Datum::TimestampTz(_) | Datum::Null => {
                            continue;
                        }
                        Datum::String(s) => {
                            // TODO: The decoder cannot differentiate between empty string and null.
                            if s.trim() == copy_csv_params.null || s.trim().is_empty() {
                                continue;
                            } else {
                                Datum::String(s)
                            }
                        }
                        other => other,
                    };

                    let result = try_roundtrip_datum(scalar_type, updated_datum);
                    prop_assert!(result.is_ok(), "failure: {result:?}");
                }
            }
        }
    }
}
