// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conversion between datums and the Arrow IPC batches arrow-udf guests
//! exchange.
//!
//! The Arrow representation of each [`UdfType`] is fixed by what the
//! arrow-udf macros generate for the guest, which is why this codec is
//! separate from `mz_arrow_util`'s COPY TO and Iceberg mappings: those choose
//! `LargeUtf8`, `LargeBinary` and `Decimal128`, where guests expect `Utf8`,
//! `Binary` and decimal text.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder,
    Date32Array, Date32Builder, Float32Array, Float32Builder, Float64Array, Float64Builder,
    Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    IntervalMonthDayNanoArray, IntervalMonthDayNanoBuilder, ListArray, ListBuilder, RecordBatch,
    StringArray, StringBuilder, Time64MicrosecondArray, Time64MicrosecondBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt16Array, UInt16Builder,
    UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, make_builder,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, IntervalMonthDayNano, IntervalUnit, Schema, TimeUnit};
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::{FileDecoder, read_footer_length};
use arrow_ipc::writer::FileWriter;
use arrow_ipc::{root_as_footer_with_opts, root_as_message_with_opts};
use chrono::{DateTime, NaiveTime, Timelike};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RowArena, SqlScalarType, strconv};
use mz_wasm_udf_abi::{
    DECIMAL_EXTENSION, ERROR_COLUMN, EXTENSION_NAME_KEY, JSON_EXTENSION, UdfType,
};

use crate::call::{MAX_MESSAGE_BYTES, truncated_str};

/// A value's SQL type together with the arrow-udf type it crosses as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueType {
    pub sql: SqlScalarType,
    pub udf: UdfType,
}

impl ValueType {
    pub fn new(sql: SqlScalarType) -> Result<Self, mz_wasm_udf_abi::UnsupportedType> {
        let udf = UdfType::from_sql(&sql)?;
        Ok(ValueType { sql, udf })
    }

    fn element(&self) -> ValueType {
        let (UdfType::List(udf), Some(sql)) = (&self.udf, list_element(&self.sql)) else {
            panic!("not a list type: {self:?}");
        };
        ValueType {
            sql: sql.clone(),
            udf: (**udf).clone(),
        }
    }
}

fn list_element(typ: &SqlScalarType) -> Option<&SqlScalarType> {
    match typ {
        SqlScalarType::List { element_type, .. } => Some(element_type),
        SqlScalarType::Array(element_type) => Some(element_type),
        _ => None,
    }
}

/// The Arrow type the guest expects for `typ`.
pub fn data_type(typ: &UdfType) -> DataType {
    match typ {
        UdfType::Boolean => DataType::Boolean,
        UdfType::Int16 => DataType::Int16,
        UdfType::Int32 => DataType::Int32,
        UdfType::Int64 => DataType::Int64,
        UdfType::UInt16 => DataType::UInt16,
        UdfType::UInt32 => DataType::UInt32,
        UdfType::UInt64 => DataType::UInt64,
        UdfType::Float32 => DataType::Float32,
        UdfType::Float64 => DataType::Float64,
        UdfType::Decimal | UdfType::Json | UdfType::String => DataType::Utf8,
        UdfType::Date32 => DataType::Date32,
        UdfType::Time64 => DataType::Time64(TimeUnit::Microsecond),
        UdfType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        UdfType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
        UdfType::Binary => DataType::Binary,
        UdfType::List(elem) => DataType::List(Arc::new(field("item", elem))),
    }
}

// `Field::with_metadata` takes a std `HashMap`, and field metadata is never
// iterated here.
#[allow(clippy::disallowed_types)]
fn field(name: &str, typ: &UdfType) -> Field {
    let field = Field::new(name, data_type(typ), true);
    let extension = match typ {
        UdfType::Decimal => Some(DECIMAL_EXTENSION),
        UdfType::Json => Some(JSON_EXTENSION),
        _ => None,
    };
    match extension {
        Some(extension) => field.with_metadata(std::collections::HashMap::from([(
            EXTENSION_NAME_KEY.to_string(),
            extension.to_string(),
        )])),
        None => field,
    }
}

/// Checks that `datum` can be encoded as `typ`, so that encoding a batch of
/// checked rows cannot fail partway through.
pub fn check(typ: &ValueType, datum: Datum) -> Result<(), String> {
    match (&typ.udf, datum) {
        (_, Datum::Null) => Ok(()),
        (UdfType::Interval, Datum::Interval(iv)) => {
            iv.micros.checked_mul(1000).map(|_| ()).ok_or_else(|| {
                format!("interval {iv} is out of range for the guest's nanosecond representation")
            })
        }
        (UdfType::Decimal, Datum::Numeric(n)) => {
            // The arrow-udf Rust SDK parses a decimal argument into a
            // `rust_decimal::Decimal`, which holds at most 28 significant
            // digits at a scale of at most 28, and panics on anything else.
            let n = n.0;
            let digits = i32::try_from(n.digits()).expect("numeric digits fit in i32");
            let fits =
                n.is_finite() && digits <= 28 && n.exponent() >= -28 && digits + n.exponent() <= 28;
            if fits {
                Ok(())
            } else {
                Err(format!(
                    "numeric {n} is out of range for the guest, which supports at most 28 \
                     significant digits and 28 decimal places, and no NaN"
                ))
            }
        }
        (UdfType::List(_), Datum::Array(array)) => {
            if array.dims().ndims() > 1 {
                return Err("multidimensional arrays are not supported".into());
            }
            // The guest sees a plain list, so it cannot return the bounds.
            if array.dims().into_iter().any(|dim| dim.lower_bound != 1) {
                return Err("arrays with a lower bound other than 1 are not supported".into());
            }
            let elem = typ.element();
            array.elements().iter().try_for_each(|d| check(&elem, d))
        }
        (UdfType::List(_), Datum::List(list)) => {
            let elem = typ.element();
            list.iter().try_for_each(|d| check(&elem, d))
        }
        _ => Ok(()),
    }
}

/// An estimate of the bytes `datum` occupies once encoded, for batch sizing.
pub fn encoded_size(datum: Datum) -> usize {
    match datum {
        Datum::String(s) => s.len() + 4,
        Datum::Bytes(b) => b.len() + 4,
        Datum::Array(a) => a.elements().iter().map(encoded_size).sum::<usize>() + 4,
        Datum::List(l) => l.iter().map(encoded_size).sum::<usize>() + 4,
        // A jsonb object, encoded as JSON text.
        Datum::Map(m) => {
            m.iter()
                .map(|(k, v)| k.len() + 4 + encoded_size(v))
                .sum::<usize>()
                + 4
        }
        // Decimal text, up to 39 digits with a sign, point and exponent.
        Datum::Numeric(_) => 48,
        _ => 16,
    }
}

/// Encodes one Arrow IPC file with a single record batch holding `rows`,
/// one column per argument. Every datum must have passed [`check`].
pub fn encode(arg_types: &[ValueType], rows: &[&[Datum]]) -> Result<Vec<u8>, String> {
    let fields: Vec<Field> = arg_types
        .iter()
        .enumerate()
        .map(|(i, t)| field(&format!("arg{i}"), &t.udf))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let columns: Vec<ArrayRef> = arg_types
        .iter()
        .enumerate()
        .map(|(col, typ)| {
            let mut builder = make_builder(&data_type(&typ.udf), rows.len());
            for row in rows {
                append(builder.as_mut(), typ, row[col]);
            }
            builder.finish()
        })
        .collect();
    let batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
    } else {
        RecordBatch::try_new(schema, columns)
    }
    .map_err(|e| e.to_string())?;

    let mut writer = FileWriter::try_new(Vec::new(), &batch.schema()).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())?;
    writer.into_inner().map_err(|e| e.to_string())
}

fn downcast<B: 'static>(builder: &mut dyn ArrayBuilder) -> &mut B {
    builder
        .as_any_mut()
        .downcast_mut::<B>()
        .expect("builder matches data_type")
}

fn append(builder: &mut dyn ArrayBuilder, typ: &ValueType, datum: Datum) {
    macro_rules! value {
        ($builder:ty, $value:expr) => {{
            let b = downcast::<$builder>(builder);
            if datum.is_null() {
                b.append_null();
            } else {
                b.append_value($value);
            }
        }};
    }
    match &typ.udf {
        UdfType::Boolean => value!(BooleanBuilder, datum.unwrap_bool()),
        UdfType::Int16 => value!(Int16Builder, datum.unwrap_int16()),
        UdfType::Int32 => value!(Int32Builder, datum.unwrap_int32()),
        UdfType::Int64 => value!(Int64Builder, datum.unwrap_int64()),
        UdfType::UInt16 => value!(UInt16Builder, datum.unwrap_uint16()),
        UdfType::UInt32 => value!(UInt32Builder, datum.unwrap_uint32()),
        UdfType::UInt64 => value!(UInt64Builder, datum.unwrap_uint64()),
        UdfType::Float32 => value!(Float32Builder, datum.unwrap_float32()),
        UdfType::Float64 => value!(Float64Builder, datum.unwrap_float64()),
        UdfType::Decimal => value!(StringBuilder, {
            let mut s = String::new();
            strconv::format_numeric(&mut s, &datum.unwrap_numeric());
            s
        }),
        UdfType::Json => value!(StringBuilder, JsonbRef::from_datum(datum).to_string()),
        UdfType::String => value!(StringBuilder, datum.unwrap_str()),
        UdfType::Binary => value!(BinaryBuilder, datum.unwrap_bytes()),
        UdfType::Date32 => value!(Date32Builder, datum.unwrap_date().unix_epoch_days()),
        UdfType::Time64 => value!(Time64MicrosecondBuilder, {
            let t = datum.unwrap_time();
            i64::from(t.num_seconds_from_midnight()) * 1_000_000 + i64::from(t.nanosecond() / 1000)
        }),
        UdfType::Timestamp => value!(
            TimestampMicrosecondBuilder,
            match datum {
                Datum::Timestamp(ts) => ts.and_utc().timestamp_micros(),
                Datum::TimestampTz(ts) => ts.timestamp_micros(),
                d => panic!("not a timestamp: {d:?}"),
            }
        ),
        UdfType::Interval => value!(IntervalMonthDayNanoBuilder, {
            let iv = datum.unwrap_interval();
            let nanos = iv
                .micros
                .checked_mul(1000)
                .expect("checked before encoding");
            IntervalMonthDayNano::new(iv.months, iv.days, nanos)
        }),
        UdfType::List(_) => {
            let elem = typ.element();
            let b = downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder);
            match datum {
                Datum::Null => b.append_null(),
                Datum::Array(array) => {
                    for d in array.elements().iter() {
                        append(b.values().as_mut(), &elem, d);
                    }
                    b.append(true);
                }
                Datum::List(list) => {
                    for d in list.iter() {
                        append(b.values().as_mut(), &elem, d);
                    }
                    b.append(true);
                }
                d => panic!("not a list: {d:?}"),
            }
        }
    }
}

/// The guest's answer for one row.
#[derive(Debug)]
pub enum RowResult<'a> {
    Value(Datum<'a>),
    /// The guest returned an error message for the row.
    GuestError(String),
    /// The guest returned a value that is not valid for the declared type.
    Invalid(String),
}

/// Reads the one record batch of a guest's output IPC file, which must have
/// `rows` rows and the output schema of a function returning `ret`: the
/// result in column 0, optionally followed by a Utf8 error column.
///
/// The output is untrusted. `arrow_ipc::reader::FileReader` allocates
/// whatever footer and block lengths a file declares and unwraps some of
/// them, so this checks the framing, the declared row count and the schema
/// against `bytes` before decoding anything, and decodes from a copy of the
/// one block. Decoding requires aligned buffers: realigning copies each
/// buffer, and buffers may overlap, so a small output could otherwise make
/// the host copy far more than the guest's memory limit. Schema conversion
/// and array decoding still unwrap on some malformed metadata, so they run
/// under `catch_unwind`.
fn read_batch(bytes: &[u8], rows: usize, ret: &DataType) -> Result<RecordBatch, String> {
    const MAGIC: &[u8] = b"ARROW1";
    // The leading magic and its padding.
    const HEADER_LEN: usize = 8;
    // The footer length and the trailing magic.
    const TRAILER_LEN: usize = 10;
    // A block's metadata starts with a continuation marker and a length.
    const MESSAGE_PREFIX_LEN: usize = 8;

    if bytes.len() < HEADER_LEN + TRAILER_LEN
        || !bytes.starts_with(MAGIC)
        || !bytes.ends_with(MAGIC)
    {
        return Err("output is not an Arrow IPC file".into());
    }
    let trailer_start = bytes.len() - TRAILER_LEN;
    let footer_len = read_footer_length(bytes[trailer_start..].try_into().expect("10 bytes"))
        .map_err(|e| e.to_string())?;
    let footer_start = trailer_start
        .checked_sub(footer_len)
        .filter(|start| *start >= HEADER_LEN)
        .ok_or("output footer is out of bounds")?;
    let footer_bytes = &bytes[footer_start..trailer_start];
    let footer = root_as_footer_with_opts(&verifier_options(footer_bytes), footer_bytes)
        .map_err(|e| format!("invalid output footer: {e}"))?;
    if footer.dictionaries().is_some_and(|d| !d.is_empty()) {
        return Err("output contains dictionaries".into());
    }
    let blocks = footer.recordBatches().unwrap_or_default();
    if blocks.len() != 1 {
        return Err(format!(
            "output contains {} record batches, expected 1",
            blocks.len()
        ));
    }
    let block = blocks.get(0);
    let metadata_len = usize::try_from(block.metaDataLength()).ok();
    let block_range = (|| {
        let start = usize::try_from(block.offset()).ok()?;
        let body_len = usize::try_from(block.bodyLength()).ok()?;
        let end = start.checked_add(metadata_len?)?.checked_add(body_len)?;
        (start >= HEADER_LEN && end <= footer_start).then_some(start..end)
    })();
    let (Some(block_range), Some(metadata_len)) = (block_range, metadata_len) else {
        return Err("output record batch is out of bounds".into());
    };
    if metadata_len < MESSAGE_PREFIX_LEN {
        return Err("output record batch metadata is truncated".into());
    }

    let metadata = &bytes[block_range.start..block_range.start + metadata_len];
    let message = match metadata[..4] == [0xff; 4] {
        true => &metadata[MESSAGE_PREFIX_LEN..],
        false => &metadata[4..],
    };
    let declared_rows = root_as_message_with_opts(&verifier_options(message), message)
        .map_err(|e| format!("invalid output message: {e}"))?
        .header_as_record_batch()
        .ok_or("output block is not a record batch")?
        .length();
    if usize::try_from(declared_rows).ok() != Some(rows) {
        return Err(format!(
            "output has {declared_rows} rows for {rows} input rows"
        ));
    }
    let schema = footer.schema().ok_or("output has no schema")?;
    if !schema.endianness().equals_to_target_endianness() {
        return Err("output is not little-endian".into());
    }
    check_raw_schema(&schema)?;

    let buffer = Buffer::from_slice_ref(&bytes[block_range]);
    let version = footer.version();
    let message = |e: &dyn std::fmt::Display| {
        truncated_str(&format!("invalid output: {e}"), MAX_MESSAGE_BYTES)
    };
    mz_ore::panic::catch_unwind_str(|| {
        let schema = fb_to_schema(schema);
        check_output_schema(&schema, ret)?;
        // Decode against the declared types alone. The guest's field names
        // and metadata could otherwise reach error messages, and Arrow prints
        // field metadata in hash order, which is not deterministic.
        let mut fields = vec![Field::new("result", ret.clone(), true)];
        if schema.fields().len() > 1 {
            fields.push(Field::new(ERROR_COLUMN, DataType::Utf8, true));
        }
        FileDecoder::new(Arc::new(Schema::new(fields)), version)
            .with_require_alignment(true)
            .read_record_batch(block, &buffer)
            .map_err(|e| message(&e))?
            .ok_or_else(|| "output contains no record batch".to_string())
    })
    .map_err(|panic| message(&panic))?
}

/// FlatBuffers verification limits for a buffer of guest output. The default
/// limits allow a buffer whose tables share children to expand to 2 GiB when
/// read as a tree, a cost the host would pay outside the guest's fuel.
fn verifier_options(buffer: &[u8]) -> flatbuffers::VerifierOptions {
    flatbuffers::VerifierOptions {
        max_depth: 32,
        max_apparent_size: buffer.len().saturating_mul(4).saturating_add(4096),
        ..Default::default()
    }
}

/// Checks the size of a guest output schema before Arrow converts it to owned
/// values, which copies every field, name and metadata entry it reaches. A
/// valid output has one or two columns of shallow types and short names.
fn check_raw_schema(schema: &arrow_ipc::Schema<'_>) -> Result<(), String> {
    const MAX_FIELDS: usize = 64;
    const MAX_STRING_BYTES: usize = 256;
    const MAX_METADATA_ENTRIES: usize = 8;

    fn check_metadata(
        metadata: Option<
            flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<arrow_ipc::KeyValue<'_>>>,
        >,
    ) -> Result<(), String> {
        let Some(metadata) = metadata else {
            return Ok(());
        };
        if metadata.len() > MAX_METADATA_ENTRIES {
            return Err("output schema has too much metadata".into());
        }
        for entry in metadata {
            let len = |s: Option<&str>| s.map_or(0, str::len);
            if len(entry.key()) > MAX_STRING_BYTES || len(entry.value()) > MAX_STRING_BYTES {
                return Err("output schema has too much metadata".into());
            }
        }
        Ok(())
    }

    check_metadata(schema.custom_metadata())?;
    let top = schema.fields().map_or(0, |fields| fields.len());
    if !(1..=2).contains(&top) {
        return Err(format!("output has {top} columns, expected 1 or 2"));
    }
    let mut pending: Vec<_> = schema.fields().into_iter().flatten().collect();
    let mut seen = 0;
    while let Some(field) = pending.pop() {
        seen += 1;
        if seen > MAX_FIELDS {
            return Err("output schema has too many fields".into());
        }
        if field.name().map_or(0, str::len) > MAX_STRING_BYTES {
            return Err("output schema has a field name that is too long".into());
        }
        check_metadata(field.custom_metadata())?;
        pending.extend(field.children().into_iter().flatten());
    }
    Ok(())
}

/// Checks that `schema` is the output schema of a function returning `ret`.
///
/// arrow-udf names the result column after the function, so the error column
/// is identified by position and only then checked by name.
fn check_output_schema(schema: &Schema, ret: &DataType) -> Result<(), String> {
    let fields = schema.fields();
    let Some(values) = fields.first() else {
        return Err("output has no columns".into());
    };
    if !types_match(values.data_type(), ret) {
        return Err(format!(
            "output column has type {}, expected {ret}",
            values.data_type()
        ));
    }
    match fields.get(1) {
        None => {}
        Some(errors) if errors.name() == ERROR_COLUMN && errors.data_type() == &DataType::Utf8 => {}
        Some(field) => {
            return Err(format!(
                "output column 1 must be a Utf8 column named {ERROR_COLUMN:?}, found {:?} of \
                 type {}",
                field.name(),
                field.data_type()
            ));
        }
    }
    if fields.len() > 2 {
        return Err(format!(
            "output has {} columns, expected at most 2",
            fields.len()
        ));
    }
    Ok(())
}

/// Decodes a guest's output batch for `rows` input rows.
///
/// Errors describe a batch that does not follow the ABI, which the caller
/// treats as a failure of the whole call.
///
/// The output may decode to at most `max_values` values, nested values
/// included. Exceeding that is an error for the whole batch.
pub fn decode<'a>(
    bytes: &[u8],
    ret: &ValueType,
    rows: usize,
    max_values: usize,
    arena: &'a RowArena,
) -> Result<Vec<RowResult<'a>>, String> {
    let batch = read_batch(bytes, rows, &data_type(&ret.udf))?;
    let values = batch.column(0);
    let errors = (batch.num_columns() > 1).then(|| get::<StringArray>(batch.column(1).as_ref()));

    let mut budget = max_values;
    let mut results = Vec::with_capacity(rows);
    for i in 0..rows {
        if let Some(errors) = &errors {
            if errors.is_valid(i) {
                let message = truncated_str(errors.value(i), MAX_MESSAGE_BYTES);
                results.push(RowResult::GuestError(message));
                continue;
            }
        }
        results.push(
            match datum_at(values.as_ref(), i, ret, arena, &mut budget) {
                Ok(d) => RowResult::Value(d),
                Err(DecodeError::Invalid(e)) => RowResult::Invalid(e),
                Err(DecodeError::TooManyValues) => {
                    return Err(format!("output decodes to more than {max_values} values"));
                }
            },
        );
    }
    Ok(results)
}

/// Why a value of a guest's output could not be decoded.
enum DecodeError {
    /// The value does not represent a value of its SQL type. An error for its
    /// row.
    Invalid(String),
    /// The output decodes to more values than its budget allows. An error for
    /// the whole batch.
    TooManyValues,
}

impl From<String> for DecodeError {
    fn from(message: String) -> Self {
        DecodeError::Invalid(message)
    }
}

/// Whether `actual` is `expected`, ignoring list field names and
/// nullability, which guests are free to choose.
fn types_match(actual: &DataType, expected: &DataType) -> bool {
    match (actual, expected) {
        (DataType::List(a), DataType::List(e)) => types_match(a.data_type(), e.data_type()),
        (a, e) => a == e,
    }
}

fn get<A: 'static>(array: &dyn Array) -> &A {
    array
        .as_any()
        .downcast_ref::<A>()
        .expect("type checked against data_type")
}

/// Decodes value `i` of `array`, charging each value it decodes, nested
/// values included, against `budget`.
fn datum_at<'a>(
    array: &dyn Array,
    i: usize,
    typ: &ValueType,
    arena: &'a RowArena,
    budget: &mut usize,
) -> Result<Datum<'a>, DecodeError> {
    *budget = budget.checked_sub(1).ok_or(DecodeError::TooManyValues)?;
    if array.is_null(i) {
        return Ok(Datum::Null);
    }
    // Guest text quoted in a message is capped like every guest message.
    let quoted = |s: &str| format!("{:?}", truncated_str(s, MAX_MESSAGE_BYTES));
    Ok(match &typ.udf {
        UdfType::Boolean => Datum::from(get::<BooleanArray>(array).value(i)),
        UdfType::Int16 => Datum::Int16(get::<Int16Array>(array).value(i)),
        UdfType::Int32 => Datum::Int32(get::<Int32Array>(array).value(i)),
        UdfType::Int64 => Datum::Int64(get::<Int64Array>(array).value(i)),
        UdfType::UInt16 => Datum::UInt16(get::<UInt16Array>(array).value(i)),
        UdfType::UInt32 => Datum::UInt32(get::<UInt32Array>(array).value(i)),
        UdfType::UInt64 => Datum::UInt64(get::<UInt64Array>(array).value(i)),
        UdfType::Float32 => Datum::from(get::<Float32Array>(array).value(i)),
        UdfType::Float64 => Datum::from(get::<Float64Array>(array).value(i)),
        UdfType::Decimal => {
            let s = get::<StringArray>(array).value(i);
            let n = strconv::parse_numeric(s).map_err(|e| {
                truncated_str(
                    &format!("invalid decimal {}: {e}", quoted(s)),
                    MAX_MESSAGE_BYTES,
                )
            })?;
            Datum::Numeric(n)
        }
        UdfType::Json => {
            let s = get::<StringArray>(array).value(i);
            let jsonb = strconv::parse_jsonb(s).map_err(|e| {
                truncated_str(
                    &format!("invalid JSON {}: {e}", quoted(s)),
                    MAX_MESSAGE_BYTES,
                )
            })?;
            arena.push_unary_row(jsonb.into_row())
        }
        UdfType::String => {
            let s = get::<StringArray>(array).value(i);
            Datum::String(arena.push_string(s.to_owned()))
        }
        UdfType::Binary => {
            let b = get::<BinaryArray>(array).value(i);
            Datum::Bytes(arena.push_bytes(b))
        }
        UdfType::Date32 => {
            let days = get::<Date32Array>(array).value(i);
            Datum::Date(Date::from_unix_epoch(days).map_err(|e| format!("invalid date: {e}"))?)
        }
        UdfType::Time64 => {
            let micros = get::<Time64MicrosecondArray>(array).value(i);
            let time = u64::try_from(micros)
                .ok()
                .and_then(|m| {
                    let secs = u32::try_from(m / 1_000_000).ok()?;
                    let nanos = u32::try_from(m % 1_000_000).ok()? * 1000;
                    NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                })
                .ok_or_else(|| format!("time {micros}us is out of range"))?;
            Datum::Time(time)
        }
        UdfType::Timestamp => {
            let micros = get::<TimestampMicrosecondArray>(array).value(i);
            let ts = DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| format!("timestamp {micros}us is out of range"))?;
            match &typ.sql {
                SqlScalarType::TimestampTz { .. } => Datum::TimestampTz(
                    CheckedTimestamp::from_timestamplike(ts).map_err(|e| e.to_string())?,
                ),
                _ => Datum::Timestamp(
                    CheckedTimestamp::from_timestamplike(ts.naive_utc())
                        .map_err(|e| e.to_string())?,
                ),
            }
        }
        UdfType::Interval => {
            let v = get::<IntervalMonthDayNanoArray>(array).value(i);
            // Materialize intervals have microsecond precision. Sub-microsecond
            // parts are truncated toward zero.
            Datum::Interval(Interval::new(v.months, v.days, v.nanoseconds / 1000))
        }
        UdfType::List(_) => {
            let elem = typ.element();
            let list = get::<ListArray>(array).value(i);
            let elems = (0..list.len())
                .map(|j| datum_at(list.as_ref(), j, &elem, arena, budget))
                .collect::<Result<Vec<_>, _>>()?;
            match &typ.sql {
                SqlScalarType::Array(_) => {
                    let dims = if elems.is_empty() {
                        vec![]
                    } else {
                        vec![ArrayDimension {
                            lower_bound: 1,
                            length: elems.len(),
                        }]
                    };
                    arena.make_datum(|packer| {
                        packer
                            .try_push_array(&dims, elems.iter())
                            .expect("one dimension matches the element count")
                    })
                }
                _ => arena.make_datum(|packer| packer.push_list(elems.iter())),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output(values: &[Option<i32>]) -> (ValueType, Vec<u8>) {
        let typ = ValueType::new(SqlScalarType::Int32).unwrap();
        let datums: Vec<[Datum; 1]> = values.iter().map(|v| [Datum::from(*v)]).collect();
        let rows: Vec<&[Datum]> = datums.iter().map(|r| r.as_slice()).collect();
        let bytes = encode(std::slice::from_ref(&typ), &rows).unwrap();
        (typ, bytes)
    }

    #[mz_ore::test]
    fn numeric_arguments_must_fit_the_guest_decimal() {
        let typ = ValueType::new(SqlScalarType::Numeric { max_scale: None }).unwrap();
        let check_str = |s: &str| check(&typ, Datum::Numeric(strconv::parse_numeric(s).unwrap()));
        for ok in [
            "0",
            "-1.5",
            "1234567890123456789012345678",
            "0.0000000000000000000000000001",
        ] {
            assert!(check_str(ok).is_ok(), "{ok}");
        }
        for bad in [
            "NaN",
            "12345678901234567890123456789",
            "1e30",
            "0.00000000000000000000000000001",
        ] {
            assert!(check_str(bad).is_err(), "{bad}");
        }
    }

    #[mz_ore::test]
    fn arrays_must_start_at_one() {
        let typ = ValueType::new(SqlScalarType::Array(Box::new(SqlScalarType::Int32))).unwrap();
        let arena = RowArena::new();
        let array = |lower_bound| {
            arena.make_datum(|packer| {
                packer
                    .try_push_array(
                        &[ArrayDimension {
                            lower_bound,
                            length: 1,
                        }],
                        [Datum::Int32(1)],
                    )
                    .unwrap()
            })
        };
        assert!(check(&typ, array(1)).is_ok());
        assert!(check(&typ, array(0)).is_err());
    }

    #[mz_ore::test]
    fn decoding_is_bounded_by_value_count() {
        let typ = ValueType::new(SqlScalarType::Array(Box::new(SqlScalarType::Bool))).unwrap();
        let arena = RowArena::new();
        let list = arena.make_datum(|packer| {
            packer
                .try_push_array(
                    &[ArrayDimension {
                        lower_bound: 1,
                        length: 100,
                    }],
                    std::iter::repeat_n(Datum::False, 100),
                )
                .unwrap()
        });
        let bytes = encode(std::slice::from_ref(&typ), &[&[list]]).unwrap();
        // One value for the list and one per element.
        assert!(decode(&bytes, &typ, 1, 101, &arena).is_ok());
        let err = decode(&bytes, &typ, 1, 100, &arena).unwrap_err();
        assert!(err.contains("more than 100 values"), "{err}");
    }

    #[mz_ore::test]
    fn decodes_well_formed_output() {
        let (typ, bytes) = output(&[Some(1), None, Some(3)]);
        let arena = RowArena::new();
        let out = decode(&bytes, &typ, 3, usize::MAX, &arena).unwrap();
        let values: Vec<_> = out
            .into_iter()
            .map(|r| match r {
                RowResult::Value(d) => d,
                r => panic!("unexpected {r:?}"),
            })
            .collect();
        assert_eq!(values, [Datum::Int32(1), Datum::Null, Datum::Int32(3)]);
    }

    #[mz_ore::test]
    fn rejects_out_of_bounds_footer() {
        let (typ, mut bytes) = output(&[Some(1)]);
        let at = bytes.len() - 10;
        bytes[at..at + 4].copy_from_slice(&i32::MAX.to_le_bytes());
        let arena = RowArena::new();
        let err = decode(&bytes, &typ, 1, usize::MAX, &arena).unwrap_err();
        assert_eq!(err, "output footer is out of bounds");
    }

    #[mz_ore::test]
    fn checks_row_count_before_decoding() {
        let (typ, bytes) = output(&[Some(1), Some(2)]);
        let arena = RowArena::new();
        let err = decode(&bytes, &typ, 3, usize::MAX, &arena).unwrap_err();
        assert_eq!(err, "output has 2 rows for 3 input rows");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn corrupt_output_never_panics() {
        let (typ, bytes) = output(&[Some(1), None, Some(3)]);
        let arena = RowArena::new();
        for at in 0..bytes.len() {
            for value in [0x00, 0x7f, 0x80, 0xff] {
                let mut corrupt = bytes.clone();
                corrupt[at] = value;
                let _ = decode(&corrupt, &typ, 3, usize::MAX, &arena);
            }
        }
    }
}
