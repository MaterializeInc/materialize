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

use std::io::Cursor;
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
use arrow::datatypes::{DataType, Field, IntervalMonthDayNano, IntervalUnit, Schema, TimeUnit};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
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
        (UdfType::List(_), Datum::Array(array)) => {
            if array.dims().ndims() > 1 {
                return Err("multidimensional arrays are not supported".into());
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

/// Decodes a guest's output batch for `rows` input rows.
///
/// Errors describe a batch that does not follow the ABI, which the caller
/// treats as a failure of the whole call.
pub fn decode<'a>(
    bytes: &[u8],
    ret: &ValueType,
    rows: usize,
    arena: &'a RowArena,
) -> Result<Vec<RowResult<'a>>, String> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| format!("invalid output: {e}"))?;
    let batch = reader
        .next()
        .ok_or_else(|| "output contains no record batch".to_string())?
        .map_err(|e| format!("invalid output: {e}"))?;
    if batch.num_rows() != rows {
        return Err(format!(
            "output has {} rows for {rows} input rows",
            batch.num_rows()
        ));
    }
    if batch.num_columns() == 0 {
        return Err("output has no columns".into());
    }
    let values = batch.column(0);
    if !types_match(values.data_type(), &data_type(&ret.udf)) {
        return Err(format!(
            "output column has type {}, expected {}",
            values.data_type(),
            data_type(&ret.udf)
        ));
    }
    let errors = match batch.schema().index_of(ERROR_COLUMN) {
        Ok(i) => Some(
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "error column is not Utf8".to_string())?
                .clone(),
        ),
        Err(_) => None,
    };

    Ok((0..rows)
        .map(|i| {
            if let Some(errors) = &errors {
                if errors.is_valid(i) {
                    return RowResult::GuestError(errors.value(i).to_string());
                }
            }
            match datum_at(values.as_ref(), i, ret, arena) {
                Ok(d) => RowResult::Value(d),
                Err(e) => RowResult::Invalid(e),
            }
        })
        .collect())
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

fn datum_at<'a>(
    array: &dyn Array,
    i: usize,
    typ: &ValueType,
    arena: &'a RowArena,
) -> Result<Datum<'a>, String> {
    if array.is_null(i) {
        return Ok(Datum::Null);
    }
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
            let n = strconv::parse_numeric(s).map_err(|e| format!("invalid decimal {s:?}: {e}"))?;
            Datum::Numeric(n)
        }
        UdfType::Json => {
            let s = get::<StringArray>(array).value(i);
            let jsonb = strconv::parse_jsonb(s).map_err(|e| format!("invalid JSON {s:?}: {e}"))?;
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
                .map(|j| datum_at(list.as_ref(), j, &elem, arena))
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
