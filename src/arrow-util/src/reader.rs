// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reader for [`arrow`] data that outputs [`Row`]s.

use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, NaiveTime};
use dec::OrderedDecimal;
use mz_ore::cast::CastFrom;
use mz_repr::adt::date::Date;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RelationDesc, Row, RowPacker, SharedRow, SqlScalarType};
use ordered_float::OrderedFloat;
use uuid::Uuid;

use crate::mask_nulls;

/// Type that can read out of an [`arrow::array::StructArray`] and into a [`Row`], given a
/// [`RelationDesc`].
///
/// The inverse of a [`crate::builder::ArrowBuilder`].
///
/// Note: When creating an [`ArrowReader`] we perform a "one-time downcast" of the children Arrays
/// from the [`StructArray`], into `enum ColReader`s. This is a much more verbose approach than the
/// alternative of downcasting from a `dyn arrow::array::Array` every time we read a [`Row`], but
/// it is _much_ more performant.
pub struct ArrowReader {
    len: usize,
    readers: Vec<ColReader>,
}

impl ArrowReader {
    /// Create an [`ArrowReader`] validating that the provided [`RelationDesc`] and [`StructArray`]
    /// have a matching schema.
    ///
    /// The [`RelationDesc`] and [`StructArray`] need to uphold the following to be a valid pair:
    ///
    /// * Same number of columns.
    /// * Columns of all the same name.
    /// * Columns of compatible types.
    ///
    /// TODO(cf2): Relax some of these restrictions by allowing users to map column names, omit
    /// columns, perform some lightweight casting, and matching not on column name but column
    /// position.
    /// TODO(cf2): Allow specifying an optional `arrow::Schema` for extra metadata.
    pub fn new(desc: &RelationDesc, array: StructArray) -> Result<Self, anyhow::Error> {
        let inner_columns = array.columns();
        let desc_columns = desc.typ().columns();

        if inner_columns.len() != desc_columns.len() {
            return Err(anyhow::anyhow!(
                "wrong number of columns {} vs {}",
                inner_columns.len(),
                desc_columns.len()
            ));
        }

        let mut readers = Vec::with_capacity(desc_columns.len());
        for (col_name, col_type) in desc.iter() {
            let column = array
                .column_by_name(col_name)
                .ok_or_else(|| anyhow::anyhow!("'{col_name}' not found"))?;
            let reader = scalar_type_and_array_to_reader(&col_type.scalar_type, Arc::clone(column))
                .context(col_name.clone())?;

            readers.push(reader);
        }

        Ok(ArrowReader {
            len: array.len(),
            readers,
        })
    }

    /// Read the value at `idx` into the provided `Row`.
    pub fn read(&self, idx: usize, row: &mut Row) -> Result<(), anyhow::Error> {
        let mut packer = row.packer();
        for reader in &self.readers {
            reader.read(idx, &mut packer).context(idx)?;
        }
        Ok(())
    }

    /// Read all of the values in this [`ArrowReader`] into `rows`.
    pub fn read_all(&self, rows: &mut Vec<Row>) -> Result<usize, anyhow::Error> {
        for idx in 0..self.len {
            let mut row = Row::default();
            self.read(idx, &mut row).context(idx)?;
            rows.push(row);
        }
        Ok(self.len)
    }
}

fn scalar_type_and_array_to_reader(
    scalar_type: &SqlScalarType,
    array: Arc<dyn Array>,
) -> Result<ColReader, anyhow::Error> {
    fn downcast_array<T: arrow::array::Array + Clone + 'static>(array: Arc<dyn Array>) -> T {
        array
            .as_any()
            .downcast_ref::<T>()
            .expect("checked DataType")
            .clone()
    }

    match (scalar_type, array.data_type()) {
        (SqlScalarType::Bool, DataType::Boolean) => {
            Ok(ColReader::Boolean(downcast_array::<BooleanArray>(array)))
        }
        (SqlScalarType::Int16 | SqlScalarType::Int32 | SqlScalarType::Int64, DataType::Int8) => {
            let array = downcast_array::<Int8Array>(array);
            let cast: fn(i8) -> Datum<'static> = match scalar_type {
                SqlScalarType::Int16 => |x| Datum::Int16(i16::cast_from(x)),
                SqlScalarType::Int32 => |x| Datum::Int32(i32::cast_from(x)),
                SqlScalarType::Int64 => |x| Datum::Int64(i64::cast_from(x)),
                _ => unreachable!("checked above"),
            };
            Ok(ColReader::Int8 { array, cast })
        }
        (SqlScalarType::Int16, DataType::Int16) => {
            Ok(ColReader::Int16(downcast_array::<Int16Array>(array)))
        }
        (SqlScalarType::Int32, DataType::Int32) => {
            Ok(ColReader::Int32(downcast_array::<Int32Array>(array)))
        }
        (SqlScalarType::Int64, DataType::Int64) => {
            Ok(ColReader::Int64(downcast_array::<Int64Array>(array)))
        }
        (
            SqlScalarType::UInt16 | SqlScalarType::UInt32 | SqlScalarType::UInt64,
            DataType::UInt8,
        ) => {
            let array = downcast_array::<UInt8Array>(array);
            let cast: fn(u8) -> Datum<'static> = match scalar_type {
                SqlScalarType::UInt16 => |x| Datum::UInt16(u16::cast_from(x)),
                SqlScalarType::UInt32 => |x| Datum::UInt32(u32::cast_from(x)),
                SqlScalarType::UInt64 => |x| Datum::UInt64(u64::cast_from(x)),
                _ => unreachable!("checked above"),
            };
            Ok(ColReader::UInt8 { array, cast })
        }
        (SqlScalarType::UInt16, DataType::UInt16) => {
            Ok(ColReader::UInt16(downcast_array::<UInt16Array>(array)))
        }
        (SqlScalarType::UInt32, DataType::UInt32) => {
            Ok(ColReader::UInt32(downcast_array::<UInt32Array>(array)))
        }
        (SqlScalarType::UInt64, DataType::UInt64) => {
            Ok(ColReader::UInt64(downcast_array::<UInt64Array>(array)))
        }
        (SqlScalarType::Float32 | SqlScalarType::Float64, DataType::Float16) => {
            let array = downcast_array::<Float16Array>(array);
            let cast: fn(half::f16) -> Datum<'static> = match scalar_type {
                SqlScalarType::Float32 => |x| Datum::Float32(OrderedFloat::from(x.to_f32())),
                SqlScalarType::Float64 => |x| Datum::Float64(OrderedFloat::from(x.to_f64())),
                _ => unreachable!("checked above"),
            };
            Ok(ColReader::Float16 { array, cast })
        }
        (SqlScalarType::Float32, DataType::Float32) => {
            Ok(ColReader::Float32(downcast_array::<Float32Array>(array)))
        }
        (SqlScalarType::Float64, DataType::Float64) => {
            Ok(ColReader::Float64(downcast_array::<Float64Array>(array)))
        }
        // TODO(cf3): Consider the max_scale for numeric.
        (SqlScalarType::Numeric { .. }, DataType::Decimal128(precision, scale)) => {
            use num_traits::Pow;

            let base = Numeric::from(10);
            let scale = Numeric::from(*scale);
            let scale_factor = base.pow(scale);

            let precision = usize::cast_from(*precision);
            // Don't use the context here, but make sure the precision is valid.
            let mut ctx = dec::Context::<Numeric>::default();
            ctx.set_precision(precision).map_err(|e| {
                anyhow::anyhow!("invalid precision from Decimal128, {precision}, {e}")
            })?;

            let array = downcast_array::<Decimal128Array>(array);

            Ok(ColReader::Decimal128 {
                array,
                scale_factor,
                precision,
            })
        }
        // TODO(cf3): Consider the max_scale for numeric.
        (SqlScalarType::Numeric { .. }, DataType::Decimal256(precision, scale)) => {
            use num_traits::Pow;

            let base = Numeric::from(10);
            let scale = Numeric::from(*scale);
            let scale_factor = base.pow(scale);

            let precision = usize::cast_from(*precision);
            // Don't use the context here, but make sure the precision is valid.
            let mut ctx = dec::Context::<Numeric>::default();
            ctx.set_precision(precision).map_err(|e| {
                anyhow::anyhow!("invalid precision from Decimal256, {precision}, {e}")
            })?;

            let array = downcast_array::<Decimal256Array>(array);

            Ok(ColReader::Decimal256 {
                array,
                scale_factor,
                precision,
            })
        }
        (SqlScalarType::Bytes, DataType::Binary) => {
            Ok(ColReader::Binary(downcast_array::<BinaryArray>(array)))
        }
        (SqlScalarType::Bytes, DataType::LargeBinary) => {
            let array = downcast_array::<LargeBinaryArray>(array);
            Ok(ColReader::LargeBinary(array))
        }
        (SqlScalarType::Bytes, DataType::FixedSizeBinary(_)) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array);
            Ok(ColReader::FixedSizeBinary(array))
        }
        (SqlScalarType::Bytes, DataType::BinaryView) => {
            let array = downcast_array::<BinaryViewArray>(array);
            Ok(ColReader::BinaryView(array))
        }
        (
            SqlScalarType::Uuid,
            DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_),
        ) => {
            let reader = scalar_type_and_array_to_reader(&SqlScalarType::Bytes, array)
                .context("uuid reader")?;
            Ok(ColReader::Uuid(Box::new(reader)))
        }
        (SqlScalarType::String, DataType::Utf8) => {
            Ok(ColReader::String(downcast_array::<StringArray>(array)))
        }
        (SqlScalarType::String, DataType::LargeUtf8) => {
            let array = downcast_array::<LargeStringArray>(array);
            Ok(ColReader::LargeString(array))
        }
        (SqlScalarType::String, DataType::Utf8View) => {
            let array = downcast_array::<StringViewArray>(array);
            Ok(ColReader::StringView(array))
        }
        (SqlScalarType::Jsonb, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            let reader = scalar_type_and_array_to_reader(&SqlScalarType::String, array)
                .context("json reader")?;
            Ok(ColReader::Jsonb(Box::new(reader)))
        }
        (SqlScalarType::Timestamp { .. }, DataType::Timestamp(TimeUnit::Second, None)) => {
            let array = downcast_array::<TimestampSecondArray>(array);
            Ok(ColReader::TimestampSecond(array))
        }
        (SqlScalarType::Timestamp { .. }, DataType::Timestamp(TimeUnit::Millisecond, None)) => {
            let array = downcast_array::<TimestampMillisecondArray>(array);
            Ok(ColReader::TimestampMillisecond(array))
        }
        (SqlScalarType::Timestamp { .. }, DataType::Timestamp(TimeUnit::Microsecond, None)) => {
            let array = downcast_array::<TimestampMicrosecondArray>(array);
            Ok(ColReader::TimestampMicrosecond(array))
        }
        (SqlScalarType::Timestamp { .. }, DataType::Timestamp(TimeUnit::Nanosecond, None)) => {
            let array = downcast_array::<TimestampNanosecondArray>(array);
            Ok(ColReader::TimestampNanosecond(array))
        }
        (SqlScalarType::Date, DataType::Date32) => {
            let array = downcast_array::<Date32Array>(array);
            Ok(ColReader::Date32(array))
        }
        (SqlScalarType::Date, DataType::Date64) => {
            let array = downcast_array::<Date64Array>(array);
            Ok(ColReader::Date64(array))
        }
        (SqlScalarType::Time, DataType::Time32(TimeUnit::Second)) => {
            let array = downcast_array::<Time32SecondArray>(array);
            Ok(ColReader::Time32Seconds(array))
        }
        (SqlScalarType::Time, DataType::Time32(TimeUnit::Millisecond)) => {
            let array = downcast_array::<Time32MillisecondArray>(array);
            Ok(ColReader::Time32Milliseconds(array))
        }
        (
            SqlScalarType::List {
                element_type,
                custom_id: _,
            },
            DataType::List(_),
        ) => {
            let array = downcast_array::<ListArray>(array);
            let inner_decoder =
                scalar_type_and_array_to_reader(element_type, Arc::clone(array.values()))
                    .context("list")?;
            Ok(ColReader::List {
                offsets: array.offsets().clone(),
                values: Box::new(inner_decoder),
                nulls: array.nulls().cloned(),
            })
        }
        (
            SqlScalarType::List {
                element_type,
                custom_id: _,
            },
            DataType::LargeList(_),
        ) => {
            let array = downcast_array::<LargeListArray>(array);
            let inner_decoder =
                scalar_type_and_array_to_reader(element_type, Arc::clone(array.values()))
                    .context("large list")?;
            Ok(ColReader::LargeList {
                offsets: array.offsets().clone(),
                values: Box::new(inner_decoder),
                nulls: array.nulls().cloned(),
            })
        }
        (
            SqlScalarType::Record {
                fields,
                custom_id: _,
            },
            DataType::Struct(_),
        ) => {
            let record_array = downcast_array::<StructArray>(array);
            let null_mask = record_array.nulls();

            let mut decoders = Vec::with_capacity(fields.len());
            for (name, typ) in fields.iter() {
                let inner_array = record_array
                    .column_by_name(name)
                    .ok_or_else(|| anyhow::anyhow!("missing name '{name}'"))?;
                let inner_array = mask_nulls(inner_array, null_mask);
                let inner_decoder = scalar_type_and_array_to_reader(&typ.scalar_type, inner_array)
                    .context(name.clone())?;

                decoders.push(Box::new(inner_decoder));
            }

            Ok(ColReader::Record {
                fields: decoders,
                nulls: null_mask.cloned(),
            })
        }
        other => anyhow::bail!("unsupported: {other:?}"),
    }
}

/// A "downcasted" version of [`arrow::array::Array`] that supports reading [`Datum`]s.
///
/// Note: While this is fairly verbose, one-time "downcasting" to an enum is _much_ more performant
/// than downcasting every time we read a [`Datum`].
enum ColReader {
    Boolean(arrow::array::BooleanArray),

    Int8 {
        array: arrow::array::Int8Array,
        cast: fn(i8) -> Datum<'static>,
    },
    Int16(arrow::array::Int16Array),
    Int32(arrow::array::Int32Array),
    Int64(arrow::array::Int64Array),

    UInt8 {
        array: arrow::array::UInt8Array,
        cast: fn(u8) -> Datum<'static>,
    },
    UInt16(arrow::array::UInt16Array),
    UInt32(arrow::array::UInt32Array),
    UInt64(arrow::array::UInt64Array),

    Float16 {
        array: arrow::array::Float16Array,
        cast: fn(half::f16) -> Datum<'static>,
    },
    Float32(arrow::array::Float32Array),
    Float64(arrow::array::Float64Array),

    Decimal128 {
        array: Decimal128Array,
        scale_factor: Numeric,
        precision: usize,
    },
    Decimal256 {
        array: Decimal256Array,
        scale_factor: Numeric,
        precision: usize,
    },

    Binary(arrow::array::BinaryArray),
    LargeBinary(arrow::array::LargeBinaryArray),
    FixedSizeBinary(arrow::array::FixedSizeBinaryArray),
    BinaryView(arrow::array::BinaryViewArray),
    Uuid(Box<ColReader>),

    String(arrow::array::StringArray),
    LargeString(arrow::array::LargeStringArray),
    StringView(arrow::array::StringViewArray),
    Jsonb(Box<ColReader>),

    TimestampSecond(arrow::array::TimestampSecondArray),
    TimestampMillisecond(arrow::array::TimestampMillisecondArray),
    TimestampMicrosecond(arrow::array::TimestampMicrosecondArray),
    TimestampNanosecond(arrow::array::TimestampNanosecondArray),

    Date32(Date32Array),
    Date64(Date64Array),

    Time32Seconds(Time32SecondArray),
    Time32Milliseconds(arrow::array::Time32MillisecondArray),

    List {
        offsets: OffsetBuffer<i32>,
        values: Box<ColReader>,
        nulls: Option<NullBuffer>,
    },
    LargeList {
        offsets: OffsetBuffer<i64>,
        values: Box<ColReader>,
        nulls: Option<NullBuffer>,
    },

    Record {
        fields: Vec<Box<ColReader>>,
        nulls: Option<NullBuffer>,
    },
}

impl ColReader {
    fn read(&self, idx: usize, packer: &mut RowPacker) -> Result<(), anyhow::Error> {
        let datum = match self {
            ColReader::Boolean(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|x| if x { Datum::True } else { Datum::False }),
            ColReader::Int8 { array, cast } => {
                array.is_valid(idx).then(|| array.value(idx)).map(cast)
            }
            ColReader::Int16(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Int16),
            ColReader::Int32(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Int32),
            ColReader::Int64(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Int64),
            ColReader::UInt8 { array, cast } => {
                array.is_valid(idx).then(|| array.value(idx)).map(cast)
            }
            ColReader::UInt16(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::UInt16),
            ColReader::UInt32(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::UInt32),
            ColReader::UInt64(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::UInt64),
            ColReader::Float16 { array, cast } => {
                array.is_valid(idx).then(|| array.value(idx)).map(cast)
            }
            ColReader::Float32(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|x| Datum::Float32(OrderedFloat(x))),
            ColReader::Float64(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|x| Datum::Float64(OrderedFloat(x))),
            ColReader::Decimal128 {
                array,
                scale_factor,
                precision,
            } => array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                // Create a Numeric from our i128 with precision.
                let mut ctx = dec::Context::<Numeric>::default();
                ctx.set_precision(*precision).expect("checked before");
                let mut num = ctx.from_i128(x);

                // Scale the number.
                ctx.div(&mut num, scale_factor);

                Datum::Numeric(OrderedDecimal(num))
            }),
            ColReader::Decimal256 {
                array,
                scale_factor,
                precision,
            } => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|x| {
                    let s = x.to_string();

                    // Parse a i256 from it's String representation.
                    //
                    // TODO(cf3): See if we can add support for 256-bit numbers to the `dec` crate.
                    let mut ctx = dec::Context::<Numeric>::default();
                    ctx.set_precision(*precision).expect("checked before");
                    let mut num = ctx
                        .parse(s)
                        .map_err(|e| anyhow::anyhow!("decimal out of range: {e}"))?;

                    // Scale the number.
                    ctx.div(&mut num, scale_factor);

                    Ok::<_, anyhow::Error>(Datum::Numeric(OrderedDecimal(num)))
                })
                .transpose()?,
            ColReader::Binary(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Bytes),
            ColReader::LargeBinary(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Bytes),
            ColReader::FixedSizeBinary(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Bytes),
            ColReader::BinaryView(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Bytes),
            ColReader::Uuid(reader) => {
                // First read a binary value into a temp row, and later parse that as UUID into our
                // actual Row Packer.
                let mut temp_row = SharedRow::get();
                reader.read(idx, &mut temp_row.packer()).context("uuid")?;
                let slice = match temp_row.unpack_first() {
                    Datum::Bytes(slice) => slice,
                    Datum::Null => {
                        packer.push(Datum::Null);
                        return Ok(());
                    }
                    other => anyhow::bail!("expected String, found {other:?}"),
                };

                let uuid = Uuid::from_slice(slice).context("parsing uuid")?;
                Some(Datum::Uuid(uuid))
            }
            ColReader::String(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::String),
            ColReader::LargeString(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::String),
            ColReader::StringView(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::String),
            ColReader::Jsonb(reader) => {
                // First read a string value into a temp row, and later parse that as JSON into our
                // actual Row Packer.
                let mut temp_row = SharedRow::get();
                reader.read(idx, &mut temp_row.packer()).context("jsonb")?;
                let value = match temp_row.unpack_first() {
                    Datum::String(value) => value,
                    Datum::Null => {
                        packer.push(Datum::Null);
                        return Ok(());
                    }
                    other => anyhow::bail!("expected String, found {other:?}"),
                };

                JsonbPacker::new(packer)
                    .pack_str(value)
                    .context("roundtrip json")?;

                // Return early because we've already packed the necessasry Datums.
                return Ok(());
            }
            ColReader::TimestampSecond(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|secs| {
                    let dt = DateTime::from_timestamp(secs, 0)
                        .ok_or_else(|| anyhow::anyhow!("invalid timestamp seconds {secs}"))?;
                    let dt = CheckedTimestamp::from_timestamplike(dt.naive_utc())
                        .context("TimestampSeconds")?;
                    Ok::<_, anyhow::Error>(Datum::Timestamp(dt))
                })
                .transpose()?,
            ColReader::TimestampMillisecond(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|millis| {
                    let dt = DateTime::from_timestamp_millis(millis).ok_or_else(|| {
                        anyhow::anyhow!("invalid timestamp milliseconds {millis}")
                    })?;
                    let dt = CheckedTimestamp::from_timestamplike(dt.naive_utc())
                        .context("TimestampMillis")?;
                    Ok::<_, anyhow::Error>(Datum::Timestamp(dt))
                })
                .transpose()?,
            ColReader::TimestampMicrosecond(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|micros| {
                    let dt = DateTime::from_timestamp_micros(micros).ok_or_else(|| {
                        anyhow::anyhow!("invalid timestamp microseconds {micros}")
                    })?;
                    let dt = CheckedTimestamp::from_timestamplike(dt.naive_utc())
                        .context("TimestampMicros")?;
                    Ok::<_, anyhow::Error>(Datum::Timestamp(dt))
                })
                .transpose()?,
            ColReader::TimestampNanosecond(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|nanos| {
                    let dt = DateTime::from_timestamp_nanos(nanos);
                    let dt = CheckedTimestamp::from_timestamplike(dt.naive_utc())
                        .context("TimestampNanos")?;
                    Ok::<_, anyhow::Error>(Datum::Timestamp(dt))
                })
                .transpose()?,
            ColReader::Date32(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|unix_days| {
                    let date = Date::from_unix_epoch(unix_days).context("date32")?;
                    Ok::<_, anyhow::Error>(Datum::Date(date))
                })
                .transpose()?,
            ColReader::Date64(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|unix_millis| {
                    let date = DateTime::from_timestamp_millis(unix_millis)
                        .ok_or_else(|| anyhow::anyhow!("invalid Date64 {unix_millis}"))?;
                    let unix_epoch = DateTime::from_timestamp(0, 0)
                        .expect("UNIX epoch")
                        .date_naive();
                    let delta = date.date_naive().signed_duration_since(unix_epoch);
                    let days: i32 = delta.num_days().try_into().context("date64")?;
                    let date = Date::from_unix_epoch(days).context("date64")?;
                    Ok::<_, anyhow::Error>(Datum::Date(date))
                })
                .transpose()?,
            ColReader::Time32Seconds(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|secs| {
                    let usecs: u32 = secs.try_into().context("time32 seconds")?;
                    let time = NaiveTime::from_num_seconds_from_midnight_opt(usecs, 0)
                        .ok_or_else(|| anyhow::anyhow!("invalid Time32 Seconds {secs}"))?;
                    Ok::<_, anyhow::Error>(Datum::Time(time))
                })
                .transpose()?,
            ColReader::Time32Milliseconds(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|millis| {
                    let umillis: u32 = millis.try_into().context("time32 milliseconds")?;
                    let usecs = umillis / 1000;
                    let unanos = (umillis % 1000).saturating_mul(1_000_000);
                    let time = NaiveTime::from_num_seconds_from_midnight_opt(usecs, unanos)
                        .ok_or_else(|| anyhow::anyhow!("invalid Time32 Milliseconds {umillis}"))?;
                    Ok::<_, anyhow::Error>(Datum::Time(time))
                })
                .transpose()?,
            ColReader::List {
                offsets,
                values,
                nulls,
            } => {
                let is_valid = nulls.as_ref().map(|n| n.is_valid(idx)).unwrap_or(true);
                if !is_valid {
                    packer.push(Datum::Null);
                    return Ok(());
                }

                let start: usize = offsets[idx].try_into().context("list start offset")?;
                let end: usize = offsets[idx + 1].try_into().context("list end offset")?;

                packer
                    .push_list_with(|packer| {
                        for idx in start..end {
                            values.read(idx, packer)?;
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .context("pack list")?;

                // Return early because we've already packed the necessasry Datums.
                return Ok(());
            }
            ColReader::LargeList {
                offsets,
                values,
                nulls,
            } => {
                let is_valid = nulls.as_ref().map(|n| n.is_valid(idx)).unwrap_or(true);
                if !is_valid {
                    packer.push(Datum::Null);
                    return Ok(());
                }

                let start: usize = offsets[idx].try_into().context("list start offset")?;
                let end: usize = offsets[idx + 1].try_into().context("list end offset")?;

                packer
                    .push_list_with(|packer| {
                        for idx in start..end {
                            values.read(idx, packer)?;
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .context("pack list")?;

                // Return early because we've already packed the necessasry Datums.
                return Ok(());
            }
            ColReader::Record { fields, nulls } => {
                let is_valid = nulls.as_ref().map(|n| n.is_valid(idx)).unwrap_or(true);
                if !is_valid {
                    packer.push(Datum::Null);
                    return Ok(());
                }

                packer
                    .push_list_with(|packer| {
                        for field in fields {
                            field.read(idx, packer)?;
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .context("pack record")?;

                // Return early because we've already packed the necessasry Datums.
                return Ok(());
            }
        };

        match datum {
            Some(d) => packer.push(d),
            None => packer.push(Datum::Null),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;
    use mz_ore::collections::CollectionExt;

    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn smoketest_reader() {
        let desc = RelationDesc::builder()
            .with_column("bool", SqlScalarType::Bool.nullable(true))
            .with_column("int4", SqlScalarType::Int32.nullable(true))
            .with_column("uint8", SqlScalarType::UInt64.nullable(true))
            .with_column("float32", SqlScalarType::Float32.nullable(true))
            .with_column("string", SqlScalarType::String.nullable(true))
            .with_column("bytes", SqlScalarType::Bytes.nullable(true))
            .with_column("uuid", SqlScalarType::Uuid.nullable(true))
            .with_column("json", SqlScalarType::Jsonb.nullable(true))
            .with_column(
                "list",
                SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::UInt32),
                    custom_id: None,
                }
                .nullable(true),
            )
            .finish();

        let mut og_row = Row::default();
        let mut packer = og_row.packer();

        packer.extend([
            Datum::True,
            Datum::Int32(42),
            Datum::UInt64(10000),
            Datum::Float32(OrderedFloat::from(-1.1f32)),
            Datum::String("hello world"),
            Datum::Bytes(b"1010101"),
            Datum::Uuid(uuid::Uuid::new_v4()),
        ]);
        JsonbPacker::new(&mut packer)
            .pack_serde_json(
                serde_json::json!({"code": 200, "email": "space_monkey@materialize.com"}),
            )
            .expect("failed to pack JSON");
        packer.push_list([Datum::UInt32(200), Datum::UInt32(300)]);

        let null_row = Row::pack(vec![Datum::Null; 9]);

        // Encode our data with our ArrowBuilder.
        let mut builder = crate::builder::ArrowBuilder::new(&desc, 2, 46).unwrap();
        builder.add_row(&og_row).unwrap();
        builder.add_row(&null_row).unwrap();
        let record_batch = builder.to_record_batch().unwrap();

        // Decode our data!
        let reader =
            ArrowReader::new(&desc, arrow::array::StructArray::from(record_batch)).unwrap();
        let mut rnd_row = Row::default();

        reader.read(0, &mut rnd_row).unwrap();
        assert_eq!(&og_row, &rnd_row);

        // Create a packer to clear the row alloc.
        rnd_row.packer();

        reader.read(1, &mut rnd_row).unwrap();
        assert_eq!(&null_row, &rnd_row);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn smoketest_decimal128() {
        let desc = RelationDesc::builder()
            .with_column(
                "a",
                SqlScalarType::Numeric { max_scale: None }.nullable(true),
            )
            .finish();

        let mut dec128 = arrow::array::Decimal128Builder::new();
        dec128 = dec128.with_precision_and_scale(12, 3).unwrap();

        // 1.234
        dec128.append_value(1234);
        dec128.append_null();
        // 100000000.009
        dec128.append_value(100000000009);

        let dec128 = dec128.finish();
        #[allow(clippy::as_conversions)]
        let batch = StructArray::from(vec![(
            Arc::new(Field::new("a", dec128.data_type().clone(), true)),
            Arc::new(dec128) as arrow::array::ArrayRef,
        )]);

        // Decode our data!
        let reader = ArrowReader::new(&desc, batch).unwrap();
        let mut rnd_row = Row::default();

        reader.read(0, &mut rnd_row).unwrap();
        let num = rnd_row.into_element().unwrap_numeric();
        assert_eq!(num.0, Numeric::from(1.234f64));

        // Create a packer to clear the row alloc.
        rnd_row.packer();

        reader.read(1, &mut rnd_row).unwrap();
        let num = rnd_row.into_element();
        assert_eq!(num, Datum::Null);

        // Create a packer to clear the row alloc.
        rnd_row.packer();

        reader.read(2, &mut rnd_row).unwrap();
        let num = rnd_row.into_element().unwrap_numeric();
        assert_eq!(num.0, Numeric::from(100000000.009f64));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn smoketest_decimal256() {
        let desc = RelationDesc::builder()
            .with_column(
                "a",
                SqlScalarType::Numeric { max_scale: None }.nullable(true),
            )
            .finish();

        let mut dec256 = arrow::array::Decimal256Builder::new();
        dec256 = dec256.with_precision_and_scale(12, 3).unwrap();

        // 1.234
        dec256.append_value(arrow::datatypes::i256::from(1234));
        dec256.append_null();
        // 100000000.009
        dec256.append_value(arrow::datatypes::i256::from(100000000009i64));

        let dec256 = dec256.finish();
        #[allow(clippy::as_conversions)]
        let batch = StructArray::from(vec![(
            Arc::new(Field::new("a", dec256.data_type().clone(), true)),
            Arc::new(dec256) as arrow::array::ArrayRef,
        )]);

        // Decode our data!
        let reader = ArrowReader::new(&desc, batch).unwrap();
        let mut rnd_row = Row::default();

        reader.read(0, &mut rnd_row).unwrap();
        let num = rnd_row.into_element().unwrap_numeric();
        assert_eq!(num.0, Numeric::from(1.234f64));

        // Create a packer to clear the row alloc.
        rnd_row.packer();

        reader.read(1, &mut rnd_row).unwrap();
        let num = rnd_row.into_element();
        assert_eq!(num, Datum::Null);

        // Create a packer to clear the row alloc.
        rnd_row.packer();

        reader.read(2, &mut rnd_row).unwrap();
        let num = rnd_row.into_element().unwrap_numeric();
        assert_eq!(num.0, Numeric::from(100000000.009f64));
    }
}
