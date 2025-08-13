// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A permanent storage encoding for rows.
//!
//! See row.proto for details.

use std::fmt::Debug;
use std::ops::AddAssign;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBufferBuilder,
    BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Array, Float32Builder,
    Float64Array, Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array,
    Int64Builder, ListArray, ListBuilder, MapArray, StringArray, StringBuilder, StructArray,
    UInt8Array, UInt8Builder, UInt16Array, UInt16Builder, UInt32Array, UInt32Builder, UInt64Array,
    UInt64Builder, make_array,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, ToByteSlice};
use bytes::{BufMut, Bytes};
use chrono::Timelike;
use dec::{Context, Decimal, OrderedDecimal};
use itertools::{EitherOrBoth, Itertools};
use mz_ore::assert_none;
use mz_ore::cast::CastFrom;
use mz_persist_types::Codec;
use mz_persist_types::arrow::ArrayOrd;
use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, FixedSizeCodec, Schema};
use mz_persist_types::stats::{
    ColumnNullStats, ColumnStatKinds, ColumnarStats, ColumnarStatsBuilder, FixedSizeBytesStatsKind,
    OptionStats, PrimitiveStats, StructStats,
};
use mz_proto::chrono::ProtoNaiveTime;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use prost::Message;
use timely::Container;
use uuid::Uuid;

use crate::adt::array::{ArrayDimension, PackedArrayDimension};
use crate::adt::date::Date;
use crate::adt::datetime::PackedNaiveTime;
use crate::adt::interval::PackedInterval;
use crate::adt::jsonb::{JsonbPacker, JsonbRef};
use crate::adt::mz_acl_item::{PackedAclItem, PackedMzAclItem};
use crate::adt::numeric::{Numeric, PackedNumeric};
use crate::adt::range::{Range, RangeInner, RangeLowerBound, RangeUpperBound};
use crate::adt::timestamp::{CheckedTimestamp, PackedNaiveDateTime};
use crate::row::proto_datum::DatumType;
use crate::row::{
    ProtoArray, ProtoArrayDimension, ProtoDatum, ProtoDatumOther, ProtoDict, ProtoDictElement,
    ProtoNumeric, ProtoRange, ProtoRangeInner, ProtoRow,
};
use crate::stats::{fixed_stats_from_column, numeric_stats_from_column, stats_for_json};
use crate::{Datum, ProtoRelationDesc, RelationDesc, Row, RowPacker, SqlScalarType, Timestamp};

// TODO(parkmycar): Benchmark the difference between `FixedSizeBinaryArray` and `BinaryArray`.
//
// `FixedSizeBinaryArray`s push empty bytes when a value is null which for larger binary types
// could result in poor performance.
#[allow(clippy::as_conversions)]
mod fixed_binary_sizes {
    use super::*;

    pub const TIME_FIXED_BYTES: i32 = PackedNaiveTime::SIZE as i32;
    pub const TIMESTAMP_FIXED_BYTES: i32 = PackedNaiveDateTime::SIZE as i32;
    pub const INTERVAL_FIXED_BYTES: i32 = PackedInterval::SIZE as i32;
    pub const ACL_ITEM_FIXED_BYTES: i32 = PackedAclItem::SIZE as i32;
    pub const _MZ_ACL_ITEM_FIXED_BYTES: i32 = PackedMzAclItem::SIZE as i32;
    pub const ARRAY_DIMENSION_FIXED_BYTES: i32 = PackedArrayDimension::SIZE as i32;

    pub const UUID_FIXED_BYTES: i32 = 16;
    static_assertions::const_assert_eq!(UUID_FIXED_BYTES as usize, std::mem::size_of::<Uuid>());
}
use fixed_binary_sizes::*;

/// Returns true iff the ordering of the "raw" and Persist-encoded versions of this columm would match:
/// ie. `sort(encode(column)) == encode(sort(column))`. This encoding has been designed so that this
/// is true for many types.
pub fn preserves_order(scalar_type: &SqlScalarType) -> bool {
    match scalar_type {
        // These types have short, fixed-length encodings that are designed to sort identically.
        SqlScalarType::Bool
        | SqlScalarType::Int16
        | SqlScalarType::Int32
        | SqlScalarType::Int64
        | SqlScalarType::UInt16
        | SqlScalarType::UInt32
        | SqlScalarType::UInt64
        | SqlScalarType::Date
        | SqlScalarType::Time
        | SqlScalarType::Timestamp { .. }
        | SqlScalarType::TimestampTz { .. }
        | SqlScalarType::Interval
        | SqlScalarType::Bytes
        | SqlScalarType::String
        | SqlScalarType::Uuid
        | SqlScalarType::MzTimestamp
        | SqlScalarType::MzAclItem
        | SqlScalarType::AclItem => true,
        // We sort records lexicographically; a record has a meaningful sort if all its fields do.
        SqlScalarType::Record { fields, .. } => fields
            .iter()
            .all(|(_, field_type)| preserves_order(&field_type.scalar_type)),
        // Our floating-point encoding preserves order generally, but differs when comparing
        // -0 and 0. Opt these out for now.
        SqlScalarType::Float32 | SqlScalarType::Float64 => false,
        // Numeric is sensitive to similar ordering issues as floating point numbers, and requires
        // some special handling we don't have yet.
        SqlScalarType::Numeric { .. } => false,
        // For all other types: either the encoding is known to not preserve ordering, or we
        // don't yet care to make strong guarantees one way or the other.
        SqlScalarType::PgLegacyChar
        | SqlScalarType::PgLegacyName
        | SqlScalarType::Char { .. }
        | SqlScalarType::VarChar { .. }
        | SqlScalarType::Jsonb
        | SqlScalarType::Array(_)
        | SqlScalarType::List { .. }
        | SqlScalarType::Oid
        | SqlScalarType::Map { .. }
        | SqlScalarType::RegProc
        | SqlScalarType::RegType
        | SqlScalarType::RegClass
        | SqlScalarType::Int2Vector
        | SqlScalarType::Range { .. } => false,
    }
}

/// An encoder for a column of [`Datum`]s.
#[derive(Debug)]
struct DatumEncoder {
    nullable: bool,
    encoder: DatumColumnEncoder,
}

impl DatumEncoder {
    fn goodbytes(&self) -> usize {
        self.encoder.goodbytes()
    }

    fn push(&mut self, datum: Datum) {
        assert!(
            !datum.is_null() || self.nullable,
            "tried pushing Null into non-nullable column"
        );
        self.encoder.push(datum);
    }

    fn push_invalid(&mut self) {
        self.encoder.push_invalid();
    }

    fn finish(self) -> ArrayRef {
        self.encoder.finish()
    }
}

/// An encoder for a single column of [`Datum`]s. To encode an entire row see
/// [`RowColumnarEncoder`].
///
/// Note: We specifically structure the encoder as an enum instead of using trait objects because
/// Datum encoding is an extremely hot path and downcasting objects is relatively slow.
#[derive(Debug)]
enum DatumColumnEncoder {
    Bool(BooleanBuilder),
    U8(UInt8Builder),
    U16(UInt16Builder),
    U32(UInt32Builder),
    U64(UInt64Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Numeric {
        /// The raw bytes so we can losslessly roundtrip Numerics.
        binary_values: BinaryBuilder,
        /// Also maintain a float64 approximation for sorting.
        approx_values: Float64Builder,
        /// Re-usable `libdecimal` context for conversions.
        numeric_context: Context<Numeric>,
    },
    Bytes(BinaryBuilder),
    String(StringBuilder),
    Date(Int32Builder),
    Time(FixedSizeBinaryBuilder),
    Timestamp(FixedSizeBinaryBuilder),
    TimestampTz(FixedSizeBinaryBuilder),
    MzTimestamp(UInt64Builder),
    Interval(FixedSizeBinaryBuilder),
    Uuid(FixedSizeBinaryBuilder),
    AclItem(FixedSizeBinaryBuilder),
    MzAclItem(BinaryBuilder),
    Range(BinaryBuilder),
    /// Hand rolled "StringBuilder" that reduces the number of copies required
    /// to serialize JSON.
    ///
    /// An alternative would be to use [`StringBuilder`] but that requires
    /// serializing to an intermediary string, and then copying that
    /// intermediary string into an underlying buffer.
    Jsonb {
        /// Monotonically increasing offsets of each encoded segment.
        offsets: Vec<i32>,
        /// Buffer that contains UTF-8 encoded JSON.
        buf: Vec<u8>,
        /// Null entries, if any.
        nulls: Option<BooleanBufferBuilder>,
    },
    Array {
        /// Binary encoded `ArrayDimension`s.
        dims: ListBuilder<FixedSizeBinaryBuilder>,
        /// Lengths of each `Array` in this column.
        val_lengths: Vec<usize>,
        /// Contiguous array of underlying data.
        vals: Box<DatumColumnEncoder>,
        /// Null entires, if any.
        nulls: Option<BooleanBufferBuilder>,
    },
    List {
        /// Lengths of each `List` in this column.
        lengths: Vec<usize>,
        /// Contiguous array of underlying data.
        values: Box<DatumColumnEncoder>,
        /// Null entires, if any.
        nulls: Option<BooleanBufferBuilder>,
    },
    Map {
        /// Lengths of each `Map` in this column
        lengths: Vec<usize>,
        /// Contiguous array of key data.
        keys: StringBuilder,
        /// Contiguous array of val data.
        vals: Box<DatumColumnEncoder>,
        /// Null entires, if any.
        nulls: Option<BooleanBufferBuilder>,
    },
    Record {
        /// Columns in the record.
        fields: Vec<DatumEncoder>,
        /// Null entries, if any.
        nulls: Option<BooleanBufferBuilder>,
        /// Number of values we've pushed into this builder thus far.
        length: usize,
    },
    /// Special encoder for a [`ScalarType::Record`] that has no inner fields.
    ///
    /// We have a special case for this scenario because Arrow does not allow a
    /// [`StructArray`] (what normally use to encod a `Record`) with no fields.
    RecordEmpty(BooleanBuilder),
}

impl DatumColumnEncoder {
    fn goodbytes(&self) -> usize {
        match self {
            DatumColumnEncoder::Bool(a) => a.len(),
            DatumColumnEncoder::U8(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::U16(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::U32(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::U64(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::I16(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::I32(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::I64(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::F32(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::F64(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::Numeric {
                binary_values,
                approx_values,
                ..
            } => {
                binary_values.values_slice().len()
                    + approx_values.values_slice().to_byte_slice().len()
            }
            DatumColumnEncoder::Bytes(a) => a.values_slice().len(),
            DatumColumnEncoder::String(a) => a.values_slice().len(),
            DatumColumnEncoder::Date(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::Time(a) => a.len() * PackedNaiveTime::SIZE,
            DatumColumnEncoder::Timestamp(a) => a.len() * PackedNaiveDateTime::SIZE,
            DatumColumnEncoder::TimestampTz(a) => a.len() * PackedNaiveDateTime::SIZE,
            DatumColumnEncoder::MzTimestamp(a) => a.values_slice().to_byte_slice().len(),
            DatumColumnEncoder::Interval(a) => a.len() * PackedInterval::SIZE,
            DatumColumnEncoder::Uuid(a) => a.len() * size_of::<Uuid>(),
            DatumColumnEncoder::AclItem(a) => a.len() * PackedAclItem::SIZE,
            DatumColumnEncoder::MzAclItem(a) => a.values_slice().len(),
            DatumColumnEncoder::Range(a) => a.values_slice().len(),
            DatumColumnEncoder::Jsonb { buf, .. } => buf.len(),
            DatumColumnEncoder::Array { dims, vals, .. } => {
                dims.len() * PackedArrayDimension::SIZE + vals.goodbytes()
            }
            DatumColumnEncoder::List { values, .. } => values.goodbytes(),
            DatumColumnEncoder::Map { keys, vals, .. } => {
                keys.values_slice().len() + vals.goodbytes()
            }
            DatumColumnEncoder::Record { fields, .. } => fields.iter().map(|f| f.goodbytes()).sum(),
            DatumColumnEncoder::RecordEmpty(a) => a.len(),
        }
    }

    fn push<'e, 'd>(&'e mut self, datum: Datum<'d>) {
        match (self, datum) {
            (DatumColumnEncoder::Bool(bool_builder), Datum::True) => {
                bool_builder.append_value(true)
            }
            (DatumColumnEncoder::Bool(bool_builder), Datum::False) => {
                bool_builder.append_value(false)
            }
            (DatumColumnEncoder::U8(builder), Datum::UInt8(val)) => builder.append_value(val),
            (DatumColumnEncoder::U16(builder), Datum::UInt16(val)) => builder.append_value(val),
            (DatumColumnEncoder::U32(builder), Datum::UInt32(val)) => builder.append_value(val),
            (DatumColumnEncoder::U64(builder), Datum::UInt64(val)) => builder.append_value(val),
            (DatumColumnEncoder::I16(builder), Datum::Int16(val)) => builder.append_value(val),
            (DatumColumnEncoder::I32(builder), Datum::Int32(val)) => builder.append_value(val),
            (DatumColumnEncoder::I64(builder), Datum::Int64(val)) => builder.append_value(val),
            (DatumColumnEncoder::F32(builder), Datum::Float32(val)) => builder.append_value(*val),
            (DatumColumnEncoder::F64(builder), Datum::Float64(val)) => builder.append_value(*val),
            (
                DatumColumnEncoder::Numeric {
                    approx_values,
                    binary_values,
                    numeric_context,
                },
                Datum::Numeric(val),
            ) => {
                let float_approx = numeric_context.try_into_f64(val.0).unwrap_or_else(|_| {
                    numeric_context.clear_status();
                    if val.0.is_negative() {
                        f64::NEG_INFINITY
                    } else {
                        f64::INFINITY
                    }
                });
                let packed = PackedNumeric::from_value(val.0);

                approx_values.append_value(float_approx);
                binary_values.append_value(packed.as_bytes());
            }
            (DatumColumnEncoder::String(builder), Datum::String(val)) => builder.append_value(val),
            (DatumColumnEncoder::Bytes(builder), Datum::Bytes(val)) => builder.append_value(val),
            (DatumColumnEncoder::Date(builder), Datum::Date(val)) => {
                builder.append_value(val.pg_epoch_days())
            }
            (DatumColumnEncoder::Time(builder), Datum::Time(val)) => {
                let packed = PackedNaiveTime::from_value(val);
                builder
                    .append_value(packed.as_bytes())
                    .expect("known correct size");
            }
            (DatumColumnEncoder::Timestamp(builder), Datum::Timestamp(val)) => {
                let packed = PackedNaiveDateTime::from_value(val.to_naive());
                builder
                    .append_value(packed.as_bytes())
                    .expect("known correct size");
            }
            (DatumColumnEncoder::TimestampTz(builder), Datum::TimestampTz(val)) => {
                let packed = PackedNaiveDateTime::from_value(val.to_naive());
                builder
                    .append_value(packed.as_bytes())
                    .expect("known correct size");
            }
            (DatumColumnEncoder::MzTimestamp(builder), Datum::MzTimestamp(val)) => {
                builder.append_value(val.into());
            }
            (DatumColumnEncoder::Interval(builder), Datum::Interval(val)) => {
                let packed = PackedInterval::from_value(val);
                builder
                    .append_value(packed.as_bytes())
                    .expect("known correct size");
            }
            (DatumColumnEncoder::Uuid(builder), Datum::Uuid(val)) => builder
                .append_value(val.as_bytes())
                .expect("known correct size"),
            (DatumColumnEncoder::AclItem(builder), Datum::AclItem(val)) => {
                let packed = PackedAclItem::from_value(val);
                builder
                    .append_value(packed.as_bytes())
                    .expect("known correct size");
            }
            (DatumColumnEncoder::MzAclItem(builder), Datum::MzAclItem(val)) => {
                let packed = PackedMzAclItem::from_value(val);
                builder.append_value(packed.as_bytes());
            }
            (DatumColumnEncoder::Range(builder), d @ Datum::Range(_)) => {
                let proto = ProtoDatum::from(d);
                let bytes = proto.encode_to_vec();
                builder.append_value(&bytes);
            }
            (
                DatumColumnEncoder::Jsonb {
                    offsets,
                    buf,
                    nulls,
                },
                d @ Datum::JsonNull
                | d @ Datum::True
                | d @ Datum::False
                | d @ Datum::Numeric(_)
                | d @ Datum::String(_)
                | d @ Datum::List(_)
                | d @ Datum::Map(_),
            ) => {
                // TODO(parkmycar): Why do we need to re-borrow here?
                let mut buf = buf;
                let json = JsonbRef::from_datum(d);

                // Serialize our JSON.
                json.to_writer(&mut buf)
                    .expect("failed to serialize Datum to jsonb");
                let offset: i32 = buf.len().try_into().expect("wrote more than 4GB of JSON");
                offsets.push(offset);

                if let Some(nulls) = nulls {
                    nulls.append(true);
                }
            }
            (
                DatumColumnEncoder::Array {
                    dims,
                    val_lengths,
                    vals,
                    nulls,
                },
                Datum::Array(array),
            ) => {
                // Store our array dimensions.
                for dimension in array.dims() {
                    let packed = PackedArrayDimension::from_value(dimension);
                    dims.values()
                        .append_value(packed.as_bytes())
                        .expect("known correct size");
                }
                dims.append(true);

                // Store the values of the array.
                let mut count = 0;
                for datum in &array.elements() {
                    count += 1;
                    vals.push(datum);
                }
                val_lengths.push(count);

                if let Some(nulls) = nulls {
                    nulls.append(true);
                }
            }
            (
                DatumColumnEncoder::List {
                    lengths,
                    values,
                    nulls,
                },
                Datum::List(list),
            ) => {
                let mut count = 0;
                for datum in &list {
                    count += 1;
                    values.push(datum);
                }
                lengths.push(count);

                if let Some(nulls) = nulls {
                    nulls.append(true);
                }
            }
            (
                DatumColumnEncoder::Map {
                    lengths,
                    keys,
                    vals,
                    nulls,
                },
                Datum::Map(map),
            ) => {
                let mut count = 0;
                for (key, datum) in &map {
                    count += 1;
                    keys.append_value(key);
                    vals.push(datum);
                }
                lengths.push(count);

                if let Some(nulls) = nulls {
                    nulls.append(true);
                }
            }
            (
                DatumColumnEncoder::Record {
                    fields,
                    nulls,
                    length,
                },
                Datum::List(records),
            ) => {
                let mut count = 0;
                // `zip_eq` will panic if the number of records != number of fields.
                for (datum, encoder) in records.into_iter().zip_eq(fields.iter_mut()) {
                    count += 1;
                    encoder.push(datum);
                }
                assert_eq!(count, fields.len());

                length.add_assign(1);
                if let Some(nulls) = nulls.as_mut() {
                    nulls.append(true);
                }
            }
            (DatumColumnEncoder::RecordEmpty(builder), Datum::List(records)) => {
                assert_none!(records.into_iter().next());
                builder.append_value(true);
            }
            (encoder, Datum::Null) => encoder.push_invalid(),
            (encoder, datum) => panic!("can't encode {datum:?} into {encoder:?}"),
        }
    }

    fn push_invalid(&mut self) {
        match self {
            DatumColumnEncoder::Bool(builder) => builder.append_null(),
            DatumColumnEncoder::U8(builder) => builder.append_null(),
            DatumColumnEncoder::U16(builder) => builder.append_null(),
            DatumColumnEncoder::U32(builder) => builder.append_null(),
            DatumColumnEncoder::U64(builder) => builder.append_null(),
            DatumColumnEncoder::I16(builder) => builder.append_null(),
            DatumColumnEncoder::I32(builder) => builder.append_null(),
            DatumColumnEncoder::I64(builder) => builder.append_null(),
            DatumColumnEncoder::F32(builder) => builder.append_null(),
            DatumColumnEncoder::F64(builder) => builder.append_null(),
            DatumColumnEncoder::Numeric {
                approx_values,
                binary_values,
                numeric_context: _,
            } => {
                approx_values.append_null();
                binary_values.append_null();
            }
            DatumColumnEncoder::String(builder) => builder.append_null(),
            DatumColumnEncoder::Bytes(builder) => builder.append_null(),
            DatumColumnEncoder::Date(builder) => builder.append_null(),
            DatumColumnEncoder::Time(builder) => builder.append_null(),
            DatumColumnEncoder::Timestamp(builder) => builder.append_null(),
            DatumColumnEncoder::TimestampTz(builder) => builder.append_null(),
            DatumColumnEncoder::MzTimestamp(builder) => builder.append_null(),
            DatumColumnEncoder::Interval(builder) => builder.append_null(),
            DatumColumnEncoder::Uuid(builder) => builder.append_null(),
            DatumColumnEncoder::AclItem(builder) => builder.append_null(),
            DatumColumnEncoder::MzAclItem(builder) => builder.append_null(),
            DatumColumnEncoder::Range(builder) => builder.append_null(),
            DatumColumnEncoder::Jsonb {
                offsets,
                buf: _,
                nulls,
            } => {
                let nulls = nulls.get_or_insert_with(|| {
                    let mut buf = BooleanBufferBuilder::new(offsets.len());
                    // The offsets buffer has one more value than there are elements.
                    buf.append_n(offsets.len() - 1, true);
                    buf
                });

                offsets.push(offsets.last().copied().unwrap_or(0));
                nulls.append(false);
            }
            DatumColumnEncoder::Array {
                dims,
                val_lengths,
                vals: _,
                nulls,
            } => {
                let nulls = nulls.get_or_insert_with(|| {
                    let mut buf = BooleanBufferBuilder::new(dims.len() + 1);
                    buf.append_n(dims.len(), true);
                    buf
                });
                dims.append_null();

                val_lengths.push(0);
                nulls.append(false);
            }
            DatumColumnEncoder::List {
                lengths,
                values: _,
                nulls,
            } => {
                let nulls = nulls.get_or_insert_with(|| {
                    let mut buf = BooleanBufferBuilder::new(lengths.len() + 1);
                    buf.append_n(lengths.len(), true);
                    buf
                });

                lengths.push(0);
                nulls.append(false);
            }
            DatumColumnEncoder::Map {
                lengths,
                keys: _,
                vals: _,
                nulls,
            } => {
                let nulls = nulls.get_or_insert_with(|| {
                    let mut buf = BooleanBufferBuilder::new(lengths.len() + 1);
                    buf.append_n(lengths.len(), true);
                    buf
                });

                lengths.push(0);
                nulls.append(false);
            }
            DatumColumnEncoder::Record {
                fields,
                nulls,
                length,
            } => {
                let nulls = nulls.get_or_insert_with(|| {
                    let mut buf = BooleanBufferBuilder::new(*length + 1);
                    buf.append_n(*length, true);
                    buf
                });
                nulls.append(false);
                length.add_assign(1);

                for field in fields {
                    field.push_invalid();
                }
            }
            DatumColumnEncoder::RecordEmpty(builder) => builder.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            DatumColumnEncoder::Bool(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::U8(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::U16(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::U32(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::U64(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::I16(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::I32(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::I64(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::F32(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::F64(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::Numeric {
                mut approx_values,
                mut binary_values,
                numeric_context: _,
            } => {
                let approx_array = approx_values.finish();
                let binary_array = binary_values.finish();

                assert_eq!(approx_array.len(), binary_array.len());
                // This is O(n) so we only enable it for debug assertions.
                debug_assert_eq!(approx_array.logical_nulls(), binary_array.logical_nulls());

                let fields = Fields::from(vec![
                    Field::new("approx", approx_array.data_type().clone(), true),
                    Field::new("binary", binary_array.data_type().clone(), true),
                ]);
                let nulls = approx_array.logical_nulls();
                let array = StructArray::new(
                    fields,
                    vec![Arc::new(approx_array), Arc::new(binary_array)],
                    nulls,
                );
                Arc::new(array)
            }
            DatumColumnEncoder::String(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::Bytes(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::Date(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::Time(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::Timestamp(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::TimestampTz(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::MzTimestamp(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::Interval(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::Uuid(mut builder) => {
                let array = builder.finish();
                Arc::new(array)
            }
            DatumColumnEncoder::AclItem(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::MzAclItem(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Range(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Jsonb {
                offsets,
                buf,
                mut nulls,
            } => {
                let values = Buffer::from_vec(buf);
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
                let nulls = nulls.as_mut().map(|n| NullBuffer::from(n.finish()));
                let array = StringArray::new(offsets, values, nulls);
                Arc::new(array)
            }
            DatumColumnEncoder::Array {
                mut dims,
                val_lengths,
                vals,
                mut nulls,
            } => {
                let nulls = nulls.as_mut().map(|n| NullBuffer::from(n.finish()));
                let vals = vals.finish();

                // Note: Values in an Array can always be Null, regardless of whether or not the
                // column is nullable.
                let field = Field::new_list_field(vals.data_type().clone(), true);
                let val_offsets = OffsetBuffer::from_lengths(val_lengths);
                let values =
                    ListArray::new(Arc::new(field), val_offsets, Arc::new(vals), nulls.clone());

                let dims = dims.finish();
                assert_eq!(values.len(), dims.len());

                // Note: The inner arrays are always nullable, and we let the higher-level array
                // drive nullability for the entire column.
                let fields = Fields::from(vec![
                    Field::new("dims", dims.data_type().clone(), true),
                    Field::new("vals", values.data_type().clone(), true),
                ]);
                let array = StructArray::new(fields, vec![Arc::new(dims), Arc::new(values)], nulls);

                Arc::new(array)
            }
            DatumColumnEncoder::List {
                lengths,
                values,
                mut nulls,
            } => {
                let values = values.finish();

                // Note: Values in an Array can always be Null, regardless of whether or not the
                // column is nullable.
                let field = Field::new_list_field(values.data_type().clone(), true);
                let offsets = OffsetBuffer::<i32>::from_lengths(lengths.iter().copied());
                let nulls = nulls.as_mut().map(|n| NullBuffer::from(n.finish()));

                let array = ListArray::new(Arc::new(field), offsets, values, nulls);
                Arc::new(array)
            }
            DatumColumnEncoder::Map {
                lengths,
                mut keys,
                vals,
                mut nulls,
            } => {
                let keys = keys.finish();
                let vals = vals.finish();

                let offsets = OffsetBuffer::<i32>::from_lengths(lengths.iter().copied());
                let nulls = nulls.as_mut().map(|n| NullBuffer::from(n.finish()));

                // Note: Values in an Map can always be Null, regardless of whether or not the
                // column is nullable, but Keys cannot.
                assert_none!(keys.logical_nulls());
                let key_field = Arc::new(Field::new("key", keys.data_type().clone(), false));
                let val_field = Arc::new(Field::new("val", vals.data_type().clone(), true));
                let fields = Fields::from(vec![Arc::clone(&key_field), Arc::clone(&val_field)]);
                let entries = StructArray::new(fields, vec![Arc::new(keys), vals], None);

                // Note: DatumMap is always sorted, and Arrow enforces that the inner 'map_entries'
                // array can never be null.
                let field = Field::new("map_entries", entries.data_type().clone(), false);
                let array = ListArray::new(Arc::new(field), offsets, Arc::new(entries), nulls);
                Arc::new(array)
            }
            DatumColumnEncoder::Record {
                fields,
                mut nulls,
                length: _,
            } => {
                let (fields, arrays): (Vec<_>, Vec<_>) = fields
                    .into_iter()
                    .enumerate()
                    .map(|(tag, encoder)| {
                        // Note: We mark all columns as nullable at the Arrow/Parquet level because
                        // it has a negligible performance difference, but it protects us from
                        // unintended nullability changes in the columns of SQL objects.
                        //
                        // See: <https://github.com/MaterializeInc/database-issues/issues/2488>
                        let nullable = true;
                        let array = encoder.finish();
                        let field =
                            Field::new(tag.to_string(), array.data_type().clone(), nullable);
                        (field, array)
                    })
                    .unzip();
                let nulls = nulls.as_mut().map(|n| NullBuffer::from(n.finish()));

                let array = StructArray::new(Fields::from(fields), arrays, nulls);
                Arc::new(array)
            }
            DatumColumnEncoder::RecordEmpty(mut builder) => Arc::new(builder.finish()),
        }
    }
}

/// A decoder for a column of [`Datum`]s.
///
/// Note: We specifically structure the decoder as an enum instead of using trait objects because
/// Datum decoding is an extremely hot path and downcasting objects is relatively slow.
#[derive(Debug)]
enum DatumColumnDecoder {
    Bool(BooleanArray),
    U8(UInt8Array),
    U16(UInt16Array),
    U32(UInt32Array),
    U64(UInt64Array),
    I16(Int16Array),
    I32(Int32Array),
    I64(Int64Array),
    F32(Float32Array),
    F64(Float64Array),
    Numeric(BinaryArray),
    Bytes(BinaryArray),
    String(StringArray),
    Date(Int32Array),
    Time(FixedSizeBinaryArray),
    Timestamp(FixedSizeBinaryArray),
    TimestampTz(FixedSizeBinaryArray),
    MzTimestamp(UInt64Array),
    Interval(FixedSizeBinaryArray),
    Uuid(FixedSizeBinaryArray),
    Json(StringArray),
    Array {
        dim_offsets: OffsetBuffer<i32>,
        dims: FixedSizeBinaryArray,
        val_offsets: OffsetBuffer<i32>,
        vals: Box<DatumColumnDecoder>,
        nulls: Option<NullBuffer>,
    },
    List {
        offsets: OffsetBuffer<i32>,
        values: Box<DatumColumnDecoder>,
        nulls: Option<NullBuffer>,
    },
    Map {
        offsets: OffsetBuffer<i32>,
        keys: StringArray,
        vals: Box<DatumColumnDecoder>,
        nulls: Option<NullBuffer>,
    },
    RecordEmpty(BooleanArray),
    Record {
        fields: Vec<Box<DatumColumnDecoder>>,
        nulls: Option<NullBuffer>,
    },
    Range(BinaryArray),
    MzAclItem(BinaryArray),
    AclItem(FixedSizeBinaryArray),
}

impl DatumColumnDecoder {
    fn get<'a>(&'a self, idx: usize, packer: &'a mut RowPacker) {
        let datum = match self {
            DatumColumnDecoder::Bool(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|x| if x { Datum::True } else { Datum::False }),
            DatumColumnDecoder::U8(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::UInt8),
            DatumColumnDecoder::U16(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::UInt16),
            DatumColumnDecoder::U32(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::UInt32),
            DatumColumnDecoder::U64(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::UInt64),
            DatumColumnDecoder::I16(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Int16),
            DatumColumnDecoder::I32(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Int32),
            DatumColumnDecoder::I64(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Int64),
            DatumColumnDecoder::F32(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|x| Datum::Float32(ordered_float::OrderedFloat(x))),
            DatumColumnDecoder::F64(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|x| Datum::Float64(ordered_float::OrderedFloat(x))),
            DatumColumnDecoder::Numeric(array) => array.is_valid(idx).then(|| {
                let val = array.value(idx);
                let val = PackedNumeric::from_bytes(val)
                    .expect("failed to roundtrip Numeric")
                    .into_value();
                Datum::Numeric(OrderedDecimal(val))
            }),
            DatumColumnDecoder::String(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::String),
            DatumColumnDecoder::Bytes(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(Datum::Bytes),
            DatumColumnDecoder::Date(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let date = Date::from_pg_epoch(x).expect("failed to roundtrip");
                    Datum::Date(date)
                })
            }
            DatumColumnDecoder::Time(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let packed = PackedNaiveTime::from_bytes(x).expect("failed to roundtrip time");
                    Datum::Time(packed.into_value())
                })
            }
            DatumColumnDecoder::Timestamp(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let packed = PackedNaiveDateTime::from_bytes(x)
                        .expect("failed to roundtrip PackedNaiveDateTime");
                    let timestamp = CheckedTimestamp::from_timestamplike(packed.into_value())
                        .expect("failed to roundtrip timestamp");
                    Datum::Timestamp(timestamp)
                })
            }
            DatumColumnDecoder::TimestampTz(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let packed = PackedNaiveDateTime::from_bytes(x)
                        .expect("failed to roundtrip PackedNaiveDateTime");
                    let timestamp =
                        CheckedTimestamp::from_timestamplike(packed.into_value().and_utc())
                            .expect("failed to roundtrip timestamp");
                    Datum::TimestampTz(timestamp)
                })
            }
            DatumColumnDecoder::MzTimestamp(array) => array
                .is_valid(idx)
                .then(|| array.value(idx))
                .map(|x| Datum::MzTimestamp(Timestamp::from(x))),
            DatumColumnDecoder::Interval(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let packed =
                        PackedInterval::from_bytes(x).expect("failed to roundtrip interval");
                    Datum::Interval(packed.into_value())
                })
            }
            DatumColumnDecoder::Uuid(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let uuid = Uuid::from_slice(x).expect("failed to roundtrip uuid");
                    Datum::Uuid(uuid)
                })
            }
            DatumColumnDecoder::AclItem(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let packed =
                        PackedAclItem::from_bytes(x).expect("failed to roundtrip MzAclItem");
                    Datum::AclItem(packed.into_value())
                })
            }
            DatumColumnDecoder::MzAclItem(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let packed =
                        PackedMzAclItem::from_bytes(x).expect("failed to roundtrip MzAclItem");
                    Datum::MzAclItem(packed.into_value())
                })
            }
            DatumColumnDecoder::Range(array) => {
                let Some(val) = array.is_valid(idx).then(|| array.value(idx)) else {
                    packer.push(Datum::Null);
                    return;
                };

                let proto = ProtoDatum::decode(val).expect("failed to roundtrip Range");
                packer
                    .try_push_proto(&proto)
                    .expect("failed to pack ProtoRange");

                // Return early because we've already packed the necessary Datums.
                return;
            }
            DatumColumnDecoder::Json(array) => {
                let Some(val) = array.is_valid(idx).then(|| array.value(idx)) else {
                    packer.push(Datum::Null);
                    return;
                };
                JsonbPacker::new(packer)
                    .pack_str(val)
                    .expect("failed to roundtrip JSON");

                // Return early because we've already packed the necessary Datums.
                return;
            }
            DatumColumnDecoder::Array {
                dim_offsets,
                dims,
                val_offsets,
                vals,
                nulls,
            } => {
                let is_valid = nulls.as_ref().map(|n| n.is_valid(idx)).unwrap_or(true);
                if !is_valid {
                    packer.push(Datum::Null);
                    return;
                }

                let start: usize = dim_offsets[idx]
                    .try_into()
                    .expect("unexpected negative offset");
                let end: usize = dim_offsets[idx + 1]
                    .try_into()
                    .expect("unexpected negative offset");
                let dimensions = (start..end).map(|idx| {
                    PackedArrayDimension::from_bytes(dims.value(idx))
                        .expect("failed to roundtrip ArrayDimension")
                        .into_value()
                });

                let start: usize = val_offsets[idx]
                    .try_into()
                    .expect("unexpected negative offset");
                let end: usize = val_offsets[idx + 1]
                    .try_into()
                    .expect("unexpected negative offset");
                packer
                    .push_array_with_row_major(dimensions, |packer| {
                        for x in start..end {
                            vals.get(x, packer);
                        }
                        // Return the numer of Datums we just packed.
                        end - start
                    })
                    .expect("failed to pack Array");

                // Return early because we've already packed the necessary Datums.
                return;
            }
            DatumColumnDecoder::List {
                offsets,
                values,
                nulls,
            } => {
                let is_valid = nulls.as_ref().map(|n| n.is_valid(idx)).unwrap_or(true);
                if !is_valid {
                    packer.push(Datum::Null);
                    return;
                }

                let start: usize = offsets[idx].try_into().expect("unexpected negative offset");
                let end: usize = offsets[idx + 1]
                    .try_into()
                    .expect("unexpected negative offset");

                packer.push_list_with(|packer| {
                    for idx in start..end {
                        values.get(idx, packer)
                    }
                });

                // Return early because we've already packed the necessary Datums.
                return;
            }
            DatumColumnDecoder::Map {
                offsets,
                keys,
                vals,
                nulls,
            } => {
                let is_valid = nulls.as_ref().map(|n| n.is_valid(idx)).unwrap_or(true);
                if !is_valid {
                    packer.push(Datum::Null);
                    return;
                }

                let start: usize = offsets[idx].try_into().expect("unexpected negative offset");
                let end: usize = offsets[idx + 1]
                    .try_into()
                    .expect("unexpected negative offset");

                packer.push_dict_with(|packer| {
                    for idx in start..end {
                        packer.push(Datum::String(keys.value(idx)));
                        vals.get(idx, packer);
                    }
                });

                // Return early because we've already packed the necessary Datums.
                return;
            }
            DatumColumnDecoder::RecordEmpty(array) => array.is_valid(idx).then(Datum::empty_list),
            DatumColumnDecoder::Record { fields, nulls } => {
                let is_valid = nulls.as_ref().map(|n| n.is_valid(idx)).unwrap_or(true);
                if !is_valid {
                    packer.push(Datum::Null);
                    return;
                }

                // let mut datums = Vec::with_capacity(fields.len());
                packer.push_list_with(|packer| {
                    for field in fields {
                        field.get(idx, packer);
                    }
                });

                // Return early because we've already packed the necessary Datums.
                return;
            }
        };

        match datum {
            Some(d) => packer.push(d),
            None => packer.push(Datum::Null),
        }
    }

    fn stats(&self) -> ColumnStatKinds {
        match self {
            DatumColumnDecoder::Bool(a) => PrimitiveStats::<bool>::from_column(a).into(),
            DatumColumnDecoder::U8(a) => PrimitiveStats::<u8>::from_column(a).into(),
            DatumColumnDecoder::U16(a) => PrimitiveStats::<u16>::from_column(a).into(),
            DatumColumnDecoder::U32(a) => PrimitiveStats::<u32>::from_column(a).into(),
            DatumColumnDecoder::U64(a) => PrimitiveStats::<u64>::from_column(a).into(),
            DatumColumnDecoder::I16(a) => PrimitiveStats::<i16>::from_column(a).into(),
            DatumColumnDecoder::I32(a) => PrimitiveStats::<i32>::from_column(a).into(),
            DatumColumnDecoder::I64(a) => PrimitiveStats::<i64>::from_column(a).into(),
            DatumColumnDecoder::F32(a) => PrimitiveStats::<f32>::from_column(a).into(),
            DatumColumnDecoder::F64(a) => PrimitiveStats::<f64>::from_column(a).into(),
            DatumColumnDecoder::Numeric(a) => numeric_stats_from_column(a),
            DatumColumnDecoder::String(a) => PrimitiveStats::<String>::from_column(a).into(),
            DatumColumnDecoder::Bytes(a) => PrimitiveStats::<Vec<u8>>::from_column(a).into(),
            DatumColumnDecoder::Date(a) => PrimitiveStats::<i32>::from_column(a).into(),
            DatumColumnDecoder::Time(a) => {
                fixed_stats_from_column(a, FixedSizeBytesStatsKind::PackedTime)
            }
            DatumColumnDecoder::Timestamp(a) => {
                fixed_stats_from_column(a, FixedSizeBytesStatsKind::PackedDateTime)
            }
            DatumColumnDecoder::TimestampTz(a) => {
                fixed_stats_from_column(a, FixedSizeBytesStatsKind::PackedDateTime)
            }
            DatumColumnDecoder::MzTimestamp(a) => PrimitiveStats::<u64>::from_column(a).into(),
            DatumColumnDecoder::Interval(a) => {
                fixed_stats_from_column(a, FixedSizeBytesStatsKind::PackedInterval)
            }
            DatumColumnDecoder::Uuid(a) => {
                fixed_stats_from_column(a, FixedSizeBytesStatsKind::Uuid)
            }
            DatumColumnDecoder::AclItem(_)
            | DatumColumnDecoder::MzAclItem(_)
            | DatumColumnDecoder::Range(_) => ColumnStatKinds::None,
            DatumColumnDecoder::Json(a) => stats_for_json(a.iter()).values,
            DatumColumnDecoder::Array { .. }
            | DatumColumnDecoder::List { .. }
            | DatumColumnDecoder::Map { .. }
            | DatumColumnDecoder::Record { .. }
            | DatumColumnDecoder::RecordEmpty(_) => ColumnStatKinds::None,
        }
    }

    fn goodbytes(&self) -> usize {
        match self {
            DatumColumnDecoder::Bool(a) => ArrayOrd::Bool(a.clone()).goodbytes(),
            DatumColumnDecoder::U8(a) => ArrayOrd::UInt8(a.clone()).goodbytes(),
            DatumColumnDecoder::U16(a) => ArrayOrd::UInt16(a.clone()).goodbytes(),
            DatumColumnDecoder::U32(a) => ArrayOrd::UInt32(a.clone()).goodbytes(),
            DatumColumnDecoder::U64(a) => ArrayOrd::UInt64(a.clone()).goodbytes(),
            DatumColumnDecoder::I16(a) => ArrayOrd::Int16(a.clone()).goodbytes(),
            DatumColumnDecoder::I32(a) => ArrayOrd::Int32(a.clone()).goodbytes(),
            DatumColumnDecoder::I64(a) => ArrayOrd::Int64(a.clone()).goodbytes(),
            DatumColumnDecoder::F32(a) => ArrayOrd::Float32(a.clone()).goodbytes(),
            DatumColumnDecoder::F64(a) => ArrayOrd::Float64(a.clone()).goodbytes(),
            DatumColumnDecoder::Numeric(a) => ArrayOrd::Binary(a.clone()).goodbytes(),
            DatumColumnDecoder::String(a) => ArrayOrd::String(a.clone()).goodbytes(),
            DatumColumnDecoder::Bytes(a) => ArrayOrd::Binary(a.clone()).goodbytes(),
            DatumColumnDecoder::Date(a) => ArrayOrd::Int32(a.clone()).goodbytes(),
            DatumColumnDecoder::Time(a) => ArrayOrd::FixedSizeBinary(a.clone()).goodbytes(),
            DatumColumnDecoder::Timestamp(a) => ArrayOrd::FixedSizeBinary(a.clone()).goodbytes(),
            DatumColumnDecoder::TimestampTz(a) => ArrayOrd::FixedSizeBinary(a.clone()).goodbytes(),
            DatumColumnDecoder::MzTimestamp(a) => ArrayOrd::UInt64(a.clone()).goodbytes(),
            DatumColumnDecoder::Interval(a) => ArrayOrd::FixedSizeBinary(a.clone()).goodbytes(),
            DatumColumnDecoder::Uuid(a) => ArrayOrd::FixedSizeBinary(a.clone()).goodbytes(),
            DatumColumnDecoder::AclItem(a) => ArrayOrd::FixedSizeBinary(a.clone()).goodbytes(),
            DatumColumnDecoder::MzAclItem(a) => ArrayOrd::Binary(a.clone()).goodbytes(),
            DatumColumnDecoder::Range(a) => ArrayOrd::Binary(a.clone()).goodbytes(),
            DatumColumnDecoder::Json(a) => ArrayOrd::String(a.clone()).goodbytes(),
            DatumColumnDecoder::Array { dims, vals, .. } => {
                (dims.len() * PackedArrayDimension::SIZE) + vals.goodbytes()
            }
            DatumColumnDecoder::List { values, .. } => values.goodbytes(),
            DatumColumnDecoder::Map { keys, vals, .. } => {
                ArrayOrd::String(keys.clone()).goodbytes() + vals.goodbytes()
            }
            DatumColumnDecoder::Record { fields, .. } => fields.iter().map(|f| f.goodbytes()).sum(),
            DatumColumnDecoder::RecordEmpty(a) => ArrayOrd::Bool(a.clone()).goodbytes(),
        }
    }
}

impl Schema<Row> for RelationDesc {
    type ArrowColumn = arrow::array::StructArray;
    type Statistics = OptionStats<StructStats>;

    type Decoder = RowColumnarDecoder;
    type Encoder = RowColumnarEncoder;

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        RowColumnarDecoder::new(col, self)
    }

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        RowColumnarEncoder::new(self)
            .ok_or_else(|| anyhow::anyhow!("Cannot encode a RelationDesc with no columns"))
    }
}

/// A [`ColumnDecoder`] for a [`Row`].
#[derive(Debug)]
pub struct RowColumnarDecoder {
    /// The length of all columns in this decoder; matching all child arrays and the null array
    /// if present.
    len: usize,
    /// Field-specific information: the user-readable field name, the null count (or None if the
    /// column is non-nullable), and the decoder which wraps the column-specific array.
    decoders: Vec<(Arc<str>, Option<usize>, DatumColumnDecoder)>,
    /// The null buffer for this row, if present. (At time of writing, all rows are assumed to be
    /// logically nullable.)
    nullability: Option<NullBuffer>,
}

/// Merge the provided null buffer with the existing array's null buffer, if any.
fn mask_nulls(column: &ArrayRef, null_mask: Option<&NullBuffer>) -> ArrayRef {
    if null_mask.is_none() {
        Arc::clone(column)
    } else {
        // We calculate stats on the nested arrays, so make sure we don't count entries
        // that are masked off at a higher level.
        let nulls = NullBuffer::union(null_mask, column.nulls());
        let data = column
            .to_data()
            .into_builder()
            .nulls(nulls)
            .build()
            .expect("changed only null mask");
        make_array(data)
    }
}

impl RowColumnarDecoder {
    /// Creates a [`RowColumnarDecoder`] that decodes from the provided [`StructArray`].
    ///
    /// Returns an error if the schema of the [`StructArray`] does not match
    /// the provided [`RelationDesc`].
    pub fn new(col: StructArray, desc: &RelationDesc) -> Result<Self, anyhow::Error> {
        let inner_columns = col.columns();
        let desc_columns = desc.typ().columns();

        if desc_columns.len() > inner_columns.len() {
            anyhow::bail!(
                "provided array has too few columns! {desc_columns:?} > {inner_columns:?}"
            );
        }

        // For performance reasons we downcast just a single time.
        let mut decoders = Vec::with_capacity(desc_columns.len());

        let null_mask = col.nulls();

        // The columns of the `StructArray` are named with their column index.
        for (col_idx, col_name, col_type) in desc.iter_all() {
            let field_name = col_idx.to_stable_name();
            let column = col.column_by_name(&field_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "StructArray did not contain column name {field_name}, found {:?}",
                    col.column_names()
                )
            })?;
            let column = mask_nulls(column, null_mask);
            let null_count = col_type.nullable.then(|| column.null_count());
            let decoder = array_to_decoder(&column, &col_type.scalar_type)?;
            decoders.push((col_name.as_str().into(), null_count, decoder));
        }

        Ok(RowColumnarDecoder {
            len: col.len(),
            decoders,
            nullability: col.logical_nulls(),
        })
    }

    // Returns the number of null entries in this array of Row structs. This
    // will be 0 when `Row` is encoded directly, but could be non-zero when it's
    // used inside `SourceDataEncoder`.
    pub fn null_count(&self) -> usize {
        self.nullability.as_ref().map_or(0, |n| n.null_count())
    }
}

impl ColumnDecoder<Row> for RowColumnarDecoder {
    fn decode(&self, idx: usize, val: &mut Row) {
        let mut packer = val.packer();

        for (_, _, decoder) in &self.decoders {
            decoder.get(idx, &mut packer);
        }
    }

    fn is_null(&self, idx: usize) -> bool {
        let Some(nullability) = self.nullability.as_ref() else {
            return false;
        };
        nullability.is_null(idx)
    }

    fn goodbytes(&self) -> usize {
        let decoders_size: usize = self
            .decoders
            .iter()
            .map(|(_name, _null_count, decoder)| decoder.goodbytes())
            .sum();

        decoders_size
            + self
                .nullability
                .as_ref()
                .map(|nulls| nulls.inner().inner().len())
                .unwrap_or(0)
    }

    fn stats(&self) -> StructStats {
        StructStats {
            len: self.len,
            cols: self
                .decoders
                .iter()
                .map(|(name, null_count, decoder)| {
                    let name = name.to_string();
                    let stats = ColumnarStats {
                        nulls: null_count.map(|count| ColumnNullStats { count }),
                        values: decoder.stats(),
                    };
                    (name, stats)
                })
                .collect(),
        }
    }
}

/// A [`ColumnEncoder`] for a [`Row`].
#[derive(Debug)]
pub struct RowColumnarEncoder {
    encoders: Vec<DatumEncoder>,
    // TODO(parkmycar): Replace the `usize` with a `ColumnIdx` type.
    col_names: Vec<(usize, Arc<str>)>,
    // TODO(parkmycar): Optionally omit this.
    nullability: BooleanBufferBuilder,
}

impl RowColumnarEncoder {
    /// Creates a [`RowColumnarEncoder`] for the provided [`RelationDesc`].
    ///
    /// Returns `None` if the provided [`RelationDesc`] has no columns.
    ///
    /// # Note
    /// Internally we represent a [`Row`] as a [`StructArray`] which is
    /// required to have at least one field. Instead of handling this case by
    /// adding some special "internal" column we let a higher level encoder
    /// (e.g. `SourceDataColumnarEncoder`) handle this case.
    pub fn new(desc: &RelationDesc) -> Option<Self> {
        if desc.typ().columns().is_empty() {
            return None;
        }

        let (col_names, encoders): (Vec<_>, Vec<_>) = desc
            .iter_all()
            .map(|(col_idx, col_name, col_type)| {
                let encoder = scalar_type_to_encoder(&col_type.scalar_type)
                    .expect("failed to create encoder");
                let encoder = DatumEncoder {
                    nullable: col_type.nullable,
                    encoder,
                };

                // We name the Fields in Parquet with the column index, but for
                // backwards compat use the column name for stats.
                let name = (col_idx.to_raw(), col_name.as_str().into());

                (name, encoder)
            })
            .unzip();

        Some(RowColumnarEncoder {
            encoders,
            col_names,
            nullability: BooleanBufferBuilder::new(100),
        })
    }
}

impl ColumnEncoder<Row> for RowColumnarEncoder {
    type FinishedColumn = StructArray;

    fn goodbytes(&self) -> usize {
        self.encoders.iter().map(|e| e.goodbytes()).sum()
    }

    fn append(&mut self, val: &Row) {
        let mut num_datums = 0;
        for (datum, encoder) in val.iter().zip(self.encoders.iter_mut()) {
            encoder.push(datum);
            num_datums += 1;
        }
        assert_eq!(
            num_datums,
            self.encoders.len(),
            "tried to encode {val:?}, but only have {:?}",
            self.encoders
        );

        self.nullability.append(true);
    }

    fn append_null(&mut self) {
        for encoder in self.encoders.iter_mut() {
            encoder.push_invalid();
        }
        self.nullability.append(false);
    }

    fn finish(self) -> Self::FinishedColumn {
        let RowColumnarEncoder {
            encoders,
            col_names,
            nullability,
            ..
        } = self;

        let (arrays, fields): (Vec<_>, Vec<_>) = col_names
            .iter()
            .zip_eq(encoders)
            .map(|((col_idx, _col_name), encoder)| {
                // Note: We mark all columns as nullable at the Arrow/Parquet level because it has
                // a negligible performance difference, but it protects us from unintended
                // nullability changes in the columns of SQL objects.
                //
                // See: <https://github.com/MaterializeInc/database-issues/issues/2488>
                let nullable = true;
                let array = encoder.finish();
                let field = Field::new(col_idx.to_string(), array.data_type().clone(), nullable);

                (array, field)
            })
            .multiunzip();

        let null_buffer = NullBuffer::from(BooleanBuffer::from(nullability));

        let array = StructArray::new(Fields::from(fields), arrays, Some(null_buffer));

        array
    }
}

/// Small helper method to make downcasting an [`Array`] return an error.
///
/// Note: it is _super_ important that we downcast as few times as possible. Datum encoding is a
/// very hot path and downcasting is relatively slow
#[inline]
fn downcast_array<T: 'static>(array: &Arc<dyn Array>) -> Result<&T, anyhow::Error> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("expected {}, found {array:?}", std::any::type_name::<T>()))
}

/// Small helper function to downcast from an array to a [`DatumColumnDecoder`].
///
/// Note: it is _super_ important that we downcast as few times as possible. Datum encoding is a
/// very hot path and downcasting is relatively slow
fn array_to_decoder(
    array: &Arc<dyn Array>,
    col_ty: &SqlScalarType,
) -> Result<DatumColumnDecoder, anyhow::Error> {
    let decoder = match (array.data_type(), col_ty) {
        (DataType::Boolean, SqlScalarType::Bool) => {
            let array = downcast_array::<BooleanArray>(array)?;
            DatumColumnDecoder::Bool(array.clone())
        }
        (DataType::UInt8, SqlScalarType::PgLegacyChar) => {
            let array = downcast_array::<UInt8Array>(array)?;
            DatumColumnDecoder::U8(array.clone())
        }
        (DataType::UInt16, SqlScalarType::UInt16) => {
            let array = downcast_array::<UInt16Array>(array)?;
            DatumColumnDecoder::U16(array.clone())
        }
        (
            DataType::UInt32,
            SqlScalarType::UInt32
            | SqlScalarType::Oid
            | SqlScalarType::RegClass
            | SqlScalarType::RegProc
            | SqlScalarType::RegType,
        ) => {
            let array = downcast_array::<UInt32Array>(array)?;
            DatumColumnDecoder::U32(array.clone())
        }
        (DataType::UInt64, SqlScalarType::UInt64) => {
            let array = downcast_array::<UInt64Array>(array)?;
            DatumColumnDecoder::U64(array.clone())
        }
        (DataType::Int16, SqlScalarType::Int16) => {
            let array = downcast_array::<Int16Array>(array)?;
            DatumColumnDecoder::I16(array.clone())
        }
        (DataType::Int32, SqlScalarType::Int32) => {
            let array = downcast_array::<Int32Array>(array)?;
            DatumColumnDecoder::I32(array.clone())
        }
        (DataType::Int64, SqlScalarType::Int64) => {
            let array = downcast_array::<Int64Array>(array)?;
            DatumColumnDecoder::I64(array.clone())
        }
        (DataType::Float32, SqlScalarType::Float32) => {
            let array = downcast_array::<Float32Array>(array)?;
            DatumColumnDecoder::F32(array.clone())
        }
        (DataType::Float64, SqlScalarType::Float64) => {
            let array = downcast_array::<Float64Array>(array)?;
            DatumColumnDecoder::F64(array.clone())
        }
        (DataType::Struct(_), SqlScalarType::Numeric { .. }) => {
            let array = downcast_array::<StructArray>(array)?;
            // Note: We only use the approx column for sorting, and ignore it
            // when decoding.
            let binary_values = array
                .column_by_name("binary")
                .expect("missing binary column");

            let array = downcast_array::<BinaryArray>(binary_values)?;
            DatumColumnDecoder::Numeric(array.clone())
        }
        (
            DataType::Utf8,
            SqlScalarType::String
            | SqlScalarType::PgLegacyName
            | SqlScalarType::Char { .. }
            | SqlScalarType::VarChar { .. },
        ) => {
            let array = downcast_array::<StringArray>(array)?;
            DatumColumnDecoder::String(array.clone())
        }
        (DataType::Binary, SqlScalarType::Bytes) => {
            let array = downcast_array::<BinaryArray>(array)?;
            DatumColumnDecoder::Bytes(array.clone())
        }
        (DataType::Int32, SqlScalarType::Date) => {
            let array = downcast_array::<Int32Array>(array)?;
            DatumColumnDecoder::Date(array.clone())
        }
        (DataType::FixedSizeBinary(TIME_FIXED_BYTES), SqlScalarType::Time) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::Time(array.clone())
        }
        (DataType::FixedSizeBinary(TIMESTAMP_FIXED_BYTES), SqlScalarType::Timestamp { .. }) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::Timestamp(array.clone())
        }
        (DataType::FixedSizeBinary(TIMESTAMP_FIXED_BYTES), SqlScalarType::TimestampTz { .. }) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::TimestampTz(array.clone())
        }
        (DataType::UInt64, SqlScalarType::MzTimestamp) => {
            let array = downcast_array::<UInt64Array>(array)?;
            DatumColumnDecoder::MzTimestamp(array.clone())
        }
        (DataType::FixedSizeBinary(INTERVAL_FIXED_BYTES), SqlScalarType::Interval) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::Interval(array.clone())
        }
        (DataType::FixedSizeBinary(UUID_FIXED_BYTES), SqlScalarType::Uuid) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::Uuid(array.clone())
        }
        (DataType::FixedSizeBinary(ACL_ITEM_FIXED_BYTES), SqlScalarType::AclItem) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::AclItem(array.clone())
        }
        (DataType::Binary, SqlScalarType::MzAclItem) => {
            let array = downcast_array::<BinaryArray>(array)?;
            DatumColumnDecoder::MzAclItem(array.clone())
        }
        (DataType::Binary, SqlScalarType::Range { .. }) => {
            let array = downcast_array::<BinaryArray>(array)?;
            DatumColumnDecoder::Range(array.clone())
        }
        (DataType::Utf8, SqlScalarType::Jsonb) => {
            let array = downcast_array::<StringArray>(array)?;
            DatumColumnDecoder::Json(array.clone())
        }
        (DataType::Struct(_), s @ SqlScalarType::Array(_) | s @ SqlScalarType::Int2Vector) => {
            let element_type = match s {
                SqlScalarType::Array(inner) => inner,
                SqlScalarType::Int2Vector => &SqlScalarType::Int16,
                _ => unreachable!("checked above"),
            };

            let array = downcast_array::<StructArray>(array)?;
            let nulls = array.nulls().cloned();

            let dims = array
                .column_by_name("dims")
                .expect("missing dimensions column");
            let dims = downcast_array::<ListArray>(dims).cloned()?;
            let dim_offsets = dims.offsets().clone();
            let dims = downcast_array::<FixedSizeBinaryArray>(dims.values()).cloned()?;

            let vals = array.column_by_name("vals").expect("missing values column");
            let vals = downcast_array::<ListArray>(vals)?;
            let val_offsets = vals.offsets().clone();
            let vals = array_to_decoder(vals.values(), element_type)?;

            DatumColumnDecoder::Array {
                dim_offsets,
                dims,
                val_offsets,
                vals: Box::new(vals),
                nulls,
            }
        }
        (DataType::List(_), SqlScalarType::List { element_type, .. }) => {
            let array = downcast_array::<ListArray>(array)?;
            let inner_decoder = array_to_decoder(array.values(), &*element_type)?;
            DatumColumnDecoder::List {
                offsets: array.offsets().clone(),
                values: Box::new(inner_decoder),
                nulls: array.nulls().cloned(),
            }
        }
        (DataType::Map(_, true), SqlScalarType::Map { value_type, .. }) => {
            let array = downcast_array::<MapArray>(array)?;
            let keys = downcast_array::<StringArray>(array.keys())?;
            let vals = array_to_decoder(array.values(), value_type)?;
            DatumColumnDecoder::Map {
                offsets: array.offsets().clone(),
                keys: keys.clone(),
                vals: Box::new(vals),
                nulls: array.nulls().cloned(),
            }
        }
        (DataType::List(_), SqlScalarType::Map { value_type, .. }) => {
            let array: &ListArray = downcast_array(array)?;
            let entries: &StructArray = downcast_array(array.values())?;
            let [keys, values]: &[ArrayRef; 2] = entries.columns().try_into()?;
            let keys: &StringArray = downcast_array(keys)?;
            let vals: DatumColumnDecoder = array_to_decoder(values, value_type)?;
            DatumColumnDecoder::Map {
                offsets: array.offsets().clone(),
                keys: keys.clone(),
                vals: Box::new(vals),
                nulls: array.nulls().cloned(),
            }
        }
        (DataType::Boolean, SqlScalarType::Record { fields, .. }) if fields.is_empty() => {
            let empty_record_array = downcast_array::<BooleanArray>(array)?;
            DatumColumnDecoder::RecordEmpty(empty_record_array.clone())
        }
        (DataType::Struct(_), SqlScalarType::Record { fields, .. }) => {
            let record_array = downcast_array::<StructArray>(array)?;
            let null_mask = record_array.nulls();
            let mut decoders = Vec::with_capacity(fields.len());
            for (tag, (_name, col_type)) in fields.iter().enumerate() {
                let inner_array = record_array
                    .column_by_name(&tag.to_string())
                    .ok_or_else(|| anyhow::anyhow!("no column named '{tag}'"))?;
                let inner_array = mask_nulls(inner_array, null_mask);
                let inner_decoder = array_to_decoder(&inner_array, &col_type.scalar_type)?;

                decoders.push(Box::new(inner_decoder));
            }

            DatumColumnDecoder::Record {
                fields: decoders,
                nulls: record_array.nulls().cloned(),
            }
        }
        (x, y) => {
            let msg = format!("can't decode column of {x:?} for scalar type {y:?}");
            mz_ore::soft_panic_or_log!("{msg}");
            anyhow::bail!("{msg}");
        }
    };

    Ok(decoder)
}

/// Small helper function to create a [`DatumColumnEncoder`] from a [`ScalarType`]
fn scalar_type_to_encoder(col_ty: &SqlScalarType) -> Result<DatumColumnEncoder, anyhow::Error> {
    let encoder = match &col_ty {
        SqlScalarType::Bool => DatumColumnEncoder::Bool(BooleanBuilder::new()),
        SqlScalarType::PgLegacyChar => DatumColumnEncoder::U8(UInt8Builder::new()),
        SqlScalarType::UInt16 => DatumColumnEncoder::U16(UInt16Builder::new()),
        SqlScalarType::UInt32
        | SqlScalarType::Oid
        | SqlScalarType::RegClass
        | SqlScalarType::RegProc
        | SqlScalarType::RegType => DatumColumnEncoder::U32(UInt32Builder::new()),
        SqlScalarType::UInt64 => DatumColumnEncoder::U64(UInt64Builder::new()),
        SqlScalarType::Int16 => DatumColumnEncoder::I16(Int16Builder::new()),
        SqlScalarType::Int32 => DatumColumnEncoder::I32(Int32Builder::new()),
        SqlScalarType::Int64 => DatumColumnEncoder::I64(Int64Builder::new()),
        SqlScalarType::Float32 => DatumColumnEncoder::F32(Float32Builder::new()),
        SqlScalarType::Float64 => DatumColumnEncoder::F64(Float64Builder::new()),
        SqlScalarType::Numeric { .. } => DatumColumnEncoder::Numeric {
            approx_values: Float64Builder::new(),
            binary_values: BinaryBuilder::new(),
            numeric_context: crate::adt::numeric::cx_datum().clone(),
        },
        SqlScalarType::String
        | SqlScalarType::PgLegacyName
        | SqlScalarType::Char { .. }
        | SqlScalarType::VarChar { .. } => DatumColumnEncoder::String(StringBuilder::new()),
        SqlScalarType::Bytes => DatumColumnEncoder::Bytes(BinaryBuilder::new()),
        SqlScalarType::Date => DatumColumnEncoder::Date(Int32Builder::new()),
        SqlScalarType::Time => {
            DatumColumnEncoder::Time(FixedSizeBinaryBuilder::new(TIME_FIXED_BYTES))
        }
        SqlScalarType::Timestamp { .. } => {
            DatumColumnEncoder::Timestamp(FixedSizeBinaryBuilder::new(TIMESTAMP_FIXED_BYTES))
        }
        SqlScalarType::TimestampTz { .. } => {
            DatumColumnEncoder::TimestampTz(FixedSizeBinaryBuilder::new(TIMESTAMP_FIXED_BYTES))
        }
        SqlScalarType::MzTimestamp => DatumColumnEncoder::MzTimestamp(UInt64Builder::new()),
        SqlScalarType::Interval => {
            DatumColumnEncoder::Interval(FixedSizeBinaryBuilder::new(INTERVAL_FIXED_BYTES))
        }
        SqlScalarType::Uuid => {
            DatumColumnEncoder::Uuid(FixedSizeBinaryBuilder::new(UUID_FIXED_BYTES))
        }
        SqlScalarType::AclItem => {
            DatumColumnEncoder::AclItem(FixedSizeBinaryBuilder::new(ACL_ITEM_FIXED_BYTES))
        }
        SqlScalarType::MzAclItem => DatumColumnEncoder::MzAclItem(BinaryBuilder::new()),
        SqlScalarType::Range { .. } => DatumColumnEncoder::Range(BinaryBuilder::new()),
        SqlScalarType::Jsonb => DatumColumnEncoder::Jsonb {
            offsets: vec![0],
            buf: Vec::new(),
            nulls: None,
        },
        s @ SqlScalarType::Array(_) | s @ SqlScalarType::Int2Vector => {
            let element_type = match s {
                SqlScalarType::Array(inner) => inner,
                SqlScalarType::Int2Vector => &SqlScalarType::Int16,
                _ => unreachable!("checked above"),
            };
            let inner = scalar_type_to_encoder(element_type)?;
            DatumColumnEncoder::Array {
                dims: ListBuilder::new(FixedSizeBinaryBuilder::new(ARRAY_DIMENSION_FIXED_BYTES)),
                val_lengths: Vec::new(),
                vals: Box::new(inner),
                nulls: None,
            }
        }
        SqlScalarType::List { element_type, .. } => {
            let inner = scalar_type_to_encoder(&*element_type)?;
            DatumColumnEncoder::List {
                lengths: Vec::new(),
                values: Box::new(inner),
                nulls: None,
            }
        }
        SqlScalarType::Map { value_type, .. } => {
            let inner = scalar_type_to_encoder(&*value_type)?;
            DatumColumnEncoder::Map {
                lengths: Vec::new(),
                keys: StringBuilder::new(),
                vals: Box::new(inner),
                nulls: None,
            }
        }
        SqlScalarType::Record { fields, .. } if fields.is_empty() => {
            DatumColumnEncoder::RecordEmpty(BooleanBuilder::new())
        }
        SqlScalarType::Record { fields, .. } => {
            let encoders = fields
                .iter()
                .map(|(_name, ty)| {
                    scalar_type_to_encoder(&ty.scalar_type).map(|e| DatumEncoder {
                        nullable: ty.nullable,
                        encoder: e,
                    })
                })
                .collect::<Result<_, _>>()?;

            DatumColumnEncoder::Record {
                fields: encoders,
                nulls: None,
                length: 0,
            }
        }
    };
    Ok(encoder)
}

impl Codec for Row {
    type Storage = ProtoRow;
    type Schema = RelationDesc;

    fn codec_name() -> String {
        "protobuf[Row]".into()
    }

    /// Encodes a row into the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::decode]. It's guaranteed to be
    /// readable by future versions of Materialize through v(TODO: Figure out
    /// our policy).
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    /// Decodes a row from the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::encode]. It can read rows
    /// encoded by historical versions of Materialize back to v(TODO: Figure out
    /// our policy).
    fn decode(buf: &[u8], schema: &RelationDesc) -> Result<Row, String> {
        // NB: We could easily implement this directly instead of via
        // `decode_from`, but do this so that we get maximal coverage of the
        // more complicated codepath.
        //
        // The length of the encoded ProtoRow (i.e. `buf.len()`) doesn't perfect
        // predict the length of the resulting Row, but it's definitely
        // correlated, so probably a decent estimate.
        let mut row = Row::with_capacity(buf.len());
        <Self as Codec>::decode_from(&mut row, buf, &mut None, schema)?;
        Ok(row)
    }

    fn decode_from<'a>(
        &mut self,
        buf: &'a [u8],
        storage: &mut Option<ProtoRow>,
        schema: &RelationDesc,
    ) -> Result<(), String> {
        let mut proto = storage.take().unwrap_or_default();
        proto.clear();
        proto.merge(buf).map_err(|err| err.to_string())?;
        let ret = self.decode_from_proto(&proto, schema);
        storage.replace(proto);
        ret
    }

    fn validate(row: &Self, desc: &Self::Schema) -> Result<(), String> {
        for x in Itertools::zip_longest(desc.iter_types(), row.iter()) {
            match x {
                EitherOrBoth::Both(typ, datum) if datum.is_instance_of(typ) => continue,
                _ => return Err(format!("row {:?} did not match desc {:?}", row, desc)),
            };
        }
        Ok(())
    }

    fn encode_schema(schema: &Self::Schema) -> Bytes {
        schema.into_proto().encode_to_vec().into()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        let proto = ProtoRelationDesc::decode(buf.as_ref()).expect("valid schema");
        proto.into_rust().expect("valid schema")
    }
}

impl<'a> From<Datum<'a>> for ProtoDatum {
    fn from(x: Datum<'a>) -> Self {
        let datum_type = match x {
            Datum::False => DatumType::Other(ProtoDatumOther::False.into()),
            Datum::True => DatumType::Other(ProtoDatumOther::True.into()),
            Datum::Int16(x) => DatumType::Int16(x.into()),
            Datum::Int32(x) => DatumType::Int32(x),
            Datum::UInt8(x) => DatumType::Uint8(x.into()),
            Datum::UInt16(x) => DatumType::Uint16(x.into()),
            Datum::UInt32(x) => DatumType::Uint32(x),
            Datum::UInt64(x) => DatumType::Uint64(x),
            Datum::Int64(x) => DatumType::Int64(x),
            Datum::Float32(x) => DatumType::Float32(x.into_inner()),
            Datum::Float64(x) => DatumType::Float64(x.into_inner()),
            Datum::Date(x) => DatumType::Date(x.into_proto()),
            Datum::Time(x) => DatumType::Time(ProtoNaiveTime {
                secs: x.num_seconds_from_midnight(),
                frac: x.nanosecond(),
            }),
            Datum::Timestamp(x) => DatumType::Timestamp(x.into_proto()),
            Datum::TimestampTz(x) => DatumType::TimestampTz(x.into_proto()),
            Datum::Interval(x) => DatumType::Interval(x.into_proto()),
            Datum::Bytes(x) => DatumType::Bytes(Bytes::copy_from_slice(x)),
            Datum::String(x) => DatumType::String(x.to_owned()),
            Datum::Array(x) => DatumType::Array(ProtoArray {
                elements: Some(ProtoRow {
                    datums: x.elements().iter().map(|x| x.into()).collect(),
                }),
                dims: x
                    .dims()
                    .into_iter()
                    .map(|x| ProtoArrayDimension {
                        lower_bound: i64::cast_from(x.lower_bound),
                        length: u64::cast_from(x.length),
                    })
                    .collect(),
            }),
            Datum::List(x) => DatumType::List(ProtoRow {
                datums: x.iter().map(|x| x.into()).collect(),
            }),
            Datum::Map(x) => DatumType::Dict(ProtoDict {
                elements: x
                    .iter()
                    .map(|(k, v)| ProtoDictElement {
                        key: k.to_owned(),
                        val: Some(v.into()),
                    })
                    .collect(),
            }),
            Datum::Numeric(x) => {
                // TODO: Do we need this defensive clone?
                let mut x = x.0.clone();
                if let Some((bcd, scale)) = x.to_packed_bcd() {
                    DatumType::Numeric(ProtoNumeric { bcd, scale })
                } else if x.is_nan() {
                    DatumType::Other(ProtoDatumOther::NumericNaN.into())
                } else if x.is_infinite() {
                    if x.is_negative() {
                        DatumType::Other(ProtoDatumOther::NumericNegInf.into())
                    } else {
                        DatumType::Other(ProtoDatumOther::NumericPosInf.into())
                    }
                } else if x.is_special() {
                    panic!("internal error: unhandled special numeric value: {}", x);
                } else {
                    panic!(
                        "internal error: to_packed_bcd returned None for non-special value: {}",
                        x
                    )
                }
            }
            Datum::JsonNull => DatumType::Other(ProtoDatumOther::JsonNull.into()),
            Datum::Uuid(x) => DatumType::Uuid(x.as_bytes().to_vec()),
            Datum::MzTimestamp(x) => DatumType::MzTimestamp(x.into()),
            Datum::Dummy => DatumType::Other(ProtoDatumOther::Dummy.into()),
            Datum::Null => DatumType::Other(ProtoDatumOther::Null.into()),
            Datum::Range(super::Range { inner }) => DatumType::Range(Box::new(ProtoRange {
                inner: inner.map(|RangeInner { lower, upper }| {
                    Box::new(ProtoRangeInner {
                        lower_inclusive: lower.inclusive,
                        lower: lower.bound.map(|bound| Box::new(bound.datum().into())),
                        upper_inclusive: upper.inclusive,
                        upper: upper.bound.map(|bound| Box::new(bound.datum().into())),
                    })
                }),
            })),
            Datum::MzAclItem(x) => DatumType::MzAclItem(x.into_proto()),
            Datum::AclItem(x) => DatumType::AclItem(x.into_proto()),
        };
        ProtoDatum {
            datum_type: Some(datum_type),
        }
    }
}

impl RowPacker<'_> {
    pub(crate) fn try_push_proto(&mut self, x: &ProtoDatum) -> Result<(), String> {
        match &x.datum_type {
            Some(DatumType::Other(o)) => match ProtoDatumOther::try_from(*o) {
                Ok(ProtoDatumOther::Unknown) => return Err("unknown datum type".into()),
                Ok(ProtoDatumOther::Null) => self.push(Datum::Null),
                Ok(ProtoDatumOther::False) => self.push(Datum::False),
                Ok(ProtoDatumOther::True) => self.push(Datum::True),
                Ok(ProtoDatumOther::JsonNull) => self.push(Datum::JsonNull),
                Ok(ProtoDatumOther::Dummy) => {
                    // We plan to remove the `Dummy` variant soon (materialize#17099). To prepare for that, we
                    // emit a log to Sentry here, to notify us of any instances that might have
                    // been made durable.
                    #[cfg(feature = "tracing")]
                    tracing::error!("protobuf decoding found Dummy datum");
                    self.push(Datum::Dummy);
                }
                Ok(ProtoDatumOther::NumericPosInf) => self.push(Datum::from(Numeric::infinity())),
                Ok(ProtoDatumOther::NumericNegInf) => self.push(Datum::from(-Numeric::infinity())),
                Ok(ProtoDatumOther::NumericNaN) => self.push(Datum::from(Numeric::nan())),
                Err(_) => return Err(format!("unknown datum type: {}", o)),
            },
            Some(DatumType::Int16(x)) => {
                let x = i16::try_from(*x)
                    .map_err(|_| format!("int16 field stored with out of range value: {}", *x))?;
                self.push(Datum::Int16(x))
            }
            Some(DatumType::Int32(x)) => self.push(Datum::Int32(*x)),
            Some(DatumType::Int64(x)) => self.push(Datum::Int64(*x)),
            Some(DatumType::Uint8(x)) => {
                let x = u8::try_from(*x)
                    .map_err(|_| format!("uint8 field stored with out of range value: {}", *x))?;
                self.push(Datum::UInt8(x))
            }
            Some(DatumType::Uint16(x)) => {
                let x = u16::try_from(*x)
                    .map_err(|_| format!("uint16 field stored with out of range value: {}", *x))?;
                self.push(Datum::UInt16(x))
            }
            Some(DatumType::Uint32(x)) => self.push(Datum::UInt32(*x)),
            Some(DatumType::Uint64(x)) => self.push(Datum::UInt64(*x)),
            Some(DatumType::Float32(x)) => self.push(Datum::Float32((*x).into())),
            Some(DatumType::Float64(x)) => self.push(Datum::Float64((*x).into())),
            Some(DatumType::Bytes(x)) => self.push(Datum::Bytes(x)),
            Some(DatumType::String(x)) => self.push(Datum::String(x)),
            Some(DatumType::Uuid(x)) => {
                // Uuid internally has a [u8; 16] so we'll have to do at least
                // one copy, but there's currently an additional one when the
                // Vec is created. Perhaps the protobuf Bytes support will let
                // us fix one of them.
                let u = Uuid::from_slice(x).map_err(|err| err.to_string())?;
                self.push(Datum::Uuid(u));
            }
            Some(DatumType::Date(x)) => self.push(Datum::Date(x.clone().into_rust()?)),
            Some(DatumType::Time(x)) => self.push(Datum::Time(x.clone().into_rust()?)),
            Some(DatumType::Timestamp(x)) => self.push(Datum::Timestamp(x.clone().into_rust()?)),
            Some(DatumType::TimestampTz(x)) => {
                self.push(Datum::TimestampTz(x.clone().into_rust()?))
            }
            Some(DatumType::Interval(x)) => self.push(Datum::Interval(
                x.clone()
                    .into_rust()
                    .map_err(|e: TryFromProtoError| e.to_string())?,
            )),
            Some(DatumType::List(x)) => self.push_list_with(|row| -> Result<(), String> {
                for d in x.datums.iter() {
                    row.try_push_proto(d)?;
                }
                Ok(())
            })?,
            Some(DatumType::Array(x)) => {
                let dims = x
                    .dims
                    .iter()
                    .map(|x| ArrayDimension {
                        lower_bound: isize::cast_from(x.lower_bound),
                        length: usize::cast_from(x.length),
                    })
                    .collect::<Vec<_>>();
                match x.elements.as_ref() {
                    None => self.try_push_array(&dims, [].iter()),
                    Some(elements) => {
                        // TODO: Could we avoid this Row alloc if we made a
                        // push_array_with?
                        let elements_row = Row::try_from(elements)?;
                        self.try_push_array(&dims, elements_row.iter())
                    }
                }
                .map_err(|err| err.to_string())?
            }
            Some(DatumType::Dict(x)) => self.push_dict_with(|row| -> Result<(), String> {
                for e in x.elements.iter() {
                    row.push(Datum::from(e.key.as_str()));
                    let val = e
                        .val
                        .as_ref()
                        .ok_or_else(|| format!("missing val for key: {}", e.key))?;
                    row.try_push_proto(val)?;
                }
                Ok(())
            })?,
            Some(DatumType::Numeric(x)) => {
                // Reminder that special values like NaN, PosInf, and NegInf are
                // represented as variants of ProtoDatumOther.
                let n = Decimal::from_packed_bcd(&x.bcd, x.scale).map_err(|err| err.to_string())?;
                self.push(Datum::from(n))
            }
            Some(DatumType::MzTimestamp(x)) => self.push(Datum::MzTimestamp((*x).into())),
            Some(DatumType::Range(inner)) => {
                let ProtoRange { inner } = &**inner;
                match inner {
                    None => self.push_range(Range { inner: None }).unwrap(),
                    Some(inner) => {
                        let ProtoRangeInner {
                            lower_inclusive,
                            lower,
                            upper_inclusive,
                            upper,
                        } = &**inner;

                        self.push_range_with(
                            RangeLowerBound {
                                inclusive: *lower_inclusive,
                                bound: lower
                                    .as_ref()
                                    .map(|d| |row: &mut RowPacker| row.try_push_proto(&*d)),
                            },
                            RangeUpperBound {
                                inclusive: *upper_inclusive,
                                bound: upper
                                    .as_ref()
                                    .map(|d| |row: &mut RowPacker| row.try_push_proto(&*d)),
                            },
                        )
                        .expect("decoding ProtoRow must succeed");
                    }
                }
            }
            Some(DatumType::MzAclItem(x)) => self.push(Datum::MzAclItem(x.clone().into_rust()?)),
            Some(DatumType::AclItem(x)) => self.push(Datum::AclItem(x.clone().into_rust()?)),
            None => return Err("unknown datum type".into()),
        };
        Ok(())
    }
}

/// TODO: remove this in favor of [`RustType::from_proto`].
impl TryFrom<&ProtoRow> for Row {
    type Error = String;

    fn try_from(x: &ProtoRow) -> Result<Self, Self::Error> {
        // TODO: Try to pre-size this.
        // see https://github.com/MaterializeInc/database-issues/issues/3640
        let mut row = Row::default();
        let mut packer = row.packer();
        for d in x.datums.iter() {
            packer.try_push_proto(d)?;
        }
        Ok(row)
    }
}

impl RustType<ProtoRow> for Row {
    fn into_proto(&self) -> ProtoRow {
        let datums = self.iter().map(|x| x.into()).collect();
        ProtoRow { datums }
    }

    fn from_proto(proto: ProtoRow) -> Result<Self, TryFromProtoError> {
        // TODO: Try to pre-size this.
        // see https://github.com/MaterializeInc/database-issues/issues/3640
        let mut row = Row::default();
        let mut packer = row.packer();
        for d in proto.datums.iter() {
            packer
                .try_push_proto(d)
                .map_err(TryFromProtoError::RowConversionError)?;
        }
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::array::{ArrayData, make_array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::ArrowNativeType;
    use arrow::row::SortField;
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use mz_ore::assert_err;
    use mz_ore::collections::CollectionExt;
    use mz_persist::indexed::columnar::arrow::realloc_array;
    use mz_persist::metrics::ColumnarMetrics;
    use mz_persist_types::Codec;
    use mz_persist_types::arrow::{ArrayBound, ArrayOrd};
    use mz_persist_types::columnar::{codec_to_schema, schema_to_codec};
    use mz_proto::{ProtoType, RustType};
    use proptest::prelude::*;
    use proptest::strategy::Strategy;
    use uuid::Uuid;

    use super::*;
    use crate::adt::array::ArrayDimension;
    use crate::adt::interval::Interval;
    use crate::adt::numeric::Numeric;
    use crate::adt::timestamp::CheckedTimestamp;
    use crate::fixed_length::ToDatumIter;
    use crate::relation::arb_relation_desc;
    use crate::{ColumnName, RowArena, SqlColumnType, arb_datum_for_column, arb_row_for_relation};
    use crate::{Datum, RelationDesc, Row, SqlScalarType};

    #[track_caller]
    fn roundtrip_datum<'a>(
        ty: SqlColumnType,
        datum: impl Iterator<Item = Datum<'a>>,
        metrics: &ColumnarMetrics,
    ) {
        let desc = RelationDesc::builder().with_column("a", ty).finish();
        let rows = datum.map(|d| Row::pack_slice(&[d])).collect();
        roundtrip_rows(&desc, rows, metrics)
    }

    #[track_caller]
    fn roundtrip_rows(desc: &RelationDesc, rows: Vec<Row>, metrics: &ColumnarMetrics) {
        let mut encoder = <RelationDesc as Schema<Row>>::encoder(desc).unwrap();
        for row in &rows {
            encoder.append(row);
        }
        let col = encoder.finish();

        // Exercise reallocating columns with lgalloc.
        let col = realloc_array(&col, metrics);
        // Exercise our ProtoArray format.
        {
            let proto = col.to_data().into_proto();
            let bytes = proto.encode_to_vec();
            let proto = mz_persist_types::arrow::ProtoArrayData::decode(&bytes[..]).unwrap();
            let array_data: ArrayData = proto.into_rust().unwrap();

            let col_rnd = StructArray::from(array_data.clone());
            assert_eq!(col, col_rnd);

            let col_dyn = arrow::array::make_array(array_data);
            let col_dyn = col_dyn.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(&col, col_dyn);
        }

        let decoder = <RelationDesc as Schema<Row>>::decoder(desc, col.clone()).unwrap();
        let stats = decoder.stats();

        // Collect all of our lower and upper bounds.
        let arena = RowArena::new();
        let (stats, stat_nulls): (Vec<_>, Vec<_>) = desc
            .iter()
            .map(|(name, ty)| {
                let col_stats = stats.cols.get(name.as_str()).unwrap();
                let lower_upper =
                    crate::stats::col_values(&ty.scalar_type, &col_stats.values, &arena);
                let null_count = col_stats.nulls.map_or(0, |n| n.count);

                (lower_upper, null_count)
            })
            .unzip();
        // Track how many nulls we saw for each column so we can assert stats match.
        let mut actual_nulls = vec![0usize; stats.len()];

        let mut rnd_row = Row::default();
        for (idx, og_row) in rows.iter().enumerate() {
            decoder.decode(idx, &mut rnd_row);
            assert_eq!(og_row, &rnd_row);

            // Check for each Datum in each Row that we're within our stats bounds.
            for (c_idx, (rnd_datum, ty)) in rnd_row.iter().zip_eq(desc.typ().columns()).enumerate()
            {
                let lower_upper = stats[c_idx];

                // Assert our stat bounds are correct.
                if rnd_datum.is_null() {
                    actual_nulls[c_idx] += 1;
                } else if let Some((lower, upper)) = lower_upper {
                    assert!(rnd_datum >= lower, "{rnd_datum:?} is not >= {lower:?}");
                    assert!(rnd_datum <= upper, "{rnd_datum:?} is not <= {upper:?}");
                } else {
                    match &ty.scalar_type {
                        // JSON stats are handled separately.
                        SqlScalarType::Jsonb => (),
                        // We don't collect stats for these types.
                        SqlScalarType::AclItem
                        | SqlScalarType::MzAclItem
                        | SqlScalarType::Range { .. }
                        | SqlScalarType::Array(_)
                        | SqlScalarType::Map { .. }
                        | SqlScalarType::List { .. }
                        | SqlScalarType::Record { .. }
                        | SqlScalarType::Int2Vector => (),
                        other => panic!("should have collected stats for {other:?}"),
                    }
                }
            }
        }

        // Validate that the null counts in our stats matched the actual counts.
        for (col_idx, (stats_count, actual_count)) in
            stat_nulls.iter().zip_eq(actual_nulls.iter()).enumerate()
        {
            assert_eq!(
                stats_count, actual_count,
                "column {col_idx} has incorrect number of nulls!"
            );
        }

        // Validate that we can convert losslessly to codec and back
        let codec = schema_to_codec::<Row>(desc, &col).unwrap();
        let col2 = codec_to_schema::<Row>(desc, &codec).unwrap();
        assert_eq!(col2.as_ref(), &col);

        // Validate that we only generate supported array types
        let converter = arrow::row::RowConverter::new(vec![SortField::new_with_options(
            col.data_type().clone(),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
        .expect("sortable");
        let rows = converter
            .convert_columns(&[Arc::new(col.clone())])
            .expect("convertible");
        let mut row_vec = rows.iter().collect::<Vec<_>>();
        row_vec.sort();
        let row_col = converter
            .convert_rows(row_vec)
            .expect("convertible")
            .into_element();
        assert_eq!(row_col.len(), col.len());

        let ord = ArrayOrd::new(&col);
        let mut indices = (0..u64::usize_as(col.len())).collect::<Vec<_>>();
        indices.sort_by_key(|i| ord.at(i.as_usize()));
        let indices = UInt64Array::from(indices);
        let ord_col = ::arrow::compute::take(&col, &indices, None).expect("takeable");
        assert_eq!(row_col.as_ref(), ord_col.as_ref());

        // Check that our order matches the datum-native order when `preserves_order` is true.
        let ordered_prefix_len = desc
            .iter()
            .take_while(|(_, c)| preserves_order(&c.scalar_type))
            .count();
        let decoder = <RelationDesc as Schema<Row>>::decoder_any(desc, ord_col.as_ref()).unwrap();
        let rows = (0..ord_col.len()).map(|i| {
            let mut row = Row::default();
            decoder.decode(i, &mut row);
            row
        });
        for (a, b) in rows.tuple_windows() {
            let a_prefix = a.iter().take(ordered_prefix_len);
            let b_prefix = b.iter().take(ordered_prefix_len);
            assert!(
                a_prefix.cmp(b_prefix).is_le(),
                "ordering should be consistent on preserves_order columns: {:#?}\n{:?}\n{:?}",
                desc.iter().take(ordered_prefix_len).collect_vec(),
                a.to_datum_iter().take(ordered_prefix_len).collect_vec(),
                b.to_datum_iter().take(ordered_prefix_len).collect_vec()
            );
        }

        // Check that our size estimates are consistent.
        assert_eq!(
            ord.goodbytes(),
            (0..col.len()).map(|i| ord.at(i).goodbytes()).sum::<usize>(),
            "total size should match the sum of the sizes at each index"
        );

        // Check that our lower bounds work as expected.
        if !ord_col.is_empty() {
            let min_idx = indices.values()[0].as_usize();
            let lower_bound = ArrayBound::new(ord_col, min_idx);
            let max_encoded_len = 1000;
            if let Some(proto) = lower_bound.to_proto_lower(max_encoded_len) {
                assert!(
                    proto.encoded_len() <= max_encoded_len,
                    "should respect the max len"
                );
                let array_data = proto.into_rust().expect("valid array");
                let new_lower_bound = ArrayBound::new(make_array(array_data), 0);
                assert!(
                    new_lower_bound.get() <= lower_bound.get(),
                    "proto-roundtripped bound should be <= the original"
                );
            }
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn proptest_datums() {
        let strat = any::<SqlColumnType>().prop_flat_map(|ty| {
            proptest::collection::vec(arb_datum_for_column(ty.clone()), 0..16)
                .prop_map(move |d| (ty.clone(), d))
        });
        let metrics = ColumnarMetrics::disconnected();

        proptest!(|((ty, datums) in strat)| {
            roundtrip_datum(ty.clone(), datums.iter().map(Datum::from), &metrics);
        })
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn proptest_non_empty_relation_descs() {
        let strat = arb_relation_desc(1..8).prop_flat_map(|desc| {
            proptest::collection::vec(arb_row_for_relation(&desc), 0..12)
                .prop_map(move |rows| (desc.clone(), rows))
        });
        let metrics = ColumnarMetrics::disconnected();

        proptest!(|((desc, rows) in strat)| {
            roundtrip_rows(&desc, rows, &metrics)
        })
    }

    #[mz_ore::test]
    fn empty_relation_desc_returns_error() {
        let empty_desc = RelationDesc::empty();
        let result = <RelationDesc as Schema<Row>>::encoder(&empty_desc);
        assert_err!(result);
    }

    #[mz_ore::test]
    fn smoketest_collections() {
        let mut row = Row::default();
        let mut packer = row.packer();
        let metrics = ColumnarMetrics::disconnected();

        packer
            .try_push_array(
                &[ArrayDimension {
                    lower_bound: 0,
                    length: 3,
                }],
                [Datum::UInt32(4), Datum::UInt32(5), Datum::UInt32(6)],
            )
            .unwrap();

        let array = row.unpack_first();
        roundtrip_datum(
            SqlScalarType::Array(Box::new(SqlScalarType::UInt32)).nullable(true),
            [array].into_iter(),
            &metrics,
        );
    }

    #[mz_ore::test]
    fn smoketest_row() {
        let desc = RelationDesc::builder()
            .with_column("a", SqlScalarType::Int64.nullable(true))
            .with_column("b", SqlScalarType::String.nullable(true))
            .with_column("c", SqlScalarType::Bool.nullable(true))
            .with_column(
                "d",
                SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::UInt32),
                    custom_id: None,
                }
                .nullable(true),
            )
            .with_column(
                "e",
                SqlScalarType::Map {
                    value_type: Box::new(SqlScalarType::Int16),
                    custom_id: None,
                }
                .nullable(true),
            )
            .finish();
        let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();

        let mut og_row = Row::default();
        {
            let mut packer = og_row.packer();
            packer.push(Datum::Int64(100));
            packer.push(Datum::String("hello world"));
            packer.push(Datum::True);
            packer.push_list([Datum::UInt32(1), Datum::UInt32(2), Datum::UInt32(3)]);
            packer.push_dict([("bar", Datum::Int16(9)), ("foo", Datum::Int16(3))]);
        }
        let mut og_row_2 = Row::default();
        {
            let mut packer = og_row_2.packer();
            packer.push(Datum::Null);
            packer.push(Datum::Null);
            packer.push(Datum::Null);
            packer.push(Datum::Null);
            packer.push(Datum::Null);
        }

        encoder.append(&og_row);
        encoder.append(&og_row_2);
        let col = encoder.finish();

        let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();

        let mut rnd_row = Row::default();
        decoder.decode(0, &mut rnd_row);
        assert_eq!(og_row, rnd_row);

        let mut rnd_row = Row::default();
        decoder.decode(1, &mut rnd_row);
        assert_eq!(og_row_2, rnd_row);
    }

    #[mz_ore::test]
    fn test_nested_list() {
        let desc = RelationDesc::builder()
            .with_column(
                "a",
                SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::List {
                        element_type: Box::new(SqlScalarType::Int64),
                        custom_id: None,
                    }),
                    custom_id: None,
                }
                .nullable(false),
            )
            .finish();
        let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();

        let mut og_row = Row::default();
        {
            let mut packer = og_row.packer();
            packer.push_list_with(|inner| {
                inner.push_list([Datum::Int64(1), Datum::Int64(2)]);
                inner.push_list([Datum::Int64(5)]);
                inner.push_list([Datum::Int64(9), Datum::Int64(99), Datum::Int64(999)]);
            });
        }

        encoder.append(&og_row);
        let col = encoder.finish();

        let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();
        let mut rnd_row = Row::default();
        decoder.decode(0, &mut rnd_row);

        assert_eq!(og_row, rnd_row);
    }

    #[mz_ore::test]
    fn test_record() {
        let desc = RelationDesc::builder()
            .with_column(
                "a",
                SqlScalarType::Record {
                    fields: [
                        (
                            ColumnName::from("foo"),
                            SqlScalarType::Int64.nullable(false),
                        ),
                        (
                            ColumnName::from("bar"),
                            SqlScalarType::String.nullable(true),
                        ),
                        (
                            ColumnName::from("baz"),
                            SqlScalarType::List {
                                element_type: Box::new(SqlScalarType::UInt32),
                                custom_id: None,
                            }
                            .nullable(false),
                        ),
                    ]
                    .into(),
                    custom_id: None,
                }
                .nullable(true),
            )
            .finish();
        let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();

        let mut og_row = Row::default();
        {
            let mut packer = og_row.packer();
            packer.push_list_with(|inner| {
                inner.push(Datum::Int64(42));
                inner.push(Datum::Null);
                inner.push_list([Datum::UInt32(1), Datum::UInt32(2), Datum::UInt32(3)]);
            });
        }
        let null_row = Row::pack_slice(&[Datum::Null]);

        encoder.append(&og_row);
        encoder.append(&null_row);
        let col = encoder.finish();

        let decoder = <RelationDesc as Schema<Row>>::decoder(&desc, col).unwrap();
        let mut rnd_row = Row::default();

        decoder.decode(0, &mut rnd_row);
        assert_eq!(og_row, rnd_row);

        rnd_row.packer();
        decoder.decode(1, &mut rnd_row);
        assert_eq!(null_row, rnd_row);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn roundtrip() {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.extend([
            Datum::False,
            Datum::True,
            Datum::Int16(1),
            Datum::Int32(2),
            Datum::Int64(3),
            Datum::Float32(4f32.into()),
            Datum::Float64(5f64.into()),
            Datum::Date(
                NaiveDate::from_ymd_opt(6, 7, 8)
                    .unwrap()
                    .try_into()
                    .unwrap(),
            ),
            Datum::Time(NaiveTime::from_hms_opt(9, 10, 11).unwrap()),
            Datum::Timestamp(
                CheckedTimestamp::from_timestamplike(
                    NaiveDate::from_ymd_opt(12, 13 % 12, 14)
                        .unwrap()
                        .and_time(NaiveTime::from_hms_opt(15, 16, 17).unwrap()),
                )
                .unwrap(),
            ),
            Datum::TimestampTz(
                CheckedTimestamp::from_timestamplike(DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(18, 19 % 12, 20)
                        .unwrap()
                        .and_time(NaiveTime::from_hms_opt(21, 22, 23).unwrap()),
                    Utc,
                ))
                .unwrap(),
            ),
            Datum::Interval(Interval {
                months: 24,
                days: 42,
                micros: 25,
            }),
            Datum::Bytes(&[26, 27]),
            Datum::String("28"),
            Datum::from(Numeric::from(29)),
            Datum::from(Numeric::infinity()),
            Datum::from(-Numeric::infinity()),
            Datum::from(Numeric::nan()),
            Datum::JsonNull,
            Datum::Uuid(Uuid::from_u128(30)),
            Datum::Dummy,
            Datum::Null,
        ]);
        packer
            .try_push_array(
                &[ArrayDimension {
                    lower_bound: 2,
                    length: 2,
                }],
                vec![Datum::Int32(31), Datum::Int32(32)],
            )
            .expect("valid array");
        packer.push_list_with(|packer| {
            packer.push(Datum::String("33"));
            packer.push_list_with(|packer| {
                packer.push(Datum::String("34"));
                packer.push(Datum::String("35"));
            });
            packer.push(Datum::String("36"));
            packer.push(Datum::String("37"));
        });
        packer.push_dict_with(|row| {
            // Add a bunch of data to the hash to ensure we don't get a
            // HashMap's random iteration anywhere in the encode/decode path.
            let mut i = 38;
            for _ in 0..20 {
                row.push(Datum::String(&i.to_string()));
                row.push(Datum::Int32(i + 1));
                i += 2;
            }
        });

        let mut desc = RelationDesc::builder();
        for (idx, _) in row.iter().enumerate() {
            // HACK(parkmycar): We don't currently validate the types of the `RelationDesc` are
            // correct, just the number of columns. So we can fill in any type here.
            desc = desc.with_column(idx.to_string(), SqlScalarType::Int32.nullable(true));
        }
        let desc = desc.finish();

        let encoded = row.encode_to_vec();
        assert_eq!(Row::decode(&encoded, &desc), Ok(row));
    }

    #[mz_ore::test]
    fn smoketest_projection() {
        let desc = RelationDesc::builder()
            .with_column("a", SqlScalarType::Int64.nullable(true))
            .with_column("b", SqlScalarType::String.nullable(true))
            .with_column("c", SqlScalarType::Bool.nullable(true))
            .finish();
        let mut encoder = <RelationDesc as Schema<Row>>::encoder(&desc).unwrap();

        let mut og_row = Row::default();
        {
            let mut packer = og_row.packer();
            packer.push(Datum::Int64(100));
            packer.push(Datum::String("hello world"));
            packer.push(Datum::True);
        }
        let mut og_row_2 = Row::default();
        {
            let mut packer = og_row_2.packer();
            packer.push(Datum::Null);
            packer.push(Datum::Null);
            packer.push(Datum::Null);
        }

        encoder.append(&og_row);
        encoder.append(&og_row_2);
        let col = encoder.finish();

        let projected_desc = desc.apply_demand(&BTreeSet::from([0, 2]));

        let decoder = <RelationDesc as Schema<Row>>::decoder(&projected_desc, col).unwrap();

        let mut rnd_row = Row::default();
        decoder.decode(0, &mut rnd_row);
        let expected_row = Row::pack_slice(&[Datum::Int64(100), Datum::True]);
        assert_eq!(expected_row, rnd_row);

        let mut rnd_row = Row::default();
        decoder.decode(1, &mut rnd_row);
        let expected_row = Row::pack_slice(&[Datum::Null, Datum::Null]);
        assert_eq!(expected_row, rnd_row);
    }
}
