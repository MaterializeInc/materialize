// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO(parkmycar): Remove this.
#![allow(clippy::as_conversions)]

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::AddAssign;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{
    Array, ArrayBuilder, BinaryArray, BinaryBuilder, BooleanArray, BooleanBufferBuilder,
    BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Array, Float32Builder,
    Float64Array, Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array,
    Int64Builder, ListArray, ListBuilder, MapArray, StringArray, StringBuilder, StructArray,
    UInt16Array, UInt16Builder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array,
    UInt8Builder,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use chrono::DateTime;
use dec::{Context, OrderedDecimal};
use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, FixedSizeCodec, Schema2};
use prost::Message;
use timely::Container;
use uuid::Uuid;

use crate::adt::array::PackedArrayDimension;
use crate::adt::date::Date;
use crate::adt::datetime::PackedNaiveTime;
use crate::adt::interval::PackedInterval;
use crate::adt::jsonb::{JsonbPacker, JsonbRef};
use crate::adt::mz_acl_item::{PackedAclItem, PackedMzAclItem};
use crate::adt::numeric::PackedNumeric;
use crate::adt::timestamp::CheckedTimestamp;
use crate::row::ProtoDatum;
use crate::{Datum, RelationDesc, Row, RowPacker, ScalarType, Timestamp};

// TODO(parkmycar): Benchmark the difference between `FixedSizeBinaryArray` and `BinaryArray`.
//
// `FixedSizeBinaryArray`s push empty bytes when a value is null which for larger binary types
// could result in poor performance.
const TIME_FIXED_BYTES: i32 = PackedNaiveTime::SIZE as i32;
const INTERVAL_FIXED_BYTES: i32 = PackedInterval::SIZE as i32;
const ACL_ITEM_FIXED_BYTES: i32 = PackedAclItem::SIZE as i32;
const _MZ_ACL_ITEM_FIXED_BYTES: i32 = PackedMzAclItem::SIZE as i32;
const ARRAY_DIMENSION_FIXED_BYTES: i32 = PackedArrayDimension::SIZE as i32;

const UUID_FIXED_BYTES: i32 = 16;
static_assertions::const_assert_eq!(UUID_FIXED_BYTES as usize, std::mem::size_of::<Uuid>());

/// An encoder for a column of [`Datum`]s.
#[derive(Debug)]
struct DatumEncoder {
    nullable: bool,
    encoder: DatumColumnEncoder,
}

impl DatumEncoder {
    fn push<'e, 'd>(&'e mut self, datum: Datum<'d>) -> Result<(), anyhow::Error> {
        assert!(
            !datum.is_null() || self.nullable,
            "tried pushing Null into non-nullable column"
        );
        self.encoder.push(datum);

        Ok(())
    }

    fn push_invalid(&mut self) {
        self.encoder.push_invalid()
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
    },
    Bytes(BinaryBuilder),
    String(StringBuilder),
    Date(Int32Builder),
    Time(FixedSizeBinaryBuilder),
    Timestamp(Int64Builder),
    TimestampTz(Int64Builder),
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
        buf: Cursor<Vec<u8>>,
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
        fields: Vec<Box<DatumColumnEncoder>>,
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
                },
                Datum::Numeric(val),
            ) => {
                let mut ctx = Context::default();
                let float_approx = ctx.try_into_f64(val.0).unwrap_or_else(|_| {
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
                let micros = val.and_utc().timestamp_micros();
                builder.append_value(micros);
            }
            (DatumColumnEncoder::TimestampTz(builder), Datum::TimestampTz(val)) => {
                let micros = val.timestamp_micros();
                builder.append_value(micros);
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
                JsonbRef::from_datum(d)
                    .to_writer(&mut buf)
                    .expect("failed to serialize Datum to jsonb");
                let offset: i32 = buf
                    .position()
                    .try_into()
                    .expect("wrote more than 4GB of JSON");
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
                for (datum, encoder) in records.into_iter().zip(fields.iter_mut()) {
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
                assert!(records.into_iter().next().is_none());
                builder.append_value(true);
            }
            (encoder, Datum::Null) => encoder.push_invalid(),
            (encoder, datum) => unimplemented!("can't encode {datum:?} into {encoder:?}"),
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

    fn finish(self) -> Arc<dyn Array> {
        match self {
            DatumColumnEncoder::Bool(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::U8(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::U16(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::U32(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::U64(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::I16(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::I32(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::I64(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::F32(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::F64(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Numeric {
                mut approx_values,
                mut binary_values,
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
            DatumColumnEncoder::String(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Bytes(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Date(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Time(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Timestamp(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::TimestampTz(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::MzTimestamp(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Interval(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Uuid(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::AclItem(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::MzAclItem(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Range(mut builder) => Arc::new(builder.finish()),
            DatumColumnEncoder::Jsonb {
                offsets,
                buf,
                mut nulls,
            } => {
                let values = Buffer::from_vec(buf.into_inner());
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
                let nulls = nulls.as_mut().map(|n| NullBuffer::from(n.finish()));

                Arc::new(StringArray::new(offsets, values, nulls))
            }
            DatumColumnEncoder::Array {
                mut dims,
                val_lengths,
                vals,
                mut nulls,
            } => {
                let nulls = nulls.as_mut().map(|n| NullBuffer::from(n.finish()));

                let vals = vals.finish();
                // Note: A value being null is managed by the null buffer we manually maintain.
                assert!(vals.logical_nulls().is_none());

                let field = Field::new_list_field(vals.data_type().clone(), nulls.is_some());
                let val_offsets = OffsetBuffer::from_lengths(val_lengths);
                let values =
                    ListArray::new(Arc::new(field), val_offsets, Arc::new(vals), nulls.clone());

                let dims = dims.finish();

                assert_eq!(values.len(), dims.len());
                assert_eq!(values.is_nullable(), dims.is_nullable());

                let fields = Fields::from(vec![
                    Field::new("dims", dims.data_type().clone(), dims.is_nullable()),
                    Field::new("vals", values.data_type().clone(), values.is_nullable()),
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
                // Note: A value being null is managed by the null buffer we manually maintain.
                assert!(values.logical_nulls().is_none());

                let field = Field::new_list_field(values.data_type().clone(), nulls.is_some());
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
                // Note: A value being null is managed by the null buffer we manually maintain.
                assert!(keys.logical_nulls().is_none());
                assert!(vals.logical_nulls().is_none());

                let offsets = OffsetBuffer::<i32>::from_lengths(lengths.iter().copied());
                let nulls = nulls.as_mut().map(|n| NullBuffer::from(n.finish()));

                let key_field =
                    Arc::new(Field::new("key", keys.data_type().clone(), nulls.is_some()));
                let val_field =
                    Arc::new(Field::new("val", vals.data_type().clone(), nulls.is_some()));
                let fields = Fields::from(vec![Arc::clone(&key_field), Arc::clone(&val_field)]);
                let entries = StructArray::new(fields, vec![Arc::new(keys), vals], None);

                // DatumMap is always sorted.
                let field = Field::new(
                    "map_entries",
                    entries.data_type().clone(),
                    entries.is_nullable(),
                );
                let array = MapArray::new(Arc::new(field), offsets, entries, nulls, true);
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
                        let array = encoder.finish();
                        let field = Field::new(
                            tag.to_string(),
                            array.data_type().clone(),
                            array.is_nullable(),
                        );

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
    Timestamp(Int64Array),
    TimestampTz(Int64Array),
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
                    let timestamp = DateTime::from_timestamp_micros(x)
                        .and_then(|dt| CheckedTimestamp::from_timestamplike(dt.naive_utc()).ok())
                        .expect("failed to roundtrip timestamp");
                    Datum::Timestamp(timestamp)
                })
            }
            DatumColumnDecoder::TimestampTz(array) => {
                array.is_valid(idx).then(|| array.value(idx)).map(|x| {
                    let timestamp = DateTime::from_timestamp_micros(x)
                        .and_then(|dt| CheckedTimestamp::from_timestamplike(dt).ok())
                        .expect("failed to roundtrip timestamptz");
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

                let start = dim_offsets[idx] as usize;
                let end = dim_offsets[idx + 1] as usize;
                let dimensions = (start..end).map(|idx| {
                    PackedArrayDimension::from_bytes(dims.value(idx))
                        .expect("failed to roundtrip ArrayDimension")
                        .into_value()
                });

                let start = val_offsets[idx] as usize;
                let end = val_offsets[idx + 1] as usize;
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

                let start = offsets[idx] as usize;
                let end = offsets[idx + 1] as usize;

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

                let start = offsets[idx] as usize;
                let end = offsets[idx + 1] as usize;

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
}

impl Schema2<Row> for RelationDesc {
    type ArrowColumn = arrow::array::StructArray;

    type Decoder = RowColumnarDecoder;
    type Encoder = RowColumnarEncoder;

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        RowColumnarDecoder::new(col, self)
    }

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(RowColumnarEncoder::new(self))
    }
}

/// A [`ColumnDecoder`] for a [`Row`].
#[derive(Debug)]
pub struct RowColumnarDecoder {
    decoders: Vec<DatumColumnDecoder>,
    nullability: Option<NullBuffer>,
}

impl RowColumnarDecoder {
    /// Creates a [`RowColumnarDecoder`] that decodes from the provided [`StructArray`] if the
    /// schema of the [`StructArray`] matches that of the provided [`RelationDesc`].
    pub fn new(col: StructArray, desc: &RelationDesc) -> Result<Self, anyhow::Error> {
        let inner_columns = col.columns();
        let desc_columns = desc.typ().columns();

        if inner_columns.len() != desc_columns.len() {
            anyhow::bail!(
                "provided array has {inner_columns:?}, relation desc has {desc_columns:?}"
            );
        }

        // For performance reasons we downcast just a single time.
        let mut decoders = Vec::with_capacity(desc_columns.len());
        for (col_array, col_type) in inner_columns.iter().zip(desc_columns.iter()) {
            let decoder = array_to_decoder(col_array, &col_type.scalar_type)?;
            decoders.push(decoder);
        }

        Ok(RowColumnarDecoder {
            decoders,
            nullability: col.logical_nulls(),
        })
    }
}

impl ColumnDecoder<Row> for RowColumnarDecoder {
    fn decode(&self, idx: usize, val: &mut Row) {
        let mut packer = val.packer();

        for decoder in &self.decoders {
            decoder.get(idx, &mut packer);
        }
    }

    fn is_null(&self, idx: usize) -> bool {
        let Some(nullability) = self.nullability.as_ref() else {
            return false;
        };
        nullability.is_null(idx)
    }
}

/// A [`ColumnEncoder`] for a [`Row`].
#[derive(Debug)]
pub struct RowColumnarEncoder {
    encoders: Vec<DatumEncoder>,
    col_names: Vec<Arc<str>>,
    // TODO(parkmycar): Optionally omit this.
    nullability: BooleanBufferBuilder,
}

impl RowColumnarEncoder {
    pub fn new(desc: &RelationDesc) -> Self {
        let encoders = desc
            .typ()
            .columns()
            .iter()
            .map(|column_type| {
                let encoder = scalar_type_to_encoder(&column_type.scalar_type)
                    .expect("failed to create encoder");
                DatumEncoder {
                    nullable: column_type.nullable,
                    encoder,
                }
            })
            .collect();
        let col_names = desc.iter_names().map(|name| name.as_str().into()).collect();

        RowColumnarEncoder {
            encoders,
            col_names,
            nullability: BooleanBufferBuilder::new(100),
        }
    }
}

impl ColumnEncoder<Row> for RowColumnarEncoder {
    type FinishedColumn = StructArray;

    fn append(&mut self, val: &Row) {
        for (datum, encoder) in val.iter().zip(self.encoders.iter_mut()) {
            encoder.push(datum).expect("failed to push datum");
        }
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
            .zip(encoders.into_iter())
            .map(|(name, encoder)| {
                let array = encoder.encoder.finish();
                let field = Field::new(name.as_ref(), array.data_type().clone(), encoder.nullable);
                (array, field)
            })
            .unzip();
        let null_buffer = NullBuffer::from(BooleanBuffer::from(nullability));

        StructArray::new(Fields::from(fields), arrays, Some(null_buffer))
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

/// Small helper function to downcast from an array to a [`DatumDecoder`].
///
/// Note: it is _super_ important that we downcast as few times as possible. Datum encoding is a
/// very hot path and downcasting is relatively slow
fn array_to_decoder(
    array: &Arc<dyn Array>,
    col_ty: &ScalarType,
) -> Result<DatumColumnDecoder, anyhow::Error> {
    let decoder = match (array.data_type(), col_ty) {
        (DataType::Boolean, ScalarType::Bool) => {
            let array = downcast_array::<BooleanArray>(array)?;
            DatumColumnDecoder::Bool(array.clone())
        }
        (DataType::UInt8, ScalarType::PgLegacyChar) => {
            let array = downcast_array::<UInt8Array>(array)?;
            DatumColumnDecoder::U8(array.clone())
        }
        (DataType::UInt16, ScalarType::UInt16) => {
            let array = downcast_array::<UInt16Array>(array)?;
            DatumColumnDecoder::U16(array.clone())
        }
        (
            DataType::UInt32,
            ScalarType::UInt32
            | ScalarType::Oid
            | ScalarType::RegClass
            | ScalarType::RegProc
            | ScalarType::RegType,
        ) => {
            let array = downcast_array::<UInt32Array>(array)?;
            DatumColumnDecoder::U32(array.clone())
        }
        (DataType::UInt64, ScalarType::UInt64) => {
            let array = downcast_array::<UInt64Array>(array)?;
            DatumColumnDecoder::U64(array.clone())
        }
        (DataType::Int16, ScalarType::Int16) => {
            let array = downcast_array::<Int16Array>(array)?;
            DatumColumnDecoder::I16(array.clone())
        }
        (DataType::Int32, ScalarType::Int32) => {
            let array = downcast_array::<Int32Array>(array)?;
            DatumColumnDecoder::I32(array.clone())
        }
        (DataType::Int64, ScalarType::Int64) => {
            let array = downcast_array::<Int64Array>(array)?;
            DatumColumnDecoder::I64(array.clone())
        }
        (DataType::Float32, ScalarType::Float32) => {
            let array = downcast_array::<Float32Array>(array)?;
            DatumColumnDecoder::F32(array.clone())
        }
        (DataType::Float64, ScalarType::Float64) => {
            let array = downcast_array::<Float64Array>(array)?;
            DatumColumnDecoder::F64(array.clone())
        }
        (DataType::Struct(_), ScalarType::Numeric { .. }) => {
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
            ScalarType::String
            | ScalarType::PgLegacyName
            | ScalarType::Char { .. }
            | ScalarType::VarChar { .. },
        ) => {
            let array = downcast_array::<StringArray>(array)?;
            DatumColumnDecoder::String(array.clone())
        }
        (DataType::Binary, ScalarType::Bytes) => {
            let array = downcast_array::<BinaryArray>(array)?;
            DatumColumnDecoder::Bytes(array.clone())
        }
        (DataType::Int32, ScalarType::Date) => {
            let array = downcast_array::<Int32Array>(array)?;
            DatumColumnDecoder::Date(array.clone())
        }
        (DataType::FixedSizeBinary(TIME_FIXED_BYTES), ScalarType::Time) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::Time(array.clone())
        }
        (DataType::Int64, ScalarType::Timestamp { .. }) => {
            let array = downcast_array::<Int64Array>(array)?;
            DatumColumnDecoder::Timestamp(array.clone())
        }
        (DataType::Int64, ScalarType::TimestampTz { .. }) => {
            let array = downcast_array::<Int64Array>(array)?;
            DatumColumnDecoder::TimestampTz(array.clone())
        }
        (DataType::UInt64, ScalarType::MzTimestamp) => {
            let array = downcast_array::<UInt64Array>(array)?;
            DatumColumnDecoder::MzTimestamp(array.clone())
        }
        (DataType::FixedSizeBinary(INTERVAL_FIXED_BYTES), ScalarType::Interval) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::Interval(array.clone())
        }
        (DataType::FixedSizeBinary(UUID_FIXED_BYTES), ScalarType::Uuid) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::Uuid(array.clone())
        }
        (DataType::FixedSizeBinary(ACL_ITEM_FIXED_BYTES), ScalarType::AclItem) => {
            let array = downcast_array::<FixedSizeBinaryArray>(array)?;
            DatumColumnDecoder::AclItem(array.clone())
        }
        (DataType::Binary, ScalarType::MzAclItem) => {
            let array = downcast_array::<BinaryArray>(array)?;
            DatumColumnDecoder::MzAclItem(array.clone())
        }
        (DataType::Binary, ScalarType::Range { .. }) => {
            let array = downcast_array::<BinaryArray>(array)?;
            DatumColumnDecoder::Range(array.clone())
        }
        (DataType::Utf8, ScalarType::Jsonb) => {
            let array = downcast_array::<StringArray>(array)?;
            DatumColumnDecoder::Json(array.clone())
        }
        (DataType::Struct(_), s @ ScalarType::Array(_) | s @ ScalarType::Int2Vector) => {
            let element_type = match s {
                ScalarType::Array(inner) => inner,
                ScalarType::Int2Vector => &ScalarType::Int16,
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
        (DataType::List(_), ScalarType::List { element_type, .. }) => {
            let array = downcast_array::<ListArray>(array)?;
            let inner_decoder = array_to_decoder(array.values(), &*element_type)?;
            DatumColumnDecoder::List {
                offsets: array.offsets().clone(),
                values: Box::new(inner_decoder),
                nulls: array.nulls().cloned(),
            }
        }
        (DataType::Map(_, true), ScalarType::Map { value_type, .. }) => {
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
        (DataType::Boolean, ScalarType::Record { fields, .. }) if fields.is_empty() => {
            let empty_record_array = downcast_array::<BooleanArray>(array)?;
            DatumColumnDecoder::RecordEmpty(empty_record_array.clone())
        }
        (DataType::Struct(_), ScalarType::Record { fields, .. }) => {
            let record_array = downcast_array::<StructArray>(array)?;

            let mut decoders = Vec::with_capacity(fields.len());
            for (tag, (_name, col_type)) in fields.iter().enumerate() {
                let inner_array = record_array.column_by_name(&tag.to_string()).expect("TODO");
                let inner_decoder = array_to_decoder(inner_array, &col_type.scalar_type)?;

                decoders.push(Box::new(inner_decoder));
            }

            DatumColumnDecoder::Record {
                fields: decoders,
                nulls: record_array.nulls().cloned(),
            }
        }
        (x, y) => unimplemented!("column type {x:?}, datum type {y:?}"),
    };

    Ok(decoder)
}

/// Small helper function to create a [`DatumColumnEncoder`] from a [`ScalarType`]
fn scalar_type_to_encoder(col_ty: &ScalarType) -> Result<DatumColumnEncoder, anyhow::Error> {
    let encoder = match &col_ty {
        ScalarType::Bool => DatumColumnEncoder::Bool(BooleanBuilder::new()),
        ScalarType::PgLegacyChar => DatumColumnEncoder::U8(UInt8Builder::new()),
        ScalarType::UInt16 => DatumColumnEncoder::U16(UInt16Builder::new()),
        ScalarType::UInt32
        | ScalarType::Oid
        | ScalarType::RegClass
        | ScalarType::RegProc
        | ScalarType::RegType => DatumColumnEncoder::U32(UInt32Builder::new()),
        ScalarType::UInt64 => DatumColumnEncoder::U64(UInt64Builder::new()),
        ScalarType::Int16 => DatumColumnEncoder::I16(Int16Builder::new()),
        ScalarType::Int32 => DatumColumnEncoder::I32(Int32Builder::new()),
        ScalarType::Int64 => DatumColumnEncoder::I64(Int64Builder::new()),
        ScalarType::Float32 => DatumColumnEncoder::F32(Float32Builder::new()),
        ScalarType::Float64 => DatumColumnEncoder::F64(Float64Builder::new()),
        ScalarType::Numeric { .. } => DatumColumnEncoder::Numeric {
            approx_values: Float64Builder::new(),
            binary_values: BinaryBuilder::new(),
        },
        ScalarType::String
        | ScalarType::PgLegacyName
        | ScalarType::Char { .. }
        | ScalarType::VarChar { .. } => DatumColumnEncoder::String(StringBuilder::new()),
        ScalarType::Bytes => DatumColumnEncoder::Bytes(BinaryBuilder::new()),
        ScalarType::Date => DatumColumnEncoder::Date(Int32Builder::new()),
        ScalarType::Time => DatumColumnEncoder::Time(FixedSizeBinaryBuilder::new(TIME_FIXED_BYTES)),
        ScalarType::Timestamp { .. } => DatumColumnEncoder::Timestamp(Int64Builder::new()),
        ScalarType::TimestampTz { .. } => DatumColumnEncoder::TimestampTz(Int64Builder::new()),
        ScalarType::MzTimestamp => DatumColumnEncoder::MzTimestamp(UInt64Builder::new()),
        ScalarType::Interval => {
            DatumColumnEncoder::Interval(FixedSizeBinaryBuilder::new(INTERVAL_FIXED_BYTES))
        }
        ScalarType::Uuid => DatumColumnEncoder::Uuid(FixedSizeBinaryBuilder::new(UUID_FIXED_BYTES)),
        ScalarType::AclItem => {
            DatumColumnEncoder::AclItem(FixedSizeBinaryBuilder::new(ACL_ITEM_FIXED_BYTES))
        }
        ScalarType::MzAclItem => DatumColumnEncoder::MzAclItem(BinaryBuilder::new()),
        ScalarType::Range { .. } => DatumColumnEncoder::Range(BinaryBuilder::new()),
        ScalarType::Jsonb => DatumColumnEncoder::Jsonb {
            offsets: vec![0],
            buf: Cursor::new(Vec::new()),
            nulls: None,
        },
        s @ ScalarType::Array(_) | s @ ScalarType::Int2Vector => {
            let element_type = match s {
                ScalarType::Array(inner) => inner,
                ScalarType::Int2Vector => &ScalarType::Int16,
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
        ScalarType::List { element_type, .. } => {
            let inner = scalar_type_to_encoder(&*element_type)?;
            DatumColumnEncoder::List {
                lengths: Vec::new(),
                values: Box::new(inner),
                nulls: None,
            }
        }
        ScalarType::Map { value_type, .. } => {
            let inner = scalar_type_to_encoder(&*value_type)?;
            DatumColumnEncoder::Map {
                lengths: Vec::new(),
                keys: StringBuilder::new(),
                vals: Box::new(inner),
                nulls: None,
            }
        }
        ScalarType::Record { fields, .. } if fields.is_empty() => {
            DatumColumnEncoder::RecordEmpty(BooleanBuilder::new())
        }
        ScalarType::Record { fields, .. } => {
            let encoders = fields
                .iter()
                .map(|(_name, ty)| scalar_type_to_encoder(&ty.scalar_type).map(Box::new))
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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::strategy::Strategy;

    use super::*;
    use crate::adt::array::ArrayDimension;
    use crate::{arb_datum_for_column, ColumnName, ColumnType};

    #[track_caller]
    fn roundtrip_datum<'a>(ty: ColumnType, datums: impl Iterator<Item = Datum<'a>>) {
        let desc = RelationDesc::empty().with_column("a", ty);
        let mut encoder = <RelationDesc as Schema2<Row>>::encoder(&desc).unwrap();

        let mut row = Row::default();
        let datums: Vec<_> = datums.collect();

        for d in &datums {
            row.packer().push(d);
            encoder.append(&row);
        }
        let col = encoder.finish();

        let decoder = <RelationDesc as Schema2<Row>>::decoder(&desc, col).unwrap();

        for (idx, datum) in datums.into_iter().enumerate() {
            // Creating a packer clears the row.
            row.packer();

            decoder.decode(idx, &mut row);
            let rnd_datum = row.unpack_first();

            assert_eq!(datum, rnd_datum);
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn proptest_datums() {
        let strat = any::<ColumnType>().prop_flat_map(|ty| {
            proptest::collection::vec(arb_datum_for_column(&ty), 0..16)
                .prop_map(move |d| (ty.clone(), d))
        });

        proptest!(|((ty, datums) in strat)| {
            roundtrip_datum(ty.clone(), datums.iter().map(Datum::from));
        })
    }

    #[mz_ore::test]
    fn smoketest_collections() {
        let mut row = Row::default();
        let mut packer = row.packer();

        packer
            .push_array(
                &[ArrayDimension {
                    lower_bound: 0,
                    length: 3,
                }],
                [Datum::UInt32(4), Datum::UInt32(5), Datum::UInt32(6)],
            )
            .unwrap();

        let array = row.unpack_first();
        roundtrip_datum(
            ScalarType::Array(Box::new(ScalarType::UInt32)).nullable(true),
            [array].into_iter(),
        );
    }

    #[mz_ore::test]
    fn smoketest_row() {
        let desc = RelationDesc::empty()
            .with_column("a", ScalarType::Int64.nullable(true))
            .with_column("b", ScalarType::String.nullable(true))
            .with_column("c", ScalarType::Bool.nullable(true))
            .with_column(
                "d",
                ScalarType::List {
                    element_type: Box::new(ScalarType::UInt32),
                    custom_id: None,
                }
                .nullable(true),
            )
            .with_column(
                "e",
                ScalarType::Map {
                    value_type: Box::new(ScalarType::Int16),
                    custom_id: None,
                }
                .nullable(true),
            );
        let mut encoder = <RelationDesc as Schema2<Row>>::encoder(&desc).unwrap();

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

        let decoder = <RelationDesc as Schema2<Row>>::decoder(&desc, col).unwrap();

        let mut rnd_row = Row::default();
        decoder.decode(0, &mut rnd_row);
        assert_eq!(og_row, rnd_row);

        let mut rnd_row = Row::default();
        decoder.decode(1, &mut rnd_row);
        assert_eq!(og_row_2, rnd_row);
    }

    #[mz_ore::test]
    fn test_nested_list() {
        let desc = RelationDesc::empty().with_column(
            "a",
            ScalarType::List {
                element_type: Box::new(ScalarType::List {
                    element_type: Box::new(ScalarType::Int64),
                    custom_id: None,
                }),
                custom_id: None,
            }
            .nullable(false),
        );
        let mut encoder = <RelationDesc as Schema2<Row>>::encoder(&desc).unwrap();

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

        let decoder = <RelationDesc as Schema2<Row>>::decoder(&desc, col).unwrap();
        let mut rnd_row = Row::default();
        decoder.decode(0, &mut rnd_row);

        assert_eq!(og_row, rnd_row);
    }

    #[mz_ore::test]
    fn test_record() {
        let desc = RelationDesc::empty().with_column(
            "a",
            ScalarType::Record {
                fields: vec![
                    (ColumnName::from("foo"), ScalarType::Int64.nullable(true)),
                    (ColumnName::from("bar"), ScalarType::String.nullable(false)),
                    (
                        ColumnName::from("baz"),
                        ScalarType::List {
                            element_type: Box::new(ScalarType::UInt32),
                            custom_id: None,
                        }
                        .nullable(false),
                    ),
                ],
                custom_id: None,
            }
            .nullable(true),
        );
        let mut encoder = <RelationDesc as Schema2<Row>>::encoder(&desc).unwrap();

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

        let decoder = <RelationDesc as Schema2<Row>>::decoder(&desc, col).unwrap();
        let mut rnd_row = Row::default();

        decoder.decode(0, &mut rnd_row);
        assert_eq!(og_row, rnd_row);

        rnd_row.packer();
        decoder.decode(1, &mut rnd_row);
        assert_eq!(null_row, rnd_row);
    }
}
