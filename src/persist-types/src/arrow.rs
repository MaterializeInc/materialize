// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [protobuf] representation of [Apache Arrow] arrays.
//!
//! # Motivation
//!
//! Persist can store a small amount of data inline at the consensus layer.
//! Because we are space constrained, we take particular care to store only the
//! data that is necessary. Other Arrow serialization formats, e.g. [Parquet]
//! or [Arrow IPC], include data that we don't need and would be wasteful to
//! store.
//!
//! [protobuf]: https://protobuf.dev/
//! [Apache Arrow]: https://arrow.apache.org/
//! [Parquet]: https://parquet.apache.org/docs/
//! [Arrow IPC]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc

use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef, Fields};
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::soft_assert_eq_no_log;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use prost::Message;

#[allow(missing_docs)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/mz_persist_types.arrow.rs"));
}
use crate::arrow::proto::data_type;
pub use proto::{DataType as ProtoDataType, ProtoArrayData};

/// Extract the list of fields for our recursive datatypes.
pub fn fields_for_type(data_type: &DataType) -> &[FieldRef] {
    match data_type {
        DataType::Struct(fields) => fields,
        DataType::List(field) => std::slice::from_ref(field),
        DataType::Map(field, _) => std::slice::from_ref(field),
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => &[],
        DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _) => unimplemented!("not supported"),
    }
}

/// Encode the array into proto. If an expected data type is passed, that implies it is
/// encoded at some higher level, and we omit it from the data.
fn into_proto_with_type(data: &ArrayData, expected_type: Option<&DataType>) -> ProtoArrayData {
    let data_type = match expected_type {
        Some(expected) => {
            // Equality is recursive, and this function is itself called recursively,
            // skip the call in production to avoid a quadratic overhead.
            soft_assert_eq_no_log!(
                expected,
                data.data_type(),
                "actual type should match expected type"
            );
            None
        }
        None => Some(data.data_type().into_proto()),
    };

    ProtoArrayData {
        data_type,
        length: u64::cast_from(data.len()),
        offset: u64::cast_from(data.offset()),
        buffers: data.buffers().iter().map(|b| b.into_proto()).collect(),
        children: data
            .child_data()
            .iter()
            .zip_eq(fields_for_type(
                expected_type.unwrap_or_else(|| data.data_type()),
            ))
            .map(|(child, expect)| into_proto_with_type(child, Some(expect.data_type())))
            .collect(),
        nulls: data.nulls().map(|n| n.inner().into_proto()),
    }
}

/// Decode the array data.
/// If the data type is omitted from the proto, we decode it as the expected type.
fn from_proto_with_type(
    proto: ProtoArrayData,
    expected_type: Option<&DataType>,
) -> Result<ArrayData, TryFromProtoError> {
    let ProtoArrayData {
        data_type,
        length,
        offset,
        buffers,
        children,
        nulls,
    } = proto;
    let data_type: Option<DataType> = data_type.into_rust()?;
    let data_type = match (data_type, expected_type) {
        (Some(data_type), None) => data_type,
        (Some(data_type), Some(expected_type)) => {
            // Equality is recursive, and this function is itself called recursively,
            // skip the call in production to avoid a quadratic overhead.
            soft_assert_eq_no_log!(
                data_type,
                *expected_type,
                "expected type should match actual type"
            );
            data_type
        }
        (None, Some(expected_type)) => expected_type.clone(),
        (None, None) => {
            return Err(TryFromProtoError::MissingField(
                "ProtoArrayData::data_type".to_string(),
            ));
        }
    };
    let nulls = nulls
        .map(|n| n.into_rust())
        .transpose()?
        .map(NullBuffer::new);

    let mut builder = ArrayDataBuilder::new(data_type.clone())
        .len(usize::cast_from(length))
        .offset(usize::cast_from(offset))
        .nulls(nulls);

    for b in buffers.into_iter().map(|b| b.into_rust()) {
        builder = builder.add_buffer(b?);
    }
    for c in children
        .into_iter()
        .zip_eq(fields_for_type(&data_type))
        .map(|(c, field)| from_proto_with_type(c, Some(field.data_type())))
    {
        builder = builder.add_child_data(c?);
    }

    // Construct the builder which validates all inputs and aligns data.
    builder
        .align_buffers(true)
        .build()
        .map_err(|e| TryFromProtoError::RowConversionError(e.to_string()))
}

impl RustType<ProtoArrayData> for arrow::array::ArrayData {
    fn into_proto(&self) -> ProtoArrayData {
        into_proto_with_type(self, None)
    }

    fn from_proto(proto: ProtoArrayData) -> Result<Self, TryFromProtoError> {
        from_proto_with_type(proto, None)
    }
}

impl RustType<proto::DataType> for arrow::datatypes::DataType {
    fn into_proto(&self) -> proto::DataType {
        let kind = match self {
            DataType::Null => proto::data_type::Kind::Null(()),
            DataType::Boolean => proto::data_type::Kind::Boolean(()),
            DataType::UInt8 => proto::data_type::Kind::Uint8(()),
            DataType::UInt16 => proto::data_type::Kind::Uint16(()),
            DataType::UInt32 => proto::data_type::Kind::Uint32(()),
            DataType::UInt64 => proto::data_type::Kind::Uint64(()),
            DataType::Int8 => proto::data_type::Kind::Int8(()),
            DataType::Int16 => proto::data_type::Kind::Int16(()),
            DataType::Int32 => proto::data_type::Kind::Int32(()),
            DataType::Int64 => proto::data_type::Kind::Int64(()),
            DataType::Float32 => proto::data_type::Kind::Float32(()),
            DataType::Float64 => proto::data_type::Kind::Float64(()),
            DataType::Utf8 => proto::data_type::Kind::String(()),
            DataType::Binary => proto::data_type::Kind::Binary(()),
            DataType::FixedSizeBinary(size) => proto::data_type::Kind::FixedBinary(*size),
            DataType::List(inner) => proto::data_type::Kind::List(Box::new(inner.into_proto())),
            DataType::Map(inner, sorted) => {
                let map = proto::data_type::Map {
                    value: Some(Box::new(inner.into_proto())),
                    sorted: *sorted,
                };
                proto::data_type::Kind::Map(Box::new(map))
            }
            DataType::Struct(children) => {
                let children = children.into_iter().map(|f| f.into_proto()).collect();
                proto::data_type::Kind::Struct(proto::data_type::Struct { children })
            }
            other => unimplemented!("unsupported data type {other:?}"),
        };

        proto::DataType { kind: Some(kind) }
    }

    fn from_proto(proto: proto::DataType) -> Result<Self, TryFromProtoError> {
        let data_type = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("kind"))?;
        let data_type = match data_type {
            proto::data_type::Kind::Null(()) => DataType::Null,
            proto::data_type::Kind::Boolean(()) => DataType::Boolean,
            proto::data_type::Kind::Uint8(()) => DataType::UInt8,
            proto::data_type::Kind::Uint16(()) => DataType::UInt16,
            proto::data_type::Kind::Uint32(()) => DataType::UInt32,
            proto::data_type::Kind::Uint64(()) => DataType::UInt64,
            proto::data_type::Kind::Int8(()) => DataType::Int8,
            proto::data_type::Kind::Int16(()) => DataType::Int16,
            proto::data_type::Kind::Int32(()) => DataType::Int32,
            proto::data_type::Kind::Int64(()) => DataType::Int64,
            proto::data_type::Kind::Float32(()) => DataType::Float32,
            proto::data_type::Kind::Float64(()) => DataType::Float64,
            proto::data_type::Kind::String(()) => DataType::Utf8,
            proto::data_type::Kind::Binary(()) => DataType::Binary,
            proto::data_type::Kind::FixedBinary(size) => DataType::FixedSizeBinary(size),
            proto::data_type::Kind::List(inner) => DataType::List(Arc::new((*inner).into_rust()?)),
            proto::data_type::Kind::Map(inner) => {
                let value = inner
                    .value
                    .ok_or_else(|| TryFromProtoError::missing_field("map.value"))?;
                DataType::Map(Arc::new((*value).into_rust()?), inner.sorted)
            }
            proto::data_type::Kind::Struct(inner) => {
                let children: Vec<Field> = inner
                    .children
                    .into_iter()
                    .map(|c| c.into_rust())
                    .collect::<Result<_, _>>()?;
                DataType::Struct(Fields::from(children))
            }
        };

        Ok(data_type)
    }
}

impl RustType<proto::Field> for arrow::datatypes::Field {
    fn into_proto(&self) -> proto::Field {
        proto::Field {
            name: self.name().clone(),
            nullable: self.is_nullable(),
            data_type: Some(Box::new(self.data_type().into_proto())),
        }
    }

    fn from_proto(proto: proto::Field) -> Result<Self, TryFromProtoError> {
        let proto::Field {
            name,
            nullable,
            data_type,
        } = proto;
        let data_type =
            data_type.ok_or_else(|| TryFromProtoError::missing_field("field.data_type"))?;
        let data_type = (*data_type).into_rust()?;

        Ok(Field::new(name, data_type, nullable))
    }
}

impl RustType<proto::Buffer> for arrow::buffer::Buffer {
    fn into_proto(&self) -> proto::Buffer {
        // Wrapping since arrow's buffer doesn't implement AsRef, though the deref impl exists.
        #[repr(transparent)]
        struct BufferWrapper(arrow::buffer::Buffer);
        impl AsRef<[u8]> for BufferWrapper {
            fn as_ref(&self) -> &[u8] {
                &*self.0
            }
        }
        proto::Buffer {
            data: bytes::Bytes::from_owner(BufferWrapper(self.clone())),
        }
    }

    fn from_proto(proto: proto::Buffer) -> Result<Self, TryFromProtoError> {
        Ok(arrow::buffer::Buffer::from(proto.data))
    }
}

impl RustType<proto::BooleanBuffer> for arrow::buffer::BooleanBuffer {
    fn into_proto(&self) -> proto::BooleanBuffer {
        proto::BooleanBuffer {
            buffer: Some(self.sliced().into_proto()),
            length: u64::cast_from(self.len()),
        }
    }

    fn from_proto(proto: proto::BooleanBuffer) -> Result<Self, TryFromProtoError> {
        let proto::BooleanBuffer { buffer, length } = proto;
        let buffer = buffer.into_rust_if_some("buffer")?;
        Ok(BooleanBuffer::new(buffer, 0, usize::cast_from(length)))
    }
}

/// Wraps a single arrow array, downcasted to a specific type.
#[derive(Clone)]
pub enum ArrayOrd {
    /// Wraps a `NullArray`.
    Null(NullArray),
    /// Wraps a `Bool` array.
    Bool(BooleanArray),
    /// Wraps a `Int8` array.
    Int8(Int8Array),
    /// Wraps a `Int16` array.
    Int16(Int16Array),
    /// Wraps a `Int32` array.
    Int32(Int32Array),
    /// Wraps a `Int64` array.
    Int64(Int64Array),
    /// Wraps a `UInt8` array.
    UInt8(UInt8Array),
    /// Wraps a `UInt16` array.
    UInt16(UInt16Array),
    /// Wraps a `UInt32` array.
    UInt32(UInt32Array),
    /// Wraps a `UInt64` array.
    UInt64(UInt64Array),
    /// Wraps a `Float32` array.
    Float32(Float32Array),
    /// Wraps a `Float64` array.
    Float64(Float64Array),
    /// Wraps a `String` array.
    String(StringArray),
    /// Wraps a `Binary` array.
    Binary(BinaryArray),
    /// Wraps a `FixedSizeBinary` array.
    FixedSizeBinary(FixedSizeBinaryArray),
    /// Wraps a `List` array.
    List(Option<NullBuffer>, OffsetBuffer<i32>, Box<ArrayOrd>),
    /// Wraps a `Struct` array.
    Struct(Option<NullBuffer>, Vec<ArrayOrd>),
}

impl ArrayOrd {
    /// Downcast the provided array to a specific type in our enum.
    pub fn new(array: &dyn Array) -> Self {
        match array.data_type() {
            DataType::Null => ArrayOrd::Null(NullArray::from(array.to_data())),
            DataType::Boolean => ArrayOrd::Bool(array.as_boolean().clone()),
            DataType::Int8 => ArrayOrd::Int8(array.as_primitive().clone()),
            DataType::Int16 => ArrayOrd::Int16(array.as_primitive().clone()),
            DataType::Int32 => ArrayOrd::Int32(array.as_primitive().clone()),
            DataType::Int64 => ArrayOrd::Int64(array.as_primitive().clone()),
            DataType::UInt8 => ArrayOrd::UInt8(array.as_primitive().clone()),
            DataType::UInt16 => ArrayOrd::UInt16(array.as_primitive().clone()),
            DataType::UInt32 => ArrayOrd::UInt32(array.as_primitive().clone()),
            DataType::UInt64 => ArrayOrd::UInt64(array.as_primitive().clone()),
            DataType::Float32 => ArrayOrd::Float32(array.as_primitive().clone()),
            DataType::Float64 => ArrayOrd::Float64(array.as_primitive().clone()),
            DataType::Binary => ArrayOrd::Binary(array.as_binary().clone()),
            DataType::Utf8 => ArrayOrd::String(array.as_string().clone()),
            DataType::FixedSizeBinary(_) => {
                ArrayOrd::FixedSizeBinary(array.as_fixed_size_binary().clone())
            }
            DataType::List(_) => {
                let list_array = array.as_list();
                ArrayOrd::List(
                    list_array.nulls().cloned(),
                    list_array.offsets().clone(),
                    Box::new(ArrayOrd::new(list_array.values())),
                )
            }
            DataType::Struct(_) => {
                let struct_array = array.as_struct();
                let nulls = array.nulls().cloned();
                let columns: Vec<_> = struct_array
                    .columns()
                    .iter()
                    .map(|a| ArrayOrd::new(a))
                    .collect();
                ArrayOrd::Struct(nulls, columns)
            }
            data_type => unimplemented!("array type {data_type:?} not yet supported"),
        }
    }

    /// Returns the rough amount of space required for the data in this array in bytes.
    /// (Not counting nulls, dictionary encoding, or other space optimizations.)
    pub fn goodbytes(&self) -> usize {
        match self {
            ArrayOrd::Null(_) => 0,
            // This is, strictly speaking, wrong - but consistent with `ArrayIdx::goodbytes`,
            // which counts one byte per bool.
            ArrayOrd::Bool(b) => b.len(),
            ArrayOrd::Int8(a) => a.values().inner().len(),
            ArrayOrd::Int16(a) => a.values().inner().len(),
            ArrayOrd::Int32(a) => a.values().inner().len(),
            ArrayOrd::Int64(a) => a.values().inner().len(),
            ArrayOrd::UInt8(a) => a.values().inner().len(),
            ArrayOrd::UInt16(a) => a.values().inner().len(),
            ArrayOrd::UInt32(a) => a.values().inner().len(),
            ArrayOrd::UInt64(a) => a.values().inner().len(),
            ArrayOrd::Float32(a) => a.values().inner().len(),
            ArrayOrd::Float64(a) => a.values().inner().len(),
            ArrayOrd::String(a) => a.values().len(),
            ArrayOrd::Binary(a) => a.values().len(),
            ArrayOrd::FixedSizeBinary(a) => a.values().len(),
            ArrayOrd::List(_, _, nested) => nested.goodbytes(),
            ArrayOrd::Struct(_, nested) => nested.iter().map(|a| a.goodbytes()).sum(),
        }
    }

    /// Return a struct representing the value at a particular index in this array.
    pub fn at(&self, idx: usize) -> ArrayIdx<'_> {
        ArrayIdx { idx, array: self }
    }
}

impl Debug for ArrayOrd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct DebugType<'a>(&'a ArrayOrd);

        impl Debug for DebugType<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self.0 {
                    ArrayOrd::Null(_) => write!(f, "Null"),
                    ArrayOrd::Bool(_) => write!(f, "Bool"),
                    ArrayOrd::Int8(_) => write!(f, "Int8"),
                    ArrayOrd::Int16(_) => write!(f, "Int16"),
                    ArrayOrd::Int32(_) => write!(f, "Int32"),
                    ArrayOrd::Int64(_) => write!(f, "Int64"),
                    ArrayOrd::UInt8(_) => write!(f, "UInt8"),
                    ArrayOrd::UInt16(_) => write!(f, "UInt16"),
                    ArrayOrd::UInt32(_) => write!(f, "UInt32"),
                    ArrayOrd::UInt64(_) => write!(f, "UInt64"),
                    ArrayOrd::Float32(_) => write!(f, "Float32"),
                    ArrayOrd::Float64(_) => write!(f, "Float64"),
                    ArrayOrd::String(_) => write!(f, "String"),
                    ArrayOrd::Binary(_) => write!(f, "Binary"),
                    ArrayOrd::FixedSizeBinary(a) => f
                        .debug_tuple("FixedSizeBinary")
                        .field(&a.value_length())
                        .finish(),
                    ArrayOrd::List(_, _, nested) => f.debug_tuple("List").field(&*nested).finish(),
                    ArrayOrd::Struct(_, fields) => {
                        let mut tuple = f.debug_tuple("Struct");
                        for field in fields {
                            tuple.field(field);
                        }
                        tuple.finish()
                    }
                }
            }
        }

        f.debug_struct("ArrayOrd")
            .field("type", &DebugType(self))
            .field("goodbytes", &self.goodbytes())
            .finish()
    }
}

/// A struct representing a particular entry in a particular array. Most useful for its `Ord`
/// implementation, which can compare entire rows across similarly-typed arrays.
///
/// It is an error to compare indices from arrays with different types, with one exception:
/// it is valid to compare two `StructArray`s, one of which is a prefix of the other...
/// in which case we'll compare the values on that subset of the fields, and the shorter
/// of the two structs will compare less if they're otherwise equal.
#[derive(Clone, Copy, Debug)]
pub struct ArrayIdx<'a> {
    /// An index into a particular array.
    pub idx: usize,
    /// The particular array.
    pub array: &'a ArrayOrd,
}

impl Display for ArrayIdx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.array {
            ArrayOrd::Null(_) => write!(f, "null"),
            ArrayOrd::Bool(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::Int8(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::Int16(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::Int32(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::Int64(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::UInt8(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::UInt16(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::UInt32(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::UInt64(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::Float32(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::Float64(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::String(a) => write!(f, "{}", a.value(self.idx)),
            ArrayOrd::Binary(a) => {
                for byte in a.value(self.idx) {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
            ArrayOrd::FixedSizeBinary(a) => {
                for byte in a.value(self.idx) {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
            ArrayOrd::List(_, offsets, nested) => {
                write!(
                    f,
                    "[{}]",
                    mz_ore::str::separated(", ", list_range(offsets, nested, self.idx))
                )
            }
            ArrayOrd::Struct(_, nested) => write!(
                f,
                "{{{}}}",
                mz_ore::str::separated(", ", nested.iter().map(|f| f.at(self.idx)))
            ),
        }
    }
}

#[inline]
fn list_range<'a>(
    offsets: &OffsetBuffer<i32>,
    values: &'a ArrayOrd,
    idx: usize,
) -> impl Iterator<Item = ArrayIdx<'a>> + Clone {
    let offsets = offsets.inner();
    let from = offsets[idx].as_usize();
    let to = offsets[idx + 1].as_usize();
    (from..to).map(|i| values.at(i))
}

impl<'a> ArrayIdx<'a> {
    /// Returns the rough amount of space required for this entry in bytes.
    /// (Not counting nulls, dictionary encoding, or other space optimizations.)
    pub fn goodbytes(&self) -> usize {
        match self.array {
            ArrayOrd::Null(_) => 0,
            ArrayOrd::Bool(_) => size_of::<bool>(),
            ArrayOrd::Int8(_) => size_of::<i8>(),
            ArrayOrd::Int16(_) => size_of::<i16>(),
            ArrayOrd::Int32(_) => size_of::<i32>(),
            ArrayOrd::Int64(_) => size_of::<i64>(),
            ArrayOrd::UInt8(_) => size_of::<u8>(),
            ArrayOrd::UInt16(_) => size_of::<u16>(),
            ArrayOrd::UInt32(_) => size_of::<u32>(),
            ArrayOrd::UInt64(_) => size_of::<u64>(),
            ArrayOrd::Float32(_) => size_of::<f32>(),
            ArrayOrd::Float64(_) => size_of::<f64>(),
            ArrayOrd::String(a) => a.value(self.idx).len(),
            ArrayOrd::Binary(a) => a.value(self.idx).len(),
            ArrayOrd::FixedSizeBinary(a) => a.value_length().as_usize(),
            ArrayOrd::List(_, offsets, nested) => {
                // Range over the list, summing up the bytes for each entry.
                list_range(offsets, nested, self.idx)
                    .map(|a| a.goodbytes())
                    .sum()
            }
            ArrayOrd::Struct(_, nested) => nested.iter().map(|a| a.at(self.idx).goodbytes()).sum(),
        }
    }
}

impl<'a> Ord for ArrayIdx<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        #[inline]
        fn is_null(buffer: &Option<NullBuffer>, idx: usize) -> bool {
            buffer.as_ref().map_or(false, |b| b.is_null(idx))
        }
        #[inline]
        fn cmp<A: ArrayAccessor>(
            left: A,
            left_idx: usize,
            right: A,
            right_idx: usize,
            cmp: fn(&A::Item, &A::Item) -> Ordering,
        ) -> Ordering {
            // NB: nulls sort last, conveniently matching psql / mz_repr
            match (left.is_null(left_idx), right.is_null(right_idx)) {
                (false, true) => Ordering::Less,
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, false) => cmp(&left.value(left_idx), &right.value(right_idx)),
            }
        }
        match (&self.array, &other.array) {
            (ArrayOrd::Null(s), ArrayOrd::Null(o)) => {
                debug_assert!(
                    self.idx < s.len() && other.idx < o.len(),
                    "null array indices in bounds"
                );
                Ordering::Equal
            }
            // For arrays with "simple" value types, we fetch and compare the underlying values directly.
            (ArrayOrd::Bool(s), ArrayOrd::Bool(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Int8(s), ArrayOrd::Int8(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Int16(s), ArrayOrd::Int16(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Int32(s), ArrayOrd::Int32(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Int64(s), ArrayOrd::Int64(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::UInt8(s), ArrayOrd::UInt8(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::UInt16(s), ArrayOrd::UInt16(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::UInt32(s), ArrayOrd::UInt32(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::UInt64(s), ArrayOrd::UInt64(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Float32(s), ArrayOrd::Float32(o)) => {
                cmp(s, self.idx, o, other.idx, f32::total_cmp)
            }
            (ArrayOrd::Float64(s), ArrayOrd::Float64(o)) => {
                cmp(s, self.idx, o, other.idx, f64::total_cmp)
            }
            (ArrayOrd::String(s), ArrayOrd::String(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::Binary(s), ArrayOrd::Binary(o)) => cmp(s, self.idx, o, other.idx, Ord::cmp),
            (ArrayOrd::FixedSizeBinary(s), ArrayOrd::FixedSizeBinary(o)) => {
                cmp(s, self.idx, o, other.idx, Ord::cmp)
            }
            // For lists, we generate an iterator for each side that ranges over the correct
            // indices into the value buffer, then compare them lexicographically.
            (
                ArrayOrd::List(s_nulls, s_offset, s_values),
                ArrayOrd::List(o_nulls, o_offset, o_values),
            ) => match (is_null(s_nulls, self.idx), is_null(o_nulls, other.idx)) {
                (false, true) => Ordering::Less,
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, false) => list_range(s_offset, s_values, self.idx)
                    .cmp(list_range(o_offset, o_values, other.idx)),
            },
            // For structs, we iterate over the same index in each field for each input,
            // comparing them lexicographically in order.
            (ArrayOrd::Struct(s_nulls, s_cols), ArrayOrd::Struct(o_nulls, o_cols)) => {
                match (is_null(s_nulls, self.idx), is_null(o_nulls, other.idx)) {
                    (false, true) => Ordering::Less,
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, false) => {
                        let s = s_cols.iter().map(|array| array.at(self.idx));
                        let o = o_cols.iter().map(|array| array.at(other.idx));
                        s.cmp(o)
                    }
                }
            }
            (a, b) => panic!("array types did not match! {a:?} vs. {b:?}",),
        }
    }
}

impl<'a> PartialOrd for ArrayIdx<'a> {
    fn partial_cmp(&self, other: &ArrayIdx) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for ArrayIdx<'a> {
    fn eq(&self, other: &ArrayIdx) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a> Eq for ArrayIdx<'a> {}

/// An array with precisely one entry, for use as a lower bound.
#[derive(Debug, Clone)]
pub struct ArrayBound {
    raw: ArrayRef,
    ord: ArrayOrd,
    index: usize,
}

impl PartialEq for ArrayBound {
    fn eq(&self, other: &Self) -> bool {
        self.get().eq(&other.get())
    }
}

impl Eq for ArrayBound {}

impl ArrayBound {
    /// Create a new `ArrayBound` for this array, with the bound at the provided index.
    pub fn new(array: ArrayRef, index: usize) -> Self {
        Self {
            ord: ArrayOrd::new(array.as_ref()),
            raw: array,
            index,
        }
    }

    /// Get the value of the bound.
    pub fn get(&self) -> ArrayIdx<'_> {
        self.ord.at(self.index)
    }

    /// Convert to an array-data proto, respecting a maximum data size. The resulting proto will
    /// decode to a single-row array, such that `ArrayBound::new(decoded, 0).get() <= self.get()`,
    /// which makes it suitable as a lower bound.
    pub fn to_proto_lower(&self, max_len: usize) -> Option<ProtoArrayData> {
        // Use `take` instead of slice to make sure we encode just the relevant row to proto,
        // instead of some larger buffer with an offset.
        let indices = UInt64Array::from_value(u64::usize_as(self.index), 1);
        let taken = arrow::compute::take(self.raw.as_ref(), &indices, None).ok()?;
        let array_data = taken.into_data();

        let mut proto = array_data.into_proto();
        let original_len = proto.encoded_len();
        if original_len <= max_len {
            return Some(proto);
        }

        let mut data_type = proto.data_type.take()?;
        maybe_trim_proto(&mut data_type, &mut proto, max_len);
        proto.data_type = Some(data_type);

        if cfg!(debug_assertions) {
            let array: ArrayData = proto
                .clone()
                .into_rust()
                .expect("trimmed array data can still be decoded");
            assert_eq!(array.len(), 1);
            let new_bound = Self::new(make_array(array), 0);
            assert!(
                new_bound.get() <= self.get(),
                "trimmed bound should be comparable to and no larger than the original data"
            )
        }

        if proto.encoded_len() <= max_len {
            Some(proto)
        } else {
            None
        }
    }
}

/// Makes a best effort to shrink the proto while preserving the ordering.
/// (The proto might not be smaller after this method is called, but it should always
/// be a valid lower bound.)
///
/// Note that we pass in the data type and the array data separately, since we only keep
/// type info at the top level. If a caller does have a top-level `ArrayData` instance,
/// they should take that type and pass it in separately.
fn maybe_trim_proto(data_type: &mut proto::DataType, body: &mut ProtoArrayData, max_len: usize) {
    assert!(body.data_type.is_none(), "expected separate data type");
    // TODO: consider adding cases for strings and byte arrays
    let encoded_len = data_type.encoded_len() + body.encoded_len();
    match &mut data_type.kind {
        Some(data_type::Kind::Struct(data_type::Struct { children: fields })) => {
            // Pop off fields one by one, keeping an estimate of the encoded length.
            let mut struct_len = encoded_len;
            while struct_len > max_len {
                let Some(mut child) = body.children.pop() else {
                    break;
                };
                let Some(mut field) = fields.pop() else { break };

                struct_len -= field.encoded_len() + child.encoded_len();
                if let Some(remaining_len) = max_len.checked_sub(struct_len) {
                    // We're under budget after removing this field! See if we can
                    // shrink it to fit, but exit the loop regardless.
                    let Some(field_type) = field.data_type.as_mut() else {
                        break;
                    };
                    maybe_trim_proto(field_type, &mut child, remaining_len);
                    if field.encoded_len() + child.encoded_len() <= remaining_len {
                        fields.push(field);
                        body.children.push(child);
                    }
                    break;
                }
            }
        }
        _ => {}
    };
}

#[cfg(test)]
mod tests {
    use crate::arrow::{ArrayBound, ArrayOrd};
    use arrow::array::{
        ArrayRef, AsArray, BooleanArray, StringArray, StructArray, UInt64Array, make_array,
    };
    use arrow::datatypes::{DataType, Field, Fields};
    use mz_ore::assert_none;
    use mz_proto::ProtoType;
    use std::sync::Arc;

    #[mz_ore::test]
    fn trim_proto() {
        let nested_fields: Fields = vec![Field::new("a", DataType::UInt64, true)].into();
        let array: ArrayRef = Arc::new(StructArray::new(
            vec![
                Field::new("a", DataType::UInt64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new_struct("c", nested_fields.clone(), true),
            ]
            .into(),
            vec![
                Arc::new(UInt64Array::from_iter_values([1])),
                Arc::new(StringArray::from_iter_values(["large".repeat(50)])),
                Arc::new(StructArray::new_null(nested_fields, 1)),
            ],
            None,
        ));
        let bound = ArrayBound::new(array, 0);

        assert_none!(bound.to_proto_lower(0));
        assert_none!(bound.to_proto_lower(1));

        let proto = bound
            .to_proto_lower(100)
            .expect("can fit something in less than 100 bytes");
        let array = make_array(proto.into_rust().expect("valid proto"));
        assert_eq!(
            array.as_struct().column_names().as_slice(),
            &["a"],
            "only the first column should fit"
        );

        let proto = bound
            .to_proto_lower(1000)
            .expect("can fit everything in less than 1000 bytes");
        let array = make_array(proto.into_rust().expect("valid proto"));
        assert_eq!(
            array.as_struct().column_names().as_slice(),
            &["a", "b", "c"],
            "all columns should fit"
        )
    }

    #[mz_ore::test]
    fn struct_ord() {
        let prefix = StructArray::new(
            vec![Field::new("a", DataType::UInt64, true)].into(),
            vec![Arc::new(UInt64Array::from_iter_values([1, 3, 5]))],
            None,
        );
        let full = StructArray::new(
            vec![
                Field::new("a", DataType::UInt64, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
            vec![
                Arc::new(UInt64Array::from_iter_values([2, 3, 4])),
                Arc::new(StringArray::from_iter_values(["a", "b", "c"])),
            ],
            None,
        );
        let prefix_ord = ArrayOrd::new(&prefix);
        let full_ord = ArrayOrd::new(&full);

        // Comparison works as normal over the shared columns... but when those columns are identical,
        // the shorter struct is always smaller.
        assert!(prefix_ord.at(0) < full_ord.at(0), "(1) < (2, 'a')");
        assert!(prefix_ord.at(1) < full_ord.at(1), "(3) < (3, 'b')");
        assert!(prefix_ord.at(2) > full_ord.at(2), "(5) < (4, 'c')");
    }

    #[mz_ore::test]
    #[should_panic(expected = "array types did not match")]
    fn struct_ord_incompat() {
        // This test is descriptive, not prescriptive: we declare it is an error to compare
        // structs like this, but not what the result of comparing them is.
        let string = StructArray::new(
            vec![
                Field::new("a", DataType::UInt64, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
            vec![
                Arc::new(UInt64Array::from_iter_values([1])),
                Arc::new(StringArray::from_iter_values(["a"])),
            ],
            None,
        );
        let boolean = StructArray::new(
            vec![
                Field::new("a", DataType::UInt64, true),
                Field::new("b", DataType::Boolean, true),
            ]
            .into(),
            vec![
                Arc::new(UInt64Array::from_iter_values([1])),
                Arc::new(BooleanArray::from_iter([Some(true)])),
            ],
            None,
        );
        let string_ord = ArrayOrd::new(&string);
        let bool_ord = ArrayOrd::new(&boolean);

        // Despite the matching first column, this will panic with a type mismatch.
        assert!(string_ord.at(0) < bool_ord.at(0));
    }
}
