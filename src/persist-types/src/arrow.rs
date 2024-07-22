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
//! # Design Considerations
//!
//! Initially this was implemented as a "protobuf port" of
//! [`arrow::array::ArrayData`], with the goal of zero-copy deserialization
//! via [`arrow::buffer::Buffer`]s and [`bytes::Bytes`]. This doesn't work
//! though because Arrow needs the underlying Buffers to be aligned for the
//! type of data stored, e.g. an array of `u64`s needs to be 8 byte aligned.
//! While it is technically possible to make this work, it is much easier to
//! structure the data in protobuf and use native types.
//!
//! Using protobuf native types like `uint64` is nice because we benefit from
//! the space savings of `LEB128` encoding, with a tradeoff being runtime
//! performance. Given the above complications, and the need to store only a
//! small amount of data, this is a decent tradeoff.
//!
//!
//! [protobuf]: https://protobuf.dev/
//! [Arrow]: https://arrow.apache.org/
//! [Parquet]: https://parquet.apache.org/docs/
//! [Arrow IPC]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc

use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    GenericByteArray, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, MapArray,
    NullArray, StringArray, StructArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ByteArrayType, DataType, Field, Fields};
use mz_ore::assert_none;
use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

#[allow(missing_docs)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/mz_persist_types.arrow.rs"));
}
pub use proto::Array as ProtoArray;

/// Encode an [`arrow::array::Array`] to and from a [`proto::Array`].
pub trait ArrayProtobuf: Sized + arrow::array::Array {
    /// Encodes `self` and the provided [`Field`] into a [`proto::Array`].
    ///
    /// # Panics
    ///
    /// * Not all Arrow Array types are supported, panics on unsupported arrays.
    ///
    fn into_proto(self, field: Arc<Field>) -> proto::Array;

    /// Decodes an instance of `Self` and a [`Field`] from the provided
    /// [`proto::Array`]. Returns an error if we can't decode an instance of [`Self`].
    fn from_proto(proto: proto::Array) -> Result<(Field, Self), mz_proto::TryFromProtoError>;
}

impl ArrayProtobuf for Arc<dyn arrow::array::Array> {
    fn into_proto(self, field: Arc<Field>) -> proto::Array {
        fn inner<T>(field: Arc<Field>, array: Arc<dyn arrow::array::Array>) -> proto::Array
        where
            T: ArrayProtobuf + arrow::array::Array + Clone + 'static,
        {
            let Some(array) = array.as_any().downcast_ref::<T>() else {
                panic!("expected {} but got {array:?}", std::any::type_name::<T>());
            };
            array.clone().into_proto(field)
        }

        match self.data_type() {
            DataType::Boolean => inner::<BooleanArray>(field, self),
            DataType::UInt8 => inner::<UInt8Array>(field, self),
            DataType::UInt16 => inner::<UInt16Array>(field, self),
            DataType::UInt32 => inner::<UInt32Array>(field, self),
            DataType::UInt64 => inner::<UInt64Array>(field, self),
            DataType::Int8 => inner::<Int8Array>(field, self),
            DataType::Int16 => inner::<Int16Array>(field, self),
            DataType::Int32 => inner::<Int32Array>(field, self),
            DataType::Int64 => inner::<Int64Array>(field, self),
            DataType::Float32 => inner::<Float32Array>(field, self),
            DataType::Float64 => inner::<Float64Array>(field, self),
            DataType::Utf8 => inner::<StringArray>(field, self),
            DataType::Binary => inner::<BinaryArray>(field, self),
            DataType::FixedSizeBinary(_) => inner::<FixedSizeBinaryArray>(field, self),
            DataType::List(_) => inner::<ListArray>(field, self),
            DataType::Map(_, _) => inner::<MapArray>(field, self),
            DataType::Struct(_) => inner::<StructArray>(field, self),
            DataType::Null => inner::<NullArray>(field, self),
            other => unimplemented!("unsupported array type {other:?}"),
        }
    }

    fn from_proto(proto: proto::Array) -> Result<(Field, Self), mz_proto::TryFromProtoError> {
        fn inner<T: ArrayProtobuf + 'static>(
            array: proto::Array,
        ) -> Result<(Field, Arc<dyn arrow::array::Array>), mz_proto::TryFromProtoError> {
            let (field, array) = T::from_proto(array)?;
            let array: Arc<dyn arrow::array::Array> = Arc::new(array);

            Ok((field, array))
        }

        match &proto.kind {
            None => Err(TryFromProtoError::missing_field("kind")),
            Some(proto::array::Kind::Boolean(_)) => inner::<BooleanArray>(proto),
            Some(proto::array::Kind::Uint8(_)) => inner::<UInt8Array>(proto),
            Some(proto::array::Kind::Uint16(_)) => inner::<UInt16Array>(proto),
            Some(proto::array::Kind::Uint32(_)) => inner::<UInt32Array>(proto),
            Some(proto::array::Kind::Uint64(_)) => inner::<UInt64Array>(proto),
            Some(proto::array::Kind::Int8(_)) => inner::<Int8Array>(proto),
            Some(proto::array::Kind::Int16(_)) => inner::<Int16Array>(proto),
            Some(proto::array::Kind::Int32(_)) => inner::<Int32Array>(proto),
            Some(proto::array::Kind::Int64(_)) => inner::<Int64Array>(proto),
            Some(proto::array::Kind::Float32(_)) => inner::<Float32Array>(proto),
            Some(proto::array::Kind::Float64(_)) => inner::<Float64Array>(proto),
            Some(proto::array::Kind::String(_)) => inner::<StringArray>(proto),
            Some(proto::array::Kind::Binary(_)) => inner::<BinaryArray>(proto),
            Some(proto::array::Kind::FixedBinary(_)) => inner::<FixedSizeBinaryArray>(proto),
            Some(proto::array::Kind::List(_)) => inner::<ListArray>(proto),
            Some(proto::array::Kind::Map(_)) => inner::<MapArray>(proto),
            Some(proto::array::Kind::Struct(_)) => inner::<StructArray>(proto),
            Some(proto::array::Kind::Null(_)) => inner::<NullArray>(proto),
        }
    }
}

impl ArrayProtobuf for BooleanArray {
    fn into_proto(self, field: Arc<Field>) -> proto::Array {
        let (buffer, nulls) = self.into_parts();
        let buffer = Some(buffer.into_proto());
        let array = proto::BooleanArray { buffer };

        proto::Array {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            nulls: nulls.map(|n| n.into_inner().into_proto()),
            kind: Some(proto::array::Kind::Boolean(array)),
        }
    }

    fn from_proto(proto: proto::Array) -> Result<(Field, Self), TryFromProtoError> {
        let proto::Array {
            name,
            nullable,
            nulls,
            kind,
        } = proto;

        let Some(proto::array::Kind::Boolean(array)) = kind else {
            return Err(TryFromProtoError::missing_field("kind.boolean"));
        };

        let bool_buffer: BooleanBuffer = array.buffer.into_rust_if_some("buffer")?;
        let nulls = nulls
            .map(|n| n.into_rust())
            .transpose()?
            .map(NullBuffer::new);
        let field = Field::new(name, DataType::Boolean, nullable);
        let array = BooleanArray::new(bool_buffer, nulls);

        Ok((field, array))
    }
}

macro_rules! primitive_array {
    ($rust_ty:ty, $arrow_array:ty, $proto_var:ident, $proto_array:ident) => {
        impl ArrayProtobuf for $arrow_array {
            fn into_proto(self, field: Arc<Field>) -> proto::Array {
                let nulls = self.logical_nulls().map(|n| n.into_inner().into_proto());
                let values = self.values().iter().map(|x| (*x).into()).collect();
                let array = proto::$proto_array { values };

                proto::Array {
                    name: field.name().clone(),
                    nullable: field.is_nullable(),
                    nulls,
                    kind: Some(proto::array::Kind::$proto_var(array)),
                }
            }

            fn from_proto(
                proto: proto::Array,
            ) -> Result<(Field, Self), mz_proto::TryFromProtoError> {
                let proto::Array {
                    name,
                    nullable,
                    nulls,
                    kind,
                } = proto;
                let kind_str = stringify!($proto_var);

                let Some(proto::array::Kind::$proto_var(array)) = kind else {
                    let msg = format!("expected {kind_str}, found {kind:?}");
                    return Err(TryFromProtoError::missing_field(msg));
                };

                let values: Vec<$rust_ty> = array
                    .values
                    .into_iter()
                    .map(|x| {
                        x.try_into().map_err(|x| {
                            let msg = format!("failed to roundtrip {kind_str}, found {x:?}");
                            TryFromProtoError::RowConversionError(msg)
                        })
                    })
                    .collect::<Result<_, _>>()?;
                let buffer = ScalarBuffer::from(values);

                let nulls = nulls
                    .map(|n| n.into_rust())
                    .transpose()?
                    .map(NullBuffer::new);
                let array = <$arrow_array>::new(buffer, nulls);
                let field = Field::new(name, array.data_type().clone(), nullable);

                Ok((field, array))
            }
        }
    };
}

primitive_array!(i8, Int8Array, Int8, I64Array);
primitive_array!(i16, Int16Array, Int16, I64Array);
primitive_array!(i32, Int32Array, Int32, I64Array);
primitive_array!(i64, Int64Array, Int64, I64Array);
primitive_array!(u8, UInt8Array, Uint8, U64Array);
primitive_array!(u16, UInt16Array, Uint16, U64Array);
primitive_array!(u32, UInt32Array, Uint32, U64Array);
primitive_array!(u64, UInt64Array, Uint64, U64Array);
primitive_array!(f32, Float32Array, Float32, F32Array);
primitive_array!(f64, Float64Array, Float64, F64Array);

impl<T> ArrayProtobuf for arrow::array::GenericByteArray<T>
where
    T: ByteArrayType<Offset = i32>,
{
    fn into_proto(self, field: Arc<Field>) -> proto::Array {
        let data_type = self.data_type().clone();
        let (offsets, buffer, nulls) = self.into_parts();

        let array = proto::VariableSizeArray {
            data: Some(buffer.into_proto()),
            offsets: offsets.into_iter().copied().collect(),
        };
        let kind = match data_type {
            DataType::Binary => proto::array::Kind::Binary(array),
            DataType::Utf8 => proto::array::Kind::String(array),
            other => unimplemented!("{other:?}"),
        };

        proto::Array {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            nulls: nulls.map(|n| n.into_inner().into_proto()),
            kind: Some(kind),
        }
    }

    fn from_proto(proto: proto::Array) -> Result<(Field, Self), mz_proto::TryFromProtoError> {
        let proto::Array {
            name,
            nullable,
            nulls,
            kind,
        } = proto;
        let kind = kind.ok_or_else(|| TryFromProtoError::missing_field("kind"))?;
        let nulls = nulls
            .map(|n| n.into_rust())
            .transpose()?
            .map(NullBuffer::new);

        let array = match (T::DATA_TYPE, kind) {
            (DataType::Binary, proto::array::Kind::Binary(array)) => {
                let offsets = ScalarBuffer::from(array.offsets);
                let offsets = OffsetBuffer::new(offsets);
                let values = array.data.into_rust_if_some("data")?;

                GenericByteArray::new(offsets, values, nulls)
            }
            (DataType::Utf8, proto::array::Kind::String(array)) => {
                let offsets = ScalarBuffer::from(array.offsets);
                let offsets = OffsetBuffer::new(offsets);
                let values = array.data.into_rust_if_some("data")?;

                GenericByteArray::new(offsets, values, nulls)
            }
            other => {
                let msg = format!("wrong type, found {other:?}");
                return Err(TryFromProtoError::missing_field(msg));
            }
        };

        let field = Field::new(name, array.data_type().clone(), nullable);

        Ok((field, array))
    }
}

impl ArrayProtobuf for FixedSizeBinaryArray {
    fn into_proto(self, field: Arc<Field>) -> proto::Array {
        let (size, buffer, nulls) = self.into_parts();
        let array = proto::FixedSizeArray {
            data: Some(buffer.into_proto()),
            size,
        };

        proto::Array {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            nulls: nulls.map(|n| n.into_inner().into_proto()),
            kind: Some(proto::array::Kind::FixedBinary(array)),
        }
    }

    fn from_proto(proto: proto::Array) -> Result<(Field, Self), mz_proto::TryFromProtoError> {
        let proto::Array {
            name,
            nullable,
            nulls,
            kind,
        } = proto;

        let Some(proto::array::Kind::FixedBinary(array)) = kind else {
            return Err(TryFromProtoError::missing_field("kind.fixed_size"));
        };

        let nulls = nulls
            .map(|n| n.into_rust())
            .transpose()?
            .map(NullBuffer::new);
        let values = array.data.into_rust_if_some("data")?;
        let array = FixedSizeBinaryArray::new(array.size, values, nulls);
        let field = Field::new(name, array.data_type().clone(), nullable);

        Ok((field, array))
    }
}

impl ArrayProtobuf for ListArray {
    fn into_proto(self, field: Arc<Field>) -> proto::Array {
        let (child_field, offsets, child, nulls) = self.into_parts();
        let child = child.into_proto(child_field);
        let array = Box::new(proto::ListArray {
            values: Some(Box::new(child)),
            offsets: offsets.into_iter().copied().collect(),
        });

        proto::Array {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            nulls: nulls.map(|n| n.into_inner().into_proto()),
            kind: Some(proto::array::Kind::List(array)),
        }
    }

    fn from_proto(proto: proto::Array) -> Result<(Field, Self), mz_proto::TryFromProtoError> {
        let proto::Array {
            name,
            nullable,
            nulls,
            kind,
        } = proto;

        let Some(proto::array::Kind::List(array)) = kind else {
            let msg = format!("expected kind.list, found {kind:?}");
            return Err(TryFromProtoError::missing_field(msg));
        };

        let values = array
            .values
            .ok_or_else(|| TryFromProtoError::missing_field("values"))?;
        let (values_field, values): (_, Arc<dyn arrow::array::Array>) =
            ArrayProtobuf::from_proto(*values)?;
        let values_field = Arc::new(values_field);

        let nulls: Option<NullBuffer> = nulls
            .map(|n| n.into_rust())
            .transpose()?
            .map(NullBuffer::new);
        let offsets = ScalarBuffer::from(array.offsets);
        let offsets = OffsetBuffer::new(offsets);

        let array = ListArray::new(values_field, offsets, values, nulls);
        let field = Field::new(name, array.data_type().clone(), nullable);

        Ok((field, array))
    }
}

impl ArrayProtobuf for MapArray {
    fn into_proto(self, field: Arc<Field>) -> proto::Array {
        let (child_field, offsets, child, nulls, sorted) = self.into_parts();
        let child = child.into_proto(child_field);
        let array = Box::new(proto::MapArray {
            values: Some(Box::new(child)),
            offsets: offsets.into_iter().copied().collect(),
            sorted,
        });

        proto::Array {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            nulls: nulls.map(|n| n.into_inner().into_proto()),
            kind: Some(proto::array::Kind::Map(array)),
        }
    }

    fn from_proto(proto: proto::Array) -> Result<(Field, Self), mz_proto::TryFromProtoError> {
        let proto::Array {
            name,
            nullable,
            nulls,
            kind,
        } = proto;

        let Some(proto::array::Kind::Map(array)) = kind else {
            let msg = format!("expected kind.map, found {kind:?}");
            return Err(TryFromProtoError::missing_field(msg));
        };

        let values = array
            .values
            .ok_or_else(|| TryFromProtoError::missing_field("values"))?;
        let (entries_field, entries): (_, StructArray) = ArrayProtobuf::from_proto(*values)?;

        let offsets = ScalarBuffer::from(array.offsets);
        let offsets = OffsetBuffer::new(offsets);
        let nulls = nulls
            .map(|n| n.into_rust())
            .transpose()?
            .map(NullBuffer::new);

        let array = MapArray::new(
            Arc::new(entries_field),
            offsets,
            entries,
            nulls,
            array.sorted,
        );
        let field = Field::new(name, array.data_type().clone(), nullable);

        Ok((field, array))
    }
}

impl ArrayProtobuf for StructArray {
    fn into_proto(self, field: Arc<Field>) -> proto::Array {
        let (fields, children, nulls) = self.into_parts();

        assert_eq!(fields.len(), children.len());
        let children = fields
            .into_iter()
            .zip(children.into_iter())
            .map(|(field, child)| child.into_proto(Arc::clone(field)))
            .collect();
        let array = proto::StructArray { children };

        proto::Array {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            nulls: nulls.map(|n| n.into_inner().into_proto()),
            kind: Some(proto::array::Kind::Struct(array)),
        }
    }

    fn from_proto(proto: proto::Array) -> Result<(Field, Self), mz_proto::TryFromProtoError> {
        let proto::Array {
            name,
            nullable,
            nulls,
            kind,
        } = proto;

        let Some(proto::array::Kind::Struct(array)) = kind else {
            let msg = format!("expected kind.struct, found {kind:?}");
            return Err(TryFromProtoError::missing_field(msg));
        };

        let mut fields = Vec::with_capacity(array.children.len());
        let mut arrays = Vec::with_capacity(array.children.len());
        for child in array.children {
            let (child_field, child_array): (_, Arc<dyn arrow::array::Array>) =
                ArrayProtobuf::from_proto(child)?;
            fields.push(child_field);
            arrays.push(child_array);
        }

        let fields = Fields::from(fields);
        let nulls = nulls
            .map(|n| n.into_rust())
            .transpose()?
            .map(NullBuffer::new);

        let array = StructArray::new(fields, arrays, nulls);
        let field = Field::new(name, array.data_type().clone(), nullable);

        Ok((field, array))
    }
}

impl ArrayProtobuf for NullArray {
    fn into_proto(self, field: Arc<Field>) -> proto::Array {
        let array = proto::NullArray {
            len: u64::cast_from(self.len()),
        };

        proto::Array {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            nulls: None,
            kind: Some(proto::array::Kind::Null(array)),
        }
    }

    fn from_proto(proto: proto::Array) -> Result<(Field, Self), mz_proto::TryFromProtoError> {
        let proto::Array {
            name,
            nullable,
            nulls,
            kind,
        } = proto;
        assert_none!(nulls);

        let Some(proto::array::Kind::Null(array)) = kind else {
            let msg = format!("expected kind.null, found {kind:?}");
            return Err(TryFromProtoError::missing_field(msg));
        };

        let array = NullArray::new(usize::cast_from(array.len));
        let field = Field::new(name, array.data_type().clone(), nullable);

        Ok((field, array))
    }
}

impl RustType<proto::Buffer> for arrow::buffer::Buffer {
    fn into_proto(&self) -> proto::Buffer {
        // TODO(parkmycar): There is probably something better we can do here.
        proto::Buffer {
            data: bytes::Bytes::from(self.as_slice().to_vec()),
        }
    }

    fn from_proto(proto: proto::Buffer) -> Result<Self, TryFromProtoError> {
        Ok(arrow::buffer::Buffer::from_bytes(proto.data.into()))
    }
}

impl RustType<proto::BooleanBuffer> for arrow::buffer::BooleanBuffer {
    fn into_proto(&self) -> proto::BooleanBuffer {
        proto::BooleanBuffer {
            buffer: Some(self.inner().clone().into_proto()),
            offset: u64::cast_from(self.offset()),
            length: u64::cast_from(self.len()),
        }
    }

    fn from_proto(proto: proto::BooleanBuffer) -> Result<Self, TryFromProtoError> {
        let proto::BooleanBuffer {
            buffer,
            offset,
            length,
        } = proto;

        let buffer = buffer.into_rust_if_some("buffer")?;
        let offset = usize::cast_from(offset);
        let length = usize::cast_from(length);

        Ok(BooleanBuffer::new(buffer, offset, length))
    }
}
