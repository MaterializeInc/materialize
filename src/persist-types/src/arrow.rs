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

use std::sync::Arc;

use arrow::array::ArrayDataBuilder;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};

#[allow(missing_docs)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/mz_persist_types.arrow.rs"));
}
pub use proto::ArrayData as ProtoArrayData;

impl RustType<proto::ArrayData> for arrow::array::ArrayData {
    fn into_proto(&self) -> proto::ArrayData {
        proto::ArrayData {
            data_type: Some(self.data_type().into_proto()),
            length: u64::cast_from(self.len()),
            offset: u64::cast_from(self.offset()),
            buffers: self.buffers().iter().map(|b| b.into_proto()).collect(),
            children: self.child_data().iter().map(|c| c.into_proto()).collect(),
            nulls: self.nulls().map(|n| n.inner().into_proto()),
        }
    }

    fn from_proto(proto: proto::ArrayData) -> Result<Self, TryFromProtoError> {
        let proto::ArrayData {
            data_type,
            length,
            offset,
            buffers,
            children,
            nulls,
        } = proto;
        let data_type = data_type.into_rust_if_some("data_type")?;
        let nulls = nulls
            .map(|n| n.into_rust())
            .transpose()?
            .map(NullBuffer::new);

        let mut builder = ArrayDataBuilder::new(data_type)
            .len(usize::cast_from(length))
            .offset(usize::cast_from(offset))
            .nulls(nulls);

        for b in buffers.into_iter().map(|b| b.into_rust()) {
            builder = builder.add_buffer(b?);
        }
        for c in children.into_iter().map(|c| c.into_rust()) {
            builder = builder.add_child_data(c?);
        }

        // Construct the builder which validates all inputs and aligns data.
        builder
            .build_aligned()
            .map_err(|e| TryFromProtoError::RowConversionError(e.to_string()))
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
