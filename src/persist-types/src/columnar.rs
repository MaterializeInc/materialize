// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Columnar understanding of persisted data
//!
//! For efficiency/performance, we directly expose the columnar structure of
//! persist's internal encoding to users during encoding and decoding. Interally
//! we use the [`arrow`] crate that gets durably written as parquet data.
//!
//! Some of the requirements that led to this design:
//! - Support a separation of data and schema because Row is not
//!   self-describing: e.g. a Datum::Null can be one of many possible column
//!   types. A RelationDesc is necessary to describe a Row schema.
//! - Narrow down [`arrow::datatypes::DataType`] (the arrow "logical" types) to a
//!   set we want to support in persist.
//! - Do `dyn Any` downcasting of columns once per part, not once per update.
//!
//! Finally, the [Schema2] trait maps an implementor of [Codec] to the underlying
//! column structure. It also provides a [ColumnEncoder] and [ColumnDecoder] for
//! amortizing any downcasting that does need to happen.

use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder};
use arrow::datatypes::DataType;
use std::fmt::Debug;
use std::sync::Arc;

use crate::stats::{DynStats, StructStats};
use crate::Codec;

/// A __stable__ encoding for a type that gets durably persisted in an
/// [`arrow::array::FixedSizeBinaryArray`].
pub trait FixedSizeCodec<T>: Debug + PartialEq + Eq {
    /// Number of bytes the encoded format requires.
    const SIZE: usize;

    /// Returns the encoded bytes as a slice.
    fn as_bytes(&self) -> &[u8];
    /// Create an instance of `self` from a slice.
    ///
    /// Note: It is the responsibility of the caller to make sure the provided
    /// data is valid for `self`.
    fn from_bytes(val: &[u8]) -> Result<Self, String>
    where
        Self: Sized;

    /// Encode a type of `T` into this format.
    fn from_value(value: T) -> Self;
    /// Decode an instance of `T` from this format.
    fn into_value(self) -> T;
}

/// A decoder for values of a fixed schema.
///
/// This allows us to amortize the cost of downcasting columns into concrete
/// types.
pub trait ColumnDecoder<T> {
    /// Decode the value at `idx` into the buffer `val`.
    ///
    /// Behavior for when the value at `idx` is null is implementation-defined.
    /// Panics if decoding an `idx` that is out-of-bounds.
    fn decode(&self, idx: usize, val: &mut T);

    /// Returns if the value at `idx` is null.
    fn is_null(&self, idx: usize) -> bool;

    /// Returns the number of bytes used by this decoder.
    fn goodbytes(&self) -> usize;

    /// Returns statistics for the column. This structure is defined by Persist,
    /// but the contents are determined by the client; Persist will preserve
    /// them in the part metadata and make them available to readers.
    ///
    /// TODO: For now, we require that the stats be structured as a non-nullable
    /// struct. For a single column, map them to a struct with a single column
    /// named the empty string. Fix this restriction if we end up with non-test
    /// code that isn't naturally a struct.
    fn stats(&self) -> StructStats;
}

/// An encoder for values of a fixed schema
///
/// This allows us to amortize the cost of downcasting columns into concrete
/// types.
pub trait ColumnEncoder<T> {
    /// Type of column that this encoder returns when finalized.
    type FinishedColumn: arrow::array::Array + Debug + 'static;

    /// The amount of "actual data" encoded by this encoder so far.
    fn goodbytes(&self) -> usize;

    /// Appends `val` onto this encoder.
    fn append(&mut self, val: &T);

    /// Appends a null value onto this encoder.
    fn append_null(&mut self);

    /// Finish this encoder, returning an immutable column.
    fn finish(self) -> Self::FinishedColumn;
}

/// Description of a type that we encode into Persist.
pub trait Schema2<T>: Debug + Send + Sync {
    /// The type of column we decode from, and encoder will finish into.
    type ArrowColumn: arrow::array::Array + Debug + Clone + 'static;
    /// Statistics we collect for a schema of this type.
    type Statistics: DynStats + 'static;

    /// Type that is able to decode values of `T` from [`Self::ArrowColumn`].
    type Decoder: ColumnDecoder<T> + Debug + Send + Sync;
    /// Type that is able to encoder values of `T`.
    type Encoder: ColumnEncoder<T, FinishedColumn = Self::ArrowColumn> + Debug + Send + Sync;

    /// Returns a type that is able to decode instances of `T` from the provider column.
    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error>;
    /// Returns a type that is able to decode instances of `T` from a type erased
    /// [`arrow::array::Array`], erroring if the provided array is not [`Self::ArrowColumn`].
    fn decoder_any(&self, col: &dyn arrow::array::Array) -> Result<Self::Decoder, anyhow::Error> {
        let col = col
            .as_any()
            .downcast_ref::<Self::ArrowColumn>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "failed downcasting to {}",
                    std::any::type_name::<Self::ArrowColumn>()
                )
            })?
            .clone();
        self.decoder(col)
    }

    /// Returns a type that can encode values of `T`.
    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error>;
}

/// Returns the data type of arrays generated by this schema.
///
/// This obtains the dsta type by encoding an empty array and checking its type.
/// The caller is generally expected to make sure that all columns generated by
/// this schema have the same datatype.
pub fn data_type<A: Codec>(schema: &A::Schema) -> anyhow::Result<DataType> {
    let array = schema.encoder()?.finish();
    Ok(Array::data_type(&array).clone())
}

/// Helper to convert from codec-encoded data to structured data.
pub fn codec_to_schema2<A: Codec + Default>(
    schema: &A::Schema,
    data: &BinaryArray,
) -> anyhow::Result<ArrayRef> {
    let mut encoder = Schema2::encoder(schema)?;

    let mut value: A = A::default();
    let mut storage = Some(A::Storage::default());

    for bytes in data.iter() {
        if let Some(bytes) = bytes {
            A::decode_from(&mut value, bytes, &mut storage, schema).map_err(|e| {
                anyhow!(
                    "unable to decode bytes with {} codec: {e:#?}",
                    A::codec_name()
                )
            })?;
            encoder.append(&value);
        } else {
            encoder.append_null();
        }
    }

    Ok(Arc::new(encoder.finish()))
}

/// Helper to convert from structured data to codec-encoded data.
pub fn schema2_to_codec<A: Codec + Default>(
    schema: &A::Schema,
    data: &dyn Array,
) -> anyhow::Result<BinaryArray> {
    let len = data.len();
    let decoder = Schema2::decoder_any(schema, data)?;
    let mut builder = BinaryBuilder::new();

    let mut value: A = A::default();
    let mut buffer = vec![];

    for i in 0..len {
        // The binary encoding of key/value types can never be null.
        // Defer to the implementation-defined behaviour for null entries in that case.
        decoder.decode(i, &mut value);
        Codec::encode(&value, &mut buffer);
        builder.append_value(&buffer);
        buffer.clear()
    }

    Ok(BinaryBuilder::finish(&mut builder))
}
