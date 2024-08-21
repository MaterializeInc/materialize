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
//! persist's internal encoding to users during encoding and decoding. For
//! ergonomics, we wrap the [`arrow`] crate we use to read and write parquet data.
//!
//! Some of the requirements that led to this design:
//! - Support a separation of data and schema because Row is not
//!   self-describing: e.g. a Datum::Null can be one of many possible column
//!   types. A RelationDesc is necessary to describe a Row schema.
//! - Narrow down [`arrow::datatypes::DataType`] (the arrow "logical" types) to a
//!   set we want to support in persist.
//! - Associate a [`parquet::basic::Encoding`] with each of those types.
//! - Do `dyn Any` downcasting of columns once per part, not once per update.
//! - Unlike [`arrow`], be precise about whether each column is optional or not.
//!
//! The primary presentation of this abstraction is a sealed trait [Data], which
//! is implemented for the owned version of each type of data that can be stored
//! in persist: `int64`, `Option<String>`, etc.
//!
//! Under the hood, it's necessary to store something like a map of `name ->
//! column`. A natural instinct is to make Data object safe, but I couldn't
//! figure out a way to make that work without severe limitations. As a result,
//! the DataType enum is introduced with a 1:1 relationship between variants and
//! implementations of Data. This allows for easy type erasure and guardrails
//! when downcasting the types back.
//!
//! Note: The "Data" strategy is roughly how columnation works and the
//! "DataType" strategy is roughly how [`arrow`] works. Doing both of them gets us
//! the benefits of both, while the downside is code duplication and cognitive
//! overhead.
//!
//! The Data trait has associated types for the exclusive "builder" type for the
//! column and for the shared "reader" type. These also implement some common traits
//! to make relationships between types more structured.
//!
//! Finally, the [Schema] trait maps an implementor of [Codec] to the underlying
//! column structure. It also provides a [PartEncoder] and [PartDecoder] for
//! amortizing any downcasting that does need to happen.

use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder};
use std::fmt::Debug;
use std::sync::Arc;

use crate::stats::{ColumnarStats, DynStats};
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

    /// Returns statistics for the column. This structure is defined by Persist,
    /// but the contents are determined by the client; Persist will preserve them
    /// in the part metadata and make them available to readers.
    fn stats(&self) -> ColumnarStats;
}

/// An encoder for values of a fixed schema
///
/// This allows us to amortize the cost of downcasting columns into concrete
/// types.
pub trait ColumnEncoder<T> {
    /// Type of column that this encoder returns when finalized.
    type FinishedColumn: arrow::array::Array + Debug + 'static;

    /// Appends `val` onto this encoder.
    fn append(&mut self, val: &T);

    /// Appends a null value onto this encoder.
    fn append_null(&mut self);

    /// Finish this encoder, returning an immutable column and statistics.
    fn finish(self) -> Self::FinishedColumn;
}

/// Description of a type that we encode into Persist.
pub trait Schema2<T>: Debug + Send + Sync {
    /// The type of column we decode from, and encoder will finish into.
    type ArrowColumn: arrow::array::Array + Debug + Clone + 'static;
    /// Statistics we collect for a schema of this type.
    type Statistics: DynStats + 'static;

    /// Type that is able to decode values of `T` from [`Self::ArrowColumn`].
    type Decoder: ColumnDecoder<T> + Debug;
    /// Type that is able to encoder values of `T`.
    type Encoder: ColumnEncoder<T, FinishedColumn = Self::ArrowColumn> + Debug;

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
        if decoder.is_null(i) {
            builder.append_null();
        } else {
            decoder.decode(i, &mut value);
            Codec::encode(&value, &mut buffer);
            builder.append_value(&buffer);
            buffer.clear()
        }
    }

    Ok(BinaryBuilder::finish(&mut builder))
}
