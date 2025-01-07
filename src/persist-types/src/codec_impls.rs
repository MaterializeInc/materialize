// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of [Codec] for stdlib types.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::marker::PhantomData;

use arrow::array::{
    Array, ArrayBuilder, BinaryArray, BinaryBuilder, NullArray, StringArray, StringBuilder,
    StructArray,
};
use bytes::{BufMut, Bytes};
use timely::order::Product;

use crate::arrow::ArrayOrd;
use crate::columnar::{ColumnDecoder, ColumnEncoder, Schema2};
use crate::stats::{ColumnStatKinds, ColumnarStats, NoneStats, StructStats};
use crate::{Codec, Codec64, Opaque, ShardId};

/// All codecs that are compatible with a blob of bytes use that same name. This allows us to
/// switch between the codecs without any incompatibility errors. The name is chosen for historical
/// reasons.
const BYTES_CODEC_NAME: &str = "Vec<u8>";

/// An implementation of [Schema2] for [()].
#[derive(Debug, Default, PartialEq)]
pub struct UnitSchema;

impl Codec for () {
    type Storage = ();
    type Schema = UnitSchema;

    fn codec_name() -> String {
        "()".into()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
        // No-op.
    }

    fn decode<'a>(buf: &'a [u8], _schema: &UnitSchema) -> Result<Self, String> {
        if !buf.is_empty() {
            return Err(format!("decode expected empty buf got {} bytes", buf.len()));
        }
        Ok(())
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        UnitSchema
    }
}

/// An encoder and decoder for [`UnitSchema`].
#[derive(Debug)]
pub struct UnitColumnar {
    /// Number of entries in this column.
    len: usize,
}

impl UnitColumnar {
    /// Returns a new [`UnitColumnar`] with the number of entries specified.
    pub fn new(len: usize) -> Self {
        UnitColumnar { len }
    }
}

impl ColumnDecoder<()> for UnitColumnar {
    fn decode(&self, idx: usize, _val: &mut ()) {
        if idx >= self.len {
            panic!("index out of bounds, idx: {idx}, len: {}", self.len);
        }
    }

    fn is_null(&self, idx: usize) -> bool {
        if idx < self.len {
            true
        } else {
            panic!("index out of bounds, idx: {idx}, len: {}", self.len);
        }
    }

    fn goodbytes(&self) -> usize {
        0
    }

    fn stats(&self) -> StructStats {
        StructStats {
            len: self.len,
            cols: BTreeMap::new(),
        }
    }
}

impl ColumnEncoder<()> for UnitColumnar {
    type FinishedColumn = NullArray;

    fn goodbytes(&self) -> usize {
        0
    }

    fn append(&mut self, _val: &()) {
        self.len += 1;
    }

    fn append_null(&mut self) {
        self.len += 1;
    }

    fn finish(self) -> Self::FinishedColumn {
        NullArray::new(self.len)
    }
}

impl Schema2<()> for UnitSchema {
    type ArrowColumn = NullArray;
    type Statistics = NoneStats;

    type Decoder = UnitColumnar;
    type Encoder = UnitColumnar;

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(UnitColumnar::new(col.len()))
    }

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(UnitColumnar::new(0))
    }
}

/// Simple type of data that can be columnar encoded.
pub trait SimpleColumnarData {
    /// Type of [`arrow`] builder that we encode data into.
    type ArrowBuilder: arrow::array::ArrayBuilder + Default;
    /// Type of [`arrow`] array the we decode data from.
    type ArrowColumn: arrow::array::Array + Clone + 'static;

    /// The number of actual data bytes this item represents.
    fn goodbytes(builder: &Self::ArrowBuilder) -> usize;

    /// Encode `self` into `builder`.
    fn push(&self, builder: &mut Self::ArrowBuilder);
    /// Encode a null value into `builder`.
    fn push_null(builder: &mut Self::ArrowBuilder);

    /// Decode an instance of `self` from `column`.
    fn read(&mut self, idx: usize, column: &Self::ArrowColumn);
}

impl SimpleColumnarData for String {
    type ArrowBuilder = StringBuilder;
    type ArrowColumn = StringArray;

    fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
        builder.values_slice().len()
    }

    fn push(&self, builder: &mut Self::ArrowBuilder) {
        builder.append_value(self.as_str())
    }
    fn push_null(builder: &mut Self::ArrowBuilder) {
        builder.append_null()
    }
    fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
        self.clear();
        self.push_str(column.value(idx));
    }
}

impl SimpleColumnarData for Vec<u8> {
    type ArrowBuilder = BinaryBuilder;
    type ArrowColumn = BinaryArray;

    fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
        builder.values_slice().len()
    }

    fn push(&self, builder: &mut Self::ArrowBuilder) {
        builder.append_value(self.as_slice())
    }
    fn push_null(builder: &mut Self::ArrowBuilder) {
        builder.append_null()
    }
    fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
        self.clear();
        self.extend(column.value(idx));
    }
}

impl SimpleColumnarData for Bytes {
    type ArrowBuilder = BinaryBuilder;
    type ArrowColumn = BinaryArray;

    fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
        builder.values_slice().len()
    }

    fn push(&self, builder: &mut Self::ArrowBuilder) {
        builder.append_value(&self)
    }
    fn push_null(builder: &mut Self::ArrowBuilder) {
        builder.append_null()
    }
    fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
        *self = Bytes::copy_from_slice(column.value(idx));
    }
}

impl SimpleColumnarData for ShardId {
    type ArrowBuilder = StringBuilder;
    type ArrowColumn = StringArray;

    fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
        builder.values_slice().len()
    }

    fn push(&self, builder: &mut Self::ArrowBuilder) {
        builder.append_value(&self.to_string());
    }
    fn push_null(builder: &mut Self::ArrowBuilder) {
        builder.append_null();
    }
    fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
        *self = column.value(idx).parse().expect("should be valid ShardId");
    }
}

/// A type that implements [`ColumnEncoder`] for [`SimpleColumnarData`].
#[derive(Debug, Default)]
pub struct SimpleColumnarEncoder<T: SimpleColumnarData>(T::ArrowBuilder);

impl<T: SimpleColumnarData> ColumnEncoder<T> for SimpleColumnarEncoder<T> {
    type FinishedColumn = T::ArrowColumn;

    fn goodbytes(&self) -> usize {
        T::goodbytes(&self.0)
    }

    fn append(&mut self, val: &T) {
        T::push(val, &mut self.0);
    }
    fn append_null(&mut self) {
        T::push_null(&mut self.0)
    }
    fn finish(mut self) -> Self::FinishedColumn {
        let array = ArrayBuilder::finish(&mut self.0);
        let array = array
            .as_any()
            .downcast_ref::<T::ArrowColumn>()
            .expect("created using StringBuilder")
            .clone();

        array
    }
}

/// A type that implements [`ColumnDecoder`] for [`SimpleColumnarData`].
#[derive(Debug)]
pub struct SimpleColumnarDecoder<T: SimpleColumnarData>(T::ArrowColumn);

impl<T: SimpleColumnarData> SimpleColumnarDecoder<T> {
    /// Returns a new [`SimpleColumnarDecoder`] with the provided column.
    pub fn new(col: T::ArrowColumn) -> Self {
        SimpleColumnarDecoder(col)
    }
}

impl<T: SimpleColumnarData> ColumnDecoder<T> for SimpleColumnarDecoder<T> {
    fn decode(&self, idx: usize, val: &mut T) {
        T::read(val, idx, &self.0)
    }
    fn is_null(&self, idx: usize) -> bool {
        self.0.is_null(idx)
    }
    fn goodbytes(&self) -> usize {
        ArrayOrd::new(&self.0).goodbytes()
    }

    fn stats(&self) -> StructStats {
        ColumnarStats::one_column_struct(self.0.len(), ColumnStatKinds::None)
    }
}

/// An implementation of [Schema2] for [String].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StringSchema;

impl Schema2<String> for StringSchema {
    type ArrowColumn = StringArray;
    type Statistics = NoneStats;

    type Decoder = SimpleColumnarDecoder<String>;
    type Encoder = SimpleColumnarEncoder<String>;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SimpleColumnarEncoder::default())
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(SimpleColumnarDecoder::new(col))
    }
}

impl Codec for String {
    type Storage = ();
    type Schema = StringSchema;

    fn codec_name() -> String {
        "String".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put(self.as_bytes())
    }

    fn decode<'a>(buf: &'a [u8], _schema: &StringSchema) -> Result<Self, String> {
        String::from_utf8(buf.to_owned()).map_err(|err| err.to_string())
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        StringSchema
    }
}

/// An implementation of [Schema2] for [`Vec<u8>`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VecU8Schema;

impl Schema2<Vec<u8>> for VecU8Schema {
    type ArrowColumn = BinaryArray;
    type Statistics = NoneStats;

    type Decoder = SimpleColumnarDecoder<Vec<u8>>;
    type Encoder = SimpleColumnarEncoder<Vec<u8>>;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SimpleColumnarEncoder::default())
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(SimpleColumnarDecoder::new(col))
    }
}

impl Schema2<Bytes> for VecU8Schema {
    type ArrowColumn = BinaryArray;
    type Statistics = NoneStats;

    type Decoder = SimpleColumnarDecoder<Bytes>;
    type Encoder = SimpleColumnarEncoder<Bytes>;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SimpleColumnarEncoder::default())
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(SimpleColumnarDecoder::new(col))
    }
}

impl Codec for Vec<u8> {
    type Storage = ();
    type Schema = VecU8Schema;

    fn codec_name() -> String {
        BYTES_CODEC_NAME.into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put(self.as_slice())
    }

    fn decode<'a>(buf: &'a [u8], _schema: &VecU8Schema) -> Result<Self, String> {
        Ok(buf.to_owned())
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        VecU8Schema
    }
}

impl Codec for Bytes {
    type Storage = ();
    type Schema = VecU8Schema;

    fn codec_name() -> String {
        BYTES_CODEC_NAME.into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put(self.into_iter().as_slice())
    }

    fn decode<'a>(buf: &'a [u8], _schema: &VecU8Schema) -> Result<Self, String> {
        Ok(Bytes::copy_from_slice(buf))
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        VecU8Schema
    }
}

impl Codec for ShardId {
    type Storage = ();
    type Schema = ShardIdSchema;
    fn codec_name() -> String {
        "ShardId".into()
    }
    fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put(self.to_string().as_bytes())
    }
    fn decode<'a>(buf: &'a [u8], _schema: &ShardIdSchema) -> Result<Self, String> {
        let shard_id = String::from_utf8(buf.to_owned()).map_err(|err| err.to_string())?;
        shard_id.parse()
    }
    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }
    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        ShardIdSchema
    }
}

/// An implementation of [Schema2] for [ShardId].
#[derive(Debug, PartialEq)]
pub struct ShardIdSchema;

impl Schema2<ShardId> for ShardIdSchema {
    type ArrowColumn = StringArray;
    type Statistics = NoneStats;

    type Decoder = SimpleColumnarDecoder<ShardId>;
    type Encoder = SimpleColumnarEncoder<ShardId>;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SimpleColumnarEncoder::default())
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(SimpleColumnarDecoder::new(col))
    }
}

impl Codec64 for i64 {
    fn codec_name() -> String {
        "i64".to_owned()
    }

    fn encode(&self) -> [u8; 8] {
        self.to_le_bytes()
    }

    fn decode(buf: [u8; 8]) -> Self {
        i64::from_le_bytes(buf)
    }
}

impl Codec64 for u64 {
    fn codec_name() -> String {
        "u64".to_owned()
    }

    fn encode(&self) -> [u8; 8] {
        self.to_le_bytes()
    }

    fn decode(buf: [u8; 8]) -> Self {
        u64::from_le_bytes(buf)
    }
}

impl Opaque for u64 {
    fn initial() -> Self {
        u64::MIN
    }
}

impl Codec64 for Product<u32, u32> {
    fn codec_name() -> String {
        "Product<u32, u32>".to_owned()
    }

    fn encode(&self) -> [u8; 8] {
        let o = self.outer.to_le_bytes();
        let i = self.inner.to_le_bytes();
        [o[0], o[1], o[2], o[3], i[0], i[1], i[2], i[3]]
    }

    fn decode(buf: [u8; 8]) -> Self {
        let outer = [buf[0], buf[1], buf[2], buf[3]];
        let inner = [buf[4], buf[5], buf[6], buf[7]];
        Product::new(u32::from_le_bytes(outer), u32::from_le_bytes(inner))
    }
}

// TODO: Remove this once we wrap coord epochs in an `Epoch` struct and impl
// Opaque on `Epoch` instead.
impl Opaque for i64 {
    fn initial() -> Self {
        i64::MIN
    }
}

/// A placeholder for a [Codec] impl that hasn't yet gotten a real [Schema2].
#[derive(Debug)]
pub struct TodoSchema<T>(PhantomData<T>);

impl<T> Default for TodoSchema<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> PartialEq for TodoSchema<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Debug + Send + Sync> Schema2<T> for TodoSchema<T> {
    type ArrowColumn = StructArray;
    type Statistics = NoneStats;

    type Decoder = TodoColumnarDecoder<T>;
    type Encoder = TodoColumnarEncoder<T>;

    fn decoder(&self, _col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        panic!("TODO")
    }

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        panic!("TODO")
    }
}

/// A [`ColumnEncoder`] that has no implementation.
#[derive(Debug)]
pub struct TodoColumnarEncoder<T>(PhantomData<T>);

impl<T> ColumnEncoder<T> for TodoColumnarEncoder<T> {
    type FinishedColumn = StructArray;

    fn goodbytes(&self) -> usize {
        panic!("TODO")
    }

    fn append(&mut self, _val: &T) {
        panic!("TODO")
    }

    fn append_null(&mut self) {
        panic!("TODO")
    }

    fn finish(self) -> Self::FinishedColumn {
        panic!("TODO")
    }
}

/// A [`ColumnDecoder`] that has no implementation.
#[derive(Debug)]
pub struct TodoColumnarDecoder<T>(PhantomData<T>);

impl<T> ColumnDecoder<T> for TodoColumnarDecoder<T> {
    fn decode(&self, _idx: usize, _val: &mut T) {
        panic!("TODO")
    }

    fn is_null(&self, _idx: usize) -> bool {
        panic!("TODO")
    }

    fn goodbytes(&self) -> usize {
        panic!("TODO")
    }

    fn stats(&self) -> StructStats {
        panic!("TODO")
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;

    #[mz_ore::test]
    fn fmt_ids() {
        assert_eq!(
            format!("{}", ShardId([0u8; 16])),
            "s00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            format!("{:?}", ShardId([0u8; 16])),
            "ShardId(00000000-0000-0000-0000-000000000000)"
        );

        // ShardId can be parsed back from its Display/to_string format.
        assert_eq!(
            ShardId::from_str("s00000000-0000-0000-0000-000000000000"),
            Ok(ShardId([0u8; 16]))
        );
        assert_eq!(
            ShardId::from_str("x00000000-0000-0000-0000-000000000000"),
            Err(
                "invalid ShardId x00000000-0000-0000-0000-000000000000: incorrect prefix"
                    .to_string()
            )
        );
        assert_eq!(
            ShardId::from_str("s0"),
            Err(
                "invalid ShardId s0: invalid length: expected length 32 for simple format, found 1"
                    .to_string()
            )
        );
        assert_eq!(
            ShardId::from_str("s00000000-0000-0000-0000-000000000000FOO"),
            Err("invalid ShardId s00000000-0000-0000-0000-000000000000FOO: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `O` at 38".to_string())
        );
    }

    #[mz_ore::test]
    fn shard_id_human_readable_serde() {
        #[derive(Debug, Serialize, Deserialize)]
        struct ShardIdContainer {
            shard_id: ShardId,
        }

        // roundtrip id through json
        let id =
            ShardId::from_str("s00000000-1234-5678-0000-000000000000").expect("valid shard id");
        assert_eq!(
            id,
            serde_json::from_value(serde_json::to_value(id).expect("serializable"))
                .expect("deserializable")
        );

        // deserialize a serialized string directly
        assert_eq!(
            id,
            serde_json::from_str("\"s00000000-1234-5678-0000-000000000000\"")
                .expect("deserializable")
        );

        // roundtrip shard id through a container type
        let json = json!({ "shard_id": id });
        assert_eq!(
            "{\"shard_id\":\"s00000000-1234-5678-0000-000000000000\"}",
            &json.to_string()
        );
        let container: ShardIdContainer = serde_json::from_value(json).expect("deserializable");
        assert_eq!(container.shard_id, id);
    }
}
