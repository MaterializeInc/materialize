// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of [Codec] for stdlib types.

use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, BinaryArray, BinaryBuilder, BooleanArray, BooleanBufferBuilder,
    BooleanBuilder, NullArray, PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder,
    StructArray,
};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use bytes::BufMut;
use mz_ore::assert_none;
use timely::order::Product;

use crate::columnar::sealed::{ColumnMut, ColumnRef};
use crate::columnar::{
    ColumnCfg, ColumnDecoder, ColumnEncoder, ColumnFormat, ColumnGet, ColumnPush, Data, DataType,
    OpaqueData, PartDecoder, PartEncoder, Schema, Schema2,
};
use crate::dyn_col::DynColumnMut;
use crate::dyn_struct::{
    ColumnsMut, ColumnsRef, DynStruct, DynStructCfg, DynStructCol, DynStructMut, DynStructRef,
};
use crate::stats::{BytesStats, NoneStats, OptionStats, PrimitiveStats, StatsFn, StructStats};
use crate::txn::TxnsDataSchema;
use crate::{Codec, Codec64, Opaque, ShardId};

/// An implementation of [Schema] for [()].
#[derive(Debug, Default, PartialEq)]
pub struct UnitSchema;

/// [`PartEncoder`] for [`UnitSchema`].
#[derive(Debug)]
pub struct UnitSchemaEncoder {
    len: usize,
}

impl PartEncoder<()> for UnitSchemaEncoder {
    fn encode(&mut self, _val: &()) {
        self.len += 1;
    }

    fn finish(self) -> (usize, Vec<DynColumnMut>) {
        (self.len, vec![])
    }
}

impl PartDecoder<()> for UnitSchema {
    fn decode(&self, _idx: usize, _val: &mut ()) {}
}

impl Schema<()> for UnitSchema {
    type Encoder = UnitSchemaEncoder;
    type Decoder = Self;

    fn columns(&self) -> DynStructCfg {
        DynStructCfg::from(Vec::new())
    }

    fn decoder(&self, cols: ColumnsRef) -> Result<Self::Decoder, String> {
        let () = cols.finish()?;
        Ok(UnitSchema)
    }

    fn encoder(&self, cols: ColumnsMut) -> Result<Self::Encoder, String> {
        let (len, ()) = cols.finish()?;
        Ok(UnitSchemaEncoder { len })
    }
}

impl TxnsDataSchema for UnitSchema {}

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

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        if !buf.is_empty() {
            return Err(format!("decode expected empty buf got {} bytes", buf.len()));
        }
        Ok(())
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
}

impl ColumnEncoder<()> for UnitColumnar {
    type FinishedColumn = NullArray;
    type FinishedStats = NoneStats;

    fn append(&mut self, _val: &()) {
        self.len += 1;
    }

    fn append_null(&mut self) {
        self.len += 1;
    }

    fn finish(self) -> (Self::FinishedColumn, Self::FinishedStats) {
        (NullArray::new(self.len), NoneStats)
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

impl SimpleColumnarData for ShardId {
    type ArrowBuilder = StringBuilder;
    type ArrowColumn = StringArray;

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
    type FinishedStats = NoneStats;

    fn append(&mut self, val: &T) {
        T::push(val, &mut self.0);
    }
    fn append_null(&mut self) {
        T::push_null(&mut self.0)
    }
    fn finish(mut self) -> (Self::FinishedColumn, Self::FinishedStats) {
        let array = ArrayBuilder::finish(&mut self.0);
        let array = array
            .as_any()
            .downcast_ref::<T::ArrowColumn>()
            .expect("created using StringBuilder")
            .clone();

        (array, NoneStats)
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
}

/// An implementation of [PartEncoder] for a single column.
pub struct SimpleEncoder<X, T: Data>(usize, SimpleEncoderFn<X, T>);

impl<X, T: Data> fmt::Debug for SimpleEncoder<X, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleEncoder").finish()
    }
}

enum SimpleEncoderFn<X, T: Data> {
    Cast {
        col: Box<T::Mut>,
        encode: for<'b> fn(&'b X) -> T::Ref<'b>,
    },
    Push {
        col: Box<T::Mut>,
        encode: fn(&mut T::Mut, &X),
    },
}

impl<X, T: Data> PartEncoder<X> for SimpleEncoder<X, T> {
    fn encode(&mut self, val: &X) {
        self.0 += 1;
        match &mut self.1 {
            SimpleEncoderFn::Cast { col, encode } => {
                ColumnPush::<T>::push(col.as_mut(), encode(val))
            }
            SimpleEncoderFn::Push { col, encode } => encode(col, val),
        }
    }

    fn finish(self) -> (usize, Vec<DynColumnMut>) {
        let col = match self.1 {
            SimpleEncoderFn::Cast { col, .. } | SimpleEncoderFn::Push { col, .. } => col,
        };
        (self.0, vec![DynColumnMut::new::<T>(col)])
    }
}

/// An implementation of [PartDecoder] for a single column.
pub struct SimpleDecoder<X, T: Data> {
    col: Arc<T::Col>,
    decode: for<'a> fn(T::Ref<'a>, &mut X),
}

impl<X: Default, T: Data> PartDecoder<X> for SimpleDecoder<X, T> {
    fn decode(&self, idx: usize, val: &mut X) {
        (self.decode)(ColumnGet::<T>::get(self.col.as_ref(), idx), val)
    }
}

impl<X, T: Data> fmt::Debug for SimpleDecoder<X, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleDecoder")
            .field("col", &std::any::type_name::<T::Col>())
            .finish()
    }
}

/// A helper for writing Schemas of a single column.
pub struct SimpleSchema<X, T: Data>(PhantomData<(X, T)>);

// TODO: Feels like it should be possible to write a single impl of Schema to
// cover StringSchema, VecU8Schema, MaelstromKeySchema, etc in terms of
// ColumnRef and ColumnMut. We might have to pull in the rust experts though,
// the lifetimes get tricky.
impl<X, T: Data> SimpleSchema<X, T> {
    /// A helper for [Schema::columns] impls of a single column.
    pub fn columns(cfg: &T::Cfg) -> DynStructCfg {
        DynStructCfg::from(vec![("".to_owned(), cfg.as_type(), StatsFn::Default)])
    }

    /// A helper for [Schema::decoder] impls of a single column.
    pub fn decoder(
        mut cols: ColumnsRef,
        decode: for<'a> fn(T::Ref<'a>, &mut X),
    ) -> Result<SimpleDecoder<X, T>, String> {
        let col = cols.col::<T>("")?;
        let () = cols.finish()?;
        Ok(SimpleDecoder { col, decode })
    }

    /// A helper for [Schema::encoder] impls of a single column.
    pub fn encoder(
        mut cols: ColumnsMut,
        encode: for<'b> fn(&'b X) -> T::Ref<'b>,
    ) -> Result<SimpleEncoder<X, T>, String> {
        let col = cols.col::<T>("")?;
        let (len, ()) = cols.finish()?;
        Ok(SimpleEncoder(len, SimpleEncoderFn::Cast { col, encode }))
    }

    /// A helper for [Schema::encoder] impls of a single column.
    pub fn push_encoder(
        mut cols: ColumnsMut,
        encode: fn(&mut T::Mut, &X),
    ) -> Result<SimpleEncoder<X, T>, String> {
        let col = cols.col::<T>("")?;
        let (len, ()) = cols.finish()?;
        Ok(SimpleEncoder(len, SimpleEncoderFn::Push { col, encode }))
    }
}

/// An implementation of [Schema] for [String].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StringSchema;

impl Schema<String> for StringSchema {
    type Encoder = SimpleEncoder<String, String>;

    type Decoder = SimpleDecoder<String, String>;

    fn columns(&self) -> DynStructCfg {
        SimpleSchema::<String, String>::columns(&())
    }

    fn decoder(&self, cols: ColumnsRef) -> Result<Self::Decoder, String> {
        SimpleSchema::<String, String>::decoder(cols, |val, ret| val.clone_into(ret))
    }

    fn encoder(&self, cols: ColumnsMut) -> Result<Self::Encoder, String> {
        SimpleSchema::<String, String>::encoder(cols, |val| val.as_str())
    }
}

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

impl TxnsDataSchema for StringSchema {}

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

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        String::from_utf8(buf.to_owned()).map_err(|err| err.to_string())
    }
}

/// An implementation of [Schema] for [`Vec<u8>`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VecU8Schema;

impl Schema<Vec<u8>> for VecU8Schema {
    type Encoder = SimpleEncoder<Vec<u8>, Vec<u8>>;

    type Decoder = SimpleDecoder<Vec<u8>, Vec<u8>>;

    fn columns(&self) -> DynStructCfg {
        SimpleSchema::<Vec<u8>, Vec<u8>>::columns(&())
    }

    fn decoder(&self, cols: ColumnsRef) -> Result<Self::Decoder, String> {
        SimpleSchema::<Vec<u8>, Vec<u8>>::decoder(cols, |val, ret| val.clone_into(ret))
    }

    fn encoder(&self, cols: ColumnsMut) -> Result<Self::Encoder, String> {
        SimpleSchema::<Vec<u8>, Vec<u8>>::encoder(cols, |val| val.as_slice())
    }
}

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

impl Codec for Vec<u8> {
    type Storage = ();
    type Schema = VecU8Schema;

    fn codec_name() -> String {
        "Vec<u8>".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put(self.as_slice())
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        Ok(buf.to_owned())
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
    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let shard_id = String::from_utf8(buf.to_owned()).map_err(|err| err.to_string())?;
        shard_id.parse()
    }
}

/// An implementation of [Schema] for [ShardId].
#[derive(Debug)]
pub struct ShardIdSchema;

impl Schema<ShardId> for ShardIdSchema {
    type Encoder = SimpleEncoder<ShardId, String>;
    type Decoder = SimpleDecoder<ShardId, String>;

    fn columns(&self) -> DynStructCfg {
        SimpleSchema::<ShardId, String>::columns(&())
    }

    fn decoder(&self, cols: ColumnsRef) -> Result<Self::Decoder, String> {
        SimpleSchema::<ShardId, String>::decoder(cols, |val, ret| {
            *ret = val.parse().expect("should be valid ShardId")
        })
    }

    fn encoder(&self, cols: ColumnsMut) -> Result<Self::Encoder, String> {
        SimpleSchema::<ShardId, String>::push_encoder(cols, |col, val| {
            ColumnPush::<String>::push(col, &val.to_string())
        })
    }
}

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

impl Data for bool {
    type Cfg = ();
    type Ref<'a> = bool;
    type Col = BooleanBuffer;
    type Mut = BooleanBufferBuilder;
    type Stats = PrimitiveStats<bool>;
}

impl ColumnCfg<bool> for () {
    fn as_type(&self) -> DataType {
        DataType {
            optional: false,
            format: ColumnFormat::Bool,
        }
    }
}

impl Data for Option<bool> {
    type Cfg = ();
    type Ref<'a> = Option<bool>;
    type Col = BooleanArray;
    type Mut = BooleanBuilder;
    type Stats = OptionStats<PrimitiveStats<bool>>;
}

impl ColumnCfg<Option<bool>> for () {
    fn as_type(&self) -> DataType {
        DataType {
            optional: true,
            format: ColumnFormat::Bool,
        }
    }
}

macro_rules! data_primitive {
    ($data:ident, $arrow_type:ident, $format:expr) => {
        impl Data for $data {
            type Cfg = ();
            type Ref<'a> = $data;
            type Col = PrimitiveArray<$arrow_type>;
            type Mut = PrimitiveBuilder<$arrow_type>;
            type Stats = PrimitiveStats<$data>;
        }

        impl ColumnCfg<$data> for () {
            fn as_type(&self) -> DataType {
                DataType {
                    optional: false,
                    format: $format,
                }
            }
        }

        impl Data for Option<$data> {
            type Cfg = ();
            type Ref<'a> = Option<$data>;
            type Col = PrimitiveArray<$arrow_type>;
            type Mut = PrimitiveBuilder<$arrow_type>;
            type Stats = OptionStats<PrimitiveStats<$data>>;
        }

        impl ColumnCfg<Option<$data>> for () {
            fn as_type(&self) -> DataType {
                DataType {
                    optional: true,
                    format: $format,
                }
            }
        }
    };
}

data_primitive!(u8, UInt8Type, ColumnFormat::U8);
data_primitive!(u16, UInt16Type, ColumnFormat::U16);
data_primitive!(u32, UInt32Type, ColumnFormat::U32);
data_primitive!(u64, UInt64Type, ColumnFormat::U64);
data_primitive!(i8, Int8Type, ColumnFormat::I8);
data_primitive!(i16, Int16Type, ColumnFormat::I16);
data_primitive!(i32, Int32Type, ColumnFormat::I32);
data_primitive!(i64, Int64Type, ColumnFormat::I64);
data_primitive!(f32, Float32Type, ColumnFormat::F32);
data_primitive!(f64, Float64Type, ColumnFormat::F64);

impl Data for Vec<u8> {
    type Cfg = ();
    type Ref<'a> = &'a [u8];
    // TODO: Something that more obviously isn't optional.
    type Col = BinaryArray;
    type Mut = BinaryBuilder;
    type Stats = BytesStats;
}

impl ColumnCfg<Vec<u8>> for () {
    fn as_type(&self) -> DataType {
        DataType {
            optional: false,
            format: ColumnFormat::Bytes,
        }
    }
}

impl Data for Option<Vec<u8>> {
    type Cfg = ();
    type Ref<'a> = Option<&'a [u8]>;
    type Col = BinaryArray;
    type Mut = BinaryBuilder;
    type Stats = OptionStats<BytesStats>;
}

impl ColumnCfg<Option<Vec<u8>>> for () {
    fn as_type(&self) -> DataType {
        DataType {
            optional: true,
            format: ColumnFormat::Bytes,
        }
    }
}

impl Data for String {
    type Cfg = ();
    type Ref<'a> = &'a str;
    // TODO: Something that more obviously isn't optional.
    type Col = StringArray;
    type Mut = StringBuilder;
    type Stats = PrimitiveStats<String>;
}

impl ColumnCfg<String> for () {
    fn as_type(&self) -> DataType {
        DataType {
            optional: false,
            format: ColumnFormat::String,
        }
    }
}

impl Data for Option<String> {
    type Cfg = ();
    type Ref<'a> = Option<&'a str>;
    type Col = StringArray;
    type Mut = StringBuilder;
    type Stats = OptionStats<PrimitiveStats<String>>;
}

impl ColumnCfg<Option<String>> for () {
    fn as_type(&self) -> DataType {
        DataType {
            optional: true,
            format: ColumnFormat::String,
        }
    }
}

impl Data for DynStruct {
    type Cfg = DynStructCfg;
    type Ref<'a> = DynStructRef<'a>;
    type Col = DynStructCol;
    type Mut = DynStructMut;
    type Stats = StructStats;
}

impl ColumnCfg<DynStruct> for DynStructCfg {
    fn as_type(&self) -> DataType {
        DataType {
            optional: false,
            format: ColumnFormat::Struct(self.clone()),
        }
    }
}

impl Data for Option<DynStruct> {
    type Cfg = DynStructCfg;
    type Ref<'a> = Option<DynStructRef<'a>>;
    type Col = DynStructCol;
    type Mut = DynStructMut;
    type Stats = OptionStats<StructStats>;
}

impl ColumnCfg<Option<DynStruct>> for DynStructCfg {
    fn as_type(&self) -> DataType {
        DataType {
            optional: true,
            format: ColumnFormat::Struct(self.clone()),
        }
    }
}

impl Data for OpaqueData {
    type Cfg = ();
    type Ref<'a> = &'a [u8];
    type Col = BinaryArray;
    type Mut = BinaryBuilder;
    type Stats = NoneStats;
}

impl ColumnCfg<OpaqueData> for () {
    fn as_type(&self) -> DataType {
        DataType {
            optional: false,
            format: ColumnFormat::OpaqueData,
        }
    }
}

impl Data for Option<OpaqueData> {
    type Cfg = ();
    type Ref<'a> = Option<&'a [u8]>;
    type Col = BinaryArray;
    type Mut = BinaryBuilder;
    type Stats = OptionStats<NoneStats>;
}

impl ColumnCfg<Option<OpaqueData>> for () {
    fn as_type(&self) -> DataType {
        DataType {
            optional: true,
            format: ColumnFormat::OpaqueData,
        }
    }
}

impl ColumnRef<()> for BooleanBuffer {
    fn cfg(&self) -> &() {
        &()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn to_arrow(&self) -> Arc<dyn Array> {
        Arc::new(BooleanArray::new(self.clone(), None))
    }

    fn from_arrow(_cfg: &(), array: &dyn Array) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("expected BooleanArray but was {:?}", array.data_type()))?;
        Ok(array.values().clone())
    }
}

impl ColumnGet<bool> for BooleanBuffer {
    fn get<'a>(&'a self, idx: usize) -> bool {
        self.value(idx)
    }
}

impl ColumnPush<bool> for BooleanBufferBuilder {
    fn push<'a>(&mut self, val: bool) {
        <BooleanBufferBuilder>::append(self, val)
    }

    fn finish(mut self) -> <bool as Data>::Col {
        <BooleanBufferBuilder>::finish(&mut self)
    }
}

impl ColumnMut<()> for BooleanBufferBuilder {
    fn new(_cfg: &()) -> Self {
        // `BooleanBuilder` uses 1024 for its default capacity.
        BooleanBufferBuilder::new(1024)
    }

    fn cfg(&self) -> &() {
        &()
    }
}

impl ColumnRef<()> for BooleanArray {
    fn cfg(&self) -> &() {
        &()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn to_arrow(&self) -> Arc<dyn Array> {
        Arc::new(self.clone())
    }

    fn from_arrow(_cfg: &(), array: &dyn Array) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("expected BooleanArray but was {:?}", array.data_type()))?;
        Ok(array.clone())
    }
}

impl ColumnGet<Option<bool>> for BooleanArray {
    fn get<'a>(&'a self, idx: usize) -> Option<bool> {
        if self
            .logical_nulls()
            .map_or(true, |nulls| nulls.is_valid(idx))
        {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl ColumnMut<()> for BooleanBuilder {
    fn new(_cfg: &()) -> Self {
        BooleanBuilder::default()
    }

    fn cfg(&self) -> &() {
        &()
    }
}

impl ColumnPush<Option<bool>> for BooleanBuilder {
    fn push<'a>(&mut self, val: Option<bool>) {
        <BooleanBuilder>::append_option(self, val)
    }

    fn finish(mut self) -> <Option<bool> as Data>::Col {
        <BooleanBuilder>::finish(&mut self)
    }
}

macro_rules! arrowable_primitive {
    ($data:ident, $arrow_type:ident) => {
        impl ColumnGet<$data> for PrimitiveArray<$arrow_type> {
            fn get<'a>(&'a self, idx: usize) -> $data {
                assert!(self.is_valid(idx));
                self.value(idx)
            }
        }

        impl ColumnPush<$data> for PrimitiveBuilder<$arrow_type> {
            fn push<'a>(&mut self, val: $data) {
                <PrimitiveBuilder<$arrow_type>>::append_value(self, val)
            }

            fn finish(mut self) -> <$data as Data>::Col {
                <PrimitiveBuilder<$arrow_type>>::finish(&mut self)
            }
        }

        impl ColumnRef<()> for PrimitiveArray<$arrow_type> {
            fn cfg(&self) -> &() {
                &()
            }

            fn len(&self) -> usize {
                self.len()
            }

            fn to_arrow(&self) -> Arc<dyn Array> {
                Arc::new(self.clone())
            }

            fn from_arrow(_cfg: &(), array: &dyn Array) -> Result<Self, String> {
                let array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$arrow_type>>()
                    .ok_or_else(|| {
                        format!(
                            "expected {} but was {:?}",
                            std::any::type_name::<PrimitiveArray<$arrow_type>>(),
                            array.data_type()
                        )
                    })?;

                Ok(array.clone())
            }
        }

        impl ColumnGet<Option<$data>> for PrimitiveArray<$arrow_type> {
            fn get<'a>(&'a self, idx: usize) -> Option<$data> {
                if self
                    .logical_nulls()
                    .map_or(true, |nulls| nulls.is_valid(idx))
                {
                    Some(self.value(idx))
                } else {
                    None
                }
            }
        }

        impl ColumnMut<()> for PrimitiveBuilder<$arrow_type> {
            fn new(_cfg: &()) -> Self {
                PrimitiveBuilder::<$arrow_type>::default()
            }

            fn cfg(&self) -> &() {
                &()
            }
        }

        impl ColumnPush<Option<$data>> for PrimitiveBuilder<$arrow_type> {
            fn push<'a>(&mut self, val: Option<$data>) {
                <PrimitiveBuilder<$arrow_type>>::append_option(self, val)
            }

            fn finish(mut self) -> <Option<$data> as Data>::Col {
                <PrimitiveBuilder<$arrow_type>>::finish(&mut self)
            }
        }
    };
}

arrowable_primitive!(u8, UInt8Type);
arrowable_primitive!(u16, UInt16Type);
arrowable_primitive!(u32, UInt32Type);
arrowable_primitive!(u64, UInt64Type);
arrowable_primitive!(i8, Int8Type);
arrowable_primitive!(i16, Int16Type);
arrowable_primitive!(i32, Int32Type);
arrowable_primitive!(i64, Int64Type);
arrowable_primitive!(f32, Float32Type);
arrowable_primitive!(f64, Float64Type);

impl ColumnRef<()> for BinaryArray {
    fn cfg(&self) -> &() {
        &()
    }

    fn len(&self) -> usize {
        <BinaryArray as Array>::len(self)
    }

    fn to_arrow(&self) -> Arc<dyn Array> {
        Arc::new(self.clone())
    }

    fn from_arrow(_cfg: &(), array: &dyn Array) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| format!("expected BinaryArray but was {:?}", array.data_type()))?;
        Ok(array.clone())
    }
}

impl ColumnGet<Vec<u8>> for BinaryArray {
    fn get<'a>(&'a self, idx: usize) -> &'a [u8] {
        assert!(self.is_valid(idx));
        self.value(idx)
    }
}

impl ColumnGet<Option<Vec<u8>>> for BinaryArray {
    fn get<'a>(&'a self, idx: usize) -> Option<&'a [u8]> {
        if self
            .logical_nulls()
            .map_or(true, |nulls| nulls.is_valid(idx))
        {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl ColumnMut<()> for BinaryBuilder {
    fn new(_cfg: &()) -> Self {
        BinaryBuilder::default()
    }

    fn cfg(&self) -> &() {
        &()
    }
}

impl ColumnPush<Vec<u8>> for BinaryBuilder {
    fn push<'a>(&mut self, val: &'a [u8]) {
        assert_none!(self.validity_slice());
        <BinaryBuilder>::append_value(self, val)
    }

    fn finish(mut self) -> <Vec<u8> as Data>::Col {
        <BinaryBuilder>::finish(&mut self)
    }
}

impl ColumnPush<Option<Vec<u8>>> for BinaryBuilder {
    fn push<'a>(&mut self, val: Option<&'a [u8]>) {
        <BinaryBuilder>::append_option(self, val)
    }

    fn finish(mut self) -> <Vec<u8> as Data>::Col {
        <BinaryBuilder>::finish(&mut self)
    }
}

impl ColumnRef<()> for StringArray {
    fn cfg(&self) -> &() {
        &()
    }

    fn len(&self) -> usize {
        <StringArray as Array>::len(self)
    }

    fn to_arrow(&self) -> Arc<dyn Array> {
        Arc::new(self.clone())
    }

    fn from_arrow(_cfg: &(), array: &dyn Array) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| format!("expected StringArray but was {:?}", array.data_type()))?;
        Ok(array.clone())
    }
}

impl ColumnGet<String> for StringArray {
    fn get<'a>(&'a self, idx: usize) -> &'a str {
        assert!(self.is_valid(idx));
        self.value(idx)
    }
}

impl ColumnGet<Option<String>> for StringArray {
    fn get<'a>(&'a self, idx: usize) -> Option<&'a str> {
        if self
            .logical_nulls()
            .map_or(true, |nulls| nulls.is_valid(idx))
        {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl ColumnMut<()> for StringBuilder {
    fn new(_cfg: &()) -> Self {
        StringBuilder::default()
    }

    fn cfg(&self) -> &() {
        &()
    }
}

impl ColumnPush<String> for StringBuilder {
    fn push<'a>(&mut self, val: &'a str) {
        assert_none!(self.validity_slice());
        <StringBuilder>::append_value(self, val)
    }

    fn finish(mut self) -> <String as Data>::Col {
        <StringBuilder>::finish(&mut self)
    }
}

impl ColumnPush<Option<String>> for StringBuilder {
    fn push<'a>(&mut self, val: Option<&'a str>) {
        <StringBuilder>::append_option(self, val)
    }

    fn finish(mut self) -> <Option<String> as Data>::Col {
        <StringBuilder>::finish(&mut self)
    }
}

impl ColumnGet<OpaqueData> for BinaryArray {
    fn get<'a>(&'a self, idx: usize) -> <OpaqueData as Data>::Ref<'a> {
        assert!(self.is_valid(idx));
        self.value(idx)
    }
}

impl ColumnGet<Option<OpaqueData>> for BinaryArray {
    fn get<'a>(&'a self, idx: usize) -> <Option<OpaqueData> as Data>::Ref<'a> {
        if self
            .logical_nulls()
            .map_or(true, |nulls| nulls.is_valid(idx))
        {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl ColumnPush<OpaqueData> for BinaryBuilder {
    fn push<'a>(&mut self, val: <OpaqueData as Data>::Ref<'a>) {
        assert_none!(self.validity_slice());
        <BinaryBuilder>::append_value(self, val)
    }

    fn finish(mut self) -> <OpaqueData as Data>::Col {
        <BinaryBuilder>::finish(&mut self)
    }
}

impl ColumnPush<Option<OpaqueData>> for BinaryBuilder {
    fn push<'a>(&mut self, val: <Option<OpaqueData> as Data>::Ref<'a>) {
        <BinaryBuilder>::append_option(self, val)
    }

    fn finish(mut self) -> <Option<OpaqueData> as Data>::Col {
        <BinaryBuilder>::finish(&mut self)
    }
}

/// A placeholder for a [Codec] impl that hasn't yet gotten a real [Schema].
#[derive(Debug)]
pub struct TodoSchema<T>(PhantomData<T>);

impl<T> Default for TodoSchema<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> PartEncoder<T> for TodoSchema<T> {
    fn encode(&mut self, _val: &T) {
        panic!("TODO")
    }

    fn finish(self) -> (usize, Vec<DynColumnMut>) {
        panic!("TODO")
    }
}

impl<T> PartDecoder<T> for TodoSchema<T> {
    fn decode(&self, _idx: usize, _val: &mut T) {
        panic!("TODO")
    }
}

impl<T: Debug + Send + Sync> Schema<T> for TodoSchema<T> {
    type Encoder = Self;
    type Decoder = Self;

    fn columns(&self) -> DynStructCfg {
        panic!("TODO")
    }

    fn decoder(&self, _cols: ColumnsRef) -> Result<Self::Decoder, String> {
        panic!("TODO")
    }

    fn encoder(&self, _cols: ColumnsMut) -> Result<Self::Encoder, String> {
        panic!("TODO")
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
    type FinishedStats = NoneStats;

    fn append(&mut self, _val: &T) {
        panic!("TODO")
    }

    fn append_null(&mut self) {
        panic!("TODO")
    }

    fn finish(self) -> (Self::FinishedColumn, Self::FinishedStats) {
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
