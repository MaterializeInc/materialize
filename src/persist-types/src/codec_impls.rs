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

use arrow2::array::{
    Array, BinaryArray, BooleanArray, MutableArray, MutableBinaryArray, MutableBooleanArray,
    MutablePrimitiveArray, MutableUtf8Array, PrimitiveArray, Utf8Array,
};
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::buffer::Buffer;
use arrow2::datatypes::DataType as ArrowLogicalType;
use arrow2::io::parquet::write::Encoding;
use arrow2::types::NativeType;
use bytes::BufMut;

use crate::columnar::sealed::ColumnRef;
use crate::columnar::{
    ColumnCfg, ColumnFormat, ColumnGet, ColumnPush, Data, DataType, OpaqueData, PartDecoder,
    PartEncoder, Schema,
};
use crate::dyn_col::DynColumnMut;
use crate::dyn_struct::{
    ColumnsMut, ColumnsRef, DynStruct, DynStructCfg, DynStructCol, DynStructMut, DynStructRef,
};
use crate::stats::{BytesStats, NoneStats, OptionStats, PrimitiveStats, StatsFn, StructStats};
use crate::{Codec, Codec64, Opaque, ShardId};

/// Static instance of [`UnitSchema`] provided for convenience.
pub static UNIT_SCHEMA: UnitSchema = UnitSchema;

/// An implementation of [Schema] for [()].
#[derive(Debug, Default)]
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
    fn decode(&self, _idx: usize) {}
    fn decode_into(&self, _idx: usize, _val: &mut ()) {}
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
    fn decode(&self, idx: usize) -> X {
        let mut val = X::default();
        self.decode_into(idx, &mut val);
        val
    }

    fn decode_into(&self, idx: usize, val: &mut X) {
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
#[derive(Debug, Clone, Default)]
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
#[derive(Debug, Clone, Default)]
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
    type Col = Bitmap;
    type Mut = MutableBitmap;
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
    type Mut = MutableBooleanArray;
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
    ($data:ident, $format:expr) => {
        impl Data for $data {
            type Cfg = ();
            type Ref<'a> = $data;
            type Col = Buffer<$data>;
            type Mut = Vec<$data>;
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
            type Col = PrimitiveArray<$data>;
            type Mut = MutablePrimitiveArray<$data>;
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

data_primitive!(u8, ColumnFormat::U8);
data_primitive!(u16, ColumnFormat::U16);
data_primitive!(u32, ColumnFormat::U32);
data_primitive!(u64, ColumnFormat::U64);
data_primitive!(i8, ColumnFormat::I8);
data_primitive!(i16, ColumnFormat::I16);
data_primitive!(i32, ColumnFormat::I32);
data_primitive!(i64, ColumnFormat::I64);
data_primitive!(f32, ColumnFormat::F32);
data_primitive!(f64, ColumnFormat::F64);

impl Data for Vec<u8> {
    type Cfg = ();
    type Ref<'a> = &'a [u8];
    // TODO: Something that more obviously isn't optional.
    type Col = BinaryArray<i32>;
    type Mut = MutableBinaryArray<i32>;
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
    type Col = BinaryArray<i32>;
    type Mut = MutableBinaryArray<i32>;
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
    type Col = Utf8Array<i32>;
    type Mut = MutableUtf8Array<i32>;
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
    type Col = Utf8Array<i32>;
    type Mut = MutableUtf8Array<i32>;
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
    type Col = BinaryArray<i32>;
    type Mut = MutableBinaryArray<i32>;
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
    type Col = BinaryArray<i32>;
    type Mut = MutableBinaryArray<i32>;
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

impl ColumnRef<()> for Bitmap {
    fn cfg(&self) -> &() {
        &()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        let array = BooleanArray::new(ArrowLogicalType::Boolean, self.clone(), None);
        (Encoding::Plain, Box::new(array))
    }
    fn from_arrow(_cfg: &(), array: &Box<dyn Array>) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("expected BooleanArray but was {:?}", array.data_type()))?;
        if array.validity().is_some() {
            return Err("unexpected validity for non-optional bool".to_owned());
        }
        Ok(array.values().clone())
    }
}

impl ColumnGet<bool> for Bitmap {
    fn get<'a>(&'a self, idx: usize) -> bool {
        self.get_bit(idx)
    }
}

impl ColumnPush<bool> for MutableBitmap {
    fn push<'a>(&mut self, val: bool) {
        <MutableBitmap>::push(self, val)
    }
}

impl ColumnRef<()> for BooleanArray {
    fn cfg(&self) -> &() {
        &()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        (Encoding::Plain, Box::new(self.clone()))
    }
    fn from_arrow(_cfg: &(), array: &Box<dyn Array>) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| format!("expected BooleanArray but was {:?}", array.data_type()))?;
        Ok(array.clone())
    }
}

impl ColumnGet<Option<bool>> for BooleanArray {
    fn get<'a>(&'a self, idx: usize) -> Option<bool> {
        if self.validity().map_or(true, |x| x.get_bit(idx)) {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl ColumnPush<Option<bool>> for MutableBooleanArray {
    fn push<'a>(&mut self, val: Option<bool>) {
        <MutableBooleanArray>::push(self, val)
    }
}

macro_rules! arrowable_primitive {
    ($data:ident, $encoding:expr) => {
        impl ColumnRef<()> for Buffer<$data> {
            fn cfg(&self) -> &() {
                &()
            }
            fn len(&self) -> usize {
                self.len()
            }
            fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
                let array = PrimitiveArray::new($data::PRIMITIVE.into(), self.clone(), None);
                ($encoding, Box::new(array.clone()))
            }
            fn from_arrow(_cfg: &(), array: &Box<dyn Array>) -> Result<Self, String> {
                let array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$data>>()
                    .ok_or_else(|| {
                        format!(
                            "expected {} but was {:?}",
                            std::any::type_name::<PrimitiveArray<$data>>(),
                            array.data_type()
                        )
                    })?;
                if array.validity().is_some() {
                    return Err(format!(
                        "unexpected validity for non-optional {}",
                        std::any::type_name::<$data>()
                    ));
                }
                Ok(array.values().clone())
            }
        }

        impl ColumnGet<$data> for Buffer<$data> {
            fn get<'a>(&'a self, idx: usize) -> $data {
                self[idx]
            }
        }

        impl ColumnPush<$data> for Vec<$data> {
            fn push<'a>(&mut self, val: $data) {
                <Vec<$data>>::push(self, val)
            }
        }

        impl ColumnRef<()> for PrimitiveArray<$data> {
            fn cfg(&self) -> &() {
                &()
            }
            fn len(&self) -> usize {
                self.len()
            }
            fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
                ($encoding, Box::new(self.clone()))
            }
            fn from_arrow(_cfg: &(), array: &Box<dyn Array>) -> Result<Self, String> {
                let array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$data>>()
                    .ok_or_else(|| {
                        format!(
                            "expected {} but was {:?}",
                            std::any::type_name::<PrimitiveArray<$data>>(),
                            array.data_type()
                        )
                    })?;
                Ok(array.clone())
            }
        }

        impl ColumnGet<Option<$data>> for PrimitiveArray<$data> {
            fn get<'a>(&'a self, idx: usize) -> Option<$data> {
                if self.validity().map_or(true, |x| x.get_bit(idx)) {
                    Some(self.value(idx))
                } else {
                    None
                }
            }
        }

        impl ColumnPush<Option<$data>> for MutablePrimitiveArray<$data> {
            fn push<'a>(&mut self, val: Option<$data>) {
                <MutablePrimitiveArray<$data>>::push(self, val)
            }
        }
    };
}

arrowable_primitive!(u8, Encoding::Plain);
arrowable_primitive!(u16, Encoding::Plain);
arrowable_primitive!(u32, Encoding::Plain);
arrowable_primitive!(u64, Encoding::Plain);
arrowable_primitive!(i8, Encoding::Plain);
arrowable_primitive!(i16, Encoding::Plain);
arrowable_primitive!(i32, Encoding::Plain);
arrowable_primitive!(i64, Encoding::Plain);
arrowable_primitive!(f32, Encoding::Plain);
arrowable_primitive!(f64, Encoding::Plain);

impl ColumnRef<()> for BinaryArray<i32> {
    fn cfg(&self) -> &() {
        &()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        (Encoding::Plain, Box::new(self.clone()))
    }
    fn from_arrow(_cfg: &(), array: &Box<dyn Array>) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<BinaryArray<i32>>()
            .ok_or_else(|| format!("expected BinaryArray<i32> but was {:?}", array.data_type()))?;
        Ok(array.clone())
    }
}

impl ColumnGet<Vec<u8>> for BinaryArray<i32> {
    fn get<'a>(&'a self, idx: usize) -> &'a [u8] {
        assert!(self.validity().is_none());
        self.value(idx)
    }
}

impl ColumnGet<Option<Vec<u8>>> for BinaryArray<i32> {
    fn get<'a>(&'a self, idx: usize) -> Option<&'a [u8]> {
        if self.validity().map_or(true, |x| x.get_bit(idx)) {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl ColumnPush<Vec<u8>> for MutableBinaryArray<i32> {
    fn push<'a>(&mut self, val: &'a [u8]) {
        assert!(self.validity().is_none());
        <MutableBinaryArray<i32>>::push(self, Some(val))
    }
}

impl ColumnPush<Option<Vec<u8>>> for MutableBinaryArray<i32> {
    fn push<'a>(&mut self, val: Option<&'a [u8]>) {
        <MutableBinaryArray<i32>>::push(self, val)
    }
}

impl ColumnRef<()> for Utf8Array<i32> {
    fn cfg(&self) -> &() {
        &()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        (Encoding::Plain, Box::new(self.clone()))
    }
    fn from_arrow(_cfg: &(), array: &Box<dyn Array>) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| format!("expected Utf8Array<i32> but was {:?}", array.data_type()))?;
        Ok(array.clone())
    }
}

impl ColumnGet<String> for Utf8Array<i32> {
    fn get<'a>(&'a self, idx: usize) -> &'a str {
        assert!(self.validity().is_none());
        self.value(idx)
    }
}

impl ColumnGet<Option<String>> for Utf8Array<i32> {
    fn get<'a>(&'a self, idx: usize) -> Option<&'a str> {
        if self.validity().map_or(true, |x| x.get_bit(idx)) {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl ColumnPush<String> for MutableUtf8Array<i32> {
    fn push<'a>(&mut self, val: &'a str) {
        assert!(self.validity().is_none());
        <MutableUtf8Array<i32>>::push(self, Some(val))
    }
}

impl ColumnPush<Option<String>> for MutableUtf8Array<i32> {
    fn push<'a>(&mut self, val: Option<&'a str>) {
        <MutableUtf8Array<i32>>::push(self, val)
    }
}

impl ColumnGet<OpaqueData> for BinaryArray<i32> {
    fn get<'a>(&'a self, idx: usize) -> <OpaqueData as Data>::Ref<'a> {
        assert!(self.validity().is_none());
        self.value(idx)
    }
}

impl ColumnGet<Option<OpaqueData>> for BinaryArray<i32> {
    fn get<'a>(&'a self, idx: usize) -> <Option<OpaqueData> as Data>::Ref<'a> {
        if self.validity().map_or(true, |x| x.get_bit(idx)) {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl ColumnPush<OpaqueData> for MutableBinaryArray<i32> {
    fn push<'a>(&mut self, val: <OpaqueData as Data>::Ref<'a>) {
        assert!(self.validity().is_none());
        <MutableBinaryArray<i32>>::push(self, Some(val))
    }
}

impl ColumnPush<Option<OpaqueData>> for MutableBinaryArray<i32> {
    fn push<'a>(&mut self, val: <Option<OpaqueData> as Data>::Ref<'a>) {
        <MutableBinaryArray<i32>>::push(self, val)
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
    fn decode(&self, _idx: usize) -> T {
        panic!("TODO")
    }

    fn decode_into(&self, _idx: usize, _val: &mut T) {
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
