// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of [Codec] for stdlib types.

use std::fmt::Debug;
use std::marker::PhantomData;

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
    ColumnFormat, ColumnGet, ColumnPush, Data, DataType, PartDecoder, PartEncoder, Schema,
};
use crate::part::{ColumnsMut, ColumnsRef};
use crate::stats::{BytesStats, OptionStats, PrimitiveStats, StatsFn};
use crate::{Codec, Codec64, Opaque};

/// An implementation of [Schema] for [()].
#[derive(Debug, Default)]
pub struct UnitSchema;

impl PartEncoder<'_, ()> for UnitSchema {
    fn encode(&mut self, _val: &()) {}
}

impl PartDecoder<'_, ()> for UnitSchema {
    fn decode(&self, _idx: usize, _val: &mut ()) {}
}

impl Schema<()> for UnitSchema {
    type Encoder<'a> = Self;
    type Decoder<'a> = Self;

    fn columns(&self) -> Vec<(String, DataType, StatsFn)> {
        Vec::new()
    }

    fn decoder<'a>(&self, cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        let () = cols.finish()?;
        Ok(UnitSchema)
    }

    fn encoder<'a>(&self, cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        let () = cols.finish()?;
        Ok(UnitSchema)
    }
}

impl Codec for () {
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
pub struct SimpleEncoder<'a, X, T: Data>(SimpleEncoderFn<'a, X, T>);

enum SimpleEncoderFn<'a, X, T: Data> {
    Cast {
        col: &'a mut T::Mut,
        encode: for<'b> fn(&'b X) -> T::Ref<'b>,
    },
    Push {
        col: &'a mut T::Mut,
        encode: fn(&mut T::Mut, &X),
    },
}

impl<'a, X, T: Data> PartEncoder<'a, X> for SimpleEncoder<'a, X, T> {
    fn encode(&mut self, val: &X) {
        match &mut self.0 {
            SimpleEncoderFn::Cast { col, encode } => ColumnPush::<T>::push(*col, encode(val)),
            SimpleEncoderFn::Push { col, encode } => encode(col, val),
        }
    }
}

/// An implementation of [PartDecoder] for a single column.
pub struct SimpleDecoder<'a, X, T: Data> {
    col: &'a T::Col,
    decode: fn(T::Ref<'a>, &mut X),
}

impl<'a, X, T: Data> PartDecoder<'a, X> for SimpleDecoder<'a, X, T> {
    fn decode(&self, idx: usize, val: &mut X) {
        (self.decode)(ColumnGet::<T>::get(self.col, idx), val)
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
    pub fn columns() -> Vec<(String, DataType, StatsFn)> {
        vec![("".to_owned(), T::TYPE, StatsFn::Default)]
    }

    /// A helper for [Schema::decoder] impls of a single column.
    pub fn decoder<'a>(
        mut cols: ColumnsRef<'a>,
        decode: fn(T::Ref<'a>, &mut X),
    ) -> Result<SimpleDecoder<'a, X, T>, String> {
        let col = cols.col::<T>("")?;
        let () = cols.finish()?;
        Ok(SimpleDecoder { col, decode })
    }

    /// A helper for [Schema::encoder] impls of a single column.
    pub fn encoder<'a>(
        mut cols: ColumnsMut<'a>,
        encode: for<'b> fn(&'b X) -> T::Ref<'b>,
    ) -> Result<SimpleEncoder<'a, X, T>, String> {
        let col = cols.col::<T>("")?;
        let () = cols.finish()?;
        Ok(SimpleEncoder(SimpleEncoderFn::Cast { col, encode }))
    }

    /// A helper for [Schema::encoder] impls of a single column.
    pub fn push_encoder<'a>(
        mut cols: ColumnsMut<'a>,
        encode: fn(&mut T::Mut, &X),
    ) -> Result<SimpleEncoder<'a, X, T>, String> {
        let col = cols.col::<T>("")?;
        let () = cols.finish()?;
        Ok(SimpleEncoder(SimpleEncoderFn::Push { col, encode }))
    }
}

/// An implementation of [Schema] for [String].
#[derive(Debug, Clone, Default)]
pub struct StringSchema;

impl Schema<String> for StringSchema {
    type Encoder<'a> = SimpleEncoder<'a, String, String>;

    type Decoder<'a> = SimpleDecoder<'a, String, String>;

    fn columns(&self) -> Vec<(String, DataType, StatsFn)> {
        SimpleSchema::<String, String>::columns()
    }

    fn decoder<'a>(&self, cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        SimpleSchema::<String, String>::decoder(cols, |val, ret| val.clone_into(ret))
    }

    fn encoder<'a>(&self, cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        SimpleSchema::<String, String>::encoder(cols, |val| val.as_str())
    }
}

impl Codec for String {
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
    type Encoder<'a> = SimpleEncoder<'a, Vec<u8>, Vec<u8>>;

    type Decoder<'a> = SimpleDecoder<'a, Vec<u8>, Vec<u8>>;

    fn columns(&self) -> Vec<(String, DataType, StatsFn)> {
        SimpleSchema::<Vec<u8>, Vec<u8>>::columns()
    }

    fn decoder<'a>(&self, cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        SimpleSchema::<Vec<u8>, Vec<u8>>::decoder(cols, |val, ret| val.clone_into(ret))
    }

    fn encoder<'a>(&self, cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        SimpleSchema::<Vec<u8>, Vec<u8>>::encoder(cols, |val| val.as_slice())
    }
}

impl Codec for Vec<u8> {
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
    const TYPE: DataType = DataType {
        optional: false,
        format: ColumnFormat::Bool,
    };
    type Ref<'a> = bool;
    type Col = Bitmap;
    type Mut = MutableBitmap;
    type Stats = PrimitiveStats<bool>;
}

impl Data for Option<bool> {
    const TYPE: DataType = DataType {
        optional: true,
        format: ColumnFormat::Bool,
    };
    type Ref<'a> = Option<bool>;
    type Col = BooleanArray;
    type Mut = MutableBooleanArray;
    type Stats = OptionStats<PrimitiveStats<bool>>;
}

macro_rules! data_primitive {
    ($data:ident, $format:expr) => {
        impl Data for $data {
            const TYPE: DataType = DataType {
                optional: false,
                format: $format,
            };
            type Ref<'a> = $data;
            type Col = Buffer<$data>;
            type Mut = Vec<$data>;
            type Stats = PrimitiveStats<$data>;
        }

        impl Data for Option<$data> {
            const TYPE: DataType = DataType {
                optional: true,
                format: $format,
            };
            type Ref<'a> = Option<$data>;
            type Col = PrimitiveArray<$data>;
            type Mut = MutablePrimitiveArray<$data>;
            type Stats = OptionStats<PrimitiveStats<$data>>;
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
    const TYPE: DataType = DataType {
        optional: false,
        format: ColumnFormat::Bytes,
    };
    type Ref<'a> = &'a [u8];
    // TODO: Something that more obviously isn't optional.
    type Col = BinaryArray<i32>;
    type Mut = MutableBinaryArray<i32>;
    type Stats = BytesStats;
}

impl Data for Option<Vec<u8>> {
    const TYPE: DataType = DataType {
        optional: true,
        format: ColumnFormat::Bytes,
    };
    type Ref<'a> = Option<&'a [u8]>;
    type Col = BinaryArray<i32>;
    type Mut = MutableBinaryArray<i32>;
    type Stats = OptionStats<BytesStats>;
}

impl Data for String {
    const TYPE: DataType = DataType {
        optional: false,
        format: ColumnFormat::String,
    };
    type Ref<'a> = &'a str;
    // TODO: Something that more obviously isn't optional.
    type Col = Utf8Array<i32>;
    type Mut = MutableUtf8Array<i32>;
    type Stats = PrimitiveStats<String>;
}

impl Data for Option<String> {
    const TYPE: DataType = DataType {
        optional: true,
        format: ColumnFormat::String,
    };
    type Ref<'a> = Option<&'a str>;
    type Col = Utf8Array<i32>;
    type Mut = MutableUtf8Array<i32>;
    type Stats = OptionStats<PrimitiveStats<String>>;
}

impl ColumnRef for Bitmap {
    fn len(&self) -> usize {
        self.len()
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        let array = BooleanArray::new(ArrowLogicalType::Boolean, self.clone(), None);
        (Encoding::Plain, Box::new(array))
    }
    fn from_arrow(array: &Box<dyn Array>) -> Result<Self, String> {
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

impl ColumnRef for BooleanArray {
    fn len(&self) -> usize {
        self.len()
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        (Encoding::Plain, Box::new(self.clone()))
    }
    fn from_arrow(array: &Box<dyn Array>) -> Result<Self, String> {
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
        impl ColumnRef for Buffer<$data> {
            fn len(&self) -> usize {
                self.len()
            }
            fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
                let array = PrimitiveArray::new($data::PRIMITIVE.into(), self.clone(), None);
                ($encoding, Box::new(array.clone()))
            }
            fn from_arrow(array: &Box<dyn Array>) -> Result<Self, String> {
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

        impl ColumnRef for PrimitiveArray<$data> {
            fn len(&self) -> usize {
                self.len()
            }
            fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
                ($encoding, Box::new(self.clone()))
            }
            fn from_arrow(array: &Box<dyn Array>) -> Result<Self, String> {
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

impl ColumnRef for BinaryArray<i32> {
    fn len(&self) -> usize {
        self.len()
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        (Encoding::Plain, Box::new(self.clone()))
    }
    fn from_arrow(array: &Box<dyn Array>) -> Result<Self, String> {
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

impl ColumnRef for Utf8Array<i32> {
    fn len(&self) -> usize {
        self.len()
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        (Encoding::Plain, Box::new(self.clone()))
    }
    fn from_arrow(array: &Box<dyn Array>) -> Result<Self, String> {
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

/// A placeholder for a [Codec] impl that hasn't yet gotten a real [Schema].
#[derive(Debug)]
pub struct TodoSchema<T>(PhantomData<T>);

impl<T> Default for TodoSchema<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> PartEncoder<'_, T> for TodoSchema<T> {
    fn encode(&mut self, _val: &T) {
        panic!("TODO")
    }
}

impl<T> PartDecoder<'_, T> for TodoSchema<T> {
    fn decode(&self, _idx: usize, _val: &mut T) {
        panic!("TODO")
    }
}

impl<T: Debug + Send + Sync> Schema<T> for TodoSchema<T> {
    type Encoder<'a> = Self;
    type Decoder<'a> = Self;

    fn columns(&self) -> Vec<(String, DataType, StatsFn)> {
        panic!("TODO")
    }

    fn decoder<'a>(&self, _cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        panic!("TODO")
    }

    fn encoder<'a>(&self, _cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        panic!("TODO")
    }
}
