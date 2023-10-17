// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides fixed-length representations for data composed of `Datum`s of fixed-length types.
//! These representations are aimed at being more efficient in memory usage than `Row` by
//! relying on statically selected container lengths. Traits are provided that allow these
//! representations to be made into instances of `Row` or created from `Row`s. The traits are
//! trivially implemented for `Row` itself, providing a uniform interface to describe `Row`s
//! or fixed-length containers standing in for them.

use std::borrow::Borrow;
use std::iter;
use std::mem::size_of;

use chrono::{DateTime, Utc};
use mz_persist_types::Codec64;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use timely::container::columnation::{Columnation, CopyRegion};
use uuid::Uuid;

use crate::adt::interval::Interval;
use crate::adt::mz_acl_item::{AclItem, MzAclItem};
use crate::adt::timestamp::CheckedTimestamp;
use crate::row::{
    date_to_array, naive_date_to_arrays, read_byte_array, read_date, read_naive_date, read_time,
    time_to_arrays,
};
use crate::{ColumnType, Datum, Row, ScalarType, Timestamp};

/// A typedef to ease use of 9-byte fixed-length representations.
pub type Bytes9 = BytesN<9>;

/// A helper trait to get references to `Row` based on type information that only manifests
/// at runtime (typically originating from inferred schemas).
pub trait IntoRowByTypes: Sized {
    /// Obtains a reference to `Row` for an instance of `Self`, given a `Row` buffer and
    /// a schema provided by `types`.
    ///
    /// Implementations are free to not use `row_buf` if a zero-copy implementation
    /// can be provided. If the Row buffer is used, then the reference returned is to it.
    /// Implementations are also free to place specific requirements on the given schema.
    fn into_row<'a>(&'a self, row_buf: &'a mut Row, types: Option<&[ColumnType]>) -> &'a Row;
}

// Blanket identity implementation for Row.
impl IntoRowByTypes for Row {
    /// Performs a zero-copy reference forwarding without employing the `Row` buffer.
    ///
    /// This implementation panics if `types` other than `None` are provided. This is because
    /// `Row` is already self-describing and can use variable-length types, so we are
    /// explicitly not validating the given schema.
    fn into_row<'a>(&'a self, _row_buf: &'a mut Row, types: Option<&[ColumnType]>) -> &'a Row {
        assert!(types.is_none());
        self
    }
}

/// A helper trait to construct target values from input `Row` instances based on type
/// information that only manifests at runtime (typically originating from inferred schemas).
/// Typically, the target type will be of fixed-length without tags per column or `Row` itself.
pub trait FromRowByTypes: Sized {
    /// Obtains an instance of `Self` given an instance of `Row` and a schema provided
    /// by `types`.
    /// Implementations are free to place specific requirements on the given schema.
    fn from_row(row: Row, types: Option<&[ColumnType]>) -> Self {
        let iter = row.iter();
        Self::from_datum_iter(iter, types)
    }

    /// Obtains an instance of `Self' given an iterator of borrowed datums and a schema
    /// provided by `types`.
    /// Implementations are free to place specific requirements on the given schema.
    fn from_datum_iter<'a, I, D>(datum_iter: I, types: Option<&[ColumnType]>) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>;
}

impl FromRowByTypes for Row {
    /// Returns the provided row itself.
    ///
    /// This implementation panics if `types` other than `None` provided. This is because
    /// `Row` is already self-describing and can use variable-length types, so we are
    /// explicitly not validating the given schema.
    fn from_row(row: Row, types: Option<&[ColumnType]>) -> Self {
        assert!(types.is_none());
        row
    }

    /// Packs a `Row` from the given iterator of datums.
    ///
    /// This implementation panics if non-empty `types` are provided. This is because
    /// `Row` is already self-describing and can use variable-length types, so we are
    /// explicitly not validating the given schema.
    fn from_datum_iter<'a, I, D>(datum_iter: I, types: Option<&[ColumnType]>) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        assert!(types.is_none());
        let mut row = Row::default();
        row.packer().extend(datum_iter);
        row
    }
}

/// A representation of fixed-length values with N bytes, wherein one byte
/// is reserved for nullability encoding.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BytesN<const N: usize> {
    #[serde(with = "BigArray")]
    data: [u8; N],
}

impl<const N: usize> Columnation for BytesN<N> {
    type InnerRegion = CopyRegion<BytesN<N>>;
}

#[inline(always)]
fn copy_into(dst: &mut [u8], offset: &mut usize, src: &[u8]) {
    let dst = &mut dst[*offset..*offset + src.len()];
    dst.copy_from_slice(src);
    *offset = *offset + src.len();
}

#[inline(always)]
fn encoded_size_bool() -> usize {
    size_of::<u8>()
}

#[inline(always)]
fn encode_bool(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    data[*offset] = if datum.unwrap_bool() { 1u8 } else { 0u8 };
}

#[inline(always)]
fn decode_bool<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let b = u8::from_le_bytes(read_byte_array(data, offset)) != 0;
    if b {
        Datum::True
    } else {
        Datum::False
    }
}

#[inline(always)]
fn encoded_size_int16() -> usize {
    size_of::<i16>()
}

#[inline(always)]
fn encode_int16(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_int16();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_int16<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let n = i16::from_le_bytes(read_byte_array(data, offset));
    Datum::Int16(n)
}

#[inline(always)]
fn encoded_size_int32() -> usize {
    size_of::<i32>()
}

#[inline(always)]
fn encode_int32(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_int32();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_int32<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let n = i32::from_le_bytes(read_byte_array(data, offset));
    Datum::Int32(n)
}

#[inline(always)]
fn encoded_size_int64() -> usize {
    size_of::<i64>()
}

#[inline(always)]
fn encode_int64(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_int64();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_int64<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let n = i64::from_le_bytes(read_byte_array(data, offset));
    Datum::Int64(n)
}

#[inline(always)]
fn encoded_size_uint16() -> usize {
    size_of::<u16>()
}

#[inline(always)]
fn encode_uint16(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_uint16();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_uint16<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let n = u16::from_le_bytes(read_byte_array(data, offset));
    Datum::UInt16(n)
}

#[inline(always)]
fn encoded_size_uint32() -> usize {
    size_of::<u32>()
}

#[inline(always)]
fn encode_uint32(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_uint32();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_uint32<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let n = u32::from_le_bytes(read_byte_array(data, offset));
    Datum::UInt32(n)
}

#[inline(always)]
fn encoded_size_uint64() -> usize {
    size_of::<u64>()
}

#[inline(always)]
fn encode_uint64(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_uint64();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_uint64<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let n = u64::from_le_bytes(read_byte_array(data, offset));
    Datum::UInt64(n)
}

#[inline(always)]
fn encoded_size_float32() -> usize {
    size_of::<f32>()
}

#[inline(always)]
fn encode_float32(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_float32();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_float32<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let f = f32::from_le_bytes(read_byte_array(data, offset));
    let n = OrderedFloat::from(f);
    Datum::Float32(n)
}

#[inline(always)]
fn encoded_size_float64() -> usize {
    size_of::<f64>()
}

#[inline(always)]
fn encode_float64(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_float64();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_float64<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let f = f64::from_le_bytes(read_byte_array(data, offset));
    let n = OrderedFloat::from(f);
    Datum::Float64(n)
}

#[inline(always)]
fn encoded_size_date() -> usize {
    size_of::<i32>()
}

#[inline(always)]
fn encode_date(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let d = datum.unwrap_date();
    copy_into(data, offset, &date_to_array(d));
}

#[inline(always)]
fn decode_date<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let d = read_date(data, offset);
    Datum::Date(d)
}

#[inline(always)]
fn encoded_size_time() -> usize {
    2 * size_of::<u32>()
}

#[inline(always)]
fn encode_time(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let t = datum.unwrap_time();
    let (ts1, ts2) = time_to_arrays(t);
    copy_into(data, offset, &ts1);
    copy_into(data, offset, &ts2);
}

#[inline(always)]
fn decode_time<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let t = read_time(data, offset);
    Datum::Time(t)
}

#[inline(always)]
fn encoded_size_timestamp() -> usize {
    3 * size_of::<u32>() + size_of::<i32>()
}

#[inline(always)]
fn encode_timestamp(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let dt = datum.unwrap_timestamp();
    let d = dt.date();
    let (ds1, ds2) = naive_date_to_arrays(d);
    copy_into(data, offset, &ds1);
    copy_into(data, offset, &ds2);

    let t = dt.time();
    let (ts1, ts2) = time_to_arrays(t);
    copy_into(data, offset, &ts1);
    copy_into(data, offset, &ts2);
}

#[inline(always)]
fn decode_timestamp<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let d = read_naive_date(data, offset);
    let t = read_time(data, offset);
    Datum::Timestamp(
        CheckedTimestamp::from_timestamplike(d.and_time(t)).expect("unexpected timestamp"),
    )
}

#[inline(always)]
fn encoded_size_timestamptz() -> usize {
    3 * size_of::<u32>() + size_of::<i32>()
}

#[inline(always)]
fn encode_timestamptz(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let dt = datum.unwrap_timestamptz().to_naive();
    let d = dt.date();
    let (ds1, ds2) = naive_date_to_arrays(d);
    copy_into(data, offset, &ds1);
    copy_into(data, offset, &ds2);

    let t = dt.time();
    let (ts1, ts2) = time_to_arrays(t);
    copy_into(data, offset, &ts1);
    copy_into(data, offset, &ts2);
}

#[inline(always)]
fn decode_timestamptz<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let d = read_naive_date(data, offset);
    let t = read_time(data, offset);
    Datum::TimestampTz(
        CheckedTimestamp::from_timestamplike(DateTime::from_utc(d.and_time(t), Utc))
            .expect("unexpected timestamptz"),
    )
}

#[inline(always)]
fn encoded_size_interval() -> usize {
    2 * size_of::<i32>() + size_of::<i64>()
}

#[inline(always)]
fn encode_interval(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let i = datum.unwrap_interval();
    copy_into(data, offset, &i.months.to_le_bytes());
    copy_into(data, offset, &i.days.to_le_bytes());
    copy_into(data, offset, &i.micros.to_le_bytes());
}

#[inline(always)]
fn decode_interval<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let months = i32::from_le_bytes(read_byte_array(data, offset));
    let days = i32::from_le_bytes(read_byte_array(data, offset));
    let micros = i64::from_le_bytes(read_byte_array(data, offset));
    Datum::Interval(Interval {
        months,
        days,
        micros,
    })
}

#[inline(always)]
fn encoded_size_uuid() -> usize {
    size_of::<uuid::Bytes>()
}

#[inline(always)]
fn encode_uuid(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let u = datum.unwrap_uuid();
    copy_into(data, offset, u.as_bytes());
}

#[inline(always)]
fn decode_uuid<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let uuid = Uuid::from_bytes(read_byte_array(data, offset));
    Datum::Uuid(uuid)
}

#[inline(always)]
fn encoded_size_oid() -> usize {
    size_of::<u32>()
}

#[inline(always)]
fn encode_oid(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let n = datum.unwrap_uint32();
    copy_into(data, offset, &n.to_le_bytes());
}

#[inline(always)]
fn decode_oid<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let n = u32::from_le_bytes(read_byte_array(data, offset));
    Datum::UInt32(n)
}

#[inline(always)]
fn encoded_size_mz_timestamp() -> usize {
    size_of::<Timestamp>()
}

#[inline(always)]
fn encode_mz_timestamp(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let t = datum.unwrap_mz_timestamp();
    copy_into(data, offset, &t.encode());
}

#[inline(always)]
fn decode_mz_timestamp<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    let t = Timestamp::decode(read_byte_array(data, offset));
    Datum::MzTimestamp(t)
}

#[inline(always)]
fn encoded_size_mz_acl_item() -> usize {
    MzAclItem::binary_size()
}

#[inline(always)]
fn encode_mz_acl_item(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let i = datum.unwrap_mz_acl_item();
    copy_into(data, offset, &i.encode_binary());
}

#[inline(always)]
fn decode_mz_acl_item<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    const N: usize = MzAclItem::binary_size();
    let mz_acl_item =
        MzAclItem::decode_binary(&read_byte_array::<N>(data, offset)).expect("invalid mz_aclitem");
    Datum::MzAclItem(mz_acl_item)
}

#[inline(always)]
fn encoded_size_acl_item() -> usize {
    AclItem::binary_size()
}

#[inline(always)]
fn encode_acl_item(datum: &Datum, data: &mut [u8], offset: &mut usize) {
    let i = datum.unwrap_acl_item();
    copy_into(data, offset, &i.encode_binary());
}

#[inline(always)]
fn decode_acl_item<'a>(data: &[u8], offset: &mut usize) -> Datum<'a> {
    const N: usize = AclItem::binary_size();
    let acl_item =
        AclItem::decode_binary(&read_byte_array::<N>(data, offset)).expect("invalid aclitem");
    Datum::AclItem(acl_item)
}

/// Helper function to obtain the fixed length of a `ScalarType`, if there is one.
pub(super) fn fixed_length(typ: &ScalarType) -> Option<usize> {
    match typ {
        ScalarType::Bool => Some(encoded_size_bool()),
        ScalarType::Int16 => Some(encoded_size_int16()),
        ScalarType::Int32 => Some(encoded_size_int32()),
        ScalarType::Int64 => Some(encoded_size_int64()),
        ScalarType::UInt16 => Some(encoded_size_uint16()),
        ScalarType::UInt32 => Some(encoded_size_uint32()),
        ScalarType::UInt64 => Some(encoded_size_uint64()),
        ScalarType::Float32 => Some(encoded_size_float32()),
        ScalarType::Float64 => Some(encoded_size_float64()),
        ScalarType::Date => Some(encoded_size_date()),
        ScalarType::Time => Some(encoded_size_time()),
        ScalarType::Timestamp { precision: _ } => Some(encoded_size_timestamp()),
        ScalarType::TimestampTz { precision: _ } => Some(encoded_size_timestamptz()),
        ScalarType::Interval => Some(encoded_size_interval()),
        ScalarType::Uuid => Some(encoded_size_uuid()),
        ScalarType::Oid => Some(encoded_size_oid()),
        ScalarType::MzTimestamp => Some(encoded_size_mz_timestamp()),
        ScalarType::MzAclItem => Some(encoded_size_mz_acl_item()),
        ScalarType::AclItem => Some(encoded_size_acl_item()),
        ScalarType::Numeric { .. }
        | ScalarType::PgLegacyChar
        | ScalarType::PgLegacyName
        | ScalarType::Bytes
        | ScalarType::String
        | ScalarType::Char { .. }
        | ScalarType::VarChar { .. }
        | ScalarType::Jsonb
        | ScalarType::Array(_)
        | ScalarType::List { .. }
        | ScalarType::Record { .. }
        | ScalarType::Map { .. }
        | ScalarType::RegProc
        | ScalarType::RegType
        | ScalarType::RegClass
        | ScalarType::Int2Vector
        | ScalarType::Range { .. } => None,
    }
}

impl<const N: usize> BytesN<N> {
    /// Evaluates whether the provided schema can be represented by a `BytesN`` instance.
    pub fn valid_schema(types: &[ColumnType]) -> bool {
        if types.len() == 1 {
            let typ = types.first().expect("must contain one element");
            if let Some(len) = fixed_length(&typ.scalar_type) {
                return len < N;
            }
        }
        false
    }

    /// A helper function that constructs an iterator of datums from the `BytesN` instance
    /// given by `self`.
    pub fn into_datum_iter<'a>(
        &'a self,
        types: Option<&[ColumnType]>,
    ) -> impl Iterator<Item = Datum<'a>> {
        // TODO (vmarcos): The implementation should be extended in the future to support
        // multiple columns in `types` and to panic if the multi-column schema that ends up
        // being too wide for a fixed-length N-byte pattern (MaterializeInc/materialize#22102).
        assert!(N > 1);
        let types = types.expect("Schema expected for BytesN");
        assert_eq!(types.len(), 1);

        let typ = &types[0];
        let mut offset = 0;

        // Decode fixed-length representation into a Datum.
        let nullability = u8::from_le_bytes(read_byte_array(&self.data, &mut offset));
        if nullability > 1 {
            panic!(
                "Unexpected value for nullability indicator: {}",
                nullability
            );
        }
        let is_null = nullability == 1u8;
        let datum = if is_null {
            debug_assert!(typ.nullable);
            Datum::Null
        } else {
            // Non-null datums must be constructable from the remaining bytes.
            let typ_len = fixed_length(&typ.scalar_type).unwrap_or_else(|| {
                panic!(
                    "Type {:?} without fixed-length for decoding",
                    typ.scalar_type
                )
            });
            if typ_len > N - offset {
                panic!(
                    "Type {:?} too wide for fixed-length {} decoding",
                    typ.scalar_type, N
                );
            }
            match typ.scalar_type {
                ScalarType::Bool => decode_bool(&self.data, &mut offset),
                ScalarType::Int16 => decode_int16(&self.data, &mut offset),
                ScalarType::Int32 => decode_int32(&self.data, &mut offset),
                ScalarType::Int64 => decode_int64(&self.data, &mut offset),
                ScalarType::UInt16 => decode_uint16(&self.data, &mut offset),
                ScalarType::UInt32 => decode_uint32(&self.data, &mut offset),
                ScalarType::UInt64 => decode_uint64(&self.data, &mut offset),
                ScalarType::Float32 => decode_float32(&self.data, &mut offset),
                ScalarType::Float64 => decode_float64(&self.data, &mut offset),
                ScalarType::Date => decode_date(&self.data, &mut offset),
                ScalarType::Time => decode_time(&self.data, &mut offset),
                ScalarType::Timestamp { precision: _ } => decode_timestamp(&self.data, &mut offset),
                ScalarType::TimestampTz { precision: _ } => {
                    decode_timestamptz(&self.data, &mut offset)
                }
                ScalarType::Interval => decode_interval(&self.data, &mut offset),
                ScalarType::Uuid => decode_uuid(&self.data, &mut offset),
                ScalarType::Oid => decode_oid(&self.data, &mut offset),
                ScalarType::MzTimestamp => decode_mz_timestamp(&self.data, &mut offset),
                ScalarType::MzAclItem => decode_mz_acl_item(&self.data, &mut offset),
                ScalarType::AclItem => decode_acl_item(&self.data, &mut offset),
                _ => panic!(
                    "Unexpected type {:?} for fixed-length decoding",
                    typ.scalar_type
                ),
            }
        };
        iter::once(datum)
    }
}

impl<const N: usize> IntoRowByTypes for BytesN<N> {
    /// This implementation employs the schema provided by `types` to decode
    /// the N-byte value into `row_buf`, returning a reference to it.
    ///
    /// Presently, the implementation panics if more than one `ColumnType` is found
    /// in `types` or if the one column has a byte length greater than N-1 (due to one
    /// byte being reserved for nullability encoding).
    fn into_row<'a>(&'a self, row_buf: &'a mut Row, types: Option<&[ColumnType]>) -> &'a Row {
        let mut packer = row_buf.packer();
        packer.extend(self.into_datum_iter(types));
        row_buf
    }
}

impl<const N: usize> FromRowByTypes for BytesN<N> {
    /// This implementation reads the datums in `row`, validates their types
    /// against the schema provided by `types`, and returns a fixed-length
    /// N-byte value as a result.
    ///
    /// Presently, the implementation panics if more than one `ColumnType` or
    /// a type larger than what fits in an N-byte representation is found in `types`.
    /// Additionally, the implementation panics if decoding the row results in an
    /// unexpected type.
    fn from_datum_iter<'a, I, D>(iter: I, types: Option<&[ColumnType]>) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        // TODO(vmarcos): In the future, the implementation should be extended to
        // panic if a multi-column `row` has more data than fits in an N-byte tagless
        // representation with one byte reserved for nullability, or if the types of
        // the datums do not match with `types`.
        assert!(N > 1);
        let types = types.expect("Schema expected for BytesN");
        assert_eq!(types.len(), 1);

        let typ = &types[0];
        let mut datum_iter = iter.into_iter();
        let next_datum = datum_iter
            .next()
            .expect("At least one Datum expected in fixed-length encoding");
        let next_datum = *next_datum.borrow();
        let mut result = BytesN { data: [0; N] };

        // Firstly, try to encode Datum::Null.
        if let Datum::Null = next_datum {
            result.data[0] = 1u8;
        } else {
            // Non-null datums should fit the remaining bytes.
            let mut offset = 1;
            let typ_len = fixed_length(&typ.scalar_type).unwrap_or_else(|| {
                panic!(
                    "Type {:?} without fixed-length for encoding",
                    typ.scalar_type
                )
            });
            if typ_len > N - offset {
                panic!(
                    "Type {:?} too wide for fixed-length {} encoding",
                    typ.scalar_type, N
                );
            }
            match typ.scalar_type {
                ScalarType::Bool => encode_bool(&next_datum, &mut result.data, &mut offset),
                ScalarType::Int16 => encode_int16(&next_datum, &mut result.data, &mut offset),
                ScalarType::Int32 => encode_int32(&next_datum, &mut result.data, &mut offset),
                ScalarType::Int64 => encode_int64(&next_datum, &mut result.data, &mut offset),
                ScalarType::UInt16 => encode_uint16(&next_datum, &mut result.data, &mut offset),
                ScalarType::UInt32 => encode_uint32(&next_datum, &mut result.data, &mut offset),
                ScalarType::UInt64 => encode_uint64(&next_datum, &mut result.data, &mut offset),
                ScalarType::Float32 => encode_float32(&next_datum, &mut result.data, &mut offset),
                ScalarType::Float64 => encode_float64(&next_datum, &mut result.data, &mut offset),
                ScalarType::Date => encode_date(&next_datum, &mut result.data, &mut offset),
                ScalarType::Time => encode_time(&next_datum, &mut result.data, &mut offset),
                ScalarType::Timestamp { precision: _ } => {
                    encode_timestamp(&next_datum, &mut result.data, &mut offset)
                }
                ScalarType::TimestampTz { precision: _ } => {
                    encode_timestamptz(&next_datum, &mut result.data, &mut offset)
                }
                ScalarType::Interval => encode_interval(&next_datum, &mut result.data, &mut offset),
                ScalarType::Uuid => encode_uuid(&next_datum, &mut result.data, &mut offset),
                ScalarType::Oid => encode_oid(&next_datum, &mut result.data, &mut offset),
                ScalarType::MzTimestamp => {
                    encode_mz_timestamp(&next_datum, &mut result.data, &mut offset)
                }
                ScalarType::MzAclItem => {
                    encode_mz_acl_item(&next_datum, &mut result.data, &mut offset)
                }
                ScalarType::AclItem => encode_acl_item(&next_datum, &mut result.data, &mut offset),
                _ => panic!(
                    "Unexpected type {:?} for fixed-length encoding",
                    typ.scalar_type
                ),
            }
        }
        if let Some(d) = datum_iter.next() {
            panic!(
                "Row too wide: Found extra datum {:?} when expecting type {:?}",
                *d.borrow(),
                typ
            );
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use ordered_float::OrderedFloat;
    use uuid::Uuid;

    use crate::adt::date::Date;
    use crate::adt::interval::Interval;
    use crate::adt::mz_acl_item::{AclItem, MzAclItem};
    use crate::adt::timestamp::CheckedTimestamp;
    use crate::fixed_length::{fixed_length, BytesN, FromRowByTypes, IntoRowByTypes};
    use crate::scalar::AsColumnType;
    use crate::{ColumnType, Datum, Row, ScalarType, Timestamp};

    /// This test takes inspiration from miri_test_round_trip in `Row`, but instead
    /// goes from `Row` to fixed-length and back.
    #[mz_ore::test]
    fn test_round_trip() {
        fn fixed_length_type(datum: Datum) -> Option<ColumnType> {
            match datum {
                Datum::False => Some(bool::as_column_type()),
                Datum::True => Some(bool::as_column_type()),
                Datum::Int16(_) => Some(i16::as_column_type()),
                Datum::Int32(_) => Some(i32::as_column_type()),
                Datum::Int64(_) => Some(i64::as_column_type()),
                Datum::UInt16(_) => Some(u16::as_column_type()),
                Datum::UInt32(_) => Some(u32::as_column_type()),
                Datum::UInt64(_) => Some(u64::as_column_type()),
                Datum::Float32(_) => Some(f32::as_column_type()),
                Datum::Float64(_) => Some(f64::as_column_type()),
                Datum::Date(_) => Some(Date::as_column_type()),
                Datum::Time(_) => Some(NaiveTime::as_column_type()),
                Datum::Timestamp(_) => Some(CheckedTimestamp::<NaiveDateTime>::as_column_type()),
                Datum::TimestampTz(_) => Some(CheckedTimestamp::<DateTime<Utc>>::as_column_type()),
                Datum::Interval(_) => Some(Interval::as_column_type()),
                Datum::Uuid(_) => Some(Uuid::as_column_type()),
                Datum::MzTimestamp(_) => Some(Timestamp::as_column_type()),
                Datum::MzAclItem(_) => Some(MzAclItem::as_column_type()),
                Datum::AclItem(_) => Some(AclItem::as_column_type()),
                _ => None,
            }
        }

        fn round_trip<const N: usize>(datums: Vec<Datum>, types: Option<&[ColumnType]>) {
            let row = Row::pack(datums.clone());
            let bytes_n = BytesN::<N>::from_row(row.clone(), types);

            let mut row_buf = Row::default();
            let row2 = bytes_n.into_row(&mut row_buf, types);

            assert_eq!(row, row2.clone());
            assert_eq!(row, row_buf);
        }

        // In this part of the test, we get to cover all the supported types
        // by iterating on `interesting_datums`.
        for (datum, typ) in ScalarType::enumerate()
            .iter()
            .flat_map(|r#type| r#type.interesting_datums())
            .filter_map(|datum| {
                let Some(typ) = fixed_length_type(datum) else {
                    return None;
                };
                if fixed_length(&typ.scalar_type).is_some() {
                    Some((datum, typ))
                } else {
                    None
                }
            })
        {
            // NOTE(vmarcos): For now we hard-code a fixed length that is large
            // enough since we can't compute the const based on iterating over
            // ScalarType::enumerate due to https://github.com/rust-lang/rust/issues/87575.
            round_trip::<42>(vec![datum], Some(&[typ]));
        }

        // We have an interest in eventually supporting 5, 9, and 17 bytes (initially 9),
        // so we focus the tests below on these sizes.
        let nullable_u64 = ColumnType {
            nullable: true,
            scalar_type: ScalarType::UInt64,
        };
        let nullable_timestamp = ColumnType {
            nullable: true,
            scalar_type: ScalarType::MzTimestamp,
        };
        round_trip::<5>(vec![Datum::Null], Some(&[nullable_u64.clone()]));
        round_trip::<9>(vec![Datum::Null], Some(&[nullable_u64]));
        round_trip::<17>(vec![Datum::Null], Some(&[nullable_timestamp]));
        round_trip::<5>(vec![Datum::False], Some(&[bool::as_column_type()]));
        round_trip::<5>(vec![Datum::True], Some(&[bool::as_column_type()]));
        round_trip::<9>(vec![Datum::False], Some(&[bool::as_column_type()]));
        round_trip::<9>(vec![Datum::True], Some(&[bool::as_column_type()]));
        round_trip::<17>(vec![Datum::False], Some(&[bool::as_column_type()]));
        round_trip::<17>(vec![Datum::True], Some(&[bool::as_column_type()]));
        round_trip::<5>(vec![Datum::Int16(-21)], Some(&[i16::as_column_type()]));
        round_trip::<9>(vec![Datum::Int32(-42)], Some(&[i32::as_column_type()]));
        round_trip::<9>(
            vec![Datum::Int64(-2_147_483_648 - 42)],
            Some(&[i64::as_column_type()]),
        );
        round_trip::<5>(
            vec![Datum::Float32(OrderedFloat::from(-42.12))],
            Some(&[f32::as_column_type()]),
        );
        round_trip::<9>(
            vec![Datum::Float64(OrderedFloat::from(-2_147_483_648.0 - 42.12))],
            Some(&[f64::as_column_type()]),
        );
        round_trip::<9>(
            vec![Datum::Date(Date::from_pg_epoch(365 * 45 + 21).unwrap())],
            Some(&[Date::as_column_type()]),
        );
        round_trip::<17>(
            vec![Datum::Timestamp(
                CheckedTimestamp::from_timestamplike(
                    NaiveDate::from_isoywd_opt(2019, 30, chrono::Weekday::Wed)
                        .unwrap()
                        .and_hms_opt(14, 32, 11)
                        .unwrap(),
                )
                .unwrap(),
            )],
            Some(&[CheckedTimestamp::<NaiveDateTime>::as_column_type()]),
        );
        round_trip::<17>(
            vec![Datum::TimestampTz(
                CheckedTimestamp::from_timestamplike(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp_opt(61, 0).unwrap(),
                    Utc,
                ))
                .unwrap(),
            )],
            Some(&[CheckedTimestamp::<DateTime<Utc>>::as_column_type()]),
        );
        round_trip::<25>(
            vec![Datum::Interval(Interval {
                months: 312,
                ..Default::default()
            })],
            Some(&[Interval::as_column_type()]),
        );
        round_trip::<25>(
            vec![Datum::Interval(Interval::new(0, 0, 1_012_312))],
            Some(&[Interval::as_column_type()]),
        );
    }
}
