// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A permanent storage encoding for rows.
//!
//! See row.proto for details.

use bytes::BufMut;
use chrono::Timelike;
use dec::Decimal;
use enum_dispatch::enum_dispatch;
use mz_ore::cast::CastFrom;
use mz_persist_types::columnar::{
    ColumnCfg, ColumnGet, ColumnPush, Data, DataType, PartDecoder, PartEncoder, Schema,
};
use mz_persist_types::dyn_col::DynColumnRef;
use mz_persist_types::dyn_struct::{ColumnsMut, ColumnsRef, DynStructCfg, ValidityRef};
use mz_persist_types::stats::{AtomicBytesStats, BytesStats, DynStats, OptionStats, StatsFn};
use mz_persist_types::Codec;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use prost::Message;
use uuid::Uuid;

use crate::adt::array::ArrayDimension;
use crate::adt::date::{Date, ProtoDate};
use crate::adt::jsonb::Jsonb;
use crate::adt::numeric::Numeric;
use crate::adt::range::{Range, RangeInner, RangeLowerBound, RangeUpperBound};
use crate::chrono::ProtoNaiveTime;
use crate::row::proto_datum::DatumType;
use crate::row::{
    ProtoArray, ProtoArrayDimension, ProtoDatum, ProtoDatumOther, ProtoDict, ProtoDictElement,
    ProtoNumeric, ProtoRange, ProtoRangeInner, ProtoRow,
};
use crate::stats::{jsonb_stats_nulls, proto_datum_min_max_nulls};
use crate::{ColumnType, Datum, RelationDesc, Row, RowPacker, ScalarType, Timestamp};

impl Codec for Row {
    type Schema = RelationDesc;

    fn codec_name() -> String {
        "protobuf[Row]".into()
    }

    /// Encodes a row into the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::decode]. It's guaranteed to be
    /// readable by future versions of Materialize through v(TODO: Figure out
    /// our policy).
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    /// Decodes a row from the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::encode]. It can read rows
    /// encoded by historical versions of Materialize back to v(TODO: Figure out
    /// our policy).
    fn decode(buf: &[u8]) -> Result<Row, String> {
        let proto_row = ProtoRow::decode(buf).map_err(|err| err.to_string())?;
        Row::try_from(&proto_row)
    }
}

impl ColumnType {
    /// Returns the [DatumToPersist] implementation for this ColumnType.
    ///
    /// See [DatumToPersist] to map `Datum`s to/from a persist columnar type.
    /// There is a 1:N correspondence between DatumToPersist impls and
    /// ColumnTypes because a number of ScalarTypes map to the same set of
    /// `Datum`s (e.g. `String` and `VarChar`).
    ///
    /// This returns a None if we don't care to keep stats for this column type
    /// (for now). We'll have to remove this if/when we start actually using the
    /// schema'd part encoding.
    pub fn to_persist<R, F: DatumToPersistFn<R>>(&self, f: F) -> Option<R> {
        use ScalarType::*;
        let ColumnType {
            nullable,
            scalar_type,
        } = self;
        match (nullable, scalar_type) {
            (false, Bool) => Some(f.call::<bool>()),
            (true, Bool) => Some(f.call::<Option<bool>>()),
            (false, Int16) => Some(f.call::<i16>()),
            (true, Int16) => Some(f.call::<Option<i16>>()),
            (false, Int32) => Some(f.call::<i32>()),
            (true, Int32) => Some(f.call::<Option<i32>>()),
            (false, Int64) => Some(f.call::<i64>()),
            (true, Int64) => Some(f.call::<Option<i64>>()),
            (false, UInt16) => Some(f.call::<u16>()),
            (true, UInt16) => Some(f.call::<Option<u16>>()),
            (false, UInt32 | Oid | RegClass | RegProc | RegType) => Some(f.call::<u32>()),
            (true, UInt32 | Oid | RegClass | RegProc | RegType) => Some(f.call::<Option<u32>>()),
            (false, UInt64) => Some(f.call::<u64>()),
            (true, UInt64) => Some(f.call::<Option<u64>>()),
            (false, Float32) => Some(f.call::<f32>()),
            (true, Float32) => Some(f.call::<Option<f32>>()),
            (false, Float64) => Some(f.call::<f64>()),
            (true, Float64) => Some(f.call::<Option<f64>>()),
            (false, Date) => Some(f.call::<crate::adt::date::Date>()),
            (true, Date) => Some(f.call::<Option<crate::adt::date::Date>>()),
            (false, PgLegacyChar) => Some(f.call::<u8>()),
            (true, PgLegacyChar) => Some(f.call::<Option<u8>>()),
            (false, Bytes) => Some(f.call::<Vec<u8>>()),
            (true, Bytes) => Some(f.call::<Option<Vec<u8>>>()),
            (false, String | Char { .. } | VarChar { .. }) => Some(f.call::<std::string::String>()),
            (true, String | Char { .. } | VarChar { .. }) => {
                Some(f.call::<Option<std::string::String>>())
            }
            (false, Jsonb) => Some(f.call::<crate::adt::jsonb::Jsonb>()),
            (true, Jsonb) => Some(f.call::<Option<crate::adt::jsonb::Jsonb>>()),
            (false, MzTimestamp) => Some(f.call::<crate::Timestamp>()),
            (true, MzTimestamp) => Some(f.call::<Option<crate::Timestamp>>()),
            (_, Numeric { .. } | Time | Timestamp | TimestampTz | Interval | Uuid) => {
                if *nullable {
                    Some(f.call::<NullableProtoDatumToPersist>())
                } else {
                    Some(f.call::<ProtoDatumToPersist>())
                }
            }
            (
                _,
                Array(..)
                | List { .. }
                | Record { .. }
                | Map { .. }
                | Int2Vector
                | Range { .. }
                | MzAclItem,
            ) => None,
        }
    }
}

/// This returns true if we don't care to keep stats for this column type (for
/// now). We'll have to remove this if/when we start actually using the schema'd
/// part encoding.
///
/// This is only used in tests, but we sadly can't cfg(test) gate it because
/// it's also used from mz_storage_client tests.
pub fn is_no_stats_type(typ: &ScalarType) -> bool {
    impl DatumToPersistFn<()> for () {
        fn call<T: DatumToPersist>(self) -> ()
        where
            for<'a> DatumDecoder<'a>: From<DataRef<'a, T>>,
            for<'a> DatumEncoder<'a>: From<DataMut<'a, T>>,
        {
            ()
        }
    }
    typ.clone().nullable(true).to_persist(()).is_none()
}

/// Implementation of mapping between mz [Datum] and persist [Data].
///
/// This always maps to and from a single persist columnar type, but it maps a
/// _set_ of `Datum` types. For example: any nullable [ColumnType] will map
/// `Datum::Null` along with the type(s) corresponding to the [ScalarType].
/// Similarly, `ScalarType::Jsonb` maps to several `Datum` types.
///
/// See `ColumnType::to_persist` to map a ColumnType to this. There is a 1:N
/// correspondence between DatumToPersist impls and ColumnTypes because a number
/// of ScalarTypes map to the same set of `Datum`s (e.g. `String` and
/// `VarChar`).
pub trait DatumToPersist {
    /// The persist columnar type we're mapping to/from.
    type Data: Data<Cfg = Self::Cfg>;

    /// Any information, in addition to the type of `Self::Data`, that is
    /// necessary to derive the columnar schema of this type. E.g. for
    /// ScalarType::Record, this will be the schema of the record.
    type Cfg: ColumnCfg<Self::Data>;
    // TODO: This `const CFG` model won't work for things where Self::Cfg is not
    // (). Revisit when we get there.
    const CFG: Self::Cfg;

    /// Which logic to use for computing aggregate stats.
    const STATS_FN: StatsFn;

    /// Encodes and pushes the given Datum into the persist column.
    ///
    /// Panics if the Datum doesn't match the persist column type.
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum);

    /// Pushes the default value for this type into the column.
    ///
    /// NB: This is because Arrow's optional struct array representation is
    /// "dense". Even for entries in where the struct's validity bit is false,
    /// we still have to encode some ignored value. Unfortunately, we can't just
    /// call `Self::encode` with a `Datum::Null` without tripping up some
    /// (useful) debug assertions. Feel free to remove this method if you can
    /// find a nice way to do it.
    fn encode_default(col: &mut <Self::Data as Data>::Mut) {
        ColumnPush::<Self::Data>::push(col, <Self::Data as Data>::Ref::default());
    }

    /// Decodes the data with the given reference into a Datum. This Datum is
    /// returned by pushing it in to the given RowPacker.
    fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker);
}

/// `FnOnce<T: DatumToPersist>() -> R`
///
/// This saves us from needing another `enum_dispatch` for [DatumToPersist].
pub trait DatumToPersistFn<R> {
    fn call<T: DatumToPersist>(self) -> R
    where
        for<'a> DatumDecoder<'a>: From<DataRef<'a, T>>,
        for<'a> DatumEncoder<'a>: From<DataMut<'a, T>>;
}

macro_rules! data_to_persist_primitive {
    ($data:ty, $unwrap:ident) => {
        impl DatumToPersist for $data {
            type Data = $data;
            type Cfg = ();
            const CFG: Self::Cfg = ();
            const STATS_FN: StatsFn = StatsFn::Default;
            fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
                ColumnPush::<Self::Data>::push(col, datum.$unwrap());
            }
            fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
                row.push(Datum::from(val))
            }
        }
        impl DatumToPersist for Option<$data> {
            type Data = Option<$data>;
            type Cfg = ();
            const CFG: Self::Cfg = ();
            const STATS_FN: StatsFn = StatsFn::Default;
            fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
                if datum.is_null() {
                    ColumnPush::<Self::Data>::push(col, None);
                } else {
                    ColumnPush::<Self::Data>::push(col, Some(datum.$unwrap()));
                }
            }
            fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
                row.push(Datum::from(val))
            }
        }
    };
}

data_to_persist_primitive!(bool, unwrap_bool);
data_to_persist_primitive!(u8, unwrap_uint8);
data_to_persist_primitive!(u16, unwrap_uint16);
data_to_persist_primitive!(u32, unwrap_uint32);
data_to_persist_primitive!(u64, unwrap_uint64);
data_to_persist_primitive!(i16, unwrap_int16);
data_to_persist_primitive!(i32, unwrap_int32);
data_to_persist_primitive!(i64, unwrap_int64);
data_to_persist_primitive!(f32, unwrap_float32);
data_to_persist_primitive!(f64, unwrap_float64);
data_to_persist_primitive!(Vec<u8>, unwrap_bytes);
data_to_persist_primitive!(String, unwrap_str);

impl DatumToPersist for Date {
    type Data = i32;
    type Cfg = ();
    const CFG: Self::Cfg = ();
    const STATS_FN: StatsFn = StatsFn::Default;
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
        let ProtoDate { days } = datum.unwrap_date().into_proto();
        ColumnPush::<Self::Data>::push(col, days);
    }
    fn decode(days: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
        let date: Date = ProtoDate { days }.into_rust().expect("valid date");
        row.push(Datum::from(date))
    }
}

impl DatumToPersist for Option<Date> {
    type Data = Option<i32>;
    type Cfg = ();
    const CFG: Self::Cfg = ();
    const STATS_FN: StatsFn = StatsFn::Default;
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
        if datum.is_null() {
            ColumnPush::<Self::Data>::push(col, None);
        } else {
            let ProtoDate { days } = datum.unwrap_date().into_proto();
            ColumnPush::<Self::Data>::push(col, Some(days));
        }
    }
    fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
        let Some(days) = val else {
            row.push(Datum::Null);
            return;
        };
        let date: Date = ProtoDate { days }.into_rust().expect("valid date");
        row.push(Datum::from(date))
    }
}

impl DatumToPersist for Timestamp {
    type Data = u64;
    type Cfg = ();
    const CFG: Self::Cfg = ();
    const STATS_FN: StatsFn = StatsFn::Default;
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
        ColumnPush::<Self::Data>::push(col, u64::from(datum.unwrap_mz_timestamp()));
    }
    fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
        row.push(Datum::from(Timestamp::new(val)))
    }
}

impl DatumToPersist for Option<Timestamp> {
    type Data = Option<u64>;
    type Cfg = ();
    const CFG: Self::Cfg = ();
    const STATS_FN: StatsFn = StatsFn::Default;
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
        if datum.is_null() {
            ColumnPush::<Self::Data>::push(col, None);
        } else {
            ColumnPush::<Self::Data>::push(col, Some(u64::from(datum.unwrap_mz_timestamp())));
        }
    }
    fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
        let Some(val) = val else {
            row.push(Datum::Null);
            return;
        };
        row.push(Datum::from(Timestamp::new(val)))
    }
}

/// An implementation of [DatumToPersist] that maps to/from all non-nullable
/// Datum types using the ProtoDatum representation.
#[derive(Debug)]
pub struct ProtoDatumToPersist;

impl DatumToPersist for ProtoDatumToPersist {
    type Data = Vec<u8>;
    type Cfg = ();
    const CFG: Self::Cfg = ();
    const STATS_FN: StatsFn = StatsFn::Custom(
        |col: &DynColumnRef, validity: ValidityRef<'_>| -> Result<Box<dyn DynStats>, String> {
            let (lower, upper, null_count) =
                proto_datum_min_max_nulls(col.downcast::<Vec<u8>>()?, validity);
            assert_eq!(null_count, 0);
            Ok(Box::new(BytesStats::Atomic(AtomicBytesStats {
                lower,
                upper,
            })))
        },
    );
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
        let proto = ProtoDatum::from(datum);
        let buf = proto.encode_to_vec();
        ColumnPush::<Self::Data>::push(col, &buf);
    }
    fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
        let proto = ProtoDatum::decode(val).expect("col should be valid ProtoDatum");
        row.try_push_proto(&proto)
            .expect("ProtoDatum should be valid Datum");
    }
}

/// An implementation of [DatumToPersist] that maps to/from all nullable Datum
/// types using the ProtoDatum representation.
#[derive(Debug)]
pub struct NullableProtoDatumToPersist;

impl DatumToPersist for NullableProtoDatumToPersist {
    type Data = Option<Vec<u8>>;
    type Cfg = ();
    const CFG: Self::Cfg = ();
    const STATS_FN: StatsFn = StatsFn::Custom(
        |col: &DynColumnRef, validity: ValidityRef<'_>| -> Result<Box<dyn DynStats>, String> {
            let col = col.downcast::<Option<Vec<u8>>>()?;
            debug_assert!(validity.is_superset(col.validity()));
            let (lower, upper, null_count) = proto_datum_min_max_nulls(col, ValidityRef::none());
            Ok(Box::new(OptionStats {
                none: null_count,
                some: BytesStats::Atomic(AtomicBytesStats { lower, upper }),
            }))
        },
    );
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
        if datum == Datum::Null {
            ColumnPush::<Self::Data>::push(col, None);
        } else {
            let proto = ProtoDatum::from(datum);
            let buf = proto.encode_to_vec();
            ColumnPush::<Self::Data>::push(col, Some(&buf));
        }
    }
    fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
        let Some(val) = val else {
            row.push(Datum::Null);
            return;
        };
        let proto = ProtoDatum::decode(val).expect("col should be valid ProtoDatum");
        row.try_push_proto(&proto)
            .expect("ProtoDatum should be valid Datum");
    }
}

impl DatumToPersist for Jsonb {
    type Data = <ProtoDatumToPersist as DatumToPersist>::Data;
    type Cfg = ();
    const CFG: Self::Cfg = ();
    const STATS_FN: StatsFn = StatsFn::Custom(
        |col: &DynColumnRef, validity: ValidityRef<'_>| -> Result<Box<dyn DynStats>, String> {
            let (stats, null_count) = jsonb_stats_nulls(col.downcast::<Vec<u8>>()?, validity)?;
            assert_eq!(null_count, 0);
            Ok(Box::new(BytesStats::Json(stats)))
        },
    );
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
        ProtoDatumToPersist::encode(col, datum)
    }
    fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
        ProtoDatumToPersist::decode(val, row)
    }
}

impl DatumToPersist for Option<Jsonb> {
    type Data = <NullableProtoDatumToPersist as DatumToPersist>::Data;
    type Cfg = ();
    const CFG: Self::Cfg = ();
    const STATS_FN: StatsFn = StatsFn::Custom(
        |col: &DynColumnRef, validity: ValidityRef<'_>| -> Result<Box<dyn DynStats>, String> {
            let col = col.downcast::<Option<Vec<u8>>>()?;
            debug_assert!(validity.is_superset(col.validity()));
            let (stats, null_count) = jsonb_stats_nulls(col, ValidityRef::none())?;
            Ok(Box::new(OptionStats {
                some: BytesStats::Json(stats),
                none: null_count,
            }))
        },
    );
    fn encode(col: &mut <Self::Data as Data>::Mut, datum: Datum) {
        NullableProtoDatumToPersist::encode(col, datum)
    }
    fn decode(val: <Self::Data as Data>::Ref<'_>, row: &mut RowPacker) {
        NullableProtoDatumToPersist::decode(val, row)
    }
}

/// A sentinel for column types we don't care to keep stats for (for now). We'll
/// have to remove this if/when we start actually using the schema'd part
/// encoding.
#[derive(Debug)]
pub struct NoStats(ColumnType);

/// A helper for adapting mz's [Datum] to persist's columnar [Data].
#[enum_dispatch]
#[derive(Debug)]
pub enum DatumEncoder<'a> {
    Bool(DataMut<'a, bool>),
    OptBool(DataMut<'a, Option<bool>>),
    Int16(DataMut<'a, i16>),
    OptInt16(DataMut<'a, Option<i16>>),
    Int32(DataMut<'a, i32>),
    OptInt32(DataMut<'a, Option<i32>>),
    Int64(DataMut<'a, i64>),
    OptInt64(DataMut<'a, Option<i64>>),
    UInt8(DataMut<'a, u8>),
    OptUInt8(DataMut<'a, Option<u8>>),
    UInt16(DataMut<'a, u16>),
    OptUInt16(DataMut<'a, Option<u16>>),
    UInt32(DataMut<'a, u32>),
    OptUInt32(DataMut<'a, Option<u32>>),
    UInt64(DataMut<'a, u64>),
    OptUInt64(DataMut<'a, Option<u64>>),
    Float32(DataMut<'a, f32>),
    OptFloat32(DataMut<'a, Option<f32>>),
    Float64(DataMut<'a, f64>),
    OptFloat64(DataMut<'a, Option<f64>>),
    Date(DataMut<'a, Date>),
    OptDate(DataMut<'a, Option<Date>>),
    Bytes(DataMut<'a, Vec<u8>>),
    OptBytes(DataMut<'a, Option<Vec<u8>>>),
    String(DataMut<'a, String>),
    OptString(DataMut<'a, Option<String>>),
    Jsonb(DataMut<'a, Jsonb>),
    OptJsonb(DataMut<'a, Option<Jsonb>>),
    MzTimestamp(DataMut<'a, Timestamp>),
    OptMzTimestamp(DataMut<'a, Option<Timestamp>>),
    Todo(DataMut<'a, ProtoDatumToPersist>),
    OptTodo(DataMut<'a, NullableProtoDatumToPersist>),
    NoStats(NoStats),
}

/// An `enum_dispatch` companion for `DatumEncoder`.
///
/// This allows us to do Datum encoding without dynamic dispatch. It's a pretty
/// hot path, so the hassle is worth it.
#[enum_dispatch(DatumEncoder)]
pub trait DatumEncoderT<'a> {
    fn encode(&mut self, datum: Datum);
    fn encode_default(&mut self);
}

/// A newtype wrapper for `&mut T::Mut`.
///
/// This is necessary for enum_dispatch to create From/TryInto (conflicting
/// impls if we try to store `&mut T::Mut` directly in the enum variants), but
/// it's also a convenient place to hang a std::fmt::Debug impl.
pub struct DataMut<'a, T: DatumToPersist>(&'a mut <T::Data as Data>::Mut);

impl<T: DatumToPersist> std::fmt::Debug for DataMut<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("DataMut<{}>", std::any::type_name::<T>()))
            .finish_non_exhaustive()
    }
}

impl<'a, T: DatumToPersist> DatumEncoderT<'a> for DataMut<'a, T> {
    fn encode(&mut self, datum: Datum) {
        T::encode(self.0, datum);
    }
    fn encode_default(&mut self) {
        T::encode_default(self.0);
    }
}

impl DatumEncoderT<'_> for NoStats {
    fn encode(&mut self, _datum: Datum) {}
    fn encode_default(&mut self) {}
}

/// An implementation of [PartEncoder] for [Row].
#[derive(Debug)]
pub struct RowEncoder<'a> {
    len: &'a mut usize,
    col_encoders: Vec<DatumEncoder<'a>>,
}

impl<'a> RowEncoder<'a> {
    /// Returns the underlying DatumEncoders, one per column in the Row.
    pub fn col_encoders(&mut self) -> &mut [DatumEncoder<'a>] {
        &mut self.col_encoders
    }

    pub fn inc_len(&mut self) {
        *self.len += 1;
    }
}

impl<'a> PartEncoder<'a, Row> for RowEncoder<'a> {
    fn encode(&mut self, val: &Row) {
        *self.len += 1;
        for (encoder, datum) in self.col_encoders.iter_mut().zip(val.iter()) {
            encoder.encode(datum);
        }
    }
}

/// A helper for adapting mz's [Datum] to persist's columnar [Data].
#[enum_dispatch]
#[derive(Debug)]
pub enum DatumDecoder<'a> {
    Bool(DataRef<'a, bool>),
    OptBool(DataRef<'a, Option<bool>>),
    Int16(DataRef<'a, i16>),
    OptInt16(DataRef<'a, Option<i16>>),
    Int32(DataRef<'a, i32>),
    OptInt32(DataRef<'a, Option<i32>>),
    Int64(DataRef<'a, i64>),
    OptInt64(DataRef<'a, Option<i64>>),
    UInt8(DataRef<'a, u8>),
    OptUInt8(DataRef<'a, Option<u8>>),
    UInt16(DataRef<'a, u16>),
    OptUInt16(DataRef<'a, Option<u16>>),
    UInt32(DataRef<'a, u32>),
    OptUInt32(DataRef<'a, Option<u32>>),
    UInt64(DataRef<'a, u64>),
    OptUInt64(DataRef<'a, Option<u64>>),
    Float32(DataRef<'a, f32>),
    OptFloat32(DataRef<'a, Option<f32>>),
    Float64(DataRef<'a, f64>),
    OptFloat64(DataRef<'a, Option<f64>>),
    Date(DataRef<'a, Date>),
    OptDate(DataRef<'a, Option<Date>>),
    Bytes(DataRef<'a, Vec<u8>>),
    OptBytes(DataRef<'a, Option<Vec<u8>>>),
    String(DataRef<'a, String>),
    OptString(DataRef<'a, Option<String>>),
    Jsonb(DataRef<'a, Jsonb>),
    OptJsonb(DataRef<'a, Option<Jsonb>>),
    MzTimestamp(DataRef<'a, Timestamp>),
    OptMzTimestamp(DataRef<'a, Option<Timestamp>>),
    Todo(DataRef<'a, ProtoDatumToPersist>),
    OptTodo(DataRef<'a, NullableProtoDatumToPersist>),
    NoStats(NoStats),
}

/// An `enum_dispatch` companion for `DatumDecoder`.
///
/// This allows us to do Datum decoding without dynamic dispatch. It's a pretty
/// hot path, so the hassle is worth it.
#[enum_dispatch(DatumDecoder)]
pub trait DatumDecoderT<'a> {
    fn decode(&self, idx: usize, row: &mut RowPacker);
}

/// A newtype wrapper for `& T::Ref`.
///
/// This is necessary for enum_dispatch to create From/TryInto (conflicting
/// impls if we try to store `& T::Ref` directly in the enum variants), but it's
/// also a convenient place to hang a std::fmt::Debug impl.
pub struct DataRef<'a, T: DatumToPersist>(&'a <T::Data as Data>::Col);

impl<T: DatumToPersist> std::fmt::Debug for DataRef<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("DataRef<{}>", std::any::type_name::<T>()))
            .finish_non_exhaustive()
    }
}

impl<'a, T: DatumToPersist> DatumDecoderT<'a> for DataRef<'a, T> {
    fn decode(&self, idx: usize, row: &mut RowPacker) {
        T::decode(ColumnGet::<T::Data>::get(self.0, idx), row);
    }
}

impl DatumDecoderT<'_> for NoStats {
    fn decode(&self, _idx: usize, _row: &mut RowPacker) {
        unreachable!("the schema'd encoding is not yet used for: {:?}", self.0)
    }
}

/// An implementation of [PartDecoder] for [Row].
#[derive(Debug)]
pub struct RowDecoder<'a> {
    col_decoders: Vec<DatumDecoder<'a>>,
}

impl<'a> RowDecoder<'a> {
    /// Returns the underlying DatumDecoders, one per column in the Row.
    pub fn col_decoders(&self) -> &[DatumDecoder<'a>] {
        &self.col_decoders
    }
}

impl<'a> PartDecoder<'a, Row> for RowDecoder<'a> {
    fn decode(&self, idx: usize, val: &mut Row) {
        let mut packer = val.packer();
        for decoder in self.col_decoders.iter() {
            decoder.decode(idx, &mut packer);
        }
    }
}

impl RelationDesc {
    pub fn decoder<'a, V>(
        &self,
        mut part: ColumnsRef<'a, V>,
    ) -> Result<(V, RowDecoder<'a>), String> {
        struct DatumDecoderFn<'a, 'b, V>(&'b str, &'b mut ColumnsRef<'a, V>);
        impl<'a, 'b, V> DatumToPersistFn<DatumDecoder<'a>> for DatumDecoderFn<'a, 'b, V> {
            fn call<T: DatumToPersist>(self) -> DatumDecoder<'a>
            where
                for<'c> DatumDecoder<'c>: From<DataRef<'c, T>>,
            {
                let DatumDecoderFn(name, part) = self;
                let col = part
                    .col::<T::Data>(name)
                    .expect("mapping to persist column type should be consistent");
                DatumDecoder::from(DataRef::<T>(col))
            }
        }

        let mut col_decoders = Vec::new();
        for (name, typ) in self.iter() {
            let col_decoder = typ
                .to_persist(DatumDecoderFn(name.as_str(), &mut part))
                .unwrap_or_else(|| DatumDecoder::NoStats(NoStats(typ.clone())));
            col_decoders.push(col_decoder);
        }
        let validity = part.finish()?;
        Ok((validity, RowDecoder { col_decoders }))
    }

    pub fn encoder<'a, V>(
        &self,
        mut part: ColumnsMut<'a, V>,
    ) -> Result<(V, RowEncoder<'a>), String> {
        struct DatumEncoderFn<'a, 'b, V>(&'b str, &'b mut ColumnsMut<'a, V>);
        impl<'a, 'b, V> DatumToPersistFn<DatumEncoder<'a>> for DatumEncoderFn<'a, 'b, V> {
            fn call<T: DatumToPersist>(self) -> DatumEncoder<'a>
            where
                for<'c> DatumEncoder<'c>: From<DataMut<'c, T>>,
            {
                let DatumEncoderFn(name, part) = self;
                let col = part
                    .col::<T::Data>(name)
                    .expect("mapping to persist column type should be consistent");
                DatumEncoder::from(DataMut::<T>(col))
            }
        }

        let mut col_encoders = Vec::new();
        for (name, typ) in self.iter() {
            let col_encoder = typ
                .to_persist(DatumEncoderFn(name.as_str(), &mut part))
                .unwrap_or_else(|| DatumEncoder::NoStats(NoStats(typ.clone())));
            col_encoders.push(col_encoder);
        }
        let (len, validity) = part.finish()?;
        Ok((validity, RowEncoder { len, col_encoders }))
    }
}

impl Schema<Row> for RelationDesc {
    type Encoder<'a> = RowEncoder<'a>;
    type Decoder<'a> = RowDecoder<'a>;

    fn columns(&self) -> DynStructCfg {
        struct ToPersist;
        impl DatumToPersistFn<(DataType, StatsFn)> for ToPersist {
            fn call<T: DatumToPersist>(self) -> (DataType, StatsFn) {
                (ColumnCfg::<T::Data>::as_type(&T::CFG), T::STATS_FN)
            }
        }

        let cols = self
            .iter()
            .flat_map(|(name, typ)| {
                typ.to_persist(ToPersist)
                    .map(|(data_type, stats_fn)| (name.0.clone(), data_type, stats_fn))
            })
            .collect::<Vec<_>>();
        DynStructCfg::from(cols)
    }

    fn decoder<'a>(&self, part: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        let ((), decoder) = self.decoder(part)?;
        Ok(decoder)
    }

    fn encoder<'a>(&self, part: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        let ((), encoder) = self.encoder(part)?;
        Ok(encoder)
    }
}

impl<'a> From<Datum<'a>> for ProtoDatum {
    fn from(x: Datum<'a>) -> Self {
        let datum_type = match x {
            Datum::False => DatumType::Other(ProtoDatumOther::False.into()),
            Datum::True => DatumType::Other(ProtoDatumOther::True.into()),
            Datum::Int16(x) => DatumType::Int16(x.into()),
            Datum::Int32(x) => DatumType::Int32(x),
            Datum::UInt8(x) => DatumType::Uint8(x.into()),
            Datum::UInt16(x) => DatumType::Uint16(x.into()),
            Datum::UInt32(x) => DatumType::Uint32(x),
            Datum::UInt64(x) => DatumType::Uint64(x),
            Datum::Int64(x) => DatumType::Int64(x),
            Datum::Float32(x) => DatumType::Float32(x.into_inner()),
            Datum::Float64(x) => DatumType::Float64(x.into_inner()),
            Datum::Date(x) => DatumType::Date(x.into_proto()),
            Datum::Time(x) => DatumType::Time(ProtoNaiveTime {
                secs: x.num_seconds_from_midnight(),
                frac: x.nanosecond(),
            }),
            Datum::Timestamp(x) => DatumType::Timestamp(x.into_proto()),
            Datum::TimestampTz(x) => DatumType::TimestampTz(x.into_proto()),
            Datum::Interval(x) => DatumType::Interval(x.into_proto()),
            Datum::Bytes(x) => DatumType::Bytes(x.to_vec()),
            Datum::String(x) => DatumType::String(x.to_owned()),
            Datum::Array(x) => DatumType::Array(ProtoArray {
                elements: Some(ProtoRow {
                    datums: x.elements().iter().map(|x| x.into()).collect(),
                }),
                dims: x
                    .dims()
                    .into_iter()
                    .map(|x| ProtoArrayDimension {
                        lower_bound: i64::cast_from(x.lower_bound),
                        length: u64::cast_from(x.length),
                    })
                    .collect(),
            }),
            Datum::List(x) => DatumType::List(ProtoRow {
                datums: x.iter().map(|x| x.into()).collect(),
            }),
            Datum::Map(x) => DatumType::Dict(ProtoDict {
                elements: x
                    .iter()
                    .map(|(k, v)| ProtoDictElement {
                        key: k.to_owned(),
                        val: Some(v.into()),
                    })
                    .collect(),
            }),
            Datum::Numeric(x) => {
                // TODO: Do we need this defensive clone?
                let mut x = x.0.clone();
                if let Some((bcd, scale)) = x.to_packed_bcd() {
                    DatumType::Numeric(ProtoNumeric { bcd, scale })
                } else if x.is_nan() {
                    DatumType::Other(ProtoDatumOther::NumericNaN.into())
                } else if x.is_infinite() {
                    if x.is_negative() {
                        DatumType::Other(ProtoDatumOther::NumericNegInf.into())
                    } else {
                        DatumType::Other(ProtoDatumOther::NumericPosInf.into())
                    }
                } else if x.is_special() {
                    panic!("internal error: unhandled special numeric value: {}", x);
                } else {
                    panic!(
                        "internal error: to_packed_bcd returned None for non-special value: {}",
                        x
                    )
                }
            }
            Datum::JsonNull => DatumType::Other(ProtoDatumOther::JsonNull.into()),
            Datum::Uuid(x) => DatumType::Uuid(x.as_bytes().to_vec()),
            Datum::MzTimestamp(x) => DatumType::MzTimestamp(x.into()),
            Datum::Dummy => DatumType::Other(ProtoDatumOther::Dummy.into()),
            Datum::Null => DatumType::Other(ProtoDatumOther::Null.into()),
            Datum::Range(super::Range { inner }) => DatumType::Range(Box::new(ProtoRange {
                inner: inner.map(|RangeInner { lower, upper }| {
                    Box::new(ProtoRangeInner {
                        lower_inclusive: lower.inclusive,
                        lower: lower.bound.map(|bound| Box::new(bound.datum().into())),
                        upper_inclusive: upper.inclusive,
                        upper: upper.bound.map(|bound| Box::new(bound.datum().into())),
                    })
                }),
            })),
            Datum::MzAclItem(x) => DatumType::MzAclItem(x.into_proto()),
        };
        ProtoDatum {
            datum_type: Some(datum_type),
        }
    }
}

impl RowPacker<'_> {
    fn try_push_proto(&mut self, x: &ProtoDatum) -> Result<(), String> {
        match &x.datum_type {
            Some(DatumType::Other(o)) => match ProtoDatumOther::from_i32(*o) {
                Some(ProtoDatumOther::Unknown) => return Err("unknown datum type".into()),
                Some(ProtoDatumOther::Null) => self.push(Datum::Null),
                Some(ProtoDatumOther::False) => self.push(Datum::False),
                Some(ProtoDatumOther::True) => self.push(Datum::True),
                Some(ProtoDatumOther::JsonNull) => self.push(Datum::JsonNull),
                Some(ProtoDatumOther::Dummy) => {
                    // We plan to remove the `Dummy` variant soon (#17099). To prepare for that, we
                    // emit a log to Sentry here, to notify us of any instances that might have
                    // been made durable.
                    #[cfg(feature = "tracing_")]
                    tracing::error!("protobuf decoding found Dummy datum");
                    self.push(Datum::Dummy);
                }
                Some(ProtoDatumOther::NumericPosInf) => self.push(Datum::from(Numeric::infinity())),
                Some(ProtoDatumOther::NumericNegInf) => {
                    self.push(Datum::from(-Numeric::infinity()))
                }
                Some(ProtoDatumOther::NumericNaN) => self.push(Datum::from(Numeric::nan())),
                None => return Err(format!("unknown datum type: {}", o)),
            },
            Some(DatumType::Int16(x)) => {
                let x = i16::try_from(*x)
                    .map_err(|_| format!("int16 field stored with out of range value: {}", *x))?;
                self.push(Datum::Int16(x))
            }
            Some(DatumType::Int32(x)) => self.push(Datum::Int32(*x)),
            Some(DatumType::Int64(x)) => self.push(Datum::Int64(*x)),
            Some(DatumType::Uint8(x)) => {
                let x = u8::try_from(*x)
                    .map_err(|_| format!("uint8 field stored with out of range value: {}", *x))?;
                self.push(Datum::UInt8(x))
            }
            Some(DatumType::Uint16(x)) => {
                let x = u16::try_from(*x)
                    .map_err(|_| format!("uint16 field stored with out of range value: {}", *x))?;
                self.push(Datum::UInt16(x))
            }
            Some(DatumType::Uint32(x)) => self.push(Datum::UInt32(*x)),
            Some(DatumType::Uint64(x)) => self.push(Datum::UInt64(*x)),
            Some(DatumType::Float32(x)) => self.push(Datum::Float32((*x).into())),
            Some(DatumType::Float64(x)) => self.push(Datum::Float64((*x).into())),
            Some(DatumType::Bytes(x)) => self.push(Datum::Bytes(x)),
            Some(DatumType::String(x)) => self.push(Datum::String(x)),
            Some(DatumType::Uuid(x)) => {
                // Uuid internally has a [u8; 16] so we'll have to do at least
                // one copy, but there's currently an additional one when the
                // Vec is created. Perhaps the protobuf Bytes support will let
                // us fix one of them.
                let u = Uuid::from_slice(x).map_err(|err| err.to_string())?;
                self.push(Datum::Uuid(u));
            }
            Some(DatumType::Date(x)) => self.push(Datum::Date(x.clone().into_rust()?)),
            Some(DatumType::Time(x)) => self.push(Datum::Time(x.clone().into_rust()?)),
            Some(DatumType::Timestamp(x)) => self.push(Datum::Timestamp(x.clone().into_rust()?)),
            Some(DatumType::TimestampTz(x)) => {
                self.push(Datum::TimestampTz(x.clone().into_rust()?))
            }
            Some(DatumType::Interval(x)) => self.push(Datum::Interval(
                x.clone()
                    .into_rust()
                    .map_err(|e: TryFromProtoError| e.to_string())?,
            )),
            Some(DatumType::List(x)) => self.push_list_with(|row| -> Result<(), String> {
                for d in x.datums.iter() {
                    row.try_push_proto(d)?;
                }
                Ok(())
            })?,
            Some(DatumType::Array(x)) => {
                let dims = x
                    .dims
                    .iter()
                    .map(|x| ArrayDimension {
                        lower_bound: isize::cast_from(x.lower_bound),
                        length: usize::cast_from(x.length),
                    })
                    .collect::<Vec<_>>();
                match x.elements.as_ref() {
                    None => self.push_array(&dims, vec![].iter()),
                    Some(elements) => {
                        // TODO: Could we avoid this Row alloc if we made a
                        // push_array_with?
                        let elements_row = Row::try_from(elements)?;
                        self.push_array(&dims, elements_row.iter())
                    }
                }
                .map_err(|err| err.to_string())?
            }
            Some(DatumType::Dict(x)) => self.push_dict_with(|row| -> Result<(), String> {
                for e in x.elements.iter() {
                    row.push(Datum::from(e.key.as_str()));
                    let val = e
                        .val
                        .as_ref()
                        .ok_or_else(|| format!("missing val for key: {}", e.key))?;
                    row.try_push_proto(val)?;
                }
                Ok(())
            })?,
            Some(DatumType::Numeric(x)) => {
                // Reminder that special values like NaN, PosInf, and NegInf are
                // represented as variants of ProtoDatumOther.
                let n = Decimal::from_packed_bcd(&x.bcd, x.scale).map_err(|err| err.to_string())?;
                self.push(Datum::from(n))
            }
            Some(DatumType::MzTimestamp(x)) => self.push(Datum::MzTimestamp((*x).into())),
            Some(DatumType::Range(inner)) => {
                let ProtoRange { inner } = &**inner;
                match inner {
                    None => self.push_range(Range { inner: None }).unwrap(),
                    Some(inner) => {
                        let ProtoRangeInner {
                            lower_inclusive,
                            lower,
                            upper_inclusive,
                            upper,
                        } = &**inner;

                        self.push_range_with(
                            RangeLowerBound {
                                inclusive: *lower_inclusive,
                                bound: lower
                                    .as_ref()
                                    .map(|d| |row: &mut RowPacker| row.try_push_proto(&*d)),
                            },
                            RangeUpperBound {
                                inclusive: *upper_inclusive,
                                bound: upper
                                    .as_ref()
                                    .map(|d| |row: &mut RowPacker| row.try_push_proto(&*d)),
                            },
                        )
                        .expect("decoding ProtoRow must succeed");
                    }
                }
            }
            Some(DatumType::MzAclItem(x)) => self.push(Datum::MzAclItem(x.clone().into_rust()?)),
            None => return Err("unknown datum type".into()),
        };
        Ok(())
    }
}

/// TODO: remove this in favor of [`RustType::from_proto`].
impl TryFrom<&ProtoRow> for Row {
    type Error = String;

    fn try_from(x: &ProtoRow) -> Result<Self, Self::Error> {
        // TODO: Try to pre-size this.
        // see https://github.com/MaterializeInc/materialize/issues/12631
        let mut row = Row::default();
        let mut packer = row.packer();
        for d in x.datums.iter() {
            packer.try_push_proto(d)?;
        }
        Ok(row)
    }
}

impl RustType<ProtoRow> for Row {
    fn into_proto(&self) -> ProtoRow {
        let datums = self.iter().map(|x| x.into()).collect();
        ProtoRow { datums }
    }

    fn from_proto(proto: ProtoRow) -> Result<Self, TryFromProtoError> {
        // TODO: Try to pre-size this.
        // see https://github.com/MaterializeInc/materialize/issues/12631
        let mut row = Row::default();
        let mut packer = row.packer();
        for d in proto.datums.iter() {
            packer
                .try_push_proto(d)
                .map_err(TryFromProtoError::RowConversionError)?;
        }
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use mz_persist_types::Codec;
    use proptest::prelude::*;
    use uuid::Uuid;

    use crate::adt::array::ArrayDimension;
    use crate::adt::interval::Interval;
    use crate::adt::numeric::Numeric;
    use crate::adt::timestamp::CheckedTimestamp;
    use crate::{ColumnType, Datum, RelationDesc, Row, ScalarType};

    use super::*;

    // TODO: datadriven golden tests for various interesting Datums and Rows to
    // catch any changes in the encoding.

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn roundtrip() {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.extend([
            Datum::False,
            Datum::True,
            Datum::Int16(1),
            Datum::Int32(2),
            Datum::Int64(3),
            Datum::Float32(4f32.into()),
            Datum::Float64(5f64.into()),
            Datum::Date(
                NaiveDate::from_ymd_opt(6, 7, 8)
                    .unwrap()
                    .try_into()
                    .unwrap(),
            ),
            Datum::Time(NaiveTime::from_hms_opt(9, 10, 11).unwrap()),
            Datum::Timestamp(
                CheckedTimestamp::from_timestamplike(
                    NaiveDate::from_ymd_opt(12, 13 % 12, 14)
                        .unwrap()
                        .and_time(NaiveTime::from_hms_opt(15, 16, 17).unwrap()),
                )
                .unwrap(),
            ),
            Datum::TimestampTz(
                CheckedTimestamp::from_timestamplike(DateTime::from_utc(
                    NaiveDate::from_ymd_opt(18, 19 % 12, 20)
                        .unwrap()
                        .and_time(NaiveTime::from_hms_opt(21, 22, 23).unwrap()),
                    Utc,
                ))
                .unwrap(),
            ),
            Datum::Interval(Interval {
                months: 24,
                days: 42,
                micros: 25,
            }),
            Datum::Bytes(&[26, 27]),
            Datum::String("28"),
            Datum::from(Numeric::from(29)),
            Datum::from(Numeric::infinity()),
            Datum::from(-Numeric::infinity()),
            Datum::from(Numeric::nan()),
            Datum::JsonNull,
            Datum::Uuid(Uuid::from_u128(30)),
            Datum::Dummy,
            Datum::Null,
        ]);
        packer
            .push_array(
                &[ArrayDimension {
                    lower_bound: 2,
                    length: 2,
                }],
                vec![Datum::Int32(31), Datum::Int32(32)],
            )
            .expect("valid array");
        packer.push_list_with(|packer| {
            packer.push(Datum::String("33"));
            packer.push_list_with(|packer| {
                packer.push(Datum::String("34"));
                packer.push(Datum::String("35"));
            });
            packer.push(Datum::String("36"));
            packer.push(Datum::String("37"));
        });
        packer.push_dict_with(|row| {
            // Add a bunch of data to the hash to ensure we don't get a
            // HashMap's random iteration anywhere in the encode/decode path.
            let mut i = 38;
            for _ in 0..20 {
                row.push(Datum::String(&i.to_string()));
                row.push(Datum::Int32(i + 1));
                i += 2;
            }
        });

        let mut encoded = Vec::new();
        row.encode(&mut encoded);
        assert_eq!(Row::decode(&encoded), Ok(row));
    }

    fn schema_and_row() -> (RelationDesc, Row) {
        let row = Row::pack(vec![
            Datum::True,
            Datum::False,
            Datum::True,
            Datum::False,
            Datum::Null,
        ]);
        let schema = RelationDesc::from_names_and_types(vec![
            (
                "a",
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::Bool,
                },
            ),
            (
                "b",
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::Bool,
                },
            ),
            (
                "c",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::Bool,
                },
            ),
            (
                "d",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::Bool,
                },
            ),
            (
                "e",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::Bool,
                },
            ),
        ]);
        (schema, row)
    }

    #[mz_ore::test]
    fn columnar_roundtrip() {
        let (schema, row) = schema_and_row();
        assert_eq!(
            mz_persist_types::columnar::validate_roundtrip(&schema, &row, false),
            Ok(())
        );
    }

    #[mz_ore::test]
    fn parquet_roundtrip() {
        let (schema, row) = schema_and_row();
        assert_eq!(
            mz_persist_types::parquet::validate_roundtrip(&schema, &row),
            Ok(())
        );
    }

    fn scalar_type_columnar_roundtrip(scalar_type: ScalarType) {
        let skip_decode = is_no_stats_type(&scalar_type);

        use mz_persist_types::columnar::validate_roundtrip;
        let mut rows = Vec::new();
        for datum in scalar_type.interesting_datums() {
            rows.push(Row::pack(std::iter::once(datum)));
        }

        // Non-nullable version of the column.
        let schema = RelationDesc::empty().with_column("col", scalar_type.clone().nullable(false));
        for row in rows.iter() {
            assert_eq!(validate_roundtrip(&schema, row, skip_decode), Ok(()));
        }

        // Nullable version of the column.
        let schema = RelationDesc::empty().with_column("col", scalar_type.nullable(true));
        rows.push(Row::pack(std::iter::once(Datum::Null)));
        for row in rows.iter() {
            assert_eq!(validate_roundtrip(&schema, row, skip_decode), Ok(()));
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn all_scalar_types_columnar_roundtrip() {
        proptest!(|(scalar_type in any::<ScalarType>())| {
            // The proptest! macro interferes with rustfmt.
            scalar_type_columnar_roundtrip(scalar_type)
        });
    }
}
