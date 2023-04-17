// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)] // For generated protos.

//! Aggregate statistics about data stored in persist.

use std::any::Any;
use std::collections::BTreeMap;

use crate::columnar::Data;
use crate::part::DynColumnRef;

include!(concat!(env!("OUT_DIR"), "/mz_persist_types.stats.rs"));

/// The logic to use when computing stats for a column of `T: Data`.
///
/// If Custom is used, the DynStats returned must be a`<T as Data>::Stats`.
pub enum StatsFn {
    Default,
    Custom(fn(&DynColumnRef) -> Result<Box<dyn DynStats>, String>),
}

/// Aggregate statistics about a column of type `T`.
pub trait ColumnStats<T: Data>: DynStats {
    /// An inclusive lower bound on the data contained in the column, if known.
    ///
    /// This will often be a tight bound, but it's not guaranteed. Persist
    /// reserves the right to (for example) invent smaller bounds for long byte
    /// strings. SUBTLE: This means that this exact value may not be present in
    /// the column.
    ///
    /// Similarly, if the column is empty, this will contain `T: Default`.
    /// Emptiness will be indicated in statistics higher up (i.e.
    /// [StructStats]).
    fn lower<'a>(&'a self) -> Option<T::Ref<'a>>;
    /// Same as [Self::lower] but an (also inclusive) upper bound.
    fn upper<'a>(&'a self) -> Option<T::Ref<'a>>;
    /// The number of `None`s if this column is optional or 0 if it isn't.
    fn none_count(&self) -> usize;
}

/// Type-erased aggregate statistics about a column of data.
///
/// TODO(mfp): It feels like we haven't hit a global maximum here, both with
/// this `DynStats` trait and also with ProtoOptionStats.
pub trait DynStats: std::fmt::Debug + Send + Sync + 'static {
    /// Returns self as a `dyn Any` for downcasting.
    fn as_any(&self) -> &dyn Any;
    /// Returns the name of the erased type for use in error messages.
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
    /// See [mz_proto::RustType::into_proto].
    fn into_proto(&self) -> ProtoDynStats;
}

/// Statistics about a column of some non-optional parquet type.
pub struct PrimitiveStats<T> {
    /// An inclusive lower bound on the data contained in the column.
    ///
    /// This will often be a tight bound, but it's not guaranteed. Persist
    /// reserves the right to (for example) invent smaller bounds for long byte
    /// strings. SUBTLE: This means that this exact value may not be present in
    /// the column.
    ///
    /// Similarly, if the column is empty, this will contain `T: Default`.
    /// Emptiness will be indicated in statistics higher up (i.e.
    /// [StructStats]).
    pub lower: T,
    /// Same as [Self::lower] but an (also inclusive) upper bound.
    pub upper: T,
}

impl<T: std::fmt::Debug> std::fmt::Debug for PrimitiveStats<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let PrimitiveStats { lower, upper } = self;
        f.debug_tuple("p").field(lower).field(upper).finish()
    }
}

/// Statistics about a column of some optional type.
pub struct OptionStats<T> {
    /// Statistics about the `Some` values in the column.
    pub some: T,
    /// The count of `None` values in the column.
    pub none: usize,
}

impl<T: std::fmt::Debug> std::fmt::Debug for OptionStats<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let OptionStats { some, none } = self;
        f.debug_tuple("o").field(some).field(none).finish()
    }
}

/// Statistics about a column of a struct type with a uniform schema (the same
/// columns and associated `T: Data` types in each instance of the struct).
#[derive(Debug, Default)]
pub struct StructStats {
    /// The count of structs in the column.
    pub len: usize,
    /// Statistics about each of the columns in the struct.
    ///
    /// This will often be all of the columns, but it's not guaranteed. Persist
    /// reserves the right to prune statistics about some or all of the columns.
    pub cols: BTreeMap<String, Box<dyn DynStats>>,
}

impl StructStats {
    /// Returns the statistics for the given column in the struct.
    ///
    /// This will often be all of the columns, but it's not guaranteed. Persist
    /// reserves the right to prune statistics about some or all of the columns.
    pub fn col<T: Data>(&self, name: &str) -> Result<Option<&T::Stats>, String> {
        let Some(stats) = self.cols.get(name) else {
            return Ok(None);
        };
        match stats.as_any().downcast_ref() {
            Some(x) => Ok(Some(x)),
            None => Err(format!(
                "expected stats type {} got {}",
                std::any::type_name::<T::Stats>(),
                stats.type_name()
            )),
        }
    }
}

// Aggregate statistics about a column of Json elements.
//
// Each element could be any of a JsonNull, a bool, a string, a numeric, a list,
// or a map/object. The column might be a single type but could also be a
// mixture of any subset of these types.
#[derive(Default)]
pub struct JsonStats {
    /// The count of `Datum::JsonNull`s, or 0 if there were none.
    pub json_nulls: usize,
    /// The min and max bools, or None if there were none.
    pub bools: Option<PrimitiveStats<bool>>,
    /// The min and max strings, or None if there were none.
    pub string: Option<PrimitiveStats<String>>,
    /// The min and max numerics, or None if there were none.
    ///
    /// TODO(mfp): Storing this as an f64 is not correct.
    pub numeric: Option<PrimitiveStats<f64>>,
    /// The count of `Datum::List`s, or 0 if there were none.
    ///
    /// TODO: We could also do something for list indexes analogous to what we
    /// do for map keys, but it initially seems much less likely that a user
    /// would expect that to work with pushdown, so don't bother keeping the
    /// stats until someone asks for it.
    pub list: usize,
    /// Recursive statistics about the set of keys present in any maps/objects
    /// in the column, or None if there were no maps/objects.
    pub map: BTreeMap<String, JsonStats>,
    /// True if maps may be present. (This is _almost_ redundant
    /// with the above, but handles the case where a map may not have any fields
    /// or fields have been pruned.)
    pub maps: bool,
}

impl std::fmt::Debug for JsonStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let JsonStats {
            json_nulls,
            bools,
            string,
            numeric,
            list,
            map,
            maps,
        } = self;
        let mut f = &mut f.debug_tuple("json");
        if json_nulls > &0 {
            f = f.field(json_nulls);
        }
        if let Some(bools) = bools {
            f = f.field(bools);
        }
        if let Some(string) = string {
            f = f.field(string);
        }
        if let Some(numeric) = numeric {
            f = f.field(numeric);
        }
        if list > &0 {
            f = f.field(list);
        }
        if !map.is_empty() {
            f = f.field(map);
        }
        if *maps {
            f = f.field(&"maps")
        }
        f.finish()
    }
}

/// Statistics about a column of `Vec<u8>`.
pub enum BytesStats {
    Primitive(PrimitiveStats<Vec<u8>>),
    Json(JsonStats),
}

impl std::fmt::Debug for BytesStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BytesStats::Primitive(x) => std::fmt::Debug::fmt(x, f),
            BytesStats::Json(x) => std::fmt::Debug::fmt(x, f),
        }
    }
}

/// The length to truncate `Vec<u8>` and `String` stats to.
//
// Ideally, this would be in LaunchDarkly, but the plumbing is tough.
pub const TRUNCATE_LEN: usize = 100;

/// Whether a truncated value should be a lower or upper bound on the original.
pub enum TruncateBound {
    /// Truncate such that the result is <= the original.
    Lower,
    /// Truncate such that the result is >= the original.
    Upper,
}

/// Truncates a u8 slice to the given maximum byte length.
///
/// If `bound` is Lower, the returned value will sort <= the original value, and
/// if `bound` is Upper, it will sort >= the original value.
///
/// Lower bounds will always return Some. Upper bounds might return None if the
/// part that fits in `max_len` is entirely made of `u8::MAX`.
pub fn truncate_bytes(x: &[u8], max_len: usize, bound: TruncateBound) -> Option<Vec<u8>> {
    if x.len() <= max_len {
        return Some(x.to_owned());
    }
    match bound {
        // Any truncation is a lower bound.
        TruncateBound::Lower => Some(x[..max_len].to_owned()),
        TruncateBound::Upper => {
            for idx in (0..max_len).rev() {
                if x[idx] < u8::MAX {
                    let mut ret = x[..=idx].to_owned();
                    ret[idx] += 1;
                    return Some(ret);
                }
            }
            None
        }
    }
}

/// Truncates a string to the given maximum byte length.
///
/// The returned value is a valid utf-8 string. If `bound` is Lower, it will
/// sort <= the original string, and if `bound` is Upper, it will sort >= the
/// original string.
///
/// Lower bounds will always return Some. Upper bounds might return None if the
/// part that fits in `max_len` is entirely made of `char::MAX` (so in practice,
/// probably ~never).
pub fn truncate_string(x: &str, max_len: usize, bound: TruncateBound) -> Option<String> {
    if x.len() <= max_len {
        return Some(x.to_owned());
    }
    // For the output to be valid utf-8, we have to truncate along a char
    // boundary.
    let truncation_idx = x
        .char_indices()
        .map(|(idx, c)| idx + c.len_utf8())
        .take_while(|char_end| *char_end <= max_len)
        .last()
        .unwrap_or(0);
    let truncated = &x[..truncation_idx];
    match bound {
        // Any truncation is a lower bound.
        TruncateBound::Lower => Some(truncated.to_owned()),
        TruncateBound::Upper => {
            // See if we can find a char that's not already the max. If so, take
            // the last of these and increment it.
            for (idx, c) in truncated.char_indices().rev() {
                if let Ok(new_last_char) = char::try_from(u32::from(c) + 1) {
                    // NB: It's technically possible for `new_last_char` to be
                    // more bytes than `c`, which means we could go over
                    // max_len. It isn't a hard requirement for the initial
                    // caller of this, so don't bother with the complexity yet.
                    let mut ret = String::with_capacity(idx + new_last_char.len_utf8());
                    ret.push_str(&truncated[..idx]);
                    ret.push(new_last_char);
                    return Some(ret);
                }
            }
            None
        }
    }
}

mod impls {
    use std::any::Any;
    use std::collections::BTreeMap;

    use arrow2::array::{BinaryArray, BooleanArray, PrimitiveArray, Utf8Array};
    use arrow2::bitmap::Bitmap;
    use arrow2::buffer::Buffer;
    use arrow2::compute::aggregate::SimdOrd;
    use arrow2::types::simd::Simd;
    use arrow2::types::NativeType;
    use mz_proto::{ProtoType, RustType, TryFromProtoError};

    use crate::columnar::Data;
    use crate::stats::{
        proto_bytes_stats, proto_dyn_stats, proto_primitive_stats, truncate_bytes, truncate_string,
        BytesStats, ColumnStats, DynStats, JsonStats, OptionStats, PrimitiveStats, ProtoBytesStats,
        ProtoDynStats, ProtoJsonStats, ProtoOptionStats, ProtoPrimitiveStats, ProtoStructStats,
        StructStats, TruncateBound, TRUNCATE_LEN,
    };

    impl<T> DynStats for PrimitiveStats<T>
    where
        PrimitiveStats<T>: RustType<ProtoPrimitiveStats> + std::fmt::Debug + Send + Sync + 'static,
    {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn into_proto(&self) -> ProtoDynStats {
            ProtoDynStats {
                option: None,
                kind: Some(proto_dyn_stats::Kind::Primitive(RustType::into_proto(self))),
            }
        }
    }

    impl<T: DynStats> DynStats for OptionStats<T> {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn into_proto(&self) -> ProtoDynStats {
            let mut ret = self.some.into_proto();
            // This prevents us from serializing `OptionStats<OptionStats<T>>`, but
            // that's intentionally out of scope. See the comment on ProtoDynStats.
            assert!(ret.option.is_none());
            ret.option = Some(ProtoOptionStats {
                none: self.none.into_proto(),
            });
            ret
        }
    }

    impl DynStats for StructStats {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn into_proto(&self) -> ProtoDynStats {
            ProtoDynStats {
                option: None,
                kind: Some(proto_dyn_stats::Kind::Struct(RustType::into_proto(self))),
            }
        }
    }

    impl DynStats for BytesStats {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn into_proto(&self) -> ProtoDynStats {
            ProtoDynStats {
                option: None,
                kind: Some(proto_dyn_stats::Kind::Bytes(RustType::into_proto(self))),
            }
        }
    }

    macro_rules! stats_primitive {
        ($data:ty, $ref:ident) => {
            impl ColumnStats<$data> for PrimitiveStats<$data> {
                fn lower<'a>(&'a self) -> Option<<$data as Data>::Ref<'a>> {
                    Some(self.lower.$ref())
                }
                fn upper<'a>(&'a self) -> Option<<$data as Data>::Ref<'a>> {
                    Some(self.upper.$ref())
                }
                fn none_count(&self) -> usize {
                    0
                }
            }

            impl ColumnStats<Option<$data>> for OptionStats<PrimitiveStats<$data>> {
                fn lower<'a>(&'a self) -> Option<<Option<$data> as Data>::Ref<'a>> {
                    Some(self.some.lower())
                }
                fn upper<'a>(&'a self) -> Option<<Option<$data> as Data>::Ref<'a>> {
                    Some(self.some.upper())
                }
                fn none_count(&self) -> usize {
                    self.none
                }
            }
        };
    }

    stats_primitive!(bool, clone);
    stats_primitive!(u8, clone);
    stats_primitive!(u16, clone);
    stats_primitive!(u32, clone);
    stats_primitive!(u64, clone);
    stats_primitive!(i8, clone);
    stats_primitive!(i16, clone);
    stats_primitive!(i32, clone);
    stats_primitive!(i64, clone);
    stats_primitive!(f32, clone);
    stats_primitive!(f64, clone);
    stats_primitive!(Vec<u8>, as_slice);
    stats_primitive!(String, as_str);

    impl ColumnStats<Vec<u8>> for BytesStats {
        fn lower<'a>(&'a self) -> Option<<Vec<u8> as Data>::Ref<'a>> {
            match self {
                BytesStats::Primitive(x) => x.lower(),
                BytesStats::Json(_) => None,
            }
        }
        fn upper<'a>(&'a self) -> Option<<Vec<u8> as Data>::Ref<'a>> {
            match self {
                BytesStats::Primitive(x) => x.upper(),
                BytesStats::Json(_) => None,
            }
        }
        fn none_count(&self) -> usize {
            0
        }
    }

    impl ColumnStats<Option<Vec<u8>>> for OptionStats<BytesStats> {
        fn lower<'a>(&'a self) -> Option<<Option<Vec<u8>> as Data>::Ref<'a>> {
            self.some.lower().map(Some)
        }
        fn upper<'a>(&'a self) -> Option<<Option<Vec<u8>> as Data>::Ref<'a>> {
            self.some.upper().map(Some)
        }
        fn none_count(&self) -> usize {
            self.none
        }
    }

    impl From<&Bitmap> for PrimitiveStats<bool> {
        fn from(value: &Bitmap) -> Self {
            // Needing this Array is a bit unfortunate, but the clone is cheap.
            // We could probably avoid it entirely with a PR to arrow2.
            let value =
                BooleanArray::new(arrow2::datatypes::DataType::Boolean, value.clone(), None);
            let lower = arrow2::compute::aggregate::min_boolean(&value).unwrap_or_default();
            let upper = arrow2::compute::aggregate::min_boolean(&value).unwrap_or_default();
            PrimitiveStats { lower, upper }
        }
    }

    impl From<&BooleanArray> for OptionStats<PrimitiveStats<bool>> {
        fn from(value: &BooleanArray) -> Self {
            let lower = arrow2::compute::aggregate::min_boolean(value).unwrap_or_default();
            let upper = arrow2::compute::aggregate::min_boolean(value).unwrap_or_default();
            let none = value.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl<T> From<&Buffer<T>> for PrimitiveStats<T>
    where
        T: NativeType + Simd,
        T::Simd: SimdOrd<T>,
    {
        fn from(value: &Buffer<T>) -> Self {
            // Needing this Array is a bit unfortunate, but the clone is cheap.
            // We could probably avoid it entirely with a PR to arrow2.
            let value = PrimitiveArray::new(T::PRIMITIVE.into(), value.clone(), None);
            let lower = arrow2::compute::aggregate::min_primitive::<T>(&value).unwrap_or_default();
            let upper = arrow2::compute::aggregate::max_primitive::<T>(&value).unwrap_or_default();
            PrimitiveStats { lower, upper }
        }
    }

    impl<T> From<&PrimitiveArray<T>> for OptionStats<PrimitiveStats<T>>
    where
        T: NativeType + Simd,
        T::Simd: SimdOrd<T>,
    {
        fn from(value: &PrimitiveArray<T>) -> Self {
            let lower = arrow2::compute::aggregate::min_primitive::<T>(value).unwrap_or_default();
            let upper = arrow2::compute::aggregate::max_primitive::<T>(value).unwrap_or_default();
            let none = value.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl From<&BinaryArray<i32>> for PrimitiveStats<Vec<u8>> {
        fn from(value: &BinaryArray<i32>) -> Self {
            assert!(value.validity().is_none());
            let lower = arrow2::compute::aggregate::min_binary(value).unwrap_or_default();
            let lower = truncate_bytes(lower, TRUNCATE_LEN, TruncateBound::Lower)
                .expect("lower bound should always truncate");
            let upper = arrow2::compute::aggregate::max_binary(value).unwrap_or_default();
            let upper = truncate_bytes(upper, TRUNCATE_LEN, TruncateBound::Upper)
                // TODO(mfp): Instead, truncate this column's stats entirely.
                .unwrap_or_else(|| upper.to_owned());
            PrimitiveStats { lower, upper }
        }
    }

    impl From<&BinaryArray<i32>> for OptionStats<PrimitiveStats<Vec<u8>>> {
        fn from(value: &BinaryArray<i32>) -> Self {
            let lower = arrow2::compute::aggregate::min_binary(value).unwrap_or_default();
            let lower = truncate_bytes(lower, TRUNCATE_LEN, TruncateBound::Lower)
                .expect("lower bound should always truncate");
            let upper = arrow2::compute::aggregate::max_binary(value).unwrap_or_default();
            let upper = truncate_bytes(upper, TRUNCATE_LEN, TruncateBound::Upper)
                // TODO(mfp): Instead, truncate this column's stats entirely.
                .unwrap_or_else(|| upper.to_owned());
            let none = value.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl From<&BinaryArray<i32>> for BytesStats {
        fn from(value: &BinaryArray<i32>) -> Self {
            BytesStats::Primitive(value.into())
        }
    }

    impl From<&BinaryArray<i32>> for OptionStats<BytesStats> {
        fn from(value: &BinaryArray<i32>) -> Self {
            let stats = OptionStats::<PrimitiveStats<Vec<u8>>>::from(value);
            OptionStats {
                none: stats.none,
                some: BytesStats::Primitive(stats.some),
            }
        }
    }

    impl From<&Utf8Array<i32>> for PrimitiveStats<String> {
        fn from(value: &Utf8Array<i32>) -> Self {
            assert!(value.validity().is_none());
            let lower = arrow2::compute::aggregate::min_string(value).unwrap_or_default();
            let lower = truncate_string(lower, TRUNCATE_LEN, TruncateBound::Lower)
                .expect("lower bound should always truncate");
            let upper = arrow2::compute::aggregate::max_string(value).unwrap_or_default();
            let upper = truncate_string(upper, TRUNCATE_LEN, TruncateBound::Upper)
                // TODO(mfp): Instead, truncate this column's stats entirely.
                .unwrap_or_else(|| upper.to_owned());
            PrimitiveStats { lower, upper }
        }
    }

    impl From<&Utf8Array<i32>> for OptionStats<PrimitiveStats<String>> {
        fn from(value: &Utf8Array<i32>) -> Self {
            let lower = arrow2::compute::aggregate::min_string(value).unwrap_or_default();
            let lower = truncate_string(lower, TRUNCATE_LEN, TruncateBound::Lower)
                .expect("lower bound should always truncate");
            let upper = arrow2::compute::aggregate::max_string(value).unwrap_or_default();
            let upper = truncate_string(upper, TRUNCATE_LEN, TruncateBound::Upper)
                // TODO(mfp): Instead, truncate this column's stats entirely.
                .unwrap_or_else(|| upper.to_owned());
            let none = value.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl RustType<ProtoStructStats> for StructStats {
        fn into_proto(&self) -> ProtoStructStats {
            ProtoStructStats {
                len: self.len.into_proto(),
                cols: self
                    .cols
                    .iter()
                    .map(|(k, v)| (k.into_proto(), v.into_proto()))
                    .collect(),
            }
        }

        fn from_proto(proto: ProtoStructStats) -> Result<Self, TryFromProtoError> {
            let mut cols = BTreeMap::new();
            for (k, v) in proto.cols {
                cols.insert(k.into_rust()?, v.into_rust()?);
            }
            Ok(StructStats {
                len: proto.len.into_rust()?,
                cols,
            })
        }
    }

    impl RustType<ProtoJsonStats> for JsonStats {
        fn into_proto(&self) -> ProtoJsonStats {
            ProtoJsonStats {
                json_nulls: self.json_nulls.into_proto(),
                bools: self.bools.into_proto(),
                string: self.string.into_proto(),
                numeric: self.numeric.into_proto(),
                list: self.list.into_proto(),
                map: self
                    .map
                    .iter()
                    .map(|(k, v)| (k.into_proto(), RustType::into_proto(v)))
                    .collect(),
                no_maps: !self.maps.into_proto(),
            }
        }

        fn from_proto(proto: ProtoJsonStats) -> Result<Self, TryFromProtoError> {
            let mut map = BTreeMap::new();
            for (k, v) in proto.map {
                map.insert(k.into_rust()?, v.into_rust()?);
            }
            Ok(JsonStats {
                json_nulls: proto.json_nulls.into_rust()?,
                bools: proto.bools.into_rust()?,
                string: proto.string.into_rust()?,
                numeric: proto.numeric.into_rust()?,
                list: proto.list.into_rust()?,
                map,
                maps: !proto.no_maps.into_rust()?,
            })
        }
    }

    impl RustType<ProtoBytesStats> for BytesStats {
        fn into_proto(&self) -> ProtoBytesStats {
            let kind = match self {
                BytesStats::Primitive(x) => {
                    proto_bytes_stats::Kind::Primitive(RustType::into_proto(x))
                }
                BytesStats::Json(x) => proto_bytes_stats::Kind::Json(RustType::into_proto(x)),
            };
            ProtoBytesStats { kind: Some(kind) }
        }

        fn from_proto(proto: ProtoBytesStats) -> Result<Self, TryFromProtoError> {
            match proto.kind {
                Some(proto_bytes_stats::Kind::Primitive(x)) => Ok(BytesStats::Primitive(
                    PrimitiveStats::<Vec<u8>>::from_proto(x)?,
                )),
                Some(proto_bytes_stats::Kind::Json(x)) => {
                    Ok(BytesStats::Json(JsonStats::from_proto(x)?))
                }
                None => Err(TryFromProtoError::missing_field("ProtoBytesStats::kind")),
            }
        }
    }

    impl RustType<ProtoDynStats> for Box<dyn DynStats> {
        fn into_proto(&self) -> ProtoDynStats {
            DynStats::into_proto(self.as_ref())
        }

        fn from_proto(mut proto: ProtoDynStats) -> Result<Self, TryFromProtoError> {
            struct BoxFn;
            impl DynStatsFn<Box<dyn DynStats>> for BoxFn {
                fn call<T: DynStats>(self, t: T) -> Result<Box<dyn DynStats>, TryFromProtoError> {
                    Ok(Box::new(t))
                }
            }
            struct OptionStatsFn<F>(usize, F);
            impl<R, F: DynStatsFn<R>> DynStatsFn<R> for OptionStatsFn<F> {
                fn call<T: DynStats>(self, some: T) -> Result<R, TryFromProtoError> {
                    let OptionStatsFn(none, f) = self;
                    f.call(OptionStats { none, some })
                }
            }

            match proto.option.take() {
                Some(option) => {
                    let none = option.none.into_rust()?;
                    dyn_from_proto(proto, OptionStatsFn(none, BoxFn))
                }
                None => dyn_from_proto(proto, BoxFn),
            }
        }
    }

    /// Basically `FnOnce<T: DynStats>(self, t: T) -> R`, if rust would let us
    /// type that.
    ///
    /// We use this in `dyn_from_proto` so that OptionStats can hold a `some: T`
    /// instead of a `Box<dyn DynStats>`.
    trait DynStatsFn<R> {
        fn call<T: DynStats>(self, t: T) -> Result<R, TryFromProtoError>;
    }

    fn dyn_from_proto<R, F: DynStatsFn<R>>(
        proto: ProtoDynStats,
        f: F,
    ) -> Result<R, TryFromProtoError> {
        assert!(proto.option.is_none());
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoDynStats::kind"))?;
        match kind {
            // Sniff the type of x.lower and use that to determine which type of
            // PrimitiveStats to decode it as.
            proto_dyn_stats::Kind::Primitive(x) => match x.lower {
                Some(proto_primitive_stats::Lower::LowerBool(_)) => {
                    f.call(PrimitiveStats::<bool>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerU8(_)) => {
                    f.call(PrimitiveStats::<u8>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerU16(_)) => {
                    f.call(PrimitiveStats::<u16>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerU32(_)) => {
                    f.call(PrimitiveStats::<u32>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerU64(_)) => {
                    f.call(PrimitiveStats::<u64>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerI8(_)) => {
                    f.call(PrimitiveStats::<i8>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerI16(_)) => {
                    f.call(PrimitiveStats::<i16>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerI32(_)) => {
                    f.call(PrimitiveStats::<i32>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerI64(_)) => {
                    f.call(PrimitiveStats::<i64>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerF32(_)) => {
                    f.call(PrimitiveStats::<f32>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerF64(_)) => {
                    f.call(PrimitiveStats::<f64>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerBytes(_)) => {
                    f.call(PrimitiveStats::<Vec<u8>>::from_proto(x)?)
                }
                Some(proto_primitive_stats::Lower::LowerString(_)) => {
                    f.call(PrimitiveStats::<String>::from_proto(x)?)
                }
                None => Err(TryFromProtoError::missing_field("ProtoPrimitiveStats::min")),
            },
            proto_dyn_stats::Kind::Struct(x) => f.call(StructStats::from_proto(x)?),
            proto_dyn_stats::Kind::Bytes(x) => f.call(BytesStats::from_proto(x)?),
        }
    }

    macro_rules! primitive_stats_rust_type {
        ($typ:ty, $lower:ident, $upper:ident) => {
            impl RustType<ProtoPrimitiveStats> for PrimitiveStats<$typ> {
                fn into_proto(&self) -> ProtoPrimitiveStats {
                    ProtoPrimitiveStats {
                        lower: Some(proto_primitive_stats::Lower::$lower(
                            self.lower.into_proto(),
                        )),
                        upper: Some(proto_primitive_stats::Upper::$upper(
                            self.upper.into_proto(),
                        )),
                    }
                }

                fn from_proto(proto: ProtoPrimitiveStats) -> Result<Self, TryFromProtoError> {
                    let lower = proto.lower.ok_or_else(|| {
                        TryFromProtoError::missing_field("ProtoPrimitiveStats::lower")
                    })?;
                    let lower = match lower {
                        proto_primitive_stats::Lower::$lower(x) => x.into_rust()?,
                        _ => {
                            return Err(TryFromProtoError::missing_field(
                                "proto_primitive_stats::Lower::$lower",
                            ))
                        }
                    };
                    let upper = proto.upper.ok_or_else(|| {
                        TryFromProtoError::missing_field("ProtoPrimitiveStats::max")
                    })?;
                    let upper = match upper {
                        proto_primitive_stats::Upper::$upper(x) => x.into_rust()?,
                        _ => {
                            return Err(TryFromProtoError::missing_field(
                                "proto_primitive_stats::Upper::$upper",
                            ))
                        }
                    };
                    Ok(PrimitiveStats { lower, upper })
                }
            }
        };
    }

    primitive_stats_rust_type!(bool, LowerBool, UpperBool);
    primitive_stats_rust_type!(u8, LowerU8, UpperU8);
    primitive_stats_rust_type!(u16, LowerU16, UpperU16);
    primitive_stats_rust_type!(u32, LowerU32, UpperU32);
    primitive_stats_rust_type!(u64, LowerU64, UpperU64);
    primitive_stats_rust_type!(i8, LowerI8, UpperI8);
    primitive_stats_rust_type!(i16, LowerI16, UpperI16);
    primitive_stats_rust_type!(i32, LowerI32, UpperI32);
    primitive_stats_rust_type!(i64, LowerI64, UpperI64);
    primitive_stats_rust_type!(f32, LowerF32, UpperF32);
    primitive_stats_rust_type!(f64, LowerF64, UpperF64);
    primitive_stats_rust_type!(Vec<u8>, LowerBytes, UpperBytes);
    primitive_stats_rust_type!(String, LowerString, UpperString);
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_truncate_bytes() {
        #[track_caller]
        fn testcase(x: &[u8], max_len: usize, upper_should_exist: bool) {
            let lower = truncate_bytes(x, max_len, TruncateBound::Lower)
                .expect("lower should always exist");
            assert!(lower.len() <= max_len);
            assert!(lower.as_slice() <= x);
            let upper = truncate_bytes(x, max_len, TruncateBound::Upper);
            assert_eq!(upper_should_exist, upper.is_some());
            if let Some(upper) = upper {
                assert!(upper.len() <= max_len);
                assert!(upper.as_slice() >= x);
            }
        }

        testcase(&[], 0, true);
        testcase(&[], 1, true);
        testcase(&[1], 0, false);
        testcase(&[1], 1, true);
        testcase(&[1], 2, true);
        testcase(&[1, 2], 1, true);
        testcase(&[1, 255], 2, true);
        testcase(&[255, 255], 2, true);
        testcase(&[255, 255, 255], 2, false);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_truncate_bytes_proptest() {
        fn testcase(x: &[u8]) {
            for max_len in 0..=x.len() {
                let lower = truncate_bytes(x, max_len, TruncateBound::Lower)
                    .expect("lower should always exist");
                let upper = truncate_bytes(x, max_len, TruncateBound::Upper);
                assert!(lower.len() <= max_len);
                assert!(lower.as_slice() <= x);
                if let Some(upper) = upper {
                    assert!(upper.len() <= max_len);
                    assert!(upper.as_slice() >= x);
                }
            }
        }

        proptest!(|(x in any::<Vec<u8>>())| {
            // The proptest! macro interferes with rustfmt.
            testcase(x.as_slice())
        });
    }

    #[test]
    fn test_truncate_string() {
        #[track_caller]
        fn testcase(x: &str, max_len: usize, upper_should_exist: bool) {
            let lower = truncate_string(x, max_len, TruncateBound::Lower)
                .expect("lower should always exist");
            let upper = truncate_string(x, max_len, TruncateBound::Upper);
            assert!(lower.len() <= max_len);
            assert!(lower.as_str() <= x);
            assert_eq!(upper_should_exist, upper.is_some());
            if let Some(upper) = upper {
                assert!(upper.len() <= max_len);
                assert!(upper.as_str() >= x);
            }
        }

        testcase("", 0, true);
        testcase("1", 0, false);
        testcase("1", 1, true);
        testcase("12", 1, true);
        testcase("⛄", 0, false);
        testcase("⛄", 1, false);
        testcase("⛄", 3, true);
        testcase("\u{10FFFF}", 3, false);
        testcase("\u{10FFFF}", 4, true);
        testcase("\u{10FFFF}", 5, true);
        testcase("⛄⛄", 3, true);
        testcase("⛄⛄", 4, true);
        testcase("⛄\u{10FFFF}", 6, true);
        testcase("⛄\u{10FFFF}", 7, true);
        testcase("\u{10FFFF}\u{10FFFF}", 7, false);
        testcase("\u{10FFFF}\u{10FFFF}", 8, true);

        // Just because I find this to be delightful.
        assert_eq!(
            truncate_string("⛄⛄", 3, TruncateBound::Upper),
            Some("⛅".to_string())
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_truncate_string_proptest() {
        fn testcase(x: &str) {
            for max_len in 0..=x.len() {
                let lower = truncate_string(x, max_len, TruncateBound::Lower)
                    .expect("lower should always exist");
                let upper = truncate_string(x, max_len, TruncateBound::Upper);
                assert!(lower.len() <= max_len);
                assert!(lower.as_str() <= x);
                if let Some(upper) = upper {
                    assert!(upper.len() <= max_len);
                    assert!(upper.as_str() >= x);
                }
            }
        }

        proptest!(|(x in any::<String>())| {
            // The proptest! macro interferes with rustfmt.
            testcase(x.as_str())
        });
    }
}
