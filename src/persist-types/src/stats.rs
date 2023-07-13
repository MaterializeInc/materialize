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
use std::fmt::Debug;

use proptest_derive::Arbitrary;
use prost::Message;
use serde::ser::{SerializeMap, SerializeStruct};
use serde_json::json;

use crate::columnar::Data;
use crate::dyn_col::DynColumnRef;
use crate::dyn_struct::ValidityRef;
use crate::stats::impls::any_struct_stats_cols;

include!(concat!(env!("OUT_DIR"), "/mz_persist_types.stats.rs"));

/// The logic to use when computing stats for a column of `T: Data`.
///
/// If Custom is used, the DynStats returned must be a`<T as Data>::Stats`.
pub enum StatsFn {
    Default,
    Custom(fn(&DynColumnRef, ValidityRef<'_>) -> Result<Box<dyn DynStats>, String>),
}

#[cfg(debug_assertions)]
impl PartialEq for StatsFn {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StatsFn::Default, StatsFn::Default) => true,
            (StatsFn::Custom(s), StatsFn::Custom(o)) => {
                let s: fn(
                    &'static DynColumnRef,
                    ValidityRef<'static>,
                ) -> Result<Box<dyn DynStats>, String> = *s;
                let o: fn(
                    &'static DynColumnRef,
                    ValidityRef<'static>,
                ) -> Result<Box<dyn DynStats>, String> = *o;
                // I think this is not always correct, but it's only used in
                // debug_assertions so as long as CI is happy with it, probably
                // good enough.
                s == o
            }
            (StatsFn::Default, StatsFn::Custom(_)) | (StatsFn::Custom(_), StatsFn::Default) => {
                false
            }
        }
    }
}

impl std::fmt::Debug for StatsFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "Default"),
            Self::Custom(_) => f.debug_struct("Custom").finish_non_exhaustive(),
        }
    }
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

/// A source of aggregate statistics about a column of data.
pub trait StatsFrom<T> {
    /// Computes statistics from a column of data.
    ///
    /// The validity, if given, indicates which values in the columns are and
    /// are not used for stats. This allows us to model non-nullable columns in
    /// a nullable struct. For optional columns (i.e. ones with their own
    /// validity) it _must be a subset_ of the column's validity, otherwise this
    /// panics.
    fn stats_from(col: &T, validity: ValidityRef<'_>) -> Self;
}

/// Type-erased aggregate statistics about a column of data.
pub trait DynStats: Debug + Send + Sync + 'static {
    /// Returns self as a `dyn Any` for downcasting.
    fn as_any(&self) -> &dyn Any;
    /// Returns the name of the erased type for use in error messages.
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
    /// See [mz_proto::RustType::into_proto].
    fn into_proto(&self) -> ProtoDynStats;
    /// Formats these statistics for use in `INSPECT SHARD` and debugging.
    fn debug_json(&self) -> serde_json::Value;
}

/// Statistics about a column of some non-optional parquet type.
#[cfg_attr(any(test), derive(Clone))]
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

/// Statistics about a column of some optional type.
pub struct OptionStats<T> {
    /// Statistics about the `Some` values in the column.
    pub some: T,
    /// The count of `None` values in the column.
    pub none: usize,
}

/// Statistics about a column of a struct type with a uniform schema (the same
/// columns and associated `T: Data` types in each instance of the struct).
#[derive(Arbitrary, Default)]
pub struct StructStats {
    /// The count of structs in the column.
    pub len: usize,
    /// Statistics about each of the columns in the struct.
    ///
    /// This will often be all of the columns, but it's not guaranteed. Persist
    /// reserves the right to prune statistics about some or all of the columns.
    #[proptest(strategy = "any_struct_stats_cols()")]
    pub cols: BTreeMap<String, Box<dyn DynStats>>,
}

impl std::fmt::Debug for StructStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.debug_json(), f)
    }
}

impl serde::Serialize for StructStats {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let StructStats { len, cols } = self;
        let mut s = s.serialize_struct("StructStats", 2)?;
        let () = s.serialize_field("len", len)?;
        let () = s.serialize_field("cols", &DynStatsCols(cols))?;
        s.end()
    }
}

struct DynStatsCols<'a>(&'a BTreeMap<String, Box<dyn DynStats>>);

impl serde::Serialize for DynStatsCols<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut s = s.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0.iter() {
            let v = v.debug_json();
            let () = s.serialize_entry(k, &v)?;
        }
        s.end()
    }
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
#[cfg_attr(any(test), derive(Clone))]
pub enum JsonStats {
    /// A sentinel that indicates there were no elements.
    None,
    /// There were elements from more than one category of: bools, strings,
    /// numerics, lists, maps.
    Mixed,
    /// A sentinel that indicates all elements were `Datum::JsonNull`s.
    JsonNulls,
    /// The min and max bools, or None if there were none.
    Bools(PrimitiveStats<bool>),
    /// The min and max strings, or None if there were none.
    Strings(PrimitiveStats<String>),
    /// The min and max numerics, or None if there were none.
    /// Since we don't have a decimal type here yet, this is stored in serialized
    /// form.
    Numerics(PrimitiveStats<Vec<u8>>),
    /// A sentinel that indicates all elements were `Datum::List`s.
    ///
    /// TODO: We could also do something for list indexes analogous to what we
    /// do for map keys, but it initially seems much less likely that a user
    /// would expect that to work with pushdown, so don't bother keeping the
    /// stats until someone asks for it.
    Lists,
    /// Recursive statistics about the set of keys present in any maps/objects
    /// in the column, or None if there were no maps/objects.
    Maps(BTreeMap<String, JsonMapElementStats>),
}

#[derive(Default)]
#[cfg_attr(any(test), derive(Clone))]
pub struct JsonMapElementStats {
    pub len: usize,
    pub stats: JsonStats,
}

impl Default for JsonStats {
    fn default() -> Self {
        JsonStats::None
    }
}

impl Debug for JsonStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.debug_json(), f)
    }
}

impl JsonStats {
    pub fn debug_json(&self) -> serde_json::Value {
        match self {
            JsonStats::None => json!({}),
            JsonStats::Mixed => "json_mixed".into(),
            JsonStats::JsonNulls => "json_nulls".into(),
            JsonStats::Bools(x) => x.debug_json(),
            JsonStats::Strings(x) => x.debug_json(),
            JsonStats::Numerics(x) => x.debug_json(),
            JsonStats::Lists => "json_lists".into(),
            JsonStats::Maps(x) => x
                .iter()
                .map(|(k, v)| (k.clone(), v.debug_json()))
                .collect::<serde_json::Map<_, _>>()
                .into(),
        }
    }
}

impl JsonMapElementStats {
    pub fn debug_json(&self) -> serde_json::Value {
        json!({"len": self.len, "stats": self.stats.debug_json()})
    }
}

/// Statistics about a column of `Vec<u8>`.
pub enum BytesStats {
    Primitive(PrimitiveStats<Vec<u8>>),
    Json(JsonStats),
    Atomic(AtomicBytesStats),
}

/// `Vec<u8>` stats that cannot safely be trimmed.
#[derive(Debug)]
#[cfg_attr(any(test), derive(Clone))]
pub struct AtomicBytesStats {
    /// See [PrimitiveStats::lower]
    pub lower: Vec<u8>,
    /// See [PrimitiveStats::upper]
    pub upper: Vec<u8>,
}

impl AtomicBytesStats {
    fn debug_json(&self) -> serde_json::Value {
        serde_json::json!({
            "lower": hex::encode(&self.lower),
            "upper": hex::encode(&self.upper),
        })
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

pub trait TrimStats: Message {
    /// Attempts to reduce the serialization costs of these stats.
    ///
    /// This is lossy (might increase the false positive rate) and so should
    /// be avoided if the full fidelity stats are within an acceptable cost
    /// threshold.
    fn trim(&mut self);
}

impl TrimStats for ProtoPrimitiveStats {
    fn trim(&mut self) {
        use proto_primitive_stats::*;
        match (&mut self.lower, &mut self.upper) {
            (Some(Lower::LowerString(lower)), Some(Upper::UpperString(upper))) => {
                let common_prefix = lower
                    .char_indices()
                    .zip(upper.chars())
                    .take_while(|((_, x), y)| x == y)
                    .last();
                if let Some(((o, x), y)) = common_prefix {
                    let new_len = o + std::cmp::max(x.len_utf8(), y.len_utf8());
                    *lower = truncate_string(lower, new_len, TruncateBound::Lower)
                        .expect("lower bound should always truncate");
                    if let Some(new_upper) = truncate_string(upper, new_len, TruncateBound::Upper) {
                        *upper = new_upper;
                    }
                }
            }
            _ => {}
        }
    }
}
impl TrimStats for ProtoPrimitiveBytesStats {
    fn trim(&mut self) {
        let common_prefix = self
            .lower
            .iter()
            .zip(self.upper.iter())
            .take_while(|(x, y)| x == y)
            .count();
        self.lower = truncate_bytes(&self.lower, common_prefix + 1, TruncateBound::Lower)
            .expect("lower bound should always truncate");
        if let Some(upper) = truncate_bytes(&self.upper, common_prefix + 1, TruncateBound::Upper) {
            self.upper = upper;
        }
    }
}

impl TrimStats for ProtoJsonStats {
    fn trim(&mut self) {
        use proto_json_stats::*;
        match &mut self.kind {
            Some(Kind::Strings(stats)) => {
                stats.trim();
            }
            Some(Kind::Maps(stats)) => {
                for value in &mut stats.elements {
                    if let Some(stats) = &mut value.stats {
                        stats.trim();
                    }
                }
            }
            Some(
                Kind::None(_)
                | Kind::Mixed(_)
                | Kind::JsonNulls(_)
                | Kind::Bools(_)
                | Kind::Numerics(_)
                | Kind::Lists(_),
            ) => {}
            None => {}
        }
    }
}

impl TrimStats for ProtoBytesStats {
    fn trim(&mut self) {
        use proto_bytes_stats::*;
        match &mut self.kind {
            Some(Kind::Primitive(stats)) => stats.trim(),
            Some(Kind::Json(stats)) => stats.trim(),
            // We explicitly don't trim atomic stats!
            Some(Kind::Atomic(_)) => {}
            None => {}
        }
    }
}

impl TrimStats for ProtoStructStats {
    fn trim(&mut self) {
        use proto_dyn_stats::*;

        for value in self.cols.values_mut() {
            match &mut value.kind {
                Some(Kind::Primitive(stats)) => stats.trim(),
                Some(Kind::Bytes(stats)) => stats.trim(),
                Some(Kind::Struct(stats)) => stats.trim(),
                None => {}
            }
        }
    }
}

/// Trims the included column status until they fit within a budget.
///
/// This might remove stats for a column entirely, unless `force_keep_col`
/// returns true for that column. The resulting StructStats object is
/// guaranteed to fit within the passed budget, except when the columns that
/// are force-kept are collectively larger than the budget.
pub fn trim_to_budget(
    stats: &mut ProtoStructStats,
    budget: usize,
    force_keep_col: impl Fn(&str) -> bool,
) {
    // No trimming necessary should be the overwhelming common case in practice.
    if stats.encoded_len() <= budget {
        return;
    }

    // First try any lossy trimming that doesn't lose an entire column.
    stats.trim();
    if stats.encoded_len() <= budget {
        return;
    }

    // That wasn't enough. Sort the columns in order of ascending size and
    // keep however many fit within the budget. This strategy both keeps the
    // largest total number of columns and also optimizes for the sort of
    // columns we expect to need stats in practice (timestamps are numbers
    // or small strings).
    let mut col_costs: Vec<_> = stats
        .cols
        .iter()
        .map(|(name, stats)| (name.to_owned(), stats.encoded_len()))
        .collect();
    col_costs.sort_unstable_by_key(|(_, c)| *c);

    // We'd like to remove columns until the overall stats' encoded_len
    // is less than the budget, but we don't want to call encoded_len
    // for every column we remove, to avoid quadratic costs in the number
    // of columns. Instead, we keep a running upper-bound estimate of
    // encoded_len as we go.
    let mut encoded_len = stats.encoded_len();
    while encoded_len > budget {
        let Some((name, cost)) = col_costs.pop() else {
            break;
        };

        if force_keep_col(&name) {
            continue;
        }

        stats.cols.remove(&name);
        // Each field costs at least the cost of serializing the value
        // and a byte for the tag. (Though a tag may be more than one
        // byte in extreme cases.)
        encoded_len -= cost + 1;
    }
}

mod impls {
    use std::any::Any;
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    use arrow2::array::{BinaryArray, BooleanArray, PrimitiveArray, Utf8Array};
    use arrow2::bitmap::Bitmap;
    use arrow2::buffer::Buffer;
    use arrow2::compute::aggregate::SimdOrd;
    use arrow2::types::simd::Simd;
    use arrow2::types::NativeType;
    use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
    use proptest::strategy::Union;
    use proptest::{collection, prelude::*};
    use serde::Serialize;

    use crate::columnar::Data;
    use crate::dyn_struct::{DynStruct, DynStructCol, ValidityRef};
    use crate::stats::{
        proto_bytes_stats, proto_dyn_stats, proto_json_stats, proto_primitive_stats,
        truncate_bytes, truncate_string, AtomicBytesStats, BytesStats, ColumnStats, DynStats,
        JsonMapElementStats, JsonStats, OptionStats, PrimitiveStats, ProtoAtomicBytesStats,
        ProtoBytesStats, ProtoDynStats, ProtoJsonMapElementStats, ProtoJsonMapStats,
        ProtoJsonStats, ProtoOptionStats, ProtoPrimitiveBytesStats, ProtoPrimitiveStats,
        ProtoStructStats, StatsFrom, StructStats, TruncateBound, TRUNCATE_LEN,
    };

    impl<T: Serialize> Debug for PrimitiveStats<T>
    where
        PrimitiveStats<T>: RustType<ProtoPrimitiveStats>,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let l = serde_json::to_value(&self.lower).expect("valid json");
            let u = serde_json::to_value(&self.upper).expect("valid json");
            Debug::fmt(&serde_json::json!({"lower": l, "upper": u}), f)
        }
    }

    impl<T: Serialize> DynStats for PrimitiveStats<T>
    where
        PrimitiveStats<T>: RustType<ProtoPrimitiveStats> + Send + Sync + 'static,
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
        fn debug_json(&self) -> serde_json::Value {
            let l = serde_json::to_value(&self.lower).expect("valid json");
            let u = serde_json::to_value(&self.upper).expect("valid json");
            serde_json::json!({"lower": l, "upper": u})
        }
    }

    impl Debug for PrimitiveStats<Vec<u8>> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            Debug::fmt(&self.debug_json(), f)
        }
    }

    impl DynStats for PrimitiveStats<Vec<u8>> {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn into_proto(&self) -> ProtoDynStats {
            ProtoDynStats {
                option: None,
                kind: Some(proto_dyn_stats::Kind::Bytes(ProtoBytesStats {
                    kind: Some(proto_bytes_stats::Kind::Primitive(RustType::into_proto(
                        self,
                    ))),
                })),
            }
        }
        fn debug_json(&self) -> serde_json::Value {
            serde_json::json!({
                "lower": hex::encode(&self.lower),
                "upper": hex::encode(&self.upper),
            })
        }
    }

    impl<T: DynStats> Debug for OptionStats<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            Debug::fmt(&self.debug_json(), f)
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
        fn debug_json(&self) -> serde_json::Value {
            match self.some.debug_json() {
                serde_json::Value::Object(mut x) => {
                    if self.none > 0 {
                        x.insert("nulls".to_owned(), self.none.into());
                    }
                    serde_json::Value::Object(x)
                }
                s => {
                    serde_json::json!({"nulls": self.none, "not nulls": s})
                }
            }
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
        fn debug_json(&self) -> serde_json::Value {
            let mut cols = serde_json::Map::new();
            cols.insert("len".to_owned(), self.len.into());
            for (name, stats) in self.cols.iter() {
                cols.insert(name.clone(), stats.debug_json());
            }
            cols.into()
        }
    }

    impl Debug for BytesStats {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            Debug::fmt(&self.debug_json(), f)
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
        fn debug_json(&self) -> serde_json::Value {
            match self {
                BytesStats::Primitive(x) => x.debug_json(),
                BytesStats::Json(x) => x.debug_json(),
                BytesStats::Atomic(x) => x.debug_json(),
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
                BytesStats::Atomic(x) => Some(&x.lower),
            }
        }
        fn upper<'a>(&'a self) -> Option<<Vec<u8> as Data>::Ref<'a>> {
            match self {
                BytesStats::Primitive(x) => x.upper(),
                BytesStats::Json(_) => None,
                BytesStats::Atomic(x) => Some(&x.upper),
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

    impl ColumnStats<DynStruct> for StructStats {
        fn lower<'a>(&'a self) -> Option<<DynStruct as Data>::Ref<'a>> {
            // Not meaningful for structs
            None
        }
        fn upper<'a>(&'a self) -> Option<<DynStruct as Data>::Ref<'a>> {
            // Not meaningful for structs
            None
        }
        fn none_count(&self) -> usize {
            0
        }
    }

    impl ColumnStats<Option<DynStruct>> for OptionStats<StructStats> {
        fn lower<'a>(&'a self) -> Option<<Option<DynStruct> as Data>::Ref<'a>> {
            self.some.lower().map(Some)
        }
        fn upper<'a>(&'a self) -> Option<<Option<DynStruct> as Data>::Ref<'a>> {
            self.some.upper().map(Some)
        }
        fn none_count(&self) -> usize {
            self.none
        }
    }

    impl StatsFrom<Bitmap> for PrimitiveStats<bool> {
        fn stats_from(col: &Bitmap, validity: ValidityRef<'_>) -> Self {
            let array = BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                col.clone(),
                validity.0.cloned(),
            );
            let lower = arrow2::compute::aggregate::min_boolean(&array).unwrap_or_default();
            let upper = arrow2::compute::aggregate::max_boolean(&array).unwrap_or_default();
            PrimitiveStats { lower, upper }
        }
    }

    impl StatsFrom<BooleanArray> for OptionStats<PrimitiveStats<bool>> {
        fn stats_from(col: &BooleanArray, validity: ValidityRef<'_>) -> Self {
            debug_assert!(validity.is_superset(col.validity()));
            let lower = arrow2::compute::aggregate::min_boolean(col).unwrap_or_default();
            let upper = arrow2::compute::aggregate::max_boolean(col).unwrap_or_default();
            let none = col.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl<T> StatsFrom<Buffer<T>> for PrimitiveStats<T>
    where
        T: NativeType + Simd,
        T::Simd: SimdOrd<T>,
    {
        fn stats_from(col: &Buffer<T>, validity: ValidityRef<'_>) -> Self {
            let array = PrimitiveArray::new(T::PRIMITIVE.into(), col.clone(), validity.0.cloned());
            let lower = arrow2::compute::aggregate::min_primitive::<T>(&array).unwrap_or_default();
            let upper = arrow2::compute::aggregate::max_primitive::<T>(&array).unwrap_or_default();
            PrimitiveStats { lower, upper }
        }
    }

    impl<T> StatsFrom<PrimitiveArray<T>> for OptionStats<PrimitiveStats<T>>
    where
        T: Data + NativeType + Simd,
        T::Simd: SimdOrd<T>,
    {
        fn stats_from(col: &PrimitiveArray<T>, validity: ValidityRef<'_>) -> Self {
            debug_assert!(validity.is_superset(col.validity()));
            let lower = arrow2::compute::aggregate::min_primitive::<T>(col).unwrap_or_default();
            let upper = arrow2::compute::aggregate::max_primitive::<T>(col).unwrap_or_default();
            let none = col.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl StatsFrom<BinaryArray<i32>> for PrimitiveStats<Vec<u8>> {
        fn stats_from(col: &BinaryArray<i32>, validity: ValidityRef<'_>) -> Self {
            assert!(col.validity().is_none());
            let mut array = col.clone();
            array.set_validity(validity.0.cloned());
            let lower = arrow2::compute::aggregate::min_binary(&array).unwrap_or_default();
            let lower = truncate_bytes(lower, TRUNCATE_LEN, TruncateBound::Lower)
                .expect("lower bound should always truncate");
            let upper = arrow2::compute::aggregate::max_binary(&array).unwrap_or_default();
            let upper = truncate_bytes(upper, TRUNCATE_LEN, TruncateBound::Upper)
                // NB: The cost+trim stuff will remove the column entirely if
                // it's still too big (also this should be extremely rare in
                // practice).
                .unwrap_or_else(|| upper.to_owned());
            PrimitiveStats { lower, upper }
        }
    }

    impl StatsFrom<BinaryArray<i32>> for OptionStats<PrimitiveStats<Vec<u8>>> {
        fn stats_from(col: &BinaryArray<i32>, validity: ValidityRef<'_>) -> Self {
            debug_assert!(validity.is_superset(col.validity()));
            let lower = arrow2::compute::aggregate::min_binary(col).unwrap_or_default();
            let lower = truncate_bytes(lower, TRUNCATE_LEN, TruncateBound::Lower)
                .expect("lower bound should always truncate");
            let upper = arrow2::compute::aggregate::max_binary(col).unwrap_or_default();
            let upper = truncate_bytes(upper, TRUNCATE_LEN, TruncateBound::Upper)
                // NB: The cost+trim stuff will remove the column entirely if
                // it's still too big (also this should be extremely rare in
                // practice).
                .unwrap_or_else(|| upper.to_owned());
            let none = col.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl StatsFrom<BinaryArray<i32>> for BytesStats {
        fn stats_from(col: &BinaryArray<i32>, validity: ValidityRef<'_>) -> Self {
            BytesStats::Primitive(<PrimitiveStats<Vec<u8>>>::stats_from(col, validity))
        }
    }

    impl StatsFrom<BinaryArray<i32>> for OptionStats<BytesStats> {
        fn stats_from(col: &BinaryArray<i32>, validity: ValidityRef<'_>) -> Self {
            let stats = OptionStats::<PrimitiveStats<Vec<u8>>>::stats_from(col, validity);
            OptionStats {
                none: stats.none,
                some: BytesStats::Primitive(stats.some),
            }
        }
    }

    impl StatsFrom<Utf8Array<i32>> for PrimitiveStats<String> {
        fn stats_from(col: &Utf8Array<i32>, validity: ValidityRef<'_>) -> Self {
            assert!(col.validity().is_none());
            let mut array = col.clone();
            array.set_validity(validity.0.cloned());
            let lower = arrow2::compute::aggregate::min_string(&array).unwrap_or_default();
            let lower = truncate_string(lower, TRUNCATE_LEN, TruncateBound::Lower)
                .expect("lower bound should always truncate");
            let upper = arrow2::compute::aggregate::max_string(&array).unwrap_or_default();
            let upper = truncate_string(upper, TRUNCATE_LEN, TruncateBound::Upper)
                // NB: The cost+trim stuff will remove the column entirely if
                // it's still too big (also this should be extremely rare in
                // practice).
                .unwrap_or_else(|| upper.to_owned());
            PrimitiveStats { lower, upper }
        }
    }

    impl StatsFrom<Utf8Array<i32>> for OptionStats<PrimitiveStats<String>> {
        fn stats_from(col: &Utf8Array<i32>, validity: ValidityRef<'_>) -> Self {
            debug_assert!(validity.is_superset(col.validity()));
            let lower = arrow2::compute::aggregate::min_string(col).unwrap_or_default();
            let lower = truncate_string(lower, TRUNCATE_LEN, TruncateBound::Lower)
                .expect("lower bound should always truncate");
            let upper = arrow2::compute::aggregate::max_string(col).unwrap_or_default();
            let upper = truncate_string(upper, TRUNCATE_LEN, TruncateBound::Upper)
                // NB: The cost+trim stuff will remove the column entirely if
                // it's still too big (also this should be extremely rare in
                // practice).
                .unwrap_or_else(|| upper.to_owned());
            let none = col.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl StatsFrom<DynStructCol> for StructStats {
        fn stats_from(col: &DynStructCol, validity: ValidityRef<'_>) -> Self {
            assert!(col.validity.is_none());
            col.stats(validity).expect("valid stats").some
        }
    }

    impl StatsFrom<DynStructCol> for OptionStats<StructStats> {
        fn stats_from(col: &DynStructCol, validity: ValidityRef<'_>) -> Self {
            debug_assert!(validity.is_superset(col.validity.as_ref()));
            col.stats(validity).expect("valid stats")
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
                kind: Some(match self {
                    JsonStats::None => proto_json_stats::Kind::None(()),
                    JsonStats::Mixed => proto_json_stats::Kind::Mixed(()),
                    JsonStats::JsonNulls => proto_json_stats::Kind::JsonNulls(()),
                    JsonStats::Bools(x) => proto_json_stats::Kind::Bools(RustType::into_proto(x)),
                    JsonStats::Strings(x) => {
                        proto_json_stats::Kind::Strings(RustType::into_proto(x))
                    }
                    JsonStats::Numerics(x) => {
                        proto_json_stats::Kind::Numerics(RustType::into_proto(x))
                    }
                    JsonStats::Lists => proto_json_stats::Kind::Lists(()),
                    JsonStats::Maps(x) => proto_json_stats::Kind::Maps(ProtoJsonMapStats {
                        elements: x
                            .iter()
                            .map(|(k, v)| ProtoJsonMapElementStats {
                                name: k.into_proto(),
                                len: v.len.into_proto(),
                                stats: Some(RustType::into_proto(&v.stats)),
                            })
                            .collect(),
                    }),
                }),
            }
        }

        fn from_proto(proto: ProtoJsonStats) -> Result<Self, TryFromProtoError> {
            Ok(match proto.kind {
                Some(proto_json_stats::Kind::None(())) => JsonStats::None,
                Some(proto_json_stats::Kind::Mixed(())) => JsonStats::Mixed,
                Some(proto_json_stats::Kind::JsonNulls(())) => JsonStats::JsonNulls,
                Some(proto_json_stats::Kind::Bools(x)) => JsonStats::Bools(x.into_rust()?),
                Some(proto_json_stats::Kind::Strings(x)) => JsonStats::Strings(x.into_rust()?),
                Some(proto_json_stats::Kind::Numerics(x)) => JsonStats::Numerics(x.into_rust()?),
                Some(proto_json_stats::Kind::Lists(())) => JsonStats::Lists,
                Some(proto_json_stats::Kind::Maps(x)) => {
                    let mut elements = BTreeMap::new();
                    for x in x.elements {
                        let stats = JsonMapElementStats {
                            len: x.len.into_rust()?,
                            stats: x.stats.into_rust_if_some("JsonMapElementStats::stats")?,
                        };
                        elements.insert(x.name.into_rust()?, stats);
                    }
                    JsonStats::Maps(elements)
                }
                // Unknown JSON stats type: assume this might have any value.
                None => JsonStats::Mixed,
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
                BytesStats::Atomic(x) => proto_bytes_stats::Kind::Atomic(RustType::into_proto(x)),
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
                Some(proto_bytes_stats::Kind::Atomic(x)) => {
                    Ok(BytesStats::Atomic(AtomicBytesStats::from_proto(x)?))
                }
                None => Err(TryFromProtoError::missing_field("ProtoBytesStats::kind")),
            }
        }
    }

    impl RustType<ProtoAtomicBytesStats> for AtomicBytesStats {
        fn into_proto(&self) -> ProtoAtomicBytesStats {
            ProtoAtomicBytesStats {
                lower: self.lower.into_proto(),
                upper: self.upper.into_proto(),
            }
        }

        fn from_proto(proto: ProtoAtomicBytesStats) -> Result<Self, TryFromProtoError> {
            Ok(AtomicBytesStats {
                lower: proto.lower.into_rust()?,
                upper: proto.upper.into_rust()?,
            })
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
    primitive_stats_rust_type!(String, LowerString, UpperString);

    impl RustType<ProtoPrimitiveBytesStats> for PrimitiveStats<Vec<u8>> {
        fn into_proto(&self) -> ProtoPrimitiveBytesStats {
            ProtoPrimitiveBytesStats {
                lower: self.lower.into_proto(),
                upper: self.upper.into_proto(),
            }
        }

        fn from_proto(proto: ProtoPrimitiveBytesStats) -> Result<Self, TryFromProtoError> {
            let lower = proto.lower.into_rust()?;
            let upper = proto.upper.into_rust()?;
            Ok(PrimitiveStats { lower, upper })
        }
    }

    pub(crate) fn any_struct_stats_cols(
    ) -> impl Strategy<Value = BTreeMap<String, Box<dyn DynStats>>> {
        collection::btree_map(any::<String>(), any_box_dyn_stats(), 1..5)
    }

    fn any_primitive_stats<T>() -> impl Strategy<Value = PrimitiveStats<T>>
    where
        T: Arbitrary + Ord + Serialize,
        PrimitiveStats<T>: RustType<ProtoPrimitiveStats>,
    {
        Strategy::prop_map(any::<(T, T)>(), |(x0, x1)| {
            if x0 <= x1 {
                PrimitiveStats {
                    lower: x0,
                    upper: x1,
                }
            } else {
                PrimitiveStats {
                    lower: x1,
                    upper: x0,
                }
            }
        })
    }

    fn any_primitive_vec_u8_stats() -> impl Strategy<Value = PrimitiveStats<Vec<u8>>> {
        Strategy::prop_map(any::<(Vec<u8>, Vec<u8>)>(), |(x0, x1)| {
            if x0 <= x1 {
                PrimitiveStats {
                    lower: x0,
                    upper: x1,
                }
            } else {
                PrimitiveStats {
                    lower: x1,
                    upper: x0,
                }
            }
        })
    }

    fn any_bytes_stats() -> impl Strategy<Value = BytesStats> {
        Union::new(vec![
            any_primitive_vec_u8_stats()
                .prop_map(BytesStats::Primitive)
                .boxed(),
            any_json_stats().prop_map(BytesStats::Json).boxed(),
            any_primitive_vec_u8_stats()
                .prop_map(|x| {
                    BytesStats::Atomic(AtomicBytesStats {
                        lower: x.lower,
                        upper: x.upper,
                    })
                })
                .boxed(),
        ])
    }

    fn any_json_stats() -> impl Strategy<Value = JsonStats> {
        let leaf = Union::new(vec![
            any::<()>().prop_map(|_| JsonStats::None).boxed(),
            any::<()>().prop_map(|_| JsonStats::Mixed).boxed(),
            any::<()>().prop_map(|_| JsonStats::JsonNulls).boxed(),
            any_primitive_stats::<bool>()
                .prop_map(JsonStats::Bools)
                .boxed(),
            any_primitive_stats::<String>()
                .prop_map(JsonStats::Strings)
                .boxed(),
            any::<()>().prop_map(|_| JsonStats::Lists).boxed(),
        ]);
        leaf.prop_recursive(2, 5, 3, |inner| {
            (collection::btree_map(any::<String>(), inner, 0..3)).prop_map(|cols| {
                let cols = cols
                    .into_iter()
                    .map(|(k, stats)| (k, JsonMapElementStats { len: 1, stats }))
                    .collect();
                JsonStats::Maps(cols)
            })
        })
    }

    fn any_box_dyn_stats() -> impl Strategy<Value = Box<dyn DynStats>> {
        fn into_box_dyn_stats<T: DynStats>(x: T) -> Box<dyn DynStats> {
            let x: Box<dyn DynStats> = Box::new(x);
            x
        }
        let leaf = Union::new(vec![
            any_primitive_stats::<bool>()
                .prop_map(into_box_dyn_stats)
                .boxed(),
            any_primitive_stats::<i64>()
                .prop_map(into_box_dyn_stats)
                .boxed(),
            any_primitive_stats::<String>()
                .prop_map(into_box_dyn_stats)
                .boxed(),
            any_bytes_stats().prop_map(into_box_dyn_stats).boxed(),
        ]);
        leaf.prop_recursive(2, 10, 3, |inner| {
            (
                any::<usize>(),
                collection::btree_map(any::<String>(), inner, 0..3),
            )
                .prop_map(|(len, cols)| into_box_dyn_stats(StructStats { len, cols }))
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow2::array::BinaryArray;
    use mz_proto::RustType;
    use proptest::prelude::*;

    use crate::columnar::sealed::ColumnMut;
    use crate::columnar::ColumnPush;
    use crate::dyn_struct::ValidityRef;

    use super::*;

    #[mz_ore::test]
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

    #[mz_ore::test]
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

    #[mz_ore::test]
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

    #[mz_ore::test]
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
                    // As explained in a comment in the impl, we don't quite
                    // treat the max_len as a hard bound here. Give it a little
                    // wiggle room.
                    assert!(upper.len() <= max_len + char::MAX.len_utf8());
                    assert!(upper.as_str() >= x);
                }
            }
        }

        proptest!(|(x in any::<String>())| {
            // The proptest! macro interferes with rustfmt.
            testcase(x.as_str())
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn primitive_cost_trim_proptest() {
        fn primitive_stats<'a, T: Data<Cfg = ()>, F>(xs: &'a [T], f: F) -> (&'a [T], T::Stats)
        where
            F: for<'b> Fn(&'b T) -> T::Ref<'b>,
        {
            let mut col = T::Mut::new(&());
            for x in xs {
                col.push(f(x));
            }
            let col = T::Col::from(col);
            let stats = T::Stats::stats_from(&col, ValidityRef(None));
            (xs, stats)
        }
        fn testcase<T: Data + PartialOrd + Clone + Debug, P>(xs_stats: (&[T], PrimitiveStats<T>))
        where
            PrimitiveStats<T>: RustType<P> + DynStats,
            P: TrimStats,
        {
            let (xs, stats) = xs_stats;
            for x in xs {
                assert!(&stats.lower <= x);
                assert!(&stats.upper >= x);
            }

            let mut proto_stats = RustType::into_proto(&stats);
            let cost_before = proto_stats.encoded_len();
            proto_stats.trim();
            assert!(proto_stats.encoded_len() <= cost_before);
            let stats: PrimitiveStats<T> = RustType::from_proto(proto_stats).unwrap();
            for x in xs {
                assert!(&stats.lower <= x);
                assert!(&stats.upper >= x);
            }
        }

        proptest!(|(a in any::<bool>(), b in any::<bool>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<u8>(), b in any::<u8>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<u16>(), b in any::<u16>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<u32>(), b in any::<u32>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<u64>(), b in any::<u64>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<i8>(), b in any::<i8>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<i16>(), b in any::<i16>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<i32>(), b in any::<i32>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<i64>(), b in any::<i64>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<f32>(), b in any::<f32>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });
        proptest!(|(a in any::<f64>(), b in any::<f64>())| {
            testcase(primitive_stats(&[a, b], |x| *x))
        });

        // Construct strings that are "interesting" in that they have some
        // (possibly empty) shared prefix.
        proptest!(|(prefix in any::<String>(), a in any::<String>(), b in any::<String>())| {
            let vals = &[format!("{}{}", prefix, a), format!("{}{}", prefix, b)];
            testcase(primitive_stats(vals, |x| x))
        });

        // Construct strings that are "interesting" in that they have some
        // (possibly empty) shared prefix.
        proptest!(|(prefix in any::<Vec<u8>>(), a in any::<Vec<u8>>(), b in any::<Vec<u8>>())| {
            let mut sa = prefix.clone();
            sa.extend(&a);
            let mut sb = prefix;
            sb.extend(&b);
            let vals = &[sa, sb];
            let stats = PrimitiveStats::<Vec<u8>>::stats_from(&BinaryArray::<i32>::from_slice(vals), ValidityRef(None));
            testcase((vals, stats));
        });
    }

    #[mz_ore::test]
    fn struct_trim_to_budget() {
        #[track_caller]
        fn testcase(cols: &[(&str, usize)], required: Option<&str>) {
            let cols = cols
                .iter()
                .map(|(key, cost)| {
                    let stats: Box<dyn DynStats> = Box::new(PrimitiveStats {
                        lower: vec![],
                        upper: vec![0u8; *cost],
                    });
                    ((*key).to_owned(), stats)
                })
                .collect();
            let mut stats: ProtoStructStats = RustType::into_proto(&StructStats { len: 0, cols });
            let mut budget = stats.encoded_len().next_power_of_two();
            while budget > 0 {
                let cost_before = stats.encoded_len();
                trim_to_budget(&mut stats, budget, |col| Some(col) == required);
                let cost_after = stats.encoded_len();
                assert!(cost_before >= cost_after);
                if let Some(required) = required {
                    assert!(stats.cols.contains_key(required));
                } else {
                    assert!(cost_after <= budget);
                }
                budget = budget / 2;
            }
        }

        testcase(&[], None);
        testcase(&[("a", 100)], None);
        testcase(&[("a", 1), ("b", 2), ("c", 4)], None);
        testcase(&[("a", 1), ("b", 2), ("c", 4)], Some("b"));
    }

    // Regression test for a bug found during code review of initial stats
    // trimming PR.
    #[mz_ore::test]
    fn stats_trim_regression_json() {
        // Make sure we recursively trim json string and map stats by asserting
        // that the goes down after trimming.
        #[track_caller]
        fn testcase(stats: JsonStats) {
            let mut stats = stats.into_proto();
            let before = stats.encoded_len();
            stats.trim();
            let after = stats.encoded_len();
            assert!(after < before, "{} vs {}: {:?}", after, before, stats);
        }

        let col = JsonStats::Strings(PrimitiveStats {
            lower: "foobar".into(),
            upper: "foobaz".into(),
        });
        testcase(col.clone());
        let mut cols = BTreeMap::new();
        cols.insert("col".into(), JsonMapElementStats { len: 1, stats: col });
        testcase(JsonStats::Maps(cols));
    }

    // Confirm that fields are being trimmed from largest to smallest.
    #[mz_ore::test]
    fn trim_order_regression() {
        fn dyn_stats(lower: &'static str, upper: &'static str) -> Box<dyn DynStats> {
            Box::new(PrimitiveStats {
                lower: lower.to_owned(),
                upper: upper.to_owned(),
            })
        }
        let stats = StructStats {
            len: 2,
            cols: BTreeMap::from([
                ("foo".to_owned(), dyn_stats("a", "b")),
                (
                    "bar".to_owned(),
                    dyn_stats("aaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaab"),
                ),
            ]),
        };

        // The threshold here is arbitrary... we just care that there's some budget where
        // we'll discard the large field before the small one.
        let mut proto_stats = RustType::into_proto(&stats);
        trim_to_budget(&mut proto_stats, 30, |_| false);
        assert!(proto_stats.cols.contains_key("foo"));
        assert!(!proto_stats.cols.contains_key("bar"));
    }
}
