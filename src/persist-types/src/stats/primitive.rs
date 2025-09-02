// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Debug};

use mz_ore::str::redact;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use proptest::arbitrary::Arbitrary;
use proptest::strategy::Strategy;
use serde::Serialize;

use crate::stats::{
    BytesStats, ColumnStatKinds, ColumnStats, ColumnarStats, DynStats, OptionStats,
    ProtoPrimitiveBytesStats, ProtoPrimitiveStats, TrimStats, proto_primitive_stats,
};
use crate::timestamp::try_parse_monotonic_iso8601_timestamp;

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

/// Statistics about a primitive non-optional column.
#[derive(Copy, Clone)]
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
    /// [`crate::stats::StructStats`]).
    pub lower: T,
    /// Same as [Self::lower] but an (also inclusive) upper bound.
    pub upper: T,
}

impl<T: Serialize> Debug for PrimitiveStats<T>
where
    // Note: We introduce this bound so we can have a different impl for PrimitiveStats<Vec<u8>>.
    PrimitiveStats<T>: RustType<ProtoPrimitiveStats>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let l = serde_json::to_value(&self.lower).expect("valid json");
        let u = serde_json::to_value(&self.upper).expect("valid json");
        f.debug_struct("PrimitiveStats")
            .field("lower", &redact(l))
            .field("upper", &redact(u))
            .finish()
    }
}

impl Debug for PrimitiveStats<Vec<u8>> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveStats")
            .field("lower", &redact(hex::encode(&self.lower)))
            .field("upper", &redact(hex::encode(&self.upper)))
            .finish()
    }
}

impl<T: Serialize> DynStats for PrimitiveStats<T>
where
    PrimitiveStats<T>: RustType<ProtoPrimitiveStats>
        + Into<PrimitiveStatsVariants>
        + Clone
        + Send
        + Sync
        + 'static,
{
    fn debug_json(&self) -> serde_json::Value {
        let l = serde_json::to_value(&self.lower).expect("valid json");
        let u = serde_json::to_value(&self.upper).expect("valid json");
        serde_json::json!({"lower": l, "upper": u})
    }

    fn into_columnar_stats(self) -> ColumnarStats {
        ColumnarStats {
            nulls: None,
            values: ColumnStatKinds::Primitive(self.into()),
        }
    }
}

impl DynStats for PrimitiveStats<Vec<u8>> {
    fn debug_json(&self) -> serde_json::Value {
        serde_json::json!({
            "lower": hex::encode(&self.lower),
            "upper": hex::encode(&self.upper),
        })
    }

    fn into_columnar_stats(self) -> ColumnarStats {
        ColumnarStats {
            nulls: None,
            values: ColumnStatKinds::Bytes(BytesStats::Primitive(self)),
        }
    }
}

/// This macro implements the [`ColumnStats`] trait for all variants of [`PrimitiveStats`].
macro_rules! stats_primitive {
    ($data:ty, $ref_ty:ty, $ref:ident, $variant:ident) => {
        impl ColumnStats for PrimitiveStats<$data> {
            type Ref<'a> = $ref_ty;

            fn lower<'a>(&'a self) -> Option<Self::Ref<'a>> {
                Some(self.lower.$ref())
            }
            fn upper<'a>(&'a self) -> Option<Self::Ref<'a>> {
                Some(self.upper.$ref())
            }
            fn none_count(&self) -> usize {
                0
            }
        }

        impl ColumnStats for OptionStats<PrimitiveStats<$data>> {
            type Ref<'a> = Option<$ref_ty>;

            fn lower<'a>(&'a self) -> Option<Self::Ref<'a>> {
                Some(self.some.lower())
            }
            fn upper<'a>(&'a self) -> Option<Self::Ref<'a>> {
                Some(self.some.upper())
            }
            fn none_count(&self) -> usize {
                self.none
            }
        }

        impl From<PrimitiveStats<$data>> for PrimitiveStatsVariants {
            fn from(value: PrimitiveStats<$data>) -> Self {
                PrimitiveStatsVariants::$variant(value)
            }
        }
    };
}

stats_primitive!(bool, bool, clone, Bool);
stats_primitive!(u8, u8, clone, U8);
stats_primitive!(u16, u16, clone, U16);
stats_primitive!(u32, u32, clone, U32);
stats_primitive!(u64, u64, clone, U64);
stats_primitive!(i8, i8, clone, I8);
stats_primitive!(i16, i16, clone, I16);
stats_primitive!(i32, i32, clone, I32);
stats_primitive!(i64, i64, clone, I64);
stats_primitive!(f32, f32, clone, F32);
stats_primitive!(f64, f64, clone, F64);
stats_primitive!(String, &'a str, as_str, String);

/// This macro implements the [`RustType`] trait for all variants of [`PrimitiveStats`].
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
                        ));
                    }
                };
                let upper = proto
                    .upper
                    .ok_or_else(|| TryFromProtoError::missing_field("ProtoPrimitiveStats::max"))?;
                let upper = match upper {
                    proto_primitive_stats::Upper::$upper(x) => x.into_rust()?,
                    _ => {
                        return Err(TryFromProtoError::missing_field(
                            "proto_primitive_stats::Upper::$upper",
                        ));
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

impl TrimStats for ProtoPrimitiveStats {
    fn trim(&mut self) {
        use proto_primitive_stats::*;
        match (&mut self.lower, &mut self.upper) {
            (Some(Lower::LowerString(lower)), Some(Upper::UpperString(upper))) => {
                // If the lower and upper strings both look like iso8601
                // timestamps, then (1) they're small and (2) that's an
                // extremely high signal that we might want to keep them around
                // for filtering. We technically could still recover useful
                // bounds here in the interpret code, but the complexity isn't
                // worth it, so just skip any trimming.
                if try_parse_monotonic_iso8601_timestamp(lower).is_some()
                    && try_parse_monotonic_iso8601_timestamp(upper).is_some()
                {
                    return;
                }

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

/// Enum wrapper around [`PrimitiveStats`] for each variant that we support.
#[derive(Debug, Clone)]
pub enum PrimitiveStatsVariants {
    /// [`bool`]
    Bool(PrimitiveStats<bool>),
    /// [`u8`]
    U8(PrimitiveStats<u8>),
    /// [`u16`]
    U16(PrimitiveStats<u16>),
    /// [`u32`]
    U32(PrimitiveStats<u32>),
    /// [`u64`]
    U64(PrimitiveStats<u64>),
    /// [`i8`]
    I8(PrimitiveStats<i8>),
    /// [`i16`]
    I16(PrimitiveStats<i16>),
    /// [`i32`]
    I32(PrimitiveStats<i32>),
    /// [`i64`]
    I64(PrimitiveStats<i64>),
    /// [`f32`]
    F32(PrimitiveStats<f32>),
    /// [`f64`]
    F64(PrimitiveStats<f64>),
    /// [`String`]
    String(PrimitiveStats<String>),
}

impl DynStats for PrimitiveStatsVariants {
    fn debug_json(&self) -> serde_json::Value {
        use PrimitiveStatsVariants::*;

        match self {
            Bool(stats) => stats.debug_json(),
            U8(stats) => stats.debug_json(),
            U16(stats) => stats.debug_json(),
            U32(stats) => stats.debug_json(),
            U64(stats) => stats.debug_json(),
            I8(stats) => stats.debug_json(),
            I16(stats) => stats.debug_json(),
            I32(stats) => stats.debug_json(),
            I64(stats) => stats.debug_json(),
            F32(stats) => stats.debug_json(),
            F64(stats) => stats.debug_json(),
            String(stats) => stats.debug_json(),
        }
    }

    fn into_columnar_stats(self) -> ColumnarStats {
        ColumnarStats {
            nulls: None,
            values: ColumnStatKinds::Primitive(self),
        }
    }
}

impl RustType<ProtoPrimitiveStats> for PrimitiveStatsVariants {
    fn into_proto(&self) -> ProtoPrimitiveStats {
        use PrimitiveStatsVariants::*;

        match self {
            Bool(stats) => RustType::into_proto(stats),
            U8(stats) => RustType::into_proto(stats),
            U16(stats) => RustType::into_proto(stats),
            U32(stats) => RustType::into_proto(stats),
            U64(stats) => RustType::into_proto(stats),
            I8(stats) => RustType::into_proto(stats),
            I16(stats) => RustType::into_proto(stats),
            I32(stats) => RustType::into_proto(stats),
            I64(stats) => RustType::into_proto(stats),
            F32(stats) => RustType::into_proto(stats),
            F64(stats) => RustType::into_proto(stats),
            String(stats) => RustType::into_proto(stats),
        }
    }

    fn from_proto(proto: ProtoPrimitiveStats) -> Result<Self, mz_proto::TryFromProtoError> {
        use crate::stats::proto_primitive_stats::{Lower, Upper};

        let stats = match (proto.lower, proto.upper) {
            (Some(Lower::LowerBool(lower)), Some(Upper::UpperBool(upper))) => {
                PrimitiveStatsVariants::Bool(PrimitiveStats { lower, upper })
            }
            (Some(Lower::LowerU8(lower)), Some(Upper::UpperU8(upper))) => {
                PrimitiveStatsVariants::U8(PrimitiveStats {
                    lower: lower.into_rust()?,
                    upper: upper.into_rust()?,
                })
            }
            (Some(Lower::LowerU16(lower)), Some(Upper::UpperU16(upper))) => {
                PrimitiveStatsVariants::U16(PrimitiveStats {
                    lower: lower.into_rust()?,
                    upper: upper.into_rust()?,
                })
            }
            (Some(Lower::LowerU32(lower)), Some(Upper::UpperU32(upper))) => {
                PrimitiveStatsVariants::U32(PrimitiveStats { lower, upper })
            }
            (Some(Lower::LowerU64(lower)), Some(Upper::UpperU64(upper))) => {
                PrimitiveStatsVariants::U64(PrimitiveStats { lower, upper })
            }
            (Some(Lower::LowerI8(lower)), Some(Upper::UpperI8(upper))) => {
                PrimitiveStatsVariants::I8(PrimitiveStats {
                    lower: lower.into_rust()?,
                    upper: upper.into_rust()?,
                })
            }
            (Some(Lower::LowerI16(lower)), Some(Upper::UpperI16(upper))) => {
                PrimitiveStatsVariants::I16(PrimitiveStats {
                    lower: lower.into_rust()?,
                    upper: upper.into_rust()?,
                })
            }
            (Some(Lower::LowerI32(lower)), Some(Upper::UpperI32(upper))) => {
                PrimitiveStatsVariants::I32(PrimitiveStats { lower, upper })
            }
            (Some(Lower::LowerI64(lower)), Some(Upper::UpperI64(upper))) => {
                PrimitiveStatsVariants::I64(PrimitiveStats { lower, upper })
            }
            (Some(Lower::LowerF32(lower)), Some(Upper::UpperF32(upper))) => {
                PrimitiveStatsVariants::F32(PrimitiveStats { lower, upper })
            }
            (Some(Lower::LowerF64(lower)), Some(Upper::UpperF64(upper))) => {
                PrimitiveStatsVariants::F64(PrimitiveStats { lower, upper })
            }
            (Some(Lower::LowerString(lower)), Some(Upper::UpperString(upper))) => {
                PrimitiveStatsVariants::String(PrimitiveStats { lower, upper })
            }
            (lower, upper) => {
                return Err(TryFromProtoError::missing_field(format!(
                    "expected lower and upper to be the same type, found {} and {}",
                    std::any::type_name_of_val(&lower),
                    std::any::type_name_of_val(&upper)
                )));
            }
        };

        Ok(stats)
    }
}

/// Returns a [`Strategy`] for generating arbitrary [`PrimitiveStats`].
pub(crate) fn any_primitive_stats<T>() -> impl Strategy<Value = PrimitiveStats<T>>
where
    T: Arbitrary + Ord + Serialize,
    PrimitiveStats<T>: RustType<ProtoPrimitiveStats>,
{
    Strategy::prop_map(proptest::arbitrary::any::<(T, T)>(), |(x0, x1)| {
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

/// Returns a [`Strategy`] for generating arbitrary [`PrimitiveStats<Vec<u8>>`].
pub(crate) fn any_primitive_vec_u8_stats() -> impl Strategy<Value = PrimitiveStats<Vec<u8>>> {
    Strategy::prop_map(
        proptest::arbitrary::any::<(Vec<u8>, Vec<u8>)>(),
        |(x0, x1)| {
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
        },
    )
}

#[cfg(test)]
mod tests {
    use arrow::array::{BinaryArray, StringArray};
    use proptest::prelude::*;

    use super::*;
    use crate::stats::ColumnarStatsBuilder;

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
    fn proptest_cost_trim() {
        fn primitive_stats<T, A>(vals: &[T]) -> (&[T], PrimitiveStats<T>)
        where
            T: Clone,
            A: arrow::array::Array + From<Vec<T>>,
            PrimitiveStats<T>: ColumnarStatsBuilder<T, ArrowColumn = A>,
        {
            let column = A::from(vals.to_vec());
            let stats = <PrimitiveStats<T> as ColumnarStatsBuilder<T>>::from_column(&column);

            (vals, stats)
        }

        fn testcase<T: PartialOrd + Clone + Debug, P>(xs_stats: (&[T], PrimitiveStats<T>))
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
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<u8>(), b in any::<u8>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<u16>(), b in any::<u16>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<u32>(), b in any::<u32>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<u64>(), b in any::<u64>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<i8>(), b in any::<i8>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<i16>(), b in any::<i16>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<i32>(), b in any::<i32>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<i64>(), b in any::<i64>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<f32>(), b in any::<f32>())| {
            testcase(primitive_stats(&[a, b]))
        });
        proptest!(|(a in any::<f64>(), b in any::<f64>())| {
            testcase(primitive_stats(&[a, b]))
        });

        // Construct strings that are "interesting" in that they have some
        // (possibly empty) shared prefix.
        proptest!(|(prefix in any::<String>(), a in any::<String>(), b in any::<String>())| {
            let vals = &[format!("{}{}", prefix, a), format!("{}{}", prefix, b)];
            let col = StringArray::from(vals.to_vec());
            let stats = PrimitiveStats::<String>::from_column(&col);
            testcase((&vals[..], stats))
        });

        // Construct strings that are "interesting" in that they have some
        // (possibly empty) shared prefix.
        proptest!(|(prefix in any::<Vec<u8>>(), a in any::<Vec<u8>>(), b in any::<Vec<u8>>())| {
            let mut sa = prefix.clone();
            sa.extend(&a);
            let mut sb = prefix;
            sb.extend(&b);
            let vals = &[sa, sb];
            let array = BinaryArray::from_vec(vals.iter().map(|v| v.as_ref()).collect());
            let stats = PrimitiveStats::<Vec<u8>>::from_column(&array);
            testcase((vals, stats));
        });
    }

    // Regression test for a bug where "lossless" trimming would truncate an
    // upper and lower bound that both parsed as our special iso8601 timestamps
    // into something that no longer did.
    #[mz_ore::test]
    fn stats_trim_iso8601_recursion() {
        use proto_primitive_stats::*;

        let orig = PrimitiveStats {
            lower: "2023-08-19T12:00:00.000Z".to_owned(),
            upper: "2023-08-20T12:00:00.000Z".to_owned(),
        };
        let mut stats = RustType::into_proto(&orig);
        stats.trim();
        // Before the fix, this resulted in "2023-08-" and "2023-08.".
        assert_eq!(stats.lower.unwrap(), Lower::LowerString(orig.lower));
        assert_eq!(stats.upper.unwrap(), Upper::UpperString(orig.upper));
    }
}
