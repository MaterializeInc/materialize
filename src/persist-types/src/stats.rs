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

include!(concat!(env!("OUT_DIR"), "/mz_persist_types.stats.rs"));

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

    use crate::stats::{
        proto_dyn_stats, proto_primitive_stats, DynStats, OptionStats, PrimitiveStats,
        ProtoDynStats, ProtoOptionStats, ProtoPrimitiveStats, ProtoStructStats, StructStats,
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
                kind: Some(proto_dyn_stats::Kind::Struct(ProtoStructStats {
                    len: self.len.into_proto(),
                    cols: self
                        .cols
                        .iter()
                        .map(|(k, v)| (k.into_proto(), v.into_proto()))
                        .collect(),
                })),
            }
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
            let lower = arrow2::compute::aggregate::min_binary(value)
                .unwrap_or_default()
                .to_owned();
            let upper = arrow2::compute::aggregate::max_binary(value)
                .unwrap_or_default()
                .to_owned();
            PrimitiveStats { lower, upper }
        }
    }

    impl From<&BinaryArray<i32>> for OptionStats<PrimitiveStats<Vec<u8>>> {
        fn from(value: &BinaryArray<i32>) -> Self {
            let lower = arrow2::compute::aggregate::min_binary(value)
                .unwrap_or_default()
                .to_owned();
            let upper = arrow2::compute::aggregate::max_binary(value)
                .unwrap_or_default()
                .to_owned();
            let none = value.validity().map_or(0, |x| x.unset_bits());
            OptionStats {
                none,
                some: PrimitiveStats { lower, upper },
            }
        }
    }

    impl From<&Utf8Array<i32>> for PrimitiveStats<String> {
        fn from(value: &Utf8Array<i32>) -> Self {
            assert!(value.validity().is_none());
            let lower = arrow2::compute::aggregate::min_string(value)
                .unwrap_or_default()
                .to_owned();
            let upper = arrow2::compute::aggregate::max_string(value)
                .unwrap_or_default()
                .to_owned();
            PrimitiveStats { lower, upper }
        }
    }

    impl From<&Utf8Array<i32>> for OptionStats<PrimitiveStats<String>> {
        fn from(value: &Utf8Array<i32>) -> Self {
            let lower = arrow2::compute::aggregate::min_string(value)
                .unwrap_or_default()
                .to_owned();
            let upper = arrow2::compute::aggregate::max_string(value)
                .unwrap_or_default()
                .to_owned();
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
            proto_dyn_stats::Kind::Struct(x) => {
                let len = x.len.into_rust()?;
                let mut cols = BTreeMap::new();
                for (k, v) in x.cols {
                    cols.insert(k.into_rust()?, v.into_rust()?);
                }
                f.call(StructStats { len, cols })
            }
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
