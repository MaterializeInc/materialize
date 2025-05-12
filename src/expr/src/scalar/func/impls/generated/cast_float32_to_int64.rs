// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.


#[derive(
    proptest_derive::Arbitrary,
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    Hash,
    mz_lowertest::MzReflect
)]
pub struct CastFloat32ToInt64;
impl<'a> crate::func::EagerUnaryFunc<'a> for CastFloat32ToInt64 {
    type Input = f32;
    type Output = Result<i64, EvalError>;
    fn call(&self, a: Self::Input) -> Self::Output {
        let f = round_float32(a);
        #[allow(clippy::as_conversions)]
        if (f >= (i64::MIN as f32)) && (f < -(i64::MIN as f32)) {
            Ok(f as i64)
        } else {
            Err(EvalError::Int64OutOfRange(f.to_string().into()))
        }
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastInt64ToFloat32)
    }
    fn is_monotone(&self) -> bool {
        true
    }
}
impl std::fmt::Display for CastFloat32ToInt64 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("real_to_bigint")
    }
}
