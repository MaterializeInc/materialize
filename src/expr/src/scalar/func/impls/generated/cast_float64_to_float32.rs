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
pub struct CastFloat64ToFloat32;
impl<'a> crate::func::EagerUnaryFunc<'a> for CastFloat64ToFloat32 {
    type Input = f64;
    type Output = Result<f32, EvalError>;
    fn call(&self, a: Self::Input) -> Self::Output {
        #[allow(clippy::as_conversions)]
        let result = a as f32;
        if result.is_infinite() && !a.is_infinite() {
            Err(EvalError::FloatOverflow)
        } else if result == 0.0 && a != 0.0 {
            Err(EvalError::FloatUnderflow)
        } else {
            Ok(result)
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
        to_unary!(super::CastFloat32ToFloat64)
    }
    fn is_monotone(&self) -> bool {
        true
    }
}
impl std::fmt::Display for CastFloat64ToFloat32 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("double_to_real")
    }
}
