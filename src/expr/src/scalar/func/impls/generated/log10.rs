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
pub struct Log10;
impl<'a> crate::func::EagerUnaryFunc<'a> for Log10 {
    type Input = f64;
    type Output = Result<f64, EvalError>;
    fn call(&self, a: Self::Input) -> Self::Output {
        if a.is_sign_negative() {
            return Err(EvalError::NegativeOutOfDomain("log10".into()));
        }
        if a == 0.0 {
            return Err(EvalError::ZeroOutOfDomain("log10".into()));
        }
        Ok(a.log10())
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
}
impl std::fmt::Display for Log10 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("log10f64")
    }
}
