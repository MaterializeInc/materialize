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
pub struct CastNumericToInt16;
impl<'a> crate::func::EagerUnaryFunc<'a> for CastNumericToInt16 {
    type Input = Numeric;
    type Output = Result<i16, EvalError>;
    fn call(&self, a: Self::Input) -> Self::Output {
        let mut a = a;
        {
            let mut cx = numeric::cx_datum();
            cx.round(&mut a);
            cx.clear_status();
            let i = cx
                .try_into_i32(a)
                .or_else(|_| Err(EvalError::Int16OutOfRange(a.to_string().into())))?;
            i16::try_from(i)
                .or_else(|_| Err(EvalError::Int16OutOfRange(i.to_string().into())))
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
        to_unary!(super::CastInt16ToNumeric(None))
    }
    fn is_monotone(&self) -> bool {
        true
    }
}
impl std::fmt::Display for CastNumericToInt16 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("numeric_to_smallint")
    }
}
