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
pub struct JustifyHours;
impl<'a> crate::func::EagerUnaryFunc<'a> for JustifyHours {
    type Input = Interval;
    type Output = Result<Interval, EvalError>;
    fn call(&self, i: Self::Input) -> Self::Output {
        i.justify_hours()
            .map_err(|_| EvalError::IntervalOutOfRange(i.to_string().into()))
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
}
impl std::fmt::Display for JustifyHours {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("justify_hours")
    }
}
