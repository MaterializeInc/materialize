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
pub struct BitNotInt32;
impl<'a> crate::func::EagerUnaryFunc<'a> for BitNotInt32 {
    type Input = i32;
    type Output = i32;
    fn call(&self, a: Self::Input) -> Self::Output {
        !a
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(BitNotInt32)
    }
    fn preserves_uniqueness(&self) -> bool {
        true
    }
}
impl std::fmt::Display for BitNotInt32 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("~")
    }
}
