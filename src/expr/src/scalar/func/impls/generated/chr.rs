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
pub struct Chr;
impl<'a> crate::func::EagerUnaryFunc<'a> for Chr {
    type Input = i32;
    type Output = Result<String, EvalError>;
    fn call(&self, a: Self::Input) -> Self::Output {
        let codepoint = u32::try_from(a)
            .map_err(|_| EvalError::CharacterTooLargeForEncoding(a))?;
        if codepoint == 0 {
            Err(EvalError::NullCharacterNotPermitted)
        } else if 0xd800 <= codepoint && codepoint < 0xe000 {
            Err(EvalError::CharacterNotValidForEncoding(a))
        } else {
            char::from_u32(codepoint)
                .map(|u| u.to_string())
                .ok_or(EvalError::CharacterTooLargeForEncoding(a))
        }
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
}
impl std::fmt::Display for Chr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(stringify!(chr))
    }
}
