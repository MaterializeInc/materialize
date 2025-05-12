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
pub struct ArrayLength;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for ArrayLength {
    type Input1 = Datum<'a>;
    type Input2 = Datum<'a>;
    type Output = Result<Datum<'a>, EvalError>;
    fn call(
        &self,
        a: Self::Input1,
        b: Self::Input2,
        temp_storage: &'a mz_repr::RowArena,
    ) -> Self::Output {
        {
            let i = match usize::try_from(b.unwrap_int64()) {
                Ok(0) | Err(_) => return Ok(Datum::Null),
                Ok(n) => n - 1,
            };
            Ok(
                match a.unwrap_array().dims().into_iter().nth(i) {
                    None => Datum::Null,
                    Some(dim) => {
                        Datum::Int32(
                            dim
                                .length
                                .try_into()
                                .map_err(|_| EvalError::Int32OutOfRange(
                                    dim.length.to_string().into(),
                                ))?,
                        )
                    }
                },
            )
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = <Option<i32>>::as_column_type();
        let propagates_nulls = crate::func::binary::EagerBinaryFunc::propagates_nulls(
            self,
        );
        let nullable = output.nullable;
        output
            .nullable(
                nullable
                    || (propagates_nulls
                        && (input_type_a.nullable || input_type_b.nullable)),
            )
    }
    fn introduces_nulls(&self) -> bool {
        true
    }
    fn is_infix_op(&self) -> bool {
        true
    }
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for ArrayLength {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("array_length")
    }
}
