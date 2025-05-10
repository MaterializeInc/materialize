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
pub struct DivFloat32;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for DivFloat32 {
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
            let a = a.unwrap_float32();
            let b = b.unwrap_float32();
            if b == 0.0f32 && !a.is_nan() {
                Err(EvalError::DivisionByZero)
            } else {
                let quotient = a / b;
                if quotient.is_infinite() && !a.is_infinite() {
                    Err(EvalError::FloatOverflow)
                } else if quotient == 0.0f32 && a != 0.0f32 && !b.is_infinite() {
                    Err(EvalError::FloatUnderflow)
                } else {
                    Ok(Datum::from(quotient))
                }
            }
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = <f32>::as_column_type();
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
        <f32 as ::mz_repr::DatumType<'_, ()>>::nullable()
    }
    fn is_infix_op(&self) -> bool {
        true
    }
    fn is_monotone(&self) -> (bool, bool) {
        (true, false)
    }
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for DivFloat32 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("/")
    }
}
