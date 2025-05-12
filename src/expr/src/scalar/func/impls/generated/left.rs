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
pub struct Left;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for Left {
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
            let string: &'a str = a.unwrap_str();
            let n = i64::from(b.unwrap_int32());
            let mut byte_indices = string.char_indices().map(|(i, _)| i);
            let end_in_bytes = match n.cmp(&0) {
                Ordering::Equal => 0,
                Ordering::Greater => {
                    let n = usize::try_from(n)
                        .map_err(|_| {
                            EvalError::InvalidParameterValue(
                                format!("invalid parameter n: {:?}", n).into(),
                            )
                        })?;
                    byte_indices.nth(n).unwrap_or(string.len())
                }
                Ordering::Less => {
                    let n = usize::try_from(n.abs() - 1)
                        .map_err(|_| {
                            EvalError::InvalidParameterValue(
                                format!("invalid parameter n: {:?}", n).into(),
                            )
                        })?;
                    byte_indices.rev().nth(n).unwrap_or(0)
                }
            };
            Ok(Datum::String(&string[..end_in_bytes]))
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = <String>::as_column_type();
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
        <String as ::mz_repr::DatumType<'_, ()>>::nullable()
    }
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for Left {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(stringify!(left))
    }
}
