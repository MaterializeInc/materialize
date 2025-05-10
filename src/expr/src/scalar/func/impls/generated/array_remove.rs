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
pub struct ArrayRemove;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for ArrayRemove {
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
            if a.is_null() {
                return Ok(a);
            }
            let arr = a.unwrap_array();
            if arr.dims().len() == 0 {
                return Ok(a);
            }
            if arr.dims().len() > 1 {
                return Err(EvalError::MultidimensionalArrayRemovalNotSupported);
            }
            let elems: Vec<_> = arr.elements().iter().filter(|v| v != &b).collect();
            let mut dims = arr.dims().into_iter().collect::<Vec<_>>();
            dims[0] = ArrayDimension {
                lower_bound: 1,
                length: elems.len(),
            };
            Ok(
                temp_storage
                    .try_make_datum(|packer| packer.try_push_array(&dims, elems))?,
            )
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = input_type_a.scalar_type.without_modifiers().nullable(true);
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
        false
    }
    fn propagates_nulls(&self) -> bool {
        false
    }
}
impl std::fmt::Display for ArrayRemove {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("array_remove")
    }
}
