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
pub struct ListListConcat;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for ListListConcat {
    type Input1 = Datum<'a>;
    type Input2 = Datum<'a>;
    type Output = Datum<'a>;
    fn call(
        &self,
        a: Self::Input1,
        b: Self::Input2,
        temp_storage: &'a mz_repr::RowArena,
    ) -> Self::Output {
        {
            if a.is_null() {
                return b;
            } else if b.is_null() {
                return a;
            }
            let a = a.unwrap_list().iter();
            let b = b.unwrap_list().iter();
            temp_storage.make_datum(|packer| packer.push_list(a.chain(b)))
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
    fn is_infix_op(&self) -> bool {
        true
    }
    fn propagates_nulls(&self) -> bool {
        false
    }
}
impl std::fmt::Display for ListListConcat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("||")
    }
}
