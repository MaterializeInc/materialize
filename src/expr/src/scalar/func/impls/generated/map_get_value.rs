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
pub struct MapGetValue;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for MapGetValue {
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
            let target_key = b.unwrap_str();
            match a.unwrap_map().iter().find(|(key, _v)| target_key == *key) {
                Some((_k, v)) => v,
                None => Datum::Null,
            }
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = input_type_a
            .scalar_type
            .unwrap_map_value_type()
            .clone()
            .nullable(true);
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
impl std::fmt::Display for MapGetValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("->")
    }
}
