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
pub struct JsonbContainsJsonb;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for JsonbContainsJsonb {
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
            fn contains(a: Datum, b: Datum, at_top_level: bool) -> bool {
                match (a, b) {
                    (Datum::JsonNull, Datum::JsonNull) => true,
                    (Datum::False, Datum::False) => true,
                    (Datum::True, Datum::True) => true,
                    (Datum::Numeric(a), Datum::Numeric(b)) => a == b,
                    (Datum::String(a), Datum::String(b)) => a == b,
                    (Datum::List(a), Datum::List(b)) => {
                        b.iter()
                            .all(|b_elem| {
                                a.iter().any(|a_elem| contains(a_elem, b_elem, false))
                            })
                    }
                    (Datum::Map(a), Datum::Map(b)) => {
                        b.iter()
                            .all(|(b_key, b_val)| {
                                a.iter()
                                    .any(|(a_key, a_val)| {
                                        (a_key == b_key) && contains(a_val, b_val, false)
                                    })
                            })
                    }
                    (Datum::List(a), b) => {
                        at_top_level && a.iter().any(|a_elem| contains(a_elem, b, false))
                    }
                    _ => false,
                }
            }
            contains(a, b, true).into()
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = <bool>::as_column_type();
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
        <bool as ::mz_repr::DatumType<'_, ()>>::nullable()
    }
    fn is_infix_op(&self) -> bool {
        true
    }
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for JsonbContainsJsonb {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("@>")
    }
}
