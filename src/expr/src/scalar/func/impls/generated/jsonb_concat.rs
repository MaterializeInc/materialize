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
pub struct JsonbConcat;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for JsonbConcat {
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
            match (a, b) {
                (Datum::Map(dict_a), Datum::Map(dict_b)) => {
                    let mut pairs = dict_b
                        .iter()
                        .chain(dict_a.iter())
                        .collect::<Vec<_>>();
                    pairs.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));
                    pairs.dedup_by(|(k1, _v1), (k2, _v2)| k1 == k2);
                    temp_storage.make_datum(|packer| packer.push_dict(pairs))
                }
                (Datum::List(list_a), Datum::List(list_b)) => {
                    let elems = list_a.iter().chain(list_b.iter());
                    temp_storage.make_datum(|packer| packer.push_list(elems))
                }
                (Datum::List(list_a), b) => {
                    let elems = list_a.iter().chain(Some(b));
                    temp_storage.make_datum(|packer| packer.push_list(elems))
                }
                (a, Datum::List(list_b)) => {
                    let elems = Some(a).into_iter().chain(list_b.iter());
                    temp_storage.make_datum(|packer| packer.push_list(elems))
                }
                _ => Datum::Null,
            }
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = ScalarType::Jsonb.nullable(true);
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
impl std::fmt::Display for JsonbConcat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("||")
    }
}
