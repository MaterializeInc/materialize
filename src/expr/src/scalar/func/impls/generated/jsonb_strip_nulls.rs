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
pub struct JsonbStripNulls;
impl<'a> crate::func::EagerUnaryFunc<'a> for JsonbStripNulls {
    type Input = JsonbRef<'a>;
    type Output = Jsonb;
    fn call(&self, a: Self::Input) -> Self::Output {
        fn strip_nulls(a: Datum, row: &mut RowPacker) {
            match a {
                Datum::Map(dict) => {
                    row.push_dict_with(|row| {
                        for (k, v) in dict.iter() {
                            match v {
                                Datum::JsonNull => {}
                                _ => {
                                    row.push(Datum::String(k));
                                    strip_nulls(v, row);
                                }
                            }
                        }
                    })
                }
                Datum::List(list) => {
                    row.push_list_with(|row| {
                        for elem in list.iter() {
                            strip_nulls(elem, row);
                        }
                    })
                }
                _ => row.push(a),
            }
        }
        let mut row = Row::default();
        strip_nulls(a.into_datum(), &mut row.packer());
        Jsonb::from_row(row)
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
}
impl std::fmt::Display for JsonbStripNulls {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(stringify!(jsonb_strip_nulls))
    }
}
