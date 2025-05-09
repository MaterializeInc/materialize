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
pub struct CastJsonbableToJsonb;
impl<'a> crate::func::EagerUnaryFunc<'a> for CastJsonbableToJsonb {
    type Input = JsonbRef<'a>;
    type Output = JsonbRef<'a>;
    fn call(&self, a: Self::Input) -> Self::Output {
        match a.into_datum() {
            Datum::Numeric(n) => {
                let n = n.into_inner();
                let datum = if n.is_finite() {
                    Datum::from(n)
                } else if n.is_nan() {
                    Datum::String("NaN")
                } else if n.is_negative() {
                    Datum::String("-Infinity")
                } else {
                    Datum::String("Infinity")
                };
                JsonbRef::from_datum(datum)
            }
            datum => JsonbRef::from_datum(datum),
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
impl std::fmt::Display for CastJsonbableToJsonb {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("jsonbable_to_jsonb")
    }
}
