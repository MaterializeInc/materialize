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
pub struct DatePartTimestampTimestampTzF64;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for DatePartTimestampTimestampTzF64 {
    type Input1 = &'a str;
    type Input2 = CheckedTimestamp<DateTime<Utc>>;
    type Output = Result<f64, EvalError>;
    fn call(
        &self,
        units: Self::Input1,
        ts: Self::Input2,
        temp_storage: &'a mz_repr::RowArena,
    ) -> Self::Output {
        {
            match units.parse() {
                Ok(units) => date_part_timestamp_inner(units, &*ts),
                Err(_) => Err(EvalError::UnknownUnits(units.into())),
            }
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
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
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for DatePartTimestampTimestampTzF64 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("date_parttstz")
    }
}
