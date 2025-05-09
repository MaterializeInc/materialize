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
pub struct CastDateToMzTimestamp;
impl<'a> crate::func::EagerUnaryFunc<'a> for CastDateToMzTimestamp {
    type Input = Date;
    type Output = Result<Timestamp, EvalError>;
    fn call(&self, a: Self::Input) -> Self::Output {
        let ts = CheckedTimestamp::try_from(
            NaiveDate::from(a).and_hms_opt(0, 0, 0).unwrap(),
        )?;
        ts.and_utc()
            .timestamp_millis()
            .try_into()
            .map_err(|_| EvalError::MzTimestampOutOfRange(a.to_string().into()))
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
    fn is_monotone(&self) -> bool {
        true
    }
    fn preserves_uniqueness(&self) -> bool {
        true
    }
}
impl std::fmt::Display for CastDateToMzTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("date_to_mz_timestamp")
    }
}
