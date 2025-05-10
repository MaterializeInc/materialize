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
pub struct CastMzTimestampToTimestampTz;
impl<'a> crate::func::EagerUnaryFunc<'a> for CastMzTimestampToTimestampTz {
    type Input = Timestamp;
    type Output = Result<CheckedTimestamp<DateTime<Utc>>, EvalError>;
    fn call(&self, a: Self::Input) -> Self::Output {
        let ms: i64 = a.try_into().map_err(|_| EvalError::TimestampOutOfRange)?;
        let ct = DateTime::from_timestamp_millis(ms)
            .and_then(|dt| {
                let ct: Option<CheckedTimestamp<DateTime<Utc>>> = dt.try_into().ok();
                ct
            });
        ct.ok_or(EvalError::TimestampOutOfRange)
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
    fn inverse(&self) -> Option<crate::UnaryFunc> {
        to_unary!(super::CastTimestampTzToMzTimestamp)
    }
    fn preserves_uniqueness(&self) -> bool {
        true
    }
}
impl std::fmt::Display for CastMzTimestampToTimestampTz {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("mz_timestamp_to_timestamp_tz")
    }
}
