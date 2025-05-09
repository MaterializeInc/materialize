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
pub struct ToTimestamp;
impl<'a> crate::func::EagerUnaryFunc<'a> for ToTimestamp {
    type Input = f64;
    type Output = Result<CheckedTimestamp<DateTime<Utc>>, EvalError>;
    fn call(&self, f: Self::Input) -> Self::Output {
        const NANO_SECONDS_PER_SECOND: i64 = 1_000_000_000;
        if f.is_nan() {
            Err(EvalError::TimestampCannotBeNan)
        } else if f.is_infinite() {
            Err(EvalError::TimestampOutOfRange)
        } else {
            let mut secs = i64::try_cast_from(f.trunc())
                .ok_or(EvalError::TimestampOutOfRange)?;
            let microsecs = (f.fract() * 1_000_000.0).round();
            let mut nanosecs = i64::try_cast_from(microsecs * 1_000.0)
                .ok_or(EvalError::TimestampOutOfRange)?;
            if nanosecs < 0 {
                secs = secs.checked_sub(1).ok_or(EvalError::TimestampOutOfRange)?;
                nanosecs = NANO_SECONDS_PER_SECOND
                    .checked_add(nanosecs)
                    .ok_or(EvalError::TimestampOutOfRange)?;
            }
            secs = secs
                .checked_add(nanosecs / NANO_SECONDS_PER_SECOND)
                .ok_or(EvalError::TimestampOutOfRange)?;
            nanosecs %= NANO_SECONDS_PER_SECOND;
            let nanosecs = u32::try_from(nanosecs)
                .map_err(|_| EvalError::TimestampOutOfRange)?;
            match DateTime::from_timestamp(secs, nanosecs) {
                Some(dt) => {
                    CheckedTimestamp::from_timestamplike(dt)
                        .map_err(|_| EvalError::TimestampOutOfRange)
                }
                None => Err(EvalError::TimestampOutOfRange),
            }
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
impl std::fmt::Display for ToTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("tots")
    }
}
