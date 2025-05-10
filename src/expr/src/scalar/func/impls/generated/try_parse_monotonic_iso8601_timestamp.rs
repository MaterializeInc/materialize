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
pub struct TryParseMonotonicIso8601Timestamp;
impl<'a> crate::func::EagerUnaryFunc<'a> for TryParseMonotonicIso8601Timestamp {
    type Input = &'a str;
    type Output = Option<CheckedTimestamp<NaiveDateTime>>;
    fn call(&self, a: Self::Input) -> Self::Output {
        let ts = mz_persist_types::timestamp::try_parse_monotonic_iso8601_timestamp(a)?;
        let ts = CheckedTimestamp::from_timestamplike(ts)
            .expect("monotonic_iso8601 range is a subset of CheckedTimestamp domain");
        Some(ts)
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
}
impl std::fmt::Display for TryParseMonotonicIso8601Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("try_parse_monotonic_iso8601_timestamp")
    }
}
