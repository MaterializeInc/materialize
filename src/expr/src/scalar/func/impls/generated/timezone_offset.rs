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
pub struct TimezoneOffset;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for TimezoneOffset {
    type Input1 = Datum<'a>;
    type Input2 = Datum<'a>;
    type Output = Result<Datum<'a>, EvalError>;
    fn call(
        &self,
        a: Self::Input1,
        b: Self::Input2,
        temp_storage: &'a mz_repr::RowArena,
    ) -> Self::Output {
        {
            let tz_str = a.unwrap_str();
            let tz = match Tz::from_str_insensitive(tz_str) {
                Ok(tz) => tz,
                Err(_) => return Err(EvalError::InvalidIanaTimezoneId(tz_str.into())),
            };
            let offset = tz
                .offset_from_utc_datetime(&b.unwrap_timestamptz().naive_utc());
            Ok(
                temp_storage
                    .make_datum(|packer| {
                        packer
                            .push_list_with(|packer| {
                                packer.push(Datum::from(offset.abbreviation()));
                                packer.push(Datum::from(offset.base_utc_offset()));
                                packer.push(Datum::from(offset.dst_offset()));
                            });
                    }),
            )
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = ScalarType::Record {
            fields: [
                ("abbrev".into(), ScalarType::String.nullable(false)),
                ("base_utc_offset".into(), ScalarType::Interval.nullable(false)),
                ("dst_offset".into(), ScalarType::Interval.nullable(false)),
            ]
                .into(),
            custom_id: None,
        }
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
        false
    }
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for TimezoneOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(stringify!(timezone_offset))
    }
}
