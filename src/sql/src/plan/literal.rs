// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::adt::interval::{Interval, RoundBehavior};
use mz_repr::strconv;
use mz_sql_parser::ast::IntervalValue;

use crate::plan::PlanError;

/// Convert an [`IntervalValue`] into an [`Interval`].
///
/// The reverse of [`unplan_interval`].
pub fn plan_interval(iv: &IntervalValue) -> Result<Interval, PlanError> {
    let leading_precision = parser_datetimefield_to_adt(iv.precision_high);
    let mut i = strconv::parse_interval_w_disambiguator(
        &iv.value,
        match leading_precision {
            mz_repr::adt::datetime::DateTimeField::Hour
            | mz_repr::adt::datetime::DateTimeField::Minute => Some(leading_precision),
            _ => None,
        },
        parser_datetimefield_to_adt(iv.precision_low),
    )?;
    i.truncate_high_fields(parser_datetimefield_to_adt(iv.precision_high));
    i.truncate_low_fields(
        parser_datetimefield_to_adt(iv.precision_low),
        iv.fsec_max_precision,
        RoundBehavior::Nearest,
    )?;
    Ok(i)
}

/// Convert an [`Interval`] into an [`IntervalValue`].
///
/// The reverse of [`plan_interval`].
pub fn unplan_interval(i: &Interval) -> IntervalValue {
    let mut iv = IntervalValue::default();
    iv.value = i.to_string();
    iv
}

fn parser_datetimefield_to_adt(
    dtf: mz_sql_parser::ast::DateTimeField,
) -> mz_repr::adt::datetime::DateTimeField {
    use mz_sql_parser::ast::DateTimeField::*;
    match dtf {
        Millennium => mz_repr::adt::datetime::DateTimeField::Millennium,
        Century => mz_repr::adt::datetime::DateTimeField::Century,
        Decade => mz_repr::adt::datetime::DateTimeField::Decade,
        Year => mz_repr::adt::datetime::DateTimeField::Year,
        Month => mz_repr::adt::datetime::DateTimeField::Month,
        Day => mz_repr::adt::datetime::DateTimeField::Day,
        Hour => mz_repr::adt::datetime::DateTimeField::Hour,
        Minute => mz_repr::adt::datetime::DateTimeField::Minute,
        Second => mz_repr::adt::datetime::DateTimeField::Second,
        Milliseconds => mz_repr::adt::datetime::DateTimeField::Milliseconds,
        Microseconds => mz_repr::adt::datetime::DateTimeField::Microseconds,
    }
}
