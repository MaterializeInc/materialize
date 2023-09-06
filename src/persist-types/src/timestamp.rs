// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Stats-related timestamp code.

use std::ops::Range;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

/// Parses a specific subset of ISO8061 timestamps.
///
/// This has very specific semantics so that it can enable pushdown on string
/// timestamps in JSON. See doc/user/content/sql/functions/pushdown.md for
/// details.
pub fn try_parse_monotonic_iso8601_timestamp<'a>(a: &'a str) -> Option<NaiveDateTime> {
    const YYYY: Range<usize> = 0..0 + "YYYY".len();
    const LIT_DASH_0: Range<usize> = YYYY.end..YYYY.end + "-".len();
    const MM: Range<usize> = LIT_DASH_0.end..LIT_DASH_0.end + "MM".len();
    const LIT_DASH_1: Range<usize> = MM.end..MM.end + "-".len();
    const DD: Range<usize> = LIT_DASH_1.end..LIT_DASH_1.end + "DD".len();
    const LIT_T: Range<usize> = DD.end..DD.end + "T".len();
    const HH: Range<usize> = LIT_T.end..LIT_T.end + "HH".len();
    const LIT_COLON_0: Range<usize> = HH.end..HH.end + ":".len();
    const MI: Range<usize> = LIT_COLON_0.end..LIT_COLON_0.end + "MI".len();
    const LIT_COLON_1: Range<usize> = MI.end..MI.end + ":".len();
    const SS: Range<usize> = LIT_COLON_1.end..LIT_COLON_1.end + "SS".len();
    const LIT_DOT: Range<usize> = SS.end..SS.end + ".".len();
    // NB "MS" pattern is shorter than what it matches, so hardcode the 3.
    const MS: Range<usize> = LIT_DOT.end..LIT_DOT.end + 3;
    const LIT_Z: Range<usize> = MS.end..MS.end + "Z".len();

    // The following assumes this is ASCII so do a quick check first.
    if !a.is_ascii() {
        return None;
    }

    if a.len() != LIT_Z.end {
        return None;
    }
    if &a[LIT_DASH_0] != "-"
        || &a[LIT_DASH_1] != "-"
        || &a[LIT_T] != "T"
        || &a[LIT_COLON_0] != ":"
        || &a[LIT_COLON_1] != ":"
        || &a[LIT_DOT] != "."
        || &a[LIT_Z] != "Z"
    {
        return None;
    }
    let yyyy = a[YYYY].parse().ok()?;
    let mm = a[MM].parse().ok()?;
    let dd = a[DD].parse().ok()?;
    let hh = a[HH].parse().ok()?;
    let mi = a[MI].parse().ok()?;
    let ss = a[SS].parse().ok()?;
    let ms = a[MS].parse().ok()?;
    let date = NaiveDate::from_ymd_opt(yyyy, mm, dd)?;
    let time = NaiveTime::from_hms_milli_opt(hh, mi, ss, ms)?;
    Some(NaiveDateTime::new(date, time))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn monotonic_iso8601() {
        // The entire point of this method is that the lexicographic order
        // corresponds to chronological order (ignoring None/NULL). So, verify.
        let mut inputs = vec![
            "0000-01-01T00:00:00.000Z",
            "0001-01-01T00:00:00.000Z",
            "2015-00-00T00:00:00.000Z",
            "2015-09-00T00:00:00.000Z",
            "2015-09-18T00:00:00.000Z",
            "2015-09-18T23:00:00.000Z",
            "2015-09-18T23:56:00.000Z",
            "2015-09-18T23:56:04.000Z",
            "2015-09-18T23:56:04.123Z",
            "2015-09-18T23:56:04.1234Z",
            "2015-09-18T23:56:04.124Z",
            "2015-09-18T23:56:05.000Z",
            "2015-09-18T23:57:00.000Z",
            "2015-09-18T24:00:00.000Z",
            "2015-09-19T00:00:00.000Z",
            "2015-10-00T00:00:00.000Z",
            "2016-10-00T00:00:00.000Z",
            "9999-12-31T23:59:59.999Z",
        ];
        // Sort the inputs so we can't accidentally pass by hardcoding them in
        // the wrong order.
        inputs.sort();
        let outputs = inputs
            .into_iter()
            .flat_map(try_parse_monotonic_iso8601_timestamp)
            .collect::<Vec<_>>();
        // Sanity check that we don't trivially pass by always returning None.
        assert!(!outputs.is_empty());
        let mut outputs_sorted = outputs.clone();
        outputs_sorted.sort();
        assert_eq!(outputs, outputs_sorted);
    }
}
