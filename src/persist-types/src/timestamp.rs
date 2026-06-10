// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Stats-related timestamp code.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use mz_ore::cast::CastFrom;

/// Parses a specific subset of ISO8061 timestamps.
///
/// This has very specific semantics so that it can enable pushdown on string
/// timestamps in JSON. See doc/user/content/sql/functions/pushdown.md for
/// details.
pub fn try_parse_monotonic_iso8601_timestamp(a: &str) -> Option<NaiveDateTime> {
    fn parse_lit(str: &mut &[u8], byte: u8) -> Option<()> {
        if *str.split_off_first()? == byte {
            Some(())
        } else {
            None
        }
    }

    fn parse_int(str: &mut &[u8], digits: usize) -> Option<u32> {
        let mut acc = 0u32;
        for digit in str.split_off(..digits)? {
            if !digit.is_ascii_digit() {
                return None;
            }
            acc = acc * 10 + u32::cast_from(*digit - b'0');
        }
        Some(acc)
    }

    // The following assumes this is ASCII so do a quick check first.
    if !a.is_ascii() {
        return None;
    }
    let bytes = &mut a.as_bytes();

    let yyyy = parse_int(bytes, 4)?;
    parse_lit(bytes, b'-')?;
    let mm = parse_int(bytes, 2)?;
    parse_lit(bytes, b'-')?;
    let dd = parse_int(bytes, 2)?;
    parse_lit(bytes, b'T')?;
    let hh = parse_int(bytes, 2)?;
    parse_lit(bytes, b':')?;
    let mi = parse_int(bytes, 2)?;
    parse_lit(bytes, b':')?;
    let ss = parse_int(bytes, 2)?;
    parse_lit(bytes, b'.')?;
    let ms = parse_int(bytes, 3)?;
    parse_lit(bytes, b'Z')?;

    if !bytes.is_empty() {
        return None;
    }

    // YYYY is a max 4-digit unsigned int, which can always be represented as a positive i32.
    let date = NaiveDate::from_ymd_opt(i32::try_from(yyyy).ok()?, mm, dd)?;
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
            "-005-01-01T00:00:00.000Z",
            "-002-01-01T00:00:00.000Z",
            "0000-01-01T00:00:00.000Z",
            "0001-01-01T00:00:00.000Z",
            "+000-01-01T00:00:00.000Z",
            "+001-01-01T00:00:00.000Z",
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
            "2015-09-18T23:57:00.000Zextra",
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
