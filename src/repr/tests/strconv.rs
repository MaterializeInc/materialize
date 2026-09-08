// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use mz_repr::adt::date::Date;
use mz_repr::adt::datetime::DateTimeField;
use mz_repr::adt::interval::Interval;
use mz_repr::strconv;

#[mz_ore::test]
fn test_parse_date() {
    run_test_parse_date("000203", NaiveDate::from_ymd_opt(2000, 2, 3).unwrap());
    run_test_parse_date("690203", NaiveDate::from_ymd_opt(2069, 2, 3).unwrap());
    run_test_parse_date("700203", NaiveDate::from_ymd_opt(1970, 2, 3).unwrap());
    run_test_parse_date("010203", NaiveDate::from_ymd_opt(2001, 2, 3).unwrap());
    run_test_parse_date("0010203", NaiveDate::from_ymd_opt(1, 2, 3).unwrap());
    run_test_parse_date("00010203", NaiveDate::from_ymd_opt(1, 2, 3).unwrap());
    run_test_parse_date("20010203", NaiveDate::from_ymd_opt(2001, 2, 3).unwrap());
    run_test_parse_date("99990203", NaiveDate::from_ymd_opt(9999, 2, 3).unwrap());
    run_test_parse_date("2001-02-03", NaiveDate::from_ymd_opt(2001, 2, 3).unwrap());
    run_test_parse_date("2001 02 03", NaiveDate::from_ymd_opt(2001, 2, 3).unwrap());
    run_test_parse_date(
        "2001-02-03 04:05:06.789",
        NaiveDate::from_ymd_opt(2001, 2, 3).unwrap(),
    );
    fn run_test_parse_date(s: &str, n: NaiveDate) {
        assert_eq!(NaiveDate::from(strconv::parse_date(s).unwrap()), n);
    }
}

#[mz_ore::test]
fn test_parse_date_errors() {
    run_test_parse_date_errors(
        "0000203",
        "invalid input syntax for type date: YEAR cannot be zero: \"0000203\"",
    );
    run_test_parse_date_errors(
        "00000203",
        "invalid input syntax for type date: YEAR cannot be zero: \"00000203\"",
    );
    run_test_parse_date_errors(
        "0000-02-03",
        "invalid input syntax for type date: YEAR cannot be zero: \"0000-02-03\"",
    );
    run_test_parse_date_errors(
        "0010230",
        "invalid input syntax for type date: invalid or out-of-range date: \"0010230\"",
    );
    run_test_parse_date_errors(
        "00011303",
        "invalid input syntax for type date: MONTH must be [1, 12], got 13: \"00011303\"",
    );
    run_test_parse_date_errors(
        "-123456789",
        "invalid input syntax for type date: MONTH must be [1, 12], got 123456789: \"-123456789\"",
    );
    run_test_parse_date_errors(
        "2001-01",
        "invalid input syntax for type date: YEAR, MONTH, DAY are all required: \"2001-01\"",
    );
    run_test_parse_date_errors(
        "2001",
        "invalid input syntax for type date: YEAR, MONTH, DAY are all required: \"2001\"",
    );
    run_test_parse_date_errors(
        "2019-02-29",
        "invalid input syntax for type date: invalid or out-of-range date: \"2019-02-29\"",
    );
    run_test_parse_date_errors(
        "2020-02-30",
        "invalid input syntax for type date: invalid or out-of-range date: \"2020-02-30\"",
    );
    run_test_parse_date_errors(
        "2001-13-01",
        "invalid input syntax for type date: MONTH must be [1, 12], got 13: \"2001-13-01\"",
    );
    run_test_parse_date_errors(
        "2001-12-32",
        "invalid input syntax for type date: DAY must be [1, 31], got 32: \"2001-12-32\"",
    );
    run_test_parse_date_errors(
        "2001-01-02 04",
        "invalid input syntax for type date: have unprocessed tokens 4: \"2001-01-02 04\"",
    );
    fn run_test_parse_date_errors(s: &str, e: &str) {
        assert_eq!(
            e.to_string(),
            format!("{}", strconv::parse_date(s).unwrap_err())
        );
    }
}

#[mz_ore::test]
fn test_parse_time() {
    run_test_parse_time(
        "01:02:03.456",
        NaiveTime::from_hms_nano_opt(1, 2, 3, 456_000_000).unwrap(),
    );
    run_test_parse_time("01:02:03", NaiveTime::from_hms_opt(1, 2, 3).unwrap());
    run_test_parse_time(
        "02:03.456",
        NaiveTime::from_hms_nano_opt(0, 2, 3, 456_000_000).unwrap(),
    );
    run_test_parse_time("01:02", NaiveTime::from_hms_opt(1, 2, 0).unwrap());

    // Regression for database-issues#1933.
    run_test_parse_time(
        "9::60",
        NaiveTime::from_hms_nano_opt(9, 0, 59, 1_000_000_000).unwrap(),
    );

    fn run_test_parse_time(s: &str, t: NaiveTime) {
        assert_eq!(strconv::parse_time(s).unwrap(), t);
    }
}

#[mz_ore::test]
fn test_parse_time_errors() {
    run_test_parse_time_errors(
        "26:01:02.345",
        "invalid input syntax for type time: HOUR must be [0, 23], got 26: \"26:01:02.345\"",
    );
    run_test_parse_time_errors(
        "01:60:02.345",
        "invalid input syntax for type time: MINUTE must be [0, 59], got 60: \"01:60:02.345\"",
    );
    run_test_parse_time_errors(
        "01:02:61.345",
        "invalid input syntax for type time: SECOND must be [0, 60], got 61: \"01:02:61.345\"",
    );
    run_test_parse_time_errors(
        "03.456",
        "invalid input syntax for type time: have unprocessed tokens 3.456000000: \"03.456\"",
    );
    run_test_parse_time_errors(
        "03.456",
        "invalid input syntax for type time: have unprocessed tokens 3.456000000: \"03.456\"",
    );
    // A string that parses without naming a single time field is rejected rather
    // than read as midnight, matching PostgreSQL.
    run_test_parse_time_errors(
        "",
        "invalid input syntax for type time: no time fields found: \"\"",
    );
    run_test_parse_time_errors(
        " ",
        "invalid input syntax for type time: no time fields found: \" \"",
    );
    run_test_parse_time_errors(
        ":",
        "invalid input syntax for type time: no time fields found: \":\"",
    );

    fn run_test_parse_time_errors(s: &str, e: &str) {
        assert_eq!(
            e.to_string(),
            format!("{}", strconv::parse_time(s).unwrap_err())
        );
    }
}

/// The frozen storage-cast reading of a TIME string with no time field in it.
/// See the stability contract in `mz_storage_types::sources::casts`.
#[mz_ore::test]
fn test_parse_time_legacy_fieldless() {
    for s in ["", " ", ":"] {
        assert_eq!(
            strconv::parse_time_legacy(s).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            "for input {s:?}"
        );
    }
    // Everything else parses identically to `parse_time`.
    assert_eq!(
        strconv::parse_time_legacy("01:02:03").unwrap(),
        strconv::parse_time("01:02:03").unwrap()
    );
}

#[mz_ore::test]
fn test_parse_timestamp() {
    use mz_repr::adt::timestamp::CheckedTimestamp;

    run_test_parse_timestamp(
        "2001-02-03 04:05:06.789",
        NaiveDate::from_ymd_opt(2001, 2, 3)
            .unwrap()
            .and_hms_nano_opt(4, 5, 6, 789_000_000)
            .unwrap(),
    );
    run_test_parse_timestamp(
        "2001-02-03",
        NaiveDate::from_ymd_opt(2001, 2, 3)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );
    run_test_parse_timestamp(
        "2001-02-03 01:02:03",
        NaiveDate::from_ymd_opt(2001, 2, 3)
            .unwrap()
            .and_hms_opt(1, 2, 3)
            .unwrap(),
    );
    run_test_parse_timestamp(
        "2001-02-03 02:03.456",
        NaiveDate::from_ymd_opt(2001, 2, 3)
            .unwrap()
            .and_hms_nano_opt(0, 2, 3, 456_000_000)
            .unwrap(),
    );
    run_test_parse_timestamp(
        "2001-02-03 01:02",
        NaiveDate::from_ymd_opt(2001, 2, 3)
            .unwrap()
            .and_hms_opt(1, 2, 0)
            .unwrap(),
    );

    fn run_test_parse_timestamp(s: &str, ts: NaiveDateTime) {
        assert_eq!(
            strconv::parse_timestamp(s).unwrap(),
            CheckedTimestamp::from_timestamplike(ts).unwrap()
        );
    }
}

#[mz_ore::test]
fn test_parse_timestamp_errors() {
    run_test_parse_timestamp_errors(
        "2001-01",
        "invalid input syntax for type timestamp: YEAR, MONTH, DAY are all required: \"2001-01\"",
    );
    run_test_parse_timestamp_errors(
        "2001",
        "invalid input syntax for type timestamp: YEAR, MONTH, DAY are all required: \"2001\"",
    );
    run_test_parse_timestamp_errors(
        "2001-13-01",
        "invalid input syntax for type timestamp: MONTH must be [1, 12], got 13: \"2001-13-01\"",
    );
    run_test_parse_timestamp_errors(
        "2001-12-32",
        "invalid input syntax for type timestamp: DAY must be [1, 31], got 32: \"2001-12-32\"",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 04",
        "invalid input syntax for type timestamp: have unprocessed tokens 4: \"2001-01-02 04\"",
    );

    run_test_parse_timestamp_errors(
        "2001-01-02 26:01:02.345",
        "invalid input syntax for type timestamp: HOUR must be [0, 23], got 26: \"2001-01-02 26:01:02.345\"",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 01:60:02.345",
        "invalid input syntax for type timestamp: MINUTE must be [0, 59], got 60: \"2001-01-02 01:60:02.345\"",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 01:02:61.345",
        "invalid input syntax for type timestamp: SECOND must be [0, 60], got 61: \"2001-01-02 01:02:61.345\"",
    );

    fn run_test_parse_timestamp_errors(s: &str, e: &str) {
        assert_eq!(
            e.to_string(),
            format!("{}", strconv::parse_timestamp(s).unwrap_err())
        );
    }
}

#[mz_ore::test]
fn test_parse_timestamptz() {
    use mz_repr::adt::timestamp::CheckedTimestamp;

    #[rustfmt::skip]
    let test_cases = [("1999-01-01 01:23:34.555", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555+0:00", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555+0", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555z", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555Z", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555 z", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555 Z", 1999, 1, 1, 1, 23, 34, 555_000_000, 0),
        ("1999-01-01 01:23:34.555+4:00", 1999, 1, 1, 1, 23, 34, 555_000_000, 14400),
        ("1999-01-01 01:23:34.555-4:00", 1999, 1, 1, 1, 23, 34, 555_000_000, -14400),
        ("1999-01-01 01:23:34.555+400", 1999, 1, 1, 1, 23, 34, 555_000_000, 14400),
        ("1999-01-01 01:23:34.555+4", 1999, 1, 1, 1, 23, 34, 555_000_000, 14400),
        ("1999-01-01 01:23:34.555+4:30", 1999, 1, 1, 1, 23, 34, 555_000_000, 16200),
        ("1999-01-01 01:23:34.555+430", 1999, 1, 1, 1, 23, 34, 555_000_000, 16200),
        ("1999-01-01 01:23:34.555+4:45", 1999, 1, 1, 1, 23, 34, 555_000_000, 17100),
        ("1999-01-01 01:23:34.555+445", 1999, 1, 1, 1, 23, 34, 555_000_000, 17100),
        ("1999-01-01 01:23:34.555+14:45", 1999, 1, 1, 1, 23, 34, 555_000_000, 53100),
        ("1999-01-01 01:23:34.555-14:45", 1999, 1, 1, 1, 23, 34, 555_000_000, -53100),
        ("1999-01-01 01:23:34.555+1445", 1999, 1, 1, 1, 23, 34, 555_000_000, 53100),
        ("1999-01-01 01:23:34.555-1445", 1999, 1, 1, 1, 23, 34, 555_000_000, -53100),
        ("1999-01-01 01:23:34.555 +14:45", 1999, 1, 1, 1, 23, 34, 555_000_000, 53100),
        ("1999-01-01 01:23:34.555 -14:45", 1999, 1, 1, 1, 23, 34, 555_000_000, -53100),
        ("1999-01-01 01:23:34.555 +1445", 1999, 1, 1, 1, 23, 34, 555_000_000, 53100),
        ("1999-01-01 01:23:34.555 -1445", 1999, 1, 1, 1, 23, 34, 555_000_000, -53100),
    ];

    for test in test_cases.iter() {
        let actual = strconv::parse_timestamptz(test.0).unwrap();

        let expected = NaiveDate::from_ymd_opt(test.1, test.2, test.3)
            .unwrap()
            .and_hms_nano_opt(test.4, test.5, test.6, test.7)
            .unwrap();
        let offset = FixedOffset::east_opt(test.8).unwrap();
        let dt_fixed_offset = offset.from_local_datetime(&expected).earliest().unwrap();
        let expected = CheckedTimestamp::from_timestamplike(dt_fixed_offset.to_utc()).unwrap();

        assert_eq!(actual, expected);
    }
}

#[mz_ore::test]
fn test_parse_timestamptz_errors() {
    run_test_parse_timestamptz_errors(
        "1999-01-01 01:23:34.555 +25:45",
        "invalid input syntax for type timestamp with time zone: Invalid timezone string \
         (+25:45): timezone hour invalid 25: \"1999-01-01 01:23:34.555 +25:45\"",
    );
    run_test_parse_timestamptz_errors(
        "1999-01-01 01:23:34.555 +15:61",
        "invalid input syntax for type timestamp with time zone: Invalid timezone string \
         (+15:61): timezone minute invalid 61: \"1999-01-01 01:23:34.555 +15:61\"",
    );
    run_test_parse_timestamptz_errors(
        "1999-01-01 01:23:34.555 4",
        "invalid input syntax for type timestamp with time zone: Cannot parse timezone offset 4: \
         \"1999-01-01 01:23:34.555 4\"",
    );

    fn run_test_parse_timestamptz_errors(s: &str, e: &str) {
        assert_eq!(
            e.to_string(),
            format!("{}", strconv::parse_timestamptz(s).unwrap_err())
        );
    }
}

#[mz_ore::test]
fn test_parse_timestamptz_offset_overflow() {
    // `HIGH_DATE` is exactly `chrono::NaiveDate::MAX` and the low bound is
    // PostgreSQL's 4713 BC, so applying the offset to a value just inside either
    // bound leaves chrono's range. The offset is applied before the
    // `CheckedTimestamp` bound check, so that check cannot catch it, and chrono's
    // own `NaiveDateTime - FixedOffset` panics instead of erroring.
    for s in [
        // High end: a westward offset moves the value past `NaiveDate::MAX`. One
        // second of offset is enough on the last second of the day.
        "262142-12-31 23:00:00-01",
        "262142-12-31 23:59:59-00:00:01",
        // Low end: an eastward offset moves the value below chrono's minimum,
        // January 1, 262144 BCE.
        "262144-01-01 00:00:00+01 BC",
        "262144-01-01 00:00:00+00:00:01 BC",
    ] {
        assert_eq!(
            format!("{}", strconv::parse_timestamptz(s).unwrap_err()),
            format!("{s:?} is out of range for type timestamp with time zone"),
        );
    }

    // The opposite offset direction on the high boundary stays in range and is
    // still accepted, so the check is not simply rejecting the boundary day. The
    // low boundary has no such counterpart: `LOW_DATE` is 4713 BC, far above
    // chrono's minimum, so every value near that minimum is rejected either way.
    assert!(strconv::parse_timestamptz("262142-12-31 23:00:00+01").is_ok());
    assert!(strconv::parse_timestamptz("262142-12-31 23:59:59+00:00:01").is_ok());
}

#[mz_ore::test]
fn test_parse_timestamptz_leap_second_offset_fold() {
    // A parsed `:60` rolls over into the next minute before the offset is
    // applied, so every offset lands on a regular second.
    for (input, expected) in [
        ("1970-01-01 00:00:60+00:00:30", "1970-01-01 00:00:30+00"),
        ("1970-01-01 12:00:60-00:00:30", "1970-01-01 12:01:30+00"),
        ("1970-01-01 00:00:60+01", "1969-12-31 23:01:00+00"),
    ] {
        let ts = strconv::parse_timestamptz(input).unwrap();
        assert_eq!(ts.nanosecond(), 0, "leap-second nanos survived parsing");
        let mut buf = String::new();
        strconv::format_timestamptz(&mut buf, &ts);
        assert_eq!(buf, expected);
    }

    // The frozen legacy parse keeps chrono's leap-second representation
    // (sub-second >= 1s), which is only representable at a second-of-minute of
    // 59. An offset that is not a whole number of minutes shifts it off `:59`,
    // and the resulting value used to panic in `Row` encoding. Fold it into the
    // next regular second instead, which is also what PostgreSQL does with
    // `:60`. The folded values render the same as the rolled-over ones above.
    for (input, expected) in [
        ("1970-01-01 00:00:60+00:00:30", "1970-01-01 00:00:30+00"),
        ("1970-01-01 12:00:60-00:00:30", "1970-01-01 12:01:30+00"),
    ] {
        let ts = strconv::parse_timestamptz_legacy(input).unwrap();
        assert_eq!(ts.nanosecond(), 0, "leap-second nanos survived the fold");
        let mut buf = String::new();
        strconv::format_timestamptz(&mut buf, &ts);
        assert_eq!(buf, expected);
    }

    // A whole-minute offset keeps the legacy value on `:59`, where the
    // leap-second representation is legal, so it is preserved rather than
    // folded.
    for input in ["1970-01-01 00:00:60+01", "1970-01-01 12:00:60-05:30"] {
        let ts = strconv::parse_timestamptz_legacy(input).unwrap();
        assert_eq!(ts.nanosecond(), 1_000_000_000);
    }
}

#[mz_ore::test]
fn test_parse_interval_monthlike() {
    run_test_parse_interval_monthlike(
        "2 year",
        Interval {
            months: 24,
            ..Default::default()
        },
    );
    run_test_parse_interval_monthlike(
        "3-",
        Interval {
            months: 36,
            ..Default::default()
        },
    );
    run_test_parse_interval_monthlike(
        "2 year 2 months",
        Interval {
            months: 26,
            ..Default::default()
        },
    );
    run_test_parse_interval_monthlike(
        "3-3",
        Interval {
            months: 39,
            ..Default::default()
        },
    );

    fn run_test_parse_interval_monthlike(s: &str, expected: Interval) {
        let actual = strconv::parse_interval(s).unwrap();
        assert_eq!(actual, expected);
    }
}

#[mz_ore::test]
fn test_parse_interval_durationlike() {
    use DateTimeField::*;

    run_test_parse_interval_durationlike("10", Interval::new(0, 0, 10 * 1_000_000));

    run_test_parse_interval_durationlike_from_sql("10", Day, Interval::new(0, 10, 0));

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Hour,
        Interval::new(0, 0, 10 * 60 * 60 * 1_000_000),
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Minute,
        Interval::new(0, 0, 10 * 60 * 1_000_000),
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Second,
        Interval::new(0, 0, 10 * 1_000_000),
    );

    run_test_parse_interval_durationlike("0.01", Interval::new(0, 0, 10_000));

    run_test_parse_interval_durationlike(
        "1 2:3:4.5",
        Interval::new(
            0,
            1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        ),
    );

    run_test_parse_interval_durationlike(
        "-1 2:3:4.5",
        Interval::new(
            0,
            -1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        ),
    );

    fn run_test_parse_interval_durationlike(s: &str, expected: Interval) {
        let actual = strconv::parse_interval(s).unwrap();
        assert_eq!(actual, expected);
    }
    fn run_test_parse_interval_durationlike_from_sql(
        s: &str,
        d: DateTimeField,
        expected: Interval,
    ) {
        let actual = strconv::parse_interval_w_disambiguator(s, None, d).unwrap();
        assert_eq!(actual, expected);
    }
}

#[mz_ore::test]
fn test_parse_interval_full() {
    use DateTimeField::*;

    run_test_parse_interval_full(
        "6-7 1 2:3:4.5",
        Interval::new(
            79,
            1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        ),
    );

    run_test_parse_interval_full(
        "-6-7 1 2:3:4.5",
        Interval::new(
            -79,
            1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        ),
    );

    run_test_parse_interval_full(
        "6-7 -1 -2:3:4.5",
        Interval::new(
            79,
            -1,
            (-2 * 60 * 60 * 1_000_000) + (-3 * 60 * 1_000_000) + (-4 * 1_000_000) + -500_000,
        ),
    );

    run_test_parse_interval_full(
        "-6-7 -1 -2:3:4.5",
        Interval::new(
            -79,
            -1,
            (-2 * 60 * 60 * 1_000_000) + (-3 * 60 * 1_000_000) + (-4 * 1_000_000) + -500_000,
        ),
    );

    run_test_parse_interval_full(
        "-6-7 1 -2:3:4.5",
        Interval::new(
            -79,
            1,
            (-2 * 60 * 60 * 1_000_000) + (-3 * 60 * 1_000_000) + (-4 * 1_000_000) + -500_000,
        ),
    );

    run_test_parse_interval_full(
        "-6-7 -1 2:3:4.5",
        Interval::new(
            -79,
            -1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        ),
    );

    run_test_parse_interval_full_from_sql(
        "-6-7 1",
        Minute,
        Interval::new(-79, 0, 1 * 60 * 1_000_000),
    );

    fn run_test_parse_interval_full(s: &str, expected: Interval) {
        let actual = strconv::parse_interval(s).unwrap();
        assert_eq!(actual, expected);
    }
    fn run_test_parse_interval_full_from_sql(s: &str, d: DateTimeField, expected: Interval) {
        let actual = strconv::parse_interval_w_disambiguator(s, None, d).unwrap();
        assert_eq!(actual, expected);
    }
}

#[mz_ore::test]
fn parse_interval_error() {
    fn run_test_parse_interval_errors(s: &str, e: &str) {
        assert_eq!(
            e.to_string(),
            format!("{}", strconv::parse_interval(s).unwrap_err())
        );
    }

    run_test_parse_interval_errors(
        "1 1-1",
        "invalid input syntax for type interval: Cannot determine format of all parts. Add explicit time \
         components, e.g. INTERVAL '1 day' or INTERVAL '1' DAY: \"1 1-1\"",
    );
}

#[mz_ore::test]
fn miri_test_format_list() {
    let list = vec![
        Some("a"),
        Some("a\"b"),
        Some(""),
        None,
        Some("NULL"),
        Some("nUlL"),
        Some("  spaces "),
        Some("a,b"),
        Some("\\"),
        Some("a\\b\"c\\d\""),
    ];
    let mut out = String::new();
    strconv::format_list(&mut out, &list, |lw, el| match el {
        None => Ok::<_, ()>(lw.write_null()),
        Some(el) => Ok(strconv::format_string(lw.nonnull_buffer(), el)),
    })
    .unwrap();
    assert_eq!(
        out,
        r#"{a,"a\"b","",NULL,"NULL","nUlL","  spaces ","a,b","\\","a\\b\"c\\d\""}"#
    );
}

#[mz_ore::test]
fn test_format_date() {
    run_test_format_date(NaiveDate::from_ymd_opt(20000, 2, 3).unwrap(), "20000-02-03");
    run_test_format_date(NaiveDate::from_ymd_opt(2000, 2, 3).unwrap(), "2000-02-03");
    run_test_format_date(NaiveDate::from_ymd_opt(200, 2, 3).unwrap(), "0200-02-03");
    run_test_format_date(NaiveDate::from_ymd_opt(20, 2, 3).unwrap(), "0020-02-03");
    run_test_format_date(NaiveDate::from_ymd_opt(2, 2, 3).unwrap(), "0002-02-03");
    run_test_format_date(NaiveDate::from_ymd_opt(0, 2, 3).unwrap(), "0001-02-03 BC");
    run_test_format_date(NaiveDate::from_ymd_opt(-1, 2, 3).unwrap(), "0002-02-03 BC");
    run_test_format_date(NaiveDate::from_ymd_opt(-19, 2, 3).unwrap(), "0020-02-03 BC");
    run_test_format_date(
        NaiveDate::from_ymd_opt(-199, 2, 3).unwrap(),
        "0200-02-03 BC",
    );
    run_test_format_date(
        NaiveDate::from_ymd_opt(-1999, 2, 3).unwrap(),
        "2000-02-03 BC",
    );

    fn run_test_format_date(n: NaiveDate, e: &str) {
        let mut buf = String::new();
        strconv::format_date(&mut buf, Date::try_from(n).unwrap());
        assert_eq!(buf, e);
    }
}

#[mz_ore::test]
fn test_format_timestamp() {
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(20000, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "20000-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(2000, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "2000-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(2000, 2, 3)
            .unwrap()
            .and_hms_nano_opt(4, 5, 6, 789_000_000)
            .unwrap(),
        "2000-02-03 04:05:06.789",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(200, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "0200-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(200, 2, 3)
            .unwrap()
            .and_hms_nano_opt(4, 5, 6, 789_000_000)
            .unwrap(),
        "0200-02-03 04:05:06.789",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(20, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "0020-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(20, 2, 3)
            .unwrap()
            .and_hms_nano_opt(4, 5, 6, 789_000_000)
            .unwrap(),
        "0020-02-03 04:05:06.789",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(2, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "0002-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(2, 2, 3)
            .unwrap()
            .and_hms_nano_opt(4, 5, 6, 789_000_000)
            .unwrap(),
        "0002-02-03 04:05:06.789",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(0, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "0001-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(-1, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "0002-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(-19, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "0020-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(-19, 2, 3)
            .unwrap()
            .and_hms_nano_opt(4, 5, 6, 789_000_000)
            .unwrap(),
        "0020-02-03 04:05:06.789 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(-199, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "0200-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(-199, 2, 3)
            .unwrap()
            .and_hms_nano_opt(4, 5, 6, 789_000_000)
            .unwrap(),
        "0200-02-03 04:05:06.789 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(-1999, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
        "2000-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd_opt(-1999, 2, 3)
            .unwrap()
            .and_hms_nano_opt(4, 5, 6, 789_000_000)
            .unwrap(),
        "2000-02-03 04:05:06.789 BC",
    );

    fn run_test_format_timestamp(n: NaiveDateTime, e: &str) {
        let mut buf = String::new();
        strconv::format_timestamp(&mut buf, &n);
        assert_eq!(buf, e);
    }
}

#[mz_ore::test]
fn test_format_subsecond_carry() {
    // A sub-second fraction of `.9999995` or more rounds up to a full second.
    // The renderer writes microseconds, so that second has to reach the seconds
    // field. Written into the fraction instead it becomes `1000000` microseconds,
    // which the trailing-zero stripper reduces to `.1`, i.e. a value rendered
    // roughly one second early with a nonsense fraction.
    //
    // Nothing rounds on the way in: `pgrepr`'s text decoding (COPY, text-format
    // bind parameters) and the TIME cast both keep the parsed nanoseconds, so
    // these values reach the renderer as stored.
    for (input, expected) in [
        ("2020-01-01 00:00:00.9999999", "2020-01-01 00:00:01"),
        ("2020-01-01 00:00:00.9999995", "2020-01-01 00:00:01"),
        ("2020-01-01 00:00:00.9999994", "2020-01-01 00:00:00.999999"),
        // The carry crosses minute, day, month and year boundaries.
        ("2020-01-01 00:00:59.9999999", "2020-01-01 00:01:00"),
        ("2020-01-31 23:59:59.9999999", "2020-02-01 00:00:00"),
        ("2020-12-31 23:59:59.9999999", "2021-01-01 00:00:00"),
        // A `:60` rolls over into the next minute at parse, crossing the day
        // boundary here. The renderer's own leap-second handling is covered
        // below by constructing the value directly.
        ("2020-01-01 23:59:60", "2020-01-02 00:00:00"),
        // `HIGH_DATE` is exactly `chrono::NaiveDate::MAX`, so the carry has
        // nowhere to go and the fraction saturates instead.
        (
            "262142-12-31 23:59:59.9999999",
            "262142-12-31 23:59:59.999999",
        ),
    ] {
        let ts = strconv::parse_timestamp(input).unwrap();
        let mut buf = String::new();
        strconv::format_timestamp(&mut buf, &ts);
        assert_eq!(buf, expected, "formatting {input}");

        // TIMESTAMPTZ shares the renderer, so it carries identically.
        let tz = strconv::parse_timestamptz(input).unwrap();
        let mut buf = String::new();
        strconv::format_timestamptz(&mut buf, &tz);
        assert_eq!(buf, format!("{expected}+00"), "formatting {input} as tz");
    }

    // The renderer takes a bare `NaiveDateTime`, so it also has to hold up on
    // chrono's leap-second representation, which the SQL parser no longer
    // produces but persisted data and the frozen storage source casts still
    // do. A whole-second leap renders as `:60` via chrono's `%S`; a fractional
    // one carries, and the second after `23:59:60` is `00:00:00` of the next
    // minute.
    for (nanos, expected) in [
        (1_000_000_000, "2020-01-01 23:59:60"),
        (1_500_000_000, "2020-01-01 23:59:60.5"),
        (1_999_999_999, "2020-01-02 00:00:00"),
    ] {
        let ts = NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .and_hms_opt(23, 59, 59)
            .unwrap()
            .with_nanosecond(nanos)
            .unwrap();
        let mut buf = String::new();
        strconv::format_timestamp(&mut buf, &ts);
        assert_eq!(buf, expected, "formatting {nanos}ns past 23:59:59");
    }

    for (input, expected) in [
        ("12:34:56.9999999", "12:34:57"),
        ("12:34:56.9999994", "12:34:56.999999"),
        ("23:59:60", "23:59:60"),
        // A `NaiveTime` wraps to midnight rather than reaching PostgreSQL's
        // `24:00:00`, so the carry out of the last second of the day is dropped
        // and the fraction saturates.
        ("23:59:59.9999999", "23:59:59.999999"),
    ] {
        let t = strconv::parse_time(input).unwrap();
        let mut buf = String::new();
        strconv::format_time(&mut buf, t);
        assert_eq!(buf, expected, "formatting {input}");
    }
}

#[mz_ore::test]
fn test_format_timestamptz() {
    run_test_format_timestamptz(
        datetime_utc(20000, 2, 3, 4, 5, 6, 0),
        "20000-02-03 04:05:06+00",
    );
    run_test_format_timestamptz(
        datetime_utc(2000, 2, 3, 4, 5, 6, 0),
        "2000-02-03 04:05:06+00",
    );
    run_test_format_timestamptz(
        datetime_utc(2000, 2, 3, 4, 5, 6, 789_000_000),
        "2000-02-03 04:05:06.789+00",
    );
    run_test_format_timestamptz(
        datetime_utc(200, 2, 3, 4, 5, 6, 0),
        "0200-02-03 04:05:06+00",
    );
    run_test_format_timestamptz(
        datetime_utc(200, 2, 3, 4, 5, 6, 789_000_000),
        "0200-02-03 04:05:06.789+00",
    );
    run_test_format_timestamptz(datetime_utc(20, 2, 3, 4, 5, 6, 0), "0020-02-03 04:05:06+00");
    run_test_format_timestamptz(
        datetime_utc(20, 2, 3, 4, 5, 6, 789_000_000),
        "0020-02-03 04:05:06.789+00",
    );
    run_test_format_timestamptz(datetime_utc(2, 2, 3, 4, 5, 6, 0), "0002-02-03 04:05:06+00");
    run_test_format_timestamptz(
        datetime_utc(2, 2, 3, 4, 5, 6, 789_000_000),
        "0002-02-03 04:05:06.789+00",
    );
    run_test_format_timestamptz(
        datetime_utc(0, 2, 3, 4, 5, 6, 0),
        "0001-02-03 04:05:06+00 BC",
    );
    run_test_format_timestamptz(
        datetime_utc(-1, 2, 3, 4, 5, 6, 0),
        "0002-02-03 04:05:06+00 BC",
    );
    run_test_format_timestamptz(
        datetime_utc(-19, 2, 3, 4, 5, 6, 0),
        "0020-02-03 04:05:06+00 BC",
    );
    run_test_format_timestamptz(
        datetime_utc(-19, 2, 3, 4, 5, 6, 789_000_000),
        "0020-02-03 04:05:06.789+00 BC",
    );
    run_test_format_timestamptz(
        datetime_utc(-199, 2, 3, 4, 5, 6, 0),
        "0200-02-03 04:05:06+00 BC",
    );
    run_test_format_timestamptz(
        datetime_utc(-199, 2, 3, 4, 5, 6, 789_000_000),
        "0200-02-03 04:05:06.789+00 BC",
    );
    run_test_format_timestamptz(
        datetime_utc(-1999, 2, 3, 4, 5, 6, 0),
        "2000-02-03 04:05:06+00 BC",
    );
    run_test_format_timestamptz(
        datetime_utc(-1999, 2, 3, 4, 5, 6, 789_000_000),
        "2000-02-03 04:05:06.789+00 BC",
    );

    fn datetime_utc(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        nano: u32,
    ) -> DateTime<Utc> {
        Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(year, month, day)
                .unwrap()
                .and_hms_nano_opt(hour, min, sec, nano)
                .unwrap(),
        )
    }

    fn run_test_format_timestamptz(n: DateTime<Utc>, e: &str) {
        let mut buf = String::new();
        strconv::format_timestamptz(&mut buf, &n);
        assert_eq!(buf, e);
    }
}
