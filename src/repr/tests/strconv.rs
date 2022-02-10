// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

use mz_repr::adt::datetime::DateTimeField;
use mz_repr::adt::interval::Interval;
use mz_repr::strconv;

#[test]
fn test_parse_date() {
    run_test_parse_date("000203", NaiveDate::from_ymd(2000, 2, 3));
    run_test_parse_date("690203", NaiveDate::from_ymd(2069, 2, 3));
    run_test_parse_date("700203", NaiveDate::from_ymd(1970, 2, 3));
    run_test_parse_date("010203", NaiveDate::from_ymd(2001, 2, 3));
    run_test_parse_date("0010203", NaiveDate::from_ymd(1, 2, 3));
    run_test_parse_date("00010203", NaiveDate::from_ymd(1, 2, 3));
    run_test_parse_date("20010203", NaiveDate::from_ymd(2001, 2, 3));
    run_test_parse_date("99990203", NaiveDate::from_ymd(9999, 2, 3));
    run_test_parse_date("2001-02-03", NaiveDate::from_ymd(2001, 2, 3));
    run_test_parse_date("2001 02 03", NaiveDate::from_ymd(2001, 2, 3));
    run_test_parse_date("2001-02-03 04:05:06.789", NaiveDate::from_ymd(2001, 2, 3));
    fn run_test_parse_date(s: &str, n: NaiveDate) {
        assert_eq!(strconv::parse_date(s).unwrap(), n);
    }
}

#[test]
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

#[test]
fn test_parse_time() {
    run_test_parse_time(
        "01:02:03.456",
        NaiveTime::from_hms_nano(1, 2, 3, 456_000_000),
    );
    run_test_parse_time("01:02:03", NaiveTime::from_hms(1, 2, 3));
    run_test_parse_time("02:03.456", NaiveTime::from_hms_nano(0, 2, 3, 456_000_000));
    run_test_parse_time("01:02", NaiveTime::from_hms(1, 2, 0));

    // Regression for #6272.
    run_test_parse_time("9::60", NaiveTime::from_hms_nano(9, 0, 59, 1_000_000_000));

    fn run_test_parse_time(s: &str, t: NaiveTime) {
        assert_eq!(strconv::parse_time(s).unwrap(), t);
    }
}

#[test]
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

    fn run_test_parse_time_errors(s: &str, e: &str) {
        assert_eq!(
            e.to_string(),
            format!("{}", strconv::parse_time(s).unwrap_err())
        );
    }
}

#[test]
fn test_parse_timestamp() {
    run_test_parse_timestamp(
        "2001-02-03 04:05:06.789",
        NaiveDate::from_ymd(2001, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
    );
    run_test_parse_timestamp(
        "2001-02-03",
        NaiveDate::from_ymd(2001, 2, 3).and_hms(0, 0, 0),
    );
    run_test_parse_timestamp(
        "2001-02-03 01:02:03",
        NaiveDate::from_ymd(2001, 2, 3).and_hms(1, 2, 3),
    );
    run_test_parse_timestamp(
        "2001-02-03 02:03.456",
        NaiveDate::from_ymd(2001, 2, 3).and_hms_nano(0, 2, 3, 456_000_000),
    );
    run_test_parse_timestamp(
        "2001-02-03 01:02",
        NaiveDate::from_ymd(2001, 2, 3).and_hms(1, 2, 0),
    );

    fn run_test_parse_timestamp(s: &str, ts: NaiveDateTime) {
        assert_eq!(strconv::parse_timestamp(s).unwrap(), ts);
    }
}

#[test]
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

#[test]
fn test_parse_timestamptz() {
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

        let expected = NaiveDate::from_ymd(test.1, test.2, test.3)
            .and_hms_nano(test.4, test.5, test.6, test.7);
        let offset = FixedOffset::east(test.8);
        let dt_fixed_offset = offset.from_local_datetime(&expected).earliest().unwrap();
        let expected = DateTime::<Utc>::from_utc(dt_fixed_offset.naive_utc(), Utc);

        assert_eq!(actual, expected);
    }
}

#[test]
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

#[test]
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

#[test]
fn test_parse_interval_durationlike() {
    use DateTimeField::*;

    run_test_parse_interval_durationlike("10", Interval::new(0, 0, 10 * 1_000_000).unwrap());

    run_test_parse_interval_durationlike_from_sql("10", Day, Interval::new(0, 10, 0).unwrap());

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Hour,
        Interval::new(0, 0, 10 * 60 * 60 * 1_000_000).unwrap(),
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Minute,
        Interval::new(0, 0, 10 * 60 * 1_000_000).unwrap(),
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Second,
        Interval::new(0, 0, 10 * 1_000_000).unwrap(),
    );

    run_test_parse_interval_durationlike("0.01", Interval::new(0, 0, 10_000).unwrap());

    run_test_parse_interval_durationlike(
        "1 2:3:4.5",
        Interval::new(
            0,
            1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        )
        .unwrap(),
    );

    run_test_parse_interval_durationlike(
        "-1 2:3:4.5",
        Interval::new(
            0,
            -1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        )
        .unwrap(),
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

#[test]
fn test_parse_interval_full() {
    use DateTimeField::*;

    run_test_parse_interval_full(
        "6-7 1 2:3:4.5",
        Interval::new(
            79,
            1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        )
        .unwrap(),
    );

    run_test_parse_interval_full(
        "-6-7 1 2:3:4.5",
        Interval::new(
            -79,
            1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        )
        .unwrap(),
    );

    run_test_parse_interval_full(
        "6-7 -1 -2:3:4.5",
        Interval::new(
            79,
            -1,
            (-2 * 60 * 60 * 1_000_000) + (-3 * 60 * 1_000_000) + (-4 * 1_000_000) + -500_000,
        )
        .unwrap(),
    );

    run_test_parse_interval_full(
        "-6-7 -1 -2:3:4.5",
        Interval::new(
            -79,
            -1,
            (-2 * 60 * 60 * 1_000_000) + (-3 * 60 * 1_000_000) + (-4 * 1_000_000) + -500_000,
        )
        .unwrap(),
    );

    run_test_parse_interval_full(
        "-6-7 1 -2:3:4.5",
        Interval::new(
            -79,
            1,
            (-2 * 60 * 60 * 1_000_000) + (-3 * 60 * 1_000_000) + (-4 * 1_000_000) + -500_000,
        )
        .unwrap(),
    );

    run_test_parse_interval_full(
        "-6-7 -1 2:3:4.5",
        Interval::new(
            -79,
            -1,
            (2 * 60 * 60 * 1_000_000) + (3 * 60 * 1_000_000) + (4 * 1_000_000) + 500_000,
        )
        .unwrap(),
    );

    run_test_parse_interval_full_from_sql(
        "-6-7 1",
        Minute,
        Interval::new(-79, 0, 1 * 60 * 1_000_000).unwrap(),
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

#[test]
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

#[test]
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
        None => lw.write_null(),
        Some(el) => strconv::format_string(lw.nonnull_buffer(), el),
    });
    assert_eq!(
        out,
        r#"{a,"a\"b","",NULL,"NULL",nUlL,"  spaces ","a,b","\\","a\\b\"c\\d\""}"#
    );
}

#[test]
fn test_format_date() {
    run_test_format_date(NaiveDate::from_ymd(20000, 2, 3), "20000-02-03");
    run_test_format_date(NaiveDate::from_ymd(2000, 2, 3), "2000-02-03");
    run_test_format_date(NaiveDate::from_ymd(200, 2, 3), "0200-02-03");
    run_test_format_date(NaiveDate::from_ymd(20, 2, 3), "0020-02-03");
    run_test_format_date(NaiveDate::from_ymd(2, 2, 3), "0002-02-03");
    run_test_format_date(NaiveDate::from_ymd(0, 2, 3), "0001-02-03 BC");
    run_test_format_date(NaiveDate::from_ymd(-1, 2, 3), "0002-02-03 BC");
    run_test_format_date(NaiveDate::from_ymd(-19, 2, 3), "0020-02-03 BC");
    run_test_format_date(NaiveDate::from_ymd(-199, 2, 3), "0200-02-03 BC");
    run_test_format_date(NaiveDate::from_ymd(-1999, 2, 3), "2000-02-03 BC");

    fn run_test_format_date(n: NaiveDate, e: &str) {
        let mut buf = String::new();
        strconv::format_date(&mut buf, n);
        assert_eq!(buf, e);
    }
}

#[test]
fn test_format_timestamp() {
    run_test_format_timestamp(
        NaiveDate::from_ymd(20000, 2, 3).and_hms(4, 5, 6),
        "20000-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(2000, 2, 3).and_hms(4, 5, 6),
        "2000-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(2000, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
        "2000-02-03 04:05:06.789",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(200, 2, 3).and_hms(4, 5, 6),
        "0200-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(200, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
        "0200-02-03 04:05:06.789",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(20, 2, 3).and_hms(4, 5, 6),
        "0020-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(20, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
        "0020-02-03 04:05:06.789",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(2, 2, 3).and_hms(4, 5, 6),
        "0002-02-03 04:05:06",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(2, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
        "0002-02-03 04:05:06.789",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(0, 2, 3).and_hms(4, 5, 6),
        "0001-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(-1, 2, 3).and_hms(4, 5, 6),
        "0002-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(-19, 2, 3).and_hms(4, 5, 6),
        "0020-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(-19, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
        "0020-02-03 04:05:06.789 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(-199, 2, 3).and_hms(4, 5, 6),
        "0200-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(-199, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
        "0200-02-03 04:05:06.789 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(-1999, 2, 3).and_hms(4, 5, 6),
        "2000-02-03 04:05:06 BC",
    );
    run_test_format_timestamp(
        NaiveDate::from_ymd(-1999, 2, 3).and_hms_nano(4, 5, 6, 789_000_000),
        "2000-02-03 04:05:06.789 BC",
    );

    fn run_test_format_timestamp(n: NaiveDateTime, e: &str) {
        let mut buf = String::new();
        strconv::format_timestamp(&mut buf, n);
        assert_eq!(buf, e);
    }
}

#[test]
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
        DateTime::from_utc(
            NaiveDate::from_ymd(year, month, day).and_hms_nano(hour, min, sec, nano),
            Utc,
        )
    }

    fn run_test_format_timestamptz(n: DateTime<Utc>, e: &str) {
        let mut buf = String::new();
        strconv::format_timestamptz(&mut buf, n);
        assert_eq!(buf, e);
    }
}
