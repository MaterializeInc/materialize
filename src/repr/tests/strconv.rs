// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

use repr::{strconv, Interval};

#[test]
fn test_parse_date() {
    run_test_parse_date("2001-02-03", NaiveDate::from_ymd(2001, 2, 3));
    run_test_parse_date("2001-02-03 04:05:06.789", NaiveDate::from_ymd(2001, 2, 3));
    fn run_test_parse_date(s: &str, n: NaiveDate) {
        assert_eq!(strconv::parse_date(s).unwrap(), n);
    }
}

#[test]
fn test_parse_date_errors() {
    run_test_parse_date_errors(
        "2001-01",
        "Invalid DATE \'2001-01\': YEAR, MONTH, DAY are all required",
    );
    run_test_parse_date_errors(
        "2001",
        "Invalid DATE \'2001\': YEAR, MONTH, DAY are all required",
    );
    run_test_parse_date_errors(
        "2001-13-01",
        "Invalid DATE \'2001-13-01\': MONTH must be (1, 12), got 13",
    );
    run_test_parse_date_errors(
        "2001-12-32",
        "Invalid DATE \'2001-12-32\': DAY must be (1, 31), got 32",
    );
    run_test_parse_date_errors(
        "2001-01-02 04",
        "Invalid DATE '2001-01-02 04': Unknown format",
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
    fn run_test_parse_time(s: &str, t: NaiveTime) {
        assert_eq!(strconv::parse_time(s).unwrap(), t);
    }
}

#[test]
fn test_parse_time_errors() {
    run_test_parse_time_errors(
        "26:01:02.345",
        "Invalid TIME \'26:01:02.345\': HOUR must be (0, 23), got 26",
    );
    run_test_parse_time_errors(
        "01:60:02.345",
        "Invalid TIME \'01:60:02.345\': MINUTE must be (0, 59), got 60",
    );
    run_test_parse_time_errors(
        "01:02:61.345",
        "Invalid TIME \'01:02:61.345\': SECOND must be (0, 60), got 61",
    );
    run_test_parse_time_errors("03.456", "Invalid TIME \'03.456\': Unknown format");

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
        "Invalid TIMESTAMP \'2001-01\': YEAR, MONTH, DAY are all required",
    );
    run_test_parse_timestamp_errors(
        "2001",
        "Invalid TIMESTAMP \'2001\': YEAR, MONTH, DAY are all required",
    );
    run_test_parse_timestamp_errors(
        "2001-13-01",
        "Invalid TIMESTAMP \'2001-13-01\': MONTH must be (1, 12), got 13",
    );
    run_test_parse_timestamp_errors(
        "2001-12-32",
        "Invalid TIMESTAMP \'2001-12-32\': DAY must be (1, 31), got 32",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 04",
        "Invalid TIMESTAMP \'2001-01-02 04\': Unknown format",
    );

    run_test_parse_timestamp_errors(
        "2001-01-02 26:01:02.345",
        "Invalid TIMESTAMP \'2001-01-02 26:01:02.345\': HOUR must be (0, 23), got 26",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 01:60:02.345",
        "Invalid TIMESTAMP \'2001-01-02 01:60:02.345\': MINUTE must be (0, 59), got 60",
    );
    run_test_parse_timestamp_errors(
        "2001-01-02 01:02:61.345",
        "Invalid TIMESTAMP \'2001-01-02 01:02:61.345\': SECOND must be (0, 60), got 61",
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
        "Invalid TIMESTAMPTZ \'1999-01-01 01:23:34.555 +25:45\': Invalid timezone string \
         (+25:45): timezone hour invalid 25",
    );
    run_test_parse_timestamptz_errors(
        "1999-01-01 01:23:34.555 +21:61",
        "Invalid TIMESTAMPTZ \'1999-01-01 01:23:34.555 +21:61\': Invalid timezone string \
         (+21:61): timezone minute invalid 61",
    );
    run_test_parse_timestamptz_errors(
        "1999-01-01 01:23:34.555 4",
        "Invalid TIMESTAMPTZ \'1999-01-01 01:23:34.555 4\': Cannot parse timezone offset 4",
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
    use sql_parser::ast::DateTimeField::*;
    use std::time::Duration;

    run_test_parse_interval_durationlike(
        "10",
        Interval {
            duration: Duration::new(10, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Day,
        Interval {
            duration: Duration::new(10 * 24 * 60 * 60, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Hour,
        Interval {
            duration: Duration::new(10 * 60 * 60, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Minute,
        Interval {
            duration: Duration::new(10 * 60, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike_from_sql(
        "10",
        Second,
        Interval {
            duration: Duration::new(10, 0),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "0.01",
        Interval {
            duration: Duration::new(0, 10_000_000),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "1 2:3:4.5",
        Interval {
            duration: Duration::new(93_784, 500_000_000),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "-1 2:3:4.5",
        Interval {
            duration: Duration::new(79_015, 500_000_000),
            is_positive_dur: false,
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "1 -2:3:4.5",
        Interval {
            duration: Duration::new(79_015, 500_000_000),
            ..Default::default()
        },
    );

    run_test_parse_interval_durationlike(
        "1 2:3",
        Interval {
            duration: Duration::new(93_780, 0),
            ..Default::default()
        },
    );
    fn run_test_parse_interval_durationlike(s: &str, expected: Interval) {
        let actual = strconv::parse_interval(s).unwrap();
        assert_eq!(actual, expected);
    }
    fn run_test_parse_interval_durationlike_from_sql(
        s: &str,
        d: sql_parser::ast::DateTimeField,
        expected: Interval,
    ) {
        let actual = strconv::parse_interval_w_disambiguator(s, d).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn test_parse_interval_full() {
    use sql_parser::ast::DateTimeField::*;
    use std::time::Duration;

    run_test_parse_interval_full(
        "6-7 1 2:3:4.5",
        Interval {
            months: 79,
            duration: Duration::new(93_784, 500_000_000),
            is_positive_dur: true,
        },
    );

    run_test_parse_interval_full(
        "-6-7 1 2:3:4.5",
        Interval {
            months: -79,
            duration: Duration::new(93_784, 500_000_000),
            is_positive_dur: true,
        },
    );

    run_test_parse_interval_full(
        "6-7 -1 -2:3:4.5",
        Interval {
            months: 79,
            duration: Duration::new(93_784, 500_000_000),
            is_positive_dur: false,
        },
    );

    run_test_parse_interval_full(
        "-6-7 -1 -2:3:4.5",
        Interval {
            months: -79,
            duration: Duration::new(93_784, 500_000_000),
            is_positive_dur: false,
        },
    );

    run_test_parse_interval_full(
        "-6-7 1 -2:3:4.5",
        Interval {
            months: -79,
            duration: Duration::new(79_015, 500_000_000),
            is_positive_dur: true,
        },
    );

    run_test_parse_interval_full(
        "-6-7 -1 2:3:4.5",
        Interval {
            months: -79,
            duration: Duration::new(79_015, 500_000_000),
            is_positive_dur: false,
        },
    );

    run_test_parse_interval_full_from_sql(
        "-6-7 1",
        Minute,
        Interval {
            months: -79,
            duration: Duration::new(60, 0),
            is_positive_dur: true,
        },
    );

    fn run_test_parse_interval_full(s: &str, expected: Interval) {
        let actual = strconv::parse_interval(s).unwrap();
        assert_eq!(actual, expected);
    }
    fn run_test_parse_interval_full_from_sql(
        s: &str,
        d: sql_parser::ast::DateTimeField,
        expected: Interval,
    ) {
        let actual = strconv::parse_interval_w_disambiguator(s, d).unwrap();
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
        "Invalid INTERVAL '1 1-1': Cannot determine format of all parts. Add explicit time \
         components, e.g. INTERVAL '1 day' or INTERVAL '1' DAY",
    );
}
