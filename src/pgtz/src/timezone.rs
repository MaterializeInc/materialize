// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::fmt;

use chrono::FixedOffset;
use chrono_tz::Tz;
use mz_lowertest::MzReflect;
use mz_proto::chrono::{any_fixed_offset, any_timezone};
use mz_proto::{RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use uncased::UncasedStr;

use crate::abbrev::TIMEZONE_ABBREVS;

include!(concat!(env!("OUT_DIR"), "/mz_pgtz.timezone.rs"));

/// The SQL definition of the contents of the `mz_timezone_names` view.
pub const MZ_CATALOG_TIMEZONE_NAMES_SQL: &str =
    include_str!(concat!(env!("OUT_DIR"), "/timezone.gen.sql"));

/// Parsed timezone.
#[derive(Arbitrary, Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, MzReflect)]
pub enum Timezone {
    #[serde(with = "fixed_offset_serde")]
    FixedOffset(#[proptest(strategy = "any_fixed_offset()")] FixedOffset),
    Tz(#[proptest(strategy = "any_timezone()")] Tz),
}

impl Timezone {
    pub fn parse(tz: &str, spec: TimezoneSpec) -> Result<Self, String> {
        build_timezone_offset_second(&tokenize_timezone(tz)?, tz, spec)
    }
}

impl RustType<ProtoTimezone> for Timezone {
    fn into_proto(&self) -> ProtoTimezone {
        use proto_timezone::Kind;
        ProtoTimezone {
            kind: Some(match self {
                Timezone::FixedOffset(fo) => Kind::FixedOffset(fo.into_proto()),
                Timezone::Tz(tz) => Kind::Tz(tz.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoTimezone) -> Result<Self, TryFromProtoError> {
        use proto_timezone::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoTimezone::kind"))?;
        Ok(match kind {
            Kind::FixedOffset(pof) => Timezone::FixedOffset(FixedOffset::from_proto(pof)?),
            Kind::Tz(ptz) => Timezone::Tz(Tz::from_proto(ptz)?),
        })
    }
}

// We need to implement Serialize and Deserialize traits to include Timezone in the UnaryFunc enum.
// FixedOffset doesn't implement these, even with the "serde" feature enabled.
mod fixed_offset_serde {
    use serde::de::Error;
    use serde::{Deserializer, Serializer};

    use super::*;

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<FixedOffset, D::Error> {
        let offset = i32::deserialize(deserializer)?;
        FixedOffset::east_opt(offset).ok_or_else(|| {
            Error::custom(format!("Invalid timezone offset: |{}| >= 86_400", offset))
        })
    }

    pub fn serialize<S: Serializer>(
        offset: &FixedOffset,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_i32(offset.local_minus_utc())
    }
}

impl PartialOrd for Timezone {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// We need to implement Ord and PartialOrd to include Timezone in the UnaryFunc enum. Neither FixedOffset nor Tz
// implement these so we do a simple ordinal comparison (FixedOffset variant < Tz variant), and break ties using
// i32/str comparisons respectively.
impl Ord for Timezone {
    fn cmp(&self, other: &Self) -> Ordering {
        use Timezone::*;
        match (self, other) {
            (FixedOffset(a), FixedOffset(b)) => a.local_minus_utc().cmp(&b.local_minus_utc()),
            (Tz(a), Tz(b)) => a.name().cmp(b.name()),
            (FixedOffset(_), Tz(_)) => Ordering::Less,
            (Tz(_), FixedOffset(_)) => Ordering::Greater,
        }
    }
}

impl Default for Timezone {
    fn default() -> Self {
        Self::FixedOffset(FixedOffset::east_opt(0).unwrap())
    }
}

impl fmt::Display for Timezone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Timezone::FixedOffset(offset) => offset.fmt(f),
            Timezone::Tz(tz) => tz.fmt(f),
        }
    }
}

/// TimeStrToken represents valid tokens in time-like strings,
/// i.e those used in INTERVAL, TIMESTAMP/TZ, DATE, and TIME.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TimeStrToken {
    Dash,
    Colon,
    Plus,
    Zulu,
    Num(u64, usize),
    TzName(String),
    Delim,
}

impl std::fmt::Display for TimeStrToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TimeStrToken::*;
        match self {
            Dash => write!(f, "-"),
            Colon => write!(f, ":"),
            Plus => write!(f, "+"),
            Zulu => write!(f, "Z"),
            Num(i, digits) => write!(f, "{:01$}", i, digits - 1),
            TzName(n) => write!(f, "{}", n),
            Delim => write!(f, " "),
        }
    }
}

fn tokenize_timezone(value: &str) -> Result<Vec<TimeStrToken>, String> {
    let mut toks: Vec<TimeStrToken> = vec![];
    let mut num_buf = String::with_capacity(4);
    // If the timezone string has a colon, we need to parse all numbers naively.
    // Otherwise we need to parse long sequences of digits as [..hhhhmm]
    let split_nums: bool = !value.contains(':');

    let value = value.trim_matches(|c: char| {
        (c.is_ascii_whitespace() || c.is_ascii_punctuation()) && (c != '+' && c != '-')
    });

    // Takes a string and tries to parse it as a number token and insert it into
    // the token list
    fn parse_num(
        toks: &mut Vec<TimeStrToken>,
        n: &str,
        split_nums: bool,
        idx: usize,
    ) -> Result<(), String> {
        if n.is_empty() {
            return Ok(());
        }

        let (first, second) = if n.len() > 2 && split_nums {
            let (first, second) = n.split_at(n.len() - 2);
            (first, Some(second))
        } else {
            (n, None)
        };

        toks.push(TimeStrToken::Num(
            first.parse().map_err(|e| {
                format!(
                    "Unable to tokenize value {} as a number at index {}: {}",
                    first, idx, e
                )
            })?,
            first.len(),
        ));

        if let Some(second) = second {
            toks.push(TimeStrToken::Num(
                second.parse().map_err(|e| {
                    format!(
                        "Unable to tokenize value {} as a number at index {}: {}",
                        second, idx, e
                    )
                })?,
                second.len(),
            ));
        }

        Ok(())
    }

    // Toggles whether or not we should skip whitespace. This would be nicer to
    // do inline but ownership makes that annoying.
    let mut space_skip_mode = false;
    for (i, chr) in value.chars().enumerate() {
        // Stay in space skip mode iff already in it and element is space.
        if space_skip_mode && chr.is_ascii_whitespace() {
            continue;
        } else {
            space_skip_mode = false;
        }

        match chr {
            ':' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Colon);
            }
            '-' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Dash);
                space_skip_mode = true;
            }
            '+' => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Plus);
                space_skip_mode = true;
            }
            chr if (chr == 'z' || chr == 'Z') && (i == value.len() - 1) => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Zulu);
            }
            chr if chr.is_digit(10) => num_buf.push(chr),
            chr if chr.is_ascii_alphabetic() => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                let substring = &value[i..];
                toks.push(TimeStrToken::TzName(substring.to_string()));
                return Ok(toks);
            }
            // PG allows arbitrary punctuation marks, which represent delim
            chr if chr.is_ascii_whitespace() || chr.is_ascii_punctuation() => {
                parse_num(&mut toks, &num_buf, split_nums, i)?;
                num_buf.clear();
                toks.push(TimeStrToken::Delim);
            }
            chr => {
                return Err(format!(
                    "Error tokenizing timezone string ('{}'): invalid character {:?} at offset {}",
                    value, chr, i
                ))
            }
        }
    }
    parse_num(&mut toks, &num_buf, split_nums, 0)?;
    Ok(toks)
}

#[derive(Debug, Clone, Copy)]
pub enum TimezoneSpec {
    /// Offsets should be treated as an ISO 8601 time zone specification.
    Iso,
    /// Offsets should be treated as a POSIX-style time zone specification.
    Posix,
}

fn build_timezone_offset_second(
    tokens: &[TimeStrToken],
    value: &str,
    spec: TimezoneSpec,
) -> Result<Timezone, String> {
    use TimeStrToken::*;
    let all_formats = [
        vec![Plus, Num(0, 1), Colon, Num(0, 1), Colon, Num(0, 1)],
        vec![Dash, Num(0, 1), Colon, Num(0, 1), Colon, Num(0, 1)],
        vec![Plus, Num(0, 1), Colon, Num(0, 1)],
        vec![Dash, Num(0, 1), Colon, Num(0, 1)],
        vec![Plus, Num(0, 1), Num(0, 1), Num(0, 1)],
        vec![Dash, Num(0, 1), Num(0, 1), Num(0, 1)],
        vec![Plus, Num(0, 1), Num(0, 1)],
        vec![Dash, Num(0, 1), Num(0, 1)],
        vec![Plus, Num(0, 1)],
        vec![Dash, Num(0, 1)],
        vec![TzName("".to_string())],
        vec![Zulu],
    ];

    let mut is_positive = true;
    let mut hour_offset: Option<i32> = None;
    let mut minute_offset: Option<i32> = None;
    let mut second_offset: Option<i32> = None;

    for format in all_formats.iter() {
        let actual = tokens.iter();

        if actual.len() != format.len() {
            continue;
        }

        for (i, (atok, etok)) in actual.zip(format).enumerate() {
            match (atok, etok) {
                (Colon, Colon) | (Plus, Plus) => { /* Matching punctuation */ }
                (Dash, Dash) => {
                    is_positive = false;
                }
                (Num(val, _), Num(_, _)) => {
                    let val = *val;
                    match (hour_offset, minute_offset, second_offset) {
                        (None, None, None) => {
                            // Postgres allows timezones in the range -15:59:59..15:59:59
                            if val <= 15 {
                                hour_offset = Some(i32::try_from(val).expect(
                                    "number between 0 and 15 should fit in signed 32-bit integer",
                                ));
                            } else {
                                return Err(format!(
                                    "Invalid timezone string ({}): timezone hour invalid {}",
                                    value, val
                                ));
                            }
                        }
                        (Some(_), None, None) => {
                            if val < 60 {
                                minute_offset = Some(i32::try_from(val).expect(
                                    "number between 0 and 59 should fit in signed 32-bit integer",
                                ));
                            } else {
                                return Err(format!(
                                    "Invalid timezone string ({}): timezone minute invalid {}",
                                    value, val
                                ));
                            }
                        }
                        (Some(_), Some(_), None) => {
                            if val < 60 {
                                second_offset = Some(i32::try_from(val).expect(
                                    "number between 0 and 59 should fit in signed 32-bit integer",
                                ));
                            } else {
                                return Err(format!(
                                    "Invalid timezone string ({}): timezone second invalid {}",
                                    value, val
                                ));
                            }
                        }
                        // We've already seen an hour a minute and a second so we should
                        // never see another number
                        (Some(_), Some(_), Some(_)) => {
                            return Err(format!(
                                "Invalid timezone string ({}): invalid value {} at token index {}",
                                value, val, i
                            ))
                        }
                        _ => unreachable!("parsed a minute before an hour!"),
                    }
                }
                (Zulu, Zulu) => return Ok(Default::default()),
                (TzName(val), TzName(_)) => {
                    if let Some(abbrev) = TIMEZONE_ABBREVS.get(UncasedStr::new(val)) {
                        return Ok(abbrev.timezone());
                    }

                    return match Tz::from_str_insensitive(val) {
                        Ok(tz) => Ok(Timezone::Tz(tz)),
                        Err(err) => Err(format!(
                            "Invalid timezone string ({}): {}. \
                            Failed to parse {} at token index {}",
                            value, err, val, i
                        )),
                    };
                }
                (_, _) => {
                    // Theres a mismatch between this format and the actual
                    // token stream Stop trying to parse in this format and go
                    // to the next one
                    is_positive = true;
                    hour_offset = None;
                    minute_offset = None;
                    second_offset = None;
                    break;
                }
            }
        }

        // Return the first valid parsed result
        if let Some(hour_offset) = hour_offset {
            let mut tz_offset_second = hour_offset * 60 * 60;

            if let Some(minute_offset) = minute_offset {
                tz_offset_second += minute_offset * 60;
            }

            if let Some(second_offset) = second_offset {
                tz_offset_second += second_offset;
            }

            let offset = match (is_positive, spec) {
                (true, TimezoneSpec::Iso) | (false, TimezoneSpec::Posix) => {
                    FixedOffset::east_opt(tz_offset_second).unwrap()
                }
                (false, TimezoneSpec::Iso) | (true, TimezoneSpec::Posix) => {
                    FixedOffset::west_opt(tz_offset_second).unwrap()
                }
            };

            return Ok(Timezone::FixedOffset(offset));
        }
    }

    Err(format!("Cannot parse timezone offset {}", value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_parse_timezone_offset_second() {
        use Timezone::{FixedOffset as F, Tz as T};
        let test_cases = [
            ("+0:00", F(FixedOffset::east_opt(0).unwrap())),
            ("-0:00", F(FixedOffset::east_opt(0).unwrap())),
            ("+0:000000", F(FixedOffset::east_opt(0).unwrap())),
            ("+000000:00", F(FixedOffset::east_opt(0).unwrap())),
            ("+000000:000000", F(FixedOffset::east_opt(0).unwrap())),
            ("+0", F(FixedOffset::east_opt(0).unwrap())),
            ("+00", F(FixedOffset::east_opt(0).unwrap())),
            ("+000", F(FixedOffset::east_opt(0).unwrap())),
            ("+0000", F(FixedOffset::east_opt(0).unwrap())),
            ("+00000000", F(FixedOffset::east_opt(0).unwrap())),
            ("+0000001:000000", F(FixedOffset::east_opt(3600).unwrap())),
            ("+0000000:000001", F(FixedOffset::east_opt(60).unwrap())),
            ("+0000001:000001", F(FixedOffset::east_opt(3660).unwrap())),
            (
                "+0000001:000001:000001",
                F(FixedOffset::east_opt(3661).unwrap()),
            ),
            ("+4:00", F(FixedOffset::east_opt(14400).unwrap())),
            ("-4:00", F(FixedOffset::west_opt(14400).unwrap())),
            ("+2:30", F(FixedOffset::east_opt(9000).unwrap())),
            ("-5:15", F(FixedOffset::west_opt(18900).unwrap())),
            ("+0:20", F(FixedOffset::east_opt(1200).unwrap())),
            ("-0:20", F(FixedOffset::west_opt(1200).unwrap())),
            ("+0:0:20", F(FixedOffset::east_opt(20).unwrap())),
            ("+5", F(FixedOffset::east_opt(18000).unwrap())),
            ("-5", F(FixedOffset::west_opt(18000).unwrap())),
            ("+05", F(FixedOffset::east_opt(18000).unwrap())),
            ("-05", F(FixedOffset::west_opt(18000).unwrap())),
            ("+500", F(FixedOffset::east_opt(18000).unwrap())),
            ("-500", F(FixedOffset::west_opt(18000).unwrap())),
            ("+530", F(FixedOffset::east_opt(19800).unwrap())),
            ("-530", F(FixedOffset::west_opt(19800).unwrap())),
            ("+050", F(FixedOffset::east_opt(3000).unwrap())),
            ("-050", F(FixedOffset::west_opt(3000).unwrap())),
            ("+15", F(FixedOffset::east_opt(54000).unwrap())),
            ("-15", F(FixedOffset::west_opt(54000).unwrap())),
            ("+1515", F(FixedOffset::east_opt(54900).unwrap())),
            ("+15:15:15", F(FixedOffset::east_opt(54915).unwrap())),
            ("+015", F(FixedOffset::east_opt(900).unwrap())),
            ("-015", F(FixedOffset::west_opt(900).unwrap())),
            ("+0015", F(FixedOffset::east_opt(900).unwrap())),
            ("-0015", F(FixedOffset::west_opt(900).unwrap())),
            ("+00015", F(FixedOffset::east_opt(900).unwrap())),
            ("-00015", F(FixedOffset::west_opt(900).unwrap())),
            ("+005", F(FixedOffset::east_opt(300).unwrap())),
            ("-005", F(FixedOffset::west_opt(300).unwrap())),
            ("+0000005", F(FixedOffset::east_opt(300).unwrap())),
            ("+00000100", F(FixedOffset::east_opt(3600).unwrap())),
            ("Z", F(FixedOffset::east_opt(0).unwrap())),
            ("z", F(FixedOffset::east_opt(0).unwrap())),
            ("UTC", F(FixedOffset::east_opt(0).unwrap())),
            ("Pacific/Auckland", T(Tz::Pacific__Auckland)),
            ("America/New_York", T(Tz::America__New_York)),
            ("America/Los_Angeles", T(Tz::America__Los_Angeles)),
            ("utc", F(FixedOffset::east_opt(0).unwrap())),
            ("pAcIfIc/AUcKlAnD", T(Tz::Pacific__Auckland)),
            ("AMERICA/NEW_YORK", T(Tz::America__New_York)),
            ("america/los_angeles", T(Tz::America__Los_Angeles)),
            // Formatting test cases
            ("+5:", F(FixedOffset::east_opt(18000).unwrap())),
            ("-5:15:", F(FixedOffset::west_opt(18900).unwrap())),
            ("-   5:15:", F(FixedOffset::west_opt(18900).unwrap())),
            (
                " ! ? ! - 5:15 ? ! ? ",
                F(FixedOffset::west_opt(18900).unwrap()),
            ),
            (" UTC", F(FixedOffset::east_opt(0).unwrap())),
            (" UTC ", F(FixedOffset::east_opt(0).unwrap())),
            (" ? UTC ! ", F(FixedOffset::east_opt(0).unwrap())),
        ];

        for (timezone, expected) in test_cases.iter() {
            match Timezone::parse(timezone, TimezoneSpec::Iso) {
                Ok(tz) => assert_eq!(&tz, expected),
                Err(e) => panic!(
                    "Test failed when expected to pass test case: {} error: {}",
                    timezone, e
                ),
            }
        }

        let failure_test_cases = [
            "+25:00", "+120:00", "+0:61", "+0:500", " 12:30", "+-12:30", "+2525", "+2561",
            "+255900", "+25", "+5::30", "++5:00", "--5:00", "a", "zzz", "ZZZ", "ZZ Top", " +",
            " -", " ", "1", "12", "1234", "+16", "-17", "-14:60", "1:30:60",
        ];

        for test in failure_test_cases.iter() {
            match Timezone::parse(test, TimezoneSpec::Iso) {
                Ok(t) => panic!("Test passed when expected to fail test case: {} parsed tz offset (seconds): {}", test, t),
                Err(e) => println!("{}", e),
            }
        }
    }
}
