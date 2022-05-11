// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data type formatting functions.
//!
//! <https://www.postgresql.org/docs/current/functions-formatting.html>

#![allow(non_camel_case_types)]

use std::fmt;

use aho_corasick::AhoCorasickBuilder;
use enum_iterator::IntoEnumIterator;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::scalar::func::TimestampLike;

/// The raw tokens that can appear in a format string. Many of these tokens
/// overlap, in which case the longest matching token should be selected.
#[repr(u8)]
#[derive(Eq, PartialEq, TryFromPrimitive, IntoPrimitive, IntoEnumIterator)]
enum DateTimeToken {
    a_d,
    A_D,
    a_m,
    A_M,
    ad,
    AD,
    am,
    AM,
    b_c,
    B_C,
    bc,
    BC,
    cc,
    CC,
    d,
    D,
    day,
    Day,
    DAY,
    dd,
    DD,
    ddd,
    DDD,
    dy,
    Dy,
    DY,
    fm,
    FM,
    fx,
    FX,
    hh,
    HH,
    hh12,
    HH12,
    hh24,
    HH24,
    i,
    I,
    id,
    ID,
    iddd,
    IDDD,
    iw,
    IW,
    iy,
    IY,
    iyy,
    IYY,
    iyyy,
    IYYY,
    j,
    J,
    mi,
    MI,
    mm,
    MM,
    mon,
    Mon,
    MON,
    month,
    Month,
    MONTH,
    ms,
    MS,
    OF,
    p_m,
    P_M,
    pm,
    PM,
    q,
    Q,
    rm,
    RM,
    ss,
    SS,
    ssss,
    SSSS,
    sssss,
    SSSSS,
    tz,
    TZ,
    TZH,
    TZM,
    us,
    US,
    w,
    W,
    ww,
    WW,
    y_yyy,
    Y_YYY,
    y,
    Y,
    yy,
    YY,
    yyy,
    YYY,
    yyyy,
    YYYY,
    th,
    TH,
    EscQuote,
    Quote,
}

impl DateTimeToken {
    /// Returns the literal sequence of characters that this `DateTimeToken`
    /// matches.
    fn pattern(&self) -> &'static str {
        match self {
            DateTimeToken::AD => "AD",
            DateTimeToken::ad => "ad",
            DateTimeToken::A_D => "A.D.",
            DateTimeToken::a_d => "a.d.",
            DateTimeToken::AM => "AM",
            DateTimeToken::am => "am",
            DateTimeToken::A_M => "A.M.",
            DateTimeToken::a_m => "a.m.",
            DateTimeToken::BC => "BC",
            DateTimeToken::bc => "bc",
            DateTimeToken::B_C => "B.C.",
            DateTimeToken::b_c => "b.c.",
            DateTimeToken::CC => "CC",
            DateTimeToken::cc => "cc",
            DateTimeToken::D => "D",
            DateTimeToken::d => "d",
            DateTimeToken::DAY => "DAY",
            DateTimeToken::Day => "Day",
            DateTimeToken::day => "day",
            DateTimeToken::DD => "DD",
            DateTimeToken::dd => "dd",
            DateTimeToken::DDD => "DDD",
            DateTimeToken::ddd => "ddd",
            DateTimeToken::DY => "DY",
            DateTimeToken::Dy => "Dy",
            DateTimeToken::dy => "dy",
            DateTimeToken::FM => "FM",
            DateTimeToken::fm => "fm",
            DateTimeToken::FX => "FX",
            DateTimeToken::fx => "fx",
            DateTimeToken::HH => "HH",
            DateTimeToken::hh => "hh",
            DateTimeToken::HH12 => "HH12",
            DateTimeToken::hh12 => "hh12",
            DateTimeToken::HH24 => "HH24",
            DateTimeToken::hh24 => "hh24",
            DateTimeToken::I => "I",
            DateTimeToken::i => "i",
            DateTimeToken::ID => "ID",
            DateTimeToken::id => "id",
            DateTimeToken::IDDD => "IDDD",
            DateTimeToken::iddd => "iddd",
            DateTimeToken::IW => "IW",
            DateTimeToken::iw => "iw",
            DateTimeToken::IY => "IY",
            DateTimeToken::iy => "iy",
            DateTimeToken::IYY => "IYY",
            DateTimeToken::iyy => "iyy",
            DateTimeToken::IYYY => "IYYY",
            DateTimeToken::iyyy => "iyyy",
            DateTimeToken::J => "J",
            DateTimeToken::j => "j",
            DateTimeToken::MI => "MI",
            DateTimeToken::mi => "mi",
            DateTimeToken::MM => "MM",
            DateTimeToken::mm => "mm",
            DateTimeToken::MON => "MON",
            DateTimeToken::Mon => "Mon",
            DateTimeToken::mon => "mon",
            DateTimeToken::MONTH => "MONTH",
            DateTimeToken::Month => "Month",
            DateTimeToken::month => "month",
            DateTimeToken::MS => "MS",
            DateTimeToken::ms => "ms",
            DateTimeToken::OF => "OF",
            DateTimeToken::PM => "PM",
            DateTimeToken::pm => "pm",
            DateTimeToken::P_M => "P.M.",
            DateTimeToken::p_m => "p.m.",
            DateTimeToken::Q => "Q",
            DateTimeToken::q => "q",
            DateTimeToken::rm => "rm",
            DateTimeToken::RM => "RM",
            DateTimeToken::SS => "ss",
            DateTimeToken::ss => "SS",
            DateTimeToken::SSSS => "SSSS",
            DateTimeToken::ssss => "ssss",
            DateTimeToken::SSSSS => "SSSSS",
            DateTimeToken::sssss => "sssss",
            DateTimeToken::TZ => "TZ",
            DateTimeToken::tz => "tz",
            DateTimeToken::TZH => "TZH",
            DateTimeToken::TZM => "TZM",
            DateTimeToken::US => "US",
            DateTimeToken::us => "us",
            DateTimeToken::W => "W",
            DateTimeToken::w => "w",
            DateTimeToken::WW => "ww",
            DateTimeToken::ww => "WW",
            DateTimeToken::Y => "Y",
            DateTimeToken::y => "y",
            DateTimeToken::Y_YYY => "Y,YYY",
            DateTimeToken::y_yyy => "y,yyy",
            DateTimeToken::YY => "YY",
            DateTimeToken::yy => "yy",
            DateTimeToken::YYY => "YYY",
            DateTimeToken::yyy => "yyy",
            DateTimeToken::YYYY => "YYYY",
            DateTimeToken::yyyy => "yyyy",
            DateTimeToken::Quote => "\"",
            DateTimeToken::EscQuote => "\\\"",
            DateTimeToken::TH => "TH",
            DateTimeToken::th => "th",
        }
    }

    /// Returns the list of all known patterns, in the same order as the enum
    /// variants.
    fn patterns() -> Vec<&'static str> {
        Self::into_enum_iter().map(|v| v.pattern()).collect()
    }

    /// Returns the `DateTimeField` associated with this token, if any.
    ///
    /// Some tokens do not correspond directly to a field, but instead modify
    /// other fields.
    fn field(&self) -> Option<DateTimeField> {
        use DateTimeToken::*;
        use WordCaps::*;
        match self {
            AD | BC => Some(DateTimeField::Era {
                dots: false,
                caps: true,
            }),
            ad | bc => Some(DateTimeField::Era {
                dots: false,
                caps: false,
            }),
            A_D | B_C => Some(DateTimeField::Era {
                dots: true,
                caps: true,
            }),
            a_d | b_c => Some(DateTimeField::Era {
                dots: true,
                caps: false,
            }),
            AM | PM => Some(DateTimeField::Meridiem {
                dots: false,
                caps: true,
            }),
            am | pm => Some(DateTimeField::Meridiem {
                dots: false,
                caps: false,
            }),
            A_M | P_M => Some(DateTimeField::Meridiem {
                dots: true,
                caps: true,
            }),
            a_m | p_m => Some(DateTimeField::Meridiem {
                dots: true,
                caps: false,
            }),
            cc | CC => Some(DateTimeField::Century),
            d | D => Some(DateTimeField::DayOfWeek),
            day => Some(DateTimeField::DayName {
                abbrev: false,
                caps: NoCaps,
            }),
            Day => Some(DateTimeField::DayName {
                abbrev: false,
                caps: FirstCaps,
            }),
            DAY => Some(DateTimeField::DayName {
                abbrev: false,
                caps: AllCaps,
            }),
            dy => Some(DateTimeField::DayName {
                abbrev: true,
                caps: NoCaps,
            }),
            Dy => Some(DateTimeField::DayName {
                abbrev: true,
                caps: FirstCaps,
            }),
            DY => Some(DateTimeField::DayName {
                abbrev: true,
                caps: AllCaps,
            }),
            dd | DD => Some(DateTimeField::DayOfMonth),
            ddd | DDD => Some(DateTimeField::DayOfYear),
            fm | FM | fx | FX | th | TH | Quote | EscQuote => None,
            hh | HH | hh12 | HH12 => Some(DateTimeField::Hour12),
            hh24 | HH24 => Some(DateTimeField::Hour24),
            id | ID => Some(DateTimeField::IsoDayOfWeek),
            iddd | IDDD => Some(DateTimeField::IsoDayOfYear),
            iw | IW => Some(DateTimeField::IsoWeekOfYear),
            j | J => Some(DateTimeField::JulianDay),
            mi | MI => Some(DateTimeField::Minute),
            mm | MM => Some(DateTimeField::MonthOfYear),
            mon => Some(DateTimeField::MonthName {
                abbrev: true,
                caps: NoCaps,
            }),
            Mon => Some(DateTimeField::MonthName {
                abbrev: true,
                caps: FirstCaps,
            }),
            MON => Some(DateTimeField::MonthName {
                abbrev: true,
                caps: AllCaps,
            }),
            month => Some(DateTimeField::MonthName {
                abbrev: false,
                caps: NoCaps,
            }),
            Month => Some(DateTimeField::MonthName {
                abbrev: false,
                caps: FirstCaps,
            }),
            MONTH => Some(DateTimeField::MonthName {
                abbrev: false,
                caps: AllCaps,
            }),
            ms | MS => Some(DateTimeField::Millisecond),
            OF => Some(DateTimeField::TimezoneOffset),
            q | Q => Some(DateTimeField::Quarter),
            rm => Some(DateTimeField::MonthInRomanNumerals { caps: false }),
            RM => Some(DateTimeField::MonthInRomanNumerals { caps: true }),
            ss | SS => Some(DateTimeField::Second),
            ssss | SSSS | sssss | SSSSS => Some(DateTimeField::SecondsPastMidnight),
            tz => Some(DateTimeField::Timezone { caps: false }),
            TZ => Some(DateTimeField::Timezone { caps: true }),
            TZH => Some(DateTimeField::TimezoneHours),
            TZM => Some(DateTimeField::TimezoneMinutes),
            us | US => Some(DateTimeField::Microsecond),
            w | W => Some(DateTimeField::WeekOfMonth),
            ww | WW => Some(DateTimeField::WeekOfYear),
            y | Y => Some(DateTimeField::Year1),
            yy | YY => Some(DateTimeField::Year2),
            yyy | YYY => Some(DateTimeField::Year3),
            yyyy | YYYY => Some(DateTimeField::Year4 { separator: false }),
            y_yyy | Y_YYY => Some(DateTimeField::Year4 { separator: true }),
            i | I => Some(DateTimeField::IsoYear1),
            iy | IY => Some(DateTimeField::IsoYear2),
            iyy | IYY => Some(DateTimeField::IsoYear3),
            iyyy | IYYY => Some(DateTimeField::IsoYear4),
        }
    }

    /// Returns how this token should be rendered if it appears within quotes.
    /// This is usually the same string as the `pattern` method returns, but
    /// not always.
    fn as_literal(&self) -> &'static str {
        match self {
            DateTimeToken::Quote => "",
            DateTimeToken::EscQuote => "\"",
            _ => self.pattern(),
        }
    }

    /// Returns whether this token is a fill mode toggle.
    fn is_fill_mode_toggle(&self) -> bool {
        matches!(self, DateTimeToken::fm | DateTimeToken::FM)
    }

    /// Returns how this token affects the ordinal mode, if at all.
    fn ordinal_mode(&self) -> OrdinalMode {
        match self {
            DateTimeToken::th => OrdinalMode::Lower,
            DateTimeToken::TH => OrdinalMode::Upper,
            _ => OrdinalMode::None,
        }
    }
}

/// Specifies the ordinal suffix that should be attached to numeric fields.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum OrdinalMode {
    /// No ordinal suffix.
    None,
    /// A lowercase ordinal suffix.
    Lower,
    /// An uppercase ordinal suffix.
    Upper,
}

impl OrdinalMode {
    fn render(self, out: &mut impl fmt::Write, n: impl Into<i64>) -> Result<(), fmt::Error> {
        let n = n.into();
        // Numbers that end in teen always use "th" as their ordinal suffix.
        // Otherwise the last digit determines the ordinal suffix.
        let n = match n % 100 {
            10..=19 => 0,
            _ => n % 10,
        };
        match self {
            OrdinalMode::None => Ok(()),
            OrdinalMode::Lower => match n {
                1 => out.write_str("st"),
                2 => out.write_str("nd"),
                3 => out.write_str("rd"),
                _ => out.write_str("th"),
            },
            OrdinalMode::Upper => match n {
                1 => out.write_str("ST"),
                2 => out.write_str("ND"),
                3 => out.write_str("RD"),
                _ => out.write_str("TH"),
            },
        }
    }
}

/// Specifies the capitalization of a word.
#[allow(clippy::enum_variant_names)] // Having "Caps" in the variant names is clarifying.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum WordCaps {
    /// All of the letters should be capitalized.
    AllCaps,
    /// Only the first letter should be capitalized.
    FirstCaps,
    /// None of the letters should be capitalized.
    NoCaps,
}

/// A date-time field.
///
/// The variants are largely self-evident, but are described in detail in the
/// PostgreSQL documentation if necessary.
#[derive(Debug, Eq, PartialEq, Clone)]
enum DateTimeField {
    Hour12,
    Hour24,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    SecondsPastMidnight,
    Meridiem { dots: bool, caps: bool },
    Year1,
    Year2,
    Year3,
    Year4 { separator: bool },
    IsoYear1,
    IsoYear2,
    IsoYear3,
    IsoYear4,
    Era { dots: bool, caps: bool },
    MonthName { abbrev: bool, caps: WordCaps },
    MonthOfYear,
    DayName { abbrev: bool, caps: WordCaps },
    DayOfWeek,
    IsoDayOfWeek,
    DayOfMonth,
    DayOfYear,
    IsoDayOfYear,
    WeekOfMonth,
    WeekOfYear,
    IsoWeekOfYear,
    Century,
    JulianDay,
    Quarter,
    MonthInRomanNumerals { caps: bool },
    Timezone { caps: bool },
    TimezoneHours,
    TimezoneMinutes,
    TimezoneOffset,
}

/// An element of a date-time format string.
#[derive(Debug)]
enum DateTimeFormatNode {
    /// A field whose value will be computed from the input timestamp.
    Field {
        /// The inner field.
        field: DateTimeField,
        /// Whether the field should be padded with spaces to its maximum width.
        /// Does not have an effect for all fields, as the width of some fields
        /// is unknowable.
        fill: bool,
        /// Whether the field should be followed with an ordinal suffix, like
        /// "th." Only meaningful for numeric fields.
        ordinal: OrdinalMode,
    },
    /// A literal character.
    Literal(char),
}

const WEEKDAYS_ALL_CAPS: [&str; 7] = [
    "SUNDAY",
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
];

const WEEKDAYS_FIRST_CAPS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

const WEEKDAYS_NO_CAPS: [&str; 7] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

const WEEKDAYS_ABBREV_ALL_CAPS: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

const WEEKDAYS_ABBREV_FIRST_CAPS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

const WEEKDAYS_ABBREV_NO_CAPS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

const MONTHS_ALL_CAPS: [&str; 12] = [
    "JANUARY",
    "FEBRUARY",
    "MARCH",
    "APRIL",
    "MAY",
    "JUNE",
    "JULY",
    "AUGUST",
    "SEPTEMBER",
    "OCTOBER",
    "NOVEMBER",
    "DECEMBER",
];

const MONTHS_FIRST_CAPS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

const MONTHS_NO_CAPS: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

const MONTHS_ABBREV_ALL_CAPS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];

const MONTHS_ABBREV_FIRST_CAPS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

const MONTHS_ABBREV_NO_CAPS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];

const MONTHS_ROMAN_NO_CAPS: [&str; 12] = [
    "i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x", "xi", "xii",
];

const MONTHS_ROMAN_CAPS: [&str; 12] = [
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII",
];

impl DateTimeFormatNode {
    fn render(&self, buf: &mut impl fmt::Write, ts: &impl TimestampLike) -> Result<(), fmt::Error> {
        use WordCaps::*;
        match self {
            DateTimeFormatNode::Literal(ch) => buf.write_char(*ch),
            DateTimeFormatNode::Field {
                field,
                fill,
                ordinal,
            } => {
                macro_rules! write_num {
                    ($n:expr, $width:expr) => {{
                        write!(
                            buf,
                            "{:0width$}",
                            $n,
                            width = if *fill { $width } else { 0 }
                        )?;
                        ordinal.render(buf, $n)
                    }};
                    ($n:expr) => {
                        write_num!($n, 0)
                    };
                }

                macro_rules! write_str {
                    ($s:expr, $width:expr) => {{
                        write!(buf, "{:width$}", $s, width = if *fill { $width } else { 0 })
                    }};
                    ($s:expr) => {
                        write_str!($s, 0)
                    };
                }

                match field {
                    DateTimeField::Era {
                        dots: false,
                        caps: true,
                    } => write_str!(if ts.year_ce().0 { "AD" } else { "BC" }),
                    DateTimeField::Era {
                        dots: false,
                        caps: false,
                    } => write_str!(if ts.year_ce().0 { "ad" } else { "bc" }),
                    DateTimeField::Era {
                        dots: true,
                        caps: true,
                    } => write_str!(if ts.year_ce().0 { "A.D." } else { "B.C." }),
                    DateTimeField::Era {
                        dots: true,
                        caps: false,
                    } => write_str!(if ts.year_ce().0 { "a.d." } else { "b.c." }),
                    DateTimeField::Meridiem {
                        dots: false,
                        caps: true,
                    } => write_str!(if ts.hour12().0 { "PM" } else { "AM" }),
                    DateTimeField::Meridiem {
                        dots: false,
                        caps: false,
                    } => write_str!(if ts.hour12().0 { "pm" } else { "am" }),
                    DateTimeField::Meridiem {
                        dots: true,
                        caps: true,
                    } => write_str!(if ts.hour12().0 { "P.M." } else { "A.M." }),
                    DateTimeField::Meridiem {
                        dots: true,
                        caps: false,
                    } => write_str!(if ts.hour12().0 { "p.m." } else { "a.m." }),
                    DateTimeField::Century => {
                        let n = if ts.year() > 0 {
                            (ts.year() - 1) / 100 + 1
                        } else {
                            ts.year() / 100 - 1
                        };
                        write_num!(n, if n >= 0 { 2 } else { 3 })
                    }
                    DateTimeField::DayOfWeek => write_num!(ts.weekday().number_from_sunday(), 1),
                    DateTimeField::IsoDayOfWeek => write_num!(ts.weekday().number_from_monday(), 1),
                    DateTimeField::DayName {
                        abbrev: false,
                        caps: AllCaps,
                    } => write_str!(WEEKDAYS_ALL_CAPS[ts.weekday0()], 9),
                    DateTimeField::DayName {
                        abbrev: false,
                        caps: FirstCaps,
                    } => write_str!(WEEKDAYS_FIRST_CAPS[ts.weekday0()], 9),
                    DateTimeField::DayName {
                        abbrev: false,
                        caps: NoCaps,
                    } => write_str!(WEEKDAYS_NO_CAPS[ts.weekday0()], 9),
                    DateTimeField::DayName {
                        abbrev: true,
                        caps: AllCaps,
                    } => write_str!(WEEKDAYS_ABBREV_ALL_CAPS[ts.weekday0()]),
                    DateTimeField::DayName {
                        abbrev: true,
                        caps: FirstCaps,
                    } => write_str!(WEEKDAYS_ABBREV_FIRST_CAPS[ts.weekday0()]),
                    DateTimeField::DayName {
                        abbrev: true,
                        caps: NoCaps,
                    } => write_str!(WEEKDAYS_ABBREV_NO_CAPS[ts.weekday0()]),
                    DateTimeField::DayOfMonth => write_num!(ts.day(), 2),
                    DateTimeField::DayOfYear => write_num!(ts.ordinal(), 3),
                    DateTimeField::Hour12 => write_num!(ts.hour12().1, 2),
                    DateTimeField::Hour24 => write_num!(ts.hour(), 2),
                    DateTimeField::IsoYear1 => write_num!(ts.iso_year_ce() % 10, 1),
                    DateTimeField::IsoYear2 => write_num!(ts.iso_year_ce() % 100, 2),
                    DateTimeField::IsoYear3 => write_num!(ts.iso_year_ce() % 1000, 3),
                    DateTimeField::IsoYear4 => write_num!(ts.iso_year_ce(), 4),
                    DateTimeField::IsoDayOfYear => write_num!(
                        ts.iso_week().week0() * 7 + ts.weekday().number_from_monday(),
                        3
                    ),
                    DateTimeField::IsoWeekOfYear => write_num!(ts.iso_week().week(), 2),
                    DateTimeField::JulianDay => write_num!(ts.num_days_from_ce() + 1_721_425),
                    DateTimeField::Minute => write_num!(ts.minute(), 2),
                    DateTimeField::MonthOfYear => write_num!(ts.month(), 2),
                    DateTimeField::MonthName {
                        abbrev: true,
                        caps: AllCaps,
                    } => write_str!(MONTHS_ABBREV_ALL_CAPS[ts.month0() as usize]),
                    DateTimeField::MonthName {
                        abbrev: true,
                        caps: FirstCaps,
                    } => write_str!(MONTHS_ABBREV_FIRST_CAPS[ts.month0() as usize]),
                    DateTimeField::MonthName {
                        abbrev: true,
                        caps: NoCaps,
                    } => write_str!(MONTHS_ABBREV_NO_CAPS[ts.month0() as usize]),
                    DateTimeField::MonthName {
                        abbrev: false,
                        caps: AllCaps,
                    } => write_str!(MONTHS_ALL_CAPS[ts.month0() as usize], 9),
                    DateTimeField::MonthName {
                        abbrev: false,
                        caps: FirstCaps,
                    } => write_str!(MONTHS_FIRST_CAPS[ts.month0() as usize], 9),
                    DateTimeField::MonthName {
                        abbrev: false,
                        caps: NoCaps,
                    } => write_str!(MONTHS_NO_CAPS[ts.month0() as usize], 9),
                    DateTimeField::Millisecond => write_num!(ts.nanosecond() / 1_000_000, 3),
                    DateTimeField::Quarter => write_num!(ts.month0() / 3 + 1),
                    DateTimeField::MonthInRomanNumerals { caps: true } => {
                        write_str!(MONTHS_ROMAN_CAPS[ts.month0() as usize], 4)
                    }
                    DateTimeField::MonthInRomanNumerals { caps: false } => {
                        write_str!(MONTHS_ROMAN_NO_CAPS[ts.month0() as usize], 4)
                    }
                    DateTimeField::Second => write_num!(ts.second(), 2),
                    DateTimeField::SecondsPastMidnight => {
                        write_num!(ts.num_seconds_from_midnight())
                    }
                    DateTimeField::Timezone { caps } => write_str!(ts.timezone_name(*caps)),
                    DateTimeField::TimezoneOffset => write_str!(ts.timezone_offset()),
                    DateTimeField::TimezoneHours => write_str!(ts.timezone_hours()),
                    DateTimeField::TimezoneMinutes => write_str!(ts.timezone_minutes()),
                    DateTimeField::Microsecond => write_num!(ts.nanosecond() / 1_000, 6),
                    DateTimeField::WeekOfMonth => write_num!(ts.day0() / 7 + 1, 1),
                    DateTimeField::WeekOfYear => write_num!(ts.ordinal0() / 7 + 1, 2),
                    DateTimeField::Year1 => write_num!(ts.year_ce().1 % 10, 1),
                    DateTimeField::Year2 => write_num!(ts.year_ce().1 % 100, 2),
                    DateTimeField::Year3 => write_num!(ts.year_ce().1 % 1000, 3),
                    DateTimeField::Year4 { separator: false } => write_num!(ts.year_ce().1, 4),
                    DateTimeField::Year4 { separator: true } => {
                        let n = ts.year_ce().1;
                        write!(buf, "{},{:03}", n / 1000, n % 1000)?;
                        ordinal.render(buf, n)
                    }
                }
            }
        }
    }
}

/// A compiled date-time format string.
pub struct DateTimeFormat(Vec<DateTimeFormatNode>);

impl DateTimeFormat {
    /// Compiles a new `DateTimeFormat` from the input string `s`.
    pub fn compile(s: &str) -> DateTimeFormat {
        // The approach here uses the Aho-Corasick string searching algorithm to
        // repeatedly and efficiently find the next token of interest. Tokens of
        // interest are typically field specifiers, like "DDDD", or field
        // modifiers, like "FM". Characters in between tokens of interest are
        // recorded as literals. We also consider a double quote a token of
        // interest, as a double quote disables matching of field
        // specifiers/modifiers until the next double quote.

        struct Match {
            start: usize,
            end: usize,
            token: DateTimeToken,
        }

        let matcher = AhoCorasickBuilder::new()
            .match_kind(aho_corasick::MatchKind::LeftmostLongest)
            .build(DateTimeToken::patterns());

        let matches: Vec<_> = matcher
            .find_iter(&s)
            .map(|m| Match {
                start: m.start(),
                end: m.end(),
                token: DateTimeToken::try_from(m.pattern() as u8).expect("match pattern missing"),
            })
            .collect();

        let mut out = Vec::new();
        let mut pos = 0;
        let mut in_quotes = false;
        for i in 0..matches.len() {
            // Any characters since the last match are to be taken literally.
            for c in s[pos..matches[i].start].chars() {
                if !(in_quotes && c == '\\') {
                    // Backslash is an escape character inside of quotes.
                    out.push(DateTimeFormatNode::Literal(c));
                }
            }

            if in_quotes {
                // If we see a format specifier inside of a quoted block, it
                // is taken literally.
                for c in matches[i].token.as_literal().chars() {
                    out.push(DateTimeFormatNode::Literal(c))
                }
            } else if let Some(field) = matches[i].token.field() {
                // We found a format specifier. Look backwards for a fill mode
                // toggle (fill mode is on by default), and forwards for an
                // ordinal suffix specifier (default is no ordinal suffix).
                let fill = i == 0
                    || matches[i - 1].end != matches[i].start
                    || !matches[i - 1].token.is_fill_mode_toggle();
                let ordinal = match matches.get(i + 1) {
                    Some(m) if m.start == matches[i].end => m.token.ordinal_mode(),
                    _ => OrdinalMode::None,
                };
                out.push(DateTimeFormatNode::Field {
                    field,
                    fill,
                    ordinal,
                });
            }

            if matches[i].token == DateTimeToken::Quote {
                in_quotes = !in_quotes;
            }
            pos = matches[i].end;
        }
        for c in s[pos..].chars() {
            out.push(DateTimeFormatNode::Literal(c));
        }
        DateTimeFormat(out)
    }

    /// Renders the format string using the timestamp `ts` as the input. The
    /// placeholders in the format string will be filled in appropriately
    /// according to the value of `ts`.
    pub fn render(&self, ts: impl TimestampLike) -> String {
        let mut out = String::new();
        for node in &self.0 {
            node.render(&mut out, &ts)
                .expect("rendering to string cannot fail");
        }
        out
    }
}
