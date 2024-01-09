// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display};
use std::str::FromStr;

use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_ore::cast::CastLossy;

/// Struct for postgres compatible size units which is slightly different from
/// `bytesize::ByteSize`. Instead of MiB or GiB and so on, it uses MB, GB for the sizes
/// with 1024 multiplier.
/// Valid units are B, kB, MB, GB, TB, PB with multiples of 1024
/// where 1MB = 1024kB
#[derive(
    Arbitrary, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub const fn b(size: u64) -> ByteSize {
        ByteSize(size)
    }

    pub const fn kb(size: u64) -> ByteSize {
        ByteSize(size * BytesUnit::Kb.value())
    }

    pub const fn mb(size: u64) -> ByteSize {
        ByteSize(size * BytesUnit::Mb.value())
    }

    pub const fn gb(size: u64) -> ByteSize {
        ByteSize(size * BytesUnit::Gb.value())
    }

    pub const fn tb(size: u64) -> ByteSize {
        ByteSize(size * BytesUnit::Tb.value())
    }

    pub const fn pb(size: u64) -> ByteSize {
        ByteSize(size * BytesUnit::Pb.value())
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
    /// Copied logic from `bytesize::ByteSize::to_string`
    /// with slight change to have no space between the number and the unit
    /// to have the same behaviour as Postgres.
    pub fn to_string(bytes: u64) -> String {
        let unit = 1024;
        let unit_base = 6.907755279; // ln 1000
        let unit_prefix = ['k', 'M', 'G', 'T', 'P'];
        let unit_suffix = 'B';

        if bytes < unit {
            format!("{}B", bytes)
        } else {
            let size: f64 = f64::cast_lossy(bytes);
            let exp: usize = match usize::cast_lossy(size.ln() / unit_base) {
                e if e == 0 => 1,
                e if e > unit_prefix.len() => unit_prefix.len(), // maxing out at PB
                e => e,
            };

            format!(
                "{:.1}{}{}",
                size / f64::cast_lossy(unit.pow(exp.try_into().expect("usize into u32"))),
                unit_prefix[exp - 1],
                unit_suffix
            )
        }
    }
}

impl Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&Self::to_string(self.0))
    }
}

impl FromStr for ByteSize {
    type Err = String;

    /// Copied logic from `bytesize::ByteSize`
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Ok(v) = value.parse::<u64>() {
            return Ok(Self(v));
        }
        let number: String = value
            .chars()
            .take_while(|c| c.is_digit(10) || c == &'.')
            .collect();
        match number.parse::<f64>() {
            Ok(v) => {
                let suffix: String = value
                    .chars()
                    .skip_while(|c| c.is_whitespace() || c.is_digit(10) || c == &'.')
                    .collect();
                match suffix.parse::<BytesUnit>() {
                    Ok(u) => Ok(Self(u64::cast_lossy(v * f64::cast_lossy(u.value())))),
                    Err(error) => Err(format!(
                        "couldn't parse {:?} into a known SI unit, {}",
                        suffix, error
                    )),
                }
            }
            Err(error) => Err(format!(
                "couldn't parse {:?} into a BytesValue, {}",
                value, error
            )),
        }
    }
}

/// Valid units for representing bytes
#[derive(Debug)]
pub enum BytesUnit {
    B,
    Kb,
    Mb,
    Gb,
    Tb,
    Pb,
}

impl BytesUnit {
    const fn value(&self) -> u64 {
        match &self {
            BytesUnit::B => 1,
            BytesUnit::Kb => 1_024,
            BytesUnit::Mb => 1_048_576,
            BytesUnit::Gb => 1_073_741_824,
            BytesUnit::Tb => 1_099_511_627_776,
            BytesUnit::Pb => 1_125_899_906_842_624,
        }
    }
}

impl fmt::Display for BytesUnit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            BytesUnit::B => "B",
            BytesUnit::Kb => "kB",
            BytesUnit::Mb => "MB",
            BytesUnit::Gb => "GB",
            BytesUnit::Tb => "TB",
            BytesUnit::Pb => "PB",
        })
    }
}

impl FromStr for BytesUnit {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_ref() {
            "B" => Ok(Self::B),
            "KB" => Ok(Self::Kb),
            "MB" => Ok(Self::Mb),
            "GB" => Ok(Self::Gb),
            "TB" => Ok(Self::Tb),
            "PB" => Ok(Self::Pb),
            _ => Err(format!("invalid BytesUnit: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes::ByteSize;
    use mz_ore::cast::CastLossy;
    use proptest::prelude::*;
    use proptest::proptest;

    #[mz_ore::test]
    fn test_to_string() {
        assert_to_string("609.0PB", ByteSize::pb(609));
        assert_to_string("10000.0PB", ByteSize::pb(10000));
        assert_to_string("1.9GB", ByteSize::mb(1907));
    }

    fn assert_to_string(expected: &str, b: ByteSize) {
        assert_eq!(expected.to_string(), b.to_string());
    }

    fn assert_display(expected: &str, b: ByteSize) {
        assert_eq!(expected, format!("{}", b));
    }

    #[mz_ore::test]
    fn test_display() {
        assert_display("215B", ByteSize::b(215));
        assert_display("1.0kB", ByteSize::kb(1));
        assert_display("301.0kB", ByteSize::kb(301));
        assert_display("419.0MB", ByteSize::mb(419));
        assert_display("518.0GB", ByteSize::gb(518));
        assert_display("815.0TB", ByteSize::tb(815));
        assert_display("609.0PB", ByteSize::pb(609));
    }

    #[mz_ore::test]
    fn test_parse() {
        // shortcut for writing test cases
        fn parse(s: &str) -> u64 {
            s.parse::<ByteSize>().unwrap().0
        }

        assert_eq!("0".parse::<ByteSize>().unwrap().0, 0);
        assert_eq!(parse("0"), 0);
        assert_eq!(parse("500"), 500);
        assert_eq!(parse("1kB"), ByteSize::kb(1).as_u64());
        assert_eq!(
            parse("1.5kb"),
            u64::cast_lossy(1.5 * f64::cast_lossy(ByteSize::kb(1).as_u64()))
        );
        assert_eq!(parse("1 KB"), ByteSize::kb(1).as_u64());
        assert_eq!(parse("3 MB"), ByteSize::mb(3).as_u64());
        assert_eq!(parse("6 GB"), ByteSize::gb(6).as_u64());
        assert_eq!(parse("4GB"), ByteSize::gb(4).as_u64());
        assert_eq!(parse("88TB"), ByteSize::tb(88).as_u64());
        assert_eq!(parse("521  TB"), ByteSize::tb(521).as_u64());
        assert_eq!(parse("8 PB"), ByteSize::pb(8).as_u64());
        assert_eq!(parse("81 PB"), ByteSize::pb(81).as_u64());

        // parsing errors
        assert!("".parse::<ByteSize>().is_err());
        assert!("a124GB".parse::<ByteSize>().is_err());
        assert!("1K".parse::<ByteSize>().is_err());
    }

    proptest! {
      #[mz_ore::test]
      fn proptest_bytes_roundtrips_string(og: ByteSize) {
        // Not all [`ByteSize`] values can successfully roundtrip.
        // For example, '1025 B' will be rounded off to '1 kB'
        // which is in turn '1024 B'.
        // So instead testing roundtrip of string representations to
        // [`ByteSize`] and vice versa.
        let og_string = og.to_string();
        let roundtrip = og_string.parse::<ByteSize>().expect("roundtrip").to_string();
        prop_assert_eq!(og_string, roundtrip);
      }
    }
}
