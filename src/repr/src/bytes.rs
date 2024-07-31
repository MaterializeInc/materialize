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

/// Struct for postgres compatible size units which is different from
/// `bytesize::ByteSize`. Instead of MiB or GiB and so on, it uses MB, GB for the sizes
/// with 1024 multiplier. Valid units are B, kB, MB, GB, TB with multiples of 1024
/// where 1MB = 1024kB.
///
/// In postgres, each setting has a base unit (for eg. B, kB) and the value can either
/// be integer or float. The base unit serves as the default unit if a number is provided
/// without a unit and it's also the minimum unit in which values
/// can be rounded to. For example, with base unit of kB, 30.1kB will be rounded to
/// 30kB since it can't have a lower unit, but 30.1MB will be rounded to 30822kB.
/// For [`ByteSize`], the value is an integer and the base unit is bytes (`B`).
#[derive(
    Arbitrary, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct ByteSize(u64);

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

    pub fn as_bytes(&self) -> u64 {
        self.0
    }

    fn format_string(&self) -> String {
        match self.0 {
            zero if zero == 0 => "0".to_string(),
            tb if tb % BytesUnit::Tb.value() == 0 => {
                format!("{}{}", tb / BytesUnit::Tb.value(), BytesUnit::Tb)
            }
            gb if gb % BytesUnit::Gb.value() == 0 => {
                format!("{}{}", gb / BytesUnit::Gb.value(), BytesUnit::Gb)
            }
            mb if mb % BytesUnit::Mb.value() == 0 => {
                format!("{}{}", mb / BytesUnit::Mb.value(), BytesUnit::Mb)
            }
            kb if kb % BytesUnit::Kb.value() == 0 => {
                format!("{}{}", kb / BytesUnit::Kb.value(), BytesUnit::Kb)
            }
            b => format!("{}{}", b, BytesUnit::B),
        }
    }
}

impl Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&self.format_string())
    }
}

impl FromStr for ByteSize {
    type Err = String;

    // To behave the same as in postgres, this always
    // rounds down to the next lower unit if possible.
    // For example 30.9B, will be rounded to 31B, since there's no
    // lower unit than B. But 30.1kB will be rounded to
    // 31642B.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let number: String = value
            .chars()
            .take_while(|c| c.is_digit(10) || c == &'.')
            .collect();

        let suffix: String = value
            .chars()
            .skip_while(|c| c.is_whitespace() || c.is_digit(10) || c == &'.')
            .collect();

        let unit = if suffix.is_empty() {
            BytesUnit::B
        } else {
            suffix
            .parse::<BytesUnit>()
            .map_err(|e| format!("couldn't parse {:?} into a known SI unit, {}. Valid units are B, kB, MB, GB, and TB", suffix, e))?
        };

        let (size, unit) = if let Ok(integer) = number.parse::<u64>() {
            (integer, unit)
        } else {
            let num = number
                .parse::<f64>()
                .map_err(|e| format!("couldn't parse {} as a number, {}", number, e))?;

            // checking if number has no fractional part
            if num.trunc() == num {
                let size = u64::cast_lossy(num);
                (size, unit)
            } else {
                match unit {
                    BytesUnit::B => (u64::cast_lossy(num.round()), BytesUnit::B),
                    BytesUnit::Kb => (u64::cast_lossy((num * 1024.0).round()), BytesUnit::B),
                    BytesUnit::Mb => (u64::cast_lossy((num * 1024.0).round()), BytesUnit::Kb),
                    BytesUnit::Gb => (u64::cast_lossy((num * 1024.0).round()), BytesUnit::Mb),
                    BytesUnit::Tb => (u64::cast_lossy((num * 1024.0).round()), BytesUnit::Gb),
                }
            }
        };

        let bytes = size
            .checked_mul(unit.value())
            .ok_or_else(|| "bytes value exceeds u64 range".to_string())?;
        Ok(Self(bytes))
    }
}

/// Valid units for representing bytes
#[derive(
    Arbitrary, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub enum BytesUnit {
    #[default]
    B,
    Kb,
    Mb,
    Gb,
    Tb,
}

impl BytesUnit {
    const fn value(&self) -> u64 {
        match &self {
            BytesUnit::B => 1,
            BytesUnit::Kb => 1_024,
            BytesUnit::Mb => 1_048_576,
            BytesUnit::Gb => 1_073_741_824,
            BytesUnit::Tb => 1_099_511_627_776,
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
        })
    }
}

impl FromStr for BytesUnit {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "B" => Ok(Self::B),
            "kB" => Ok(Self::Kb),
            "MB" => Ok(Self::Mb),
            "GB" => Ok(Self::Gb),
            "TB" => Ok(Self::Tb),
            _ => Err(format!(
                "invalid BytesUnit: {}. Valid units are B, kB, MB, GB, and TB",
                s
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes::ByteSize;
    use mz_ore::assert_err;
    use proptest::prelude::*;
    use proptest::proptest;

    #[mz_ore::test]
    fn test_to_string() {
        fn assert_to_string(expected: &str, b: ByteSize) {
            assert_eq!(expected.to_string(), b.to_string());
        }
        assert_to_string("0", ByteSize::gb(0));
        assert_to_string("1GB", ByteSize::mb(1024));
        assert_to_string("215B", ByteSize::b(215));
        assert_to_string("1kB", ByteSize::kb(1));
        assert_to_string("301kB", ByteSize::kb(301));
        assert_to_string("419MB", ByteSize::mb(419));
        assert_to_string("518GB", ByteSize::gb(518));
        assert_to_string("815TB", ByteSize::tb(815));
        assert_to_string("10kB", ByteSize::b(10240));
        assert_to_string("10MB", ByteSize::kb(10240));
        assert_to_string("10GB", ByteSize::mb(10240));
        assert_to_string("10TB", ByteSize::gb(10240));
        assert_to_string("10240TB", ByteSize::tb(10240));
    }

    #[mz_ore::test]
    fn test_parse() {
        // shortcut for writing test cases
        fn parse(s: &str) -> ByteSize {
            s.parse::<ByteSize>().unwrap()
        }

        assert_eq!(parse("0"), ByteSize::b(0));
        assert_eq!(parse("9.9"), ByteSize::b(10));
        assert_eq!(parse("0B"), ByteSize::b(0));
        assert_eq!(parse("0MB"), ByteSize::b(0));
        assert_eq!(parse("500"), ByteSize::b(500));
        assert_eq!(parse("1kB"), ByteSize::kb(1));
        assert_eq!(parse("1.5kB"), ByteSize::b(1536));
        assert_eq!(parse("1 kB"), ByteSize::kb(1));
        assert_eq!(parse("3 MB"), ByteSize::mb(3));
        assert_eq!(parse("6 GB"), ByteSize::gb(6));
        assert_eq!(parse("4GB"), ByteSize::gb(4));
        assert_eq!(parse("88TB"), ByteSize::tb(88));
        assert_eq!(parse("521  TB"), ByteSize::tb(521));

        // parsing errors
        assert_err!("".parse::<ByteSize>());
        assert_err!("a124GB".parse::<ByteSize>());
        assert_err!("1K".parse::<ByteSize>());
        assert_err!("B".parse::<ByteSize>());
        // postgres is strict about matching capitalization
        assert_err!("1gb".parse::<ByteSize>());
        assert_err!("1KB".parse::<ByteSize>());
    }

    #[mz_ore::test]
    fn test_rounding() {
        // shortcut for writing test cases
        fn parse(s: &str) -> ByteSize {
            s.parse::<ByteSize>().unwrap()
        }

        fn assert_equivalent(v1: &str, v2: &str) {
            assert_eq!(parse(v1), parse(v2))
        }

        assert_equivalent("0", "0");
        assert_equivalent("0 TB", "0");
        assert_equivalent("0kB", "0");
        assert_equivalent("13.89", "14B");
        assert_equivalent("500", "500B");
        assert_equivalent("1073741824", "1GB");
        assert_equivalent("1073741824.0", "1GB");
        assert_equivalent("1073741824.1", "1GB");
        assert_equivalent("1073741824.9", "1073741825B");
        assert_equivalent("2147483648", "2GB");
        assert_equivalent("3221225472", "3GB");
        assert_equivalent("4294967296", "4GB");
        assert_equivalent("4294967295", "4294967295B");
        assert_equivalent("1024.1", "1kB");
        assert_equivalent("1024.9", "1025B");
        assert_equivalent("1024.1MB", "1048678kB");
        assert_equivalent("1024.9MB", "1049498kB");
        assert_equivalent("1.01B", "1B");
        assert_equivalent("1.01kB", "1034B");
        assert_equivalent("1.0kB", "1kB");
        assert_equivalent("10240B", "10kB");
        assert_equivalent("1.5kB", "1536B");
        assert_equivalent("30.1GB", "30822MB");
        assert_equivalent("30.1MB", "30822kB");
        assert_equivalent("30.1TB", "30822GB");
        assert_equivalent("39.9TB", "40858GB");
        assert_equivalent("30.9B", "31B");
    }

    proptest! {
      #[mz_ore::test]
      fn proptest_bytes_roundtrips_string(og: ByteSize) {
        // Not all [`ByteSize`] values can successfully roundtrip.
        // For example, '30.1 MB' will be rounded off to '30822 kB'.
        // So instead testing roundtrip of string representations to
        // [`ByteSize`] and vice versa.
        let og_string = og.to_string();
        let roundtrip = og_string.parse::<ByteSize>().expect("roundtrip").to_string();
        prop_assert_eq!(og_string, roundtrip);
      }
    }
}
