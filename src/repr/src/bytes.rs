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
/// with 1024 multiplier.
/// Valid units are B, kB, MB, GB, TB with multiples of 1024
/// where 1MB = 1024kB
#[derive(
    Arbitrary, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct ByteSize {
    size: u32,
    unit: BytesUnit,
}

impl ByteSize {
    const fn new(size: u32, unit: BytesUnit) -> ByteSize {
        ByteSize { size, unit }
    }

    pub const fn b(size: u32) -> ByteSize {
        ByteSize::new(size, BytesUnit::B)
    }

    pub const fn kb(size: u32) -> ByteSize {
        ByteSize::new(size, BytesUnit::Kb)
    }

    pub const fn mb(size: u32) -> ByteSize {
        ByteSize::new(size, BytesUnit::Mb)
    }

    pub const fn gb(size: u32) -> ByteSize {
        ByteSize::new(size, BytesUnit::Gb)
    }

    pub const fn tb(size: u32) -> ByteSize {
        ByteSize::new(size, BytesUnit::Tb)
    }

    pub fn as_bytes(&self) -> u64 {
        Into::<u64>::into(self.size) * self.unit.value()
    }

    fn format_string(&self) -> String {
        let (size, unit) = Self::normalize_size_unit(&self.size, &self.unit);

        if size == 0 {
            // If the size is 0, then no unit is returned in postgres
            return "0".to_string();
        }
        format!("{}{}", size, unit)
    }

    // Helper method which converts to a higher unit if it's perfectly divisible by 1024
    // For eg: 1024B will be 1kB
    fn normalize_size_unit(size: &u32, unit: &BytesUnit) -> (u32, BytesUnit) {
        let mut size = *size;
        let mut unit = unit.clone();

        if size == 0 {
            unit = BytesUnit::B
        } else {
            loop {
                if size % 1024 == 0 && unit != BytesUnit::Tb {
                    size = size / 1024;
                    unit = unit.higher();
                } else {
                    break;
                }
            }
        }

        (size, unit)
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
        let u32_err = |_| format!("number exceeds u32 limit");

        let number: String = value
            .chars()
            .take_while(|c| c.is_digit(10) || c == &'.')
            .collect();

        let suffix: String = value
            .chars()
            .skip_while(|c| c.is_whitespace() || c.is_digit(10) || c == &'.')
            .collect();

        let num = number
            .parse::<f64>()
            .map_err(|e| format!("couldn't parse {} as a number, {}", number, e))?;

        if suffix.is_empty() {
            let size = u64::cast_lossy(num).try_into().map_err(u32_err)?;
            let (size, unit) = Self::normalize_size_unit(&size, &BytesUnit::B);
            Ok(Self::new(size, unit))
        } else {
            let unit = suffix
                .parse::<BytesUnit>()
                .map_err(|e| format!("couldn't parse {:?} into a known SI unit, {}. Valid units are B, kB, MB, GB, and TB", suffix, e))?;

            // checking if number has no fractional part
            if num.trunc() == num {
                let size: u32 = u64::cast_lossy(num).try_into().map_err(u32_err)?;
                let (size, unit) = Self::normalize_size_unit(&size, &unit);
                Ok(Self::new(size, unit))
            } else {
                match unit {
                    BytesUnit::B => Ok(Self::b(
                        u64::cast_lossy(num.round()).try_into().map_err(u32_err)?,
                    )),
                    _ => {
                        let size = u64::cast_lossy((num * 1024.0).round())
                            .try_into()
                            .map_err(u32_err)?;
                        Ok(Self::new(size, unit.lower()))
                    }
                }
            }
        }
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

    // Returns the next higher unit
    fn higher(&self) -> Self {
        match &self {
            BytesUnit::B => BytesUnit::Kb,
            BytesUnit::Kb => BytesUnit::Mb,
            BytesUnit::Mb => BytesUnit::Gb,
            BytesUnit::Gb => BytesUnit::Tb,
            BytesUnit::Tb => BytesUnit::Tb,
        }
    }

    // Returns the previous lower unit
    fn lower(&self) -> Self {
        match &self {
            BytesUnit::B => BytesUnit::B,
            BytesUnit::Kb => BytesUnit::B,
            BytesUnit::Mb => BytesUnit::Kb,
            BytesUnit::Gb => BytesUnit::Mb,
            BytesUnit::Tb => BytesUnit::Gb,
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
        match s.as_ref() {
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
    use crate::bytes::BytesUnit;
    use proptest::prelude::*;
    use proptest::proptest;

    fn assert_to_string_and_display(expected: &str, b: ByteSize) {
        assert_eq!(expected.to_string(), b.to_string());
        assert_eq!(expected, format!("{}", b));
    }

    #[mz_ore::test]
    fn test_to_string() {
        assert_to_string_and_display("0", ByteSize::gb(0));
        assert_to_string_and_display("1GB", ByteSize::mb(1024)); // 1024MB is convered to 1GB
        assert_to_string_and_display("215B", ByteSize::b(215));
        assert_to_string_and_display("1kB", ByteSize::kb(1));
        assert_to_string_and_display("301kB", ByteSize::kb(301));
        assert_to_string_and_display("419MB", ByteSize::mb(419));
        assert_to_string_and_display("518GB", ByteSize::gb(518));
        assert_to_string_and_display("815TB", ByteSize::tb(815));
        assert_to_string_and_display("10kB", ByteSize::b(10240));
        assert_to_string_and_display("10MB", ByteSize::kb(10240));
        assert_to_string_and_display("10GB", ByteSize::mb(10240));
        assert_to_string_and_display("10TB", ByteSize::gb(10240));
        assert_to_string_and_display("10240TB", ByteSize::tb(10240));
    }

    #[mz_ore::test]
    fn test_parse() {
        // shortcut for writing test cases
        fn parse(s: &str) -> ByteSize {
            s.parse::<ByteSize>().unwrap()
        }

        assert_eq!(parse("0"), ByteSize::b(0));
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
        assert!("".parse::<ByteSize>().is_err());
        assert!("4294967298".parse::<ByteSize>().is_err()); // > u32::MAX
        assert!("a124GB".parse::<ByteSize>().is_err());
        assert!("1K".parse::<ByteSize>().is_err());
        // postgres is strict about matching capitalization
        assert!("1gb".parse::<ByteSize>().is_err());
        assert!("1KB".parse::<ByteSize>().is_err());
    }

    #[mz_ore::test]
    fn test_rounding() {
        // shortcut for writing test cases
        fn parse(s: &str) -> ByteSize {
            s.parse::<ByteSize>().unwrap()
        }

        fn assert_equivalent(v1: &str, v2: &str) {
            assert_eq!(parse(v1).to_string(), v2)
        }

        assert_equivalent("0", "0");
        assert_equivalent("0 TB", "0");
        assert_equivalent("0kB", "0");
        assert_equivalent("500", "500B");
        assert_equivalent("1073741824", "1GB");
        assert_equivalent("1.01B", "1B");
        assert_equivalent("10240B", "10kB");
        assert_equivalent("1.5kB", "1536B");
        assert_equivalent("30.1GB", "30822MB");
        assert_equivalent("30.1MB", "30822kB");
        assert_equivalent("30.1TB", "30822GB");
        assert_equivalent("39.9TB", "40858GB");
        assert_equivalent("30.9B", "31B");
    }

    #[mz_ore::test]
    fn test_bytes_equivalent() {
        assert_eq!(
            ByteSize::new(10240, BytesUnit::B).as_bytes(),
            ByteSize::new(10, BytesUnit::Kb).as_bytes()
        );

        assert_eq!(
            ByteSize::new(1024 * 1024, BytesUnit::B).as_bytes(),
            ByteSize::new(1, BytesUnit::Mb).as_bytes()
        );

        assert_eq!(
            ByteSize::new(1024 * 1024, BytesUnit::Kb).as_bytes(),
            ByteSize::new(1, BytesUnit::Gb).as_bytes()
        );
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
