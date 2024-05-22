// Copyright (c) 2020 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! Functions and types related to the mysql decimal type.

use byteorder::{BigEndian as BE, ReadBytesExt, WriteBytesExt};

use std::{
    cmp::{Ord, Ordering, PartialEq, PartialOrd},
    fmt,
    io::{self, Read, Write},
    mem::size_of,
    str::FromStr,
};

#[cfg(test)]
mod test;

#[derive(Debug, Clone, Copy)]
pub struct ParseDecimalError;

/// Type of a base 9 digit.
pub type Digit = i32;

/// Number of decimal digits per `Digit`.
pub const DIG_PER_DEC: usize = 9;

/// Base of the `Digit`.
pub const DIG_BASE: usize = 1_000_000_000;

/// Number of bytes required to store given number of decimal digits.
pub const DIG_TO_BYTES: [u8; DIG_PER_DEC + 1] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

pub const POWERS_10: [i32; DIG_PER_DEC + 1] = [
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
];

/// MySql decimal.
///
/// The only purpose of this type is to parse binary decimal from binlogs,
/// so it isn't meant to be an alternative to `decimal_t`.
///
/// This type supports:
///
/// *   serialization/deserialization to/from binary format
///     (see `read_bin` and `write_bin` functions);
/// *   parsing from decimal string/buffer (see `Decimal::parse_bytes`, `FromStr` impl);
/// *   conversion to decimal string (using `Display`).
///
/// # Notes
///
/// *   `Ord` and `Eq` impls are relied on the binary representation,
///     i.e. both `rhs` and `lhs` will be serialized into temporary buffers;
/// *   even though MySql's `string2decimal` function allows scientific notation,
///     this implementation denies it.
#[derive(Default, Debug, Eq)]
pub struct Decimal {
    /// The number of *decimal* digits (NOT number of `Digit`s!) before the point.
    intg: usize,
    /// The number of decimal digits after the point.
    frac: usize,
    /// `false` means positive, `true` means negative.
    sign: bool,
    /// Array of `Digit`s.
    buf: Vec<Digit>,
}

impl Decimal {
    pub fn bin_size(&self) -> usize {
        decimal_bin_size(self.intg + self.frac, self.frac)
    }

    pub fn parse_bytes(bytes: &[u8]) -> Result<Self, ParseDecimalError> {
        match std::str::from_utf8(bytes) {
            Ok(string) => Decimal::from_str(string),
            Err(_) => Err(ParseDecimalError),
        }
    }

    pub fn write_bin<T: Write>(&self, mut output: T) -> io::Result<()> {
        // result bits must be inverted if the sign is negative,
        // we'll XOR it with `mask` to achieve this.
        let mask: Digit = if self.sign {
            // XOR with this mask will invert bits
            -1
        } else {
            // XOR with this mask will do nothing
            0
        };

        let mut out_buf = Vec::with_capacity(self.buf.len() * size_of::<Digit>());

        let mut digits = self.buf.iter();

        let mut intg = self.intg;
        let num_prefix_digits = self.intg % DIG_PER_DEC;
        if num_prefix_digits > 0 {
            let digit = *digits.next().expect("decimal is ill-formed");
            match DIG_TO_BYTES[num_prefix_digits] {
                1 => out_buf.write_i8((digit ^ mask) as i8)?,
                2 => out_buf.write_i16::<BE>((digit ^ mask) as i16)?,
                3 => out_buf.write_i24::<BE>((digit ^ mask) as i32)?,
                4 => out_buf.write_i32::<BE>((digit ^ mask) as i32)?,
                _ => unreachable!(),
            }
            intg -= num_prefix_digits;
        }
        while intg > 0 {
            let digit = *digits.next().expect("decimal is ill-formed");
            out_buf.write_i32::<BE>((digit ^ mask) as i32)?;
            intg -= DIG_PER_DEC;
        }
        let mut frac = self.frac;
        while frac > 0 {
            let len = std::cmp::min(DIG_PER_DEC, frac);
            let mut digit = *digits.next().expect("decimal is ill-formed");
            if len < DIG_PER_DEC {
                digit /= POWERS_10[DIG_PER_DEC - len];
                match DIG_TO_BYTES[len] {
                    1 => out_buf.write_i8((digit ^ mask) as i8)?,
                    2 => out_buf.write_i16::<BE>((digit ^ mask) as i16)?,
                    3 => out_buf.write_i24::<BE>((digit ^ mask) as i32)?,
                    4 => out_buf.write_i32::<BE>((digit ^ mask) as i32)?,
                    _ => unreachable!(),
                }
            } else {
                out_buf.write_i32::<BE>((digit ^ mask) as i32)?;
            }
            frac -= len
        }

        out_buf[0] ^= 0x80;

        output.write_all(&out_buf)
    }

    pub fn read_bin<T: Read>(
        mut input: T,
        precision: usize,
        scale: usize,
        keep_prec: bool,
    ) -> io::Result<Self> {
        let mut out = Self::default();

        let bin_size = decimal_bin_size(precision, scale);
        let mut buffer = vec![0_u8; bin_size];
        input.read_exact(&mut buffer)?;

        // we should invert back the very first bit of a binary representation
        if let Some(x) = buffer.get_mut(0) {
            *x ^= 0x80
        }

        // is it negative or not
        let mask = if buffer.first().copied().unwrap_or(0) & 0x80 == 0 {
            // positive, so mask should do noghing
            0
        } else {
            // negative, so mask snould invert bits
            -1
        };

        let intg = precision - scale;
        let prefix_len = intg % DIG_PER_DEC;
        let intg_full = intg / DIG_PER_DEC;

        let frac = scale;
        let suffix_len = frac % DIG_PER_DEC;
        let frac_full = frac / DIG_PER_DEC;

        out.sign = mask != 0;
        out.intg = intg;
        out.frac = frac;

        let mut input = &buffer[..];

        let mut trimmed = keep_prec;
        if prefix_len > 0 {
            let len = DIG_TO_BYTES[prefix_len];
            let x = match len {
                1 => input.read_i8()? as i32,
                2 => input.read_i16::<BE>()? as i32,
                3 => input.read_i24::<BE>()?,
                4 => input.read_i32::<BE>()?,
                _ => unreachable!(),
            } ^ mask;
            if x == 0 && !trimmed {
                out.intg -= prefix_len;
            } else {
                trimmed = true;
                out.buf.push(x);
            }
        }
        for _ in 0..intg_full {
            let x = input.read_i32::<BE>()? ^ mask;
            if x == 0 && !trimmed {
                out.intg -= DIG_PER_DEC;
            } else {
                trimmed = true;
                out.buf.push(x);
            }
        }
        for _ in 0..frac_full {
            out.buf.push(input.read_i32::<BE>()? ^ mask);
        }
        if suffix_len > 0 {
            let len = DIG_TO_BYTES[suffix_len];
            let mut x = match len {
                1 => input.read_i8()? as i32,
                2 => input.read_i16::<BE>()? as i32,
                3 => input.read_i24::<BE>()?,
                4 => input.read_i32::<BE>()?,
                _ => unreachable!(),
            } ^ mask;
            x *= POWERS_10[DIG_PER_DEC - suffix_len];
            out.buf.push(x);
        }

        if out.intg == 0 && out.frac == 0 {
            out.intg = 1;
            out.frac = 0;
            out.sign = false;
            out.buf.resize(1, 0);
            out.buf[0] = 0;
        }

        Ok(out)
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut left = Vec::with_capacity(self.bin_size());
        let mut right = Vec::with_capacity(self.bin_size());
        self.write_bin(&mut left).expect("OOM");
        other.write_bin(&mut right).expect("OOM");
        left.cmp(&right)
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix_len = self.intg % DIG_PER_DEC;
        let suffix_len = self.frac % DIG_PER_DEC;

        let mut intg = self.intg;
        if self.sign {
            '-'.fmt(f)?;
        }

        let mut i = 0;

        while i < self.buf.len() && intg > 0 {
            let x = self.buf[i];
            i += 1;
            if prefix_len > 0 && i == 1 {
                x.fmt(f)?;
                intg -= prefix_len;
            } else {
                if i == 1 {
                    x.fmt(f)?;
                } else {
                    write!(f, "{:09}", x)?;
                }
                intg -= DIG_PER_DEC;
            }
        }

        if self.intg == 0 {
            '0'.fmt(f)?;
        }

        if i < self.buf.len() {
            '.'.fmt(f)?;
        }

        while i < self.buf.len() {
            let mut x = self.buf[i];
            i += 1;
            if i == self.buf.len() && suffix_len > 0 {
                x /= POWERS_10[DIG_PER_DEC - suffix_len];
                write!(&mut *f, "{:0width$}", x, width = suffix_len)?;
            } else {
                write!(f, "{:09}", x)?;
            }
        }

        Ok(())
    }
}

impl FromStr for Decimal {
    type Err = ParseDecimalError;
    fn from_str(mut from: &str) -> Result<Self, Self::Err> {
        let mut out = Decimal::default();

        from = from.trim();

        if from.is_empty() {
            return Err(ParseDecimalError);
        }

        // Skip leading zeros.
        while from.starts_with("00") {
            from = &from[1..];
        }

        if from.starts_with('-') {
            out.sign = true;
            from = &from[1..];
        } else if from.starts_with('+') {
            from = &from[1..];
        }

        let point_idx = from.find('.').unwrap_or(from.len());
        let (mut integral, mut fractional) = from.split_at(point_idx);
        fractional = fractional.get(1..).unwrap_or(fractional);

        out.intg = integral.len();
        out.frac = fractional.len();

        if out.intg + out.frac == 0 {
            return Err(ParseDecimalError);
        }

        if integral.bytes().any(|x| !x.is_ascii_digit())
            || fractional.bytes().any(|x| !x.is_ascii_digit())
        {
            return Err(ParseDecimalError);
        }

        let mut prefix_len = integral.len() % DIG_PER_DEC;
        if prefix_len == 0 {
            prefix_len = DIG_PER_DEC;
        }
        while !integral.is_empty() {
            let prefix = &integral[..prefix_len];
            let x: i32 = prefix.parse().expect("should not fail");
            out.buf.push(x);
            integral = &integral[prefix_len..];
            prefix_len = DIG_PER_DEC;
        }

        while !fractional.is_empty() {
            let len = std::cmp::min(DIG_PER_DEC, fractional.len());
            let prefix = &fractional[..len];
            let mut x: i32 = prefix.parse().expect("should not fail");
            if len < DIG_PER_DEC {
                x *= POWERS_10[DIG_PER_DEC - len];
            }
            out.buf.push(x);
            fractional = &fractional[len..];
        }

        if out.buf.iter().all(|x| *x == 0) {
            out.sign = false;
        }

        Ok(out)
    }
}

/// Returns binary representation size (in bytes) for given precision and scale.
#[inline]
pub fn decimal_bin_size(precision: usize, scale: usize) -> usize {
    let intg = precision - scale;
    let intg0 = intg / DIG_PER_DEC;
    let frac0 = scale / DIG_PER_DEC;
    let intg0x = intg - intg0 * DIG_PER_DEC;
    let frac0x = scale - frac0 * DIG_PER_DEC;

    intg0 * size_of::<Digit>()
        + DIG_TO_BYTES[intg0x] as usize
        + frac0 * size_of::<Digit>()
        + DIG_TO_BYTES[frac0x] as usize
}
