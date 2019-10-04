// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A specialized, high-performance decimal type.
//!
//! This module has a somewhat unusual design that is tightly coupled to
//! Materialize's requirements. It is unlikely to be useful in other contexts
//! without significant refactoring. If you're looking for a self-contained
//! arbitrary precision decimal library in Rust, you probably want something
//! like [bigdecimal].
//!
//! The design supports 128-bit fixed point decimal arithmetic. Wikipedia has a
//! good overview of [fixed-point arithmetic], if you're new to the subject. The
//! basic idea is to scale decimal numbers up until no fractional component
//! remains. Arithmetic operations are then simple integer arithmetic
//! operations.
//!
//! For example, suppose we want to represent the decimal number 123.45. This
//! number is said to have precision 5, because it has five digits in total, and
//! scale 2, because it has two digits after the decimal point. We can represent
//! this number as an integer by multiplying it by 100, resulting in 12345. We
//! call this resulting number the "significand."
//!
//! Arithmetic operations can be performed directly on the significand using
//! simple integer arithmetic. For example, suppose we want to add 1.55 to
//! 123.45. The corresponding significands are 155 and 12345, respectively, and
//! we can sum the significands directly, resulting in 12500. To display the
//! result, we divide by 100 (the inverse of the multiplication by 100 that we
//! performed to construct the significands originally), resulting in the
//! correct decimal result 125.00.
//!
//! Note that multiplication and division require more care, to account for
//! shifting scales, but the basic idea of relying on integral operations on the
//! significand still holds. See the Wikipedia article for details if you're
//! curious.
//!
//! This module does *not* provide a complete implementation of fixed-point
//! decimal arithmetic. The types here instead provide interoperability, i.e.,
//! parsing decimals from other systems and printing decimals to strings.
//!
//! [bigdecimal]: https://crates.io/crates/bigdecimal
//! [fixed-point arithmetic]: https://en.wikipedia.org/wiki/Fixed-point_arithmetic

use failure::bail;
use serde::{Deserialize, Serialize};

use std::cmp::PartialEq;
use std::fmt;
use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::iter::Sum;
use std::ops::{Add, AddAssign, Div, Mul, Neg, Rem, Sub};

/// The significand of a decimal number with up to 38 digits of precision.
///
/// `Significand`s are unintepretable without their corresponding scale, which
/// indicates the location of the decimal point. You may be interested in the
/// [`Decimal`] type, which bundles the significand together with its scale.
///
/// The advantage of the `Significand` type is that it uses less memory by not
/// redundantly storing the scale if the scale is known to be the same across
/// a large collection of decimals.
///
/// Note that arithmetic on significands is raw 128-bit integer arithmetic.
/// Multiplying two significands, for example, will simply multiply the two
/// underlying 128-bit integers. It is up to the caller to interpret the result
/// semantically, e.g., accounting for the new output scale, if decimal
/// multiplication was desired.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Significand(i128);

/// The maximum precision (i.e., number of digits) permitted in a
/// [`Significand`]. Note that this includes the total number of digits,
/// including the digits both before and after the decimal point.
///
/// This number was chosen so that significands fit neatly in an i128.
pub const MAX_DECIMAL_PRECISION: u8 = 38;

impl Significand {
    /// Constructs a new `Significand` from an `i128`.
    pub fn new(d: i128) -> Significand {
        Significand(d)
    }

    /// Parses a `Significand` from a buffer storing the two's complement
    /// representation of the significand in big-endian byte order.
    pub fn from_twos_complement_be(input: &[u8]) -> Result<Significand, failure::Error> {
        if input.len() > 16 {
            bail!("decimal exceeds maximum precision")
        }
        let mut buf = [0; 16];
        for (i, digit) in input.iter().rev().enumerate() {
            buf[i] = *digit;
        }
        let mut significand = i128::from_le_bytes(buf);
        if !input.is_empty() && input.len() < 16 && ((input[0] & 0x80) != 0) {
            // This is tricky. In two's-complement representation, the weight of
            // the high order digit is negative. If we're filling out the entire
            // i128, then i128::from_le_bytes has already accounted for this.
            // Otherwise, if the high-order bit in this particular decimal is
            // set, we've incorrectly used it to contribute a positive weight.
            //
            // For example, consider the one-byte number 0xba. In one-byte two's
            // complement, this represents -70:
            //
            //     1(-2^7) + 0(2^6) + 1(2^5) + 1(2^4) + 1(2^3) + 0(2^2) + 1(2^1) + 0(2^0)
            //
            // We'll, however, have interpreted it as 186, because we'll have
            // assigned the highest bit a weight of 128, instead of -128. To
            // compensate, we subtract twice the weight of the highest digit--
            // in the example, 256.
            significand -= 2_i128.pow((input.len() * 8) as u32);
        }
        Ok(Significand(significand))
    }

    /// Returns the underlying `i128`.
    pub fn as_i128(&self) -> i128 {
        self.0
    }

    /// Constructs a [`Decimal`] by imbuing this `Significand` with the
    /// specified `scale`.
    pub fn with_scale(self, scale: u8) -> Decimal {
        Decimal {
            significand: self.0,
            scale,
        }
    }
}

impl Add<Significand> for Significand {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl<T> Add<T> for Significand
where
    i128: Add<T, Output = i128>,
{
    type Output = Self;

    fn add(self, other: T) -> Self {
        Self(self.0 + other)
    }
}

impl AddAssign<Significand> for Significand {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}

impl<T> AddAssign<T> for Significand
where
    i128: AddAssign<T>,
{
    fn add_assign(&mut self, other: T) {
        self.0 += other;
    }
}

impl Sub<Significand> for Significand {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl<T> Sub<T> for Significand
where
    i128: Sub<T, Output = i128>,
{
    type Output = Self;

    fn sub(self, other: T) -> Self {
        Self(self.0 - other)
    }
}

impl Mul<Significand> for Significand {
    type Output = Self;

    fn mul(self, other: Self) -> Self {
        Self(self.0 * other.0)
    }
}

impl<T> Mul<T> for Significand
where
    i128: Mul<T, Output = i128>,
{
    type Output = Self;

    fn mul(self, other: T) -> Self {
        Self(self.0 * other)
    }
}

impl Div<Significand> for Significand {
    type Output = Self;

    fn div(self, other: Self) -> Self {
        Self(self.0 / other.0)
    }
}

impl<T> Div<T> for Significand
where
    i128: Div<T, Output = i128>,
{
    type Output = Self;

    fn div(self, other: T) -> Self {
        Self(self.0 / other)
    }
}

impl Rem<Significand> for Significand {
    type Output = Self;

    fn rem(self, other: Self) -> Self {
        Self(self.0 % other.0)
    }
}

impl<T> Rem<T> for Significand
where
    i128: Rem<T, Output = i128>,
{
    type Output = Self;

    fn rem(self, other: T) -> Self {
        Self(self.0 % other)
    }
}

impl PartialEq<Significand> for Significand {
    fn eq(&self, other: &Significand) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<i128> for Significand {
    fn eq(&self, other: &i128) -> bool {
        &self.0 == other
    }
}

impl Hash for Significand {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl Sum for Significand {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        iter.fold(Significand::new(0), Add::add)
    }
}

impl Neg for Significand {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(-self.0)
    }
}

/// A decimal number, which bundles a significand and its scale.
///
/// At present, the only useful operation that `Decimal`s support is
/// stringification. This support is thought to be complete, however; even
/// esoteric format string options, like padding characters and width, are
/// properly handled.
#[derive(Debug)]
pub struct Decimal {
    significand: i128,
    scale: u8,
}

fn rounding_downscale(v: i128, scale: usize) -> i128 {
    let factor = 10_i128.pow(scale as u32);
    v / factor + (v % factor) / (factor / 2)
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let significand = self.significand.abs();
        let factor = 10_i128.pow(u32::from(self.scale));
        let ip = significand / factor;
        let mut buf = ip.to_string();
        // NOTE(benesch): `fmt:Formatter` uses "precision" to mean "how many
        // digits to display to the right of the decimal point," which we call
        // scale.
        let desired_scale = f.precision().unwrap_or(self.scale as usize);
        if desired_scale > 0 {
            let mut fp = significand - (ip * factor);
            let mut scale = self.scale as usize;
            if desired_scale < scale {
                // The format string requests less fractional digits than
                // present. Round to the desired scale.
                fp = rounding_downscale(fp, scale - desired_scale);
                scale = desired_scale;
            }
            // The fractional digits must have all leading zeros present to be
            // correct. Consider: .07 and .7 are very different numbers.
            write!(buf, ".{:0width$}", fp, width = scale)?;
            for _ in scale..desired_scale {
                // The format string requests more fractional digits than
                // present. Fill in the missing digits as zeros.
                buf.push('0');
            }
        }
        let nonneg = self.significand >= 0 || (ip == 0 && desired_scale == 0);
        f.pad_integral(nonneg, "", &buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_decimal() {
        let pos = Significand::new(12345).with_scale(2);
        assert_eq!(format!("{}", pos), "123.45");
        assert_eq!(format!("{:.2}", pos), "123.45");
        assert_eq!(format!("{:.3}", pos), "123.450");
        assert_eq!(format!("{:.7}", pos), "123.4500000");
        assert_eq!(format!("{:+}", pos), "+123.45");
        assert_eq!(format!("{:8}", pos), "  123.45");
        assert_eq!(format!("{:^8}", pos), " 123.45 ");
        assert_eq!(format!("{:z^8}", pos), "z123.45z");
        assert_eq!(format!("{:0}", pos), "123.45");
        assert_eq!(format!("{:08}", pos), "00123.45");

        let neg = Significand::new(-12345).with_scale(2);
        assert_eq!(format!("{}", neg), "-123.45");
        assert_eq!(format!("{:z^9}", neg), "z-123.45z");
        assert_eq!(format!("{:0}", neg), "-123.45");
        assert_eq!(format!("{:09}", neg), "-00123.45");

        assert_eq!(format!("{}", Significand::new(0).with_scale(0)), "0");
        assert_eq!(format!("{}", Significand::new(0).with_scale(5)), "0.00000");
        assert_eq!(format!("{:.0}", Significand::new(-10).with_scale(5)), "0");
        assert_eq!(format!("{}", Significand::new(42).with_scale(0)), "42");
        assert_eq!(format!("{}", Significand::new(70).with_scale(2)), "0.70");
        assert_eq!(format!("{}", Significand::new(7).with_scale(2)), "0.07");
        assert_eq!(format!("{:.1}", Significand::new(45).with_scale(2)), "0.5");
        assert_eq!(
            format!("{:.1}", Significand::new(-45).with_scale(2)),
            "-0.5"
        );
        assert_eq!(format!("{:.1}", Significand::new(46).with_scale(2)), "0.5");
        assert_eq!(
            format!("{:.1}", Significand::new(-46).with_scale(2)),
            "-0.5"
        );
    }
}
