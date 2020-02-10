// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

use failure::{bail, format_err};
use serde::{Deserialize, Serialize};

use std::cmp::PartialEq;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::Sum;
use std::ops::{Add, AddAssign, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

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
/// At present, the only useful operations that `Decimal`s support are
/// conversions to and from strings. This support is thought to be complete,
/// however; even esoteric format string options, like padding characters and
/// width, are properly handled.
#[derive(Debug)]
pub struct Decimal {
    significand: i128,
    scale: u8,
}

impl Decimal {
    /// Returns the significand of the decimal.
    pub fn significand(&self) -> i128 {
        self.significand
    }

    /// Returns the scale of the decimal.
    pub fn scale(&self) -> u8 {
        self.scale
    }

    /// Floor a decimal number
    ///
    /// ```
    /// use repr::decimal::Decimal;
    ///
    /// let d: Decimal = "100.11".parse().unwrap();
    /// assert_eq!(&d.floor().to_string(), "100.00");
    ///
    /// let d: Decimal = "99.0".parse().unwrap();
    /// assert_eq!(&d.floor().to_string(), "99.0");
    ///
    /// let nd: Decimal = "-40.1".parse().unwrap();
    /// assert_eq!(nd.floor().to_string(), "-41.0");
    /// ```
    pub fn floor(&self) -> Decimal {
        let factor = 10_i128.pow(self.scale as u32);
        let int = self.significand / factor;
        let frac = self.significand % factor;
        let sub = if int < 0 && frac != 0 { 1 } else { 0 };

        Decimal {
            significand: (int - sub) * factor,
            scale: self.scale,
        }
    }

    /// Ceil a decimal number
    ///
    /// ```
    /// use repr::decimal::Decimal;
    ///
    /// let d: Decimal = "100.11".parse().unwrap();
    /// assert_eq!(&d.ceil().to_string(), "101.00");
    ///
    /// let d: Decimal = "99.0".parse().unwrap();
    /// assert_eq!(&d.ceil().to_string(), "99.0");
    ///
    /// let nd: Decimal = "-40.1".parse().unwrap();
    /// assert_eq!(nd.ceil().to_string(), "-40.0")
    /// ```
    pub fn ceil(&self) -> Decimal {
        let factor = 10_i128.pow(self.scale as u32);
        let int = self.significand / factor;
        let frac = self.significand % factor;
        let add = if int > 0 && frac != 0 { 1 } else { 0 };

        Decimal {
            significand: (add + int) * factor,
            scale: self.scale,
        }
    }
}

impl FromStr for Decimal {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut significand: i128 = 0;
        let mut precision = 0;
        let mut scale: u8 = 0;
        let mut seen_decimal = false;
        let mut negative = false;
        let mut seen_e_notation = false;
        let mut e_exponent: u8 = 0;
        let mut e_negative = false;

        let mut z = s.chars().peekable();

        if let Some('-') = z.peek() {
            // Consume the negative sign.
            z.next();
            negative = true;
        }

        while let Some(&ch) = z.peek() {
            match ch {
                '0'..='9' => {
                    let digit = ch
                        .to_digit(10)
                        .ok_or_else(|| format_err!("invalid digit in numeric literal: {}", s))?;
                    precision += 1;
                    if seen_decimal {
                        scale += 1;
                    }
                    significand = significand
                        .checked_mul(10)
                        .ok_or_else(|| format_err!("numeric literal overflows i128: {}", s))?;
                    significand = significand
                        .checked_add(i128::from(digit))
                        .ok_or_else(|| format_err!("numeric literal overflows i128: {}", s))?;
                }
                '.' => {
                    if !seen_decimal {
                        seen_decimal = true;
                    } else {
                        bail!("multiple decimal points in numeric literal: {}", s)
                    }
                }
                _ => break,
            }
            z.next();
        }

        // Check for e-notation.
        // Note that 'e' is changed to 'E' during parsing step.
        if let Some('E') = z.peek() {
            // Consume the e-notation signifier.
            z.next();
            seen_e_notation = true;
            if let Some('-') = z.peek() {
                // Consume the negative sign.
                z.next();
                e_negative = true;
            }
            while let Some(&ch) = z.peek() {
                match ch {
                    '0'..='9' => {
                        let digit = ch.to_digit(10).ok_or_else(|| {
                            format_err!("invalid digit in numeric literal: {}", s)
                        })?;
                        e_exponent = e_exponent.checked_mul(10).ok_or_else(|| {
                            format_err!("exponent in e-notation overflows u8: {}", s)
                        })?;
                        e_exponent = e_exponent.checked_add(digit as u8).ok_or_else(|| {
                            format_err!("exponent in e-notation overflows u8: {}", s)
                        })?;
                    }
                    _ => break,
                }
                z.next();
            }
        }

        if z.peek().is_some() {
            bail!("malformed numeric literal: {}", s);
        }

        if precision > MAX_DECIMAL_PRECISION {
            bail!("numeric literal exceeds maximum precision: {}", s);
        }
        if negative {
            significand *= -1;
        }
        if seen_e_notation {
            if e_negative {
                scale = scale.checked_add(e_exponent).ok_or_else(|| {
                    format_err!("numeric literal exceeds maximum precision: {}", s)
                })?;
                if scale > MAX_DECIMAL_PRECISION {
                    bail!("numeric literal exceeds maximum precision: {}", s);
                }
            } else if scale > e_exponent {
                scale -= e_exponent;
            } else {
                e_exponent -= scale;
                scale = 0;
                let p = 10_i128.checked_pow(e_exponent as u32).ok_or_else(|| {
                    format_err!(
                        "exponent in numeric literal {} overflows i128: 10^{}",
                        s,
                        e_exponent
                    )
                })?;

                significand = significand
                    .checked_mul(p)
                    .ok_or_else(|| format_err!("numeric literal overflows i128: {}", s))?;
            }
        }
        Ok(Decimal { scale, significand })
    }
}

fn rounding_downscale(v: i128, scale: usize) -> i128 {
    if scale == 0 {
        v
    } else {
        let factor = 10_i128.pow(scale as u32);
        v / factor + (v % factor) / (factor / 2)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // NOTE(benesch): `fmt:Formatter` uses "precision" to mean "how many
        // digits to display to the right of the decimal point," which we call
        // scale.
        let desired_scale = f.precision().unwrap_or(self.scale as usize);
        // A desired scale of zero is special because it requires rounding the
        // integral part. For all other desired scales, the rounding instead
        // applies to the fractional part.
        if desired_scale == 0 {
            let ip = rounding_downscale(self.significand, self.scale as usize);
            f.pad_integral(ip >= 0, "", &ip.abs().to_string())
        } else {
            let significand = self.significand.abs();
            let factor = 10_i128.pow(u32::from(self.scale));
            let ip = significand / factor;
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
            let mut buf = format!("{}.{:0width$}", ip, fp, width = scale);
            for _ in scale..desired_scale {
                // The format string requests more fractional digits than
                // present. Fill in the missing digits as zeros.
                buf.push('0');
            }
            f.pad_integral(self.significand >= 0, "", &buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_decimal() {
        let pos = Significand::new(12345).with_scale(2);
        let parsed: Decimal = "123.45".parse().unwrap();
        assert_eq!(pos.significand(), parsed.significand());
        assert_eq!(pos.scale(), parsed.scale());

        let pos = Decimal::from_str("12345E-2").unwrap();
        assert_eq!(pos.significand(), parsed.significand());
        assert_eq!(pos.scale(), parsed.scale());

        let pos = Significand::new(-12345).with_scale(2);
        let parsed: Decimal = "-123.45".parse().unwrap();
        assert_eq!(pos.significand(), parsed.significand());
        assert_eq!(pos.scale(), parsed.scale());
    }

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
        assert_eq!(format!("{:.0}", Significand::new(-6).with_scale(1)), "-1");
        assert_eq!(format!("{:.0}", Significand::new(-19).with_scale(1)), "-2");
        assert_eq!(format!("{:.0}", Significand::new(19).with_scale(1)), "2");
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
