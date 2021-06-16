// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functions related to Materialize's numeric type, which is largely a wrapper
//! around [`rust-dec`].
//!
//! [`rust-dec`]: https://github.com/MaterializeInc/rust-dec/

use std::convert::TryFrom;

use anyhow::bail;
use dec::{Context, Decimal};
use lazy_static::lazy_static;

/// The maximum number of digits expressable in an APD.
pub const APD_DATUM_WIDTH: usize = 13;
pub const APD_DATUM_MAX_PRECISION: usize = APD_DATUM_WIDTH * 3;
pub type Apd = Decimal<APD_DATUM_WIDTH>;
// Generating negative values' two's complement requires an additional digit of
// precision.
pub type ApdTwosComp = Decimal<{ APD_DATUM_WIDTH + 1 }>;

pub const APD_AGG_WIDTH: usize = 27;
pub const APD_AGG_MAX_PRECISION: usize = APD_AGG_WIDTH * 3;
pub type ApdAgg = Decimal<APD_AGG_WIDTH>;

lazy_static! {
    static ref CX_DATUM: Context<Apd> = {
        let mut cx = Context::<Apd>::default();
        cx.set_max_exponent(isize::try_from(APD_DATUM_MAX_PRECISION - 1).unwrap())
            .unwrap();
        cx.set_min_exponent(-(isize::try_from(APD_DATUM_MAX_PRECISION).unwrap()))
            .unwrap();
        cx
    };
    static ref CX_AGG: Context<ApdAgg> = {
        let mut cx = Context::<ApdAgg>::default();
        cx.set_max_exponent(isize::try_from(APD_AGG_MAX_PRECISION - 1).unwrap())
            .unwrap();
        cx.set_min_exponent(-(isize::try_from(APD_AGG_MAX_PRECISION).unwrap()))
            .unwrap();
        cx
    };
    static ref UNIT_SPLITTER_TWOS_COMP: ApdTwosComp = {
        let mut cx = ApdTwosComp::twos_comp_cx();
        // 1 << 128
        cx.from_u128(170141183460469231731687303715884105728u128)
    };
    static ref UNIT_SPLITTER_AGG: ApdAgg = {
        let mut cx = cx_agg();
        // 1 << 128
        cx.from_u128(170141183460469231731687303715884105728u128)
    };
    // Equivalent to
    static ref INVERTER_TWOS_COMP: ApdTwosComp = {
        let mut cx = ApdTwosComp::twos_comp_cx();
        // 1 << 136 or 2 ^ (17 * 8)
        cx.parse("87112285931760246646623899502532662132736").unwrap()
    };
    static ref INVERTER_AGG: ApdAgg = {
        let mut cx = cx_agg();
        // 1 << 264 or 2 ^ (33 * 8)
        cx.parse("29642774844752946028434172162224104410437116074403984394101141506025761187823616").unwrap()
    };
}

/// Traits to generalize converting [`Decimal`] values to and from their
/// coefficients' two's complements.
///
/// Note that [`Apd`] does not implement this trait, because generating its
/// two's complement requires an additional digit of precision, which is why we
/// have [`ApdTwosComp`].
pub trait TwosCompDec<const N: usize> {
    const TWOS_COMPLEMENT_BYTE_WIDTH: usize;
    // Convenience method for generating appropriate default contexts.
    fn twos_comp_cx() -> Context<Decimal<N>>;
    // Provides value to break decimal into units of `i128`s
    fn unit_splitter() -> &'static Decimal<N>;
    // Provides value which you can add to negative number to return a number
    // whose unsigned representation is equivalent to the negative number's
    // signed representation.4
    fn inverter() -> &'static Decimal<N>;
}

impl TwosCompDec<{ APD_DATUM_WIDTH + 1 }> for ApdTwosComp {
    const TWOS_COMPLEMENT_BYTE_WIDTH: usize = 17;
    fn twos_comp_cx() -> Context<ApdTwosComp> {
        Context::<ApdTwosComp>::default()
    }
    fn unit_splitter() -> &'static ApdTwosComp {
        &UNIT_SPLITTER_TWOS_COMP
    }
    fn inverter() -> &'static ApdTwosComp {
        &INVERTER_TWOS_COMP
    }
}

impl TwosCompDec<APD_AGG_WIDTH> for ApdAgg {
    const TWOS_COMPLEMENT_BYTE_WIDTH: usize = 33;
    fn twos_comp_cx() -> Context<ApdAgg> {
        CX_AGG.clone()
    }
    fn unit_splitter() -> &'static ApdAgg {
        &UNIT_SPLITTER_AGG
    }
    fn inverter() -> &'static Decimal<APD_AGG_WIDTH> {
        &INVERTER_AGG
    }
}

/// Returns a new context appropriate for operating on APD datums.
pub fn cx_datum() -> Context<Apd> {
    CX_DATUM.clone()
}

/// Returns a new context appropriate for operating on APD aggregates.
pub fn cx_agg() -> Context<ApdAgg> {
    CX_AGG.clone()
}

fn twos_complement_be_to_u128(input: &[u8]) -> Result<u128, anyhow::Error> {
    assert!(input.len() <= 16);
    let mut buf = [0; 16];
    buf[16 - input.len()..16].copy_from_slice(input);
    Ok(u128::from_be_bytes(buf))
}

/// Parses am `i128` from a buffer storing the two's complement representation
/// of a set of bytes in big-endian byte order.
fn twos_complement_be_to_i128(input: &[u8]) -> Result<i128, anyhow::Error> {
    assert!(input.len() <= 16);
    let mut buf = [0; 16];
    for (i, digit) in input.iter().rev().enumerate() {
        buf[i] = *digit;
    }
    let mut v = i128::from_le_bytes(buf);
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
        v -= 2_i128.pow((input.len() * 8) as u32);
    }

    Ok(v)
}

pub fn apd_to_twos_complement_be(apd: Apd) -> [u8; ApdTwosComp::TWOS_COMPLEMENT_BYTE_WIDTH] {
    let mut buf = [0; ApdTwosComp::TWOS_COMPLEMENT_BYTE_WIDTH];
    let mut cx = ApdTwosComp::twos_comp_cx();
    let mut apd = cx.to_width(apd);

    // Ensure `apd` is a canonical coefficient.
    if apd.exponent() < 0 {
        let s = ApdTwosComp::from(-apd.exponent());
        cx.scaleb(&mut apd, &s);
    }

    apd_to_twos_complement_inner::<ApdTwosComp, { APD_DATUM_WIDTH + 1 }>(apd, &mut cx, &mut buf);
    buf
}

/// Provides a two's complement version of an APD appropriate for cases where a
/// [`ScalarType::APD`]'s scale is unknown. This creates a value with 78 digits
/// of precision and a scale of 39, which can definitively represent all valid
/// [`Apd`] values.
pub fn apd_to_twos_complement_wide(apd: Apd) -> [u8; ApdAgg::TWOS_COMPLEMENT_BYTE_WIDTH] {
    let mut buf = [0; ApdAgg::TWOS_COMPLEMENT_BYTE_WIDTH];
    let mut cx = ApdAgg::twos_comp_cx();
    let mut d = cx.to_width(apd);
    let mut scaler = ApdAgg::from(APD_DATUM_MAX_PRECISION);
    cx.neg(&mut scaler);
    // Shape `d` so that its exponent is -APD_DATUM_MAX_PRECISION
    cx.rescale(&mut d, &scaler);
    cx.abs(&mut scaler);
    // Adjust `d` so it is a canonical coefficient, i.e. its exact value can be
    // recovered by setting its exponent to -39.
    cx.scaleb(&mut d, &scaler);

    apd_to_twos_complement_inner::<ApdAgg, APD_AGG_WIDTH>(d, &mut cx, &mut buf);
    buf
}

pub fn apd_to_twos_complement_inner<D: TwosCompDec<N>, const N: usize>(
    mut d: Decimal<N>,
    cx: &mut Context<Decimal<N>>,
    buf: &mut [u8],
) {
    const S: usize = std::mem::size_of::<u128>();

    // Adjust negative values to be writable as series of `u128`.
    if d.is_negative() {
        cx.add(&mut d, D::inverter());
    }

    // Values have all been made into canonical coefficients.
    assert!(d.exponent() >= 0);

    let mut buf_cursor = 0;
    while !d.is_zero() {
        let mut w = d.clone();
        // Take the remainder; this represents one of our "units" to take the coefficient of, i.e. d & i128::MAX
        cx.rem(&mut w, D::unit_splitter());

        // Take the `u128` version of the coefficient, which will always be what
        // we want given that we adjusted negative values to have an unsigned
        // integer representation.
        let c = w.coefficient::<u128>().unwrap();

        // Determine the width of the coefficient we want to take, i.e. the full
        // coefficient or a part of it to fill the buffer.
        let width = std::cmp::min(buf_cursor + S, D::TWOS_COMPLEMENT_BYTE_WIDTH);

        // We're putting less significant bytes at index 0, which is little endian.
        buf[buf_cursor..width].copy_from_slice(&c.to_le_bytes()[0..width - buf_cursor]);
        // Advance cursor; ok that it will go past buffer on final + 1th iteration.
        buf_cursor += S;

        // Take the quotient to represent the next unit, i.e. d >> 128
        cx.div_integer(&mut d, D::unit_splitter());
    }

    // Convert from little endian to big endian.
    buf.reverse();
}

pub fn twos_complement_be_to_apd(input: &[u8]) -> Result<Apd, anyhow::Error> {
    let mut cx = cx_datum();
    Ok(if input.len() <= 17 {
        let n = twos_complement_be_to_apd_inner::<ApdTwosComp, { APD_DATUM_WIDTH + 1 }>(input)?;
        cx.to_width(n)
    } else {
        let n = twos_complement_be_to_apd_inner::<ApdAgg, APD_AGG_WIDTH>(input)?;
        cx.to_width(n)
    })
}

/// Parses a buffer of two's complement digits in big-endian order and converts
/// them to [`Decimal<N>`].
///
/// Note that in cases where you want to use this value as a [`Datum::APD`], you
/// will need to use `cx_datum()`'s `to_width` method.
pub fn twos_complement_be_to_apd_inner<D: TwosCompDec<N>, const N: usize>(
    input: &[u8],
) -> Result<Decimal<N>, anyhow::Error> {
    const S: usize = std::mem::size_of::<u128>();

    // Only the first bytes should be parsed as signed, so take as few of those
    // as will let us handle the rest as unsigned values.
    let mut head = input.len() % S;
    if head == 0 {
        head = S;
    }
    let i = twos_complement_be_to_i128(&input[0..head]).unwrap();

    let mut cx = D::twos_comp_cx();
    let mut d = cx.from_i128(i);

    for c in input[head..].chunks(S) {
        assert_eq!(c.len(), S);
        // essentially d << 128
        cx.mul(&mut d, D::unit_splitter());
        let i = twos_complement_be_to_u128(&c).unwrap();
        let i = cx.from_u128(i);
        cx.add(&mut d, &i);
    }

    if cx.status().inexact() {
        bail!("value exceeds Apd")
    } else if cx.status().any() {
        bail!("unexpected status {:?}", cx.status());
    }
    Ok(d)
}

#[test]
fn test_twos_complement_roundtrip() {
    fn inner(s: &str) {
        let mut cx = cx_datum();
        let d = cx.parse(s).unwrap();
        let e = d.exponent();
        let b = apd_to_twos_complement_be(d.clone());
        let x = twos_complement_be_to_apd(&b).unwrap();
        let mut x = cx.to_width(x);
        x.set_exponent(e);
        assert_eq!(d, x);
    }
    inner("0");
    inner("0.000000000000000000000000000000000012345");
    inner("0.123456789012345678901234567890123456789");
    inner("1.000000000000000000000000000000000000000");
    inner("1");
    inner("2");
    inner("170141183460469231731687303715884105727");
    inner("170141183460469231731687303715884105728");
    inner("12345678901234567890.1234567890123456789");
    inner("999999999999999999999999999999999999999");
    inner("-0.000000000000000000000000000000000012345");
    inner("-0.123456789012345678901234567890123456789");
    inner("-1.000000000000000000000000000000000000000");
    inner("-1");
    inner("-2");
    inner("-170141183460469231731687303715884105727");
    inner("-170141183460469231731687303715884105728");
    inner("-12345678901234567890.1234567890123456789");
    inner("-999999999999999999999999999999999999999");
}

#[test]
fn test_wide_twos_complement_roundtrip() {
    fn inner(s: &str) {
        let mut cx = cx_datum();
        let d = cx.parse(s).unwrap();
        let b = apd_to_twos_complement_wide(d.clone());
        let mut x = twos_complement_be_to_apd(&b).unwrap();
        x.set_exponent(-(APD_DATUM_MAX_PRECISION as i32));
        assert_eq!(d, cx.to_width(x));
    }
    inner("0");
    inner("0.000000000000000000000000000000000012345");
    inner("0.123456789012345678901234567890123456789");
    inner("1.000000000000000000000000000000000000000");
    inner("1");
    inner("2");
    inner("170141183460469231731687303715884105727");
    inner("170141183460469231731687303715884105728");
    inner("12345678901234567890.1234567890123456789");
    inner("999999999999999999999999999999999999999");
    inner("-0.000000000000000000000000000000000012345");
    inner("-0.123456789012345678901234567890123456789");
    inner("-1.000000000000000000000000000000000000000");
    inner("-1");
    inner("-2");
    inner("-170141183460469231731687303715884105727");
    inner("-170141183460469231731687303715884105728");
    inner("-12345678901234567890.1234567890123456789");
    inner("-999999999999999999999999999999999999999");
}

/// Returns `n`'s precision, i.e. the total number of digits represented by `n`
/// in standard notation not including a zero in the "one's place" in (-1,1).
pub fn get_precision<const N: usize>(n: &Decimal<N>) -> u32 {
    let e = n.exponent();
    if e >= 0 {
        // Positive exponent
        n.digits() + u32::try_from(e).unwrap()
    } else {
        // Negative exponent
        let d = n.digits();
        let e = u32::try_from(e.abs()).unwrap();
        // Precision is...
        // - d if decimal point splits numbers
        // - e if e dominates number of digits
        std::cmp::max(d, e)
    }
}

/// Returns `n`'s scale, i.e. the number of digits used after the decimal point.
pub fn get_scale(n: &Apd) -> u8 {
    let exp = n.exponent();
    if exp >= 0 {
        0
    } else {
        u8::try_from(-exp).unwrap()
    }
}

/// Ensures [`Apd`] values are:
/// - Within `Apd`'s max precision ([`APD_DATUM_MAX_PRECISION`]), or errors if not.
/// - Never possible but invalid representations (i.e. never -Nan or -0).
///
/// Should be called after any operation that can change an [`Apd`]'s scale or
/// generate negative values (except addition and subtraction).
pub fn munge_apd(n: &mut Apd) -> Result<(), anyhow::Error> {
    rescale_within_max_precision(n)?;
    if (n.is_zero() || n.is_nan()) && n.is_negative() {
        cx_datum().neg(n);
    }
    Ok(())
}

/// Rescale's `n` to fit within [`Apd`]'s max precision or error if not
/// possible.
fn rescale_within_max_precision(n: &mut Apd) -> Result<(), anyhow::Error> {
    let current_precision = get_precision(n);
    if current_precision > APD_DATUM_MAX_PRECISION as u32 {
        if n.exponent() < 0 {
            let precision_diff = current_precision - APD_DATUM_MAX_PRECISION as u32;
            let current_scale = get_scale(n);
            let scale_diff = current_scale - u8::try_from(precision_diff).unwrap();
            rescale(n, scale_diff)?;
        } else {
            bail!(
                "APD value {} exceed maximum precision {}",
                n,
                APD_DATUM_MAX_PRECISION
            )
        }
    }
    Ok(())
}

/// Rescale `n` as an `OrderedDecimal` with the described scale, or error if:
/// - Rescaling exceeds max precision
/// - `n` requires > [`APD_DATUM_MAX_PRECISION`] - `scale` digits of precision
///   left of the decimal point
pub fn rescale(n: &mut Apd, scale: u8) -> Result<(), anyhow::Error> {
    let mut cx = cx_datum();
    cx.rescale(n, &Apd::from(-i32::from(scale)));
    if cx.status().invalid_operation() || get_precision(n) > APD_DATUM_MAX_PRECISION as u32 {
        bail!(
            "APD value {} exceed maximum precision {}",
            n,
            APD_DATUM_MAX_PRECISION
        )
    }

    Ok(())
}
