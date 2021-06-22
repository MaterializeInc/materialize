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
    static ref U128_SPLITTER_DATUM: Apd = {
        let mut cx = Apd::context();
        // 1 << 128
        cx.parse("340282366920938463463374607431768211456").unwrap()
    };
    static ref U128_SPLITTER_AGG: ApdAgg = {
        let mut cx = ApdAgg::context();
        // 1 << 128
        cx.parse("340282366920938463463374607431768211456").unwrap()
    };
}

/// Traits to generalize converting [`Decimal`] values to and from their
/// coefficients' two's complements.
pub trait Dec<const N: usize> {
    // The number of bytes required to represent the min/max value of a decimal
    // using two's complement.
    const TWOS_COMPLEMENT_BYTE_WIDTH: usize;
    // Convenience method for generating appropriate default contexts.
    fn context() -> Context<Decimal<N>>;
    // Provides value to break decimal into units of `u128`s for binary
    // encoding/decoding.
    fn u128_splitter() -> &'static Decimal<N>;
}

impl Dec<APD_DATUM_WIDTH> for Apd {
    const TWOS_COMPLEMENT_BYTE_WIDTH: usize = 17;
    fn context() -> Context<Apd> {
        CX_DATUM.clone()
    }
    fn u128_splitter() -> &'static Apd {
        &U128_SPLITTER_DATUM
    }
}

impl Dec<APD_AGG_WIDTH> for ApdAgg {
    const TWOS_COMPLEMENT_BYTE_WIDTH: usize = 33;
    fn context() -> Context<ApdAgg> {
        CX_AGG.clone()
    }
    fn u128_splitter() -> &'static ApdAgg {
        &U128_SPLITTER_AGG
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

fn twos_complement_be_to_u128(input: &[u8]) -> u128 {
    assert!(input.len() <= 16);
    let mut buf = [0; 16];
    buf[16 - input.len()..16].copy_from_slice(input);
    u128::from_be_bytes(buf)
}

/// Using negative binary numbers can require more digits of precision than
/// [`Apd`] offers, so we need to have the option to swap bytes' signs at the
/// byte- rather than the library-level.
fn negate_twos_complement_le<'a, I>(b: I)
where
    I: Iterator<Item = &'a mut u8>,
{
    let mut seen_first_one = false;
    for i in b {
        if seen_first_one {
            *i = *i ^ 0xFF;
        } else if *i > 0 {
            seen_first_one = true;
            if i == &0x80 {
                continue;
            }
            let tz = i.trailing_zeros();
            *i = *i ^ (0xFF << tz + 1);
        }
    }
}

/// Converts an [`Apd`] into its big endian two's complement representation.
pub fn apd_to_twos_complement_be(mut apd: Apd) -> [u8; Apd::TWOS_COMPLEMENT_BYTE_WIDTH] {
    let mut buf = [0; Apd::TWOS_COMPLEMENT_BYTE_WIDTH];
    // Avro doesn't specify how to handle NaN/infinity, so we simply treat them
    // as zeroes so as to avoid erroring (encoding values is meant to be
    // infallible) and retain downstream associativity/commutativity.
    if apd.is_special() {
        return buf;
    }

    let mut cx = Apd::context();

    // Ensure `apd` is a canonical coefficient.
    if apd.exponent() < 0 {
        let s = Apd::from(-apd.exponent());
        cx.scaleb(&mut apd, &s);
    }

    apd_to_twos_complement_inner::<Apd, APD_DATUM_WIDTH>(apd, &mut cx, &mut buf);
    buf
}

/// Converts an [`Apd`] into a big endian two's complement representation where
/// the encoded value has [`APD_AGG_MAX_PRECISION`] digits and a scale of
/// [`APD_DATUM_MAX_PRECISION`].
///
/// This representation is appropriate to use in
/// contexts requiring two's complement representation but `Apd` values' scale
/// isn't known, e.g. when working with columns with an explicitly defined
/// scale.
pub fn apd_to_twos_complement_wide(apd: Apd) -> [u8; ApdAgg::TWOS_COMPLEMENT_BYTE_WIDTH] {
    let mut buf = [0; ApdAgg::TWOS_COMPLEMENT_BYTE_WIDTH];
    // Avro doesn't specify how to handle NaN/infinity, so we simply treat them
    // as zeroes so as to avoid erroring (encoding values is meant to be
    // infallible) and retain downstream associativity/commutativity.
    if apd.is_special() {
        return buf;
    }
    let mut cx = ApdAgg::context();
    let mut d = cx.to_width(apd);
    let mut scaler = ApdAgg::from(APD_DATUM_MAX_PRECISION);
    cx.neg(&mut scaler);
    // Shape `d` so that its exponent is -APD_DATUM_MAX_PRECISION
    cx.rescale(&mut d, &scaler);
    // Adjust `d` so it is a canonical coefficient, i.e. its exact value can be
    // recovered by setting its exponent to -39.
    cx.abs(&mut scaler);
    cx.scaleb(&mut d, &scaler);

    apd_to_twos_complement_inner::<ApdAgg, APD_AGG_WIDTH>(d, &mut cx, &mut buf);
    buf
}

fn apd_to_twos_complement_inner<D: Dec<N>, const N: usize>(
    mut d: Decimal<N>,
    cx: &mut Context<Decimal<N>>,
    buf: &mut [u8],
) {
    // Adjust negative values to be writable as series of `u128`.
    let is_neg = if d.is_negative() {
        cx.neg(&mut d);
        true
    } else {
        false
    };

    // Values have all been made into canonical coefficients.
    assert!(d.exponent() >= 0);

    let mut buf_cursor = 0;
    while !d.is_zero() {
        let mut w = d.clone();
        // Take the remainder; this represents one of our "units" to take the coefficient of, i.e. d & u128::MAX
        cx.rem(&mut w, D::u128_splitter());

        // Take the `u128` version of the coefficient, which will always be what
        // we want given that we adjusted negative values to have an unsigned
        // integer representation.
        let c = w.coefficient::<u128>().unwrap();

        // Determine the width of the coefficient we want to take, i.e. the full
        // coefficient or a part of it to fill the buffer.
        let e = std::cmp::min(buf_cursor + 16, D::TWOS_COMPLEMENT_BYTE_WIDTH);

        // We're putting less significant bytes at index 0, which is little endian.
        buf[buf_cursor..e].copy_from_slice(&c.to_le_bytes()[0..e - buf_cursor]);
        // Advance cursor; ok that it will go past buffer on final + 1th iteration.
        buf_cursor += 16;

        // Take the quotient to represent the next unit, i.e. d >> 128
        cx.div_integer(&mut d, D::u128_splitter());
    }

    if is_neg {
        negate_twos_complement_le(buf.iter_mut());
    }

    // Convert from little endian to big endian.
    buf.reverse();
}

pub fn twos_complement_be_to_apd(input: &mut [u8], scale: u8) -> Result<Apd, anyhow::Error> {
    let mut cx = cx_datum();
    if input.len() <= 17 {
        if let Ok(mut n) = twos_complement_be_to_apd_inner::<Apd, APD_DATUM_WIDTH>(input) {
            n.set_exponent(-i32::from(scale));
            return Ok(n);
        }
    }
    // If bytes were invalid for narrower representation, try to use wider
    // representation in case e.g. simply has more trailing zeroes.
    let mut n = twos_complement_be_to_apd_inner::<ApdAgg, APD_AGG_WIDTH>(input)?;
    // Exponent must be set before converting to `Apd` width, otherwise values can overflow 39 dop.
    n.set_exponent(-i32::from(scale));
    let d = cx.to_width(n);
    if cx.status().inexact() {
        bail!("Value exceeds maximum APD value")
    }
    Ok(d)
}

/// Parses a buffer of two's complement digits in big-endian order and converts
/// them to [`Decimal<N>`].
pub fn twos_complement_be_to_apd_inner<D: Dec<N>, const N: usize>(
    input: &mut [u8],
) -> Result<Decimal<N>, anyhow::Error> {
    let is_neg = if (input[0] & 0x80) != 0 {
        // byte-level negate all negative values, guaranteeing all bytes are
        // readable as unsigned.
        negate_twos_complement_le(input.iter_mut().rev());
        true
    } else {
        false
    };

    let head = input.len() % 16;
    let i = twos_complement_be_to_u128(&input[0..head]);
    let mut cx = D::context();
    let mut d = cx.from_u128(i);

    for c in input[head..].chunks(16) {
        assert_eq!(c.len(), 16);
        // essentially d << 128
        cx.mul(&mut d, D::u128_splitter());
        let i = twos_complement_be_to_u128(&c);
        let i = cx.from_u128(i);
        cx.add(&mut d, &i);
    }

    if cx.status().inexact() {
        bail!("Value exceeds maximum APD value")
    } else if cx.status().any() {
        bail!("unexpected status {:?}", cx.status());
    }
    if is_neg {
        cx.neg(&mut d);
    }
    Ok(d)
}

#[test]
fn test_twos_complement_roundtrip() {
    fn inner(s: &str) {
        let mut cx = cx_datum();
        let d = cx.parse(s).unwrap();
        let scale = std::cmp::min(d.exponent(), 0).abs();
        let mut b = apd_to_twos_complement_be(d.clone());
        let x = twos_complement_be_to_apd(&mut b, u8::try_from(scale).unwrap()).unwrap();
        assert_eq!(d, x);
    }
    inner("0");
    inner("0.000000000000000000000000000000000012345");
    inner("0.123456789012345678901234567890123456789");
    inner("1.00000000000000000000000000000000000000");
    inner("1");
    inner("2");
    inner("170141183460469231731687303715884105727");
    inner("170141183460469231731687303715884105728");
    inner("12345678901234567890.1234567890123456789");
    inner("999999999999999999999999999999999999999");
    inner("7e35");
    inner("7e-35");
    inner("-0.000000000000000000000000000000000012345");
    inner("-0.12345678901234567890123456789012345678");
    inner("-1.00000000000000000000000000000000000000");
    inner("-1");
    inner("-2");
    inner("-170141183460469231731687303715884105727");
    inner("-170141183460469231731687303715884105728");
    inner("-12345678901234567890.1234567890123456789");
    inner("-999999999999999999999999999999999999999");
    inner("-7.2e35");
    inner("-7.2e-35");
}

#[test]
fn test_twos_comp_apd_primitive() {
    fn inner_inner<P>(i: P, i_be_bytes: &mut [u8])
    where
        P: Into<Apd> + TryFrom<Apd> + Eq + PartialEq + std::fmt::Debug + Copy,
    {
        use std::convert::TryInto;
        let mut e = [0; Apd::TWOS_COMPLEMENT_BYTE_WIDTH];
        e[Apd::TWOS_COMPLEMENT_BYTE_WIDTH - i_be_bytes.len()..].copy_from_slice(&i_be_bytes);
        let mut w = [0; ApdAgg::TWOS_COMPLEMENT_BYTE_WIDTH];
        w[ApdAgg::TWOS_COMPLEMENT_BYTE_WIDTH - i_be_bytes.len()..].copy_from_slice(&i_be_bytes);

        let d: Apd = i.into();

        // Extend negative sign into most-significant bits
        if d.is_negative() {
            for i in e[..Apd::TWOS_COMPLEMENT_BYTE_WIDTH - i_be_bytes.len()].iter_mut() {
                *i = 0xFF;
            }
            for i in w[..ApdAgg::TWOS_COMPLEMENT_BYTE_WIDTH - i_be_bytes.len()].iter_mut() {
                *i = 0xFF;
            }
        }

        // Ensure decimal value's two's complement representation matches an
        // extended version of `to_be_bytes`.
        let d_be_bytes = apd_to_twos_complement_be(d);
        assert_eq!(
            e, d_be_bytes,
            "expected repr of {:?}, got {:?}",
            e, d_be_bytes
        );

        // Ensure extended version of `to_be_bytes` generates same `i128`.
        let e_apd = twos_complement_be_to_apd(&mut e, 0).unwrap();
        let e_p: P = match e_apd.try_into() {
            Ok(e_p) => e_p,
            Err(_) => panic!(),
        };
        assert_eq!(i, e_p, "expected val of {:?}, got {:?}", i, e_p);

        // Wide representation produces same result.
        let w_apd = twos_complement_be_to_apd(&mut w, 0).unwrap();
        let w_p: P = match w_apd.try_into() {
            Ok(w_p) => w_p,
            Err(_) => panic!(),
        };
        assert_eq!(i, w_p, "expected val of {:?}, got {:?}", i, e_p);

        // Bytes do not need to be in `Apd`-specific format
        let p_apd = twos_complement_be_to_apd(i_be_bytes, 0).unwrap();
        let p_p: P = match p_apd.try_into() {
            Ok(p_p) => p_p,
            Err(_) => panic!(),
        };
        assert_eq!(i, p_p, "expected val of {:?}, got {:?}", i, p_p);
    }

    fn inner_i32(i: i32) {
        inner_inner(i, &mut i.to_be_bytes());
    }

    fn inner_i64(i: i64) {
        inner_inner(i, &mut i.to_be_bytes());
    }

    // We need a wrapper around i128 to implement the same traits as the other
    // primitive types. This is less code than a second implementation of the
    // same test that takes unwrapped i128s.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct FromableI128 {
        i: i128,
    }
    impl From<i128> for FromableI128 {
        fn from(i: i128) -> FromableI128 {
            FromableI128 { i }
        }
    }
    impl From<FromableI128> for Apd {
        fn from(n: FromableI128) -> Apd {
            Apd::try_from(n.i).unwrap()
        }
    }
    impl TryFrom<Apd> for FromableI128 {
        type Error = ();
        fn try_from(n: Apd) -> Result<FromableI128, Self::Error> {
            match i128::try_from(n) {
                Ok(i) => Ok(FromableI128 { i }),
                Err(_) => Err(()),
            }
        }
    }

    fn inner_i128(i: i128) {
        inner_inner(FromableI128::from(i), &mut i.to_be_bytes());
    }

    inner_i32(0);
    inner_i32(1);
    inner_i32(2);
    inner_i32(-1);
    inner_i32(-2);
    inner_i32(i32::MAX);
    inner_i32(i32::MIN);
    inner_i32(i32::MAX / 7 + 7);
    inner_i32(i32::MIN / 7 + 7);
    inner_i64(0);
    inner_i64(1);
    inner_i64(2);
    inner_i64(-1);
    inner_i64(-2);
    inner_i64(i64::MAX);
    inner_i64(i64::MIN);
    inner_i64(i64::MAX / 7 + 7);
    inner_i64(i64::MIN / 7 + 7);
    inner_i128(0);
    inner_i128(1);
    inner_i128(2);
    inner_i128(-1);
    inner_i128(-2);
    inner_i128(i64::MAX as i128);
    inner_i128(i64::MIN as i128);
    inner_i128(i128::MAX);
    inner_i128(i128::MIN);
    inner_i128(i128::MAX / 7 + 7);
    inner_i128(i128::MIN / 7 + 7);
}

#[test]
fn test_twos_complement_to_apd_fail() {
    fn inner(b: &mut [u8]) {
        let r = twos_complement_be_to_apd(b, 0);
        assert!(r.is_err());
    }
    // 17-byte signed digit's max value exceeds 39 digits of precision
    let mut e = [0xFF; Apd::TWOS_COMPLEMENT_BYTE_WIDTH];
    e[0] -= 0x80;
    inner(&mut e);

    // 1 << 17 * 8 exceeds exceeds 39 digits of precision
    let mut e = [0; Apd::TWOS_COMPLEMENT_BYTE_WIDTH + 1];
    e[0] = 1;
    inner(&mut e);
}

#[test]
fn test_wide_twos_complement_roundtrip() {
    fn inner(s: &str) {
        let mut cx = cx_datum();
        let d = cx.parse(s).unwrap();
        let mut b = apd_to_twos_complement_wide(d.clone());
        let x = twos_complement_be_to_apd(&mut b, APD_DATUM_MAX_PRECISION as u8).unwrap();
        assert_eq!(d, x);
    }
    inner("0");
    inner("0.000000000000000000000000000000000012345");
    inner("0.123456789012345678901234567890123456789");
    inner("1.00000000000000000000000000000000000000");
    inner("1");
    inner("2");
    inner("170141183460469231731687303715884105727");
    inner("170141183460469231731687303715884105728");
    inner("12345678901234567890.1234567890123456789");
    inner("999999999999999999999999999999999999999");
    inner("-0.000000000000000000000000000000000012345");
    inner("-0.123456789012345678901234567890123456789");
    inner("-1.00000000000000000000000000000000000000");
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
    munge_apd(n)?;

    Ok(())
}
