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

use std::error::Error;
use std::fmt;
use std::sync::LazyLock;

use anyhow::bail;
use dec::{Context, Decimal};
use mz_lowertest::MzReflect;
use mz_ore::cast;
use mz_persist_types::columnar::FixedSizeCodec;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.numeric.rs"));

/// The number of internal decimal units in a [`Numeric`] value.
pub const NUMERIC_DATUM_WIDTH: u8 = 13;

/// The value of [`NUMERIC_DATUM_WIDTH`] as a [`u8`].
pub const NUMERIC_DATUM_WIDTH_USIZE: usize = cast::u8_to_usize(NUMERIC_DATUM_WIDTH);

/// The maximum number of digits expressable in a [`Numeric`] value.
pub const NUMERIC_DATUM_MAX_PRECISION: u8 = NUMERIC_DATUM_WIDTH * 3;

/// A numeric value.
pub type Numeric = Decimal<NUMERIC_DATUM_WIDTH_USIZE>;

/// The number of internal decimal units in a [`NumericAgg`] value.
pub const NUMERIC_AGG_WIDTH: u8 = 27;

/// The value of [`NUMERIC_AGG_WIDTH`] as a [`u8`].
pub const NUMERIC_AGG_WIDTH_USIZE: usize = cast::u8_to_usize(NUMERIC_AGG_WIDTH);

/// The maximum number of digits expressable in a [`NumericAgg`] value.
pub const NUMERIC_AGG_MAX_PRECISION: u8 = NUMERIC_AGG_WIDTH * 3;

/// A double-width version of [`Numeric`] for use in aggregations.
pub type NumericAgg = Decimal<NUMERIC_AGG_WIDTH_USIZE>;

static CX_DATUM: LazyLock<Context<Numeric>> = LazyLock::new(|| {
    let mut cx = Context::<Numeric>::default();
    cx.set_max_exponent(isize::from(NUMERIC_DATUM_MAX_PRECISION - 1))
        .unwrap();
    cx.set_min_exponent(-isize::from(NUMERIC_DATUM_MAX_PRECISION))
        .unwrap();
    cx
});
static CX_AGG: LazyLock<Context<NumericAgg>> = LazyLock::new(|| {
    let mut cx = Context::<NumericAgg>::default();
    cx.set_max_exponent(isize::from(NUMERIC_AGG_MAX_PRECISION - 1))
        .unwrap();
    cx.set_min_exponent(-isize::from(NUMERIC_AGG_MAX_PRECISION))
        .unwrap();
    cx
});
static U128_SPLITTER_DATUM: LazyLock<Numeric> = LazyLock::new(|| {
    let mut cx = Numeric::context();
    // 1 << 128
    cx.parse("340282366920938463463374607431768211456").unwrap()
});
static U128_SPLITTER_AGG: LazyLock<NumericAgg> = LazyLock::new(|| {
    let mut cx = NumericAgg::context();
    // 1 << 128
    cx.parse("340282366920938463463374607431768211456").unwrap()
});

/// Module to simplify serde'ing a `Numeric` through its string representation.
pub mod str_serde {
    use std::str::FromStr;

    use serde::Deserialize;

    use super::Numeric;

    /// Deserializing a [`Numeric`] value from its `String` representation.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Numeric, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        Numeric::from_str(&buf).map_err(serde::de::Error::custom)
    }
}

/// The `max_scale` of a [`ScalarType::Numeric`].
///
/// This newtype wrapper ensures that the scale is within the valid range.
///
/// [`ScalarType::Numeric`]: crate::ScalarType::Numeric
#[derive(
    Arbitrary,
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    MzReflect,
)]
pub struct NumericMaxScale(pub(crate) u8);

impl NumericMaxScale {
    /// A max scale of zero.
    pub const ZERO: NumericMaxScale = NumericMaxScale(0);

    /// Consumes the newtype wrapper, returning the inner `u8`.
    pub fn into_u8(self) -> u8 {
        self.0
    }
}

impl TryFrom<i64> for NumericMaxScale {
    type Error = InvalidNumericMaxScaleError;

    fn try_from(max_scale: i64) -> Result<Self, Self::Error> {
        match u8::try_from(max_scale) {
            Ok(max_scale) if max_scale <= NUMERIC_DATUM_MAX_PRECISION => {
                Ok(NumericMaxScale(max_scale))
            }
            _ => Err(InvalidNumericMaxScaleError),
        }
    }
}

impl TryFrom<usize> for NumericMaxScale {
    type Error = InvalidNumericMaxScaleError;

    fn try_from(max_scale: usize) -> Result<Self, Self::Error> {
        Self::try_from(i64::try_from(max_scale).map_err(|_| InvalidNumericMaxScaleError)?)
    }
}

impl RustType<ProtoNumericMaxScale> for NumericMaxScale {
    fn into_proto(&self) -> ProtoNumericMaxScale {
        ProtoNumericMaxScale {
            value: self.0.into_proto(),
        }
    }

    fn from_proto(max_scale: ProtoNumericMaxScale) -> Result<Self, TryFromProtoError> {
        Ok(NumericMaxScale(max_scale.value.into_rust()?))
    }
}

impl RustType<ProtoOptionalNumericMaxScale> for Option<NumericMaxScale> {
    fn into_proto(&self) -> ProtoOptionalNumericMaxScale {
        ProtoOptionalNumericMaxScale {
            value: self.into_proto(),
        }
    }

    fn from_proto(max_scale: ProtoOptionalNumericMaxScale) -> Result<Self, TryFromProtoError> {
        max_scale.value.into_rust()
    }
}

/// The error returned when constructing a [`NumericMaxScale`] from an invalid
/// value.
#[derive(Debug, Clone)]
pub struct InvalidNumericMaxScaleError;

impl fmt::Display for InvalidNumericMaxScaleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "scale for type numeric must be between 0 and {}",
            NUMERIC_DATUM_MAX_PRECISION
        )
    }
}

impl Error for InvalidNumericMaxScaleError {}

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

impl Dec<NUMERIC_DATUM_WIDTH_USIZE> for Numeric {
    const TWOS_COMPLEMENT_BYTE_WIDTH: usize = 17;
    fn context() -> Context<Numeric> {
        CX_DATUM.clone()
    }
    fn u128_splitter() -> &'static Numeric {
        &U128_SPLITTER_DATUM
    }
}

impl Dec<NUMERIC_AGG_WIDTH_USIZE> for NumericAgg {
    const TWOS_COMPLEMENT_BYTE_WIDTH: usize = 33;
    fn context() -> Context<NumericAgg> {
        CX_AGG.clone()
    }
    fn u128_splitter() -> &'static NumericAgg {
        &U128_SPLITTER_AGG
    }
}

/// Returns a new context appropriate for operating on numeric datums.
pub fn cx_datum() -> Context<Numeric> {
    CX_DATUM.clone()
}

/// Returns a new context appropriate for operating on numeric aggregates.
pub fn cx_agg() -> Context<NumericAgg> {
    CX_AGG.clone()
}

fn twos_complement_be_to_u128(input: &[u8]) -> u128 {
    assert!(input.len() <= 16);
    let mut buf = [0; 16];
    buf[16 - input.len()..16].copy_from_slice(input);
    u128::from_be_bytes(buf)
}

/// Using negative binary numbers can require more digits of precision than
/// [`Numeric`] offers, so we need to have the option to swap bytes' signs at the
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

/// Converts an [`Numeric`] into its big endian two's complement representation.
pub fn numeric_to_twos_complement_be(
    mut numeric: Numeric,
) -> [u8; Numeric::TWOS_COMPLEMENT_BYTE_WIDTH] {
    let mut buf = [0; Numeric::TWOS_COMPLEMENT_BYTE_WIDTH];
    // Avro doesn't specify how to handle NaN/infinity, so we simply treat them
    // as zeroes so as to avoid erroring (encoding values is meant to be
    // infallible) and retain downstream associativity/commutativity.
    if numeric.is_special() {
        return buf;
    }

    let mut cx = Numeric::context();

    // Ensure `numeric` is a canonical coefficient.
    if numeric.exponent() < 0 {
        let s = Numeric::from(-numeric.exponent());
        cx.scaleb(&mut numeric, &s);
    }

    numeric_to_twos_complement_inner::<Numeric, NUMERIC_DATUM_WIDTH_USIZE>(
        numeric, &mut cx, &mut buf,
    );
    buf
}

/// Converts an [`Numeric`] into a big endian two's complement representation where
/// the encoded value has [`NUMERIC_AGG_MAX_PRECISION`] digits and a scale of
/// [`NUMERIC_DATUM_MAX_PRECISION`].
///
/// This representation is appropriate to use in
/// contexts requiring two's complement representation but `Numeric` values' scale
/// isn't known, e.g. when working with columns with an explicitly defined
/// scale.
pub fn numeric_to_twos_complement_wide(
    numeric: Numeric,
) -> [u8; NumericAgg::TWOS_COMPLEMENT_BYTE_WIDTH] {
    let mut buf = [0; NumericAgg::TWOS_COMPLEMENT_BYTE_WIDTH];
    // Avro doesn't specify how to handle NaN/infinity, so we simply treat them
    // as zeroes so as to avoid erroring (encoding values is meant to be
    // infallible) and retain downstream associativity/commutativity.
    if numeric.is_special() {
        return buf;
    }
    let mut cx = NumericAgg::context();
    let mut d = cx.to_width(numeric);
    let mut scaler = NumericAgg::from(NUMERIC_DATUM_MAX_PRECISION);
    cx.neg(&mut scaler);
    // Shape `d` so that its exponent is -NUMERIC_DATUM_MAX_PRECISION
    cx.rescale(&mut d, &scaler);
    // Adjust `d` so it is a canonical coefficient, i.e. its exact value can be
    // recovered by setting its exponent to -39.
    cx.abs(&mut scaler);
    cx.scaleb(&mut d, &scaler);

    numeric_to_twos_complement_inner::<NumericAgg, NUMERIC_AGG_WIDTH_USIZE>(d, &mut cx, &mut buf);
    buf
}

fn numeric_to_twos_complement_inner<D: Dec<N>, const N: usize>(
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

pub fn twos_complement_be_to_numeric(
    input: &mut [u8],
    scale: u8,
) -> Result<Numeric, anyhow::Error> {
    let mut cx = cx_datum();
    if input.len() <= 17 {
        if let Ok(mut n) =
            twos_complement_be_to_numeric_inner::<Numeric, NUMERIC_DATUM_WIDTH_USIZE>(input)
        {
            n.set_exponent(-i32::from(scale));
            return Ok(n);
        }
    }
    // If bytes were invalid for narrower representation, try to use wider
    // representation in case e.g. simply has more trailing zeroes.
    let mut n = twos_complement_be_to_numeric_inner::<NumericAgg, NUMERIC_AGG_WIDTH_USIZE>(input)?;
    // Exponent must be set before converting to `Numeric` width, otherwise values can overflow 39 dop.
    n.set_exponent(-i32::from(scale));
    let d = cx.to_width(n);
    if cx.status().inexact() {
        bail!("Value exceeds maximum numeric value")
    }
    Ok(d)
}

/// Parses a buffer of two's complement digits in big-endian order and converts
/// them to [`Decimal<N>`].
pub fn twos_complement_be_to_numeric_inner<D: Dec<N>, const N: usize>(
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
        let i = twos_complement_be_to_u128(c);
        let i = cx.from_u128(i);
        cx.add(&mut d, &i);
    }

    if cx.status().inexact() {
        bail!("Value exceeds maximum numeric value")
    } else if cx.status().any() {
        bail!("unexpected status {:?}", cx.status());
    }
    if is_neg {
        cx.neg(&mut d);
    }
    Ok(d)
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_twos_complement_roundtrip() {
    fn inner(s: &str) {
        let mut cx = cx_datum();
        let d = cx.parse(s).unwrap();
        let scale = std::cmp::min(d.exponent(), 0).abs();
        let mut b = numeric_to_twos_complement_be(d.clone());
        let x = twos_complement_be_to_numeric(&mut b, u8::try_from(scale).unwrap()).unwrap();
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

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_twos_comp_numeric_primitive() {
    fn inner_inner<P>(i: P, i_be_bytes: &mut [u8])
    where
        P: Into<Numeric> + TryFrom<Numeric> + Eq + PartialEq + std::fmt::Debug + Copy,
    {
        let mut e = [0; Numeric::TWOS_COMPLEMENT_BYTE_WIDTH];
        e[Numeric::TWOS_COMPLEMENT_BYTE_WIDTH - i_be_bytes.len()..].copy_from_slice(i_be_bytes);
        let mut w = [0; NumericAgg::TWOS_COMPLEMENT_BYTE_WIDTH];
        w[NumericAgg::TWOS_COMPLEMENT_BYTE_WIDTH - i_be_bytes.len()..].copy_from_slice(i_be_bytes);

        let d: Numeric = i.into();

        // Extend negative sign into most-significant bits
        if d.is_negative() {
            for i in e[..Numeric::TWOS_COMPLEMENT_BYTE_WIDTH - i_be_bytes.len()].iter_mut() {
                *i = 0xFF;
            }
            for i in w[..NumericAgg::TWOS_COMPLEMENT_BYTE_WIDTH - i_be_bytes.len()].iter_mut() {
                *i = 0xFF;
            }
        }

        // Ensure decimal value's two's complement representation matches an
        // extended version of `to_be_bytes`.
        let d_be_bytes = numeric_to_twos_complement_be(d);
        assert_eq!(
            e, d_be_bytes,
            "expected repr of {:?}, got {:?}",
            e, d_be_bytes
        );

        // Ensure extended version of `to_be_bytes` generates same `i128`.
        let e_numeric = twos_complement_be_to_numeric(&mut e, 0).unwrap();
        let e_p: P = match e_numeric.try_into() {
            Ok(e_p) => e_p,
            Err(_) => panic!(),
        };
        assert_eq!(i, e_p, "expected val of {:?}, got {:?}", i, e_p);

        // Wide representation produces same result.
        let w_numeric = twos_complement_be_to_numeric(&mut w, 0).unwrap();
        let w_p: P = match w_numeric.try_into() {
            Ok(w_p) => w_p,
            Err(_) => panic!(),
        };
        assert_eq!(i, w_p, "expected val of {:?}, got {:?}", i, e_p);

        // Bytes do not need to be in `Numeric`-specific format
        let p_numeric = twos_complement_be_to_numeric(i_be_bytes, 0).unwrap();
        let p_p: P = match p_numeric.try_into() {
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
    impl From<FromableI128> for Numeric {
        fn from(n: FromableI128) -> Numeric {
            Numeric::try_from(n.i).unwrap()
        }
    }
    impl TryFrom<Numeric> for FromableI128 {
        type Error = ();
        fn try_from(n: Numeric) -> Result<FromableI128, Self::Error> {
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
    inner_i128(i128::from(i64::MAX));
    inner_i128(i128::from(i64::MIN));
    inner_i128(i128::MAX);
    inner_i128(i128::MIN);
    inner_i128(i128::MAX / 7 + 7);
    inner_i128(i128::MIN / 7 + 7);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_twos_complement_to_numeric_fail() {
    fn inner(b: &mut [u8]) {
        let r = twos_complement_be_to_numeric(b, 0);
        mz_ore::assert_err!(r);
    }
    // 17-byte signed digit's max value exceeds 39 digits of precision
    let mut e = [0xFF; Numeric::TWOS_COMPLEMENT_BYTE_WIDTH];
    e[0] -= 0x80;
    inner(&mut e);

    // 1 << 17 * 8 exceeds exceeds 39 digits of precision
    let mut e = [0; Numeric::TWOS_COMPLEMENT_BYTE_WIDTH + 1];
    e[0] = 1;
    inner(&mut e);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_wide_twos_complement_roundtrip() {
    fn inner(s: &str) {
        let mut cx = cx_datum();
        let d = cx.parse(s).unwrap();
        let mut b = numeric_to_twos_complement_wide(d.clone());
        let x = twos_complement_be_to_numeric(&mut b, NUMERIC_DATUM_MAX_PRECISION).unwrap();
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
pub fn get_scale(n: &Numeric) -> u32 {
    let exp = n.exponent();
    if exp >= 0 { 0 } else { exp.unsigned_abs() }
}

/// Ensures [`Numeric`] values are:
/// - Within `Numeric`'s max precision ([`NUMERIC_DATUM_MAX_PRECISION`]), or errors if not.
/// - Never possible but invalid representations (i.e. never -Nan or -0).
///
/// Should be called after any operation that can change an [`Numeric`]'s scale or
/// generate negative values (except addition and subtraction).
pub fn munge_numeric(n: &mut Numeric) -> Result<(), anyhow::Error> {
    rescale_within_max_precision(n)?;
    if (n.is_zero() || n.is_nan()) && n.is_negative() {
        cx_datum().neg(n);
    }
    Ok(())
}

/// Rescale's `n` to fit within [`Numeric`]'s max precision or error if not
/// possible.
fn rescale_within_max_precision(n: &mut Numeric) -> Result<(), anyhow::Error> {
    let current_precision = get_precision(n);
    if current_precision > u32::from(NUMERIC_DATUM_MAX_PRECISION) {
        if n.exponent() < 0 {
            let precision_diff = current_precision - u32::from(NUMERIC_DATUM_MAX_PRECISION);
            let current_scale = u8::try_from(get_scale(n))?;
            let scale_diff = current_scale - u8::try_from(precision_diff).unwrap();
            rescale(n, scale_diff)?;
        } else {
            bail!(
                "numeric value {} exceed maximum precision {}",
                n,
                NUMERIC_DATUM_MAX_PRECISION
            )
        }
    }
    Ok(())
}

/// Rescale `n` as an `OrderedDecimal` with the described scale, or error if:
/// - Rescaling exceeds max precision
/// - `n` requires > [`NUMERIC_DATUM_MAX_PRECISION`] - `scale` digits of precision
///   left of the decimal point
pub fn rescale(n: &mut Numeric, scale: u8) -> Result<(), anyhow::Error> {
    let mut cx = cx_datum();
    cx.rescale(n, &Numeric::from(-i32::from(scale)));
    if cx.status().invalid_operation() || get_precision(n) > u32::from(NUMERIC_DATUM_MAX_PRECISION)
    {
        bail!(
            "numeric value {} exceed maximum precision {}",
            n,
            NUMERIC_DATUM_MAX_PRECISION
        )
    }
    munge_numeric(n)?;

    Ok(())
}

/// A type that can represent Real Numbers. Useful for interoperability between Numeric and
/// floating point.
pub trait DecimalLike:
    From<u8>
    + From<u16>
    + From<u32>
    + From<i8>
    + From<i16>
    + From<i32>
    + From<f32>
    + From<f64>
    + std::ops::Add<Output = Self>
    + std::ops::Sub<Output = Self>
    + std::ops::Mul<Output = Self>
    + std::ops::Div<Output = Self>
{
    /// Used to do value-to-value conversions while consuming the input value. Depending on the
    /// implementation it may be potentially lossy.
    fn lossy_from(i: i64) -> Self;
}

impl DecimalLike for f64 {
    // No other known way to convert `i64` to `f64`.
    #[allow(clippy::as_conversions)]
    fn lossy_from(i: i64) -> Self {
        i as f64
    }
}

impl DecimalLike for Numeric {
    fn lossy_from(i: i64) -> Self {
        Numeric::from(i)
    }
}

/// An encoded packed variant of [`Numeric`].
///
/// Unlike other "Packed" types we _DO NOT_ uphold the invariant that
/// [`PackedNumeric`] sorts the same as [`Numeric`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PackedNumeric(pub [u8; 40]);

impl FixedSizeCodec<Numeric> for PackedNumeric {
    const SIZE: usize = 40;

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn from_bytes(slice: &[u8]) -> Result<Self, String> {
        let buf: [u8; Self::SIZE] = slice.try_into().map_err(|_| {
            format!(
                "size for PackedNumeric is {} bytes, got {}",
                Self::SIZE,
                slice.len()
            )
        })?;
        Ok(PackedNumeric(buf))
    }

    fn from_value(val: Numeric) -> PackedNumeric {
        let (digits, exponent, bits, lsu) = val.to_raw_parts();

        let mut buf = [0u8; 40];

        buf[0..4].copy_from_slice(&digits.to_le_bytes());
        buf[4..8].copy_from_slice(&exponent.to_le_bytes());

        for i in 0..13 {
            buf[(i * 2) + 8..(i * 2) + 10].copy_from_slice(&lsu[i].to_le_bytes());
        }

        buf[34..35].copy_from_slice(&bits.to_le_bytes());

        PackedNumeric(buf)
    }

    fn into_value(self) -> Numeric {
        let digits: [u8; 4] = self.0[0..4].try_into().unwrap();
        let digits = u32::from_le_bytes(digits);

        let exponent: [u8; 4] = self.0[4..8].try_into().unwrap();
        let exponent = i32::from_le_bytes(exponent);

        let mut lsu = [0u16; 13];
        for i in 0..13 {
            let x: [u8; 2] = self.0[(i * 2) + 8..(i * 2) + 10].try_into().unwrap();
            let x = u16::from_le_bytes(x);
            lsu[i] = x;
        }

        let bits: [u8; 1] = self.0[34..35].try_into().unwrap();
        let bits = u8::from_le_bytes(bits);

        Numeric::from_raw_parts(digits, exponent, bits, lsu)
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    use crate::scalar::arb_numeric;

    use super::*;

    proptest! {
        #[mz_ore::test]
        fn numeric_max_scale_protobuf_roundtrip(expect in any::<NumericMaxScale>()) {
            let actual = protobuf_roundtrip::<_, ProtoNumericMaxScale>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }

        #[mz_ore::test]
        fn optional_numeric_max_scale_protobuf_roundtrip(expect in any::<Option<NumericMaxScale>>()) {
            let actual = protobuf_roundtrip::<_, ProtoOptionalNumericMaxScale>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn smoketest_packed_numeric_roundtrips() {
        let og = PackedNumeric::from_value(Numeric::from(-42));
        let bytes = og.as_bytes();
        let rnd = PackedNumeric::from_bytes(bytes).expect("valid");
        assert_eq!(og, rnd);

        // Returns an error if the size of the slice is invalid.
        mz_ore::assert_err!(PackedNumeric::from_bytes(&[0, 0, 0, 0]));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberCopyNegate` on OS `linux`
    fn proptest_packed_numeric_roundtrip() {
        fn test(og: Numeric) {
            let packed = PackedNumeric::from_value(og);
            let rnd = packed.into_value();

            if og.is_nan() && rnd.is_nan() {
                return;
            }
            assert_eq!(og, rnd);
        }

        proptest!(|(num in arb_numeric())| {
            test(num);
        });
    }

    // Note: It's expected that this test will fail if you update the strategy
    // for generating an arbitrary Numeric. In that case feel free to
    // regenerate the snapshot.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberCopyNegate` on OS `linux`
    fn packed_numeric_stability() {
        /// This is the seed [`proptest`] uses for their deterministic RNG. We
        /// copy it here to prevent breaking this test if [`proptest`] changes.
        const RNG_SEED: [u8; 32] = [
            0xf4, 0x16, 0x16, 0x48, 0xc3, 0xac, 0x77, 0xac, 0x72, 0x20, 0x0b, 0xea, 0x99, 0x67,
            0x2d, 0x6d, 0xca, 0x9f, 0x76, 0xaf, 0x1b, 0x09, 0x73, 0xa0, 0x59, 0x22, 0x6d, 0xc5,
            0x46, 0x39, 0x1c, 0x4a,
        ];

        let rng = proptest::test_runner::TestRng::from_seed(
            proptest::test_runner::RngAlgorithm::ChaCha,
            &RNG_SEED,
        );
        // Generate a collection of Rows.
        let config = proptest::test_runner::Config {
            // We let the loop below drive how much data we generate.
            cases: u32::MAX,
            rng_algorithm: proptest::test_runner::RngAlgorithm::ChaCha,
            ..Default::default()
        };
        let mut runner = proptest::test_runner::TestRunner::new_with_rng(config, rng);

        let test_cases = 2_000;
        let strat = arb_numeric();

        let mut all_numerics = Vec::new();
        for _ in 0..test_cases {
            let value_tree = strat.new_tree(&mut runner).unwrap();
            let numeric = value_tree.current();
            let packed = PackedNumeric::from_value(numeric);
            let hex_bytes = format!("{:x?}", packed.as_bytes());

            all_numerics.push((numeric, hex_bytes));
        }

        insta::assert_debug_snapshot!(all_numerics);
    }
}
