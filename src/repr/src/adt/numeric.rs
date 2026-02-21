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
use dec::{Context, Decimal, OrderedDecimal};
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

/// The `max_scale` of a [`SqlScalarType::Numeric`].
///
/// This newtype wrapper ensures that the scale is within the valid range.
///
/// [`SqlScalarType::Numeric`]: crate::SqlScalarType::Numeric
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
    MzReflect
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

/// Extract the coefficient of a finite `Numeric` as an `i64`, if it fits.
///
/// Returns `None` for special values (NaN/Inf) or values with more than
/// 18 significant digits (which might overflow i64).
#[inline]
fn coeff_to_i64(d: &Numeric) -> Option<i64> {
    if d.is_special() {
        return None;
    }
    let units = d.coefficient_units();
    // Each unit holds 0-999. 6 units = up to 18 digits, which fits in i64
    // (i64::MAX = 9_223_372_036_854_775_807, 19 digits).
    if units.len() > 6 {
        return None;
    }
    let mut val: i64 = 0;
    for &u in units.iter().rev() {
        val = val * 1000 + u as i64;
    }
    if d.is_negative() {
        val = -val;
    }
    Some(val)
}

/// Build a `Numeric` from an i64 coefficient and exponent.
///
/// The caller must ensure the value is representable (≤ 18 significant digits).
#[inline]
pub fn numeric_from_i64_coeff(mut val: i64, exponent: i32) -> Numeric {
    if val == 0 {
        let lsu = [0u16; 13];
        return Numeric::from_raw_parts(1, exponent, 0, lsu);
    }
    let (bits, magnitude) = if val < 0 {
        (0x80u8, (val as u64).wrapping_neg()) // DECNEG; handles i64::MIN correctly
    } else {
        (0u8, val as u64)
    };
    let mut lsu = [0u16; 13];
    let mut remaining = magnitude;
    let mut n_units = 0usize;
    while remaining > 0 {
        lsu[n_units] = (remaining % 1000) as u16;
        remaining /= 1000;
        n_units += 1;
    }
    let top_unit = lsu[n_units - 1];
    let top_digits = if top_unit >= 100 {
        3
    } else if top_unit >= 10 {
        2
    } else {
        1
    };
    let sig_digits = (n_units as u32 - 1) * 3 + top_digits;
    Numeric::from_raw_parts(sig_digits, exponent, bits, lsu)
}

/// Build a `Numeric` from a u64 coefficient and exponent.
///
/// The caller must ensure the value is representable (≤ 19 significant digits).
#[inline]
pub fn numeric_from_u64_coeff(val: u64, exponent: i32) -> Numeric {
    if val == 0 {
        let lsu = [0u16; 13];
        return Numeric::from_raw_parts(1, exponent, 0, lsu);
    }
    let mut lsu = [0u16; 13];
    let mut remaining = val;
    let mut n_units = 0usize;
    while remaining > 0 {
        lsu[n_units] = (remaining % 1000) as u16;
        remaining /= 1000;
        n_units += 1;
    }
    let top_unit = lsu[n_units - 1];
    let top_digits = if top_unit >= 100 {
        3
    } else if top_unit >= 10 {
        2
    } else {
        1
    };
    let sig_digits = (n_units as u32 - 1) * 3 + top_digits;
    Numeric::from_raw_parts(sig_digits, exponent, 0, lsu)
}

/// Powers of 10, precomputed for fast integer rescaling.
const POW10_TABLE: [i64; 19] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

/// Build a `Numeric` from an i64 value with optional rescaling, bypassing FFI.
///
/// When `scale` is `None`, constructs Numeric with exponent 0.
/// When `scale` is `Some(s)`, constructs with exponent `-s` (equivalent to
/// rescaling), which requires multiplying the value by `10^s`. Returns `None`
/// if the scaled value would overflow i64 (> 18 significant digits).
#[inline]
pub fn try_numeric_from_i64(val: i64, scale: Option<u8>) -> Option<Numeric> {
    match scale {
        None => Some(numeric_from_i64_coeff(val, 0)),
        Some(s) => {
            let s_usize = s as usize;
            if s_usize >= POW10_TABLE.len() {
                return None;
            }
            let pow = POW10_TABLE[s_usize];
            let scaled = val.checked_mul(pow)?;
            Some(numeric_from_i64_coeff(scaled, -(s as i32)))
        }
    }
}

/// Build a `Numeric` from a u64 value with optional rescaling, bypassing FFI.
///
/// Same semantics as [`try_numeric_from_i64`] but for unsigned values.
/// Returns `None` if the scaled value would overflow u64.
#[inline]
pub fn try_numeric_from_u64(val: u64, scale: Option<u8>) -> Option<Numeric> {
    match scale {
        None => Some(numeric_from_u64_coeff(val, 0)),
        Some(s) => {
            let s_usize = s as usize;
            if s_usize >= POW10_TABLE.len() {
                return None;
            }
            let pow = POW10_TABLE[s_usize] as u64;
            let scaled = val.checked_mul(pow)?;
            Some(numeric_from_u64_coeff(scaled, -(s as i32)))
        }
    }
}

/// Fast-path addition of two `Numeric` values, bypassing the C FFI.
///
/// Returns `Some(result)` if both operands are finite, have the same exponent
/// (scale), and their coefficients and result all fit in i64 (≤18 digits).
/// Returns `None` to fall back to the general FFI path.
#[inline]
pub fn try_add_fast(a: &Numeric, b: &Numeric) -> Option<Numeric> {
    if a.exponent() != b.exponent() {
        return None;
    }
    let ca = coeff_to_i64(a)?;
    let cb = coeff_to_i64(b)?;
    let result = ca.checked_add(cb)?;
    // 18 significant digits is the limit for our i64 reconstruction.
    // i64::MAX = 9_223_372_036_854_775_807 (19 digits), so any value
    // that fits in checked_add also fits in our reconstruction path
    // (max 18+18 digit inputs, result could be 19 digits from carry,
    // but checked_add already guards against i64 overflow).
    Some(numeric_from_i64_coeff(result, a.exponent()))
}

/// Fast-path subtraction of two `Numeric` values, bypassing the C FFI.
///
/// Same constraints as [`try_add_fast`].
#[inline]
pub fn try_sub_fast(a: &Numeric, b: &Numeric) -> Option<Numeric> {
    if a.exponent() != b.exponent() {
        return None;
    }
    let ca = coeff_to_i64(a)?;
    let cb = coeff_to_i64(b)?;
    let result = ca.checked_sub(cb)?;
    Some(numeric_from_i64_coeff(result, a.exponent()))
}

/// Fast-path multiplication of two `Numeric` values, bypassing the C FFI.
///
/// Returns `Some(result)` if both operands are finite, their coefficients fit
/// in i64 (≤18 digits each), and the product fits within
/// [`NUMERIC_DATUM_MAX_PRECISION`] (39 digits) without rescaling.
/// Returns `None` to fall back to the general FFI path.
#[inline]
pub fn try_mul_fast(a: &Numeric, b: &Numeric) -> Option<Numeric> {
    if a.is_special() || b.is_special() {
        return None;
    }
    if a.is_zero() || b.is_zero() {
        let lsu = [0u16; 13];
        return Some(Numeric::from_raw_parts(1, 0, 0, lsu));
    }
    let ca = coeff_to_i64(a)?;
    let cb = coeff_to_i64(b)?;
    // i64 × i64 always fits in i128 (max 36 digits < 39 i128 digits).
    let product: i128 = ca as i128 * cb as i128;
    let result_exp = a.exponent() + b.exponent();
    numeric_from_i128_coeff(product, result_exp)
}

/// Build a `Numeric` from an i128 coefficient and exponent, if it fits within
/// [`NUMERIC_DATUM_MAX_PRECISION`] (39 digits).
///
/// Returns `None` if the result would exceed max precision (needs rescaling).
#[inline]
fn numeric_from_i128_coeff(val: i128, exponent: i32) -> Option<Numeric> {
    if val == 0 {
        let lsu = [0u16; 13];
        return Some(Numeric::from_raw_parts(1, exponent, 0, lsu));
    }
    let (bits, magnitude) = if val < 0 {
        (0x80u8, (val as u128).wrapping_neg()) // DECNEG
    } else {
        (0u8, val as u128)
    };
    let mut lsu = [0u16; 13];
    let mut remaining = magnitude;
    let mut n_units = 0usize;
    while remaining > 0 {
        lsu[n_units] = (remaining % 1000) as u16;
        remaining /= 1000;
        n_units += 1;
        if n_units > 13 {
            return None; // Overflow, shouldn't happen for i64×i64
        }
    }
    let top_unit = lsu[n_units - 1];
    let top_digits = if top_unit >= 100 {
        3
    } else if top_unit >= 10 {
        2
    } else {
        1
    };
    let sig_digits = (n_units as u32 - 1) * 3 + top_digits;
    // Check if result precision fits within max (39 digits).
    // precision = max(sig_digits, -exponent) for negative exponents,
    //           = sig_digits + exponent for non-negative exponents.
    let precision = if exponent >= 0 {
        sig_digits + exponent as u32
    } else {
        std::cmp::max(sig_digits, (-exponent) as u32)
    };
    if precision > NUMERIC_DATUM_MAX_PRECISION as u32 {
        return None;
    }
    Some(Numeric::from_raw_parts(sig_digits, exponent, bits, lsu))
}

/// Fast-path comparison of two `Numeric` values, bypassing the C FFI.
///
/// `OrderedDecimal<Decimal<N>>::cmp()` calls `reduce` on both operands (2 C FFI
/// calls) plus `partial_cmp` (1 C FFI call) on every comparison. This function
/// avoids all three FFI calls for the common case where both values are finite.
///
/// For values with the same exponent (which covers >99% of comparisons since
/// column values share the same SQL scale), this compares coefficient units
/// directly as integers. For different exponents, it uses the adjusted exponent
/// (magnitude) for fast ordering and only falls back to C FFI when the adjusted
/// exponents are equal but raw exponents differ (which requires rescaling).
///
/// Maintains the `OrderedDecimal` convention: NaN > all non-NaN values.
#[inline]
pub fn fast_numeric_cmp(a: &Numeric, b: &Numeric) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // Special values (NaN, Inf) are rare — fall back to C FFI.
    if a.is_special() || b.is_special() {
        return OrderedDecimal(*a).cmp(&OrderedDecimal(*b));
    }

    // Both finite.
    let a_zero = a.is_zero();
    let b_zero = b.is_zero();
    if a_zero && b_zero {
        return Ordering::Equal;
    }

    let a_neg = a.is_negative();
    let b_neg = b.is_negative();

    // Zeros: +0 and -0 are equal, handled above. A zero vs non-zero:
    if a_zero {
        return if b_neg {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    if b_zero {
        return if a_neg {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }

    // Different signs — no need to examine coefficients.
    if a_neg != b_neg {
        return if a_neg {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }

    // Same sign. Compare magnitudes via coefficient units.
    // For negative numbers, the magnitude ordering is reversed.

    if a.exponent() == b.exponent() {
        // Same exponent: compare coefficient_units from MSU to LSU.
        let a_units = a.coefficient_units();
        let b_units = b.coefficient_units();
        let a_len = a_units.len();
        let b_len = b_units.len();
        let max_len = if a_len > b_len { a_len } else { b_len };

        // Compare from MSU (highest index) to LSU.
        let mut i = max_len;
        while i > 0 {
            i -= 1;
            let a_u = if i < a_len { a_units[i] } else { 0 };
            let b_u = if i < b_len { b_units[i] } else { 0 };
            if a_u != b_u {
                let ord = if a_u < b_u {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                return if a_neg { ord.reverse() } else { ord };
            }
        }
        Ordering::Equal
    } else {
        // Different exponents — compare adjusted exponents (magnitude).
        // adjusted_exponent = exponent + digits - 1, giving the order of
        // magnitude of the value.
        let a_adj = a.exponent() + a.digits() as i32 - 1;
        let b_adj = b.exponent() + b.digits() as i32 - 1;
        if a_adj != b_adj {
            let ord = a_adj.cmp(&b_adj);
            return if a_neg { ord.reverse() } else { ord };
        }
        // Same adjusted exponent but different raw exponents — rare case
        // requiring coefficient rescaling. Fall back to C FFI.
        OrderedDecimal(*a).cmp(&OrderedDecimal(*b))
    }
}

/// Reduces a `Numeric` value by stripping trailing zeros from the coefficient,
/// entirely in Rust without C FFI calls.
///
/// This is equivalent to `Context::reduce()` for the NUMERIC_DATUM context but
/// avoids the C FFI overhead. After reduction:
/// - Zero values are normalized to +0 with exponent 0
/// - Non-zero values have trailing zeros stripped (up to the context's
///   max_exponent of 38)
/// - Special values (NaN, Infinity) are unchanged
/// - Values with extreme exponents (outside [-39, 38]) fall back to FFI
#[inline]
pub fn fast_numeric_reduce(n: &mut Numeric) {
    if n.is_special() {
        return;
    }

    if n.is_zero() {
        // Canonical zero: +0E+0 (1 digit, exponent 0, positive)
        *n = Numeric::zero();
        return;
    }

    let exponent = n.exponent();

    // For extreme exponents (e.g., Numeric::from(f64::MAX) with exponent ~292),
    // fall back to FFI reduce which handles clamping/overflow correctly.
    let max_exponent = NUMERIC_DATUM_MAX_PRECISION as i32 - 1; // 38
    let min_exponent = -(NUMERIC_DATUM_MAX_PRECISION as i32); // -39
    if exponent > max_exponent || exponent < min_exponent {
        cx_datum().reduce(n);
        return;
    }

    // Quick check via coefficient_units() (no struct copy) — if the LSU
    // is not divisible by 10, the value is already reduced.
    let units = n.coefficient_units();
    if units[0] % 10 != 0 {
        return;
    }

    let digits = n.digits();

    // Count total trailing decimal zeros in the coefficient.
    let mut total_trailing = 0u32;
    for &u in units.iter() {
        if u == 0 {
            total_trailing += 3;
        } else {
            let mut tmp = u;
            while tmp % 10 == 0 {
                tmp /= 10;
                total_trailing += 1;
            }
            break;
        }
    }

    // Cap stripping so the new exponent doesn't exceed max_exponent.
    // The FFI reduce with cx_datum() (max_exponent=38) behaves the same way.
    let max_strip = (max_exponent - exponent) as u32;
    let total_trailing = total_trailing.min(max_strip);

    if total_trailing == 0 {
        return; // Capped at max_exponent
    }

    let new_digits = digits - total_trailing;
    let new_exponent = exponent + total_trailing as i32;
    // For non-special, non-zero values, bits is just the sign flag.
    let bits: u8 = if n.is_negative() { 0x80 } else { 0 };

    // Split: how many whole base-1000 units to skip, and how many
    // remaining decimal zeros to divide out of the partial unit.
    let whole_units_to_skip = (total_trailing / 3) as usize;
    let partial_zeros = total_trailing % 3;

    let remaining = &units[whole_units_to_skip..];

    let mut new_lsu = [0u16; NUMERIC_DATUM_WIDTH_USIZE];

    if partial_zeros == 0 {
        // Only whole units were zero — just copy remaining units.
        new_lsu[..remaining.len()].copy_from_slice(remaining);
    } else {
        // Divide the remaining coefficient by 10^partial_zeros to strip
        // partial trailing zeros. Process from MSU to LSU, carrying
        // remainders downward through the base-1000 units.
        let divisor = if partial_zeros == 1 { 10u32 } else { 100u32 };
        let mut carry = 0u32;
        for i in (0..remaining.len()).rev() {
            let val = carry * 1000 + remaining[i] as u32;
            new_lsu[i] = (val / divisor) as u16;
            carry = val % divisor;
        }
        debug_assert_eq!(carry, 0, "trailing zeros should divide evenly");
    }

    *n = Numeric::from_raw_parts(new_digits, new_exponent, bits, new_lsu);
}

/// Computes the digit count of a `Numeric` after reduction (trailing-zero
/// stripping), without any C FFI calls or cloning.
///
/// This is used by `datum_size` to compute the encoded size of a Numeric
/// value without the overhead of cloning + FFI reduce.
#[inline]
pub fn reduced_numeric_digit_count(n: &Numeric) -> u32 {
    if n.is_special() || n.is_zero() {
        return 1;
    }

    let exponent = n.exponent();
    let max_exponent = NUMERIC_DATUM_MAX_PRECISION as i32 - 1; // 38
    let min_exponent = -(NUMERIC_DATUM_MAX_PRECISION as i32); // -39

    // For extreme exponents, fall back to clone + FFI.
    if exponent > max_exponent || exponent < min_exponent {
        let mut d = *n;
        cx_datum().reduce(&mut d);
        return d.digits();
    }

    let units = n.coefficient_units();

    // Count total trailing decimal zeros.
    let mut trailing = 0u32;
    for &u in units.iter() {
        if u == 0 {
            trailing += 3;
        } else {
            let mut tmp = u;
            while tmp % 10 == 0 {
                tmp /= 10;
                trailing += 1;
            }
            break;
        }
    }

    // Cap stripping so the new exponent doesn't exceed max_exponent.
    let max_strip = (max_exponent - exponent) as u32;
    let trailing = trailing.min(max_strip);

    n.digits() - trailing
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

    // Fast path: for values with ≤18 significant digits (coefficient fits in i64)
    // and non-positive exponent. When exponent <= 0, scaleb only adjusts the exponent
    // to 0 without changing the coefficient, so we can write the coefficient directly
    // as 17-byte big-endian twos complement without any FFI calls.
    // (Positive exponents require value = coeff * 10^exp, which we skip here.)
    if numeric.exponent() <= 0 {
        if let Some(coeff) = coeff_to_i64(&numeric) {
            // i64 → i128 sign-extension → 16-byte big-endian
            let c128 = coeff as i128;
            let be = c128.to_be_bytes();
            // 17-byte buffer: byte 0 is sign extension, bytes 1-16 are the i128 big-endian
            buf[0] = if coeff < 0 { 0xFF } else { 0x00 };
            buf[1..17].copy_from_slice(&be);
            return buf;
        }
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

    // Fast path: for values with ≤18 significant digits (coefficient fits in i64)
    // and exponent in [-39, 0]. The wide encoding represents value * 10^39, which
    // for i64-range coefficients fits in ~192 bits (well within 33-byte / 264-bit
    // buffer). We compute this using 256-bit arithmetic (u128 pair) entirely in Rust,
    // avoiding all C FFI calls (to_width, rescale, scaleb, rem, div_integer, coefficient).
    if numeric.exponent() >= -39 && numeric.exponent() <= 0 {
        if let Some(coeff) = coeff_to_i64(&numeric) {
            let pow_exp = (39 + numeric.exponent()) as usize; // 0..=39
            // Compute |coeff| * 10^pow_exp using 256-bit arithmetic.
            let is_neg = coeff < 0;
            let abs_coeff = if coeff == i64::MIN {
                i64::MIN as u64 // 2^63, wrapping neg would overflow
            } else if coeff < 0 {
                (-coeff) as u64
            } else {
                coeff as u64
            };
            let (lo, hi) = u256_mul_u64_pow10(abs_coeff as u128, pow_exp);
            write_twos_complement_be_33(&mut buf, lo, hi, is_neg);
            return buf;
        }
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

/// Powers of 10 as u128, precomputed for fast 256-bit multiplication.
/// Covers 10^0 through 10^38 (u128 max is ~3.4×10^38; 10^39 overflows).
const POW10_U128: [u128; 39] = {
    let mut table = [0u128; 39];
    table[0] = 1;
    let mut i = 1;
    while i < 39 {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

/// Multiply a u128 value by 10^pow_exp, returning a 256-bit result as (lo, hi).
/// The result is `lo + hi * 2^128`. Supports pow_exp 0..=39.
#[inline]
fn u256_mul_u64_pow10(val: u128, pow_exp: usize) -> (u128, u128) {
    debug_assert!(pow_exp <= 39);
    if pow_exp <= 38 {
        u256_mul(val, POW10_U128[pow_exp])
    } else {
        // pow_exp == 39: 10^39 overflows u128, but val fits in u64.
        // Compute val * 10^38 * 10.
        let (lo, hi) = u256_mul(val, POW10_U128[38]);
        u256_mul_small(lo, hi, 10)
    }
}

/// Multiply two u128 values, returning a 256-bit result as (lo, hi).
#[inline]
fn u256_mul(a: u128, b: u128) -> (u128, u128) {
    // Schoolbook multiplication using 64-bit halves.
    let a_lo = a as u64 as u128;
    let a_hi = (a >> 64) as u64 as u128;
    let b_lo = b as u64 as u128;
    let b_hi = (b >> 64) as u64 as u128;

    let ll = a_lo * b_lo;
    let lh = a_lo * b_hi;
    let hl = a_hi * b_lo;
    let hh = a_hi * b_hi;

    // Accumulate: result = ll + (lh + hl) << 64 + hh << 128
    let (mid, mid_carry) = lh.overflowing_add(hl);

    let (lo, lo_carry) = ll.overflowing_add(mid << 64);
    let hi = hh + (mid >> 64) + ((mid_carry as u128) << 64) + lo_carry as u128;

    (lo, hi)
}

/// Multiply a 256-bit value (lo, hi) by a small factor, returning a 256-bit result.
/// The small factor must fit in u64. The result must fit in 256 bits.
#[inline]
fn u256_mul_small(lo: u128, hi: u128, factor: u64) -> (u128, u128) {
    let factor = factor as u128;
    // Multiply lo part: result can overflow u128.
    let lo_lo = (lo as u64 as u128) * factor;
    let lo_hi = ((lo >> 64) as u64 as u128) * factor;
    let new_lo = lo_lo + (lo_hi << 64);
    let carry = (lo_hi >> 64) + if new_lo < lo_lo { 1 } else { 0 };
    let new_hi = hi * factor + carry;
    (new_lo, new_hi)
}

/// Write a 256-bit unsigned value as 33-byte big-endian twos complement.
/// The value is represented as `lo + hi * 2^128`. If `is_neg`, the value
/// is negated in twos complement.
#[inline]
fn write_twos_complement_be_33(buf: &mut [u8; 33], lo: u128, hi: u128, is_neg: bool) {
    let (lo, hi) = if is_neg {
        // Twos complement negation: !val + 1
        let neg_lo = !lo;
        let (neg_lo, carry) = neg_lo.overflowing_add(1);
        let neg_hi = !hi + carry as u128;
        (neg_lo, neg_hi)
    } else {
        (lo, hi)
    };
    // byte 0: sign extension byte (top of 264-bit value)
    buf[0] = if is_neg { 0xFF } else { 0x00 };
    // bytes 1..17: hi as big-endian
    buf[1..17].copy_from_slice(&hi.to_be_bytes());
    // bytes 17..33: lo as big-endian
    buf[17..33].copy_from_slice(&lo.to_be_bytes());
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

/// Writes a `Decimal` in standard notation directly to a `fmt::Write`
/// implementation with **zero heap allocations**.
///
/// This replaces `Decimal::to_standard_notation_string()` which performs two
/// heap allocations per call: one for `coefficient_digits()` (Vec<u8>) and
/// one for the result String. Instead, this function extracts digits from
/// `coefficient_units()` (a zero-copy &[u16] slice) into a stack-allocated
/// buffer and writes the entire result with a single `write_str()` call.
pub fn write_numeric_standard_notation<const N: usize>(
    f: &mut impl fmt::Write,
    d: &Decimal<N>,
) -> fmt::Result {
    // Non-finite values (NaN, Infinity) delegate to Display
    if !d.is_finite() {
        return write!(f, "{}", d);
    }

    let units = d.coefficient_units();
    let ndigits = d.digits() as usize;
    let exponent = d.exponent();
    let nunit = units.len();

    // Convert coefficient units (base-1000, little-endian) to decimal digits.
    // Each unit holds 3 decimal digits (DECDPUN = 3).
    // Max 39 digits for Numeric (Decimal<13>), 81 for NumericAgg (Decimal<27>).
    let mut digit_buf = [0u8; 82]; // 82 > 81 = max for Decimal<27>
    let mut pos = 0;

    // Most-significant unit may have 1, 2, or 3 digits
    let msu = units[nunit - 1];
    let msu_ndigits = if ndigits % 3 == 0 { 3 } else { ndigits % 3 };
    if msu_ndigits >= 3 {
        digit_buf[pos] = (msu / 100) as u8;
        pos += 1;
    }
    if msu_ndigits >= 2 {
        digit_buf[pos] = ((msu / 10) % 10) as u8;
        pos += 1;
    }
    digit_buf[pos] = (msu % 10) as u8;
    pos += 1;

    // Remaining units (full 3-digit groups), most-significant first
    for i in (0..nunit.saturating_sub(1)).rev() {
        let u = units[i];
        digit_buf[pos] = (u / 100) as u8;
        digit_buf[pos + 1] = ((u / 10) % 10) as u8;
        digit_buf[pos + 2] = (u % 10) as u8;
        pos += 3;
    }

    // Strip leading zeros (matching to_standard_notation_string behavior)
    let raw_digits = &digit_buf[..ndigits];
    let first_nonzero = raw_digits.iter().position(|&d| d != 0).unwrap_or(ndigits - 1);
    let digits = &raw_digits[first_nonzero..];
    let sig_digits = digits.len() as i32;

    // Build output in a stack buffer. Max size:
    // 1 (sign) + 3*N (digits) + (3*N - 1) (trailing zeros) + 1 (point) = 6*N + 1
    // For N=27: 163. Use 200 for safety.
    let mut out = [0u8; 200];
    let mut opos = 0;

    if d.is_negative() {
        out[opos] = b'-';
        opos += 1;
    }

    if exponent >= 0 {
        // All digits before the decimal point, plus trailing zeros
        for &digit in digits {
            out[opos] = b'0' + digit;
            opos += 1;
        }
        if !d.is_zero() {
            for _ in 0..exponent {
                out[opos] = b'0';
                opos += 1;
            }
        }
    } else if sig_digits > -exponent {
        // Digits span the decimal point
        let before_point = (sig_digits + exponent) as usize;
        for &digit in &digits[..before_point] {
            out[opos] = b'0' + digit;
            opos += 1;
        }
        out[opos] = b'.';
        opos += 1;
        for &digit in &digits[before_point..] {
            out[opos] = b'0' + digit;
            opos += 1;
        }
    } else {
        // All digits after the decimal point
        out[opos] = b'0';
        out[opos + 1] = b'.';
        opos += 2;
        for _ in 0..(-exponent - sig_digits) {
            out[opos] = b'0';
            opos += 1;
        }
        for &digit in digits {
            out[opos] = b'0' + digit;
            opos += 1;
        }
    }

    // Safety: we only wrote ASCII bytes (0-9, '-', '.')
    let s = unsafe { std::str::from_utf8_unchecked(&out[..opos]) };
    f.write_str(s)
}

/// A stack-allocated string buffer for numeric formatting without heap allocation.
///
/// Captures the output of [`write_numeric_standard_notation`] in a fixed-size
/// stack buffer. This avoids the two heap allocations that
/// `Decimal::to_standard_notation_string()` performs (one for the coefficient
/// digits Vec, one for the result String).
///
/// The 200-byte buffer is sufficient for any `Decimal<N>` representation:
/// max digits (81 for Decimal<27>) + sign + decimal point + leading zeros.
#[derive(Debug)]
pub struct NumericStackStr {
    buf: [u8; 200],
    len: usize,
}

impl NumericStackStr {
    /// Format a `Decimal` into a stack-allocated string.
    pub fn new<const N: usize>(d: &Decimal<N>) -> Self {
        let mut s = Self {
            buf: [0; 200],
            len: 0,
        };
        write_numeric_standard_notation(&mut s, d).expect("200 bytes sufficient for any Decimal");
        s
    }

    /// Returns the formatted string as a `&str`.
    pub fn as_str(&self) -> &str {
        // Safety: write_numeric_standard_notation only writes ASCII bytes (0-9, '-', '.')
        unsafe { std::str::from_utf8_unchecked(&self.buf[..self.len]) }
    }
}

impl fmt::Write for NumericStackStr {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let bytes = s.as_bytes();
        let new_len = self.len + bytes.len();
        if new_len > self.buf.len() {
            return Err(fmt::Error);
        }
        self.buf[self.len..new_len].copy_from_slice(bytes);
        self.len = new_len;
        Ok(())
    }
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
fn test_write_numeric_standard_notation() {
    fn check(input: &str) {
        let mut cx = cx_datum();
        let d: Numeric = cx.parse(input).unwrap();
        let expected = d.to_standard_notation_string();
        let mut got = String::new();
        write_numeric_standard_notation(&mut got, &d).unwrap();
        assert_eq!(got, expected, "mismatch for input {input:?}");
    }

    // Zero and signed zero
    check("0");
    check("-0");
    check("0.00");
    check("0E+10");

    // Small integers
    check("1");
    check("42");
    check("-1");
    check("-42");

    // Large integers
    check("999999999999999999999999999999999999999");
    check("-999999999999999999999999999999999999999");
    check("170141183460469231731687303715884105727");

    // Decimals
    check("123.456789");
    check("-3.14159265358979323846264338327950288");
    check("0.000001");
    check("0.000000000000000000000000000000000012345");
    check("0.123456789012345678901234567890123456789");
    check("1.00000000000000000000000000000000000000");

    // Trailing zeros after decimal point
    check("12345678901234567890.1234567890123456789");

    // Large exponents (trailing zeros)
    check("7E+35");
    check("1E+38");
    check("-7.2E+35");

    // Small exponents (leading zeros after decimal)
    check("7E-35");
    check("1E-38");
    check("-7.2E-35");

    // Edge cases
    check("1E-39"); // minimum exponent for Numeric
    check("9.99999999999999999999999999999999999999E+38"); // near max

    // Single digit values
    check("5");
    check("0.5");
    check("5E+10");
    check("5E-10");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_numeric_stack_str() {
    fn check(input: &str) {
        let mut cx = cx_datum();
        let d: Numeric = cx.parse(input).unwrap();
        let expected = d.to_standard_notation_string();
        let buf = NumericStackStr::new(&d);
        assert_eq!(buf.as_str(), expected, "mismatch for input {input:?}");
    }

    check("0");
    check("-0");
    check("1");
    check("42");
    check("123.456789");
    check("-3.14159265358979323846264338327950288");
    check("0.000001");
    check("999999999999999999999999999999999999999");
    check("1E+10");
    check("1E-10");
    check("0.123456789012345678901234567890123456789");
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
        // Fast path: bypass C FFI for i64 values.
        numeric_from_i64_coeff(i, 0)
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
        fn optional_numeric_max_scale_protobuf_roundtrip(
            expect in any::<Option<NumericMaxScale>>(),
        ) {
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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_try_add_sub_fast() {
        let mut cx = cx_datum();

        // Helper: parse, add via fast path, compare with FFI result
        fn check_add(a_str: &str, b_str: &str) {
            let mut cx = cx_datum();
            let a: Numeric = cx.parse(a_str).unwrap();
            let b: Numeric = cx.parse(b_str).unwrap();
            // FFI reference
            let mut expected = a.clone();
            cx.add(&mut expected, &b);
            // Fast path
            if let Some(fast) = try_add_fast(&a, &b) {
                assert_eq!(
                    fast, expected,
                    "add mismatch for {a_str} + {b_str}: fast={fast}, expected={expected}"
                );
            }
            // Also check subtraction: a - b
            let mut expected_sub = a.clone();
            cx.sub(&mut expected_sub, &b);
            if let Some(fast_sub) = try_sub_fast(&a, &b) {
                assert_eq!(
                    fast_sub, expected_sub,
                    "sub mismatch for {a_str} - {b_str}: fast={fast_sub}, expected={expected_sub}"
                );
            }
        }

        // Same scale integers
        check_add("0", "0");
        check_add("1", "2");
        check_add("100", "200");
        check_add("-1", "1");
        check_add("-50", "-50");
        check_add("999999999999999999", "1"); // 18 digits + 1
        check_add("-999999999999999999", "-1");

        // Same scale decimals
        check_add("1.5", "2.5");
        check_add("123.456", "789.012");
        check_add("-1.5", "2.5");
        check_add("0.001", "0.002");
        check_add("999999999999.999999", "0.000001");

        // Different scales should return None (fall through)
        let a: Numeric = cx.parse("1.5").unwrap(); // exponent -1
        let b: Numeric = cx.parse("2.50").unwrap(); // exponent -2
        // These have different exponents, so fast path should return None
        if a.exponent() != b.exponent() {
            assert!(try_add_fast(&a, &b).is_none());
        }

        // Large values (>18 digits) should return None
        let big: Numeric = cx.parse("1234567890123456789").unwrap(); // 19 digits
        let one: Numeric = cx.parse("1").unwrap();
        assert!(try_add_fast(&big, &one).is_none());

        // Zero edge cases
        check_add("0", "5");
        check_add("5", "0");
        check_add("0", "0");
        check_add("-0", "0");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_try_numeric_from_int() {
        // Helper: compare fast-path with FFI-based construction
        fn check_i64(val: i64, scale: Option<u8>) {
            let fast = try_numeric_from_i64(val, scale);
            // Build reference via FFI
            let mut cx = cx_datum();
            let mut reference = Numeric::from(val);
            if let Some(s) = scale {
                let _ = rescale(&mut reference, s);
            }
            match fast {
                Some(f) => {
                    assert_eq!(
                        f.to_standard_notation_string(),
                        reference.to_standard_notation_string(),
                        "i64 mismatch for val={val}, scale={scale:?}: fast={f}, ref={reference}"
                    );
                }
                None => {
                    // Fast path declined - that's ok, just ensure it's a valid edge case
                }
            }
        }

        fn check_u64(val: u64, scale: Option<u8>) {
            let fast = try_numeric_from_u64(val, scale);
            let mut cx = cx_datum();
            let mut reference = Numeric::from(val);
            if let Some(s) = scale {
                let _ = rescale(&mut reference, s);
            }
            match fast {
                Some(f) => {
                    assert_eq!(
                        f.to_standard_notation_string(),
                        reference.to_standard_notation_string(),
                        "u64 mismatch for val={val}, scale={scale:?}: fast={f}, ref={reference}"
                    );
                }
                None => {}
            }
        }

        // No scale
        check_i64(0, None);
        check_i64(1, None);
        check_i64(-1, None);
        check_i64(42, None);
        check_i64(-42, None);
        check_i64(i32::MAX as i64, None);
        check_i64(i32::MIN as i64, None);
        check_i64(i64::MAX, None);
        check_i64(i64::MIN, None);
        check_i64(999_999_999_999_999_999, None);
        check_i64(-999_999_999_999_999_999, None);

        // With scale
        check_i64(42, Some(0));
        check_i64(42, Some(2));
        check_i64(42, Some(6));
        check_i64(-100, Some(3));
        check_i64(1, Some(10));
        check_i64(0, Some(5));
        check_i64(i32::MAX as i64, Some(2));
        check_i64(i16::MAX as i64, Some(10));

        // Unsigned
        check_u64(0, None);
        check_u64(1, None);
        check_u64(42, None);
        check_u64(u32::MAX as u64, None);
        check_u64(u64::MAX, None);
        check_u64(42, Some(2));
        check_u64(u16::MAX as u64, Some(6));
        check_u64(u32::MAX as u64, Some(2));

        // The fast path must always handle the no-scale case for all integer sizes
        assert!(try_numeric_from_i64(0, None).is_some());
        assert!(try_numeric_from_i64(i64::MAX, None).is_some());
        assert!(try_numeric_from_i64(i64::MIN, None).is_some());
        assert!(try_numeric_from_u64(0, None).is_some());
        assert!(try_numeric_from_u64(u64::MAX, None).is_some());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_fast_numeric_reduce() {
        let mut cx = cx_datum();

        // Helper: compare fast_numeric_reduce result against FFI reduce.
        fn check(n: &Numeric) {
            let mut fast = *n;
            fast_numeric_reduce(&mut fast);

            let mut ffi = *n;
            cx_datum().reduce(&mut ffi);

            // Compare all raw parts.
            let (fd, fe, fb, fl) = fast.to_raw_parts();
            let (cd, ce, cb, cl) = ffi.to_raw_parts();
            assert_eq!(
                (fd, fe, fb),
                (cd, ce, cb),
                "mismatch for input={n}: fast=({fd},{fe},{fb:#x}), ffi=({cd},{ce},{cb:#x})"
            );
            let fast_units = &fl[..Numeric::digits_to_lsu_elements_len(fd)];
            let ffi_units = &cl[..Numeric::digits_to_lsu_elements_len(cd)];
            assert_eq!(
                fast_units, ffi_units,
                "lsu mismatch for input={n}: fast={fast_units:?}, ffi={ffi_units:?}"
            );

            // Also check reduced_numeric_digit_count matches.
            let count = reduced_numeric_digit_count(n);
            assert_eq!(count, fd, "digit count mismatch for input={n}");
        }

        // Already reduced values
        check(&Numeric::from(0i32));
        check(&Numeric::from(1i32));
        check(&Numeric::from(-1i32));
        check(&Numeric::from(42i32));
        check(&Numeric::from(-999i32));
        check(&Numeric::from(i32::MAX));
        check(&Numeric::from(i32::MIN));

        // Values with trailing zeros
        let v100: Numeric = cx.parse("100").unwrap();
        check(&v100);
        let v1000: Numeric = cx.parse("1000").unwrap();
        check(&v1000);
        let v10000: Numeric = cx.parse("10000").unwrap();
        check(&v10000);

        // Values with trailing fractional zeros
        let v1_50: Numeric = cx.parse("1.50").unwrap();
        check(&v1_50);
        let v1_500: Numeric = cx.parse("1.500").unwrap();
        check(&v1_500);
        let v1_000: Numeric = cx.parse("1.000").unwrap();
        check(&v1_000);

        // Large values with trailing zeros
        let large: Numeric = cx.parse("123456789000").unwrap();
        check(&large);
        let large2: Numeric = cx.parse("100000000000000").unwrap();
        check(&large2);

        // Negative values with trailing zeros
        let neg100: Numeric = cx.parse("-100").unwrap();
        check(&neg100);
        let neg1_50: Numeric = cx.parse("-1.50").unwrap();
        check(&neg1_50);

        // Zero variants
        let mut neg_zero = Numeric::from(0i32);
        cx.minus(&mut neg_zero);
        check(&neg_zero);
        let zero_exp: Numeric = cx.parse("0E+5").unwrap();
        check(&zero_exp);

        // Special values
        check(&Numeric::nan());
        let mut inf = Numeric::infinity();
        check(&inf);
        cx.minus(&mut inf);
        check(&inf);

        // Money-like values (already reduced, common case)
        let money: Numeric = cx.parse("12345.67").unwrap();
        check(&money);
        let money2: Numeric = cx.parse("99999.99").unwrap();
        check(&money2);

        // Edge case: single-digit values
        for i in 0..10 {
            check(&Numeric::from(i as i32));
        }

        // Edge case: powers of 10
        let mut pow = 1i64;
        for _ in 0..18 {
            let s = pow.to_string();
            let n: Numeric = cx.parse(s.as_bytes()).unwrap();
            check(&n);
            pow *= 10;
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_fast_numeric_cmp() {
        use dec::OrderedDecimal;

        let mut cx = cx_datum();

        // Helper: compare fast_numeric_cmp result against OrderedDecimal::cmp.
        fn check(a: &Numeric, b: &Numeric) {
            let fast = fast_numeric_cmp(a, b);
            let ffi = OrderedDecimal(*a).cmp(&OrderedDecimal(*b));
            assert_eq!(
                fast, ffi,
                "mismatch for a={}, b={}: fast={fast:?}, ffi={ffi:?}",
                a, b,
            );
            // Also check reverse.
            let fast_rev = fast_numeric_cmp(b, a);
            let ffi_rev = OrderedDecimal(*b).cmp(&OrderedDecimal(*a));
            assert_eq!(fast_rev, ffi_rev, "reverse mismatch for a={a}, b={b}");
        }

        // Zero comparisons
        let zero = Numeric::from(0i32);
        let pos = Numeric::from(42i32);
        let neg = Numeric::from(-42i32);
        check(&zero, &zero);
        check(&zero, &pos);
        check(&zero, &neg);
        check(&pos, &zero);
        check(&neg, &zero);

        // Same exponent, different values
        check(&Numeric::from(1i32), &Numeric::from(2i32));
        check(&Numeric::from(100i32), &Numeric::from(99i32));
        check(&Numeric::from(-50i32), &Numeric::from(-100i32));

        // Equal values
        check(&Numeric::from(42i32), &Numeric::from(42i32));
        check(&Numeric::from(-7i32), &Numeric::from(-7i32));

        // Same exponent, large coefficients
        let a_large: Numeric = cx.parse("123456789012345678").unwrap();
        let b_large: Numeric = cx.parse("987654321098765432").unwrap();
        check(&a_large, &b_large);

        // Money-like (same exponent -2)
        let a_money: Numeric = cx.parse("12345.67").unwrap();
        let b_money: Numeric = cx.parse("98765.43").unwrap();
        check(&a_money, &b_money);
        check(&b_money, &a_money);

        // Different exponents
        let a_diff: Numeric = cx.parse("100").unwrap();
        let b_diff: Numeric = cx.parse("99.99").unwrap();
        check(&a_diff, &b_diff);

        // Different exponents, same adjusted exponent (1.20 vs 1.2)
        let a_trail: Numeric = cx.parse("1.20").unwrap();
        let b_trail: Numeric = cx.parse("1.2").unwrap();
        check(&a_trail, &b_trail);

        // Negative values
        let a_neg: Numeric = cx.parse("-500.25").unwrap();
        let b_neg: Numeric = cx.parse("-100.50").unwrap();
        check(&a_neg, &b_neg);

        // Mixed signs
        check(&a_neg, &b_money);
        check(&b_money, &a_neg);

        // NaN
        let nan = Numeric::nan();
        check(&nan, &pos);
        check(&pos, &nan);
        check(&nan, &nan);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_try_mul_fast() {
        let mut cx = cx_datum();

        // Helper: compare fast-path result against FFI multiplication.
        fn check(a: &Numeric, b: &Numeric) {
            let fast = try_mul_fast(a, b);
            // Compute FFI result.
            let mut cx = cx_datum();
            let mut ffi_result = *a;
            cx.mul(&mut ffi_result, b);
            assert!(
                !cx.status().overflow() && !cx.status().subnormal(),
                "FFI mul overflowed for a={a}, b={b}"
            );
            super::munge_numeric(&mut ffi_result).unwrap();

            if let Some(fast_val) = fast {
                // Reduce both for comparison (trailing zeros may differ).
                let mut fast_reduced = fast_val;
                let mut ffi_reduced = ffi_result;
                cx_datum().reduce(&mut fast_reduced);
                cx_datum().reduce(&mut ffi_reduced);
                assert_eq!(
                    fast_reduced.to_string(),
                    ffi_reduced.to_string(),
                    "mismatch for {a} * {b}: fast={fast_val}, ffi={ffi_result}",
                );
            }
            // If fast returns None, that's OK — it means fallback to FFI.
        }

        // Basic cases
        check(&Numeric::from(0i32), &Numeric::from(42i32));
        check(&Numeric::from(42i32), &Numeric::from(0i32));
        check(&Numeric::from(1i32), &Numeric::from(1i32));
        check(&Numeric::from(7i32), &Numeric::from(6i32));
        check(&Numeric::from(-7i32), &Numeric::from(6i32));
        check(&Numeric::from(-7i32), &Numeric::from(-6i32));

        // Money-like: price * quantity
        let price: Numeric = cx.parse("12345.67").unwrap();
        let qty: Numeric = cx.parse("99").unwrap();
        check(&price, &qty);

        // Same scale
        let a_money: Numeric = cx.parse("100.00").unwrap();
        let b_money: Numeric = cx.parse("50.00").unwrap();
        check(&a_money, &b_money);

        // Large values (still < 18 digits each)
        let a_large: Numeric = cx.parse("999999999999.99").unwrap();
        let b_large: Numeric = cx.parse("123456").unwrap();
        check(&a_large, &b_large);

        // Negative × positive
        let a_neg: Numeric = cx.parse("-500.25").unwrap();
        let b_pos: Numeric = cx.parse("200.50").unwrap();
        check(&a_neg, &b_pos);

        // Negative × negative
        check(&a_neg, &a_neg);

        // Very small values
        let tiny: Numeric = cx.parse("0.000001").unwrap();
        let small: Numeric = cx.parse("0.000002").unwrap();
        check(&tiny, &small);

        // One is 1
        check(&Numeric::from(1i32), &price);
        check(&price, &Numeric::from(1i32));

        // Large coefficient (> 18 digits) should return None (fall back)
        let big: Numeric = cx.parse("1234567890123456789.12").unwrap();
        assert!(
            try_mul_fast(&big, &Numeric::from(2i32)).is_none(),
            "should fall back for >18 digit coefficient"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_numeric_to_twos_complement_be_fast_path() {
        let mut cx = cx_datum();

        // Reference implementation: always uses FFI path.
        fn reference_twos_complement_be(
            mut numeric: Numeric,
        ) -> [u8; Numeric::TWOS_COMPLEMENT_BYTE_WIDTH] {
            let mut buf = [0; Numeric::TWOS_COMPLEMENT_BYTE_WIDTH];
            if numeric.is_special() {
                return buf;
            }
            let mut cx = Numeric::context();
            if numeric.exponent() < 0 {
                let s = Numeric::from(-numeric.exponent());
                cx.scaleb(&mut numeric, &s);
            }
            numeric_to_twos_complement_inner::<Numeric, NUMERIC_DATUM_WIDTH_USIZE>(
                numeric, &mut cx, &mut buf,
            );
            buf
        }

        fn check(n: Numeric) {
            let fast = numeric_to_twos_complement_be(n);
            let slow = reference_twos_complement_be(n);
            assert_eq!(
                fast, slow,
                "twos complement mismatch for {n} (exp={}): fast={fast:?}, slow={slow:?}",
                n.exponent()
            );
        }

        // Zero
        check(Numeric::from(0i32));

        // Small positive/negative integers
        check(Numeric::from(1i32));
        check(Numeric::from(-1i32));
        check(Numeric::from(42i32));
        check(Numeric::from(-42i32));
        check(Numeric::from(i32::MAX));
        check(Numeric::from(i32::MIN));

        // Money-like decimals (negative exponent)
        check(cx.parse("12345.67").unwrap());
        check(cx.parse("-99999.99").unwrap());
        check(cx.parse("0.001").unwrap());
        check(cx.parse("-0.001").unwrap());

        // Large coefficients that still fit in i64
        check(cx.parse("999999999999999999").unwrap());
        check(cx.parse("-999999999999999999").unwrap());

        // Values after rescale (simulating Avro encoding)
        let mut v: Numeric = cx.parse("123.45").unwrap();
        let _ = rescale(&mut v, 6);
        check(v);

        let mut v: Numeric = cx.parse("-9876543.21").unwrap();
        let _ = rescale(&mut v, 10);
        check(v);

        // Positive exponent (should fallback to FFI, but still correct)
        let v: Numeric = cx.parse("1E+10").unwrap();
        check(v);
        let v: Numeric = cx.parse("-5E+5").unwrap();
        check(v);

        // Large coefficient (> 18 digits, must use FFI path)
        check(cx.parse("12345678901234567890.12").unwrap());
        check(cx.parse("-99999999999999999999.99").unwrap());

        // Special values
        check(Numeric::infinity());
        let nan: Numeric = cx.parse("NaN").unwrap();
        check(nan);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_numeric_to_twos_complement_wide_fast_path() {
        let mut cx = cx_datum();

        /// Reference implementation: always uses the FFI path.
        fn reference_twos_complement_wide(
            numeric: Numeric,
        ) -> [u8; NumericAgg::TWOS_COMPLEMENT_BYTE_WIDTH] {
            let mut buf = [0; NumericAgg::TWOS_COMPLEMENT_BYTE_WIDTH];
            if numeric.is_special() {
                return buf;
            }
            let mut cx = NumericAgg::context();
            let mut d = cx.to_width(numeric);
            let mut scaler = NumericAgg::from(NUMERIC_DATUM_MAX_PRECISION);
            cx.neg(&mut scaler);
            cx.rescale(&mut d, &scaler);
            cx.abs(&mut scaler);
            cx.scaleb(&mut d, &scaler);
            numeric_to_twos_complement_inner::<NumericAgg, NUMERIC_AGG_WIDTH_USIZE>(
                d, &mut cx, &mut buf,
            );
            buf
        }

        fn check(n: Numeric) {
            let fast = numeric_to_twos_complement_wide(n);
            let slow = reference_twos_complement_wide(n);
            assert_eq!(
                fast, slow,
                "wide twos complement mismatch for {n} (exp={}): fast={fast:?}, slow={slow:?}",
                n.exponent()
            );
        }

        // Zero
        check(Numeric::from(0i32));

        // Small positive/negative integers
        check(Numeric::from(1i32));
        check(Numeric::from(-1i32));
        check(Numeric::from(42i32));
        check(Numeric::from(-42i32));
        check(Numeric::from(i32::MAX));
        check(Numeric::from(i32::MIN));

        // Money-like decimals (negative exponent)
        check(cx.parse("12345.67").unwrap());
        check(cx.parse("-99999.99").unwrap());
        check(cx.parse("0.001").unwrap());
        check(cx.parse("-0.001").unwrap());

        // Large coefficients that still fit in i64
        check(cx.parse("999999999999999999").unwrap());
        check(cx.parse("-999999999999999999").unwrap());

        // Values after rescale (simulating Avro encoding)
        let mut v: Numeric = cx.parse("123.45").unwrap();
        let _ = rescale(&mut v, 6);
        check(v);

        let mut v: Numeric = cx.parse("-9876543.21").unwrap();
        let _ = rescale(&mut v, 10);
        check(v);

        // Positive exponent (should fallback to FFI, but still correct)
        let v: Numeric = cx.parse("1E+10").unwrap();
        check(v);
        let v: Numeric = cx.parse("-5E+5").unwrap();
        check(v);

        // Large coefficient (> 18 digits, must use FFI path)
        check(cx.parse("12345678901234567890.12").unwrap());
        check(cx.parse("-99999999999999999999.99").unwrap());

        // Values with extreme negative exponents
        check(cx.parse("0.000000000000000000000000000000000000001").unwrap()); // 1E-39
        check(cx.parse("-0.000000000000000000000000000000000000001").unwrap()); // -1E-39

        // Edge case: coeff=1 with exp=-39 → 1 * 10^0 = 1
        let mut v: Numeric = cx.parse("1").unwrap();
        cx.rescale(&mut v, &Numeric::from(-39i32));
        check(v);

        // Large coefficient with scale
        check(cx.parse("999999999999.999999").unwrap());
        check(cx.parse("-999999999999.999999").unwrap());

        // Special values
        check(Numeric::infinity());
        let nan: Numeric = cx.parse("NaN").unwrap();
        check(nan);
    }
}
