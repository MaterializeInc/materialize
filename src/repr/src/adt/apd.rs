// Copyright Materialize, Inc. All rights reserved.
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

lazy_static! {
    static ref CX_DATUM: Context<Apd> = {
        let mut cx = Context::<Apd>::default();
        cx.set_max_exponent(isize::try_from(APD_DATUM_MAX_PRECISION - 1).unwrap())
            .unwrap();
        cx.set_min_exponent(-(isize::try_from(APD_DATUM_MAX_PRECISION).unwrap()))
            .unwrap();
        cx
    };
}

/// Returns a new context appropriate for operating on APD datums.
pub fn cx_datum() -> Context<Apd> {
    CX_DATUM.clone()
}

/// Parses am `i128` from a buffer storing the two's complement representation
/// of a set of bytes in big-endian byte order.
pub fn twos_complement_be_to_i128(input: &[u8]) -> Result<i128, anyhow::Error> {
    if input.len() > 16 {
        bail!("value exceeds i128; more than 16 bytes");
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

    Ok(significand)
}

/// Returns `n`'s precision, i.e. the total number of digits represented by `n`
/// in standard notation not including a zero in the "one's place" in (-1,1).
fn get_precision(n: &Apd) -> u32 {
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
    u8::try_from(-n.exponent()).unwrap()
}

/// Rescale's `n` to fit within [`Apd`]'s max precision or error if not
/// possible.
pub fn rescale_within_max_precision(n: &mut Apd) -> Result<(), anyhow::Error> {
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

/// Rescale `n` as an `OrderedDecimal` with the described scale and return the
/// scale used.
pub fn rescale(n: &mut Apd, scale: u8) -> Result<(), anyhow::Error> {
    let mut cx = Context::<Apd>::default();
    cx.rescale(n, &Apd::from(-i32::from(scale)));
    if get_precision(n) > APD_DATUM_MAX_PRECISION as u32 {
        bail!(
            "APD value {} exceed maximum precision {}",
            n,
            APD_DATUM_MAX_PRECISION
        )
    }

    Ok(())
}
