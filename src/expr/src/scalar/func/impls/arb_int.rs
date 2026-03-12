// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Arbitrary precision integer arithmetic on byte slices.
//!
//! Numbers are represented as little-endian byte slices with no trailing zero
//! bytes (i.e., no leading zeros in the number). The empty slice represents zero.
//!
//! **NUM** functions operate on unsigned numbers (raw `bytea`).
//!
//! **INT** functions operate on signed numbers encoded as `bytea` where the first
//! byte is the sign (`0x01` = positive/zero, `0x00` = negative) and the remaining
//! bytes are the magnitude in little-endian order with no trailing zeros.

use mz_expr_derive::sqlfunc;

// ---------------------------------------------------------------------------
// NUM (unsigned) helpers
// ---------------------------------------------------------------------------

/// Unsigned addition of two little-endian byte slices.
fn num_add(a: &[u8], b: &[u8]) -> Vec<u8> {
    let (long, short) = if a.len() >= b.len() { (a, b) } else { (b, a) };
    let mut result = Vec::with_capacity(long.len() + 1);
    let mut carry: u16 = 0;
    for i in 0..short.len() {
        let sum = carry + long[i] as u16 + short[i] as u16;
        result.push(sum as u8);
        carry = sum >> 8;
    }
    for i in short.len()..long.len() {
        let sum = carry + long[i] as u16;
        result.push(sum as u8);
        carry = sum >> 8;
    }
    if carry != 0 {
        result.push(carry as u8);
    }
    result
}

/// Unsigned multiplication of two little-endian byte slices.
fn num_mul(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut tally: u16 = 0;
    let mut carries: usize = 0;
    for idx in 0..(a.len() + b.len() - 1) {
        let a_min = idx.saturating_sub(b.len() - 1);
        let b_min = idx.saturating_sub(a.len() - 1);
        let a_max = (idx + 1).min(a.len());
        for (a_idx, b_idx) in (a_min..a_max).rev().zip(b_min..) {
            let prod = (a[a_idx] as u16) * (b[b_idx] as u16);
            let (sum, overflow) = tally.overflowing_add(prod);
            tally = sum;
            if overflow {
                carries += 1;
            }
        }
        result.push(tally as u8);
        let upper = (tally >> 8) as u8;
        let carried = carries as u8;
        carries >>= 8;
        tally = upper as u16 + ((carried as u16) << 8);
    }
    while tally != 0 {
        result.push(tally as u8);
        tally >>= 8;
    }
    // Trim trailing zeros.
    while result.last() == Some(&0) {
        result.pop();
    }
    result
}

// ---------------------------------------------------------------------------
// INT (signed) helpers
// ---------------------------------------------------------------------------

/// Compare two unsigned little-endian byte slices. Returns Ordering.
fn num_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match a.len().cmp(&b.len()) {
        Ordering::Equal => {
            for i in (0..a.len()).rev() {
                match a[i].cmp(&b[i]) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            Ordering::Equal
        }
        other => other,
    }
}

/// Unsigned subtraction: a - b where a >= b. Little-endian byte slices.
/// Result has no trailing zeros.
fn num_sub(a: &[u8], b: &[u8]) -> Vec<u8> {
    debug_assert!(num_cmp(a, b) != std::cmp::Ordering::Less);
    let mut result = Vec::with_capacity(a.len());
    let mut borrow: u16 = 0;
    for i in 0..b.len() {
        let x = a[i] as u16;
        let y = b[i] as u16 + borrow;
        if x >= y {
            result.push((x - y) as u8);
            borrow = 0;
        } else {
            result.push((256 + x - y) as u8);
            borrow = 1;
        }
    }
    for i in b.len()..a.len() {
        let x = a[i] as u16;
        if x >= borrow {
            result.push((x - borrow) as u8);
            borrow = 0;
        } else {
            result.push((256 + x - borrow) as u8);
            borrow = 1;
        }
    }
    debug_assert_eq!(borrow, 0);
    // Trim trailing zeros.
    while result.last() == Some(&0) {
        result.pop();
    }
    result
}

/// Decode a signed int from bytea: returns (positive, magnitude).
/// An empty slice is treated as positive zero.
fn int_decode(bytes: &[u8]) -> (bool, &[u8]) {
    if bytes.is_empty() {
        return (true, &[]);
    }
    let positive = bytes[0] != 0;
    (positive, &bytes[1..])
}

/// Encode a signed int to bytea: sign byte followed by magnitude.
fn int_encode(positive: bool, magnitude: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(1 + magnitude.len());
    result.push(if positive || magnitude.is_empty() { 0x01 } else { 0x00 });
    result.extend_from_slice(magnitude);
    result
}

/// Signed addition of two encoded ints.
fn int_add(a: &[u8], b: &[u8]) -> Vec<u8> {
    let (a_pos, a_mag) = int_decode(a);
    let (b_pos, b_mag) = int_decode(b);

    if a_pos == b_pos {
        // Same sign: add magnitudes, keep sign.
        let mag = num_add(a_mag, b_mag);
        int_encode(a_pos, &mag)
    } else {
        // Different signs: subtract smaller from larger.
        match num_cmp(a_mag, b_mag) {
            std::cmp::Ordering::Equal => int_encode(true, &[]),
            std::cmp::Ordering::Greater => {
                let mag = num_sub(a_mag, b_mag);
                int_encode(a_pos, &mag)
            }
            std::cmp::Ordering::Less => {
                let mag = num_sub(b_mag, a_mag);
                int_encode(b_pos, &mag)
            }
        }
    }
}

/// Signed subtraction: a - b, implemented as a + (-b).
fn int_sub(a: &[u8], b: &[u8]) -> Vec<u8> {
    let (b_pos, b_mag) = int_decode(b);
    // Negate b by flipping sign.
    let neg_b = int_encode(!b_pos, b_mag);
    int_add(a, &neg_b)
}

/// Signed multiplication.
fn int_mul(a: &[u8], b: &[u8]) -> Vec<u8> {
    let (a_pos, a_mag) = int_decode(a);
    let (b_pos, b_mag) = int_decode(b);
    let mag = num_mul(a_mag, b_mag);
    // Positive if signs match, or if result is zero.
    int_encode(a_pos == b_pos, &mag)
}

// ---------------------------------------------------------------------------
// SQL function wrappers
// ---------------------------------------------------------------------------

#[sqlfunc(sqlname = "add_num")]
fn add_num<'a>(a: &'a [u8], b: &'a [u8]) -> Vec<u8> {
    num_add(a, b)
}

#[sqlfunc(sqlname = "mul_num")]
fn mul_num<'a>(a: &'a [u8], b: &'a [u8]) -> Vec<u8> {
    num_mul(a, b)
}

#[sqlfunc(sqlname = "add_int")]
fn add_arb_int<'a>(a: &'a [u8], b: &'a [u8]) -> Vec<u8> {
    int_add(a, b)
}

#[sqlfunc(sqlname = "sub_int")]
fn sub_arb_int<'a>(a: &'a [u8], b: &'a [u8]) -> Vec<u8> {
    int_sub(a, b)
}

#[sqlfunc(sqlname = "mul_int")]
fn mul_arb_int<'a>(a: &'a [u8], b: &'a [u8]) -> Vec<u8> {
    int_mul(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_num_add_basic() {
        // 0 + 0 = 0
        assert_eq!(num_add(&[], &[]), Vec::<u8>::new());
        // 1 + 0 = 1
        assert_eq!(num_add(&[1], &[]), vec![1]);
        // 255 + 1 = 256 = [0, 1]
        assert_eq!(num_add(&[255], &[1]), vec![0, 1]);
        // 200 + 200 = 400 = [144, 1]
        assert_eq!(num_add(&[200], &[200]), vec![144, 1]);
    }

    #[mz_ore::test]
    fn test_num_mul_basic() {
        // 0 * anything = 0
        assert_eq!(num_mul(&[], &[5]), Vec::<u8>::new());
        // 1 * 1 = 1
        assert_eq!(num_mul(&[1], &[1]), vec![1]);
        // 2 * 3 = 6
        assert_eq!(num_mul(&[2], &[3]), vec![6]);
        // 16 * 16 = 256 = [0, 1]
        assert_eq!(num_mul(&[16], &[16]), vec![0, 1]);
        // 255 * 255 = 65025 = [1, 254]
        assert_eq!(num_mul(&[255], &[255]), vec![1, 254]);
    }

    #[mz_ore::test]
    fn test_int_add_same_sign() {
        // (+5) + (+3) = +8
        let a = int_encode(true, &[5]);
        let b = int_encode(true, &[3]);
        let r = int_add(&a, &b);
        assert_eq!(int_decode(&r), (true, [8].as_slice()));
    }

    #[mz_ore::test]
    fn test_int_add_diff_sign() {
        // (+5) + (-3) = +2
        let a = int_encode(true, &[5]);
        let b = int_encode(false, &[3]);
        let r = int_add(&a, &b);
        assert_eq!(int_decode(&r), (true, [2].as_slice()));

        // (+3) + (-5) = -2
        let a = int_encode(true, &[3]);
        let b = int_encode(false, &[5]);
        let r = int_add(&a, &b);
        assert_eq!(int_decode(&r), (false, [2].as_slice()));
    }

    #[mz_ore::test]
    fn test_int_sub() {
        // (+5) - (+3) = +2
        let a = int_encode(true, &[5]);
        let b = int_encode(true, &[3]);
        let r = int_sub(&a, &b);
        assert_eq!(int_decode(&r), (true, [2].as_slice()));

        // (+3) - (+5) = -2
        let r = int_sub(&b, &a);
        assert_eq!(int_decode(&r), (false, [2].as_slice()));
    }

    #[mz_ore::test]
    fn test_int_mul() {
        // (+3) * (-4) = -12
        let a = int_encode(true, &[3]);
        let b = int_encode(false, &[4]);
        let r = int_mul(&a, &b);
        assert_eq!(int_decode(&r), (false, [12].as_slice()));

        // (-3) * (-4) = +12
        let a = int_encode(false, &[3]);
        let b = int_encode(false, &[4]);
        let r = int_mul(&a, &b);
        assert_eq!(int_decode(&r), (true, [12].as_slice()));
    }

    #[mz_ore::test]
    fn test_cancelation() {
        // x + (-x) = 0
        let x = int_encode(true, &[42, 7]);
        let neg_x = int_encode(false, &[42, 7]);
        let r = int_add(&x, &neg_x);
        let (pos, mag) = int_decode(&r);
        assert!(pos);
        assert!(mag.is_empty());
    }
}
