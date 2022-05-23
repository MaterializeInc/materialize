// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::as_conversions)]

use std::error::Error;
use std::fmt;

use byteorder::{NetworkEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use dec::OrderedDecimal;
use once_cell::sync::Lazy;
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

use mz_repr::adt::numeric::{self, cx_datum, Numeric as AdtNumeric, NumericAgg};

/// (TO BE DEPRECATED) A wrapper for the `repr` crate's `Decimal` type that can be serialized to
/// and deserialized from the PostgreSQL binary format.
#[derive(Debug)]
pub struct Numeric(pub OrderedDecimal<AdtNumeric>);

impl fmt::Display for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<AdtNumeric> for Numeric {
    fn from(n: AdtNumeric) -> Numeric {
        Numeric(OrderedDecimal(n))
    }
}

/// `ToSql` and `FromSql` use base 10,000 units.
const TO_FROM_SQL_BASE_POW: usize = 4;

static TO_SQL_BASER: Lazy<AdtNumeric> =
    Lazy::new(|| AdtNumeric::from(10u32.pow(TO_FROM_SQL_BASE_POW as u32)));
static FROM_SQL_SCALER: Lazy<AdtNumeric> = Lazy::new(|| AdtNumeric::from(TO_FROM_SQL_BASE_POW));

/// The maximum number of units necessary to represent a valid [`AdtNumeric`] value.
const UNITS_LEN: usize = 11;

impl ToSql for Numeric {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        let mut d = self.0 .0.clone();
        let scale = u16::from(numeric::get_scale(&d));
        let is_nan = d.is_nan();
        let is_neg = d.is_negative();
        let is_infinite = d.is_infinite();

        let mut cx = numeric::cx_datum();
        // Need to extend exponents slightly because fractional components need
        // to be aligned to base 10,000.
        cx.set_max_exponent(cx.max_exponent() + TO_FROM_SQL_BASE_POW as isize)
            .unwrap();
        cx.set_min_exponent(cx.min_exponent() - TO_FROM_SQL_BASE_POW as isize)
            .unwrap();
        cx.abs(&mut d);

        let mut digits = [0u16; UNITS_LEN];
        let mut d_i = UNITS_LEN;

        let (fract_units, leading_zero_units) = if d.exponent() < 0 {
            let pos_exp = usize::try_from(-d.exponent()).expect("positive value < 40");
            // You have leading zeroes in the case where:
            // - The exponent's absolute value exceeds the number of digits
            // - `d` only contains fractional zeroes
            let leading_zero_units = if pos_exp >= d.digits() as usize {
                // If the value is zero, one zero digit gets double counted
                // (this is also why the above inequality is not strict)
                let digits = if d.is_zero() { 0 } else { d.digits() as usize };
                // integer division with rounding up instead of down
                (pos_exp - digits + TO_FROM_SQL_BASE_POW - 1) / TO_FROM_SQL_BASE_POW
            } else {
                0
            };

            // Ensure most significant fractional digit in ten's place of base
            // 10,000 value.
            let s = pos_exp % TO_FROM_SQL_BASE_POW;
            let unit_shift_exp = if s != 0 {
                pos_exp + TO_FROM_SQL_BASE_POW - s
            } else {
                pos_exp
            };

            // Convert d into a "canonical coefficient" with most significant
            // fractional digit properly aligned.
            cx.scaleb(&mut d, &AdtNumeric::from(unit_shift_exp));

            (
                u16::try_from(unit_shift_exp / TO_FROM_SQL_BASE_POW).expect("value < 40"),
                leading_zero_units,
            )
        } else {
            (0, 0)
        };

        let mut w = d.clone();
        while !d.is_zero() && !d.is_special() {
            d_i -= 1;
            // Get unit value, i.e. d % 10,000
            cx.rem(&mut d, &TO_SQL_BASER);
            // Decimal library doesn't support direct u16 conversion.
            digits[d_i] =
                u16::try_from(u32::try_from(d).expect("value < 10,000")).expect("value < 10,000");
            cx.div_integer(&mut w, &TO_SQL_BASER);
            d = w;
        }
        d_i -= leading_zero_units;

        let units = u16::try_from(UNITS_LEN - d_i).unwrap();
        let weight = i16::try_from(units - fract_units).unwrap() - 1;

        out.put_u16(units);
        out.put_i16(weight);
        // sign
        out.put_u16(if is_infinite {
            if is_neg {
                0xF000
            } else {
                0xD000
            }
        } else if is_nan {
            0xC000
        } else if is_neg {
            0x4000
        } else {
            0
        });
        out.put_u16(scale);
        if !is_nan {
            for digit in digits[d_i..].iter() {
                out.put_u16(*digit);
            }
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Numeric {
    fn from_sql(_: &Type, mut raw: &'a [u8]) -> Result<Numeric, Box<dyn Error + Sync + Send>> {
        let units = usize::from(raw.read_u16::<NetworkEndian>()?);
        let weight = raw.read_i16::<NetworkEndian>()?;
        let sign = raw.read_u16::<NetworkEndian>()?;
        let in_scale = raw.read_i16::<NetworkEndian>()?;
        let mut digits = vec![];
        for _ in 0..units {
            digits.push(raw.read_u16::<NetworkEndian>()?)
        }

        // We need wider context because decoding values can require >39 digits
        // of precision given how alignment works.
        let mut cx = numeric::cx_agg();
        let mut d = NumericAgg::zero();

        for digit in digits[..units].iter() {
            cx.scaleb(&mut d, &FROM_SQL_SCALER);
            let n = AdtNumeric::from(u32::from(*digit));
            cx.add(&mut d, &n);
        }

        match sign {
            0 => (),
            // Infinity
            0xD000 => return Ok(Numeric::from(AdtNumeric::infinity())),
            // -Infinity
            0xF000 => {
                let mut cx = numeric::cx_datum();
                let mut d = AdtNumeric::infinity();
                cx.neg(&mut d);
                return Ok(Numeric::from(d));
            }
            // Negative
            0x4000 => cx.neg(&mut d),
            // NaN
            0xC000 => return Ok(Numeric::from(AdtNumeric::nan())),
            _ => return Err("bad sign in numeric".into()),
        }

        let mut scale = (units as i16 - weight - 1) * 4;

        // Adjust scales
        if scale < 0 {
            // Multiply by 10^scale
            cx.scaleb(&mut d, &AdtNumeric::from(-i32::from(scale)));
            scale = 0;
        } else if scale > in_scale {
            // Divide by 10^(difference in scale and in_scale)
            cx.scaleb(&mut d, &AdtNumeric::from(-i32::from(scale - in_scale)));
            scale = in_scale;
        }

        cx.scaleb(&mut d, &AdtNumeric::from(-i32::from(scale)));
        cx.reduce(&mut d);

        let mut cx = cx_datum();
        let d_datum = cx.to_width(d);

        // Reducing before taking to datum width lets us check for any status
        // for errors.
        if d.is_infinite() || cx.status().any() {
            return Err(format!("Unable to take bytes to numeric value; rendered {}", d).into());
        }
        Ok(Numeric::from(d_datum))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

#[test]
fn test_to_from_sql_roundtrip() {
    fn inner(s: &str) {
        let mut cx = numeric::cx_datum();
        let d = cx.parse(s).unwrap();
        let r = Numeric(OrderedDecimal(d));

        let mut out = BytesMut::new();

        let _ = r.to_sql(&Type::NUMERIC, &mut out).unwrap();

        let d_from_sql = Numeric::from_sql(&Type::NUMERIC, &out).unwrap();
        assert_eq!(r.0, d_from_sql.0);
    }
    inner("0");
    inner("-0");
    inner("0.1");
    inner("0.0");
    inner("0.00");
    inner("0.000");
    inner("0.0000");
    inner("0.00000");
    inner("123456789.012346789");
    inner("000000000000000000000000000000000000001");
    inner("000000000000000000000000000000000000000");
    inner("999999999999999999999999999999999999999");
    inner("123456789012345678901234567890123456789");
    inner("-123456789012345678901234567890123456789");
    inner(".123456789012345678901234567890123456789");
    inner(".000000000000000000000000000000000000001");
    inner(".000000000000000000000000000000000000000");
    inner(".999999999999999999999999999999999999999");
    inner("-0.123456789012345678901234567890123456789");
    inner("1e25");
    inner("-1e25");
    inner("9.876e-25");
    inner("-9.876e-25");
    inner("98760000");
    inner(".00009876");
    inner("-.00009876");
    inner("NaN");

    // Test infinity, which is a valid value in aggregations over numeric
    let mut cx = numeric::cx_datum();
    let v = vec![
        cx.parse("-999999999999999999999999999999999999999")
            .unwrap(),
        cx.parse("-999999999999999999999999999999999999999")
            .unwrap(),
    ];
    // -Infinity
    let s = cx.sum(v.iter());
    assert!(s.is_infinite());
    let r = Numeric::from(s);
    let mut out = BytesMut::new();

    let _ = r.to_sql(&Type::NUMERIC, &mut out).unwrap();

    let d_from_sql = Numeric::from_sql(&Type::NUMERIC, &out).unwrap();
    assert_eq!(r.0, d_from_sql.0);
}
