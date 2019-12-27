// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::fmt;

use bytes::{BufMut, BytesMut};
use postgres_types::{to_sql_checked, IsNull, ToSql, Type};

use repr::decimal::Decimal;

/// A wrapper for [`repr::decimal::Decimal`] that can be serialized and
/// deserialized to the PostgreSQL binary format.
#[derive(Debug)]
pub struct Numeric(pub Decimal);

impl fmt::Display for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ToSql for Numeric {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        // This implementation is derived from Diesel.
        // https://github.com/diesel-rs/diesel/blob/bd13f2460/diesel/src/pg/types/numeric.rs
        let mut significand = self.0.significand();
        let scale = u16::from(self.0.scale());
        let non_neg = significand >= 0;
        significand = significand.abs();

        // Ensure that the significand will always lie on a digit boundary.
        for _ in 0..(4 - scale % 4) {
            significand *= 10;
        }

        let mut digits = vec![];
        while significand > 0 {
            digits.push((significand % 10_000) as i16);
            significand /= 10_000;
        }
        digits.reverse();
        let digits_after_decimal = scale / 4 + 1;
        let weight = digits.len() as i16 - digits_after_decimal as i16 - 1;

        let unnecessary_zeroes = if weight >= 0 {
            let index_of_decimal = (weight + 1) as usize;
            digits
                .get(index_of_decimal..)
                .expect("enough digits exist")
                .iter()
                .rev()
                .take_while(|i| **i == 0)
                .count()
        } else {
            0
        };

        let relevant_digits = digits.len() - unnecessary_zeroes;
        digits.truncate(relevant_digits);

        let sign = if non_neg { 0 } else { 0x4000 };

        out.put_u16(digits.len() as u16);
        out.put_i16(weight);
        out.put_u16(sign);
        out.put_u16(scale);
        for digit in digits.iter() {
            out.put_i16(*digit);
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::NUMERIC => true,
            _ => false,
        }
    }

    to_sql_checked!();
}
