// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This module implements conversion from/to `Value` for `Decimal` type.

#![cfg(feature = "rust_decimal")]

use rust_decimal::Decimal;

use std::{
    convert::TryFrom,
    str::{from_utf8, FromStr},
};

use super::{FromValue, FromValueError, ParseIr, Value};

#[cfg_attr(docsrs, doc(cfg(feature = "rust_decimal")))]
impl TryFrom<Value> for ParseIr<Decimal> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Int(x) => Ok(ParseIr(x.into(), v)),
            Value::UInt(x) => Ok(ParseIr(x.into(), v)),
            Value::Bytes(ref bytes) => match from_utf8(bytes) {
                Ok(x) => match Decimal::from_str(x) {
                    Ok(x) => Ok(ParseIr(x, v)),
                    Err(_) => Err(FromValueError(v)),
                },
                Err(_) => Err(FromValueError(v)),
            },
            _ => Err(FromValueError(v)),
        }
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "rust_decimal")))]
impl From<ParseIr<Decimal>> for Decimal {
    fn from(value: ParseIr<Decimal>) -> Self {
        value.commit()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "rust_decimal")))]
impl From<ParseIr<Decimal>> for Value {
    fn from(value: ParseIr<Decimal>) -> Self {
        value.rollback()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "rust_decimal")))]
impl FromValue for Decimal {
    type Intermediate = ParseIr<Decimal>;
}

#[cfg_attr(docsrs, doc(cfg(feature = "rust_decimal")))]
impl From<Decimal> for Value {
    fn from(decimal: Decimal) -> Value {
        Value::Bytes(decimal.to_string().into())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal::Decimal;

    use super::super::*;

    proptest! {
        #[test]
        fn decimal_roundtrip(
            sign in r"-?",
            m in r"[0-7][0-9]{1,13}",
            d in r"[0-9]{0,14}",
        ) {
            let m = match m.trim_start_matches('0') {
                "" => "0",
                m => m,
            };
            let sign = if m == "0" && d.chars().all(|b| b == '0') {
                String::new()
            } else {
                sign
            };
            let d = if d.is_empty() {
                String::new()
            } else {
                format!(".{}", d)
            };
            let num = format!("{}{}{}", sign, m , d);
            let val = Value::Bytes(num.as_bytes().to_vec());
            let decimal = from_value::<Decimal>(val.clone());
            let val2 = Value::from(decimal);
            assert_eq!(val, val2);
        }
    }
}
