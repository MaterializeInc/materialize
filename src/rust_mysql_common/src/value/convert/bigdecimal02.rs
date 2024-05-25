// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This module implements conversion from/to `Value` for `BigDecimal` type.

#![cfg(feature = "bigdecimal02")]

use std::convert::{TryFrom, TryInto};

use bigdecimal02::BigDecimal;

use super::{FromValue, FromValueError, ParseIr, Value};

#[cfg_attr(docsrs, doc(cfg(feature = "bigdecimal02")))]
impl TryFrom<Value> for ParseIr<BigDecimal> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Int(x) => Ok(ParseIr(x.into(), v)),
            Value::UInt(x) => Ok(ParseIr(x.into(), v)),
            Value::Float(x) => match x.try_into() {
                Ok(x) => Ok(ParseIr(x, v)),
                Err(_) => Err(FromValueError(v)),
            },
            Value::Double(x) => match x.try_into() {
                Ok(x) => Ok(ParseIr(x, v)),
                Err(_) => Err(FromValueError(v)),
            },
            Value::Bytes(ref bytes) => match BigDecimal::parse_bytes(bytes, 10) {
                Some(x) => Ok(ParseIr(x, v)),
                None => Err(FromValueError(v)),
            },
            _ => Err(FromValueError(v)),
        }
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "bigdecimal02")))]
impl From<ParseIr<BigDecimal>> for BigDecimal {
    fn from(value: ParseIr<BigDecimal>) -> Self {
        value.commit()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "bigdecimal02")))]
impl From<ParseIr<BigDecimal>> for Value {
    fn from(value: ParseIr<BigDecimal>) -> Self {
        value.rollback()
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "bigdecimal02")))]
impl FromValue for BigDecimal {
    type Intermediate = ParseIr<BigDecimal>;
}

#[cfg_attr(docsrs, doc(cfg(feature = "bigdecimal02")))]
impl From<BigDecimal> for Value {
    fn from(big_decimal: BigDecimal) -> Value {
        Value::Bytes(big_decimal.to_string().into())
    }
}

#[cfg(test)]
mod tests {
    use bigdecimal02::BigDecimal;
    use proptest::prelude::*;

    use crate::value::{convert::from_value, Value};

    proptest! {
        #[test]
        fn big_decimal_roundtrip(
            sign in r"-?",
            m in r"[0-9]{1,38}",
            d in r"[0-9]{0,38}",
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
            let decimal = from_value::<BigDecimal>(val.clone());
            let val2 = Value::from(decimal);
            assert_eq!(val, val2);
        }
    }
}
