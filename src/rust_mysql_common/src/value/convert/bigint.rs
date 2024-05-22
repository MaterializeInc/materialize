// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This module implements conversion from/to `Value` for `BigInt` and `BigUint` types.

use std::convert::TryFrom;

use num_bigint::{BigInt, BigUint};
use num_traits::{FromPrimitive, ToPrimitive};

use super::{FromValue, FromValueError, ParseIr, Value};

impl TryFrom<Value> for ParseIr<BigInt> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Int(x) => Ok(ParseIr(x.into(), v)),
            Value::UInt(x) => Ok(ParseIr(x.into(), v)),
            Value::Bytes(ref bytes) => match BigInt::parse_bytes(bytes, 10) {
                Some(x) => Ok(ParseIr(x, v)),
                None => Err(FromValueError(v)),
            },
            _ => Err(FromValueError(v)),
        }
    }
}

impl From<ParseIr<BigInt>> for BigInt {
    fn from(value: ParseIr<BigInt>) -> Self {
        value.commit()
    }
}

impl From<ParseIr<BigInt>> for Value {
    fn from(value: ParseIr<BigInt>) -> Self {
        value.rollback()
    }
}

impl FromValue for BigInt {
    type Intermediate = ParseIr<BigInt>;
}

impl From<BigInt> for Value {
    fn from(x: BigInt) -> Value {
        if let Some(x) = x.to_i64() {
            Value::Int(x)
        } else if let Some(x) = x.to_u64() {
            Value::UInt(x)
        } else {
            Value::Bytes(x.to_string().into())
        }
    }
}

impl TryFrom<Value> for ParseIr<BigUint> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Int(x) => {
                if let Some(x) = BigUint::from_i64(x) {
                    Ok(ParseIr(x, v))
                } else {
                    Err(FromValueError(v))
                }
            }
            Value::UInt(x) => Ok(ParseIr(x.into(), v)),
            Value::Bytes(ref bytes) => match BigUint::parse_bytes(bytes, 10) {
                Some(x) => Ok(ParseIr(x, v)),
                None => Err(FromValueError(v)),
            },
            _ => Err(FromValueError(v)),
        }
    }
}

impl From<ParseIr<BigUint>> for BigUint {
    fn from(value: ParseIr<BigUint>) -> Self {
        value.commit()
    }
}

impl From<ParseIr<BigUint>> for Value {
    fn from(value: ParseIr<BigUint>) -> Self {
        value.rollback()
    }
}

impl FromValue for BigUint {
    type Intermediate = ParseIr<BigUint>;
}

impl From<BigUint> for Value {
    fn from(x: BigUint) -> Value {
        if let Some(x) = x.to_u64() {
            Value::UInt(x)
        } else {
            Value::Bytes(x.to_string().into())
        }
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::{BigInt, BigUint};
    use proptest::prelude::*;

    use crate::value::{convert::from_value, Value};

    proptest! {
        #[test]
        fn big_int_roundtrip(
            bytes_pos in r"[1-9][0-9]{31}",
            bytes_neg in r"-[1-9][0-9]{31}",
            uint in (i64::max_value() as u64 + 1)..u64::max_value(),
            int: i64,
        ) {
            let val_bytes_pos = Value::Bytes(bytes_pos.as_bytes().into());
            let val_bytes_neg = Value::Bytes(bytes_neg.as_bytes().into());
            let val_uint = Value::UInt(uint);
            let val_int = Value::Int(int);

            assert_eq!(Value::from(from_value::<BigInt>(val_bytes_pos.clone())), val_bytes_pos);
            assert_eq!(Value::from(from_value::<BigInt>(val_bytes_neg.clone())), val_bytes_neg);
            assert_eq!(Value::from(from_value::<BigInt>(val_uint.clone())), val_uint);
            assert_eq!(Value::from(from_value::<BigInt>(val_int.clone())), val_int);
        }

        #[test]
        fn big_uint_roundtrip(
            bytes in r"[1-9][0-9]{31}",
            uint: u64,
            int in 0i64..i64::max_value(),
        ) {
            let val_bytes = Value::Bytes(bytes.as_bytes().into());
            let val_uint = Value::UInt(uint);
            let val_int = Value::Int(int);

            assert_eq!(Value::from(from_value::<BigUint>(val_bytes.clone())), val_bytes);
            assert_eq!(Value::from(from_value::<BigUint>(val_uint.clone())), val_uint);
            assert_eq!(Value::from(from_value::<BigUint>(val_int)), Value::UInt(int as u64));
        }
    }
}
