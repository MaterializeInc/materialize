// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This module implements conversion from/to `Value` for `Uuid`.

use std::convert::TryFrom;

use uuid::Uuid;

use crate::value::Value;

use super::{FromValue, FromValueError, ParseIr};

impl From<Uuid> for Value {
    fn from(uuid: Uuid) -> Value {
        Value::Bytes(uuid.as_bytes().to_vec())
    }
}

impl TryFrom<Value> for ParseIr<Uuid> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Bytes(ref bytes) => match Uuid::from_slice(bytes.as_slice()) {
                Ok(val) => Ok(ParseIr(val, v)),
                Err(_) => Err(FromValueError(v)),
            },
            v => Err(FromValueError(v)),
        }
    }
}

impl From<ParseIr<Uuid>> for Uuid {
    fn from(value: ParseIr<Uuid>) -> Self {
        value.commit()
    }
}

impl From<ParseIr<Uuid>> for Value {
    fn from(value: ParseIr<Uuid>) -> Self {
        value.rollback()
    }
}

impl FromValue for Uuid {
    type Intermediate = ParseIr<Uuid>;
}
