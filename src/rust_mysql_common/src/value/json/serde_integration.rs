// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::convert::TryFrom;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::{self, Value as Json};

use super::{Deserialized, Serialized};
use crate::value::{
    convert::{FromValue, FromValueError, ParseIr},
    Value,
};

impl TryFrom<Value> for ParseIr<Json> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Bytes(ref bytes) => match serde_json::from_slice(bytes) {
                Ok(x) => Ok(ParseIr(x, v)),
                Err(_) => Err(FromValueError(v)),
            },
            v => Err(FromValueError(v)),
        }
    }
}

impl From<ParseIr<Json>> for Json {
    fn from(value: ParseIr<Json>) -> Self {
        value.commit()
    }
}

impl From<ParseIr<Json>> for Value {
    fn from(value: ParseIr<Json>) -> Self {
        value.rollback()
    }
}

impl FromValue for Json {
    type Intermediate = ParseIr<Json>;
}

impl<T: DeserializeOwned> TryFrom<Value> for ParseIr<Deserialized<T>> {
    type Error = FromValueError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Bytes(ref bytes) => match serde_json::from_slice(bytes) {
                Ok(x) => Ok(ParseIr(Deserialized(x), v)),
                Err(_) => Err(FromValueError(v)),
            },
            v => Err(FromValueError(v)),
        }
    }
}

impl<T: DeserializeOwned> From<ParseIr<Deserialized<T>>> for Deserialized<T> {
    fn from(value: ParseIr<Deserialized<T>>) -> Self {
        value.commit()
    }
}

impl<T: DeserializeOwned> From<ParseIr<Deserialized<T>>> for Value {
    fn from(value: ParseIr<Deserialized<T>>) -> Self {
        value.rollback()
    }
}

impl<T: DeserializeOwned> FromValue for Deserialized<T> {
    type Intermediate = ParseIr<Deserialized<T>>;
}

impl From<Json> for Value {
    fn from(x: Json) -> Value {
        Value::Bytes(serde_json::to_string(&x).unwrap().into())
    }
}

impl<T: Serialize> From<Serialized<T>> for Value {
    fn from(x: Serialized<T>) -> Value {
        Value::Bytes(serde_json::to_string(&x.0).unwrap().into())
    }
}
