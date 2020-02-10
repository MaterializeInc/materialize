// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;

use failure::bail;

use crate::{Datum, Row, RowPacker};

#[derive(Debug)]
pub struct Jsonb {
    inner: serde_json::Value,
}

impl Jsonb {
    pub fn new(inner: serde_json::Value) -> Result<Self, failure::Error> {
        fn validate(val: &serde_json::Value) -> Result<(), failure::Error> {
            use serde_json::Value;
            match val {
                Value::Number(n) => {
                    if let Some(n) = n.as_i64() {
                        if n > (1 << 53) || n < (-1 << 53) {
                            bail!("{} is out of range for a jsonb number", n)
                        }
                    }
                }
                Value::Null | Value::Bool(_) | Value::String(_) => (),
                Value::Array(vals) => {
                    for v in vals {
                        validate(v)?;
                    }
                }
                Value::Object(vals) => {
                    for (_, v) in vals {
                        validate(v)?;
                    }
                }
            }
            Ok(())
        }
        validate(&inner)?;
        Ok(Jsonb { inner })
    }

    #[allow(clippy::float_cmp)]
    pub fn from_datum(datum: Datum) -> Self {
        fn inner(datum: Datum) -> serde_json::Value {
            use serde_json::Value;
            match datum {
                Datum::JsonNull => Value::Null,
                Datum::True => Value::Bool(true),
                Datum::False => Value::Bool(false),
                Datum::Float64(f) => {
                    let f: f64 = f.into();
                    if let Some(n) = serde_json::Number::from_f64(f) {
                        Value::Number(n)
                    } else {
                        // This should only be reachable for NaN/Infinity, which
                        // aren't allowed to be cast to Jsonb.
                        panic!("Not a valid json number: {}", f)
                    }
                }
                Datum::String(s) => Value::String(s.to_owned()),
                Datum::List(list) => Value::Array(list.iter().map(|e| inner(e)).collect()),
                Datum::Dict(dict) => {
                    Value::Object(dict.iter().map(|(k, v)| (k.to_owned(), inner(v))).collect())
                }
                _ => panic!("Not a json-compatible datum: {:?}", datum),
            }
        }
        Jsonb {
            inner: inner(datum),
        }
    }

    pub fn as_serde_json(&self) -> &serde_json::Value {
        &self.inner
    }

    pub fn pack_into(self, packer: RowPacker) -> RowPacker {
        fn pack(val: serde_json::Value, mut packer: RowPacker) -> RowPacker {
            use serde_json::Value;
            match val {
                Value::Null => packer.push(Datum::JsonNull),
                Value::Bool(b) => {
                    if b {
                        packer.push(Datum::True)
                    } else {
                        packer.push(Datum::False)
                    }
                }
                Value::Number(n) => {
                    let f = n.as_f64().expect("numbers have been validated to be f64s");
                    packer.push(Datum::Float64(f.into()))
                }
                Value::String(s) => packer.push(Datum::String(&s)),
                Value::Array(array) => {
                    let start = unsafe { packer.start_list() };
                    for elem in array {
                        // If we panic here the packer might be in an invalid
                        // state, but we throw the packer away so it's safe.
                        packer = pack(elem, packer);
                    }
                    unsafe { packer.finish_list(start) };
                }
                Value::Object(object) => {
                    let start = unsafe { packer.start_dict() };
                    let mut pairs = object.into_iter().collect::<Vec<_>>();
                    // dict keys must be in ascending order
                    pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(&k2));
                    for (key, val) in pairs {
                        packer.push(Datum::String(&key));
                        // If we panic here the packer might be in an invalid
                        // state, but we throw the packer away so it's safe.
                        packer = pack(val, packer);
                    }
                    unsafe { packer.finish_dict(start) };
                }
            }
            packer
        }
        pack(self.inner, packer)
    }

    pub fn into_row(self) -> Row {
        self.pack_into(RowPacker::new()).finish()
    }
}

impl fmt::Display for Jsonb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl FromStr for Jsonb {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Jsonb::new(serde_json::from_str(s)?)
    }
}
