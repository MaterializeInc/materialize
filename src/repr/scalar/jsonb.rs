// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::fmt;
use std::str::FromStr;

use failure::bail;

use crate::{Datum, DatumPacker, Row, RowPacker};

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

    pub fn pack_into(self, packer: &mut RowPacker) {
        fn pack(val: serde_json::Value, packer: DatumPacker) {
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
                Value::Array(array) => packer.push_list_with(|packer| {
                    for elem in array {
                        pack(elem, packer.as_datum_packer());
                    }
                }),
                Value::Object(object) => packer.push_dict_with(|dict| {
                    let mut pairs = object.into_iter().collect::<Vec<_>>();
                    // dict keys must be in ascending order
                    pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(&k2));
                    for (key, val) in pairs {
                        dict.push_with(&key, |packer| pack(val, packer));
                    }
                }),
            }
        }
        pack(self.inner, packer.as_datum_packer())
    }

    pub fn into_row(self) -> Row {
        let mut packer = RowPacker::new();
        self.pack_into(&mut packer);
        packer.finish()
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
