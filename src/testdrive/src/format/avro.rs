// Copyright 2018 Flavien Raynaud
// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the ToAvro implementation for
// serde_json::Value that is shipped with the avro_rs project. The original
// source code was retrieved on April 25, 2019 from:
//
//     https://github.com/flavray/avro-rs/blob/c4971ac08f52750db6bc95559c2b5faa6c0c9a06/src/types.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use std::convert::{TryFrom, TryInto};
use std::num::TryFromIntError;

use avro::schema::{SchemaNode, SchemaPiece};
use avro::types::{DecimalValue, Value as AvroValue};

use serde_json::Value as JsonValue;

// This function is derived from code in the avro_rs project. Update the license
// header on this file accordingly if you move it to a new home.
pub fn json_to_avro(json: &JsonValue, schema: SchemaNode) -> Result<AvroValue, String> {
    match (json, schema.inner) {
        (JsonValue::Null, SchemaPiece::Null) => Ok(AvroValue::Null),
        (JsonValue::Bool(b), SchemaPiece::Boolean) => Ok(AvroValue::Boolean(*b)),
        (JsonValue::Number(ref n), SchemaPiece::Int) => Ok(AvroValue::Int(
            n.as_i64()
                .unwrap()
                .try_into()
                .map_err(|e: TryFromIntError| e.to_string())?,
        )),
        (JsonValue::Number(ref n), SchemaPiece::Long) => Ok(AvroValue::Long(n.as_i64().unwrap())),
        (JsonValue::Number(ref n), SchemaPiece::Float) => {
            Ok(AvroValue::Float(n.as_f64().unwrap() as f32))
        }
        (JsonValue::Number(ref n), SchemaPiece::Double) => {
            Ok(AvroValue::Double(n.as_f64().unwrap()))
        }
        (JsonValue::Number(ref n), SchemaPiece::Date) => Ok(AvroValue::Date(
            chrono::NaiveDate::from_ymd(1970, 1, 1) + chrono::Duration::days(n.as_i64().unwrap()),
        )),
        (JsonValue::Number(ref n), SchemaPiece::TimestampMilli) => {
            let ts = n.as_i64().unwrap();
            Ok(AvroValue::Timestamp(chrono::NaiveDateTime::from_timestamp(
                ts / 1_000,
                ts as u32 % 1_000,
            )))
        }
        (JsonValue::Number(ref n), SchemaPiece::TimestampMicro) => {
            let ts = n.as_i64().unwrap();
            Ok(AvroValue::Timestamp(chrono::NaiveDateTime::from_timestamp(
                ts / 1_000_000,
                ts as u32 % 1_000_000,
            )))
        }
        (JsonValue::Array(items), SchemaPiece::Array(inner)) => Ok(AvroValue::Array(
            items
                .iter()
                .map(|x| json_to_avro(x, schema.step(&**inner)))
                .collect::<Result<_, _>>()?,
        )),
        (JsonValue::String(s), SchemaPiece::String) => Ok(AvroValue::String(s.clone())),
        (
            JsonValue::Array(items),
            SchemaPiece::Decimal {
                precision, scale, ..
            },
        ) => {
            let bytes = match items
                .iter()
                .map(|x| x.as_i64().and_then(|x| u8::try_from(x).ok()))
                .collect::<Option<Vec<u8>>>()
            {
                Some(bytes) => bytes,
                None => return Err("decimal was not represented by byte array".into()),
            };
            Ok(AvroValue::Decimal(DecimalValue {
                unscaled: bytes,
                precision: *precision,
                scale: *scale,
            }))
        }
        (JsonValue::String(s), SchemaPiece::Json) => {
            let j = serde_json::from_str(s).map_err(|e| e.to_string())?;
            Ok(AvroValue::Json(j))
        }
        (JsonValue::Object(items), SchemaPiece::Record { fields, .. }) => Ok(AvroValue::Record(
            items
                .iter()
                .zip(fields)
                .map(|((key, value), field)| {
                    Ok((
                        key.clone(),
                        json_to_avro(value, schema.step(&field.schema))?,
                    ))
                })
                .collect::<Result<_, String>>()?,
        )),
        (val, SchemaPiece::Union(us)) => {
            let variants = us.variants();
            let mut last_err = format!("Union schema {:?} did not match {:?}", variants, val);
            for (i, variant) in variants.iter().enumerate() {
                match json_to_avro(val, schema.step(variant)) {
                    Ok(avro) => return Ok(AvroValue::Union(i, Box::new(avro))),
                    Err(msg) => last_err = msg,
                }
            }
            Err(last_err)
        }
        _ => Err(format!(
            "unable to match JSON value to schema: {:?} vs {:?}",
            json, schema
        )),
    }
}
