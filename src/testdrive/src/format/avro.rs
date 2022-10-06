// Copyright 2018 Flavien Raynaud
// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use anyhow::{anyhow, bail};

use serde_json::Value as JsonValue;

// Re-export components from the various other Avro libraries, so that other
// testdrive modules can import just this one.

pub use mz_avro::schema::{Schema, SchemaKind, SchemaNode, SchemaPiece, SchemaPieceOrNamed};
pub use mz_avro::types::{AvroMap, DecimalValue, ToAvro, Value};
pub use mz_avro::{from_avro_datum, to_avro_datum, Codec, Reader, Writer};
pub use mz_interchange::avro::parse_schema;

// This function is derived from code in the avro_rs project. Update the license
// header on this file accordingly if you move it to a new home.
pub fn from_json(json: &JsonValue, schema: SchemaNode) -> Result<Value, anyhow::Error> {
    match (json, schema.inner) {
        (JsonValue::Null, SchemaPiece::Null) => Ok(Value::Null),
        (JsonValue::Bool(b), SchemaPiece::Boolean) => Ok(Value::Boolean(*b)),
        (JsonValue::Number(ref n), SchemaPiece::Int) => {
            Ok(Value::Int(n.as_i64().unwrap().try_into()?))
        }
        (JsonValue::Number(ref n), SchemaPiece::Long) => Ok(Value::Long(n.as_i64().unwrap())),
        (JsonValue::Number(ref n), SchemaPiece::Float) => {
            Ok(Value::Float(n.as_f64().unwrap() as f32))
        }
        (JsonValue::Number(ref n), SchemaPiece::Double) => Ok(Value::Double(n.as_f64().unwrap())),
        (JsonValue::Number(ref n), SchemaPiece::Date) => {
            Ok(Value::Date(i32::try_from(n.as_i64().unwrap())?))
        }
        (JsonValue::Number(ref n), SchemaPiece::TimestampMilli) => {
            let ts = n.as_i64().unwrap();
            Ok(Value::Timestamp(chrono::NaiveDateTime::from_timestamp(
                ts / 1_000,
                ((ts % 1_000).abs() * 1_000_000) as u32,
            )))
        }
        (JsonValue::Number(ref n), SchemaPiece::TimestampMicro) => {
            let ts = n.as_i64().unwrap();
            Ok(Value::Timestamp(chrono::NaiveDateTime::from_timestamp(
                ts / 1_000_000,
                ((ts % 1_000_000).abs() * 1_000) as u32,
            )))
        }
        (JsonValue::Array(items), SchemaPiece::Array(inner)) => Ok(Value::Array(
            items
                .iter()
                .map(|x| from_json(x, schema.step(&**inner)))
                .collect::<Result<_, _>>()?,
        )),
        (JsonValue::String(s), SchemaPiece::String) => Ok(Value::String(s.clone())),
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
                None => bail!("decimal was not represented by byte array"),
            };
            Ok(Value::Decimal(DecimalValue {
                unscaled: bytes,
                precision: *precision,
                scale: *scale,
            }))
        }
        (JsonValue::Array(items), SchemaPiece::Fixed { size }) => {
            let bytes = match items
                .iter()
                .map(|x| x.as_i64().and_then(|x| u8::try_from(x).ok()))
                .collect::<Option<Vec<u8>>>()
            {
                Some(bytes) => bytes,
                None => bail!("fixed was not represented by byte array"),
            };
            if *size != bytes.len() {
                bail!("expected fixed size {}, got {}", *size, bytes.len())
            } else {
                Ok(Value::Fixed(*size, bytes))
            }
        }
        (JsonValue::String(s), SchemaPiece::Json) => {
            let j = serde_json::from_str(s)?;
            Ok(Value::Json(j))
        }
        (JsonValue::String(s), SchemaPiece::Uuid) => {
            let u = uuid::Uuid::parse_str(s)?;
            Ok(Value::Uuid(u))
        }
        (JsonValue::String(s), SchemaPiece::Enum { symbols, .. }) => {
            if symbols.contains(s) {
                Ok(Value::String(s.clone()))
            } else {
                bail!("Unrecognized enum variant: {}", s)
            }
        }
        (JsonValue::Object(items), SchemaPiece::Record { .. }) => {
            let mut builder = mz_avro::types::Record::new(schema)
                .expect("`Record::new` should never fail if schema piece is a record!");
            for (key, val) in items {
                let field = builder
                    .field_by_name(key)
                    .ok_or_else(|| anyhow!("No such field in record: {}", key))?;
                let val = from_json(val, schema.step(&field.schema))?;
                builder.put(key, val);
            }
            Ok(builder.avro())
        }
        (JsonValue::Object(items), SchemaPiece::Map(m)) => {
            let mut map = HashMap::new();
            for (k, v) in items {
                let (inner, name) = m.get_piece_and_name(schema.root);
                map.insert(
                    k.to_owned(),
                    from_json(
                        v,
                        SchemaNode {
                            root: schema.root,
                            inner,
                            name,
                        },
                    )?,
                );
            }
            Ok(Value::Map(AvroMap(map)))
        }
        (val, SchemaPiece::Union(us)) => {
            let variants = us.variants();
            let null_variant = variants
                .iter()
                .position(|v| v == &SchemaPieceOrNamed::Piece(SchemaPiece::Null));
            if let JsonValue::Null = val {
                return if let Some(nv) = null_variant {
                    Ok(Value::Union {
                        index: nv,
                        inner: Box::new(Value::Null),
                        n_variants: variants.len(),
                        null_variant,
                    })
                } else {
                    bail!("No `null` value in union schema.")
                };
            }
            let items = match val {
                JsonValue::Object(items) => items,
                _ => bail!("Union schema element must be `null` or a map from type name to value; found {:?}", val),
            };
            let (name, val) = if items.len() == 1 {
                (items.keys().next().unwrap(), items.values().next().unwrap())
            } else {
                bail!(
                    "Expected one-element object to match union schema: {:?} vs {:?}",
                    json,
                    schema
                );
            };
            for (i, variant) in variants.iter().enumerate() {
                let name_matches = match variant {
                    SchemaPieceOrNamed::Piece(piece) => SchemaKind::from(piece).name() == name,
                    SchemaPieceOrNamed::Named(idx) => {
                        let schema_name = &schema.root.lookup(*idx).name;
                        if name.chars().any(|ch| ch == '.') {
                            name == &format!(
                                "{}.{}",
                                schema_name.namespace(),
                                schema_name.base_name()
                            )
                        } else {
                            name == schema_name.base_name()
                        }
                    }
                };
                if name_matches {
                    match from_json(val, schema.step(variant)) {
                        Ok(avro) => {
                            return Ok(Value::Union {
                                index: i,
                                inner: Box::new(avro),
                                n_variants: variants.len(),
                                null_variant,
                            })
                        }
                        Err(msg) => return Err(msg),
                    }
                }
            }
            bail!(
                "Type not found in union: {}. variants: {:#?}",
                name,
                variants
            )
        }
        _ => bail!(
            "unable to match JSON value to schema: {:?} vs {:?}",
            json,
            schema
        ),
    }
}
