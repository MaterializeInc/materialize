// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt;

use mz_ore::collections::CollectionExt;
use mz_repr::adt::char;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{NUMERIC_AGG_MAX_PRECISION, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::{ColumnName, ColumnType, Datum, RelationDesc, ScalarType};
use serde_json::{json, Map};

use crate::encode::{column_names_and_types, Encode, TypedDatum};

// Manages encoding of JSON-encoded bytes
pub struct JsonEncoder {
    key_columns: Option<Vec<(ColumnName, ColumnType)>>,
    value_columns: Vec<(ColumnName, ColumnType)>,
    include_transaction: bool,
}

impl JsonEncoder {
    pub fn new(
        key_desc: Option<RelationDesc>,
        value_desc: RelationDesc,
        include_transaction: bool,
    ) -> Self {
        JsonEncoder {
            key_columns: if let Some(desc) = key_desc {
                Some(column_names_and_types(desc))
            } else {
                None
            },
            value_columns: column_names_and_types(value_desc),
            include_transaction,
        }
    }

    pub fn encode_row(
        &self,
        row: mz_repr::Row,
        names_types: &[(ColumnName, ColumnType)],
    ) -> Vec<u8> {
        let value = encode_datums_as_json(row.iter(), names_types, self.include_transaction);
        value.to_string().into_bytes()
    }
}

impl Encode for JsonEncoder {
    fn get_format_name(&self) -> &str {
        "json"
    }

    fn encode_key_unchecked(&self, row: mz_repr::Row) -> Vec<u8> {
        self.encode_row(
            row,
            self.key_columns.as_ref().expect("key schema must exist"),
        )
    }

    fn encode_value_unchecked(&self, row: mz_repr::Row) -> Vec<u8> {
        self.encode_row(row, &self.value_columns)
    }
}

impl fmt::Debug for JsonEncoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("JsonEncoder")
            .field(
                "schema",
                &format!("{:?}", build_row_schema_json(&self.value_columns, "schema")),
            )
            .finish()
    }
}

/// Encodes a sequence of `Datum` as JSON, using supplied column names and types.
pub fn encode_datums_as_json<'a, I>(
    datums: I,
    names_types: &[(ColumnName, ColumnType)],
    include_transaction: bool,
) -> serde_json::value::Value
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut name_idx = 0;
    let namer = &mut move || {
        let ret = format!("record{}", name_idx);
        name_idx += 1;
        ret
    };
    // todo@jldlaughlin: this is awful and hacky! revisit
    let value_fields = datums
        .into_iter()
        .enumerate()
        .map(|(i, datum)| {
            if i >= names_types.len() && include_transaction {
                let transaction_id = TypedDatum::new(
                    datum.unwrap_list().into_first(),
                    ColumnType {
                        scalar_type: ScalarType::String,
                        nullable: false,
                    },
                )
                .json(namer);
                ("transaction".to_owned(), json!({ "id": transaction_id }))
            } else {
                (
                    names_types[i].0.as_str().to_owned(),
                    TypedDatum::new(datum, names_types[i].1.clone()).json(namer),
                )
            }
        })
        .collect();
    serde_json::value::Value::Object(value_fields)
}

pub trait ToJson {
    /// Transforms this value to a JSON value.
    fn json<F: FnMut() -> String>(self, namer: &mut F) -> serde_json::value::Value;
}

impl<'a> ToJson for TypedDatum<'_> {
    fn json<F: FnMut() -> String>(self, namer: &mut F) -> serde_json::value::Value {
        let TypedDatum { datum, typ } = self;
        if typ.nullable && datum.is_null() {
            serde_json::value::Value::Null
        } else {
            match &typ.scalar_type {
                ScalarType::Bool => json!(datum.unwrap_bool()),
                ScalarType::Int16 => json!(datum.unwrap_int16()),
                ScalarType::Int32 => json!(datum.unwrap_int32()),
                ScalarType::Int64 => json!(datum.unwrap_int64()),
                ScalarType::Oid
                | ScalarType::RegClass
                | ScalarType::RegProc
                | ScalarType::RegType => {
                    json!(datum.unwrap_uint32())
                }
                ScalarType::Float32 => json!(datum.unwrap_float32()),
                ScalarType::Float64 => json!(datum.unwrap_float64()),
                ScalarType::Numeric { .. } => {
                    json!(datum.unwrap_numeric().0.to_standard_notation_string())
                }
                // https://stackoverflow.com/questions/10286204/what-is-the-right-json-date-format
                ScalarType::Date => {
                    serde_json::value::Value::String(format!("{:?}", datum.unwrap_date()))
                }
                ScalarType::Time => {
                    serde_json::value::Value::String(format!("{:?}", datum.unwrap_time()))
                }
                ScalarType::Timestamp => serde_json::value::Value::String(format!(
                    "{:?}",
                    datum.unwrap_timestamp().timestamp_millis()
                )),
                ScalarType::TimestampTz => serde_json::value::Value::String(format!(
                    "{:?}",
                    datum.unwrap_timestamptz().timestamp_millis()
                )),
                ScalarType::Interval => {
                    serde_json::value::Value::String(format!("{}", datum.unwrap_interval()))
                }
                ScalarType::Bytes => json!(datum.unwrap_bytes()),
                ScalarType::String | ScalarType::VarChar { .. } => json!(datum.unwrap_str()),
                ScalarType::Char { length } => {
                    let s = char::format_str_pad(datum.unwrap_str(), *length);
                    serde_json::value::Value::String(s)
                }
                ScalarType::Jsonb => JsonbRef::from_datum(datum).to_serde_json(),
                ScalarType::Uuid => json!(datum.unwrap_uuid()),
                ty @ (ScalarType::Array(..) | ScalarType::Int2Vector | ScalarType::List { .. }) => {
                    let list = match typ.scalar_type {
                        ScalarType::Array(_) | ScalarType::Int2Vector => {
                            datum.unwrap_array().elements()
                        }
                        ScalarType::List { .. } => datum.unwrap_list(),
                        _ => unreachable!(),
                    };
                    let values = list
                        .into_iter()
                        .map(|datum| {
                            let datum = TypedDatum::new(
                                datum,
                                ColumnType {
                                    nullable: true,
                                    scalar_type: ty.unwrap_collection_element_type().clone(),
                                },
                            );
                            datum.json(namer)
                        })
                        .collect();
                    serde_json::value::Value::Array(values)
                }
                ScalarType::Record {
                    fields,
                    custom_name,
                    ..
                } => {
                    let list = datum.unwrap_list();
                    let fields: Map<String, serde_json::value::Value> = fields
                        .iter()
                        .zip(list.into_iter())
                        .map(|((name, typ), datum)| {
                            let name = name.to_string();
                            let datum = TypedDatum::new(datum, typ.clone());
                            let value = datum.json(namer);
                            (name, value)
                        })
                        .collect();

                    let name = match custom_name {
                        Some(name) => name.clone(),
                        None => namer(),
                    };
                    json!({ name: fields })
                }
                ScalarType::Map { value_type, .. } => {
                    let map = datum.unwrap_map();
                    let elements = map
                        .into_iter()
                        .map(|(key, datum)| {
                            let datum = TypedDatum::new(
                                datum,
                                ColumnType {
                                    nullable: true,
                                    scalar_type: (**value_type).clone(),
                                },
                            );
                            let value = datum.json(namer);
                            (key.to_string(), value)
                        })
                        .collect();
                    serde_json::value::Value::Object(elements)
                }
            }
        }
    }
}

fn build_row_schema_field<F: FnMut() -> String>(
    namer: &mut F,
    names_seen: &mut HashSet<String>,
    typ: &ColumnType,
) -> serde_json::value::Value {
    let mut field_type = match &typ.scalar_type {
        ScalarType::Bool => json!("boolean"),
        ScalarType::Int16 | ScalarType::Int32 => {
            json!("int")
        }
        ScalarType::Int64 => json!("long"),
        ScalarType::Oid | ScalarType::RegClass | ScalarType::RegProc | ScalarType::RegType => {
            json!({
                "type": "fixed",
                "size": 4,
            })
        }
        ScalarType::Float32 => json!("float"),
        ScalarType::Float64 => json!("double"),
        ScalarType::Date => json!({
            "type": "int",
            "logicalType": "date",
        }),
        ScalarType::Time => json!({
            "type": "long",
            "logicalType": "time-micros",
        }),
        ScalarType::Timestamp | ScalarType::TimestampTz => json!({
            "type": "long",
            "logicalType": "timestamp-micros"
        }),
        ScalarType::Interval => json!({
            "type": "fixed",
            "size": 12,
            "logicalType": "duration"
        }),
        ScalarType::Bytes => json!("bytes"),
        ScalarType::String | ScalarType::Char { .. } | ScalarType::VarChar { .. } => {
            json!("string")
        }
        ScalarType::Jsonb => json!({
            "type": "string",
            "connect.name": "io.debezium.data.Json",
        }),
        ScalarType::Uuid => json!({
            "type": "string",
            "logicalType": "uuid",
        }),
        ty @ (ScalarType::Array(..) | ScalarType::Int2Vector | ScalarType::List { .. }) => {
            let inner = build_row_schema_field(
                namer,
                names_seen,
                &ColumnType {
                    nullable: true,
                    scalar_type: ty.unwrap_collection_element_type().clone(),
                },
            );
            json!({
                "type": "array",
                "items": inner
            })
        }
        ScalarType::Map { value_type, .. } => {
            let inner = build_row_schema_field(
                namer,
                names_seen,
                &ColumnType {
                    nullable: true,
                    scalar_type: (**value_type).clone(),
                },
            );
            json!({
                "type": "map",
                "values": inner
            })
        }
        ScalarType::Record {
            fields,
            custom_name,
            ..
        } => {
            let (name, name_seen) = match custom_name {
                Some(name) => (name.clone(), !names_seen.insert(name.clone())),
                None => (namer(), false),
            };
            if name_seen {
                json!(name)
            } else {
                let fields = fields.to_vec();
                let json_fields = build_row_schema_fields(&fields, names_seen, namer);
                json!({
                    "type": "record",
                    "name": name,
                    "fields": json_fields
                })
            }
        }
        ScalarType::Numeric { max_scale } => {
            let (p, s) = match max_scale {
                Some(max_scale) => (NUMERIC_DATUM_MAX_PRECISION, max_scale.into_u8()),
                None => (NUMERIC_AGG_MAX_PRECISION, NUMERIC_DATUM_MAX_PRECISION),
            };
            json!({
                "type": "bytes",
                "logicalType": "decimal",
                "precision": p,
                "scale": s,
            })
        }
    };
    if typ.nullable {
        field_type = json!(["null", field_type]);
    }
    field_type
}

pub(super) fn build_row_schema_fields<F: FnMut() -> String>(
    columns: &[(ColumnName, ColumnType)],
    names_seen: &mut HashSet<String>,
    namer: &mut F,
) -> Vec<serde_json::value::Value> {
    let mut fields = Vec::new();
    for (name, typ) in columns.iter() {
        let field_type = build_row_schema_field(namer, names_seen, typ);
        fields.push(json!({
            "name": name,
            "type": field_type,
        }));
    }
    fields
}

/// Builds the JSON for the row schema, which can be independently useful.
pub fn build_row_schema_json(
    columns: &[(ColumnName, ColumnType)],
    name: &str,
) -> serde_json::value::Value {
    let mut name_idx = 0;
    let fields = build_row_schema_fields(columns, &mut Default::default(), &mut move || {
        let ret = format!("com.materialize.sink.record{}", name_idx);
        name_idx += 1;
        ret
    });
    json!({
        "type": "record",
        "fields": fields,
        "name": name
    })
}
