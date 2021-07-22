// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use repr::adt::numeric::{NUMERIC_AGG_MAX_PRECISION, NUMERIC_DATUM_MAX_PRECISION};
use repr::{ColumnName, ColumnType, ScalarType};
use serde_json::json;

fn build_row_schema_field<F: FnMut() -> String>(
    namer: &mut F,
    names_seen: &mut HashSet<String>,
    typ: &ColumnType,
) -> serde_json::value::Value {
    let mut field_type = match &typ.scalar_type {
        ScalarType::Bool => json!("boolean"),
        ScalarType::Int16 | ScalarType::Int32 | ScalarType::Oid => json!("int"),
        ScalarType::Int64 => json!("long"),
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
        ScalarType::String => json!("string"),
        ScalarType::Jsonb => json!({
            "type": "string",
            "connect.name": "io.debezium.data.Json",
        }),
        ScalarType::Uuid => json!({
            "type": "string",
            "logicalType": "uuid",
        }),
        ScalarType::Array(element_type) | ScalarType::List { element_type, .. } => {
            let inner = build_row_schema_field(
                namer,
                names_seen,
                &ColumnType {
                    nullable: true,
                    scalar_type: (**element_type).clone(),
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
        ScalarType::Numeric { scale } => {
            let (p, s) = match scale {
                Some(scale) => (NUMERIC_DATUM_MAX_PRECISION, usize::from(*scale)),
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
