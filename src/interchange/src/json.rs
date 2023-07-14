// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use mz_repr::adt::char;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{NUMERIC_AGG_MAX_PRECISION, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::{ColumnName, ColumnType, Datum, GlobalId, RelationDesc, ScalarType};
use serde_json::{json, Map};

use crate::encode::{column_names_and_types, Encode, TypedDatum};
use crate::envelopes;

const AVRO_NAMESPACE: &str = "com.materialize.sink";

// Manages encoding of JSON-encoded bytes
pub struct JsonEncoder {
    key_columns: Option<Vec<(ColumnName, ColumnType)>>,
    value_columns: Vec<(ColumnName, ColumnType)>,
}

impl JsonEncoder {
    pub fn new(key_desc: Option<RelationDesc>, value_desc: RelationDesc, debezium: bool) -> Self {
        let mut value_columns = column_names_and_types(value_desc);
        if debezium {
            value_columns = envelopes::dbz_envelope(value_columns);
        }
        JsonEncoder {
            key_columns: if let Some(desc) = key_desc {
                Some(column_names_and_types(desc))
            } else {
                None
            },
            value_columns,
        }
    }

    pub fn encode_row(
        &self,
        row: mz_repr::Row,
        names_types: &[(ColumnName, ColumnType)],
    ) -> Vec<u8> {
        let value = encode_datums_as_json(row.iter(), names_types);
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
                &format!(
                    "{:?}",
                    build_row_schema_json(&self.value_columns, "schema", &BTreeMap::new())
                ),
            )
            .finish()
    }
}

/// Encodes a sequence of `Datum` as JSON, using supplied column names and types.
pub fn encode_datums_as_json<'a, I>(
    datums: I,
    names_types: &[(ColumnName, ColumnType)],
) -> serde_json::Value
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let value_fields = datums
        .into_iter()
        .zip(names_types)
        .map(|(datum, (name, typ))| (name.to_string(), TypedDatum::new(datum, typ).json()))
        .collect();
    serde_json::Value::Object(value_fields)
}

pub trait ToJson {
    /// Transforms this value to a JSON value.
    fn json(self) -> serde_json::Value;
}

impl ToJson for TypedDatum<'_> {
    fn json(self) -> serde_json::Value {
        let TypedDatum { datum, typ } = self;
        if typ.nullable && datum.is_null() {
            serde_json::Value::Null
        } else {
            match &typ.scalar_type {
                ScalarType::Bool => json!(datum.unwrap_bool()),
                ScalarType::PgLegacyChar => json!(datum.unwrap_uint8()),
                ScalarType::Int16 => json!(datum.unwrap_int16()),
                ScalarType::Int32 => json!(datum.unwrap_int32()),
                ScalarType::Int64 => json!(datum.unwrap_int64()),
                ScalarType::UInt16 => json!(datum.unwrap_uint16()),
                ScalarType::UInt32
                | ScalarType::Oid
                | ScalarType::RegClass
                | ScalarType::RegProc
                | ScalarType::RegType => {
                    json!(datum.unwrap_uint32())
                }
                ScalarType::UInt64 => json!(datum.unwrap_uint64()),
                ScalarType::Float32 => json!(datum.unwrap_float32()),
                ScalarType::Float64 => json!(datum.unwrap_float64()),
                ScalarType::Numeric { .. } => {
                    json!(datum.unwrap_numeric().0.to_standard_notation_string())
                }
                // https://stackoverflow.com/questions/10286204/what-is-the-right-json-date-format
                ScalarType::Date => serde_json::Value::String(format!("{}", datum.unwrap_date())),
                ScalarType::Time => serde_json::Value::String(format!("{:?}", datum.unwrap_time())),
                ScalarType::Timestamp => serde_json::Value::String(format!(
                    "{:?}",
                    datum.unwrap_timestamp().to_naive().timestamp_millis()
                )),
                ScalarType::TimestampTz => serde_json::Value::String(format!(
                    "{:?}",
                    datum.unwrap_timestamptz().to_naive().timestamp_millis()
                )),
                ScalarType::Interval => {
                    serde_json::Value::String(format!("{}", datum.unwrap_interval()))
                }
                ScalarType::Bytes => json!(datum.unwrap_bytes()),
                ScalarType::String | ScalarType::VarChar { .. } | ScalarType::PgLegacyName => {
                    json!(datum.unwrap_str())
                }
                ScalarType::Char { length } => {
                    let s = char::format_str_pad(datum.unwrap_str(), *length);
                    serde_json::Value::String(s)
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
                            TypedDatum::new(
                                datum,
                                &ColumnType {
                                    nullable: true,
                                    scalar_type: ty.unwrap_collection_element_type().clone(),
                                },
                            )
                            .json()
                        })
                        .collect();
                    serde_json::Value::Array(values)
                }
                ScalarType::Record { fields, .. } => {
                    let list = datum.unwrap_list();
                    let fields: Map<String, serde_json::Value> = fields
                        .iter()
                        .zip(list.into_iter())
                        .map(|((name, typ), datum)| {
                            let name = name.to_string();
                            let datum = TypedDatum::new(datum, typ);
                            let value = datum.json();
                            (name, value)
                        })
                        .collect();
                    fields.into()
                }
                ScalarType::Map { value_type, .. } => {
                    let map = datum.unwrap_map();
                    let elements = map
                        .into_iter()
                        .map(|(key, datum)| {
                            let value = TypedDatum::new(
                                datum,
                                &ColumnType {
                                    nullable: true,
                                    scalar_type: (**value_type).clone(),
                                },
                            )
                            .json();
                            (key.to_string(), value)
                        })
                        .collect();
                    serde_json::Value::Object(elements)
                }
                ScalarType::MzTimestamp => json!(datum.unwrap_mz_timestamp().to_string()),
                ScalarType::Range { .. } => {
                    // Ranges' interiors are not expected to be types whose
                    // string representations are misleading/wrong, e.g.
                    // records.
                    json!(datum.unwrap_range().to_string())
                }
                ScalarType::MzAclItem => json!(datum.unwrap_mz_acl_item().to_string()),
            }
        }
    }
}

fn build_row_schema_field(
    type_namer: &mut Namer,
    custom_names: &BTreeMap<GlobalId, String>,
    typ: &ColumnType,
) -> serde_json::Value {
    let mut field_type = match &typ.scalar_type {
        ScalarType::Bool => json!("boolean"),
        ScalarType::PgLegacyChar => json!({
            "type": "fixed",
            "size": 1,
        }),
        ScalarType::Int16 | ScalarType::Int32 => {
            json!("int")
        }
        ScalarType::Int64 => json!("long"),
        ScalarType::UInt16 => type_namer.unsigned_type(2),
        ScalarType::UInt32
        | ScalarType::Oid
        | ScalarType::RegClass
        | ScalarType::RegProc
        | ScalarType::RegType => type_namer.unsigned_type(4),
        ScalarType::UInt64 => type_namer.unsigned_type(8),
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
        ScalarType::Interval => type_namer.interval_type(),
        ScalarType::Bytes => json!("bytes"),
        ScalarType::String
        | ScalarType::Char { .. }
        | ScalarType::VarChar { .. }
        | ScalarType::PgLegacyName => {
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
                type_namer,
                custom_names,
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
                type_namer,
                custom_names,
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
            fields, custom_id, ..
        } => {
            let (name, name_seen) = match custom_id.as_ref().and_then(|id| custom_names.get(id)) {
                Some(name) => type_namer.valid_name(name),
                None => (type_namer.anonymous_record_name(), false),
            };
            if name_seen {
                json!(name)
            } else {
                let fields = fields.to_vec();
                let json_fields = build_row_schema_fields(&fields, type_namer, custom_names);
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
        ScalarType::MzTimestamp => json!("string"),
        // https://debezium.io/documentation/reference/stable/connectors/postgresql.html
        ScalarType::Range { .. } => json!("string"),
        ScalarType::MzAclItem => json!("string"),
    };
    if typ.nullable {
        field_type = json!(["null", field_type]);
    }
    field_type
}

fn build_row_schema_fields(
    columns: &[(ColumnName, ColumnType)],
    type_namer: &mut Namer,
    custom_names: &BTreeMap<GlobalId, String>,
) -> Vec<serde_json::Value> {
    let mut fields = Vec::new();
    let mut field_namer = Namer::default();
    for (name, typ) in columns.iter() {
        let (name, _seen) = field_namer.valid_name(name.as_str());
        let field_type = build_row_schema_field(type_namer, custom_names, typ);
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
    custom_names: &BTreeMap<GlobalId, String>,
) -> Result<serde_json::Value, anyhow::Error> {
    let fields = build_row_schema_fields(columns, &mut Namer::default(), custom_names);
    let _ = mz_avro::schema::Name::parse_simple(name)?;
    Ok(json!({
        "type": "record",
        "fields": fields,
        "name": name
    }))
}

/// Naming helper for use when constructing an Avro schema.
#[derive(Default)]
struct Namer {
    record_index: usize,
    seen_interval: bool,
    seen_unsigneds: BTreeSet<usize>,
    seen_names: BTreeMap<String, String>,
    valid_names_count: BTreeMap<String, usize>,
}

impl Namer {
    /// Returns the schema for an interval type.
    fn interval_type(&mut self) -> serde_json::Value {
        let name = format!("{AVRO_NAMESPACE}.interval");
        if self.seen_interval {
            json!(name)
        } else {
            self.seen_interval = true;
            json!({
            "type": "fixed",
            "size": 16,
            "name": name,
            })
        }
    }

    /// Returns the schema for an unsigned integer with the given width.
    fn unsigned_type(&mut self, width: usize) -> serde_json::Value {
        let name = format!("{AVRO_NAMESPACE}.uint{width}");
        if self.seen_unsigneds.contains(&width) {
            json!(name)
        } else {
            self.seen_unsigneds.insert(width);
            json!({
                "type": "fixed",
                "size": width,
                "name": name,
            })
        }
    }

    /// Returns a name to use for a new anonymous record.
    fn anonymous_record_name(&mut self) -> String {
        let out = format!("{AVRO_NAMESPACE}.record{}", self.record_index);
        self.record_index += 1;
        out
    }

    /// Turns `name` into a valid, unique name for use in the Avro schema.
    ///
    /// Returns the valid name and whether `name` has been seen before.
    fn valid_name(&mut self, name: &str) -> (String, bool) {
        if let Some(valid_name) = self.seen_names.get(name) {
            (valid_name.into(), true)
        } else {
            let mut valid_name = mz_avro::schema::Name::make_valid(name);
            let valid_name_count = self
                .valid_names_count
                .entry(valid_name.clone())
                .or_default();
            if *valid_name_count != 0 {
                valid_name += &valid_name_count.to_string();
            }
            *valid_name_count += 1;
            self.seen_names.insert(name.into(), valid_name.clone());
            (valid_name, false)
        }
    }
}
