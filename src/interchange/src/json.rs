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

use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::char;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{NUMERIC_AGG_MAX_PRECISION, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::{CatalogItemId, ColumnName, Datum, RelationDesc, SqlColumnType, SqlScalarType};
use serde_json::{Map, json};

use crate::avro::DocTarget;
use crate::encode::{Encode, TypedDatum, column_names_and_types};
use crate::envelopes;

const AVRO_NAMESPACE: &str = "com.materialize.sink";
const MICROS_PER_MILLIS: u32 = 1_000;

// Manages encoding of JSON-encoded bytes
pub struct JsonEncoder {
    columns: Vec<(ColumnName, SqlColumnType)>,
}

impl JsonEncoder {
    pub fn new(desc: RelationDesc, debezium: bool) -> Self {
        let mut columns = column_names_and_types(desc);
        if debezium {
            columns = envelopes::dbz_envelope(columns);
        };
        JsonEncoder { columns }
    }
}

impl Encode for JsonEncoder {
    fn encode_unchecked(&self, row: mz_repr::Row) -> Vec<u8> {
        let value = encode_datums_as_json(row.iter(), self.columns.as_ref());
        value.to_string().into_bytes()
    }
}

impl fmt::Debug for JsonEncoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("JsonEncoder")
            .field(
                "schema",
                &format!(
                    "{:?}",
                    build_row_schema_json(
                        &self.columns,
                        "schema",
                        &BTreeMap::new(),
                        None,
                        &Default::default(),
                    )
                ),
            )
            .finish()
    }
}

/// Encodes a sequence of `Datum` as JSON, using supplied column names and types.
pub fn encode_datums_as_json<'a, I>(
    datums: I,
    names_types: &[(ColumnName, SqlColumnType)],
) -> serde_json::Value
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let value_fields = datums
        .into_iter()
        .zip(names_types)
        .map(|(datum, (name, typ))| {
            (
                name.to_string(),
                TypedDatum::new(datum, typ).json(&JsonNumberPolicy::KeepAsNumber),
            )
        })
        .collect();
    serde_json::Value::Object(value_fields)
}

/// Policies for how to handle Numbers in JSON.
#[derive(Debug)]
pub enum JsonNumberPolicy {
    /// Do not change Numbers.
    KeepAsNumber,
    /// Convert Numbers to their String representation. Useful for JavaScript consumers that may
    /// interpret some numbers incorrectly.
    ConvertNumberToString,
}

pub trait ToJson {
    /// Transforms this value to a JSON value.
    fn json(self, number_policy: &JsonNumberPolicy) -> serde_json::Value;
}

impl ToJson for TypedDatum<'_> {
    fn json(self, number_policy: &JsonNumberPolicy) -> serde_json::Value {
        let TypedDatum { datum, typ } = self;
        if typ.nullable && datum.is_null() {
            return serde_json::Value::Null;
        }
        let value = match &typ.scalar_type {
            SqlScalarType::AclItem => json!(datum.unwrap_acl_item().to_string()),
            SqlScalarType::Bool => json!(datum.unwrap_bool()),
            SqlScalarType::PgLegacyChar => json!(datum.unwrap_uint8()),
            SqlScalarType::Int16 => json!(datum.unwrap_int16()),
            SqlScalarType::Int32 => json!(datum.unwrap_int32()),
            SqlScalarType::Int64 => json!(datum.unwrap_int64()),
            SqlScalarType::UInt16 => json!(datum.unwrap_uint16()),
            SqlScalarType::UInt32
            | SqlScalarType::Oid
            | SqlScalarType::RegClass
            | SqlScalarType::RegProc
            | SqlScalarType::RegType => {
                json!(datum.unwrap_uint32())
            }
            SqlScalarType::UInt64 => json!(datum.unwrap_uint64()),
            SqlScalarType::Float32 => json!(datum.unwrap_float32()),
            SqlScalarType::Float64 => json!(datum.unwrap_float64()),
            SqlScalarType::Numeric { .. } => {
                json!(datum.unwrap_numeric().0.to_standard_notation_string())
            }
            // https://stackoverflow.com/questions/10286204/what-is-the-right-json-date-format
            SqlScalarType::Date => serde_json::Value::String(format!("{}", datum.unwrap_date())),
            SqlScalarType::Time => serde_json::Value::String(format!("{:?}", datum.unwrap_time())),
            SqlScalarType::Timestamp { .. } => {
                let dt = datum.unwrap_timestamp().to_naive().and_utc();
                let millis = dt.timestamp_millis();
                let micros = dt.timestamp_subsec_micros()
                    - (dt.timestamp_subsec_millis() * MICROS_PER_MILLIS);
                serde_json::Value::String(format!("{millis}.{micros:0>3}"))
            }
            SqlScalarType::TimestampTz { .. } => {
                let dt = datum.unwrap_timestamptz().to_utc();
                let millis = dt.timestamp_millis();
                let micros = dt.timestamp_subsec_micros()
                    - (dt.timestamp_subsec_millis() * MICROS_PER_MILLIS);
                serde_json::Value::String(format!("{millis}.{micros:0>3}"))
            }
            SqlScalarType::Interval => {
                serde_json::Value::String(format!("{}", datum.unwrap_interval()))
            }
            SqlScalarType::Bytes => json!(datum.unwrap_bytes()),
            SqlScalarType::String | SqlScalarType::VarChar { .. } | SqlScalarType::PgLegacyName => {
                json!(datum.unwrap_str())
            }
            SqlScalarType::Char { length } => {
                let s = char::format_str_pad(datum.unwrap_str(), *length);
                serde_json::Value::String(s)
            }
            SqlScalarType::Jsonb => JsonbRef::from_datum(datum).to_serde_json(),
            SqlScalarType::Uuid => json!(datum.unwrap_uuid()),
            ty @ (SqlScalarType::Array(..) | SqlScalarType::Int2Vector) => {
                let array = datum.unwrap_array();
                let dims = array.dims().into_iter().collect::<Vec<_>>();
                let mut datums = array.elements().iter();
                encode_array(&mut datums, &dims, &mut |datum| {
                    TypedDatum::new(
                        datum,
                        &SqlColumnType {
                            nullable: true,
                            scalar_type: ty.unwrap_collection_element_type().clone(),
                        },
                    )
                    .json(number_policy)
                })
            }
            SqlScalarType::List { element_type, .. } => {
                let values = datum
                    .unwrap_list()
                    .into_iter()
                    .map(|datum| {
                        TypedDatum::new(
                            datum,
                            &SqlColumnType {
                                nullable: true,
                                scalar_type: (**element_type).clone(),
                            },
                        )
                        .json(number_policy)
                    })
                    .collect();
                serde_json::Value::Array(values)
            }
            SqlScalarType::Record { fields, .. } => {
                let list = datum.unwrap_list();
                let fields: Map<String, serde_json::Value> = fields
                    .iter()
                    .zip(&list)
                    .map(|((name, typ), datum)| {
                        let name = name.to_string();
                        let datum = TypedDatum::new(datum, typ);
                        let value = datum.json(number_policy);
                        (name, value)
                    })
                    .collect();
                fields.into()
            }
            SqlScalarType::Map { value_type, .. } => {
                let map = datum.unwrap_map();
                let elements = map
                    .into_iter()
                    .map(|(key, datum)| {
                        let value = TypedDatum::new(
                            datum,
                            &SqlColumnType {
                                nullable: true,
                                scalar_type: (**value_type).clone(),
                            },
                        )
                        .json(number_policy);
                        (key.to_string(), value)
                    })
                    .collect();
                serde_json::Value::Object(elements)
            }
            SqlScalarType::MzTimestamp => json!(datum.unwrap_mz_timestamp().to_string()),
            SqlScalarType::Range { .. } => {
                // Ranges' interiors are not expected to be types whose
                // string representations are misleading/wrong, e.g.
                // records.
                json!(datum.unwrap_range().to_string())
            }
            SqlScalarType::MzAclItem => json!(datum.unwrap_mz_acl_item().to_string()),
        };
        // We don't need to recurse into map or object here because those already recursively call
        // .json() with the number policy to generate the member Values.
        match (number_policy, value) {
            (JsonNumberPolicy::KeepAsNumber, value) => value,
            (JsonNumberPolicy::ConvertNumberToString, serde_json::Value::Number(n)) => {
                serde_json::Value::String(n.to_string())
            }
            (JsonNumberPolicy::ConvertNumberToString, value) => value,
        }
    }
}

fn encode_array<'a>(
    elems: &mut impl Iterator<Item = Datum<'a>>,
    dims: &[ArrayDimension],
    elem_encoder: &mut impl FnMut(Datum<'_>) -> serde_json::Value,
) -> serde_json::Value {
    serde_json::Value::Array(match dims {
        [] => vec![],
        [dim] => elems.take(dim.length).map(elem_encoder).collect(),
        [dim, rest @ ..] => (0..dim.length)
            .map(|_| encode_array(elems, rest, elem_encoder))
            .collect(),
    })
}

fn build_row_schema_field_type(
    type_namer: &mut Namer,
    custom_names: &BTreeMap<CatalogItemId, String>,
    typ: &SqlColumnType,
    item_id: Option<CatalogItemId>,
    options: &SchemaOptions,
) -> serde_json::Value {
    let mut field_type = match &typ.scalar_type {
        SqlScalarType::AclItem => json!("string"),
        SqlScalarType::Bool => json!("boolean"),
        SqlScalarType::PgLegacyChar => json!({
            "type": "fixed",
            "size": 1,
        }),
        SqlScalarType::Int16 | SqlScalarType::Int32 => {
            json!("int")
        }
        SqlScalarType::Int64 => json!("long"),
        SqlScalarType::UInt16 => type_namer.unsigned_type(2),
        SqlScalarType::UInt32
        | SqlScalarType::Oid
        | SqlScalarType::RegClass
        | SqlScalarType::RegProc
        | SqlScalarType::RegType => type_namer.unsigned_type(4),
        SqlScalarType::UInt64 => type_namer.unsigned_type(8),
        SqlScalarType::Float32 => json!("float"),
        SqlScalarType::Float64 => json!("double"),
        SqlScalarType::Date => json!({
            "type": "int",
            "logicalType": "date",
        }),
        SqlScalarType::Time => json!({
            "type": "long",
            "logicalType": "time-micros",
        }),
        SqlScalarType::Timestamp { precision } | SqlScalarType::TimestampTz { precision } => {
            json!({
                "type": "long",
                "logicalType": match precision {
                    Some(precision) if precision.into_u8() <= 3 => "timestamp-millis",
                    _ => "timestamp-micros",
                },
            })
        }
        SqlScalarType::Interval => type_namer.interval_type(),
        SqlScalarType::Bytes => json!("bytes"),
        SqlScalarType::String
        | SqlScalarType::Char { .. }
        | SqlScalarType::VarChar { .. }
        | SqlScalarType::PgLegacyName => {
            json!("string")
        }
        SqlScalarType::Jsonb => json!({
            "type": "string",
            "connect.name": "io.debezium.data.Json",
        }),
        SqlScalarType::Uuid => json!({
            "type": "string",
            "logicalType": "uuid",
        }),
        ty
        @ (SqlScalarType::Array(..) | SqlScalarType::Int2Vector | SqlScalarType::List { .. }) => {
            let inner = build_row_schema_field_type(
                type_namer,
                custom_names,
                &SqlColumnType {
                    nullable: true,
                    scalar_type: ty.unwrap_collection_element_type().clone(),
                },
                item_id,
                options,
            );
            json!({
                "type": "array",
                "items": inner
            })
        }
        SqlScalarType::Map { value_type, .. } => {
            let inner = build_row_schema_field_type(
                type_namer,
                custom_names,
                &SqlColumnType {
                    nullable: true,
                    scalar_type: (**value_type).clone(),
                },
                item_id,
                options,
            );
            json!({
                "type": "map",
                "values": inner
            })
        }
        SqlScalarType::Record {
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
                let json_fields =
                    build_row_schema_fields(&fields, type_namer, custom_names, *custom_id, options);
                if let Some(comment) =
                    custom_id.and_then(|id| options.doc_comments.get(&DocTarget::Type(id)))
                {
                    json!({
                        "type": "record",
                        "name": name,
                        "doc": comment,
                        "fields": json_fields
                    })
                } else {
                    json!({
                        "type": "record",
                        "name": name,
                        "fields": json_fields
                    })
                }
            }
        }
        SqlScalarType::Numeric { max_scale } => {
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
        SqlScalarType::MzTimestamp => json!("string"),
        // https://debezium.io/documentation/reference/stable/connectors/postgresql.html
        SqlScalarType::Range { .. } => json!("string"),
        SqlScalarType::MzAclItem => json!("string"),
    };
    if typ.nullable {
        // Should be revisited if we ever support a different kind of union scheme.
        // Currently adding the "null" at the beginning means we can set the default
        // value to "null" if such a preference is set.
        field_type = json!(["null", field_type]);
    }
    field_type
}

fn build_row_schema_fields(
    columns: &[(ColumnName, SqlColumnType)],
    type_namer: &mut Namer,
    custom_names: &BTreeMap<CatalogItemId, String>,
    item_id: Option<CatalogItemId>,
    options: &SchemaOptions,
) -> Vec<serde_json::Value> {
    let mut fields = Vec::new();
    let mut field_namer = Namer::default();
    for (name, typ) in columns.iter() {
        let (name, _seen) = field_namer.valid_name(name);
        let field_type =
            build_row_schema_field_type(type_namer, custom_names, typ, item_id, options);

        let mut field = json!({
            "name": name,
            "type": field_type,
        });

        // It's a nullable union if the type is an array and the first option is "null"
        let is_nullable_union = field_type
            .as_array()
            .is_some_and(|array| array.first().is_some_and(|first| first == &json!("null")));

        if options.set_null_defaults && is_nullable_union {
            field
                .as_object_mut()
                .expect("`field` initialized to JSON object above")
                .insert("default".to_string(), json!(null));
        }

        if let Some(comment) = item_id.and_then(|item_id| {
            options.doc_comments.get(&DocTarget::Field {
                object_id: item_id,
                column_name: name.into(),
            })
        }) {
            field
                .as_object_mut()
                .expect("`field` initialized to JSON object above")
                .insert("doc".to_string(), json!(comment));
        }

        fields.push(field);
    }
    fields
}

#[derive(Default, Clone, Debug)]
/// Struct to pass around options to create the json schema
pub struct SchemaOptions {
    /// Boolean flag to enable null defaults.
    pub set_null_defaults: bool,
    /// Map containing comments for an item or field, used to populate
    /// documentation in the generated avro schema
    pub doc_comments: BTreeMap<DocTarget, String>,
}

/// Builds the JSON for the row schema, which can be independently useful.
pub fn build_row_schema_json(
    columns: &[(ColumnName, SqlColumnType)],
    name: &str,
    custom_names: &BTreeMap<CatalogItemId, String>,
    item_id: Option<CatalogItemId>,
    options: &SchemaOptions,
) -> Result<serde_json::Value, anyhow::Error> {
    let fields = build_row_schema_fields(
        columns,
        &mut Namer::default(),
        custom_names,
        item_id,
        options,
    );

    let _ = mz_avro::schema::Name::parse_simple(name)?;
    if let Some(comment) =
        item_id.and_then(|item_id| options.doc_comments.get(&DocTarget::Type(item_id)))
    {
        Ok(json!({
            "type": "record",
            "doc": comment,
            "fields": fields,
            "name": name
        }))
    } else {
        Ok(json!({
            "type": "record",
            "fields": fields,
            "name": name
        }))
    }
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
