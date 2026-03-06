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

use itertools::Itertools;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::char;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{
    self, NUMERIC_AGG_MAX_PRECISION, NUMERIC_DATUM_MAX_PRECISION, Numeric,
};
use mz_repr::{CatalogItemId, ColumnName, Datum, RelationDesc, Row, SqlColumnType, SqlScalarType};
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
        .zip_eq(names_types)
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
                    .zip_eq(list)
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

// ---------------------------------------------------------------------------
// Debezium JSON Decoder
// ---------------------------------------------------------------------------

/// Errors from decoding Debezium JSON messages.
#[derive(Debug)]
pub enum DebeziumJsonError {
    /// Invalid JSON bytes.
    InvalidJson(serde_json::Error),
    /// Missing required field in the Debezium envelope.
    MissingField(&'static str),
    /// Unsupported Debezium operation type.
    UnsupportedOp(String),
    /// Type mismatch when decoding a column value.
    TypeMismatch {
        column: String,
        expected: String,
        got: String,
    },
    /// Both `before` and `after` are null.
    BothNull,
}

impl std::fmt::Display for DebeziumJsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidJson(e) => write!(f, "Failed to decode Debezium JSON: {e}"),
            Self::MissingField(field) => {
                write!(f, "Debezium JSON message missing required '{field}' field")
            }
            Self::UnsupportedOp(op) => {
                write!(f, "Unsupported Debezium operation type: \"{op}\"")
            }
            Self::TypeMismatch {
                column,
                expected,
                got,
            } => write!(
                f,
                "Failed to decode Debezium JSON: expected {expected} for column \"{column}\", got {got}"
            ),
            Self::BothNull => write!(
                f,
                "Debezium JSON message has both 'before' and 'after' set to null"
            ),
        }
    }
}

impl std::error::Error for DebeziumJsonError {}

/// Decodes a Debezium JSON message into a Row with `[before, after]` structure.
///
/// The output Row must match the layout that the Avro Debezium path produces:
/// `[before: Datum::List|Null, after: Datum::List|Null]`. The `after_idx` in
/// `UpsertStyle::Debezium { after_idx }` is hardcoded to 1 for JSON (the index
/// of the `after` column in this two-column Row).
///
/// The `desc` parameter is the user-declared table schema (not the synthetic
/// before/after desc). Each `before`/`after` JSON object is decoded against
/// this schema to produce a typed Record (List of datums).
///
/// Auto-detects wrapped vs unwrapped Debezium format by checking for a
/// top-level `"payload"` key. Extra envelope fields (`source`, `ts_ms`,
/// `transaction`) are silently ignored.
///
/// Returns `Ok(None)` for tombstone messages (empty bytes).
/// Returns `Ok(Some(row))` where row contains `[before_record_or_null, after_record_or_null]`.
pub fn decode_debezium_json(
    bytes: &[u8],
    desc: &RelationDesc,
) -> Result<Option<Row>, DebeziumJsonError> {
    if bytes.is_empty() {
        // Tombstone message: null value
        return Ok(None);
    }

    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(DebeziumJsonError::InvalidJson)?;

    // Auto-detect wrapped vs unwrapped form.
    let envelope = if value.get("payload").is_some() {
        // Wrapped form: {"schema": ..., "payload": {...}}
        value.get("payload").expect("checked above")
    } else {
        // Unwrapped form: envelope fields at top level
        &value
    };

    // Extract and validate the "op" field.
    let op = envelope
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or(DebeziumJsonError::MissingField("op"))?;

    match op {
        "c" | "u" | "d" | "r" => {}
        other => return Err(DebeziumJsonError::UnsupportedOp(other.to_string())),
    }

    // Extract "before" and "after" fields.
    let before_val = envelope.get("before");
    let after_val = envelope.get("after");

    // Check for both-null case.
    let before_is_null = before_val.is_none() || before_val == Some(&serde_json::Value::Null);
    let after_is_null = after_val.is_none() || after_val == Some(&serde_json::Value::Null);
    if before_is_null && after_is_null {
        return Err(DebeziumJsonError::BothNull);
    }

    let columns: Vec<(ColumnName, SqlColumnType)> = desc
        .iter()
        .map(|(name, typ)| (name.clone(), typ.clone()))
        .collect();

    let mut row = Row::default();
    let mut packer = row.packer();

    // Pack "before" as a Record (List) or Null.
    if before_is_null {
        packer.push(Datum::Null);
    } else {
        let before_obj = before_val
            .unwrap()
            .as_object()
            .ok_or(DebeziumJsonError::MissingField("before"))?;
        packer.push_list_with(|inner| pack_json_object_as_record(inner, before_obj, &columns))?;
    }

    // Pack "after" as a Record (List) or Null.
    if after_is_null {
        packer.push(Datum::Null);
    } else {
        let after_obj = after_val
            .unwrap()
            .as_object()
            .ok_or(DebeziumJsonError::MissingField("after"))?;
        packer.push_list_with(|inner| pack_json_object_as_record(inner, after_obj, &columns))?;
    }

    Ok(Some(row))
}

/// Decodes a flat JSON object into a typed Row matching the given schema.
///
/// Used for key decoding in Debezium JSON sources where key columns must be
/// decoded into typed columns (not raw jsonb) for upsert hash consistency.
pub fn decode_json_typed_row(
    bytes: &[u8],
    desc: &RelationDesc,
) -> Result<Option<Row>, DebeziumJsonError> {
    if bytes.is_empty() {
        return Ok(None);
    }

    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(DebeziumJsonError::InvalidJson)?;

    let obj = value
        .as_object()
        .ok_or(DebeziumJsonError::MissingField("key object"))?;

    let columns: Vec<(ColumnName, SqlColumnType)> = desc
        .iter()
        .map(|(name, typ)| (name.clone(), typ.clone()))
        .collect();

    let mut row = Row::default();
    let mut packer = row.packer();
    pack_json_object_as_record(&mut packer, obj, &columns)?;
    Ok(Some(row))
}

/// Packs a JSON object's fields into a Row packer according to the declared column schema.
fn pack_json_object_as_record(
    packer: &mut mz_repr::RowPacker,
    obj: &Map<String, serde_json::Value>,
    columns: &[(ColumnName, SqlColumnType)],
) -> Result<(), DebeziumJsonError> {
    for (col_name, col_type) in columns {
        let json_val = obj.get(col_name.as_str());
        match json_val {
            None | Some(serde_json::Value::Null) => {
                if col_type.nullable {
                    packer.push(Datum::Null);
                } else if json_val.is_none() {
                    // Missing column: for now, treat as null if nullable (handled above),
                    // otherwise error. A future iteration could use configurable behavior.
                    return Err(DebeziumJsonError::TypeMismatch {
                        column: col_name.to_string(),
                        expected: format!("{:?}", col_type.scalar_type),
                        got: "missing (column not present in JSON)".to_string(),
                    });
                } else {
                    return Err(DebeziumJsonError::TypeMismatch {
                        column: col_name.to_string(),
                        expected: format!("{:?}", col_type.scalar_type),
                        got: "null".to_string(),
                    });
                }
            }
            Some(val) => {
                pack_json_value(packer, val, col_name, col_type)?;
            }
        }
    }
    Ok(())
}

/// Converts a single JSON value to the appropriate Datum and pushes it.
fn pack_json_value(
    packer: &mut mz_repr::RowPacker,
    val: &serde_json::Value,
    col_name: &ColumnName,
    col_type: &SqlColumnType,
) -> Result<(), DebeziumJsonError> {
    let type_err = |expected: &str, val: &serde_json::Value| DebeziumJsonError::TypeMismatch {
        column: col_name.to_string(),
        expected: expected.to_string(),
        got: format!("{}", val),
    };

    match &col_type.scalar_type {
        SqlScalarType::Bool => {
            let b = val.as_bool().ok_or_else(|| type_err("boolean", val))?;
            packer.push(Datum::from(b));
        }
        SqlScalarType::Int16 => {
            let n = val.as_i64().ok_or_else(|| type_err("int16", val))?;
            let v = i16::try_from(n).map_err(|_| type_err("int16", val))?;
            packer.push(Datum::Int16(v));
        }
        SqlScalarType::Int32 => {
            let n = val.as_i64().ok_or_else(|| type_err("int32", val))?;
            let v = i32::try_from(n).map_err(|_| type_err("int32", val))?;
            packer.push(Datum::Int32(v));
        }
        SqlScalarType::Int64 => {
            let n = val.as_i64().ok_or_else(|| type_err("int64", val))?;
            packer.push(Datum::Int64(n));
        }
        SqlScalarType::Float32 => {
            let n = val.as_f64().ok_or_else(|| type_err("float32", val))?;
            #[allow(clippy::as_conversions)]
            packer.push(Datum::Float32((n as f32).into()));
        }
        SqlScalarType::Float64 => {
            let n = val.as_f64().ok_or_else(|| type_err("float64", val))?;
            packer.push(Datum::Float64(n.into()));
        }
        SqlScalarType::Numeric { .. } => {
            // Accept both numbers and strings for numeric.
            let s = match val {
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::String(s) => s.clone(),
                _ => return Err(type_err("numeric", val)),
            };
            let mut cx = numeric::cx_datum();
            let mut n: Numeric = cx.parse(s.trim()).map_err(|_| type_err("numeric", val))?;
            numeric::munge_numeric(&mut n).map_err(|_| type_err("numeric", val))?;
            packer.push(Datum::from(n));
        }
        SqlScalarType::String | SqlScalarType::VarChar { .. } | SqlScalarType::PgLegacyName => {
            let s = val.as_str().ok_or_else(|| type_err("text", val))?;
            packer.push(Datum::String(s));
        }
        SqlScalarType::Char { length } => {
            let s = val.as_str().ok_or_else(|| type_err("char", val))?;
            let s = char::format_str_pad(s, *length);
            packer.push(Datum::String(&s));
        }
        SqlScalarType::Jsonb => {
            // Any JSON value is valid for jsonb — pack it directly.
            mz_repr::adt::jsonb::JsonbPacker::new(packer)
                .pack_serde_json(val.clone())
                .map_err(|_| type_err("jsonb", val))?;
        }
        SqlScalarType::Timestamp { .. } => {
            // Accept strings (ISO 8601) or numbers (milliseconds since epoch).
            let ndt = match val {
                serde_json::Value::String(s) => {
                    // Parse ISO 8601 string.
                    let ndt: chrono::NaiveDateTime = s
                        .parse()
                        .or_else(|_| {
                            chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.naive_utc())
                        })
                        .map_err(|_| type_err("timestamp", val))?;
                    ndt
                }
                serde_json::Value::Number(n) => {
                    let millis = n.as_i64().ok_or_else(|| type_err("timestamp", val))?;
                    chrono::DateTime::from_timestamp_millis(millis)
                        .ok_or_else(|| type_err("timestamp", val))?
                        .naive_utc()
                }
                _ => return Err(type_err("timestamp", val)),
            };
            let ts = mz_repr::adt::timestamp::CheckedTimestamp::from_timestamplike(ndt)
                .map_err(|_| type_err("timestamp", val))?;
            packer.push(Datum::Timestamp(ts));
        }
        SqlScalarType::TimestampTz { .. } => {
            let dt = match val {
                serde_json::Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.to_utc())
                    .or_else(|_| s.parse::<chrono::NaiveDateTime>().map(|ndt| ndt.and_utc()))
                    .map_err(|_| type_err("timestamptz", val))?,
                serde_json::Value::Number(n) => {
                    let millis = n.as_i64().ok_or_else(|| type_err("timestamptz", val))?;
                    chrono::DateTime::from_timestamp_millis(millis)
                        .ok_or_else(|| type_err("timestamptz", val))?
                }
                _ => return Err(type_err("timestamptz", val)),
            };
            let ts = mz_repr::adt::timestamp::CheckedTimestamp::from_timestamplike(dt)
                .map_err(|_| type_err("timestamptz", val))?;
            packer.push(Datum::TimestampTz(ts));
        }
        SqlScalarType::Date => {
            let s = val.as_str().ok_or_else(|| type_err("date", val))?;
            let d: mz_repr::adt::date::Date = s.parse().map_err(|_| type_err("date", val))?;
            packer.push(Datum::Date(d));
        }
        SqlScalarType::Bytes => {
            // Bytes are not commonly used in Debezium JSON.
            // Accept JSON strings, interpreting them as UTF-8 bytes.
            let s = val.as_str().ok_or_else(|| type_err("bytes", val))?;
            packer.push(Datum::Bytes(s.as_bytes()));
        }
        SqlScalarType::Uuid => {
            let s = val.as_str().ok_or_else(|| type_err("uuid", val))?;
            let u: uuid::Uuid = s.parse().map_err(|_| type_err("uuid", val))?;
            packer.push(Datum::from(u));
        }
        // For any other type, try to coerce from a string representation.
        other => {
            let s = val
                .as_str()
                .ok_or_else(|| type_err(&format!("{other:?}"), val))?;
            packer.push(Datum::String(s));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::{Datum, RelationDesc, SqlScalarType};

    fn test_desc() -> RelationDesc {
        RelationDesc::builder()
            .with_column("id", SqlScalarType::Int32.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(true))
            .finish()
    }

    #[mz_ore::test]
    fn test_tombstone() {
        let desc = test_desc();
        let result = decode_debezium_json(b"", &desc).unwrap();
        assert!(result.is_none());
    }

    #[mz_ore::test]
    fn test_insert_unwrapped() {
        let desc = test_desc();
        let msg = br#"{"before":null,"after":{"id":1,"name":"Alice"},"op":"c"}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let mut iter = row.iter();
        // before is null
        assert_eq!(iter.next(), Some(Datum::Null));
        // after is a list [1, "Alice"]
        let after = iter.next().unwrap();
        let after_list: Vec<_> = after.unwrap_list().iter().collect();
        assert_eq!(after_list[0], Datum::Int32(1));
        assert_eq!(after_list[1], Datum::String("Alice"));
    }

    #[mz_ore::test]
    fn test_delete_unwrapped() {
        let desc = test_desc();
        let msg = br#"{"before":{"id":1,"name":"Alice"},"after":null,"op":"d"}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let mut iter = row.iter();
        // before is a list
        let before = iter.next().unwrap();
        let before_list: Vec<_> = before.unwrap_list().iter().collect();
        assert_eq!(before_list[0], Datum::Int32(1));
        assert_eq!(before_list[1], Datum::String("Alice"));
        // after is null
        assert_eq!(iter.next(), Some(Datum::Null));
    }

    #[mz_ore::test]
    fn test_update_unwrapped() {
        let desc = test_desc();
        let msg = br#"{"before":{"id":1,"name":"Alice"},"after":{"id":1,"name":"Bob"},"op":"u"}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let mut iter = row.iter();
        let before_list: Vec<_> = iter.next().unwrap().unwrap_list().iter().collect();
        assert_eq!(before_list[1], Datum::String("Alice"));
        let after_list: Vec<_> = iter.next().unwrap().unwrap_list().iter().collect();
        assert_eq!(after_list[1], Datum::String("Bob"));
    }

    #[mz_ore::test]
    fn test_snapshot_op() {
        let desc = test_desc();
        let msg = br#"{"before":null,"after":{"id":1,"name":"snap"},"op":"r"}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let mut iter = row.iter();
        assert_eq!(iter.next(), Some(Datum::Null));
        let after_list: Vec<_> = iter.next().unwrap().unwrap_list().iter().collect();
        assert_eq!(after_list[1], Datum::String("snap"));
    }

    #[mz_ore::test]
    fn test_wrapped_form() {
        let desc = test_desc();
        let msg = br#"{"schema":{},"payload":{"before":null,"after":{"id":1,"name":"wrapped"},"op":"c"}}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let mut iter = row.iter();
        assert_eq!(iter.next(), Some(Datum::Null));
        let after_list: Vec<_> = iter.next().unwrap().unwrap_list().iter().collect();
        assert_eq!(after_list[1], Datum::String("wrapped"));
    }

    #[mz_ore::test]
    fn test_unsupported_op() {
        let desc = test_desc();
        let msg = br#"{"before":null,"after":{"id":1,"name":"x"},"op":"t"}"#;
        let err = decode_debezium_json(msg, &desc).unwrap_err();
        assert!(matches!(err, DebeziumJsonError::UnsupportedOp(_)));
    }

    #[mz_ore::test]
    fn test_missing_op() {
        let desc = test_desc();
        let msg = br#"{"before":null,"after":{"id":1,"name":"x"}}"#;
        let err = decode_debezium_json(msg, &desc).unwrap_err();
        assert!(matches!(err, DebeziumJsonError::MissingField("op")));
    }

    #[mz_ore::test]
    fn test_both_null() {
        let desc = test_desc();
        let msg = br#"{"before":null,"after":null,"op":"c"}"#;
        let err = decode_debezium_json(msg, &desc).unwrap_err();
        assert!(matches!(err, DebeziumJsonError::BothNull));
    }

    #[mz_ore::test]
    fn test_invalid_json() {
        let desc = test_desc();
        let err = decode_debezium_json(b"not json", &desc).unwrap_err();
        assert!(matches!(err, DebeziumJsonError::InvalidJson(_)));
    }

    #[mz_ore::test]
    fn test_nullable_column_null() {
        let desc = test_desc();
        let msg = br#"{"before":null,"after":{"id":1,"name":null},"op":"c"}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let after_list: Vec<_> = row.iter().nth(1).unwrap().unwrap_list().iter().collect();
        assert_eq!(after_list[0], Datum::Int32(1));
        assert_eq!(after_list[1], Datum::Null);
    }

    #[mz_ore::test]
    fn test_type_mismatch() {
        let desc = test_desc();
        // "id" expects int32 but gets a string
        let msg = br#"{"before":null,"after":{"id":"not_a_number","name":"x"},"op":"c"}"#;
        let err = decode_debezium_json(msg, &desc).unwrap_err();
        assert!(matches!(err, DebeziumJsonError::TypeMismatch { .. }));
    }

    #[mz_ore::test]
    fn test_typed_json_key_decode() {
        let desc = RelationDesc::builder()
            .with_column("id", SqlScalarType::Int32.nullable(false))
            .finish();
        let row = decode_json_typed_row(br#"{"id":42}"#, &desc)
            .unwrap()
            .unwrap();
        assert_eq!(row.iter().next(), Some(Datum::Int32(42)));
    }

    #[mz_ore::test]
    fn test_typed_json_key_empty() {
        let desc = RelationDesc::builder()
            .with_column("id", SqlScalarType::Int32.nullable(false))
            .finish();
        let result = decode_json_typed_row(b"", &desc).unwrap();
        assert!(result.is_none());
    }

    #[mz_ore::test]
    fn test_missing_before_field() {
        // When "before" key is entirely absent (not null), treat as null.
        // This is common for insert-only sources like TiCDC.
        let desc = test_desc();
        let msg = br#"{"after":{"id":1,"name":"ok"},"op":"c"}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let mut iter = row.iter();
        assert_eq!(iter.next(), Some(Datum::Null));
        let after_list: Vec<_> = iter.next().unwrap().unwrap_list().iter().collect();
        assert_eq!(after_list[0], Datum::Int32(1));
    }

    #[mz_ore::test]
    fn test_composite_key_decode() {
        // Composite primary keys produce a multi-column typed JSON key.
        let desc = RelationDesc::builder()
            .with_column("org_id", SqlScalarType::Int32.nullable(false))
            .with_column("user_id", SqlScalarType::Int64.nullable(false))
            .finish();
        let row = decode_json_typed_row(br#"{"org_id":10,"user_id":200}"#, &desc)
            .unwrap()
            .unwrap();
        let datums: Vec<_> = row.iter().collect();
        assert_eq!(datums[0], Datum::Int32(10));
        assert_eq!(datums[1], Datum::Int64(200));
    }

    #[mz_ore::test]
    fn test_boolean_types() {
        let desc = RelationDesc::builder()
            .with_column("id", SqlScalarType::Int32.nullable(false))
            .with_column("flag", SqlScalarType::Bool.nullable(false))
            .finish();
        let msg = br#"{"before":null,"after":{"id":1,"flag":true},"op":"c"}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let after_list: Vec<_> = row.iter().nth(1).unwrap().unwrap_list().iter().collect();
        assert_eq!(after_list[1], Datum::True);
    }

    #[mz_ore::test]
    fn test_non_nullable_null_errors() {
        // A NOT NULL column receiving null should error.
        let desc = RelationDesc::builder()
            .with_column("id", SqlScalarType::Int32.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .finish();
        let msg = br#"{"before":null,"after":{"id":1,"name":null},"op":"c"}"#;
        let err = decode_debezium_json(msg, &desc).unwrap_err();
        assert!(matches!(err, DebeziumJsonError::TypeMismatch { .. }));
    }

    #[mz_ore::test]
    fn test_extra_fields_ignored() {
        let desc = test_desc();
        // Extra metadata fields (source, ts_ms) in the envelope should not cause errors
        let msg = br#"{"before":null,"after":{"id":1,"name":"ok"},"op":"c","ts_ms":123,"source":{"db":"test"}}"#;
        let row = decode_debezium_json(msg, &desc).unwrap().unwrap();
        let after_list: Vec<_> = row.iter().nth(1).unwrap().unwrap_list().iter().collect();
        assert_eq!(after_list[0], Datum::Int32(1));
        assert_eq!(after_list[1], Datum::String("ok"));
    }
}
