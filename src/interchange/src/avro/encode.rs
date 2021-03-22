// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt;

use byteorder::{NetworkEndian, WriteBytesExt};
use chrono::Timelike;
use itertools::Itertools;
use lazy_static::lazy_static;
use repr::adt::jsonb::JsonbRef;
use repr::adt::rdn;
use repr::{ColumnName, ColumnType, Datum, RelationDesc, Row, ScalarType};
use serde_json::json;

use mz_avro::types::{DecimalValue, Value};
use mz_avro::Schema;

lazy_static! {
    // TODO(rkhaitan): this schema intentionally omits the data_collections field
    // that is typically present in Debezium transaction metadata topics. See
    // https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-transaction-metadata
    // for more information. We chose to omit this field because it is redundant
    // for sinks where each consistency topic corresponds to exactly one sink.
    // We will need to add it in order to be able to reingest sinked topics.
    static ref DEBEZIUM_TRANSACTION_SCHEMA: Schema =
    Schema::parse(&json!({
        "type": "record",
        "name": "envelope",
        "fields": [
            {
                "name": "id",
                "type": "string"
            },
            {
                "name": "status",
                "type": "string"
            },
            {
                "name": "event_count",
                "type": [
                  "null",
                  "long"
                ]
            }
        ]
    })).expect("valid schema constructed");
}

/// Builds a Debezium-encoded Avro schema that corresponds to `desc`.
///
/// Requires that all column names in `desc` are present. The returned schema
/// has some special properties to ease encoding:
///
///   * Union schemas are only used to represent nullability. The first
///     variant is always the null variant, and the second and last variant
///     is the non-null variant.
fn build_schema(columns: &[(ColumnName, ColumnType)]) -> Schema {
    let row_schema = build_row_schema_json(&columns, "envelope");
    Schema::parse(&row_schema).expect("valid schema constructed")
}

fn encode_avro_header(buf: &mut Vec<u8>, schema_id: i32) {
    // The first byte is a magic byte (0) that indicates the Confluent
    // serialization format version, and the next four bytes are a
    // 32-bit schema ID.
    //
    // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
    buf.write_u8(0).expect("writing to vec cannot fail");
    buf.write_i32::<NetworkEndian>(schema_id)
        .expect("writing to vec cannot fail");
}

struct KeyInfo {
    columns: Vec<(ColumnName, ColumnType)>,
    schema: Schema,
}

fn encode_message_unchecked(
    schema_id: i32,
    row: Row,
    schema: &Schema,
    columns: &[(ColumnName, ColumnType)],
) -> Vec<u8> {
    let mut buf = vec![];
    encode_avro_header(&mut buf, schema_id);
    let value = encode_datums_as_avro(row.iter(), columns);
    mz_avro::encode_unchecked(&value, schema, &mut buf);
    buf
}

/// Manages encoding of Avro-encoded bytes.
pub struct Encoder {
    value_columns: Vec<(ColumnName, ColumnType)>,
    key_info: Option<KeyInfo>,
    writer_schema: Schema,
}

impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Encoder")
            .field("writer_schema", &self.writer_schema)
            .finish()
    }
}

impl Encoder {
    pub fn new(
        key_desc: Option<RelationDesc>,
        value_desc: RelationDesc,
        include_transaction: bool,
    ) -> Self {
        let mut value_columns = column_names_and_types(value_desc);
        if include_transaction {
            // TODO(rkhaitan): this schema omits the total_order and data collection_order
            // fields found in Debezium's transaction metadata struct. We chose to omit
            // those because the order is not stable across reruns and has no semantic
            // meaning for records within a timestamp in Materialize. These fields may
            // be useful in the future for deduplication.
            value_columns.push((
                "transaction".into(),
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::Record {
                        fields: vec![(
                            "id".into(),
                            ColumnType {
                                scalar_type: ScalarType::String,
                                nullable: false,
                            },
                        )],
                        custom_oid: None,
                        custom_name: Some("transaction".to_string()),
                    },
                },
            ));
        }
        let writer_schema = build_schema(&value_columns);
        let key_info = key_desc.map(|key_desc| {
            let columns = column_names_and_types(key_desc);
            let row_schema = build_row_schema_json(&columns, "row");
            KeyInfo {
                schema: Schema::parse(&row_schema).expect("valid schema constructed"),
                columns,
            }
        });
        Encoder {
            value_columns,
            key_info,
            writer_schema,
        }
    }

    pub fn value_writer_schema(&self) -> &Schema {
        &self.writer_schema
    }

    pub fn value_columns(&self) -> &[(ColumnName, ColumnType)] {
        &self.value_columns
    }

    pub fn key_writer_schema(&self) -> Option<&Schema> {
        self.key_info.as_ref().map(|KeyInfo { schema, .. }| schema)
    }

    pub fn key_columns(&self) -> Option<&[(ColumnName, ColumnType)]> {
        self.key_info
            .as_ref()
            .map(|KeyInfo { columns, .. }| columns.as_slice())
    }

    pub fn encode_key_unchecked(&self, schema_id: i32, row: Row) -> Vec<u8> {
        let schema = self.key_writer_schema().unwrap();
        let columns = self.key_columns().unwrap();
        encode_message_unchecked(schema_id, row, schema, columns)
    }

    pub fn encode_value_unchecked(&self, schema_id: i32, row: Row) -> Vec<u8> {
        let schema = self.value_writer_schema();
        let columns = self.value_columns();
        encode_message_unchecked(schema_id, row, schema, columns)
    }
}

/// Extracts deduplicated column names and types from a relation description.
pub fn column_names_and_types(desc: RelationDesc) -> Vec<(ColumnName, ColumnType)> {
    // Invent names for columns that don't have a name.
    let mut columns: Vec<_> = desc
        .into_iter()
        .enumerate()
        .map(|(i, (name, ty))| match name {
            None => (ColumnName::from(format!("column{}", i + 1)), ty),
            Some(name) => (name, ty),
        })
        .collect();

    // Deduplicate names.
    let mut seen = HashSet::new();
    for (name, _ty) in &mut columns {
        let stem_len = name.as_str().len();
        let mut i = 1;
        while seen.contains(name) {
            name.as_mut_str().truncate(stem_len);
            if name.as_str().ends_with(|c: char| c.is_ascii_digit()) {
                name.as_mut_str().push('_');
            }
            name.as_mut_str().push_str(&i.to_string());
            i += 1;
        }
        seen.insert(name);
    }
    columns
}

/// Encodes a sequence of `Datum` as Avro (key and value), using supplied column names and types.
pub fn encode_datums_as_avro<'a, I>(datums: I, names_types: &[(ColumnName, ColumnType)]) -> Value
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let value_fields: Vec<(String, Value)> = names_types
        .iter()
        .zip_eq(datums)
        .map(|((name, typ), datum)| {
            let name = name.as_str().to_owned();
            use mz_avro::types::ToAvro;
            (name, TypedDatum::new(datum, typ.clone()).avro())
        })
        .collect();
    let v = Value::Record(value_fields);
    v
}

/// Bundled information sufficient to encode as Avro.
#[derive(Debug)]
pub struct TypedDatum<'a> {
    datum: Datum<'a>,
    typ: ColumnType,
}

impl<'a> TypedDatum<'a> {
    /// Pairs a datum and its type, for Avro encoding.
    pub fn new(datum: Datum<'a>, typ: ColumnType) -> Self {
        Self { datum, typ }
    }
}

impl<'a> mz_avro::types::ToAvro for TypedDatum<'a> {
    fn avro(self) -> Value {
        let TypedDatum { datum, typ } = self;
        if typ.nullable && datum.is_null() {
            Value::Union {
                index: 0,
                inner: Box::new(Value::Null),
                n_variants: 2,
                null_variant: Some(0),
            }
        } else {
            let mut val = match &typ.scalar_type {
                ScalarType::Bool => Value::Boolean(datum.unwrap_bool()),
                ScalarType::Int32 | ScalarType::Oid => Value::Int(datum.unwrap_int32()),
                ScalarType::Int64 => Value::Long(datum.unwrap_int64()),
                ScalarType::Float32 => Value::Float(datum.unwrap_float32()),
                ScalarType::Float64 => Value::Double(datum.unwrap_float64()),
                ScalarType::Decimal(p, s) => Value::Decimal(DecimalValue {
                    unscaled: datum.unwrap_decimal().as_i128().to_be_bytes().to_vec(),
                    precision: (*p).into(),
                    scale: (*s).into(),
                }),
                ScalarType::Date => Value::Date(datum.unwrap_date()),
                ScalarType::Time => Value::Long({
                    let time = datum.unwrap_time();
                    (time.num_seconds_from_midnight() * 1_000_000) as i64
                        + (time.nanosecond() as i64) / 1_000
                }),
                ScalarType::Timestamp => Value::Timestamp(datum.unwrap_timestamp()),
                ScalarType::TimestampTz => Value::Timestamp(datum.unwrap_timestamptz().naive_utc()),
                // This feature isn't actually supported by the Avro Java
                // client (https://issues.apache.org/jira/browse/AVRO-2123),
                // so no one is likely to be using it, so we're just using
                // our own very convenient format.
                ScalarType::Interval => Value::Fixed(20, {
                    let iv = datum.unwrap_interval();
                    let mut buf = Vec::with_capacity(24);
                    buf.extend(&iv.months.to_le_bytes());
                    buf.extend(&iv.duration.to_le_bytes());
                    debug_assert_eq!(buf.len(), 20);
                    buf
                }),
                ScalarType::Bytes => Value::Bytes(Vec::from(datum.unwrap_bytes())),
                ScalarType::String => Value::String(datum.unwrap_str().to_owned()),
                ScalarType::Jsonb => Value::Json(JsonbRef::from_datum(datum).to_serde_json()),
                ScalarType::Uuid => Value::Uuid(datum.unwrap_uuid()),
                ScalarType::Array(_t) => unimplemented!("array types"),
                ScalarType::List { .. } => unimplemented!("list types"),
                ScalarType::Map { .. } => unimplemented!("map types"),
                ScalarType::Record { fields, .. } => {
                    let list = datum.unwrap_list();
                    let fields = fields
                        .iter()
                        .zip(list.into_iter())
                        .map(|((name, typ), datum)| {
                            let name = name.to_string();
                            let datum = TypedDatum::new(datum, typ.clone());
                            let value = datum.avro();
                            (name, value)
                        })
                        .collect();
                    Value::Record(fields)
                }
                ScalarType::Numeric { scale } => {
                    // Ensure datum has appropriate scale.
                    let mut s = datum.unwrap_numeric();
                    if let Some(scale) = scale {
                        rdn::rescale(&mut s, *scale).unwrap();
                    }

                    Value::Decimal(DecimalValue {
                        unscaled: s.0.to_be_bytes().to_vec(),
                        precision: rdn::RDN_MAX_PRECISION,
                        scale: rdn::get_scale(&s),
                    })
                }
            };
            if typ.nullable {
                val = Value::Union {
                    index: 1,
                    inner: Box::new(val),
                    n_variants: 2,
                    null_variant: Some(0),
                };
            }
            val
        }
    }
}

pub fn get_debezium_transaction_schema() -> &'static Schema {
    &DEBEZIUM_TRANSACTION_SCHEMA
}

pub fn encode_debezium_transaction_unchecked(
    schema_id: i32,
    id: &str,
    status: &str,
    message_count: Option<i64>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_avro_header(&mut buf, schema_id);

    let transaction_id = Value::String(id.to_owned());
    let status = Value::String(status.to_owned());
    let event_count = match message_count {
        None => Value::Union {
            index: 0,
            inner: Box::new(Value::Null),
            n_variants: 2,
            null_variant: Some(0),
        },
        Some(count) => Value::Union {
            index: 1,
            inner: Box::new(Value::Long(count)),
            n_variants: 2,
            null_variant: Some(0),
        },
    };

    let avro = Value::Record(vec![
        ("id".into(), transaction_id),
        ("status".into(), status),
        ("event_count".into(), event_count),
    ]);
    debug_assert!(avro.validate(DEBEZIUM_TRANSACTION_SCHEMA.top_node()));
    mz_avro::encode_unchecked(&avro, &DEBEZIUM_TRANSACTION_SCHEMA, &mut buf);
    buf
}

pub(super) fn build_row_schema_fields<F: FnMut() -> String>(
    columns: &[(ColumnName, ColumnType)],
    names_seen: &mut HashSet<String>,
    namer: &mut F,
) -> Vec<serde_json::value::Value> {
    let mut fields = Vec::new();
    for (name, typ) in columns.iter() {
        let mut field_type = match &typ.scalar_type {
            ScalarType::Bool => json!("boolean"),
            ScalarType::Int32 | ScalarType::Oid => json!("int"),
            ScalarType::Int64 => json!("long"),
            ScalarType::Float32 => json!("float"),
            ScalarType::Float64 => json!("double"),
            ScalarType::Decimal(p, s) => json!({
                "type": "bytes",
                "logicalType": "decimal",
                "precision": p,
                "scale": s,
            }),
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
            ScalarType::Array(_t) => unimplemented!("array types"),
            ScalarType::List { .. } => unimplemented!("list types"),
            ScalarType::Map { .. } => unimplemented!("map types"),
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
            ScalarType::Numeric { scale } => json!({
                "type": "bytes",
                "logicalType": "decimal",
                "precision": rdn::RDN_MAX_PRECISION,
                "scale": scale.unwrap_or(8),
            }),
        };
        if typ.nullable {
            field_type = json!(["null", field_type]);
        }
        fields.push(json!({
            "name": name,
            "type": field_type,
        }));
    }
    fields
}

/// Builds the JSON for the row schema, which can be independently useful.
pub(super) fn build_row_schema_json(
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
