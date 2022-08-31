// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;

use byteorder::{NetworkEndian, WriteBytesExt};
use chrono::Timelike;
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde_json::json;

use mz_avro::types::{AvroMap, DecimalValue, Value};
use mz_avro::Schema;
use mz_ore::cast::CastFrom;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{self, NUMERIC_AGG_MAX_PRECISION, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::{ColumnName, ColumnType, Datum, RelationDesc, Row, ScalarType};

use crate::encode::{column_names_and_types, Encode, TypedDatum};
use crate::envelopes::{self, ENVELOPE_CUSTOM_NAMES};
use crate::json::build_row_schema_json;

// TODO(rkhaitan): this schema intentionally omits the data_collections field
// that is typically present in Debezium transaction metadata topics. See
// https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-transaction-metadata
// for more information. We chose to omit this field because it is redundant
// for sinks where each consistency topic corresponds to exactly one sink.
// We will need to add it in order to be able to reingest sinked topics.
static DEBEZIUM_TRANSACTION_SCHEMA: Lazy<Schema> = Lazy::new(|| {
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
            },
            {
                "name": "data_collections",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "data_collection",
                            "fields": [
                                {
                                    "name": "data_collection",
                                    "type": "string"
                                },
                                {
                                    "name": "event_count",
                                    "type": "long"
                                },
                            ]
                        }
                    }
                ],
                "default": null,
            },
        ]
    }))
    .expect("valid schema constructed")
});

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

/// Generates key and value Avro schemas
pub struct AvroSchemaGenerator {
    value_columns: Vec<(ColumnName, ColumnType)>,
    key_info: Option<KeyInfo>,
    writer_schema: Schema,
}

impl AvroSchemaGenerator {
    pub fn new(
        key_fullname: Option<&str>,
        value_fullname: Option<&str>,
        key_desc: Option<RelationDesc>,
        value_desc: RelationDesc,
        debezium: bool,
        include_transaction: bool,
    ) -> Self {
        let mut value_columns = column_names_and_types(value_desc);
        if debezium {
            value_columns = envelopes::dbz_envelope(value_columns);
        }
        if include_transaction {
            // TODO(rkhaitan): this schema omits the total_order and data collection_order
            // fields found in Debezium's transaction metadata struct. We chose to omit
            // those because the order is not stable across reruns and has no semantic
            // meaning for records within a timestamp in Materialize. These fields may
            // be useful in the future for deduplication.
            envelopes::txn_metadata(&mut value_columns);
        }
        let row_schema = build_row_schema_json(
            &value_columns,
            value_fullname.unwrap_or("envelope"),
            &ENVELOPE_CUSTOM_NAMES,
        );
        let writer_schema = Schema::parse(&row_schema).expect("valid schema constructed");
        let key_info = key_desc.map(|key_desc| {
            let columns = column_names_and_types(key_desc);
            let row_schema =
                build_row_schema_json(&columns, key_fullname.unwrap_or("row"), &HashMap::new());
            KeyInfo {
                schema: Schema::parse(&row_schema).expect("valid schema constructed"),
                columns,
            }
        });
        AvroSchemaGenerator {
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
}

impl fmt::Debug for AvroSchemaGenerator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SchemaGenerator")
            .field("writer_schema", &self.writer_schema)
            .finish()
    }
}

/// Manages encoding of Avro-encoded bytes.
pub struct AvroEncoder {
    schema_generator: AvroSchemaGenerator,
    key_schema_id: Option<i32>,
    value_schema_id: i32,
}

impl fmt::Debug for AvroEncoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AvroEncoder")
            .field("writer_schema", &self.schema_generator.writer_schema)
            .finish()
    }
}

impl AvroEncoder {
    pub fn new(
        schema_generator: AvroSchemaGenerator,
        key_schema_id: Option<i32>,
        value_schema_id: i32,
    ) -> Self {
        AvroEncoder {
            schema_generator,
            key_schema_id,
            value_schema_id,
        }
    }

    pub fn encode_key_unchecked(&self, schema_id: i32, row: Row) -> Vec<u8> {
        let schema = self.schema_generator.key_writer_schema().unwrap();
        let columns = self.schema_generator.key_columns().unwrap();
        encode_message_unchecked(schema_id, row, schema, columns)
    }

    pub fn encode_value_unchecked(&self, schema_id: i32, row: Row) -> Vec<u8> {
        let schema = self.schema_generator.value_writer_schema();
        let columns = self.schema_generator.value_columns();
        encode_message_unchecked(schema_id, row, schema, columns)
    }
}

impl Encode for AvroEncoder {
    fn get_format_name(&self) -> &str {
        "avro"
    }

    fn encode_key_unchecked(&self, row: Row) -> Vec<u8> {
        self.encode_key_unchecked(self.key_schema_id.unwrap(), row)
    }

    fn encode_value_unchecked(&self, row: Row) -> Vec<u8> {
        self.encode_value_unchecked(self.value_schema_id, row)
    }
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
                ScalarType::PgLegacyChar => {
                    Value::Fixed(1, datum.unwrap_uint8().to_le_bytes().into())
                }
                ScalarType::Int16 => Value::Int(i32::from(datum.unwrap_int16())),
                ScalarType::Int32 => Value::Int(datum.unwrap_int32()),
                ScalarType::Int64 => Value::Long(datum.unwrap_int64()),
                ScalarType::UInt16 => Value::Fixed(2, datum.unwrap_uint16().to_le_bytes().into()),
                ScalarType::UInt32 => Value::Fixed(4, datum.unwrap_uint32().to_le_bytes().into()),
                ScalarType::UInt64 => Value::Fixed(8, datum.unwrap_uint64().to_le_bytes().into()),
                ScalarType::Oid
                | ScalarType::RegClass
                | ScalarType::RegProc
                | ScalarType::RegType => {
                    Value::Fixed(4, datum.unwrap_uint32().to_le_bytes().into())
                }
                ScalarType::Float32 => Value::Float(datum.unwrap_float32()),
                ScalarType::Float64 => Value::Double(datum.unwrap_float64()),
                ScalarType::Numeric { max_scale } => {
                    let mut d = datum.unwrap_numeric().0;
                    let (unscaled, precision, scale) = match max_scale {
                        Some(max_scale) => {
                            // Values must be rescaled to resaturate trailing zeroes
                            numeric::rescale(&mut d, max_scale.into_u8()).unwrap();
                            (
                                numeric::numeric_to_twos_complement_be(d).to_vec(),
                                NUMERIC_DATUM_MAX_PRECISION,
                                max_scale.into_u8(),
                            )
                        }
                        // Decimals without specified scale must nonetheless be
                        // expressed as a fixed scale, so we write everything as
                        // a 78-digit number with a scale of 39, which
                        // definitively expresses all valid numeric values.
                        None => (
                            numeric::numeric_to_twos_complement_wide(d).to_vec(),
                            NUMERIC_AGG_MAX_PRECISION,
                            NUMERIC_DATUM_MAX_PRECISION,
                        ),
                    };
                    Value::Decimal(DecimalValue {
                        unscaled,
                        precision: usize::cast_from(precision),
                        scale: usize::cast_from(scale),
                    })
                }
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
                ScalarType::Interval => Value::Fixed(16, {
                    let iv = datum.unwrap_interval();
                    let mut buf = Vec::with_capacity(16);
                    buf.extend(&iv.months.to_le_bytes());
                    buf.extend(&iv.days.to_le_bytes());
                    buf.extend(&iv.micros.to_le_bytes());
                    debug_assert_eq!(buf.len(), 16);
                    buf
                }),
                ScalarType::Bytes => Value::Bytes(Vec::from(datum.unwrap_bytes())),
                ScalarType::String | ScalarType::VarChar { .. } => {
                    Value::String(datum.unwrap_str().to_owned())
                }
                ScalarType::Char { length } => {
                    let s = mz_repr::adt::char::format_str_pad(datum.unwrap_str(), *length);
                    Value::String(s)
                }
                ScalarType::Jsonb => Value::Json(JsonbRef::from_datum(datum).to_serde_json()),
                ScalarType::Uuid => Value::Uuid(datum.unwrap_uuid()),
                ty @ (ScalarType::Array(..) | ScalarType::Int2Vector | ScalarType::List { .. }) => {
                    let list = match ty {
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
                            datum.avro()
                        })
                        .collect();
                    Value::Array(values)
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
                            let value = datum.avro();
                            (key.to_string(), value)
                        })
                        .collect();
                    Value::Map(AvroMap(elements))
                }
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
    collection: &str,
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

    let data_collections = if let Some(message_count) = message_count {
        let collection = Value::Record(vec![
            ("data_collection".into(), Value::String(collection.into())),
            ("event_count".into(), Value::Long(message_count)),
        ]);
        Value::Union {
            index: 1,
            inner: Box::new(Value::Array(vec![collection])),
            n_variants: 2,
            null_variant: Some(0),
        }
    } else {
        Value::Union {
            index: 0,
            inner: Box::new(Value::Null),
            n_variants: 2,
            null_variant: Some(0),
        }
    };

    let record_contents = vec![
        ("id".into(), transaction_id),
        ("status".into(), status),
        ("event_count".into(), event_count),
        ("data_collections".into(), data_collections),
    ];
    let avro = Value::Record(record_contents);
    debug_assert!(avro.validate(DEBEZIUM_TRANSACTION_SCHEMA.top_node()));
    mz_avro::encode_unchecked(&avro, &DEBEZIUM_TRANSACTION_SCHEMA, &mut buf);
    buf
}
