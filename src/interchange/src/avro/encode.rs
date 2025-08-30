// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::LazyLock;

use anyhow::Ok;
use byteorder::{NetworkEndian, WriteBytesExt};
use chrono::Timelike;
use itertools::Itertools;
use mz_avro::Schema;
use mz_avro::types::{DecimalValue, ToAvro, Value};
use mz_ore::cast::CastFrom;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::adt::numeric::{self, NUMERIC_AGG_MAX_PRECISION, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::{CatalogItemId, ColumnName, ColumnType, Datum, RelationDesc, Row, ScalarType};
use serde_json::json;

use crate::encode::{Encode, TypedDatum, column_names_and_types};
use crate::envelopes::{self, DBZ_ROW_TYPE_ID, ENVELOPE_CUSTOM_NAMES};
use crate::json::{SchemaOptions, build_row_schema_json};

// TODO(rkhaitan): this schema intentionally omits the data_collections field
// that is typically present in Debezium transaction metadata topics. See
// https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-transaction-metadata
// for more information. We chose to omit this field because it is redundant
// for sinks where each consistency topic corresponds to exactly one sink.
// We will need to add it in order to be able to reingest sinked topics.
static DEBEZIUM_TRANSACTION_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
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
    // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
    buf.write_u8(0).expect("writing to vec cannot fail");
    buf.write_i32::<NetworkEndian>(schema_id)
        .expect("writing to vec cannot fail");
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum DocTarget {
    Type(CatalogItemId),
    Field {
        object_id: CatalogItemId,
        column_name: ColumnName,
    },
}

impl DocTarget {
    fn id(&self) -> CatalogItemId {
        match self {
            DocTarget::Type(object_id) => *object_id,
            DocTarget::Field { object_id, .. } => *object_id,
        }
    }
}

/// Generates an Avro schema
pub struct AvroSchemaGenerator {
    columns: Vec<(ColumnName, ColumnType)>,
    schema: Schema,
}

impl fmt::Debug for AvroSchemaGenerator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SchemaGenerator")
            .field("writer_schema", &self.schema())
            .finish()
    }
}

impl AvroSchemaGenerator {
    pub fn new(
        desc: RelationDesc,
        debezium: bool,
        mut doc_options: BTreeMap<DocTarget, String>,
        avro_fullname: &str,
        set_null_defaults: bool,
        sink_from: Option<CatalogItemId>,
        use_custom_envelope_names: bool,
    ) -> Result<Self, anyhow::Error> {
        let mut columns = column_names_and_types(desc);
        if debezium {
            columns = envelopes::dbz_envelope(columns);
            // With DEBEZIUM envelope the message is wrapped into "before" and "after"
            // with `DBZ_ROW_TYPE_ID` instead of `sink_from`.
            // Replacing comments for the columns and type in `sink_from` to `DBZ_ROW_TYPE_ID`.
            if let Some(sink_from_id) = sink_from {
                let mut new_column_docs = BTreeMap::new();
                doc_options.iter().for_each(|(k, v)| {
                    if k.id() == sink_from_id {
                        match k {
                            DocTarget::Field { column_name, .. } => {
                                new_column_docs.insert(
                                    DocTarget::Field {
                                        object_id: DBZ_ROW_TYPE_ID,
                                        column_name: column_name.clone(),
                                    },
                                    v.clone(),
                                );
                            }
                            DocTarget::Type(_) => {
                                new_column_docs.insert(DocTarget::Type(DBZ_ROW_TYPE_ID), v.clone());
                            }
                        }
                    }
                });
                doc_options.append(&mut new_column_docs);
                doc_options.retain(|k, _v| k.id() != sink_from_id);
            }
        }
        let custom_names = if use_custom_envelope_names {
            &ENVELOPE_CUSTOM_NAMES
        } else {
            &BTreeMap::new()
        };
        let row_schema = build_row_schema_json(
            &columns,
            avro_fullname,
            custom_names,
            sink_from,
            &SchemaOptions {
                set_null_defaults,
                doc_comments: doc_options,
            },
        )?;
        let schema = Schema::parse(&row_schema).expect("valid schema constructed");
        Ok(AvroSchemaGenerator { columns, schema })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn columns(&self) -> &[(ColumnName, ColumnType)] {
        &self.columns
    }
}

/// Manages encoding of Avro-encoded bytes.
pub struct AvroEncoder {
    columns: Vec<(ColumnName, ColumnType)>,
    schema: Schema,
    schema_id: i32,
}

impl fmt::Debug for AvroEncoder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AvroEncoder")
            .field("writer_schema", &self.schema)
            .finish()
    }
}

impl AvroEncoder {
    pub fn new(desc: RelationDesc, debezium: bool, schema: &str, schema_id: i32) -> Self {
        let mut columns = column_names_and_types(desc);
        if debezium {
            columns = envelopes::dbz_envelope(columns);
        };
        AvroEncoder {
            columns,
            schema: Schema::parse(&serde_json::from_str(schema).expect("valid schema json"))
                .expect("valid schema"),
            schema_id,
        }
    }
}

impl Encode for AvroEncoder {
    fn encode_unchecked(&self, row: Row) -> Vec<u8> {
        encode_message_unchecked(self.schema_id, row, &self.schema, &self.columns)
    }

    fn hash(&self, buf: &[u8]) -> u64 {
        // Compute a stable hash by ignoring the avro header which might contain a
        // non-deterministic schema id.
        let (_schema_id, payload) = crate::confluent::extract_avro_header(buf).unwrap();
        seahash::hash(payload)
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
            (name, TypedDatum::new(datum, typ).avro())
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
                ScalarType::AclItem => Value::String(datum.unwrap_acl_item().to_string()),
                ScalarType::Bool => Value::Boolean(datum.unwrap_bool()),
                ScalarType::PgLegacyChar => {
                    Value::Fixed(1, datum.unwrap_uint8().to_le_bytes().into())
                }
                ScalarType::Int16 => Value::Int(i32::from(datum.unwrap_int16())),
                ScalarType::Int32 => Value::Int(datum.unwrap_int32()),
                ScalarType::Int64 => Value::Long(datum.unwrap_int64()),
                ScalarType::UInt16 => Value::Fixed(2, datum.unwrap_uint16().to_be_bytes().into()),
                ScalarType::UInt32 => Value::Fixed(4, datum.unwrap_uint32().to_be_bytes().into()),
                ScalarType::UInt64 => Value::Fixed(8, datum.unwrap_uint64().to_be_bytes().into()),
                ScalarType::Oid
                | ScalarType::RegClass
                | ScalarType::RegProc
                | ScalarType::RegType => {
                    Value::Fixed(4, datum.unwrap_uint32().to_be_bytes().into())
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
                ScalarType::Date => Value::Date(datum.unwrap_date().unix_epoch_days()),
                ScalarType::Time => Value::Long({
                    let time = datum.unwrap_time();
                    i64::from(time.num_seconds_from_midnight()) * 1_000_000
                        + i64::from(time.nanosecond()) / 1_000
                }),
                ScalarType::Timestamp { .. } => {
                    Value::Timestamp(datum.unwrap_timestamp().to_naive())
                }
                ScalarType::TimestampTz { .. } => {
                    Value::Timestamp(datum.unwrap_timestamptz().to_naive())
                }
                // SQL intervals and Avro durations differ quite a lot (signed
                // vs unsigned, different int sizes), so SQL intervals are their
                // own bespoke type.
                ScalarType::Interval => Value::Fixed(16, {
                    let iv = datum.unwrap_interval();
                    let mut buf = Vec::with_capacity(16);
                    buf.extend(iv.months.to_le_bytes());
                    buf.extend(iv.days.to_le_bytes());
                    buf.extend(iv.micros.to_le_bytes());
                    debug_assert_eq!(buf.len(), 16);
                    buf
                }),
                ScalarType::Bytes => Value::Bytes(Vec::from(datum.unwrap_bytes())),
                ScalarType::String | ScalarType::VarChar { .. } | ScalarType::PgLegacyName => {
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
                            TypedDatum::new(
                                datum,
                                &ColumnType {
                                    nullable: true,
                                    scalar_type: ty.unwrap_collection_element_type().clone(),
                                },
                            )
                            .avro()
                        })
                        .collect();
                    Value::Array(values)
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
                            .avro();
                            (key.to_string(), value)
                        })
                        .collect();
                    Value::Map(elements)
                }
                ScalarType::Record { fields, .. } => {
                    let list = datum.unwrap_list();
                    let fields = fields
                        .iter()
                        .zip_eq(&list)
                        .map(|((name, typ), datum)| {
                            let name = name.to_string();
                            let datum = TypedDatum::new(datum, typ);
                            let value = datum.avro();
                            (name, value)
                        })
                        .collect();
                    Value::Record(fields)
                }
                ScalarType::MzTimestamp => Value::String(datum.unwrap_mz_timestamp().to_string()),
                ScalarType::Range { .. } => Value::String(datum.unwrap_range().to_string()),
                ScalarType::MzAclItem => Value::String(datum.unwrap_mz_acl_item().to_string()),
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
