// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for the Avro representation of the CDCv2 protocol.

use mz_avro::schema::{FullName, SchemaNode};
use mz_repr::{Diff, Row, Timestamp};
use serde_json::json;

use anyhow::anyhow;
use differential_dataflow::capture::{Message, Progress};
use mz_avro::error::{DecodeError, Error as AvroError};
use mz_avro::schema::Schema;
use mz_avro::{
    define_unexpected, ArrayAsVecDecoder, AvroDecodable, AvroDecode, AvroDeserializer, AvroRead,
    StatefulAvroDecodable,
};
use mz_avro_derive::AvroDecodable;
use std::{cell::RefCell, rc::Rc};

use super::decode::RowWrapper;

pub fn extract_data_columns<'a>(schema: &'a Schema) -> anyhow::Result<SchemaNode<'a>> {
    let data_name = FullName::from_parts("data", Some("com.materialize.cdc"), "");
    let data_schema = &schema
        .try_lookup_name(&data_name)
        .ok_or_else(|| anyhow!("record not found: {:?}", data_name))?
        .piece;
    Ok(SchemaNode {
        root: &schema,
        inner: data_schema,
        name: None,
    })
}

#[derive(AvroDecodable)]
#[state_type(Rc<RefCell<Row>>, Rc<RefCell<Vec<u8>>>)]
struct MyUpdate {
    #[state_expr(Rc::clone(&self._STATE.0), Rc::clone(&self._STATE.1))]
    data: RowWrapper,
    time: Timestamp,
    diff: Diff,
}

#[derive(AvroDecodable)]
struct Count {
    time: Timestamp,
    count: usize,
}

fn make_counts_decoder() -> impl AvroDecode<Out = Vec<(Timestamp, usize)>> {
    ArrayAsVecDecoder::new(|| {
        <Count as AvroDecodable>::new_decoder().map_decoder(|ct| Ok((ct.time, ct.count)))
    })
}

#[derive(AvroDecodable)]
struct MyProgress {
    lower: Vec<Timestamp>,
    upper: Vec<Timestamp>,
    #[decoder_factory(make_counts_decoder)]
    counts: Vec<(Timestamp, usize)>,
}

impl AvroDecode for Decoder {
    type Out = Message<Row, Timestamp, Diff>;
    fn union_branch<'a, R: AvroRead, D: AvroDeserializer>(
        self,
        idx: usize,
        _n_variants: usize,
        _null_variant: Option<usize>,
        deserializer: D,
        r: &'a mut R,
    ) -> Result<Self::Out, AvroError> {
        match idx {
            0 => {
                let packer = Rc::new(RefCell::new(Row::default()));
                let buf = Rc::new(RefCell::new(vec![]));
                let d = ArrayAsVecDecoder::new(|| {
                    <MyUpdate as StatefulAvroDecodable>::new_decoder((
                        Rc::clone(&packer),
                        Rc::clone(&buf),
                    ))
                    .map_decoder(|update| Ok((update.data.0, update.time, update.diff)))
                });
                let updates = deserializer.deserialize(r, d)?;
                Ok(Message::Updates(updates))
            }
            1 => {
                let progress =
                    deserializer.deserialize(r, <MyProgress as AvroDecodable>::new_decoder())?;
                let progress = Progress {
                    lower: progress.lower,
                    upper: progress.upper,
                    counts: progress.counts,
                };
                Ok(Message::Progress(progress))
            }

            other => Err(DecodeError::Custom(format!(
                "Unrecognized union variant in CDCv2 decoder: {}",
                other
            ))
            .into()),
        }
    }
    define_unexpected! {
        record, array, map, enum_variant, scalar, decimal, bytes, string, json, uuid, fixed
    }
}

/// Collected state to decode update batches and progress statements.
#[derive(Debug)]
pub struct Decoder;

/// Construct the schema for the CDC V2 protocol.
pub fn build_schema(row_schema: serde_json::Value) -> Schema {
    let updates_schema = json!({
        "type": "array",
        "items": {
            "name" : "update",
            "type" : "record",
            "fields" : [
                {
                    "name": "data",
                    "type": row_schema,
                },
                {
                    "name" : "time",
                    "type" : "long",
                },
                {
                    "name" : "diff",
                    "type" : "long",
                },
            ],
        },
    });

    let progress_schema = json!({
        "name" : "progress",
        "type" : "record",
        "fields" : [
            {
                "name": "lower",
                "type": {
                    "type": "array",
                    "items": "long"
                }
            },
            {
                "name": "upper",
                "type": {
                    "type": "array",
                    "items": "long"
                }
            },
            {
                "name": "counts",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "counts",
                        "fields": [
                            {
                                "name": "time",
                                "type": "long",
                            },
                            {
                                "name": "count",
                                "type": "long",
                            },
                        ],
                    }
                }
            },
        ],
    });
    let message_schema = json!([updates_schema, progress_schema,]);

    Schema::parse(&message_schema).expect("schema constrution failed")
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::avro::encode_datums_as_avro;
    use crate::encode::column_names_and_types;
    use mz_avro::types::Value;
    use mz_avro::AvroDeserializer;
    use mz_avro::GeneralDeserializer;
    use mz_repr::{ColumnName, ColumnType, RelationDesc, Row, ScalarType};

    use crate::json::build_row_schema_json;

    /// Collected state to encode update batches and progress statements.
    #[derive(Debug)]
    struct Encoder {
        columns: Vec<(ColumnName, ColumnType)>,
    }

    impl Encoder {
        /// Creates a new CDCv2 encoder from a relation description.
        pub fn new(desc: RelationDesc) -> Self {
            let columns = column_names_and_types(desc);
            Self { columns }
        }

        /// Encodes a batch of updates as an Avro value.
        pub fn encode_updates(&self, updates: &[(Row, i64, i64)]) -> Value {
            let mut enc_updates = Vec::new();
            for (data, time, diff) in updates {
                let enc_data = encode_datums_as_avro(&**data, &self.columns);
                let enc_time = Value::Long(time.clone());
                let enc_diff = Value::Long(diff.clone());
                enc_updates.push(Value::Record(vec![
                    ("data".to_string(), enc_data),
                    ("time".to_string(), enc_time),
                    ("diff".to_string(), enc_diff),
                ]));
            }
            Value::Union {
                index: 0,
                inner: Box::new(Value::Array(enc_updates)),
                n_variants: 2,
                null_variant: None,
            }
        }

        /// Encodes the contents of a progress statement as an Avro value.
        pub fn encode_progress(
            &self,
            lower: &[i64],
            upper: &[i64],
            counts: &[(i64, i64)],
        ) -> Value {
            let enc_lower = Value::Array(lower.iter().cloned().map(Value::Long).collect());
            let enc_upper = Value::Array(upper.iter().cloned().map(Value::Long).collect());
            let enc_counts = Value::Array(
                counts
                    .iter()
                    .map(|(time, count)| {
                        Value::Record(vec![
                            ("time".to_string(), Value::Long(time.clone())),
                            ("count".to_string(), Value::Long(count.clone())),
                        ])
                    })
                    .collect(),
            );
            let enc_progress = Value::Record(vec![
                ("lower".to_string(), enc_lower),
                ("upper".to_string(), enc_upper),
                ("counts".to_string(), enc_counts),
            ]);

            Value::Union {
                index: 1,
                inner: Box::new(enc_progress),
                n_variants: 2,
                null_variant: None,
            }
        }
    }

    #[test]
    fn test_roundtrip() {
        let desc = RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("price", ScalarType::Float64.nullable(true));

        let encoder = Encoder::new(desc.clone());
        let row_schema =
            build_row_schema_json(&crate::encode::column_names_and_types(desc), "data");
        let schema = build_schema(row_schema);

        let values = vec![
            encoder.encode_updates(&[]),
            encoder.encode_progress(&[0], &[3], &[]),
            encoder.encode_progress(&[3], &[], &[]),
        ];
        use mz_avro::encode::encode_to_vec;
        let mut values: Vec<_> = values
            .into_iter()
            .map(|v| encode_to_vec(&v, &schema))
            .collect();

        let g = GeneralDeserializer {
            schema: schema.top_node(),
        };
        assert!(matches!(
            g.deserialize(&mut &values.remove(0)[..], Decoder).unwrap(),
            Message::Updates(_)
        ),);
        assert!(matches!(
            g.deserialize(&mut &values.remove(0)[..], Decoder).unwrap(),
            Message::Progress(_)
        ),);
        assert!(matches!(
            g.deserialize(&mut &values.remove(0)[..], Decoder).unwrap(),
            Message::Progress(_)
        ),);
    }
}
