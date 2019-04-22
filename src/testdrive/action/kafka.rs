// Copyright 2018 Flavien Raynaud
// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//
// Portions of this file are derived from the ToAvro implementation for
// serde_json::Value that is shipped with the avro_rs project. The original
// source code was retrieved on April 25, 2019 from:
//
//     https://github.com/flavray/avro-rs/blob/c4971ac08f52750db6bc95559c2b5faa6c0c9a06/src/types.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use avro_rs::types::Value as AvroValue;
use avro_rs::Schema;
use byteorder::{NetworkEndian, WriteBytesExt};
use futures::stream::FuturesUnordered;
use futures::Future;
use rdkafka::error::RDKafkaError;
use rdkafka::producer::FutureRecord;
use serde_json::Value as JsonValue;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;
use ore::future::StreamExt;
use ore::vec::VecExt;

pub struct IngestAction {
    topic: String,
    schema: String,
    rows: Vec<String>,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let topic = cmd
        .args
        .remove("topic")
        .ok_or_else(|| String::from("missing topic argument"))?;
    let schema = cmd
        .args
        .remove("schema")
        .ok_or_else(|| String::from("missing schema argument"))?;
    Ok(IngestAction {
        topic,
        schema,
        rows: cmd.input,
    })
}

impl Action for IngestAction {
    fn undo(&self, state: &mut State) -> Result<(), String> {
        println!("Deleting Kafka topic {:?}", self.topic);
        let res = state
            .kafka_admin
            .delete_topics(&[&self.topic], &state.kafka_admin_opts)
            .wait();
        let res = match res {
            Err(err) => return Err(err.to_string()),
            Ok(res) => res,
        };
        if res.len() != 1 {
            return Err(format!(
                "kafka topic deletion returned {} results, but exactly one result was expected",
                res.len()
            ));
        }
        match res.into_element() {
            Ok(_) | Err((_, RDKafkaError::UnknownTopicOrPartition)) => Ok(()),
            Err((_, err)) => Err(err.to_string()),
        }
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        println!("Ingesting data into Kafka topic {:?}", self.topic);
        let schema = Schema::parse_str(&self.schema).map_err(|e| e.to_string())?;
        let mut futs = FuturesUnordered::new();
        for row in &self.rows {
            let val = json_to_avro(serde_json::from_str(row).map_err(|e| e.to_string())?);

            // The first byte is a magic byte (0) that indicates the Confluent
            // serialization format version, and the next four bytes are a
            // 32-bit schema ID.
            //
            // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
            let mut buf = Vec::new();
            buf.write_u8(0).unwrap();
            buf.write_u32::<NetworkEndian>(1).unwrap();
            buf.extend(avro_rs::to_avro_datum(&schema, val).map_err(|e| e.to_string())?);

            let record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&self.topic).payload(&buf);
            futs.push(state.kafka_producer.send(record, 1000 /* block_ms */));
        }
        futs.drain().wait().map_err(|e| e.to_string())
    }
}

// This function is derived from code in the avro_rs project. Update the license
// header on this file accordingly if you move it to a new home.
fn json_to_avro(json: JsonValue) -> AvroValue {
    match json {
        JsonValue::Null => AvroValue::Null,
        JsonValue::Bool(b) => AvroValue::Boolean(b),
        JsonValue::Number(ref n) if n.is_i64() => AvroValue::Long(n.as_i64().unwrap()),
        JsonValue::Number(ref n) if n.is_f64() => AvroValue::Double(n.as_f64().unwrap()),
        // TODO(benesch): this is silently wrong for large numbers
        JsonValue::Number(n) => AvroValue::Long(n.as_u64().unwrap() as i64),
        JsonValue::String(s) => AvroValue::String(s),
        JsonValue::Array(items) => AvroValue::Array(items.into_iter().map(json_to_avro).collect()),
        JsonValue::Object(items) => AvroValue::Record(
            items
                .into_iter()
                .map(|(key, value)| (key, json_to_avro(value)))
                .collect(),
        ),
    }
}
