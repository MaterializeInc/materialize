// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use differential_dataflow::hashable::Hashable;
use timely::dataflow::{
    channels::pact::Exchange,
    channels::pushers::buffer::Session,
    channels::pushers::Counter as PushCounter,
    channels::pushers::Tee,
    operators::{map::Map, Operator},
    Scope, Stream,
};

use dataflow_types::LinearOperator;
use dataflow_types::{DataEncoding, Diff, Envelope, Timestamp};
use futures::executor::block_on;
use repr::Datum;
use repr::{Row, RowPacker};

mod avro;
mod csv;
mod protobuf;
mod regex;

use self::csv::csv;
use self::regex::regex as regex_fn;
use crate::operator::StreamExt;
use ::avro::{types::Value, Schema};
use interchange::avro::{extract_debezium_slow, extract_row, DiffPair};

use log::error;
use std::iter;

pub fn decode_avro_values<G>(
    stream: &Stream<G, (Value, Option<i64>)>,
    envelope: &Envelope,
    schema: Schema,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    // TODO(brennan) -- If this ends up being a bottleneck,
    // refactor the avro `Reader` to separate reading from decoding,
    // so that we can spread the decoding among all the workers.
    // See #2133
    let envelope = envelope.clone();
    stream
        .pass_through("AvroValues")
        .flat_map(move |((value, index), r, d)| {
            let top_node = schema.top_node();
            let diffs = match envelope {
                Envelope::None => {
                    extract_row(value, index.map(Datum::from), top_node).map(|r| DiffPair {
                        before: None,
                        after: r,
                    })
                }
                Envelope::Debezium => extract_debezium_slow(value, top_node),
                Envelope::Upsert(_) => unreachable!("Upsert is not supported for AvroOCF"),
            }
            .unwrap_or_else(|e| {
                // TODO(#489): Handle this in a better way,
                // once runtime error handling exists.
                error!("Failed to extract avro row: {}", e);
                DiffPair {
                    before: None,
                    after: None,
                }
            });

            diffs
                .before
                .into_iter()
                .chain(diffs.after.into_iter())
                .map(move |row| (row, r, d))
        })
}

pub type PushSession<'a, R> =
    Session<'a, Timestamp, R, PushCounter<Timestamp, R, Tee<Timestamp, R>>>;

#[async_trait(?Send)]
pub trait DecoderState {
    /// Reset number of success and failures with decoding
    fn reset_event_count(&mut self);
    async fn decode_key(&mut self, bytes: &[u8]) -> Result<Row, String>;
    /// give a session a key-value pair
    async fn give_key_value<'a>(
        &mut self,
        key: Row,
        bytes: &[u8],
        aux_num: Option<i64>,
        session: &mut PushSession<'a, (Row, Option<Row>, Timestamp)>,
        time: Timestamp,
    );
    /// give a session a plain value
    async fn give_value<'a>(
        &mut self,
        bytes: &[u8],
        aux_num: Option<i64>,
        session: &mut PushSession<'a, (Row, Timestamp, Diff)>,
        time: Timestamp,
    );
    /// Register number of success and failures with decoding
    fn log_error_count(&self);
}

fn pack_with_line_no(datum: Datum, line_no: Option<i64>) -> Row {
    Row::pack(iter::once(datum).chain(line_no.map(Datum::from)))
}

fn bytes_to_datum(bytes: &[u8]) -> Datum {
    Datum::from(bytes)
}

fn text_to_datum(bytes: &[u8]) -> Datum {
    Datum::from(std::str::from_utf8(bytes).ok())
}

struct OffsetDecoderState<F: Fn(&[u8]) -> Datum> {
    datum_func: F,
}

#[async_trait(?Send)]
impl<F> DecoderState for OffsetDecoderState<F>
where
    F: Fn(&[u8]) -> Datum + Send,
{
    fn reset_event_count(&mut self) {}

    async fn decode_key(&mut self, bytes: &[u8]) -> Result<Row, String> {
        let mut result = RowPacker::new();
        result.push((self.datum_func)(bytes));
        Ok(result.finish())
    }

    /// give a session a key-value pair
    async fn give_key_value<'a>(
        &mut self,
        key: Row,
        bytes: &[u8],
        line_no: Option<i64>,
        session: &mut PushSession<'a, (Row, Option<Row>, Timestamp)>,
        time: Timestamp,
    ) {
        session.give((
            key,
            Some(pack_with_line_no((self.datum_func)(bytes), line_no)),
            time,
        ));
    }

    /// give a session a plain value
    async fn give_value<'a>(
        &mut self,
        bytes: &[u8],
        line_no: Option<i64>,
        session: &mut PushSession<'a, (Row, Timestamp, Diff)>,
        time: Timestamp,
    ) {
        session.give((
            pack_with_line_no((self.datum_func)(bytes), line_no),
            time,
            1,
        ));
    }

    /// Register number of success and failures with decoding
    fn log_error_count(&self) {}
}

/// Inner method for decoding an upsert source
/// Mostly, this inner method exists that way static dispatching
/// can be used for different combinations of key-value decoders
/// as opposed to dynamic dispatching
fn decode_upsert_inner<G, K, V>(
    stream: &Stream<G, (Vec<u8>, (Vec<u8>, Option<i64>))>,
    mut key_decoder_state: K,
    mut value_decoder_state: V,
    op_name: &str,
) -> Stream<G, (Row, Option<Row>, Timestamp)>
where
    G: Scope<Timestamp = Timestamp>,
    K: DecoderState + 'static,
    V: DecoderState + 'static,
{
    stream.unary(
        Exchange::new(|x: &(Vec<u8>, (_, _))| (x.0).hashed()),
        &op_name,
        move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for (key, (payload, aux_num)) in data.iter() {
                        if key.is_empty() {
                            error!("{}", "Encountered empty key");
                            continue;
                        }
                        match block_on(key_decoder_state.decode_key(key)) {
                            Ok(key) => {
                                if payload.is_empty() {
                                    session.give((key, None, *cap.time()));
                                } else {
                                    block_on(value_decoder_state.give_key_value(
                                        key,
                                        payload,
                                        *aux_num,
                                        &mut session,
                                        *cap.time(),
                                    ));
                                }
                            }
                            Err(err) => {
                                error!("{}", err);
                            }
                        }
                    }
                });
                key_decoder_state.log_error_count();
                value_decoder_state.log_error_count();
            }
        },
    )
}

pub fn decode_upsert<G>(
    stream: &Stream<G, (Vec<u8>, (Vec<u8>, Option<i64>))>,
    value_encoding: DataEncoding,
    key_encoding: DataEncoding,
) -> Stream<G, (Row, Option<Row>, Timestamp)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!(
        "{}-{}Decode",
        key_encoding.op_name(),
        value_encoding.op_name()
    );
    let decoded_stream = match (key_encoding, value_encoding) {
        (DataEncoding::Bytes, DataEncoding::Avro(val_enc)) => decode_upsert_inner(
            stream,
            OffsetDecoderState {
                datum_func: bytes_to_datum,
            },
            avro::AvroDecoderState::new(
                &val_enc.value_schema,
                val_enc.schema_registry_config,
                interchange::avro::EnvelopeType::Upsert,
                false,
            ),
            &op_name,
        ),
        (DataEncoding::Text, DataEncoding::Avro(val_enc)) => decode_upsert_inner(
            stream,
            OffsetDecoderState {
                datum_func: text_to_datum,
            },
            avro::AvroDecoderState::new(
                &val_enc.value_schema,
                val_enc.schema_registry_config,
                interchange::avro::EnvelopeType::Upsert,
                false,
            ),
            &op_name,
        ),
        (DataEncoding::Avro(key_enc), DataEncoding::Avro(val_enc)) => decode_upsert_inner(
            stream,
            avro::AvroDecoderState::new(
                &key_enc.value_schema,
                key_enc.schema_registry_config,
                interchange::avro::EnvelopeType::None,
                false,
            ),
            avro::AvroDecoderState::new(
                &val_enc.value_schema,
                val_enc.schema_registry_config,
                interchange::avro::EnvelopeType::Upsert,
                false,
            ),
            &op_name,
        ),
        (DataEncoding::Text, DataEncoding::Bytes) => decode_upsert_inner(
            stream,
            OffsetDecoderState {
                datum_func: text_to_datum,
            },
            OffsetDecoderState {
                datum_func: bytes_to_datum,
            },
            &op_name,
        ),
        (DataEncoding::Bytes, DataEncoding::Bytes) => decode_upsert_inner(
            stream,
            OffsetDecoderState {
                datum_func: bytes_to_datum,
            },
            OffsetDecoderState {
                datum_func: bytes_to_datum,
            },
            &op_name,
        ),
        (DataEncoding::Text, DataEncoding::Text) => decode_upsert_inner(
            stream,
            OffsetDecoderState {
                datum_func: text_to_datum,
            },
            OffsetDecoderState {
                datum_func: text_to_datum,
            },
            &op_name,
        ),
        _ => unreachable!(),
    };
    decoded_stream.map(|(key, value, timestamp)| {
        if let Some(value) = value {
            let mut value_with_key = RowPacker::new();
            value_with_key.extend_by_row(&key);
            value_with_key.extend_by_row(&value);
            (key, Some(value_with_key.finish()), timestamp)
        } else {
            (key, None, timestamp)
        }
    })
}

fn decode_values_inner<G, V>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    mut value_decoder_state: V,
    op_name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    V: DecoderState + 'static,
{
    stream.unary(
        Exchange::new(|x: &(Vec<u8>, _)| (x.0.hashed())),
        &op_name,
        move |_, _| {
            move |input, output| {
                value_decoder_state.reset_event_count();
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for (payload, aux_num) in data.iter() {
                        if !payload.is_empty() {
                            block_on(value_decoder_state.give_value(
                                payload,
                                *aux_num,
                                &mut session,
                                *cap.time(),
                            ));
                        }
                    }
                });
                value_decoder_state.log_error_count();
            }
        },
    )
}

pub fn decode_values<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    encoding: DataEncoding,
    name: &str,
    envelope: &Envelope,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
    fast_forwarded: bool,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!("{}Decode", encoding.op_name());

    match (encoding, envelope) {
        (_, Envelope::Upsert(_)) => {
            unreachable!("Internal error: Upsert is not supported yet on non-Kafka sources.")
        }
        (DataEncoding::Csv(enc), Envelope::None) => {
            csv(stream, enc.header_row, enc.n_cols, enc.delimiter, operators)
        }
        (DataEncoding::Avro(enc), envelope) => decode_values_inner(
            stream,
            avro::AvroDecoderState::new(
                &enc.value_schema,
                enc.schema_registry_config,
                envelope.get_avro_envelope_type(),
                fast_forwarded,
            ),
            &op_name,
        ),
        (DataEncoding::AvroOcf { .. }, _) => {
            unreachable!("Internal error: Cannot decode Avro OCF separately from reading")
        }
        (_, Envelope::Debezium) => unreachable!(
            "Internal error: A non-Avro Debezium-envelope source should not have been created."
        ),
        (DataEncoding::Regex { regex }, Envelope::None) => regex_fn(stream, regex, name),
        (DataEncoding::Protobuf(enc), Envelope::None) => decode_values_inner(
            stream,
            protobuf::ProtobufDecoderState::new(&enc.descriptors, &enc.message_name),
            &op_name,
        ),
        (DataEncoding::Bytes, Envelope::None) => decode_values_inner(
            stream,
            OffsetDecoderState {
                datum_func: bytes_to_datum,
            },
            &op_name,
        ),
        (DataEncoding::Text, Envelope::None) => decode_values_inner(
            stream,
            OffsetDecoderState {
                datum_func: text_to_datum,
            },
            &op_name,
        ),
    }
}
