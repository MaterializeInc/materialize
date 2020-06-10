// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;

use anyhow::anyhow;
use async_trait::async_trait;
use differential_dataflow::hashable::Hashable;
use futures::executor::block_on;
use timely::dataflow::{
    channels::pact::{Exchange, ParallelizationContract},
    channels::pushers::buffer::Session,
    channels::pushers::Counter as PushCounter,
    channels::pushers::Tee,
    operators::{map::Map, Operator},
    Scope, Stream,
};

use ::avro::{types::Value, Schema};
use dataflow_types::LinearOperator;
use dataflow_types::{DataEncoding, Envelope, Timestamp};
use expr::Diff;
use interchange::avro::{extract_row, DebeziumDecodeState, DiffPair};
use log::error;
use repr::Datum;
use repr::{Row, RowPacker};

use self::csv::csv;
use self::regex::regex as regex_fn;
use crate::{operator::StreamExt, source::SourceOutput};

mod avro;
mod csv;
mod protobuf;
mod regex;

pub fn decode_avro_values<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Value>>,
    envelope: &Envelope,
    schema: Schema,
    debug_name: &str,
) -> Stream<G, ((Row, Option<i64>), Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    // TODO(brennan) -- If this ends up being a bottleneck,
    // refactor the avro `Reader` to separate reading from decoding,
    // so that we can spread the decoding among all the workers.
    // See #2133
    let envelope = envelope.clone();
    let mut dbz_state = if let Envelope::Debezium(dedup_strat) = envelope {
        DebeziumDecodeState::new(
            &schema,
            debug_name.to_string(),
            stream.scope().index(),
            dedup_strat,
        )
    } else {
        None
    };
    stream.pass_through("AvroValues").flat_map(
        move |(
            SourceOutput {
                value,
                position: index,
                key: _,
            },
            r,
            d,
        )| {
            let top_node = schema.top_node();
            let diffs = match envelope {
                Envelope::None => {
                    extract_row(value, index.map(Datum::from), top_node).map(|r| DiffPair {
                        before: None,
                        after: r,
                    })
                }
                Envelope::Debezium(_) => {
                    if let Some(dbz_state) = dbz_state.as_mut() {
                        dbz_state.extract(value, top_node, index)
                    } else {
                        Err(anyhow!(
                            "No debezium schema information -- could not decode row"
                        ))
                    }
                }
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
                .map(move |row| ((row, None), r, d))
        },
    )
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
        session: &mut PushSession<'a, ((Row, Option<i64>), Timestamp, Diff)>,
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
    row_packer: RowPacker,
}

impl<F: Fn(&[u8]) -> Datum> From<F> for OffsetDecoderState<F> {
    fn from(datum_func: F) -> Self {
        Self {
            datum_func,
            row_packer: RowPacker::new(),
        }
    }
}

#[async_trait(?Send)]
impl<F> DecoderState for OffsetDecoderState<F>
where
    F: Fn(&[u8]) -> Datum + Send,
{
    fn reset_event_count(&mut self) {}

    async fn decode_key(&mut self, bytes: &[u8]) -> Result<Row, String> {
        Ok(self.row_packer.pack(&[(self.datum_func)(bytes)]))
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
        session: &mut PushSession<'a, ((Row, Option<i64>), Timestamp, Diff)>,
        time: Timestamp,
    ) {
        session.give((
            (pack_with_line_no((self.datum_func)(bytes), line_no), None),
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
    stream: &Stream<G, ((Vec<u8>, (Vec<u8>, Option<i64>)), Timestamp)>,
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
        Exchange::new(|x: &((Vec<u8>, (_, _)), _)| (x.0).hashed()),
        &op_name,
        move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for ((key, (payload, aux_num)), time) in data.iter() {
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
                                        *time,
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
    stream: &Stream<G, ((Vec<u8>, (Vec<u8>, Option<i64>)), Timestamp)>,
    value_encoding: DataEncoding,
    key_encoding: DataEncoding,
    debug_name: &str,
    worker_index: usize,
) -> Stream<G, (Row, Option<Row>, Timestamp)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!(
        "{}-{}Decode",
        key_encoding.op_name(),
        value_encoding.op_name()
    );
    let avro_err = "Failed to create Avro decoder";
    match (key_encoding, value_encoding) {
        (DataEncoding::Bytes, DataEncoding::Avro(val_enc)) => decode_upsert_inner(
            stream,
            OffsetDecoderState::from(bytes_to_datum),
            avro::AvroDecoderState::new(
                &val_enc.value_schema,
                val_enc.schema_registry_config,
                interchange::avro::EnvelopeType::Upsert,
                false,
                format!("{}-values", debug_name),
                worker_index,
                None,
            )
            .expect(avro_err),
            &op_name,
        ),
        (DataEncoding::Text, DataEncoding::Avro(val_enc)) => decode_upsert_inner(
            stream,
            OffsetDecoderState::from(text_to_datum),
            avro::AvroDecoderState::new(
                &val_enc.value_schema,
                val_enc.schema_registry_config,
                interchange::avro::EnvelopeType::Upsert,
                false,
                format!("{}-values", debug_name),
                worker_index,
                None,
            )
            .expect(avro_err),
            &op_name,
        ),
        (DataEncoding::Avro(key_enc), DataEncoding::Avro(val_enc)) => decode_upsert_inner(
            stream,
            avro::AvroDecoderState::new(
                &key_enc.value_schema,
                key_enc.schema_registry_config,
                interchange::avro::EnvelopeType::None,
                false,
                format!("{}-keys", debug_name),
                worker_index,
                None,
            )
            .expect(avro_err),
            avro::AvroDecoderState::new(
                &val_enc.value_schema,
                val_enc.schema_registry_config,
                interchange::avro::EnvelopeType::Upsert,
                false,
                format!("{}-values", debug_name),
                worker_index,
                None,
            )
            .expect(avro_err),
            &op_name,
        ),
        (DataEncoding::Text, DataEncoding::Bytes) => decode_upsert_inner(
            stream,
            OffsetDecoderState::from(text_to_datum),
            OffsetDecoderState::from(bytes_to_datum),
            &op_name,
        ),
        (DataEncoding::Bytes, DataEncoding::Text) => decode_upsert_inner(
            stream,
            OffsetDecoderState::from(bytes_to_datum),
            OffsetDecoderState::from(text_to_datum),
            &op_name,
        ),
        (DataEncoding::Bytes, DataEncoding::Bytes) => decode_upsert_inner(
            stream,
            OffsetDecoderState::from(bytes_to_datum),
            OffsetDecoderState::from(bytes_to_datum),
            &op_name,
        ),
        (DataEncoding::Text, DataEncoding::Text) => decode_upsert_inner(
            stream,
            OffsetDecoderState::from(text_to_datum),
            OffsetDecoderState::from(text_to_datum),
            &op_name,
        ),
        _ => unreachable!("Unsupported encoding combination"),
    }
}

fn decode_values_inner<G, V, C>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    mut value_decoder_state: V,
    op_name: &str,
    contract: C,
) -> Stream<G, ((Row, Option<i64>), Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    V: DecoderState + 'static,
    C: ParallelizationContract<Timestamp, SourceOutput<Vec<u8>, Vec<u8>>>,
{
    stream.unary(contract, &op_name, move |_, _| {
        move |input, output| {
            value_decoder_state.reset_event_count();
            input.for_each(|cap, data| {
                let mut session = output.session(&cap);
                for SourceOutput {
                    key: _,
                    value: payload,
                    position: aux_num,
                } in data.iter()
                {
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
    })
}

pub fn decode_values<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    encoding: DataEncoding,
    debug_name: &str,
    envelope: &Envelope,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
    fast_forwarded: bool,
) -> Stream<G, ((Row, Option<i64>), Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!("{}Decode", encoding.op_name());
    let worker_index = stream.scope().index();
    match (encoding, envelope) {
        (_, Envelope::Upsert(_)) => {
            unreachable!("Internal error: Upsert is not supported yet on non-Kafka sources.")
        }
        (DataEncoding::Csv(enc), Envelope::None) => {
            csv(stream, enc.header_row, enc.n_cols, enc.delimiter, operators)
        }
        (DataEncoding::Avro(enc), Envelope::Debezium(_)) => {
            // can't get this from the above match arm because:
            // `error[E0658]: binding by-move and by-ref in the same pattern is unstable`
            let dedup_strat = match envelope {
                Envelope::Debezium(ds) => *ds,
                _ => unreachable!(),
            };
            decode_values_inner(
                stream,
                avro::AvroDecoderState::new(
                    &enc.value_schema,
                    enc.schema_registry_config,
                    envelope.get_avro_envelope_type(),
                    fast_forwarded,
                    debug_name.to_string(),
                    worker_index,
                    Some(dedup_strat),
                )
                .expect("Failed to create Avro decoder"),
                &op_name,
                SourceOutput::<Vec<u8>, Vec<u8>>::key_contract(),
            )
        }
        (DataEncoding::Avro(enc), envelope) => decode_values_inner(
            stream,
            avro::AvroDecoderState::new(
                &enc.value_schema,
                enc.schema_registry_config,
                envelope.get_avro_envelope_type(),
                fast_forwarded,
                debug_name.to_string(),
                worker_index,
                None,
            )
            .expect("Failed to create Avro decoder"),
            &op_name,
            SourceOutput::<Vec<u8>, Vec<u8>>::value_contract(),
        ),
        (DataEncoding::AvroOcf { .. }, _) => {
            unreachable!("Internal error: Cannot decode Avro OCF separately from reading")
        }
        (_, Envelope::Debezium(_)) => unreachable!(
            "Internal error: A non-Avro Debezium-envelope source should not have been created."
        ),
        (DataEncoding::Regex { regex }, Envelope::None) => regex_fn(stream, regex, debug_name),
        (DataEncoding::Protobuf(enc), Envelope::None) => decode_values_inner(
            stream,
            protobuf::ProtobufDecoderState::new(&enc.descriptors, &enc.message_name),
            &op_name,
            SourceOutput::<Vec<u8>, Vec<u8>>::value_contract(),
        ),
        (DataEncoding::Bytes, Envelope::None) => decode_values_inner(
            stream,
            OffsetDecoderState::from(bytes_to_datum),
            &op_name,
            SourceOutput::<Vec<u8>, Vec<u8>>::value_contract(),
        ),
        (DataEncoding::Text, Envelope::None) => decode_values_inner(
            stream,
            OffsetDecoderState::from(text_to_datum),
            &op_name,
            SourceOutput::<Vec<u8>, Vec<u8>>::value_contract(),
        ),
    }
}
