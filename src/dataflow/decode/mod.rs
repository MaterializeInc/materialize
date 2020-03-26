// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use lazy_static::lazy_static;

use differential_dataflow::hashable::Hashable;
use prometheus::{register_int_counter_vec, IntCounterVec};
use prometheus_static_metric::make_static_metric;
use timely::dataflow::{
    channels::pact::{Exchange, ParallelizationContract, Pipeline},
    channels::pushers::buffer::Session,
    channels::pushers::Counter as PushCounter,
    channels::pushers::Tee,
    operators::{map::Map, Operator},
    Scope, Stream,
};

use dataflow_types::{DataEncoding, Diff, Envelope, Timestamp};
use repr::Datum;
use repr::{Row, RowPacker};

mod avro;
mod csv;
mod protobuf;
mod regex;

use self::csv::csv;
use self::regex::regex as regex_fn;
use ::avro::types::Value;
use interchange::avro::{extract_debezium_slow, extract_row, DiffPair};

use log::error;
use std::iter;

make_static_metric! {
    pub struct EventsRead: IntCounter {
        "format" => { avro, csv, protobuf },
        "status" => { success, error }
    }
}

lazy_static! {
    static ref EVENTS_COUNTER_INTERNAL: IntCounterVec = register_int_counter_vec!(
        "mz_dataflow_events_read_total",
        "Count of events we have read from the wire",
        &["format", "status"]
    )
    .unwrap();
    static ref EVENTS_COUNTER: EventsRead = EventsRead::from(&EVENTS_COUNTER_INTERNAL);
}

/// Take a Timely stream and convert it to a Differential stream, where each diff is "1"
/// and each time is the current Timely timestamp.
fn pass_through<G, Data, P>(
    stream: &Stream<G, Data>,
    name: &str,
    pact: P,
) -> Stream<G, (Data, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    Data: timely::Data,
    P: ParallelizationContract<Timestamp, Data>,
{
    stream.unary(pact, name, move |_, _| {
        move |input, output| {
            input.for_each(|cap, data| {
                let mut v = Vec::new();
                data.swap(&mut v);
                let mut session = output.session(&cap);
                session.give_iterator(v.into_iter().map(|payload| (payload, *cap.time(), 1)));
            });
        }
    })
}

pub fn decode_avro_values<G>(
    stream: &Stream<G, (Value, Option<i64>)>,
    envelope: &Envelope,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    // TODO(brennan) -- If this ends up being a bottleneck,
    // refactor the avro `Reader` to separate reading from decoding,
    // so that we can spread the decoding among all the workers.
    // See #2133
    let envelope = envelope.clone();
    pass_through(stream, "AvroValues", Pipeline).flat_map(move |((value, index), r, d)| {
        let diffs = match envelope {
            Envelope::None => extract_row(value, false, index.map(Datum::from)).map(|r| DiffPair {
                before: None,
                after: r,
            }),
            Envelope::Debezium => extract_debezium_slow(value),
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

pub type PushSession<'a, R> = Session<
    'a,
    Timestamp,
    (R, Timestamp, Diff),
    PushCounter<Timestamp, (R, Timestamp, Diff), Tee<Timestamp, (R, Timestamp, Diff)>>,
>;

pub trait DecoderState {
    /// Reset number of success and failures with decoding
    fn reset_event_count(&mut self);
    fn decode_key(&mut self, bytes: &[u8]) -> Result<Row, String>;
    /// give a session a key-value pair
    fn give_key_value<'a>(
        &mut self,
        key: Row,
        bytes: &[u8],
        aux_num: &Option<i64>,
        session: &mut PushSession<'a, (Row, Option<Row>)>,
        time: Timestamp,
    );
    /// give a session a plain value
    fn give_value<'a>(
        &mut self,
        bytes: &[u8],
        aux_num: &Option<i64>,
        session: &mut PushSession<'a, Row>,
        time: Timestamp,
    );
    /// Register number of success and failures with decoding
    fn log_error_count(&self);
}

fn pack_with_line_no(datum: Datum, line_no: &Option<i64>) -> Row {
    Row::pack(iter::once(datum).chain(line_no.map(Datum::from)))
}

pub struct BytesDecoderState;

impl DecoderState for BytesDecoderState {
    fn reset_event_count(&mut self) {}

    fn decode_key(&mut self, bytes: &[u8]) -> Result<Row, String> {
        let mut result = RowPacker::new();
        result.push(Datum::from(bytes));
        Ok(result.finish())
    }

    /// give a session a key-value pair
    fn give_key_value<'a>(
        &mut self,
        key: Row,
        bytes: &[u8],
        line_no: &Option<i64>,
        session: &mut PushSession<'a, (Row, Option<Row>)>,
        time: Timestamp,
    ) {
        session.give((
            (key, Some(pack_with_line_no(Datum::from(bytes), line_no))),
            time,
            1,
        ));
    }

    /// give a session a plain value
    fn give_value<'a>(
        &mut self,
        bytes: &[u8],
        line_no: &Option<i64>,
        session: &mut PushSession<'a, Row>,
        time: Timestamp,
    ) {
        session.give((pack_with_line_no(Datum::from(bytes), line_no), time, 1));
    }

    /// Register number of success and failures with decoding
    fn log_error_count(&self) {}
}

pub struct TextDecoderState;

impl DecoderState for TextDecoderState {
    fn reset_event_count(&mut self) {}

    fn decode_key(&mut self, bytes: &[u8]) -> Result<Row, String> {
        let mut result = RowPacker::new();
        result.push(Datum::from(std::str::from_utf8(&bytes).ok()));
        Ok(result.finish())
    }

    /// give a session a key-value pair
    fn give_key_value<'a>(
        &mut self,
        key: Row,
        bytes: &[u8],
        line_no: &Option<i64>,
        session: &mut PushSession<'a, (Row, Option<Row>)>,
        time: Timestamp,
    ) {
        session.give((
            (
                key,
                Some(pack_with_line_no(
                    Datum::from(std::str::from_utf8(bytes).ok()),
                    line_no,
                )),
            ),
            time,
            1,
        ));
    }

    /// give a session a plain value
    fn give_value<'a>(
        &mut self,
        bytes: &[u8],
        line_no: &Option<i64>,
        session: &mut PushSession<'a, Row>,
        time: Timestamp,
    ) {
        session.give((
            pack_with_line_no(Datum::from(std::str::from_utf8(bytes).ok()), line_no),
            time,
            1,
        ));
    }

    /// Register number of success and failures with decoding
    fn log_error_count(&self) {}
}

fn decode_kafka_upsert_inner<G, K: 'static, V: 'static>(
    stream: &Stream<G, ((Vec<u8>, Vec<u8>), Option<i64>)>,
    mut key_decoder_state: K,
    mut value_decoder_state: V,
    op_name: &str,
) -> Stream<G, ((Row, Option<Row>), Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    K: DecoderState,
    V: DecoderState,
{
    stream.unary(
        Exchange::new(|x: &((Vec<u8>, _), _)| (x.0).0.hashed()),
        &op_name,
        move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for ((key, payload), aux_num) in data.iter() {
                        match key_decoder_state.decode_key(key) {
                            Ok(key) => {
                                value_decoder_state.give_key_value(
                                    key,
                                    payload,
                                    aux_num,
                                    &mut session,
                                    *cap.time(),
                                );
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

pub fn decode_kafka_upsert<G>(
    stream: &Stream<G, ((Vec<u8>, Vec<u8>), Option<i64>)>,
    value_encoding: DataEncoding,
    key_encoding: DataEncoding,
) -> Stream<G, ((Row, Option<Row>), Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!(
        "{}-{}Decode",
        key_encoding.op_name(),
        value_encoding.op_name()
    );
    match (key_encoding, value_encoding) {
        (DataEncoding::Text, DataEncoding::Avro(val_enc)) => decode_kafka_upsert_inner(
            stream,
            TextDecoderState,
            avro::AvroDecoderState::new(
                &val_enc.value_schema,
                val_enc.schema_registry_url,
                interchange::avro::EnvelopeType::Upsert,
            ),
            &op_name,
        ),
        (DataEncoding::Avro(key_enc), DataEncoding::Avro(val_enc)) => decode_kafka_upsert_inner(
            stream,
            avro::AvroDecoderState::new(
                &key_enc.value_schema,
                key_enc.schema_registry_url,
                interchange::avro::EnvelopeType::None,
            ),
            avro::AvroDecoderState::new(
                &val_enc.value_schema,
                val_enc.schema_registry_url,
                interchange::avro::EnvelopeType::Upsert,
            ),
            &op_name,
        ),
        _ => unreachable!(),
    }
}

fn decode_kafka_inner<G, V: 'static>(
    stream: &Stream<G, ((Vec<u8>, Vec<u8>), Option<i64>)>,
    mut value_decoder_state: V,
    op_name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    V: DecoderState,
{
    stream.unary(
        Exchange::new(|x: &((_, Vec<u8>), _)| (x.0).1.hashed()),
        &op_name,
        move |_, _| {
            move |input, output| {
                value_decoder_state.reset_event_count();
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for ((_, payload), aux_num) in data.iter() {
                        value_decoder_state.give_value(payload, aux_num, &mut session, *cap.time());
                    }
                });
                value_decoder_state.log_error_count();
            }
        },
    )
}

pub fn decode_kafka<G>(
    stream: &Stream<G, ((Vec<u8>, Vec<u8>), Option<i64>)>,
    encoding: DataEncoding,
    envelope: &Envelope,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!("{}Decode", encoding.op_name());
    match encoding {
        DataEncoding::Avro(enc) => decode_kafka_inner(
            stream,
            avro::AvroDecoderState::new(
                &enc.value_schema,
                enc.schema_registry_url,
                envelope.get_avro_envelope_type(),
            ),
            &op_name,
        ),
        DataEncoding::Protobuf(enc) => decode_kafka_inner(
            stream,
            protobuf::ProtobufDecoderState::new(&enc.descriptors, &enc.message_name),
            &op_name,
        ),
        DataEncoding::Bytes => decode_kafka_inner(stream, BytesDecoderState, &op_name),
        _ => unreachable!(),
    }
}

fn decode_inner<G, V: 'static>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    mut value_decoder_state: V,
    op_name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    V: DecoderState,
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
                        value_decoder_state.give_value(payload, aux_num, &mut session, *cap.time());
                    }
                });
                value_decoder_state.log_error_count();
            }
        },
    )
}

pub fn decode<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    encoding: DataEncoding,
    name: &str,
    envelope: &Envelope,
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
            csv(stream, enc.header_row, enc.n_cols, enc.delimiter)
        }
        (DataEncoding::Avro(enc), envelope) => decode_inner(
            stream,
            avro::AvroDecoderState::new(
                &enc.value_schema,
                enc.schema_registry_url,
                envelope.get_avro_envelope_type(),
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
        (DataEncoding::Protobuf(enc), Envelope::None) => decode_inner(
            stream,
            protobuf::ProtobufDecoderState::new(&enc.descriptors, &enc.message_name),
            &op_name,
        ),
        (DataEncoding::Bytes, Envelope::None) => decode_inner(stream, BytesDecoderState, &op_name),
        (DataEncoding::Text, Envelope::None) => decode_inner(stream, TextDecoderState, &op_name),
    }
}
