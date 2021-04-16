// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::{any::Any, cell::RefCell, collections::VecDeque, rc::Rc, time::Duration};

use anyhow::anyhow;
use differential_dataflow::capture::YieldingIter;
use differential_dataflow::{AsCollection, Collection};
use expr::SourceInstanceId;
use futures::executor::block_on;
use mz_avro::{AvroDeserializer, GeneralDeserializer};
use prometheus::UIntGauge;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::{map::Map, OkErr, Operator};
use timely::dataflow::{Scope, Stream};
use timely::scheduling::SyncActivator;

use ::mz_avro::{types::Value, Schema};
use dataflow_types::{DataEncoding, DebeziumMode, DecodeError, RegexEncoding, SourceEnvelope};
use dataflow_types::{DataflowError, LinearOperator};
use interchange::avro::{extract_row, ConfluentAvroResolver, DebeziumDecodeState};
use log::error;
use repr::Datum;
use repr::{Diff, Row, Timestamp};

use self::csv::csv;
use self::regex::regex as regex_fn;
use crate::operator::StreamExt;
use crate::source::SourceOutput;

mod avro;
mod csv;
mod metrics;
mod protobuf;
mod regex;

/// Decode for the special case of Avro OCFs
pub fn decode_avro_values<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Value>>,
    envelope: &SourceEnvelope,
    schema: Schema,
    debug_name: &str,
) -> (
    Collection<G, Row, Diff>,
    Option<Collection<G, dataflow_types::DataflowError, Diff>>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // TODO(brennan) -- If this ends up being a bottleneck,
    // refactor the avro `Reader` to separate reading from decoding,
    // so that we can spread the decoding among all the workers.
    // See #2133
    let envelope = envelope.clone();
    let mut dbz_state = match envelope {
        SourceEnvelope::Debezium(dedup_strat, DebeziumMode::Plain) => DebeziumDecodeState::new(
            &schema,
            debug_name.to_string(),
            stream.scope().index(),
            dedup_strat,
        ),
        SourceEnvelope::Debezium(_, DebeziumMode::Upsert) => {
            error!("Upsert doesn't make sense for Avro OCFs because they don't have keys");
            None
        }
        _ => None,
    };

    let stream = stream.pass_through("AvroValues").flat_map(
        move |(
            SourceOutput {
                value,
                position: index,
                upstream_time_millis,
                key: _,
            },
            r,
            d,
        )| {
            let top_node = schema.top_node();
            let maybe_row = match envelope {
                SourceEnvelope::None => extract_row(value, index.map(Datum::from), top_node),
                SourceEnvelope::Debezium(_, _) => {
                    if let Some(dbz_state) = dbz_state.as_mut() {
                        dbz_state.extract(value, upstream_time_millis)
                    } else {
                        Err(anyhow!(
                            "No debezium schema information -- could not decode row"
                        ))
                    }
                }
                SourceEnvelope::Upsert(_) => unreachable!("Upsert is not supported for AvroOCF"),
                SourceEnvelope::CdcV2 => unreachable!("CDC envelope is not supported for AvroOCF"),
            }
            .unwrap_or_else(|e| {
                // TODO(#489): Handle this in a better way,
                // once runtime error handling exists.
                error!("Failed to extract avro row: {}", e);
                None
            });

            maybe_row.into_iter().map(move |row| (row, r, d))
        },
    );

    (stream.as_collection(), None)
}

pub trait DecoderState {
    fn decode_key(&mut self, bytes: &[u8]) -> Result<Option<Row>, String>;
    /// Decode the value in the upsert context, which means it might be a None.
    fn decode_upsert_value(
        &mut self,
        bytes: &[u8],
        aux_num: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Result<Option<Row>, String>;
    /// give a session a plain value
    fn get_value(
        &mut self,
        bytes: &[u8],
        aux_num: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Option<Result<Row, DataflowError>>;

    /// Register number of success and failures with decoding,
    /// and reset count of pending events if necessary
    fn log_error_count(&mut self);
}

fn pack_with_line_no(datum: Datum, line_no: Option<i64>) -> Row {
    if let Some(line_no) = line_no {
        Row::pack_slice(&[datum, Datum::from(line_no)])
    } else {
        Row::pack_slice(&[datum])
    }
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

impl<F: Fn(&[u8]) -> Datum> From<F> for OffsetDecoderState<F> {
    fn from(datum_func: F) -> Self {
        Self { datum_func }
    }
}

impl<F> DecoderState for OffsetDecoderState<F>
where
    F: Fn(&[u8]) -> Datum + Send,
{
    fn decode_key(&mut self, bytes: &[u8]) -> Result<Option<Row>, String> {
        let datum = (self.datum_func)(bytes);
        let row = Row::pack_slice(&[datum]);
        Ok(Some(row))
    }

    fn decode_upsert_value<'a>(
        &mut self,
        bytes: &[u8],
        line_no: Option<i64>,
        _upstream_time_millis: Option<i64>,
    ) -> Result<Option<Row>, String> {
        Ok(Some(pack_with_line_no((self.datum_func)(bytes), line_no)))
    }

    /// give a session a plain value
    fn get_value(
        &mut self,
        bytes: &[u8],
        line_no: Option<i64>,
        _upstream_time_millis: Option<i64>,
    ) -> Option<Result<Row, DataflowError>> {
        Some(Ok(pack_with_line_no((self.datum_func)(bytes), line_no)))
    }

    fn log_error_count(&mut self) {}
}

/// Get the `DecoderState` corresponding to a particular `DataEncoding`.
/// Note that for code simplicity, this uses dynamic dispatch, which is said to
/// be slower than using templates. The use of dynamic dispatch has not been
/// observed to noticeably impact upsert performance at the time of this writing.
/// TODO (wangandi): reimplement in terms of generics. DataEncoding could be a
/// trait that returns a corresponding DecoderState.
pub(crate) fn get_decoder(encoding: DataEncoding, debug_name: &str) -> Box<dyn DecoderState> {
    let avro_err = "Failed to create Avro decoder";
    match encoding {
        DataEncoding::Protobuf(enc) => Box::new(protobuf::ProtobufDecoderState::new(
            &enc.descriptors,
            &enc.message_name,
        )),
        DataEncoding::Avro(enc) => Box::new(
            avro::AvroDecoderState::new(
                enc.key_schema.as_deref(),
                &enc.value_schema,
                enc.schema_registry_config,
                interchange::avro::EnvelopeType::Upsert,
                false,
                format!("{}-values", debug_name),
                enc.confluent_wire_format,
            )
            .expect(avro_err),
        ),
        DataEncoding::Bytes => Box::new(OffsetDecoderState::from(bytes_to_datum)),
        DataEncoding::Text => Box::new(OffsetDecoderState::from(text_to_datum)),
        _ => unreachable!("Unsupported encoding combination"),
    }
}

fn decode_values_inner<G, V, C>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    mut decoder_state: V,
    op_name: &str,
    source_id: SourceInstanceId,
    contract: C,
    upsert_debezium: bool,
) -> (
    Collection<G, Row, Diff>,
    Option<Collection<G, dataflow_types::DataflowError, Diff>>,
)
where
    G: Scope<Timestamp = Timestamp>,
    V: DecoderState + 'static,
    C: ParallelizationContract<Timestamp, SourceOutput<Vec<u8>, Vec<u8>>>,
{
    let stream = stream.unary(contract, &op_name, move |_, _| {
        let mut trackstate = if upsert_debezium {
            Some((
                HashMap::new(),
                metrics::DEBEZIUM_UPSERT_COUNT.with_label_values(&[
                    &source_id.source_id.to_string(),
                    &source_id.dataflow_id.to_string(),
                ]),
            ))
        } else {
            None
        };
        move |input, output| {
            input.for_each(|cap, data| {
                let mut session = output.session(&cap);
                for SourceOutput {
                    key,
                    value: payload,
                    position: aux_num,
                    upstream_time_millis,
                } in data.iter()
                {
                    if !payload.is_empty() {
                        let val =
                            decoder_state.get_value(payload, *aux_num, *upstream_time_millis);

                        if let Some(val) = val {
                            let val = if let Some((keys, metrics)) = trackstate.as_mut() {
                                match decoder_state.decode_key(key) {
                                    Ok(Some(decoded_key)) => {
                                        rewrite_for_upsert(val, keys, decoded_key, metrics)
                                    }
                                    Ok(None) => Err(DecodeError::Text(format!(
                                        "[customer-data] All upsert keys should decode to a value: {:?}",
                                        key
                                    ))
                                    .into()),
                                    Err(e) => {
                                        Err(DecodeError::Text(format!("Error decoding key: {}", e))
                                            .into())
                                    }
                                }
                            } else {
                                val
                            };

                            session.give((val, *cap.time(), 1))
                        }
                    }
                }
            });
            decoder_state.log_error_count();
        }
    });

    let (oks, errs) = stream.ok_err(|(data, time, diff)| match data {
        Ok(data) => Ok((data, time, diff)),
        Err(err) => Err((err, time, diff)),
    });

    (oks.as_collection(), Some(errs.as_collection()))
}

/// Update row to blank out retractions of rows that we have never seen
pub fn rewrite_for_upsert(
    val: Result<Row, DataflowError>,
    keys: &mut HashMap<Row, Row>,
    key: Row,
    metrics: &mut UIntGauge,
) -> Result<Row, DataflowError> {
    if let Ok(row) = val {
        // often off by one, but is only tracked every N seconds so it will always be off
        metrics.set(keys.len() as u64);

        let entry = keys.entry(key.into());

        let mut rowiter = row.iter();
        let before = rowiter.next().expect("must have a before list");
        let after = rowiter.next().expect("must have an after list");

        assert!(
            matches!(before, Datum::List { .. } | Datum::Null),
            "[customer-data] Debezium logic should be a List or absent, got {:?}",
            before
        );

        match entry {
            Entry::Vacant(vacant) => {
                // if the key is new, then we know that we always need to ignore the "before" part,
                // so zero it out
                vacant.insert(Row::pack_slice(&[after]));

                if before.is_null() {
                    Ok(row)
                } else {
                    Ok(Row::pack_slice(&[Datum::Null, after]))
                }
            }
            Entry::Occupied(mut occupied) => {
                if occupied.get().iter().next() == Some(before) {
                    if after.is_null() {
                        occupied.remove_entry();
                    } else {
                        occupied.insert(Row::pack_slice(&[after]));
                    }
                    // this matches the modifications we'd make in the next step
                    Ok(row)
                } else {
                    let previous_insert = if after.is_null() {
                        // We are trying to retract something that doesn't exist, so just assume
                        // that the key is supposed to be empty at this point
                        let (_k, v) = occupied.remove_entry();
                        v
                    } else {
                        occupied.insert(Row::pack_slice(&[after]))
                    };

                    Ok(Row::pack_slice(&[previous_insert.unpack_first(), after]))
                }
            }
        }
    } else {
        val
    }
}

fn decode_cdcv2<G: Scope<Timestamp = Timestamp>>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    schema: &str,
    registry: Option<ccsr::ClientConfig>,
    confluent_wire_format: bool,
) -> (
    (
        Collection<G, Row, Diff>,
        Option<Collection<G, dataflow_types::DataflowError, Diff>>,
    ),
    Option<Box<dyn Any>>,
) {
    // We will have already checked validity of the schema by now, so this can't fail.
    let mut resolver = ConfluentAvroResolver::new(schema, registry, confluent_wire_format).unwrap();
    let channel = Rc::new(RefCell::new(VecDeque::new()));
    let activator: Rc<RefCell<Option<SyncActivator>>> = Rc::new(RefCell::new(None));
    let mut vector = Vec::new();
    stream.sink(
        SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract(),
        "CDCv2-Decode",
        {
            let channel = channel.clone();
            let activator = activator.clone();
            move |input| {
                input.for_each(|_time, data| {
                    data.swap(&mut vector);
                    for data in vector.drain(..) {
                        let (mut data, schema) = match block_on(resolver.resolve(&data.value)) {
                            Ok(ok) => ok,
                            Err(e) => {
                                error!("Failed to get schema info for CDCv2 record: {}", e);
                                continue;
                            }
                        };
                        let d = GeneralDeserializer {
                            schema: schema.top_node(),
                        };
                        let dec = interchange::avro::cdc_v2::Decoder;
                        let message = match d.deserialize(&mut data, dec) {
                            Ok(ok) => ok,
                            Err(e) => {
                                error!("Failed to deserialize avro message: {}", e);
                                continue;
                            }
                        };
                        channel.borrow_mut().push_back(message);
                    }
                });
                if let Some(activator) = activator.borrow_mut().as_mut() {
                    activator.activate().unwrap()
                }
            }
        },
    );
    struct VdIterator<T>(Rc<RefCell<VecDeque<T>>>);
    impl<T> Iterator for VdIterator<T> {
        type Item = T;
        fn next(&mut self) -> Option<T> {
            self.0.borrow_mut().pop_front()
        }
    }
    let (token, stream) =
        differential_dataflow::capture::source::build(stream.scope(), move |ac| {
            *activator.borrow_mut() = Some(ac);
            YieldingIter::new_from(VdIterator(channel), Duration::from_millis(10))
        });
    ((stream.as_collection(), None), Some(token))
}

/// Decode a stream of values from a stream of bytes.
/// Returns the corresponding stream of Row/Timestamp/Diff tuples,
/// and, optionally, a token that can be dropped to stop the decoding operator
/// (if it isn't automatically stopped by the upstream operator stopping)
pub fn decode_values<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    encoding: DataEncoding,
    debug_name: &str,
    envelope: &SourceEnvelope,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
    fast_forwarded: bool,
    source_id: SourceInstanceId,
) -> (
    (
        Collection<G, Row, Diff>,
        Option<Collection<G, dataflow_types::DataflowError, Diff>>,
    ),
    Option<Box<dyn Any>>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!("{}Decode", encoding.op_name());
    match (encoding, envelope) {
        (_, SourceEnvelope::Upsert(_)) => {
            unreachable!("Internal error: Upsert is not supported yet on non-Kafka sources.")
        }
        (DataEncoding::Csv(enc), SourceEnvelope::None) => (
            csv(stream, enc.header_row, enc.n_cols, enc.delimiter, operators),
            None,
        ),
        (DataEncoding::Avro(enc), SourceEnvelope::CdcV2) => decode_cdcv2(
            stream,
            &enc.value_schema,
            enc.schema_registry_config,
            enc.confluent_wire_format,
        ),
        (_, SourceEnvelope::CdcV2) => {
            unreachable!("Internal error: CDCv2 is not supported yet on non-Avro sources.")
        }
        (DataEncoding::Avro(enc), SourceEnvelope::Debezium(_ds, mode)) => (
            decode_values_inner(
                stream,
                avro::AvroDecoderState::new(
                    enc.key_schema.as_deref(),
                    &enc.value_schema,
                    enc.schema_registry_config,
                    envelope.get_avro_envelope_type(),
                    fast_forwarded && !matches!(mode, DebeziumMode::Upsert), // `start_offset` should work fine for `DEBEZIUM UPSERT`.
                    debug_name.to_string(),
                    enc.confluent_wire_format,
                )
                .expect("Failed to create Avro decoder"),
                &op_name,
                source_id,
                SourceOutput::<Vec<u8>, Vec<u8>>::key_contract(),
                *mode == DebeziumMode::Upsert,
            ),
            None,
        ),
        (DataEncoding::Avro(enc), envelope) => (
            decode_values_inner(
                stream,
                avro::AvroDecoderState::new(
                    enc.key_schema.as_deref(),
                    &enc.value_schema,
                    enc.schema_registry_config,
                    envelope.get_avro_envelope_type(),
                    fast_forwarded,
                    debug_name.to_string(),
                    enc.confluent_wire_format,
                )
                .expect("Failed to create Avro decoder"),
                &op_name,
                source_id,
                SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract(),
                false,
            ),
            None,
        ),
        (DataEncoding::AvroOcf { .. }, _) => {
            unreachable!("Internal error: Cannot decode Avro OCF separately from reading")
        }
        (_, SourceEnvelope::Debezium(_, _)) => unreachable!(
            "Internal error: A non-Avro Debezium-envelope source should not have been created."
        ),
        (DataEncoding::Regex(RegexEncoding { regex }), SourceEnvelope::None) => {
            (regex_fn(stream, regex, debug_name), None)
        }
        (DataEncoding::Protobuf(enc), SourceEnvelope::None) => (
            decode_values_inner(
                stream,
                protobuf::ProtobufDecoderState::new(&enc.descriptors, &enc.message_name),
                &op_name,
                source_id,
                SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract(),
                false,
            ),
            None,
        ),
        (DataEncoding::Bytes, SourceEnvelope::None) => (
            decode_values_inner(
                stream,
                OffsetDecoderState::from(bytes_to_datum),
                &op_name,
                source_id,
                SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract(),
                false,
            ),
            None,
        ),
        (DataEncoding::Text, SourceEnvelope::None) => (
            decode_values_inner(
                stream,
                OffsetDecoderState::from(text_to_datum),
                &op_name,
                source_id,
                SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract(),
                false,
            ),
            None,
        ),
        (DataEncoding::Postgres(_), _) => {
            unreachable!("Internal error: postgres sources are never decoded");
        }
    }
}
