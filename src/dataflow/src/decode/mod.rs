// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter;
use std::{any::Any, cell::RefCell, collections::VecDeque, rc::Rc, time::Duration};

use ::regex::Regex;
use dataflow_types::AvroEncoding;
use dataflow_types::AvroOcfEncoding;
use dataflow_types::ProtobufEncoding;
use differential_dataflow::capture::YieldingIter;
use differential_dataflow::Hashable;
use differential_dataflow::{AsCollection, Collection};
use futures::executor::block_on;
use mz_avro::{AvroDeserializer, GeneralDeserializer};
use prometheus::UIntGauge;
use repr::MessagePayload;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::SyncActivator;

use dataflow_types::{DataEncoding, DebeziumMode, DecodeError, RegexEncoding, SourceEnvelope};
use dataflow_types::{DataflowError, LinearOperator};
use interchange::avro::ConfluentAvroResolver;
use log::error;
use repr::Datum;
use repr::{Diff, Row, Timestamp};

use self::avro::AvroDecoderState;
use self::csv::CsvDecoderState;
use self::protobuf::ProtobufDecoderState;
use crate::metrics::Metrics;
use crate::operator::StreamExt;
use crate::source::DecodeResult;
use crate::source::SourceOutput;

mod avro;
mod csv;
mod protobuf;

/// Update row to blank out retractions of rows that we have never seen
pub fn rewrite_for_upsert(
    val: Result<Row, DataflowError>,
    keys: &mut HashMap<Row, Row>,
    key: Row,
    metrics: &UIntGauge,
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

pub fn decode_cdcv2<G: Scope<Timestamp = Timestamp>>(
    stream: &Stream<G, SourceOutput<Vec<u8>, MessagePayload>>,
    schema: &str,
    registry: Option<ccsr::ClientConfig>,
    confluent_wire_format: bool,
) -> (Collection<G, Row, Diff>, Box<dyn Any>) {
    // We will have already checked validity of the schema by now, so this can't fail.
    let mut resolver = ConfluentAvroResolver::new(schema, registry, confluent_wire_format).unwrap();
    let channel = Rc::new(RefCell::new(VecDeque::new()));
    let activator: Rc<RefCell<Option<SyncActivator>>> = Rc::new(RefCell::new(None));
    let mut vector = Vec::new();
    stream.sink(
        SourceOutput::<Vec<u8>, MessagePayload>::position_value_contract(),
        "CDCv2-Decode",
        {
            let channel = channel.clone();
            let activator = activator.clone();
            move |input| {
                input.for_each(|_time, data| {
                    data.swap(&mut vector);
                    for data in vector.drain(..) {
                        let value = match &data.value {
                            MessagePayload::Data(value) => value,
                            MessagePayload::EOF => continue,
                        };
                        let (mut data, schema) = match block_on(resolver.resolve(&*value)) {
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
    (stream.as_collection(), token)
}

// These don't know how to find delimiters --
// they just go from sequences of vectors of bytes (for which we already know the delimiters)
// to rows, and can eventually just be planned as `HirRelationExpr::Map`. (TODO)
#[derive(Debug)]
pub(crate) enum PreDelimitedFormat {
    Bytes,
    Text,
    Regex(Regex, Row),
    Protobuf(ProtobufDecoderState),
}

impl PreDelimitedFormat {
    pub fn decode(
        &mut self,
        bytes: &[u8],
        upstream_coord: Option<i64>,
        push_metadata: bool,
    ) -> Result<Option<Row>, DataflowError> {
        match self {
            PreDelimitedFormat::Bytes => Ok(Some(if push_metadata {
                Row::pack(
                    iter::once(Datum::Bytes(bytes)).chain(iter::once(Datum::from(upstream_coord))),
                )
            } else {
                Row::pack(Some(Datum::Bytes(bytes)))
            })),
            PreDelimitedFormat::Text => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeError::Text("Failed to decode UTF-8".to_string()))?;
                Ok(Some(if push_metadata {
                    Row::pack(
                        iter::once(Datum::String(s)).chain(iter::once(Datum::from(upstream_coord))),
                    )
                } else {
                    Row::pack(Some(Datum::String(s)))
                }))
            }
            PreDelimitedFormat::Regex(regex, row_packer) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeError::Text("Failed to decode UTF-8".to_string()))?;
                let captures = match regex.captures(s) {
                    Some(captures) => captures,
                    None => return Ok(None),
                };
                row_packer.extend(
                    captures
                        .iter()
                        .skip(1)
                        .map(|c| Datum::from(c.map(|c| c.as_str()))),
                );
                if push_metadata {
                    row_packer.push(Datum::from(upstream_coord));
                }
                Ok(Some(row_packer.finish_and_reuse()))
            }
            PreDelimitedFormat::Protobuf(pb) => pb
                .get_value(bytes, upstream_coord, push_metadata)
                .transpose(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum DataDecoderInner {
    Avro(AvroDecoderState),
    DelimitedBytes {
        delimiter: u8,
        format: PreDelimitedFormat,
    },
    Csv(CsvDecoderState),

    PreDelimited(PreDelimitedFormat),
}

#[derive(Debug)]
struct DataDecoder {
    inner: DataDecoderInner,
    metrics: Metrics,
}

impl DataDecoder {
    pub async fn next(
        &mut self,
        bytes: &mut &[u8],
        upstream_coord: Option<i64>,
        upstream_time_millis: Option<i64>,
        push_metadata: bool,
    ) -> Result<Option<Row>, DataflowError> {
        match &mut self.inner {
            DataDecoderInner::DelimitedBytes { delimiter, format } => {
                let delimiter = *delimiter;
                let chunk_idx = match bytes.iter().position(move |&byte| byte == delimiter) {
                    None => return Ok(None),
                    Some(i) => i,
                };
                let data = &bytes[0..chunk_idx];
                *bytes = &bytes[chunk_idx + 1..];
                format.decode(data, upstream_coord, push_metadata)
            }
            DataDecoderInner::Avro(avro) => {
                avro.decode(bytes, upstream_coord, upstream_time_millis, push_metadata)
                    .await
            }
            DataDecoderInner::Csv(csv) => csv.decode(bytes, upstream_coord, push_metadata),
            DataDecoderInner::PreDelimited(format) => {
                let result = format.decode(*bytes, upstream_coord, push_metadata);
                *bytes = &[];
                result
            }
        }
    }

    /// Get the next record if it exists, assuming an EOF has occurred.
    ///
    /// This is distinct from `next` because, for example, a CSV record should be returned even if it
    /// does not end in a newline.
    pub fn eof(
        &mut self,
        bytes: &mut &[u8],
        upstream_coord: Option<i64>,
        push_metadata: bool,
    ) -> Result<Option<Row>, DataflowError> {
        match &mut self.inner {
            DataDecoderInner::Csv(csv) => {
                let result = csv.decode(bytes, upstream_coord, push_metadata);
                csv.reset_for_new_object();
                result
            }
            DataDecoderInner::DelimitedBytes { format, .. } => {
                let data = std::mem::take(bytes);
                // If we hit EOF with no bytes left in the buffer it means the file had a trailing
                // \n character that can be ignored. Otherwise, we decode the final bytes as normal
                if data.is_empty() {
                    Ok(None)
                } else {
                    format.decode(data, upstream_coord, push_metadata)
                }
            }
            _ => Ok(None),
        }
    }

    pub fn log_errors(&self, n: usize) {
        self.metrics.count_errors(&self.inner, n);
    }

    pub fn log_successes(&self, n: usize) {
        self.metrics.count_successes(&self.inner, n);
    }
}

fn get_decoder(
    encoding: DataEncoding,
    debug_name: &str,
    envelope: &SourceEnvelope,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
    fast_forwarded: bool,
    is_connector_delimited: bool,
    metrics: Metrics,
) -> DataDecoder {
    match encoding {
        DataEncoding::Avro(AvroEncoding {
            schema,
            schema_registry_config,
            confluent_wire_format,
        }) => {
            let reject_non_inserts = match envelope {
                SourceEnvelope::Debezium(_, mode) => {
                    // `start_offset` should work fine for `DEBEZIUM UPSERT`
                    fast_forwarded && !matches!(mode, DebeziumMode::Upsert)
                }
                _ => false,
            };
            let state = avro::AvroDecoderState::new(
                &schema,
                schema_registry_config,
                envelope.get_avro_envelope_type(),
                reject_non_inserts,
                debug_name.to_string(),
                confluent_wire_format,
            )
            .expect("Failed to create avro decoder");
            DataDecoder {
                inner: DataDecoderInner::Avro(state),
                metrics,
            }
        }
        DataEncoding::Text
        | DataEncoding::Bytes
        | DataEncoding::Protobuf(_)
        | DataEncoding::Regex(_) => {
            let after_delimiting = match encoding {
                DataEncoding::Regex(RegexEncoding { regex }) => {
                    PreDelimitedFormat::Regex(regex, Default::default())
                }
                DataEncoding::Protobuf(ProtobufEncoding {
                    descriptors,
                    message_name,
                }) => PreDelimitedFormat::Protobuf(ProtobufDecoderState::new(
                    &descriptors,
                    message_name,
                )),
                DataEncoding::Bytes => PreDelimitedFormat::Bytes,
                DataEncoding::Text => PreDelimitedFormat::Text,
                _ => unreachable!(),
            };
            let inner = if is_connector_delimited {
                DataDecoderInner::PreDelimited(after_delimiting)
            } else {
                DataDecoderInner::DelimitedBytes {
                    delimiter: b'\n',
                    format: after_delimiting,
                }
            };
            DataDecoder { inner, metrics }
        }
        DataEncoding::AvroOcf(AvroOcfEncoding { reader_schema }) => match envelope {
            SourceEnvelope::Debezium(..) => {
                let state = avro::AvroDecoderState::new(
                    &reader_schema,
                    None,
                    envelope.get_avro_envelope_type(),
                    fast_forwarded,
                    debug_name.to_string(),
                    false,
                )
                .expect("Schema was verified to be correct during purification");
                DataDecoder {
                    inner: DataDecoderInner::Avro(state),
                    metrics,
                }
            }

            _ => {
                let state = avro::AvroDecoderState::new(
                    &reader_schema,
                    None,
                    envelope.get_avro_envelope_type(),
                    false,
                    debug_name.to_string(),
                    false,
                )
                .expect("Schema was verified to be correct during purification");
                DataDecoder {
                    inner: DataDecoderInner::Avro(state),
                    metrics,
                }
            }
        },
        DataEncoding::Csv(enc) => {
            let state = CsvDecoderState::new(enc, operators);
            DataDecoder {
                inner: DataDecoderInner::Csv(state),
                metrics,
            }
        }
        DataEncoding::Postgres => {
            unreachable!("Postgres sources should not go through the general decoding path.")
        }
    }
}

/// Decode already delimited records of data.
///
/// Precondition: each record in the stream has at most one key and at most one value.
/// This function is useful mainly for decoding data from systems like Kafka,
/// that have already separated the stream into records/messages/etc. before we
/// decode them.
///
/// Because we expect the upstream connector to have already delimited the data,
/// we return an error here if the decoder does not consume all the bytes. This
/// often lets us, for example, detect when Avro decoding has gone off the rails
/// (which is not always possible otherwise, since often gibberish strings can be interpreted as Avro,
///  so the only signal is how many bytes you managed to decode).
pub fn render_decode_delimited<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, MessagePayload>>,
    key_encoding: Option<DataEncoding>,
    value_encoding: DataEncoding,
    debug_name: &str,
    envelope: &SourceEnvelope,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
    fast_forwarded: bool,
    metrics: Metrics,
) -> (Stream<G, DecodeResult>, Option<Box<dyn Any>>)
where
    G: Scope,
{
    let op_name = format!(
        "{}{}DecodeDelimited",
        key_encoding
            .as_ref()
            .map(|key_encoding| key_encoding.op_name())
            .unwrap_or(""),
        value_encoding.op_name()
    );
    let key_decoder = key_encoding.map(|key_encoding| {
        get_decoder(
            key_encoding,
            debug_name,
            &SourceEnvelope::None,
            operators,
            false,
            true,
            metrics.clone(),
        )
    });

    // push the `mz_offset` column for everything but Kafka/Avro _OR_ Debezium.
    // There is no logical reason for this but historical practice
    let push_metadata = !matches!(value_encoding, DataEncoding::Avro(_))
        && !matches!(envelope, SourceEnvelope::Debezium(..));

    let value_decoder = get_decoder(
        value_encoding,
        debug_name,
        envelope,
        operators,
        fast_forwarded,
        true,
        metrics,
    );

    // The Debezium deduplication and upsert logic rely on elements for the same key going to the same worker.
    // Other decoders don't care; so we distribute things round-robin (i.e., by "position"), and
    // fall back to arbitrarily hashing by value if the upstream didn't give us a position.
    let use_key_contract = matches!(envelope, SourceEnvelope::Debezium(..));
    let results = stream.unary_async(
        Exchange::new(move |x: &SourceOutput<Vec<u8>, MessagePayload>| {
            if use_key_contract {
                x.key.hashed()
            } else if let Some(position) = x.position {
                position.hashed()
            } else {
                x.value.hashed()
            }
        }),
        &op_name,
        move |_, _| {
            let key_decoder = Rc::new(RefCell::new(key_decoder));
            let value_decoder = Rc::new(RefCell::new(value_decoder));
            move |mut input, mut raw_output| {
                let key_decoder = Rc::clone(&key_decoder);
                let value_decoder = Rc::clone(&value_decoder);
                async move {
                    let mut key_decoder = key_decoder.borrow_mut();
                    let mut value_decoder = value_decoder.borrow_mut();

                    let mut n_errors = 0;
                    let mut n_successes = 0;
                    let mut results = vec![];
                    while let Some((cap, data)) = input.next() {
                        for SourceOutput {
                            key,
                            value,
                            position,
                            upstream_time_millis,
                        } in data.iter()
                        {
                            let key_cursor = &mut key.as_slice();
                            let key = if let (Some(key_decoder), false) =
                                (key_decoder.as_mut(), key.is_empty())
                            {
                                let mut key = key_decoder
                                    .next(key_cursor, None, *upstream_time_millis, false).await
                                    .transpose();
                                if let (Some(Ok(_)), false) = (&key, key_cursor.is_empty()) {
                                    key = Some(Err(DecodeError::Text(format!(
                                        "Unexpected bytes remaining for decoded key: {:?}",
                                        key_cursor
                                    ))
                                    .into()));
                                }
                                key.or_else(|| key_decoder.eof(&mut &[][..], None, false).transpose())
                            } else {
                                None
                            };

                            if value == &MessagePayload::Data(vec![]) {
                                results.push((cap.delayed(cap.time()), DecodeResult {
                                    key,
                                    value: None,
                                    position: *position,
                                }));
                            } else {
                                let value = match &value {
                                    MessagePayload::Data(value) => {
                                        let value_bytes_remaining = &mut value.as_slice();
                                        let mut value = value_decoder
                                            .next(
                                                value_bytes_remaining,
                                                *position,
                                                *upstream_time_millis,
                                                push_metadata,
                                            ).await
                                            .transpose();
                                        if let (Some(Ok(_)), false) =
                                            (&value, value_bytes_remaining.is_empty())
                                        {
                                            value = Some(Err(DecodeError::Text(format!(
                                                "Unexpected bytes remaining for decoded value: {:?}",
                                                value_bytes_remaining
                                            ))
                                            .into()));
                                        }
                                        value.or_else(|| {
                                            value_decoder
                                                .eof(&mut &[][..], *position, push_metadata)
                                                .transpose()
                                        })
                                    }
                                    MessagePayload::EOF => Some(Err(DecodeError::Text(format!(
                                        "Unexpected EOF in delimited stream"
                                    ))
                                    .into())),
                                };

                                if matches!(&key, Some(Err(_))) || matches!(&value, Some(Err(_))) {
                                    n_errors += 1;
                                } else if matches!(&value, Some(Ok(_))) {
                                    n_successes += 1;
                                }
                                results.push((cap.delayed(cap.time()), DecodeResult {
                                    key,
                                    value,
                                    position: *position,
                                }));
                            }
                        }
                    }

                    let mut output = raw_output.activate();
                    for (cap, result) in results.drain(..) {
                        let mut session = output.session(&cap);
                        session.give(result);
                    }

                    // Matching historical practice, we only log metrics on the value decoder.
                    if n_errors > 0 {
                        value_decoder.log_errors(n_errors);
                    }
                    if n_successes > 0 {
                        value_decoder.log_successes(n_successes);
                    }
                    drop(output);
                    (input, raw_output)
                }
            }
        },
    );
    (results, None)
}

/// Decode arbitrary chunks of bytes into rows.
///
/// This decode API is used for upstream connectors
/// that don't discover delimiters themselves; i.e., those
/// (like CSV files) that need help from the decoding stage to discover where
/// one record ends and another begins.
///
/// As such, the connectors simply present arbitrary chunks of bytes about which
/// we can't assume any alignment properties. The `DataDecoder` API accepts these,
/// and returns `None` if it needs more bytes to discover the boundary between messages.
/// In that case, this function remembers the already-seen bytes and waits for new ones
/// before calling into the decoder again.
///
/// If the decoder does find a message, we verify (by asserting) that it consumed some bytes, to avoid
/// the possibility of infinite loops.
pub fn render_decode<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, MessagePayload>>,
    key_encoding: Option<DataEncoding>,
    value_encoding: DataEncoding,
    debug_name: &str,
    envelope: &SourceEnvelope,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
    fast_forwarded: bool,
    metrics: Metrics,
) -> (Stream<G, DecodeResult>, Option<Box<dyn Any>>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!(
        "{}{}Decode",
        key_encoding
            .as_ref()
            .map(|key_encoding| key_encoding.op_name())
            .unwrap_or(""),
        value_encoding.op_name()
    );
    let key_decoder = key_encoding.map(|key_encoding| {
        get_decoder(
            key_encoding,
            debug_name,
            &SourceEnvelope::None,
            operators,
            false,
            false,
            metrics.clone(),
        )
    });

    // push the `mz_offset` column for everything but Kafka/Avro _OR_ Debezium.
    // There is no logical reason for this but historical practice
    let push_metadata = !matches!(value_encoding, DataEncoding::Avro(_))
        && !matches!(envelope, SourceEnvelope::Debezium(..));

    let value_decoder = get_decoder(
        value_encoding,
        debug_name,
        envelope,
        operators,
        fast_forwarded,
        false,
        metrics,
    );

    // The `position` value from `SourceOutput` is meaningless here -- it's just the index of a chunk.
    // We therefore ignore it, and keep track ourselves of how many records we've seen (for filling in `mz_line_no`, etc).
    let results = stream.unary_async(Pipeline, &op_name, move |_, _| {
        let key_decoder = Rc::new(RefCell::new(key_decoder));
        let value_decoder = Rc::new(RefCell::new(value_decoder));
        let value_buf = Rc::new(RefCell::new(vec![]));
        let n_seen = Rc::new(RefCell::new(0));
        move |mut input, mut raw_output| {
            let key_decoder = Rc::clone(&key_decoder);
            let value_decoder = Rc::clone(&value_decoder);
            let value_buf = Rc::clone(&value_buf);
            let n_seen = Rc::clone(&n_seen);
            async move {
                let mut key_decoder = key_decoder.borrow_mut();
                let mut value_decoder = value_decoder.borrow_mut();
                let mut value_buf = value_buf.borrow_mut();
                let mut n_seen = n_seen.borrow_mut();

                let mut n_errors = 0;
                let mut n_successes = 0;
                let mut results = vec![];
                while let Some((cap, data)) = input.next() {
                    for SourceOutput {
                        key,
                        value,
                        position: _,
                        upstream_time_millis,
                    } in data.iter()
                    {
                        let key_cursor = &mut key.as_slice();
                        let key = if let (Some(key_decoder), false) =
                            (key_decoder.as_mut(), key.is_empty())
                        {
                            let mut key = key_decoder
                                .next(key_cursor, None, *upstream_time_millis, false)
                                .await
                                .transpose();
                            if let (Some(Ok(_)), false) = (&key, key_cursor.is_empty()) {
                                // Perhaps someday we'll assign semantics to multiple keys in one message, but for now it doesn't make sense.
                                key = Some(Err(DecodeError::Text(format!(
                                    "Unexpected bytes remaining for decoded key: {:?}",
                                    key_cursor
                                ))
                                .into()));
                            }
                            key
                        } else {
                            None
                        };

                        let value = match value {
                            MessagePayload::Data(data) => data,
                            MessagePayload::EOF => {
                                let data = &mut &value_buf[..];
                                let mut result = value_decoder
                                    .eof(data, Some(*n_seen + 1), push_metadata)
                                    .transpose();
                                if !data.is_empty() && !matches!(&result, Some(Err(_))) {
                                    result = Some(Err(DecodeError::Text(format!(
                                        "Saw unexpected EOF with bytes remaining in buffer: {:?}",
                                        data
                                    ))
                                    .into()));
                                }
                                value_buf.clear();

                                match result {
                                    None => continue,
                                    Some(value) => {
                                        if matches!(&key, Some(Err(_))) || matches!(&value, Err(_))
                                        {
                                            n_errors += 1;
                                        } else if matches!(&value, Ok(_)) {
                                            n_successes += 1;
                                        }
                                        results.push((
                                            cap.delayed(cap.time()),
                                            DecodeResult {
                                                key,
                                                value: Some(value),
                                                position: Some(*n_seen),
                                            },
                                        ));
                                        *n_seen += 1;
                                    }
                                }
                                continue;
                            }
                        };

                        // Check whether we have a partial message from last time.
                        // If so, we need to prepend it to the bytes we got from _this_ message.
                        let value = if value_buf.is_empty() {
                            value
                        } else {
                            value_buf.extend_from_slice(&*value);
                            &value_buf
                        };

                        if value.is_empty() {
                            results.push((
                                cap.delayed(cap.time()),
                                DecodeResult {
                                    key,
                                    value: None,
                                    position: None,
                                },
                            ));
                        } else {
                            let value_bytes_remaining = &mut value.as_slice();
                            // The intent is that the below loop runs as long as there are more bytes to decode.
                            //
                            // We'd like to be able to write `while !value_cursor.empty()`
                            // here, but that runs into borrow checker issues, so we use `loop`
                            // and break manually.
                            loop {
                                let old_value_cursor = *value_bytes_remaining;
                                let value = match value_decoder
                                    .next(
                                        value_bytes_remaining,
                                        Some(*n_seen + 1), // Match historical practice - files start at 1, not 0.
                                        *upstream_time_millis,
                                        push_metadata,
                                    )
                                    .await
                                {
                                    Err(e) => Err(e),
                                    Ok(None) => {
                                        let leftover = value_bytes_remaining.to_vec();
                                        *value_buf = leftover;
                                        break;
                                    }
                                    Ok(Some(value)) => Ok(value),
                                };
                                *n_seen += 1;

                                // If the decoders decoded a message, they need to have made progress consuming the bytes.
                                // Otherwise, we risk going into an infinite loop.
                                assert!(
                                    old_value_cursor != *value_bytes_remaining || value.is_err()
                                );

                                let is_err = value.is_err();
                                if matches!(&key, Some(Err(_))) || matches!(&value, Err(_)) {
                                    n_errors += 1;
                                } else if matches!(&value, Ok(_)) {
                                    n_successes += 1;
                                }
                                if value_bytes_remaining.is_empty() {
                                    results.push((
                                        cap.delayed(cap.time()),
                                        DecodeResult {
                                            key,
                                            value: Some(value),
                                            position: Some(*n_seen),
                                        },
                                    ));
                                    *value_buf = vec![];
                                    break;
                                } else {
                                    results.push((
                                        cap.delayed(cap.time()),
                                        DecodeResult {
                                            key: key.clone(),
                                            value: Some(value),
                                            position: Some(*n_seen),
                                        },
                                    ));
                                }
                                if is_err {
                                    // If decoding has gone off the rails, we can no longer be sure that the delimiters are correct, so it
                                    // makes no sense to keep going.
                                    break;
                                }
                            }
                        }
                    }
                }
                let mut output = raw_output.activate();
                for (cap, result) in results.drain(..) {
                    let mut session = output.session(&cap);
                    session.give(result);
                }
                // Matching historical practice, we only log metrics on the value decoder.
                if n_errors > 0 {
                    value_decoder.log_errors(n_errors);
                }
                if n_successes > 0 {
                    value_decoder.log_successes(n_errors);
                }
                drop(output);
                (input, raw_output)
            }
        }
    });
    (results, None)
}
