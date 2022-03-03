// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{any::Any, cell::RefCell, collections::VecDeque, rc::Rc, time::Duration};

use ::regex::Regex;
use chrono::NaiveDateTime;
use differential_dataflow::capture::YieldingIter;
use differential_dataflow::Hashable;
use differential_dataflow::{AsCollection, Collection};
use futures::executor::block_on;
use mz_avro::{AvroDeserializer, GeneralDeserializer};
use mz_expr::PartitionId;
use mz_repr::MessagePayload;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::SyncActivator;

use mz_dataflow_types::{
    sources::{
        encoding::{AvroEncoding, AvroOcfEncoding, DataEncoding, RegexEncoding},
        IncludedColumnSource, SourceEnvelope,
    },
    DecodeError, LinearOperator,
};
use mz_interchange::avro::ConfluentAvroResolver;
use mz_repr::Datum;
use mz_repr::{Diff, Row, Timestamp};
use tracing::error;

use self::avro::AvroDecoderState;
use self::csv::CsvDecoderState;
use self::protobuf::ProtobufDecoderState;
use crate::metrics::Metrics;
use crate::source::{DecodeResult, SourceOutput};

mod avro;
mod csv;
mod protobuf;

pub fn decode_cdcv2<G: Scope<Timestamp = Timestamp>>(
    stream: &Stream<G, SourceOutput<Option<Vec<u8>>, Option<Vec<u8>>>>,
    schema: &str,
    registry: Option<mz_ccsr::ClientConfig>,
    confluent_wire_format: bool,
) -> (Collection<G, Row, Diff>, Box<dyn Any>) {
    // We will have already checked validity of the schema by now, so this can't fail.
    let mut resolver = ConfluentAvroResolver::new(schema, registry, confluent_wire_format).unwrap();
    let channel = Rc::new(RefCell::new(VecDeque::new()));
    let activator: Rc<RefCell<Option<SyncActivator>>> = Rc::new(RefCell::new(None));
    let mut vector = Vec::new();
    stream.sink(
        SourceOutput::<Option<Vec<u8>>, Option<Vec<u8>>>::position_value_contract(),
        "CDCv2-Decode",
        {
            let channel = Rc::clone(&channel);
            let activator = Rc::clone(&activator);
            move |input| {
                input.for_each(|_time, data| {
                    data.swap(&mut vector);
                    for data in vector.drain(..) {
                        let value = match &data.value {
                            Some(value) => value,
                            None => continue,
                        };
                        let (mut data, schema, _) = match block_on(resolver.resolve(&*value)) {
                            Ok(ok) => ok,
                            Err(e) => {
                                error!("Failed to get schema info for CDCv2 record: {}", e);
                                continue;
                            }
                        };
                        let d = GeneralDeserializer {
                            schema: schema.top_node(),
                        };
                        let dec = mz_interchange::avro::cdc_v2::Decoder;
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
    pub fn decode(&mut self, bytes: &[u8]) -> Result<Option<Row>, DecodeError> {
        match self {
            PreDelimitedFormat::Bytes => Ok(Some(Row::pack(Some(Datum::Bytes(bytes))))),
            PreDelimitedFormat::Text => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeError::Text("Failed to decode UTF-8".to_string()))?;
                Ok(Some(Row::pack(Some(Datum::String(s)))))
            }
            PreDelimitedFormat::Regex(regex, row_buf) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeError::Text("Failed to decode UTF-8".to_string()))?;
                let captures = match regex.captures(s) {
                    Some(captures) => captures,
                    None => return Ok(None),
                };
                row_buf.packer().extend(
                    captures
                        .iter()
                        .skip(1)
                        .map(|c| Datum::from(c.map(|c| c.as_str()))),
                );
                Ok(Some(row_buf.clone()))
            }
            PreDelimitedFormat::Protobuf(pb) => pb.get_value(bytes).transpose(),
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
    pub fn next(&mut self, bytes: &mut &[u8]) -> Result<Option<Row>, DecodeError> {
        match &mut self.inner {
            DataDecoderInner::DelimitedBytes { delimiter, format } => {
                let delimiter = *delimiter;
                let chunk_idx = match bytes.iter().position(move |&byte| byte == delimiter) {
                    None => return Ok(None),
                    Some(i) => i,
                };
                let data = &bytes[0..chunk_idx];
                *bytes = &bytes[chunk_idx + 1..];
                format.decode(data)
            }
            DataDecoderInner::Avro(avro) => avro.decode(bytes),
            DataDecoderInner::Csv(csv) => csv.decode(bytes),
            DataDecoderInner::PreDelimited(format) => {
                let result = format.decode(*bytes);
                *bytes = &[];
                result
            }
        }
    }

    /// Get the next record if it exists, assuming an EOF has occurred.
    ///
    /// This is distinct from `next` because, for example, a CSV record should be returned even if it
    /// does not end in a newline.
    pub fn eof(&mut self, bytes: &mut &[u8]) -> Result<Option<Row>, DecodeError> {
        match &mut self.inner {
            DataDecoderInner::Csv(csv) => {
                let result = csv.decode(bytes);
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
                    format.decode(data)
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
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
    is_connector_delimited: bool,
    metrics: Metrics,
) -> DataDecoder {
    match encoding {
        DataEncoding::Avro(AvroEncoding {
            schema,
            schema_registry_config,
            confluent_wire_format,
        }) => {
            let state = avro::AvroDecoderState::new(
                &schema,
                schema_registry_config,
                debug_name.to_string(),
                confluent_wire_format,
            )
            .expect("Failed to create avro decoder, even though we validated ccsr client creation in purification.");
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
                    PreDelimitedFormat::Regex(regex.0, Default::default())
                }
                DataEncoding::Protobuf(encoding) => {
                    PreDelimitedFormat::Protobuf(ProtobufDecoderState::new(encoding).expect(
                        "Failed to create protobuf decoder, even though we validated ccsr \
                                    client creation in purification.",
                    ))
                }
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
        DataEncoding::AvroOcf(AvroOcfEncoding { reader_schema }) => {
            let state =
                avro::AvroDecoderState::new(&reader_schema, None, debug_name.to_string(), false)
                    .expect("Schema was verified to be correct during purification");
            DataDecoder {
                inner: DataDecoderInner::Avro(state),
                metrics,
            }
        }
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

fn try_decode(
    decoder: &mut DataDecoder,
    value: Option<&Vec<u8>>,
) -> Option<Result<Row, DecodeError>> {
    let value_buf = &mut value?.as_slice();
    let value = decoder.next(value_buf);
    if value.is_ok() && !value_buf.is_empty() {
        let err = format!(
            "Unexpected bytes remaining for decoded value: {:?}",
            value_buf
        );
        return Some(Err(DecodeError::Text(err)));
    }
    value
        .transpose()
        .or_else(|| decoder.eof(&mut &[][..]).transpose())
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
    stream: &Stream<G, SourceOutput<Option<Vec<u8>>, Option<Vec<u8>>>>,
    key_encoding: Option<DataEncoding>,
    value_encoding: DataEncoding,
    debug_name: &str,
    envelope: &SourceEnvelope,
    metadata_items: Vec<IncludedColumnSource>,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
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
    let mut key_decoder = key_encoding.map(|key_encoding| {
        get_decoder(key_encoding, debug_name, operators, true, metrics.clone())
    });

    let mut value_decoder = get_decoder(value_encoding, debug_name, operators, true, metrics);

    let dist: fn(&SourceOutput<Option<Vec<u8>>, Option<Vec<u8>>>) -> _ = match envelope {
        SourceEnvelope::Debezium(_) => |x| x.partition.hashed(),
        _ => |x| x.position.hashed(),
    };
    let results = stream.unary_frontier(Exchange::new(dist), &op_name, move |_, _| {
        move |input, output| {
            let mut n_errors = 0;
            let mut n_successes = 0;
            input.for_each(|cap, data| {
                let mut session = output.session(&cap);
                for SourceOutput {
                    key,
                    value,
                    position,
                    upstream_time_millis,
                    partition,
                } in data.iter()
                {
                    let key = key_decoder
                        .as_mut()
                        .and_then(|decoder| try_decode(decoder, key.as_ref()));

                    let value = try_decode(&mut value_decoder, value.as_ref());

                    if matches!(&key, Some(Err(_))) || matches!(&value, Some(Err(_))) {
                        n_errors += 1;
                    } else if matches!(&value, Some(Ok(_))) {
                        n_successes += 1;
                    }

                    session.give(DecodeResult {
                        key,
                        value,
                        position: *position,
                        upstream_time_millis: *upstream_time_millis,
                        partition: partition.clone(),
                        metadata: to_metadata_row(
                            &metadata_items,
                            partition.clone(),
                            *position,
                            *upstream_time_millis,
                        ),
                    });
                }
            });
            // Matching historical practice, we only log metrics on the value decoder.
            if n_errors > 0 {
                value_decoder.log_errors(n_errors);
            }
            if n_successes > 0 {
                value_decoder.log_successes(n_successes);
            }
        }
    });
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
    stream: &Stream<G, SourceOutput<(), MessagePayload>>,
    value_encoding: DataEncoding,
    debug_name: &str,
    metadata_items: Vec<IncludedColumnSource>,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    operators: &mut Option<LinearOperator>,
    metrics: Metrics,
) -> (Stream<G, DecodeResult>, Option<Box<dyn Any>>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let op_name = format!("{}Decode", value_encoding.op_name());

    let mut value_decoder = get_decoder(value_encoding, debug_name, operators, false, metrics);

    let mut value_buf = vec![];

    // The `position` value from `SourceOutput` is meaningless here -- it's just the index of a chunk.
    // We therefore ignore it, and keep track ourselves of how many records we've seen (for filling in `mz_line_no`, etc).
    // Historically, non-delimited sources have their offset start at 1
    let mut n_seen = 1..;
    let results = stream.unary_frontier(Pipeline, &op_name, move |_, _| {
        let metadata_items = metadata_items;
        move |input, output| {
            let mut n_errors = 0;
            let mut n_successes = 0;
            input.for_each(|cap, data| {
                // Currently Kafka is the only kind of source that can have metadata, and it is
                // always delimited, so we will never have metadata in `render_decode`
                let mut session = output.session(&cap);
                for SourceOutput {
                    key: _,
                    value,
                    position: _,
                    upstream_time_millis,
                    partition,
                } in data.iter()
                {
                    let value = match value {
                        MessagePayload::Data(data) => data,
                        MessagePayload::EOF => {
                            let data = &mut &value_buf[..];
                            let mut result = value_decoder.eof(data);
                            if result.is_ok() && !data.is_empty() {
                                result = Err(DecodeError::Text(format!(
                                    "Saw unexpected EOF with bytes remaining in buffer: {:?}",
                                    data
                                )));
                            }
                            value_buf.clear();

                            match result.transpose() {
                                None => continue,
                                Some(value) => {
                                    if value.is_err() {
                                        n_errors += 1;
                                    } else if matches!(&value, Ok(_)) {
                                        n_successes += 1;
                                    }
                                    // `RangeFrom` `Iterator`'s never end
                                    let position = n_seen.next().unwrap();
                                    let metadata = to_metadata_row(
                                        &metadata_items,
                                        partition.clone(),
                                        position,
                                        *upstream_time_millis,
                                    );

                                    session.give(DecodeResult {
                                        key: None,
                                        value: Some(value),
                                        position,
                                        upstream_time_millis: *upstream_time_millis,
                                        partition: partition.clone(),
                                        metadata,
                                    });
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

                    let value_bytes_remaining = &mut value.as_slice();
                    // The intent is that the below loop runs as long as there are more bytes to decode.
                    //
                    // We'd like to be able to write `while !value_cursor.empty()`
                    // here, but that runs into borrow checker issues, so we use `loop`
                    // and break manually.
                    loop {
                        let old_value_cursor = *value_bytes_remaining;
                        let value = match value_decoder.next(value_bytes_remaining) {
                            Err(e) => Err(e),
                            Ok(None) => {
                                let leftover = value_bytes_remaining.to_vec();
                                value_buf = leftover;
                                break;
                            }
                            Ok(Some(value)) => Ok(value),
                        };

                        // If the decoders decoded a message, they need to have made progress consuming the bytes.
                        // Otherwise, we risk going into an infinite loop.
                        assert!(old_value_cursor != *value_bytes_remaining || value.is_err());

                        let is_err = value.is_err();
                        if is_err {
                            n_errors += 1;
                        } else if matches!(&value, Ok(_)) {
                            n_successes += 1;
                        }
                        // `RangeFrom` `Iterator`'s never end
                        let position = n_seen.next().unwrap();
                        let metadata = to_metadata_row(
                            &metadata_items,
                            partition.clone(),
                            position,
                            *upstream_time_millis,
                        );

                        if value_bytes_remaining.is_empty() {
                            session.give(DecodeResult {
                                key: None,
                                value: Some(value),
                                position,
                                upstream_time_millis: *upstream_time_millis,
                                partition: partition.clone(),
                                metadata,
                            });
                            value_buf = vec![];
                            break;
                        } else {
                            session.give(DecodeResult {
                                key: None,
                                value: Some(value),
                                position,
                                upstream_time_millis: *upstream_time_millis,
                                partition: partition.clone(),
                                metadata,
                            });
                        }
                        if is_err {
                            // If decoding has gone off the rails, we can no longer be sure that the delimiters are correct, so it
                            // makes no sense to keep going.
                            break;
                        }
                    }
                }
            });
            // Matching historical practice, we only log metrics on the value decoder.
            if n_errors > 0 {
                value_decoder.log_errors(n_errors);
            }
            if n_successes > 0 {
                value_decoder.log_successes(n_errors);
            }
        }
    });
    (results, None)
}

fn to_metadata_row(
    metadata_items: &[IncludedColumnSource],
    partition: PartitionId,
    position: i64,
    upstream_time_millis: Option<i64>,
) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    match partition {
        PartitionId::Kafka(partition) => {
            for item in metadata_items.iter() {
                match item {
                    IncludedColumnSource::Partition => packer.push(Datum::from(partition)),
                    IncludedColumnSource::Offset | IncludedColumnSource::DefaultPosition => {
                        packer.push(Datum::from(position))
                    }
                    IncludedColumnSource::Timestamp => {
                        let ts =
                            upstream_time_millis.expect("kafka sources always have upstream_time");
                        let (secs, mut millis) = (ts / 1000, (ts.abs() % 1000) as u32);
                        if secs < 0 {
                            millis = 1000 - millis;
                        }

                        packer.push(Datum::from(NaiveDateTime::from_timestamp(
                            secs,
                            millis * 1_000_000,
                        )))
                    }
                    IncludedColumnSource::Topic => unreachable!("Topic is not implemented yet"),
                }
            }
        }
        PartitionId::None => {
            for item in metadata_items.iter() {
                match item {
                    IncludedColumnSource::DefaultPosition => packer.push(Datum::from(position)),
                    _ => unreachable!("Only Kafka supports non-defaultposition metadata items"),
                }
            }
        }
    }
    row
}
