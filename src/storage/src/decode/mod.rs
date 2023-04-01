// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module provides functions that
//! build decoding pipelines from raw source streams.
//!
//! The primary exports are [`render_decode_delimited`], and
//! [`render_decode_cdcv2`]. See their docs for more details about their differences.

use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use chrono::NaiveDateTime;
use differential_dataflow::capture::YieldingIter;
use differential_dataflow::Hashable;
use differential_dataflow::{AsCollection, Collection};
use regex::Regex;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::scheduling::SyncActivator;
use tokio::runtime::Handle as TokioHandle;
use tracing::error;

use mz_avro::{AvroDeserializer, GeneralDeserializer};
use mz_expr::PartitionId;
use mz_interchange::avro::ConfluentAvroResolver;
use mz_repr::{adt::timestamp::CheckedTimestamp, Datum};
use mz_repr::{Diff, Row, Timestamp};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::errors::{DecodeError, DecodeErrorKind};
use mz_storage_client::types::sources::encoding::{
    AvroEncoding, DataEncoding, DataEncodingInner, RegexEncoding,
};
use mz_storage_client::types::sources::{IncludedColumnSource, MzOffset};

use self::avro::AvroDecoderState;
use self::csv::CsvDecoderState;
use self::metrics::DecodeMetrics;
use self::protobuf::ProtobufDecoderState;
use crate::source::types::{DecodeResult, SourceOutput};

mod avro;
mod csv;
pub mod metrics;
mod protobuf;

/// Decode delimited CDCv2 messages.
///
/// This not only literally decodes the avro-encoded messages, but
/// also builds a differential dataflow collection that respects the
/// data and progress messages in the underlying CDCv2 stream.
pub fn render_decode_cdcv2<G: Scope<Timestamp = Timestamp>>(
    input: &Collection<G, SourceOutput<Option<Vec<u8>>, Option<Vec<u8>>>, u32>,
    schema: &str,
    registry: Option<mz_ccsr::Client>,
    confluent_wire_format: bool,
) -> (Collection<G, Row, Diff>, Box<dyn Any + Send + Sync>) {
    // We will have already checked validity of the schema by now, so this can't fail.
    let mut resolver = ConfluentAvroResolver::new(schema, registry, confluent_wire_format).unwrap();
    let channel = Rc::new(RefCell::new(VecDeque::new()));
    let activator: Rc<RefCell<Option<SyncActivator>>> = Rc::new(RefCell::new(None));
    let mut vector = Vec::new();
    input.inner.sink(
        Exchange::new(|(x, _, _): &(SourceOutput<_, _>, _, _)| x.position.hashed()),
        "CDCv2-Decode",
        {
            let channel = Rc::clone(&channel);
            let activator = Rc::clone(&activator);
            let tokio_handle = TokioHandle::current();
            move |input| {
                input.for_each(|_time, data| {
                    data.swap(&mut vector);
                    for (data, _time, _diff) in vector.drain(..) {
                        let value = match &data.value {
                            Some(value) => value,
                            None => continue,
                        };
                        let (mut data, schema, _) =
                            match tokio_handle.block_on(resolver.resolve(&*value)) {
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
    // this operator returns a thread-safe drop-token
    let (token, stream) = differential_dataflow::capture::source::build(input.scope(), move |ac| {
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
    pub fn decode(&mut self, bytes: &[u8]) -> Result<Option<Row>, DecodeErrorKind> {
        match self {
            PreDelimitedFormat::Bytes => Ok(Some(Row::pack(Some(Datum::Bytes(bytes))))),
            PreDelimitedFormat::Text => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeErrorKind::Text("Failed to decode UTF-8".to_string()))?;
                Ok(Some(Row::pack(Some(Datum::String(s)))))
            }
            PreDelimitedFormat::Regex(regex, row_buf) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeErrorKind::Text("Failed to decode UTF-8".to_string()))?;
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
    metrics: DecodeMetrics,
}

impl DataDecoder {
    pub fn next(&mut self, bytes: &mut &[u8]) -> Result<Option<Row>, DecodeErrorKind> {
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
    pub fn eof(&mut self, bytes: &mut &[u8]) -> Result<Option<Row>, DecodeErrorKind> {
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
    is_connection_delimited: bool,
    metrics: DecodeMetrics,
    connection_context: &ConnectionContext,
) -> DataDecoder {
    match encoding.inner {
        DataEncodingInner::Avro(AvroEncoding {
            schema,
            csr_connection,
            confluent_wire_format,
        }) => {
            let csr_client = match csr_connection {
                None => None,
                Some(csr_connection) => Some(
                    TokioHandle::current()
                        .block_on(csr_connection.connect(connection_context))
                        .expect("CSR connection unexpectedly missing secrets"),
                ),
            };
            let state = avro::AvroDecoderState::new(
                &schema,
                csr_client,
                debug_name.to_string(),
                confluent_wire_format,
            )
            .expect("Failed to create avro decoder, even though we validated ccsr client creation in purification.");
            DataDecoder {
                inner: DataDecoderInner::Avro(state),
                metrics,
            }
        }
        DataEncodingInner::Text
        | DataEncodingInner::Bytes
        | DataEncodingInner::Protobuf(_)
        | DataEncodingInner::Regex(_) => {
            let after_delimiting = match encoding.inner {
                DataEncodingInner::Regex(RegexEncoding { regex }) => {
                    PreDelimitedFormat::Regex(regex.0, Default::default())
                }
                DataEncodingInner::Protobuf(encoding) => {
                    PreDelimitedFormat::Protobuf(ProtobufDecoderState::new(encoding).expect(
                        "Failed to create protobuf decoder, even though we validated ccsr \
                                    client creation in purification.",
                    ))
                }
                DataEncodingInner::Bytes => PreDelimitedFormat::Bytes,
                DataEncodingInner::Text => PreDelimitedFormat::Text,
                _ => unreachable!(),
            };
            let inner = if is_connection_delimited {
                DataDecoderInner::PreDelimited(after_delimiting)
            } else {
                DataDecoderInner::DelimitedBytes {
                    delimiter: b'\n',
                    format: after_delimiting,
                }
            };
            DataDecoder { inner, metrics }
        }
        DataEncodingInner::Csv(enc) => {
            let state = CsvDecoderState::new(enc);
            DataDecoder {
                inner: DataDecoderInner::Csv(state),
                metrics,
            }
        }
        DataEncodingInner::RowCodec(_) => {
            unreachable!("RowCodec sources should not go through the general decoding path.")
        }
    }
}

fn try_decode_delimited(
    decoder: &mut DataDecoder,
    value: Option<&Vec<u8>>,
) -> Option<Result<Row, DecodeErrorKind>> {
    let value_buf = &mut value?.as_slice();
    let value = decoder.next(value_buf);
    if value.is_ok() && !value_buf.is_empty() {
        let err = format!(
            "Unexpected bytes remaining for decoded value: {:?}",
            value_buf
        );
        return Some(Err(DecodeErrorKind::Text(err)));
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
/// Because we expect the upstream connection to have already delimited the data,
/// we return an error here if the decoder does not consume all the bytes. This
/// often lets us, for example, detect when Avro decoding has gone off the rails
/// (which is not always possible otherwise, since often gibberish strings can be interpreted as Avro,
///  so the only signal is how many bytes you managed to decode).
pub fn render_decode_delimited<G>(
    input: &Collection<G, SourceOutput<Option<Vec<u8>>, Option<Vec<u8>>>, u32>,
    key_encoding: Option<DataEncoding>,
    value_encoding: DataEncoding,
    debug_name: &str,
    metadata_items: Vec<IncludedColumnSource>,
    metrics: DecodeMetrics,
    connection_context: &ConnectionContext,
) -> (
    Collection<G, DecodeResult, Diff>,
    Option<Box<dyn Any + Send + Sync>>,
)
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
        get_decoder(
            key_encoding,
            debug_name,
            true,
            metrics.clone(),
            connection_context,
        )
    });

    let mut value_decoder = get_decoder(
        value_encoding,
        debug_name,
        true,
        metrics,
        connection_context,
    );

    let dist =
        |(x, _, _): &(SourceOutput<Option<Vec<u8>>, Option<Vec<u8>>>, _, _)| x.value.hashed();

    let results = input
        .inner
        .unary_frontier(Exchange::new(dist), &op_name, move |_, _| {
            move |input, output| {
                let mut n_errors = 0;
                let mut n_successes = 0;
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for (output, ts, diff) in data.iter() {
                        let SourceOutput {
                            key,
                            value,
                            position,
                            upstream_time_millis,
                            partition,
                            headers,
                        } = output;

                        let key = key_decoder.as_mut().and_then(|decoder| {
                            try_decode_delimited(decoder, key.as_ref()).map(|result| {
                                result.map_err(|inner| DecodeError {
                                    kind: inner,
                                    raw: key.clone(),
                                })
                            })
                        });

                        let value = try_decode_delimited(&mut value_decoder, value.as_ref()).map(
                            |result| {
                                result.map_err(|inner| DecodeError {
                                    kind: inner,
                                    raw: value.clone(),
                                })
                            },
                        );

                        if matches!(&key, Some(Err(_))) || matches!(&value, Some(Err(_))) {
                            n_errors += 1;
                        } else if matches!(&value, Some(Ok(_))) {
                            n_successes += 1;
                        }

                        let result = DecodeResult {
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
                                headers.as_deref(),
                            ),
                        };
                        session.give((result, ts.clone(), Diff::from(*diff)));
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
    (results.as_collection(), None)
}

fn to_metadata_row(
    metadata_items: &[IncludedColumnSource],
    partition: PartitionId,
    position: MzOffset,
    upstream_time_millis: Option<i64>,
    headers: Option<&[(String, Option<Vec<u8>>)]>,
) -> Row {
    let position = position.offset;
    let mut row = Row::default();
    let mut packer = row.packer();
    match partition {
        PartitionId::Kafka(partition) => {
            for item in metadata_items.iter() {
                match item {
                    IncludedColumnSource::Partition => packer.push(Datum::from(partition)),
                    IncludedColumnSource::Offset => packer.push(Datum::UInt64(position)),
                    IncludedColumnSource::Timestamp => {
                        let ts =
                            upstream_time_millis.expect("kafka sources always have upstream_time");

                        let d: Datum = NaiveDateTime::from_timestamp_millis(ts)
                            .and_then(|dt| {
                                let ct: Option<CheckedTimestamp<NaiveDateTime>> =
                                    dt.try_into().ok();
                                ct
                            })
                            .into();
                        packer.push(d)
                    }
                    IncludedColumnSource::Topic => unreachable!("Topic is not implemented yet"),
                    IncludedColumnSource::Headers => {
                        packer.push_list_with(|r| {
                            // If the source asked for headers, but we didn't get any, we still
                            // want to run the `push_dict_with`, to produce an empty map value
                            //
                            // This is a `BTreeMap`, so the `push_dict_with` ordering invariant is
                            // upheld
                            if let Some(headers) = headers {
                                for (k, v) in headers {
                                    match v {
                                        Some(v) => r.push_list_with(|record_row| {
                                            record_row.push(Datum::String(k));
                                            record_row.push(Datum::Bytes(v));
                                        }),
                                        None => r.push_list_with(|record_row| {
                                            record_row.push(Datum::String(k));
                                            record_row.push(Datum::Null);
                                        }),
                                    }
                                }
                            }
                        });
                    }
                }
            }
        }
        PartitionId::None => {
            if !metadata_items.is_empty() {
                unreachable!("Only Kafka supports metadata items");
            }
        }
    }
    row
}
