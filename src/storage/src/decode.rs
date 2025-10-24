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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use differential_dataflow::capture::{Message, Progress};
use differential_dataflow::{AsCollection, Hashable, VecCollection};
use futures::StreamExt;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_repr::{Datum, Diff, Row};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::errors::{CsrConnectError, DecodeError, DecodeErrorKind};
use mz_storage_types::sources::encoding::{AvroEncoding, DataEncoding, RegexEncoding};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use regex::Regex;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Timestamp;
use timely::scheduling::SyncActivator;
use tracing::error;

use crate::decode::avro::AvroDecoderState;
use crate::decode::csv::CsvDecoderState;
use crate::decode::protobuf::ProtobufDecoderState;
use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::metrics::decode::DecodeMetricDefs;
use crate::source::types::{DecodeResult, SourceOutput};

mod avro;
mod csv;
mod protobuf;

/// Decode delimited CDCv2 messages.
///
/// This not only literally decodes the avro-encoded messages, but
/// also builds a differential dataflow collection that respects the
/// data and progress messages in the underlying CDCv2 stream.
pub fn render_decode_cdcv2<G: Scope<Timestamp = mz_repr::Timestamp>, FromTime: Timestamp>(
    input: &VecCollection<G, DecodeResult<FromTime>, Diff>,
) -> (VecCollection<G, Row, Diff>, PressOnDropButton) {
    let channel_rx = Rc::new(RefCell::new(VecDeque::new()));
    let activator_set: Rc<RefCell<Option<SyncActivator>>> = Rc::new(RefCell::new(None));

    let mut row_buf = Row::default();
    let channel_tx = Rc::clone(&channel_rx);
    let activator_get = Rc::clone(&activator_set);
    let pact = Exchange::new(|(x, _, _): &(DecodeResult<FromTime>, _, _)| x.key.hashed());
    input.inner.sink(pact, "CDCv2Unpack", move |(input, _)| {
        input.for_each(|_time, data| {
            // The inputs are rows containing two columns that encode an enum, i.e only one of them
            // is ever set while the other is unset. This is the convention we follow in our Avro
            // decoder. When the first field of the record is set then we have a data message.
            // Otherwise we have a progress message.
            for (row, _time, _diff) in data.drain(..) {
                let mut record = match &row.value {
                    Some(Ok(row)) => row.iter(),
                    Some(Err(err)) => {
                        error!("Ignoring errored record: {err}");
                        continue;
                    }
                    None => continue,
                };
                let message = match (record.next().unwrap(), record.next().unwrap()) {
                    (Datum::List(datum_updates), Datum::Null) => {
                        let mut updates = vec![];
                        for update in datum_updates.iter() {
                            let mut update = update.unwrap_list().iter();
                            let data = update.next().unwrap().unwrap_list();
                            let time = update.next().unwrap().unwrap_int64();
                            let diff = Diff::from(update.next().unwrap().unwrap_int64());

                            row_buf.packer().extend(&data);
                            let data = row_buf.clone();
                            let time = u64::try_from(time).expect("non-negative");
                            let time = mz_repr::Timestamp::from(time);
                            updates.push((data, time, diff));
                        }
                        Message::Updates(updates)
                    }
                    (Datum::Null, Datum::List(progress)) => {
                        let mut progress = progress.iter();
                        let mut lower = vec![];
                        for time in &progress.next().unwrap().unwrap_list() {
                            let time = u64::try_from(time.unwrap_int64()).expect("non-negative");
                            lower.push(mz_repr::Timestamp::from(time));
                        }
                        let mut upper = vec![];
                        for time in &progress.next().unwrap().unwrap_list() {
                            let time = u64::try_from(time.unwrap_int64()).expect("non-negative");
                            upper.push(mz_repr::Timestamp::from(time));
                        }
                        let mut counts = vec![];
                        for pair in &progress.next().unwrap().unwrap_list() {
                            let mut pair = pair.unwrap_list().iter();
                            let time = pair.next().unwrap().unwrap_int64();
                            let count = pair.next().unwrap().unwrap_int64();

                            let time = u64::try_from(time).expect("non-negative");
                            let count = usize::try_from(count).expect("non-negative");
                            counts.push((mz_repr::Timestamp::from(time), count));
                        }
                        let progress = Progress {
                            lower,
                            upper,
                            counts,
                        };
                        Message::Progress(progress)
                    }
                    _ => unreachable!("invalid input"),
                };
                channel_tx.borrow_mut().push_back(message);
            }
        });
        if let Some(activator) = activator_get.borrow_mut().as_mut() {
            activator.activate().unwrap()
        }
    });

    struct VdIterator<T>(Rc<RefCell<VecDeque<T>>>);
    impl<T> Iterator for VdIterator<T> {
        type Item = T;
        fn next(&mut self) -> Option<T> {
            self.0.borrow_mut().pop_front()
        }
    }
    // this operator returns a thread-safe drop-token
    let (token, stream) = differential_dataflow::capture::source::build(input.scope(), move |ac| {
        *activator_set.borrow_mut() = Some(ac);
        YieldingIter::new_from(VdIterator(channel_rx), Duration::from_millis(10))
    });

    // The token returned by DD's operator is not compatible with the shutdown mechanism used in
    // storage so we create a dummy operator to hold onto that token.
    let builder = AsyncOperatorBuilder::new("CDCv2-Token".to_owned(), input.scope());
    let button = builder.build(move |_caps| async move {
        let _dd_token = token;
        // Keep this operator around until shutdown
        std::future::pending::<()>().await;
    });
    (stream.as_collection(), button.press_on_drop())
}

/// An iterator that yields with a `None` every so often.
pub struct YieldingIter<I> {
    /// When set, a time after which we should return `None`.
    start: Option<std::time::Instant>,
    after: Duration,
    iter: I,
}

impl<I> YieldingIter<I> {
    /// Construct a yielding iterator from an inter-yield duration.
    pub fn new_from(iter: I, yield_after: Duration) -> Self {
        Self {
            start: None,
            after: yield_after,
            iter,
        }
    }
}

impl<I: Iterator> Iterator for YieldingIter<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        if self.start.is_none() {
            self.start = Some(std::time::Instant::now());
        }
        let start = self.start.as_ref().unwrap();
        if start.elapsed() > self.after {
            self.start = None;
            None
        } else {
            match self.iter.next() {
                Some(x) => Some(x),
                None => {
                    self.start = None;
                    None
                }
            }
        }
    }
}

// These don't know how to find delimiters --
// they just go from sequences of vectors of bytes (for which we already know the delimiters)
// to rows, and can eventually just be planned as `HirRelationExpr::Map`. (TODO)
#[derive(Debug)]
pub(crate) enum PreDelimitedFormat {
    Bytes,
    Text,
    Json,
    Regex(Regex, Row),
    Protobuf(ProtobufDecoderState),
}

impl PreDelimitedFormat {
    pub fn decode(&mut self, bytes: &[u8]) -> Result<Option<Row>, DecodeErrorKind> {
        match self {
            PreDelimitedFormat::Bytes => Ok(Some(Row::pack(Some(Datum::Bytes(bytes))))),
            PreDelimitedFormat::Json => {
                let j = mz_repr::adt::jsonb::Jsonb::from_slice(bytes).map_err(|e| {
                    DecodeErrorKind::Bytes(
                        format!("Failed to decode JSON: {}", e.display_with_causes(),).into(),
                    )
                })?;
                Ok(Some(j.into_row()))
            }
            PreDelimitedFormat::Text => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeErrorKind::Text("Failed to decode UTF-8".into()))?;
                Ok(Some(Row::pack(Some(Datum::String(s)))))
            }
            PreDelimitedFormat::Regex(regex, row_buf) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| DecodeErrorKind::Text("Failed to decode UTF-8".into()))?;
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
    metrics: DecodeMetricDefs,
}

impl DataDecoder {
    pub async fn next(
        &mut self,
        bytes: &mut &[u8],
    ) -> Result<Result<Option<Row>, DecodeErrorKind>, CsrConnectError> {
        let result = match &mut self.inner {
            DataDecoderInner::DelimitedBytes { delimiter, format } => {
                match bytes.iter().position(|&byte| byte == *delimiter) {
                    Some(chunk_idx) => {
                        let data = &bytes[0..chunk_idx];
                        *bytes = &bytes[chunk_idx + 1..];
                        format.decode(data)
                    }
                    None => Ok(None),
                }
            }
            DataDecoderInner::Avro(avro) => avro.decode(bytes).await?,
            DataDecoderInner::Csv(csv) => csv.decode(bytes),
            DataDecoderInner::PreDelimited(format) => {
                let result = format.decode(*bytes);
                *bytes = &[];
                result
            }
        };
        Ok(result)
    }

    /// Get the next record if it exists, assuming an EOF has occurred.
    ///
    /// This is distinct from `next` because, for example, a CSV record should be returned even if it
    /// does not end in a newline.
    pub fn eof(
        &mut self,
        bytes: &mut &[u8],
    ) -> Result<Result<Option<Row>, DecodeErrorKind>, CsrConnectError> {
        let result = match &mut self.inner {
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
        };
        Ok(result)
    }

    pub fn log_errors(&self, n: usize) {
        self.metrics.count_errors(&self.inner, n);
    }

    pub fn log_successes(&self, n: usize) {
        self.metrics.count_successes(&self.inner, n);
    }
}

async fn get_decoder(
    encoding: DataEncoding,
    debug_name: &str,
    // Information about optional transformations that can be eagerly done.
    // If the decoding elects to perform them, it should replace this with
    // `None`.
    is_connection_delimited: bool,
    metrics: DecodeMetricDefs,
    storage_configuration: &StorageConfiguration,
) -> Result<DataDecoder, CsrConnectError> {
    let decoder = match encoding {
        DataEncoding::Avro(AvroEncoding {
            schema,
            csr_connection,
            confluent_wire_format,
        }) => {
            let csr_client = match csr_connection {
                None => None,
                Some(csr_connection) => {
                    let csr_client = csr_connection
                        .connect(storage_configuration, InTask::Yes)
                        .await?;
                    Some(csr_client)
                }
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
        DataEncoding::Text
        | DataEncoding::Bytes
        | DataEncoding::Json
        | DataEncoding::Protobuf(_)
        | DataEncoding::Regex(_) => {
            let after_delimiting = match encoding {
                DataEncoding::Regex(RegexEncoding { regex }) => {
                    PreDelimitedFormat::Regex(regex.regex, Default::default())
                }
                DataEncoding::Protobuf(encoding) => {
                    PreDelimitedFormat::Protobuf(ProtobufDecoderState::new(encoding).expect(
                        "Failed to create protobuf decoder, even though we validated ccsr \
                                    client creation in purification.",
                    ))
                }
                DataEncoding::Bytes => PreDelimitedFormat::Bytes,
                DataEncoding::Json => PreDelimitedFormat::Json,
                DataEncoding::Text => PreDelimitedFormat::Text,
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
        DataEncoding::Csv(enc) => {
            let state = CsvDecoderState::new(enc);
            DataDecoder {
                inner: DataDecoderInner::Csv(state),
                metrics,
            }
        }
    };
    Ok(decoder)
}

async fn decode_delimited(
    decoder: &mut DataDecoder,
    buf: &[u8],
) -> Result<Result<Option<Row>, DecodeError>, CsrConnectError> {
    let mut remaining_buf = buf;
    let value = decoder.next(&mut remaining_buf).await?;

    let result = match value {
        Ok(value) => {
            if remaining_buf.is_empty() {
                match value {
                    Some(value) => Ok(Some(value)),
                    None => decoder.eof(&mut remaining_buf)?,
                }
            } else {
                Err(DecodeErrorKind::Text(
                    format!("Unexpected bytes remaining for decoded value: {remaining_buf:?}")
                        .into(),
                ))
            }
        }
        Err(err) => Err(err),
    };

    Ok(result.map_err(|inner| DecodeError {
        kind: inner,
        raw: buf.to_vec(),
    }))
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
pub fn render_decode_delimited<G: Scope, FromTime: Timestamp>(
    input: &VecCollection<G, SourceOutput<FromTime>, Diff>,
    key_encoding: Option<DataEncoding>,
    value_encoding: DataEncoding,
    debug_name: String,
    metrics: DecodeMetricDefs,
    storage_configuration: StorageConfiguration,
) -> (
    VecCollection<G, DecodeResult<FromTime>, Diff>,
    Stream<G, HealthStatusMessage>,
) {
    let op_name = format!(
        "{}{}DecodeDelimited",
        key_encoding
            .as_ref()
            .map(|key_encoding| key_encoding.op_name())
            .unwrap_or(""),
        value_encoding.op_name()
    );
    let dist = |(x, _, _): &(SourceOutput<FromTime>, _, _)| x.value.hashed();

    let mut builder = AsyncOperatorBuilder::new(op_name, input.scope());

    let (output_handle, output) = builder.new_output::<CapacityContainerBuilder<_>>();
    let mut input = builder.new_input_for(&input.inner, Exchange::new(dist), &output_handle);

    let (_, transient_errors) = builder.build_fallible(move |caps| {
        Box::pin(async move {
            let [cap_set]: &mut [_; 1] = caps.try_into().unwrap();

            let mut key_decoder = match key_encoding {
                Some(encoding) => Some(
                    get_decoder(
                        encoding,
                        &debug_name,
                        true,
                        metrics.clone(),
                        &storage_configuration,
                    )
                    .await?,
                ),
                None => None,
            };

            let mut value_decoder = get_decoder(
                value_encoding,
                &debug_name,
                true,
                metrics,
                &storage_configuration,
            )
            .await?;

            let mut output_container = Vec::new();

            while let Some(event) = input.next().await {
                match event {
                    AsyncEvent::Data(cap, data) => {
                        let mut n_errors = 0;
                        let mut n_successes = 0;
                        for (output, ts, diff) in data.iter() {
                            let key_buf = match output.key.unpack_first() {
                                Datum::Bytes(buf) => Some(buf),
                                Datum::Null => None,
                                d => unreachable!("invalid datum: {d}"),
                            };

                            let key = match key_decoder.as_mut().zip(key_buf) {
                                Some((decoder, buf)) => {
                                    decode_delimited(decoder, buf).await?.transpose()
                                }
                                None => None,
                            };

                            let value = match output.value.unpack_first() {
                                Datum::Bytes(buf) => {
                                    decode_delimited(&mut value_decoder, buf).await?.transpose()
                                }
                                Datum::Null => None,
                                d => unreachable!("invalid datum: {d}"),
                            };

                            if matches!(&key, Some(Err(_))) || matches!(&value, Some(Err(_))) {
                                n_errors += 1;
                            } else if matches!(&value, Some(Ok(_))) {
                                n_successes += 1;
                            }

                            let result = DecodeResult {
                                key,
                                value,
                                metadata: output.metadata.clone(),
                                from_time: output.from_time.clone(),
                            };
                            output_container.push((result, ts.clone(), *diff));
                        }

                        // Matching historical practice, we only log metrics on the value decoder.
                        if n_errors > 0 {
                            value_decoder.log_errors(n_errors);
                        }
                        if n_successes > 0 {
                            value_decoder.log_successes(n_successes);
                        }

                        output_handle.give_container(&cap, &mut output_container);
                    }
                    AsyncEvent::Progress(frontier) => cap_set.downgrade(frontier.iter()),
                }
            }

            Ok(())
        })
    });

    let health = transient_errors.map(|err: Rc<CsrConnectError>| {
        let halt_status = HealthStatusUpdate::halting(err.display_with_causes().to_string(), None);
        HealthStatusMessage {
            id: None,
            namespace: if matches!(&*err, CsrConnectError::Ssh(_)) {
                StatusNamespace::Ssh
            } else {
                StatusNamespace::Decode
            },
            update: halt_status,
        }
    });

    (output.as_collection(), health)
}
