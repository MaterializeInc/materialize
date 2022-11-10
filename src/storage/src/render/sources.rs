// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sources.
//!
//! See [`render_source`] for more details.

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{collection, AsCollection, Collection, Hashable};
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::{Exchange, Map, OkErr, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::runtime::Handle as TokioHandle;

use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker, Timestamp};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::source::persist_source;
use mz_storage_client::types::errors::{DataflowError, DecodeError, EnvelopeError};
use mz_storage_client::types::sources::{encoding::*, *};
use mz_timely_util::operator::{CollectionExt, StreamExt};

use crate::decode::{render_decode, render_decode_cdcv2, render_decode_delimited};
use crate::source::types::{DecodeResult, SourceOutput};
use crate::source::{self, DelimitedValueSourceConnection, RawSourceCreationConfig};

/// A type-level enum that holds one of two types of sources depending on their message type
///
/// This enum puts no restrictions to the generic parameters of the variants since it only serves
/// as a type-level enum.
enum SourceType<Delimited, ByteStream, RowSource> {
    /// A delimited source
    Delimited(Delimited),
    /// A bytestream source
    ByteStream(ByteStream),
    /// A source that produces Row's natively,
    /// and skips any `render_decode` stream
    /// adapters, and can produce
    /// retractions
    Row(RowSource),
}

/// _Renders_ complete _differential_ [`Collection`]s
/// that represent the final source and its errors
/// as requested by the original `CREATE SOURCE` statement,
/// encapsulated in the passed `SourceInstanceDesc`.
///
/// The first element in the returned tuple is the pair of [`Collection`]s,
/// the second is a type-erased token that will keep the source
/// alive as long as it is not dropped.
///
/// This function is intended to implement the recipe described here:
/// <https://github.com/MaterializeInc/materialize/blob/main/doc/developer/platform/architecture-storage.md#source-ingestion>
pub fn render_source<G>(
    scope: &mut G,
    dataflow_debug_name: &String,
    id: GlobalId,
    description: IngestionDescription<CollectionMetadata>,
    resume_upper: Antichain<G::Timestamp>,
    storage_state: &mut crate::storage_state::StorageState,
) -> (
    Vec<(Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // Tokens that we should return from the method.
    let mut needed_tokens: Vec<Rc<dyn Any>> = Vec::new();

    // Note that this `render_source` attaches a single _instance_ of a source
    // to the passed `Scope`, and this instance may be disabled if the
    // source type does not support multiple instances. `render_source`
    // is called on each timely worker as part of
    // [`super::build_storage_dataflow`].

    let connection = description.desc.connection.clone();
    let source_name = format!("{}-{}", connection.name(), id);
    let base_source_config = RawSourceCreationConfig {
        name: source_name,
        id,
        num_outputs: description.desc.num_outputs(),
        timestamp_interval: description.desc.timestamp_interval.clone(),
        worker_id: scope.index(),
        worker_count: scope.peers(),
        encoding: description.desc.encoding.clone(),
        now: storage_state.now.clone(),
        // TODO(guswynn): avoid extra clones here
        base_metrics: storage_state.source_metrics.clone(),
        resume_upper: resume_upper.clone(),
        storage_metadata: description.ingestion_metadata.clone(),
        persist_clients: Arc::clone(&storage_state.persist_clients),
    };

    // TODO(petrosagg): put the description as-is in the RawSourceCreationConfig instead of cloning
    // a million fields
    let resumption_calculator = description.clone();

    // Build the _raw_ ok and error sources using `create_raw_source` and the
    // correct `SourceReader` implementations
    let ((ok_sources, err_source), capability) = match connection {
        GenericSourceConnection::Kafka(connection) => {
            let ((oks, err), cap) = source::create_raw_source(
                scope,
                base_source_config,
                connection,
                storage_state.connection_context.clone(),
                resumption_calculator,
            );
            let oks: Vec<_> = oks.into_iter().map(SourceType::Delimited).collect();
            ((oks, err), cap)
        }
        GenericSourceConnection::Kinesis(connection) => {
            let ((oks, err), cap) = source::create_raw_source(
                scope,
                base_source_config,
                DelimitedValueSourceConnection(connection),
                storage_state.connection_context.clone(),
                resumption_calculator,
            );
            let oks = oks.into_iter().map(SourceType::Delimited).collect();
            ((oks, err), cap)
        }
        GenericSourceConnection::S3(connection) => {
            let ((oks, err), cap) = source::create_raw_source(
                scope,
                base_source_config,
                connection,
                storage_state.connection_context.clone(),
                resumption_calculator,
            );
            let oks = oks.into_iter().map(SourceType::ByteStream).collect();
            ((oks, err), cap)
        }
        GenericSourceConnection::Postgres(connection) => {
            let ((oks, err), cap) = source::create_raw_source(
                scope,
                base_source_config,
                connection,
                storage_state.connection_context.clone(),
                resumption_calculator,
            );
            let oks = oks.into_iter().map(SourceType::Row).collect();
            ((oks, err), cap)
        }
        GenericSourceConnection::LoadGenerator(connection) => {
            let ((oks, err), cap) = source::create_raw_source(
                scope,
                base_source_config,
                connection,
                storage_state.connection_context.clone(),
                resumption_calculator,
            );
            let oks = oks.into_iter().map(SourceType::Row).collect();
            ((oks, err), cap)
        }
        GenericSourceConnection::TestScript(connection) => {
            let ((oks, err), cap) = source::create_raw_source(
                scope,
                base_source_config,
                connection,
                storage_state.connection_context.clone(),
                resumption_calculator,
            );
            let oks: Vec<_> = oks.into_iter().map(SourceType::Delimited).collect();
            ((oks, err), cap)
        }
    };

    let source_token = Rc::new(capability);

    needed_tokens.push(source_token);

    let mut outputs = vec![];
    for ok_source in ok_sources {
        // All sources should push their various error streams into this vector,
        // whose contents will be concatenated and inserted along the collection.
        // All subsources include the non-definite errors of the ingestion
        let error_collections = vec![err_source
            .map(DataflowError::SourceError)
            .pass_through("source-errors", 1)
            .as_collection()];

        let (ok, err, extra_tokens) = render_source_stream(
            scope,
            dataflow_debug_name,
            id,
            ok_source,
            description.clone(),
            resume_upper.clone(),
            error_collections,
            storage_state,
        );
        needed_tokens.extend(extra_tokens);
        outputs.push((ok, err));
    }
    (outputs, Rc::new(needed_tokens))
}

type ConcreteSourceType<G> = SourceType<
    // Delimited sources
    Stream<G, SourceOutput<Option<Vec<u8>>, Option<Vec<u8>>, ()>>,
    // ByteStream sources
    Stream<G, SourceOutput<(), Option<Vec<u8>>, ()>>,
    // Row sources
    Stream<G, SourceOutput<(), Row, Diff>>,
>;

/// Completes the rendering of a particular source stream by applying decoding and envelope
/// processing as necessary
fn render_source_stream<G>(
    scope: &mut G,
    dataflow_debug_name: &String,
    id: GlobalId,
    ok_source: ConcreteSourceType<G>,
    description: IngestionDescription<CollectionMetadata>,
    resume_upper: Antichain<G::Timestamp>,
    mut error_collections: Vec<Collection<G, DataflowError, Diff>>,
    storage_state: &mut crate::storage_state::StorageState,
) -> (
    Collection<G, Row, Diff>,
    Collection<G, DataflowError, Diff>,
    Vec<Rc<dyn Any>>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let mut needed_tokens: Vec<Rc<dyn Any>> = vec![];

    let SourceDesc {
        encoding,
        envelope,
        metadata_columns,
        ..
    } = description.desc;
    let (stream, errors) = {
        let (key_encoding, value_encoding) = match encoding {
            SourceDataEncoding::KeyValue { key, value } => (Some(key), value),
            SourceDataEncoding::Single(value) => (None, value),
        };
        // CDCv2 can't quite be slotted in to the below code, since it determines
        // its own diffs/timestamps as part of decoding.
        //
        // `render_decode_cdcv2` accomplishes what decoding and envelope-processing
        // below do for other `SourceEnvelope`s
        if let SourceEnvelope::CdcV2 = &envelope {
            let AvroEncoding {
                schema,
                csr_connection,
                confluent_wire_format,
            } = match value_encoding.inner {
                DataEncodingInner::Avro(enc) => enc,
                _ => unreachable!("Attempted to create non-Avro CDCv2 source"),
            };
            let ok_source = match ok_source {
                SourceType::Delimited(s) => s,
                _ => unreachable!("Attempted to create non-delimited CDCv2 source"),
            };
            let csr_client = match csr_connection {
                None => None,
                Some(csr_connection) => Some(
                    TokioHandle::current()
                        .block_on(
                            csr_connection
                                .connect(&*storage_state.connection_context.secrets_reader),
                        )
                        .expect("CSR connection unexpectedly missing secrets"),
                ),
            };
            // TODO(petrosagg): this should move to the envelope section below and
            // made to work with a stream of Rows instead of decoding Avro directly
            let (oks, token) =
                render_decode_cdcv2(&ok_source, &schema, csr_client, confluent_wire_format);
            needed_tokens.push(Rc::new(token));
            (oks, None)
        } else {
            // Depending on the type of _raw_ source produced for the given source
            // connection, render the _decode_ part of the pipeline, that turns a raw data
            // stream into a `DecodeResult`.
            let (results, extra_token) = match ok_source {
                SourceType::Delimited(source) => render_decode_delimited(
                    &source,
                    key_encoding,
                    value_encoding,
                    dataflow_debug_name,
                    metadata_columns,
                    storage_state.decode_metrics.clone(),
                    &storage_state.connection_context,
                ),
                SourceType::ByteStream(source) => render_decode(
                    &source,
                    value_encoding,
                    dataflow_debug_name,
                    metadata_columns,
                    storage_state.decode_metrics.clone(),
                    &storage_state.connection_context,
                ),
                SourceType::Row(source) => (
                    source.map(|r| DecodeResult {
                        key: None,
                        value: Some(Ok((r.value, r.diff))),
                        position: r.position,
                        upstream_time_millis: r.upstream_time_millis,
                        partition: r.partition,
                        metadata: Row::default(),
                    }),
                    None,
                ),
            };
            if let Some(tok) = extra_token {
                needed_tokens.push(Rc::new(tok));
            }

            // render envelopes
            match &envelope {
                SourceEnvelope::Debezium(dbz_envelope) => {
                    let (stream, errors) = match &dbz_envelope.dedup.tx_metadata {
                        Some(tx_metadata) => {
                            let tx_storage_metadata = description
                                .source_imports
                                .get(&tx_metadata.tx_metadata_global_id)
                                .expect("dependent source missing from ingestion description")
                                .clone();
                            let persist_clients = Arc::clone(&storage_state.persist_clients);
                            let upper_ts = resume_upper.as_option().copied().unwrap();
                            let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));
                            let (tx_source_ok_stream, tx_source_err_stream, tx_token) =
                                persist_source::persist_source(
                                    scope,
                                    id,
                                    persist_clients,
                                    tx_storage_metadata,
                                    Some(as_of),
                                    Antichain::new(),
                                    None,
                                    // Copy the logic in DeltaJoin/Get/Join to start.
                                    |_timer, count| count > 1_000_000,
                                );
                            let (tx_source_ok, tx_source_err) = (
                                tx_source_ok_stream.as_collection(),
                                tx_source_err_stream.as_collection(),
                            );
                            needed_tokens.push(tx_token);
                            error_collections.push(tx_source_err);

                            super::debezium::render_tx(dbz_envelope, &results, tx_source_ok)
                        }
                        None => super::debezium::render(dbz_envelope, &results),
                    };
                    (stream.as_collection(), Some(errors.as_collection()))
                }
                SourceEnvelope::Upsert(upsert_envelope) => {
                    // TODO: use the key envelope to figure out when to add keys.
                    // The operator currently does it unconditionally
                    let transformed_results =
                        transform_keys_from_key_envelope(upsert_envelope, results);

                    let persist_clients = Arc::clone(&storage_state.persist_clients);

                    // persit requires an `as_of`, and presents all data before that
                    // `as_of` as if its at that `as_of`. We only care about if
                    // the data is before the `resume_upper` or not, so we pick
                    // the biggest `as_of` we can. We could always choose
                    // 0, but that may have been compacted away.
                    let previous_as_of = match resume_upper.as_option() {
                        None => {
                            // We are at the end of time, so our `as_of` is everything.
                            Some(Timestamp::MAX)
                        }
                        Some(&Timestamp::MIN) => {
                            // We are the beginning of time (no data persisted yet), so we can
                            // skip reading out of persist.
                            None
                        }
                        Some(&t) => Some(t.saturating_sub(1)),
                    };
                    let (previous_stream, previous_token) =
                        if let Some(previous_as_of) = previous_as_of {
                            let (stream, tok) = persist_source::persist_source_core(
                                scope,
                                id,
                                persist_clients,
                                // TODO(petrosagg): upsert needs to read its output and here we
                                // assume that all upsert ingestion will output their data to the
                                // same collection as the one carrying the ingestion. This is the
                                // case at the time of writing but we need a more robust
                                // implementation. Consider having the upsert operator hold private
                                // state (a copy), or encoding the fact that this operator's state
                                // and the output collection state is the same in an explicit way
                                description.ingestion_metadata,
                                Some(Antichain::from_elem(previous_as_of)),
                                Antichain::new(),
                                None,
                                // Copy the logic in DeltaJoin/Get/Join to start.
                                |_timer, count| count > 1_000_000,
                            );
                            (stream, Some(tok))
                        } else {
                            (std::iter::empty().to_stream(scope), None)
                        };
                    let (upsert_ok, upsert_err) = super::upsert::upsert(
                        &transformed_results,
                        resume_upper,
                        upsert_envelope.clone(),
                        previous_stream,
                        previous_token,
                    );

                    (upsert_ok.as_collection(), Some(upsert_err.as_collection()))
                }
                SourceEnvelope::None(none_envelope) => {
                    let results = append_metadata_to_value(results);

                    let flattened_stream = flatten_results_prepend_keys(none_envelope, results);

                    let flattened_stream = flattened_stream.pass_through("decode", 1).map(
                        |(val, time, diff)| match val {
                            Ok((val, diff)) => (Ok(val), time, diff),
                            Err(e) => (Err(e), time, diff),
                        },
                    );

                    // TODO: Maybe we should finally move this to some central
                    // place and re-use. There seem to be enough instances of this
                    // by now.
                    fn split_ok_err(
                        x: (Result<Row, DataflowError>, mz_repr::Timestamp, Diff),
                    ) -> Result<
                        (Row, mz_repr::Timestamp, Diff),
                        (DataflowError, mz_repr::Timestamp, Diff),
                    > {
                        match x {
                            (Ok(row), ts, diff) => Ok((row, ts, diff)),
                            (Err(err), ts, diff) => Err((err, ts, diff)),
                        }
                    }

                    let (stream, errors) = flattened_stream.ok_err(split_ok_err);

                    let errors = errors.as_collection();
                    (stream.as_collection(), Some(errors))
                }
                SourceEnvelope::CdcV2 => unreachable!(),
            }
        }
    };

    if let Some(errors) = errors {
        error_collections.push(errors);
    }

    // Perform various additional transformations on the collection.

    // Force a shuffling of data in case sources are not uniformly distributed.
    let collection = stream.inner.exchange(|x| x.hashed()).as_collection();

    // Flatten the error collections.
    let err_collection = match error_collections.len() {
        0 => Collection::empty(scope),
        1 => error_collections.pop().unwrap(),
        _ => collection::concatenate(scope, error_collections),
    };

    // Return the collections and any needed tokens.
    (collection, err_collection, needed_tokens)
}

/// After handling metadata insertion, we split streams into key/value parts for convenience
#[derive(Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
struct KV {
    key: Option<Result<Row, DecodeError>>,
    val: Option<Result<(Row, Diff), DecodeError>>,
}

fn append_metadata_to_value<G>(
    results: timely::dataflow::Stream<G, DecodeResult>,
) -> timely::dataflow::Stream<G, KV>
where
    G: Scope<Timestamp = Timestamp>,
{
    results.map(move |res| {
        let val = res.value.map(|val_result| {
            val_result.map(|(mut val, diff)| {
                if !res.metadata.is_empty() {
                    RowPacker::for_existing_row(&mut val).extend(res.metadata.into_iter());
                }

                (val, diff)
            })
        });

        KV { val, key: res.key }
    })
}

/// Convert from streams of [`DecodeResult`] to Rows, inserting the Key according to [`KeyEnvelope`]
// TODO(guswynn): figure out how to merge this duplicated logic with `flatten_results_prepend_keys`
fn transform_keys_from_key_envelope<G>(
    upsert_envelope: &UpsertEnvelope,
    results: timely::dataflow::Stream<G, DecodeResult>,
) -> timely::dataflow::Stream<G, DecodeResult>
where
    G: Scope<Timestamp = Timestamp>,
{
    match upsert_envelope {
        UpsertEnvelope {
            style: UpsertStyle::Default(KeyEnvelope::Flattened) | UpsertStyle::Debezium { .. },
            ..
        } => results,
        UpsertEnvelope {
            style: UpsertStyle::Default(KeyEnvelope::Named(_)),
            ..
        } => {
            let mut row_buf = mz_repr::Row::default();
            results.map(move |mut res| {
                res.key = res.key.map(|k_result| {
                    k_result.map(|k| {
                        if k.iter().nth(1).is_none() {
                            k
                        } else {
                            row_buf.packer().push_list(k.iter());
                            row_buf.clone()
                        }
                    })
                });

                res
            })
        }
        UpsertEnvelope {
            style: UpsertStyle::Default(KeyEnvelope::None),
            ..
        } => {
            unreachable!("SourceEnvelope::Upsert should never have KeyEnvelope::None")
        }
    }
}

/// Convert from streams of [`DecodeResult`] to Rows, inserting the Key according to [`KeyEnvelope`]
fn flatten_results_prepend_keys<G>(
    none_envelope: &NoneEnvelope,
    results: timely::dataflow::Stream<G, KV>,
) -> timely::dataflow::Stream<G, Result<(Row, Diff), DataflowError>>
where
    G: Scope,
{
    let NoneEnvelope {
        key_envelope,
        key_arity,
    } = none_envelope;

    let null_key_columns = Row::pack_slice(&vec![Datum::Null; *key_arity]);

    match key_envelope {
        KeyEnvelope::None => {
            results.flat_map(|KV { val, .. }| val.map(|result| result.map_err(Into::into)))
        }
        KeyEnvelope::Flattened => results
            .flat_map(raise_key_value_errors)
            .map(move |maybe_kv| {
                maybe_kv.map(|(key, value, diff)| {
                    let mut key = key.unwrap_or_else(|| null_key_columns.clone());
                    RowPacker::for_existing_row(&mut key).extend_by_row(&value);
                    (key, diff)
                })
            }),
        KeyEnvelope::Named(_) => {
            results
                .flat_map(raise_key_value_errors)
                .map(move |maybe_kv| {
                    maybe_kv.map(|(key, value, diff)| {
                        let mut key = key.unwrap_or_else(|| null_key_columns.clone());
                        // Named semantics rename a key that is a single column, and encode a
                        // multi-column field as a struct with that name
                        let row = if key.iter().nth(1).is_none() {
                            RowPacker::for_existing_row(&mut key).extend_by_row(&value);
                            key
                        } else {
                            let mut new_row = Row::default();
                            let mut packer = new_row.packer();
                            packer.push_list(key.iter());
                            packer.extend_by_row(&value);
                            new_row
                        };
                        (row, diff)
                    })
                })
        }
    }
}

/// Handle possibly missing key or value portions of messages
fn raise_key_value_errors(
    KV { key, val }: KV,
) -> Option<Result<(Option<Row>, Row, Diff), DataflowError>> {
    match (key, val) {
        (Some(Ok(key)), Some(Ok((value, diff)))) => Some(Ok((Some(key), value, diff))),
        (None, Some(Ok((value, diff)))) => Some(Ok((None, value, diff))),
        // always prioritize the value error if either or both have an error
        (_, Some(Err(e))) => Some(Err(e.into())),
        (Some(Err(e)), _) => Some(Err(e.into())),
        (None, None) => None,
        // TODO(petrosagg): these errors would be better grouped under an EnvelopeError enum
        _ => Some(Err(DataflowError::EnvelopeError(EnvelopeError::Flat(
            "Value not present for message".to_string(),
        )))),
    }
}
