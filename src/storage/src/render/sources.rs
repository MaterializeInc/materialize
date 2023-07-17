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
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{collection, AsCollection, Collection, Hashable};
use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker, Timestamp};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::source::persist_source;
use mz_storage_client::types::errors::{
    DataflowError, DecodeError, EnvelopeError, UpsertError, UpsertNullKeyError, UpsertValueError,
};
use mz_storage_client::types::sources::encoding::*;
use mz_storage_client::types::sources::*;
use mz_timely_util::operator::CollectionExt;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::generic::operator::empty;
use timely::dataflow::operators::{Concat, Exchange, Leave, OkErr};
use timely::dataflow::scopes::{Child, Scope};
use timely::dataflow::Stream;
use timely::progress::{Antichain, Timestamp as _};

use crate::decode::{render_decode_cdcv2, render_decode_delimited};
use crate::render::upsert::UpsertKey;
use crate::source::types::{DecodeResult, HealthStatusUpdate, SourceOutput};
use crate::source::{self, RawSourceCreationConfig, SourceCreationParams};

/// A type-level enum that holds one of two types of sources depending on their message type
///
/// This enum puts no restrictions to the generic parameters of the variants since it only serves
/// as a type-level enum.
pub enum SourceType<G: Scope> {
    /// A delimited source
    Delimited(Collection<G, SourceOutput<Option<Vec<u8>>, Option<Vec<u8>>>, Diff>),
    /// A source that produces Row's natively, and skips any `render_decode` stream adapters, and
    /// can produce retractions
    Row(Collection<G, SourceOutput<(), Row>, Diff>),
}

/// The output index for health streams, used to handle multiplexed streams
pub(crate) type OutputIndex = usize;

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
pub fn render_source<'g, G: Scope<Timestamp = ()>>(
    scope: &mut Child<'g, G, mz_repr::Timestamp>,
    dataflow_debug_name: &String,
    id: GlobalId,
    description: IngestionDescription<CollectionMetadata>,
    as_of: Antichain<mz_repr::Timestamp>,
    resume_uppers: BTreeMap<GlobalId, Antichain<mz_repr::Timestamp>>,
    source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
    resume_stream: &Stream<Child<'g, G, mz_repr::Timestamp>, ()>,
    storage_state: &mut crate::storage_state::StorageState,
) -> (
    Vec<(
        Collection<Child<'g, G, mz_repr::Timestamp>, Row, Diff>,
        Collection<Child<'g, G, mz_repr::Timestamp>, DataflowError, Diff>,
    )>,
    Stream<G, (OutputIndex, HealthStatusUpdate)>,
    Rc<dyn Any>,
) {
    // Tokens that we should return from the method.
    let mut needed_tokens: Vec<Rc<dyn Any>> = Vec::new();

    // Note that this `render_source` attaches a single _instance_ of a source
    // to the passed `Scope`, and this instance may be disabled if the
    // source type does not support multiple instances. `render_source`
    // is called on each timely worker as part of
    // [`super::build_storage_dataflow`].

    let connection = description.desc.connection.clone();
    let source_name = format!("{}-{}", connection.name(), id);

    let params = SourceCreationParams {
        pg_replication_timeouts: storage_state
            .dataflow_parameters
            .pg_replication_timeouts
            .clone(),
    };

    let base_source_config = RawSourceCreationConfig {
        name: source_name,
        id,
        source_exports: description.source_exports.clone(),
        timestamp_interval: description.desc.timestamp_interval,
        worker_id: scope.index(),
        worker_count: scope.peers(),
        encoding: description.desc.encoding.clone(),
        now: storage_state.now.clone(),
        // TODO(guswynn): avoid extra clones here
        base_metrics: storage_state.source_metrics.clone(),
        as_of: as_of.clone(),
        resume_uppers,
        source_resume_uppers,
        storage_metadata: description.ingestion_metadata.clone(),
        persist_clients: Arc::clone(&storage_state.persist_clients),
        source_statistics: storage_state
            .source_statistics
            .get(&id)
            .expect("statistics initialized")
            .clone(),
        shared_remap_upper: Rc::clone(
            &storage_state.source_uppers[&description.remap_collection_id],
        ),
        params,
        remap_collection_id: description.remap_collection_id.clone(),
    };

    // Build the _raw_ ok and error sources using `create_raw_source` and the
    // correct `SourceReader` implementations
    let (streams, mut health, capability) = match connection {
        GenericSourceConnection::Kafka(connection) => {
            let (streams, health, cap) = source::create_raw_source(
                scope,
                resume_stream,
                base_source_config.clone(),
                connection,
                storage_state.connection_context.clone(),
            );
            let streams: Vec<_> = streams
                .into_iter()
                .map(|(ok, err)| (SourceType::Delimited(ok), err))
                .collect();
            (streams, health, cap)
        }
        GenericSourceConnection::Postgres(connection) => {
            let (streams, health, cap) = source::create_raw_source(
                scope,
                resume_stream,
                base_source_config.clone(),
                connection,
                storage_state.connection_context.clone(),
            );
            let streams: Vec<_> = streams
                .into_iter()
                .map(|(ok, err)| (SourceType::Row(ok), err))
                .collect();
            (streams, health, cap)
        }
        GenericSourceConnection::LoadGenerator(connection) => {
            let (streams, health, cap) = source::create_raw_source(
                scope,
                resume_stream,
                base_source_config.clone(),
                connection,
                storage_state.connection_context.clone(),
            );
            let streams: Vec<_> = streams
                .into_iter()
                .map(|(ok, err)| (SourceType::Row(ok), err))
                .collect();
            (streams, health, cap)
        }
        GenericSourceConnection::TestScript(connection) => {
            let (streams, health, cap) = source::create_raw_source(
                scope,
                resume_stream,
                base_source_config.clone(),
                connection,
                storage_state.connection_context.clone(),
            );
            let streams: Vec<_> = streams
                .into_iter()
                .map(|(ok, err)| (SourceType::Delimited(ok), err))
                .collect();
            (streams, health, cap)
        }
    };

    let source_token = Rc::new(capability);

    needed_tokens.push(source_token);

    let mut outputs = vec![];
    for (ok_source, err_source) in streams {
        // All sources should push their various error streams into this vector,
        // whose contents will be concatenated and inserted along the collection.
        // All subsources include the non-definite errors of the ingestion
        let error_collections = vec![err_source.map(DataflowError::from)];

        let (ok, err, extra_tokens, health_stream) = render_source_stream(
            scope,
            dataflow_debug_name,
            id,
            ok_source,
            description.clone(),
            as_of.clone(),
            error_collections,
            storage_state,
            base_source_config.clone(),
        );
        needed_tokens.extend(extra_tokens);
        outputs.push((ok, err));

        health = health.concat(&health_stream.leave());
    }
    (outputs, health, Rc::new(needed_tokens))
}

/// Completes the rendering of a particular source stream by applying decoding and envelope
/// processing as necessary
fn render_source_stream<G>(
    scope: &mut G,
    dataflow_debug_name: &String,
    id: GlobalId,
    ok_source: SourceType<G>,
    description: IngestionDescription<CollectionMetadata>,
    as_of: Antichain<G::Timestamp>,
    mut error_collections: Vec<Collection<G, DataflowError, Diff>>,
    storage_state: &mut crate::storage_state::StorageState,
    base_source_config: RawSourceCreationConfig,
) -> (
    Collection<G, Row, Diff>,
    Collection<G, DataflowError, Diff>,
    Vec<Rc<dyn Any>>,
    Stream<G, (OutputIndex, HealthStatusUpdate)>,
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
    let (stream, errors, health) = {
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

            // TODO(petrosagg): this should move to the envelope section below and
            // made to work with a stream of Rows instead of decoding Avro directly
            let connection_context = storage_state.connection_context.clone();
            let (oks, token) = render_decode_cdcv2(
                &ok_source,
                schema,
                connection_context,
                csr_connection,
                confluent_wire_format,
            );
            needed_tokens.push(Rc::new(token));
            (oks, None, empty(scope))
        } else {
            // Depending on the type of _raw_ source produced for the given source
            // connection, render the _decode_ part of the pipeline, that turns a raw data
            // stream into a `DecodeResult`.
            let (decoded_stream, decode_health, extra_token) = match ok_source {
                SourceType::Delimited(source) => render_decode_delimited(
                    &source,
                    key_encoding,
                    value_encoding,
                    dataflow_debug_name.clone(),
                    metadata_columns,
                    storage_state.decode_metrics.clone(),
                    storage_state.connection_context.clone(),
                ),
                SourceType::Row(source) => (
                    source.map(|r| DecodeResult {
                        key: None,
                        value: Some(Ok(r.value)),
                        position: r.position,
                        upstream_time_millis: r.upstream_time_millis,
                        partition: r.partition,
                        metadata: Row::default(),
                    }),
                    empty(scope),
                    None,
                ),
            };
            if let Some(tok) = extra_token {
                needed_tokens.push(Rc::new(tok));
            }

            // render envelopes
            let (envelope_ok, envelope_err, envelope_health) = match &envelope {
                SourceEnvelope::Debezium(dbz_envelope) => {
                    let (debezium_ok, errors) = match &dbz_envelope.dedup.tx_metadata {
                        Some(tx_metadata) => {
                            let tx_storage_metadata = description
                                .source_imports
                                .get(&tx_metadata.tx_metadata_global_id)
                                .expect("dependent source missing from ingestion description")
                                .clone();
                            let persist_clients = Arc::clone(&storage_state.persist_clients);
                            let (tx_source_ok_stream, tx_source_err_stream, tx_token) =
                                persist_source::persist_source(
                                    scope,
                                    id,
                                    persist_clients,
                                    tx_storage_metadata,
                                    Some(as_of),
                                    Antichain::new(),
                                    None,
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

                            super::debezium::render_tx(dbz_envelope, &decoded_stream, tx_source_ok)
                        }
                        None => super::debezium::render(dbz_envelope, &decoded_stream),
                    };
                    (debezium_ok, Some(errors), empty(scope))
                }
                SourceEnvelope::Upsert(upsert_envelope) => {
                    let upsert_input = upsert_commands(decoded_stream, upsert_envelope.clone());

                    let persist_clients = Arc::clone(&storage_state.persist_clients);
                    // TODO: Get this to work with the as_of.
                    let resume_upper = base_source_config.resume_uppers[&id].clone();

                    let upper_ts = resume_upper
                        .as_option()
                        .expect("resuming an already finished ingestion")
                        .clone();
                    let (previous, previous_token) = if Timestamp::minimum() < upper_ts {
                        let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));

                        let (stream, tok) = persist_source::persist_source_core(
                            scope,
                            id,
                            persist_clients,
                            description.ingestion_metadata,
                            Some(as_of),
                            Antichain::new(),
                            None,
                            None,
                            // Copy the logic in DeltaJoin/Get/Join to start.
                            |_timer, count| count > 1_000_000,
                        );
                        (stream.as_collection(), Some(tok))
                    } else {
                        (Collection::new(empty(scope)), None)
                    };
                    let (upsert, health_update) = crate::render::upsert::upsert(
                        &upsert_input,
                        upsert_envelope.clone(),
                        resume_upper,
                        previous,
                        previous_token,
                        base_source_config,
                        &storage_state.instance_context,
                        &storage_state.dataflow_parameters,
                    );

                    let (upsert_ok, upsert_err) = upsert.inner.ok_err(split_ok_err);

                    (
                        upsert_ok.as_collection(),
                        Some(upsert_err.as_collection()),
                        health_update,
                    )
                }
                SourceEnvelope::None(none_envelope) => {
                    let results = append_metadata_to_value(decoded_stream);

                    let flattened_stream = flatten_results_prepend_keys(none_envelope, results);

                    let (stream, errors) = flattened_stream.inner.ok_err(split_ok_err);

                    let errors = errors.as_collection();
                    (stream.as_collection(), Some(errors), empty(scope))
                }
                SourceEnvelope::CdcV2 => unreachable!(),
            };

            (
                envelope_ok,
                envelope_err,
                decode_health.concat(&envelope_health),
            )
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
    (collection, err_collection, needed_tokens, health)
}

// TODO: Maybe we should finally move this to some central place and re-use. There seem to be
// enough instances of this by now.
fn split_ok_err<O, E, T, D>(x: (Result<O, E>, T, D)) -> Result<(O, T, D), (E, T, D)> {
    match x {
        (Ok(ok), ts, diff) => Ok((ok, ts, diff)),
        (Err(err), ts, diff) => Err((err, ts, diff)),
    }
}

/// After handling metadata insertion, we split streams into key/value parts for convenience
#[derive(Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
struct KV {
    key: Option<Result<Row, DecodeError>>,
    val: Option<Result<Row, DecodeError>>,
}

fn append_metadata_to_value<G: Scope>(
    results: Collection<G, DecodeResult, Diff>,
) -> Collection<G, KV, Diff> {
    results.map(move |res| {
        let val = res.value.map(|val_result| {
            val_result.map(|mut val| {
                if !res.metadata.is_empty() {
                    RowPacker::for_existing_row(&mut val).extend(res.metadata.into_iter());
                }
                val
            })
        });

        KV { val, key: res.key }
    })
}

/// Convert from streams of [`DecodeResult`] to UpsertCommands, inserting the Key according to [`KeyEnvelope`]
fn upsert_commands<G: Scope>(
    input: Collection<G, DecodeResult, Diff>,
    upsert_envelope: UpsertEnvelope,
) -> Collection<G, (UpsertKey, Option<Result<Row, UpsertError>>, MzOffset), Diff> {
    let mut row_buf = Row::default();
    input.map(move |result| {
        let order = result.position;

        let key = match result.key {
            Some(Ok(key)) => Ok(key),
            None => Err(UpsertError::NullKey(UpsertNullKeyError::with_partition_id(
                result.partition,
            ))),
            Some(Err(err)) => Err(UpsertError::KeyDecode(err)),
        };

        // If we have a well-formed key we can continue, otherwise we're upserting an error
        let key = match key {
            Ok(key) => key,
            err @ Err(_) => match result.value {
                Some(_) => return (UpsertKey::from_key(err.as_ref()), Some(err), order),
                None => return (UpsertKey::from_key(err.as_ref()), None, order),
            },
        };

        // We can now apply the key envelope
        let key_row = match upsert_envelope.style {
            UpsertStyle::Debezium { .. } | UpsertStyle::Default(KeyEnvelope::Flattened) => key,
            UpsertStyle::Default(KeyEnvelope::Named(_)) => {
                if key.iter().nth(1).is_none() {
                    key
                } else {
                    row_buf.packer().push_list(key.iter());
                    row_buf.clone()
                }
            }
            UpsertStyle::Default(KeyEnvelope::None) => unreachable!(),
        };

        let key = UpsertKey::from_key(Ok(&key_row));

        let metadata = result.metadata;
        let value = match result.value {
            Some(Ok(ref row)) => match upsert_envelope.style {
                UpsertStyle::Debezium { after_idx } => match row.iter().nth(after_idx).unwrap() {
                    Datum::List(after) => {
                        row_buf.packer().extend(after.iter().chain(metadata.iter()));
                        Some(Ok(row_buf.clone()))
                    }
                    Datum::Null => None,
                    d => panic!("type error: expected record, found {:?}", d),
                },
                UpsertStyle::Default(_) => {
                    let mut packer = row_buf.packer();
                    packer.extend(key_row.iter().chain(row.iter()).chain(metadata.iter()));
                    Some(Ok(row_buf.clone()))
                }
            },
            Some(Err(err)) => Some(Err(UpsertError::Value(UpsertValueError {
                for_key: key_row,
                inner: Box::new(DataflowError::DecodeError(Box::new(err))),
            }))),
            None => None,
        };

        (key, value, order)
    })
}

/// Convert from streams of [`DecodeResult`] to Rows, inserting the Key according to [`KeyEnvelope`]
fn flatten_results_prepend_keys<G>(
    none_envelope: &NoneEnvelope,
    results: Collection<G, KV, Diff>,
) -> Collection<G, Result<Row, DataflowError>, Diff>
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
                maybe_kv.map(|(key, value)| {
                    let mut key = key.unwrap_or_else(|| null_key_columns.clone());
                    RowPacker::for_existing_row(&mut key).extend_by_row(&value);
                    key
                })
            }),
        KeyEnvelope::Named(_) => {
            results
                .flat_map(raise_key_value_errors)
                .map(move |maybe_kv| {
                    maybe_kv.map(|(key, value)| {
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
                        row
                    })
                })
        }
    }
}

/// Handle possibly missing key or value portions of messages
fn raise_key_value_errors(
    KV { key, val }: KV,
) -> Option<Result<(Option<Row>, Row), DataflowError>> {
    match (key, val) {
        (Some(Ok(key)), Some(Ok(value))) => Some(Ok((Some(key), value))),
        (None, Some(Ok(value))) => Some(Ok((None, value))),
        // always prioritize the value error if either or both have an error
        (_, Some(Err(e))) => Some(Err(e.into())),
        (Some(Err(e)), _) => Some(Err(e.into())),
        (None, None) => None,
        // TODO(petrosagg): these errors would be better grouped under an EnvelopeError enum
        _ => Some(Err(DataflowError::from(EnvelopeError::Flat(
            "Value not present for message".to_string(),
        )))),
    }
}
