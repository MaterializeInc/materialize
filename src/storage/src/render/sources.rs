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
use mz_ore::cast::CastLossy;
use mz_ore::metrics::{CounterVecExt, GaugeVecExt};
use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker, Timestamp};
use mz_storage_operators::metrics::BackpressureMetrics;
use mz_storage_operators::persist_source;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::{
    DataflowError, DecodeError, EnvelopeError, UpsertError, UpsertNullKeyError, UpsertValueError,
};
use mz_storage_types::parameters::StorageMaxInflightBytesConfig;
use mz_storage_types::sources::encoding::*;
use mz_storage_types::sources::*;
use mz_timely_util::operator::CollectionExt;
use mz_timely_util::order::refine_antichain;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::generic::operator::empty;
use timely::dataflow::operators::{Concat, ConnectLoop, Exchange, Feedback, Leave, OkErr};
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
        pg_source_tcp_timeouts: storage_state
            .dataflow_parameters
            .pg_source_tcp_timeouts
            .clone(),
        pg_source_snapshot_statement_timeout: storage_state
            .dataflow_parameters
            .pg_source_snapshot_statement_timeout,
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

    // A set of channels (1 per worker) used to signal rehydration being finished
    // to raw sources. These are channels and not timely streams because they
    // have to cross a scope boundary.
    //
    // Note that these will be entirely subsumed by full `hydration` backpressure,
    // once that is implemented.
    let (starter, mut start_signal) = tokio::sync::mpsc::channel::<()>(1);
    let start_signal = async move {
        let _ = start_signal.recv().await;
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
                start_signal,
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
                start_signal,
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
                start_signal,
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
                start_signal,
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
            starter.clone(),
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
    rehydrated_token: impl std::any::Any + 'static,
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
        connection: _,
        timestamp_interval: _,
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
                    storage_state.decode_metrics.clone(),
                    storage_state.connection_context.clone(),
                ),
                SourceType::Row(source) => (
                    source.map(|r| DecodeResult {
                        key: None,
                        value: Some(Ok(r.value)),
                        metadata: Row::default(),
                        position_for_upsert: r.position_for_upsert,
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
                    let (upsert, health_update) = scope.scoped(
                        &format!("upsert_rehydration_backpressure({})", id),
                        |scope| {
                            let (previous, previous_token, feedback_handle, backpressure_metrics) =
                                if Timestamp::minimum() < upper_ts {
                                    let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));

                                    let backpressure_max_inflight_bytes =
                                        get_backpressure_max_inflight_bytes(
                                            &storage_state
                                                .dataflow_parameters
                                                .storage_dataflow_max_inflight_bytes_config,
                                            &storage_state.instance_context.cluster_memory_limit,
                                        );

                                    let (feedback_handle, flow_control, backpressure_metrics) =
                                        if let Some(storage_dataflow_max_inflight_bytes) =
                                            backpressure_max_inflight_bytes
                                        {
                                            tracing::info!(
                                                ?backpressure_max_inflight_bytes,
                                                "timely-{} using backpressure in upsert for source {}",
                                                base_source_config.worker_id,
                                                id
                                            );
                                            if !storage_state
                                                .dataflow_parameters
                                                .storage_dataflow_max_inflight_bytes_config
                                                .disk_only
                                                || storage_state
                                                    .instance_context
                                                    .scratch_directory
                                                    .is_some()
                                            {
                                                let (feedback_handle, feedback_data) =
                                                    scope.feedback(Default::default());

                                                let backpressure_metrics =
                                                    Some(BackpressureMetrics {
                                                        emitted_bytes: Arc::new(
                                                            base_source_config
                                                                .base_metrics
                                                                .upsert_backpressure_specific
                                                                .emitted_bytes
                                                                .get_delete_on_drop_counter(vec![
                                                                    id.to_string(),
                                                                    scope.index().to_string(),
                                                                ]),
                                                        ),
                                                        last_backpressured_bytes: Arc::new(
                                                            base_source_config
                                                                .base_metrics
                                                                .upsert_backpressure_specific
                                                                .last_backpressured_bytes
                                                                .get_delete_on_drop_gauge(vec![
                                                                    id.to_string(),
                                                                    scope.index().to_string(),
                                                                ]),
                                                        ),
                                                        retired_bytes: Arc::new(
                                                            base_source_config
                                                                .base_metrics
                                                                .upsert_backpressure_specific
                                                                .retired_bytes
                                                                .get_delete_on_drop_counter(vec![
                                                                    id.to_string(),
                                                                    scope.index().to_string(),
                                                                ]),
                                                        ),
                                                    });
                                                (
                                                    Some(feedback_handle),
                                                    Some(persist_source::FlowControl {
                                                        progress_stream: feedback_data,
                                                        max_inflight_bytes:
                                                            storage_dataflow_max_inflight_bytes,
                                                        summary: (Default::default(), 1),
                                                        metrics: backpressure_metrics.clone(),
                                                    }),
                                                    backpressure_metrics,
                                                )
                                            } else {
                                                (None, None, None)
                                            }
                                        } else {
                                            (None, None, None)
                                        };
                                    let (stream, tok) = persist_source::persist_source_core(
                                        scope,
                                        id,
                                        persist_clients,
                                        description.ingestion_metadata,
                                        Some(as_of),
                                        Antichain::new(),
                                        None,
                                        flow_control,
                                        // Copy the logic in DeltaJoin/Get/Join to start.
                                        |_timer, count| count > 1_000_000,
                                    );
                                    (
                                        stream.as_collection(),
                                        Some(tok),
                                        feedback_handle,
                                        backpressure_metrics,
                                    )
                                } else {
                                    (Collection::new(empty(scope)), None, None, None)
                                };
                            let (upsert, health_update, upsert_token) = crate::render::upsert::upsert(
                                &upsert_input.enter(scope),
                                upsert_envelope.clone(),
                                refine_antichain(&resume_upper),
                                previous,
                                previous_token,
                                base_source_config.clone(),
                                &storage_state.instance_context,
                                &storage_state.dataflow_parameters,
                                backpressure_metrics,
                            );

                            // Even though we register the `persist_sink` token at a top-level,
                            // which will stop any data from being committed, we also register
                            // a token for the `upsert` operator which may be in the middle of
                            // rehydration processing the `persist_source` input above.
                            needed_tokens.push(upsert_token);

                            use mz_timely_util::probe::ProbeNotify;
                            let handle = mz_timely_util::probe::Handle::default();
                            let upsert = upsert.inner.probe_notify_with(vec![handle.clone()]);
                            let probe = mz_timely_util::probe::source(
                                scope.clone(),
                                format!("upsert_probe({id})"),
                                handle,
                            );

                            // If configured, delay raw sources until we rehydrate the upsert
                            // source. Otherwise, drop the token, unblocking the sources at the
                            // end rendering.
                            if storage_state
                                .dataflow_parameters
                                .delay_sources_past_rehydration
                            {
                                crate::render::upsert::rehydration_finished(
                                    scope.clone(),
                                    &base_source_config,
                                    rehydrated_token,
                                    refine_antichain(&resume_upper),
                                    &probe,
                                );
                            } else {
                                drop(rehydrated_token)
                            };

                            // If backpressure is enabled, we probe the upsert operator's
                            // output, which is the easiest way to extract frontier information.
                            let upsert = match feedback_handle {
                                Some(feedback_handle) => {
                                    probe.connect_loop(feedback_handle);
                                    upsert.as_collection()
                                }
                                None => upsert.as_collection(),
                            };

                            (upsert.leave(), health_update.leave())
                        },
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

// Returns the maximum limit of inflight bytes for backpressure based on given config
// and the current cluster size
fn get_backpressure_max_inflight_bytes(
    inflight_bytes_config: &StorageMaxInflightBytesConfig,
    cluster_memory_limit: &Option<usize>,
) -> Option<usize> {
    let StorageMaxInflightBytesConfig {
        max_inflight_bytes_default,
        max_inflight_bytes_cluster_size_fraction,
        disk_only: _,
    } = inflight_bytes_config;

    // Will use backpressure only if the default inflight value is provided
    if max_inflight_bytes_default.is_some() {
        let current_cluster_max_bytes_limit =
            cluster_memory_limit.as_ref().and_then(|cluster_memory| {
                max_inflight_bytes_cluster_size_fraction.map(|fraction| {
                    // We just need close the correct % of bytes here, so we just use lossy casts.
                    usize::cast_lossy(f64::cast_lossy(*cluster_memory) * fraction)
                })
            });
        current_cluster_max_bytes_limit.or(*max_inflight_bytes_default)
    } else {
        None
    }
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
                    RowPacker::for_existing_row(&mut val).extend(&*res.metadata);
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
        let order = result.position_for_upsert;

        let key = match result.key {
            Some(Ok(key)) => Ok(key),
            None => Err(UpsertError::NullKey(UpsertNullKeyError)),
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
            Some(Err(inner)) => Some(Err(UpsertError::Value(UpsertValueError {
                for_key: key_row,
                inner,
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

#[cfg(test)]
mod test {
    use super::*;

    #[mz_ore::test]
    fn test_no_default() {
        let config = StorageMaxInflightBytesConfig {
            max_inflight_bytes_default: None,
            max_inflight_bytes_cluster_size_fraction: Some(0.5),
            disk_only: false,
        };
        let memory_limit = Some(1000);

        let backpressure_inflight_bytes_limit =
            get_backpressure_max_inflight_bytes(&config, &memory_limit);

        assert_eq!(backpressure_inflight_bytes_limit, None)
    }

    #[mz_ore::test]
    fn test_no_matching_size() {
        let config = StorageMaxInflightBytesConfig {
            max_inflight_bytes_default: Some(10000),
            max_inflight_bytes_cluster_size_fraction: Some(0.5),
            disk_only: false,
        };

        let backpressure_inflight_bytes_limit = get_backpressure_max_inflight_bytes(&config, &None);

        assert_eq!(
            backpressure_inflight_bytes_limit,
            config.max_inflight_bytes_default
        )
    }

    #[mz_ore::test]
    fn test_calculated_cluster_limit() {
        let config = StorageMaxInflightBytesConfig {
            max_inflight_bytes_default: Some(10000),
            max_inflight_bytes_cluster_size_fraction: Some(0.5),
            disk_only: false,
        };
        let memory_limit = Some(2000);

        let backpressure_inflight_bytes_limit =
            get_backpressure_max_inflight_bytes(&config, &memory_limit);

        // the limit should be 50% of 2000 i.e. 1000
        assert_eq!(backpressure_inflight_bytes_limit, Some(1000));
    }
}
