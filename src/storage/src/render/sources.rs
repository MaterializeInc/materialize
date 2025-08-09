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

use std::collections::BTreeMap;
use std::iter;
use std::sync::Arc;

use crate::decode::{render_decode_cdcv2, render_decode_delimited};
use crate::healthcheck::{HealthStatusMessage, StatusNamespace};
use crate::source::types::{DecodeResult, SourceOutput, SourceRender};
use crate::source::{self, RawSourceCreationConfig, SourceExportCreationConfig};
use crate::upsert::UpsertKey;
use differential_dataflow::{AsCollection, Collection};
use mz_ore::cast::CastLossy;
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_persist_client::operators::time::AsOfBounds;
use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker};
use mz_storage_operators::persist_source;
use mz_storage_operators::persist_source::Subtime;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::dyncfgs;
use mz_storage_types::errors::{
    DataflowError, DecodeError, EnvelopeError, UpsertError, UpsertNullKeyError, UpsertValueError,
};
use mz_storage_types::parameters::StorageMaxInflightBytesConfig;
use mz_storage_types::sources::envelope::{KeyEnvelope, NoneEnvelope, UpsertEnvelope, UpsertStyle};
use mz_storage_types::sources::*;
use mz_timely_util::builder_async::PressOnDropButton;
use mz_timely_util::operator::CollectionExt;
use mz_timely_util::order::refine_antichain;
use serde::{Deserialize, Serialize};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::Stream;
use timely::dataflow::operators::{ConnectLoop, Feedback, Leave, Map, OkErr};
use timely::dataflow::scopes::{Child, Scope};
use timely::progress::{Antichain, Timestamp};

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
pub fn render_source<'g, G, C>(
    scope: &mut Child<'g, G, mz_repr::Timestamp>,
    dataflow_debug_name: &String,
    connection: C,
    description: IngestionDescription<CollectionMetadata>,
    resume_stream: &Stream<Child<'g, G, mz_repr::Timestamp>, ()>,
    storage_state: &crate::storage_state::StorageState,
    base_source_config: RawSourceCreationConfig,
) -> (
    BTreeMap<
        GlobalId,
        (
            Collection<Child<'g, G, mz_repr::Timestamp>, Row, Diff>,
            Collection<Child<'g, G, mz_repr::Timestamp>, DataflowError, Diff>,
        ),
    >,
    Vec<Stream<G, HealthStatusMessage>>,
    Vec<PressOnDropButton>,
)
where
    G: Scope<Timestamp = ()>,
    C: SourceConnection + SourceRender + 'static,
{
    // Tokens that we should return from the method.
    let mut needed_tokens = Vec::new();

    // Note that this `render_source` attaches a single _instance_ of a source
    // to the passed `Scope`, and this instance may be disabled if the
    // source type does not support multiple instances. `render_source`
    // is called on each timely worker as part of
    // [`super::build_storage_dataflow`].

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
    let (exports, health, source_tokens) = source::create_raw_source(
        scope,
        storage_state,
        resume_stream,
        &base_source_config,
        connection,
        start_signal,
    );

    needed_tokens.extend(source_tokens);

    let mut health_streams = Vec::with_capacity(exports.len() + 1);
    health_streams.push(health);

    let mut outputs = BTreeMap::new();
    for (export_id, export) in exports {
        type CB<C> = CapacityContainerBuilder<C>;
        let (ok_stream, err_stream) =
            export.map_fallible::<CB<_>, CB<_>, _, _, _>("export-demux-ok-err", |r| r);

        // All sources should push their various error streams into this vector,
        // whose contents will be concatenated and inserted along the collection.
        // All subsources include the non-definite errors of the ingestion
        let mut error_collections = Vec::new();

        let data_config = base_source_config.source_exports[&export_id]
            .data_config
            .clone();
        let (ok, extra_tokens, health_stream) = render_source_stream(
            scope,
            dataflow_debug_name,
            export_id,
            ok_stream,
            data_config,
            &description,
            &mut error_collections,
            storage_state,
            &base_source_config,
            starter.clone(),
        );
        needed_tokens.extend(extra_tokens);

        // Flatten the error collections.
        let err_collection = match error_collections.len() {
            0 => err_stream,
            _ => err_stream.concatenate(error_collections),
        };

        outputs.insert(export_id, (ok, err_collection));

        health_streams.extend(health_stream.into_iter().map(|s| s.leave()));
    }
    (outputs, health_streams, needed_tokens)
}

/// Completes the rendering of a particular source stream by applying decoding and envelope
/// processing as necessary
fn render_source_stream<G, FromTime>(
    scope: &mut G,
    dataflow_debug_name: &String,
    export_id: GlobalId,
    ok_source: Collection<G, SourceOutput<FromTime>, Diff>,
    data_config: SourceExportDataConfig,
    description: &IngestionDescription<CollectionMetadata>,
    error_collections: &mut Vec<Collection<G, DataflowError, Diff>>,
    storage_state: &crate::storage_state::StorageState,
    base_source_config: &RawSourceCreationConfig,
    rehydrated_token: impl std::any::Any + 'static,
) -> (
    Collection<G, Row, Diff>,
    Vec<PressOnDropButton>,
    Vec<Stream<G, HealthStatusMessage>>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    FromTime: Timestamp + Sync,
{
    let mut needed_tokens = vec![];

    // Use the envelope and encoding configs for this particular source export
    let SourceExportDataConfig { encoding, envelope } = data_config;

    let SourceDesc {
        connection: _,
        timestamp_interval: _,
        primary_export: _,
        primary_export_details: _,
    } = description.desc;

    let (decoded_stream, decode_health) = match encoding {
        None => (
            ok_source.map(|r| DecodeResult {
                // This is safe because the current set of sources produce
                // either:
                // 1. Non-nullable keys
                // 2. No keys at all.
                //
                // Please see the comment on `key_envelope_no_encoding` in
                // `mz_sql::plan::statement::ddl` for more details.
                key: Some(Ok(r.key)),
                value: Some(Ok(r.value)),
                metadata: r.metadata,
                from_time: r.from_time,
            }),
            None,
        ),
        Some(encoding) => {
            let (decoded_stream, decode_health) = render_decode_delimited(
                &ok_source,
                encoding.key,
                encoding.value,
                dataflow_debug_name.clone(),
                storage_state.metrics.decode_defs.clone(),
                storage_state.storage_configuration.clone(),
            );
            (decoded_stream, Some(decode_health))
        }
    };

    // render envelopes
    let (envelope_ok, envelope_health) = match &envelope {
        SourceEnvelope::Upsert(upsert_envelope) => {
            let upsert_input = upsert_commands(decoded_stream, upsert_envelope.clone());

            let persist_clients = Arc::clone(&storage_state.persist_clients);
            // TODO: Get this to work with the as_of.
            let resume_upper = base_source_config.resume_uppers[&export_id].clone();

            let upper_ts = resume_upper
                .as_option()
                .expect("resuming an already finished ingestion")
                .clone();
            let (upsert, health_update) = scope.scoped(
                &format!("upsert_rehydration_backpressure({})", export_id),
                |scope| {
                    let (previous, previous_token, feedback_handle, backpressure_metrics) = {
                        let as_of = Antichain::from_elem(upper_ts.saturating_sub(1));

                        let backpressure_max_inflight_bytes = get_backpressure_max_inflight_bytes(
                            &storage_state
                                .storage_configuration
                                .parameters
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
                                    export_id
                                );
                                if !storage_state
                                    .storage_configuration
                                    .parameters
                                    .storage_dataflow_max_inflight_bytes_config
                                    .disk_only
                                    || storage_state.instance_context.scratch_directory.is_some()
                                {
                                    let (feedback_handle, feedback_data) =
                                        scope.feedback(Default::default());

                                    // TODO(guswynn): cleanup
                                    let backpressure_metrics = Some(
                                        base_source_config
                                            .metrics
                                            .get_backpressure_metrics(export_id, scope.index()),
                                    );

                                    (
                                        Some(feedback_handle),
                                        Some(persist_source::FlowControl {
                                            progress_stream: feedback_data,
                                            max_inflight_bytes: storage_dataflow_max_inflight_bytes,
                                            summary: (Default::default(), Subtime::least_summary()),
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

                        let storage_metadata = description.source_exports[&export_id]
                            .storage_metadata
                            .clone();

                        let error_handler =
                            storage_state.error_handler("upsert_rehydration", export_id);

                        let (stream, tok) = persist_source::persist_source_core(
                            scope,
                            export_id,
                            persist_clients,
                            storage_metadata,
                            None,
                            &AsOfBounds::new(error_handler.clone(), Some(as_of)),
                            SnapshotMode::Include,
                            Antichain::new(),
                            None,
                            flow_control,
                            false.then_some(|| unreachable!()),
                            async {},
                            error_handler,
                        );
                        (
                            stream.as_collection(),
                            Some(tok),
                            feedback_handle,
                            backpressure_metrics,
                        )
                    };

                    let export_statistics = storage_state
                        .aggregated_statistics
                        .get_source(&export_id)
                        .expect("statistics initialized")
                        .clone();
                    let export_config = SourceExportCreationConfig {
                        id: export_id,
                        worker_id: base_source_config.worker_id,
                        metrics: base_source_config.metrics.clone(),
                        source_statistics: export_statistics,
                    };
                    let (upsert, health_update, snapshot_progress, upsert_token) =
                        crate::upsert::upsert(
                            &upsert_input.enter(scope),
                            upsert_envelope.clone(),
                            refine_antichain(&resume_upper),
                            previous,
                            previous_token,
                            export_config,
                            &storage_state.instance_context,
                            &storage_state.storage_configuration,
                            &storage_state.dataflow_parameters,
                            backpressure_metrics,
                        );

                    // Even though we register the `persist_sink` token at a top-level,
                    // which will stop any data from being committed, we also register
                    // a token for the `upsert` operator which may be in the middle of
                    // rehydration processing the `persist_source` input above.
                    needed_tokens.push(upsert_token);

                    // If configured, delay raw sources until we rehydrate the upsert
                    // source. Otherwise, drop the token, unblocking the sources at the
                    // end rendering.
                    if dyncfgs::DELAY_SOURCES_PAST_REHYDRATION
                        .get(storage_state.storage_configuration.config_set())
                    {
                        crate::upsert::rehydration_finished(
                            scope.clone(),
                            base_source_config,
                            rehydrated_token,
                            refine_antichain(&resume_upper),
                            &snapshot_progress,
                        );
                    } else {
                        drop(rehydrated_token)
                    };

                    // If backpressure from persist is enabled, we connect the upsert operator's
                    // snapshot progress to the persist source feedback handle.
                    if let Some(feedback_handle) = feedback_handle {
                        snapshot_progress.connect_loop(feedback_handle);
                    }

                    (
                        upsert.leave(),
                        health_update
                            .map(|(id, update)| HealthStatusMessage {
                                id,
                                namespace: StatusNamespace::Upsert,
                                update,
                            })
                            .leave(),
                    )
                },
            );

            let (upsert_ok, upsert_err) = upsert.inner.ok_err(split_ok_err);
            error_collections.push(upsert_err.as_collection());

            (upsert_ok.as_collection(), Some(health_update))
        }
        SourceEnvelope::None(none_envelope) => {
            let results = append_metadata_to_value(decoded_stream);

            let flattened_stream = flatten_results_prepend_keys(none_envelope, results);

            let (stream, errors) = flattened_stream.inner.ok_err(split_ok_err);

            error_collections.push(errors.as_collection());
            (stream.as_collection(), None)
        }
        SourceEnvelope::CdcV2 => {
            let (oks, token) = render_decode_cdcv2(&decoded_stream);
            needed_tokens.push(token);
            (oks, None)
        }
    };

    // Return the collections and any needed tokens.
    let health = decode_health.into_iter().chain(envelope_health).collect();
    (envelope_ok, needed_tokens, health)
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

fn append_metadata_to_value<G: Scope, FromTime: Timestamp>(
    results: Collection<G, DecodeResult<FromTime>, Diff>,
) -> Collection<G, KV, Diff> {
    results.map(move |res| {
        let val = res.value.map(|val_result| {
            val_result.map(|mut val| {
                if !res.metadata.is_empty() {
                    RowPacker::for_existing_row(&mut val).extend_by_row(&res.metadata);
                }
                val
            })
        });

        KV { val, key: res.key }
    })
}

/// Convert from streams of [`DecodeResult`] to UpsertCommands, inserting the Key according to [`KeyEnvelope`]
fn upsert_commands<G: Scope, FromTime: Timestamp>(
    input: Collection<G, DecodeResult<FromTime>, Diff>,
    upsert_envelope: UpsertEnvelope,
) -> Collection<G, (UpsertKey, Option<Result<Row, UpsertError>>, FromTime), Diff> {
    let mut row_buf = Row::default();
    input.map(move |result| {
        let from_time = result.from_time;

        let key = match result.key {
            Some(Ok(key)) => Ok(key),
            None => Err(UpsertError::NullKey(UpsertNullKeyError)),
            Some(Err(err)) => Err(UpsertError::KeyDecode(err)),
        };

        // If we have a well-formed key we can continue, otherwise we're upserting an error
        let key = match key {
            Ok(key) => key,
            err @ Err(_) => match result.value {
                Some(_) => return (UpsertKey::from_key(err.as_ref()), Some(err), from_time),
                None => return (UpsertKey::from_key(err.as_ref()), None, from_time),
            },
        };

        // We can now apply the key envelope
        let key_row = match upsert_envelope.style {
            // flattened or debezium
            UpsertStyle::Debezium { .. }
            | UpsertStyle::Default(KeyEnvelope::Flattened)
            | UpsertStyle::ValueErrInline {
                key_envelope: KeyEnvelope::Flattened,
                error_column: _,
            } => key,
            // named
            UpsertStyle::Default(KeyEnvelope::Named(_))
            | UpsertStyle::ValueErrInline {
                key_envelope: KeyEnvelope::Named(_),
                error_column: _,
            } => {
                if key.iter().nth(1).is_none() {
                    key
                } else {
                    row_buf.packer().push_list(key.iter());
                    row_buf.clone()
                }
            }
            UpsertStyle::Default(KeyEnvelope::None)
            | UpsertStyle::ValueErrInline {
                key_envelope: KeyEnvelope::None,
                error_column: _,
            } => unreachable!(),
        };

        let key = UpsertKey::from_key(Ok(&key_row));

        let metadata = result.metadata;

        let value = match result.value {
            Some(Ok(ref row)) => match upsert_envelope.style {
                UpsertStyle::Debezium { after_idx } => match row.iter().nth(after_idx).unwrap() {
                    Datum::List(after) => {
                        let mut packer = row_buf.packer();
                        packer.extend(after.iter());
                        packer.extend_by_row(&metadata);
                        Some(Ok(row_buf.clone()))
                    }
                    Datum::Null => None,
                    d => panic!("type error: expected record, found {:?}", d),
                },
                UpsertStyle::Default(_) => {
                    let mut packer = row_buf.packer();
                    packer.extend_by_row(&key_row);
                    packer.extend_by_row(row);
                    packer.extend_by_row(&metadata);
                    Some(Ok(row_buf.clone()))
                }
                UpsertStyle::ValueErrInline { .. } => {
                    let mut packer = row_buf.packer();
                    packer.extend_by_row(&key_row);
                    // The 'error' column is null
                    packer.push(Datum::Null);
                    packer.extend_by_row(row);
                    packer.extend_by_row(&metadata);
                    Some(Ok(row_buf.clone()))
                }
            },
            Some(Err(inner)) => {
                match upsert_envelope.style {
                    UpsertStyle::ValueErrInline { .. } => {
                        let mut count = 0;
                        // inline the error in the data output
                        let err_string = inner.to_string();
                        let mut packer = row_buf.packer();
                        for datum in key_row.iter() {
                            packer.push(datum);
                            count += 1;
                        }
                        // The 'error' column is a record with a 'description' column
                        packer.push_list(iter::once(Datum::String(&err_string)));
                        count += 1;
                        let metadata_len = metadata.as_row_ref().iter().count();
                        // push nulls for all value columns
                        packer.extend(
                            iter::repeat(Datum::Null)
                                .take(upsert_envelope.source_arity - count - metadata_len),
                        );
                        packer.extend_by_row(&metadata);
                        Some(Ok(row_buf.clone()))
                    }
                    _ => Some(Err(UpsertError::Value(UpsertValueError {
                        for_key: key_row,
                        inner,
                    }))),
                }
            }
            None => None,
        };

        (key, value, from_time)
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
            "Value not present for message".into(),
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
