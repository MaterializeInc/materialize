// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sources.

use std::collections::HashMap;
use std::rc::Rc;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{collection, AsCollection, Collection};
use persist_types::Codec;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::{Concat, Map, OkErr, UnorderedInput};
use timely::dataflow::Scope;

use persist::operators::source::PersistedSource;
use persist::operators::stream::{AwaitFrontier, Seal};
use persist::operators::upsert::PersistentUpsertConfig;

use dataflow_types::sources::{encoding::*, persistence::*, *};
use dataflow_types::*;
use expr::{GlobalId, Id, PartitionId, SourceInstanceId};
use ore::now::NowFn;
use ore::result::ResultExt;
use repr::{RelationDesc, Row, ScalarType, Timestamp};

use crate::decode::decode_cdcv2;
use crate::decode::render_decode;
use crate::decode::render_decode_delimited;
use crate::decode::rewrite_for_upsert;
use crate::logging::materialized::Logger;
use crate::operator::{CollectionExt, StreamExt};
use crate::render::context::Context;
use crate::render::{RelevantTokens, RenderState};
use crate::server::LocalInput;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::timestamp::{AssignedTimestamp, SourceTimestamp};
use crate::source::{
    self, DecodeResult, FileSourceReader, KafkaSourceReader, KinesisSourceReader,
    PersistentTimestampBindingsConfig, PostgresSourceReader, PubNubSourceReader, S3SourceReader,
    SourceConfig,
};

/// A type-level enum that holds one of two types of sources depending on their message type
///
/// This enum puts no restrictions to the generic parameters of the variants since it only serves
/// as a type-level enum.
enum SourceType<Delimited, ByteStream> {
    /// A delimited source
    Delimited(Delimited),
    /// A bytestream source
    ByteStream(ByteStream),
}

impl<G> Context<G, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Render a local source and return a handle to its input, and the data and
    /// error collections.
    fn render_local_source(
        &mut self,
        render_state: &mut RenderState,
        scope: &mut G,
        persisted_name: Option<String>,
    ) -> (LocalInput, Collection<G, Row>, Collection<G, DataflowError>) {
        let ((handle, capability), ok_stream, err_collection) = {
            let ((handle, capability), ok_stream) = scope.new_unordered_input();
            let err_collection = Collection::empty(scope);
            ((handle, capability), ok_stream, err_collection)
        };
        let local_input = LocalInput { handle, capability };

        // A local "source" is either fed by a local input handle, or by reading from a
        // `persisted_source()`.
        //
        // For non-persisted sources, values that are to be inserted are sent from the
        // coordinator and pushed into the handle.
        //
        // For persisted sources, the coordinator only writes new values to a persistent
        // stream. These values will then "show up" here because we read from the same
        // persistent stream.
        let (ok_stream, error_collection) = match (&mut render_state.persist, persisted_name) {
            (Some(persist), Some(stream_name)) => {
                let (_write, read) = persist.create_or_load(&stream_name);
                let (persist_ok_stream, persist_err_stream) = scope
                    .persisted_source(read, &self.as_of_frontier)
                    .ok_err(|x| match x {
                        (Ok(kv), ts, diff) => Ok((kv, ts, diff)),
                        (Err(err), ts, diff) => Err((err, ts, diff)),
                    });
                let (persist_ok_stream, decode_err_stream) =
                    persist_ok_stream.ok_err(|((row, ()), ts, diff)| Ok((row, ts, diff)));
                let persist_err_collection = persist_err_stream
                    .concat(&decode_err_stream)
                    .map(move |(err, ts, diff)| {
                        let err = SourceError::new(
                            stream_name.clone(),
                            SourceErrorDetails::Persistence(err),
                        );
                        (err.into(), ts, diff)
                    })
                    .as_collection();
                (
                    ok_stream.concat(&persist_ok_stream),
                    err_collection.concat(&persist_err_collection),
                )
            }
            _ => (ok_stream, err_collection),
        };
        let as_of_frontier = self.as_of_frontier.clone();
        let ok_collection = ok_stream
            .map_in_place(move |(_, mut time, _)| {
                time.advance_by(as_of_frontier.borrow());
            })
            .as_collection();
        (local_input, ok_collection, error_collection)
    }

    /// Import the source described by `src` into the rendering context.
    pub(crate) fn import_source(
        &mut self,
        render_state: &mut RenderState,
        tokens: &mut RelevantTokens,
        scope: &mut G,
        materialized_logging: Option<Logger>,
        src_id: GlobalId,
        mut src: SourceDesc,
        // The original ID of the source, before it was decomposed into a bare source (which might
        // have its own transient ID) and a relational transformation (which has the original source
        // ID).
        orig_id: GlobalId,
        now: NowFn,
        base_metrics: &SourceBaseMetrics,
    ) {
        // Extract the linear operators, as we will need to manipulate them.
        // extracting them reduces the change we might accidentally communicate
        // them through `src`.
        let mut linear_operators = src.operators.take();

        // Blank out trivial linear operators.
        if let Some(operator) = &linear_operators {
            if operator.is_trivial(src.bare_desc.arity()) {
                linear_operators = None;
            }
        }

        // Before proceeding, we may need to remediate sources with non-trivial relational
        // expressions that post-process the bare source. If the expression is trivial, a
        // get of the bare source, we can present `src.operators` to the source directly.
        // Otherwise, we need to zero out `src.operators` and instead perform that logic
        // at the end of `src.optimized_expr`.
        //
        // This has a lot of potential for improvement in the near future.
        match src.connector.clone() {
            // Create a new local input (exposed as TABLEs to users). Data is inserted
            // via Command::Insert commands.
            SourceConnector::Local { persisted_name, .. } => {
                let (local_input, ok_collection, err_collection) =
                    self.render_local_source(render_state, scope, persisted_name);

                render_state.local_inputs.insert(src_id, local_input);
                self.insert_id(
                    Id::Global(src_id),
                    crate::render::CollectionBundle::from_collections(
                        ok_collection,
                        err_collection,
                    ),
                );
            }

            SourceConnector::External {
                connector,
                encoding,
                envelope,
                consistency,
                ts_frequency,
                persist,
                timeline: _,
            } => {
                // TODO(benesch): this match arm is hard to follow. Refactor.

                // This uid must be unique across all different instantiations of a source
                let uid = SourceInstanceId {
                    source_id: orig_id,
                    dataflow_id: self.dataflow_id,
                };

                // All sources should push their various error streams into this vector,
                // whose contents will be concatenated and inserted along the collection.
                let mut error_collections = Vec::<Collection<_, _>>::new();

                let source_persist_config = match (persist, render_state.persist.as_mut()) {
                    (Some(persist_desc), Some(persist)) => {
                        Some(get_persist_config(&uid, persist_desc, persist))
                    }
                    _ => None,
                };

                let fast_forwarded = match &connector {
                    ExternalSourceConnector::Kafka(KafkaSourceConnector {
                        start_offsets, ..
                    }) => start_offsets.values().any(|&val| val > 0),
                    _ => false,
                };

                // All workers are responsible for reading in Kafka sources. Other sources
                // support single-threaded ingestion only. Note that in all cases we want all
                // readers of the same source or same partition to reside on the same worker,
                // and only load-balance responsibility across distinct sources.
                let active_read_worker = if let ExternalSourceConnector::Kafka(_) = connector {
                    true
                } else {
                    // TODO: This feels icky, but getting rid of hardcoding this difference between
                    // Kafka and all other sources seems harder.
                    crate::source::responsible_for(
                        &src_id,
                        scope.index(),
                        scope.peers(),
                        &PartitionId::None,
                    )
                };

                let timestamp_histories = render_state
                    .ts_histories
                    .get(&orig_id)
                    .map(|history| history.clone());
                let source_name = format!("{}-{}", connector.name(), uid);
                let source_config = SourceConfig {
                    name: source_name.clone(),
                    sql_name: src.name.clone(),
                    upstream_name: connector.upstream_name().map(ToOwned::to_owned),
                    id: uid,
                    scope,
                    // Distribute read responsibility among workers.
                    active: active_read_worker,
                    timestamp_histories,
                    consistency,
                    timestamp_frequency: ts_frequency,
                    worker_id: scope.index(),
                    worker_count: scope.peers(),
                    logger: materialized_logging,
                    encoding: encoding.clone(),
                    now,
                    base_metrics,
                };

                let (mut collection, capability) = if let ExternalSourceConnector::PubNub(
                    pubnub_connector,
                ) = connector
                {
                    let source = PubNubSourceReader::new(src.name.clone(), pubnub_connector);
                    let ((ok_stream, err_stream), capability) =
                        source::create_source_simple(source_config, source);

                    error_collections.push(
                        err_stream
                            .map(DataflowError::SourceError)
                            .pass_through("source-errors")
                            .as_collection(),
                    );

                    (ok_stream.as_collection(), capability)
                } else if let ExternalSourceConnector::Postgres(pg_connector) = connector {
                    let source = PostgresSourceReader::new(
                        src.name.clone(),
                        pg_connector,
                        source_config.base_metrics,
                    );

                    let ((ok_stream, err_stream), capability) =
                        source::create_source_simple(source_config, source);

                    error_collections.push(
                        err_stream
                            .map(DataflowError::SourceError)
                            .pass_through("source-errors")
                            .as_collection(),
                    );

                    (ok_stream.as_collection(), capability)
                } else {
                    let ((ok_source, ts_bindings, err_source), capability) = match connector {
                        ExternalSourceConnector::Kafka(_) => {
                            let ((ok, ts, err), cap) = source::create_source::<_, KafkaSourceReader>(
                                source_config,
                                &connector,
                                source_persist_config
                                    .as_ref()
                                    .map(|config| config.bindings_config.clone()),
                            );
                            ((SourceType::Delimited(ok), ts, err), cap)
                        }
                        ExternalSourceConnector::Kinesis(_) => {
                            let ((ok, ts, err), cap) =
                                source::create_source::<_, KinesisSourceReader>(
                                    source_config,
                                    &connector,
                                    source_persist_config
                                        .as_ref()
                                        .map(|config| config.bindings_config.clone()),
                                );
                            ((SourceType::Delimited(ok), ts, err), cap)
                        }
                        ExternalSourceConnector::S3(_) => {
                            let ((ok, ts, err), cap) = source::create_source::<_, S3SourceReader>(
                                source_config,
                                &connector,
                                source_persist_config
                                    .as_ref()
                                    .map(|config| config.bindings_config.clone()),
                            );
                            ((SourceType::ByteStream(ok), ts, err), cap)
                        }
                        ExternalSourceConnector::File(_) | ExternalSourceConnector::AvroOcf(_) => {
                            let ((ok, ts, err), cap) = source::create_source::<_, FileSourceReader>(
                                source_config,
                                &connector,
                                source_persist_config
                                    .as_ref()
                                    .map(|config| config.bindings_config.clone()),
                            );
                            ((SourceType::ByteStream(ok), ts, err), cap)
                        }
                        ExternalSourceConnector::Postgres(_) => unreachable!(),
                        ExternalSourceConnector::PubNub(_) => unreachable!(),
                    };

                    // Include any source errors.
                    error_collections.push(
                        err_source
                            .map(DataflowError::SourceError)
                            .pass_through("source-errors")
                            .as_collection(),
                    );

                    let (stream, errors) = {
                        let (key_desc, _) = encoding.desc().expect("planning has verified this");
                        let (key_encoding, value_encoding) = match encoding {
                            SourceDataEncoding::KeyValue { key, value } => (Some(key), value),
                            SourceDataEncoding::Single(value) => (None, value),
                        };

                        // TODO(petrosagg): remove this inconsistency once INCLUDE (offset)
                        // syntax is implemented
                        //
                        // Default metadata is mostly configured in planning, but we need it here for upsert
                        let default_metadata = provide_default_metadata(&envelope, &value_encoding);

                        let metadata_columns = connector.metadata_column_types(default_metadata);

                        // CDCv2 can't quite be slotted in to the below code, since it determines
                        // its own diffs/timestamps as part of decoding.
                        if let SourceEnvelope::CdcV2 = &envelope {
                            let AvroEncoding {
                                schema,
                                schema_registry_config,
                                confluent_wire_format,
                            } = match value_encoding {
                                DataEncoding::Avro(enc) => enc,
                                _ => unreachable!("Attempted to create non-Avro CDCv2 source"),
                            };
                            let ok_source = match ok_source {
                                SourceType::Delimited(s) => s,
                                _ => unreachable!("Attempted to create non-delimited CDCv2 source"),
                            };
                            // TODO(petrosagg): this should move to the envelope section below and
                            // made to work with a stream of Rows instead of decoding Avro directly
                            let (oks, token) = decode_cdcv2(
                                &ok_source,
                                &schema,
                                schema_registry_config,
                                confluent_wire_format,
                            );
                            tokens
                                .additional_tokens
                                .entry(src_id)
                                .or_insert_with(Vec::new)
                                .push(Rc::new(token));
                            (oks, None)
                        } else {
                            let (results, extra_token) = match ok_source {
                                SourceType::Delimited(source) => render_decode_delimited(
                                    &source,
                                    key_encoding,
                                    value_encoding,
                                    &self.debug_name,
                                    &envelope,
                                    metadata_columns,
                                    &mut linear_operators,
                                    fast_forwarded,
                                    render_state.metrics.clone(),
                                ),
                                SourceType::ByteStream(source) => render_decode(
                                    &source,
                                    value_encoding,
                                    &self.debug_name,
                                    &envelope,
                                    metadata_columns,
                                    &mut linear_operators,
                                    fast_forwarded,
                                    render_state.metrics.clone(),
                                ),
                            };
                            if let Some(tok) = extra_token {
                                tokens
                                    .additional_tokens
                                    .entry(src_id)
                                    .or_insert_with(Vec::new)
                                    .push(Rc::new(tok));
                            }

                            // render envelopes
                            match &envelope {
                                SourceEnvelope::Debezium(dedupe_strategy, mode) => {
                                    // TODO: this needs to happen separately from trackstate
                                    let results = match mode {
                                        DebeziumMode::Upsert => {
                                            let mut trackstate = (
                                                HashMap::new(),
                                                render_state.metrics.debezium_upsert_count_for(
                                                    src_id,
                                                    self.dataflow_id,
                                                ),
                                            );
                                            results.flat_map(move |data| {
                                                let DecodeResult {
                                                    key,
                                                    value,
                                                    metadata,
                                                    position: _,
                                                } = data;
                                                let (keys, metrics) = &mut trackstate;
                                                value.map(|value| match key {
                                                    None => Err(DecodeError::Text(
                                                        "All upsert keys should decode to a value."
                                                            .into(),
                                                    )),
                                                    Some(Err(e)) => Err(e),
                                                    Some(Ok(key)) => rewrite_for_upsert(
                                                        value, metadata, keys, key, metrics,
                                                    ),
                                                })
                                            })
                                        }
                                        DebeziumMode::Plain => results.flat_map(|data| {
                                            data.value.map(|res| {
                                                res.map(|mut val| {
                                                    val.extend(data.metadata.into_iter());
                                                    val
                                                })
                                            })
                                        }),
                                    };

                                    let (stream, errors) = results.ok_err(ResultExt::err_into);
                                    let stream = stream.pass_through("decode-ok").as_collection();
                                    let errors =
                                        errors.pass_through("decode-errors").as_collection();

                                    let dbz_key_indices = if let Some(key_desc) = key_desc {
                                        let fields = match &src.bare_desc.typ().column_types[0]
                                            .scalar_type
                                        {
                                            ScalarType::Record { fields, .. } => fields.clone(),
                                            _ => unreachable!(),
                                        };
                                        let row_desc = RelationDesc::from_names_and_types(fields);
                                        // these must be available because the DDL parsing logic already
                                        // checks this and bails in case the key is not correct
                                        Some(dataflow_types::sources::match_key_indices(&key_desc, &row_desc).expect("Invalid key schema, this indicates a bug in Materialize"))
                                    } else {
                                        None
                                    };

                                    let stream = dedupe_strategy.clone().render(
                                        stream,
                                        self.debug_name.to_string(),
                                        scope.index(),
                                        // Debezium decoding has produced two extra fields, with the
                                        // dedupe information and the upstream time in millis
                                        src.bare_desc.arity() + 2,
                                        dbz_key_indices,
                                    );

                                    (stream, Some(errors))
                                }
                                SourceEnvelope::Upsert(_key_envelope) => {
                                    // TODO: use the key envelope to figure out when to add keys.
                                    // The opeator currently does it unconditionally
                                    let upsert_operator_name = format!("{}-upsert", source_name);

                                    let key_arity = key_desc.expect("SourceEnvelope::Upsert to require SourceDataEncoding::KeyValue").arity();

                                    let (upsert_ok, upsert_err) = super::upsert::upsert(
                                        &upsert_operator_name,
                                        &results,
                                        self.as_of_frontier.clone(),
                                        &mut linear_operators,
                                        key_arity,
                                        src.bare_desc.typ().arity(),
                                        source_persist_config
                                            .as_ref()
                                            .map(|config| config.upsert_config.clone()),
                                    );

                                    // When persistence is enabled we need to seal up both the
                                    // timestamp bindings and the upsert state. Otherwise, just
                                    // pass through.
                                    let (upsert_ok, upsert_err) = if let Some(
                                        source_persist_config,
                                    ) = source_persist_config
                                    {
                                        let sealed_upsert = upsert_ok.conditional_seal(
                                            &source_name,
                                            &ts_bindings,
                                            source_persist_config.upsert_config.write_handle,
                                            source_persist_config.bindings_config.write_handle,
                                        );

                                        // Don't send data forward to "dataflow" until the frontier
                                        // tells us that we both persisted and sealed it.
                                        //
                                        // This is the most pessimistic style of concurrency
                                        // control, an alternatie would be to not hold data but
                                        // only hold the frontier (which is what the persist/seal
                                        // operators do).
                                        let sealed_upsert =
                                            sealed_upsert.await_frontier(&source_name);

                                        (sealed_upsert, upsert_err)
                                    } else {
                                        (upsert_ok, upsert_err)
                                    };

                                    (upsert_ok.as_collection(), Some(upsert_err.as_collection()))
                                }
                                SourceEnvelope::None(key_envelope) => {
                                    let results = append_metadata_to_value(results);

                                    let (stream, errors) =
                                        flatten_results_prepend_keys(key_envelope, results)
                                            .ok_err(ResultExt::err_into);
                                    let stream = stream.pass_through("decode-ok").as_collection();
                                    let errors =
                                        errors.pass_through("decode-errors").as_collection();
                                    (stream, Some(errors))
                                }
                                SourceEnvelope::CdcV2 => unreachable!(),
                            }
                        }
                    };

                    if let Some(errors) = errors {
                        error_collections.push(errors);
                    }

                    (stream, capability)
                };

                // Force a shuffling of data in case sources are not uniformly distributed.
                use timely::dataflow::operators::Exchange;
                collection = collection.inner.exchange(|x| x.hashed()).as_collection();

                // Implement source filtering and projection.
                // At the moment this is strictly optional, but we perform it anyhow
                // to demonstrate the intended use.
                if let Some(operators) = linear_operators {
                    // Apply predicates and insert dummy values into undemanded columns.
                    let (collection2, errors) =
                        collection
                            .inner
                            .flat_map_fallible("SourceLinearOperators", {
                                // Produce an executable plan reflecting the linear operators.
                                let source_type = src.bare_desc.typ();
                                let linear_op_mfp =
                                    crate::render::plan::linear_to_mfp(operators, source_type)
                                        .into_plan()
                                        .unwrap_or_else(|e| panic!("{}", e));
                                // Reusable allocation for unpacking datums.
                                let mut datum_vec = repr::DatumVec::new();
                                let mut row_builder = Row::default();
                                // Closure that applies the linear operators to each `input_row`.
                                move |(input_row, time, diff)| {
                                    let arena = repr::RowArena::new();
                                    let mut datums_local = datum_vec.borrow_with(&input_row);
                                    linear_op_mfp.evaluate(
                                        &mut datums_local,
                                        &arena,
                                        time,
                                        diff,
                                        &mut row_builder,
                                    )
                                }
                            });

                    collection = collection2.as_collection();
                    error_collections.push(errors.as_collection());
                };

                // Flatten the error collections.
                let mut err_collection = match error_collections.len() {
                    0 => Collection::empty(scope),
                    1 => error_collections.pop().unwrap(),
                    _ => collection::concatenate(scope, error_collections),
                };

                // Apply `as_of` to each timestamp.
                match &envelope {
                    SourceEnvelope::Upsert(_) => {}
                    _ => {
                        let as_of_frontier1 = self.as_of_frontier.clone();
                        collection = collection
                            .inner
                            .map_in_place(move |(_, time, _)| {
                                time.advance_by(as_of_frontier1.borrow())
                            })
                            .as_collection();

                        let as_of_frontier2 = self.as_of_frontier.clone();
                        err_collection = err_collection
                            .inner
                            .map_in_place(move |(_, time, _)| {
                                time.advance_by(as_of_frontier2.borrow())
                            })
                            .as_collection();
                    }
                }

                // Consolidate the results, as there may now be cancellations.
                use differential_dataflow::operators::consolidate::ConsolidateStream;
                collection = collection.consolidate_stream();

                // Introduce the stream by name, as an unarranged collection.
                self.insert_id(
                    Id::Global(src_id),
                    crate::render::CollectionBundle::from_collections(collection, err_collection),
                );

                let token = Rc::new(capability);
                tokens.source_tokens.insert(src_id, token.clone());

                // We also need to keep track of this mapping globally to activate sources
                // on timestamp advancement queries
                render_state
                    .ts_source_mapping
                    .entry(orig_id)
                    .or_insert_with(Vec::new)
                    .push(Rc::downgrade(&token));
            }
        }
    }
}

/// Configuration for persistent sources.
///
/// Note: This is quite tailored to Kafka Upsert sources for now, but we can change/extend before
/// we add new types of sources/envelopes.
///
/// `ST` is the source timestamp, while `AT` is the timestamp that is assigned based on timestamp
/// bindings.
#[derive(Clone)]
pub struct PersistentSourceConfig<K: Codec, V: Codec, ST: Codec, AT: Codec> {
    bindings_config: PersistentTimestampBindingsConfig<ST, AT>, // wrong ordering... AT-ST
    upsert_config: PersistentUpsertConfig<K, V>,
}

impl<K: Codec, V: Codec, ST: Codec, AT: Codec> PersistentSourceConfig<K, V, ST, AT> {
    /// Creates a new [`PersistentSourceConfig`] from the given parts.
    pub fn new(
        bindings_config: PersistentTimestampBindingsConfig<ST, AT>,
        upsert_config: PersistentUpsertConfig<K, V>,
    ) -> Self {
        PersistentSourceConfig {
            bindings_config,
            upsert_config,
        }
    }
}

fn get_persist_config(
    source_id: &SourceInstanceId,
    persist_desc: SourcePersistDesc,
    persist_client: &mut persist::client::RuntimeClient,
) -> PersistentSourceConfig<
    Result<Row, DecodeError>,
    Result<Row, DecodeError>,
    SourceTimestamp,
    AssignedTimestamp,
> {
    // TODO: Ensure that we only render one materialized source when persistence is enabled. We can
    // use https://github.com/MaterializeInc/materialize/pull/8522, which has most of the plumbing.

    let (bindings_write, bindings_read) = persist_client
        .create_or_load::<SourceTimestamp, AssignedTimestamp>(
            &persist_desc.timestamp_bindings_stream.name,
        );

    let (data_write, data_read) = persist_client
        .create_or_load::<Result<Row, DecodeError>, Result<Row, DecodeError>>(
            &persist_desc.primary_stream.name,
        );

    let bindings_seal_ts = persist_desc.timestamp_bindings_stream.upper_seal_ts;
    let data_seal_ts = persist_desc.primary_stream.upper_seal_ts;

    tracing::debug!(
            "Persistent collections for source {}: {:?} and {:?}. Upper seal timestamps: (bindings: {}, data: {}).",
            source_id,
            persist_desc.primary_stream.name,
            persist_desc.timestamp_bindings_stream.name,
            bindings_seal_ts,
            data_seal_ts,
        );

    let bindings_config = PersistentTimestampBindingsConfig::new(
        bindings_seal_ts,
        data_seal_ts,
        bindings_read,
        bindings_write,
    );
    let upsert_config = PersistentUpsertConfig::new(data_seal_ts, data_read, data_write);

    PersistentSourceConfig::new(bindings_config, upsert_config)
}

/// After handling metadata insertion, we split streams into key/value parts for convenience
#[derive(Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
struct KV {
    key: Option<Result<Row, DecodeError>>,
    val: Option<Result<Row, DecodeError>>,
}

fn append_metadata_to_value<G>(
    results: timely::dataflow::Stream<G, DecodeResult>,
) -> timely::dataflow::Stream<G, KV>
where
    G: Scope<Timestamp = Timestamp>,
{
    results.map(move |res| {
        let val = res.value.map(|val_result| {
            val_result.map(|mut val| {
                if !res.metadata.is_empty() {
                    val.extend(res.metadata.into_iter());
                }

                val
            })
        });

        KV { val, key: res.key }
    })
}

/// Convert from streams of [`DecodeResult`] to Rows, inserting the Key according to [`KeyEnvelope`]
fn flatten_results_prepend_keys<G>(
    key_envelope: &KeyEnvelope,
    results: timely::dataflow::Stream<G, KV>,
) -> timely::dataflow::Stream<G, Result<Row, DecodeError>>
where
    G: Scope<Timestamp = Timestamp>,
{
    match key_envelope {
        KeyEnvelope::None => results.flat_map(|KV { val, .. }| val),
        KeyEnvelope::Flattened | KeyEnvelope::LegacyUpsert => results
            .flat_map(raise_key_value_errors)
            .map(move |maybe_kv| {
                maybe_kv.map(|(mut key, value)| {
                    key.extend_by_row(&value);
                    key
                })
            }),
        KeyEnvelope::Named(_) => {
            results
                .flat_map(raise_key_value_errors)
                .map(move |maybe_kv| {
                    maybe_kv.map(|(mut key, value)| {
                        // Named semantics rename a key that is a single column, and encode a
                        // multi-column field as a struct with that name
                        let row = if key.iter().nth(1).is_none() {
                            key.extend_by_row(&value);
                            key
                        } else {
                            let mut new_row = Row::default();
                            new_row.push_list(key.iter());
                            new_row.extend_by_row(&value);
                            new_row
                        };
                        row
                    })
                })
        }
    }
}

/// Handle possibly missing key or value portions of messages
fn raise_key_value_errors(KV { key, val }: KV) -> Option<Result<(Row, Row), DecodeError>> {
    match (key, val) {
        (Some(key), Some(value)) => match (key, value) {
            (Ok(key), Ok(value)) => Some(Ok((key, value))),
            // always prioritize the value error if either or both have an error
            (_, Err(e)) => Some(Err(e)),
            (Err(e), _) => Some(Err(e)),
        },
        (None, None) => None,
        _ => Some(Err(DecodeError::Text(
            "Key and/or Value are not present for message".to_string(),
        ))),
    }
}
