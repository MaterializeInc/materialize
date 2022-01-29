// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sources.

use std::rc::Rc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{collection, AsCollection, Collection, Hashable};
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::{Concat, Map, OkErr, UnorderedInput};
use timely::dataflow::{Scope, Stream};

use persist::client::StreamWriteHandle;
use persist::operators::source::PersistedSource;
use persist::operators::stream::{AwaitFrontier, Seal};
use persist::operators::upsert::PersistentUpsertConfig;
use persist_types::Codec;

use dataflow_types::sources::{encoding::*, persistence::*, *};
use dataflow_types::*;
use expr::{GlobalId, PartitionId, SourceInstanceId};
use ore::now::NowFn;
use repr::{Diff, Row, Timestamp};

use crate::decode::decode_cdcv2;
use crate::decode::render_decode;
use crate::decode::render_decode_delimited;
use crate::logging::materialized::Logger;
use crate::operator::{CollectionExt, StreamExt};
use crate::render::envelope_none;
use crate::render::envelope_none::PersistentEnvelopeNoneConfig;
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

/// Imports a table (non-durable, local source of input).
pub(crate) fn import_table<G>(
    as_of_frontier: &timely::progress::Antichain<repr::Timestamp>,
    storage_state: &mut crate::render::StorageState,
    scope: &mut G,
    persisted_name: Option<String>,
) -> (
    LocalInput,
    crate::render::CollectionBundle<G, Row, Timestamp>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    let ((handle, capability), ok_stream, err_collection) = {
        let ((handle, capability), ok_stream) = scope.new_unordered_input();
        let err_collection = Collection::empty(scope);
        ((handle, capability), ok_stream, err_collection)
    };

    // A local "source" is either fed by a local input handle, or by reading from a
    // `persisted_source()`.
    //
    // For non-persisted sources, values that are to be inserted are sent from the
    // coordinator and pushed into the handle.
    //
    // For persisted sources, the coordinator only writes new values to a persistent
    // stream. These values will then "show up" here because we read from the same
    // persistent stream.
    let (ok_stream, err_collection) = match (&mut storage_state.persist, persisted_name) {
        (Some(persist), Some(stream_name)) => {
            let (_write, read) = persist.create_or_load(&stream_name);
            let (persist_ok_stream, persist_err_stream) = scope
                .persisted_source(read, &as_of_frontier)
                .ok_err(|x| match x {
                    (Ok(kv), ts, diff) => Ok((kv, ts, diff)),
                    (Err(err), ts, diff) => Err((err, ts, diff)),
                });
            let (persist_ok_stream, decode_err_stream) =
                persist_ok_stream.ok_err(|((row, ()), ts, diff)| Ok((row, ts, diff)));
            let persist_err_collection = persist_err_stream
                .concat(&decode_err_stream)
                .map(move |(err, ts, diff)| {
                    let err =
                        SourceError::new(stream_name.clone(), SourceErrorDetails::Persistence(err));
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

    let local_input = LocalInput { handle, capability };
    let as_of_frontier = as_of_frontier.clone();
    let ok_collection = ok_stream
        .map_in_place(move |(_, mut time, _)| {
            time.advance_by(as_of_frontier.borrow());
        })
        .as_collection();

    let collection_bundle =
        crate::render::CollectionBundle::from_collections(ok_collection, err_collection);

    (local_input, collection_bundle)
}

/// Constructs a `CollectionBundle` and tokens from source arguments.
///
/// The first return value is the collection bundle, and the second a pair of source and additional
/// tokens, that are used to control the demolition of the source.
pub(crate) fn import_source<G>(
    dataflow_debug_name: &String,
    dataflow_id: usize,
    as_of_frontier: &timely::progress::Antichain<repr::Timestamp>,
    SourceInstanceDesc {
        description: src,
        operators: mut linear_operators,
        persist,
    }: SourceInstanceDesc,
    storage_state: &mut crate::render::StorageState,
    scope: &mut G,
    materialized_logging: Option<Logger>,
    src_id: GlobalId,
    now: NowFn,
    base_metrics: &SourceBaseMetrics,
) -> (
    crate::render::CollectionBundle<G, Row, Timestamp>,
    (
        Rc<Option<crate::source::SourceToken>>,
        Vec<Rc<dyn std::any::Any>>,
    ),
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // Blank out trivial linear operators.
    if let Some(operator) = &linear_operators {
        if operator.is_trivial(src.desc.arity()) {
            linear_operators = None;
        }
    }

    // Tokens that we should return from the method.
    let mut additional_tokens: Vec<Rc<dyn std::any::Any>> = Vec::new();

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
            let (local_input, collection_bundle) =
                import_table(as_of_frontier, storage_state, scope, persisted_name);
            storage_state.local_inputs.insert(src_id, local_input);

            // TODO(mcsherry): Local tables are a special non-source we should relocate.
            (collection_bundle, (Rc::new(None), Vec::new()))
        }

        SourceConnector::External {
            connector,
            encoding,
            envelope,
            ts_frequency,
            timeline: _,
        } => {
            // TODO(benesch): this match arm is hard to follow. Refactor.

            // This uid must be unique across all different instantiations of a source
            let uid = SourceInstanceId {
                source_id: src_id,
                dataflow_id,
            };

            // All sources should push their various error streams into this vector,
            // whose contents will be concatenated and inserted along the collection.
            let mut error_collections = Vec::<Collection<_, _>>::new();

            let source_persist_config = match (persist, storage_state.persist.as_mut()) {
                (Some(persist_desc), Some(persist)) => {
                    Some(get_persist_config(&uid, persist_desc, persist))
                }
                _ => None,
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

            let timestamp_histories = storage_state
                .ts_histories
                .get(&src_id)
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
                        let ((ok, ts, err), cap) = source::create_source::<_, KinesisSourceReader>(
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
                        additional_tokens.push(Rc::new(token));
                        (oks, None)
                    } else {
                        let (results, extra_token) = match ok_source {
                            SourceType::Delimited(source) => render_decode_delimited(
                                &source,
                                key_encoding,
                                value_encoding,
                                dataflow_debug_name,
                                &envelope,
                                metadata_columns,
                                &mut linear_operators,
                                storage_state.metrics.clone(),
                            ),
                            SourceType::ByteStream(source) => render_decode(
                                &source,
                                value_encoding,
                                dataflow_debug_name,
                                metadata_columns,
                                &mut linear_operators,
                                storage_state.metrics.clone(),
                            ),
                        };
                        if let Some(tok) = extra_token {
                            additional_tokens.push(Rc::new(tok));
                        }

                        // render envelopes
                        match &envelope {
                            SourceEnvelope::Debezium(dbz_envelope) => {
                                let (stream, errors) = super::debezium::render(
                                    dbz_envelope,
                                    &results,
                                    dataflow_debug_name.clone(),
                                    storage_state.metrics.clone(),
                                    src_id,
                                    dataflow_id,
                                )
                                .inner
                                .ok_err(|(res, time, diff)| match res {
                                    Ok(v) => Ok((v, time, diff)),
                                    Err(e) => Err((e, time, diff)),
                                });
                                (stream.as_collection(), Some(errors.as_collection()))
                            }
                            SourceEnvelope::Upsert(_key_envelope) => {
                                // TODO: use the key envelope to figure out when to add keys.
                                // The opeator currently does it unconditionally
                                let upsert_operator_name = format!("{}-upsert", source_name);

                                let key_arity = key_desc.expect("SourceEnvelope::Upsert to require SourceDataEncoding::KeyValue").arity();

                                let (upsert_ok, upsert_err) = super::upsert::upsert(
                                    &upsert_operator_name,
                                    &results,
                                    as_of_frontier.clone(),
                                    &mut linear_operators,
                                    key_arity,
                                    src.desc.typ().arity(),
                                    source_persist_config
                                        .as_ref()
                                        .map(|config| config.upsert_config().clone()),
                                );

                                // When persistence is enabled we need to seal up both the
                                // timestamp bindings and the upsert state. Otherwise, just
                                // pass through.
                                let (upsert_ok, upsert_err) = if let Some(source_persist_config) =
                                    source_persist_config
                                {
                                    let bindings_handle =
                                        source_persist_config.bindings_config.write_handle.clone();
                                    let upsert_state_handle =
                                        source_persist_config.upsert_config().write_handle.clone();

                                    // Don't send data forward to "dataflow" until the frontier
                                    // tells us that we both persisted and sealed it.
                                    //
                                    // This is the most pessimistic style of concurrency
                                    // control, an alternatie would be to not hold data but
                                    // only hold the frontier (which is what the persist/seal
                                    // operators do).
                                    let sealed_upsert = seal_and_await(
                                        &upsert_ok,
                                        &ts_bindings,
                                        upsert_state_handle,
                                        bindings_handle,
                                        &source_name,
                                    );

                                    (sealed_upsert, upsert_err)
                                } else {
                                    (upsert_ok, upsert_err)
                                };

                                (upsert_ok.as_collection(), Some(upsert_err.as_collection()))
                            }
                            SourceEnvelope::None(key_envelope) => {
                                let results = append_metadata_to_value(results);

                                let flattened_stream =
                                    flatten_results_prepend_keys(key_envelope, results);

                                let flattened_stream = flattened_stream.pass_through("decode");

                                // When persistence is enabled we need to persist and seal up
                                // both the timestamp bindings and the data. Otherwise, just
                                // pass through.
                                let (flattened_stream, persist_errs) =
                                    if let Some(source_persist_config) = source_persist_config {
                                        let bindings_config =
                                            &source_persist_config.bindings_config;
                                        let envelope_config =
                                            source_persist_config.envelope_none_config();

                                        let (flattened_stream, persist_errs) =
                                            envelope_none::persist_and_replay(
                                                &source_name,
                                                &flattened_stream,
                                                as_of_frontier,
                                                envelope_config.clone(),
                                            );

                                        // Don't send data forward to "dataflow" until the frontie
                                        // tells us that we both persisted and sealed it.
                                        //
                                        // This is the most pessimistic style of concurrency
                                        // control, an alternatie would be to not hold data but
                                        // only hold the frontier (which is what the persist/seal
                                        // operators do).
                                        let sealed_flattened_stream = seal_and_await(
                                            &flattened_stream,
                                            &ts_bindings,
                                            envelope_config.write_handle.clone(),
                                            bindings_config.write_handle.clone(),
                                            &source_name,
                                        );

                                        // NOTE: Persistence errors don't go through the same
                                        // pipeline as data, i.e. no sealing and awaiting.
                                        (sealed_flattened_stream, persist_errs)
                                    } else {
                                        (flattened_stream, operator::empty(scope))
                                    };

                                // TODO: Maybe we should finally move this to some central
                                // place and re-use. There seem to be enough instances of this
                                // by now.
                                fn split_ok_err(
                                    x: (Result<Row, DecodeError>, u64, isize),
                                ) -> Result<(Row, u64, isize), (DataflowError, u64, isize)>
                                {
                                    match x {
                                        (Ok(row), ts, diff) => Ok((row, ts, diff)),
                                        (Err(err), ts, diff) => Err((err.into(), ts, diff)),
                                    }
                                }

                                let (stream, errors) = flattened_stream.ok_err(split_ok_err);

                                let errors = errors.concat(&persist_errs);

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
                            let source_type = src.desc.typ();
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
                    let as_of_frontier1 = as_of_frontier.clone();
                    collection = collection
                        .inner
                        .map_in_place(move |(_, time, _)| time.advance_by(as_of_frontier1.borrow()))
                        .as_collection();

                    let as_of_frontier2 = as_of_frontier.clone();
                    err_collection = err_collection
                        .inner
                        .map_in_place(move |(_, time, _)| time.advance_by(as_of_frontier2.borrow()))
                        .as_collection();
                }
            }

            // Consolidate the results, as there may now be cancellations.
            use differential_dataflow::operators::consolidate::ConsolidateStream;
            collection = collection.consolidate_stream();

            // Introduce the stream by name, as an unarranged collection.
            let collection_bundle =
                crate::render::CollectionBundle::from_collections(collection, err_collection);

            let source_token = Rc::new(capability);

            // We also need to keep track of this mapping globally to activate sources
            // on timestamp advancement queries
            storage_state
                .ts_source_mapping
                .entry(src_id)
                .or_insert_with(Vec::new)
                .push(Rc::downgrade(&source_token));

            // Return the source token for capability manipulation, and any additional tokens.
            (collection_bundle, (source_token, additional_tokens))
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
    envelope_config: PersistentEnvelopeConfig<K, V>,
}

impl<K: Codec, V: Codec, ST: Codec, AT: Codec> PersistentSourceConfig<K, V, ST, AT> {
    /// Creates a new [`PersistentSourceConfig`] from the given parts.
    pub fn new(
        bindings_config: PersistentTimestampBindingsConfig<ST, AT>,
        envelope_config: PersistentEnvelopeConfig<K, V>,
    ) -> Self {
        PersistentSourceConfig {
            bindings_config,
            envelope_config,
        }
    }

    // NOTE: These two show our problematic use of two hierarchies of enums: we keep an envelope
    // config in the `SourceDesc`, but we roughly mirror that hierachy in the `SourcePersistDesc`.

    pub fn upsert_config(&self) -> &PersistentUpsertConfig<K, V> {
        match self.envelope_config {
            PersistentEnvelopeConfig::Upsert(ref config) => config,
            PersistentEnvelopeConfig::EnvelopeNone(_) => {
                panic!("source is not an ENVELOPE UPSERT source but an ENVELOPE NONE source")
            }
        }
    }

    pub fn envelope_none_config(&self) -> &PersistentEnvelopeNoneConfig<V> {
        match self.envelope_config {
            PersistentEnvelopeConfig::Upsert(_) => {
                panic!("source is not an ENVELOPE NONE source but an ENVELOPE UPSERT source")
            }
            PersistentEnvelopeConfig::EnvelopeNone(ref config) => config,
        }
    }
}

/// Configuration for the envelope-specific part of persistent sources.
#[derive(Clone)]
pub enum PersistentEnvelopeConfig<K: Codec, V: Codec> {
    Upsert(PersistentUpsertConfig<K, V>),
    EnvelopeNone(PersistentEnvelopeNoneConfig<V>),
}

// TODO: Now it gets really obvious how the current way of structuring the persist information is
// not that good.
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

    match persist_desc.envelope_desc {
        EnvelopePersistDesc::Upsert => {
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

            PersistentSourceConfig::new(
                bindings_config,
                PersistentEnvelopeConfig::Upsert(upsert_config),
            )
        }
        EnvelopePersistDesc::None => {
            let (data_write, data_read) = persist_client
                .create_or_load::<Result<Row, DecodeError>, ()>(&persist_desc.primary_stream.name);

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

            let none_config =
                PersistentEnvelopeNoneConfig::new(data_seal_ts, data_read, data_write);

            PersistentSourceConfig::new(
                bindings_config,
                PersistentEnvelopeConfig::EnvelopeNone(none_config),
            )
        }
    }
}

/// Seals both the main stream and the stream of timestamp bindings, allows compaction on
/// underlying persistent streams, and awaits the seal frontier before passing on updates.
fn seal_and_await<G, D1, D2, K1, V1, K2, V2>(
    stream: &Stream<G, (D1, Timestamp, Diff)>,
    bindings_stream: &Stream<G, (D2, Timestamp, Diff)>,
    write: StreamWriteHandle<K1, V1>,
    bindings_write: StreamWriteHandle<K2, V2>,
    source_name: &str,
) -> Stream<G, (D1, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    D1: timely::Data,
    D2: timely::Data,
    K1: Codec + timely::Data,
    V1: Codec + timely::Data,
    K2: Codec + timely::Data,
    V2: Codec + timely::Data,
{
    let sealed_stream =
        stream.conditional_seal(&source_name, bindings_stream, write, bindings_write);

    // Don't send data forward to "dataflow" until the frontier
    // tells us that we both persisted and sealed it.
    //
    // This is the most pessimistic style of concurrency
    // control, an alternatie would be to not hold data but
    // only hold the frontier (which is what the persist/seal
    // operators do).
    let sealed_stream = sealed_stream.await_frontier(&source_name);

    sealed_stream
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
        // TODO(petrosagg): these errors would be better grouped under an EnvelopeError enum
        _ => Some(Err(DecodeError::Text(
            "Key or Value are not present for message".to_string(),
        ))),
    }
}
