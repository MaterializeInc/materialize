// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sources.

use std::any::Any;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{collection, AsCollection, Collection, Hashable};
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::{Concat, Map, OkErr, Probe, UnorderedInput};
use timely::dataflow::{ProbeHandle, Scope, Stream};
use tracing::debug;

use mz_persist::client::{MultiWriteHandle, StreamWriteHandle};
use mz_persist::operators::source::PersistedSource;
use mz_persist::operators::stream::{AwaitFrontier, Seal};
use mz_persist::operators::upsert::PersistentUpsertConfig;
use mz_persist_types::Codec;

use mz_dataflow_types::sources::{encoding::*, persistence::*, *};
use mz_dataflow_types::*;
use mz_expr::{GlobalId, PartitionId, SourceInstanceId};
use mz_repr::{Diff, Row, RowPacker, Timestamp};
use timely::progress::Antichain;

use crate::decode::decode_cdcv2;
use crate::decode::render_decode;
use crate::decode::render_decode_delimited;
use crate::logging::materialized::Logger;
use crate::operator::{CollectionExt, StreamExt};
use crate::render::envelope_none;
use crate::render::envelope_none::PersistentEnvelopeNoneConfig;
use crate::server::LocalInput;
use crate::server::StorageState;
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
    as_of_frontier: &timely::progress::Antichain<mz_repr::Timestamp>,
    storage_state: &mut crate::server::StorageState,
    scope: &mut G,
    id: SourceInstanceId,
    persisted_name: Option<String>,
) -> (
    LocalInput,
    (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>),
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
                    let err = SourceError::new(id, SourceErrorDetails::Persistence(err));
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
        .map_in_place(move |(_, time, _)| {
            time.advance_by(as_of_frontier.borrow());
        })
        .as_collection();

    (local_input, (ok_collection, err_collection))
}

/// Constructs a `CollectionBundle` and tokens from source arguments.
///
/// The first returned pair are the row and error collections, and the
/// second is a token that will keep the source alive as long as it is held.
pub(crate) fn import_source<G>(
    dataflow_debug_name: &String,
    dataflow_id: usize,
    as_of_frontier: &timely::progress::Antichain<mz_repr::Timestamp>,
    SourceInstanceDesc {
        description: src,
        operators: mut linear_operators,
        persist,
    }: SourceInstanceDesc,
    storage_state: &mut crate::server::StorageState,
    scope: &mut G,
    materialized_logging: Option<Logger>,
    src_id: GlobalId,
) -> (
    (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>),
    Rc<dyn std::any::Any>,
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
    let mut needed_tokens: Vec<Rc<dyn std::any::Any>> = Vec::new();

    // This uid must be unique across all different instantiations of a source
    let uid = SourceInstanceId {
        source_id: src_id,
        dataflow_id,
    };

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
            let (local_input, (ok, err)) =
                import_table(as_of_frontier, storage_state, scope, uid, persisted_name);
            storage_state.local_inputs.insert(src_id, local_input);

            // TODO(mcsherry): Local tables are a special non-source we should relocate.
            ((ok, err), Rc::new(()))
        }

        SourceConnector::External {
            connector,
            encoding,
            envelope,
            metadata_columns,
            ts_frequency,
            timeline: _,
        } => {
            // TODO(benesch): this match arm is hard to follow. Refactor.

            // All sources should push their various error streams into this vector,
            // whose contents will be concatenated and inserted along the collection.
            let mut error_collections = Vec::<Collection<_, _, Diff>>::new();

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
                now: storage_state.now.clone(),
                base_metrics: &storage_state.source_metrics,
                aws_external_id: storage_state.aws_external_id.clone(),
            };

            let (mut collection, capability) = if let ExternalSourceConnector::PubNub(
                pubnub_connector,
            ) = connector
            {
                let source = PubNubSourceReader::new(uid, pubnub_connector);
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
                let source =
                    PostgresSourceReader::new(uid, pg_connector, source_config.base_metrics);

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
                            storage_state.aws_external_id.clone(),
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
                            storage_state.aws_external_id.clone(),
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
                            storage_state.aws_external_id.clone(),
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
                            storage_state.aws_external_id.clone(),
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
                    let (key_encoding, value_encoding) = match encoding {
                        SourceDataEncoding::KeyValue { key, value } => (Some(key), value),
                        SourceDataEncoding::Single(value) => (None, value),
                    };

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
                        needed_tokens.push(Rc::new(token));
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
                                storage_state.unspecified_metrics.clone(),
                            ),
                            SourceType::ByteStream(source) => render_decode(
                                &source,
                                value_encoding,
                                dataflow_debug_name,
                                metadata_columns,
                                &mut linear_operators,
                                storage_state.unspecified_metrics.clone(),
                            ),
                        };
                        if let Some(tok) = extra_token {
                            needed_tokens.push(Rc::new(tok));
                        }

                        // render envelopes
                        match &envelope {
                            SourceEnvelope::Debezium(dbz_envelope) => {
                                let (stream, errors) = super::debezium::render(
                                    dbz_envelope,
                                    &results,
                                    dataflow_debug_name.clone(),
                                );
                                (stream.as_collection(), Some(errors.as_collection()))
                            }
                            SourceEnvelope::Upsert(upsert_envelope) => {
                                // TODO: use the key envelope to figure out when to add keys.
                                // The opeator currently does it unconditionally
                                let upsert_operator_name = format!(
                                    "{}-{}upsert",
                                    source_name,
                                    if let UpsertEnvelope {
                                        style: UpsertStyle::Debezium { .. },
                                        ..
                                    } = upsert_envelope
                                    {
                                        "debezium-"
                                    } else {
                                        ""
                                    }
                                );

                                let transformed_results =
                                    transform_keys_from_key_envelope(upsert_envelope, results);

                                let as_of_frontier = as_of_frontier.clone();

                                let source_arity = src.desc.typ().arity();

                                let (upsert_ok, upsert_err) = super::upsert::upsert(
                                    &upsert_operator_name,
                                    uid,
                                    &transformed_results,
                                    as_of_frontier,
                                    &mut linear_operators,
                                    source_arity,
                                    match upsert_envelope.style {
                                        UpsertStyle::Default(_) => source_persist_config
                                            .as_ref()
                                            .map(|config| config.upsert_config().clone()),
                                        UpsertStyle::Debezium { .. } => {
                                            // TODO(guswynn): make debezium upsert work with
                                            // persistence. See
                                            // https://github.com/MaterializeInc/materialize/issues/10699z
                                            // for more info.
                                            None
                                        }
                                    },
                                    upsert_envelope.clone(),
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
                                        &src_id,
                                        &source_name,
                                        storage_state,
                                        &mut needed_tokens,
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
                                                uid,
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
                                            &src_id,
                                            &source_name,
                                            storage_state,
                                            &mut needed_tokens,
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
                                    x: (Result<Row, DecodeError>, u64, Diff),
                                ) -> Result<(Row, u64, Diff), (DataflowError, u64, Diff)>
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
                            let mut datum_vec = mz_repr::DatumVec::new();
                            let mut row_builder = Row::default();
                            // Closure that applies the linear operators to each `input_row`.
                            move |(input_row, time, diff)| {
                                let arena = mz_repr::RowArena::new();
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

            let source_token = Rc::new(capability);

            // We also need to keep track of this mapping globally to activate sources
            // on timestamp advancement queries
            storage_state
                .ts_source_mapping
                .entry(src_id)
                .or_insert_with(Vec::new)
                .push(Rc::downgrade(&source_token));

            needed_tokens.push(source_token);

            // Return the collections and any needed tokens.
            ((collection, err_collection), Rc::new(needed_tokens))
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
    persist_client: &mut mz_persist::client::RuntimeClient,
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
            &persist_desc.timestamp_bindings_stream,
        );

    match persist_desc.envelope_desc {
        EnvelopePersistDesc::Upsert => {
            let (data_write, data_read) = persist_client
                .create_or_load::<Result<Row, DecodeError>, Result<Row, DecodeError>>(
                    &persist_desc.primary_stream,
                );

            let seal_ts = persist_desc.upper_seal_ts;

            debug!(
                "Persistent collections for source {}: {:?} and {:?}. Upper seal timestamp: {}.",
                source_id,
                persist_desc.primary_stream,
                persist_desc.timestamp_bindings_stream,
                seal_ts
            );

            let bindings_config =
                PersistentTimestampBindingsConfig::new(seal_ts, bindings_read, bindings_write);

            let upsert_config = PersistentUpsertConfig::new(seal_ts, data_read, data_write);

            PersistentSourceConfig::new(
                bindings_config,
                PersistentEnvelopeConfig::Upsert(upsert_config),
            )
        }
        EnvelopePersistDesc::None => {
            let (data_write, data_read) = persist_client
                .create_or_load::<Result<Row, DecodeError>, ()>(&persist_desc.primary_stream);

            let seal_ts = persist_desc.upper_seal_ts;

            debug!(
                "Persistent collections for source {}: {:?} and {:?}. Upper seal timestamp: {}.",
                source_id,
                persist_desc.primary_stream,
                persist_desc.timestamp_bindings_stream,
                seal_ts,
            );

            let bindings_config =
                PersistentTimestampBindingsConfig::new(seal_ts, bindings_read, bindings_write);

            let none_config = PersistentEnvelopeNoneConfig::new(seal_ts, data_read, data_write);

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
    source_id: &GlobalId,
    source_name: &str,
    storage_state: &mut StorageState,
    needed_tokens: &mut Vec<Rc<dyn std::any::Any>>,
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
    let mut seal_handle = MultiWriteHandle::new(&write);
    seal_handle
        .add_stream(&bindings_write)
        .expect("handles known to be from same persist runtime");

    let sealed_stream = stream.seal(&source_name, vec![bindings_stream.probe()], seal_handle);

    // The compaction since of persisted sources is upper-bounded by two frontiers: a) the
    // compaction that the coordinator allows, and b) the seal frontier of the involved persistent
    // collections.

    // We need to obey a) because that's what the coordinator wants, and b) because if we persisted
    // past (or up to) the seal frontier we would not be able to distinguish between valid and
    // invalid updates based on the upper seal frontier when restarting.
    //
    // We get the upper seal frontier using a probe handle, and we expect the coordinator to call
    // `allow_compaction` on the stashed `CompactionHandle` to present it's view of what should be
    // allowed.

    let mut probe: ProbeHandle<Timestamp> = ProbeHandle::new();
    sealed_stream.probe_with(&mut probe);

    let mut compaction_handle = MultiWriteHandle::new(&write);
    compaction_handle
        .add_stream(&bindings_write)
        .expect("only fails on usage errors");

    let source_token = storage_state.persisted_sources.add_source(
        source_id,
        source_name,
        probe,
        compaction_handle,
    );

    needed_tokens.push(source_token);

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

/// A `PersistedSourceManager` stores maps from global identifiers to the one rendered persistent
/// instance of a source.
///
/// NOTE: There can only ever be one running (rendered) instance of a persisted source. The manager
/// will panic when trying to add more than one instance for a given source id.
pub struct PersistedSourceManager {
    /// Handles that allow setting the compaction frontier for a persisted source.
    compaction_handles: HashMap<GlobalId, Weak<BoundedCompactionHandle>>,
}

impl PersistedSourceManager {
    pub fn new() -> Self {
        PersistedSourceManager {
            compaction_handles: HashMap::new(),
        }
    }

    /// Binds the given `frontier_probe` and `compaction_handle` to the `source_id`. This will
    /// panic if there is already a source for the given id. This returns a "token" that should
    /// be stashed in the customary places to prevent the stored information from vanishing before
    /// its time.
    pub fn add_source(
        &mut self,
        source_id: &GlobalId,
        source_name: &str,
        frontier_probe: ProbeHandle<Timestamp>,
        compaction_handle: MultiWriteHandle,
    ) -> Rc<dyn Any> {
        let compaction_handle = BoundedCompactionHandle::new(frontier_probe, compaction_handle);

        let compaction_token = Rc::new(compaction_handle);

        let previous = self
            .compaction_handles
            .insert(source_id.clone(), Rc::downgrade(&compaction_token));

        match previous {
            Some(compaction_handle) => {
                if compaction_handle.upgrade().is_some() {
                    panic!(
                        "cannot have multiple rendered instances for persisted source {} ({})",
                        source_id, source_name
                    );
                }
            }
            None => (), // All good!
        }

        compaction_token as Rc<dyn Any>
    }

    /// Enables compaction of sources associated with the identifier.
    pub fn allow_compaction(&mut self, id: GlobalId, frontier: Antichain<Timestamp>) {
        if let Some(compaction_handle) = self.compaction_handles.get(&id) {
            let compaction_handle = match compaction_handle.upgrade() {
                Some(handle) => handle,
                None => {
                    // Clean up the expired weak handle.
                    //
                    // NOTE: Right now, there is no explicit message when a rendered source
                    // instance is dropped. Sources are droppped implicitly when all their
                    // consumers are dropped, and we notice that when the token that holds
                    // onto the strong Rc to the compaction handle is dropped.
                    self.compaction_handles.remove(&id);
                    return;
                }
            };

            compaction_handle.allow_compaction(frontier);
        }
    }

    /// Removes the maintained state for source `id`.
    pub fn del_source(&mut self, id: &GlobalId) -> bool {
        self.compaction_handles.remove(&id).is_some()
    }
}

/// A handle that allows setting a compaction frontier that is bounded by an upper bound.
struct BoundedCompactionHandle {
    /// Upper frontier up to which we are not allowed to compact.
    upper_bound_probe: ProbeHandle<Timestamp>,

    /// Underlying compaction handle.
    compaction_handle: MultiWriteHandle,
}

impl BoundedCompactionHandle {
    /// Creates a new [`BoundedCompactionHandle`] that bounds compaction using the given
    /// `upper_bound_fn` and forwards compaction requests to the given `compaction_handle`
    pub fn new(
        upper_bound_probe: ProbeHandle<Timestamp>,
        compaction_handle: MultiWriteHandle,
    ) -> Self {
        Self {
            upper_bound_probe,
            compaction_handle,
        }
    }

    fn allow_compaction(&self, since: Antichain<Timestamp>) {
        let mut upper_bound = self.upper_bound_probe.with_frontier(|frontier| {
            Antichain::from(
                frontier
                    .iter()
                    .cloned()
                    // Is is **very** important to not compact up to the seal frontier, as
                    // mentioned above.
                    .map(|e| e.saturating_sub(1))
                    .collect::<Vec<_>>(),
            )
        });

        upper_bound.meet_assign(&since);

        // NOTE: We simply shoot off the `allow_compaction()` request and don't even block
        // on the response. This will mean we don't get any feedback and log errors. But
        // compactions are not necessary for correctness and there are means (metrics) for figuring
        // out if the frontier does in fact advance.
        let _ = self.compaction_handle.allow_compaction_all(upper_bound);
    }
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
                    RowPacker::for_existing_row(&mut val).extend(res.metadata.into_iter());
                }

                val
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
            style:
                UpsertStyle::Default(KeyEnvelope::LegacyUpsert | KeyEnvelope::Flattened)
                | UpsertStyle::Debezium { .. },
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
                    RowPacker::for_existing_row(&mut key).extend_by_row(&value);
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
