// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sources.

use std::cell::RefCell;
use std::rc::Rc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{collection, AsCollection, Collection, Hashable};
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::ActivateCapability;
use timely::dataflow::operators::{Map, OkErr, UnorderedInput};
use timely::dataflow::Scope;

use mz_dataflow_types::sources::{encoding::*, *};
use mz_dataflow_types::*;
use mz_expr::{PartitionId, SourceInstanceId};
use mz_repr::{Diff, GlobalId, Row, RowPacker, Timestamp};

use crate::decode::decode_cdcv2;
use crate::decode::render_decode;
use crate::decode::render_decode_delimited;
use crate::source::{
    self, DecodeResult, DelimitedValueSource, FileSourceReader, KafkaSourceReader,
    KinesisSourceReader, PostgresSourceReader, PubNubSourceReader, S3SourceReader, SourceConfig,
};
use crate::storage_state::LocalInput;
use crate::Logger;
use mz_timely_util::operator::{CollectionExt, StreamExt};

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

/// A description of a table imported by [`import_table`].
struct ImportedTable<G>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// The collection containing the records from the table.
    ok_collection: Collection<G, Row, Diff>,
    /// The collection containing errors from the etable.
    err_collection: Collection<G, DataflowError, Diff>,
    /// A handle for inserting records into the table.
    handle: UnorderedHandle<Timestamp, (Row, Timestamp, Diff)>,
    /// The initial capability associated with the insert handle.
    capability: ActivateCapability<Timestamp>,
}

/// Imports a table (non-durable, local source of input).
fn import_table<G>(
    as_of_frontier: &timely::progress::Antichain<mz_repr::Timestamp>,
    scope: &mut G,
) -> ImportedTable<G>
where
    G: Scope<Timestamp = Timestamp>,
{
    let ((handle, capability), ok_stream) = scope.new_unordered_input::<(Row, Timestamp, Diff)>();
    let err_collection = Collection::empty(scope);

    let as_of_frontier = as_of_frontier.clone();
    let ok_collection = ok_stream
        .map_in_place(move |(_, time, _)| {
            time.advance_by(as_of_frontier.borrow());
        })
        .as_collection();

    ImportedTable {
        ok_collection,
        err_collection,
        handle,
        capability,
    }
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
        arguments: SourceInstanceArguments {
            operators: mut linear_operators,
        },
    }: SourceInstanceDesc,
    storage_state: &mut crate::storage_state::StorageState,
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
        SourceConnector::Local { .. } => {
            let mut table = import_table(as_of_frontier, scope);

            let table_state = match storage_state.table_state.get_mut(&src_id) {
                Some(table_state) => table_state,
                None => panic!(
                    "table state {} missing for source creation at worker {}",
                    src_id,
                    scope.index()
                ),
            };

            // Make the new local input reflect the latest table state, then add the
            // local input to the table state.
            {
                let mut session = table.handle.session(table.capability.clone());
                for (row, time, diff) in &table_state.data {
                    let mut time = *time;
                    time.advance_by(table_state.since.borrow());
                    assert!(time >= *table.capability.time());
                    session.give((row.clone(), time, *diff));
                }
            }
            table.capability.downgrade(&table_state.upper);

            // Convert to reference counted, so that users can drop it.
            let capability = Rc::new(RefCell::new(table.capability));
            table_state.inputs.push(LocalInput {
                handle: table.handle,
                capability: Rc::downgrade(&capability),
            });

            ((table.ok_collection, table.err_collection), capability)
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
                name: source_name,
                upstream_name: connector.upstream_name().map(ToOwned::to_owned),
                id: uid,
                scope,
                // Distribute read responsibility among workers.
                active: active_read_worker,
                timestamp_histories,
                timestamp_frequency: ts_frequency,
                worker_id: scope.index(),
                worker_count: scope.peers(),
                logger: materialized_logging.clone(),
                encoding: encoding.clone(),
                now: storage_state.now.clone(),
                base_metrics: &storage_state.source_metrics,
                aws_external_id: storage_state.aws_external_id.clone(),
            };

            let (mut collection, capability) =
                if let ExternalSourceConnector::PubNub(pubnub_connector) = connector {
                    let source = PubNubSourceReader::new(uid, pubnub_connector);
                    let ((ok_stream, err_stream), capability) =
                        source::create_source_simple(source_config, source);

                    error_collections.push(
                        err_stream
                            .map(DataflowError::SourceError)
                            .pass_through("source-errors", 1)
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
                            .pass_through("source-errors", 1)
                            .as_collection(),
                    );

                    (ok_stream.as_collection(), capability)
                } else {
                    let ((ok_source, err_source), capability) = match connector {
                        ExternalSourceConnector::Kafka(_) => {
                            let ((ok, err), cap) = source::create_source::<_, KafkaSourceReader>(
                                source_config,
                                &connector,
                                storage_state.aws_external_id.clone(),
                            );
                            ((SourceType::Delimited(ok), err), cap)
                        }
                        ExternalSourceConnector::Kinesis(_) => {
                            let ((ok, err), cap) = source::create_source::<
                                _,
                                DelimitedValueSource<KinesisSourceReader>,
                            >(
                                source_config,
                                &connector,
                                storage_state.aws_external_id.clone(),
                            );
                            ((SourceType::Delimited(ok), err), cap)
                        }
                        ExternalSourceConnector::S3(_) => {
                            let ((ok, err), cap) = source::create_source::<_, S3SourceReader>(
                                source_config,
                                &connector,
                                storage_state.aws_external_id.clone(),
                            );
                            ((SourceType::ByteStream(ok), err), cap)
                        }
                        ExternalSourceConnector::File(_) | ExternalSourceConnector::AvroOcf(_) => {
                            let ((ok, err), cap) = source::create_source::<_, FileSourceReader>(
                                source_config,
                                &connector,
                                storage_state.aws_external_id.clone(),
                            );
                            ((SourceType::ByteStream(ok), err), cap)
                        }
                        ExternalSourceConnector::Postgres(_) => unreachable!(),
                        ExternalSourceConnector::PubNub(_) => unreachable!(),
                    };

                    // Include any source errors.
                    error_collections.push(
                        err_source
                            .map(DataflowError::SourceError)
                            .pass_through("source-errors", 1)
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
                                    storage_state.decode_metrics.clone(),
                                ),
                                SourceType::ByteStream(source) => render_decode(
                                    &source,
                                    value_encoding,
                                    dataflow_debug_name,
                                    metadata_columns,
                                    &mut linear_operators,
                                    storage_state.decode_metrics.clone(),
                                ),
                            };
                            if let Some(tok) = extra_token {
                                needed_tokens.push(Rc::new(tok));
                            }

                            // render envelopes
                            match &envelope {
                                SourceEnvelope::Debezium(dbz_envelope) => {
                                    let (stream, errors) = match dbz_envelope.mode.tx_metadata() {
                                        Some(tx_metadata) => {
                                            let tx_src_desc = storage_state
                                                .source_descriptions
                                                .get(&tx_metadata.tx_metadata_global_id)
                                                // N.B. tx_id is validated when constructing dbz_envelope
                                                .expect("bad tx metadata spec")
                                                .clone();
                                            // TODO(#11667): reuse the existing arrangement if it exists
                                            let ((tx_source_ok, tx_source_err), tx_token) =
                                                import_source(
                                                    dataflow_debug_name,
                                                    dataflow_id,
                                                    as_of_frontier,
                                                    SourceInstanceDesc {
                                                        description: tx_src_desc,
                                                        arguments: SourceInstanceArguments {
                                                            operators: None,
                                                        },
                                                    },
                                                    storage_state,
                                                    scope,
                                                    materialized_logging,
                                                    tx_metadata.tx_metadata_global_id,
                                                );
                                            needed_tokens.push(tx_token);
                                            error_collections.push(tx_source_err);

                                            super::debezium::render_tx(
                                                src_id,
                                                dbz_envelope,
                                                &results,
                                                tx_source_ok,
                                                dataflow_debug_name.clone(),
                                            )
                                        }
                                        None => super::debezium::render(
                                            dbz_envelope,
                                            &results,
                                            dataflow_debug_name.clone(),
                                        ),
                                    };
                                    (stream.as_collection(), Some(errors.as_collection()))
                                }
                                SourceEnvelope::Upsert(upsert_envelope) => {
                                    // TODO: use the key envelope to figure out when to add keys.
                                    // The operator currently does it unconditionally
                                    let transformed_results =
                                        transform_keys_from_key_envelope(upsert_envelope, results);

                                    let as_of_frontier = as_of_frontier.clone();

                                    let source_arity = src.desc.typ().arity();

                                    let (upsert_ok, upsert_err) = super::upsert::upsert(
                                        &transformed_results,
                                        as_of_frontier,
                                        &mut linear_operators,
                                        source_arity,
                                        upsert_envelope.clone(),
                                    );

                                    (upsert_ok.as_collection(), Some(upsert_err.as_collection()))
                                }
                                SourceEnvelope::None(key_envelope) => {
                                    let results = append_metadata_to_value(results);

                                    let flattened_stream =
                                        flatten_results_prepend_keys(key_envelope, results);

                                    let flattened_stream =
                                        flattened_stream.pass_through("decode", 1);

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
                                mz_dataflow_types::plan::linear_to_mfp(operators, source_type)
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
    G: Scope,
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
