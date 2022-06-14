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

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::consolidate::ConsolidateStream;
use differential_dataflow::{collection, AsCollection, Collection, Hashable};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::{Exchange, Map, OkErr};
use timely::dataflow::Scope;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_dataflow_types::sources::{encoding::*, *};
use mz_dataflow_types::*;
use mz_expr::PartitionId;
use mz_repr::{Datum, Diff, Row, RowPacker, Timestamp};

use crate::decode::{render_decode, render_decode_cdcv2, render_decode_delimited};
use crate::source::persist_source;
use crate::source::{
    self, DecodeResult, DelimitedValueSource, KafkaSourceReader, KinesisSourceReader,
    PostgresSourceReader, PubNubSourceReader, RawSourceCreationConfig, S3SourceReader,
    SourceOutput,
};
use mz_timely_util::operator::{CollectionExt, StreamExt};

/// A type-level enum that holds one of two types of sources depending on their message type
///
/// This enum puts no restrictions to the generic parameters of the variants since it only serves
/// as a type-level enum.
enum SourceType<Delimited, ByteStream, RowSource, AppendRowSource> {
    /// A delimited source
    Delimited(Delimited),
    /// A bytestream source
    ByteStream(ByteStream),
    /// A source that produces Row's natively,
    /// and skips any `render_decode` stream
    /// adapters, and can produce
    /// retractions
    Row(RowSource),
    /// A source that produces Row's natively,
    /// that are purely appended.
    AppendRow(AppendRowSource),
}

/// _Renders_ complete _differential_ [`Collection`]s
/// that represent the final source and its errors
/// as requested by the original `CREATE SOURCE` statement,
/// encapsulated in the passed [`SourceInstanceDesc`].
///
/// The first element in the returned tuple is the pair of [`Collection`]s,
/// the second is a type-erased token that will keep the source
/// alive as long as it is not dropped.
///
/// This function is intended to implement the recipe described here:
/// <https://github.com/MaterializeInc/materialize/pull/12109>
// TODO(guswynn): Link to merged document
pub fn render_source<G>(
    scope: &mut G,
    dataflow_debug_name: &String,
    ingestion: IngestionDescription<CollectionMetadata, Timestamp>,
    mut linear_operators: Option<LinearOperator>,
    storage_state: &mut crate::storage_state::StorageState,
) -> (
    (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>),
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // Blank out trivial linear operators.
    if let Some(operator) = &linear_operators {
        if operator.is_trivial(ingestion.desc.desc.arity()) {
            linear_operators = None;
        }
    }

    // Tokens that we should return from the method.
    let mut needed_tokens: Vec<Rc<dyn Any>> = Vec::new();

    // Before proceeding, we may need to remediate sources with non-trivial relational
    // expressions that post-process the bare source. If the expression is trivial, a
    // get of the bare source, we can present `src.operators` to the source directly.
    // Otherwise, we need to zero out `src.operators` and instead perform that logic
    // at the end of `src.optimized_expr`.
    //
    // This has a lot of potential for improvement in the near future.
    match ingestion.desc.connection.clone() {
        // Create a new local input (exposed as TABLEs to users). Data is inserted
        // via Command::Insert commands. Defers entirely to `render_table`
        SourceConnection::Local { .. } => unreachable!(),

        SourceConnection::External {
            connection,
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

            // Note that this `render_source` attaches a single _instance_ of a source
            // to the passed `Scope`, and this instance may be disabled if the
            // source type does not support multiple instances. `render_source`
            // is called on each timely worker as part of
            // [`super::build_storage_dataflow`].

            // All workers are responsible for reading in Kafka sources. Other sources
            // support single-threaded ingestion only. Note that in all cases we want all
            // readers of the same source or same partition to reside on the same worker,
            // and only load-balance responsibility across distinct sources.
            let active_read_worker = if let ExternalSourceConnection::Kafka(_) = connection {
                true
            } else {
                // TODO: This feels icky, but getting rid of hardcoding this difference between
                // Kafka and all other sources seems harder.
                crate::source::responsible_for(
                    &ingestion.id,
                    scope.index(),
                    scope.peers(),
                    &PartitionId::None,
                )
            };

            let source_name = format!("{}-{}", connection.name(), ingestion.id);
            let base_source_config = RawSourceCreationConfig {
                name: source_name,
                upstream_name: connection.upstream_name().map(ToOwned::to_owned),
                id: ingestion.id,
                scope,
                // Distribute read responsibility among workers.
                active: active_read_worker,
                timestamp_frequency: ts_frequency,
                worker_id: scope.index(),
                worker_count: scope.peers(),
                encoding: encoding.clone(),
                now: storage_state.now.clone(),
                base_metrics: &storage_state.source_metrics,
                as_of: ingestion.since.clone(),
                storage_metadata: ingestion.storage_metadata,
            };

            // Build the _raw_ ok and error sources using `create_raw_source` and the
            // correct `SourceReader` implementations
            let ((ok_source, err_source), capability) = match connection {
                ExternalSourceConnection::Kafka(_) => {
                    let ((ok, err), cap) = source::create_raw_source::<_, KafkaSourceReader>(
                        base_source_config,
                        &connection,
                        storage_state.connection_context.clone(),
                    );
                    ((SourceType::Delimited(ok), err), cap)
                }
                ExternalSourceConnection::Kinesis(_) => {
                    let ((ok, err), cap) =
                        source::create_raw_source::<_, DelimitedValueSource<KinesisSourceReader>>(
                            base_source_config,
                            &connection,
                            storage_state.connection_context.clone(),
                        );
                    ((SourceType::Delimited(ok), err), cap)
                }
                ExternalSourceConnection::S3(_) => {
                    let ((ok, err), cap) = source::create_raw_source::<_, S3SourceReader>(
                        base_source_config,
                        &connection,
                        storage_state.connection_context.clone(),
                    );
                    ((SourceType::ByteStream(ok), err), cap)
                }
                ExternalSourceConnection::PubNub(_) => {
                    let ((ok, err), cap) = source::create_raw_source::<_, PubNubSourceReader>(
                        base_source_config,
                        &connection,
                        storage_state.connection_context.clone(),
                    );
                    ((SourceType::AppendRow(ok), err), cap)
                }
                ExternalSourceConnection::Postgres(_) => {
                    let ((ok, err), cap) = source::create_raw_source::<_, PostgresSourceReader>(
                        base_source_config,
                        &connection,
                        storage_state.connection_context.clone(),
                    );
                    ((SourceType::Row(ok), err), cap)
                }
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
                            block_on(
                                csr_connection
                                    .connect(&storage_state.connection_context.secrets_reader),
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
                            &mut linear_operators,
                            storage_state.decode_metrics.clone(),
                            &storage_state.connection_context,
                        ),
                        SourceType::ByteStream(source) => render_decode(
                            &source,
                            value_encoding,
                            dataflow_debug_name,
                            metadata_columns,
                            &mut linear_operators,
                            storage_state.decode_metrics.clone(),
                            &storage_state.connection_context,
                        ),
                        SourceType::AppendRow(source) => (
                            source.map(
                                |SourceOutput {
                                     key: (),
                                     value,
                                     position,
                                     upstream_time_millis,
                                     partition,
                                     // Not expected to support headers
                                     // when full rows are produced
                                     headers: _,
                                     diff: (),
                                 }| DecodeResult {
                                    key: None,
                                    // The diff for appends is +1
                                    value: Some(Ok((value, 1))),
                                    position,
                                    upstream_time_millis,
                                    partition,
                                    metadata: Row::default(),
                                },
                            ),
                            None,
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
                            let (stream, errors) = match dbz_envelope.mode.tx_metadata() {
                                Some(tx_metadata) => {
                                    let tx_storage_metadata = ingestion
                                        .source_imports
                                        .get(&tx_metadata.tx_metadata_global_id)
                                        .expect(
                                            "dependent source missing from ingestion description",
                                        )
                                        .clone();
                                    let persist_clients =
                                        Arc::clone(&storage_state.persist_clients);
                                    let (tx_source_ok_stream, tx_source_err_stream, tx_token) =
                                        persist_source::persist_source(
                                            scope,
                                            tx_storage_metadata,
                                            persist_clients,
                                            ingestion.since.clone(),
                                        );
                                    let (tx_source_ok, tx_source_err) = (
                                        tx_source_ok_stream.as_collection(),
                                        tx_source_err_stream.as_collection(),
                                    );
                                    needed_tokens.push(tx_token);
                                    error_collections.push(tx_source_err);

                                    super::debezium::render_tx(
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

                            let source_arity = ingestion.desc.desc.typ().arity();

                            let (upsert_ok, upsert_err) = super::upsert::upsert(
                                &transformed_results,
                                ingestion.since.clone(),
                                &mut linear_operators,
                                source_arity,
                                upsert_envelope.clone(),
                            );

                            (upsert_ok.as_collection(), Some(upsert_err.as_collection()))
                        }
                        SourceEnvelope::None(none_envelope) => {
                            let results = append_metadata_to_value(results);

                            let flattened_stream =
                                flatten_results_prepend_keys(none_envelope, results);

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
                        SourceEnvelope::DifferentialRow => {
                            unreachable!("persist sources go through a special render path")
                        }
                    }
                }
            };

            if let Some(errors) = errors {
                error_collections.push(errors);
            }

            // Perform various additional transformations on the collection.

            // Force a shuffling of data in case sources are not uniformly distributed.
            let mut collection = stream.inner.exchange(|x| x.hashed()).as_collection();

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
                            let source_type = ingestion.desc.desc.typ();
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
                    let as_of_frontier1 = ingestion.since.clone();
                    collection = collection
                        .inner
                        .map_in_place(move |(_, time, _)| time.advance_by(as_of_frontier1.borrow()))
                        .as_collection();

                    let as_of_frontier2 = ingestion.since;
                    err_collection = err_collection
                        .inner
                        .map_in_place(move |(_, time, _)| time.advance_by(as_of_frontier2.borrow()))
                        .as_collection();
                }
            }

            // Consolidate the results, as there may now be cancellations.
            collection = collection.consolidate_stream();

            let source_token = Rc::new(capability);

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
    none_envelope: &NoneEnvelope,
    results: timely::dataflow::Stream<G, KV>,
) -> timely::dataflow::Stream<G, Result<(Row, Diff), DecodeError>>
where
    G: Scope,
{
    let NoneEnvelope {
        key_envelope,
        key_arity,
    } = none_envelope;

    let null_key_columns = Row::pack_slice(&vec![Datum::Null; *key_arity]);

    match key_envelope {
        KeyEnvelope::None => results.flat_map(|KV { val, .. }| val),
        KeyEnvelope::Flattened | KeyEnvelope::LegacyUpsert => results
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
) -> Option<Result<(Option<Row>, Row, Diff), DecodeError>> {
    match (key, val) {
        (Some(Ok(key)), Some(Ok((value, diff)))) => Some(Ok((Some(key), value, diff))),
        (None, Some(Ok((value, diff)))) => Some(Ok((None, value, diff))),
        // always prioritize the value error if either or both have an error
        (_, Some(Err(e))) => Some(Err(e)),
        (Some(Err(e)), _) => Some(Err(e)),
        (None, None) => None,
        // TODO(petrosagg): these errors would be better grouped under an EnvelopeError enum
        _ => Some(Err(DecodeError::Text(
            "Value not present for message".to_string(),
        ))),
    }
}
