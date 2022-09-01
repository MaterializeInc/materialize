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
use timely::dataflow::Scope;
use timely::progress::Antichain;
use tokio::runtime::Handle as TokioHandle;

use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker, Timestamp};
use mz_timely_util::operator::{CollectionExt, StreamExt};

use crate::controller::CollectionMetadata;
use crate::decode::{render_decode, render_decode_cdcv2, render_decode_delimited};
use crate::source::types::DecodeResult;
use crate::source::{
    self, persist_source, DelimitedValueSource, KafkaSourceReader, KinesisSourceReader,
    LoadGeneratorSourceReader, PostgresSourceReader, RawSourceCreationConfig, S3SourceReader,
};
use crate::types::errors::{DataflowError, DecodeError, EnvelopeError};
use crate::types::sources::{encoding::*, *};
use crate::types::transforms::LinearOperator;

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
/// <https://github.com/MaterializeInc/materialize/pull/12109>
// TODO(guswynn): Link to merged document
pub fn render_source<G>(
    scope: &mut G,
    dataflow_debug_name: &String,
    id: GlobalId,
    description: IngestionDescription<CollectionMetadata>,
    resume_upper: Antichain<G::Timestamp>,
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
        if operator.is_trivial(description.typ.arity()) {
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

    let SourceDesc {
        connection,
        encoding,
        envelope,
        metadata_columns,
        timestamp_interval,
    } = description.desc;

    // All sources should push their various error streams into this vector,
    // whose contents will be concatenated and inserted along the collection.
    let mut error_collections = Vec::<Collection<_, _, Diff>>::new();

    // Note that this `render_source` attaches a single _instance_ of a source
    // to the passed `Scope`, and this instance may be disabled if the
    // source type does not support multiple instances. `render_source`
    // is called on each timely worker as part of
    // [`super::build_storage_dataflow`].

    let source_name = format!("{}-{}", connection.name(), id);
    let base_source_config = RawSourceCreationConfig {
        name: source_name,
        upstream_name: connection.upstream_name().map(ToOwned::to_owned),
        id,
        scope,
        timestamp_interval,
        worker_id: scope.index(),
        worker_count: scope.peers(),
        encoding: encoding.clone(),
        now: storage_state.now.clone(),
        base_metrics: &storage_state.source_metrics,
        resume_upper: resume_upper.clone(),
        storage_metadata: description.storage_metadata.clone(),
        persist_clients: Arc::clone(&storage_state.persist_clients),
    };

    // Build the _raw_ ok and error sources using `create_raw_source` and the
    // correct `SourceReader` implementations
    let ((ok_source, err_source), capability) = match connection {
        SourceConnection::Kafka(_) => {
            let ((ok, err), cap) = source::create_raw_source::<_, KafkaSourceReader>(
                base_source_config,
                &connection,
                storage_state.connection_context.clone(),
            );
            ((SourceType::Delimited(ok), err), cap)
        }
        SourceConnection::Kinesis(_) => {
            let ((ok, err), cap) =
                source::create_raw_source::<_, DelimitedValueSource<KinesisSourceReader>>(
                    base_source_config,
                    &connection,
                    storage_state.connection_context.clone(),
                );
            ((SourceType::Delimited(ok), err), cap)
        }
        SourceConnection::S3(_) => {
            let ((ok, err), cap) = source::create_raw_source::<_, S3SourceReader>(
                base_source_config,
                &connection,
                storage_state.connection_context.clone(),
            );
            ((SourceType::ByteStream(ok), err), cap)
        }
        SourceConnection::Postgres(_) => {
            let ((ok, err), cap) = source::create_raw_source::<_, PostgresSourceReader>(
                base_source_config,
                &connection,
                storage_state.connection_context.clone(),
            );
            ((SourceType::Row(ok), err), cap)
        }
        SourceConnection::LoadGenerator(_) => {
            let ((ok, err), cap) = source::create_raw_source::<_, LoadGeneratorSourceReader>(
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
                                    as_of,
                                    Antichain::new(),
                                    None,
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
                        Some(&t) => Some(t - 1),
                    };
                    let (previous_stream, previous_token) =
                        if let Some(previous_as_of) = previous_as_of {
                            let (stream, tok) = persist_source::persist_source_core(
                                scope,
                                id,
                                persist_clients,
                                description.storage_metadata.clone(),
                                Antichain::from_elem(previous_as_of),
                                Antichain::new(),
                                None,
                            );
                            (stream, Some(tok))
                        } else {
                            (std::iter::empty().to_stream(scope), None)
                        };
                    let (upsert_ok, upsert_err) = super::upsert::upsert(
                        &transformed_results,
                        resume_upper,
                        &mut linear_operators,
                        description.typ.arity(),
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
    let mut collection = stream.inner.exchange(|x| x.hashed()).as_collection();

    // Implement source filtering and projection.
    // At the moment this is strictly optional, but we perform it anyhow
    // to demonstrate the intended use.
    if let Some(operators) = linear_operators {
        // Apply predicates and insert dummy values into undemanded columns.
        let (collection2, errors) = collection
            .inner
            .flat_map_fallible("SourceLinearOperators", {
                // Produce an executable plan reflecting the linear operators.
                let linear_op_mfp = operators
                    .to_mfp(&description.typ)
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
                        |_time| true,
                        &mut row_builder,
                    )
                }
            });

        collection = collection2.as_collection();
        error_collections.push(errors.as_collection());
    };

    // Flatten the error collections.
    let err_collection = match error_collections.len() {
        0 => Collection::empty(scope),
        1 => error_collections.pop().unwrap(),
        _ => collection::concatenate(scope, error_collections),
    };

    let source_token = Rc::new(capability);

    needed_tokens.push(source_token);

    // Return the collections and any needed tokens.
    ((collection, err_collection), Rc::new(needed_tokens))
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
