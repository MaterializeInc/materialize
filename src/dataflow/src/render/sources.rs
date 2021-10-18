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
use persist::operators::source::PersistedSource;
use timely::dataflow::operators::generic::operator::empty;
use timely::dataflow::operators::{Concat, OkErr, ToStream};
use timely::dataflow::operators::{Map, UnorderedInput};
use timely::dataflow::Scope;

use dataflow_types::*;
use expr::{GlobalId, Id, SourceInstanceId};
use ore::cast::CastFrom;
use ore::now::NowFn;
use repr::RelationDesc;
use repr::ScalarType;
use repr::{Row, Timestamp};

use crate::decode::decode_cdcv2;
use crate::decode::render_decode;
use crate::decode::render_decode_delimited;
use crate::decode::rewrite_for_upsert;
use crate::logging::materialized::Logger;
use crate::operator::{CollectionExt, StreamExt};
use crate::render::context::Context;
use crate::render::{RelevantTokens, RenderState};
use crate::server::LocalInput;
use crate::source::DecodeResult;
use crate::source::SourceConfig;
use crate::source::{
    self, metrics::SourceBaseMetrics, FileSourceReader, KafkaSourceReader, KinesisSourceReader,
    PostgresSourceReader, PubNubSourceReader, S3SourceReader,
};

impl<G> Context<G, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
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
                let ((handle, capability), ok_stream, err_collection) = {
                    let ((handle, capability), ok_stream) = scope.new_unordered_input();
                    let err_collection = Collection::empty(scope);
                    ((handle, capability), ok_stream, err_collection)
                };

                let (ok_stream, err_collection) = match (&mut render_state.persist, persisted_name)
                {
                    (Some(persist), Some(stream_name)) => {
                        let persisted_stream = persist.create_or_load(&stream_name);
                        let (persist_ok_stream, persist_err_stream) = match persisted_stream {
                            Ok((_, read)) => scope.persisted_source(&read),
                            Err(err) => {
                                let ok_stream = empty(scope);
                                let (ts, diff) = (0, 1);
                                let err_stream = vec![(err.to_string(), ts, diff)].to_stream(scope);
                                (ok_stream, err_stream)
                            }
                        };
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

                render_state.local_inputs.insert(
                    src_id,
                    LocalInput {
                        handle: handle,
                        capability,
                    },
                );
                let as_of_frontier = self.as_of_frontier.clone();
                let ok_collection = ok_stream
                    .map_in_place(move |(_, mut time, _)| {
                        time.advance_by(as_of_frontier.borrow());
                    })
                    .as_collection();
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
                key_envelope,
                consistency,
                ts_frequency,
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
                    (usize::cast_from(src_id.hashed()) % scope.peers()) == scope.index()
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

                let (collection, capability) = if let ExternalSourceConnector::PubNub(
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
                    let source = PostgresSourceReader::new(src.name.clone(), pg_connector);

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
                    let is_connector_delimited = connector.is_delimited();

                    let ((ok_source, err_source), capability) = match connector {
                        ExternalSourceConnector::Kafka(_) => {
                            source::create_source::<_, KafkaSourceReader>(source_config, &connector)
                        }
                        ExternalSourceConnector::Kinesis(_) => {
                            source::create_source::<_, KinesisSourceReader>(
                                source_config,
                                &connector,
                            )
                        }
                        ExternalSourceConnector::S3(_) => {
                            source::create_source::<_, S3SourceReader>(source_config, &connector)
                        }
                        ExternalSourceConnector::File(_) | ExternalSourceConnector::AvroOcf(_) => {
                            source::create_source::<_, FileSourceReader>(source_config, &connector)
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

                        // CDCv2 can't quite be slotted in to the below code, since it determines its own diffs/timestamps as part of decoding.
                        if let SourceEnvelope::CdcV2 = &envelope {
                            let AvroEncoding {
                                schema,
                                schema_registry_config,
                                confluent_wire_format,
                            } = match value_encoding {
                                DataEncoding::Avro(enc) => enc,
                                _ => unreachable!("Attempted to create non-Avro CDCv2 source"),
                            };
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
                            let (results, extra_token) = if is_connector_delimited {
                                render_decode_delimited(
                                    &ok_source,
                                    key_encoding,
                                    value_encoding,
                                    &self.debug_name,
                                    &envelope,
                                    &mut linear_operators,
                                    fast_forwarded,
                                    render_state.metrics.clone(),
                                )
                            } else {
                                render_decode(
                                    &ok_source,
                                    key_encoding,
                                    value_encoding,
                                    &self.debug_name,
                                    &envelope,
                                    &mut linear_operators,
                                    fast_forwarded,
                                    render_state.metrics.clone(),
                                )
                            };
                            if let Some(tok) = extra_token {
                                tokens
                                    .additional_tokens
                                    .entry(src_id)
                                    .or_insert_with(Vec::new)
                                    .push(Rc::new(tok));
                            }

                            // render debezium or regular upsert
                            match &envelope {
                                SourceEnvelope::Debezium(_, DebeziumMode::Upsert) => {
                                    let mut trackstate = (
                                        HashMap::new(),
                                        render_state
                                            .metrics
                                            .debezium_upsert_count_for(src_id, self.dataflow_id),
                                    );
                                    let results = results.flat_map(
                                            move |DecodeResult { key, value, .. }| {
                                                let (keys, metrics) = &mut trackstate;
                                                #[rustfmt::skip]
                                                let value = value.map(|value| {
                                                    match key {
                                                        None => Err::<_, DataflowError>(
                                                            DecodeError::Text(
                                                                "All upsert keys should decode to a value."
                                                                    .to_string(),
                                                            )
                                                            .into(),
                                                        ),
                                                        Some(Err(e)) => Err(e),
                                                        Some(Ok(key)) => {
                                                            rewrite_for_upsert(value, keys, key, metrics)
                                                        }
                                                    }
                                                });
                                                value
                                            },
                                        );
                                    let (stream, errors) = results.ok_err(std::convert::identity);
                                    let stream = stream.pass_through("decode-ok").as_collection();
                                    let errors =
                                        errors.pass_through("decode-errors").as_collection();
                                    (stream, Some(errors))
                                }
                                SourceEnvelope::Upsert => {
                                    let upsert_operator_name = format!("{}-upsert", source_name);

                                    super::upsert::upsert(
                                        &upsert_operator_name,
                                        &results,
                                        self.as_of_frontier.clone(),
                                        &mut linear_operators,
                                        src.bare_desc.typ().arity(),
                                        None,
                                    )
                                }
                                _ => {
                                    let (stream, errors) = flatten_results(key_envelope, results);
                                    let stream = stream.pass_through("decode-ok").as_collection();
                                    let errors =
                                        errors.pass_through("decode-errors").as_collection();
                                    (stream, Some(errors))
                                }
                            }
                        }
                    };

                    if let Some(errors) = errors {
                        error_collections.push(errors);
                    }

                    (stream, capability)
                };

                // render debezium dedupe
                let mut collection = match &envelope {
                    SourceEnvelope::Debezium(dedupe_strategy, _) => {
                        let dbz_key_indices = match &src.connector {
                            SourceConnector::External {
                                encoding:
                                    SourceDataEncoding::KeyValue {
                                        key:
                                            DataEncoding::Avro(AvroEncoding {
                                                schema: key_schema, ..
                                            }),
                                        ..
                                    },
                                ..
                            } => {
                                let fields = match &src.bare_desc.typ().column_types[0].scalar_type
                                {
                                    ScalarType::Record { fields, .. } => fields.clone(),
                                    _ => unreachable!(),
                                };
                                let row_desc = RelationDesc::from_names_and_types(
                                    fields.into_iter().map(|(n, t)| (Some(n), t)),
                                );
                                // these must be available because the DDL parsing logic already
                                // checks this and bails in case the key is not correct
                                let key_indices =
                                    interchange::avro::validate_key_schema(key_schema, &row_desc)
                                        .expect(
                                        "Invalid key schema, this indicates a bug in Materialize",
                                    );
                                Some(key_indices)
                            }
                            _ => None,
                        };
                        dedupe_strategy.clone().render(
                            collection,
                            self.debug_name.to_string(),
                            scope.index(),
                            // Debezium decoding has produced two extra fields, with the dedupe information and the upstream time in millis
                            src.bare_desc.arity() + 2,
                            dbz_key_indices,
                        )
                    }
                    _ => collection,
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
                                let mut datums = crate::render::datum_vec::DatumVec::new();
                                let mut row_builder = Row::default();
                                // Closure that applies the linear operators to each `input_row`.
                                move |(input_row, time, diff)| {
                                    let arena = repr::RowArena::new();
                                    let mut datums_local = datums.borrow_with(&input_row);
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
                    SourceEnvelope::Upsert => {}
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

/// Convert from streams of [`DecodeResult`] to Rows, inserting the Key according to [`KeyEnvelope`]
fn flatten_results<G>(
    key_envelope: KeyEnvelope,
    results: timely::dataflow::Stream<G, DecodeResult>,
) -> (
    timely::dataflow::Stream<G, Row>,
    timely::dataflow::Stream<G, DataflowError>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    match key_envelope {
        KeyEnvelope::None => results
            .flat_map(|DecodeResult { key: _, value, .. }| value)
            .ok_err(std::convert::identity),
        KeyEnvelope::Flattened | KeyEnvelope::LegacyUpsert => results
            .flat_map(flatten_key_value)
            .map(|maybe_kv| {
                maybe_kv.map(|(mut key, value)| {
                    key.extend_by_row(&value);
                    key
                })
            })
            .ok_err(std::convert::identity),
        KeyEnvelope::Named(_) => results
            .flat_map(flatten_key_value)
            .map(|maybe_kv| match maybe_kv {
                Ok((mut key, value)) => {
                    // Named semantics rename a key that is a single column, and encode a
                    // multi-column field as a struct with that name
                    match key.iter().count() {
                        1 => {
                            key.extend_by_row(&value);
                            Ok(key)
                        }
                        _ => {
                            let mut new_row = Row::default();
                            new_row.push_list(key.iter());
                            new_row.extend_by_row(&value);
                            Ok(new_row)
                        }
                    }
                }
                Err(e) => Err(e),
            })
            .ok_err(std::convert::identity),
    }
}

/// Handle possibly missing key or value portions of messages
fn flatten_key_value(result: DecodeResult) -> Option<Result<(Row, Row), DataflowError>> {
    let DecodeResult { key, value, .. } = result;
    match (key, value) {
        (Some(key), Some(value)) => match (key, value) {
            (Ok(key), Ok(value)) => Some(Ok((key, value))),
            // always prioritize the value error if either or both have an error
            (_, Err(e)) => Some(Err(e)),
            (Err(e), _) => Some(Err(e)),
        },
        (None, None) => None,
        _ => Some(Err(DataflowError::DecodeError(DecodeError::Text(
            "Key and/or Value are not present for message".to_string(),
        )))),
    }
}
