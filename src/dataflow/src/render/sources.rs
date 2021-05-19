// Copyright Materialize, Inc. All rights reserved.
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
use log::warn;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::operators::Map;
use timely::dataflow::operators::OkErr;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;

use dataflow_types::*;
use expr::{GlobalId, Id, MirRelationExpr, SourceInstanceId};
use ore::cast::CastFrom;
use repr::RelationDesc;
use repr::ScalarType;
use repr::{Datum, Row, Timestamp};

use crate::decode::decode_cdcv2;
use crate::decode::render_decode;
use crate::decode::render_decode_delimited;
use crate::decode::rewrite_for_upsert;
use crate::logging::materialized::Logger;
use crate::metrics;
use crate::operator::{CollectionExt, StreamExt};
use crate::render::context::Context;
use crate::render::RenderState;
use crate::server::LocalInput;
use crate::source::DecodeResult;
use crate::source::SourceConfig;
use crate::source::{
    self, FileSourceReader, KafkaSourceReader, KinesisSourceReader, PostgresSourceReader,
    PubNubSourceReader, S3SourceReader,
};

impl<'g, G> Context<Child<'g, G, G::Timestamp>, MirRelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Import the source described by `src` into the rendering context.
    pub(crate) fn import_source(
        &mut self,
        render_state: &mut RenderState,
        scope: &mut Child<'g, G, G::Timestamp>,
        materialized_logging: Option<Logger>,
        src_id: GlobalId,
        mut src: SourceDesc,
        // The original ID of the source, before it was decomposed into a bare source (which might
        // have its own transient ID) and a relational transformation (which has the original source
        // ID).
        orig_id: GlobalId,
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
            SourceConnector::Local => {
                let ((handle, capability), stream) = scope.new_unordered_input();
                render_state
                    .local_inputs
                    .insert(src_id, LocalInput { handle, capability });
                let err_collection = Collection::empty(scope);
                self.collections.insert(
                    MirRelationExpr::global_get(src_id, src.bare_desc.typ().clone()),
                    (stream.as_collection(), err_collection),
                );
            }

            SourceConnector::External {
                connector,
                encoding,
                envelope,
                consistency,
                ts_frequency,
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

                let caching_tx = if let (true, Some(caching_tx)) =
                    (connector.caching_enabled(), render_state.caching_tx.clone())
                {
                    Some(caching_tx)
                } else {
                    None
                };

                let source_config = SourceConfig {
                    name: format!("{}-{}", connector.name(), uid),
                    sql_name: src.name.clone(),
                    upstream_name: connector.upstream_name().map(ToOwned::to_owned),
                    id: uid,
                    scope,
                    // Distribute read responsibility among workers.
                    active: active_read_worker,
                    timestamp_histories: render_state.ts_histories.clone(),
                    consistency,
                    timestamp_frequency: ts_frequency,
                    worker_id: scope.index(),
                    worker_count: scope.peers(),
                    logger: materialized_logging,
                    encoding: encoding.clone(),
                    caching_tx,
                };

                let (collection, capability) =
                    if let ExternalSourceConnector::PubNub(pubnub_connector) = connector {
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
                                source::create_source::<_, KafkaSourceReader>(
                                    source_config,
                                    &connector,
                                )
                            }
                            ExternalSourceConnector::Kinesis(_) => {
                                source::create_source::<_, KinesisSourceReader>(
                                    source_config,
                                    &connector,
                                )
                            }
                            ExternalSourceConnector::S3(_) => {
                                source::create_source::<_, S3SourceReader>(
                                    source_config,
                                    &connector,
                                )
                            }
                            ExternalSourceConnector::File(_)
                            | ExternalSourceConnector::AvroOcf(_) => {
                                source::create_source::<_, FileSourceReader>(
                                    source_config,
                                    &connector,
                                )
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
                                self.additional_tokens
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
                                    )
                                };
                                if let Some(tok) = extra_token {
                                    self.additional_tokens
                                        .entry(src_id)
                                        .or_insert_with(Vec::new)
                                        .push(Rc::new(tok));
                                }

                                // render debezium or regular upsert
                                match &envelope {
                                    SourceEnvelope::Debezium(_, DebeziumMode::Upsert) => {
                                        let mut trackstate = (
                                            HashMap::new(),
                                            metrics::DEBEZIUM_UPSERT_COUNT.with_label_values(&[
                                                &src_id.to_string(),
                                                &self.dataflow_id.to_string(),
                                            ]),
                                        );
                                        let results = results.flat_map(
                                            move |DecodeResult { key, value, .. }| {
                                                let (keys, metrics) = &mut trackstate;
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
                                        let (stream, errors) =
                                            results.ok_err(std::convert::identity);
                                        let stream =
                                            stream.pass_through("decode-ok").as_collection();
                                        let errors =
                                            errors.pass_through("decode-errors").as_collection();
                                        (stream, Some(errors))
                                    }
                                    SourceEnvelope::Upsert => super::upsert::decode_stream(
                                        &results,
                                        self.as_of_frontier.clone(),
                                        &mut linear_operators,
                                        src.bare_desc.typ().arity(),
                                    ),
                                    _ => {
                                        let (stream, errors) = results
                                            .flat_map(|DecodeResult { key: _, value, .. }| value)
                                            .ok_err(std::convert::identity);
                                        let stream =
                                            stream.pass_through("decode-ok").as_collection();
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
                                interchange::avro::validate_key_schema(key_schema, &row_desc)
                                    .map(Some)
                                    .unwrap_or_else(|e| {
                                        warn!("Not using key due to error: {}", e);
                                        None
                                    })
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

                // Implement source filtering and projection.
                // At the moment this is strictly optional, but we perform it anyhow
                // to demonstrate the intended use.
                if let Some(mut operators) = linear_operators {
                    // Determine replacement values for unused columns.
                    let source_type = src.bare_desc.typ();
                    let position_or = (0..source_type.arity())
                        .map(|col| {
                            if operators.projection.contains(&col) {
                                Some(col)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    // Apply predicates and insert dummy values into undemanded columns.
                    let (collection2, errors) = collection.inner.flat_map_fallible({
                        let mut datums = crate::render::datum_vec::DatumVec::new();
                        let predicates = std::mem::take(&mut operators.predicates);
                        // The predicates may be temporal, which requires the nuance
                        // of an explicit plan capable of evaluating the predicates.
                        let filter_plan = expr::MapFilterProject::new(source_type.arity())
                            .filter(predicates)
                            .into_plan()
                            .unwrap_or_else(|e| panic!("{}", e));
                        move |(input_row, time, diff)| {
                            let arena = repr::RowArena::new();
                            let mut datums_local = datums.borrow_with(&input_row);
                            let times_diffs =
                                filter_plan.evaluate(&mut datums_local, &arena, time, diff);
                            // Name the iterator, to capture total size and datums.
                            let iterator = position_or.iter().map(|pos_or| match pos_or {
                                Some(index) => datums_local[*index],
                                None => Datum::Dummy,
                            });
                            let total_size = repr::datums_size(iterator.clone());
                            let mut output_row = Row::with_capacity(total_size);
                            output_row.extend(iterator);
                            // Each produced (time, diff) results in a copy of `output_row` in the output.
                            // TODO: It would be nice to avoid the `output_row.clone()` for the last output.
                            times_diffs.map(move |time_diff| {
                                time_diff.map_err(|(e, t, d)| (DataflowError::from(e), t, d))
                            })
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

                let get = MirRelationExpr::Get {
                    id: Id::Global(src_id),
                    typ: src.bare_desc.typ().clone(),
                };

                // Introduce the stream by name, as an unarranged collection.
                self.collections.insert(get, (collection, err_collection));

                let token = Rc::new(capability);
                self.source_tokens.insert(src_id, token.clone());

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
