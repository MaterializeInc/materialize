// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sources.

use std::rc::Rc;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{collection, AsCollection, Collection};
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::operators::Map;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;

use dataflow_types::*;
use expr::{GlobalId, Id, MirRelationExpr, SourceInstanceId};
use mz_avro::types::Value;
use mz_avro::Schema;
use ore::cast::CastFrom;
use repr::{Datum, Row, Timestamp};

use crate::decode::{decode_avro_values, decode_values, get_decoder};
use crate::logging::materialized::Logger;
use crate::operator::{CollectionExt, StreamExt};
use crate::render::context::Context;
use crate::render::RenderState;
use crate::server::LocalInput;
use crate::source::SourceConfig;
use crate::source::{self, FileSourceInfo, KafkaSourceInfo, KinesisSourceInfo, S3SourceInfo};

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
    ) {
        // Extract the linear operators, as we will need to manipulate them.
        // extracting them reduces the change we might accidentally communicate
        // them through `src`.
        let mut linear_operators = src.operators.take();

        // Blank out trivial linear operators.
        if let Some(operator) = &linear_operators {
            if operator.is_trivial(src.arity()) {
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
        use expr::Id::LocalBareSource;
        match &mut src.optimized_expr.0 {
            // If the expression is just the source, no need to do anything special.
            MirRelationExpr::Get {
                id: LocalBareSource,
                ..
            } => {}
            // A non-trivial expression should tack on filtering and projection, and
            // also blank out the operator so that it is not applied any earlier.
            x => {
                if let Some(operators) = linear_operators.take() {
                    // Deconstruct fields, as each will be used independently and will
                    // each invalidate `operators`.
                    let predicates = operators.predicates;
                    let projection = operators.projection;
                    // Non-empty predicates require a filter.
                    if !predicates.is_empty() {
                        *x = x.take_dangerous().filter(predicates);
                    }
                    // Non-trivial demand information calls for a map and projection.
                    let rel_typ = x.typ();
                    let arity = rel_typ.column_types.len();
                    if (0..arity).any(|x| !projection.contains(&x)) {
                        let mut dummies = Vec::new();
                        let mut demand_projection = Vec::new();
                        for (column, typ) in rel_typ.column_types.into_iter().enumerate() {
                            if projection.contains(&column) {
                                demand_projection.push(column);
                            } else {
                                demand_projection.push(arity + dummies.len());
                                dummies.push(expr::MirScalarExpr::literal_ok(
                                    Datum::Dummy,
                                    typ.scalar_type,
                                ));
                            }
                        }
                        *x = x.take_dangerous().map(dummies).project(demand_projection);
                    }
                }
            }
        }

        match src.connector {
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
                    source_id: src_id,
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

                // AvroOcf is a special case as its delimiters are discovered in the couse of decoding.
                // This means that unlike the other sources, there is not a decode step that follows.
                let (mut collection, capability) = if let ExternalSourceConnector::AvroOcf(_) =
                    connector
                {
                    let ((source, err_source), capability) =
                        source::create_source::<_, FileSourceInfo<Value>, Value>(
                            source_config,
                            connector,
                        );

                    // Include any source errors.
                    error_collections.push(
                        err_source
                            .map(DataflowError::SourceError)
                            .pass_through("AvroOCF-errors")
                            .as_collection(),
                    );

                    let reader_schema = match &encoding {
                        DataEncoding::AvroOcf(AvroOcfEncoding { reader_schema }) => reader_schema,
                        _ => unreachable!(
                            "Internal error: \
                                     Avro OCF schema should have already been resolved.\n\
                                    Encoding is: {:?}",
                            encoding
                        ),
                    };

                    let reader_schema: Schema = reader_schema.parse().unwrap();
                    let (collection, errors) =
                        decode_avro_values(&source, &envelope, reader_schema, &self.debug_name);
                    if let Some(errors) = errors {
                        error_collections.push(errors);
                    }

                    (collection, capability)
                } else if let ExternalSourceConnector::Postgres(_pg_connector) = connector {
                    unimplemented!("Postgres sources are not supported yet");
                } else {
                    let ((ok_source, err_source), capability) = match connector {
                        ExternalSourceConnector::Kafka(_) => {
                            source::create_source::<_, KafkaSourceInfo, _>(source_config, connector)
                        }
                        ExternalSourceConnector::Kinesis(_) => {
                            source::create_source::<_, KinesisSourceInfo, _>(
                                source_config,
                                connector,
                            )
                        }
                        ExternalSourceConnector::S3(_) => {
                            source::create_source::<_, S3SourceInfo, _>(source_config, connector)
                        }
                        ExternalSourceConnector::File(_) => {
                            source::create_source::<_, FileSourceInfo<Vec<u8>>, Vec<u8>>(
                                source_config,
                                connector,
                            )
                        }
                        ExternalSourceConnector::AvroOcf(_) => unreachable!(),
                        ExternalSourceConnector::Postgres(_) => unreachable!(),
                    };

                    // Include any source errors.
                    error_collections.push(
                        err_source
                            .map(DataflowError::SourceError)
                            .pass_through("source-errors")
                            .as_collection(),
                    );

                    let (stream, errors) =
                        if let SourceEnvelope::Upsert(key_encoding, mode) = &envelope {
                            let value_decoder = get_decoder(
                                encoding,
                                &self.debug_name,
                                scope.index(),
                                &envelope,
                                Some(&src.bare_desc),
                            );
                            let key_decoder = get_decoder(
                                key_encoding.clone(),
                                &self.debug_name,
                                scope.index(),
                                &envelope,
                                None,
                            );
                            super::upsert::decode_stream(
                                &ok_source,
                                self.as_of_frontier.clone(),
                                key_decoder,
                                value_decoder,
                                &mut linear_operators,
                                src.bare_desc.typ().arity(),
                                *mode,
                            )
                        } else {
                            // TODO(brennan) -- this should just be a MirRelationExpr::FlatMap using regexp_extract, csv_extract,
                            // a hypothetical future avro_extract, protobuf_extract, etc.
                            let ((stream, errors), extra_token) = decode_values(
                                &ok_source,
                                encoding,
                                &self.debug_name,
                                &envelope,
                                &mut linear_operators,
                                fast_forwarded,
                                src.bare_desc.clone(),
                            );
                            if let Some(tok) = extra_token {
                                self.additional_tokens
                                    .entry(src_id)
                                    .or_insert_with(Vec::new)
                                    .push(Rc::new(tok));
                            }
                            (stream, errors)
                        };

                    if let Some(errors) = errors {
                        error_collections.push(errors);
                    }

                    (stream, capability)
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
                        let mut row_packer = repr::RowPacker::new();
                        let predicates = std::mem::take(&mut operators.predicates);
                        // The predicates may be temporal, which requires the nuance
                        // of an explicit plan capable of evaluating the predicates.
                        let filter_plan = crate::FilterPlan::create_from(predicates)
                            .unwrap_or_else(|e| panic!(e));
                        move |(input_row, time, diff)| {
                            let mut datums_local = datums.borrow_with(&input_row);
                            let times_diffs = filter_plan.evaluate(&mut datums_local, time, diff);
                            // The output row may need to have `Datum::Dummy` values stitched in.
                            let output_row =
                                row_packer.pack(position_or.iter().map(|pos_or| match pos_or {
                                    Some(index) => datums_local[*index],
                                    None => Datum::Dummy,
                                }));
                            // Each produced (time, diff) results in a copy of `output_row` in the output.
                            // TODO: It would be nice to avoid the `output_row.clone()` for the last output.
                            times_diffs.map(move |time_diff| {
                                time_diff.map(|(t, d)| (output_row.clone(), t, d))
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
                match envelope {
                    SourceEnvelope::Upsert(_, _) => {}
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
                    id: Id::BareSource(src_id),
                    typ: src.bare_desc.typ().clone(),
                };

                // Introduce the stream by name, as an unarranged collection.
                self.collections
                    .insert(get.clone(), (collection, err_collection));

                let mut expr = src.optimized_expr.0;
                expr.visit_mut(&mut |node| {
                    if let MirRelationExpr::Get {
                        id: Id::LocalBareSource,
                        ..
                    } = node
                    {
                        *node = get.clone()
                    }
                });

                // Do whatever envelope processing is required.
                self.ensure_rendered(&expr, scope, scope.index());

                // Using `src.desc.typ()` here instead of `expr.typ()` is a bit of a hack to get around the fact
                // that the typ might have changed due to the `LinearOperator` logic above,
                // and so views, which are using the source's typ as described in the catalog, wouldn't be able to find it.
                //
                // Everything should still work out fine, since that typ has only changed in non-essential ways
                // (e.g., nullability flags and primary key information)
                let new_get = MirRelationExpr::global_get(src_id, src.desc.typ().clone());
                self.clone_from_to(&expr, &new_get);

                let token = Rc::new(capability);
                self.source_tokens.insert(src_id, token.clone());

                // We also need to keep track of this mapping globally to activate sources
                // on timestamp advancement queries
                render_state
                    .ts_source_mapping
                    .entry(uid.source_id)
                    .or_insert_with(Vec::new)
                    .push(Rc::downgrade(&token));
            }
        }
    }
}
