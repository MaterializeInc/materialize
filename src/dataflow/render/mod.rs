// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders a plan into a timely/differential dataflow computation.
//!
//! ## Error handling
//!
//! Timely and differential have no idioms for computations that can error. The
//! philosophy is, reasonably, to define the semantics of the computation such
//! that errors are unnecessary: e.g., by using wrap-around semantics for
//! integer overflow.
//!
//! Unfortunately, SQL semantics are not nearly so elegant, and require errors
//! in myriad cases. The classic example is a division by zero, but invalid
//! input for casts, overflowing integer operations, and dozens of other
//! functions need the ability to produce errors ar runtime.
//!
//! At the moment, only *scalar* expression evaluation can fail, so only
//! operators that evaluate scalar expressions can fail. At the time of writing,
//! that includes map, filter, reduce, and join operators.
//!
//! The approach taken is to build two parallel trees of computation: one for
//! the rows that have been successfully evaluated (the "oks tree"), and one for
//! the errors that have been generated (the "errs tree"). For example:
//!
//! ```text
//!    oks1  errs1       oks2  errs2
//!      |     |           |     |
//!      |     |           |     |
//!   project  |           |     |
//!      |     |           |     |
//!      |     |           |     |
//!     map    |           |     |
//!      |\    |           |     |
//!      | \   |           |     |
//!      |  \  |           |     |
//!      |   \ |           |     |
//!      |    \|           |     |
//!   project  +           +     +
//!      |     |          /     /
//!      |     |         /     /
//!    join ------------+     /
//!      |     |             /
//!      |     | +----------+
//!      |     |/
//!     oks   errs
//! ```
//!
//! The project operation cannot fail, so errors from errs1 are propagated
//! directly. Map operators are fallible and so can inject additional errors
//! into the stream. Join operators combine the errors from each of their
//! inputs.
//!
//! The semantics of the error stream are minimal. From the perspective of SQL,
//! a dataflow is considered to be in an error state if there is at least one
//! element in the final errs collection. The error value returned to the user
//! is selected arbitrarily; SQL only makes provisions to return one error to
//! the user at a time. There are plans to make the err collection accessible to
//! end users, so they can see all errors at once.
//!
//! To make errors transient, simply ensure that the operator can retract any
//! produced errors when corrected data arrives. To make errors permanent, write
//! the operator such that it never retracts the errors it produced. Future work
//! will likely want to introduce some sort of sort order for errors, so that
//! permanent errors are returned to the user ahead of transient errors—probably
//! by introducing a new error type a la:
//!
//! ```no_run
//! # struct EvalError;
//! # struct SourceError;
//! enum DataflowError {
//!     Transient(EvalError),
//!     Permanent(SourceError),
//! }
//! ```
//!
//! If the error stream is empty, the oks stream must be correct. If the error
//! stream is non-empty, then there are no semantics for the oks stream. This is
//! sufficient to support SQL in its current form, but is likely to be
//! unsatisfactory long term. We suspect that we can continue to imbue the oks
//! stream with semantics if we are very careful in describing what data should
//! and should not be produced upon encountering an error. Roughly speaking, the
//! oks stream could represent the correct result of the computation where all
//! rows that caused an error have been pruned from the stream. There are
//! strange and confusing questions here around foreign keys, though: what if
//! the optimizer proves that a particular key must exist in a collection, but
//! the key gets pruned away because its row participated in a scalar expression
//! evaluation that errored?
//!
//! In the meantime, it is probably wise for operators to keep the oks stream
//! roughly "as correct as possible" even when errors are present in the errs
//! stream. This reduces the amount of recomputation that must be performed
//! if/when the errors are retracted.

use std::any::Any;
use std::collections::HashMap;
use std::io::BufRead;
use std::rc::Rc;
use std::rc::Weak;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::arrange::upsert::arrange_from_upsert;
use differential_dataflow::{AsCollection, Collection};
use timely::communication::Allocate;
use timely::dataflow::operators::generic::{operator, Operator};
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::operators::Map;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;

use timely::dataflow::channels::pact::{Exchange, Pipeline};

use avro::Schema;
use dataflow_types::Timestamp;
use dataflow_types::*;
use expr::{GlobalId, Id, RelationExpr, ScalarExpr, SourceInstanceId};
use ore::cast::CastFrom;
use ore::iter::IteratorExt;
use repr::{Datum, RelationType, Row, RowArena};

use self::context::{ArrangementFlavor, Context};
use super::sink;
use super::source;
use super::source::FileReadStyle;
use super::source::{SourceConfig, SourceToken};
use crate::arrangement::manager::{TraceBundle, TraceManager};
use crate::decode::{decode_avro_values, decode_upsert, decode_values};
use crate::logging::materialized::{Logger, MaterializedEvent};
use crate::operator::{CollectionExt, StreamExt};
use crate::server::LocalInput;
use crate::server::{TimestampChanges, TimestampHistories};
use source::SourceOutput;

mod arrange_by;
mod context;
mod delta_join;
mod join;
mod reduce;
mod threshold;
mod top_k;

pub(crate) fn build_local_input<A: Allocate>(
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    local_inputs: &mut HashMap<GlobalId, LocalInput>,
    index_id: GlobalId,
    name: &str,
    index: IndexDesc,
    on_type: RelationType,
) {
    let worker_index = worker.index();
    let name = format!("Dataflow: {}", name);
    let worker_logging = worker.log_register().get("timely");
    worker.dataflow_core::<Timestamp, _, _, _>(&name, worker_logging, Box::new(()), |_, scope| {
        scope.clone().region(|region| {
            let mut context = Context::<_, _, _, Timestamp>::new();
            let ((handle, capability), stream) = region.new_unordered_input();
            if worker_index == 0 {
                local_inputs.insert(index.on_id, LocalInput { handle, capability });
            }
            // TODO: why do we use `index.on_id` here instead of `index_id`? Are they the same?
            let get_expr = RelationExpr::global_get(index.on_id, on_type);
            let err_collection = Collection::empty(region);
            context
                .collections
                .insert(get_expr.clone(), (stream.as_collection(), err_collection));
            context.render_arrangeby(
                &get_expr.clone().arrange_by(&[index.keys.clone()]),
                Some(&index_id.to_string()),
            );
            match context.arrangement(&get_expr, &index.keys) {
                Some(ArrangementFlavor::Local(oks, errs)) => {
                    manager.set(index_id, TraceBundle::new(oks.trace, errs.trace));
                }
                _ => {
                    panic!("Arrangement alarmingly absent!");
                }
            };
        });
    });
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_dataflow<A: Allocate>(
    dataflow: DataflowDesc,
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    dataflow_drops: &mut HashMap<GlobalId, Box<dyn Any>>,
    global_source_mappings: &mut HashMap<SourceInstanceId, Weak<Option<SourceToken>>>,
    timestamp_histories: TimestampHistories,
    timestamp_channel: TimestampChanges,
    logger: &mut Option<Logger>,
) {
    let worker_index = worker.index();
    let worker_peers = worker.peers();
    let worker_logging = worker.log_register().get("timely");
    let name = format!("Dataflow: {}", &dataflow.debug_name);

    worker.dataflow_core::<Timestamp, _, _, _>(&name, worker_logging, Box::new(()), |_, scope| {
        // The scope.clone() occurs to allow import in the region.
        // We build a region here to establish a pattern of a scope inside the dataflow,
        // so that other similar uses (e.g. with iterative scopes) do not require weird
        // alternate type signatures.
        scope.clone().region(|region| {
            let mut context = Context::<_, _, _, Timestamp>::new();

            let mut source_tokens = HashMap::new();
            // this is stopgap measure so dropping an index and recreating one with the same name
            // does not result in timestamp/reading from source errors.
            // use an export id to distinguish between different dataflows
            // TODO (materialize#1720): replace `first_export_id` by some form of dataflow identifier
            let first_export_id = if let Some((id, _, _)) = dataflow.index_exports.first() {
                *id
            } else if let Some((id, _)) = dataflow.sink_exports.first() {
                *id
            } else {
                unreachable!()
            };

            assert!(
                !dataflow
                    .source_imports
                    .iter()
                    .map(|(id, _src)| id)
                    .has_duplicates(),
                "computation of unique IDs assumes a source appears no more than once per dataflow"
            );

            // When set, `as_of` indicates a frontier that can be used to compact input timestamps
            // without affecting the results. We *should* apply it, to sources and imported traces,
            // both because it improves performance, and because potentially incorrect results are
            // visible in sinks.
            let as_of_frontier = dataflow
                .as_of
                .clone()
                .unwrap_or_else(|| Antichain::from_elem(0));

            // Load declared sources into the rendering context.
            for (src_id, mut src) in dataflow.source_imports.clone() {
                if let SourceConnector::External {
                    connector,
                    encoding,
                    envelope,
                    consistency,
                    max_ts_batch: _,
                } = src.connector
                {
                    let get_expr = RelationExpr::global_get(src_id.sid, src.desc.typ().clone());

                    // This uid must be unique across all different instantiations of a source
                    let uid = SourceInstanceId {
                        sid: src_id.sid,
                        vid: first_export_id,
                    };

                    // TODO(benesch): we force all sources to have an empty
                    // error stream. Likely we will want to plumb this
                    // collection into the source connector so that sources
                    // can produce errors.
                    let mut err_collection = Collection::empty(region);

                    let fast_forwarded = match connector {
                        ExternalSourceConnector::Kafka(KafkaSourceConnector {
                            start_offset,
                            ..
                        }) => start_offset > 0,
                        _ => false,
                    };

                    let source_config = SourceConfig {
                        name: format!("{}-{}", connector.name(), uid),
                        id: uid,
                        scope: region,
                        // Distribute read responsibility among workers.
                        active: (usize::cast_from(uid.hashed()) % worker_peers) == worker_index,
                        timestamp_histories: timestamp_histories.clone(),
                        timestamp_tx: timestamp_channel.clone(),
                        consistency,
                    };

                    let capability = if let Envelope::Upsert(key_encoding) = envelope {
                        match connector {
                            ExternalSourceConnector::Kafka(kc) => {
                                let (source, capability) = source::kafka(source_config, kc);

                                // This operator changes the timestamp from capability to message payload,
                                // and applies `as_of` frontier compaction. The compaction is important as
                                // downstream upsert preparation can compact away updates for the same keys
                                // at the same times, and by advancing times we make more of them the same.
                                let as_of_frontier = as_of_frontier.clone();
                                let source =
                                    source.unary(Pipeline, "AppendTimestamp", move |_, _| {
                                        let mut vector = Vec::new();
                                        move |input, output| {
                                            input.for_each(|cap, data| {
                                                data.swap(&mut vector);
                                                let mut time = cap.time().clone();
                                                time.advance_by(as_of_frontier.borrow());
                                                output.session(&cap).give_iterator(
                                                    vector.drain(..).map(|x| (x, time.clone())),
                                                );
                                            });
                                        }
                                    });

                                // Deduplicate records by key, decode, and then upsert arrange them.
                                let deduplicated = prepare_upsert_by_max_offset(&source);
                                let decoded = decode_upsert(
                                    &deduplicated,
                                    encoding,
                                    key_encoding,
                                    &dataflow.debug_name,
                                    worker_index,
                                );
                                let arranged = arrange_from_upsert(
                                    &decoded,
                                    &format!("UpsertArrange: {}", src_id.to_string()),
                                );

                                let keys = src.desc.typ().keys[0]
                                    .iter()
                                    .map(|k| ScalarExpr::Column(*k))
                                    .collect::<Vec<_>>();
                                context.set_local(
                                    &get_expr,
                                    &keys,
                                    (arranged, err_collection.arrange()),
                                );
                                capability
                            }
                            _ => unreachable!("Upsert envelope unsupported for non-Kafka sources"),
                        }
                    } else {
                        let (stream, capability) =
                            if let ExternalSourceConnector::AvroOcf(c) = connector {
                                // Distribute read responsibility among workers.
                                let read_style = if c.tail {
                                    FileReadStyle::TailFollowFd
                                } else {
                                    FileReadStyle::ReadOnce
                                };

                                let reader_schema = match &encoding {
                                    DataEncoding::AvroOcf { reader_schema } => reader_schema,
                                    _ => unreachable!(
                                        "Internal error: \
                                         Avro OCF schema should have already been resolved.\n\
                                        Encoding is: {:?}",
                                        encoding
                                    ),
                                };
                                let reader_schema = Schema::parse_str(reader_schema).unwrap();
                                let ctor = {
                                    let reader_schema = reader_schema.clone();
                                    move |file| avro::Reader::with_schema(&reader_schema, file)
                                };

                                let ((source, err_source), capability) =
                                    source::file(source_config, c.path, read_style, ctor);
                                err_collection = err_collection.concat(
                                    &err_source
                                        .map(DataflowError::SourceError)
                                        .pass_through("AvroOCF-errors")
                                        .as_collection(),
                                );
                                (
                                    decode_avro_values(
                                        &source,
                                        &envelope,
                                        reader_schema,
                                        &dataflow.debug_name,
                                    ),
                                    capability,
                                )
                            } else {
                                let ((ok_source, err_source), capability) = match connector {
                                    ExternalSourceConnector::Kafka(kc) => {
                                        let (ok_source, cap) = source::kafka(source_config, kc);
                                        ((ok_source, operator::empty(region)), cap)
                                    }
                                    ExternalSourceConnector::Kinesis(kc) => {
                                        let (ok_source, cap) = source::kinesis(source_config, kc);
                                        ((ok_source, operator::empty(region)), cap)
                                    }
                                    ExternalSourceConnector::File(c) => {
                                        let read_style = if c.tail {
                                            FileReadStyle::TailFollowFd
                                        } else {
                                            FileReadStyle::ReadOnce
                                        };
                                        let ctor =
                                            |file| Ok(std::io::BufReader::new(file).split(b'\n'));
                                        source::file(source_config, c.path, read_style, ctor)
                                    }
                                    ExternalSourceConnector::AvroOcf(_) => unreachable!(),
                                };
                                err_collection = err_collection.concat(
                                    &err_source
                                        .map(DataflowError::SourceError)
                                        .pass_through("source-errors")
                                        .as_collection(),
                                );

                                // TODO(brennan) -- this should just be a RelationExpr::FlatMap using regexp_extract, csv_extract,
                                // a hypothetical future avro_extract, protobuf_extract, etc.
                                let stream = decode_values(
                                    &ok_source,
                                    encoding,
                                    &dataflow.debug_name,
                                    &envelope,
                                    &mut src.operators,
                                    fast_forwarded,
                                );

                                (stream, capability)
                            };

                        let mut collection = match envelope {
                            Envelope::None => stream.as_collection(),
                            Envelope::Debezium => {
                                // TODO(btv) -- this should just be a RelationExpr::Explode (name TBD)
                                stream.as_collection().explode(|row| {
                                    let mut datums = row.unpack();
                                    let diff = datums.pop().unwrap().unwrap_int64() as isize;
                                    Some((Row::pack(datums.into_iter()), diff))
                                })
                            }
                            Envelope::Upsert(_) => unreachable!(),
                        };

                        // Implement source filtering and projection.
                        // At the moment this is strictly optional, but we perform it anyhow
                        // to demonstrate the intended use.
                        if let Some(operators) = src.operators.clone() {
                            // Determine replacement values for unused columns.
                            let source_type = src.desc.typ();
                            let position_or = (0..source_type.arity())
                                .map(|col| {
                                    if operators.projection.contains(&col) {
                                        Ok(col)
                                    } else {
                                        Err({
                                            // TODO(frank): This could be `Datum::Null` if we
                                            // are certain that no readers will consult it and
                                            // believe it to be a non-null value. That is the
                                            // intent, but it is not yet clear that we ensure
                                            // this.
                                            let typ = &source_type.column_types[col];
                                            if typ.nullable {
                                                Datum::Null
                                            } else {
                                                typ.scalar_type.dummy_datum()
                                            }
                                        })
                                    }
                                })
                                .collect::<Vec<_>>();

                            // Evaluate the predicate on each record, noting potential errors that might result.
                            let (collection2, errors) =
                                collection.flat_map_fallible(move |input_row| {
                                    let temp_storage = RowArena::new();
                                    let datums = input_row.unpack();
                                    let pred_eval = operators
                                        .predicates
                                        .iter()
                                        .map(|predicate| predicate.eval(&datums, &temp_storage))
                                        .find(|result| result != &Ok(Datum::True));
                                    match pred_eval {
                                        None => {
                                            Some(Ok(Row::pack(position_or.iter().map(|pos_or| {
                                                match pos_or {
                                                    Result::Ok(index) => datums[*index],
                                                    Result::Err(datum) => *datum,
                                                }
                                            }))))
                                        }
                                        Some(Ok(Datum::False)) => None,
                                        Some(Ok(Datum::Null)) => None,
                                        Some(Ok(x)) => {
                                            panic!("Predicate evaluated to invalid value: {:?}", x)
                                        }
                                        Some(Err(x)) => Some(Err(x.into())),
                                    }
                                });

                            collection = collection2;
                            err_collection = err_collection.concat(&errors);
                        }

                        // Apply `as_of` to each timestamp.
                        let as_of_frontier1 = as_of_frontier.clone();
                        collection = collection
                            .inner
                            .map_in_place(move |(_, time, _)| {
                                time.advance_by(as_of_frontier1.borrow())
                            })
                            .as_collection();

                        let as_of_frontier2 = as_of_frontier.clone();
                        err_collection = err_collection
                            .inner
                            .map_in_place(move |(_, time, _)| {
                                time.advance_by(as_of_frontier2.borrow())
                            })
                            .as_collection();

                        // Introduce the stream by name, as an unarranged collection.
                        context.collections.insert(
                            RelationExpr::global_get(src_id.sid, src.desc.typ().clone()),
                            (collection, err_collection),
                        );
                        capability
                    };
                    let token = Rc::new(capability);
                    source_tokens.insert(src_id.sid, token.clone());

                    // We also need to keep track of this mapping globally to activate sources
                    // on timestamp advancement queries
                    let prev = global_source_mappings.insert(uid, Rc::downgrade(&token));
                    assert!(prev.is_none());
                }
            }

            let mut index_tokens = HashMap::new();

            for (id, (index_desc, typ)) in dataflow.index_imports.iter() {
                if let Some(traces) = manager.get_mut(id) {
                    let token = traces.to_drop().clone();
                    let (ok_arranged, ok_button) = traces.oks_mut().import_frontier_core(
                        scope,
                        &format!("Index({}, {:?})", index_desc.on_id, index_desc.keys),
                        as_of_frontier.clone(),
                    );
                    let (err_arranged, err_button) = traces.errs_mut().import_frontier_core(
                        scope,
                        &format!("ErrIndex({}, {:?})", index_desc.on_id, index_desc.keys),
                        as_of_frontier.clone(),
                    );
                    let ok_arranged = ok_arranged.enter(region);
                    let err_arranged = err_arranged.enter(region);
                    let get_expr = RelationExpr::global_get(index_desc.on_id, typ.clone());
                    context.set_trace(
                        *id,
                        &get_expr,
                        &index_desc.keys,
                        (ok_arranged, err_arranged),
                    );
                    index_tokens.insert(
                        id,
                        Rc::new((ok_button.press_on_drop(), err_button.press_on_drop(), token)),
                    );
                } else {
                    panic!(
                        "import of index {} failed while building dataflow {}",
                        id, first_export_id
                    );
                }
            }

            for object in dataflow.objects_to_build.clone() {
                context.ensure_rendered(object.relation_expr.as_ref(), region, worker_index);
                if let Some(typ) = object.typ {
                    context.clone_from_to(
                        &object.relation_expr.as_ref(),
                        &RelationExpr::global_get(object.id, typ.clone()),
                    );
                } else {
                    context.render_arrangeby(
                        &object.relation_expr.as_ref(),
                        Some(&object.id.to_string()),
                    );
                    // Under the premise that this is always an arrange_by aroung a global get,
                    // this will leave behind the arrangements bound to the global get, so that
                    // we will not tidy them up in the next pass.
                }

                // After building each object, we want to tear down all other cached collections
                // and arrangements to avoid accidentally providing hits on local identifiers.
                // We could relax this if we better understood which expressions are dangerous
                // (e.g. expressions containing gets of local identifiers not covered by a let).
                //
                // TODO: Improve collection and arrangement re-use.
                context.collections.retain(|e, _| {
                    if let RelationExpr::Get {
                        id: Id::Global(_),
                        typ: _,
                    } = e
                    {
                        true
                    } else {
                        false
                    }
                });
                context.local.retain(|e, _| {
                    if let RelationExpr::Get {
                        id: Id::Global(_),
                        typ: _,
                    } = e
                    {
                        true
                    } else {
                        false
                    }
                });
                // We do not install in `context.trace`, and can skip deleting things from it.
            }

            for (export_id, index_desc, typ) in &dataflow.index_exports {
                // put together tokens that belong to the export
                let mut needed_source_tokens = Vec::new();
                let mut needed_index_tokens = Vec::new();
                for import_id in dataflow.get_imports(&index_desc.on_id) {
                    if let Some(index_token) = index_tokens.get(&import_id) {
                        if let Some(logger) = logger {
                            // Log the dependency.
                            logger.log(MaterializedEvent::DataflowDependency {
                                dataflow: *export_id,
                                source: import_id,
                            });
                        }
                        needed_index_tokens.push(index_token.clone());
                    } else if let Some(source_token) = source_tokens.get(&import_id) {
                        needed_source_tokens.push(source_token.clone());
                    }
                }
                let tokens = Rc::new((needed_source_tokens, needed_index_tokens));
                let get_expr = RelationExpr::global_get(index_desc.on_id, typ.clone());
                match context.arrangement(&get_expr, &index_desc.keys) {
                    Some(ArrangementFlavor::Local(oks, errs)) => {
                        manager.set(
                            *export_id,
                            TraceBundle::new(oks.trace.clone(), errs.trace.clone())
                                .with_drop(tokens),
                        );
                    }
                    Some(ArrangementFlavor::Trace(gid, _, _)) => {
                        // Duplicate of existing arrangement with id `gid`, so
                        // just create another handle to that arrangement.
                        let trace = manager.get(&gid).unwrap().clone();
                        manager.set(*export_id, trace);
                    }
                    None => {
                        panic!("Arrangement alarmingly absent!");
                    }
                };
            }

            for (sink_id, sink) in dataflow.sink_exports.clone() {
                // put together tokens that belong to the export
                let mut needed_source_tokens = Vec::new();
                let mut needed_index_tokens = Vec::new();
                for import_id in dataflow.get_imports(&sink.from.0) {
                    if let Some(index_token) = index_tokens.get(&import_id) {
                        needed_index_tokens.push(index_token.clone());
                    } else if let Some(source_token) = source_tokens.get(&import_id) {
                        needed_source_tokens.push(source_token.clone());
                    }
                }
                let tokens = Rc::new((needed_source_tokens, needed_index_tokens));
                let (collection, _err_collection) = context
                    .collection(&RelationExpr::global_get(
                        sink.from.0,
                        sink.from.1.typ().clone(),
                    ))
                    .expect("Sink source collection not loaded");

                // TODO(frank): consolidation is only required for a collection,
                // not for arrangements. We can perform a more complicated match
                // here to determine which case we are in to avoid this call.
                use differential_dataflow::operators::consolidate::Consolidate;
                let collection = collection.consolidate();

                // TODO(benesch): errors should stream out through the sink,
                // if we figure out a protocol for that.

                match sink.connector {
                    SinkConnector::Kafka(c) => {
                        sink::kafka(&collection.inner, sink_id, c, sink.from.1)
                    }
                    SinkConnector::Tail(c) => sink::tail(&collection.inner, sink_id, c),
                    SinkConnector::AvroOcf(c) => {
                        sink::avro_ocf(&collection.inner, sink_id, c, sink.from.1)
                    }
                }
                dataflow_drops.insert(sink_id, Box::new(tokens));
            }
        });
    })
}

/// Produces at most one entry for each `(key, time)` pair.
///
/// The incoming stream of `(key, (val, off), time)` records may have many
/// entries with the same `key` and `time`. We are able to reduce this to
/// at most one record for each pair, by retaining only the record with the
/// greatest offset: its action summarizes the sequence of many actions that
/// occur at the same moment and so are not distinguishable.
fn prepare_upsert_by_max_offset<G>(
    stream: &Stream<G, (SourceOutput<Vec<u8>, Vec<u8>>, Timestamp)>,
) -> Stream<G, ((Vec<u8>, (Vec<u8>, Option<i64>)), Timestamp)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary_frontier(
        Exchange::new(move |x: &(SourceOutput<Vec<u8>, Vec<u8>>, Timestamp)| x.0.key.hashed()),
        "UpsertCompaction",
        |_cap, _info| {
            let mut values = HashMap::<_, HashMap<_, (Vec<u8>, Option<i64>)>>::new();
            let mut vector = Vec::new();

            move |input, output| {
                // Digest each input, reduce by presented timestamp.
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    for (
                        SourceOutput {
                            key,
                            value: val,
                            position,
                        },
                        time,
                    ) in vector.drain(..)
                    {
                        let value = values
                            .entry(cap.delayed(&time))
                            .or_insert_with(HashMap::new)
                            .entry(key)
                            .or_insert_with(Default::default);

                        if let Some(new_offset) = position {
                            if let Some(offset) = value.1 {
                                if offset < new_offset {
                                    *value = (val, position);
                                }
                            } else {
                                *value = (val, position);
                            }
                        }
                    }
                });

                // Produce (key, val) pairs at any complete times.
                for (cap, map) in values.iter_mut() {
                    if !input.frontier.less_equal(cap.time()) {
                        let mut session = output.session(cap);
                        for (key, val) in map.drain() {
                            session.give(((key, val), cap.time().clone()))
                        }
                    }
                }
                // Discard entries, capabilities for complete times.
                values.retain(|_cap, map| !map.is_empty());
            }
        },
    )
}

impl<G> Context<G, RelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Ensures the context contains an entry for `relation_expr`.
    ///
    /// This method may construct new dataflow elements and register then in the context,
    /// and is only obliged to ensure that a call to `self.collection(relation_expr)` will
    /// result in a non-`None` result. This may be a raw collection or an arrangement by
    /// any set of keys.
    ///
    /// The rough structure of the logic for each expression is to ensure that any input
    /// collections are rendered,
    pub fn ensure_rendered(
        &mut self,
        relation_expr: &RelationExpr,
        scope: &mut G,
        worker_index: usize,
    ) {
        if !self.has_collection(relation_expr) {
            // Each of the `RelationExpr` variants have logic to render themselves to either
            // a collection or an arrangement. In either case, we associate the result with
            // the `relation_expr` argument in the context.
            match relation_expr {
                // The constant collection is instantiated only on worker zero.
                RelationExpr::Constant { rows, .. } => {
                    let rows = if worker_index == 0 {
                        rows.clone()
                    } else {
                        vec![]
                    };

                    let collection = rows
                        .to_stream(scope)
                        .map(|(x, diff)| (x, timely::progress::Timestamp::minimum(), diff))
                        .as_collection();

                    let err_collection = Collection::empty(scope);

                    self.collections
                        .insert(relation_expr.clone(), (collection, err_collection));
                }

                // A get should have been loaded into the context, and it is surprising to
                // reach this point given the `has_collection()` guard at the top of the method.
                RelationExpr::Get { id, typ: _ } => {
                    // TODO: something more tasteful.
                    // perhaps load an empty collection, warn?
                    panic!("Collection {} not pre-loaded", id);
                }

                RelationExpr::Let { id, value, body } => {
                    let typ = value.typ();
                    let bind = RelationExpr::Get {
                        id: Id::Local(*id),
                        typ,
                    };
                    if self.has_collection(&bind) {
                        panic!("Inappropriate to re-bind name: {:?}", bind);
                    } else {
                        self.ensure_rendered(value, scope, worker_index);
                        self.clone_from_to(value, &bind);
                        self.ensure_rendered(body, scope, worker_index);
                        self.clone_from_to(body, relation_expr);
                    }
                }

                RelationExpr::Project { input, outputs } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let outputs = outputs.clone();
                    let (ok_collection, err_collection) = self.collection(input).unwrap();
                    let ok_collection = ok_collection.map(move |row| {
                        let datums = row.unpack();
                        Row::pack(outputs.iter().map(|i| datums[*i]))
                    });

                    self.collections
                        .insert(relation_expr.clone(), (ok_collection, err_collection));
                }

                RelationExpr::Map { input, scalars } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let scalars = scalars.clone();
                    let (ok_collection, err_collection) = self.collection(input).unwrap();
                    let (ok_collection, new_err_collection) =
                        ok_collection.map_fallible(move |input_row| {
                            let mut datums = input_row.unpack();
                            let temp_storage = RowArena::new();
                            for scalar in &scalars {
                                let datum = scalar.eval(&datums, &temp_storage)?;
                                // Scalar is allowed to see the outputs of previous scalars.
                                // To avoid repeatedly unpacking input_row, we just push the outputs into datums so later scalars can see them.
                                // Note that this doesn't mutate input_row.
                                datums.push(datum);
                            }
                            Ok::<_, DataflowError>(Row::pack(&*datums))
                        });
                    let err_collection = err_collection.concat(&new_err_collection);
                    self.collections
                        .insert(relation_expr.clone(), (ok_collection, err_collection));
                }

                RelationExpr::FlatMap {
                    input,
                    func,
                    exprs,
                    demand,
                } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let func = func.clone();
                    let exprs = exprs.clone();

                    // Determine for each output column if it should be replaced by a
                    // small default value. This information comes from the "demand"
                    // analysis, and is meant to allow us to avoid reproducing the
                    // input in each output, if at all possible.
                    let types = relation_expr.typ();
                    let arity = types.column_types.len();
                    let replace = (0..arity)
                        .map(|col| {
                            if demand.as_ref().map(|d| d.contains(&col)).unwrap_or(true) {
                                None
                            } else {
                                Some({
                                    let typ = &types.column_types[col];
                                    if typ.nullable {
                                        Datum::Null
                                    } else {
                                        typ.scalar_type.dummy_datum()
                                    }
                                })
                            }
                        })
                        .collect::<Vec<_>>();

                    let (ok_collection, err_collection) = self.collection(input).unwrap();
                    let (ok_collection, new_err_collection) =
                        ok_collection.flat_map_fallible(move |input_row| {
                            let datums = input_row.unpack();
                            let replace = replace.clone();
                            let temp_storage = RowArena::new();
                            let exprs = exprs
                                .iter()
                                .map(|e| e.eval(&datums, &temp_storage))
                                .collect::<Result<Vec<_>, _>>();
                            let exprs = match exprs {
                                Ok(exprs) => exprs,
                                Err(e) => return vec![Err(e.into())],
                            };
                            let output_rows = func.eval(exprs, &temp_storage);
                            output_rows
                                .into_iter()
                                .map(move |output_row| {
                                    Ok::<_, DataflowError>(Row::pack(
                                        datums
                                            .iter()
                                            .cloned()
                                            .chain(output_row.iter())
                                            .zip(replace.iter())
                                            .map(|(datum, demand)| {
                                                if let Some(bogus) = demand {
                                                    bogus.clone()
                                                } else {
                                                    datum
                                                }
                                            }),
                                    ))
                                })
                                // The collection avoids the lifetime issues of the `datums` borrow,
                                // which allows us to avoid multiple unpackings of `input_row`. We
                                // could avoid this allocation with a custom iterator that understands
                                // the borrowing, but it probably isn't the leading order issue here.
                                .collect::<Vec<_>>()
                        });
                    let err_collection = err_collection.concat(&new_err_collection);

                    self.collections
                        .insert(relation_expr.clone(), (ok_collection, err_collection));
                }

                RelationExpr::Filter { input, predicates } => {
                    let collections = if let RelationExpr::Join {
                        inputs,
                        implementation,
                        ..
                    } = &**input
                    {
                        for input in inputs {
                            self.ensure_rendered(input, scope, worker_index);
                        }
                        let (ok_collection, err_collection) = match implementation {
                            expr::JoinImplementation::Differential(_start, _order) => {
                                self.render_join(input, predicates, scope)
                            }
                            expr::JoinImplementation::DeltaQuery(_orders) => self
                                .render_delta_join(input, predicates, scope, worker_index, |t| {
                                    t.saturating_sub(1)
                                }),
                            expr::JoinImplementation::Unimplemented => {
                                panic!("Attempt to render unimplemented join");
                            }
                        };
                        (ok_collection, err_collection.map(Into::into))
                    } else {
                        self.ensure_rendered(input, scope, worker_index);
                        let temp_storage = RowArena::new();
                        let predicates = predicates.clone();
                        let (ok_collection, err_collection) = self.collection(input).unwrap();
                        let (ok_collection, new_err_collection) =
                            ok_collection.filter_fallible(move |input_row| {
                                let datums = input_row.unpack();
                                for p in &predicates {
                                    if p.eval(&datums, &temp_storage)? != Datum::True {
                                        return Ok(false);
                                    }
                                }
                                Ok::<_, DataflowError>(true)
                            });
                        let err_collection = err_collection.concat(&new_err_collection);
                        (ok_collection, err_collection)
                    };
                    self.collections.insert(relation_expr.clone(), collections);
                }

                RelationExpr::Join {
                    inputs,
                    implementation,
                    ..
                } => {
                    for input in inputs {
                        self.ensure_rendered(input, scope, worker_index);
                    }
                    match implementation {
                        expr::JoinImplementation::Differential(_start, _order) => {
                            let collection = self.render_join(relation_expr, &[], scope);
                            self.collections.insert(relation_expr.clone(), collection);
                        }
                        expr::JoinImplementation::DeltaQuery(_orders) => {
                            let collection = self.render_delta_join(
                                relation_expr,
                                &[],
                                scope,
                                worker_index,
                                |t| t.saturating_sub(1),
                            );
                            self.collections.insert(relation_expr.clone(), collection);
                        }
                        expr::JoinImplementation::Unimplemented => {
                            panic!("Attempt to render unimplemented join");
                        }
                    }
                }

                RelationExpr::Reduce { input, .. } => {
                    self.ensure_rendered(input, scope, worker_index);
                    self.render_reduce(relation_expr, scope);
                }

                RelationExpr::TopK { input, .. } => {
                    self.ensure_rendered(input, scope, worker_index);
                    self.render_topk(relation_expr);
                }

                RelationExpr::Negate { input } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let (ok_collection, err_collection) = self.collection(input).unwrap();
                    let ok_collection = ok_collection.negate();
                    self.collections
                        .insert(relation_expr.clone(), (ok_collection, err_collection));
                }

                RelationExpr::Threshold { input } => {
                    self.ensure_rendered(input, scope, worker_index);
                    self.render_threshold(relation_expr);
                }

                RelationExpr::Union { left, right } => {
                    self.ensure_rendered(left, scope, worker_index);
                    self.ensure_rendered(right, scope, worker_index);

                    let (ok1, err1) = self.collection(left).unwrap();
                    let (ok2, err2) = self.collection(right).unwrap();

                    let ok = ok1.concat(&ok2);
                    let err = err1.concat(&err2);

                    self.collections.insert(relation_expr.clone(), (ok, err));
                }

                RelationExpr::ArrangeBy { input, keys } => {
                    // We can avoid rendering if we have all arrangements present,
                    // and there is at least one of them (to ensure the collection
                    // is available independent of arrangements).
                    if keys.is_empty()
                        || keys
                            .iter()
                            .any(|key| self.arrangement(&input, key).is_none())
                    {
                        self.ensure_rendered(input, scope, worker_index);
                    }
                    self.render_arrangeby(relation_expr, None);
                }
            };
        }
    }
}
