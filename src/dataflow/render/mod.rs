// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::worker::Worker as TimelyWorker;

use dataflow_types::*;
use expr::{EvalEnv, GlobalId, Id, RelationExpr, SourceInstanceId};
use repr::{Datum, Row, RowArena, RowPacker};

use self::context::{ArrangementFlavor, Context};
use super::sink;
use super::source;
use super::source::FileReadStyle;
use super::source::SourceToken;
use crate::arrangement::manager::{TraceManager, WithDrop};
use crate::decode::decode;
use crate::logging::materialized::{Logger, MaterializedEvent};
use crate::server::LocalInput;
use crate::server::TimestampHistories;

mod context;

pub(crate) fn build_local_input<A: Allocate>(
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    local_inputs: &mut HashMap<GlobalId, LocalInput>,
    index_id: GlobalId,
    name: &str,
    index: Index,
) {
    let worker_index = worker.index();
    let name = format!("Dataflow: {}", name);
    let worker_logging = worker.log_register().get("timely");
    worker.dataflow_core::<Timestamp, _, _, _>(&name, worker_logging, Box::new(()), |_, scope| {
        scope.clone().region(|region| {
            let mut context = Context::<_, _, _, Timestamp>::new();
            let ((handle, capability), stream) = region.new_unordered_input();
            if worker_index == 0 {
                local_inputs.insert(index.desc.on_id, LocalInput { handle, capability });
            }
            let get_expr = RelationExpr::global_get(index.desc.on_id, index.relation_type.clone());
            context
                .collections
                .insert(get_expr.clone(), stream.as_collection());
            context.render_arranged(
                &get_expr.clone().arrange_by(&[index.desc.keys.clone()]),
                &EvalEnv::default(),
                region,
                worker_index,
                Some(&index_id.to_string()),
            );
            match context.arrangement(&get_expr, &index.desc.keys) {
                Some(ArrangementFlavor::Local(local)) => {
                    manager.set_by_keys(
                        &index.desc,
                        WithDrop::new(local.trace, Rc::new(None::<source::SourceToken>)),
                    );
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
    advance_timestamp: bool,
    global_source_mappings: &mut HashMap<SourceInstanceId, Rc<Option<SourceToken>>>,
    timestamp_histories: TimestampHistories,
    logger: &mut Option<Logger>,
    executor: &tokio::runtime::Handle,
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
            // Load declared sources into the rendering context.
            for (source_number, (src_id, src)) in
                dataflow.source_imports.clone().into_iter().enumerate()
            {
                let (source, capability) = match src.connector.connector {
                    ExternalSourceConnector::Kafka(c) => {
                        // Distribute read responsibility among workers.
                        use differential_dataflow::hashable::Hashable;
                        let hash = src_id.hashed() as usize;
                        let read_from_kafka = hash % worker_peers == worker_index;
                        source::kafka(
                            region,
                            format!("kafka-{}-{}", dataflow.debug_name, source_number),
                            c,
                            src_id,
                            advance_timestamp,
                            timestamp_histories.clone(),
                            read_from_kafka,
                        )
                    }
                    ExternalSourceConnector::File(c) => {
                        let read_style = if worker_index != 0 {
                            FileReadStyle::None
                        } else if c.tail {
                            FileReadStyle::TailFollowFd
                        } else {
                            FileReadStyle::ReadOnce
                        };
                        source::file(
                            region,
                            format!("csv-{}", src_id),
                            c.path,
                            executor,
                            read_style,
                        )
                    }
                };

                let stream = decode(&source, src.connector.encoding, &dataflow.debug_name);

                // Introduce the stream by name, as an unarranged collection.
                context.collections.insert(
                    RelationExpr::global_get(src_id.sid, src.desc.typ().clone()),
                    stream.as_collection(),
                );
                let token = Rc::new(capability);
                source_tokens.insert(src_id.sid, token.clone());

                // We also need to keep track of this mapping globally to activate Kakfa sources
                // on timestamp advancement queries
                global_source_mappings.insert(src_id, token.clone());
            }

            let as_of = dataflow
                .as_of
                .as_ref()
                .map(|x| x.to_vec())
                .unwrap_or_else(|| vec![0]);

            let mut index_tokens = HashMap::new();

            for (id, (index_desc, typ)) in dataflow.index_imports.iter() {
                if let Some(trace) = manager.get_by_keys_mut(index_desc) {
                    let token = trace.to_drop().clone();
                    let (arranged, button) = trace.import_frontier_core(
                        scope,
                        &format!("Index({}, {:?})", index_desc.on_id, index_desc.keys),
                        as_of.clone(),
                    );
                    let arranged = arranged.enter(region);
                    let get_expr = RelationExpr::global_get(index_desc.on_id, typ.clone());
                    context.set_trace(&get_expr, &index_desc.keys, arranged);
                    index_tokens.insert(id, Rc::new((button.press_on_drop(), token)));
                } else {
                    panic!("Index import alarmingly absent!")
                }
            }

            for object in dataflow.objects_to_build.clone() {
                if let Some(typ) = object.typ {
                    context.ensure_rendered(
                        object.relation_expr.as_ref(),
                        &object.eval_env,
                        region,
                        worker_index,
                    );
                    context.collections.insert(
                        RelationExpr::global_get(object.id, typ.clone()),
                        context.collection(&object.relation_expr.as_ref()).unwrap(),
                    );
                } else {
                    context.render_arranged(
                        &object.relation_expr.as_ref(),
                        &object.eval_env,
                        region,
                        worker_index,
                        Some(&object.id.to_string()),
                    );
                }
            }

            for (export_id, index_desc, typ) in &dataflow.index_exports {
                // put together tokens that belong to the export
                let mut needed_source_tokens = Vec::new();
                let mut needed_index_tokens = Vec::new();
                for import_id in dataflow.get_imports(Some(&index_desc.on_id)) {
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
                    Some(ArrangementFlavor::Local(local)) => {
                        manager
                            .set_by_keys(&index_desc, WithDrop::new(local.trace.clone(), tokens));
                    }
                    Some(ArrangementFlavor::Trace(_)) => {
                        // do nothing. there already exists an system
                        // index on the same keys
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
                for import_id in dataflow.get_imports(Some(&sink.from.0)) {
                    if let Some(index_token) = index_tokens.get(&import_id) {
                        needed_index_tokens.push(index_token.clone());
                    } else if let Some(source_token) = source_tokens.get(&import_id) {
                        needed_source_tokens.push(source_token.clone());
                    }
                }
                let tokens = Rc::new((needed_source_tokens, needed_index_tokens));
                let collection = context
                    .collection(&RelationExpr::global_get(
                        sink.from.0,
                        sink.from.1.typ().clone(),
                    ))
                    .expect("No arrangements");

                match sink.connector {
                    SinkConnector::Kafka(c) => {
                        sink::kafka(&collection.inner, sink_id, c, sink.from.1)
                    }
                    SinkConnector::Tail(c) => sink::tail(&collection.inner, sink_id, c),
                }
                dataflow_drops.insert(sink_id, Box::new(tokens));
            }
        });
    })
}

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: timely::progress::Timestamp + Lattice,
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
        env: &EvalEnv,
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
                    use timely::dataflow::operators::{Map, ToStream};
                    let rows = if worker_index == 0 {
                        rows.clone()
                    } else {
                        vec![]
                    };

                    let collection = rows
                        .to_stream(scope)
                        .map(|(x, diff)| (x, timely::progress::Timestamp::minimum(), diff))
                        .as_collection();

                    self.collections.insert(relation_expr.clone(), collection);
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
                        self.ensure_rendered(value, env, scope, worker_index);
                        self.clone_from_to(value, &bind);
                        self.ensure_rendered(body, env, scope, worker_index);
                        self.clone_from_to(body, relation_expr);
                    }
                }

                RelationExpr::Project { input, outputs } => {
                    self.ensure_rendered(input, env, scope, worker_index);
                    let outputs = outputs.clone();
                    let collection = self.collection(input).unwrap().map(move |row| {
                        let datums = row.unpack();
                        Row::pack(outputs.iter().map(|i| datums[*i]))
                    });

                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Map { input, scalars } => {
                    self.ensure_rendered(input, env, scope, worker_index);
                    let env = env.clone();
                    let scalars = scalars.clone();
                    let collection = self.collection(input).unwrap().map(move |input_row| {
                        let mut datums = input_row.unpack();
                        let temp_storage = RowArena::new();
                        for scalar in &scalars {
                            let datum = scalar.eval(&datums, &env, &temp_storage);
                            // Scalar is allowed to see the outputs of previous scalars.
                            // To avoid repeatedly unpacking input_row, we just push the outputs into datums so later scalars can see them.
                            // Note that this doesn't mutate input_row.
                            datums.push(datum);
                        }
                        Row::pack(&*datums)
                    });

                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::FlatMapUnary { input, func, expr } => {
                    self.ensure_rendered(input, env, scope, worker_index);
                    let env = env.clone();
                    let func = func.clone();
                    let expr = expr.clone();
                    let collection = self.collection(input).unwrap().flat_map(move |input_row| {
                        let datums = input_row.unpack();
                        let temp_storage = RowArena::new();
                        let output_rows =
                            func.eval(expr.eval(&datums, &env, &temp_storage), &env, &temp_storage);
                        output_rows
                            .into_iter()
                            .map(|output_row| {
                                Row::pack(
                                    input_row.clone().into_iter().chain(output_row.into_iter()),
                                )
                            })
                            .collect::<Vec<_>>()
                    });

                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Filter { input, predicates } => {
                    self.ensure_rendered(input, env, scope, worker_index);
                    let env = env.clone();
                    let predicates = predicates.clone();
                    let collection = self.collection(input).unwrap().filter(move |input_row| {
                        let datums = input_row.unpack();
                        let temp_storage = RowArena::new();
                        predicates.iter().all(|predicate| {
                            match predicate.eval(&datums, &env, &temp_storage) {
                                Datum::True => true,
                                Datum::False | Datum::Null => false,
                                _ => unreachable!(),
                            }
                        })
                    });

                    self.collections.insert(relation_expr.clone(), collection);
                    // TODO: We could add filtered traces in principle, but the trace wrapper types are problematic.
                }

                RelationExpr::Join { .. } => {
                    self.render_join(relation_expr, env, scope, worker_index);
                }

                RelationExpr::Reduce { .. } => {
                    self.render_reduce(relation_expr, env, scope, worker_index);
                }

                RelationExpr::TopK { .. } => {
                    self.render_topk(relation_expr, env, scope, worker_index);
                }

                RelationExpr::Negate { input } => {
                    self.ensure_rendered(input, env, scope, worker_index);
                    let collection = self.collection(input).unwrap().negate();
                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Threshold { .. } => {
                    self.render_threshold(relation_expr, env, scope, worker_index);
                }

                RelationExpr::Union { left, right } => {
                    self.ensure_rendered(left, env, scope, worker_index);
                    self.ensure_rendered(right, env, scope, worker_index);

                    let input1 = self.collection(left).unwrap();
                    let input2 = self.collection(right).unwrap();

                    self.collections
                        .insert(relation_expr.clone(), input1.concat(&input2));
                }

                RelationExpr::ArrangeBy { .. } => {
                    self.render_arranged(relation_expr, env, scope, worker_index, None);
                }
            };
        }
    }

    fn render_arranged(
        &mut self,
        relation_expr: &RelationExpr,
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
        id: Option<&str>,
    ) {
        if let RelationExpr::ArrangeBy { input, keys } = relation_expr {
            for key_set in keys {
                if self.arrangement(&input, &key_set).is_none() {
                    self.ensure_rendered(input, env, scope, worker_index);
                    let built = self.collection(input).unwrap();
                    let keys2 = key_set.clone();
                    let env = env.clone();
                    let name = if let Some(id) = id {
                        format!("Arrange: {}", id)
                    } else {
                        "Arrange".to_string()
                    };
                    let keyed = built
                        .map(move |row| {
                            let datums = row.unpack();
                            let temp_storage = RowArena::new();
                            let key_row = Row::pack(
                                keys2.iter().map(|k| k.eval(&datums, &env, &temp_storage)),
                            );
                            (key_row, row)
                        })
                        .arrange_named::<OrdValSpine<_, _, _, _>>(&name);
                    self.set_local(&input, key_set, keyed);
                }
                if self.arrangement(relation_expr, key_set).is_none() {
                    match self.arrangement(&input, key_set).unwrap() {
                        ArrangementFlavor::Local(local) => {
                            self.set_local(relation_expr, key_set, local);
                        }
                        ArrangementFlavor::Trace(trace) => {
                            self.set_trace(relation_expr, key_set, trace);
                        }
                    }
                }
            }
        }
    }

    fn render_join(
        &mut self,
        relation_expr: &RelationExpr,
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
    ) {
        if let RelationExpr::Join {
            inputs,
            variables,
            demand,
        } = relation_expr
        {
            // For the moment, assert that each relation participates at most
            // once in each equivalence class. If not, we should be able to
            // push a filter upwards, and if we can't do that it means a bit
            // more filter logic in this operator which doesn't exist yet.
            assert!(variables.iter().all(|h| {
                let len = h.len();
                let mut list = h.iter().map(|(i, _)| i).collect::<Vec<_>>();
                list.sort();
                list.dedup();
                len == list.len()
            }));

            let variables = variables
                .iter()
                .map(|v| {
                    let mut result = v.clone();
                    result.sort();
                    result
                })
                .collect::<Vec<_>>();

            for input in inputs.iter() {
                self.ensure_rendered(input, env, scope, worker_index);
            }

            let types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
            let arities = types
                .iter()
                .map(|t| t.column_types.len())
                .collect::<Vec<_>>();
            let mut offset = 0;
            let mut prior_arities = Vec::new();
            for input in 0..inputs.len() {
                prior_arities.push(offset);
                offset += arities[input];
            }

            // Unwrap demand
            let demand = if let Some(demand) = demand {
                demand.clone()
            } else {
                // Assume demand encompasses all columns
                arities.iter().map(|arity| (0..*arity).collect()).collect()
            };
            // The relation_expr is to implement join as a `fold` over `inputs`.
            let mut input_iter = inputs.iter().enumerate();
            if let Some((index, input)) = input_iter.next() {
                // This collection will evolve as we join in more inputs.
                let mut joined = self.collection(input).unwrap();

                // Maintain sources of each in-progress column.
                let mut columns = (0..arities[index]).map(|c| (index, c)).collect::<Vec<_>>();

                // The intent is to maintain `joined` as the full cross
                // product of all input relations so far, subject to all
                // of the equality constraints in `variables`. This means
                for (index, input) in input_iter {
                    // Determine keys. there is at most one key for each
                    // equivalence class, and an equivalence class is only
                    // engaged if it contains both a new and an old column.
                    // If the class contains *more than one* new column we
                    // may need to put a `filter` in, or perhaps await a
                    // later join (and ensure that one exists).
                    let mut old_keys = Vec::new();
                    let mut new_keys = Vec::new();

                    // Determine which columns from `joined` and `input` will be kept
                    // Initialize the list of kept columns from `demand`
                    let mut old_outputs = (0..index)
                        .flat_map(|i| demand[i].iter().map(|c| (i, *c)).collect::<Vec<_>>())
                        .map(|c| columns.iter().position(|c2| c == *c2).unwrap())
                        .collect::<Vec<_>>();
                    let mut new_outputs = demand[index].clone();

                    for equivalence in variables.iter() {
                        // Keep columns that are needed for future joins
                        if equivalence.last().unwrap().0 > index {
                            match equivalence[0].0.cmp(&index) {
                                Ordering::Less => old_outputs.push(
                                    columns.iter().position(|c2| equivalence[0] == *c2).unwrap(),
                                ),
                                Ordering::Equal => new_outputs.push(equivalence[0].1),
                                Ordering::Greater => {
                                    // If the relation exceeds the current index,
                                    // we don't need to worry about retaining it
                                    // at this moment.
                                }
                            }
                        }

                        // If a key exists in `joined`
                        if equivalence[0].0 < index {
                            // Look for a key in `input`
                            let new_pos = equivalence
                                .iter()
                                .filter(|(i, _)| i == &index)
                                .map(|(_, c)| *c)
                                .next();
                            // If a key in input is found, register join keys
                            if let Some(new_pos) = new_pos {
                                old_keys.push(
                                    columns.iter().position(|i| *i == equivalence[0]).unwrap(),
                                );
                                new_keys.push(new_pos);
                            }
                        }
                    }

                    // Dedup both sets of outputs
                    old_outputs.sort();
                    old_outputs.dedup();
                    new_outputs.sort();
                    new_outputs.dedup();
                    // List the new locations the columns will be in
                    columns = old_outputs
                        .iter()
                        .map(|i| columns[*i])
                        .chain(new_outputs.iter().map(|i| (index, *i)))
                        .collect();

                    let old_keyed = joined
                        .map(move |row| {
                            let datums = row.unpack();
                            let key_row = Row::pack(old_keys.iter().map(|i| datums[*i]));
                            (key_row, row)
                        })
                        .arrange_named::<OrdValSpine<_, _, _, _>>(&format!("JoinStage: {}", index));

                    // TODO: easier idioms for detecting, re-using, and stashing.
                    if self.arrangement_columns(&input, &new_keys[..]).is_none() {
                        let built = self.collection(input).unwrap();
                        let new_keys2 = new_keys.clone();
                        let new_keyed = built
                            .map(move |row| {
                                let datums = row.unpack();
                                let key_row = Row::pack(new_keys2.iter().map(|i| datums[*i]));
                                (key_row, row)
                            })
                            .arrange_named::<OrdValSpine<_, _, _, _>>(&format!(
                                "JoinIndex: {}",
                                index
                            ));
                        self.set_local_columns(&input, &new_keys[..], new_keyed);
                    }

                    joined = match self.arrangement_columns(&input, &new_keys[..]) {
                        Some(ArrangementFlavor::Local(local)) => {
                            old_keyed.join_core(&local, move |_keys, old, new| {
                                let old_datums = old.unpack();
                                let new_datums = new.unpack();
                                Some(Row::pack(
                                    old_outputs
                                        .iter()
                                        .map(|i| &old_datums[*i])
                                        .chain(new_outputs.iter().map(|i| &new_datums[*i])),
                                ))
                            })
                        }
                        Some(ArrangementFlavor::Trace(trace)) => {
                            old_keyed.join_core(&trace, move |_keys, old, new| {
                                let old_datums = old.unpack();
                                let new_datums = new.unpack();
                                Some(Row::pack(
                                    old_outputs
                                        .iter()
                                        .map(|i| &old_datums[*i])
                                        .chain(new_outputs.iter().map(|i| &new_datums[*i])),
                                ))
                            })
                        }
                        None => {
                            panic!("Arrangement alarmingly absent!");
                        }
                    };
                }

                // Permute back to the original positions
                let mut inverse_columns: Vec<(usize, usize)> = columns
                    .iter()
                    .map(|(input, col)| prior_arities[*input] + *col)
                    .enumerate()
                    .map(|(new_col, original_col)| (original_col, new_col))
                    .collect();
                inverse_columns.sort();
                let mut inverse_columns_iter = inverse_columns.iter().peekable();
                let mut outputs = Vec::new();
                for i in 0..arities.iter().sum() {
                    if let Some((original_col, new_col)) = inverse_columns_iter.peek() {
                        if i == *original_col {
                            outputs.push(Some(*new_col));
                            inverse_columns_iter.next();
                            continue;
                        }
                    }
                    outputs.push(None);
                }
                let dummy_data = types
                    .iter()
                    .flat_map(|t| &t.column_types)
                    .map(|t| {
                        if t.nullable {
                            Datum::Null
                        } else {
                            t.scalar_type.dummy_datum()
                        }
                    })
                    .collect::<Vec<_>>();
                joined = joined.map(move |row| {
                    let datums = row.unpack();
                    Row::pack(
                        outputs
                            .iter()
                            .zip(dummy_data.iter())
                            .map(|(new_col, dummy)| {
                                if let Some(new_col) = new_col {
                                    datums[*new_col]
                                } else {
                                    // Regenerate any columns ignored during join with dummy data
                                    *dummy
                                }
                            }),
                    )
                });
                self.collections.insert(relation_expr.clone(), joined);
            } else {
                panic!("Empty join; why?");
            }
        }
    }

    fn render_reduce(
        &mut self,
        relation_expr: &RelationExpr,
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
    ) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } = relation_expr
        {
            use differential_dataflow::operators::reduce::ReduceCore;
            use timely::dataflow::operators::map::Map;

            let keys_clone = group_key.clone();

            self.ensure_rendered(input, env, scope, worker_index);
            let input = self.collection(input).unwrap();

            use expr::AggregateFunc;

            // Reduce has the ability to lift any Abelian, non-distinct aggregations
            // into the diff field. We also need to maintain the count as well, as we
            // need to distinguish "things that accumulate to zero" from "the absence
            // of things".

            // We have an additional opportunity to discard any parts of the record
            // that do not contribute to the non-Abelian or distinct aggregations.
            // This is almost surely important to reduce the in-place footprint of
            // these records.

            // Track whether aggregations are Abelian (and so accumulable) or not.
            let mut abelian = Vec::new();
            for aggregate in aggregates.iter() {
                let accumulable = match aggregate.func {
                    AggregateFunc::SumInt32 => !aggregate.distinct,
                    AggregateFunc::SumInt64 => !aggregate.distinct,
                    AggregateFunc::SumFloat32 => !aggregate.distinct,
                    AggregateFunc::SumFloat64 => !aggregate.distinct,
                    AggregateFunc::SumDecimal => !aggregate.distinct,
                    AggregateFunc::Count => !aggregate.distinct,
                    AggregateFunc::CountAll => !aggregate.distinct,
                    _ => false,
                };

                abelian.push(accumulable);
            }

            let abelian2 = abelian.clone();
            let aggregates_clone = aggregates.clone();

            let float_scale = f64::from(1 << 24);

            // Our first action is to take our input from a collection of `tuple`
            // to one structured as `((keys, vals), time, aggs)`
            let exploded = input
                .map({
                    let env = env.clone();
                    let group_key = group_key.clone();
                    let temp_storage = RowArena::new();
                    move |row| {
                        let datums = row.unpack();

                        let keys = Row::pack(
                            group_key
                                .iter()
                                .map(|i| i.eval(&datums, &env, &temp_storage)),
                        );

                        let mut vals = RowPacker::new();
                        let mut aggs = vec![1i128];

                        for (index, aggregate) in aggregates_clone.iter().enumerate() {
                            // Presently, we can accumulate in the difference field only
                            // if the aggregation has a known type and does not require
                            // us to accumulate only distinct elements.
                            //
                            // To enable the optimization where distinctness is required,
                            // consider restructuring the plan to pre-distinct the right
                            // data and then use a non-distinctness-requiring aggregation.

                            let eval = aggregate.expr.eval(&datums, &env, &temp_storage);

                            // Non-Abelian values cannot be accumulated, and just need to
                            // be passed along.
                            if !abelian2[index] {
                                vals.push(eval);
                            } else {
                                // We can promote the content of `eval` into the difference,
                                // but we need to retain the NULL-ness somewhere so that we
                                // can distinguish zero accumulations from those that are
                                // entirely NULLs.

                                // We have already retained the count in the first coordinate,
                                // and would only want to record the unit value here, anyhow.
                                match aggregate.func {
                                    AggregateFunc::CountAll => {
                                        // Nothing beyond the accumulated count is needed.
                                    }
                                    AggregateFunc::Count => {
                                        // Count needs to distinguish nulls from zero.
                                        aggs.push(if eval.is_null() { 0 } else { 1 });
                                    }
                                    _ => {
                                        // Other accumulations need to disentangle the accumulable
                                        // value from its NULL-ness, which is not quite as easily
                                        // accumulated.
                                        let (value, non_null) = match eval {
                                            Datum::Int32(i) => (i128::from(i), 1),
                                            Datum::Int64(i) => (i128::from(i), 1),
                                            Datum::Float32(f) => {
                                                ((f64::from(*f) * float_scale) as i128, 1)
                                            }
                                            Datum::Float64(f) => ((*f * float_scale) as i128, 1),
                                            Datum::Decimal(d) => (d.as_i128(), 1),
                                            Datum::Null => (0, 0),
                                            x => panic!("Accumulating non-integer data: {:?}", x),
                                        };
                                        aggs.push(value);
                                        aggs.push(non_null);
                                    }
                                }
                            }
                        }

                        // A DiffVector holds multiple monoidal accumulations.
                        (
                            keys,
                            vals.finish(),
                            differential_dataflow::difference::DiffVector::new(aggs),
                        )
                    }
                })
                .inner
                .map(|(data, time, diff)| (data, time, diff as i128))
                .as_collection()
                .explode(|(keys, vals, aggs)| Some(((keys, vals), aggs)));

            let mut sums = Vec::<i128>::new();

            // We now reduce by `keys`, performing both Abelian and non-Abelian aggregations.
            let aggregates = aggregates.clone();
            let arrangement =
                exploded
                    .arrange_named::<OrdValSpine<
                        Row,
                        _,
                        _,
                        differential_dataflow::difference::DiffVector<i128>,
                    >>("ReduceStage")
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                        "Reduce",
                        {
                            let env = env.clone();
                            move |key, source, target| {
                                sums.clear();
                                sums.extend(&source[0].1[..]);
                                for record in source[1..].iter() {
                                    for index in 0..sums.len() {
                                        sums[index] += record.1[index];
                                    }
                                }

                                // Our output will be [keys; aggregates].
                                let mut result = RowPacker::new();
                                result.extend(key.iter());

                                let mut abelian_pos = 1; // <- advance past the count
                                let mut non_abelian_pos = 0;

                                for (agg, abl) in aggregates.iter().zip(abelian.iter()) {
                                    if *abl {
                                        let value = match &agg.func {
                                            AggregateFunc::SumInt32 => {
                                                let total = sums[abelian_pos] as i32;
                                                let non_nulls = sums[abelian_pos + 1] as i32;
                                                abelian_pos += 2;
                                                if non_nulls > 0 {
                                                    Datum::Int32(total)
                                                } else {
                                                    Datum::Null
                                                }
                                            }
                                            AggregateFunc::SumInt64 => {
                                                let total = sums[abelian_pos] as i64;
                                                let non_nulls = sums[abelian_pos + 1] as i64;
                                                abelian_pos += 2;
                                                if non_nulls > 0 {
                                                    Datum::Int64(total)
                                                } else {
                                                    Datum::Null
                                                }
                                            }
                                            AggregateFunc::SumFloat32 => {
                                                let total = sums[abelian_pos];
                                                let non_nulls = sums[abelian_pos + 1];
                                                abelian_pos += 2;
                                                if non_nulls > 0 {
                                                    Datum::Float32(
                                                        (((total as f64) / float_scale) as f32).into(),
                                                    )
                                                } else {
                                                    Datum::Null
                                                }
                                            }
                                            AggregateFunc::SumFloat64 => {
                                                let total = sums[abelian_pos];
                                                let non_nulls = sums[abelian_pos + 1];
                                                abelian_pos += 2;
                                                if non_nulls > 0 {
                                                    Datum::Float64(
                                                        ((total as f64) / float_scale).into(),
                                                    )
                                                } else {
                                                    Datum::Null
                                                }
                                            }
                                            AggregateFunc::SumDecimal => {
                                                let total = sums[abelian_pos];
                                                let non_nulls = sums[abelian_pos + 1];
                                                abelian_pos += 2;
                                                if non_nulls > 0 {
                                                    Datum::from(total)
                                                } else {
                                                    Datum::Null
                                                }
                                            }
                                            AggregateFunc::Count => {
                                                // Does not count NULLs.
                                                let total = sums[abelian_pos] as i64;
                                                abelian_pos += 1;
                                                Datum::Int64(total)
                                            }
                                            AggregateFunc::CountAll => {
                                                let total = sums[0] as i64;
                                                Datum::Int64(total)
                                            }
                                            x => panic!("Surprising Abelian aggregation: {:?}", x),
                                        };
                                        result.push(value);
                                    } else {
                                        if agg.distinct {
                                            let iter = source
                                                .iter()
                                                .flat_map(|(v, w)| {
                                                    if w[0] > 0 {
                                                        // <-- really should be true
                                                        Some(v.iter().nth(non_abelian_pos).unwrap())
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect::<HashSet<_>>();
                                            let temp_storage = RowArena::new();
                                            result.push(agg.func.eval(iter, &env, &temp_storage));
                                        } else {
                                            let iter = source.iter().flat_map(|(v, w)| {
                                                // let eval = agg.expr.eval(v);
                                                std::iter::repeat(v.iter().nth(non_abelian_pos).unwrap())
                                                    .take(std::cmp::max(w[0], 0) as usize)
                                            });
                                            let temp_storage = RowArena::new();
                                            result.push(agg.func.eval(iter, &env, &temp_storage));
                                        }
                                        non_abelian_pos += 1;
                                    }
                                }
                                target.push((result.finish(), 1isize));
                            }
                        }
                    );

            let index = (0..keys_clone.len()).collect::<Vec<_>>();
            self.set_local_columns(relation_expr, &index[..], arrangement);
        }
    }

    fn render_topk(
        &mut self,
        relation_expr: &RelationExpr,
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
    ) {
        if let RelationExpr::TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
        } = relation_expr
        {
            use differential_dataflow::operators::reduce::Reduce;

            self.ensure_rendered(input, env, scope, worker_index);
            let input = self.collection(input).unwrap();

            // To provide a robust incremental orderby-limit experience, we want to avoid grouping
            // *all* records (or even large groups) and then applying the ordering and limit. Instead,
            // a more robust approach forms groups of bounded size (here, 16) and applies the offset
            // and limit to each, and then increases the sizes of the groups.

            // Builds a "stage", which uses a finer grouping than is required to reduce the volume of
            // updates, and to reduce the amount of work on the critical path for updates. The cost is
            // a larger number of arrangements when this optimization does nothing beneficial.
            fn build_topk_stage<G>(
                collection: Collection<G, ((Row, u64), Row), Diff>,
                order_key: &[expr::ColumnOrder],
                modulus: u64,
                offset: usize,
                limit: Option<usize>,
            ) -> Collection<G, ((Row, u64), Row), Diff>
            where
                G: Scope,
                G::Timestamp: Lattice,
            {
                let order_clone = order_key.to_vec();

                collection
                    .map(move |((key, hash), row)| ((key, hash % modulus), row))
                    .reduce_named("TopK", {
                        move |_key, source, target| {
                            target.extend(source.iter().map(|&(row, diff)| (row.clone(), diff)));
                            let must_shrink = offset > 0
                                || limit
                                    .map(|l| {
                                        target.iter().map(|(_, d)| *d).sum::<isize>() as usize > l
                                    })
                                    .unwrap_or(false);
                            if must_shrink {
                                if !order_clone.is_empty() {
                                    //todo: use arrangements or otherwise make the sort more performant?
                                    let sort_by = |left: &(Row, isize), right: &(Row, isize)| {
                                        compare_columns(
                                            &order_clone,
                                            &left.0.unpack(),
                                            &right.0.unpack(),
                                            || left.cmp(right),
                                        )
                                    };
                                    target.sort_by(sort_by);
                                }

                                let mut skipped = 0; // Number of records offset so far
                                let mut output = 0; // Number of produced output records.
                                let mut cursor = 0; // Position of current input record.

                                //skip forward until an offset number of records is reached
                                while cursor < target.len() {
                                    if skipped + (target[cursor].1 as usize) > offset {
                                        break;
                                    }
                                    skipped += target[cursor].1 as usize;
                                    cursor += 1;
                                }
                                let skip_cursor = cursor;
                                if cursor < target.len() {
                                    if skipped < offset {
                                        //if offset only skips some members of a group of identical
                                        //records, return the rest
                                        target[skip_cursor].1 -= (offset - skipped) as isize;
                                    }
                                    //apply limit
                                    if let Some(limit) = limit {
                                        while output < limit && cursor < target.len() {
                                            let to_emit = std::cmp::min(
                                                limit - output,
                                                target[cursor].1 as usize,
                                            );
                                            target[cursor].1 = to_emit as isize;
                                            output += to_emit;
                                            cursor += 1;
                                        }
                                        target.truncate(cursor);
                                    }
                                }
                                target.drain(..skip_cursor);
                            }
                        }
                    })
            }

            let group_clone = group_key.to_vec();
            let mut collection = input.map(move |row| {
                use differential_dataflow::hashable::Hashable;
                let row_hash = row.hashed();
                let datums = row.unpack();
                let group_row = Row::pack(group_clone.iter().map(|i| datums[*i]));
                ((group_row, row_hash), row)
            });
            // This sequence of numbers defines the shifts that happen to the 64 bit hash
            // of the record, and has the properties that 1. there are not too many of them,
            // and 2. each has a modest difference to the next.
            //
            // These two properties mean that there should be no reductions on groups that
            // are substantially larger than `offset + limit` (the largest factor should be
            // bounded by two raised to the difference between subsequent numbers);
            if let Some(limit) = limit {
                for log_modulus in
                    [60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4u64].iter()
                {
                    // here we do not apply `offset`, but instead restrict ourself with a limit
                    // that includes the offset. We cannot apply `offset` until we perform the
                    // final, complete reduction.
                    collection = build_topk_stage(
                        collection,
                        order_key,
                        1u64 << log_modulus,
                        0,
                        Some(*offset + *limit),
                    );
                }
            }

            // We do a final step, both to make sure that we complete the reduction, and to correctly
            // apply `offset` to the final group, as we have not yet been applying it to the partially
            // formed groups.
            let result = build_topk_stage(collection, order_key, 1u64, *offset, *limit)
                .map(|((_key, _hash), row)| row);
            self.collections.insert(relation_expr.clone(), result);
        }
    }

    fn render_threshold(
        &mut self,
        relation_expr: &RelationExpr,
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
    ) {
        if let RelationExpr::Threshold { input } = relation_expr {
            // TODO: re-use and publish arrangement here.
            let arity = input.arity();
            let keys = (0..arity).collect::<Vec<_>>();

            // TODO: easier idioms for detecting, re-using, and stashing.
            if self.arrangement_columns(&input, &keys[..]).is_none() {
                self.ensure_rendered(input, env, scope, worker_index);
                let built = self.collection(input).unwrap();
                let keys2 = keys.clone();
                let keyed = built
                    .map(move |row| {
                        let datums = row.unpack();
                        let key_row = Row::pack(keys2.iter().map(|i| datums[*i]));
                        (key_row, row)
                    })
                    .arrange_by_key();
                self.set_local_columns(&input, &keys[..], keyed);
            }

            use differential_dataflow::operators::reduce::ReduceCore;

            let arranged = match self.arrangement_columns(&input, &keys[..]) {
                Some(ArrangementFlavor::Local(local)) => local
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("Threshold", move |_k, s, t| {
                        for (record, count) in s.iter() {
                            if *count > 0 {
                                t.push(((*record).clone(), *count));
                            }
                        }
                    }),
                Some(ArrangementFlavor::Trace(trace)) => trace
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("Threshold", move |_k, s, t| {
                        for (record, count) in s.iter() {
                            if *count > 0 {
                                t.push(((*record).clone(), *count));
                            }
                        }
                    }),
                None => {
                    panic!("Arrangement alarmingly absent!");
                }
            };

            let index = (0..keys.len()).collect::<Vec<_>>();
            self.set_local_columns(relation_expr, &index[..], arranged);
        }
    }
}
