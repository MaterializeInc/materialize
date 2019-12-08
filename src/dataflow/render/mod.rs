// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::AsCollection;
use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::worker::Worker as TimelyWorker;
use tokio;

use dataflow_types::*;
use expr::{GlobalId, Id, RelationExpr};
use repr::{Datum, Row, RowPacker, RowUnpacker};

use super::sink;
use super::source;
use crate::arrangement::manager::{KeysValsSpine, TraceManager, WithDrop};
use crate::logging::materialized::{Logger, MaterializedEvent};
use crate::server::LocalInput;

mod context;
use context::{ArrangementFlavor, Context};

pub(crate) fn build_dataflow<A: Allocate, E: tokio::executor::Executor + Clone>(
    dataflow: DataflowDesc,
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    dataflow_drops: &mut HashMap<GlobalId, Box<dyn Any>>,
    local_inputs: &mut HashMap<GlobalId, LocalInput>,
    logger: &mut Option<Logger>,
    executor: &mut E,
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

            let mut source_tokens = Vec::new();
            // Load declared sources into the rendering context.
            for (src_id, src) in dataflow.sources {
                let (stream, capability) = match src.connector {
                    SourceConnector::Kafka(c) => {
                        // Distribute read responsibility among workers.
                        use differential_dataflow::hashable::Hashable;
                        let hash = src_id.hashed() as usize;
                        let read_from_kafka = hash % worker_peers == worker_index;
                        source::kafka(region, format!("kafka-{}", src_id), c, read_from_kafka)
                    }
                    SourceConnector::Local => {
                        let ((handle, capability), stream) = region.new_unordered_input();
                        if worker_index == 0 {
                            local_inputs.insert(src_id, LocalInput { handle, capability });
                        }
                        (stream, None)
                    }
                    SourceConnector::File(c) => match c.format {
                        FileFormat::Csv(n_cols) => {
                            let read_file = worker_index == 0;
                            let stream = source::csv(
                                region,
                                format!("csv-{}", src_id),
                                c.path,
                                n_cols,
                                executor.clone(),
                                read_file,
                            );
                            (stream, None)
                        }
                    },
                };

                // Introduce the stream by name, as an unarranged collection.
                context.collections.insert(
                    RelationExpr::Get {
                        id: Id::Global(src_id),
                        typ: src.desc.typ().clone(),
                    },
                    stream.as_collection(),
                );
                source_tokens.push(capability);
            }

            let source_tokens = Rc::new(source_tokens);

            for (view_id, view) in dataflow.views {
                let mut tokens = Vec::new();
                let as_of = dataflow
                    .as_of
                    .as_ref()
                    .map(|x| x.to_vec())
                    .unwrap_or_else(|| vec![0]);

                view.relation_expr.visit(&mut |e| {
                    // Some `Get` expressions are for let bindings, and should not be loaded.
                    // We might want explicitly enumerate assets to import.
                    if let RelationExpr::Get {
                        id: Id::Global(id),
                        typ: _,
                    } = e
                    {
                        // Import arrangements for this collection.
                        // TODO: we could import only used arrangements.
                        if let Some(traces) = manager.get_all_keyed(*id) {
                            for (key, trace) in traces {
                                let token = trace.to_drop().clone();
                                let (arranged, button) = trace.import_frontier_core(
                                    scope,
                                    &format!("View({}, {:?})", id, key),
                                    as_of.clone(),
                                );
                                let arranged = arranged.enter(region);
                                context.set_trace(&e, &key, arranged);
                                tokens.push((button.press_on_drop(), token));
                            }

                            // Log the dependency.
                            if let Some(logger) = logger {
                                logger.log(MaterializedEvent::DataflowDependency {
                                    dataflow: view_id,
                                    source: *id,
                                });
                            }
                        }
                    }
                });

                // Capture both the tokens of imported traces and those of sources.
                let tokens = Rc::new((tokens, source_tokens.clone()));

                context.ensure_rendered(&view.relation_expr, region, worker_index);

                // Having ensured that `view.relation_expr` is rendered, we can now extract it
                // or re-arrange it by other keys. The only information we have at the moment
                // is whether the dataflow results in an arranged form of the expression.

                if let Some(arrangements) = context.get_all_local(&view.relation_expr) {
                    if arrangements.is_empty() {
                        panic!("Lied to about arrangement availability");
                    }
                    // TODO: This stores all arrangements. Should we store fewer?
                    for (key, arrangement) in arrangements {
                        manager.set_by_keys(
                            view_id,
                            &key[..],
                            WithDrop::new(arrangement.trace.clone(), tokens.clone()),
                        );
                    }
                } else {
                    let mut keys = view.relation_expr.typ().keys.clone();
                    if keys.is_empty() {
                        keys.push((0..view.relation_expr.arity()).collect::<Vec<_>>());
                    }
                    for key in keys {
                        let key_clone = key.clone();
                        let mut unpacker = RowUnpacker::new();
                        let mut packer = RowPacker::new();
                        let arrangement = context
                            .collection(&view.relation_expr)
                            .expect("Render failed to produce collection")
                            .map(move |row| {
                                let datums = unpacker.unpack(&row);
                                let key_row = packer.pack(key.iter().map(|k| datums[*k]));
                                drop(datums);
                                (key_row, row)
                            })
                            .arrange_named::<KeysValsSpine>(&format!("Arrange: {}", view_id));
                        manager.set_by_keys(
                            view_id,
                            &key_clone[..],
                            WithDrop::new(arrangement.trace, tokens.clone()),
                        );
                    }
                }
            }

            for (_idx_id, idx) in dataflow.indexes {
                let mut tokens = Vec::new();
                let get_expr = RelationExpr::Get {
                    id: Id::Global(idx.on_id),
                    typ: idx.relation_type.clone(),
                };
                // TODO (wangandi) for the function-based column case,
                // think about checking if there is another index
                // with the function pre-rendered
                let (key, trace) = manager.get_default_with_key(idx.on_id).unwrap();
                let token = trace.to_drop().clone();
                let (arranged, button) = trace.import_frontier_core(
                    scope,
                    &format!("View({}, {:?})", &idx.on_id, key),
                    vec![0],
                );
                let arranged = arranged.enter(region);
                context.set_trace(&get_expr, &key, arranged);
                tokens.push((button.press_on_drop(), token));

                // Capture both the tokens of imported traces and those of sources.
                let tokens = Rc::new((tokens, source_tokens.clone()));

                let to_arrange = if idx.funcs.is_empty() {
                    get_expr
                } else {
                    RelationExpr::Map {
                        input: Box::new(get_expr),
                        scalars: idx.funcs.clone(),
                    }
                };
                context.ensure_rendered(
                    &to_arrange.clone().arrange_by(&idx.keys),
                    region,
                    worker_index,
                );

                match context.arrangement(&to_arrange, &idx.keys) {
                    Some(ArrangementFlavor::Local(local)) => {
                        manager.set_user_created(
                            idx.on_id,
                            &idx.keys,
                            WithDrop::new(local.trace.clone(), tokens.clone()),
                        );
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

            for (sink_id, sink) in dataflow.sinks {
                let (_key, trace) = manager
                    .get_all_keyed(sink.from.0)
                    .expect("View missing")
                    .next()
                    .expect("No arrangements");
                let token = trace.to_drop().clone();
                let (arrangement, button) =
                    trace.import_core(scope, &format!("Import({:?})", sink.from));

                match sink.connector {
                    SinkConnector::Kafka(c) => sink::kafka(&arrangement.stream, sink_id, c),
                    SinkConnector::Tail(c) => sink::tail(&arrangement.stream, sink_id, c),
                }

                dataflow_drops.insert(sink_id, Box::new((token, button.press_on_drop())));
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
                        .map(|(x, diff)| (x, Default::default(), diff))
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
                        self.ensure_rendered(value, scope, worker_index);
                        self.clone_from_to(value, &bind);
                        self.ensure_rendered(body, scope, worker_index);
                        self.clone_from_to(body, relation_expr);
                    }
                }

                RelationExpr::Project { input, outputs } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let outputs = outputs.clone();
                    let mut unpacker = RowUnpacker::new();
                    let mut packer = RowPacker::new();
                    let collection = self.collection(input).unwrap().map(move |row| {
                        let datums = unpacker.unpack(&row);
                        packer.pack(outputs.iter().map(|i| datums[*i]))
                    });

                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Map { input, scalars } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let scalars = scalars.clone();
                    let mut unpacker = RowUnpacker::new();
                    let mut packer = RowPacker::new();
                    let mut temp_storage = RowPacker::new();
                    let collection = self.collection(input).unwrap().map(move |input_row| {
                        let mut datums = unpacker.unpack(&input_row);
                        let temp_storage = &mut temp_storage.packable();
                        for scalar in &scalars {
                            let datum = scalar.eval(temp_storage, &datums);
                            // Scalar is allowed to see the outputs of previous scalars.
                            // To avoid repeatedly unpacking input_row, we just push the outputs into datums so later scalars can see them.
                            // Note that this doesn't mutate input_row.
                            datums.push(datum);
                        }
                        packer.pack(&*datums)
                    });

                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Filter { input, predicates } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let predicates = predicates.clone();
                    let mut unpacker = RowUnpacker::new();
                    let mut temp_storage = RowPacker::new();
                    let collection = self.collection(input).unwrap().filter(move |input_row| {
                        let datums = unpacker.unpack(input_row);
                        predicates.iter().all(|predicate| {
                            let temp_storage = &mut temp_storage.packable();
                            match predicate.eval(temp_storage, &datums) {
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
                    self.render_join(relation_expr, scope, worker_index);
                }

                RelationExpr::Reduce { .. } => {
                    self.render_reduce(relation_expr, scope, worker_index);
                }

                RelationExpr::TopK { .. } => {
                    self.render_topk(relation_expr, scope, worker_index);
                }

                RelationExpr::Negate { input } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let collection = self.collection(input).unwrap().negate();
                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Threshold { .. } => {
                    self.render_threshold(relation_expr, scope, worker_index);
                }

                RelationExpr::Union { left, right } => {
                    self.ensure_rendered(left, scope, worker_index);
                    self.ensure_rendered(right, scope, worker_index);

                    let input1 = self.collection(left).unwrap();
                    let input2 = self.collection(right).unwrap();

                    self.collections
                        .insert(relation_expr.clone(), input1.concat(&input2));
                }

                RelationExpr::ArrangeBy { input, keys } => {
                    if self.arrangement(&input, &keys[..]).is_none() {
                        self.ensure_rendered(input, scope, worker_index);
                        let built = self.collection(input).unwrap();
                        let keys2 = keys.clone();
                        let mut unpacker = RowUnpacker::new();
                        let mut packer = RowPacker::new();
                        let keyed = built
                            .map(move |row| {
                                let datums = unpacker.unpack(&row);
                                let key_row = packer.pack(keys2.iter().map(|i| datums[*i]));
                                drop(datums);
                                (key_row, row)
                            })
                            .arrange_by_key();
                        self.set_local(&input, &keys[..], keyed);
                    }
                }
            };
        }
    }

    fn render_join(&mut self, relation_expr: &RelationExpr, scope: &mut G, worker_index: usize) {
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
                self.ensure_rendered(input, scope, worker_index);
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
                            if equivalence[0].0 < index {
                                old_outputs.push(
                                    columns.iter().position(|c2| equivalence[0] == *c2).unwrap(),
                                );
                            } else if equivalence[0].0 == index {
                                new_outputs.push(equivalence[0].1);
                            }
                            // If the relation exceeds the current index,
                            // we don't need to worry about retaining it
                            // at this moment.
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

                    let mut unpacker = RowUnpacker::new();
                    let mut packer = RowPacker::new();
                    let old_keyed = joined
                        .map(move |row| {
                            let datums = unpacker.unpack(&row);
                            let key_row = packer.pack(old_keys.iter().map(|i| datums[*i]));
                            drop(datums);
                            (key_row, row)
                        })
                        .arrange_named::<OrdValSpine<_, _, _, _>>(&format!("JoinStage: {}", index));

                    // TODO: easier idioms for detecting, re-using, and stashing.
                    if self.arrangement(&input, &new_keys[..]).is_none() {
                        let built = self.collection(input).unwrap();
                        let new_keys2 = new_keys.clone();
                        let mut unpacker = RowUnpacker::new();
                        let mut packer = RowPacker::new();
                        let new_keyed = built
                            .map(move |row| {
                                let datums = unpacker.unpack(&row);
                                let key_row = packer.pack(new_keys2.iter().map(|i| datums[*i]));
                                drop(datums);
                                (key_row, row)
                            })
                            .arrange_named::<OrdValSpine<_, _, _, _>>(&format!(
                                "JoinIndex: {}",
                                index
                            ));
                        self.set_local(&input, &new_keys[..], new_keyed);
                    }

                    let mut old_unpacker = RowUnpacker::new();
                    let mut new_unpacker = RowUnpacker::new();
                    let mut packer = RowPacker::new();
                    joined = match self.arrangement(&input, &new_keys[..]) {
                        Some(ArrangementFlavor::Local(local)) => {
                            old_keyed.join_core(&local, move |_keys, old, new| {
                                let old_datums = old_unpacker.unpack(old);
                                let new_datums = new_unpacker.unpack(new);
                                Some(
                                    packer.pack(
                                        old_outputs
                                            .iter()
                                            .map(|i| &old_datums[*i])
                                            .chain(new_outputs.iter().map(|i| &new_datums[*i])),
                                    ),
                                )
                            })
                        }
                        Some(ArrangementFlavor::Trace(trace)) => {
                            old_keyed.join_core(&trace, move |_keys, old, new| {
                                let old_datums = old_unpacker.unpack(old);
                                let new_datums = new_unpacker.unpack(new);
                                Some(
                                    packer.pack(
                                        old_outputs
                                            .iter()
                                            .map(|i| &old_datums[*i])
                                            .chain(new_outputs.iter().map(|i| &new_datums[*i])),
                                    ),
                                )
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
                let mut unpacker = RowUnpacker::new();
                let mut packer = RowPacker::new();
                joined =
                    joined.map(move |row| {
                        let datums = unpacker.unpack(&row);
                        packer.pack(outputs.iter().zip(dummy_data.iter()).map(
                            |(new_col, dummy)| {
                                if let Some(new_col) = new_col {
                                    datums[*new_col]
                                } else {
                                    // Regenerate any columns ignored during join with dummy data
                                    *dummy
                                }
                            },
                        ))
                    });
                self.collections.insert(relation_expr.clone(), joined);
            } else {
                panic!("Empty join; why?");
            }
        }
    }

    fn render_reduce(&mut self, relation_expr: &RelationExpr, scope: &mut G, worker_index: usize) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } = relation_expr
        {
            use differential_dataflow::operators::reduce::ReduceCore;
            use timely::dataflow::operators::map::Map;

            let keys_clone = group_key.clone();

            self.ensure_rendered(input, scope, worker_index);
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
                    let group_key = group_key.clone();
                    let mut unpacker = RowUnpacker::new();
                    let mut packer = RowPacker::new();
                    let mut temp_storage = RowPacker::new();
                    move |row| {
                        let datums = unpacker.unpack(&row);

                        let keys = packer.pack(group_key.iter().map(|i| datums[*i]));

                        let mut vals = packer.packable();
                        let mut aggs = vec![1i128];

                        for (index, aggregate) in aggregates_clone.iter().enumerate() {
                            // Presently, we can accumulate in the difference field only
                            // if the aggregation has a known type and does not require
                            // us to accumulate only distinct elements.
                            //
                            // To enable the optimization where distinctness is required,
                            // consider restructuring the plan to pre-distinct the right
                            // data and then use a non-distinctness-requiring aggregation.

                            let temp_storage = &mut temp_storage.packable();
                            let eval = aggregate.expr.eval(temp_storage, &datums);

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
                            let mut packer = RowPacker::new();
                            let mut temp_storage = RowPacker::new();
                            move |key, source, target| {
                                sums.clear();
                                sums.extend(&source[0].1[..]);
                                for record in source[1..].iter() {
                                    for index in 0..sums.len() {
                                        sums[index] += record.1[index];
                                    }
                                }

                                // Our output will be [keys; aggregates].
                                let mut result = packer.packable();
                                result.extend(key.iter());

                                let mut abelian_pos = 1; // <- advance past the count
                                let mut non_abelian_pos = 0;

                                for (agg, abl) in aggregates.iter().zip(abelian.iter()) {
                                    if *abl {
                                        let value = match agg.func {
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
                                            let temp_storage = &mut temp_storage.packable();
                                            result.push((agg.func.func())(temp_storage, iter));
                                        } else {
                                            let iter = source.iter().flat_map(|(v, w)| {
                                                // let eval = agg.expr.eval(v);
                                                std::iter::repeat(v.iter().nth(non_abelian_pos).unwrap())
                                                    .take(std::cmp::max(w[0], 0) as usize)
                                            });
                                            let temp_storage = &mut temp_storage.packable();
                                            result.push((agg.func.func())(temp_storage, iter));
                                        }
                                        non_abelian_pos += 1;
                                    }
                                }
                                target.push((result.finish(), 1isize));
                            }
                        }
                    );

            let index = (0..keys_clone.len()).collect::<Vec<_>>();
            self.set_local(relation_expr, &index[..], arrangement.clone());
        }
    }

    fn render_topk(&mut self, relation_expr: &RelationExpr, scope: &mut G, worker_index: usize) {
        if let RelationExpr::TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
        } = relation_expr
        {
            use differential_dataflow::operators::reduce::ReduceCore;

            let group_clone = group_key.clone();
            let order_clone = order_key.clone();

            self.ensure_rendered(input, scope, worker_index);
            let input = self.collection(input).unwrap();

            let limit = *limit;
            let offset = *offset;
            let arrangement = input
                .map({
                    let mut unpacker = RowUnpacker::new();
                    let mut packer = RowPacker::new();
                    move |row| {
                        let datums = unpacker.unpack(&row);
                        let group_row = packer.pack(group_clone.iter().map(|i| datums[*i]));
                        drop(datums);
                        (group_row, row)
                    }
                })
                .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("TopK", {
                    let mut left_unpacker = RowUnpacker::new();
                    let mut right_unpacker = RowUnpacker::new();
                    move |_key, source, target| {
                        target.extend(source.iter().map(|&(row, diff)| (row.clone(), diff)));
                        if !order_clone.is_empty() {
                            //todo: use arrangements or otherwise make the sort more performant?
                            let sort_by = |left: &(Row, isize), right: &(Row, isize)| {
                                compare_columns(
                                    &order_clone,
                                    &*left_unpacker.unpack(&left.0),
                                    &*right_unpacker.unpack(&right.0),
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
                                    let to_emit =
                                        std::cmp::min(limit - output, target[cursor].1 as usize);
                                    target[cursor].1 = to_emit as isize;
                                    output += to_emit;
                                    cursor += 1;
                                }
                                target.truncate(cursor);
                            }
                        }
                        target.drain(..skip_cursor);
                    }
                });

            let index = (0..group_key.len()).collect::<Vec<_>>();
            self.set_local(relation_expr, &index[..], arrangement.clone());
        }
    }

    fn render_threshold(
        &mut self,
        relation_expr: &RelationExpr,
        scope: &mut G,
        worker_index: usize,
    ) {
        if let RelationExpr::Threshold { input } = relation_expr {
            // TODO: re-use and publish arrangement here.
            let arity = input.arity();
            let keys = (0..arity).collect::<Vec<_>>();

            // TODO: easier idioms for detecting, re-using, and stashing.
            if self.arrangement(&input, &keys[..]).is_none() {
                self.ensure_rendered(input, scope, worker_index);
                let built = self.collection(input).unwrap();
                let keys2 = keys.clone();
                let mut unpacker = RowUnpacker::new();
                let mut packer = RowPacker::new();
                let keyed = built
                    .map(move |row| {
                        let datums = unpacker.unpack(&row);
                        let key_row = packer.pack(keys2.iter().map(|i| datums[*i]));
                        drop(datums);
                        (key_row, row)
                    })
                    .arrange_by_key();
                self.set_local(&input, &keys[..], keyed);
            }

            use differential_dataflow::operators::reduce::ReduceCore;

            let arranged = match self.arrangement(&input, &keys[..]) {
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
            self.set_local(relation_expr, &index[..], arranged.clone());
        }
    }
}
