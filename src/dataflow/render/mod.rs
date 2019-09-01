// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::AsCollection;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use timely::communication::Allocate;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::worker::Worker as TimelyWorker;
use uuid::Uuid;

use dataflow_types::*;
use expr::RelationExpr;
use ore::mpmc::Mux;
use repr::Datum;

use super::sink;
use super::source;
use super::source::SharedCapability;
use crate::arrangement::TraceManager;
use crate::exfiltrate::Exfiltrator;

mod context;
use context::{ArrangementFlavor, Context};

pub enum InputCapability {
    External(SharedCapability),
}

pub fn build_dataflow<A: Allocate>(
    dataflow: Dataflow,
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    inputs: &mut HashMap<String, InputCapability>,
    local_input_mux: &mut Mux<Uuid, LocalInput>,
    exfiltrator: Rc<Exfiltrator>,
) {
    let worker_timer = worker.timer();
    let worker_index = worker.index();
    let worker_peers = worker.peers();

    worker.dataflow::<Timestamp, _, _>(|scope| match dataflow {
        Dataflow::Source(src) => {
            let relation_expr = match src.connector {
                SourceConnector::Kafka(c) => {
                    // Distribute read responsibility among workers.
                    use differential_dataflow::hashable::Hashable;
                    let hash = src.name.hashed() as usize;
                    let read_from_kafka = hash % worker_peers == worker_index;

                    let (stream, capability) = source::kafka(scope, &src.name, c, read_from_kafka);
                    if let Some(capability) = capability {
                        inputs.insert(src.name.clone(), InputCapability::External(capability));
                    }
                    stream
                }
                SourceConnector::Local(c) => {
                    let (stream, capability) =
                        source::local(scope, &src.name, c, worker_index == 0, local_input_mux);
                    if let Some(capability) = capability {
                        inputs.insert(src.name.clone(), InputCapability::External(capability));
                    }
                    stream
                }
            };

            use crate::arrangement::manager::KeysOnlySpine;
            use differential_dataflow::operators::arrange::arrangement::Arrange;

            let arrangement = relation_expr
                .as_collection()
                // TODO: We might choose to arrange by a more effective key.
                .map(|x| (x, ()))
                .arrange_named::<KeysOnlySpine>(&format!("Arrange: {}", src.name));

            manager.set_by_self(src.name.to_owned(), arrangement.trace, None);
        }
        Dataflow::Sink(sink) => {
            let done = Rc::new(Cell::new(false));
            let (arrangement, _) = manager
                .get_by_self(&sink.from.0)
                .cloned()
                .unwrap_or_else(|| panic!(format!("unable to find dataflow {:?}", sink.from)))
                .import_core(scope, &format!("Import({:?})", sink.from));
            match sink.connector {
                SinkConnector::Kafka(c) => {
                    sink::kafka(&arrangement.stream, &sink.name, c, done, worker_timer)
                }
                SinkConnector::Tail(c) => {
                    sink::tail(&arrangement.stream, &sink.name, c, exfiltrator)
                }
            }
        }
        Dataflow::View(view) => {
            let as_of = view
                .as_of
                .as_ref()
                .map(|x| x.to_vec())
                .unwrap_or_else(|| vec![0]);

            // The scope.clone() occurs to allow import in the region.
            // We build a region here to establish a pattern of a scope inside the dataflow,
            // so that other similar uses (e.g. with iterative scopes) do not require weird
            // alternate type signatures.
            scope.clone().region(|region| {
                let mut buttons = Vec::new();
                let mut context = Context::<_, _, _, Timestamp>::new();
                view.relation_expr.visit(&mut |e| {
                    if let RelationExpr::Get { name, typ: _ } = e {
                        // Import the believed-to-exist base arrangement.
                        if let Some(mut trace) = manager.get_by_self(&name).cloned() {
                            let (arranged, button) =
                                trace.import_frontier_core(scope, name, as_of.clone());
                            let arranged = arranged.enter(region);
                            context
                                .collections
                                .insert(e.clone(), arranged.as_collection(|k, _| k.clone()));

                            buttons.push(button.press_on_drop());
                        }
                        // // Experimental: add all indexed collections.
                        // // TODO: view could name arrangements of value for each get.
                        // if let Some(traces) = manager.get_all_keyed(&name) {
                        //     for (key, trace) in traces {
                        //         let (arranged, button) =
                        //             trace.import_frontier_core(scope, name, as_of.clone());
                        //         let arranged = arranged.enter(region);
                        //         context.set_trace(e, key, arranged);
                        //         buttons.push(button);
                        //     }
                        // }
                    }
                });

                use crate::arrangement::manager::KeysOnlySpine;
                use differential_dataflow::operators::arrange::arrangement::Arrange;

                context.ensure_rendered(&view.relation_expr, region, worker_index);

                // TODO: We could extract any arrangement here, but choose to arrange by self.
                let arrangement = context
                    .collection(&view.relation_expr)
                    .unwrap()
                    .map(|x| (x, ()))
                    .arrange_named::<KeysOnlySpine>(&format!("Arrange: {}", view.name));

                manager.set_by_self(view.name, arrangement.trace, Some(Box::new(buttons)));

                // TODO: We could export a variety of arrangements if we were instructed
                // to do so. We don't have a language for that at the moment.
            });
        }
    })
}

impl<G, T> Context<G, RelationExpr, Datum, T>
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
                        .map(|x| (x, Default::default(), 1))
                        .as_collection();

                    self.collections.insert(relation_expr.clone(), collection);
                }

                // A get should have been loaded into the context, and it is surprising to
                // reach this point given the `has_collection()` guard at the top of the method.
                RelationExpr::Get { name, typ: _ } => {
                    // TODO: something more tasteful.
                    // perhaps load an empty collection, warn?
                    panic!("Collection {} not pre-loaded", name);
                }

                RelationExpr::Let { name, value, body } => {
                    let typ = value.typ();
                    let bind = RelationExpr::Get {
                        name: name.to_string(),
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
                    let collection = self
                        .collection(input)
                        .unwrap()
                        .map(move |tuple| outputs.iter().map(|i| tuple[*i].clone()).collect());

                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Map { input, scalars } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let scalars = scalars.clone();
                    let collection = self.collection(input).unwrap().map(move |mut tuple| {
                        let len = tuple.len();
                        for s in scalars.iter() {
                            let to_push = s.0.eval(&tuple[..len]);
                            tuple.push(to_push);
                        }
                        tuple
                    });

                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Filter { input, predicates } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let predicates = predicates.clone();
                    let collection = self.collection(input).unwrap().filter(move |x| {
                        predicates.iter().all(|predicate| match predicate.eval(x) {
                            Datum::True => true,
                            Datum::False | Datum::Null => false,
                            _ => unreachable!(),
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

                RelationExpr::OrDefault { input, default } => {
                    use differential_dataflow::operators::reduce::Threshold;
                    use differential_dataflow::operators::Join;
                    use timely::dataflow::operators::to_stream::ToStream;

                    self.ensure_rendered(input, scope, worker_index);
                    let input = self.collection(input).unwrap();

                    let present = input.map(|_| ()).distinct();
                    let value = if worker_index == 0 {
                        vec![(((), default.clone()), Default::default(), 1isize)]
                    } else {
                        vec![]
                    };
                    let default = value
                        .to_stream(scope)
                        .as_collection()
                        .antijoin(&present)
                        .map(|((), default)| default);

                    self.collections
                        .insert(relation_expr.clone(), input.concat(&default));
                }

                RelationExpr::Negate { input } => {
                    self.ensure_rendered(input, scope, worker_index);
                    let collection = self.collection(input).unwrap().negate();
                    self.collections.insert(relation_expr.clone(), collection);
                }

                RelationExpr::Distinct { .. } => {
                    self.render_distinct(relation_expr, scope, worker_index);
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
            };
        }
    }

    fn render_join(&mut self, relation_expr: &RelationExpr, scope: &mut G, worker_index: usize) {
        if let RelationExpr::Join { inputs, variables } = relation_expr {
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

            for input in inputs.iter() {
                self.ensure_rendered(input, scope, worker_index);
            }

            let arities = inputs.iter().map(|i| i.arity()).collect::<Vec<_>>();

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

                    for sets in variables.iter() {
                        let new_pos = sets
                            .iter()
                            .filter(|(i, _)| i == &index)
                            .map(|(_, c)| *c)
                            .next();
                        let old_pos = columns.iter().position(|i| sets.contains(i));

                        // If we have both a new and an old column in the constraint ...
                        if let (Some(new_pos), Some(old_pos)) = (new_pos, old_pos) {
                            old_keys.push(old_pos);
                            new_keys.push(new_pos);
                        }
                    }

                    let old_keyed = joined
                        .map(move |tuple| {
                            (
                                old_keys
                                    .iter()
                                    .map(|i| tuple[*i].clone())
                                    .collect::<Vec<_>>(),
                                tuple,
                            )
                        })
                        .arrange_by_key();

                    // TODO: easier idioms for detecting, re-using, and stashing.
                    if self.arrangement(&input, &new_keys[..]).is_none() {
                        let built = self.collection(input).unwrap();
                        let new_keys2 = new_keys.clone();
                        let new_keyed = built
                            .map(move |tuple| {
                                (
                                    new_keys2
                                        .iter()
                                        .map(|i| tuple[*i].clone())
                                        .collect::<Vec<_>>(),
                                    tuple,
                                )
                            })
                            .arrange_by_key();
                        self.set_local(&input, &new_keys[..], new_keyed);
                    }

                    joined = match self.arrangement(&input, &new_keys[..]) {
                        Some(ArrangementFlavor::Local(local)) => {
                            old_keyed.join_core(&local, |_keys, old, new| {
                                Some(old.iter().chain(new).cloned().collect::<Vec<_>>())
                            })
                        }
                        Some(ArrangementFlavor::Trace(trace)) => {
                            old_keyed.join_core(&trace, |_keys, old, new| {
                                Some(old.iter().chain(new).cloned().collect::<Vec<_>>())
                            })
                        }
                        None => {
                            panic!("Arrangement alarmingly absent!");
                        }
                    };

                    columns.extend((0..arities[index]).map(|c| (index, c)));
                }

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
            use differential_dataflow::trace::implementations::ord::OrdValSpine;
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
            for (aggregate, _type) in aggregates.iter() {
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
            let group_key = group_key.clone();
            let exploded = input
                .map(move |tuple| {
                    let keys = group_key
                        .iter()
                        .map(|i| tuple[*i].clone())
                        .collect::<Vec<_>>();

                    let mut vals = Vec::new();
                    let mut aggs = vec![1i128];

                    for (index, (aggregate, _column_type)) in aggregates_clone.iter().enumerate() {
                        // Presently, we can accumulate in the difference field only
                        // if the aggregation has a known type and does not require
                        // us to accumulate only distinct elements.
                        //
                        // To enable the optimization where distinctness is required,
                        // consider restructuring the plan to pre-distinct the right
                        // data and then use a non-distinctness-requiring aggregation.

                        let eval = aggregate.expr.eval(&tuple[..]);

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
                                        Datum::Decimal(d) => (d.into_i128(), 1),
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
                        vals,
                        differential_dataflow::difference::DiffVector::new(aggs),
                    )
                })
                .inner
                .map(|(data, time, diff)| (data, time, diff as i128))
                .as_collection()
                .explode(|(keys, vals, aggs)| Some(((keys, vals), aggs)));

            let mut sums = Vec::<i128>::new();

            // We now reduce by `keys`, performing both Abelian and non-Abelian aggregations.
            let aggregates = aggregates.clone();
            let arrangement = exploded.reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                "Reduce",
                move |key, source, target| {
                    sums.clear();
                    sums.extend(&source[0].1[..]);
                    for record in source[1..].iter() {
                        for index in 0..sums.len() {
                            sums[index] += record.1[index];
                        }
                    }

                    // Our output will be [keys; aggregates].
                    let mut result = Vec::with_capacity(key.len() + aggregates.len());
                    result.extend(key.iter().cloned());

                    let mut abelian_pos = 1; // <- advance past the count
                    let mut non_abelian_pos = 0;

                    for ((agg, _column_type), abl) in aggregates.iter().zip(abelian.iter()) {
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
                                        Datum::Float64(((total as f64) / float_scale).into())
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
                                            Some(v[non_abelian_pos].clone())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect::<HashSet<_>>();
                                result.push((agg.func.func())(iter));
                            } else {
                                let iter = source.iter().flat_map(|(v, w)| {
                                    // let eval = agg.expr.eval(v);
                                    std::iter::repeat(v[non_abelian_pos].clone())
                                        .take(std::cmp::max(w[0], 0) as usize)
                                });
                                result.push((agg.func.func())(iter));
                            }
                            non_abelian_pos += 1;
                        }
                    }
                    target.push((result, 1isize));
                },
            );

            self.set_local(relation_expr, &keys_clone[..], arrangement.clone());
        }
    }

    fn render_topk(&mut self, relation_expr: &RelationExpr, scope: &mut G, worker_index: usize) {
        if let RelationExpr::TopK {
            input,
            group_key,
            order_key,
            limit,
        } = relation_expr
        {
            use differential_dataflow::operators::reduce::ReduceCore;
            use differential_dataflow::trace::implementations::ord::OrdValSpine;

            let group_clone = group_key.clone();
            let order_clone = order_key.clone();

            self.ensure_rendered(input, scope, worker_index);
            let input = self.collection(input).unwrap();

            let limit = *limit;
            let arrangement = input
                .map(move |tuple| {
                    (
                        group_clone
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect::<Vec<_>>(),
                        (
                            order_clone
                                .iter()
                                .map(|i| tuple[*i].clone())
                                .collect::<Vec<_>>(),
                            tuple,
                        ),
                    )
                })
                .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                    "TopK",
                    move |_key, source, target| {
                        let mut output = 0;
                        let mut cursor = 0;
                        while output < limit {
                            if cursor < source.len() {
                                let current = &(source[cursor].0).0;
                                while cursor < source.len() && &(source[cursor].0).0 == current {
                                    if source[0].1 > 0 {
                                        target.push(((source[0].0).1.clone(), source[0].1));
                                        output += source[0].1 as usize;
                                    }
                                }
                                cursor += 1;
                            }
                        }
                    },
                );

            self.set_local(&relation_expr, &group_key[..], arrangement.clone());
        }
    }

    fn render_distinct(
        &mut self,
        relation_expr: &RelationExpr,
        scope: &mut G,
        worker_index: usize,
    ) {
        if let RelationExpr::Distinct { input } = relation_expr {
            // TODO: re-use and publish arrangement here.
            let arity = input.arity();
            let keys = (0..arity).collect::<Vec<_>>();

            // TODO: easier idioms for detecting, re-using, and stashing.
            if self.arrangement(&input, &keys[..]).is_none() {
                self.ensure_rendered(input, scope, worker_index);
                let built = self.collection(input).unwrap();
                let keys2 = keys.clone();
                let keyed = built
                    .map(move |tuple| {
                        (
                            keys2.iter().map(|i| tuple[*i].clone()).collect::<Vec<_>>(),
                            tuple,
                        )
                    })
                    .arrange_by_key();
                self.set_local(&input, &keys[..], keyed);
            }

            use differential_dataflow::operators::reduce::ReduceCore;
            use differential_dataflow::trace::implementations::ord::OrdValSpine;

            let arranged = match self.arrangement(&input, &keys[..]) {
                Some(ArrangementFlavor::Local(local)) => local
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("Distinct", move |k, _s, t| {
                        t.push((k.to_vec(), 1))
                    }),
                Some(ArrangementFlavor::Trace(trace)) => trace
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("Distinct", move |k, _s, t| {
                        t.push((k.to_vec(), 1))
                    }),
                None => {
                    panic!("Arrangement alarmingly absent!");
                }
            };

            self.set_local(relation_expr, &keys[..], arranged.clone());
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
                let keyed = built
                    .map(move |tuple| {
                        (
                            keys2.iter().map(|i| tuple[*i].clone()).collect::<Vec<_>>(),
                            tuple,
                        )
                    })
                    .arrange_by_key();
                self.set_local(&input, &keys[..], keyed);
            }

            use differential_dataflow::operators::reduce::ReduceCore;
            use differential_dataflow::trace::implementations::ord::OrdValSpine;

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

            self.set_local(relation_expr, &keys[..], arranged.clone());
        }
    }
}
