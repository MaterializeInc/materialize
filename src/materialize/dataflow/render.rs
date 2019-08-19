// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::{AsCollection, Collection};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use timely::communication::Allocate;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::worker::Worker as TimelyWorker;

use super::sink;
use super::source;
use super::source::SharedCapability;
use super::types::*;
use crate::dataflow::arrangement::TraceManager;
use crate::dataflow::arrangement::{context::ArrangementFlavor, Context};
use crate::glue::LocalInputMux;
use expr::RelationExpr;
use repr::Datum;

pub enum InputCapability {
    External(SharedCapability),
}

pub fn build_dataflow<A: Allocate>(
    dataflow: &Dataflow,
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    inputs: &mut HashMap<String, InputCapability>,
    local_input_mux: &mut LocalInputMux,
) {
    let worker_timer = worker.timer();
    let worker_index = worker.index();
    let worker_peers = worker.peers();

    worker.dataflow::<Timestamp, _, _>(|scope| match dataflow {
        Dataflow::Source(src) => {
            let relation_expr = match &src.connector {
                SourceConnector::Kafka(c) => {
                    // Distribute read responsibility among workers.
                    use differential_dataflow::hashable::Hashable;
                    let hash = src.name.hashed() as usize;
                    let read_from_kafka = hash % worker_peers == worker_index;

                    let (stream, capability) = source::kafka(scope, &src.name, &c, read_from_kafka);
                    if let Some(capability) = capability {
                        inputs.insert(src.name.clone(), InputCapability::External(capability));
                    }
                    stream
                }
                SourceConnector::Local(c) => {
                    let (stream, capability) =
                        source::local(scope, &src.name, &c, worker_index == 0, local_input_mux);
                    if let Some(capability) = capability {
                        inputs.insert(src.name.clone(), InputCapability::External(capability));
                    }
                    stream
                }
            };

            use crate::dataflow::arrangement::manager::KeysOnlySpine;
            use differential_dataflow::operators::arrange::arrangement::Arrange;

            let arrangement = relation_expr
                .as_collection()
                // The following two lines implement `arrange_by_self` with a name.
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
            match &sink.connector {
                SinkConnector::Kafka(c) => {
                    sink::kafka(&arrangement.stream, &sink.name, c, done, worker_timer)
                }
            }
        }
        Dataflow::View(view) => {
            // The scope.clone() occurs to allow import in the region.
            // We build a region here to establish a pattern of a scope inside the dataflow,
            // so that other similar uses (e.g. with iterative scopes) do not require weird
            // alternate type signatures.
            scope.clone().region(|region| {
                let mut buttons = Vec::new();
                let mut context = Context::new();
                view.relation_expr.visit(&mut |e| {
                    if let RelationExpr::Get { name, typ: _ } = e {
                        // Import the believed-to-exist base arrangement.
                        if let Some(mut trace) = manager.get_by_self(&name).cloned() {
                            let (arranged, button) = trace.import_core(scope, name);
                            let arranged = arranged.enter(region);
                            context
                                .collections
                                .insert(e.clone(), arranged.as_collection(|k, _| k.clone()));
                            buttons.push(button);
                        }
                        // // Experimental: add all indexed collections.
                        // if let Some(traces) = manager.get_all_keyed(&name) {
                        //     for (key, trace) in traces {
                        //         let (arranged, button) = trace.import_core(scope, name);
                        //         let arranged = arranged.enter(region);
                        //         context.set_trace(e.clone(), key, arranged);
                        //         buttons.push(button);
                        //     }
                        // }
                    }
                });

                // Push predicates down a few times.
                let mut view = view.clone();

                let transforms: Vec<Box<dyn expr::transform::Transform>> = vec![
                    Box::new(expr::transform::reduction::FoldConstants),
                    Box::new(expr::transform::reduction::DeMorgans),
                    Box::new(expr::transform::reduction::UndistributeAnd),
                    Box::new(expr::transform::split_predicates::SplitPredicates),
                    Box::new(expr::transform::fusion::join::Join),
                    Box::new(expr::transform::predicate_pushdown::PredicatePushdown),
                    Box::new(expr::transform::fusion::filter::Filter),
                    Box::new(expr::transform::join_order::JoinOrder),
                    Box::new(expr::transform::empty_map::EmptyMap),
                    // Box::new(expr::transform::aggregation::FractureReduce),
                ];
                for transform in transforms.iter() {
                    transform.transform(&mut view.relation_expr, &view.typ);
                }

                use crate::dataflow::arrangement::manager::KeysOnlySpine;
                use differential_dataflow::operators::arrange::arrangement::Arrange;

                let arrangement = build_relation_expr(
                    view.relation_expr.clone(),
                    region,
                    &mut context,
                    worker_index,
                )
                // The following two lines implement `arrange_by_self` with a name.
                .map(|x| (x, ()))
                .arrange_named::<KeysOnlySpine>(&format!("Arrange: {}", view.name));
                manager.set_by_self(
                    view.name,
                    arrangement.trace,
                    Some(Box::new(move || {
                        for mut button in buttons.drain(..) {
                            button.press();
                        }
                    })),
                );
            });
        }
    })
}

pub fn build_relation_expr<G>(
    relation_expr: RelationExpr,
    scope: &mut G,
    context: &mut Context<G, RelationExpr, Datum, Timestamp>,
    worker_index: usize,
) -> Collection<G, Vec<Datum>, isize>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<Timestamp>,
    // S: BuildHasher + Clone,
{
    if context.collection(&relation_expr).is_none() {
        let collection = match relation_expr.clone() {
            RelationExpr::Constant { rows, .. } => {
                use timely::dataflow::operators::{Map, ToStream};
                let rows = if worker_index == 0 { rows } else { vec![] };
                rows.to_stream(scope)
                    .map(|x| (x, Default::default(), 1))
                    .as_collection()
            }
            RelationExpr::Get { name, typ: _ } => {
                // TODO: something more tasteful.
                // perhaps load an empty collection, warn?
                panic!("Collection {} not pre-loaded", name);
            }
            RelationExpr::Let { name, value, body } => {
                let typ = value.typ();
                let bind = RelationExpr::Get { name, typ };
                if context.collection(&bind).is_some() {
                    panic!("Inappropriate to re-bind name: {:?}", bind);
                } else {
                    let value = build_relation_expr(*value, scope, context, worker_index);
                    context.collections.insert(bind.clone(), value);
                    build_relation_expr(*body, scope, context, worker_index)
                }
            }
            RelationExpr::Project { input, outputs } => {
                let input = build_relation_expr(*input, scope, context, worker_index);
                input.map(move |tuple| outputs.iter().map(|i| tuple[*i].clone()).collect())
            }
            RelationExpr::Map { input, scalars } => {
                let input = build_relation_expr(*input, scope, context, worker_index);
                input.map(move |mut tuple| {
                    let len = tuple.len();
                    for s in scalars.iter() {
                        let to_push = s.0.eval(&tuple[..len]);
                        tuple.push(to_push);
                    }
                    tuple
                })
            }
            RelationExpr::Filter { input, predicates } => {
                let input = build_relation_expr(*input, scope, context, worker_index);
                input.filter(move |x| {
                    predicates.iter().all(|predicate| match predicate.eval(x) {
                        Datum::True => true,
                        Datum::False | Datum::Null => false,
                        _ => unreachable!(),
                    })
                })
            }
            RelationExpr::Join { inputs, variables } => {
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

                let arities = inputs.iter().map(|i| i.arity()).collect::<Vec<_>>();

                // The relation_expr is to implement join as a `fold` over `inputs`.
                let mut input_iter = inputs.into_iter().enumerate();
                if let Some((index, input)) = input_iter.next() {
                    let mut joined = build_relation_expr(input, scope, context, worker_index);

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
                        if context.arrangement(&input, &new_keys[..]).is_none() {
                            let built =
                                build_relation_expr(input.clone(), scope, context, worker_index);
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
                            context.set_local(input.clone(), &new_keys[..], new_keyed);
                        }

                        joined = match context.arrangement(&input, &new_keys[..]) {
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

                    joined
                } else {
                    panic!("Empty join; why?");
                }
            }
            RelationExpr::Reduce {
                input,
                group_key,
                aggregates,
            } => {
                use differential_dataflow::operators::reduce::ReduceCore;
                use differential_dataflow::trace::implementations::ord::OrdValSpine;
                use timely::dataflow::operators::map::Map;

                let self_clone = relation_expr.clone();
                let keys_clone = group_key.clone();
                let input = build_relation_expr(*input, scope, context, worker_index);

                use expr::AggregateFunc;

                // Reduce has the ability to lift any Abelian, non-distinct aggregations
                // into the diff field. We also need to maintain the count as well, as we
                // need to distinguish "things that accumulate to zero" from "the absence
                // of things". It also gives us a quick and easy story about `Avg`.

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
                        AggregateFunc::AvgInt32 => !aggregate.distinct,
                        AggregateFunc::AvgInt64 => !aggregate.distinct,
                        AggregateFunc::AvgFloat32 => !aggregate.distinct,
                        AggregateFunc::AvgFloat64 => !aggregate.distinct,
                        AggregateFunc::AvgDecimal => !aggregate.distinct,
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
                    .map(move |tuple| {
                        let keys = group_key
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect::<Vec<_>>();

                        let mut vals = Vec::new();
                        let mut aggs = vec![1i128];

                        for (index, (aggregate, _column_type)) in
                            aggregates_clone.iter().enumerate()
                        {
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
                let arrangement = exploded.reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
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
                                    AggregateFunc::AvgInt32 => {
                                        let total = sums[abelian_pos] as i32;
                                        let non_nulls = sums[abelian_pos + 1] as i32;
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            Datum::Int32(total / non_nulls)
                                        } else {
                                            Datum::Null
                                        }
                                    }
                                    AggregateFunc::AvgInt64 => {
                                        let total = sums[abelian_pos] as i64;
                                        let non_nulls = sums[abelian_pos + 1] as i64;
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            Datum::Int64(total / non_nulls)
                                        } else {
                                            Datum::Null
                                        }
                                    }
                                    AggregateFunc::AvgFloat32 => {
                                        let total = sums[abelian_pos];
                                        let non_nulls = sums[abelian_pos + 1];
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            Datum::Float32(
                                                ((((total as f64) / float_scale)
                                                    / (non_nulls as f64))
                                                    as f32)
                                                    .into(),
                                            )
                                        } else {
                                            Datum::Null
                                        }
                                    }
                                    AggregateFunc::AvgFloat64 => {
                                        let total = sums[abelian_pos];
                                        let non_nulls = sums[abelian_pos + 1];
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            Datum::Float64(
                                                (((total as f64) / float_scale)
                                                    / (non_nulls as f64))
                                                    .into(),
                                            )
                                        } else {
                                            Datum::Null
                                        }
                                    }
                                    AggregateFunc::AvgDecimal => {
                                        let total = sums[abelian_pos];
                                        let non_nulls = sums[abelian_pos + 1];
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            // TODO(benesch): This should use the same decimal
                                            // division path as the planner, rather than
                                            // hardcoding a 6 digit increase in the scale (#212).
                                            Datum::from(1_000_000 * total / non_nulls)
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

                context.set_local(self_clone, &keys_clone[..], arrangement.clone());
                arrangement.as_collection(|_key, tuple| tuple.clone())
            }

            RelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
            } => {
                use differential_dataflow::operators::reduce::ReduceCore;
                use differential_dataflow::trace::implementations::ord::OrdValSpine;

                let self_clone = relation_expr.clone();
                let group_clone = group_key.clone();
                let order_clone = order_key.clone();
                let input = build_relation_expr(*input, scope, context, worker_index);

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
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(move |_key, source, target| {
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
                    });

                context.set_local(self_clone, &group_key[..], arrangement.clone());
                arrangement.as_collection(|_key, tuple| tuple.clone())
            }

            RelationExpr::OrDefault { input, default } => {
                use differential_dataflow::operators::reduce::Threshold;
                use differential_dataflow::operators::Join;
                use timely::dataflow::operators::to_stream::ToStream;

                let input = build_relation_expr(*input, scope, context, worker_index);
                let present = input.map(|_| ()).distinct();
                let value = if worker_index == 0 {
                    vec![(((), default), Default::default(), 1isize)]
                } else {
                    vec![]
                };
                let default = value
                    .to_stream(scope)
                    .as_collection()
                    .antijoin(&present)
                    .map(|((), default)| default);

                input.concat(&default)
            }
            RelationExpr::Negate { input } => {
                let input = build_relation_expr(*input, scope, context, worker_index);
                input.negate()
            }
            RelationExpr::Distinct { input } => {
                // TODO: re-use and publish arrangement here.
                let arity = input.arity();
                let keys = (0..arity).collect::<Vec<_>>();

                // TODO: easier idioms for detecting, re-using, and stashing.
                if context.arrangement(&input, &keys[..]).is_none() {
                    let built = build_relation_expr((*input).clone(), scope, context, worker_index);
                    let keys2 = keys.clone();
                    let keyed = built
                        .map(move |tuple| {
                            (
                                keys2.iter().map(|i| tuple[*i].clone()).collect::<Vec<_>>(),
                                tuple,
                            )
                        })
                        .arrange_by_key();
                    context.set_local((*input).clone(), &keys[..], keyed);
                }

                use differential_dataflow::operators::reduce::ReduceCore;
                use differential_dataflow::trace::implementations::ord::OrdValSpine;

                let arranged = match context.arrangement(&input, &keys[..]) {
                    Some(ArrangementFlavor::Local(local)) => local
                        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(move |k, _s, t| {
                            t.push((k.to_vec(), 1))
                        }),
                    Some(ArrangementFlavor::Trace(trace)) => trace
                        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(move |k, _s, t| {
                            t.push((k.to_vec(), 1))
                        }),
                    None => {
                        panic!("Arrangement alarmingly absent!");
                    }
                };

                context.set_local(*input, &keys[..], arranged.clone());
                arranged.as_collection(|_k, v| v.clone())
            }
            RelationExpr::Threshold { input } => {
                // TODO: re-use and publish arrangement here.
                let arity = input.arity();
                let keys = (0..arity).collect::<Vec<_>>();

                // TODO: easier idioms for detecting, re-using, and stashing.
                if context.arrangement(&input, &keys[..]).is_none() {
                    let built = build_relation_expr((*input).clone(), scope, context, worker_index);
                    let keys2 = keys.clone();
                    let keyed = built
                        .map(move |tuple| {
                            (
                                keys2.iter().map(|i| tuple[*i].clone()).collect::<Vec<_>>(),
                                tuple,
                            )
                        })
                        .arrange_by_key();
                    context.set_local((*input).clone(), &keys[..], keyed);
                }

                use differential_dataflow::operators::reduce::ReduceCore;
                use differential_dataflow::trace::implementations::ord::OrdValSpine;

                let arranged = match context.arrangement(&input, &keys[..]) {
                    Some(ArrangementFlavor::Local(local)) => local
                        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(move |_k, s, t| {
                            for (record, count) in s.iter() {
                                if *count > 0 {
                                    t.push(((*record).clone(), *count));
                                }
                            }
                        }),
                    Some(ArrangementFlavor::Trace(trace)) => trace
                        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(move |_k, s, t| {
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

                context.set_local(*input, &keys[..], arranged.clone());
                arranged.as_collection(|_k, v| v.clone())
            }
            RelationExpr::Union { left, right } => {
                let input1 = build_relation_expr(*left, scope, context, worker_index);
                let input2 = build_relation_expr(*right, scope, context, worker_index);
                input1.concat(&input2)
            }
        };

        context
            .collections
            .insert(relation_expr.clone(), collection);
    }

    context
        .collection(&relation_expr)
        .expect("Collection surprisingly absent")
        .clone()
}
