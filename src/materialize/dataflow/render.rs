// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::{AsCollection, Collection};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use timely::communication::Allocate;
use timely::dataflow::operators::input::Input as TimelyInput;
use timely::dataflow::InputHandle;
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::worker::Worker as TimelyWorker;

use super::sink;
use super::source;
use super::trace::TraceManager;
use super::types::*;
use crate::dataflow::context::{ArrangementFlavor, Context};
use crate::dataflow::types::RelationExpr;
use crate::repr::{ColumnType, Datum, RelationType, ScalarType};

pub fn add_builtin_dataflows<A: Allocate>(
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
) {
    let dual_table_data = if worker.index() == 0 {
        vec![vec![Datum::String("X".into())]]
    } else {
        vec![]
    };
    worker.dataflow(|scope| {
        let (_, collection) = scope.new_collection_from(dual_table_data);
        let arrangement = collection.arrange_by_self();
        let on_delete = Box::new(|| ());
        manager.set_trace(
            &RelationExpr::Get {
                name: "dual".into(),
                typ: RelationType {
                    column_types: vec![ColumnType {
                        name: Some("x".into()),
                        nullable: false,
                        scalar_type: ScalarType::String,
                    }],
                },
            },
            arrangement.trace,
            on_delete,
        );
    })
}

pub fn build_dataflow<A: Allocate>(
    dataflow: &Dataflow,
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    // insert_mux: &source::InsertMux,
    inputs: &mut HashMap<String, InputHandle<Timestamp, (Vec<Datum>, Timestamp, isize)>>,
) {
    let worker_timer = worker.timer();
    let worker_index = worker.index();

    worker.dataflow::<Timestamp, _, _>(|scope| match dataflow {
        Dataflow::Source(src) => {
            let done = Rc::new(Cell::new(false));
            let relation_expr = match &src.connector {
                SourceConnector::Kafka(c) => {
                    source::kafka(scope, &src.name, &c, done.clone(), worker_timer)
                }
                SourceConnector::Local(_) => {
                    let (handle, stream) = scope.new_input();
                    if worker_index == 0 {
                        // Only insert if we're worker 0, to avoid duplicating
                        // the insert.
                        inputs.insert(src.name.clone(), handle);
                    }
                    stream
                }
            };
            let arrangement = relation_expr.as_collection().arrange_by_self();
            let on_delete = Box::new(move || done.set(true));
            manager.set_trace(
                &RelationExpr::Get {
                    name: src.name.clone(),
                    typ: src.typ.clone(),
                },
                arrangement.trace,
                on_delete,
            );
        }
        Dataflow::Sink(sink) => {
            let done = Rc::new(Cell::new(false));
            let (arrangement, _) = manager
                .get_trace(&RelationExpr::Get {
                    name: sink.from.0.clone(),
                    typ: sink.from.1.clone(),
                })
                .unwrap_or_else(|| panic!(format!("unable to find dataflow {:?}", sink.from)))
                .import_core(scope, &format!("Import({:?})", sink.from));
            match &sink.connector {
                SinkConnector::Kafka(c) => {
                    sink::kafka(&arrangement.stream, &sink.name, c, done, worker_timer)
                }
            }
        }
        Dataflow::View(view) => {
            let mut buttons = Vec::new();
            let mut context = Context::new();
            view.relation_expr.visit(|e| {
                if let RelationExpr::Get { name, typ: _ } = e {
                    if let Some(mut trace) = manager.get_trace(e) {
                        // TODO(frankmcsherry) do the thing
                        let (arranged, button) = trace.import_core(scope, name);
                        context
                            .collections
                            .insert(e.clone(), arranged.as_collection(|k, _| k.clone()));
                        buttons.push(button);
                    }
                }
            });

            // Push predicates down a few times.
            let mut view = view.clone();

            use crate::dataflow::transform;
            let transforms: Vec<Box<dyn transform::Transform>> = vec![
                Box::new(transform::fusion::join::Join),
                Box::new(transform::PredicatePushdown),
                Box::new(transform::fusion::filter::Filter),
                Box::new(transform::join_order::JoinOrder),
                Box::new(transform::reduction::FoldConstants),
                // Box::new(transform::aggregation::FractureReduce),
            ];
            for transform in transforms.iter() {
                transform.transform(&mut view.relation_expr, &view.typ);
            }

            let arrangement = build_relation_expr(
                view.relation_expr.clone(),
                scope,
                &mut context,
                worker_index,
            )
            .arrange_by_self();
            manager.set_trace(
                &RelationExpr::Get {
                    name: view.name.clone(),
                    typ: view.typ.clone(),
                },
                arrangement.trace,
                Box::new(move || {
                    for mut button in buttons.drain(..) {
                        button.press();
                    }
                }),
            );
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
                use differential_dataflow::collection::AsCollection;
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

                let self_clone = relation_expr.clone();
                let keys_clone = group_key.clone();
                let input = build_relation_expr(*input, scope, context, worker_index);

                use crate::dataflow::func::AggregateFunc;

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
                        AggregateFunc::AvgInt32 => !aggregate.distinct,
                        AggregateFunc::AvgInt64 => !aggregate.distinct,
                        AggregateFunc::Count => !aggregate.distinct,
                        AggregateFunc::CountAll => !aggregate.distinct,
                        _ => false,
                    };

                    abelian.push(accumulable);
                }

                let abelian2 = abelian.clone();
                let aggregates_clone = aggregates.clone();

                // Our first action is to take our input from a collection of `tuple`
                // to one structured as `((keys, vals), time, aggs)`
                let exploded = input
                    .map(move |tuple| {
                        let keys = group_key
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect::<Vec<_>>();

                        let mut vals = Vec::new();
                        let mut aggs = vec![1isize];

                        for (index, (aggregate, _type)) in aggregates_clone.iter().enumerate() {
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
                                            Datum::Int32(i) => (i as isize, 1),
                                            Datum::Int64(i) => (i as isize, 1),
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
                    .explode(|(keys, vals, aggs)| Some(((keys, vals), aggs)));

                // We now reduce by `keys`, performing both Abelian and non-Abelian aggregations.
                let arrangement = exploded.reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                    move |key, source, target| {
                        // Our output will be [keys; aggregates].
                        let mut result = Vec::with_capacity(key.len() + aggregates.len());
                        result.extend(key.iter().cloned());

                        let mut abelian_pos = 1; // <- advance past the count
                        let mut non_abelian_pos = 0;

                        for ((agg, _typ), abl) in aggregates.iter().zip(abelian.iter()) {
                            if *abl {
                                let value = match agg.func {
                                    AggregateFunc::SumInt32 => {
                                        let total =
                                            source.iter().map(|(_, w)| w[abelian_pos] as i32).sum();
                                        let non_nulls: isize =
                                            source.iter().map(|(_, w)| w[abelian_pos + 1]).sum();
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            Datum::Int32(total)
                                        } else {
                                            Datum::Null
                                        }
                                    }
                                    AggregateFunc::SumInt64 => {
                                        let total =
                                            source.iter().map(|(_, w)| w[abelian_pos] as i64).sum();
                                        let non_nulls: isize =
                                            source.iter().map(|(_, w)| w[abelian_pos + 1]).sum();
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            Datum::Int64(total)
                                        } else {
                                            Datum::Null
                                        }
                                    }
                                    AggregateFunc::AvgInt32 => {
                                        let total: i32 =
                                            source.iter().map(|(_, w)| w[abelian_pos] as i32).sum();
                                        let non_nulls: i32 = source
                                            .iter()
                                            .map(|(_, w)| w[abelian_pos + 1] as i32)
                                            .sum();
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            Datum::Int32(total / non_nulls)
                                        } else {
                                            Datum::Null
                                        }
                                    }
                                    AggregateFunc::AvgInt64 => {
                                        let total: i64 =
                                            source.iter().map(|(_, w)| w[abelian_pos] as i64).sum();
                                        let non_nulls: i64 = source
                                            .iter()
                                            .map(|(_, w)| w[abelian_pos + 1] as i64)
                                            .sum();
                                        abelian_pos += 2;
                                        if non_nulls > 0 {
                                            Datum::Int64(total / non_nulls)
                                        } else {
                                            Datum::Null
                                        }
                                    }
                                    AggregateFunc::Count => {
                                        // Does not count NULLs.
                                        let total =
                                            source.iter().map(|(_, w)| w[abelian_pos] as i64).sum();
                                        abelian_pos += 1;
                                        Datum::Int64(total)
                                    }
                                    AggregateFunc::CountAll => {
                                        let total = source.iter().map(|(_, w)| w[0] as i64).sum();
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
                        target.push((result, 1));
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
                use differential_dataflow::collection::AsCollection;
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

impl ScalarExpr {
    pub fn eval(&self, data: &[Datum]) -> Datum {
        match self {
            ScalarExpr::Column(index) => data[*index].clone(),
            ScalarExpr::Literal(datum) => datum.clone(),
            ScalarExpr::CallUnary { func, expr } => {
                let eval = expr.eval(data);
                (func.func())(eval)
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                let eval1 = expr1.eval(data);
                let eval2 = expr2.eval(data);
                (func.func())(eval1, eval2)
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                let evals = exprs.iter().map(|e| e.eval(data)).collect();
                (func.func())(evals)
            }
            ScalarExpr::If { cond, then, els } => match cond.eval(data) {
                Datum::True => then.eval(data),
                Datum::False | Datum::Null => els.eval(data),
                d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
            },
        }
    }
}
