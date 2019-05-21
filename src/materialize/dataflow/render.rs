// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::{ArrangeBySelf, ShutdownButton};
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::operators::threshold::ThresholdTotal;
use differential_dataflow::{AsCollection, Collection};
use std::cell::Cell;
use std::iter;
use std::rc::Rc;
use timely::communication::Allocate;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::Scope;
use timely::worker::Worker as TimelyWorker;

use super::source;
use super::trace::TraceManager;
use super::types::*;
use crate::clock::{Clock, Timestamp};
use crate::repr::Datum;

pub fn add_builtin_dataflows<A: Allocate>(
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
) {
    let dual_table_data = if worker.index() == 0 {
        vec![Datum::String("X".into())]
    } else {
        vec![]
    };
    worker.dataflow(|scope| {
        let (_, collection) = scope.new_collection_from(dual_table_data);
        let arrangement = collection.arrange_by_self();
        let on_delete = Box::new(|| ());
        manager.set_trace(&Plan::Source("dual".into()), arrangement.trace, on_delete);
    })
}

pub fn build_dataflow<A: Allocate>(
    dataflow: &Dataflow,
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    clock: &Clock,
    insert_mux: &source::InsertMux,
) {
    let worker_index = worker.index();
    worker.dataflow::<Timestamp, _, _>(|scope| match dataflow {
        Dataflow::Source(src) => {
            let done = Rc::new(Cell::new(false));
            let plan = match &src.connector {
                Connector::Kafka(c) => source::kafka(scope, &src.name, &c, done.clone(), clock),
                Connector::Local(l) => {
                    source::local(scope, &src.name, &l, done.clone(), clock, insert_mux)
                }
            };
            let arrangement = plan.as_collection().arrange_by_self();
            let on_delete = Box::new(move || done.set(true));
            manager.set_trace(
                &Plan::Source(src.name.clone()),
                arrangement.trace,
                on_delete,
            );
        }
        Dataflow::View(view) => {
            let mut buttons = Vec::new();
            let arrangement = build_plan(&view.plan, manager, worker_index, scope, &mut buttons)
                .arrange_by_self();
            let on_delete = Box::new(move || {
                for button in &mut buttons {
                    button.press();
                }
            });
            manager.set_trace(
                &Plan::Source(view.name.clone()),
                arrangement.trace,
                on_delete,
            );
        }
    })
}

fn build_plan<S: Scope<Timestamp = Timestamp>>(
    plan: &Plan,
    manager: &mut TraceManager,
    worker_index: usize,
    scope: &mut S,
    buttons: &mut Vec<ShutdownButton<CapabilitySet<Timestamp>>>,
) -> Collection<S, Datum, Diff> {
    match plan {
        Plan::Source(name) => {
            let (arrangement, button) = manager
                .get_trace(&Plan::Source(name.to_owned()))
                .unwrap_or_else(|| panic!(format!("unable to find dataflow {}", name)))
                .import_core(scope, &format!("Import({})", name));
            buttons.push(button);
            arrangement.as_collection(|k, ()| k.to_owned())
        }

        Plan::Project { outputs, input } => {
            let outputs = outputs.clone();
            build_plan(&input, manager, worker_index, scope, buttons).map(move |datum| {
                Datum::Tuple(outputs.iter().map(|expr| eval_expr(expr, &datum)).collect())
            })
        }

        Plan::Filter { predicate, input } => {
            let predicate = predicate.clone();
            build_plan(&input, manager, worker_index, scope, buttons).filter(move |datum| {
                match eval_expr(&predicate, &datum) {
                    Datum::False | Datum::Null => false,
                    Datum::True => true,
                    _ => unreachable!(),
                }
            })
        }

        // TODO(benesch): this is extremely inefficient. Optimize.
        Plan::Aggregate { key, aggs, input } => {
            let mut plan = {
                let key = key.clone();
                build_plan(&input, manager, worker_index, scope, buttons)
                    .map(move |datum| (eval_expr(&key, &datum), Some(datum)))
            };
            match &key {
                // empty GROUP BY, add a sentinel value so that reduce produces output even on empty inputs
                Expr::Tuple(exprs) if exprs.is_empty() => {
                    let sentinel = if worker_index == 0 {
                        vec![(Datum::Tuple(vec![]), None)]
                    } else {
                        vec![]
                    };
                    plan = plan.concat(&scope.new_collection_from(sentinel).1);
                }
                _ => (),
            }
            let aggs = aggs.clone();
            let plan = plan
                .reduce(move |_key, input, output| {
                    let res: Vec<_> = aggs
                        .iter()
                        .map(|agg| {
                            let datums = input
                                .iter()
                                .filter_map(|(datum, cnt)| {
                                    datum.as_ref().map(|datum| {
                                        let datum = eval_expr(&agg.expr, datum);
                                        iter::repeat(datum).take(*cnt as usize)
                                    })
                                })
                                .flatten();
                            (agg.func.func())(datums)
                        })
                        .collect();
                    output.push((res, 1));
                })
                .map(|(key, values)| {
                    let mut tuple = key.unwrap_tuple();
                    tuple.extend(values);
                    Datum::Tuple(tuple)
                });
            plan
        }

        Plan::Join {
            left_key,
            right_key,
            left,
            right,
            include_left_outer,
            include_right_outer,
        } => {
            use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
            use differential_dataflow::operators::join::JoinCore;
            use differential_dataflow::operators::reduce::Threshold;

            let left_plan = left;
            let right_plan = right;

            // Ensure left arrangement.
            let left_arranged = if let Some(mut trace) = manager.get_keyed_trace(&left, &left_key) {
                trace.import(scope)
            } else {
                let left_key2 = left_key.clone();
                let left_trace = build_plan(&left_plan, manager, worker_index, scope, buttons)
                    .map(move |datum| (eval_expr(&left_key2, &datum), datum))
                    .arrange_by_key();

                // manager.set_keyed_trace(
                //     &left_plan,
                //     &left_key,
                //     left_trace.trace.clone(),
                //     Box::new(|| ()),
                // );
                left_trace
            };

            // Ensure right arrangement.
            let right_arranged = if let Some(mut trace) =
                manager.get_keyed_trace(&right, &right_key)
            {
                trace.import(scope)
            } else {
                let right_key2 = right_key.clone();
                let right_trace = build_plan(&right_plan, manager, worker_index, scope, buttons)
                    .map(move |datum| (eval_expr(&right_key2, &datum), datum))
                    .arrange_by_key();

                // manager.set_keyed_trace(
                //     &right_plan,
                //     &right_key,
                //     right_trace.trace.clone(),
                //     Box::new(|| ()),
                // );
                right_trace
            };

            let mut flow = left_arranged.join_core(&right_arranged, |_key, left, right| {
                let mut tuple = left.clone().unwrap_tuple();
                tuple.extend(right.clone().unwrap_tuple());
                Some(Datum::Tuple(tuple))
            });

            if let Some(num_cols) = include_left_outer {
                let num_cols = *num_cols;

                let right_keys = right_arranged.as_collection(|k, _v| k.clone()).distinct();

                flow = left_arranged
                    .antijoin(&right_keys)
                    .map(move |(_key, left)| {
                        let mut tuple = left.unwrap_tuple();
                        tuple.extend((0..num_cols).map(|_| Datum::Null));
                        Datum::Tuple(tuple)
                    })
                    .concat(&flow);
            }

            if let Some(num_cols) = include_right_outer {
                let num_cols = *num_cols;

                let left_keys = left_arranged.as_collection(|k, _v| k.clone()).distinct();

                flow = right_arranged
                    .antijoin(&left_keys)
                    .map(move |(_key, right)| {
                        let mut tuple = (0..num_cols).map(|_| Datum::Null).collect::<Vec<_>>();
                        tuple.extend(right.unwrap_tuple());
                        Datum::Tuple(tuple)
                    })
                    .concat(&flow);
            }

            flow
        }

        Plan::Distinct(plan) => {
            build_plan(plan, manager, worker_index, scope, buttons).distinct_total()
        }
        Plan::UnionAll(plans) => {
            assert!(!plans.is_empty());
            let mut plans = plans
                .iter()
                .map(|plan| build_plan(plan, manager, worker_index, scope, buttons));
            let plan = plans.next().unwrap();
            plans.fold(plan, |p1, p2| p1.concat(&p2))
        }
    }
}

fn eval_expr(expr: &Expr, datum: &Datum) -> Datum {
    match expr {
        Expr::Ambient => datum.clone(),
        Expr::Column(index, expr) => match eval_expr(expr, datum) {
            Datum::Tuple(tuple) => tuple[*index].clone(),
            _ => unreachable!(),
        },
        Expr::Tuple(exprs) => {
            let exprs = exprs.iter().map(|e| eval_expr(e, datum)).collect();
            Datum::Tuple(exprs)
        }
        Expr::Literal(datum) => datum.clone(),
        Expr::CallUnary { func, expr } => {
            let datum = eval_expr(expr, datum);
            (func.func())(datum)
        }
        Expr::CallBinary { func, expr1, expr2 } => {
            let datum1 = eval_expr(expr1, datum);
            let datum2 = eval_expr(expr2, datum);
            (func.func())(datum1, datum2)
        }
    }
}
