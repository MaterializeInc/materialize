// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::operators::threshold::ThresholdTotal;
use differential_dataflow::{AsCollection, Collection};
use std::cell::Cell;
use std::iter;
use std::rc::Rc;
use timely::communication::Allocate;
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
    worker.dataflow(|scope| {
        let (_, collection) = scope.new_collection_from(vec![Datum::String("X".into())]);
        let arrangement = collection.arrange_by_self();
        let on_delete = Box::new(|| ());
        manager.set_trace("dual".into(), &arrangement, on_delete);
    })
}

pub fn build_dataflow<A: Allocate>(
    dataflow: &Dataflow,
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
    clock: &Clock,
    insert_mux: &source::InsertMux,
) {
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
            manager.set_trace(src.name.clone(), &arrangement, on_delete);
        }
        Dataflow::View(view) => {
            let mut buttons = Vec::new();
            let arrangement = build_plan(&view.plan, manager, scope, &mut buttons).arrange_by_self();
            let on_delete = Box::new(move || for button in buttons.iter_mut() { button.press(); });
            manager.set_trace(view.name.clone(), &arrangement, on_delete);
        }
    })
}

use timely::dataflow::operators::CapabilitySet;
use differential_dataflow::operators::arrange::ShutdownButton;

fn build_plan<S: Scope<Timestamp = Timestamp>>(
    plan: &Plan,
    manager: &mut TraceManager,
    scope: &mut S,
    buttons: &mut Vec<ShutdownButton<CapabilitySet<Timestamp>>>,
) -> Collection<S, Datum, Diff> {
    match plan {
        Plan::Source(name) => {
            let (arranged, button) =
            manager
                .get_trace(name.to_owned())
                .unwrap_or_else(|| panic!(format!("unable to find dataflow {}", name)))
                .import_core(scope, &format!("Import ({})", name));

            buttons.push(button);

            arranged
                .as_collection(|k, ()| k.to_owned())
        },


        Plan::Project { outputs, input } => {
            let outputs = outputs.clone();
            build_plan(&input, manager, scope, buttons).map(move |datum| {
                Datum::Tuple(outputs.iter().map(|expr| eval_expr(expr, &datum)).collect())
            })
        }

        Plan::Filter { predicate, input } => {
            let predicate = predicate.clone();
            build_plan(&input, manager, scope, buttons).filter(move |datum| {
                match eval_expr(&predicate, &datum) {
                    Datum::False => false,
                    Datum::True => true,
                    _ => unreachable!(),
                }
            })
        }

        // TODO(benesch): this is extremely inefficient. Optimize.
        Plan::Aggregate { key, aggs, input } => {
            let key = key.clone();
            let aggs = aggs.clone();
            build_plan(&input, manager, scope, buttons)
                .map(move |datum| (eval_expr(&key, &datum), datum))
                .reduce(move |_key, input, output| {
                    let res: Vec<_> = aggs
                        .iter()
                        .map(|agg| {
                            let datums = input
                                .iter()
                                .map(|(datum, cnt)| {
                                    let datum = eval_expr(&agg.expr, datum);
                                    iter::repeat(datum).take(*cnt as usize)
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
                })
        }

        Plan::Join {
            left_key,
            right_key,
            left,
            right,
            include_left_outer,
            include_right_outer,
        } => {
            let left_key = left_key.clone();
            let right_key = right_key.clone();
            let left = build_plan(&left, manager, scope, buttons)
                .map(move |datum| (eval_expr(&left_key, &datum), datum));
            let right = build_plan(&right, manager, scope, buttons)
                .map(move |datum| (eval_expr(&right_key, &datum), datum));

            let mut flow = left.join(&right).map(|(_key, (left, right))| {
                let mut tuple = left.unwrap_tuple();
                tuple.extend(right.unwrap_tuple());
                Datum::Tuple(tuple)
            });

            if let Some(num_cols) = include_left_outer {
                let num_cols = *num_cols;
                flow = flow.concat(
                    &left
                        .antijoin(&right.map(|(key, _)| key).distinct_total())
                        .map(move |(_key, left)| {
                            let mut tuple = left.unwrap_tuple();
                            tuple.extend((0..num_cols).map(|_| Datum::Null));
                            Datum::Tuple(tuple)
                        }),
                )
            }

            if let Some(num_cols) = include_right_outer {
                let num_cols = *num_cols;
                flow = flow.concat(
                    &right
                        .antijoin(&left.map(|(key, _)| key).distinct_total())
                        .map(move |(_key, right)| {
                            let mut tuple = (0..num_cols).map(|_| Datum::Null).collect::<Vec<_>>();
                            tuple.extend(right.unwrap_tuple());
                            Datum::Tuple(tuple)
                        }),
                )
            }

            flow
        }

        Plan::Distinct(plan) => build_plan(plan, manager, scope, buttons).distinct_total(),
        Plan::UnionAll(plans) => {
            assert!(!plans.is_empty());
            let mut plans = plans.iter().map(|plan| build_plan(plan, manager, scope, buttons));
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
