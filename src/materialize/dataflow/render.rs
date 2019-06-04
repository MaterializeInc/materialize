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
use std::collections::{HashMap, HashSet};
use std::iter;
use std::rc::Rc;
use timely::communication::Allocate;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::InputHandle;
use timely::dataflow::Scope;
use timely::worker::Worker as TimelyWorker;

use super::sink;
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
        vec![vec![Datum::String("X".into())]]
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
    // insert_mux: &source::InsertMux,
    inputs: &mut HashMap<String, InputHandle<Timestamp, (Vec<Datum>, Timestamp, isize)>>,
) {
    let worker_index = worker.index();
    worker.dataflow::<Timestamp, _, _>(|scope| match dataflow {
        Dataflow::Source(src) => {
            let done = Rc::new(Cell::new(false));
            let plan = match &src.connector {
                SourceConnector::Kafka(c) => {
                    source::kafka(scope, &src.name, &c, done.clone(), clock)
                }
                SourceConnector::Local(l) => {
                    use timely::dataflow::operators::input::Input;
                    let (handle, stream) = scope.new_input();
                    if worker_index == 0 {
                        // drop the handle if not worker zero.
                        inputs.insert(src.name.clone(), handle);
                    }
                    stream
                    // source::local(scope, &src.name, &l, done.clone(), clock, insert_mux)
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
        Dataflow::Sink(sink) => {
            let done = Rc::new(Cell::new(false));
            let (arrangement, _) = manager
                .get_trace(&Plan::Source(sink.from.to_owned()))
                .unwrap_or_else(|| panic!(format!("unable to find dataflow {}", sink.from)))
                .import_core(scope, &format!("Import({})", sink.from));
            match &sink.connector {
                SinkConnector::Kafka(c) => {
                    sink::kafka(&arrangement.stream, &sink.name, c, done, clock)
                }
            }
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
) -> Collection<S, Vec<Datum>, Diff> {
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
            build_plan(&input, manager, worker_index, scope, buttons)
                .map(move |data| outputs.iter().map(|expr| expr.eval(&data)).collect())
        }

        Plan::Filter { predicate, input } => {
            let predicate = predicate.clone();
            build_plan(&input, manager, worker_index, scope, buttons).filter(move |data| {
                match predicate.eval(&data) {
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
                build_plan(&input, manager, worker_index, scope, buttons).map(move |data| {
                    (
                        key.iter().map(|expr| expr.eval(&data)).collect(),
                        Some(data),
                    )
                })
            };
            if key.is_empty() {
                // empty GROUP BY, add a sentinel value so that reduce produces output even on empty inputs
                let sentinel = if worker_index == 0 {
                    vec![(vec![], None)]
                } else {
                    vec![]
                };
                plan = plan.concat(&scope.new_collection_from(sentinel).1);
            }
            let aggs = aggs.clone();
            let plan = plan
                .reduce(move |_key, input, output| {
                    let res: Vec<_> = aggs
                        .iter()
                        .map(|agg| {
                            if agg.distinct {
                                let datums = input
                                    .iter()
                                    .filter_map(|(data, _cnt)| {
                                        data.as_ref().map(|data| agg.expr.eval(&data))
                                    })
                                    .collect::<HashSet<_>>();
                                (agg.func.func())(datums)
                            } else {
                                let datums = input
                                    .iter()
                                    .filter_map(|(data, cnt)| {
                                        data.as_ref().map(|data| {
                                            let datum = agg.expr.eval(&data);
                                            iter::repeat(datum).take(*cnt as usize)
                                        })
                                    })
                                    .flatten();
                                (agg.func.func())(datums)
                            }
                        })
                        .collect();
                    output.push((res, 1));
                })
                .map(|(mut key, values)| {
                    key.extend(values);
                    key
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
                    .map(move |data| {
                        (
                            left_key2.iter().map(|expr| expr.eval(&data)).collect(),
                            data,
                        )
                    })
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
                    .map(move |data| {
                        (
                            right_key2.iter().map(|expr| expr.eval(&data)).collect(),
                            data,
                        )
                    })
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
                let mut left = left.clone();
                left.extend(right.clone());
                Some(left)
            });

            if let Some(num_cols) = include_left_outer {
                let num_cols = *num_cols;

                let right_keys = right_arranged.as_collection(|k, _v| k.clone()).distinct();

                flow = left_arranged
                    .antijoin(&right_keys)
                    .map(move |(_key, mut left)| {
                        left.extend((0..num_cols).map(|_| Datum::Null));
                        left
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
                        tuple.extend(right);
                        tuple
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

impl ScalarExpr {
    fn eval(&self, data: &[Datum]) -> Datum {
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
                Datum::False => els.eval(data),
                d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
            },
        }
    }
}
