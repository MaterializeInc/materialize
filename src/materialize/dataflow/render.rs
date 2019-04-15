// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use avro_rs::Schema as AvroSchema;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::{AsCollection, Collection};
use std::iter;
use timely::communication::Allocate;
use timely::dataflow::Scope;
use timely::worker::Worker as TimelyWorker;

use super::source;
use super::trace::TraceManager;
use super::types::*;
use crate::repr::Datum;

pub fn build_dataflow<A: Allocate>(
    dataflow: &Dataflow,
    manager: &mut TraceManager,
    worker: &mut TimelyWorker<A>,
) {
    worker.dataflow::<Time, _, _>(|scope| {
        match dataflow {
            Dataflow::Source(src) => {
                let (topic, addr) = match &src.connector {
                    Connector::Kafka { topic, addr } => (topic, addr),
                };

                use rdkafka::config::ClientConfig;
                use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};

                // TODO(benesch): fix this copypasta.
                let mut consumer_config = ClientConfig::new();
                consumer_config
                    .set("produce.offset.report", "true")
                    .set("auto.offset.reset", "smallest")
                    .set("group.id", &format!("materialize-{}", src.name))
                    .set("enable.auto.commit", "false")
                    .set("enable.partition.eof", "false")
                    .set("auto.offset.reset", "earliest")
                    .set("session.timeout.ms", "6000")
                    .set("bootstrap.servers", &addr.to_string());

                let consumer: BaseConsumer<DefaultConsumerContext> =
                    consumer_config.create().unwrap();
                consumer.subscribe(&[&topic]).unwrap();

                let schema = AvroSchema::parse_str(&src.raw_schema).unwrap();

                let arrangement =
                    source::kafka(scope, &src.name, consumer, move |mut bytes, cap, output| {
                        // Chomp five bytes; the first byte is a magic byte (0) that
                        // indicates the Confluent serialization format version, and
                        // the next four bytes are a 32-bit schema ID. We should
                        // deal with the schema registry eventually, but for now
                        // we require the user to hardcode their one true schema in
                        // the data source definition.
                        //
                        // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
                        bytes = &bytes[5..];
                        let val =
                            avro_rs::from_avro_datum(&schema, &mut bytes, Some(&schema)).unwrap();
                        let mut row = Vec::new();
                        match val {
                            avro_rs::types::Value::Record(cols) => {
                                for (_field_name, col) in cols {
                                    row.push(match col {
                                        avro_rs::types::Value::Long(i) => Datum::Int64(i),
                                        avro_rs::types::Value::String(s) => Datum::String(s),
                                        _ => panic!("avro deserialization went wrong"),
                                    })
                                }
                            }
                            _ => panic!("avro deserialization went wrong"),
                        }
                        let time = *cap.time();
                        output.session(cap).give((Datum::Tuple(row), time, 1));

                        // Indicate that we are not yet done.
                        false
                    })
                    .as_collection()
                    .arrange_by_self();
                manager.set_trace(src.name.clone(), &arrangement.trace);
            }
            Dataflow::View(view) => {
                let arrangement = build_plan(&view.plan, manager, scope).arrange_by_self();
                manager.set_trace(view.name.clone(), &arrangement.trace);
            }
        }
    })
}

fn build_plan<S: Scope<Timestamp = Time>>(
    plan: &Plan,
    manager: &mut TraceManager,
    scope: &mut S,
) -> Collection<S, Datum, Diff> {
    match plan {
        Plan::Source(name) => manager
            .get_trace(name.to_owned())
            .expect("unable to find dataflow")
            .import(scope)
            .as_collection(|k, ()| k.to_owned()),

        Plan::Project { outputs, input } => {
            let outputs = outputs.clone();
            build_plan(&input, manager, scope).map(move |datum| {
                Datum::Tuple(outputs.iter().map(|expr| eval_expr(expr, &datum)).collect())
            })
        }

        Plan::Filter { predicate, input } => {
            let predicate = predicate.clone();
            build_plan(&input, manager, scope).filter(move |datum| {
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
            build_plan(&input, manager, scope)
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
        } => {
            let left_key = left_key.clone();
            let right_key = right_key.clone();
            let left = build_plan(&left, manager, scope)
                .map(move |datum| (eval_expr(&left_key, &datum), datum));
            let right = build_plan(&right, manager, scope)
                .map(move |datum| (eval_expr(&right_key, &datum), datum));
            left.join(&right).map(|(_key, (left, right))| {
                let mut tuple = left.unwrap_tuple();
                tuple.extend(right.unwrap_tuple());
                Datum::Tuple(tuple)
            })
        }

        Plan::Distinct(_) => unimplemented!(),
        Plan::UnionAll(plans) => {
            assert!(!plans.is_empty());
            let mut plans = plans.iter().map(|plan| build_plan(plan, manager, scope));
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
