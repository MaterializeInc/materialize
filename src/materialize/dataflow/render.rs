// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use avro_rs::Schema as AvroSchema;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::{AsCollection, Collection};
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
                    .set("group.id", "example")
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
                Datum::Tuple(
                    outputs
                        .iter()
                        .map(|expr| eval_expr(expr, &datum))
                        .collect(),
                )
            })
        }
        Plan::Distinct(_) => unimplemented!(),
        Plan::UnionAll(_) => unimplemented!(),
        Plan::Join { .. } => unimplemented!(),
    }
}

fn eval_expr(expr: &Expr, datum: &Datum) -> Datum {
    match expr {
        Expr::Ambient => datum.clone(),
        Expr::Column(index, expr) => match eval_expr(expr, datum) {
            Datum::Tuple(tuple) => tuple[*index].clone(),
            _ => unreachable!(),
        },
        Expr::Literal(datum) => datum.clone(),
    }
}
