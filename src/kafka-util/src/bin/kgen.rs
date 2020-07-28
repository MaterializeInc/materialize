// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use clap::{App, Arg};
use rand::{distributions::Uniform, prelude::Distribution, thread_rng, Rng};
use rdkafka::{
    error::KafkaError,
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    types::RDKafkaError,
    util::Timeout,
    ClientConfig,
};
use std::{thread::sleep, time::Duration};

fn gen_value<LenDist: Distribution<usize>, DataDist: Distribution<u8>, R: Rng>(
    out: &mut Vec<u8>,
    ld: &LenDist,
    dd: &DataDist,
    rng: &mut R,
) {
    let len = ld.sample(rng);
    out.reserve(len);
    // safety - everything will be set by the below loop before ever being read
    unsafe {
        out.set_len(len);
    }
    for i in 0..len {
        out[i] = dd.sample(rng);
    }
}

fn main() -> anyhow::Result<()> {
    let matches = App::new("kgen")
        .about("Put random data in Kafka")
        .arg(
            Arg::with_name("topic")
                .short("t")
                .long("topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("num-records")
                .short("n")
                .long("num-messages")
                .takes_value(true)
                .required(true),
        )
        .arg(
            // default 1
            Arg::with_name("partitions")
                .short("p")
                .long("partitions")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("min")
                .short("m")
                .long("min-message-size")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("max")
                .short("M")
                .long("max-message-size")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("bootstrap")
                .short("b")
                .long("bootstrap-server")
                .takes_value(true),
        )
        .get_matches();
    let topic = matches.value_of("topic").unwrap();
    let n: usize = matches.value_of("num-records").unwrap().parse()?;
    let partitions: usize = matches
        .value_of("partitions")
        .map(str::parse)
        .transpose()?
        .unwrap_or(1);
    if partitions == 0 {
        bail!("Partitions must a positive number.");
    }
    let bootstrap = matches.value_of("bootstrap").unwrap_or("localhost:9092");
    let min: usize = matches.value_of("min").unwrap().parse()?;
    let max: usize = matches.value_of("max").unwrap().parse()?;

    let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;
    let ld = Uniform::new_inclusive(min, max);
    let dd = Uniform::new_inclusive(0, 255);
    let mut buf = vec![];
    let mut rng = thread_rng();
    for i in 0..n {
        if i % 10000 == 0 {
            eprintln!("Generating message {}", i);
        }
        gen_value(&mut buf, &ld, &dd, &mut rng);
        let key = i.to_le_bytes();
        let mut rec = BaseRecord::to(topic)
            .key(&key)
            .payload(&buf)
            .partition((i % partitions) as i32);
        loop {
            match producer.send(rec) {
                Ok(()) => break,
                Err((KafkaError::MessageProduction(RDKafkaError::QueueFull), rec2)) => {
                    rec = rec2;
                    sleep(Duration::from_secs(1));
                }
                Err((e, _)) => {
                    return Err(e.into());
                }
            }
        }
    }
    producer.flush(Timeout::Never);
    Ok(())
}
