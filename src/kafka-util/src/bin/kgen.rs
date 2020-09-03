// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use chrono::{NaiveDate, NaiveDateTime};
use clap::{App, Arg};
use mz_avro::{
    schema::{SchemaNode, SchemaPiece, SchemaPieceOrNamed},
    types::Value,
    Schema,
};
use rand::{
    distributions::{uniform::SampleUniform, Alphanumeric, Bernoulli, Uniform, WeightedIndex},
    prelude::{Distribution, ThreadRng},
    thread_rng,
};
use rdkafka::{
    error::KafkaError,
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    types::RDKafkaError,
    util::Timeout,
    ClientConfig,
};
use serde_json::Map;
use std::convert::{TryFrom, TryInto};
use std::ops::Add;

use std::time::Duration;
use std::{cell::RefCell, collections::HashMap, rc::Rc, thread::sleep};
use tokio::runtime::Runtime;
use url::Url;

struct RandomAvroGenerator<'a> {
    // generators
    ints: HashMap<*const SchemaPiece, Box<dyn FnMut() -> i32>>,
    longs: HashMap<*const SchemaPiece, Box<dyn FnMut() -> i64>>,
    strings: HashMap<*const SchemaPiece, Box<dyn FnMut(&mut Vec<u8>)>>,
    bytes: HashMap<*const SchemaPiece, Box<dyn FnMut(&mut Vec<u8>)>>,
    unions: HashMap<*const SchemaPiece, Box<dyn FnMut() -> usize>>,
    enums: HashMap<*const SchemaPiece, Box<dyn FnMut() -> usize>>,
    bools: HashMap<*const SchemaPiece, Box<dyn FnMut() -> bool>>,
    floats: HashMap<*const SchemaPiece, Box<dyn FnMut() -> f32>>,
    doubles: HashMap<*const SchemaPiece, Box<dyn FnMut() -> f64>>,
    array_lens: HashMap<*const SchemaPiece, Box<dyn FnMut() -> usize>>,
    _map_keys: HashMap<*const SchemaPiece, Box<dyn FnMut(&mut Vec<u8>)>>,

    schema: SchemaNode<'a>,
}

impl<'a> RandomAvroGenerator<'a> {
    fn gen_inner(&mut self, node: SchemaNode) -> Value {
        let p: *const _ = &*node.inner;
        match node.inner {
            SchemaPiece::Null => Value::Null,
            SchemaPiece::Boolean => {
                let val = self.bools.get_mut(&p).unwrap()();
                Value::Boolean(val)
            }
            SchemaPiece::Int => {
                let val = self.ints.get_mut(&p).unwrap()();
                Value::Int(val)
            }
            SchemaPiece::Long => {
                let val = self.longs.get_mut(&p).unwrap()();
                Value::Long(val)
            }
            SchemaPiece::Float => {
                let val = self.floats.get_mut(&p).unwrap()();
                Value::Float(val)
            }
            SchemaPiece::Double => {
                let val = self.doubles.get_mut(&p).unwrap()();
                Value::Double(val)
            }
            SchemaPiece::Date => {
                let days = self.ints.get_mut(&p).unwrap()();
                let val = NaiveDate::from_ymd(1970, 1, 1).add(chrono::Duration::days(days as i64));
                Value::Date(val)
            }
            SchemaPiece::TimestampMilli => {
                let millis = self.longs.get_mut(&p).unwrap()();

                let seconds = millis / 1000;
                let fraction = (millis % 1000) as u32;
                let val = NaiveDateTime::from_timestamp_opt(seconds, fraction * 1_000_000).unwrap();
                Value::Timestamp(val)
            }
            SchemaPiece::TimestampMicro => {
                let micros = self.longs.get_mut(&p).unwrap()();

                let seconds = micros / 1_000_000;
                let fraction = (micros % 1_000_000) as u32;
                let val = NaiveDateTime::from_timestamp_opt(seconds, fraction * 1_000).unwrap();
                Value::Timestamp(val)
            }
            SchemaPiece::Decimal {
                precision: _,
                scale: _,
                fixed_size: _,
            } => unreachable!(),
            SchemaPiece::Bytes => {
                let f = self.bytes.get_mut(&p).unwrap();
                let mut val = vec![];
                f(&mut val);
                Value::Bytes(val)
            }
            SchemaPiece::String => {
                let f = self.strings.get_mut(&p).unwrap();
                let mut buf = vec![];
                f(&mut buf);
                let val = String::from_utf8(buf).unwrap();
                Value::String(val)
            }
            SchemaPiece::Json => unreachable!(),
            SchemaPiece::Uuid => unreachable!(),
            SchemaPiece::Array(inner) => {
                let len = self.array_lens.get_mut(&p).unwrap()();
                let next = node.step(&**inner);
                let inner_vals = (0..len).map(move |_| self.gen_inner(next)).collect();
                Value::Array(inner_vals)
            }
            SchemaPiece::Map(_inner) => {
                // let len = self.array_lens.get_mut(&p).unwrap()();
                // let key_f = self.map_keys.get_mut(&p).unwrap();
                // let next = node.step(&**inner);
                // let inner_entries = (0..len)
                //     .map(|_| {
                //         let mut key_buf = vec![];
                //         key_f(&mut key_buf);
                //         let key = String::from_utf8(key_buf).unwrap();
                //         let val = self.gen_inner(next);
                //         (key, val)
                //     })
                //     .collect();
                // Value::Map(inner_entries)
                unreachable!()
            }
            SchemaPiece::Union(us) => {
                let index = self.unions.get_mut(&p).unwrap()();
                let next = node.step(&us.variants()[index]);
                let null_variant = us
                    .variants()
                    .iter()
                    .position(|v| v == &SchemaPieceOrNamed::Piece(SchemaPiece::Null));
                let inner = Box::new(self.gen_inner(next));
                Value::Union {
                    index,
                    inner,
                    n_variants: us.variants().len(),
                    null_variant,
                }
            }
            SchemaPiece::ResolveIntTsMilli
            | SchemaPiece::ResolveIntTsMicro
            | SchemaPiece::ResolveDateTimestamp
            | SchemaPiece::ResolveIntLong
            | SchemaPiece::ResolveIntFloat
            | SchemaPiece::ResolveIntDouble
            | SchemaPiece::ResolveLongFloat
            | SchemaPiece::ResolveLongDouble
            | SchemaPiece::ResolveFloatDouble
            | SchemaPiece::ResolveConcreteUnion { .. }
            | SchemaPiece::ResolveUnionUnion { .. }
            | SchemaPiece::ResolveUnionConcrete { .. }
            | SchemaPiece::ResolveRecord { .. }
            | SchemaPiece::ResolveEnum { .. } => {
                unreachable!("We never resolve schemas, so seeing this is impossible")
            }
            SchemaPiece::Record { fields, .. } => {
                let fields = fields
                    .iter()
                    .map(|f| {
                        let k = f.name.clone();
                        let next = node.step(&f.schema);
                        let v = self.gen_inner(next);
                        (k, v)
                    })
                    .collect();
                Value::Record(fields)
            }
            SchemaPiece::Enum { symbols, .. } => {
                let i = self.enums.get_mut(&p).unwrap()();
                Value::Enum(i, symbols[i].clone())
            }
            SchemaPiece::Fixed { size: _ } => unreachable!(),
        }
    }
    pub fn gen(&mut self) -> Value {
        self.gen_inner(self.schema)
    }
    fn new_inner(
        &mut self,
        node: SchemaNode<'a>,
        annotations: &Map<String, serde_json::Value>,
        field_name: Option<&str>,
    ) {
        let rng = Rc::new(RefCell::new(thread_rng()));
        fn bool_dist(
            json: &serde_json::Value,
            rng: Rc<RefCell<ThreadRng>>,
        ) -> impl FnMut() -> bool {
            let x = json.as_f64().unwrap();
            let dist = Bernoulli::new(x).unwrap();
            move || dist.sample(&mut *rng.borrow_mut())
        }
        fn integral_dist<T>(
            json: &serde_json::Value,
            rng: Rc<RefCell<ThreadRng>>,
        ) -> impl FnMut() -> T
        where
            T: SampleUniform + TryFrom<i64>,
            <T as TryFrom<i64>>::Error: std::fmt::Debug,
        {
            let x = json.as_array().unwrap();
            let (min, max): (T, T) = (
                x[0].as_i64().unwrap().try_into().unwrap(),
                x[1].as_i64().unwrap().try_into().unwrap(),
            );
            let dist = Uniform::new_inclusive(min, max);
            move || dist.sample(&mut *rng.borrow_mut())
        }
        fn float_dist(
            json: &serde_json::Value,
            rng: Rc<RefCell<ThreadRng>>,
        ) -> impl FnMut() -> f32 {
            let x = json.as_array().unwrap();
            let (min, max) = (x[0].as_f64().unwrap() as f32, x[1].as_f64().unwrap() as f32);
            let dist = Uniform::new_inclusive(min, max);
            move || dist.sample(&mut *rng.borrow_mut())
        }
        fn double_dist(
            json: &serde_json::Value,
            rng: Rc<RefCell<ThreadRng>>,
        ) -> impl FnMut() -> f64 {
            let x = json.as_array().unwrap();
            let (min, max) = (x[0].as_f64().unwrap(), x[1].as_f64().unwrap());
            let dist = Uniform::new_inclusive(min, max);
            move || dist.sample(&mut *rng.borrow_mut())
        }
        fn string_dist(
            json: &serde_json::Value,
            rng: Rc<RefCell<ThreadRng>>,
        ) -> impl FnMut(&mut Vec<u8>) {
            let mut len = integral_dist::<usize>(json, rng.clone());
            move |v| {
                let len = len();
                v.reserve(len);
                unsafe { v.set_len(len) }
                let cd = Alphanumeric;
                for i in 0..len {
                    v[i] = cd.sample(&mut *rng.borrow_mut()) as u8;
                }
            }
        }
        fn bytes_dist(
            json: &serde_json::Value,
            rng: Rc<RefCell<ThreadRng>>,
        ) -> impl FnMut(&mut Vec<u8>) {
            let mut len = integral_dist::<usize>(json, rng.clone());
            move |v| {
                let len = len();
                v.reserve(len);
                unsafe { v.set_len(len) }
                let bd = Uniform::new_inclusive(0, 255);
                for i in 0..len {
                    v[i] = bd.sample(&mut *rng.borrow_mut());
                }
            }
        }
        let p: *const _ = &*node.inner;

        let dist_json = field_name.and_then(|fn_| annotations.get(fn_));
        match node.inner {
            SchemaPiece::Null => {}
            SchemaPiece::Boolean => {
                let dist = bool_dist(dist_json.unwrap(), rng);
                self.bools.insert(p, Box::new(dist));
            }
            SchemaPiece::Int => {
                let dist = integral_dist(dist_json.unwrap(), rng);
                self.ints.insert(p, Box::new(dist));
            }
            SchemaPiece::Long => {
                let dist = integral_dist(dist_json.unwrap(), rng);
                self.longs.insert(p, Box::new(dist));
            }
            SchemaPiece::Float => {
                let dist = float_dist(dist_json.unwrap(), rng);
                self.floats.insert(p, Box::new(dist));
            }
            SchemaPiece::Double => {
                let dist = double_dist(dist_json.unwrap(), rng);
                self.doubles.insert(p, Box::new(dist));
            }
            SchemaPiece::Date => {}
            SchemaPiece::TimestampMilli => {}
            SchemaPiece::TimestampMicro => {}
            SchemaPiece::Decimal {
                precision: _,
                scale: _,
                fixed_size: _,
            } => unimplemented!(),
            SchemaPiece::Bytes => {
                let len_dist_json = annotations
                    .get(&format!("{}.len", field_name.unwrap()))
                    .unwrap();
                let dist = bytes_dist(len_dist_json, rng);
                self.bytes.insert(p, Box::new(dist));
            }
            SchemaPiece::String => {
                let len_dist_json = annotations
                    .get(&format!("{}.len", field_name.unwrap()))
                    .unwrap();
                let dist = string_dist(len_dist_json, rng);
                self.strings.insert(p, Box::new(dist));
            }
            SchemaPiece::Json => unimplemented!(),
            SchemaPiece::Uuid => unimplemented!(),
            SchemaPiece::Array(inner) => {
                let fn_ = field_name.unwrap();
                let len_dist_json = annotations.get(&format!("{}.len", fn_)).unwrap();
                let len = integral_dist::<usize>(len_dist_json, rng);
                self.array_lens.insert(p, Box::new(len));
                let item_fn = format!("{}[]", fn_);
                self.new_inner(node.step(&**inner), annotations, Some(&item_fn))
            }
            SchemaPiece::Map(_) => unimplemented!(),
            SchemaPiece::Union(us) => {
                let variant_jsons = dist_json.unwrap().as_array().unwrap();
                assert!(variant_jsons.len() == us.variants().len());
                let probabilities = variant_jsons.iter().map(|v| v.as_f64().unwrap());
                let dist = WeightedIndex::new(probabilities).unwrap();
                let rng = rng;
                let f = move || dist.sample(&mut *rng.borrow_mut());
                self.unions.insert(p, Box::new(f));
                let fn_ = field_name.unwrap();
                for (i, v) in us.variants().iter().enumerate() {
                    let fn_ = format!("{}.{}", fn_, i);
                    self.new_inner(node.step(v), annotations, Some(&fn_))
                }
            }
            SchemaPiece::Record {
                doc: _,
                fields,
                lookup: _,
            } => {
                let name = node.name.unwrap();
                for f in fields {
                    let fn_ = format!("{}::{}", name, f.name);
                    self.new_inner(node.step(&f.schema), annotations, Some(&fn_));
                }
            }
            SchemaPiece::Enum {
                doc: _,
                symbols: _,
                default_idx: _,
            } => unimplemented!(),
            SchemaPiece::Fixed { size: _ } => unimplemented!(),
            SchemaPiece::ResolveIntTsMilli
            | SchemaPiece::ResolveIntTsMicro
            | SchemaPiece::ResolveDateTimestamp
            | SchemaPiece::ResolveIntLong
            | SchemaPiece::ResolveIntFloat
            | SchemaPiece::ResolveIntDouble
            | SchemaPiece::ResolveLongFloat
            | SchemaPiece::ResolveLongDouble
            | SchemaPiece::ResolveFloatDouble
            | SchemaPiece::ResolveConcreteUnion { .. }
            | SchemaPiece::ResolveUnionUnion { .. }
            | SchemaPiece::ResolveUnionConcrete { .. }
            | SchemaPiece::ResolveRecord { .. }
            | SchemaPiece::ResolveEnum { .. } => unreachable!(),
        };
    }
    pub fn new(schema: &'a Schema, annotations: &serde_json::Value) -> Self {
        let mut self_ = Self {
            ints: Default::default(),
            longs: Default::default(),
            strings: Default::default(),
            bytes: Default::default(),
            unions: Default::default(),
            enums: Default::default(),
            bools: Default::default(),
            floats: Default::default(),
            doubles: Default::default(),
            array_lens: Default::default(),
            _map_keys: Default::default(),
            schema: schema.top_node(),
        };
        self_.new_inner(schema.top_node(), annotations.as_object().unwrap(), None);
        self_
    }
}

enum ValueGenerator<'a> {
    UniformBytes {
        len: Uniform<usize>,
        bytes: Uniform<u8>,
        rng: ThreadRng,
    },
    RandomAvro {
        inner: RandomAvroGenerator<'a>,
        schema: &'a Schema,
        schema_id: i32,
    },
}

impl<'a> ValueGenerator<'a> {
    pub fn next_value(&mut self, out: &mut Vec<u8>) {
        match self {
            ValueGenerator::UniformBytes { len, bytes, rng } => {
                let len = len.sample(rng);
                out.reserve(len);
                // safety - everything will be set by the below loop before ever being read
                unsafe {
                    out.set_len(len);
                }
                for i in 0..len {
                    out[i] = bytes.sample(rng);
                }
            }
            ValueGenerator::RandomAvro {
                inner,
                schema,
                schema_id,
            } => {
                let value = inner.gen();
                out.clear();
                out.push(0);
                for b in schema_id.to_be_bytes().iter() {
                    out.push(*b);
                }
                debug_assert!(value.validate(schema.top_node()));
                mz_avro::encode_unchecked(&value, schema, out);
            }
        }
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
            Arg::with_name("values")
                .short("v")
                .long("values")
                .takes_value(true)
                .possible_values(&["bytes", "avro"])
                .default_value("bytes"),
        )
        .arg(
            Arg::with_name("min")
                .short("m")
                .long("min-message-size")
                .takes_value(true)
                .required_if("values", "bytes"),
        )
        .arg(
            Arg::with_name("max")
                .short("M")
                .long("max-message-size")
                .takes_value(true)
                .required_if("values", "bytes"),
        )
        .arg(
            Arg::with_name("avro-schema")
                .long("avro-schema")
                .takes_value(true)
                .required_if("values", "avro"),
        )
        .arg(
            Arg::with_name("avro-distribution")
                .long("avro-distribution")
                .takes_value(true)
                .required_if("values", "avro"),
        )
        .arg(
            Arg::with_name("bootstrap")
                .short("b")
                .long("bootstrap-server")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("keys")
                .long("keys")
                .short("k")
                .takes_value(true)
                .possible_values(&["sequential", "random"])
                .default_value("sequential"),
        )
        .arg(
            Arg::with_name("key-min")
                .long("key-min")
                .takes_value(true)
                .required_if("keys", "random"),
        )
        .arg(
            Arg::with_name("key-max")
                .long("key-max")
                .takes_value(true)
                .required_if("keys", "random"),
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
    let bootstrap = matches.value_of("bootstrap").unwrap();
    let mut _schema_holder = None;
    let mut value_gen = match matches.value_of("values").unwrap() {
        "bytes" => {
            let min: usize = matches.value_of("min").unwrap().parse()?;
            let max: usize = matches.value_of("max").unwrap().parse()?;
            let len = Uniform::new_inclusive(min, max);
            let bytes = Uniform::new_inclusive(0, 255);
            let rng = thread_rng();

            ValueGenerator::UniformBytes { len, bytes, rng }
        }
        "avro" => {
            let schema = matches.value_of("avro-schema").unwrap();
            let ccsr =
                ccsr::ClientConfig::new(Url::parse("http://localhost:8081").unwrap()).build();
            let schema_id = Runtime::new()
                .unwrap()
                .block_on(ccsr.publish_schema(&format!("{}-value", topic), schema))?;
            _schema_holder = Some(Schema::parse_str(schema)?);
            let schema = _schema_holder.as_ref().unwrap();
            let annotations = matches.value_of("avro-distribution").unwrap();
            let annotations: serde_json::Value = serde_json::from_str(annotations)?;
            let generator = RandomAvroGenerator::new(schema, &annotations);

            ValueGenerator::RandomAvro {
                inner: generator,
                schema,
                schema_id,
            }
        }
        _ => unreachable!(),
    };

    let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;
    let mut buf = vec![];

    let mut rng = thread_rng();
    let keys_random = matches.value_of("keys").unwrap() == "random";
    let key_dist = if keys_random {
        let key_min: u64 = matches.value_of("key-min").unwrap().parse()?;
        let key_max: u64 = matches.value_of("key-max").unwrap().parse()?;
        Some(Uniform::new_inclusive(key_min, key_max))
    } else {
        None
    };
    for i in 0..n {
        if i % 10000 == 0 {
            eprintln!("Generating message {}", i);
        }
        value_gen.next_value(&mut buf);
        let key_i = if let Some(key_dist) = key_dist.as_ref() {
            key_dist.sample(&mut rng)
        } else {
            i as u64
        };
        let key = key_i.to_be_bytes();
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
