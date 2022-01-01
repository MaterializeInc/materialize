// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::iter;
use std::ops::Add;
use std::rc::Rc;
use std::time::Duration;

use anyhow::bail;
use chrono::{NaiveDate, NaiveDateTime};
use clap::arg_enum;
use rand::distributions::{
    uniform::SampleUniform, Alphanumeric, Bernoulli, Uniform, WeightedIndex,
};
use rand::prelude::{Distribution, ThreadRng};
use rand::thread_rng;
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde_json::Map;
use structopt::StructOpt;
use url::Url;

use mz_avro::schema::{SchemaNode, SchemaPiece, SchemaPieceOrNamed};
use mz_avro::types::{DecimalValue, Value};
use mz_avro::Schema;
use ore::cast::CastFrom;
use ore::retry::Retry;

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
    decimals: HashMap<*const SchemaPiece, Box<dyn FnMut() -> Vec<u8>>>,
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
                precision,
                scale,
                fixed_size: _,
            } => {
                let f = self.decimals.get_mut(&p).unwrap();
                let unscaled = f();
                Value::Decimal(DecimalValue {
                    unscaled,
                    precision: *precision,
                    scale: *scale,
                })
            }
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
                let cd = Alphanumeric;
                let sample = || cd.sample(&mut *rng.borrow_mut()) as u8;
                v.clear();
                v.extend(iter::repeat_with(sample).take(len));
            }
        }
        fn bytes_dist(
            json: &serde_json::Value,
            rng: Rc<RefCell<ThreadRng>>,
        ) -> impl FnMut(&mut Vec<u8>) {
            let mut len = integral_dist::<usize>(json, rng.clone());
            move |v| {
                let len = len();
                let bd = Uniform::new_inclusive(0, 255);
                let sample = || bd.sample(&mut *rng.borrow_mut());
                v.clear();
                v.extend(iter::repeat_with(sample).take(len));
            }
        }
        fn decimal_dist(
            json: &serde_json::Value,
            rng: Rc<RefCell<ThreadRng>>,
            precision: usize,
        ) -> impl FnMut() -> Vec<u8> {
            let x = json.as_array().unwrap();
            let (min, max): (i64, i64) = (x[0].as_i64().unwrap(), x[1].as_i64().unwrap());
            // Ensure values fit within precision bounds.
            let precision_limit = 10i64
                .checked_pow(u32::try_from(precision).unwrap())
                .unwrap();
            assert!(
                precision_limit >= max,
                "max value of {} exceeds value expressable with precision {}",
                max,
                precision
            );
            assert!(
                precision_limit >= min.abs(),
                "min value of {} exceeds value expressable with precision {}",
                min,
                precision
            );
            let dist = Uniform::<i64>::new_inclusive(min, max);
            move || dist.sample(&mut *rng.borrow_mut()).to_be_bytes().to_vec()
        }
        let p: *const _ = &*node.inner;

        let dist_json = field_name.and_then(|fn_| annotations.get(fn_));
        let err = format!(
            "Distribution annotation not found: {}",
            field_name.unwrap_or("(None)")
        );
        match node.inner {
            SchemaPiece::Null => {}
            SchemaPiece::Boolean => {
                let dist = bool_dist(dist_json.expect(&err), rng);
                self.bools.insert(p, Box::new(dist));
            }
            SchemaPiece::Int => {
                let dist = integral_dist(dist_json.expect(&err), rng);
                self.ints.insert(p, Box::new(dist));
            }
            SchemaPiece::Long => {
                let dist = integral_dist(dist_json.expect(&err), rng);
                self.longs.insert(p, Box::new(dist));
            }
            SchemaPiece::Float => {
                let dist = float_dist(dist_json.expect(&err), rng);
                self.floats.insert(p, Box::new(dist));
            }
            SchemaPiece::Double => {
                let dist = double_dist(dist_json.expect(&err), rng);
                self.doubles.insert(p, Box::new(dist));
            }
            SchemaPiece::Date => {}
            SchemaPiece::TimestampMilli => {}
            SchemaPiece::TimestampMicro => {}
            SchemaPiece::Decimal {
                precision,
                scale: _,
                fixed_size: _,
            } => {
                let dist = decimal_dist(dist_json.expect(&err), rng, *precision);
                self.decimals.insert(p, Box::new(dist));
            }
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
                let variant_jsons = dist_json.expect(&err).as_array().unwrap();
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
            decimals: Default::default(),
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
                let sample = || bytes.sample(rng);
                out.clear();
                out.extend(iter::repeat_with(sample).take(len));
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

arg_enum! {
    pub enum KeyFormat {
        Avro,
        Random,
        Sequential,
    }
}

arg_enum! {
    pub enum ValueFormat {
        Bytes,
        Avro,
    }
}

/// Write random data to Kafka.
#[derive(StructOpt)]
struct Args {
    // == Kafka configuration arguments. ==
    /// Address of one or more Kafka nodes, comma separated, in the Kafka
    /// cluster to connect to.
    #[structopt(short = "b", long, default_value = "localhost:9092")]
    bootstrap_server: String,
    /// URL of the schema registry to connect to, if using Avro keys or values.
    #[structopt(short = "s", long, default_value = "http://localhost:8081")]
    schema_registry_url: Url,
    /// Topic into which to write records.
    #[structopt(short = "t", long = "topic")]
    topic: String,
    /// Number of records to write.
    #[structopt(short = "n", long = "num-records")]
    num_records: usize,
    /// Number of partitions over which records should be distributed in a
    /// round-robin fashion, regardless of the value of the keys of these
    /// records.
    ///
    /// The default value, 0, indicates that Kafka's default strategy of
    /// distributing writes based upon the hash of their keys should be used
    /// instead.
    #[structopt(long, default_value = "0")]
    partitions_round_robin: usize,

    // == Key arguments. ==
    /// Format in which to generate keys.
    #[structopt(
        short = "k", long = "keys", case_insensitive = true,
        possible_values = &KeyFormat::variants(), default_value = "sequential"
    )]
    key_format: KeyFormat,
    /// Minimum key value to generate, if using random-formatted keys.
    #[structopt(long, required_if("keys", "random"))]
    key_min: Option<u64>,
    /// Maximum key value to generate, if using random-formatted keys.
    #[structopt(long, required_if("keys", "random"))]
    key_max: Option<u64>,
    /// Schema describing Avro key data to randomly generate, if using
    /// Avro-formatted keys.
    #[structopt(long, required_if("keys", "avro"))]
    avro_key_schema: Option<Schema>,
    /// JSON object describing the distribution parameters for each field of
    /// the Avro key object, if using Avro-formatted keys.
    #[structopt(long, required_if("keys", "avro"))]
    avro_key_distribution: Option<serde_json::Value>,

    // == Value arguments. ==
    /// Format in which to generate values.
    #[structopt(
        short = "v", long = "values", case_insensitive = true,
        possible_values = &ValueFormat::variants(), default_value = "bytes"
    )]
    value_format: ValueFormat,
    /// Minimum value size to generate, if using bytes-formatted values.
    #[structopt(
        short = "m",
        long = "min-message-size",
        required_if("value-format", "bytes")
    )]
    min_value_size: Option<usize>,
    /// Maximum value size to generate, if using bytes-formatted values.
    #[structopt(
        short = "M",
        long = "max-message-size",
        required_if("value-format", "bytes")
    )]
    max_value_size: Option<usize>,
    /// Schema describing Avro value data to randomly generate, if using
    /// Avro-formatted values.
    #[structopt(long = "avro-schema", required_if("value-format", "avro"))]
    avro_value_schema: Option<Schema>,
    /// JSON object describing the distribution parameters for each field of
    /// the Avro value object, if using Avro-formatted keys.
    #[structopt(long = "avro-distribution", required_if("value-format", "avro"))]
    avro_value_distribution: Option<serde_json::Value>,

    // == Output control. ==
    /// Suppress printing progress messages.
    #[structopt(short = "q", long)]
    quiet: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = ore::cli::parse_args();

    let mut value_gen = match args.value_format {
        ValueFormat::Bytes => {
            // Clap may one day be able to do this validation automatically.
            // See: https://github.com/clap-rs/clap/discussions/2039
            if args.avro_value_schema.is_some() {
                bail!("cannot specify --avro-schema without --values=avro");
            }
            if args.avro_value_distribution.is_some() {
                bail!("cannot specify --avro-distribution without --values=avro");
            }
            let len =
                Uniform::new_inclusive(args.min_value_size.unwrap(), args.max_value_size.unwrap());
            let bytes = Uniform::new_inclusive(0, 255);
            let rng = thread_rng();

            ValueGenerator::UniformBytes { len, bytes, rng }
        }
        ValueFormat::Avro => {
            // Clap may one day be able to do this validation automatically.
            // See: https://github.com/clap-rs/clap/discussions/2039
            if args.min_value_size.is_some() {
                bail!("cannot specify --min-message-size without --values=bytes");
            }
            if args.max_value_size.is_some() {
                bail!("cannot specify --max-message-size without --values=bytes");
            }
            let value_schema = args.avro_value_schema.as_ref().unwrap();
            let ccsr = ccsr::ClientConfig::new(args.schema_registry_url.clone()).build()?;
            let schema_id = ccsr
                .publish_schema(
                    &format!("{}-value", args.topic),
                    &value_schema.to_string(),
                    ccsr::SchemaType::Avro,
                    &[],
                )
                .await?;
            let generator =
                RandomAvroGenerator::new(value_schema, &args.avro_value_distribution.unwrap());
            ValueGenerator::RandomAvro {
                inner: generator,
                schema: value_schema,
                schema_id,
            }
        }
    };

    let mut key_gen = match args.key_format {
        KeyFormat::Avro => {
            // Clap may one day be able to do this validation automatically.
            // See: https://github.com/clap-rs/clap/discussions/2039
            if args.key_min.is_some() {
                bail!("cannot specify --key-min without --keys=bytes");
            }
            if args.key_max.is_some() {
                bail!("cannot specify --key-max without --keys=bytes");
            }
            let key_schema = args.avro_key_schema.as_ref().unwrap();
            let ccsr = ccsr::ClientConfig::new(args.schema_registry_url).build()?;
            let key_schema_id = ccsr
                .publish_schema(
                    &format!("{}-key", args.topic),
                    &key_schema.to_string(),
                    ccsr::SchemaType::Avro,
                    &[],
                )
                .await?;
            let generator =
                RandomAvroGenerator::new(key_schema, &args.avro_key_distribution.unwrap());
            Some(ValueGenerator::RandomAvro {
                inner: generator,
                schema: key_schema,
                schema_id: key_schema_id,
            })
        }
        _ => {
            // Clap may one day be able to do this validation automatically.
            // See: https://github.com/clap-rs/clap/discussions/2039
            if args.avro_key_schema.is_some() {
                bail!("cannot specify --avro-key-schema without --keys=avro");
            }
            if args.avro_key_distribution.is_some() {
                bail!("cannot specify --avro-key-distribution without --keys=avro");
            }
            None
        }
    };
    let key_dist = if let KeyFormat::Random = args.key_format {
        Some(Uniform::new_inclusive(
            args.key_min.unwrap(),
            args.key_max.unwrap(),
        ))
    } else {
        None
    };

    let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
        .set("bootstrap.servers", args.bootstrap_server.to_string())
        .create()?;
    let mut key_buf = vec![];
    let mut value_buf = vec![];
    let mut rng = thread_rng();
    for i in 0..args.num_records {
        if !args.quiet && i % 10000 == 0 {
            eprintln!("Generating message {}", i);
        }

        value_gen.next_value(&mut value_buf);
        if let Some(key_gen) = key_gen.as_mut() {
            key_gen.next_value(&mut key_buf);
        } else if let Some(key_dist) = key_dist.as_ref() {
            key_buf.clear();
            key_buf.extend(key_dist.sample(&mut rng).to_be_bytes().iter())
        } else {
            key_buf.clear();
            key_buf.extend(u64::cast_from(i).to_be_bytes().iter())
        };

        let mut rec = BaseRecord::to(&args.topic)
            .key(&key_buf)
            .payload(&value_buf);
        if args.partitions_round_robin != 0 {
            rec = rec.partition((i % args.partitions_round_robin) as i32);
        }
        let mut rec = Some(rec);

        Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .retry(|_| match producer.send(rec.take().unwrap()) {
                Ok(()) => Ok(()),
                Err((e @ KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                    rec = Some(r);
                    Err(e)
                }
                Err((e, _)) => Err(e.into()),
            })?;
    }

    producer.flush(Timeout::Never);
    Ok(())
}
