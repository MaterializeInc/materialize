// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::iter;
use std::ops::Add;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::bail;
use chrono::{NaiveDate, NaiveDateTime};
use crossbeam::thread;
use rand::distributions::{
    uniform::SampleUniform, Alphanumeric, Bernoulli, Uniform, WeightedIndex,
};
use rand::prelude::{Distribution, ThreadRng};
use rand::thread_rng;
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use serde_json::Map;
use url::Url;

use mz_avro::schema::{SchemaNode, SchemaPiece, SchemaPieceOrNamed};
use mz_avro::types::{DecimalValue, Value};
use mz_avro::Schema;
use mz_ore::cast::CastFrom;
use mz_ore::cli::{self, CliConfig};
use mz_ore::retry::Retry;

trait Generator<R>: FnMut(&mut ThreadRng) -> R + Send + Sync {
    fn clone_box(&self) -> Box<dyn Generator<R>>;
}

impl<F, R> Generator<R> for F
where
    F: FnMut(&mut ThreadRng) -> R + Clone + Send + Sync + 'static,
{
    fn clone_box(&self) -> Box<dyn Generator<R>> {
        Box::new(self.clone())
    }
}

impl<R> Clone for Box<dyn Generator<R>>
where
    R: 'static,
{
    fn clone(&self) -> Box<dyn Generator<R>> {
        (**self).clone_box()
    }
}

#[derive(Clone)]
struct RandomAvroGenerator<'a> {
    // Generator functions for each piece of the schema. These map keys are
    // morally `*const SchemaPiece`s, but represented as `usize`s so that they
    // implement `Send`.
    ints: HashMap<usize, Box<dyn Generator<i32>>>,
    longs: HashMap<usize, Box<dyn Generator<i64>>>,
    strings: HashMap<usize, Box<dyn Generator<Vec<u8>>>>,
    bytes: HashMap<usize, Box<dyn Generator<Vec<u8>>>>,
    unions: HashMap<usize, Box<dyn Generator<usize>>>,
    enums: HashMap<usize, Box<dyn Generator<usize>>>,
    bools: HashMap<usize, Box<dyn Generator<bool>>>,
    floats: HashMap<usize, Box<dyn Generator<f32>>>,
    doubles: HashMap<usize, Box<dyn Generator<f64>>>,
    decimals: HashMap<usize, Box<dyn Generator<Vec<u8>>>>,
    array_lens: HashMap<usize, Box<dyn Generator<usize>>>,

    schema: SchemaNode<'a>,
}

impl<'a> RandomAvroGenerator<'a> {
    fn gen_inner(&mut self, node: SchemaNode, rng: &mut ThreadRng) -> Value {
        let p = &*node.inner as *const _ as usize;
        match node.inner {
            SchemaPiece::Null => Value::Null,
            SchemaPiece::Boolean => {
                let val = self.bools.get_mut(&p).unwrap()(rng);
                Value::Boolean(val)
            }
            SchemaPiece::Int => {
                let val = self.ints.get_mut(&p).unwrap()(rng);
                Value::Int(val)
            }
            SchemaPiece::Long => {
                let val = self.longs.get_mut(&p).unwrap()(rng);
                Value::Long(val)
            }
            SchemaPiece::Float => {
                let val = self.floats.get_mut(&p).unwrap()(rng);
                Value::Float(val)
            }
            SchemaPiece::Double => {
                let val = self.doubles.get_mut(&p).unwrap()(rng);
                Value::Double(val)
            }
            SchemaPiece::Date => {
                let days = self.ints.get_mut(&p).unwrap()(rng);
                let val = NaiveDate::from_ymd(1970, 1, 1).add(chrono::Duration::days(days as i64));
                Value::Date(val)
            }
            SchemaPiece::TimestampMilli => {
                let millis = self.longs.get_mut(&p).unwrap()(rng);

                let seconds = millis / 1000;
                let fraction = (millis % 1000) as u32;
                let val = NaiveDateTime::from_timestamp_opt(seconds, fraction * 1_000_000).unwrap();
                Value::Timestamp(val)
            }
            SchemaPiece::TimestampMicro => {
                let micros = self.longs.get_mut(&p).unwrap()(rng);

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
                let unscaled = self.decimals.get_mut(&p).unwrap()(rng);
                Value::Decimal(DecimalValue {
                    unscaled,
                    precision: *precision,
                    scale: *scale,
                })
            }
            SchemaPiece::Bytes => {
                let val = self.bytes.get_mut(&p).unwrap()(rng);
                Value::Bytes(val)
            }
            SchemaPiece::String => {
                let buf = self.strings.get_mut(&p).unwrap()(rng);
                let val = String::from_utf8(buf).unwrap();
                Value::String(val)
            }
            SchemaPiece::Json => unreachable!(),
            SchemaPiece::Uuid => unreachable!(),
            SchemaPiece::Array(inner) => {
                let len = self.array_lens.get_mut(&p).unwrap()(rng);
                let next = node.step(&**inner);
                let inner_vals = (0..len).map(move |_| self.gen_inner(next, rng)).collect();
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
                let index = self.unions.get_mut(&p).unwrap()(rng);
                let next = node.step(&us.variants()[index]);
                let null_variant = us
                    .variants()
                    .iter()
                    .position(|v| v == &SchemaPieceOrNamed::Piece(SchemaPiece::Null));
                let inner = Box::new(self.gen_inner(next, rng));
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
                        let v = self.gen_inner(next, rng);
                        (k, v)
                    })
                    .collect();
                Value::Record(fields)
            }
            SchemaPiece::Enum { symbols, .. } => {
                let i = self.enums.get_mut(&p).unwrap()(rng);
                Value::Enum(i, symbols[i].clone())
            }
            SchemaPiece::Fixed { size: _ } => unreachable!(),
        }
    }
    pub fn gen(&mut self, rng: &mut ThreadRng) -> Value {
        self.gen_inner(self.schema, rng)
    }
    fn new_inner(
        &mut self,
        node: SchemaNode<'a>,
        annotations: &Map<String, serde_json::Value>,
        field_name: Option<&str>,
    ) {
        fn bool_dist(json: &serde_json::Value) -> impl FnMut(&mut ThreadRng) -> bool + Clone {
            let x = json.as_f64().unwrap();
            let dist = Bernoulli::new(x).unwrap();
            move |rng| dist.sample(rng)
        }
        fn integral_dist<T>(json: &serde_json::Value) -> impl FnMut(&mut ThreadRng) -> T + Clone
        where
            T: SampleUniform + TryFrom<i64> + Clone,
            T::Sampler: Clone,
            <T as TryFrom<i64>>::Error: std::fmt::Debug,
        {
            let x = json.as_array().unwrap();
            let (min, max): (T, T) = (
                x[0].as_i64().unwrap().try_into().unwrap(),
                x[1].as_i64().unwrap().try_into().unwrap(),
            );
            let dist = Uniform::new_inclusive(min, max);
            move |rng| dist.sample(rng)
        }
        fn float_dist(json: &serde_json::Value) -> impl FnMut(&mut ThreadRng) -> f32 + Clone {
            let x = json.as_array().unwrap();
            let (min, max) = (x[0].as_f64().unwrap() as f32, x[1].as_f64().unwrap() as f32);
            let dist = Uniform::new_inclusive(min, max);
            move |rng| dist.sample(rng)
        }
        fn double_dist(json: &serde_json::Value) -> impl FnMut(&mut ThreadRng) -> f64 + Clone {
            let x = json.as_array().unwrap();
            let (min, max) = (x[0].as_f64().unwrap(), x[1].as_f64().unwrap());
            let dist = Uniform::new_inclusive(min, max);
            move |rng| dist.sample(rng)
        }
        fn string_dist(json: &serde_json::Value) -> impl FnMut(&mut ThreadRng) -> Vec<u8> + Clone {
            let mut len = integral_dist::<usize>(json);
            move |rng| {
                let len = len(rng);
                let cd = Alphanumeric;
                iter::repeat_with(|| cd.sample(rng) as u8)
                    .take(len)
                    .collect()
            }
        }
        fn bytes_dist(json: &serde_json::Value) -> impl FnMut(&mut ThreadRng) -> Vec<u8> + Clone {
            let mut len = integral_dist::<usize>(json);
            move |rng| {
                let len = len(rng);
                let bd = Uniform::new_inclusive(0, 255);
                iter::repeat_with(|| bd.sample(rng)).take(len).collect()
            }
        }
        fn decimal_dist(
            json: &serde_json::Value,
            precision: usize,
        ) -> impl FnMut(&mut ThreadRng) -> Vec<u8> + Clone {
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
            move |rng| dist.sample(rng).to_be_bytes().to_vec()
        }
        let p = &*node.inner as *const _ as usize;

        let dist_json = field_name.and_then(|fn_| annotations.get(fn_));
        let err = format!(
            "Distribution annotation not found: {}",
            field_name.unwrap_or("(None)")
        );
        match node.inner {
            SchemaPiece::Null => {}
            SchemaPiece::Boolean => {
                let dist = bool_dist(dist_json.expect(&err));
                self.bools.insert(p, Box::new(dist));
            }
            SchemaPiece::Int => {
                let dist = integral_dist(dist_json.expect(&err));
                self.ints.insert(p, Box::new(dist));
            }
            SchemaPiece::Long => {
                let dist = integral_dist(dist_json.expect(&err));
                self.longs.insert(p, Box::new(dist));
            }
            SchemaPiece::Float => {
                let dist = float_dist(dist_json.expect(&err));
                self.floats.insert(p, Box::new(dist));
            }
            SchemaPiece::Double => {
                let dist = double_dist(dist_json.expect(&err));
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
                let dist = decimal_dist(dist_json.expect(&err), *precision);
                self.decimals.insert(p, Box::new(dist));
            }
            SchemaPiece::Bytes => {
                let len_dist_json = annotations
                    .get(&format!("{}.len", field_name.unwrap()))
                    .unwrap();
                let dist = bytes_dist(len_dist_json);
                self.bytes.insert(p, Box::new(dist));
            }
            SchemaPiece::String => {
                let len_dist_json = annotations
                    .get(&format!("{}.len", field_name.unwrap()))
                    .unwrap();
                let dist = string_dist(len_dist_json);
                self.strings.insert(p, Box::new(dist));
            }
            SchemaPiece::Json => unimplemented!(),
            SchemaPiece::Uuid => unimplemented!(),
            SchemaPiece::Array(inner) => {
                let fn_ = field_name.unwrap();
                let len_dist_json = annotations.get(&format!("{}.len", fn_)).unwrap();
                let len = integral_dist::<usize>(len_dist_json);
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
                let f = move |rng: &mut ThreadRng| dist.sample(rng);
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
            schema: schema.top_node(),
        };
        self_.new_inner(schema.top_node(), annotations.as_object().unwrap(), None);
        self_
    }
}

#[derive(Clone)]
enum ValueGenerator<'a> {
    UniformBytes {
        len: Uniform<usize>,
        bytes: Uniform<u8>,
    },
    RandomAvro {
        inner: RandomAvroGenerator<'a>,
        schema: &'a Schema,
        schema_id: i32,
    },
}

impl<'a> ValueGenerator<'a> {
    pub fn next_value(&mut self, out: &mut Vec<u8>, rng: &mut ThreadRng) {
        match self {
            ValueGenerator::UniformBytes { len, bytes } => {
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
                let value = inner.gen(rng);
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

#[derive(clap::ArgEnum, PartialEq, Debug, Clone)]
pub enum KeyFormat {
    Avro,
    Random,
    Sequential,
}

#[derive(clap::ArgEnum, PartialEq, Debug, Clone)]
pub enum ValueFormat {
    Bytes,
    Avro,
}

/// Write random data to Kafka.
#[derive(clap::Parser)]
struct Args {
    // == Kafka configuration arguments. ==
    /// Address of one or more Kafka nodes, comma separated, in the Kafka
    /// cluster to connect to.
    #[clap(short = 'b', long, default_value = "localhost:9092")]
    bootstrap_server: String,
    /// URL of the schema registry to connect to, if using Avro keys or values.
    #[clap(short = 's', long, default_value = "http://localhost:8081")]
    schema_registry_url: Url,
    /// Topic into which to write records.
    #[clap(short = 't', long = "topic")]
    topic: String,
    /// Number of records to write.
    #[clap(short = 'n', long = "num-records")]
    num_records: usize,
    /// Number of partitions over which records should be distributed in a
    /// round-robin fashion, regardless of the value of the keys of these
    /// records.
    ///
    /// The default value, 0, indicates that Kafka's default strategy of
    /// distributing writes based upon the hash of their keys should be used
    /// instead.
    #[clap(long, default_value = "0")]
    partitions_round_robin: usize,
    /// The number of threads to use.
    ///
    /// If zero, uses the number of physical CPUs on the machine.
    #[structopt(long, default_value = "0")]
    threads: usize,

    // == Key arguments. ==
    /// Format in which to generate keys.
    #[clap(
        short = 'k',
        long = "keys",
        ignore_case = true,
        arg_enum,
        default_value = "sequential"
    )]
    key_format: KeyFormat,
    /// Minimum key value to generate, if using random-formatted keys.
    #[clap(long, required_if_eq("key-format", "random"))]
    key_min: Option<u64>,
    /// Maximum key value to generate, if using random-formatted keys.
    #[clap(long, required_if_eq("key-format", "random"))]
    key_max: Option<u64>,
    /// Schema describing Avro key data to randomly generate, if using
    /// Avro-formatted keys.
    #[clap(long, required_if_eq("key-format", "avro"))]
    avro_key_schema: Option<Schema>,
    /// JSON object describing the distribution parameters for each field of
    /// the Avro key object, if using Avro-formatted keys.
    #[clap(long, required_if_eq("key-format", "avro"))]
    avro_key_distribution: Option<serde_json::Value>,

    // == Value arguments. ==
    /// Format in which to generate values.
    #[clap(
        short = 'v',
        long = "values",
        ignore_case = true,
        arg_enum,
        default_value = "bytes"
    )]
    value_format: ValueFormat,
    /// Minimum value size to generate, if using bytes-formatted values.
    #[clap(
        short = 'm',
        long = "min-message-size",
        required_if_eq("value-format", "bytes")
    )]
    min_value_size: Option<usize>,
    /// Maximum value size to generate, if using bytes-formatted values.
    #[clap(
        short = 'M',
        long = "max-message-size",
        required_if_eq("value-format", "bytes")
    )]
    max_value_size: Option<usize>,
    /// Schema describing Avro value data to randomly generate, if using
    /// Avro-formatted values.
    #[clap(long = "avro-schema", required_if_eq("value-format", "avro"))]
    avro_value_schema: Option<Schema>,
    /// JSON object describing the distribution parameters for each field of
    /// the Avro value object, if using Avro-formatted keys.
    #[clap(long = "avro-distribution", required_if_eq("value-format", "avro"))]
    avro_value_distribution: Option<serde_json::Value>,

    // == Output control. ==
    /// Suppress printing progress messages.
    #[clap(short = 'q', long)]
    quiet: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Args = cli::parse_args(CliConfig::default());

    let value_gen = match args.value_format {
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

            ValueGenerator::UniformBytes { len, bytes }
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
            let ccsr = mz_ccsr::ClientConfig::new(args.schema_registry_url.clone()).build()?;
            let schema_id = ccsr
                .publish_schema(
                    &format!("{}-value", args.topic),
                    &value_schema.to_string(),
                    mz_ccsr::SchemaType::Avro,
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

    let key_gen = match args.key_format {
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
            let ccsr = mz_ccsr::ClientConfig::new(args.schema_registry_url).build()?;
            let key_schema_id = ccsr
                .publish_schema(
                    &format!("{}-key", args.topic),
                    &key_schema.to_string(),
                    mz_ccsr::SchemaType::Avro,
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

    let threads = if args.threads == 0 {
        num_cpus::get_physical()
    } else {
        args.threads
    };
    println!("Using {} threads...", threads);

    let counter = AtomicUsize::new(0);
    thread::scope(|scope| {
        for thread in 0..threads {
            let counter = &counter;
            let topic = &args.topic;
            let mut key_gen = key_gen.clone();
            let mut value_gen = value_gen.clone();
            let producer: ThreadedProducer<mz_kafka_util::client::MzClientContext> =
                mz_kafka_util::client::create_new_client_config_simple()
                    .set("bootstrap.servers", args.bootstrap_server.to_string())
                    .create_with_context(mz_kafka_util::client::MzClientContext)
                    .unwrap();
            let mut key_buf = vec![];
            let mut value_buf = vec![];
            let mut n = args.num_records / threads;
            if thread < args.num_records % threads {
                n += 1;
            }
            scope.spawn(move |_| {
                let mut rng = thread_rng();
                for _ in 0..n {
                    let i = counter.fetch_add(1, Ordering::Relaxed);
                    if !args.quiet && i % 100_000 == 0 {
                        eprintln!("Generating message {}", i);
                    }
                    value_gen.next_value(&mut value_buf, &mut rng);
                    if let Some(key_gen) = key_gen.as_mut() {
                        key_gen.next_value(&mut key_buf, &mut rng);
                    } else if let Some(key_dist) = key_dist.as_ref() {
                        key_buf.clear();
                        key_buf.extend(key_dist.sample(&mut rng).to_be_bytes().iter())
                    } else {
                        key_buf.clear();
                        key_buf.extend(u64::cast_from(i).to_be_bytes().iter())
                    };

                    let mut rec = BaseRecord::to(&topic).key(&key_buf).payload(&value_buf);
                    if args.partitions_round_robin != 0 {
                        rec = rec.partition((i % args.partitions_round_robin) as i32);
                    }
                    let mut rec = Some(rec);

                    Retry::default()
                        .clamp_backoff(Duration::from_secs(1))
                        .retry(|_| match producer.send(rec.take().unwrap()) {
                            Ok(()) => Ok(()),
                            Err((
                                e @ KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
                                r,
                            )) => {
                                rec = Some(r);
                                Err(e)
                            }
                            Err((e, _)) => panic!("unexpected Kafka error: {}", e),
                        })
                        .expect("unable to produce to Kafka");
                }
                producer.flush(Timeout::Never).unwrap();
            });
        }
    })
    .unwrap();

    Ok(())
}
