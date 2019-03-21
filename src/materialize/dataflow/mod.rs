// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! The differential dataflow driver.
//!
//! This module is very much a work in progress. Don't look too closely yet.

use differential_dataflow::collection::{AsCollection, Collection};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Data;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::synchronization::Sequencer;
use timely::worker::Worker;

use metastore::MetaStore;

mod types;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Peek(String, uuid::Uuid),
    Tail(String),
}

pub type CommandSender = std::sync::mpsc::Sender<Command>;
pub type CommandReceiver = std::sync::mpsc::Receiver<Command>;

pub use types::*;

/// A trace handle for key-only data.
pub type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;
/// A trace handle for key-value data.
pub type TraceValHandle<K, V, T, R> = TraceAgent<K, V, T, R, OrdValSpine<K, V, T, R>>;
/// A key-only trace handle binding `Time` and `Diff` using `Vec<V>` as data.
pub type KeysOnlyHandle<V> = TraceKeyHandle<Vec<V>, Time, Diff>;
/// A key-value trace handle binding `Time` and `Diff` using `Vec<V>` as data.
pub type KeysValsHandle<V> = TraceValHandle<Vec<V>, Vec<V>, Time, Diff>;

/// System-wide notion of time.
pub type Time = std::time::Duration;

/// System-wide update type.
pub type Diff = isize;

/// Root handles to maintained collections.
///
/// Manages a map from plan (describing a collection)
/// to various arranged forms of that collection.
pub struct TraceManager<Value: Data> {
    inputs: HashMap<String, KeysOnlyHandle<Value>>,
}

impl<Value: Data + Hash> TraceManager<Value> {
    /// Creates a new empty trace manager.
    pub fn new() -> Self {
        Self {
            inputs: HashMap::new(),
        }
    }

    /// Advances the frontier of each maintained trace.
    pub fn advance_time(&mut self, time: &Time) {
        use differential_dataflow::trace::TraceReader;

        let frontier = &[time.clone()];
        for trace in self.inputs.values_mut() {
            trace.advance_by(frontier);
        }
    }

    /// Recover an arrangement by plan and keys, if it is cached.
    pub fn get_unkeyed(&self, name: String) -> Option<KeysOnlyHandle<Value>> {
        self.inputs.get(&name).map(|x| x.clone())
    }

    /// Installs an unkeyed arrangement for a specified plan.
    pub fn set_unkeyed(&mut self, name: String, handle: &KeysOnlyHandle<Value>) {
        use differential_dataflow::trace::TraceReader;
        let mut handle = handle.clone();
        handle.distinguish_since(&[]);
        self.inputs.insert(name.clone(), handle);
    }
}

/// Manages inputs and traces.
pub struct Manager<Value: Data> {
    /// Manages maintained traces.
    pub traces: TraceManager<Value>,
    /// Probes all computations.
    pub probe: ProbeHandle<Time>,
}

impl<Value: Data + Hash> Manager<Value> {
    /// Creates a new empty manager.
    pub fn new() -> Self {
        Manager {
            traces: TraceManager::new(),
            probe: ProbeHandle::new(),
        }
    }

    /// Clear the managed inputs and traces.
    pub fn shutdown(&mut self) {
        self.traces.inputs.clear();
    }

    /// Inserts a new input session by name.
    pub fn insert_input(&mut self, name: String, trace: KeysOnlyHandle<Value>) {
        self.traces.set_unkeyed(name, &trace);
    }

    /// Advances inputs and traces to `time`.
    pub fn advance_time(&mut self, time: &Time) {
        self.traces.advance_time(time);
    }
}

/// Manages input sessions.
pub struct InputManager<Value: Data> {
    /// Input sessions by name.
    pub sessions: HashMap<String, InputSession<Time, Vec<Value>, Diff>>,
}

impl<Value: Data> InputManager<Value> {
    /// Creates a new empty input manager.
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    /// Advances the times of all managed inputs.
    pub fn advance_time(&mut self, time: &Time) {
        for session in self.sessions.values_mut() {
            session.advance_to(time.clone());
            session.flush();
        }
    }
}

use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Capability;
use timely::dataflow::Stream;

use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::Message;

pub fn kafka_source<C, G, D, L>(
    scope: &G,
    name: &str,
    consumer: BaseConsumer<C>,
    logic: L,
) -> Stream<G, D>
where
    C: ConsumerContext + 'static,
    G: Scope<Timestamp = Time>,
    D: Data,
    L: Fn(
            &[u8],
            &mut Capability<G::Timestamp>,
            &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>,
        ) -> bool
        + 'static,
{
    use timely::dataflow::operators::generic::source;
    source(scope, name, move |capability, info| {
        let activator = scope.activator_for(&info.address[..]);
        let mut cap = Some(capability);

        let then = std::time::Instant::now();

        // define a closure to call repeatedly.
        move |output| {
            // Act only if we retain the capability to send data.
            let mut complete = false;
            if let Some(capability) = cap.as_mut() {
                // Indicate that we should run again.
                activator.activate();

                // Repeatedly interrogate Kafka for [u8] messages.
                // Cease only when Kafka stops returning new data.
                // Could cease earlier, if we had a better policy.
                while let Some(result) = consumer.poll(std::time::Duration::from_millis(0)) {
                    // If valid data back from Kafka
                    if let Ok(message) = result {
                        // Attempt to interpret bytes as utf8  ...
                        if let Some(payload) = message.payload() {
                            complete = logic(payload, capability, output) || complete;
                        }
                    } else {
                        println!("Kafka error");
                    }
                }
                // We need some rule to advance timestamps ...
                capability.downgrade(&then.elapsed());
            }

            if complete {
                cap = None;
            }
        }
    })
}

fn build_dataflow<A: Allocate>(
    dataflow: Dataflow,
    manager: &mut Manager<Scalar>,
    worker: &mut Worker<A>,
) {
    worker.dataflow::<Time, _, _>(|scope| {
        match dataflow {
            Dataflow::Source(src) => {
                let (topic, addr) = match src.connector {
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

                let then = std::time::Instant::now();
                let schema = src.schema.to_avro();

                let arrangement =
                    kafka_source(scope, &src.name, consumer, move |mut bytes, cap, output| {
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
                                        avro_rs::types::Value::Long(i) => Scalar::Int(i),
                                        avro_rs::types::Value::String(s) => Scalar::String(s),
                                        _ => panic!("avro deserialization went wrong"),
                                    })
                                }
                            }
                            _ => panic!("avro deserialization went wrong"),
                        }
                        let time = cap.time().clone();
                        output.session(cap).give((row, time, 1));

                        // Indicate that we are not yet done.
                        false
                    })
                    .as_collection()
                    .arrange_by_self();
                manager.insert_input(src.name, arrangement.trace);
            }
            Dataflow::View(view) => {
                let arrangement = build_plan(&view.plan, manager, scope).arrange_by_self();
                manager.insert_input(view.name, arrangement.trace);
            }
        }
    })
}

fn build_plan<S: Scope<Timestamp = Time>>(
    plan: &Plan,
    manager: &mut Manager<Scalar>,
    scope: &mut S,
) -> Collection<S, Vec<Scalar>, Diff> {
    match plan {
        Plan::Source(name) => manager
            .traces
            .get_unkeyed(name.to_owned())
            .unwrap()
            .import(scope)
            .as_collection(|k, ()| k.to_vec()),
        Plan::Project { outputs, input } => {
            let outputs = outputs.clone();
            build_plan(&input, manager, scope).map(move |tuple| {
                outputs
                    .iter()
                    .map(|expr| match expr {
                        Expr::Column(i) => tuple[*i].clone(),
                        Expr::Literal(s) => s.clone(),
                    })
                    .collect()
            })
        }
        Plan::Distinct(_) => unimplemented!(),
        Plan::UnionAll(_) => unimplemented!(),
        Plan::Join { .. } => unimplemented!(),
    }
}

pub fn serve(
    meta_store: MetaStore<types::Dataflow>,
    cmd_rx: CommandReceiver,
) -> Result<WorkerGuards<()>, String> {
    let cmd_rx = std::sync::Mutex::new(cmd_rx);

    timely::execute(timely::Configuration::Process(4), move |worker| {
        let cmd_rx = if worker.index() == 0 {
            Some(cmd_rx.lock().unwrap())
        } else {
            None
        };

        let dataflow_channel = meta_store.register_dataflow_watch();

        let mut sequencer = Sequencer::new(worker, std::time::Instant::now());

        let mut manager = Manager::new();
        loop {
            while let Ok(dataflow) = dataflow_channel.try_recv() {
                build_dataflow(dataflow, &mut manager, worker);
            }
            if let Some(ref cmd_rx) = cmd_rx {
                while let Ok(cmd) = cmd_rx.try_recv() {
                    sequencer.push(cmd);
                }
            }
            while let Some(cmd) = sequencer.next() {
                match cmd {
                    Command::Peek(name, uuid) => {
                        if let Some(mut trace) = manager.traces.get_unkeyed(name.clone()) {
                            let (mut cur, storage) = trace.cursor();
                            let mut out = Vec::new();
                            while cur.key_valid(&storage) {
                                out.push(cur.key(&storage));
                                cur.step_key(&storage)
                            }
                            let encoded = bincode::serialize(&out).unwrap();
                            let client = reqwest::Client::new();
                            client
                                .post("http://localhost:6875/api/peek-results")
                                .header("X-Materialize-Query-UUID", uuid.to_string())
                                .body(encoded)
                                .send()
                                .unwrap();
                        } else {
                            println!("no trace named {}", name);
                        }
                    }
                    Command::Tail(name) => unimplemented!(),
                }
            }
            worker.step();
        }
    })
}
