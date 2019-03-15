// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! The differential dataflow driver.
//!
//! This module is very much a work in progress. Don't look too closely yet.

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::Data;
use serde_derive::{Deserialize, Serialize};
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::{SQLQuery, SQLSetExpr, SQLStatement, TableFactor};
use sqlparser::sqlparser::{Parser, ParserError};
use std::collections::HashMap;
use std::hash::Hash;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::synchronization::Sequencer;
use timely::worker::Worker;

mod types;

pub use types::*;

pub type CommandSender = std::sync::mpsc::Sender<Command<Value>>;
pub type CommandReceiver = std::sync::mpsc::Receiver<Command<Value>>;

/// A trace handle for key-only data.
pub type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;
/// A trace handle for key-value data.
pub type TraceValHandle<K, V, T, R> = TraceAgent<K, V, T, R, OrdValSpine<K, V, T, R>>;
/// A key-only trace handle binding `Time` and `Diff` using `Vec<V>` as data.
pub type KeysOnlyHandle<V> = TraceKeyHandle<Vec<V>, Time, Diff>;
/// A key-value trace handle binding `Time` and `Diff` using `Vec<V>` as data.
pub type KeysValsHandle<V> = TraceValHandle<Vec<V>, Vec<V>, Time, Diff>;

/// System-wide notion of time.
pub type Time = ::std::time::Duration;

/// System-wide update type.
pub type Diff = isize;

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Value {
    Bool(bool),
    Usize(usize),
    String(String),
    Address(Vec<usize>),
    Duration(::std::time::Duration),
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Command<Value> {
    CreateQuery { name: String, query: String },
    NotImplementedYet(Value),
}

impl<Value: Data + Hash> Command<Value> {
    pub fn execute<A: Allocate>(self, manager: &mut Manager<Value>, worker: &mut Worker<A>) {
        match self {
            Command::NotImplementedYet(_) => {}
            Command::CreateQuery { query, .. } => {
                worker.dataflow::<Time, _, _>(|scope| {
                    manager
                        .build_dataflow(parse_query(&query).unwrap(), scope)
                        .unwrap();
                });
            }
        }
    }
}

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
        println!("Setting unkeyed: {:?}", name);

        use differential_dataflow::trace::TraceReader;
        let mut handle = handle.clone();
        handle.distinguish_since(&[]);
        self.inputs.insert(name.clone(), handle);
    }
}

/// Manages inputs and traces.
pub struct Manager<Value: Data> {
    /// Manages input sessions.
    pub inputs: InputManager<Value>,
    /// Manages maintained traces.
    pub traces: TraceManager<Value>,
    /// Probes all computations.
    pub probe: ProbeHandle<Time>,
}

impl<Value: Data + Hash> Manager<Value> {
    /// Creates a new empty manager.
    pub fn new() -> Self {
        Manager {
            inputs: InputManager::new(),
            traces: TraceManager::new(),
            probe: ProbeHandle::new(),
        }
    }

    /// Clear the managed inputs and traces.
    pub fn shutdown(&mut self) {
        self.inputs.sessions.clear();
        self.traces.inputs.clear();
    }

    /// Inserts a new input session by name.
    pub fn insert_input(
        &mut self,
        name: String,
        input: InputSession<Time, Vec<Value>, Diff>,
        trace: KeysOnlyHandle<Value>,
    ) {
        self.inputs.sessions.insert(name.clone(), input);
        self.traces.set_unkeyed(name, &trace);
    }

    /// Advances inputs and traces to `time`.
    pub fn advance_time(&mut self, time: &Time) {
        self.inputs.advance_time(time);
        self.traces.advance_time(time);
    }

    pub fn build_dataflow<S: Scope>(
        &mut self,
        stmt: SQLStatement,
        scope: &mut S,
    ) -> Result<(), String>
    where
        S: Scope<Timestamp = Time>,
    {
        match stmt {
            SQLStatement::SQLCreateView {
                query,
                materialized: true,
                ..
            } => self.build_dataflow_expr(query, scope),
            SQLStatement::SQLCreateView {
                materialized: false,
                ..
            }
            | _ => Err("only CREATE MATERIALIZED VIEW AS allowed".to_string()),
        }
    }

    pub fn build_dataflow_expr<S: Scope>(
        &mut self,
        query: SQLQuery,
        scope: &mut S,
    ) -> Result<(), String>
    where
        S: Scope<Timestamp = Time>,
    {
        if let SQLSetExpr::Select(select) = query.body {
            match select.relation {
                Some(TableFactor::Table { name, .. }) => {
                    self.traces
                        .get_unkeyed(name.to_string())
                        .expect(&format!("Failed to find source collection: {:?}", name))
                        .import(scope)
                        .as_collection(|k, ()| k.to_vec())
                        .inspect(|x| println!("in collection {:?}", x));
                }
                Some(TableFactor::Derived { .. }) => {
                    return Err("nested subqueries are not yet supported".to_string());
                }
                None => {
                    // https://en.wikipedia.org/wiki/DUAL_table
                    vec!['X'].to_stream(scope);
                }
            }
            Ok(())
        } else {
            Err("set operations are not yet supported".to_string())
        }
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

pub fn serve(command_rx: CommandReceiver) -> Result<WorkerGuards<()>, String> {
    let mutex = std::sync::Mutex::new(command_rx);
    timely::execute(timely::Configuration::Process(4), move |worker| {
        let guard = if worker.index() == 0 {
            Some(mutex.lock().unwrap())
        } else {
            None
        };

        let timer = ::std::time::Instant::now();
        let mut manager = Manager::new();
        let mut sequencer: Sequencer<Command<Value>> = Sequencer::new(worker, timer);

        use differential_dataflow::input::Input;
        use differential_dataflow::operators::arrange::ArrangeBySelf;

        let records: Vec<Vec<Value>> = (0..100).map(|i| vec![Value::Usize(i)]).collect();

        let (input, trace) = worker.dataflow::<Time, _, _>(|scope| {
            let (input, collection) = scope.new_collection_from(records);
            let trace = collection.arrange_by_self().trace;
            (input, trace)
        });

        manager.insert_input("foo".to_string(), input, trace);

        let then = std::time::Instant::now();
        let mut i = 0;
        loop {
            let now = then.elapsed();
            if i % 10000 == 0 {
                println!("advancing time to now {:?}", now);
                i += 1;
            }
            manager.advance_time(&now);
            if let Some(ref command_rx) = guard {
                if let Ok(cmd) = command_rx.try_recv() {
                    println!("recv cmd {:?}", cmd);
                    sequencer.push(cmd);
                }
            }
            if let Some(command) = sequencer.next() {
                println!("exec cmd {:?}", command);
                command.execute(&mut manager, worker);
            }
            std::thread::sleep(std::time::Duration::from_millis(10)); // XXX
            worker.step();
        }
    })
}

pub fn parse_query(q: &str) -> Result<SQLStatement, ParserError> {
    let dialect = AnsiSqlDialect {};
    Ok(Parser::parse_sql(&dialect, q.to_owned())?.remove(0))
}
