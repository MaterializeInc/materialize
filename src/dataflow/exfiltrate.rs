// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;

use crate::Update;
use ore::mpmc::Mux;
use repr::Datum;

/// Options for how dataflow results return to those that posed the queries.
#[derive(Clone, Debug)]
pub enum ExfiltratorConfig {
    /// A local exchange fabric that backed by inter-thread channels.
    Local(Mux<Exfiltration>),
    /// An address to send results to via the network.
    Remote(String),
}

/// A batch of data exfiltrated from the dataflow layer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Exfiltration {
    /// The complete result of a `DataflowCommand::Peek` from one worker.
    Peek(Vec<Vec<Datum>>),
    /// A chunk of updates from a tail sink.
    Tail(Vec<Update>),
}

impl Exfiltration {
    pub fn unwrap_peek(self) -> Vec<Vec<Datum>> {
        match self {
            Exfiltration::Peek(v) => v,
            _ => panic!("Exfiltration::unwrap_peeked called on a {:?} variant", self),
        }
    }

    pub fn unwrap_tail(self) -> Vec<Update> {
        match self {
            Exfiltration::Tail(v) => v,
            _ => panic!("Exfiltration::unwrap_tail called on a {:?} variant", self),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Exfiltrator {
    Local(Mux<Exfiltration>),
    Remote(Rc<RpcClient>),
}

impl Exfiltrator {
    pub fn send_peek(&self, connection_uuid: Uuid, rows: Vec<Vec<Datum>>) {
        let exfiltration = Exfiltration::Peek(rows);
        match self {
            Exfiltrator::Local(mux) => {
                // The sender is allowed disappear at any time, so the
                // error handling here is deliberately relaxed.
                let mux = mux.read().unwrap();
                if let Ok(sender) = mux.sender(&connection_uuid) {
                    drop(sender.unbounded_send(exfiltration))
                }
            }
            Exfiltrator::Remote(rpc_client) => {
                rpc_client.post(connection_uuid, exfiltration);
            }
        }
    }

    pub fn send_tail(&self, connection_uuid: Uuid, updates: Vec<Update>) {
        let exfiltration = Exfiltration::Tail(updates);
        match self {
            Exfiltrator::Local(_) => unimplemented!(),
            Exfiltrator::Remote(rpc_client) => {
                rpc_client.post(connection_uuid, exfiltration);
            }
        }
    }
}

impl From<ExfiltratorConfig> for Exfiltrator {
    fn from(c: ExfiltratorConfig) -> Exfiltrator {
        match c {
            ExfiltratorConfig::Local(mux) => Exfiltrator::Local(mux),
            ExfiltratorConfig::Remote(addr) => Exfiltrator::Remote(Rc::new(RpcClient::new(addr))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RpcClient {
    inner: RefCell<reqwest::Client>,
    addr: String,
}

impl RpcClient {
    fn new(addr: String) -> RpcClient {
        let inner = reqwest::Client::builder()
            .tcp_nodelay()
            .build()
            .expect("building reqwest client failed");

        RpcClient {
            inner: RefCell::new(inner),
            addr,
        }
    }

    fn post(&self, connection_uuid: Uuid, exfiltration: Exfiltration) {
        let encoded = bincode::serialize(&exfiltration).unwrap();
        self.inner
            .borrow_mut()
            .post(&self.addr)
            .header("X-Materialize-Query-UUID", connection_uuid.to_string())
            .body(encoded)
            .send()
            .unwrap();
    }
}
