// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::cell::RefCell;
use std::rc::Rc;

use dataflow_types::{Exfiltration, Update};
use ore::mpmc::Mux;
use repr::Datum;

/// Options for how dataflow results return to those that posed the queries.
#[derive(Clone, Debug)]
pub enum ExfiltratorConfig {
    /// A local exchange fabric that backed by inter-thread channels.
    Local(Mux<u32, Exfiltration>),
    /// An address to send results to via the network.
    Remote(String),
}

#[derive(Clone, Debug)]
pub enum Exfiltrator {
    Local(Mux<u32, Exfiltration>),
    Remote(Rc<RpcClient>),
}

impl Exfiltrator {
    pub fn send_peek(&self, conn_id: u32, rows: Vec<Vec<Datum>>) {
        let exfiltration = Exfiltration::Peek(rows);
        match self {
            Exfiltrator::Local(mux) => {
                // The sender is allowed to disappear at any time, so the error
                // handling here is deliberately relaxed.
                let mux = mux.read().unwrap();
                if let Ok(sender) = mux.sender(&conn_id) {
                    drop(sender.unbounded_send(exfiltration))
                }
            }
            Exfiltrator::Remote(rpc_client) => {
                rpc_client.post(conn_id, exfiltration);
            }
        }
    }

    pub fn send_tail(&self, conn_id: u32, updates: Vec<Update>) {
        let exfiltration = Exfiltration::Tail(updates);
        match self {
            Exfiltrator::Local(_) => unimplemented!(),
            Exfiltrator::Remote(rpc_client) => {
                rpc_client.post(conn_id, exfiltration);
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

    fn post(&self, conn_id: u32, exfiltration: Exfiltration) {
        let encoded = bincode::serialize(&exfiltration).unwrap();
        self.inner
            .borrow_mut()
            .post(&self.addr)
            .header("X-Materialize-Connection-Id", conn_id.to_string())
            .body(encoded)
            .send()
            .unwrap();
    }
}
