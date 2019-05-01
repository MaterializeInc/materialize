// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Main materialized server.

use failure::format_err;
use futures::sync::mpsc::UnboundedSender;
use futures::{future, Future};
use log::error;
use std::boxed::Box;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, RwLock};
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use crate::clock::Clock;
use crate::dataflow;
use crate::dataflow::server::Command;
use crate::dataflow::Dataflow;
use crate::repr::Datum;
use metastore::{DataflowEvent, MetaStore};
use ore::closure;
use ore::future::FutureExt;
use ore::netio;
use ore::netio::SniffingStream;

mod http;
mod pgwire;

pub struct Config {
    pub zookeeper_url: Option<String>,
}

pub struct ServerState {
    pub peek_results: HashMap<uuid::Uuid, (UnboundedSender<Datum>, usize)>,
}

#[derive(Clone)]
pub struct ConnState {
    pub meta_store: MetaStore<Dataflow>,
    pub cmd_tx: dataflow::server::CommandSender,
    pub server_state: Arc<RwLock<ServerState>>,
}

fn handle_connection(
    tcp_stream: TcpStream,
    state: ConnState,
) -> impl Future<Item = (), Error = ()> {
    // Sniff out what protocol we've received. Choosing how many bytes to sniff
    // is a delicate business. Read too many bytes and you'll stall out
    // protocols with small handshakes, like pgwire. Read too few bytes and
    // you won't be able to tell what protocol you have. For now, eight bytes
    // is the magic number, but this may need to change if we learn to speak
    // new protocols.
    let ss = SniffingStream::new(tcp_stream);
    netio::read_exact_or_eof(ss, [0; 8])
        .from_err()
        .and_then(move |(ss, buf, nread)| {
            let buf = &buf[..nread];
            if pgwire::match_handshake(buf) {
                pgwire::handle_connection(ss.into_sniffed(), state).either_a()
            } else if http::match_handshake(buf) {
                http::handle_connection(ss.into_sniffed(), state).either_b()
            } else {
                reject_connection(ss.into_sniffed()).from_err().either_c()
            }
        })
        .map_err(|err| error!("error handling request: {}", err))
}

fn reject_connection<A: AsyncWrite>(a: A) -> impl Future<Item = (), Error = io::Error> {
    io::write_all(a, "unknown protocol\n").discard()
}

/// Start the materialized server.
pub fn serve(config: Config) -> Result<(), Box<dyn StdError>> {
    let zookeeper_url = config
        .zookeeper_url
        .unwrap_or_else(|| "127.0.0.1:2181".into());
    let zookeeper_addr = to_socket_addr(zookeeper_url)?;
    let listen_addr: SocketAddr = "127.0.0.1:6875".parse()?;

    let listener = TcpListener::bind(&listen_addr)?;

    let start = future::lazy(move || {
        let meta_store = MetaStore::new(&zookeeper_addr, "materialized");

        let clock = Clock::new();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _dd_workers = dataflow::server::serve(clock.clone(), cmd_rx);

        let server_state = Arc::new(RwLock::new(ServerState {
            peek_results: HashMap::new(),
        }));

        // TODO(benesch): only pipe the metastore watch to the dataflow
        // on one machine, to avoid duplicating dataflows.
        tokio::spawn(meta_store.register_dataflow_watch().for_each(
            closure!([clone cmd_tx] |event| {
                cmd_tx.send(match event {
                    DataflowEvent::Created(dataflow) => Command::CreateDataflow(dataflow),
                    DataflowEvent::Deleted(name) => Command::DropDataflow(name),
                }).unwrap();
                Ok(())
            }),
        ));

        let server = listener
            .incoming()
            .for_each(move |stream| {
                tokio::spawn(handle_connection(
                    stream,
                    ConnState {
                        meta_store: meta_store.clone(),
                        cmd_tx: cmd_tx.clone(),
                        server_state: server_state.clone(),
                    },
                ));
                Ok(())
            })
            .map_err(|err| error!("error accepting connection: {}", err));
        tokio::spawn(server);

        Ok(())
    });

    println!("materialized listening on {}...", listen_addr);
    tokio::run(start);

    Ok(())
}

fn to_socket_addr<TSA: ToSocketAddrs>(tsa: TSA) -> Result<SocketAddr, failure::Error> {
    let mut addrs = tsa.to_socket_addrs()?;
    addrs
        .next()
        .ok_or_else(|| format_err!("address did not resolve"))
}
