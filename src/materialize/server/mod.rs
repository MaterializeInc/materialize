// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! The implementation of the materialized server.

use futures::future;
use futures::Future;
use log::error;
use std::boxed::Box;
use std::error::Error as StdError;
use std::net::SocketAddr;
use std::sync::mpsc;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use self::error::Error;
use crate::dataflow;
use crate::dataflow::{CommandSender, Dataflow};
use metastore::MetaStore;
use ore::future::FutureExt;
use ore::netio;
use ore::netio::SniffingStream;

mod error;
mod http;
mod pgwire;

pub struct ConnState {
    pub command_tx: CommandSender,
    pub meta_store: MetaStore<Dataflow>,
}

fn handle_connection(tcp_stream: TcpStream, state: ConnState) -> impl Future<Item = (), Error = ()> {
    // Sniff out what protocol we've received. Choosing how many bytes to sniff
    // is a delicate business. Read too many bytes and you'll stall out
    // protocols with small handshakes, like pgwire. Read too few bytes and
    // you won't be able to tell what protocol you have. For now, eight bytes
    // is the magic number, but this may need to change if we learn to speak
    // new protocols.
    let ss = SniffingStream::new(tcp_stream);
    netio::read_exact_or_eof(ss, [0; 8])
        .err_into()
        .and_then(move |(ss, buf, nread)| {
            let buf = &buf[..nread];
            if pgwire::match_handshake(buf) {
                pgwire::handle_connection(ss.into_sniffed(), state)
                    .err_into()
                    .either_a()
            } else if http::match_handshake(buf) {
                http::handle_connection(ss.into_sniffed())
                    .err_into()
                    .either_b()
            } else {
                reject_connection(ss.into_sniffed()).err_into().either_c()
            }
        })
        .map_err(|err: Error| error!("error handling request: {}", err))
}

fn reject_connection<A: AsyncWrite>(a: A) -> impl Future<Item = (), Error = io::Error> {
    io::write_all(a, "unknown protocol\n").discard()
}

/// Start the materialized server.
pub fn serve() -> Result<(), Box<dyn StdError>> {
    let (command_tx, command_rx) = mpsc::channel();

    let _dd_workers = dataflow::serve(command_rx);

    let zookeeper_addr: SocketAddr = "127.0.0.1:2181".parse()?;
    let listen_addr: SocketAddr = "127.0.0.1:6875".parse()?;

    let listener = TcpListener::bind(&listen_addr)?;

    let start = future::lazy(move || {
        let meta_store = MetaStore::new(&zookeeper_addr, "materialized");

        let server = listener.incoming()
            .for_each(move |stream| {
                tokio::spawn(handle_connection(stream, ConnState {
                    command_tx: command_tx.clone(),
                    meta_store: meta_store.clone(),
                }));
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
