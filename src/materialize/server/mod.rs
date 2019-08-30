// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Main materialized server.

use failure::format_err;
use futures::sync::mpsc::{self, UnboundedSender};
use futures::Future;
use log::error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use crate::pgwire;
use crate::queue;
use comm::Switchboard;
use dataflow::{self, DataflowCommand};
use dataflow_types::logging::LoggingConfig;
use ore::collections::CollectionExt;
use ore::future::FutureExt;
use ore::mpmc::Mux;
use ore::netio;
use ore::netio::{SniffedStream, SniffingStream};

mod http;

pub struct Config {
    pub logging_granularity: Option<Duration>,
    pub timely: timely::Configuration,
}

impl Config {
    /// The number of timely workers described the by the configuration.
    pub fn num_timely_workers(&self) -> usize {
        match &self.timely {
            timely::Configuration::Thread => 1,
            timely::Configuration::Process(n) => *n,
            timely::Configuration::Cluster {
                threads, addresses, ..
            } => threads * addresses.len(),
        }
    }
}

fn handle_connection(
    tcp_stream: TcpStream,
    switchboard: Switchboard<SniffedStream<TcpStream>>,
    cmdq_tx: UnboundedSender<queue::Command>,
    num_timely_workers: usize,
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
                pgwire::serve(ss.into_sniffed(), cmdq_tx, num_timely_workers).boxed()
            } else if http::match_handshake(buf) {
                http::handle_connection(ss.into_sniffed()).boxed()
            } else if comm::protocol::match_handshake(buf) {
                switchboard
                    .handle_connection(ss.into_sniffed())
                    .from_err()
                    .boxed()
            } else {
                reject_connection(ss.into_sniffed()).from_err().boxed()
            }
        })
        .map_err(|err| error!("error handling request: {}", err))
}

fn reject_connection<A: AsyncWrite>(a: A) -> impl Future<Item = (), Error = io::Error> {
    io::write_all(a, "unknown protocol\n").discard()
}

/// Start the materialized server.
pub fn serve(config: Config) -> Result<(), failure::Error> {
    // Construct shared channels for SQL command and result exchange, and
    // dataflow command and result exchange.
    let (cmdq_tx, cmdq_rx) = mpsc::unbounded::<queue::Command>();
    let (dataflow_command_sender, dataflow_command_receiver) = mpsc::unbounded::<DataflowCommand>();

    // Extract timely dataflow parameters.
    let num_timely_workers = config.num_timely_workers();
    let is_primary = match &config.timely {
        timely::Configuration::Thread => true,
        timely::Configuration::Process(_) => true,
        timely::Configuration::Cluster { process, .. } => process == &0,
    };

    let switchboard = {
        let (nodes, id) = match &config.timely {
            timely::Configuration::Thread | timely::Configuration::Process(_) => {
                (vec!["localhost:6875".into()], 0)
            }
            timely::Configuration::Cluster {
                addresses, process, ..
            } => (addresses.clone(), *process),
        };
        Switchboard::new(nodes, id)
    };

    // Initialize pgwire / http listener.
    let listener = if is_primary {
        let listen_addr: SocketAddr = "[::]:6875".parse()?;
        let listener = TcpListener::bind(&listen_addr)?;
        println!("materialized listening on {}...", listen_addr);
        Some(listener)
    } else {
        None
    };

    let logging_config = config.logging_granularity.map(|d| LoggingConfig::new(d));

    // Construct timely dataflow instance.
    let local_input_mux = Mux::default();
    let dd_workers = dataflow::serve(
        dataflow_command_receiver,
        local_input_mux.clone(),
        config.timely,
        logging_config.clone(),
    )
    .map_err(|s| format_err!("{}", s))?;

    // Initialize command queue and sql planner
    let worker0_thread = dd_workers.guards().into_first().thread();
    queue::transient::serve(
        logging_config.as_ref(),
        switchboard.clone(),
        cmdq_rx,
        dataflow_command_sender,
        worker0_thread.clone(),
        num_timely_workers,
    );

    // Draw connections off of the listener.
    if let Some(listener) = listener {
        let start = future::lazy(move || {
            let server = listener
                .incoming()
                .for_each(move |stream| {
                    // Set TCP_NODELAY to disable tinygram prevention (Nagle's
                    // algorithm), which forces a 40ms delay between each query
                    // on linux. According to John Nagle [0], the true problem
                    // is delayed acks, but disabling those is a receive-side
                    // operation (TCP_QUICKACK), and we can't always control the
                    // client. PostgreSQL sets TCP_NODELAY on both sides of its
                    // sockets, so it seems sane to just do the same.
                    //
                    // If set_nodelay fails, it's a programming error, so panic.
                    //
                    // [0]: https://news.ycombinator.com/item?id=10608356
                    stream.set_nodelay(true).expect("set_nodelay failed");
                    tokio::spawn(handle_connection(
                        stream,
                        switchboard.clone(),
                        cmdq_tx.clone(),
                        num_timely_workers,
                    ));
                    Ok(())
                })
                .map_err(|err| error!("error accepting connection: {}", err));
            tokio::spawn(server);

            Ok(())
        });
        tokio::run(start);
    }

    Ok(())
}
