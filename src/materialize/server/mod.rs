// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Main materialized server.

use failure::format_err;
use futures::sync::mpsc::{self, UnboundedSender};
use futures::Future;
use log::error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use crate::pgwire;
use comm::Switchboard;
use dataflow_types::logging::LoggingConfig;
use ore::future::FutureExt;
use ore::mpmc::Mux;
use ore::netio;
use ore::netio::{SniffedStream, SniffingStream};
use ore::tokio::net::TcpStreamExt;

mod http;

pub struct Config {
    pub logging_granularity: Option<Duration>,
    /// The version of materialized.
    pub version: String,
    pub threads: usize,
    pub process: usize,
    pub addresses: Vec<String>,
}

impl Config {
    /// The number of timely workers described the by the configuration.
    pub fn num_timely_workers(&self) -> usize {
        self.threads * self.addresses.len()
    }
}

fn handle_connection(
    conn: TcpStream,
    switchboard: Switchboard<SniffedStream<TcpStream>>,
    cmdq_tx: UnboundedSender<coord::Command>,
) -> impl Future<Item = (), Error = ()> {
    // Sniff out what protocol we've received. Choosing how many bytes to sniff
    // is a delicate business. Read too many bytes and you'll stall out
    // protocols with small handshakes, like pgwire. Read too few bytes and
    // you won't be able to tell what protocol you have. For now, eight bytes
    // is the magic number, but this may need to change if we learn to speak
    // new protocols.
    let ss = SniffingStream::new(conn);
    netio::read_exact_or_eof(ss, [0; 8])
        .from_err()
        .and_then(move |(ss, buf, nread)| {
            let buf = &buf[..nread];
            if pgwire::match_handshake(buf) {
                pgwire::serve(ss.into_sniffed(), cmdq_tx).boxed()
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
    let (cmdq_tx, cmdq_rx) = mpsc::unbounded::<coord::Command>();

    // Extract timely dataflow parameters.
    let is_primary = config.process == 0;
    let num_timely_workers = config.num_timely_workers();

    // Initialize pgwire / http listener.
    let listen_addr = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        config.addresses[config.process]
            .split(':')
            .nth(1)
            .ok_or_else(|| format_err!("unable to parse node address"))?
            .parse()?,
    );
    let listener = TcpListener::bind(&listen_addr)?;
    println!(
        "materialized v{} listening on {}...",
        config.version, listen_addr
    );

    let socket_addrs = config
        .addresses
        .iter()
        .map(|addr| match addr.to_socket_addrs() {
            // TODO(benesch): we should try all possible addresses, not just the
            // first (#502).
            Ok(mut addrs) => match addrs.next() {
                Some(addr) => Ok(addr),
                None => Err(format_err!("{} did not resolve to any addresses", addr)),
            },
            Err(err) => Err(format_err!("error resolving {}: {}", addr, err)),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let switchboard = Switchboard::new(socket_addrs, config.process);
    let mut runtime = tokio::runtime::Runtime::new()?;
    runtime.spawn({
        let switchboard = switchboard.clone();
        listener
            .incoming()
            .for_each(move |conn| {
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
                conn.set_nodelay(true).expect("set_nodelay failed");
                if is_primary {
                    tokio::spawn(handle_connection(
                        conn,
                        switchboard.clone(),
                        cmdq_tx.clone(),
                    ));
                } else {
                    // When not the primary, we only need to route switchboard
                    // traffic.
                    let ss = SniffingStream::new(conn).into_sniffed();
                    tokio::spawn(
                        switchboard
                            .handle_connection(ss)
                            .map_err(|err| error!("error handling connection: {}", err)),
                    );
                }
                Ok(())
            })
            .map_err(|err| error!("error accepting connection: {}", err))
    });

    let dataflow_conns = runtime
        .block_on(switchboard.rendezvous(Duration::from_secs(30)))?
        .into_iter()
        .map(|conn| match conn {
            None => Ok(None),
            Some(conn) => Ok(Some(conn.into_inner().into_std()?)),
        })
        .collect::<Result<_, io::Error>>()?;

    let logging_config = config.logging_granularity.map(|d| LoggingConfig::new(d));

    // Construct timely dataflow instance.
    let local_input_mux = Mux::default();
    let _dd_workers = dataflow::serve(
        dataflow_conns,
        config.threads,
        config.process,
        switchboard.clone(),
        runtime.executor(),
        local_input_mux.clone(),
        logging_config.clone(),
    )
    .map_err(|s| format_err!("{}", s))?;

    // Initialize command queue and sql planner, but only on the primary.
    if is_primary {
        coord::transient::serve(
            switchboard,
            num_timely_workers,
            logging_config.as_ref(),
            cmdq_rx,
        );
    }

    runtime
        .shutdown_on_idle()
        .wait()
        .map_err(|()| unreachable!())
}
