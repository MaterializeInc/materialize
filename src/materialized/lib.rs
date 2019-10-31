// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A SQL stream processor built on top of [timely dataflow] and
//! [differential dataflow].
//!
//! [differential dataflow]: ../differential_dataflow/index.html
//! [timely dataflow]: ../timely/index.html

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use failure::format_err;
use futures::sync::mpsc::{self, UnboundedSender};
use futures::{Future, Stream};
use log::error;
use std::any::Any;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::io::{self, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

use comm::Switchboard;
use dataflow_types::logging::LoggingConfig;
use ore::future::FutureExt;
use ore::netio;
use ore::netio::{SniffedStream, SniffingStream};
use ore::option::OptionExt;
use ore::tokio::net::TcpStreamExt;

mod http;

pub struct Config {
    pub logging_granularity: Option<Duration>,
    /// The version of materialized.
    pub version: String,
    pub threads: usize,
    pub process: usize,
    pub addresses: Vec<String>,
    pub sql: String,
    pub symbiosis_url: Option<String>,
    pub gather_metrics: bool,
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
    gather_metrics: bool,
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
                pgwire::serve(ss.into_sniffed(), cmdq_tx, gather_metrics).boxed()
            } else if http::match_handshake(buf) {
                http::handle_connection(ss.into_sniffed(), gather_metrics).boxed()
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
pub fn serve(config: Config) -> Result<Server, failure::Error> {
    // Construct shared channels for SQL command and result exchange, and
    // dataflow command and result exchange.
    let (cmdq_tx, cmdq_rx) = mpsc::unbounded::<coord::Command>();
    let cmdq_tx = Arc::new(cmdq_tx);

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

    let mut runtime = tokio::runtime::Runtime::new()?;
    let switchboard = Switchboard::new(socket_addrs, config.process, runtime.executor());
    let gather_metrics = config.gather_metrics;
    runtime.spawn({
        let switchboard = switchboard.clone();
        let cmdq_tx = Arc::downgrade(&cmdq_tx);
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
                    if let Some(cmdq_tx) = cmdq_tx.upgrade() {
                        tokio::spawn(handle_connection(
                            conn,
                            switchboard.clone(),
                            (*cmdq_tx).clone(),
                            gather_metrics,
                        ));
                        return Ok(());
                    }
                }
                // When not the primary, or when shutting down, we only need to
                // route switchboard traffic.
                let ss = SniffingStream::new(conn).into_sniffed();
                tokio::spawn(
                    switchboard
                        .handle_connection(ss)
                        .map_err(|err| error!("error handling connection: {}", err)),
                );
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

    // Initialize command queue and sql planner, but only on the primary.
    let coord_thread = if is_primary {
        Some(coord::transient::serve(
            switchboard.clone(),
            num_timely_workers,
            config.symbiosis_url.mz_as_deref(),
            logging_config.as_ref(),
            config.sql,
            cmdq_rx,
        )?)
    } else {
        None
    };

    // Construct timely dataflow instance.
    let dataflow_guard = dataflow::serve(
        dataflow_conns,
        config.threads,
        config.process,
        switchboard,
        runtime.executor(),
        logging_config.clone(),
    )
    .map_err(|s| format_err!("{}", s))?;

    Ok(Server {
        cmdq_tx: Some(cmdq_tx),
        dataflow_guard: Some(Box::new(dataflow_guard)),
        coord_thread,
        _runtime: runtime,
    })
}

/// A running `materialized` server.
pub struct Server {
    cmdq_tx: Option<Arc<mpsc::UnboundedSender<coord::Command>>>,
    dataflow_guard: Option<Box<dyn Any>>,
    coord_thread: Option<thread::JoinHandle<()>>,
    _runtime: Runtime,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.cmdq_tx.take();
        self.dataflow_guard.take();
        if let Some(coord_thread) = self.coord_thread.take() {
            coord_thread.join().unwrap();
        }
    }
}
