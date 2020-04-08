// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A SQL stream processor built on top of [timely dataflow] and
//! [differential dataflow].
//!
//! [differential dataflow]: ../differential_dataflow/index.html
//! [timely dataflow]: ../timely/index.html

// Temporarily disable jemalloc on macOS as we have observed latency issues
// when we run load tests with jemalloc, but not the macOS system allocator
// todo(rkhaitan) figure out which allocator we want to use for all supported
// platforms
#[cfg(not(target_os = "macos"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::any::Any;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use compile_time_run::run_command_str;
use failure::format_err;
use futures::channel::mpsc::{self, UnboundedSender};
use futures::future::TryFutureExt;
use futures::stream::StreamExt;
use log::error;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

use comm::Switchboard;
use dataflow_types::logging::LoggingConfig;
use ore::future::OreTryFutureExt;
use ore::netio;
use ore::netio::{SniffedStream, SniffingStream};
use ore::thread::{JoinHandleExt, JoinOnDropHandle};
use ore::tokio::net::TcpStreamExt;

mod http;

/// The version of the crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The SHA identifying the Git commit at which the crate was built.
pub const BUILD_SHA: &str = run_command_str!(
    "sh",
    "-c",
    r#"if [ -n "$MZ_DEV_BUILD_SHA" ]; then
        echo "$MZ_DEV_BUILD_SHA"
    else
        # Unfortunately we need to suppress error messages from `git`, as
        # run_command_str will display no error message at all if we print
        # more than one line of output to stderr.
        git rev-parse --verify HEAD 2>/dev/null || {
            printf "error: unable to determine Git SHA; " >&2
            printf "either build from working Git clone " >&2
            printf "(see https://materialize.io/docs/install/#build-from-source), " >&2
            printf "or specify SHA manually by setting MZ_DEV_BUILD_SHA environment variable" >&2
            exit 1
        }
    fi"#
);

/// The time in UTC at which the crate was built as an ISO 8601-compliant
/// string.
pub const BUILD_TIME: &str = run_command_str!("date", "-u", "+%Y-%m-%dT%H:%M:%SZ");

/// Returns a human-readable version string.
pub fn version() -> String {
    format!("v{} ({})", VERSION, &BUILD_SHA[..9])
}

/// Configuration for a `materialized` server.
pub struct Config {
    /// The interval at which the internal Timely cluster should publish updates
    /// about its state.
    pub logging_granularity: Option<Duration>,
    /// The interval at which sources should be timestamped.
    pub timestamp_frequency: Option<Duration>,
    /// The maximum size of a timestamp batch.
    pub max_increment_ts_size: i64,
    /// The historical window in which distinctions are maintained for arrangements.
    ///
    /// As arrangements accept new timestamps they may optionally collapse prior
    /// timestamps to the same value, retaining their effect but removing their
    /// distinction. A large value or `None` results in a large amount of historical
    /// detail for arrangements; this increases the logical times at which they can
    /// be accurately queried, but consumes more memory. A low value reduces the
    /// amount of memory required but also risks not being able to use the arrangement
    /// in a query that has other constraints on the timestamps used (e.g. when joined
    /// with other arrangements).
    pub logical_compaction_window: Option<Duration>,
    /// The number of Timely worker threads that this process should host.
    pub threads: usize,
    /// The ID of this process in the cluster. IDs must be contiguously
    /// allocated, starting at zero.
    pub process: usize,
    /// The addresses of each process in the cluster, including this node,
    /// in order of process ID.
    pub addresses: Vec<SocketAddr>,
    /// The directory in which `materialized` should store its own metadata.
    pub data_directory: Option<PathBuf>,
    /// An optional symbiosis endpoint. See the
    /// [`symbiosis`](../symbiosis/index.html) crate for details.
    pub symbiosis_url: Option<String>,
    /// Whether to collect metrics. If enabled, metrics can be collected by
    /// e.g. Prometheus via the `/metrics` HTTP endpoint.
    pub gather_metrics: bool,
    /// The IP address and port to listen on -- defaults to 0.0.0.0:<addr_port>,
    /// where <addr_port> is the address of this process's entry in `addresses`.
    pub listen_addr: Option<SocketAddr>,
}

impl Config {
    /// The total number of timely workers, across all processes, described the
    /// by the configuration.
    pub fn num_timely_workers(&self) -> usize {
        self.threads * self.addresses.len()
    }
}

async fn handle_connection(
    conn: TcpStream,
    switchboard: Switchboard<SniffedStream<TcpStream>>,
    cmd_tx: UnboundedSender<coord::Command>,
    gather_metrics: bool,
    start_time: Instant,
) {
    // Sniff out what protocol we've received. Choosing how many bytes to sniff
    // is a delicate business. Read too many bytes and you'll stall out
    // protocols with small handshakes, like pgwire. Read too few bytes and
    // you won't be able to tell what protocol you have. For now, eight bytes
    // is the magic number, but this may need to change if we learn to speak
    // new protocols.
    let mut ss = SniffingStream::new(conn);
    let mut buf = [0; 8];
    let nread = match netio::read_exact_or_eof(&mut ss, &mut buf).await {
        Ok(nread) => nread,
        Err(err) => {
            error!("error handling request: {}", err);
            return;
        }
    };
    let buf = &buf[..nread];

    let res = if pgwire::match_handshake(buf) {
        pgwire::serve(ss.into_sniffed(), cmd_tx, gather_metrics).await
    } else if http::match_handshake(buf) {
        http::handle_connection(ss.into_sniffed(), cmd_tx, gather_metrics, start_time).await
    } else if comm::protocol::match_handshake(buf) {
        switchboard
            .handle_connection(ss.into_sniffed())
            .err_into()
            .await
    } else {
        log::warn!("unknown protocol connection!");
        ss.into_sniffed()
            .write_all(b"unknown protocol\n")
            .discard()
            .err_into()
            .await
    };
    if let Err(err) = res {
        error!("error handling request: {}", err)
    }
}

/// Start a `materialized` server.
pub fn serve(mut config: Config) -> Result<Server, failure::Error> {
    let start_time = Instant::now();

    // Construct shared channels for SQL command and result exchange, and
    // dataflow command and result exchange.
    let (cmd_tx, cmd_rx) = mpsc::unbounded::<coord::Command>();
    let cmd_tx = Arc::new(cmd_tx);

    // Extract timely dataflow parameters.
    let is_primary = config.process == 0;
    let num_timely_workers = config.num_timely_workers();

    // Start Tokio runtime.
    let mut runtime = tokio::runtime::Runtime::new()?;
    let executor = runtime.handle().clone();

    // Initialize network listener.
    let listen_addr = config.listen_addr.unwrap_or_else(|| {
        SocketAddr::new(
            match config.addresses[config.process].ip() {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            },
            config.addresses[config.process].port(),
        )
    });
    let mut listener = runtime.block_on(TcpListener::bind(&listen_addr))?;
    let local_addr = listener.local_addr()?;
    config.addresses[config.process].set_port(local_addr.port());

    println!(
        "materialized {} listening on {}...",
        version(),
        SocketAddr::new(listen_addr.ip(), local_addr.port()),
    );

    let switchboard = Switchboard::new(config.addresses, config.process, executor.clone());
    let gather_metrics = config.gather_metrics;
    runtime.spawn({
        let switchboard = switchboard.clone();
        let cmd_tx = Arc::downgrade(&cmd_tx);
        async move {
            let mut incoming = listener.incoming();
            while let Some(conn) = incoming.next().await {
                let conn = match conn {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("error accepting connection: {}", err);
                        continue;
                    }
                };
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
                    if let Some(cmd_tx) = cmd_tx.upgrade() {
                        tokio::spawn(handle_connection(
                            conn,
                            switchboard.clone(),
                            (*cmd_tx).clone(),
                            gather_metrics,
                            start_time,
                        ));
                        continue;
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
            }
        }
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
        let mut coord = coord::Coordinator::new(coord::Config {
            switchboard: switchboard.clone(),
            num_timely_workers,
            symbiosis_url: config.symbiosis_url.as_deref(),
            logging: logging_config.as_ref(),
            data_directory: config.data_directory.as_deref(),
            timestamp: match config.timestamp_frequency {
                Some(freq) => Some(coord::TimestampConfig {
                    frequency: freq,
                    max_size: config.max_increment_ts_size,
                }),
                None => None,
            },
            logical_compaction_window: config.logical_compaction_window,
            executor: &executor,
        })?;
        Some(thread::spawn(move || coord.serve(cmd_rx)).join_on_drop())
    } else {
        None
    };

    // Construct timely dataflow instance.
    let dataflow_guard = dataflow::serve(
        dataflow_conns,
        config.threads,
        config.process,
        switchboard,
        executor,
        config.timestamp_frequency.is_some(),
        logging_config,
    )
    .map_err(|s| format_err!("{}", s))?;

    Ok(Server {
        local_addr,
        _cmd_tx: cmd_tx,
        _dataflow_guard: Box::new(dataflow_guard),
        _coord_thread: coord_thread,
        _runtime: runtime,
    })
}

/// A running `materialized` server.
pub struct Server {
    local_addr: SocketAddr,
    // Drop order matters for these fields.
    _cmd_tx: Arc<mpsc::UnboundedSender<coord::Command>>,
    _dataflow_guard: Box<dyn Any>,
    _coord_thread: Option<JoinOnDropHandle<()>>,
    _runtime: Runtime,
}

impl Server {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}
