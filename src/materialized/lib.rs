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
use futures::channel::mpsc;
use futures::stream::StreamExt;
use log::error;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use tokio::io;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

use comm::Switchboard;
use dataflow_types::logging::LoggingConfig;
use ore::thread::{JoinHandleExt, JoinOnDropHandle};
use ore::tokio::net::TcpStreamExt;

use crate::mux::Mux;

mod http;
mod mux;

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
#[derive(Debug, Clone)]
pub struct Config {
    // === Timely and Differential worker options. ===
    /// The number of Timely worker threads that this process should host.
    pub threads: usize,
    /// The ID of this process in the cluster. IDs must be contiguously
    /// allocated, starting at zero.
    pub process: usize,
    /// The addresses of each process in the cluster, including this node,
    /// in order of process ID.
    pub addresses: Vec<SocketAddr>,

    // === Performance tuning options. ===
    /// The interval at which the internal Timely cluster should publish updates
    /// about its state.
    pub logging_granularity: Option<Duration>,
    /// The historical window in which distinctions are maintained for
    /// arrangements.
    ///
    /// As arrangements accept new timestamps they may optionally collapse prior
    /// timestamps to the same value, retaining their effect but removing their
    /// distinction. A large value or `None` results in a large amount of
    /// historical detail for arrangements; this increases the logical times at
    /// which they can be accurately queried, but consumes more memory. A low
    /// value reduces the amount of memory required but also risks not being
    /// able to use the arrangement in a query that has other constraints on the
    /// timestamps used (e.g. when joined with other arrangements).
    pub logical_compaction_window: Option<Duration>,
    /// The interval at which sources should be timestamped.
    pub timestamp_frequency: Duration,
    /// The maximum size of a timestamp batch.
    pub max_increment_ts_size: i64,
    /// Whether to record realtime consistency information to the data directory
    /// and attempt to recover that information on startup.
    pub persist_ts: bool,
    /// Whether to collect metrics. If enabled, metrics can be collected by
    /// e.g. Prometheus via the `/metrics` HTTP endpoint.
    pub gather_metrics: bool,

    // === Connection options. ===
    /// The IP address and port to listen on -- defaults to 0.0.0.0:<addr_port>,
    /// where <addr_port> is the address of this process's entry in `addresses`.
    pub listen_addr: Option<SocketAddr>,
    /// TLS encryption configuration.
    pub tls: Option<TlsConfig>,

    // === Storage options. ===
    /// The directory in which `materialized` should store its own metadata.
    pub data_directory: Option<PathBuf>,
    /// An optional symbiosis endpoint. See the
    /// [`symbiosis`](../symbiosis/index.html) crate for details.
    pub symbiosis_url: Option<String>,
}

impl Config {
    /// The total number of timely workers, across all processes, described the
    /// by the configuration.
    pub fn num_timely_workers(&self) -> usize {
        self.threads * self.addresses.len()
    }
}

/// Configures TLS encryption for connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// The path to the TLS certificate.
    pub cert: PathBuf,
    /// The path to the TLS key.
    pub key: PathBuf,
}

impl TlsConfig {
    fn acceptor(&self) -> Result<SslAcceptor, failure::Error> {
        let mut builder = SslAcceptor::mozilla_modern_v5(SslMethod::tls())?;
        builder.set_certificate_file(&self.cert, SslFiletype::PEM)?;
        builder.set_private_key_file(&self.key, SslFiletype::PEM)?;
        Ok(builder.build())
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
    runtime.spawn({
        let mux = {
            let mut mux = Mux::new();
            mux.add_handler(switchboard.clone());
            if is_primary {
                // The primary is responsible for pgwire and HTTP traffic in
                // addition to switchboard traffic.
                let cmd_tx = Arc::downgrade(&cmd_tx);
                let tls = match config.tls {
                    None => None,
                    Some(tls_config) => Some(tls_config.acceptor()?),
                };
                mux.add_handler(pgwire::Server::new(
                    tls,
                    cmd_tx.clone(),
                    config.gather_metrics,
                ));
                mux.add_handler(http::Server::new(cmd_tx, config.gather_metrics, start_time));
            }
            Arc::new(mux)
        };
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
                let mux = mux.clone();
                tokio::spawn(async move { mux.handle_connection(conn).await });
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

    let logging_config = config.logging_granularity.map(LoggingConfig::new);

    // Initialize command queue and sql planner, but only on the primary.
    let coord_thread = if is_primary {
        let mut coord = coord::Coordinator::new(coord::Config {
            switchboard: switchboard.clone(),
            num_timely_workers,
            symbiosis_url: config.symbiosis_url.as_deref(),
            logging: logging_config.as_ref(),
            data_directory: config.data_directory.as_deref(),
            timestamp: coord::TimestampConfig {
                frequency: config.timestamp_frequency,
                max_size: config.max_increment_ts_size,
                persist_ts: config.persist_ts,
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
