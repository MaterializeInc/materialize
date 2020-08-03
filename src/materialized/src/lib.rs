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
use std::thread;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use compile_time_run::run_command_str;
use futures::channel::mpsc;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use stream_cancel::{StreamExt as _, Trigger, Tripwire};
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
    /// Whether to permit usage of experimental features.
    pub experimental_mode: bool,
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
    fn acceptor(&self) -> Result<SslAcceptor, anyhow::Error> {
        let mut builder = SslAcceptor::mozilla_modern_v5(SslMethod::tls())?;
        builder.set_certificate_file(&self.cert, SslFiletype::PEM)?;
        builder.set_private_key_file(&self.key, SslFiletype::PEM)?;
        Ok(builder.build())
    }
}

/// Start a `materialized` server.
pub fn serve(mut config: Config) -> Result<Server, anyhow::Error> {
    let start_time = Instant::now();

    // Construct shared channels for SQL command and result exchange, and
    // dataflow command and result exchange.
    let (cmdq_tx, cmd_rx) = mpsc::unbounded::<coord::Command>();

    // Extract timely dataflow parameters.
    let is_primary = config.process == 0;
    let num_timely_workers = config.num_timely_workers();

    // Start Tokio runtime.
    let mut runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        // The default thread name exceeds the Linux limit on thread name
        // length, so pick something shorter.
        //
        // TODO(benesch): use `thread_name_fn` to get unique names if that
        // lands upstream: https://github.com/tokio-rs/tokio/pull/1921.
        .thread_name("tokio:worker")
        .enable_all()
        .build()?;
    let executor = runtime.handle().clone();

    // Validate TLS configuration, if present.
    let tls = match &config.tls {
        None => None,
        Some(tls_config) => Some(tls_config.acceptor()?),
    };

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

    // Launch task to serve connections.
    //
    // The lifetime of this task is controlled by two triggers that activate on
    // drop. Draining marks the beginning of the server shutdown process and
    // indicates that new user connections (i.e., pgwire and HTTP connections)
    // should be rejected. Gracefully handling existing user connections
    // requires that new system (i.e., switchboard) connections continue to be
    // routed whiles draining. The shutdown trigger indicates that draining is
    // complete, so switchboard traffic can cease and the task can exit.
    let (drain_trigger, drain_tripwire) = Tripwire::new();
    let (shutdown_trigger, shutdown_tripwire) = Tripwire::new();
    runtime.spawn({
        let switchboard = switchboard.clone();
        async move {
            let incoming = &mut listener.incoming();

            // The primary is responsible for pgwire and HTTP traffic in
            // addition to switchboard traffic until draining starts.
            if is_primary {
                let mut mux = Mux::new();
                mux.add_handler(switchboard.clone());
                mux.add_handler(pgwire::Server::new(tls.clone(), cmdq_tx.clone()));
                mux.add_handler(http::Server::new(
                    tls,
                    cmdq_tx,
                    start_time,
                    &num_timely_workers.to_string(),
                ));
                mux.serve(incoming.take_until_if(drain_tripwire)).await;
            }

            // Draining primaries and non-primary servers are only responsible
            // for switchboard traffic.
            let mut mux = Mux::new();
            mux.add_handler(switchboard.clone());
            mux.serve(incoming.take_until_if(shutdown_tripwire)).await
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

    // Launch dataflow workers.
    let dataflow_guard = dataflow::serve(
        dataflow_conns,
        config.threads,
        config.process,
        switchboard.clone(),
        executor.clone(),
        logging_config.clone(),
    )
    .map_err(|s| anyhow!("{}", s))?;

    // Initialize coordinator, but only on the primary.
    //
    // Note that the coordinator must be initialized *after* launching the
    // dataflow workers, as booting the coordinator can involve sending enough
    // data to workers to fill up a `comm` channel buffer (#3280).
    let coord_thread = if is_primary {
        let mut coord = coord::Coordinator::new(coord::Config {
            switchboard,
            num_timely_workers,
            symbiosis_url: config.symbiosis_url.as_deref(),
            logging: logging_config.as_ref(),
            logging_granularity: config.logging_granularity,
            data_directory: config.data_directory.as_deref(),
            timestamp: coord::TimestampConfig {
                frequency: config.timestamp_frequency,
            },
            logical_compaction_window: config.logical_compaction_window,
            executor: &executor,
            experimental_mode: config.experimental_mode,
        })?;
        Some(thread::spawn(move || coord.serve(cmd_rx)).join_on_drop())
    } else {
        None
    };

    Ok(Server {
        local_addr,
        _drain_trigger: drain_trigger,
        _dataflow_guard: Box::new(dataflow_guard),
        _coord_thread: coord_thread,
        _shutdown_trigger: shutdown_trigger,
        _runtime: runtime,
    })
}

/// A running `materialized` server.
pub struct Server {
    local_addr: SocketAddr,
    // Drop order matters for these fields.
    _drain_trigger: Trigger,
    _dataflow_guard: Box<dyn Any>,
    _coord_thread: Option<JoinOnDropHandle<()>>,
    _shutdown_trigger: Trigger,
    _runtime: Runtime,
}

impl Server {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}
