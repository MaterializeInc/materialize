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

use std::any::Any;
use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_stream::try_stream;
use compile_time_run::run_command_str;
use futures::channel::mpsc;
use futures::StreamExt;
use openssl::ssl::{SslAcceptor, SslContext, SslFiletype, SslMethod};
use tokio::io;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

use build_info::BuildInfo;
use comm::Switchboard;
use coord::{CacheConfig, LoggingConfig};
use ore::thread::{JoinHandleExt, JoinOnDropHandle};

use crate::mux::Mux;

mod http;
mod mux;
mod version_check;

// Disable jemalloc on macOS, as it is not well supported [0][1][2].
// The issues present as runaway latency on load test workloads that are
// comfortably handled by the macOS system allocator. Consider re-evaluating if
// jemalloc's macOS support improves.
//
// [0]: https://github.com/jemalloc/jemalloc/issues/26
// [1]: https://github.com/jemalloc/jemalloc/issues/843
// [2]: https://github.com/jemalloc/jemalloc/issues/1467
#[cfg(not(target_os = "macos"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub const BUILD_INFO: BuildInfo = BuildInfo {
    version: env!("CARGO_PKG_VERSION"),
    sha: run_command_str!(
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
                printf "(see https://materialize.com/docs/install/#build-from-source), " >&2
                printf "or specify SHA manually by setting MZ_DEV_BUILD_SHA environment variable" >&2
                exit 1
            }
        fi"#
    ),
    time: run_command_str!("date", "-u", "+%Y-%m-%dT%H:%M:%SZ"),
    target_triple: env!("TARGET_TRIPLE"),
};

/// Configuration for a `materialized` server.
#[derive(Debug, Clone)]
pub struct Config {
    // === Timely and Differential worker options. ===
    /// The number of Timely worker threads that this process should host.
    pub workers: usize,
    /// The ID of this process in the cluster. IDs must be contiguously
    /// allocated, starting at zero.
    pub process: usize,
    /// The addresses of each process in the cluster, including this node,
    /// in order of process ID.
    pub addresses: Vec<SocketAddr>,
    /// The Timely worker configuration.
    pub timely_worker: timely::WorkerConfig,

    // === Performance tuning options. ===
    pub logging: Option<LoggingConfig>,
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
    pub data_directory: PathBuf,
    pub cache: Option<CacheConfig>,
    /// An optional symbiosis endpoint. See the
    /// [`symbiosis`](../symbiosis/index.html) crate for details.
    pub symbiosis_url: Option<String>,
    /// Whether to permit usage of experimental features.
    pub experimental_mode: bool,
    /// An optional telemetry endpoint. Use None to disable telemetry.
    pub telemetry_url: Option<String>,
}

impl Config {
    /// The total number of Timely workers, across all processes, described by
    /// the configuration.
    pub fn total_workers(&self) -> usize {
        self.workers * self.addresses.len()
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
    fn validate(&self) -> Result<SslContext, anyhow::Error> {
        let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
        builder.set_certificate_file(&self.cert, SslFiletype::PEM)?;
        builder.set_private_key_file(&self.key, SslFiletype::PEM)?;
        Ok(builder.build().into_context())
    }
}

/// Start a `materialized` server.
pub async fn serve(
    mut config: Config,
    // TODO(benesch): Don't pass runtime explicitly when
    // `Handle::current().block_in_place()` lands. See:
    // https://github.com/tokio-rs/tokio/pull/3097.
    runtime: Arc<Runtime>,
) -> Result<Server, anyhow::Error> {
    let start_time = Instant::now();

    // Construct shared channels for SQL command and result exchange, and
    // dataflow command and result exchange.
    let (cmdq_tx, cmd_rx) = mpsc::unbounded();
    let coord_client = coord::Client::new(cmdq_tx);

    // Extract timely dataflow parameters.
    let is_primary = config.process == 0;
    let total_workers = config.total_workers();

    // Validate TLS configuration, if present.
    let tls = match &config.tls {
        None => None,
        Some(tls_config) => Some(tls_config.validate()?),
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
    let listener = TcpListener::bind(&listen_addr).await?;
    let local_addr = listener.local_addr()?;
    config.addresses[config.process].set_port(local_addr.port());

    let switchboard = Switchboard::new(config.addresses, config.process);

    // Launch task to serve connections.
    //
    // The lifetime of this task is controlled by two triggers that activate on
    // drop. Draining marks the beginning of the server shutdown process and
    // indicates that new user connections (i.e., pgwire and HTTP connections)
    // should be rejected. Gracefully handling existing user connections
    // requires that new system (i.e., switchboard) connections continue to be
    // routed whiles draining. The shutdown trigger indicates that draining is
    // complete, so switchboard traffic can cease and the task can exit.
    let (drain_trigger, drain_tripwire) = oneshot::channel();
    let (shutdown_trigger, shutdown_tripwire) = oneshot::channel();
    tokio::spawn({
        let switchboard = switchboard.clone();
        async move {
            // TODO(benesch): replace with `listener.incoming()` if that is
            // restored when the `Stream` trait stabilizes.
            let mut incoming = Box::pin(try_stream! {
                loop {
                    let (conn, _addr) = listener.accept().await?;
                    yield conn;
                }
            });

            // The primary is responsible for pgwire and HTTP traffic in
            // addition to switchboard traffic until draining starts.
            if is_primary {
                let mut mux = Mux::new();
                mux.add_handler(switchboard.clone());
                mux.add_handler(pgwire::Server::new(tls.clone(), coord_client.clone()));
                mux.add_handler(http::Server::new(
                    tls,
                    coord_client,
                    start_time,
                    // TODO(benesch): passing this to `http::Server::new` just
                    // so it can set a static metric is silly. We should just
                    // set the metric directly here.
                    total_workers,
                ));
                mux.serve(incoming.by_ref().take_until(drain_tripwire))
                    .await;
            }

            // Draining primaries and non-primary servers are only responsible
            // for switchboard traffic.
            let mut mux = Mux::new();
            mux.add_handler(switchboard.clone());
            mux.serve(incoming.take_until(shutdown_tripwire)).await
        }
    });

    let dataflow_conns = switchboard
        .rendezvous(Duration::from_secs(30))
        .await?
        .into_iter()
        .map(|conn| match conn {
            None => Ok(None),
            Some(conn) => {
                let conn = conn.into_inner().into_std()?;
                // Tokio will have put the stream into non-blocking mode.
                // Undo that, since Timely wants blocking TCP streams.
                conn.set_nonblocking(false)?;
                Ok(Some(conn))
            }
        })
        .collect::<Result<_, io::Error>>()?;

    // Launch dataflow workers.
    let dataflow_guard = dataflow::serve(
        dataflow::Config {
            workers: config.workers,
            process: config.process,
            timely_worker: config.timely_worker,
        },
        dataflow_conns,
        switchboard.clone(),
    )
    .map_err(|s| anyhow!("{}", s))?;

    // Initialize coordinator, but only on the primary.
    //
    // Note that the coordinator must be initialized *after* launching the
    // dataflow workers, as booting the coordinator can involve sending enough
    // data to workers to fill up a `comm` channel buffer (#3280).
    let coord_thread = if is_primary {
        let (handle, cluster_id) = coord::serve(
            coord::Config {
                switchboard,
                cmd_rx,
                total_workers,
                symbiosis_url: config.symbiosis_url.as_deref(),
                logging: config.logging,
                data_directory: &config.data_directory,
                timestamp: coord::TimestampConfig {
                    frequency: config.timestamp_frequency,
                },
                cache: config.cache,
                logical_compaction_window: config.logical_compaction_window,
                experimental_mode: config.experimental_mode,
                build_info: &BUILD_INFO,
            },
            runtime,
        )
        .await?;

        // Start a task that checks for the latest version and prints a warning if it
        // finds a different version than currently running.
        if let Some(telemetry_url) = config.telemetry_url {
            tokio::spawn(version_check::check_version_loop(
                telemetry_url,
                cluster_id.to_string(),
            ));
        }
        Some(handle.join_on_drop())
    } else {
        None
    };

    Ok(Server {
        local_addr,
        _drain_trigger: drain_trigger,
        _dataflow_guard: Box::new(dataflow_guard),
        _coord_thread: coord_thread,
        _shutdown_trigger: shutdown_trigger,
    })
}

/// A running `materialized` server.
pub struct Server {
    local_addr: SocketAddr,
    // Drop order matters for these fields.
    _drain_trigger: oneshot::Sender<()>,
    _dataflow_guard: Box<dyn Any>,
    _coord_thread: Option<JoinOnDropHandle<()>>,
    _shutdown_trigger: oneshot::Sender<()>,
}

impl Server {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}
