// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::Context;
use axum::http::StatusCode;
use axum::routing;
use fail::FailScenario;
use futures::future;
use hyper_util::rt::TokioIo;
use mz_build_info::{BuildInfo, build_info};
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_cluster_client::client::TimelyConfig;
use mz_compute::server::ComputeInstanceContext;
use mz_http_util::DynamicFilterTarget;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::{MetricsRegistry, register_runtime_metrics};
use mz_ore::netio::{Listener, SocketAddr};
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::{GrpcPubSubClient, PersistPubSubClient, PersistPubSubClientConfig};
use mz_service::emit_boot_diagnostics;
use mz_service::grpc::GrpcServerMetrics;
use mz_service::secrets::SecretsReaderCliArgs;
use mz_service::transport;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use tokio::runtime::Handle;
use tower::Service;
use tracing::{Instrument, debug, error, info, info_span};

mod usage_metrics;

const BUILD_INFO: BuildInfo = build_info!();

/// Resolves a short hostname to its FQDN by parsing `/etc/hosts`.
///
/// In Kubernetes, the kubelet writes the pod's FQDN into `/etc/hosts`
/// for StatefulSet pods with hostname/subdomain configured, e.g.:
///
/// ```text
/// 10.0.1.5  clusterd-0.svc.namespace.svc.cluster.local  clusterd-0
/// ```
///
/// This approach avoids FFI (`getaddrinfo`) and works even if CoreDNS
/// is temporarily unavailable at pod startup. Falls back to the short
/// hostname if no FQDN is found.
fn resolve_fqdn(short_hostname: &str) -> String {
    if let Ok(hosts) = std::fs::read_to_string("/etc/hosts") {
        for line in hosts.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // /etc/hosts format: IP_ADDRESS CANONICAL_NAME [ALIASES...]
            let mut fields = line.split_whitespace();
            let _ip = fields.next();
            let names: Vec<&str> = fields.collect();
            if names.contains(&short_hostname) {
                if let Some(fqdn) = names.iter().find(|&&n| n.contains('.')) {
                    return fqdn.to_string();
                }
            }
        }
    }
    eprintln!(
        "warning: could not resolve FQDN for {:?} from /etc/hosts; \
         falling back to short hostname. GRPC host validation may not work correctly.",
        short_hostname,
    );
    short_hostname.to_string()
}

pub static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

/// Independent cluster server for Materialize.
#[derive(clap::Parser)]
#[clap(name = "clusterd", version = VERSION.as_str())]
struct Args {
    // === Connection options. ===
    /// The address on which to listen for a connection from the storage
    /// controller.
    #[clap(
        long,
        env = "STORAGE_CONTROLLER_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2100"
    )]
    storage_controller_listen_addr: SocketAddr,
    /// The address on which to listen for a connection from the compute
    /// controller.
    #[clap(
        long,
        env = "COMPUTE_CONTROLLER_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2101"
    )]
    compute_controller_listen_addr: SocketAddr,
    /// The address of the internal HTTP server.
    #[clap(
        long,
        env = "INTERNAL_HTTP_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6878"
    )]
    internal_http_listen_addr: SocketAddr,
    /// The FQDN of this process, for GRPC request validation.
    ///
    /// Not providing this value or setting it to the empty string disables host validation for
    /// GRPC requests.
    #[clap(long, env = "GRPC_HOST", value_name = "NAME")]
    grpc_host: Option<String>,

    // === Timely cluster options. ===
    /// Configuration for the storage Timely cluster.
    #[clap(long, env = "STORAGE_TIMELY_CONFIG")]
    storage_timely_config: TimelyConfig,
    /// Configuration for the compute Timely cluster.
    #[clap(long, env = "COMPUTE_TIMELY_CONFIG")]
    compute_timely_config: TimelyConfig,
    /// The index of the process in both Timely clusters.
    #[clap(long, env = "PROCESS")]
    process: usize,

    // === Storage options. ===
    /// The URL for the Persist PubSub service.
    #[clap(
        long,
        env = "PERSIST_PUBSUB_URL",
        value_name = "http://HOST:PORT",
        default_value = "http://localhost:6879"
    )]
    persist_pubsub_url: String,

    // === Cloud options. ===
    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, env = "AWS_EXTERNAL_ID", value_name = "ID", value_parser = AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable)]
    aws_external_id_prefix: Option<AwsExternalIdPrefix>,

    /// The ARN for a Materialize-controlled role to assume before assuming
    /// a customer's requested role for an AWS connection.
    #[clap(long, env = "AWS_CONNECTION_ROLE_ARN")]
    aws_connection_role_arn: Option<String>,

    // === Secrets reader options. ===
    #[clap(flatten)]
    secrets: SecretsReaderCliArgs,

    // === Tracing options. ===
    #[clap(flatten)]
    tracing: TracingCliArgs,

    // === Other options. ===
    /// An opaque identifier for the environment in which this process is
    /// running.
    #[clap(long, env = "ENVIRONMENT_ID")]
    environment_id: String,

    /// A scratch directory that can be used for ephemeral storage.
    #[clap(long, env = "SCRATCH_DIRECTORY", value_name = "PATH")]
    scratch_directory: Option<PathBuf>,

    /// Memory limit (bytes) of the cluster replica, if known.
    ///
    /// The limit is expected to be enforced by the orchestrator. The clusterd process only uses it
    /// to inform configuration of backpressure mechanism.
    #[clap(long)]
    announce_memory_limit: Option<usize>,

    /// Heap limit (bytes) of the cluster replica.
    ///
    /// A process heap usage is calculated as the sum of its memory and swap usage.
    ///
    /// In contrast to `announce_memory_limit`, this limit is enforced by the clusterd process. If
    /// the limit is exceeded, the process terminates itself with a 167 exit code.
    #[clap(long)]
    heap_limit: Option<usize>,

    /// Whether this size represents a modern "cc" size rather than a legacy
    /// T-shirt size.
    #[clap(long)]
    is_cc: bool,

    /// Set core affinity for Timely workers.
    ///
    /// This flag should only be set if the process is provided with exclusive access to its
    /// supplied CPU cores. If other processes are competing over the same cores, setting core
    /// affinity might degrade dataflow performance rather than improving it.
    #[clap(long)]
    worker_core_affinity: bool,
}

/// Install signal handlers so that termination signals are not ignored.
///
/// On Linux, PID 1 has special signal semantics: the kernel will not
/// deliver signals whose disposition is SIG_DFL (the default). Since
/// distroless containers run the binary directly as PID 1 (no tini),
/// signals like SIGTERM from Kubernetes pod termination would be silently
/// ignored without explicit handlers. This function registers a handler
/// that restores the default disposition and re-raises, producing the
/// expected termination behavior.
fn install_termination_signal_handlers() {
    use nix::sys::signal;

    extern "C" fn handle_signal(signum: i32) {
        // Restore default handler and re-raise so the process terminates
        // with the correct signal and exit code.
        let action = signal::SigAction::new(
            signal::SigHandler::SigDfl,
            signal::SaFlags::SA_NODEFER | signal::SaFlags::SA_ONSTACK,
            signal::SigSet::empty(),
        );
        unsafe { signal::sigaction(signum.try_into().unwrap(), &action) }
            .unwrap_or_else(|_| panic!("failed to uninstall handler for {}", signum));
        signal::raise(signum.try_into().unwrap())
            .unwrap_or_else(|_| panic!("failed to re-raise signal {}", signum));
    }

    let action = signal::SigAction::new(
        signal::SigHandler::Handler(handle_signal),
        signal::SaFlags::SA_NODEFER | signal::SaFlags::SA_ONSTACK,
        signal::SigSet::empty(),
    );
    for signum in &[
        signal::SIGHUP,
        signal::SIGINT,
        signal::SIGALRM,
        signal::SIGTERM,
    ] {
        unsafe { signal::sigaction(*signum, &action) }
            .unwrap_or_else(|e| panic!("failed to install handler for {}: {}", signum, e));
    }
}

pub fn main() {
    // Install signal handlers before anything else. As PID 1 in a
    // distroless container, the kernel ignores signals without explicit
    // handlers — without this, SIGTERM from Kubernetes would be ignored
    // and the pod would hang until SIGKILL.
    install_termination_signal_handlers();

    mz_ore::panic::install_enhanced_handler();

    // When running in Kubernetes, auto-detect the GRPC host from the pod's FQDN
    // and the process index from the StatefulSet ordinal. These are set as env
    // vars so that clap picks them up as defaults (they can still be overridden
    // via explicit env vars or CLI args).
    //
    // SAFETY: Called before any threads are spawned.
    // `install_enhanced_handler` above only registers a panic hook; it does
    // not spawn threads. The hook spawns a thread only if a panic fires,
    // which cannot happen between here and the first `unsafe` call below.
    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok() {
        if std::env::var("CLUSTERD_GRPC_HOST").is_err() {
            // Resolve the pod's FQDN via DNS, equivalent to `hostname --fqdn`.
            // In Kubernetes, /etc/hostname only has the short name (e.g.,
            // "clusterd-0"), but GRPC validation needs the FQDN (e.g.,
            // "clusterd-0.clusterd.ns.svc.cluster.local"). We resolve the
            // short hostname through DNS to get the canonical name.
            //
            // This avoids shelling out to `hostname --fqdn` which isn't
            // available in distroless images.
            if let Ok(hostname) = nix::unistd::gethostname() {
                if let Some(short) = hostname.to_str() {
                    let fqdn = resolve_fqdn(short);
                    unsafe { std::env::set_var("CLUSTERD_GRPC_HOST", &fqdn) };
                }
            }
        }
        if std::env::var("CLUSTERD_PROCESS").is_err() {
            // Extract the ordinal index from the last segment of the
            // StatefulSet hostname (e.g., "mz5ncn-cluster-s1-replica-s1-gen-1-0"
            // → "0"). This matches orchestrator-kubernetes which also uses
            // split('-').next_back() to extract the process ID from pod names.
            if let Ok(hostname) = std::env::var("HOSTNAME") {
                if let Some(ordinal) = hostname.rsplit('-').next() {
                    unsafe { std::env::set_var("CLUSTERD_PROCESS", ordinal) };
                }
            }
        }
    }

    let args = cli::parse_args(CliConfig {
        env_prefix: Some("CLUSTERD_"),
        enable_version_flag: true,
    });

    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        .thread_stack_size(3 * 1024 * 1024) // 3 MiB
        // The default thread name exceeds the Linux limit on thread name
        // length, so pick something shorter. The maximum length is 16 including
        // a \0 terminator. This gives us four decimals, which should be enough
        // for most existing computers.
        .thread_name_fn(|| {
            use std::sync::atomic::{AtomicUsize, Ordering};
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
            format!("tokio:work-{}", id)
        })
        .enable_all()
        .build()
        .unwrap();
    if let Err(err) = runtime.block_on(run(args)) {
        panic!("clusterd: fatal: {}", err.display_with_causes());
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    let tracing_handle = args
        .tracing
        .configure_tracing(
            StaticTracingConfig {
                service_name: "clusterd",
                build_info: BUILD_INFO,
            },
            metrics_registry.clone(),
        )
        .await?;

    let tracing_handle = Arc::new(tracing_handle);
    register_runtime_metrics("main", Handle::current().metrics(), &metrics_registry);

    // Keep this _after_ the mz_ore::tracing::configure call so that its panic
    // hook runs _before_ the one that sends things to sentry.
    mz_timely_util::panic::halt_on_timely_communication_panic();

    let _failpoint_scenario = FailScenario::setup();

    emit_boot_diagnostics!(&BUILD_INFO);

    mz_alloc::register_metrics_into(&metrics_registry).await;
    mz_metrics::register_metrics_into(&metrics_registry, mz_dyncfgs::all_dyncfgs()).await;

    if let Some(heap_limit) = args.heap_limit {
        mz_compute::memory_limiter::start_limiter(heap_limit, &metrics_registry);
    } else {
        info!("no heap limit announced; disabling memory limiter");
    }

    let secrets_reader = args
        .secrets
        .load()
        .await
        .context("loading secrets reader")?;

    let usage_collector = Arc::new(usage_metrics::Collector {
        disk_root: args.scratch_directory.clone(),
    });

    mz_ore::task::spawn(|| "clusterd_internal_http_server", {
        let metrics_registry = metrics_registry.clone();
        tracing::info!(
            "serving internal HTTP server on {}",
            args.internal_http_listen_addr
        );
        let listener = Listener::bind(args.internal_http_listen_addr).await?;
        let mut make_service = mz_prof_http::router(&BUILD_INFO)
            .route(
                "/api/livez",
                routing::get(mz_http_util::handle_liveness_check),
            )
            .route(
                "/metrics",
                routing::get(move || async move {
                    mz_http_util::handle_prometheus(&metrics_registry).await
                }),
            )
            .route("/api/tracing", routing::get(mz_http_util::handle_tracing))
            .route(
                "/api/opentelemetry/config",
                routing::put({
                    move |_: axum::Json<DynamicFilterTarget>| async {
                        (
                            StatusCode::BAD_REQUEST,
                            "This endpoint has been replaced. \
                                Use the `opentelemetry_filter` system variable."
                                .to_string(),
                        )
                    }
                }),
            )
            .route(
                "/api/stderr/config",
                routing::put({
                    move |_: axum::Json<DynamicFilterTarget>| async {
                        (
                            StatusCode::BAD_REQUEST,
                            "This endpoint has been replaced. \
                                Use the `log_filter` system variable."
                                .to_string(),
                        )
                    }
                }),
            )
            .route(
                "/api/usage-metrics",
                routing::get(async move || axum::Json(usage_collector.collect())),
            )
            .into_make_service();

        // Once https://github.com/tokio-rs/axum/pull/2479 lands, this can become just a call to
        // `axum::serve`.
        async move {
            loop {
                let (conn, remote_addr) = match listener.accept().await {
                    Ok(peer) => peer,
                    Err(error) => {
                        // Match hyper's AddrIncoming error handling:
                        // connection errors are per-connection and can be
                        // skipped immediately; all other errors (e.g., EMFILE)
                        // sleep to avoid a tight loop on resource exhaustion.
                        if is_connection_error(&error) {
                            debug!("accepted connection already errored: {error:#}");
                        } else {
                            error!("internal_http accept error: {error:#}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        continue;
                    }
                };

                let tower_service = make_service.call(&conn).await.expect("infallible");
                let hyper_service =
                    hyper::service::service_fn(move |req| tower_service.clone().call(req));

                mz_ore::task::spawn(
                    || format!("clusterd_internal_http_server:{remote_addr}"),
                    async move {
                        if let Err(error) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(conn), hyper_service)
                            .await
                        {
                            // This can happen when the client performs an unclean shutdown, so a
                            // high severity isn't warranted. Might even downgrade this to DEBUG if
                            // it turns out too noisy.
                            info!("error serving internal_http connection: {error:#}");
                        }
                    },
                );
            }
        }
    });

    let pubsub_caller_id = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| args.tracing.log_prefix.clone())
        .unwrap_or_default();
    let mut persist_cfg =
        PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone(), mz_dyncfgs::all_dyncfgs());
    persist_cfg.is_cc_active = args.is_cc;
    persist_cfg.announce_memory_limit = args.announce_memory_limit;
    // Start with compaction disabled, will get enabled once a cluster receives AllowWrites.
    persist_cfg.disable_compaction();

    let persist_clients = Arc::new(PersistClientCache::new(
        persist_cfg,
        &metrics_registry,
        |persist_cfg, metrics| {
            let cfg = PersistPubSubClientConfig {
                url: args.persist_pubsub_url,
                caller_id: pubsub_caller_id,
                persist_cfg: persist_cfg.clone(),
            };
            GrpcPubSubClient::connect(cfg, metrics)
        },
    ));
    let txns_ctx = TxnsContext::default();

    let connection_context = ConnectionContext::from_cli_args(
        args.environment_id,
        &args.tracing.startup_log_filter,
        args.aws_external_id_prefix,
        args.aws_connection_role_arn,
        secrets_reader,
        None,
    );

    let grpc_host = args.grpc_host.and_then(|h| (!h.is_empty()).then_some(h));
    let grpc_server_metrics = GrpcServerMetrics::register_with(&metrics_registry);

    let mut storage_timely_config = args.storage_timely_config;
    storage_timely_config.process = args.process;
    let mut compute_timely_config = args.compute_timely_config;
    compute_timely_config.process = args.process;

    // Start storage server.
    let storage_client_builder = mz_storage::serve(
        storage_timely_config,
        &metrics_registry,
        Arc::clone(&persist_clients),
        txns_ctx.clone(),
        Arc::clone(&tracing_handle),
        SYSTEM_TIME.clone(),
        connection_context.clone(),
        StorageInstanceContext::new(args.scratch_directory.clone(), args.announce_memory_limit)?,
    )
    .await?;
    info!(
        "listening for storage controller connections on {}",
        args.storage_controller_listen_addr
    );
    mz_ore::task::spawn(
        || "storage_server",
        transport::serve(
            args.storage_controller_listen_addr,
            BUILD_INFO.semver_version(),
            grpc_host.clone(),
            Duration::MAX,
            storage_client_builder,
            grpc_server_metrics.for_server("storage"),
        )
        .instrument(info_span!("ctp", name = "storage")),
    );

    // Start compute server.
    let compute_client_builder = mz_compute::server::serve(
        compute_timely_config,
        &metrics_registry,
        persist_clients,
        txns_ctx,
        tracing_handle,
        ComputeInstanceContext {
            scratch_directory: args.scratch_directory,
            worker_core_affinity: args.worker_core_affinity,
            connection_context,
        },
    )
    .await?;
    info!(
        "listening for compute controller connections on {}",
        args.compute_controller_listen_addr
    );
    mz_ore::task::spawn(
        || "compute_server",
        transport::serve(
            args.compute_controller_listen_addr,
            BUILD_INFO.semver_version(),
            grpc_host.clone(),
            Duration::MAX,
            compute_client_builder,
            grpc_server_metrics.for_server("compute"),
        )
        .instrument(info_span!("ctp", name = "compute")),
    );

    // TODO: unify storage and compute servers to use one timely cluster.

    // Block forever.
    future::pending().await
}

/// Per-connection errors from `accept()` that can be skipped immediately.
/// All other errors (e.g., EMFILE/ENFILE resource exhaustion) warrant a sleep
/// before retrying. Mirrors hyper's `AddrIncoming` classification.
fn is_connection_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}
