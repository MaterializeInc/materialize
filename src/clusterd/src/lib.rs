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
use mz_compute::server::{ComputeInstanceContext, ComputeRuntimeRole};
use mz_compute_client::multiplex::Multiplexer;
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
use mz_service::secrets::SecretsReaderCliArgs;
use mz_service::transport;
use mz_service::transport::ClusterServerMetrics;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_types::connections::ConnectionContext;
use mz_timely_util::capture::arc_event_link;
use mz_txn_wal::operator::TxnsContext;
use tokio::runtime::Handle;
use tower::Service;
use tracing::{Instrument, debug, error, info, info_span};

mod usage_metrics;

const BUILD_INFO: BuildInfo = build_info!();

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
    /// Configuration for a second, interactive compute Timely cluster.
    ///
    /// When present, the process runs a second compute runtime alongside the maintenance runtime,
    /// sharing the maintenance runtime's process ordinal and peer count but supplying its own
    /// inter-worker addresses. When absent, the process runs exactly one compute runtime.
    #[clap(long, env = "INTERACTIVE_COMPUTE_TIMELY_CONFIG")]
    interactive_compute_timely_config: Option<TimelyConfig>,
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

    /// Forward storage's timely logging events to compute so storage operators appear in
    /// `mz_introspection.mz_dataflow_*` tables.
    #[clap(long)]
    enable_storage_introspection_logs: bool,
}

/// The process ordinal for a StatefulSet pod, taken from the trailing
/// `-`-delimited segment of its hostname (e.g.
/// "mz5ncn-cluster-s1-replica-s1-gen-1-0" → "0"). This mirrors how
/// orchestrator-kubernetes recovers the process id from pod names.
///
/// Returns `None` when the trailing segment is not a non-negative integer, so
/// an unexpected hostname leaves `CLUSTERD_PROCESS` unset rather than set to a
/// value that fails to parse as the process index.
fn process_ordinal_from_hostname(hostname: &str) -> Option<&str> {
    let ordinal = hostname.rsplit('-').next()?;
    ordinal.parse::<usize>().ok().map(|_| ordinal)
}

/// Derives the interactive compute runtime's `TimelyConfig` from the CLI arg, if present.
///
/// Sets the interactive config's process ordinal to match the maintenance runtime, since both
/// runtimes are the same process. Asserts that the two runtimes span an equal number of Timely
/// peers: the arrangement sharing registry pairs worker `i` of one runtime with worker `i` of the
/// other, and reads are sound only because both shard keys across the same peer count.
///
/// Returns `None` when the arg is absent, in which case the process runs exactly one compute
/// runtime.
fn prepare_interactive_compute_config(
    arg: Option<TimelyConfig>,
    process: usize,
    maintenance: &TimelyConfig,
) -> Option<TimelyConfig> {
    let mut interactive = arg?;
    interactive.process = process;
    let maintenance_peers = maintenance.workers * maintenance.addresses.len();
    let interactive_peers = interactive.workers * interactive.addresses.len();
    assert_eq!(
        maintenance_peers, interactive_peers,
        "interactive and maintenance compute runtimes must span an equal number of Timely peers",
    );
    Some(interactive)
}

pub fn main() {
    mz_ore::panic::install_enhanced_handler();

    // Pin the rustls crypto provider to aws-lc-rs. The LaunchDarkly SDK uses
    // hyper-rustls, so building its client resolves the process-default rustls
    // provider. The workspace also links rustls' `ring` feature (pulled by
    // other hyper-rustls chains), and with both provider features enabled
    // rustls cannot choose a default on its own and panics. The call is
    // idempotent, so ignore the result.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Derive `CLUSTERD_PROCESS` (the process ordinal) from the pod hostname
    // when running under Kubernetes and it was not set explicitly. The
    // distroless image has no shell entrypoint to do this, so clusterd does it
    // itself.
    if std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
        && std::env::var("CLUSTERD_PROCESS").is_err()
    {
        if let Ok(hostname) = std::env::var("HOSTNAME") {
            if let Some(ordinal) = process_ordinal_from_hostname(&hostname) {
                // SAFETY: `set_var` is called before any threads are spawned.
                // `install_enhanced_handler` above only registers a panic hook.
                // That hook spawns a thread only on panic, which cannot happen
                // before this call.
                unsafe { std::env::set_var("CLUSTERD_PROCESS", ordinal) };
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
                routing::get(move |headers: axum::http::HeaderMap| async move {
                    mz_http_util::handle_prometheus(&metrics_registry, headers).await
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
    let cluster_server_metrics = ClusterServerMetrics::register_with(&metrics_registry);

    let mut storage_timely_config = args.storage_timely_config;
    storage_timely_config.process = args.process;
    let mut compute_timely_config = args.compute_timely_config;
    compute_timely_config.process = args.process;

    // We assume each storage worker has a corresponding compute worker that can process its logs.
    assert_eq!(
        storage_timely_config.workers, compute_timely_config.workers,
        "storage and compute must have equal workers-per-process",
    );

    // Create per-worker bridges for forwarding storage timely logging events to compute.
    let (storage_log_writers, storage_log_readers) = if args.enable_storage_introspection_logs {
        (0..storage_timely_config.workers)
            .map(|_| arc_event_link())
            .unzip()
    } else {
        (Vec::new(), Vec::new())
    };

    // Start storage server.
    let storage_client_builder = mz_storage::serve(
        storage_timely_config,
        &metrics_registry,
        Arc::clone(&persist_clients),
        txns_ctx.clone(),
        Arc::clone(&tracing_handle),
        SYSTEM_TIME.clone(),
        connection_context.clone(),
        StorageInstanceContext::new(args.scratch_directory.clone(), args.announce_memory_limit),
        storage_log_writers,
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
            cluster_server_metrics.for_server("storage"),
        )
        .instrument(info_span!("ctp", name = "storage")),
    );

    // Start compute server.
    //
    // Create the per-process arrangement sharing registry once here, so all compute workers of this
    // process, across both runtimes, share one registry.
    let sharing_registry = mz_compute::sharing::ArrangementSharingRegistry::new();

    // Derive the interactive runtime's config before the maintenance config is moved into `serve`,
    // since the equal-peers check needs both.
    let interactive_compute_timely_config = prepare_interactive_compute_config(
        args.interactive_compute_timely_config,
        args.process,
        &compute_timely_config,
    );

    // Without an interactive runtime this process runs a single `Solo` compute runtime, which is
    // behaviorally identical to a deployment without the second runtime. With one, the first
    // `serve` becomes the `Maintenance` runtime whose published arrangements the interactive runtime
    // reads.
    let maintenance_role = if interactive_compute_timely_config.is_some() {
        ComputeRuntimeRole::Maintenance
    } else {
        ComputeRuntimeRole::Solo
    };

    // The interactive runtime shares these process resources with the maintenance runtime, so build
    // a shareable compute context and clone what each `serve` moves.
    let compute_context = ComputeInstanceContext {
        scratch_directory: args.scratch_directory,
        worker_core_affinity: args.worker_core_affinity,
        connection_context,
    };

    let compute_client_builder = mz_compute::server::serve(
        compute_timely_config,
        maintenance_role,
        &metrics_registry,
        Arc::clone(&persist_clients),
        sharing_registry.clone(),
        txns_ctx.clone(),
        Arc::clone(&tracing_handle),
        compute_context.clone(),
        storage_log_readers,
    )
    .await?;
    info!(
        "listening for compute controller connections on {}",
        args.compute_controller_listen_addr
    );

    // Start the interactive compute runtime, if configured. It reads arrangements the maintenance
    // runtime publishes into the shared `sharing_registry`. When present, the single controller
    // endpoint is served by a `Multiplexer` that fronts both runtimes: it routes each command to
    // the owning runtime and merges their responses. When absent, the endpoint is served directly
    // by the maintenance client builder, byte-unchanged from the single-runtime deployment.
    //
    // Shared fate: this runtime's worker and reader threads are covered by the same process-global
    // panic hook `install_enhanced_handler` installs at the top of `main`, before this `serve`
    // call. A panic on any of its threads aborts the whole process, exactly as it would for the
    // maintenance runtime. That is what bounds an interactive import's read hold (task 2f) to the
    // life of the replica without a separate lease-expiry mechanism: there is no way for this
    // runtime to wedge or half-fail while the rest of the process, and the maintenance runtime's
    // read holds, continue on.
    if let Some(interactive_config) = interactive_compute_timely_config {
        let interactive_compute_client_builder = mz_compute::server::serve(
            interactive_config,
            ComputeRuntimeRole::Interactive,
            &metrics_registry,
            Arc::clone(&persist_clients),
            sharing_registry.clone(),
            txns_ctx.clone(),
            Arc::clone(&tracing_handle),
            compute_context.clone(),
            // The interactive runtime does not consume storage introspection logs.
            Vec::new(),
        )
        .await?;
        info!("started interactive compute runtime");

        // Per controller connection, build a fresh multiplexer over one client from each runtime.
        let multiplexed_builder = move || {
            Multiplexer::new(
                compute_client_builder(),
                interactive_compute_client_builder(),
            )
        };
        mz_ore::task::spawn(
            || "compute_server",
            transport::serve(
                args.compute_controller_listen_addr,
                BUILD_INFO.semver_version(),
                grpc_host.clone(),
                Duration::MAX,
                multiplexed_builder,
                cluster_server_metrics.for_server("compute"),
            )
            .instrument(info_span!("ctp", name = "compute")),
        );
    } else {
        mz_ore::task::spawn(
            || "compute_server",
            transport::serve(
                args.compute_controller_listen_addr,
                BUILD_INFO.semver_version(),
                grpc_host.clone(),
                Duration::MAX,
                compute_client_builder,
                cluster_server_metrics.for_server("compute"),
            )
            .instrument(info_span!("ctp", name = "compute")),
        );
    }

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

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn timely_config(workers: usize, addresses: &[&str]) -> TimelyConfig {
        TimelyConfig {
            workers,
            process: 0,
            addresses: addresses.iter().map(|a| a.to_string()).collect(),
            ..Default::default()
        }
    }

    #[mz_ore::test]
    fn prepare_interactive_config_absent_yields_single_runtime() {
        let maintenance = timely_config(2, &["a", "b"]);
        // With no interactive config supplied, the process runs exactly one compute runtime.
        assert_eq!(
            prepare_interactive_compute_config(None, 1, &maintenance),
            None
        );
    }

    #[mz_ore::test]
    fn prepare_interactive_config_sets_process_and_accepts_equal_peers() {
        let maintenance = timely_config(2, &["a", "b"]);
        let arg = timely_config(2, &["c", "d"]);
        let got = prepare_interactive_compute_config(Some(arg), 1, &maintenance)
            .expect("interactive config present");
        // The interactive runtime adopts the maintenance runtime's process ordinal.
        assert_eq!(got.process, 1);
    }

    #[mz_ore::test]
    #[should_panic(expected = "equal number of Timely peers")]
    fn prepare_interactive_config_rejects_unequal_peers() {
        let maintenance = timely_config(2, &["a", "b"]);
        // One worker over two processes is two peers; maintenance has four. Must fail.
        let arg = timely_config(1, &["c", "d"]);
        let _ = prepare_interactive_compute_config(Some(arg), 0, &maintenance);
    }

    #[mz_ore::test]
    fn cli_parses_interactive_compute_timely_config() {
        let cfg = timely_config(1, &["127.0.0.1:2102"]).to_string();
        let base = |extra: Vec<&str>| {
            let mut argv = vec![
                "clusterd",
                "--storage-timely-config",
                &cfg,
                "--compute-timely-config",
                &cfg,
                "--process",
                "0",
                "--environment-id",
                "test-env",
                "--secrets-reader=local-file",
                "--secrets-reader-local-file-dir=/tmp",
            ];
            argv.extend(extra);
            Args::try_parse_from(argv).expect("args parse")
        };

        // Absent flag leaves the interactive config unset, so the single-runtime path is taken.
        assert!(base(vec![]).interactive_compute_timely_config.is_none());

        // The new flag parses into an `Option<TimelyConfig>`.
        let interactive = timely_config(1, &["127.0.0.1:2103"]).to_string();
        let args = base(vec!["--interactive-compute-timely-config", &interactive]);
        assert!(args.interactive_compute_timely_config.is_some());
    }

    #[mz_ore::test]
    fn test_process_ordinal_from_hostname() {
        // A StatefulSet pod name ends in the process ordinal.
        assert_eq!(
            process_ordinal_from_hostname("mz5ncn-cluster-s1-replica-s1-gen-1-0"),
            Some("0")
        );
        assert_eq!(
            process_ordinal_from_hostname("mz5ncn-cluster-s1-replica-s1-gen-1-11"),
            Some("11")
        );
        // A bare numeric hostname is its own ordinal.
        assert_eq!(process_ordinal_from_hostname("7"), Some("7"));

        // A trailing segment that is not a non-negative integer yields `None`,
        // so `CLUSTERD_PROCESS` stays unset rather than being set to a value
        // that fails to parse as the process index.
        assert_eq!(process_ordinal_from_hostname("clusterd"), None);
        assert_eq!(process_ordinal_from_hostname("replica-abc"), None);
        assert_eq!(process_ordinal_from_hostname("replica-"), None);
        assert_eq!(process_ordinal_from_hostname(""), None);
    }
}
