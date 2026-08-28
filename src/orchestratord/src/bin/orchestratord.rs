// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    env, future,
    net::SocketAddr,
    pin::pin,
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum_server::tls_rustls::RustlsConfig;
use futures::future::{Either, join4, select};
use http::HeaderValue;
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{
            Affinity, ConfigMap, ResourceRequirements, Service, ServiceAccount, Toleration,
        },
        networking::v1::NetworkPolicy,
        rbac::v1::{Role, RoleBinding},
    },
    apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceColumnDefinition, CustomResourceDefinition,
    },
};
use kube::{Api, api::ListParams, runtime::watcher};
use tokio::signal::unix::{SignalKind, signal};

use mz_cloud_provider::CloudProvider;
use mz_cloud_resources::crd::generated::cert_manager::certificates::Certificate;
use tracing::info;

use mz_build_info::{BuildInfo, build_info};
use mz_orchestrator_kubernetes::{KubernetesImagePullPolicy, util::create_client};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_orchestratord::{
    controller, gcp_node_upgrade,
    k8s::{ConversionWebhookConfig, register_crds},
    metrics::{self, Metrics},
    tls::DefaultCertificateSpecs,
    webhook,
};
use mz_ore::{
    cli::{self, CliConfig, KeyValueArg},
    error::ErrorExt,
    metrics::MetricsRegistry,
};

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

/// How long to spend releasing the leadership lease during shutdown. Releasing
/// is best-effort, since the lease expires on its own anyway, and it must be
/// bounded because it precedes the webhook drain: the kube client's default
/// read timeout is far longer than the termination grace period, so an
/// unreachable API server would otherwise consume the whole grace period here
/// and leave no time to drain.
const LEASE_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to keep accepting conversion webhook requests after receiving
/// SIGTERM. Kubernetes removes the pod from the webhook service's endpoints
/// asynchronously, so requests are still routed here for a short while after
/// the signal, and refusing them would fail writes to Materialize resources.
const WEBHOOK_SHUTDOWN_DELAY: Duration = Duration::from_secs(5);

/// How long to wait for in-flight conversion webhook requests to finish once
/// the server has stopped accepting connections.
///
/// This, [`LEASE_RELEASE_TIMEOUT`] and [`WEBHOOK_SHUTDOWN_DELAY`] run in
/// sequence during shutdown, so their sum must stay well inside the pod's
/// termination grace period, otherwise Kubernetes kills the process mid-drain.
/// Our Helm chart sets that grace period explicitly to keep the two in sync.
const WEBHOOK_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(clap::Parser)]
#[clap(name = "orchestratord", version = VERSION.as_str())]
pub struct Args {
    #[structopt(long, env = "KUBERNETES_CONTEXT", default_value = "minikube")]
    kubernetes_context: String,

    #[clap(long, default_value = "[::]:8004")]
    profiling_listen_address: SocketAddr,
    #[clap(long, default_value = "[::]:3100")]
    metrics_listen_address: SocketAddr,
    #[clap(long, default_value = "[::]:8001")]
    webhook_listen_address: SocketAddr,

    /// Whether to install the v1 version of the Materialize CRD and the
    /// conversion webhook between v1 and v1alpha1. When false, only the
    /// v1alpha1 version is installed and the webhook server is not started.
    #[clap(long)]
    install_v1_crd: bool,
    /// Required when --install-v1-crd is set.
    #[clap(long, required_if_eq("install_v1_crd", "true"))]
    webhook_service_name: Option<String>,
    /// Required when --install-v1-crd is set.
    #[clap(long, required_if_eq("install_v1_crd", "true"))]
    webhook_service_namespace: Option<String>,
    #[clap(long, default_value = "8001")]
    webhook_service_port: u16,
    #[clap(long, default_value = "/etc/tls/ca.crt")]
    tls_ca: String,
    #[clap(long, default_value = "/etc/tls/tls.crt")]
    tls_cert: String,
    #[clap(long, default_value = "/etc/tls/tls.key")]
    tls_key: String,
    /// How often to reload the webhook TLS serving certificate from disk and,
    /// when the CA changes, refresh the conversion webhook's CA bundle. The
    /// certificate is rotated out-of-band (e.g. by cert-manager), so this must
    /// be short enough that rotations are picked up before the old certificate
    /// expires.
    #[clap(long, default_value = "1h", value_parser = humantime::parse_duration)]
    webhook_cert_reload_interval: Duration,

    /// Identity for the controllers' leader election, which must be unique per
    /// replica. Replicas sharing an identity each mistake the others' lease
    /// renewals for their own and all act as leader simultaneously, with no
    /// error to indicate it.
    ///
    /// Defaults to `$HOSTNAME`, which in a pod is the pod name unless the pod
    /// sets `spec.hostname`, and then to a random identity so that running
    /// outside of a pod works too.
    #[clap(long, env = "LEADER_ELECTION_IDENTITY")]
    leader_election_identity: Option<String>,

    #[clap(long)]
    cloud_provider: CloudProvider,
    #[clap(long)]
    region: String,
    #[clap(long)]
    create_balancers: bool,
    #[clap(long)]
    create_console: bool,
    #[clap(long)]
    helm_chart_version: Option<String>,
    #[clap(long, default_value = "kubernetes")]
    secrets_controller: String,
    #[clap(long)]
    collect_pod_metrics: bool,
    #[clap(long)]
    enable_prometheus_scrape_annotations: bool,
    #[clap(long)]
    disable_authentication: bool,

    #[clap(long)]
    segment_api_key: Option<String>,
    #[clap(long)]
    segment_client_side: bool,

    #[clap(long)]
    console_image_tag_default: String,
    #[clap(long)]
    console_image_tag_map: Vec<KeyValueArg<String, String>>,

    /// Trigger rollouts of Materialize instances when GKE upgrades the node
    /// pools underneath them. Requires `--install-v1-crd` and
    /// `--cloud-provider=gcp`, and the watched node pools should be
    /// configured to use the blue-green upgrade strategy.
    #[clap(long)]
    enable_gcp_node_upgrade_rollout_trigger: bool,
    /// The Pub/Sub subscription receiving GKE cluster notifications for this
    /// cluster, in `projects/{project}/subscriptions/{subscription}` form.
    /// Required when `--enable-gcp-node-upgrade-rollout-trigger` is set.
    #[clap(
        long,
        required_if_eq("enable_gcp_node_upgrade_rollout_trigger", "true")
    )]
    gcp_node_upgrade_notification_subscription: Option<String>,
    /// The name of the GKE cluster this orchestratord is running in.
    /// Required when `--enable-gcp-node-upgrade-rollout-trigger` is set.
    #[clap(
        long,
        required_if_eq("enable_gcp_node_upgrade_rollout_trigger", "true")
    )]
    gcp_cluster_name: Option<String>,
    /// The location (region or zone) of the GKE cluster this orchestratord
    /// is running in. Required when
    /// `--enable-gcp-node-upgrade-rollout-trigger` is set.
    #[clap(
        long,
        required_if_eq("enable_gcp_node_upgrade_rollout_trigger", "true")
    )]
    gcp_cluster_location: Option<String>,
    /// A node pool to watch for GKE node upgrades; may be passed multiple
    /// times. When not passed, all node pools are watched.
    #[clap(long = "gcp-node-upgrade-watched-node-pool")]
    gcp_node_upgrade_watched_node_pools: Vec<String>,

    #[clap(long)]
    aws_account_id: Option<String>,
    #[clap(long)]
    environmentd_iam_role_arn: Option<String>,
    #[clap(long)]
    environmentd_connection_role_arn: Option<String>,
    #[clap(long)]
    aws_secrets_controller_tags: Vec<String>,
    #[clap(long)]
    environmentd_availability_zones: Option<Vec<String>>,

    #[clap(long)]
    ephemeral_volume_class: Option<String>,
    #[clap(long)]
    scheduler_name: Option<String>,
    #[clap(long)]
    enable_security_context: bool,
    #[clap(long)]
    enable_internal_statement_logging: bool,
    /// Overrides environmentd's default `statement_logging_max_sample_rate`. A
    /// rate of 0 disables statement logging entirely. Leave unset to keep
    /// environmentd's own default.
    #[clap(long, value_parser = parse_sample_rate)]
    statement_logging_max_sample_rate: Option<f64>,
    /// Overrides environmentd's default `statement_logging_target_data_rate`,
    /// in bytes per second. Unlike the sample rate, this caps the sustained
    /// volume statement logging writes. Leave unset to keep environmentd's own
    /// default.
    #[clap(long, value_parser = parse_data_rate)]
    statement_logging_target_data_rate: Option<usize>,

    #[clap(long)]
    orchestratord_pod_selector_labels: Vec<KeyValueArg<String, String>>,
    #[clap(long)]
    environmentd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, value_parser = parse_affinity)]
    environmentd_affinity: Option<Affinity>,
    #[clap(long = "environmentd-toleration", value_parser = parse_tolerations)]
    environmentd_tolerations: Option<Vec<Toleration>>,
    #[clap(long, value_parser = parse_resources)]
    environmentd_default_resources: Option<ResourceRequirements>,
    #[clap(long)]
    clusterd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, value_parser = parse_affinity)]
    clusterd_affinity: Option<Affinity>,
    #[clap(long = "clusterd-toleration", value_parser = parse_tolerations)]
    clusterd_tolerations: Option<Vec<Toleration>>,
    #[clap(long)]
    balancerd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, value_parser = parse_affinity)]
    balancerd_affinity: Option<Affinity>,
    #[clap(long = "balancerd-toleration", value_parser = parse_tolerations)]
    balancerd_tolerations: Option<Vec<Toleration>>,
    #[clap(long, value_parser = parse_resources)]
    balancerd_default_resources: Option<ResourceRequirements>,
    #[clap(long)]
    console_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, value_parser = parse_affinity)]
    console_affinity: Option<Affinity>,
    #[clap(long = "console-toleration", value_parser = parse_tolerations)]
    console_tolerations: Option<Vec<Toleration>>,
    #[clap(long, value_parser = parse_resources)]
    console_default_resources: Option<ResourceRequirements>,
    #[clap(long, default_value = "always", value_enum)]
    image_pull_policy: KubernetesImagePullPolicy,
    #[clap(long)]
    network_policies_internal_enabled: bool,
    #[clap(long)]
    network_policies_ingress_enabled: bool,
    #[clap(long)]
    network_policies_ingress_cidrs: Vec<String>,
    #[clap(long)]
    network_policies_egress_enabled: bool,
    #[clap(long)]
    network_policies_egress_cidrs: Vec<String>,

    #[clap(long)]
    environmentd_cluster_replica_sizes: Option<String>,
    #[clap(long)]
    bootstrap_default_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_system_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_probe_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_support_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_catalog_server_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_analytics_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_system_cluster_replication_factor: Option<u32>,
    #[clap(long)]
    bootstrap_builtin_probe_cluster_replication_factor: Option<u32>,
    #[clap(long)]
    bootstrap_builtin_support_cluster_replication_factor: Option<u32>,
    #[clap(long)]
    bootstrap_builtin_analytics_cluster_replication_factor: Option<u32>,

    #[clap(
        long,
        default_values = &["http://local.dev.materialize.com:3000", "http://local.mtrlz.com:3000", "http://localhost:3000", "https://staging.console.materialize.com"],
    )]
    environmentd_allowed_origins: Vec<HeaderValue>,
    #[clap(long, default_value = "https://console.materialize.com")]
    internal_console_proxy_url: String,

    #[clap(long, default_value = "6875")]
    environmentd_sql_port: u16,
    #[clap(long, default_value = "6876")]
    environmentd_http_port: u16,
    #[clap(long, default_value = "6877")]
    environmentd_internal_sql_port: u16,
    #[clap(long, default_value = "6878")]
    environmentd_internal_http_port: u16,
    #[clap(long, default_value = "6879")]
    environmentd_internal_persist_pubsub_port: u16,

    #[clap(long, default_value = "6875")]
    balancerd_sql_port: u16,
    #[clap(long, default_value = "6876")]
    balancerd_http_port: u16,
    #[clap(long, default_value = "8080")]
    balancerd_internal_http_port: u16,

    #[clap(long, default_value = "8080")]
    console_http_port: u16,

    #[clap(long, default_value = "{}")]
    default_certificate_specs: DefaultCertificateSpecs,

    #[clap(long, hide = true)]
    disable_license_key_checks: bool,

    #[clap(flatten)]
    tracing: TracingCliArgs,

    #[clap(long, hide = true, value_parser(parse_crd_columns))]
    additional_crd_columns: Option<std::vec::Vec<CustomResourceColumnDefinition>>,
}

fn parse_affinity(s: &str) -> anyhow::Result<Affinity> {
    Ok(serde_json::from_str(s)?)
}

fn parse_tolerations(s: &str) -> anyhow::Result<Toleration> {
    Ok(serde_json::from_str(s)?)
}

fn parse_resources(s: &str) -> anyhow::Result<ResourceRequirements> {
    Ok(serde_json::from_str(s)?)
}

fn parse_crd_columns(val: &str) -> Result<Vec<CustomResourceColumnDefinition>, serde_json::Error> {
    serde_json::from_str(val)
}

/// Rejects rates environmentd's `NUMERIC_BOUNDED_0_1_INCLUSIVE` constraint
/// would reject, which it does by refusing to open its catalog. Validating here
/// surfaces the mistake where the rate was configured.
fn parse_sample_rate(s: &str) -> anyhow::Result<f64> {
    let rate: f64 = s.parse()?;
    if !(0.0..=1.0).contains(&rate) {
        anyhow::bail!("sample rate must be between 0 and 1, got {rate}");
    }
    Ok(rate)
}

/// Rejects a rate of 0. The token bucket starts empty and refills at this rate,
/// so 0 throttles every statement forever rather than disabling logging
/// legibly. Use the sample rate to opt out instead.
fn parse_data_rate(s: &str) -> anyhow::Result<usize> {
    let rate: usize = s.parse()?;
    if rate == 0 {
        anyhow::bail!("target data rate must be greater than 0");
    }
    Ok(rate)
}

#[tokio::main]
async fn main() {
    mz_ore::panic::install_enhanced_handler();

    let args = cli::parse_args(CliConfig {
        env_prefix: Some("ORCHESTRATORD_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args).await {
        panic!("orchestratord: fatal: {}", err.display_with_causes());
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    // Installed before anything else, because the default action for SIGTERM
    // kills the process outright. The conversion webhook server starts below,
    // and the pod's readiness probe only checks that server, so this pod can be
    // in the webhook service's endpoints well before the rest of startup
    // finishes. Without a handler in place, a SIGTERM in that window would drop
    // the conversion requests already in flight. Tokio records a signal
    // received before the first `recv()`, so one arriving during startup is
    // still handled once the shutdown path below is reached.
    //
    // NOTE: installing the handler does not abort startup. A SIGTERM during
    // `register_crds`, which is bounded at 120 seconds, is only acted on once
    // startup finishes, which can outlast the pod's termination grace period.
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

    let metrics_registry = MetricsRegistry::new();
    args.tracing
        .configure_tracing(
            StaticTracingConfig {
                service_name: "orchestratord",
                build_info: BUILD_INFO,
            },
            metrics_registry.clone(),
        )
        .await?;

    let metrics = Arc::new(Metrics::register_into(&metrics_registry));

    let tls_cert = args.tls_cert;
    let tls_key = args.tls_key;
    let tls_ca = args.tls_ca;
    // `reload_config` is the TLS config to reload rotated certificates into,
    // and `webhook_server` is what shutdown uses to drain the server. Both are
    // set only when the conversion webhook server is running.
    let (reload_config, webhook_server) = if args.install_v1_crd {
        // Pin the rustls crypto provider to aws-lc-rs. `RustlsConfig` builds its
        // `ServerConfig` via `ServerConfig::builder()`, which resolves the
        // process-default provider. Installing it explicitly keeps the choice
        // deterministic even in workspace builds where rustls' `ring` feature is
        // also enabled by another crate (with both features on, rustls cannot
        // pick a default on its own and would otherwise panic).
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("installing the aws-lc-rs crypto provider should not fail");
        let config = RustlsConfig::from_pem_file(&tls_cert, &tls_key)
            .await
            .unwrap();
        let reload_config = config.clone();
        let webhook_listen_address = args.webhook_listen_address;

        let webhook_handle = axum_server::Handle::new();
        let webhook_task = mz_ore::task::spawn(|| "webhook server", {
            let webhook_handle = webhook_handle.clone();
            async move {
                if let Err(e) = axum_server::bind_rustls(webhook_listen_address, config)
                    .handle(webhook_handle)
                    .serve(webhook::router().into_make_service())
                    .await
                {
                    panic!("webhook server failed: {}", e.display_with_causes());
                }
            }
        });

        (Some(reload_config), Some((webhook_handle, webhook_task)))
    } else {
        (None, None)
    };

    let (client, namespace) = create_client(args.kubernetes_context.clone()).await?;
    let additional_crd_columns = args.additional_crd_columns.unwrap_or_default();
    let conversion_webhook = args.install_v1_crd.then(|| ConversionWebhookConfig {
        service_name: args
            .webhook_service_name
            .expect("clap requires --webhook-service-name with --install-v1-crd"),
        service_namespace: args
            .webhook_service_namespace
            .expect("clap requires --webhook-service-namespace with --install-v1-crd"),
        service_port: args.webhook_service_port,
        ca_cert_path: tls_ca.clone(),
    });
    register_crds(
        client.clone(),
        additional_crd_columns.clone(),
        conversion_webhook.clone(),
    )
    .await?;

    // Periodically reload the webhook serving certificate from disk, and
    // refresh the conversion webhook's CA bundle whenever the CA changes.
    //
    // The certificate is rotated out-of-band (e.g. by cert-manager). The
    // serving certificate is signed by a stable root CA, so routine rotations
    // reuse the same CA and the CA bundle registered into the CRD at startup
    // keeps working. But if the CA itself rotates (e.g. on root CA renewal),
    // that startup CA bundle would not trust the served certificate, and the
    // Kubernetes API server would reject every conversion request. Refreshing
    // the CA bundle when the CA changes keeps the webhook working across CA
    // rotations.
    if let Some(reload_config) = reload_config {
        let conversion_webhook = conversion_webhook
            .expect("conversion webhook config is set whenever the webhook server is started");
        let reload_interval = args.webhook_cert_reload_interval;
        let client = client.clone();
        let additional_crd_columns = additional_crd_columns.clone();
        mz_ore::task::spawn(|| "webhook certificate reload", async move {
            let mut last_ca = tokio::fs::read(&tls_ca).await.ok();
            let mut interval = tokio::time::interval(reload_interval);
            // The first tick completes immediately; skip it so we don't
            // re-register the CRDs we just registered above.
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(err) = reload_config
                    .reload_from_pem_file(&tls_cert, &tls_key)
                    .await
                {
                    tracing::error!("failed to reload webhook TLS certificate: {err}");
                    continue;
                }
                let current_ca = match tokio::fs::read(&tls_ca).await {
                    Ok(ca) => ca,
                    Err(err) => {
                        tracing::error!("failed to read webhook CA certificate: {err}");
                        continue;
                    }
                };
                if last_ca.as_deref() == Some(current_ca.as_slice()) {
                    continue;
                }
                // The CA changed, meaning the certificate was rotated. Re-register
                // the CRDs so the conversion webhook's caBundle matches the
                // newly-served certificate.
                match register_crds(
                    client.clone(),
                    additional_crd_columns.clone(),
                    Some(conversion_webhook.clone()),
                )
                .await
                {
                    Ok(()) => {
                        tracing::info!(
                            "refreshed conversion webhook CA bundle after certificate rotation"
                        );
                        last_ca = Some(current_ca);
                    }
                    Err(err) => {
                        tracing::error!(
                            "failed to refresh conversion webhook CA bundle after rotation: {err}"
                        );
                    }
                }
            }
        });
    }

    let crd_api: Api<CustomResourceDefinition> = Api::all(client.clone());
    let crds = crd_api.list(&ListParams::default()).await?;
    let has_cert_manager = crds
        .iter()
        .any(|crd| crd.spec.group == "cert-manager.io" && crd.spec.names.kind == "Certificate");

    {
        let router = mz_prof_http::router(&BUILD_INFO);
        let address = args.profiling_listen_address.clone();
        mz_ore::task::spawn(|| "profiling API internal web server", async move {
            if let Err(e) = axum::serve(
                tokio::net::TcpListener::bind(&address).await.unwrap(),
                router.into_make_service(),
            )
            .await
            {
                panic!(
                    "profiling API internal web server failed: {}",
                    e.display_with_causes()
                );
            }
        });
    }

    {
        let router = metrics::router(metrics_registry.clone());
        let address = args.metrics_listen_address.clone();
        mz_ore::task::spawn(|| "metrics server", async move {
            if let Err(e) = axum::serve(
                tokio::net::TcpListener::bind(&address).await.unwrap(),
                router.into_make_service(),
            )
            .await
            {
                panic!("metrics server failed: {}", e.display_with_causes());
            }
        });
    }

    // Validated and built here rather than where the watcher runs, so that a
    // misconfiguration fails startup instead of only surfacing once this
    // replica wins the election. The watcher itself runs under the leadership
    // lease, since it triggers rollouts of Materialize instances and only one
    // replica may do that.
    let gcp_node_upgrade_config = if args.enable_gcp_node_upgrade_rollout_trigger {
        anyhow::ensure!(
            args.cloud_provider == CloudProvider::Gcp,
            "--enable-gcp-node-upgrade-rollout-trigger requires --cloud-provider=gcp"
        );
        anyhow::ensure!(
            args.install_v1_crd,
            "--enable-gcp-node-upgrade-rollout-trigger requires --install-v1-crd, since \
             rollouts are triggered through the v1 Materialize CRD"
        );
        Some(gcp_node_upgrade::Config::new(
            args.gcp_node_upgrade_notification_subscription
                .clone()
                .expect("clap requires --gcp-node-upgrade-notification-subscription"),
            args.gcp_cluster_name
                .clone()
                .expect("clap requires --gcp-cluster-name"),
            args.gcp_cluster_location
                .clone()
                .expect("clap requires --gcp-cluster-location"),
            args.gcp_node_upgrade_watched_node_pools.clone(),
        )?)
    } else {
        None
    };

    // Leader election allows running multiple replicas of orchestratord
    // (e.g. to avoid downtime of the conversion webhook during rollouts)
    // while ensuring that the controllers reconcile on only one replica at
    // a time. A single lease guards all of the controllers, so that they
    // can't end up scattered across replicas.
    //
    // See `Args::leader_election_identity` for what the identity must satisfy.
    // Our Helm chart sets it from the pod name via the downward API.
    let leader_election_identity = args
        .leader_election_identity
        .or_else(|| env::var("HOSTNAME").ok())
        .unwrap_or_else(|| format!("orchestratord-{}", uuid::Uuid::new_v4()));
    let leader_election = k8s_controller::LeaderElection::new(
        client.clone(),
        &namespace,
        "orchestratord",
        &leader_election_identity,
    );

    // Each of these is rebuilt every time this replica wins the election, since
    // running a controller consumes it and we rejoin the election after losing
    // the lease.
    let make_materialize_controller = {
        let client = client.clone();
        let metrics = Arc::clone(&metrics);
        let config = controller::materialize::Config {
            cloud_provider: args.cloud_provider,
            region: args.region,
            create_balancers: args.create_balancers,
            create_console: args.create_console,
            helm_chart_version: args.helm_chart_version,
            secrets_controller: args.secrets_controller,
            collect_pod_metrics: args.collect_pod_metrics,
            enable_prometheus_scrape_annotations: args.enable_prometheus_scrape_annotations,
            segment_api_key: args.segment_api_key,
            segment_client_side: args.segment_client_side,
            console_image_tag_default: args.console_image_tag_default,
            console_image_tag_map: args.console_image_tag_map,
            aws_account_id: args.aws_account_id,
            environmentd_iam_role_arn: args.environmentd_iam_role_arn,
            environmentd_connection_role_arn: args.environmentd_connection_role_arn,
            aws_secrets_controller_tags: args.aws_secrets_controller_tags,
            environmentd_availability_zones: args.environmentd_availability_zones,
            ephemeral_volume_class: args.ephemeral_volume_class,
            scheduler_name: args.scheduler_name.clone(),
            enable_security_context: args.enable_security_context,
            enable_internal_statement_logging: args.enable_internal_statement_logging,
            statement_logging_max_sample_rate: args.statement_logging_max_sample_rate,
            statement_logging_target_data_rate: args.statement_logging_target_data_rate,
            orchestratord_pod_selector_labels: args.orchestratord_pod_selector_labels,
            environmentd_node_selector: args.environmentd_node_selector,
            environmentd_affinity: args.environmentd_affinity,
            environmentd_tolerations: args.environmentd_tolerations,
            environmentd_default_resources: args.environmentd_default_resources,
            clusterd_node_selector: args.clusterd_node_selector,
            clusterd_affinity: args.clusterd_affinity,
            clusterd_tolerations: args.clusterd_tolerations,
            image_pull_policy: args.image_pull_policy,
            network_policies_internal_enabled: args.network_policies_internal_enabled,
            network_policies_ingress_enabled: args.network_policies_ingress_enabled,
            network_policies_ingress_cidrs: args.network_policies_ingress_cidrs.clone(),
            network_policies_egress_enabled: args.network_policies_egress_enabled,
            network_policies_egress_cidrs: args.network_policies_egress_cidrs,
            environmentd_cluster_replica_sizes: args.environmentd_cluster_replica_sizes,
            bootstrap_default_cluster_replica_size: args.bootstrap_default_cluster_replica_size,
            bootstrap_builtin_system_cluster_replica_size: args
                .bootstrap_builtin_system_cluster_replica_size,
            bootstrap_builtin_probe_cluster_replica_size: args
                .bootstrap_builtin_probe_cluster_replica_size,
            bootstrap_builtin_support_cluster_replica_size: args
                .bootstrap_builtin_support_cluster_replica_size,
            bootstrap_builtin_catalog_server_cluster_replica_size: args
                .bootstrap_builtin_catalog_server_cluster_replica_size,
            bootstrap_builtin_analytics_cluster_replica_size: args
                .bootstrap_builtin_analytics_cluster_replica_size,
            bootstrap_builtin_system_cluster_replication_factor: args
                .bootstrap_builtin_system_cluster_replication_factor,
            bootstrap_builtin_probe_cluster_replication_factor: args
                .bootstrap_builtin_probe_cluster_replication_factor,
            bootstrap_builtin_support_cluster_replication_factor: args
                .bootstrap_builtin_support_cluster_replication_factor,
            bootstrap_builtin_analytics_cluster_replication_factor: args
                .bootstrap_builtin_analytics_cluster_replication_factor,
            environmentd_allowed_origins: args.environmentd_allowed_origins,
            internal_console_proxy_url: args.internal_console_proxy_url,
            environmentd_sql_port: args.environmentd_sql_port,
            environmentd_http_port: args.environmentd_http_port,
            environmentd_internal_sql_port: args.environmentd_internal_sql_port,
            environmentd_internal_http_port: args.environmentd_internal_http_port,
            environmentd_internal_persist_pubsub_port: args
                .environmentd_internal_persist_pubsub_port,
            default_certificate_specs: args.default_certificate_specs.clone(),
            disable_license_key_checks: args.disable_license_key_checks,
            tracing: args.tracing,
            orchestratord_namespace: namespace,
        };
        move || {
            k8s_controller::Controller::namespaced_all(
                client.clone(),
                controller::materialize::Context::new(config.clone(), Arc::clone(&metrics)),
                watcher::Config::default().timeout(29),
            )
            .with_controller(|controller| {
                let controller = controller
                    .owns(
                        Api::<NetworkPolicy>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                    .owns(
                        Api::<ServiceAccount>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                    .owns(
                        Api::<Role>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                    .owns(
                        Api::<RoleBinding>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    );
                if has_cert_manager {
                    controller.owns(
                        Api::<Certificate>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                } else {
                    controller
                }
            })
        }
    };
    let make_balancer_controller = {
        let client = client.clone();
        let config = controller::balancer::Config {
            enable_security_context: args.enable_security_context,
            enable_prometheus_scrape_annotations: args.enable_prometheus_scrape_annotations,
            image_pull_policy: args.image_pull_policy,
            scheduler_name: args.scheduler_name.clone(),
            balancerd_node_selector: args.balancerd_node_selector,
            balancerd_affinity: args.balancerd_affinity,
            balancerd_tolerations: args.balancerd_tolerations,
            balancerd_default_resources: args.balancerd_default_resources,
            default_certificate_specs: args.default_certificate_specs.clone(),
            environmentd_sql_port: args.environmentd_sql_port,
            environmentd_http_port: args.environmentd_http_port,
            balancerd_sql_port: args.balancerd_sql_port,
            balancerd_http_port: args.balancerd_http_port,
            balancerd_internal_http_port: args.balancerd_internal_http_port,
        };
        move || {
            k8s_controller::Controller::namespaced_all(
                client.clone(),
                controller::balancer::Context::new(config.clone()),
                watcher::Config::default().timeout(29),
            )
            .with_controller(|controller| {
                let controller = controller
                    .owns(
                        Api::<Deployment>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                    .owns(
                        Api::<Service>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    );
                if has_cert_manager {
                    controller.owns(
                        Api::<Certificate>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                } else {
                    controller
                }
            })
        }
    };
    let make_console_controller = {
        let client = client.clone();
        let config = controller::console::Config {
            enable_security_context: args.enable_security_context,
            enable_prometheus_scrape_annotations: args.enable_prometheus_scrape_annotations,
            image_pull_policy: args.image_pull_policy,
            scheduler_name: args.scheduler_name,
            console_node_selector: args.console_node_selector,
            console_affinity: args.console_affinity,
            console_tolerations: args.console_tolerations,
            console_default_resources: args.console_default_resources,
            network_policies_ingress_enabled: args.network_policies_ingress_enabled,
            network_policies_ingress_cidrs: args.network_policies_ingress_cidrs,
            default_certificate_specs: args.default_certificate_specs,
            console_http_port: args.console_http_port,
            balancerd_http_port: args.balancerd_http_port,
        };
        move || {
            k8s_controller::Controller::namespaced_all(
                client.clone(),
                controller::console::Context::new(config.clone()),
                watcher::Config::default().timeout(29),
            )
            .with_controller(|controller| {
                let controller = controller
                    .owns(
                        Api::<Deployment>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                    .owns(
                        Api::<Service>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                    .owns(
                        Api::<NetworkPolicy>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                    .owns(
                        Api::<ConfigMap>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    );
                if has_cert_manager {
                    controller.owns(
                        Api::<Certificate>::all(client.clone()),
                        watcher::Config::default()
                            .labels("materialize.cloud/mz-resource-id")
                            .timeout(29),
                    )
                } else {
                    controller
                }
            })
        }
    };
    let make_gcp_node_upgrade_watcher = {
        let client = client.clone();
        move || {
            let client = client.clone();
            let config = gcp_node_upgrade_config.clone();
            async move {
                match config {
                    Some(config) => gcp_node_upgrade::run(client, config).await,
                    // The trigger is disabled, so there is nothing to watch.
                    // This future must still never complete, since completing
                    // is how the joined futures below report that they stopped.
                    None => future::pending().await,
                }
            }
        }
    };
    mz_ore::task::spawn(|| "controllers", async move {
        loop {
            // All of the controllers, and the node upgrade watcher, run under
            // the single leadership lease, on whichever replica holds it. The
            // watcher belongs here because it triggers rollouts, which only the
            // leader may do. None of these futures ever complete on their own.
            //
            // `with_lease` polls this future only once it holds the lease, so
            // building the controllers here, rather than passing them in, ties
            // both them and the metric to the moment leadership is acquired.
            //
            // Each of them runs as its own task, so that they are scheduled
            // independently and none can hold up the poll of the others, or of
            // the lease renewal that `with_lease` runs alongside them. Their
            // handles abort the tasks when dropped, so that none of them
            // outlives the lease.
            let controllers = Box::pin(leader_election.with_lease(async {
                metrics.leadership_acquired();
                join4(
                    mz_ore::task::spawn(
                        || "materialize controller",
                        make_materialize_controller().run(),
                    )
                    .abort_on_drop(),
                    mz_ore::task::spawn(|| "balancer controller", make_balancer_controller().run())
                        .abort_on_drop(),
                    mz_ore::task::spawn(|| "console controller", make_console_controller().run())
                        .abort_on_drop(),
                    mz_ore::task::spawn(
                        || "gcp node upgrade watcher",
                        make_gcp_node_upgrade_watcher(),
                    )
                    .abort_on_drop(),
                )
                .await
            }));
            match select(controllers, pin!(sigterm.recv())).await {
                Either::Left((None, _)) => {
                    // Losing the lease dropped the handles above, which aborts
                    // the in-flight reconciliations and the node upgrade watch,
                    // so we can rejoin the election in process instead of
                    // restarting. Staying up keeps this replica serving the
                    // conversion webhook.
                    //
                    // An aborted task runs until its next await point, so a
                    // reconciliation can still finish a request it had already
                    // issued. That is within the margin the lease timings
                    // leave: the renew deadline is shorter than the lease
                    // duration, so we give leadership up before another
                    // candidate is allowed to take the lease over.
                    //
                    // The leader-only gauges describe what this replica saw
                    // while it was reconciling, and this process outlives its
                    // own leadership, so leaving them set would keep publishing
                    // a stale count alongside the new leader's.
                    metrics.leadership_lost();
                    tracing::warn!("lost leadership lease, rejoining the election");
                }
                Either::Left((Some(_), _)) => {
                    // The controllers only finish if their watches end, which
                    // leaves this process unable to reconcile anything.
                    // `with_lease` released the lease before returning, so a
                    // standby has taken leadership over already. Let Kubernetes
                    // restart us rather than rejoining the election only to
                    // fall over the same way again.
                    tracing::error!("controllers exited unexpectedly, exiting");
                    std::process::exit(1);
                }
                Either::Right((_, controllers)) => {
                    // Abort the controllers before releasing the lease, so that
                    // reconciliation stops before another replica picks
                    // leadership up. Releasing hands leadership over
                    // immediately rather than making the other replicas wait
                    // for the lease to expire.
                    drop(controllers);
                    // The webhook keeps serving for a while below, so this
                    // replica is still scraped after it stops reconciling.
                    metrics.leadership_lost();
                    let _ = tokio::time::timeout(LEASE_RELEASE_TIMEOUT, leader_election.release())
                        .await;
                    if let Some((webhook_handle, webhook_task)) = webhook_server {
                        // Removing this pod from the webhook service's
                        // endpoints is not synchronous with SIGTERM, so
                        // conversion requests keep arriving for a while after
                        // it. Keep accepting them until the endpoints have
                        // caught up, then stop accepting and let the in-flight
                        // requests finish.
                        tokio::time::sleep(WEBHOOK_SHUTDOWN_DELAY).await;
                        webhook_handle.graceful_shutdown(Some(WEBHOOK_DRAIN_TIMEOUT));
                        let _ = webhook_task.await;
                    }
                    info!("received SIGTERM, shut down cleanly");
                    std::process::exit(0);
                }
            }
        }
    });

    info!("All tasks started successfully.");

    future::pending().await
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Args;

    const REQUIRED_ARGS: &[&str] = &[
        "orchestratord",
        "--cloud-provider=local",
        "--region=kind",
        "--console-image-tag-default=latest",
    ];

    #[mz_ore::test]
    fn webhook_service_args_required_with_install_v1_crd() {
        let args = Args::try_parse_from(REQUIRED_ARGS).expect("parses without webhook args");
        assert!(!args.install_v1_crd);

        assert!(
            Args::try_parse_from(REQUIRED_ARGS.iter().copied().chain(["--install-v1-crd"]))
                .is_err(),
            "--install-v1-crd should require the webhook service args"
        );

        let args = Args::try_parse_from(REQUIRED_ARGS.iter().copied().chain([
            "--install-v1-crd",
            "--webhook-service-name=orchestratord",
            "--webhook-service-namespace=materialize",
        ]))
        .expect("parses with webhook args");
        assert!(args.install_v1_crd);
        assert_eq!(args.webhook_service_name.as_deref(), Some("orchestratord"));
        assert_eq!(
            args.webhook_service_namespace.as_deref(),
            Some("materialize")
        );
    }
}
