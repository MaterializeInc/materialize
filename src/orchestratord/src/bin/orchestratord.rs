// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    future,
    net::SocketAddr,
    sync::{Arc, LazyLock},
};

use http::HeaderValue;
use k8s_openapi::{
    api::core::v1::{Affinity, ResourceRequirements, Toleration},
    apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceColumnDefinition,
};
use kube::runtime::watcher;
use mz_cloud_provider::CloudProvider;
use tracing::info;

use mz_build_info::{BuildInfo, build_info};
use mz_orchestrator_kubernetes::{KubernetesImagePullPolicy, util::create_client};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_orchestratord::{
    controller,
    k8s::register_crds,
    metrics::{self, Metrics},
    tls::DefaultCertificateSpecs,
};
use mz_ore::{
    cli::{self, CliConfig, KeyValueArg},
    error::ErrorExt,
    metrics::MetricsRegistry,
};

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

#[derive(clap::Parser)]
#[clap(name = "orchestratord", version = VERSION.as_str())]
pub struct Args {
    #[structopt(long, env = "KUBERNETES_CONTEXT", default_value = "minikube")]
    kubernetes_context: String,

    #[clap(long, default_value = "[::]:8004")]
    profiling_listen_address: SocketAddr,
    #[clap(long, default_value = "[::]:3100")]
    metrics_listen_address: SocketAddr,

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
    #[clap(long, default_value = "false")]
    disable_statement_logging: bool,

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

    let (client, namespace) = create_client(args.kubernetes_context.clone()).await?;
    register_crds(
        client.clone(),
        args.additional_crd_columns.unwrap_or_default(),
    )
    .await?;

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

    mz_ore::task::spawn(
        || "materialize controller",
        k8s_controller::Controller::namespaced_all(
            client.clone(),
            controller::materialize::Context::new(
                controller::materialize::Config {
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
                    scheduler_name: args.scheduler_name,
                    enable_security_context: args.enable_security_context,
                    enable_internal_statement_logging: args.enable_internal_statement_logging,
                    disable_statement_logging: args.disable_statement_logging,
                    orchestratord_pod_selector_labels: args.orchestratord_pod_selector_labels,
                    environmentd_node_selector: args.environmentd_node_selector,
                    environmentd_affinity: args.environmentd_affinity,
                    environmentd_tolerations: args.environmentd_tolerations,
                    environmentd_default_resources: args.environmentd_default_resources,
                    clusterd_node_selector: args.clusterd_node_selector,
                    clusterd_affinity: args.clusterd_affinity,
                    clusterd_tolerations: args.clusterd_tolerations,
                    balancerd_node_selector: args.balancerd_node_selector,
                    balancerd_affinity: args.balancerd_affinity,
                    balancerd_tolerations: args.balancerd_tolerations,
                    balancerd_default_resources: args.balancerd_default_resources,
                    console_node_selector: args.console_node_selector,
                    console_affinity: args.console_affinity,
                    console_tolerations: args.console_tolerations,
                    console_default_resources: args.console_default_resources,
                    image_pull_policy: args.image_pull_policy,
                    network_policies_internal_enabled: args.network_policies_internal_enabled,
                    network_policies_ingress_enabled: args.network_policies_ingress_enabled,
                    network_policies_ingress_cidrs: args.network_policies_ingress_cidrs,
                    network_policies_egress_enabled: args.network_policies_egress_enabled,
                    network_policies_egress_cidrs: args.network_policies_egress_cidrs,
                    environmentd_cluster_replica_sizes: args.environmentd_cluster_replica_sizes,
                    bootstrap_default_cluster_replica_size: args
                        .bootstrap_default_cluster_replica_size,
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
                    balancerd_sql_port: args.balancerd_sql_port,
                    balancerd_http_port: args.balancerd_http_port,
                    balancerd_internal_http_port: args.balancerd_internal_http_port,
                    console_http_port: args.console_http_port,
                    default_certificate_specs: args.default_certificate_specs,
                    disable_license_key_checks: args.disable_license_key_checks,
                    tracing: args.tracing,
                    orchestratord_namespace: namespace,
                },
                Arc::clone(&metrics),
                client.clone(),
            )
            .await,
            watcher::Config::default().timeout(29),
        )
        .run(),
    );

    info!("All tasks started successfully.");

    future::pending().await
}
