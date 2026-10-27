// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use anyhow::{Context, ensure};
use mz_adapter::config::{SystemParameterSyncClientConfig, SystemParameterSyncConfig};
use mz_catalog::catalog::Catalog;
use mz_catalog::config::{AwsPrincipalContext, BuiltinItemMigrationConfig, StateConfig};
use mz_catalog::durable::{BootstrapArgs, CatalogTimestampOracle};
use mz_repr::Timestamp;

/// Opens a fresh deployment or reconstructs an initialized same-generation one.
///
/// Restart joins without migrations or positive snapshot publication. The launcher
/// must pair same-build children and preserve deployment configuration. This does
/// not implement recovery from interrupted fresh initialization or upgrades.
pub async fn open_catalog(config: &crate::Config) -> anyhow::Result<Catalog> {
    ensure!(
        config.joined_adapter_id.is_none(),
        "controller cannot be an adapter joiner"
    );
    ensure!(
        config.force_builtin_schema_migration.is_none(),
        "controller startup does not support forced migrations"
    );
    let persist = config
        .catalog_config
        .persist_clients
        .open(config.controller.persist_location.clone())
        .await?;
    let url = config
        .timestamp_oracle_url
        .as_ref()
        .context("controller requires a timestamp oracle URL")?;
    let oracle_config =
        mz_timestamp_oracle::TimestampOracleConfig::from_url(url, &config.metrics_registry)?;
    let now = match &config.controller.timestamp_oracle_clock_file {
        Some(path) => mz_timestamp_oracle::fixture_clock::open(path.clone())?,
        None => config.now.clone(),
    };
    let oracle = oracle_config
        .open(
            mz_storage_types::sources::Timeline::EpochMilliseconds.to_string(),
            Timestamp::MIN,
            now.clone(),
            false,
        )
        .await;
    // Catalog synchronization establishes the shared Persist format before any
    // table or expression-cache shard is opened.
    let mut openable = mz_catalog::durable::persist_backed_catalog_state(
        persist.clone(),
        config.environment_id.organization_id(),
        crate::BUILD_INFO.semver_version(),
        Some(config.controller.deploy_generation),
        Arc::clone(&config.catalog_config.metrics),
        Some(CatalogTimestampOracle::new(Arc::clone(&oracle), now)),
    )
    .await?;
    let sync_client = match (&config.launchdarkly_sdk_key, &config.config_sync_file_path) {
        (None, None) => None,
        (None, Some(path)) => Some(SystemParameterSyncClientConfig::File { path: path.clone() }),
        (Some(key), None) => Some(SystemParameterSyncClientConfig::LaunchDarkly {
            sdk_key: key.clone(),
            base_uri: config.launchdarkly_base_uri.clone(),
            now_fn: config.now.clone(),
        }),
        (Some(_), Some(_)) => {
            anyhow::bail!("cannot configure both file and LaunchDarkly synchronization")
        }
    };
    let sync_config = sync_client.map(|client| {
        SystemParameterSyncConfig::new(
            config.environment_id.clone(),
            &crate::BUILD_INFO,
            &config.metrics_registry,
            config.launchdarkly_key_map.clone(),
            client,
        )
    });
    let remote_system_parameters = mz_adapter::load_remote_system_parameters(
        &mut openable,
        sync_config,
        config.config_sync_timeout,
    )
    .await?;
    let fresh = !openable.is_initialized().await?;
    let mut storage = if fresh {
        openable
            .open(
                (config.now)().into(),
                &BootstrapArgs {
                    default_adapter_size: mz_catalog::durable::DEFAULT_ADAPTER_SIZE.into(),
                    default_cluster_replica_size: config
                        .bootstrap_default_cluster_replica_size
                        .clone(),
                    default_cluster_replication_factor: config
                        .bootstrap_default_cluster_replication_factor,
                    bootstrap_role: config.bootstrap_role.clone(),
                    cluster_replica_size_map: config.cluster_replica_sizes.clone(),
                },
            )
            .await?
    } else {
        // Joining validates generation and schema compatibility without promoting
        // the controller or running replacement migrations.
        openable.join().await?
    };
    oracle.apply_write(storage.current_upper().await).await;
    let boot_ts = oracle.write_ts().await.timestamp;
    let aws_principal_context = match (
        config.aws_account_id.clone(),
        config
            .controller
            .connection_context
            .aws_external_id_prefix
            .clone(),
    ) {
        (Some(aws_account_id), Some(aws_external_id_prefix)) => Some(AwsPrincipalContext {
            aws_account_id,
            aws_external_id_prefix,
        }),
        _ => None,
    };
    let catalog_config = mz_catalog::config::Config {
        storage,
        metrics_registry: &config.metrics_registry,
        state: StateConfig {
            unsafe_mode: config.unsafe_mode,
            all_features: config.all_features,
            build_info: &crate::BUILD_INFO,
            environment_id: config.environment_id.clone(),
            read_only: false,
            now: config.now.clone(),
            boot_ts,
            skip_migrations: false,
            cluster_replica_sizes: config.cluster_replica_sizes.clone(),
            builtin_system_cluster_config: config.bootstrap_builtin_system_cluster_config.clone(),
            builtin_catalog_server_cluster_config: config
                .bootstrap_builtin_catalog_server_cluster_config
                .clone(),
            builtin_probe_cluster_config: config.bootstrap_builtin_probe_cluster_config.clone(),
            builtin_support_cluster_config: config.bootstrap_builtin_support_cluster_config.clone(),
            builtin_analytics_cluster_config: config
                .bootstrap_builtin_analytics_cluster_config
                .clone(),
            system_parameter_defaults: config.system_parameter_defaults.clone(),
            remote_system_parameters,
            availability_zones: config.availability_zones.clone(),
            egress_addresses: config.egress_addresses.clone(),
            aws_principal_context,
            aws_privatelink_availability_zones: config
                .aws_privatelink_availability_zones
                .as_ref()
                .map(|zones| zones.iter().cloned().collect()),
            connection_context: config.controller.connection_context.clone(),
            http_host_name: config.http_host_name.clone(),
            builtin_item_migration_config: BuiltinItemMigrationConfig {
                persist_client: persist.clone(),
                read_only: false,
                force_migration: None,
            },
            persist_client: persist,
            enable_expression_cache_override: None,
            helm_chart_version: config.helm_chart_version.clone(),
            external_login_password_mz_system: config.external_login_password_mz_system.clone(),
            license_key: config.license_key.clone(),
        },
    };
    let catalog = if fresh {
        super::initialize_fresh(catalog_config, oracle.as_ref()).await?
    } else {
        Catalog::open(catalog_config).await?.catalog
    };
    ensure!(
        catalog.state().catalog_read_protection_enabled(),
        "controllerd requires catalog-following execution"
    );
    Ok(catalog)
}
