// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use super::provisioning::{AdapterAllocation, AdapterProvisioningConfig};

/// Reconciles committed process inventory. SQL and maintained execution run in
/// children, while this task owns only the catalog projection and backend handles.
pub async fn run(
    config: crate::Config,
    adapter_launch: AdapterProvisioningConfig,
    adapter_sizes: BTreeMap<String, AdapterAllocation>,
    adapter_plan_build: String,
) -> anyhow::Result<()> {
    let build: semver::Version = adapter_plan_build.parse()?;
    anyhow::ensure!(
        build
            .cmp_precedence(&crate::BUILD_INFO.semver_version())
            .is_eq(),
        "adapter written-plan version does not match controller release"
    );
    anyhow::ensure!(
        !crate::BUILD_INFO.is_dev() || !build.build.is_empty(),
        "development adapter plan namespace requires the build metadata from adapterd --version"
    );
    let mut catalog = super::open_catalog(&config, &adapter_sizes).await?;
    let adapters = config.controller.orchestrator.namespace("adapter");
    let replicas = config.controller.orchestrator.namespace("cluster");
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        catalog.sync_to_current_updates().await?;
        catalog
            .system_config()
            .dyncfg_updates()
            .apply(&config.system_dyncfgs);
        let mut follower = catalog.replica_config();
        // Development sibling binaries can have distinct executable build IDs.
        // The launcher supplies the actual SQL writer's namespace, not ours.
        follower.plan_build = adapter_plan_build.clone();
        let follower = serde_json::to_string(&follower)?;
        let replica_launch = mz_controller::clusters::provisioning::ReplicaProvisioningConfig {
            deploy_generation: config.controller.deploy_generation,
            connection_context: &config.controller.connection_context,
            persist_pubsub_url: &config.controller.persist_pubsub_url,
            catalog_persist_location: Some(&config.controller.persist_location),
            catalog_timestamp_oracle_url: config.timestamp_oracle_url.as_ref(),
            timestamp_oracle_clock_file: config.controller.timestamp_oracle_clock_file.as_deref(),
            catalog_follower_config: Some(&follower),
            secrets_args: &config.controller.secrets_args,
            dyncfg: &config.system_dyncfgs,
            replica_dyncfg_overrides: None,
            clusterd_image: &config.controller.clusterd_image,
            init_container_image: config.controller.init_container_image.as_deref(),
        };
        super::reconcile_replicas(&mut catalog, replicas.as_ref(), &replica_launch).await?;
        let inventory = super::reconcile_adapters(
            &mut catalog,
            adapters.as_ref(),
            &adapter_launch,
            &adapter_sizes,
        )
        .await?;
        for adapter in inventory {
            if let Some(error) = adapter.provisioning_error {
                tracing::warn!(adapter_id = %adapter.id, %error, "adapter provisioning failed");
            }
        }
    }
}
