// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Preflight checks for deployments.

use std::sync::Arc;
use std::time::Duration;

use mz_adapter::ResultExt;
use mz_catalog::durable::{BootstrapArgs, CatalogError, Metrics, OpenableDurableCatalogState};
use mz_ore::channel::trigger;
use mz_ore::exit;
use mz_ore::halt;
use mz_persist_client::PersistClient;
use mz_sql::catalog::EnvironmentId;
use tracing::info;

use crate::deployment::state::DeploymentState;
use crate::BUILD_INFO;

/// The necessary input for preflight checks.
pub struct PreflightInput {
    pub boot_ts: u64,
    pub environment_id: EnvironmentId,
    pub persist_client: PersistClient,
    pub bootstrap_default_cluster_replica_size: String,
    pub bootstrap_role: Option<String>,
    pub deploy_generation: u64,
    pub deployment_state: DeploymentState,
    pub openable_adapter_storage: Box<dyn OpenableDurableCatalogState>,
    pub catalog_metrics: Arc<Metrics>,
    pub hydration_max_wait: Duration,
}

/// Output of preflight checks.
pub struct PreflightOutput {
    pub openable_adapter_storage: Box<dyn OpenableDurableCatalogState>,
    pub read_only: bool,
    pub clusters_hydrated_trigger: Option<trigger::Trigger>,
}

/// Perform a legacy (non-0dt) preflight check.
pub async fn preflight_legacy(
    PreflightInput {
        boot_ts,
        environment_id,
        persist_client,
        bootstrap_default_cluster_replica_size,
        bootstrap_role,
        deploy_generation,
        deployment_state,
        mut openable_adapter_storage,
        catalog_metrics,
        hydration_max_wait: _,
    }: PreflightInput,
) -> Result<Box<dyn OpenableDurableCatalogState>, CatalogError> {
    tracing::info!("Requested deploy generation {deploy_generation}");

    if !openable_adapter_storage.is_initialized().await? {
        tracing::info!("Catalog storage doesn't exist so there's no current deploy generation. We won't wait to be leader");
        return Ok(openable_adapter_storage);
    }
    let catalog_generation = openable_adapter_storage.get_deployment_generation().await?;
    tracing::info!("Found catalog generation {catalog_generation:?}");
    if catalog_generation < deploy_generation {
        tracing::info!("Catalog generation {catalog_generation:?} is less than deploy generation {deploy_generation}. Performing pre-flight checks");
        match openable_adapter_storage
            .open_savepoint(
                boot_ts.clone(),
                &BootstrapArgs {
                    default_cluster_replica_size: bootstrap_default_cluster_replica_size,
                    bootstrap_role,
                },
            )
            .await
        {
            Ok(adapter_storage) => Box::new(adapter_storage).expire().await,
            Err(CatalogError::Durable(e)) if e.can_recover_with_write_mode() => {
                // This is theoretically possible if catalog implementation A is
                // initialized, implementation B is uninitialized, and we are going to
                // migrate from A to B. The current code avoids this by always
                // initializing all implementations, regardless of the target
                // implementation. Still it's easy to protect against this and worth it in
                // case things change in the future.
                tracing::warn!("Unable to perform upgrade test because the target implementation is uninitialized");
                return Ok(mz_catalog::durable::persist_backed_catalog_state(
                    persist_client,
                    environment_id.organization_id(),
                    BUILD_INFO.semver_version(),
                    Some(deploy_generation),
                    Arc::clone(&catalog_metrics),
                )
                .await?);
            }
            Err(e) => {
                tracing::warn!(error = %e, "catalog upgrade would have failed");
                return Err(e);
            }
        }

        let promoted = deployment_state.set_ready_to_promote();

        tracing::info!("Waiting for user to promote this envd to leader");
        promoted.await;

        Ok(mz_catalog::durable::persist_backed_catalog_state(
            persist_client,
            environment_id.organization_id(),
            BUILD_INFO.semver_version(),
            Some(deploy_generation),
            Arc::clone(&catalog_metrics),
        )
        .await?)
    } else if catalog_generation == deploy_generation {
        tracing::info!("Server requested generation {deploy_generation} which is equal to catalog's generation");
        Ok(openable_adapter_storage)
    } else {
        mz_ore::halt!("Server started with requested generation {deploy_generation} but catalog was already at {catalog_generation:?}. Deploy generations must increase monotonically");
    }
}

/// Perform a 0dt preflight check.
///
/// Returns the openable adapter storage to use and whether or not to boot in
/// read only mode.
pub async fn preflight_0dt(
    PreflightInput {
        boot_ts,
        environment_id,
        persist_client,
        bootstrap_default_cluster_replica_size,
        bootstrap_role,
        deploy_generation,
        deployment_state,
        mut openable_adapter_storage,
        catalog_metrics,
        hydration_max_wait,
    }: PreflightInput,
) -> Result<PreflightOutput, CatalogError> {
    info!(%deploy_generation, ?hydration_max_wait, "performing 0dt preflight checks");

    if !openable_adapter_storage.is_initialized().await? {
        info!("catalog not initialized; booting with writes allowed");
        return Ok(PreflightOutput {
            openable_adapter_storage,
            read_only: false,
            clusters_hydrated_trigger: None,
        });
    }

    let catalog_generation = openable_adapter_storage.get_deployment_generation().await?;
    info!(%catalog_generation, "catalog initialized");
    if catalog_generation < deploy_generation {
        info!("this deployment is a new generation; booting in read only mode");

        let (clusters_hydrated_trigger, clusters_hydrated_receiver) = trigger::channel();

        // Spawn a background task to handle promotion to leader.
        mz_ore::task::spawn(|| "preflight_0dt", async move {
            info!("waiting for clusters to be hydrated");

            let hydration_max_wait_fut = async {
                tokio::time::sleep(hydration_max_wait).await;
                ()
            };

            tokio::select! {
                () = clusters_hydrated_receiver => {
                    info!("all clusters hydrated");
                }
                () = hydration_max_wait_fut => {
                    info!("not all clusters hydrated within {:?}, proceeding now", hydration_max_wait);
                }
            }

            // Announce that we're ready to promote.
            let promoted = deployment_state.set_ready_to_promote();
            info!("announced as ready to promote; waiting for promotion");
            promoted.await;

            // Take over the catalog.
            info!("promoted; attempting takeover");

            let openable_adapter_storage = mz_catalog::durable::persist_backed_catalog_state(
                persist_client.clone(),
                environment_id.organization_id(),
                BUILD_INFO.semver_version(),
                Some(deploy_generation),
                Arc::clone(&catalog_metrics),
            )
            .await
            .expect("incompatible catalog/persist version");

            openable_adapter_storage
                .open(
                    boot_ts,
                    &BootstrapArgs {
                        default_cluster_replica_size: bootstrap_default_cluster_replica_size
                            .clone(),
                        bootstrap_role: bootstrap_role.clone(),
                    },
                )
                .await
                .unwrap_or_terminate("unexpected error while fencing out old deployment");

            // Reboot as the leader.
            halt!("fenced out old deployment; rebooting as leader")
        });

        Ok(PreflightOutput {
            openable_adapter_storage,
            read_only: true,
            clusters_hydrated_trigger: Some(clusters_hydrated_trigger),
        })
    } else if catalog_generation == deploy_generation {
        info!("this deployment is the current generation; booting with writes allowed");
        Ok(PreflightOutput {
            openable_adapter_storage,
            read_only: false,
            clusters_hydrated_trigger: None,
        })
    } else {
        exit!(0, "this deployment has been fenced out");
    }
}
