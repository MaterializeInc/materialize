// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Preflight checks for deployments.

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use mz_adapter::ResultExt;
use mz_catalog::durable::{BootstrapArgs, CatalogError, Metrics, OpenableDurableCatalogState};
use mz_ore::channel::trigger;
use mz_ore::exit;
use mz_ore::halt;
use mz_persist_client::PersistClient;
use mz_repr::Timestamp;
use mz_sql::catalog::EnvironmentId;
use tracing::info;

use crate::BUILD_INFO;
use crate::deployment::state::DeploymentState;

/// The necessary input for preflight checks.
pub struct PreflightInput {
    pub boot_ts: Timestamp,
    pub environment_id: EnvironmentId,
    pub persist_client: PersistClient,
    pub deploy_generation: u64,
    pub deployment_state: DeploymentState,
    pub openable_adapter_storage: Box<dyn OpenableDurableCatalogState>,
    pub catalog_metrics: Arc<Metrics>,
    pub caught_up_max_wait: Duration,
    pub ddl_check_interval: Duration,
    pub panic_after_timeout: bool,
    pub bootstrap_args: BootstrapArgs,
}

/// Output of preflight checks.
pub struct PreflightOutput {
    pub openable_adapter_storage: Box<dyn OpenableDurableCatalogState>,
    pub read_only: bool,
    pub caught_up_trigger: Option<trigger::Trigger>,
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
        deploy_generation,
        deployment_state,
        mut openable_adapter_storage,
        catalog_metrics,
        caught_up_max_wait,
        ddl_check_interval,
        panic_after_timeout,
        bootstrap_args,
    }: PreflightInput,
) -> Result<PreflightOutput, CatalogError> {
    info!(%deploy_generation, ?caught_up_max_wait, "performing 0dt preflight checks");

    if !openable_adapter_storage.is_initialized().await? {
        info!("catalog not initialized; booting with writes allowed");
        return Ok(PreflightOutput {
            openable_adapter_storage,
            read_only: false,
            caught_up_trigger: None,
        });
    }

    let catalog_generation = openable_adapter_storage.get_deployment_generation().await?;
    info!(%catalog_generation, "catalog initialized");
    if catalog_generation < deploy_generation {
        info!("this deployment is a new generation; booting in read only mode");

        let (caught_up_trigger, mut caught_up_receiver) = trigger::channel();

        // Spawn a background task to handle promotion to leader.
        mz_ore::task::spawn(|| "preflight_0dt", async move {
            let (initial_next_user_item_id, initial_next_replica_id) = get_next_ids(
                boot_ts,
                persist_client.clone(),
                environment_id.clone(),
                deploy_generation,
                Arc::clone(&catalog_metrics),
                bootstrap_args.clone(),
            )
            .await;

            info!(
                %initial_next_user_item_id,
                %initial_next_replica_id,
                "waiting for deployment to be caught up");

            let caught_up_max_wait_fut = async {
                tokio::time::sleep(caught_up_max_wait).await;
                ()
            };
            let mut caught_up_max_wait_fut = pin!(caught_up_max_wait_fut);

            let mut skip_catchup = deployment_state.set_catching_up();

            let mut check_ddl_changes_interval = tokio::time::interval(ddl_check_interval);
            check_ddl_changes_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut should_skip_catchup = false;
            loop {
                tokio::select! {
                    biased;

                    () = &mut skip_catchup => {
                        info!("skipping waiting for deployment to catch up due to administrator request");
                        should_skip_catchup = true;
                        break;
                    }
                    () = &mut caught_up_receiver => {
                        info!("deployment caught up");
                        break;
                    }
                    () = &mut caught_up_max_wait_fut => {
                        if panic_after_timeout {
                            panic!("not caught up within {:?}", caught_up_max_wait);
                        }
                        info!("not caught up within {:?}, proceeding now", caught_up_max_wait);
                        break;
                    }
                    _ = check_ddl_changes_interval.tick() => {
                        check_ddl_changes(
                            boot_ts,
                            persist_client.clone(),
                            environment_id.clone(),
                            deploy_generation,
                            Arc::clone(&catalog_metrics),
                            bootstrap_args.clone(),
                            initial_next_user_item_id,
                            initial_next_replica_id,
                        )
                        .await;
                    }
                }
            }

            // Check for DDL changes one last time before announcing as ready to
            // promote.
            if !should_skip_catchup {
                check_ddl_changes(
                    boot_ts,
                    persist_client.clone(),
                    environment_id.clone(),
                    deploy_generation,
                    Arc::clone(&catalog_metrics),
                    bootstrap_args.clone(),
                    initial_next_user_item_id,
                    initial_next_replica_id,
                )
                .await;
            }

            // Announce that we're ready to promote.
            let promoted = deployment_state.set_ready_to_promote();
            info!("announced as ready to promote; waiting for promotion");
            promoted.await;

            // Take over the catalog.
            info!("promoted; attempting takeover");

            // NOTE: There _is_ a window where DDL can happen in the old
            // environment, between checking above, us announcing as ready to
            // promote, and cloud giving us the go-ahead signal. Its size
            // depends on how quickly cloud will trigger promotion once we
            // report as ready.
            //
            // We could add another check here, right before cutting over, but I
            // think this requires changes in Cloud: with this additional check,
            // it can now happen that cloud gives us the promote signal but we
            // then notice there were changes and restart. Could would have to
            // notice this and give us the promote signal again, once we're
            // ready again.

            let openable_adapter_storage = mz_catalog::durable::persist_backed_catalog_state(
                persist_client.clone(),
                environment_id.organization_id(),
                BUILD_INFO.semver_version(),
                Some(deploy_generation),
                Arc::clone(&catalog_metrics),
            )
            .await
            .expect("incompatible catalog/persist version");

            let (_catalog, _audit_logs) = openable_adapter_storage
                .open(boot_ts, &bootstrap_args)
                .await
                .unwrap_or_terminate("unexpected error while fencing out old deployment");

            // Reboot as the leader.
            halt!("fenced out old deployment; rebooting as leader")
        });

        Ok(PreflightOutput {
            openable_adapter_storage,
            read_only: true,
            caught_up_trigger: Some(caught_up_trigger),
        })
    } else if catalog_generation == deploy_generation {
        info!("this deployment is the current generation; booting with writes allowed");
        Ok(PreflightOutput {
            openable_adapter_storage,
            read_only: false,
            caught_up_trigger: None,
        })
    } else {
        exit!(0, "this deployment has been fenced out");
    }
}

/// Check if there have been any DDL that create new collections or replicas,
/// restart in read-only mode if so, in order to pick up those new items and
/// start hydrating them before cutting over.
async fn check_ddl_changes(
    boot_ts: Timestamp,
    persist_client: PersistClient,
    environment_id: EnvironmentId,
    deploy_generation: u64,
    catalog_metrics: Arc<Metrics>,
    bootstrap_args: BootstrapArgs,
    initial_next_user_item_id: u64,
    initial_next_replica_id: u64,
) {
    let (next_user_item_id, next_replica_id) = get_next_ids(
        boot_ts,
        persist_client.clone(),
        environment_id.clone(),
        deploy_generation,
        Arc::clone(&catalog_metrics),
        bootstrap_args.clone(),
    )
    .await;

    tracing::info!(
        %initial_next_user_item_id,
        %initial_next_replica_id,
        %next_user_item_id,
        %next_replica_id,
        "checking if there was any relevant DDL");

    if next_user_item_id > initial_next_user_item_id || next_replica_id > initial_next_replica_id {
        halt!("there have been DDL that we need to react to; rebooting in read-only mode")
    }
}

/// Gets and returns the next user item ID and user replica ID that would be
/// allocated as of the current catalog state.
async fn get_next_ids(
    boot_ts: Timestamp,
    persist_client: PersistClient,
    environment_id: EnvironmentId,
    deploy_generation: u64,
    catalog_metrics: Arc<Metrics>,
    bootstrap_args: BootstrapArgs,
) -> (u64, u64) {
    let openable_adapter_storage = mz_catalog::durable::persist_backed_catalog_state(
        persist_client,
        environment_id.organization_id(),
        BUILD_INFO.semver_version(),
        Some(deploy_generation),
        catalog_metrics,
    )
    .await
    .expect("incompatible catalog/persist version");

    let (mut catalog, _audit_logs) = openable_adapter_storage
        .open_savepoint(boot_ts, &bootstrap_args)
        .await
        .unwrap_or_terminate("can open in savepoint mode");

    let next_user_item_id = catalog
        .get_next_user_item_id()
        .await
        .expect("can access catalog");
    let next_replica_item_id = catalog
        .get_next_user_replica_id()
        .await
        .expect("can access catalog");

    (next_user_item_id, next_replica_item_id)
}
