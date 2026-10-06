// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Preflight checks for deployments.

use std::collections::BTreeSet;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use mz_adapter::ResultExt;
use mz_catalog::durable::{
    BootstrapArgs, CatalogError, DurableCatalogState, Metrics, OpenableDurableCatalogState,
};
use mz_controller_types::ReplicaId;
use mz_ore::channel::trigger;
use mz_ore::exit;
use mz_ore::halt;
use mz_ore::str::separated;
use mz_persist_client::PersistClient;
use mz_repr::{CatalogItemId, Timestamp};
use mz_sql::catalog::EnvironmentId;
use tokio::sync::oneshot;
use tracing::info;

use crate::BUILD_INFO;
use crate::deployment::state::DeploymentState;

/// Configuration for catching up and promoting a read-only deployment.
pub struct CatchupConfig {
    pub boot_ts: Timestamp,
    pub environment_id: EnvironmentId,
    pub persist_client: PersistClient,
    pub deploy_generation: u64,
    pub deployment_state: DeploymentState,
    pub catalog_metrics: Arc<Metrics>,
    pub caught_up_max_wait: Duration,
    pub ddl_check_interval: Duration,
    pub panic_after_timeout: bool,
    pub bootstrap_args: BootstrapArgs,
}

/// Returns whether this deployment should boot in read-only mode.
pub async fn preflight_0dt(
    openable_adapter_storage: &mut dyn OpenableDurableCatalogState,
    deploy_generation: u64,
) -> Result<bool, CatalogError> {
    if !openable_adapter_storage.is_initialized().await? {
        info!("catalog not initialized; booting with writes allowed");
        return Ok(false);
    }

    let catalog_generation = openable_adapter_storage.get_deployment_generation().await?;
    info!(%catalog_generation, %deploy_generation, "catalog initialized");
    if catalog_generation < deploy_generation {
        info!("this deployment is a new generation; booting in read only mode");
        Ok(true)
    } else if catalog_generation == deploy_generation {
        info!("this deployment is the current generation; booting with writes allowed");
        Ok(false)
    } else {
        exit!(0, "this deployment has been fenced out");
    }
}

/// Starts catching up and promoting a read-only deployment.
///
/// An administrative skip is accepted right away and promotes without waiting
/// for bootstrap. Otherwise, catch-up checks and the catch-up timeout start
/// once `bootstrapped` yields the ID baseline, which must come from the
/// savepoint used to bootstrap the adapter. The task exits if `bootstrapped`
/// is dropped.
pub fn spawn_catchup(
    CatchupConfig {
        boot_ts,
        environment_id,
        persist_client,
        deploy_generation,
        deployment_state,
        catalog_metrics,
        caught_up_max_wait,
        ddl_check_interval,
        panic_after_timeout,
        bootstrap_args,
    }: CatchupConfig,
    mut caught_up_receiver: trigger::Receiver,
    bootstrapped: oneshot::Receiver<(BTreeSet<CatalogItemId>, BTreeSet<ReplicaId>)>,
) {
    mz_ore::task::spawn(|| "deployment_catchup", async move {
        let mut skip_catchup = deployment_state.set_catching_up();

        let initial_ids = tokio::select! {
            biased;

            () = &mut skip_catchup => None,
            result = bootstrapped => match result {
                Ok(ids) => Some(ids),
                Err(_) => return,
            },
        };

        if let Some((initial_user_items, initial_user_replicas)) = initial_ids {
            info!(
                user_items = initial_user_items.len(),
                user_replicas = initial_user_replicas.len(),
                ?caught_up_max_wait,
                "waiting for deployment to be caught up"
            );

            let mut caught_up_max_wait_fut = pin!(tokio::time::sleep(caught_up_max_wait));

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
                            &initial_user_items,
                            &initial_user_replicas,
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
                    &initial_user_items,
                    &initial_user_replicas,
                )
                .await;
            }
        } else {
            info!("skipping bootstrap and catch-up due to administrator request");
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

        let _catalog = openable_adapter_storage
            .open(boot_ts, &bootstrap_args)
            .await
            .unwrap_or_terminate("unexpected error while fencing out old deployment");

        // Reboot as the leader.
        halt!("fenced out old deployment; rebooting as leader")
    });
}

/// Restart in read-only mode when user items or replicas have been created or
/// dropped, so bootstrap hydrates new objects and releases dropped resources.
async fn check_ddl_changes(
    boot_ts: Timestamp,
    persist_client: PersistClient,
    environment_id: EnvironmentId,
    deploy_generation: u64,
    catalog_metrics: Arc<Metrics>,
    bootstrap_args: BootstrapArgs,
    initial_user_items: &BTreeSet<CatalogItemId>,
    initial_user_replicas: &BTreeSet<ReplicaId>,
) {
    let openable_adapter_storage = mz_catalog::durable::persist_backed_catalog_state(
        persist_client,
        environment_id.organization_id(),
        BUILD_INFO.semver_version(),
        Some(deploy_generation),
        catalog_metrics,
    )
    .await
    .expect("incompatible catalog/persist version");

    let mut catalog = openable_adapter_storage
        .open_savepoint(boot_ts, &bootstrap_args)
        .await
        .unwrap_or_terminate("can open in savepoint mode");

    // `transaction` rejects unapplied catalog content. This reader has no derived catalog to
    // update, so discard the initial update stream before opening the transaction.
    let _ = catalog
        .sync_to_current_updates()
        .await
        .unwrap_or_terminate("unexpected error while draining initial catalog updates");
    let tx = catalog
        .transaction()
        .await
        .unwrap_or_terminate("unexpected error while getting transaction");

    // We must explicitly check the catalog for these IDs since IDs can be
    // allocated during sequencing/planning but not yet committed to the catalog.
    // Furthermore, these IDs might never be committed to the catalog because
    // their sequencing has been aborted.
    let mut current_replicas = BTreeSet::new();
    let mut new_replicas = Vec::new();
    for replica in tx.get_cluster_replicas() {
        current_replicas.insert(replica.replica_id);
        if replica.replica_id.is_user() && !initial_user_replicas.contains(&replica.replica_id) {
            new_replicas.push(replica);
        }
    }

    let mut current_items = BTreeSet::new();
    let mut new_objects = Vec::new();
    for item in tx.get_items() {
        current_items.insert(item.id);
        if item.id.is_user() && !initial_user_items.contains(&item.id) {
            new_objects.push(item);
        }
    }

    let dropped_items = initial_user_items
        .difference(&current_items)
        .collect::<Vec<_>>();
    let dropped_replicas = initial_user_replicas
        .difference(&current_replicas)
        .collect::<Vec<_>>();

    if new_replicas.is_empty()
        && new_objects.is_empty()
        && dropped_items.is_empty()
        && dropped_replicas.is_empty()
    {
        return;
    }

    let mut info_parts = Vec::new();

    if !dropped_items.is_empty() {
        info_parts.push(format!(
            "Dropped objects: [{}]",
            separated(", ", dropped_items)
        ));
    }
    if !dropped_replicas.is_empty() {
        info_parts.push(format!(
            "Dropped replicas: [{}]",
            separated(", ", dropped_replicas)
        ));
    }

    if !new_replicas.is_empty() {
        let replicas = new_replicas.iter().map(|r| {
            format!(
                "{{replica_id: {}, replica_name: {}, cluster_id: {}}}",
                r.replica_id, r.name, r.cluster_id
            )
        });
        info_parts.push(format!("New replicas: [{}]", separated(", ", replicas)));
    }

    if !new_objects.is_empty() {
        let objects = new_objects
            .iter()
            .map(|o| format!("{{object_id: {}, object_name: {}}}", o.id, o.name));
        info_parts.push(format!("New objects: [{}]", separated(", ", objects)));
    }

    let extra_info = separated(". ", info_parts);

    halt!(
        "there have been DDL that we need to react to; rebooting in read-only mode. {}",
        extra_info
    )
}

/// Snapshot committed user IDs, not allocator counters: IDs can be allocated in
/// batches and committed out of order, or never committed at all.
pub async fn get_user_ids(
    catalog: &mut dyn DurableCatalogState,
) -> Result<(BTreeSet<CatalogItemId>, BTreeSet<ReplicaId>), CatalogError> {
    // Preserve the pending updates that adapter bootstrap must consume.
    let snapshot = catalog.snapshot().await?;
    let mut dry_run = catalog.transaction_from_snapshot(snapshot)?;
    let tx = dry_run.transaction_mut();

    let items = tx
        .get_items()
        .map(|item| item.id)
        .filter(|id| id.is_user())
        .collect();
    let replicas = tx
        .get_cluster_replicas()
        .map(|replica| replica.replica_id)
        .filter(|id| matches!(id, ReplicaId::User(_)))
        .collect();
    Ok((items, replicas))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_catalog::durable::{TestCatalogStateBuilder, test_bootstrap_args};
    use mz_orchestratord::controller::materialize::generation::DeploymentStatus;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::PersistLocation;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_repr::GlobalId;
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

    use crate::deployment::state::DeploymentStateHandle;

    async fn setup() -> (
        TestCatalogStateBuilder,
        CatchupConfig,
        DeploymentStateHandle,
    ) {
        let mut config = PersistConfig::new_for_tests();
        config.build_version = BUILD_INFO.semver_version();
        let cache = PersistClientCache::new(config, &MetricsRegistry::new(), |_, _| {
            PubSubClientConnection::noop()
        });
        let persist_client = cache.open(PersistLocation::new_in_mem()).await.unwrap();
        let environment_id = EnvironmentId::for_tests();
        let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
        let builder = TestCatalogStateBuilder::new(persist_client.clone())
            .with_organization_id(environment_id.organization_id())
            .with_version(BUILD_INFO.semver_version())
            .with_metrics(Arc::clone(&metrics))
            .with_deploy_generation(0);
        let boot_ts = SYSTEM_TIME().into();
        let catalog = builder
            .clone()
            .unwrap_build()
            .await
            .open(boot_ts, &test_bootstrap_args())
            .await
            .unwrap();
        catalog.expire().await;

        let (deployment_state, handle) = DeploymentState::new();
        let config = CatchupConfig {
            boot_ts,
            environment_id,
            persist_client,
            deploy_generation: 1,
            deployment_state,
            catalog_metrics: metrics,
            caught_up_max_wait: Duration::from_secs(1),
            ddl_check_interval: Duration::from_millis(10),
            panic_after_timeout: false,
            bootstrap_args: test_bootstrap_args(),
        };
        (builder, config, handle)
    }

    async fn wait_ready(handle: &DeploymentStateHandle) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while handle.status() != DeploymentStatus::ReadyToPromote {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[mz_ore::test(tokio::test)]
    async fn catchup_starts_after_bootstrap() {
        let (builder, config, handle) = setup().await;
        let mut openable = builder.with_deploy_generation(1).unwrap_build().await;
        assert!(preflight_0dt(openable.as_mut(), 1).await.unwrap());
        let (_trigger, receiver) = trigger::channel();
        let (bootstrapped, bootstrapped_receiver) = oneshot::channel();
        let metrics = Arc::clone(&config.catalog_metrics);
        let before = metrics.transactions_started.get();
        let caught_up_max_wait = config.caught_up_max_wait;
        let (boot_ts, bootstrap_args) = (config.boot_ts, config.bootstrap_args.clone());

        tokio::time::pause();
        spawn_catchup(config, receiver, bootstrapped_receiver);
        tokio::time::sleep(2 * caught_up_max_wait).await;
        assert_eq!(metrics.transactions_started.get(), before);
        assert_eq!(handle.status(), DeploymentStatus::Initializing);

        let mut catalog = openable
            .open_savepoint(boot_ts, &bootstrap_args)
            .await
            .unwrap();
        bootstrapped
            .send(get_user_ids(catalog.as_mut()).await.unwrap())
            .unwrap();
        wait_ready(&handle).await;
        catalog.expire().await;
    }

    #[mz_ore::test(tokio::test)]
    async fn skip_before_bootstrap() {
        let (_, config, handle) = setup().await;
        let (_trigger, receiver) = trigger::channel();
        let (_bootstrapped, bootstrapped_receiver) = oneshot::channel();
        spawn_catchup(config, receiver, bootstrapped_receiver);
        tokio::time::timeout(Duration::from_secs(10), async {
            while handle.try_skip_catchup().is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        wait_ready(&handle).await;
    }

    #[mz_ore::test(tokio::test)]
    async fn caught_up_before_task_starts() {
        let (builder, mut config, handle) = setup().await;
        let mut catalog = builder
            .with_deploy_generation(1)
            .unwrap_build()
            .await
            .open_savepoint(config.boot_ts, &config.bootstrap_args)
            .await
            .unwrap();
        let (bootstrapped, bootstrapped_receiver) = oneshot::channel();
        bootstrapped
            .send(get_user_ids(catalog.as_mut()).await.unwrap())
            .unwrap();
        let (trigger, receiver) = trigger::channel();
        drop(trigger);
        config.caught_up_max_wait = Duration::ZERO;
        config.panic_after_timeout = true;
        spawn_catchup(config, receiver, bootstrapped_receiver);
        wait_ready(&handle).await;
        catalog.expire().await;
    }

    #[mz_ore::test(tokio::test)]
    async fn baseline_uses_bootstrap_snapshot() {
        let (builder, config, _) = setup().await;
        let mut writer = builder
            .clone()
            .unwrap_build()
            .await
            .open(config.boot_ts, &config.bootstrap_args)
            .await
            .unwrap();
        let mut catalog = builder
            .with_deploy_generation(1)
            .unwrap_build()
            .await
            .open_savepoint(config.boot_ts, &config.bootstrap_args)
            .await
            .unwrap();
        let initial_ids = get_user_ids(catalog.as_mut()).await.unwrap();
        assert!(!catalog.sync_to_current_updates().await.unwrap().is_empty());

        writer.sync_to_current_updates().await.unwrap();
        let mut tx = writer.transaction().await.unwrap();
        let item_id = 1000;
        let replica_id = 1000;
        let schema_id = tx.get_schemas().find(|s| s.name == "public").unwrap().id;
        tx.insert_item(
            CatalogItemId::User(item_id),
            20000,
            GlobalId::User(item_id),
            schema_id,
            "new_item",
            "CREATE TABLE new_item (a int)".into(),
            MZ_SYSTEM_ROLE_ID,
            Vec::new(),
            Default::default(),
            None,
        )
        .unwrap();
        let replica = tx.get_cluster_replicas().next().unwrap();
        tx.insert_cluster_replica_with_id(
            replica.cluster_id,
            ReplicaId::User(replica_id),
            "new_replica",
            replica.config,
            replica.owner_id,
        )
        .unwrap();
        let commit_ts = tx.upper();
        let _ = tx.get_and_commit_op_updates();
        tx.commit(commit_ts).await.unwrap();

        assert_eq!(get_user_ids(catalog.as_mut()).await.unwrap(), initial_ids);
        let mut expected = initial_ids;
        expected.0.insert(CatalogItemId::User(item_id));
        expected.1.insert(ReplicaId::User(replica_id));
        assert_eq!(get_user_ids(writer.as_mut()).await.unwrap(), expected);
        catalog.expire().await;
        writer.expire().await;
    }
}
