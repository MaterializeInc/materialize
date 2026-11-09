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
use mz_catalog::durable::{BootstrapArgs, Metrics};
use mz_ore::channel::trigger;
use mz_ore::halt;
use mz_persist_client::PersistClient;
use mz_repr::Timestamp;
use mz_sql::catalog::EnvironmentId;
use tokio::sync::oneshot;
use tracing::info;

use crate::BUILD_INFO;
use crate::deployment::state::DeploymentState;

/// Configuration for catching up and promoting an admitted deployment.
pub struct CatchupConfig {
    pub boot_ts: Timestamp,
    pub environment_id: EnvironmentId,
    pub persist_client: PersistClient,
    pub deploy_generation: u64,
    pub deployment_state: DeploymentState,
    pub catalog_metrics: Arc<Metrics>,
    pub timestamp_oracle: mz_catalog::durable::CatalogTimestampOracle,
    pub caught_up_max_wait: Duration,
    pub panic_after_timeout: bool,
    pub bootstrap_args: BootstrapArgs,
}

/// Waits for readiness and an explicit request to promote an admitted deployment.
///
/// An administrative skip bypasses bootstrap and catch-up. Otherwise the timeout
/// begins only after bootstrap completes. Dropping `bootstrapped` exits the task.
/// Native replicas follow committed DDL without restarting their warmed execution.
pub fn spawn_catchup(
    CatchupConfig {
        boot_ts,
        environment_id,
        persist_client,
        deploy_generation,
        deployment_state,
        catalog_metrics,
        timestamp_oracle,
        caught_up_max_wait,
        panic_after_timeout,
        bootstrap_args,
    }: CatchupConfig,
    mut caught_up_receiver: trigger::Receiver,
    bootstrapped: oneshot::Receiver<()>,
) {
    mz_ore::task::spawn(|| "deployment_catchup", async move {
        let mut skip_catchup = deployment_state.set_catching_up();

        let wait_for_catchup = tokio::select! {
            biased;

            () = &mut skip_catchup => false,
            result = bootstrapped => match result {
                Ok(()) => true,
                Err(_) => return,
            },
        };

        if wait_for_catchup {
            info!(
                ?caught_up_max_wait,
                "waiting for deployment to be caught up"
            );
            tokio::select! {
                biased;

                () = &mut skip_catchup => {
                    info!("skipping waiting for deployment to catch up due to administrator request");
                }
                () = &mut caught_up_receiver => {
                    info!("deployment caught up");
                }
                () = tokio::time::sleep(caught_up_max_wait) => {
                    if panic_after_timeout {
                        panic!("not caught up within {:?}", caught_up_max_wait);
                    }
                    info!("not caught up within {:?}, proceeding now", caught_up_max_wait);
                }
            }
        } else {
            info!("skipping bootstrap and catch-up due to administrator request");
        }

        // Announce that we're ready to promote.
        let promoted = deployment_state.set_ready_to_promote();
        info!("announced as ready to promote; waiting for promotion");
        promoted.await;

        // Promotion changes active administrative authority without retiring
        // compatible writers. Only the candidate adapter restarts afterward.
        info!("promoted; updating active deployment");

        let openable_adapter_storage = mz_catalog::durable::persist_backed_catalog_state(
            persist_client.clone(),
            environment_id.organization_id(),
            BUILD_INFO.semver_version(),
            Some(deploy_generation),
            Arc::clone(&catalog_metrics),
            Some(timestamp_oracle),
        )
        .await
        .unwrap_or_terminate("unexpected error while promoting deployment");

        let _catalog = openable_adapter_storage
            .open_for_promotion(boot_ts, &bootstrap_args)
            .await
            .unwrap_or_terminate("unexpected error while promoting deployment");

        // Rejoin the promoted generation without reinitializing its catalog.
        halt!("deployment promoted; restarting adapter")
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_catalog::durable::test_bootstrap_args;
    use mz_orchestratord::controller::materialize::generation::DeploymentStatus;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::PersistLocation;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;

    use crate::deployment::state::DeploymentStateHandle;

    #[derive(Debug)]
    struct PromotionOracle;

    #[async_trait::async_trait]
    impl mz_timestamp_oracle::TimestampOracle<Timestamp> for PromotionOracle {
        async fn write_ts(&self) -> mz_timestamp_oracle::WriteTimestamp<Timestamp> {
            panic!("readiness checks must not write promotion timestamps")
        }
        async fn peek_write_ts(&self) -> Timestamp {
            panic!("readiness checks must not access the promotion oracle")
        }
        async fn read_ts(&self) -> Timestamp {
            panic!("readiness checks must not access the promotion oracle")
        }
        async fn apply_write(&self, _: Timestamp) {
            panic!("readiness checks must not complete promotion timestamps")
        }
    }

    async fn setup() -> (CatchupConfig, DeploymentStateHandle) {
        let mut config = PersistConfig::new_for_tests();
        config.build_version = BUILD_INFO.semver_version();
        let cache = PersistClientCache::new(config, &MetricsRegistry::new(), |_, _| {
            PubSubClientConnection::noop()
        });
        let persist_client = cache.open(PersistLocation::new_in_mem()).await.unwrap();
        let environment_id = EnvironmentId::for_tests();
        let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
        let (deployment_state, handle) = DeploymentState::new();
        let config = CatchupConfig {
            boot_ts: SYSTEM_TIME().into(),
            environment_id,
            persist_client,
            deploy_generation: 1,
            deployment_state,
            catalog_metrics: metrics,
            timestamp_oracle: mz_catalog::durable::CatalogTimestampOracle::new(
                Arc::new(PromotionOracle),
                SYSTEM_TIME.clone(),
            ),
            caught_up_max_wait: Duration::from_secs(1),
            panic_after_timeout: false,
            bootstrap_args: test_bootstrap_args(),
        };
        (config, handle)
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
        let (config, handle) = setup().await;
        let (_trigger, receiver) = trigger::channel();
        let (bootstrapped, bootstrapped_receiver) = oneshot::channel();
        let metrics = Arc::clone(&config.catalog_metrics);
        let before = metrics.transactions_started.get();
        let caught_up_max_wait = config.caught_up_max_wait;

        tokio::time::pause();
        spawn_catchup(config, receiver, bootstrapped_receiver);
        tokio::time::sleep(2 * caught_up_max_wait).await;
        assert_eq!(metrics.transactions_started.get(), before);
        assert_eq!(handle.status(), DeploymentStatus::Initializing);

        bootstrapped.send(()).unwrap();
        tokio::task::yield_now().await;
        tokio::time::sleep(caught_up_max_wait / 2).await;
        assert_eq!(handle.status(), DeploymentStatus::Initializing);
        wait_ready(&handle).await;
        assert_eq!(metrics.transactions_started.get(), before);
    }

    #[mz_ore::test(tokio::test)]
    async fn skip_before_bootstrap() {
        let (config, handle) = setup().await;
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
        let (mut config, handle) = setup().await;
        let (bootstrapped, bootstrapped_receiver) = oneshot::channel();
        bootstrapped.send(()).unwrap();
        let (trigger, receiver) = trigger::channel();
        drop(trigger);
        config.caught_up_max_wait = Duration::ZERO;
        config.panic_after_timeout = true;
        spawn_catchup(config, receiver, bootstrapped_receiver);
        wait_ready(&handle).await;
    }

    #[mz_ore::test(tokio::test)]
    async fn failed_bootstrap_does_not_become_ready() {
        let (config, handle) = setup().await;
        let (trigger, receiver) = trigger::channel();
        let (bootstrapped, bootstrapped_receiver) = oneshot::channel();
        drop(bootstrapped);
        drop(trigger);
        let caught_up_max_wait = config.caught_up_max_wait;
        tokio::time::pause();
        spawn_catchup(config, receiver, bootstrapped_receiver);
        tokio::time::sleep(2 * caught_up_max_wait).await;
        assert_eq!(handle.status(), DeploymentStatus::Initializing);
        assert!(handle.try_promote().is_err());
    }
}
