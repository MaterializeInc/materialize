// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adapter-local storage inventory, observations and replica HTTP discovery.
//!
//! Replicas enact maintained compute and storage from the protected catalog.
//! Process provisioning is exposed separately for controllerd.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use mz_cluster_client::WallclockLagFn;
use mz_cluster_client::metrics::ControllerMetrics;
use mz_orchestrator::{NamespacedOrchestrator, Orchestrator};
use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use mz_repr::{GlobalId, Timestamp};
use mz_secrets_cli::SecretsReaderCliArgs;
use mz_storage_client::controller::{StorageController, StorageTxn};
use mz_storage_client::storage_collections::{self, StorageCollections};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::StorageError;
use mz_txn_wal::metrics::Metrics as TxnMetrics;

pub mod clusters;
pub mod replica_http_locator;

// Export this on behalf of the storage controller to provide a unified
// interface, allowing other crates to depend on this crate alone.
pub use mz_storage_controller::adapter_storage::AdapterStorageWriter;
pub use mz_storage_controller::rtr::real_time_recency_ts;
pub use replica_http_locator::ReplicaHttpLocator;

/// Configures a controller.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// The orchestrator implementation to use.
    pub orchestrator: Arc<dyn Orchestrator>,
    /// The persist location where all storage collections will be written to.
    pub persist_location: PersistLocation,
    /// Explicit fixture clock shared with local replica catalog writers.
    /// Only timestamp allocation and policy checks may use this clock.
    pub timestamp_oracle_clock_file: Option<std::path::PathBuf>,
    /// A process-global cache of (blob_uri, consensus_uri) ->
    /// PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<PersistClientCache>,
    /// The clusterd image to use when starting new cluster processes.
    pub clusterd_image: String,
    /// The init container image to use for clusterd.
    pub init_container_image: Option<String>,
    /// A number representing the environment's generation.
    ///
    /// This is incremented to request that the new process perform a graceful
    /// transition of power from the prior generation.
    pub deploy_generation: u64,
    /// The now function to advance the controller's introspection collections.
    pub now: NowFn,
    /// The metrics registry.
    pub metrics_registry: MetricsRegistry,
    /// The URL for Persist PubSub.
    pub persist_pubsub_url: String,
    /// Arguments for secrets readers.
    pub secrets_args: SecretsReaderCliArgs,
    /// The connection context, to thread through to clusterd, with cli flags.
    pub connection_context: ConnectionContext,
    /// Locator for HTTP addresses of cluster replicas, used to proxy HTTP
    /// requests from environmentd to clusterd.
    pub replica_http_locator: Arc<ReplicaHttpLocator>,
}

/// Adapter-local storage state and passive replica observations.
pub struct Controller {
    pub storage: Box<dyn StorageController>,
    pub storage_collections: Arc<dyn StorageCollections + Send + Sync>,
    /// Shared collection metrics for lifecycle owners and passive observers.
    pub metrics: ControllerMetrics,
    /// This participant's deployment generation.
    deploy_generation: u64,
    /// Whether local storage writers must refrain from shared writes.
    read_only: bool,
    /// Resolves addresses of services provisioned by controllerd.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// Locator for HTTP addresses of cluster replicas.
    replica_http_locator: Arc<ReplicaHttpLocator>,
}

impl Controller {
    /// Returns the connection context installed in the controller.
    ///
    /// This is purely a helper, and can be obtained from `self.storage`.
    pub fn connection_context(&self) -> &ConnectionContext {
        &self.storage.config().connection_context
    }

    /// Returns the storage configuration installed in the storage controller.
    ///
    /// This is purely a helper, and can be obtained from `self.storage`.
    pub fn storage_configuration(&self) -> &StorageConfiguration {
        self.storage.config()
    }

    /// Returns the state of the [`Controller`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    pub async fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Note: We purposefully use the `Debug` formatting for the value of all fields in the
        // returned object as a tradeoff between usability and stability. `serde_json` will fail
        // to serialize an object if the keys aren't strings, so `Debug` formatting the values
        // prevents a future unrelated change from silently breaking this method.

        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            storage_collections,
            storage,
            metrics: _,
            deploy_generation,
            read_only,
            orchestrator: _,
            replica_http_locator: _,
        } = self;

        let storage_collections = storage_collections.dump()?;
        let storage = storage.dump()?;
        Ok(serde_json::json!({
            "storage_collections": storage_collections,
            "storage": storage,
            "deploy_generation": deploy_generation,
            "read_only": read_only,
        }))
    }
}

impl Controller {
    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.storage.initialization_complete();
    }

    /// Reports whether the controller is in read only mode.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Waits for pending storage observations or maintenance.
    ///
    /// Cancellation is safe: storage retains received observations and scheduled
    /// maintenance until [`Self::process`] consumes them.
    pub async fn ready(&mut self) {
        self.storage.ready().await;
    }

    /// Processes the observations or maintenance queued by [`Self::ready`].
    #[mz_ore::instrument(level = "debug")]
    pub fn process(&mut self) -> Result<(), anyhow::Error> {
        self.storage.process()
    }

    /// Determine the "real-time recency" timestamp for all `ids`.
    ///
    /// Real-time recency is defined as the minimum value of `T` that all
    /// objects can be queried at to return all data visible in the upstream
    /// system the query was issued. In this case, "the upstream systems" are
    /// any user sources that connect to objects outside of Materialize, such as
    /// Kafka sources.
    ///
    /// If no items in `ids` connect to external systems, this function will
    /// return `Ok(T::minimum)`.
    pub async fn determine_real_time_recent_timestamp(
        &self,
        ids: BTreeSet<GlobalId>,
        timeout: Duration,
    ) -> Result<BoxFuture<'static, Result<Timestamp, StorageError>>, StorageError> {
        self.storage.real_time_recent_timestamp(ids, timeout).await
    }
}

impl Controller {
    /// Creates a new controller.
    ///
    /// The transaction WAL identity in `storage_txn` must be durably initialized
    /// by catalog bootstrap before construction opens Persist handles.
    ///
    /// # Panics
    /// If catalog read protection is disabled or `storage_txn` lacks the WAL identity.
    #[instrument(name = "controller::new")]
    pub async fn new(
        config: ControllerConfig,
        envd_epoch: std::num::NonZeroI64,
        read_only: bool,
        catalog_read_protection_enabled: bool,
        storage_txn: &dyn StorageTxn,
        txns_metrics: Arc<TxnMetrics>,
    ) -> Self {
        assert!(
            catalog_read_protection_enabled,
            "controllers require a protected native catalog"
        );
        if read_only {
            tracing::info!("starting controllers in read-only mode!");
        }

        let now_fn = config.now.clone();
        let wallclock_lag_fn = WallclockLagFn::new(now_fn);

        let controller_metrics = ControllerMetrics::new(&config.metrics_registry);

        let collections_ctl = storage_collections::StorageCollectionsImpl::new(
            config.persist_location.clone(),
            Arc::clone(&config.persist_clients),
            &config.metrics_registry,
            config.now.clone(),
            Arc::clone(&txns_metrics),
            envd_epoch,
            read_only,
            catalog_read_protection_enabled,
            config.connection_context.clone(),
            storage_txn,
        )
        .await;

        let collections_ctl: Arc<dyn StorageCollections + Send + Sync> = Arc::new(collections_ctl);

        let storage_controller = mz_storage_controller::Controller::new(
            config.persist_location.clone(),
            config.persist_clients,
            config.now.clone(),
            wallclock_lag_fn,
            Arc::clone(&txns_metrics),
            read_only,
            config.deploy_generation,
            &config.metrics_registry,
            controller_metrics.clone(),
            config.connection_context,
            storage_txn,
            Arc::clone(&collections_ctl),
        )
        .await;

        Self {
            storage: Box::new(storage_controller),
            storage_collections: collections_ctl,
            metrics: controller_metrics,
            deploy_generation: config.deploy_generation,
            read_only,
            orchestrator: config.orchestrator.namespace("cluster"),
            replica_http_locator: config.replica_http_locator,
        }
    }
}
