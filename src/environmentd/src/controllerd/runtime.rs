// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::Duration;

use axum::http::StatusCode;
use futures::StreamExt;
use mz_catalog::catalog::{Catalog, CatalogError, Op};
use mz_catalog::memory::error::ErrorKind;
use mz_controller::clusters::provisioning::ReplicaProvisioningConfig;
use serde_json::{Value, json};

use super::http::{Action, ApiError, Command};
use super::observations::Observations;
use super::provisioning::{AdapterAllocation, AdapterProvisioningConfig};

/// Reconciles committed process inventory and serves catalog administration.
/// Replica-set policy, SQL execution and shared maintenance remain in the children.
pub async fn run(
    config: crate::Config,
    adapter_launch: AdapterProvisioningConfig,
    adapter_sizes: BTreeMap<String, AdapterAllocation>,
    adapter_plan_build: String,
    http_listen_addr: SocketAddr,
    mut pubsub: Option<super::pubsub::ManagedPubsub>,
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
    let listener = tokio::net::TcpListener::bind(http_listen_addr).await?;
    let mut catalog = super::open_catalog(&config, &adapter_sizes).await?;
    let persist = config
        .catalog_config
        .persist_clients
        .open(config.controller.persist_location.clone())
        .await?;
    let mut replica_history = super::status::ReplicaHistory::new(
        &catalog,
        persist.clone(),
        config.controller.deploy_generation,
    )?;
    // Bind retained services before polling their initial watch events. Keeping
    // only the latest event until the first tick would lose intervening failures.
    replica_history.set_desired(
        super::desired_replicas(&catalog, config.controller.deploy_generation)
            .into_iter()
            .map(|replica| {
                (
                    replica.service,
                    u64::from(replica.resources.allocation.scale.get()),
                )
            })
            .collect(),
    );
    let adapters = config.controller.orchestrator.namespace("adapter");
    let replicas = config.controller.orchestrator.namespace("cluster");
    let mut replica_metrics = super::metrics::ReplicaMetrics::new(
        &catalog,
        persist,
        config.controller.deploy_generation,
        std::sync::Arc::clone(&replicas),
    )?;
    let mut adapter_events = adapters.watch_services();
    let mut replica_events = replicas.watch_services();
    let mut pubsub_events = pubsub
        .as_ref()
        .map(|relay| relay.watch())
        .unwrap_or_else(|| Box::pin(futures::stream::pending()));
    let (sender, mut commands) = tokio::sync::mpsc::channel::<Command>(32);
    tracing::info!(address = %listener.local_addr()?, "controller HTTP listening without authentication");
    let mut server =
        mz_ore::task::spawn(|| "controller-http", super::http::serve(listener, sender))
            .abort_on_drop();
    let mut observations = Observations::new()?;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = &mut replica_metrics.task => {
                anyhow::bail!("replica metrics publisher stopped");
            }
            _ = &mut replica_history.task => {
                anyhow::bail!("replica status publisher stopped");
            }
            result = &mut server => {
                result?;
                anyhow::bail!("controller HTTP server stopped");
            }
            Some(result) = observations.probes.next(), if !observations.probes.is_empty() => {
                observations.complete_probe(result, &adapter_launch);
            }
            Some(event) = adapter_events.next() => observations.record(event),
            Some(event) = replica_events.next() => {
                if let Ok(event) = &event { replica_history.record(event); }
                observations.record(event);
            },
            Some(event) = pubsub_events.next() => {
                if let Some(relay) = &mut pubsub { relay.record(event); }
            },
            Some(command) = commands.recv() => {
                // Finish submitted catalog writes even if the HTTP caller disconnects.
                let result = administer(
                    &mut catalog,
                    &adapter_launch,
                    &adapter_sizes,
                    &observations,
                    pubsub.as_ref(),
                    command.action,
                ).await;
                let _ = command.response.send(result);
            }
            _ = interval.tick() => {
                catalog.sync_to_current_updates().await?;
                if let Some(relay) = &mut pubsub { relay.reconcile(); }
                catalog.system_config().dyncfg_updates().apply(&config.system_dyncfgs);
                let mut follower = catalog.replica_config();
                // Development sibling binaries can have distinct executable build IDs.
                follower.plan_build = adapter_plan_build.clone();
                let follower = serde_json::to_string(&follower)?;
                let replica_launch = ReplicaProvisioningConfig {
                    deploy_generation: config.controller.deploy_generation,
                    connection_context: &config.controller.connection_context,
                    persist_pubsub_url: &config.controller.persist_pubsub_url,
                    catalog_persist_location: Some(&config.controller.persist_location),
                    catalog_timestamp_oracle_url: config.timestamp_oracle_url.as_ref(),
                    timestamp_oracle_clock_file: config.controller
                        .timestamp_oracle_clock_file.as_deref(),
                    catalog_follower_config: Some(&follower),
                    secrets_args: &config.controller.secrets_args,
                    dyncfg: &config.system_dyncfgs,
                    replica_dyncfg_overrides: None,
                    clusterd_image: &config.controller.clusterd_image,
                    init_container_image: config.controller.init_container_image.as_deref(),
                };
                observations.replicas = super::reconcile_replicas(
                    &mut catalog, replicas.as_ref(), &replica_launch,
                ).await?.into_iter().map(|entry| (entry.service.clone(), entry)).collect();
                observations.adapters = super::reconcile_adapters(
                    &mut catalog, adapters.as_ref(), &adapter_launch, &adapter_sizes,
                ).await?.into_iter().map(|entry| (entry.service.clone(), entry)).collect();
                let desired: BTreeSet<_> = observations.adapters.keys()
                    .chain(observations.replicas.keys()).cloned().collect();
                observations.retain(&desired);
                let replica_inventory = observations.replicas.iter().map(|(service, replica)| {
                    (service.clone(), u64::from(replica.resources.allocation.scale.get()))
                }).collect::<BTreeMap<_, _>>();
                replica_metrics.set_desired(replica_inventory.clone());
                replica_history.set_desired(replica_inventory);
                // Initial watch events can precede the first inventory reconciliation.
                for event in observations.process_events() {
                    replica_history.record(event);
                }
                observations.seed_readiness(&catalog);
                observations.start_probes(&adapter_launch);
                match observations.publish(&mut catalog, &adapter_launch, &adapter_sizes).await {
                    Err(error) if catalog_out_of_sync(&error) => {},
                    result => result?,
                }
                for replica in observations.replicas.values() {
                    if let Some(error) = &replica.provisioning_error {
                        tracing::warn!(replica_id = %replica.replica_id, %error, "replica provisioning failed");
                    }
                }
                for adapter in observations.adapters.values() {
                    if let Some(error) = &adapter.provisioning_error {
                        tracing::warn!(adapter_id = %adapter.id, %error, "adapter provisioning failed");
                    }
                }
            }
        }
    }
}

async fn administer(
    catalog: &mut Catalog,
    launch: &AdapterProvisioningConfig,
    sizes: &BTreeMap<String, AdapterAllocation>,
    observations: &Observations,
    pubsub: Option<&super::pubsub::ManagedPubsub>,
    action: Action,
) -> Result<Value, ApiError> {
    // A failed catalog CAS has committed nothing. Refresh and rebind the request,
    // but bound retries so HTTP contention cannot indefinitely delay provisioning.
    for attempt in 0..3 {
        catalog
            .sync_to_current_updates()
            .await
            .map_err(|error| catalog_error(error.into()))?;
        let op = match &action {
            Action::ListAdapters => {
                return Ok(json!(observations.adapters(catalog, launch, sizes)));
            }
            Action::Inventory => {
                return Ok(json!({
                    "generation": launch.generation,
                    "pubsub": pubsub.map(|relay| relay.inventory()),
                    "adapters": observations.adapters(catalog, launch, sizes),
                    "replicas": observations.replicas(catalog, launch.generation),
                }));
            }
            Action::Collections(filters) => {
                return Ok(super::collections::inspect(catalog, filters));
            }
            Action::CreateAdapter { name, size } => {
                Catalog::validate_adapter_size(size, &sizes.keys().cloned().collect())
                    .map_err(|error| catalog_error(error.into()))?;
                Op::CreateAdapter {
                    name: name.clone(),
                    size: size.clone(),
                }
            }
            Action::DropAdapter { id } => {
                let name = catalog
                    .state()
                    .adapter_declarations()
                    .get(id)
                    .ok_or_else(|| ApiError {
                        status: StatusCode::NOT_FOUND,
                        message: format!("unknown adapter {id}"),
                    })?
                    .name
                    .clone();
                Op::DropAdapter { id: *id, name }
            }
        };
        match catalog
            .transact(None, None, mz_repr::Timestamp::MIN, None, vec![op])
            .await
        {
            Err(error) if catalog_out_of_sync(&error) && attempt < 2 => continue,
            Err(error) => return Err(catalog_error(error)),
            Ok(_) => {
                return Ok(match &action {
                    Action::CreateAdapter { name, size } => {
                        let declaration = catalog
                            .state()
                            .adapter_declarations()
                            .values()
                            .find(|adapter| adapter.name == name.as_str())
                            .expect("just committed adapter");
                        json!({"id": declaration.id.to_string(), "name": name, "size": size})
                    }
                    Action::DropAdapter { id } => json!({"dropped": id.to_string()}),
                    Action::ListAdapters | Action::Inventory | Action::Collections(_) => {
                        unreachable!("listing returns before mutation")
                    }
                });
            }
        }
    }
    unreachable!("last attempt returns its result")
}

fn catalog_out_of_sync(error: &CatalogError) -> bool {
    matches!(error, CatalogError::Catalog(inner) if matches!(
        inner.kind,
        ErrorKind::Durable(mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. })
    ))
}

fn catalog_error(error: CatalogError) -> ApiError {
    match &error {
        CatalogError::PlanError(mz_sql::plan::PlanError::Catalog(inner)) => ApiError {
            status: match inner {
                mz_sql::catalog::CatalogError::UnknownAdapter(_) => StatusCode::NOT_FOUND,
                mz_sql::catalog::CatalogError::AdapterAlreadyExists(_) => StatusCode::CONFLICT,
                _ => StatusCode::BAD_REQUEST,
            },
            message: error.to_string(),
        },
        CatalogError::Catalog(inner)
            if matches!(
                inner.kind,
                ErrorKind::InvalidAdapterName(_) | ErrorKind::InvalidAdapterSize { .. }
            ) =>
        {
            ApiError {
                status: StatusCode::BAD_REQUEST,
                message: error.to_string(),
            }
        }
        CatalogError::ReadOnly | CatalogError::DDLTransactionRace => ApiError {
            status: StatusCode::CONFLICT,
            message: error.to_string(),
        },
        CatalogError::Catalog(inner)
            if matches!(
                inner.kind,
                ErrorKind::Durable(
                    mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
                )
            ) =>
        {
            ApiError {
                status: StatusCode::CONFLICT,
                message: "catalog changed; retry the request".into(),
            }
        }
        _ => {
            tracing::warn!(%error, "controller catalog operation failed");
            ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: "catalog operation unavailable".into(),
            }
        }
    }
}
