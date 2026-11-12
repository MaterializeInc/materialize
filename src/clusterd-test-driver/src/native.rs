// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Process-lifetime native runtime for programmable renderer tests.
//!
//! The script owns its raw dataflows through the same in-process endpoint used
//! by the catalog follower. This is not a production catalog or an external
//! maintenance protocol. Peeks use a query-only connection with an applied test
//! catalog position. Transient exports, including subscriptions, are admitted
//! through query ingress rather than the maintained owner's response path.

use std::sync::Arc;

use mz_cluster_client::CatalogPosition;
use mz_cluster_client::client::TimelyConfig;
use mz_compute::server::{ComputeInstanceContext, ComputeRuntimeRole, ReplicaCompute};
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::ShardId;
use mz_repr::Timestamp;
use mz_service::client::GenericClient;
use mz_storage::server::ReplicaStorage;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_client::client::StorageCommand;
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use uuid::Uuid;

use crate::persist_host::PersistHost;

pub(crate) struct NativeCompute {
    replica: ReplicaCompute,
    storage: ReplicaStorage,
    query: Box<dyn ComputeClient>,
    position: CatalogPosition,
}

impl NativeCompute {
    pub async fn start(host: &PersistHost, workers: usize, unified: bool) -> anyhow::Result<Self> {
        anyhow::ensure!(workers > 0, "the native runtime needs at least one worker");
        let registry = MetricsRegistry::new();
        let timely = TimelyConfig {
            workers,
            addresses: vec!["127.0.0.1:0".into()],
            ..Default::default()
        };
        let persist = host.cache();
        let txns = TxnsContext::default();
        let tracing = Arc::new(TracingHandle::disabled());
        let connections =
            ConnectionContext::for_tests(Arc::new(mz_secrets::InMemorySecretsController::new()));
        let context = ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context: connections.clone(),
        };
        let storage_context = StorageInstanceContext::new(None, None);
        let (mut server, storage) = if unified {
            let (server, storage, _factory) = mz_compute::server::serve_unified(
                timely,
                ComputeRuntimeRole::Solo,
                true,
                &registry,
                persist,
                txns,
                tracing,
                context,
                SYSTEM_TIME.clone(),
                connections,
                storage_context,
            )
            .await?;
            (server, storage)
        } else {
            let mut storage = mz_storage::server::serve_with_replica(
                timely.clone(),
                true,
                &registry,
                Arc::clone(&persist),
                txns.clone(),
                Arc::clone(&tracing),
                SYSTEM_TIME.clone(),
                connections,
                storage_context,
            )
            .await?;
            let server = mz_compute::server::serve(
                timely,
                ComputeRuntimeRole::Solo,
                true,
                &registry,
                persist,
                txns,
                tracing,
                context,
            )
            .await?;
            (server, storage.take_replica())
        };
        let replica = server.take_replica().expect("process zero owns compute");
        let mut storage = storage.expect("process zero owns storage");
        storage.send(StorageCommand::InitializationComplete);
        let mut query = server.client_builder()();
        query
            .send(ComputeCommand::HelloQuery {
                nonce: Uuid::new_v4(),
            })
            .await?;
        query
            .send(ComputeCommand::SetQueryMaxResultSize {
                max_result_size: u64::MAX,
            })
            .await?;
        Ok(Self {
            replica,
            storage,
            query,
            position: CatalogPosition {
                shard_id: ShardId::new(),
                deployment_generation: 0,
                upper: Timestamp::MIN,
            },
        })
    }

    pub async fn send(&mut self, command: ComputeCommand) -> anyhow::Result<()> {
        match command {
            ComputeCommand::CreateQueryDataflow {
                request_id,
                dataflow,
                subscription_incarnation,
                ..
            } => {
                self.query
                    .send(ComputeCommand::CreateQueryDataflow {
                        request_id,
                        dataflow,
                        subscription_incarnation,
                        catalog_position: Some(Box::new(self.position)),
                    })
                    .await?;
            }
            ComputeCommand::Peek(mut peek) => {
                peek.catalog_position = Some(self.position);
                self.query.send(ComputeCommand::Peek(peek)).await?;
            }
            command @ ComputeCommand::CancelPeek { .. } => self.query.send(command).await?,
            ComputeCommand::Schedule(id) if id.is_transient() => {
                self.query.send(ComputeCommand::Schedule(id)).await?;
            }
            ComputeCommand::AllowCompaction { id, frontier } if id.is_transient() => {
                self.query
                    .send(ComputeCommand::AllowCompaction { id, frontier })
                    .await?;
            }
            command => {
                self.replica.send(command);
                // Sequence every scripted owner operation before subsequent reads.
                // No durable catalog is fabricated: this marker belongs only to
                // this isolated test runtime's explicitly owned command stream.
                self.position.upper = self.position.upper.step_forward();
                self.replica
                    .send(ComputeCommand::ApplyCatalogPosition(Box::new(
                        self.position,
                    )));
            }
        }
        Ok(())
    }

    pub async fn recv(&mut self) -> anyhow::Result<Option<ComputeResponse>> {
        loop {
            tokio::select! {
                response = self.replica.recv() => return response,
                response = self.query.recv() => {
                    let response = response?;
                    // Maintained progress is also broadcast to query connections.
                    // Use its native-owner stream only, so a delayed duplicate
                    // cannot regress a frontier watch.
                    if matches!(&response, Some(ComputeResponse::Frontiers(id, _)) if !id.is_transient()) {
                        continue;
                    }
                    return Ok(response);
                },
                response = self.storage.recv() => {
                    anyhow::ensure!(response?.is_some(), "native storage owner lost progress");
                }
            }
        }
    }
}
