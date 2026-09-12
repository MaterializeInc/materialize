// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Request-owned storage execution. Staged rows are committed by the adapter.

use std::collections::BTreeSet;
use std::time::Duration;

use futures::StreamExt;
use mz_compute_types::ComputeInstanceId;
use mz_persist_client::batch::ProtoBatch;
use mz_service::client::GenericClient;
use mz_storage_client::client::{
    RunOneshotIngestion, StorageClient, StorageCommand, StorageResponse,
};
use uuid::Uuid;

use super::QueryClient;
use super::connections::StorageReplicaTarget;

type Batches = Vec<Result<ProtoBatch, String>>;

impl QueryClient {
    /// Stage one COPY request on query connections. The first complete replica
    /// result wins, including execution errors. Dropping the future closes all
    /// connections and cancels their work. Failed execution is never replayed.
    pub(crate) async fn stage_oneshot(
        &self,
        cluster: ComputeInstanceId,
        command: RunOneshotIngestion,
    ) -> anyhow::Result<Batches> {
        let mut topology = self.connections.changes();
        let mut attempted = BTreeSet::new();
        let mut executions = futures::stream::FuturesUnordered::new();
        let mut last_error = None;
        loop {
            topology.borrow_and_update();
            for target in self.connections.storage_targets(cluster) {
                if attempted.insert(target.replica_id) {
                    executions.push(execute(target, command.clone()));
                }
            }
            if executions.is_empty() && !attempted.is_empty() {
                return Err(last_error.expect("finished executions failed"));
            }
            tokio::select! {
                response = executions.next(), if !executions.is_empty() => {
                    match response.expect("nonempty executions") {
                        Ok(batches) => return Ok(batches),
                        Err(error) => last_error = Some(error),
                    }
                }
                result = topology.changed() => {
                    result?;
                }
            }
        }
    }
}

async fn execute(
    target: StorageReplicaTarget,
    command: RunOneshotIngestion,
) -> anyhow::Result<Batches> {
    let mut lifetime = target.retired.clone();
    let retired = async { while lifetime.changed().await.is_ok() {} };
    let execution = async {
        let mut backoff = Duration::from_millis(100);
        // A query can overtake provisioning. Retry connection setup, but never
        // replay a request after it could have begun staging batches.
        let client = loop {
            let connection = async { ready(target.connect().await?).await }.await;
            match connection {
                Ok(client) => break client,
                Err(error) => tracing::debug!(replica = %target.replica_id, %error,
                    "storage query connection is not ready"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(1));
        };
        run(client, command).await
    };
    tokio::select! {
        biased;
        _ = retired => anyhow::bail!("storage query replica was removed"),
        result = execution => result,
    }
}

async fn ready(mut client: Box<dyn StorageClient>) -> anyhow::Result<Box<dyn StorageClient>> {
    client
        .send(StorageCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await?;
    anyhow::ensure!(
        matches!(client.recv().await?, Some(StorageResponse::QueryReady)),
        "storage query handshake did not return readiness"
    );
    Ok(client)
}

async fn run(
    mut client: Box<dyn StorageClient>,
    command: RunOneshotIngestion,
) -> anyhow::Result<Batches> {
    let id = command.ingestion_id;
    client
        .send(StorageCommand::RunOneshotIngestion(Box::new(command)))
        .await?;
    while let Some(response) = client.recv().await? {
        if let StorageResponse::StagedBatches(mut batches) = response {
            if let Some(result) = batches.remove(&id) {
                // Completion also ends this query connection, so no descriptor
                // remains to replay and no other replica keeps staging work.
                return Ok(result);
            }
        } else {
            anyhow::bail!("unexpected response on storage query connection");
        }
    }
    anyhow::bail!("storage query connection closed before completion")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_service::local::LocalClient;
    use mz_storage_types::controller::CollectionMetadata;
    use mz_storage_types::oneshot_sources::{
        ContentFilter, ContentFormat, ContentShape, ContentSource, OneshotIngestionRequest,
    };
    use tokio::sync::mpsc;

    fn request(id: Uuid) -> RunOneshotIngestion {
        let desc = mz_repr::RelationDesc::empty();
        RunOneshotIngestion {
            ingestion_id: id,
            collection_id: mz_repr::GlobalId::User(1),
            collection_meta: CollectionMetadata {
                persist_location: mz_persist_types::PersistLocation::new_in_mem(),
                data_shard: mz_persist_client::ShardId::new(),
                relation_desc: desc.clone(),
                txns_shard: None,
            },
            request: OneshotIngestionRequest {
                source: ContentSource::Http {
                    url: "http://example.invalid/input".parse().expect("valid URL"),
                },
                format: ContentFormat::Parquet,
                filter: ContentFilter::None,
                shape: ContentShape {
                    source_desc: desc,
                    source_mfp: mz_expr::SafeMfpPlan::from_mfp(mz_expr::MapFilterProject::new(0)),
                },
            },
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn query_handshake_completion_and_cancellation_own_the_connection() {
        enum StopAt {
            Handshake,
            Execution,
            Completion,
        }
        for stop in [StopAt::Handshake, StopAt::Execution, StopAt::Completion] {
            let (commands, mut command_rx) = mpsc::unbounded_channel();
            let (responses, response_rx) = mpsc::unbounded_channel();
            let client: Box<dyn StorageClient> = Box::new(LocalClient::new(
                response_rx,
                commands,
                std::thread::current(),
            ));
            let id = Uuid::new_v4();
            let mut execution =
                Box::pin(async move { run(ready(client).await?, request(id)).await });
            assert!(futures::poll!(&mut execution).is_pending());
            assert!(matches!(
                command_rx.try_recv(),
                Ok(StorageCommand::HelloQuery { .. })
            ));
            assert!(command_rx.try_recv().is_err());
            if !matches!(stop, StopAt::Handshake) {
                responses
                    .send(StorageResponse::QueryReady)
                    .expect("handshake is pending");
                assert!(futures::poll!(&mut execution).is_pending());
                assert!(
                    matches!(command_rx.try_recv(), Ok(StorageCommand::RunOneshotIngestion(cmd))
                    if cmd.ingestion_id == id)
                );
            }
            if matches!(stop, StopAt::Completion) {
                responses
                    .send(StorageResponse::StagedBatches(
                        std::collections::BTreeMap::from([(id, vec![Err("source error".into())])]),
                    ))
                    .expect("execution is pending");
                let batches = execution.await.expect("terminal response is delivered");
                assert_eq!(batches.len(), 1);
                assert_eq!(
                    batches[0].as_ref().expect_err("source error"),
                    "source error"
                );
            } else {
                drop(execution);
            }
            assert!(responses.is_closed());
            assert!(command_rx.recv().await.is_none());
        }
    }
}
