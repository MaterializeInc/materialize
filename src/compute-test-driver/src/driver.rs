// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The headless `Driver`: the mechanism's top-level API. Use cases call it.

use std::time::Duration;

use mz_compute_client::protocol::command::{ComputeCommand, Peek, PeekTarget};
use mz_compute_client::protocol::response::{FrontiersResponse, PeekResponse};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_expr::{MapFilterProject, RowSetFinishing};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, IntoRowIterator, RelationDesc, Row, RowIterator, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use timely::progress::Antichain;

use crate::ctp::connect_and_handshake;
use crate::persist_host::PersistHost;
use crate::responses::{ComputeSender, Responses};

/// Headless frontend to a clusterd replica.
pub struct Driver {
    pub host: PersistHost,
    sender: ComputeSender,
    responses: Responses,
}

impl Driver {
    /// Connects to `compute_addr`, completes the handshake, and starts the
    /// response pump. `host` provides persist + pubsub.
    pub async fn connect(host: PersistHost, compute_addr: &str) -> anyhow::Result<Self> {
        let client = connect_and_handshake(compute_addr, host.location().clone()).await?;
        let (responses, sender) = Responses::spawn(client);
        Ok(Driver {
            host,
            sender,
            responses,
        })
    }

    /// Sends a raw `ComputeCommand`. The primitive behind every interaction;
    /// use cases drive side effects (`AllowCompaction`, `UpdateConfiguration`,
    /// `CancelPeek`, ...) through this without the mechanism interpreting them.
    pub fn send(&self, cmd: ComputeCommand) -> anyhow::Result<()> {
        self.sender.send(cmd)
    }

    /// Submits a dataflow. Does NOT schedule it — the caller decides when to
    /// `schedule`, so side-effect timing stays under test control.
    pub fn submit_dataflow(
        &self,
        df: DataflowDescription<RenderPlan, CollectionMetadata>,
    ) -> anyhow::Result<()> {
        self.send(ComputeCommand::CreateDataflow(Box::new(df)))
    }

    /// Schedules a previously-submitted collection, allowing it to make progress.
    pub fn schedule(&self, id: GlobalId) -> anyhow::Result<()> {
        self.send(ComputeCommand::Schedule(id))
    }

    /// A receiver for an id's full merged frontiers, for use cases that need
    /// write/input frontiers rather than just output.
    pub fn frontiers(&self, id: GlobalId) -> tokio::sync::watch::Receiver<FrontiersResponse> {
        self.responses.frontier(id)
    }

    /// Waits until `id`'s output frontier reaches at least `target`, or fails.
    pub async fn expect_frontier(
        &self,
        id: GlobalId,
        target: Timestamp,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let mut rx = self.responses.frontier(id);
        let want = Antichain::from_elem(target);
        tokio::time::timeout(timeout, async {
            loop {
                let reached = rx
                    .borrow_and_update()
                    .output_frontier
                    .as_ref()
                    .is_some_and(|of| timely::PartialOrder::less_equal(&want, of));
                if reached {
                    return;
                }
                if rx.changed().await.is_err() {
                    // The watch sender is gone (pump exited, e.g. clusterd
                    // disconnected). The frontier can no longer advance, so
                    // park and let the outer timeout fire with its message.
                    futures::future::pending::<()>().await;
                }
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("frontier for {id} did not reach {target:?} in time"))
    }

    /// Peeks an index at `ts`, returning the decoded rows.
    pub async fn peek(
        &self,
        id: GlobalId,
        result_desc: RelationDesc,
        ts: Timestamp,
    ) -> anyhow::Result<Vec<Row>> {
        let uuid = uuid::Uuid::new_v4();
        let rx = self.responses.register_peek(uuid);
        let arity = result_desc.arity();
        // Build an identity MFP: no maps, no filters, project all columns.
        let map_filter_project = MapFilterProject::new(arity)
            .into_plan()
            .map_err(|e| anyhow::anyhow!("failed to plan MFP: {e}"))?
            .into_nontemporal()
            .map_err(|_| anyhow::anyhow!("unexpected temporal MFP for identity plan"))?;
        let peek = Peek {
            target: PeekTarget::Index { id },
            result_desc: result_desc.clone(),
            literal_constraints: None,
            uuid,
            timestamp: ts,
            finishing: RowSetFinishing::trivial(arity),
            map_filter_project,
            otel_ctx: OpenTelemetryContext::empty(),
        };
        self.send(ComputeCommand::Peek(Box::new(peek)))?;
        match rx.await? {
            PeekResponse::Rows(collections) => {
                let mut rows = Vec::new();
                for collection in collections {
                    let mut iter = collection.into_row_iter();
                    while let Some(row_ref) = iter.next() {
                        rows.push(row_ref.to_owned());
                    }
                }
                Ok(rows)
            }
            PeekResponse::Error(e) => anyhow::bail!("peek error: {e}"),
            PeekResponse::Canceled => anyhow::bail!("peek canceled"),
            PeekResponse::Stashed(_) => anyhow::bail!("unexpected stashed peek result"),
        }
    }

    /// Convenience: total row count from a peek.
    pub async fn peek_count(
        &self,
        id: GlobalId,
        result_desc: RelationDesc,
        ts: Timestamp,
    ) -> anyhow::Result<usize> {
        Ok(self.peek(id, result_desc, ts).await?.len())
    }
}
