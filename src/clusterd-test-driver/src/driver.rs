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

use mz_compute_client::protocol::command::{
    ComputeCommand, ComputeParameters, InstanceConfig, Peek, PeekTarget,
};
use mz_compute_client::protocol::response::{FrontiersResponse, PeekResponse};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::dyncfgs::ENABLE_PEEK_RESPONSE_STASH;
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_dyncfg::ConfigUpdates;
use mz_expr::{MapFilterProject, RowSetFinishing};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, IntoRowIterator, RelationDesc, Row, RowIterator, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use timely::progress::Antichain;

use crate::ctp::connect_and_hello;
use crate::persist_host::PersistHost;
use crate::responses::{ComputeSender, Responses};

/// Headless frontend to a clusterd replica.
pub struct Driver {
    pub host: PersistHost,
    compute_addr: String,
    sender: ComputeSender,
    responses: Responses,
}

impl Driver {
    /// Connects to `compute_addr`, sends `Hello`, and starts the response pump.
    /// `host` provides persist + pubsub. The controller handshake proper
    /// (`create_instance`, `update_configuration`, `InitializationComplete`) is
    /// driven by the caller — see the script commands of the same name.
    pub async fn connect(host: PersistHost, compute_addr: &str) -> anyhow::Result<Self> {
        let client = connect_and_hello(compute_addr).await?;
        let (responses, sender) = Responses::spawn(client);
        Ok(Driver {
            host,
            compute_addr: compute_addr.to_string(),
            sender,
            responses,
        })
    }

    /// Drops the current connection and opens a new one, sending only `Hello`, so
    /// the caller can re-`create_instance`, replay the dataflows it expects the
    /// replica to be running, and then send `InitializationComplete` to close the
    /// reconciliation window.
    ///
    /// Replacing `sender` drops the previous [`ComputeSender`]; with no other
    /// clones, the old pump task's command channel closes and the pump exits,
    /// dropping the old CTP client and closing the old connection.
    pub async fn reconnect(&mut self) -> anyhow::Result<()> {
        let client = connect_and_hello(&self.compute_addr).await?;
        let (responses, sender) = Responses::spawn(client);
        self.responses = responses;
        self.sender = sender;
        Ok(())
    }

    /// Sends `CreateInstance`, opening the compute instance (and the reconciliation
    /// window).
    ///
    /// `expiration_offset`, `arrangement_dictionary_compression`, and `initial_config` are the
    /// caller-settable [`InstanceConfig`] knobs; `logging` is left at its default
    /// (introspection logging off — enabling it safely needs `index_logs` wiring)
    /// and `peek_stash_persist_location` is necessarily the host's, since the driver
    /// hosts persist.
    ///
    /// `initial_config` is the create-time configuration snapshot the controller would build from
    /// its synced dyncfg. The replica applies it before create-time setup, so a script can assert
    /// that create-time work observes synced values rather than defaults. The peek-response stash
    /// is always force-disabled on top of it: the driver reads peek results inline, so a stashed
    /// peek would break [`Self::peek`]/`count`. It is patched here rather than exposed as a knob,
    /// so neither `initial_config` nor a later `update-configuration` can turn it back on.
    pub fn create_instance(
        &self,
        expiration_offset: Option<Duration>,
        arrangement_dictionary_compression: bool,
        mut initial_config: ConfigUpdates,
    ) -> anyhow::Result<()> {
        initial_config.add(&ENABLE_PEEK_RESPONSE_STASH, false);
        self.send(ComputeCommand::CreateInstance(Box::new(InstanceConfig {
            logging: Default::default(),
            expiration_offset,
            peek_stash_persist_location: self.host.location().clone(),
            arrangement_dictionary_compression,
            initial_config,
        })))?;
        let mut dyncfg_updates = ConfigUpdates::default();
        dyncfg_updates.add(&ENABLE_PEEK_RESPONSE_STASH, false);
        self.send(ComputeCommand::UpdateConfiguration(Box::new(
            ComputeParameters {
                dyncfg_updates,
                ..Default::default()
            },
        )))
    }

    /// Sends `UpdateConfiguration` with a set of dyncfg updates assembled by the
    /// caller. Generic over any configuration; the peek-response stash is not among
    /// them — it is force-disabled in [`Self::create_instance`].
    pub fn update_configuration(&self, dyncfg_updates: ConfigUpdates) -> anyhow::Result<()> {
        self.send(ComputeCommand::UpdateConfiguration(Box::new(
            ComputeParameters {
                dyncfg_updates,
                ..Default::default()
            },
        )))
    }

    /// Sends a raw `ComputeCommand`. The primitive behind every interaction;
    /// use cases drive side effects (`AllowCompaction`, `CancelPeek`, ...) through
    /// this without the mechanism interpreting them.
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

    /// Peeks `target` at `ts`, returning the decoded rows. The target is an index
    /// (served from the replica's arrangement) or a persist collection — notably a
    /// materialized-view sink's output shard, which is how `SELECT * FROM mv` reads.
    /// A persist peek blocks (async-friendly) until the shard seals through `ts`, so
    /// it doubles as a wait for the writing sink to catch up.
    pub async fn peek(
        &self,
        target: PeekTarget,
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
            target,
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
        target: PeekTarget,
        result_desc: RelationDesc,
        ts: Timestamp,
    ) -> anyhow::Result<usize> {
        Ok(self.peek(target, result_desc, ts).await?.len())
    }

    /// Registers a subscribe-sink buffer for `id`, so the response pump accumulates
    /// its batches. Call this before scheduling the sink.
    pub fn register_subscribe(&self, id: GlobalId) {
        let _ = self.responses.ensure_subscribe(id);
    }

    /// Waits until subscribe `id`'s upper frontier reaches at least `up_to`, then
    /// drains and returns its buffered `(row, time, diff)` updates. Fails on timeout
    /// or if the replica reported a subscribe error.
    pub async fn await_subscribe(
        &self,
        id: GlobalId,
        up_to: Timestamp,
        timeout: Duration,
    ) -> anyhow::Result<Vec<(Row, Timestamp, i64)>> {
        let mut rx = self.responses.ensure_subscribe(id);
        let want = Antichain::from_elem(up_to);
        tokio::time::timeout(timeout, async {
            loop {
                // An empty upper (the subscribe was dropped / completed) is past any
                // finite target, so `less_equal` against it also unblocks.
                let reached = timely::PartialOrder::less_equal(&want, &*rx.borrow_and_update());
                if reached {
                    return;
                }
                if rx.changed().await.is_err() {
                    // The pump exited (clusterd disconnected); the upper can no longer
                    // advance, so park and let the outer timeout fire.
                    futures::future::pending::<()>().await;
                }
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("subscribe {id} did not reach {up_to:?} in time"))?;
        self.responses.drain_subscribe(id)
    }
}
