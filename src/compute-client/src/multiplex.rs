// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A process-level command/response multiplexer over two compute runtimes.
//!
//! A clusterd process can host two compute runtimes: a `Maintenance` runtime that renders durable,
//! maintained work, and an `Interactive` runtime that serves ephemeral peeks. The compute
//! controller still connects to a single endpoint. [`Multiplexer`] bridges the two: it presents one
//! [`ComputeClient`] to the controller, routes each command to the runtime that owns the referenced
//! work, and merges the two response streams back into one.
//!
//! Routing is derived entirely from command contents (see [`Multiplexer::send`]).
//!
//! The split would otherwise lose one invariant: an index's `since` must not pass the `as_of` of a
//! dataflow importing it. A single command stream ordered the create against every later compaction.
//! Routing the two commands to different runtimes loses that, so `AllowCompaction` for a
//! maintenance-owned collection is *broadcast*: interactive sees it too, applies it as a standing hold
//! on the shared arrangement, and the publisher compacts only as far as the slower of the two runtimes
//! has applied. Interactive therefore has the create and the compactions that follow it back on one
//! ordered stream. See `doc/developer/design/20260720_two_runtime_compute/broadcast-compaction.md`.
//!
//! A second, older mechanism guards the same invariant from the other side: the multiplexer
//! synthesizes `AcquireHolds` onto maintenance's stream when it routes an importing create to
//! interactive. It does not modify compaction frontiers, and the guarantee is entirely within
//! maintenance's own stream: this is the only point that observes both, and it is sequential, so the
//! acquisition precedes every compaction that follows the create. Nothing about interactive's stream
//! enters the argument, which is what makes it hold when interactive is arbitrarily behind or never
//! processes the create at all. The two are independent, and holding both is only redundant, since the
//! trace's `since` is the meet of every hold on it.
//!
//! `ReleaseHolds` goes to interactive instead, so it is ordered against the holder's own lifecycle
//! there. That asymmetry is load-bearing and was forced by the TLA+ model under
//! `doc/developer/design/20260720_two_runtime_compute/protocol-holds`: a release on maintenance's
//! stream can overtake a create interactive has not processed, and the dataflow then renders against
//! compacted data.
//!
//! State is therefore only which runtime renders each transient collection (`transient_owner`) and
//! which of those exports has holds outstanding (`held_exports`). Both are per-connection and
//! discarded by `Hello`, see `Multiplexer::reset`.
//!
//! The multiplexer does not deduplicate peek responses. The exactly-one-`PeekResponse`-per-uuid
//! contract is already upheld below and above it: the per-worker `PartitionedComputeState` inside
//! each process collapses a cancel-versus-complete split across that process's workers into one
//! response, and the controller's per-process `PartitionedComputeState` merges one response per
//! process. Peeks route only to the interactive runtime, so the multiplexer receives exactly one
//! `PeekResponse` per uuid and forwards it verbatim. A multiplexer on a non-zero process never
//! observes the originating `Peek` command anyway (commands other than `Hello`/`UpdateConfiguration`
//! are sent to process 0 only, reaching other processes' workers through the intra-runtime command
//! channel), so it cannot gate responses on having seen the command.

use std::collections::BTreeSet;

use async_trait::async_trait;
use mz_repr::GlobalId;
use mz_service::client::GenericClient;

use crate::protocol::command::{ComputeCommand, HoldRequest};
use crate::protocol::response::ComputeResponse;
use crate::service::ComputeClient;

/// Which of a process's two compute runtimes a piece of work lives on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Runtime {
    /// The runtime that renders durable, maintained collections.
    Maintenance,
    /// The runtime that serves ephemeral, interactive peeks.
    Interactive,
}

/// A single [`ComputeClient`] presented to the controller over two compute runtimes.
///
/// See the module documentation for the routing and merge policy.
#[derive(Debug)]
pub struct Multiplexer {
    /// The runtime that renders durable, maintained collections.
    maintenance: Box<dyn ComputeClient>,
    /// The runtime that serves ephemeral, interactive peeks.
    interactive: Box<dyn ComputeClient>,
    /// The transient collections rendered by the interactive runtime, learned from `CreateDataflow`.
    ///
    /// Only interactive-owned transient ids are recorded. Maintenance is the default in `owner_of`,
    /// so this is a set rather than a map. An entry is evicted when the collection's
    /// `AllowCompaction` reaches the empty frontier, so the set does not grow without bound.
    transient_owner: BTreeSet<GlobalId>,
    /// The interactive exports for which an `AcquireHolds` was synthesized, so that the matching
    /// `ReleaseHolds` is synthesized exactly for those and only once.
    ///
    /// An entry is evicted when the export's `AllowCompaction` reaches the empty frontier, which is
    /// also what emits the release, so the set does not grow without bound.
    held_exports: BTreeSet<GlobalId>,
}

impl Multiplexer {
    /// Wraps a maintenance and an interactive compute client into one multiplexed client.
    pub fn new(maintenance: Box<dyn ComputeClient>, interactive: Box<dyn ComputeClient>) -> Self {
        Self {
            maintenance,
            interactive,
            transient_owner: BTreeSet::new(),
            held_exports: BTreeSet::new(),
        }
    }

    /// Discards all per-connection routing and hold state.
    ///
    /// A `Hello` opens a new protocol epoch: the controller then replays its command history, which
    /// re-establishes ownership and re-derives the holds from the replayed `CreateDataflow`s. Both
    /// replicas discard their own hold state at the same boundary, so carrying `held_exports` across
    /// would synthesize a release for a hold the new epoch never acquired.
    fn reset(&mut self) {
        self.transient_owner.clear();
        self.held_exports.clear();
    }

    /// The runtime that owns `id`. A recorded transient owner wins, otherwise maintenance.
    fn owner_of(&self, id: GlobalId) -> Runtime {
        if self.transient_owner.contains(&id) {
            Runtime::Interactive
        } else {
            Runtime::Maintenance
        }
    }

    /// A mutable handle to the client for `runtime`.
    fn client_mut(&mut self, runtime: Runtime) -> &mut Box<dyn ComputeClient> {
        match runtime {
            Runtime::Maintenance => &mut self.maintenance,
            Runtime::Interactive => &mut self.interactive,
        }
    }

    /// Decides whether a response received from `source` is forwarded to the controller.
    ///
    /// Only `Frontiers` reports are filtered; every other response forwards verbatim.
    ///
    /// Each runtime reports frontiers only for collections it exclusively hosts, so the two streams
    /// never overlap on a collection id:
    ///
    /// * The maintenance runtime hosts every durable, maintained collection, plus the internally
    ///   created logging/introspection indexes, and owns their frontiers. Its transient collections
    ///   are subscribes and copy-tos, which do not emit `Frontiers` (they report through
    ///   `SubscribeResponse`/`CopyToResponse`). So maintenance reports frontiers only for
    ///   non-transient ids.
    /// * The interactive runtime hosts only wholly-transient query dataflows. It installs empty
    ///   copies of maintenance's introspection indexes but does not report their frontiers (see
    ///   `report_frontiers`, which reports only transient collections on the interactive runtime). So
    ///   interactive reports frontiers only for transient ids.
    ///
    /// Filtering on `id.is_transient()` for the interactive source captures that split exactly. It
    /// deliberately does not consult `transient_owner`: that map is evicted when a collection's
    /// `AllowCompaction{empty}` drop is forwarded, which races ahead of the collection's final
    /// (empty) frontier reports. Gating on ownership would drop those trailing reports, so the
    /// controller would never observe the collection's frontiers reach the empty antichain, would
    /// never run `cleanup_collections` for it, and would strand its read holds on its inputs (a stale
    /// `since` on any upstream index/MV the transient read). Forwarding on `is_transient()` delivers
    /// every frontier report for the collections interactive owns, terminal or not.
    fn filter_response(
        &self,
        source: Runtime,
        response: ComputeResponse,
    ) -> Option<ComputeResponse> {
        match response {
            ComputeResponse::Frontiers(id, frontiers) => {
                let forward = match source {
                    Runtime::Maintenance => true,
                    Runtime::Interactive => id.is_transient(),
                };
                forward.then_some(ComputeResponse::Frontiers(id, frontiers))
            }
            other => Some(other),
        }
    }
}

#[async_trait]
impl GenericClient<ComputeCommand, ComputeResponse> for Multiplexer {
    async fn send(&mut self, command: ComputeCommand) -> Result<(), anyhow::Error> {
        use ComputeCommand::*;

        match command {
            // Lifecycle commands drive both runtimes. Send to maintenance first, then interactive.
            // A failure on either surfaces via `?` rather than being swallowed.
            cmd @ Hello { .. } => {
                self.reset();
                self.maintenance.send(cmd.clone()).await?;
                self.interactive.send(cmd).await?;
            }
            cmd @ (CreateInstance(_) | InitializationComplete | UpdateConfiguration(_)) => {
                self.maintenance.send(cmd.clone()).await?;
                self.interactive.send(cmd).await?;
            }
            CreateDataflow(desc) => {
                // Interactive serves a dataflow only when it is wholly transient, has a bounded
                // (non-empty) `until`, and carries no subscribe or copy-to sink. Transience is
                // required, not just a finite `until`: a durable dataflow can also get a finite
                // `until` (a `REFRESH AT` materialized view sets it to the last refresh, see
                // `create_materialized_view.rs`), and `filter_response` forwards interactive's
                // frontier reports only for transient ids. Routing such a dataflow to interactive
                // would make its frontier reports get dropped by that gate, so it must stay on
                // maintenance regardless of `until`. A finite `until` alone marks the dataflow as
                // an ephemeral read that stops on its own, safe to render outside the durable,
                // reconciled maintenance runtime. Subscribes stay on maintenance regardless of
                // `until`. Copy-to is transient and finite-until too, but it drives an S3 sink and
                // is refused by reconciliation, so it is excluded here for that reason, not a
                // frontier one.
                let to_interactive = desc.is_transient()
                    && !desc.until.is_empty()
                    && desc.subscribe_ids().next().is_none()
                    && desc.copy_to_ids().next().is_none();
                if to_interactive {
                    // Acquire the holds on maintenance's stream BEFORE forwarding the create. Only
                    // the position within maintenance's own stream matters, and it is what makes the
                    // invariant hold: this send path is the only place that observes both streams and
                    // it is sequential, so every `AllowCompaction` the controller sends after this
                    // create arrives at maintenance after the acquisition.
                    //
                    // Index imports only. A source import is served from persist, which carries its
                    // own read hold, and the replica has no trace to pin for it.
                    let ids: BTreeSet<_> = desc.index_imports.keys().copied().collect();
                    if !ids.is_empty() {
                        let as_of = desc
                            .as_of
                            .clone()
                            .expect("dataflow as_of is set before it reaches a replica");
                        // One holder per export rather than one per dataflow. The release is driven
                        // by the export's own drop, and a dataflow's exports may drop at different
                        // times, so a single holder would release while another export still reads.
                        for holder in desc.export_ids() {
                            self.maintenance
                                .send(AcquireHolds(Box::new(HoldRequest {
                                    holder,
                                    ids: ids.clone(),
                                    as_of: as_of.clone(),
                                })))
                                .await?;
                            self.held_exports.insert(holder);
                        }
                    }
                    for id in desc.export_ids() {
                        self.transient_owner.insert(id);
                    }
                    self.interactive.send(CreateDataflow(desc)).await?;
                } else {
                    self.maintenance.send(CreateDataflow(desc)).await?;
                }
            }
            Schedule(id) => {
                let runtime = self.owner_of(id);
                self.client_mut(runtime).send(Schedule(id)).await?;
            }
            AllowWrites(id) => {
                let runtime = self.owner_of(id);
                self.client_mut(runtime).send(AllowWrites(id)).await?;
            }
            AllowCompaction { id, frontier } => {
                let runtime = self.owner_of(id);
                // The empty frontier drops the collection. Evict its ownership after forwarding so
                // `transient_owner` does not grow without bound.
                let dropping = frontier.is_empty();
                let evict = dropping && self.transient_owner.contains(&id);
                let release = dropping && self.held_exports.remove(&id);

                // Forwarded verbatim. The frontier is never modified: an importing dataflow's read is
                // protected by the hold acquired for it, not by withholding compaction here. That is
                // also what removes the regression hazard a cap carries, since the command history
                // derives a dataflow's effective `as_of` from the last frontier seen per export.
                self.client_mut(runtime)
                    .send(AllowCompaction {
                        id,
                        frontier: frontier.clone(),
                    })
                    .await?;

                // Broadcast a maintenance-owned compaction to interactive as well, where it advances
                // the standing hold on the shared arrangement rather than compacting a local trace.
                // This is what puts the create and the compactions that follow it on one ordered
                // stream for the runtime that renders the importing dataflow, so a compaction
                // interactive has not applied cannot advance the arrangement's `since` past the `as_of`
                // of a create still queued there. Interactive-owned collections are not published to
                // maintenance and it hosts nothing for them, so those stay routed.
                if runtime == Runtime::Maintenance {
                    self.interactive
                        .send(AllowCompaction { id, frontier })
                        .await?;
                }

                if release {
                    // After the drop, and on interactive's stream, so it is ordered behind both the
                    // create and the drop of the dataflow it releases. Sending it to maintenance
                    // instead would let it overtake a create interactive has not processed, which is
                    // the ordering the model refuted.
                    self.interactive.send(ReleaseHolds { holder: id }).await?;
                }
                if evict {
                    self.transient_owner.remove(&id);
                }
            }
            Peek(peek) => {
                // Every peek is served by interactive.
                self.interactive.send(Peek(peek)).await?;
            }
            cmd @ (AcquireHolds(_) | ReleaseHolds { .. }) => {
                // This multiplexer synthesizes these; the controller never issues them, so
                // receiving one means something upstream is generating commands it should not.
                // Forwarding it would install or drop a hold nobody accounted for.
                anyhow::bail!(
                    "multiplexer received a hold command it should have synthesized: {cmd:?}"
                );
            }
            CancelPeek { uuid } => {
                // The peek lives on interactive, so its cancellation goes there too.
                self.interactive.send(CancelPeek { uuid }).await?;
            }
        }

        Ok(())
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. It `select!`s over the two inner `recv`s, each of which is
    /// cancel safe: dropping the non-selected branch loses no message, and dropping the whole
    /// future (the caller cancelling us) drops both inner futures without loss. The only value
    /// taken from an inner client is returned or dropped synchronously, with no intervening await,
    /// so a cancellation can never strand a response.
    ///
    /// This method never sends, so nothing here can be stranded half-done by a cancellation.
    async fn recv(&mut self) -> Result<Option<ComputeResponse>, anyhow::Error> {
        loop {
            let (source, response) = tokio::select! {
                r = self.maintenance.recv() => (Runtime::Maintenance, r?),
                r = self.interactive.recv() => (Runtime::Interactive, r?),
            };
            match response {
                // Either runtime terminating ends the multiplexed endpoint. The caller must then
                // drop this client, matching the process's all-or-nothing runtime lifecycle.
                None => return Ok(None),
                Some(response) => {
                    if let Some(forward) = self.filter_response(source, response) {
                        return Ok(Some(forward));
                    }
                    // A dropped duplicate `PeekResponse` or a non-owner frontier report. Poll again
                    // for the next response.
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use mz_expr::{MapFilterProject, RowSetFinishing};
    use mz_ore::tracing::OpenTelemetryContext;
    use mz_repr::{GlobalId, RelationDesc, Row, Timestamp};
    use mz_service::client::GenericClient;
    use timely::progress::Antichain;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::protocol::command::{ComputeCommand, Peek, PeekTarget};
    use crate::protocol::response::{ComputeResponse, PeekResponse};
    use crate::service::ComputeClient;

    use super::{Multiplexer, Runtime};

    /// A fake [`ComputeClient`] that records the commands it is sent and replays scripted responses.
    ///
    /// Every send is appended to a `timeline` shared with the other side as well as to this side's own
    /// list, so a test can assert the order in which the two runtimes were addressed. Per-side lists
    /// alone cannot express that: the interesting property is that an acquisition reaches maintenance
    /// before the create reaches interactive.
    #[derive(Debug)]
    struct MockClient {
        side: Runtime,
        sent: Arc<Mutex<Vec<ComputeCommand>>>,
        timeline: Arc<Mutex<Vec<(Runtime, ComputeCommand)>>>,
        responses: mpsc::UnboundedReceiver<ComputeResponse>,
    }

    #[async_trait::async_trait]
    impl GenericClient<ComputeCommand, ComputeResponse> for MockClient {
        async fn send(&mut self, command: ComputeCommand) -> Result<(), anyhow::Error> {
            self.sent
                .lock()
                .expect("lock poisoned")
                .push(command.clone());
            self.timeline
                .lock()
                .expect("lock poisoned")
                .push((self.side, command));
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<ComputeResponse>, anyhow::Error> {
            // `mpsc::UnboundedReceiver::recv` is cancel safe.
            Ok(self.responses.recv().await)
        }
    }

    /// A [`Multiplexer`] over two [`MockClient`]s, with handles to inspect and drive each side.
    struct Harness {
        mux: Multiplexer,
        maint_sent: Arc<Mutex<Vec<ComputeCommand>>>,
        inter_sent: Arc<Mutex<Vec<ComputeCommand>>>,
        timeline: Arc<Mutex<Vec<(Runtime, ComputeCommand)>>>,
        maint_tx: mpsc::UnboundedSender<ComputeResponse>,
        inter_tx: mpsc::UnboundedSender<ComputeResponse>,
    }

    fn harness() -> Harness {
        let maint_sent = Arc::new(Mutex::new(Vec::new()));
        let inter_sent = Arc::new(Mutex::new(Vec::new()));
        let timeline = Arc::new(Mutex::new(Vec::new()));
        let (maint_tx, maint_rx) = mpsc::unbounded_channel();
        let (inter_tx, inter_rx) = mpsc::unbounded_channel();

        let maintenance: Box<dyn ComputeClient> = Box::new(MockClient {
            side: Runtime::Maintenance,
            sent: Arc::clone(&maint_sent),
            timeline: Arc::clone(&timeline),
            responses: maint_rx,
        });
        let interactive: Box<dyn ComputeClient> = Box::new(MockClient {
            side: Runtime::Interactive,
            sent: Arc::clone(&inter_sent),
            timeline: Arc::clone(&timeline),
            responses: inter_rx,
        });

        Harness {
            mux: Multiplexer::new(maintenance, interactive),
            maint_sent,
            inter_sent,
            timeline,
            maint_tx,
            inter_tx,
        }
    }

    /// Every send across both runtimes, in the order the multiplexer made them.
    fn timeline(h: &Harness) -> Vec<(Runtime, ComputeCommand)> {
        h.timeline.lock().expect("lock poisoned").clone()
    }

    /// The compaction frontiers maintenance was told for `id`, in order.
    fn compactions_for(h: &Harness, id: GlobalId) -> Vec<Antichain<Timestamp>> {
        compactions_in(maint_commands(h), id)
    }

    /// The compaction frontiers `cmds` carries for `id`, in order.
    fn compactions_in(cmds: Vec<ComputeCommand>, id: GlobalId) -> Vec<Antichain<Timestamp>> {
        cmds.into_iter()
            .filter_map(|cmd| match cmd {
                ComputeCommand::AllowCompaction { id: seen, frontier } if seen == id => {
                    Some(frontier)
                }
                _ => None,
            })
            .collect()
    }

    fn maint_commands(h: &Harness) -> Vec<ComputeCommand> {
        h.maint_sent.lock().expect("lock poisoned").clone()
    }

    fn inter_commands(h: &Harness) -> Vec<ComputeCommand> {
        h.inter_sent.lock().expect("lock poisoned").clone()
    }

    /// Builds a `CreateDataflow` command exporting `index_ids` as indexes, `subscribe_ids` as
    /// subscribe sinks, and `copy_to_ids` as copy-to-S3 sinks, with the given `until` frontier.
    fn create_dataflow_with(
        index_ids: &[GlobalId],
        subscribe_ids: &[GlobalId],
        copy_to_ids: &[GlobalId],
        until: Antichain<Timestamp>,
    ) -> ComputeCommand {
        use mz_compute_types::dataflows::{DataflowDescription, IndexDesc};
        use mz_compute_types::plan::render_plan::RenderPlan;
        use mz_compute_types::sinks::{
            ComputeSinkConnection, ComputeSinkDesc, CopyToS3OneshotSinkConnection,
            SubscribeSinkConnection,
        };
        use mz_repr::{CatalogItemId, ReprRelationType};
        use mz_storage_types::connections::aws::{AwsAuth, AwsConnection, AwsCredentials};
        use mz_storage_types::connections::string_or_secret::StringOrSecret;
        use mz_storage_types::controller::CollectionMetadata;
        use mz_storage_types::sinks::{S3SinkFormat, S3UploadInfo};

        let mut desc = DataflowDescription::<RenderPlan, CollectionMetadata>::new("test".into());
        desc.until = until;
        for id in index_ids {
            desc.index_exports.insert(
                *id,
                (
                    IndexDesc {
                        on_id: *id,
                        key: Vec::new(),
                    },
                    ReprRelationType::empty(),
                ),
            );
        }
        for id in subscribe_ids {
            desc.sink_exports.insert(
                *id,
                ComputeSinkDesc {
                    from: *id,
                    from_desc: RelationDesc::empty(),
                    connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {
                        output: Vec::new(),
                    }),
                    with_snapshot: true,
                    up_to: Antichain::new(),
                    non_null_assertions: Vec::new(),
                    refresh_schedule: None,
                },
            );
        }
        for id in copy_to_ids {
            desc.sink_exports.insert(
                *id,
                ComputeSinkDesc {
                    from: *id,
                    from_desc: RelationDesc::empty(),
                    connection: ComputeSinkConnection::CopyToS3Oneshot(
                        CopyToS3OneshotSinkConnection {
                            upload_info: S3UploadInfo {
                                uri: "s3://test-bucket/test-path".into(),
                                max_file_size: 1024,
                                desc: RelationDesc::empty(),
                                format: S3SinkFormat::Parquet,
                            },
                            aws_connection: AwsConnection {
                                auth: AwsAuth::Credentials(AwsCredentials {
                                    access_key_id: StringOrSecret::String("access-key".into()),
                                    secret_access_key: CatalogItemId::User(1),
                                    session_token: None,
                                }),
                                region: None,
                                endpoint: None,
                            },
                            connection_id: CatalogItemId::User(2),
                            output_batch_count: 1,
                        },
                    ),
                    with_snapshot: true,
                    up_to: Antichain::new(),
                    non_null_assertions: Vec::new(),
                    refresh_schedule: None,
                },
            );
        }
        ComputeCommand::CreateDataflow(Box::new(desc))
    }

    /// Builds a bounded `CreateDataflow` exporting `index_ids` as indexes and `subscribe_ids` as
    /// subscribe sinks: the shape of a peek-serving dataflow, which is eligible for interactive
    /// routing unless a subscribe sink is present.
    fn create_dataflow(index_ids: &[GlobalId], subscribe_ids: &[GlobalId]) -> ComputeCommand {
        create_dataflow_with(
            index_ids,
            subscribe_ids,
            &[],
            Antichain::from_elem(Timestamp::from(100u64)),
        )
    }

    /// Builds a `Peek` command with the given uuid targeting an index.
    fn peek(uuid: Uuid, literal: Option<Vec<Row>>) -> ComputeCommand {
        let map_filter_project = match MapFilterProject::new(0)
            .into_plan()
            .expect("valid mfp plan")
            .into_nontemporal()
        {
            Ok(safe) => safe,
            Err(_) => unreachable!("empty mfp is non-temporal"),
        };
        ComputeCommand::Peek(Box::new(Peek {
            target: PeekTarget::Index {
                id: GlobalId::Transient(1),
            },
            result_desc: RelationDesc::empty(),
            literal_constraints: literal,
            uuid,
            timestamp: Timestamp::MIN,
            finishing: RowSetFinishing::trivial(0),
            map_filter_project,
            otel_ctx: OpenTelemetryContext::empty(),
        }))
    }

    fn peek_response(uuid: Uuid, resp: PeekResponse) -> ComputeResponse {
        ComputeResponse::PeekResponse(uuid, resp, OpenTelemetryContext::empty())
    }

    #[mz_ore::test(tokio::test)]
    async fn peek_routes_to_interactive() {
        let mut h = harness();
        let uuid = Uuid::from_u128(1);
        h.mux.send(peek(uuid, None)).await.expect("send");

        assert_eq!(inter_commands(&h).len(), 1, "peek must reach interactive");
        assert!(
            maint_commands(&h).is_empty(),
            "peek must not reach maintenance"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn maintained_dataflow_routes_to_maintenance() {
        let mut h = harness();
        // A plain index never bounds `until`, so an unbounded (empty) `until` keeps the dataflow
        // on maintenance regardless of the export id's namespace.
        let cmd = create_dataflow_with(&[GlobalId::User(7)], &[], &[], Antichain::new());
        h.mux.send(cmd).await.expect("send");

        assert_eq!(
            maint_commands(&h).len(),
            1,
            "maintained dataflow to maintenance"
        );
        assert!(inter_commands(&h).is_empty());
    }

    #[mz_ore::test(tokio::test)]
    async fn transient_dataflow_routes_to_interactive_and_records_ownership() {
        let mut h = harness();
        let id = GlobalId::Transient(42);
        h.mux.send(create_dataflow(&[id], &[])).await.expect("send");

        assert_eq!(
            inter_commands(&h).len(),
            1,
            "transient dataflow to interactive"
        );
        assert!(maint_commands(&h).is_empty());

        // Ownership is recorded, so lifecycle commands for the id route to interactive.
        h.mux
            .send(ComputeCommand::Schedule(id))
            .await
            .expect("send");
        assert_eq!(
            inter_commands(&h).len(),
            2,
            "schedule follows transient owner"
        );
        assert!(maint_commands(&h).is_empty());
    }

    #[mz_ore::test(tokio::test)]
    async fn transient_subscribe_dataflow_routes_to_maintenance() {
        let mut h = harness();
        // Transient id, but the dataflow carries a subscribe sink: it must go to maintenance.
        let id = GlobalId::Transient(9);
        h.mux.send(create_dataflow(&[], &[id])).await.expect("send");

        assert_eq!(
            maint_commands(&h).len(),
            1,
            "subscribe stays on maintenance"
        );
        assert!(inter_commands(&h).is_empty());

        // No ownership recorded: lifecycle commands for the id route to maintenance.
        h.mux
            .send(ComputeCommand::Schedule(id))
            .await
            .expect("send");
        assert_eq!(maint_commands(&h).len(), 2);
        assert!(inter_commands(&h).is_empty());
    }

    /// The acquisition reaches maintenance before the create reaches interactive, and compaction is
    /// then forwarded unmodified.
    ///
    /// This is the protocol invariant the runtime split otherwise loses, and the ordering is the whole
    /// mechanism: `CreateDataflow` goes only to interactive while `AllowCompaction` goes to
    /// maintenance, so without the acquisition ahead of it on maintenance's stream, maintenance can
    /// compact the published arrangement out from under a dataflow that has not started.
    ///
    /// The frontier itself is forwarded verbatim. Withholding compaction was the previous mechanism
    /// and it carried a regression hazard, since the command history derives a dataflow's effective
    /// `as_of` from the last frontier seen per export. A hold protects the read instead, so there is
    /// nothing to withhold.
    #[mz_ore::test(tokio::test)]
    async fn acquire_precedes_the_create_and_compaction_is_not_capped() {
        let mut h = harness();
        let source = GlobalId::User(1);
        let export = GlobalId::Transient(7);
        let as_of = Antichain::from_elem(Timestamp::from(100u64));
        let beyond = Antichain::from_elem(Timestamp::from(200u64));

        h.mux
            .send(interactive_import_of(source, export, &as_of))
            .await
            .expect("send create");

        // Both sides were addressed, in this order. Asserted on the shared timeline rather than on the
        // per-side lists, because the ordering across the two runtimes is the claim.
        let seen = timeline(&h);
        assert_eq!(seen.len(), 2, "one acquisition and one create: {seen:?}");
        match &seen[0] {
            (Runtime::Maintenance, ComputeCommand::AcquireHolds(request)) => {
                assert_eq!(request.holder, export);
                assert_eq!(request.ids, BTreeSet::from([source]));
                assert_eq!(request.as_of, as_of);
            }
            other => panic!("expected AcquireHolds to maintenance first, got {other:?}"),
        }
        assert!(
            matches!(
                &seen[1],
                (Runtime::Interactive, ComputeCommand::CreateDataflow(_))
            ),
            "expected the create to interactive second, got {:?}",
            seen[1]
        );

        // The controller now says its own readers are done with `source` beyond the `as_of`.
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id: source,
                frontier: beyond.clone(),
            })
            .await
            .expect("send compaction");

        assert_eq!(
            compactions_for(&h, source),
            vec![beyond],
            "compaction must forward unmodified: the hold protects the read, not a cap"
        );
    }

    /// The release goes to the runtime that renders the holder, ordered behind that holder's own drop.
    ///
    /// Sending it to maintenance instead is the ordering the TLA+ model under
    /// `doc/developer/design/20260720_two_runtime_compute/protocol-holds` refuted on its first run: a
    /// release on maintenance's stream can overtake a create interactive has not processed, so
    /// maintenance would apply acquire, release and compaction while the dataflow was still queued and
    /// the dataflow would then render against compacted data.
    #[mz_ore::test(tokio::test)]
    async fn release_goes_to_the_rendering_runtime_after_the_drop() {
        let mut h = harness();
        let source = GlobalId::User(1);
        let export = GlobalId::Transient(7);
        let as_of = Antichain::from_elem(Timestamp::from(100u64));

        h.mux
            .send(interactive_import_of(source, export, &as_of))
            .await
            .expect("send create");
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id: export,
                frontier: Antichain::new(),
            })
            .await
            .expect("send drop");

        let tail: Vec<_> = timeline(&h).into_iter().skip(2).collect();
        assert!(
            matches!(
                &tail[0],
                (
                    Runtime::Interactive,
                    ComputeCommand::AllowCompaction { id, frontier }
                ) if *id == export && frontier.is_empty()
            ),
            "the drop must reach interactive first, got {:?}",
            tail[0]
        );
        assert!(
            matches!(
                &tail[1],
                (Runtime::Interactive, ComputeCommand::ReleaseHolds { holder }) if *holder == export
            ),
            "the release must follow the drop on interactive's stream, got {:?}",
            tail[1]
        );
        assert_eq!(tail.len(), 2, "nothing else was sent: {tail:?}");
        assert!(
            !maint_commands(&h)
                .iter()
                .any(|cmd| matches!(cmd, ComputeCommand::ReleaseHolds { .. })),
            "the release must never reach the runtime that owns the held collections"
        );
    }

    /// A second drop for the same export does not release twice.
    ///
    /// The replica consumes a release record exactly once, and a spurious second record would be
    /// consumed by the next acquisition for that holder, which would then install no hold at all.
    #[mz_ore::test(tokio::test)]
    async fn release_is_synthesized_once_per_holder() {
        let mut h = harness();
        let source = GlobalId::User(1);
        let export = GlobalId::Transient(7);
        let as_of = Antichain::from_elem(Timestamp::from(100u64));

        h.mux
            .send(interactive_import_of(source, export, &as_of))
            .await
            .expect("send create");
        for _ in 0..2 {
            h.mux
                .send(ComputeCommand::AllowCompaction {
                    id: export,
                    frontier: Antichain::new(),
                })
                .await
                .expect("send drop");
        }

        let releases = inter_commands(&h)
            .into_iter()
            .filter(|cmd| matches!(cmd, ComputeCommand::ReleaseHolds { .. }))
            .count();
        assert_eq!(releases, 1, "exactly one release per holder");
    }

    /// A dataflow with no index imports acquires nothing.
    ///
    /// Its only imports are sources, which are served from persist and carry their own read hold. The
    /// replica has no trace to pin for one, and asking it to would report a missing collection.
    #[mz_ore::test(tokio::test)]
    async fn no_holds_for_a_dataflow_without_index_imports() {
        let mut h = harness();
        let export = GlobalId::Transient(7);
        let mut cmd = create_dataflow_with(
            &[export],
            &[],
            &[],
            Antichain::from_elem(Timestamp::from(300u64)),
        );
        if let ComputeCommand::CreateDataflow(desc) = &mut cmd {
            desc.as_of = Some(Antichain::from_elem(Timestamp::from(100u64)));
        }
        h.mux.send(cmd).await.expect("send create");

        assert!(
            maint_commands(&h).is_empty(),
            "no index imports, so nothing to hold"
        );
        assert_eq!(inter_commands(&h).len(), 1);

        // And no release is synthesized for a holder that acquired nothing.
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id: export,
                frontier: Antichain::new(),
            })
            .await
            .expect("send drop");
        assert!(
            !inter_commands(&h)
                .iter()
                .any(|cmd| matches!(cmd, ComputeCommand::ReleaseHolds { .. })),
            "a holder that acquired nothing must not be released"
        );
    }

    /// A `Hello` discards hold state, so a drop arriving in the new epoch does not synthesize a release
    /// for a hold that epoch never acquired.
    ///
    /// Both replicas discard their own hold state at the same boundary. A stale release would be
    /// consumed by the new epoch's acquisition for the same holder, which would then install nothing
    /// and leave that reader unprotected.
    #[mz_ore::test(tokio::test)]
    async fn hello_discards_stale_hold_state() {
        let mut h = harness();
        let source = GlobalId::User(1);
        let export = GlobalId::Transient(7);
        let as_of = Antichain::from_elem(Timestamp::from(100u64));

        h.mux
            .send(interactive_import_of(source, export, &as_of))
            .await
            .expect("send create");
        h.mux
            .send(ComputeCommand::Hello {
                nonce: Uuid::from_u128(2),
            })
            .await
            .expect("send hello");
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id: export,
                frontier: Antichain::new(),
            })
            .await
            .expect("send drop");

        assert!(
            !inter_commands(&h)
                .iter()
                .any(|cmd| matches!(cmd, ComputeCommand::ReleaseHolds { .. })),
            "a hold from the previous connection must not be released in the new epoch"
        );
    }

    /// A bounded transient dataflow exporting `export` and importing `export`'s source at `as_of`,
    /// which the multiplexer routes to interactive.
    fn interactive_import_of(
        source: GlobalId,
        export: GlobalId,
        as_of: &Antichain<Timestamp>,
    ) -> ComputeCommand {
        let mut cmd = create_dataflow_with(
            &[export],
            &[],
            &[],
            Antichain::from_elem(Timestamp::from(300u64)),
        );
        if let ComputeCommand::CreateDataflow(desc) = &mut cmd {
            desc.as_of = Some(as_of.clone());
            desc.index_imports.insert(
                source,
                mz_compute_types::dataflows::IndexImport {
                    desc: mz_compute_types::dataflows::IndexDesc {
                        on_id: source,
                        key: Vec::new(),
                    },
                    typ: mz_repr::ReprRelationType::empty(),
                    monotonic: false,
                    with_snapshot: true,
                },
            );
        }
        cmd
    }

    #[mz_ore::test(tokio::test)]
    async fn routing_excludes_copy_to_and_subscribe_and_unbounded() {
        let bounded = Antichain::from_elem(Timestamp::from(100u64));

        // A bounded, transient, sinkless dataflow routes to interactive.
        let mut h = harness();
        let cmd = create_dataflow_with(&[GlobalId::Transient(21)], &[], &[], bounded.clone());
        h.mux.send(cmd).await.expect("send");
        assert_eq!(
            inter_commands(&h).len(),
            1,
            "bounded sinkless dataflow to interactive"
        );
        assert!(maint_commands(&h).is_empty());

        // A copy-to dataflow (finite until, transient) routes to maintenance: it drives an S3
        // sink and is refused by reconciliation, so it cannot live on interactive.
        let mut h = harness();
        let cmd = create_dataflow_with(&[], &[], &[GlobalId::Transient(22)], bounded.clone());
        h.mux.send(cmd).await.expect("send");
        assert_eq!(
            maint_commands(&h).len(),
            1,
            "copy-to stays on maintenance despite bounded until"
        );
        assert!(inter_commands(&h).is_empty());

        // A subscribe dataflow routes to maintenance.
        let mut h = harness();
        let cmd = create_dataflow_with(&[], &[GlobalId::Transient(23)], &[], bounded.clone());
        h.mux.send(cmd).await.expect("send");
        assert_eq!(
            maint_commands(&h).len(),
            1,
            "subscribe stays on maintenance despite bounded until"
        );
        assert!(inter_commands(&h).is_empty());

        // An unbounded-until (empty antichain) dataflow routes to maintenance.
        let mut h = harness();
        let cmd = create_dataflow_with(&[GlobalId::Transient(24)], &[], &[], Antichain::new());
        h.mux.send(cmd).await.expect("send");
        assert_eq!(
            maint_commands(&h).len(),
            1,
            "unbounded until stays on maintenance"
        );
        assert!(inter_commands(&h).is_empty());

        // A durable (non-transient), bounded, sinkless dataflow routes to maintenance: the shape
        // of a `REFRESH AT` materialized view, whose `until` is set to its last refresh even
        // though the collection itself is durable. Routing it to interactive would make its
        // frontier reports get dropped by the `is_transient()` gate in `filter_response`.
        let mut h = harness();
        let cmd = create_dataflow_with(&[GlobalId::User(25)], &[], &[], bounded);
        h.mux.send(cmd).await.expect("send");
        assert_eq!(
            maint_commands(&h).len(),
            1,
            "durable dataflow with bounded until stays on maintenance"
        );
        assert!(inter_commands(&h).is_empty());
    }

    #[mz_ore::test(tokio::test)]
    async fn allow_compaction_routes_by_owner_and_evicts_on_empty_frontier() {
        let mut h = harness();
        let id = GlobalId::Transient(5);
        h.mux.send(create_dataflow(&[id], &[])).await.expect("send");

        // Non-empty frontier: routes to interactive, ownership retained.
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id,
                frontier: Antichain::from_elem(Timestamp::from(10u64)),
            })
            .await
            .expect("send");
        assert_eq!(inter_commands(&h).len(), 2);

        // Empty frontier: routes to interactive, then ownership is evicted.
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id,
                frontier: Antichain::new(),
            })
            .await
            .expect("send");
        assert_eq!(
            inter_commands(&h).len(),
            3,
            "empty-frontier compaction to interactive"
        );

        // After eviction, a further command for the id defaults to maintenance.
        h.mux
            .send(ComputeCommand::Schedule(id))
            .await
            .expect("send");
        assert_eq!(
            maint_commands(&h).len(),
            1,
            "evicted id defaults to maintenance"
        );
    }

    /// A compaction for a collection maintenance hosts reaches interactive too, verbatim.
    ///
    /// This is the routing half of I1c. Interactive applies it as a standing hold on the shared
    /// arrangement, which is what puts an importing create and the compactions that follow it back on
    /// one ordered stream there. Withholding it would leave the arrangement compacting at the
    /// controller's pace regardless of what interactive has applied.
    #[mz_ore::test(tokio::test)]
    async fn maintained_compaction_is_broadcast_to_both() {
        let mut h = harness();
        let id = GlobalId::User(7);
        let ten = Antichain::from_elem(Timestamp::from(10u64));

        h.mux
            .send(ComputeCommand::AllowCompaction {
                id,
                frontier: ten.clone(),
            })
            .await
            .expect("send");
        assert_eq!(compactions_for(&h, id), vec![ten.clone()]);
        assert_eq!(
            compactions_in(inter_commands(&h), id),
            vec![ten],
            "interactive must see a maintenance-owned compaction"
        );

        // The drop travels the same way. Interactive releasing its standing hold matters least of all
        // (the arrangement is going away), but the id must not be left with a hold pinning a slot a
        // later collection could reuse.
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id,
                frontier: Antichain::new(),
            })
            .await
            .expect("send");
        assert_eq!(
            compactions_in(inter_commands(&h), id).len(),
            2,
            "the drop is broadcast as well"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn lifecycle_commands_go_to_both() {
        let mut h = harness();
        h.mux
            .send(ComputeCommand::InitializationComplete)
            .await
            .expect("send");
        assert_eq!(maint_commands(&h).len(), 1);
        assert_eq!(inter_commands(&h).len(), 1);
    }

    #[mz_ore::test(tokio::test)]
    async fn peek_response_forwarded_verbatim() {
        let mut h = harness();
        let uuid = Uuid::from_u128(1);
        h.mux.send(peek(uuid, None)).await.expect("send");

        h.inter_tx
            .send(peek_response(uuid, PeekResponse::Rows(Vec::new())))
            .expect("send resp");

        let got = h.mux.recv().await.expect("recv");
        assert!(
            matches!(got, Some(ComputeResponse::PeekResponse(u, _, _)) if u == uuid),
            "peek response forwarded"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn peek_response_forwarded_even_without_prior_command() {
        // A multiplexer on a non-zero process never observes the originating `Peek` command
        // (commands other than lifecycle ones are sent to process 0 only), yet its interactive
        // runtime still produces a response via the intra-runtime command channel. The multiplexer
        // must forward that response rather than gate on having seen the command.
        let mut h = harness();
        let uuid = Uuid::from_u128(6);

        h.inter_tx
            .send(peek_response(uuid, PeekResponse::Rows(Vec::new())))
            .expect("send resp");

        let got = h.mux.recv().await.expect("recv");
        assert!(
            matches!(got, Some(ComputeResponse::PeekResponse(u, _, _)) if u == uuid),
            "response forwarded despite no prior Peek command on this multiplexer"
        );
        assert!(
            inter_commands(&h).is_empty(),
            "no command was sent to this multiplexer"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn point_lookup_peek_yields_exactly_one_response() {
        let mut h = harness();
        let uuid = Uuid::from_u128(3);
        h.mux
            .send(peek(uuid, Some(vec![Row::default()])))
            .await
            .expect("send");
        h.inter_tx
            .send(peek_response(uuid, PeekResponse::Rows(Vec::new())))
            .expect("send resp");

        let got = h.mux.recv().await.expect("recv");
        assert!(
            matches!(got, Some(ComputeResponse::PeekResponse(u, _, _)) if u == uuid),
            "point-lookup response forwarded once"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn peek_responses_forwarded_without_dedup() {
        // The multiplexer does not deduplicate peek responses; that is the job of the per-worker
        // `PartitionedComputeState` below it (which collapses a cancel-versus-complete split) and
        // the per-process one above it. If two responses arrive for one uuid, the multiplexer
        // forwards both verbatim and lets the layers around it enforce exactly-one.
        let mut h = harness();
        let uuid = Uuid::from_u128(4);
        h.mux.send(peek(uuid, None)).await.expect("send");
        h.inter_tx
            .send(peek_response(uuid, PeekResponse::Rows(Vec::new())))
            .expect("send rows");
        h.inter_tx
            .send(peek_response(uuid, PeekResponse::Canceled))
            .expect("send canceled");

        let first = h.mux.recv().await.expect("recv");
        assert!(matches!(
            first,
            Some(ComputeResponse::PeekResponse(u, PeekResponse::Rows(_), _)) if u == uuid
        ));
        let second = h.mux.recv().await.expect("recv");
        assert!(
            matches!(
                second,
                Some(ComputeResponse::PeekResponse(u, PeekResponse::Canceled, _)) if u == uuid
            ),
            "second response forwarded verbatim, not dropped"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn status_forwarded_from_both_sides() {
        use crate::protocol::response::StatusResponse;

        let mut h = harness();
        h.maint_tx
            .send(ComputeResponse::Status(StatusResponse::Placeholder))
            .expect("send maint status");
        h.inter_tx
            .send(ComputeResponse::Status(StatusResponse::Placeholder))
            .expect("send inter status");

        let a = h.mux.recv().await.expect("recv");
        let b = h.mux.recv().await.expect("recv");
        assert!(matches!(a, Some(ComputeResponse::Status(_))));
        assert!(matches!(b, Some(ComputeResponse::Status(_))));
    }

    fn frontiers(id: GlobalId, ts: u64) -> ComputeResponse {
        use crate::protocol::response::FrontiersResponse;
        ComputeResponse::Frontiers(
            id,
            FrontiersResponse {
                write_frontier: Some(Antichain::from_elem(Timestamp::from(ts))),
                input_frontier: None,
                output_frontier: None,
            },
        )
    }

    #[mz_ore::test(tokio::test)]
    async fn frontiers_forwarded_from_owning_runtime() {
        let mut h = harness();
        let id = GlobalId::Transient(11);
        // The dataflow is transient, so interactive owns the collection.
        h.mux.send(create_dataflow(&[id], &[])).await.expect("send");
        // Interactive emits frontiers for its transient collection: forwarded.
        h.inter_tx.send(frontiers(id, 1)).expect("send frontiers");
        let got = h.mux.recv().await.expect("recv");
        assert!(matches!(got, Some(ComputeResponse::Frontiers(g, _)) if g == id));
    }

    #[mz_ore::test(tokio::test)]
    async fn frontiers_dropped_from_non_owning_runtime() {
        // Both runtimes install the internally-created logging/introspection dataflows and report
        // frontiers for the same (maintained) collection id. Only the owner's (maintenance's) report
        // may reach the controller; the interactive runtime's empty copy must be dropped, else the
        // controller sees the collection's frontier regress.
        let mut h = harness();
        // A maintained id: never recorded as a transient owner, so maintenance owns it.
        let id = GlobalId::System(42);
        // The interactive runtime's empty logging collection reports the empty frontier first, then
        // maintenance reports a real, finite frontier. The interactive report must be dropped.
        h.inter_tx
            .send(ComputeResponse::Frontiers(
                id,
                crate::protocol::response::FrontiersResponse {
                    write_frontier: Some(Antichain::new()),
                    input_frontier: None,
                    output_frontier: None,
                },
            ))
            .expect("send empty frontier from interactive");
        h.maint_tx.send(frontiers(id, 100)).expect("send maint");

        // The first forwarded frontier is maintenance's finite one, not interactive's empty one.
        let got = h.mux.recv().await.expect("recv");
        match got {
            Some(ComputeResponse::Frontiers(g, f)) => {
                assert_eq!(g, id);
                assert_eq!(
                    f.write_frontier,
                    Some(Antichain::from_elem(Timestamp::from(100u64))),
                    "interactive's empty frontier was dropped; maintenance's forwarded"
                );
            }
            other => panic!("expected maintenance frontier, got {other:?}"),
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn interactive_transient_frontiers_forwarded_including_after_eviction() {
        // The interactive runtime reports frontiers only for the transient collections it hosts, so
        // every such report forwards, regardless of `transient_owner`. In particular the trailing
        // (empty) reports a dropped transient emits must reach the controller even though its
        // `AllowCompaction{empty}` already evicted the ownership entry: the controller runs
        // `cleanup_collections` and releases the collection's input read holds only once it observes
        // all of those frontiers reach the empty antichain. Dropping any of them strands the holds
        // and pins upstream read frontiers.
        let mut h = harness();
        let id = GlobalId::Transient(7);
        h.mux.send(create_dataflow(&[id], &[])).await.expect("send");
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id,
                frontier: Antichain::new(),
            })
            .await
            .expect("send");
        // Ownership is now evicted, so `owner_of(id)` resolves to maintenance.

        // A non-empty trailing report for the evicted transient still forwards (not gated on
        // ownership).
        h.inter_tx.send(frontiers(id, 5)).expect("send inter");
        let got = h.mux.recv().await.expect("recv");
        match got {
            Some(ComputeResponse::Frontiers(g, f)) => {
                assert_eq!(g, id);
                assert_eq!(
                    f.write_frontier,
                    Some(Antichain::from_elem(Timestamp::from(5u64)))
                );
            }
            other => panic!("expected forwarded interactive frontier, got {other:?}"),
        }

        // The terminal all-empty report also forwards.
        h.inter_tx
            .send(ComputeResponse::Frontiers(
                id,
                crate::protocol::response::FrontiersResponse {
                    write_frontier: Some(Antichain::new()),
                    input_frontier: Some(Antichain::new()),
                    output_frontier: Some(Antichain::new()),
                },
            ))
            .expect("send terminal frontier");
        let got = h.mux.recv().await.expect("recv");
        assert!(
            matches!(got, Some(ComputeResponse::Frontiers(g, _)) if g == id),
            "terminal transient frontier must be forwarded after eviction, got {got:?}"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn recv_loses_no_message_when_both_sides_ready() {
        // Both runtimes have a message ready. `select!` picks one and drops the other's future;
        // the dropped side's message must survive to the next `recv` (recv cancel-safety).
        use crate::protocol::response::StatusResponse;

        let mut h = harness();
        h.maint_tx
            .send(ComputeResponse::Status(StatusResponse::Placeholder))
            .expect("send");
        h.inter_tx
            .send(ComputeResponse::Status(StatusResponse::Placeholder))
            .expect("send");

        let first = h.mux.recv().await.expect("recv");
        let second = h.mux.recv().await.expect("recv");
        assert!(matches!(first, Some(ComputeResponse::Status(_))));
        assert!(
            matches!(second, Some(ComputeResponse::Status(_))),
            "the non-selected side's message was not lost"
        );
    }

    // Silence the unused-variant warning for the private `Runtime` enum in case a future edit drops
    // a match arm. This keeps the enum exercised by tests.
    #[mz_ore::test]
    fn runtime_variants_distinct() {
        assert_ne!(Runtime::Maintenance, Runtime::Interactive);
    }
}
