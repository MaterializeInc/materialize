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
//! Routing is derived entirely from command contents (see [`Multiplexer::send`]). The only state the
//! multiplexer keeps is which runtime renders each transient collection (`transient_owner`).
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

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use mz_repr::{GlobalId, Timestamp};
use mz_service::client::GenericClient;
use timely::progress::Antichain;

use crate::protocol::command::ComputeCommand;
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
    /// Read holds an interactive dataflow needs on the maintenance collections it imports, keyed by
    /// the importing dataflow's exports. See [`Multiplexer::hold_floor`].
    interactive_holds: BTreeMap<GlobalId, InteractiveHold>,
    /// The last `AllowCompaction` frontier the controller asked for on a held collection, so the
    /// deferred part can be released once the holds are gone.
    deferred_compaction: BTreeMap<GlobalId, Antichain<Timestamp>>,
    /// The lowest frontier each collection may still be told to compact to.
    ///
    /// Seeded from a collection's own dataflow `as_of` and raised by every `AllowCompaction` we
    /// forward. Compaction frontiers may not regress: the command history derives a dataflow's
    /// effective `as_of` from the last frontier seen per export, so a lower one later reads as the
    /// dataflow's `as_of` moving backwards. Capping must respect this even when it means declining
    /// to cap.
    compaction_floor: BTreeMap<GlobalId, Antichain<Timestamp>>,
}

/// The imports an in-flight interactive dataflow reads, and the frontier it reads them at.
#[derive(Debug)]
struct InteractiveHold {
    imports: BTreeSet<GlobalId>,
    as_of: Antichain<Timestamp>,
}

/// Copies a hold, so a dataflow with several exports records one per export and any of them
/// releasing the dataflow releases its holds.
fn hold_clone(hold: &InteractiveHold) -> InteractiveHold {
    InteractiveHold {
        imports: hold.imports.clone(),
        as_of: hold.as_of.clone(),
    }
}

impl Multiplexer {
    /// Wraps a maintenance and an interactive compute client into one multiplexed client.
    pub fn new(maintenance: Box<dyn ComputeClient>, interactive: Box<dyn ComputeClient>) -> Self {
        Self {
            maintenance,
            interactive,
            transient_owner: BTreeSet::new(),
            interactive_holds: BTreeMap::new(),
            deferred_compaction: BTreeMap::new(),
            compaction_floor: BTreeMap::new(),
        }
    }

    /// The lowest `as_of` any in-flight interactive dataflow reads `id` at, if any.
    ///
    /// Maintenance may not compact `id` past this, see the protocol invariants in the design doc.
    /// The controller holds a read hold covering these reads, but the hold is only realized on the
    /// replica when the interactive runtime renders the dataflow, and the interactive runtime can be
    /// arbitrarily behind. Capping here restores the ordering that a single command stream used to
    /// provide, at the one point that observes both streams.
    fn hold_floor(&self, id: GlobalId) -> Option<Antichain<Timestamp>> {
        self.interactive_holds
            .values()
            .filter(|hold| hold.imports.contains(&id))
            .map(|hold| hold.as_of.clone())
            .min_by(|a, b| {
                // Antichains are only partially ordered. Any minimal element is a sound floor, and
                // for the single-element antichains a dataflow `as_of` carries this is the minimum.
                if timely::PartialOrder::less_equal(a, b) {
                    std::cmp::Ordering::Less
                } else if timely::PartialOrder::less_equal(b, a) {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
    }

    /// Releases the holds of the interactive dataflow exporting `id`, returning the collections
    /// whose deferred compaction may now be forwarded.
    fn release_holds(&mut self, id: GlobalId) -> Vec<(GlobalId, Antichain<Timestamp>)> {
        let Some(hold) = self.interactive_holds.remove(&id) else {
            return Vec::new();
        };
        let released: Vec<_> = hold
            .imports
            .into_iter()
            .filter(|import| self.hold_floor(*import).is_none())
            .collect();
        released
            .into_iter()
            .filter_map(|import| {
                self.deferred_compaction
                    .remove(&import)
                    .map(|frontier| (import, frontier))
            })
            .collect()
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
            cmd @ (Hello { .. }
            | CreateInstance(_)
            | InitializationComplete
            | UpdateConfiguration(_)) => {
                self.maintenance.send(cmd.clone()).await?;
                self.interactive.send(cmd).await?;
            }
            CreateDataflow(desc) => {
                // A collection can never be told to compact below its own dataflow's `as_of`.
                if let Some(as_of) = desc.as_of.clone() {
                    for id in desc.export_ids() {
                        self.compaction_floor.insert(id, as_of.clone());
                    }
                }
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
                    // Record what this dataflow reads BEFORE forwarding it, so any later
                    // `AllowCompaction` on maintenance's stream is already capped. Ordering here is
                    // what makes the invariant hold: this send path is the only place that observes
                    // both runtimes' command streams, and it is sequential.
                    let imports: BTreeSet<_> = desc.import_ids().collect();
                    if !imports.is_empty() {
                        let hold = InteractiveHold {
                            imports,
                            as_of: desc
                                .as_of
                                .clone()
                                .expect("dataflow as_of is set before it reaches a replica"),
                        };
                        for id in desc.export_ids() {
                            self.interactive_holds.insert(id, hold_clone(&hold));
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
                let frontier_was_empty = frontier.is_empty();
                let evict = frontier_was_empty && self.transient_owner.contains(&id);

                // Cap the frontier at what in-flight interactive dataflows still read. The
                // controller is telling us its own readers are done, but an interactive dataflow it
                // created may not have rendered yet, and maintenance would otherwise compact the
                // published arrangement out from under it. Under-compacting is always safe.
                let below_floor = |f: &Antichain<Timestamp>| {
                    self.compaction_floor
                        .get(&id)
                        .is_some_and(|floor| !timely::PartialOrder::less_equal(floor, f))
                };
                let capped = match self.hold_floor(id) {
                    // Declining to cap when the hold sits below the collection's floor is not a
                    // choice, it is the only legal move: the frontier has already been released past
                    // what this reader needs, so capping now would regress the collection's `as_of`
                    // without restoring anything. The interactive import's own `as_of` check is what
                    // catches the read that can no longer be served.
                    Some(hold) if !timely::PartialOrder::less_equal(&frontier, &hold) => {
                        if below_floor(&hold) {
                            frontier
                        } else {
                            self.deferred_compaction.insert(id, frontier);
                            hold
                        }
                    }
                    _ => frontier,
                };
                self.compaction_floor.insert(id, capped.clone());
                self.client_mut(runtime)
                    .send(AllowCompaction {
                        id,
                        frontier: capped,
                    })
                    .await?;

                if evict {
                    self.transient_owner.remove(&id);
                }
                // An interactive dataflow's collection reaching the empty frontier is the drop
                // signal for that dataflow, so its holds go with it. Forward whatever compaction was
                // deferred behind them, otherwise the imported collections never compact again.
                //
                // Only the empty frontier releases. A non-empty `AllowCompaction` on an export is
                // routine, the controller sends one whenever the collection's read frontier moves,
                // and releasing on those would drop the hold while the interactive runtime still has
                // the create queued. That is the exact regression this capping exists to prevent.
                let released = if frontier_was_empty {
                    self.release_holds(id)
                } else {
                    Vec::new()
                };
                for (import, frontier) in released {
                    let runtime = self.owner_of(import);
                    self.compaction_floor.insert(import, frontier.clone());
                    self.client_mut(runtime)
                        .send(AllowCompaction {
                            id: import,
                            frontier,
                        })
                        .await?;
                }
            }
            Peek(peek) => {
                // Every peek is served by interactive.
                self.interactive.send(Peek(peek)).await?;
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
    #[derive(Debug)]
    struct MockClient {
        sent: Arc<Mutex<Vec<ComputeCommand>>>,
        responses: mpsc::UnboundedReceiver<ComputeResponse>,
    }

    #[async_trait::async_trait]
    impl GenericClient<ComputeCommand, ComputeResponse> for MockClient {
        async fn send(&mut self, command: ComputeCommand) -> Result<(), anyhow::Error> {
            self.sent.lock().expect("lock poisoned").push(command);
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
        maint_tx: mpsc::UnboundedSender<ComputeResponse>,
        inter_tx: mpsc::UnboundedSender<ComputeResponse>,
    }

    fn harness() -> Harness {
        let maint_sent = Arc::new(Mutex::new(Vec::new()));
        let inter_sent = Arc::new(Mutex::new(Vec::new()));
        let (maint_tx, maint_rx) = mpsc::unbounded_channel();
        let (inter_tx, inter_rx) = mpsc::unbounded_channel();

        let maintenance: Box<dyn ComputeClient> = Box::new(MockClient {
            sent: Arc::clone(&maint_sent),
            responses: maint_rx,
        });
        let interactive: Box<dyn ComputeClient> = Box::new(MockClient {
            sent: Arc::clone(&inter_sent),
            responses: inter_rx,
        });

        Harness {
            mux: Multiplexer::new(maintenance, interactive),
            maint_sent,
            inter_sent,
            maint_tx,
            inter_tx,
        }
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

    /// An interactive dataflow's imports are not compacted past its `as_of`, even when the
    /// controller allows it before the interactive runtime has rendered anything.
    ///
    /// This is the protocol invariant the runtime split otherwise loses. `CreateDataflow` goes only
    /// to interactive while `AllowCompaction` goes to maintenance, and the two runtimes drain their
    /// streams independently, so without capping here maintenance can compact the published
    /// arrangement out from under a dataflow that has not started.
    #[mz_ore::test(tokio::test)]
    async fn interactive_imports_are_not_compacted_past_their_as_of() {
        let mut h = harness();
        let source = GlobalId::User(1);
        let as_of = Antichain::from_elem(Timestamp::from(100u64));
        let beyond = Antichain::from_elem(Timestamp::from(200u64));

        // A bounded transient dataflow importing `source`, routed to interactive.
        let mut cmd = create_dataflow_with(
            &[GlobalId::Transient(7)],
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
        h.mux.send(cmd).await.expect("send create");

        // The controller now says its own readers are done with `source` beyond the `as_of`.
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id: source,
                frontier: beyond.clone(),
            })
            .await
            .expect("send compaction");

        let capped = maint_commands(&h)
            .into_iter()
            .find_map(|cmd| match cmd {
                ComputeCommand::AllowCompaction { id, frontier } if id == source => Some(frontier),
                _ => None,
            })
            .expect("maintenance sees a compaction for the imported collection");
        assert_eq!(
            capped, as_of,
            "compaction must be capped at the interactive dataflow's as_of, not {beyond:?}"
        );

        // Dropping the interactive dataflow releases the hold, and the deferred compaction is
        // forwarded so the collection is not pinned forever.
        h.mux
            .send(ComputeCommand::AllowCompaction {
                id: GlobalId::Transient(7),
                frontier: Antichain::new(),
            })
            .await
            .expect("send drop");

        let forwarded: Vec<_> = maint_commands(&h)
            .into_iter()
            .filter_map(|cmd| match cmd {
                ComputeCommand::AllowCompaction { id, frontier } if id == source => Some(frontier),
                _ => None,
            })
            .collect();
        assert_eq!(
            forwarded.last(),
            Some(&beyond),
            "the deferred compaction must be forwarded once the hold is released"
        );
    }

    /// Capping never sends a collection a frontier below one already sent for it.
    ///
    /// Compaction frontiers may not regress. The command history derives a dataflow's effective
    /// `as_of` from the last frontier seen per export, so a lower one later reads as that dataflow's
    /// `as_of` moving backwards and trips the history's own check. A hold below what has already
    /// been released cannot restore anything, so the only legal move is to decline to cap.
    #[mz_ore::test(tokio::test)]
    async fn capping_never_regresses_a_compaction_frontier() {
        let mut h = harness();
        let source = GlobalId::User(1);
        let stale = Antichain::from_elem(Timestamp::from(50u64));
        let released = Antichain::from_elem(Timestamp::from(100u64));

        // The imported collection's own dataflow starts at 100, so it may never be told to compact
        // below that.
        let mut maintained = create_dataflow_with(&[source], &[], &[], Antichain::new());
        if let ComputeCommand::CreateDataflow(desc) = &mut maintained {
            desc.as_of = Some(released.clone());
        }
        h.mux.send(maintained).await.expect("send maintained");

        // An interactive dataflow claiming to read it at 50, below that floor.
        let mut cmd = create_dataflow_with(
            &[GlobalId::Transient(7)],
            &[],
            &[],
            Antichain::from_elem(Timestamp::from(300u64)),
        );
        if let ComputeCommand::CreateDataflow(desc) = &mut cmd {
            desc.as_of = Some(stale);
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
        h.mux.send(cmd).await.expect("send create");

        h.mux
            .send(ComputeCommand::AllowCompaction {
                id: source,
                frontier: Antichain::from_elem(Timestamp::from(200u64)),
            })
            .await
            .expect("send compaction");

        let forwarded = maint_commands(&h)
            .into_iter()
            .find_map(|cmd| match cmd {
                ComputeCommand::AllowCompaction { id, frontier } if id == source => Some(frontier),
                _ => None,
            })
            .expect("maintenance sees a compaction");
        assert_eq!(
            forwarded,
            Antichain::from_elem(Timestamp::from(200u64)),
            "must not cap below the collection's own dataflow as_of"
        );
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
