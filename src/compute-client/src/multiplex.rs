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

use std::collections::BTreeMap;

use async_trait::async_trait;
use mz_repr::GlobalId;
use mz_service::client::GenericClient;
use timely::progress::Antichain;

use crate::protocol::command::ComputeCommand;
use crate::protocol::response::{ComputeResponse, FrontiersResponse};
use crate::service::ComputeClient;

/// Whether a [`FrontiersResponse`] reports only empty frontiers, i.e. every frontier it carries is
/// the empty antichain. This is the terminal report a collection emits when it is dropped.
fn frontiers_all_empty(frontiers: &FrontiersResponse) -> bool {
    let empty =
        |f: &Option<Antichain<mz_repr::Timestamp>>| f.as_ref().map_or(true, |f| f.is_empty());
    empty(&frontiers.write_frontier)
        && empty(&frontiers.input_frontier)
        && empty(&frontiers.output_frontier)
}

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
    /// The runtime that renders each transient collection, learned from `CreateDataflow`.
    ///
    /// An entry is evicted when the collection's `AllowCompaction` reaches the empty frontier, so
    /// the map does not grow without bound.
    transient_owner: BTreeMap<GlobalId, Runtime>,
}

impl Multiplexer {
    /// Wraps a maintenance and an interactive compute client into one multiplexed client.
    pub fn new(maintenance: Box<dyn ComputeClient>, interactive: Box<dyn ComputeClient>) -> Self {
        Self {
            maintenance,
            interactive,
            transient_owner: BTreeMap::new(),
        }
    }

    /// The runtime that owns `id`. A recorded transient owner wins, otherwise maintenance.
    fn owner_of(&self, id: GlobalId) -> Runtime {
        match self.transient_owner.get(&id) {
            Some(Runtime::Interactive) => Runtime::Interactive,
            _ => Runtime::Maintenance,
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
    /// One response kind is filtered; the rest forward verbatim:
    ///
    /// * A `Frontiers` report from a runtime that does not own the collection. Both runtimes
    ///   install the internally-created logging/introspection dataflows (the interactive runtime's
    ///   are empty, see `initialize_logging`), so both would report frontiers for the same
    ///   collection ids. The controller tracks one frontier stream per collection, so a second
    ///   stream regresses it (the interactive runtime's empty collection reports the empty frontier,
    ///   then maintenance's real one reports a finite frontier). Forwarding only the owner's report
    ///   keeps the interactive runtime's copies off the wire while still delivering frontiers for
    ///   the transient collections it does own.
    ///
    /// The exception is the empty (terminal) `Frontiers` report the interactive runtime emits when
    /// one of its transient collections is dropped. `AllowCompaction{empty}` evicts the transient's
    /// ownership above (to bound `transient_owner`), and that eviction races ahead of this response.
    /// Without the exception the owner check would then drop the report, and the controller would
    /// never learn the collection finished. It waits for exactly that empty frontier before releasing
    /// the collection's read holds on its inputs, so dropping it strands those holds and pins the
    /// inputs' read frontiers (a stale `since` on any upstream index/MV the transient read). Always
    /// forwarding the interactive runtime's terminal report for a transient id closes that gap. It is
    /// safe: the maintenance runtime never hosts the interactive runtime's transients, so there is no
    /// competing report for the same id.
    fn filter_response(
        &self,
        source: Runtime,
        response: ComputeResponse,
    ) -> Option<ComputeResponse> {
        match response {
            ComputeResponse::Frontiers(id, frontiers) => {
                let terminal_transient_drop = source == Runtime::Interactive
                    && id.is_transient()
                    && frontiers_all_empty(&frontiers);
                if source == self.owner_of(id) || terminal_transient_drop {
                    Some(ComputeResponse::Frontiers(id, frontiers))
                } else {
                    None
                }
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
                // Interactive serves only wholly-transient, non-subscribe dataflows. A mixed
                // (non-homogeneous) dataflow returns `is_transient() == false` and stays on
                // maintenance, which is safe.
                let to_interactive = desc.is_transient() && desc.subscribe_ids().next().is_none();
                if to_interactive {
                    for id in desc.export_ids() {
                        self.transient_owner.insert(id, Runtime::Interactive);
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
                let evict = frontier.is_empty() && self.transient_owner.contains_key(&id);
                self.client_mut(runtime)
                    .send(AllowCompaction { id, frontier })
                    .await?;
                if evict {
                    self.transient_owner.remove(&id);
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

    /// Builds a `CreateDataflow` command exporting `index_ids` as indexes and `subscribe_ids` as
    /// subscribe sinks.
    fn create_dataflow(index_ids: &[GlobalId], subscribe_ids: &[GlobalId]) -> ComputeCommand {
        use mz_compute_types::dataflows::{DataflowDescription, IndexDesc};
        use mz_compute_types::plan::render_plan::RenderPlan;
        use mz_compute_types::sinks::{
            ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection,
        };
        use mz_repr::ReprRelationType;
        use mz_storage_types::controller::CollectionMetadata;

        let mut desc = DataflowDescription::<RenderPlan, CollectionMetadata>::new("test".into());
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
        ComputeCommand::CreateDataflow(Box::new(desc))
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
        // A `User` (non-transient) export id makes the dataflow maintained.
        let cmd = create_dataflow(&[GlobalId::User(7)], &[]);
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
    async fn terminal_transient_frontier_forwarded_after_eviction() {
        // A transient collection's drop evicts its ownership (`AllowCompaction{empty}`), which races
        // ahead of the interactive runtime's terminal empty-frontier report. That report must still
        // be forwarded: the controller releases the collection's read holds only on seeing it, so
        // dropping it would strand the holds and pin upstream read frontiers.
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

        // The terminal all-empty report from interactive is still forwarded.
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

        // A non-empty (non-terminal) interactive report for the evicted transient stays dropped: it
        // is not the owner and the exception only covers all-empty reports. Maintenance's finite
        // report for the same id is what the multiplexer forwards next.
        h.inter_tx.send(frontiers(id, 5)).expect("send inter");
        h.maint_tx.send(frontiers(id, 200)).expect("send maint");
        let got = h.mux.recv().await.expect("recv");
        match got {
            Some(ComputeResponse::Frontiers(g, f)) => {
                assert_eq!(g, id);
                assert_eq!(
                    f.write_frontier,
                    Some(Antichain::from_elem(Timestamp::from(200u64))),
                    "non-terminal interactive report dropped; maintenance's forwarded"
                );
            }
            other => panic!("expected maintenance frontier, got {other:?}"),
        }
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
