// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use mz_compute_types::dataflows::{DataflowClass, DataflowDescription, IndexDesc};
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_expr::{MapFilterProject, RowSetFinishing};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, RelationDesc, ReprRelationType, Timestamp};
use mz_service::client::GenericClient;
use mz_storage_types::controller::CollectionMetadata;
use timely::progress::Antichain;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::protocol::command::{ComputeCommand, Peek, PeekTarget};
use crate::protocol::response::{ComputeResponse, FrontiersResponse, StatusResponse};
use crate::service::ComputeClient;

use super::Multiplexer;

/// Which mock a command reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Maintenance,
    Interactive,
}

/// A fake [`ComputeClient`] that records the commands it is sent and replays scripted responses.
///
/// Every send is appended to a `timeline` shared with the other side, so a test can assert the
/// order in which the two runtimes were addressed.
#[derive(Debug)]
struct MockClient {
    side: Side,
    timeline: Arc<Mutex<Vec<(Side, ComputeCommand)>>>,
    responses: mpsc::UnboundedReceiver<ComputeResponse>,
}

#[async_trait::async_trait]
impl GenericClient<ComputeCommand, ComputeResponse> for MockClient {
    async fn send(&mut self, command: ComputeCommand) -> Result<(), anyhow::Error> {
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
    timeline: Arc<Mutex<Vec<(Side, ComputeCommand)>>>,
    maint_tx: mpsc::UnboundedSender<ComputeResponse>,
    inter_tx: mpsc::UnboundedSender<ComputeResponse>,
}

fn harness() -> Harness {
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let (maint_tx, maint_rx) = mpsc::unbounded_channel();
    let (inter_tx, inter_rx) = mpsc::unbounded_channel();
    let maintenance: Box<dyn ComputeClient> = Box::new(MockClient {
        side: Side::Maintenance,
        timeline: Arc::clone(&timeline),
        responses: maint_rx,
    });
    let interactive: Box<dyn ComputeClient> = Box::new(MockClient {
        side: Side::Interactive,
        timeline: Arc::clone(&timeline),
        responses: inter_rx,
    });
    Harness {
        mux: Multiplexer::new(maintenance, interactive),
        timeline,
        maint_tx,
        inter_tx,
    }
}

impl Harness {
    /// Every send across both runtimes, in the order the multiplexer made them.
    fn timeline(&self) -> Vec<(Side, ComputeCommand)> {
        self.timeline.lock().expect("lock poisoned").clone()
    }
}

/// A `CreateDataflow` exporting `id` as an index, of class `class`.
fn create_index(id: GlobalId, class: DataflowClass) -> ComputeCommand {
    let mut desc = DataflowDescription::<RenderPlan, CollectionMetadata>::new("test".into());
    desc.as_of = Some(Antichain::from_elem(Timestamp::from(0u64)));
    desc.class = class;
    desc.index_exports.insert(
        id,
        (
            IndexDesc {
                on_id: id,
                key: Vec::new(),
            },
            ReprRelationType::empty(),
        ),
    );
    ComputeCommand::CreateDataflow(Box::new(desc))
}

fn peek(uuid: Uuid) -> ComputeCommand {
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
            id: GlobalId::User(1),
        },
        result_desc: RelationDesc::empty(),
        literal_constraints: None,
        uuid,
        timestamp: Timestamp::MIN,
        finishing: RowSetFinishing::trivial(0),
        map_filter_project,
        otel_ctx: OpenTelemetryContext::empty(),
    }))
}

fn frontiers(id: GlobalId, ts: u64) -> ComputeResponse {
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
async fn every_command_but_peeks_reaches_both_runtimes_maintenance_first() {
    let mut h = harness();
    let id = GlobalId::User(1);
    let commands = vec![
        ComputeCommand::Hello {
            nonce: Uuid::from_u128(7),
        },
        create_index(id, DataflowClass::Maintained),
        create_index(GlobalId::Transient(2), DataflowClass::OneShotRead),
        ComputeCommand::Schedule(id),
        ComputeCommand::AllowWrites(id),
        ComputeCommand::AllowCompaction {
            id,
            frontier: Antichain::new(),
        },
        ComputeCommand::InitializationComplete,
    ];
    for command in commands.clone() {
        h.mux.send(command).await.expect("send");
    }

    let expected: Vec<_> = commands
        .into_iter()
        .flat_map(|c| [(Side::Maintenance, c.clone()), (Side::Interactive, c)])
        .collect();
    assert_eq!(h.timeline(), expected);
}

#[mz_ore::test(tokio::test)]
async fn peeks_and_their_cancellations_reach_only_the_interactive_runtime() {
    let mut h = harness();
    let uuid = Uuid::from_u128(1);
    h.mux.send(peek(uuid)).await.expect("send");
    h.mux
        .send(ComputeCommand::CancelPeek { uuid })
        .await
        .expect("send");

    assert_eq!(
        h.timeline(),
        vec![
            (Side::Interactive, peek(uuid)),
            (Side::Interactive, ComputeCommand::CancelPeek { uuid }),
        ]
    );
}

#[mz_ore::test(tokio::test)]
async fn frontiers_from_either_runtime_are_forwarded_verbatim() {
    let mut h = harness();
    h.maint_tx
        .send(frontiers(GlobalId::User(1), 3))
        .expect("send");
    let got = h.mux.recv().await.expect("recv");
    assert_eq!(got, Some(frontiers(GlobalId::User(1), 3)));

    h.inter_tx
        .send(frontiers(GlobalId::Transient(2), 5))
        .expect("send");
    let got = h.mux.recv().await.expect("recv");
    assert_eq!(got, Some(frontiers(GlobalId::Transient(2), 5)));
}

#[mz_ore::test(tokio::test)]
async fn recv_loses_no_message_when_both_sides_ready() {
    // Both runtimes have a message ready. `select!` picks one and drops the other's future, and
    // the dropped side's message must survive to the next `recv`.
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

#[mz_ore::test(tokio::test)]
async fn either_runtime_ending_ends_the_endpoint() {
    let mut h = harness();
    drop(h.inter_tx);
    assert_eq!(h.mux.recv().await.expect("recv"), None);
}
