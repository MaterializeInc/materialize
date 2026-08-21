// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
            ComputeCommand::AllowCompaction { id: seen, frontier } if seen == id => Some(frontier),
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
                connection: ComputeSinkConnection::CopyToS3Oneshot(CopyToS3OneshotSinkConnection {
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

    // And its compaction is NOT broadcast. Interactive can import an arrangement, and a
    // maintenance-owned transient collection is a sink with none, so interactive has installed
    // nothing under this id. Handing it the drop asks it to drop a collection it does not have.
    for frontier in [
        Antichain::from_elem(Timestamp::from(10u64)),
        Antichain::new(),
    ] {
        h.mux
            .send(ComputeCommand::AllowCompaction { id, frontier })
            .await
            .expect("send");
    }
    assert_eq!(compactions_for(&h, id).len(), 2);
    assert!(
        inter_commands(&h).is_empty(),
        "a maintenance-owned transient collection's compaction must not reach interactive"
    );
}

/// An importing create goes only to interactive, and the compaction that follows is forwarded
/// unmodified to both runtimes.
///
/// The frontier is never withheld. Capping it was an earlier mechanism and it carried a regression
/// hazard, since the command history derives a dataflow's effective `as_of` from the last frontier
/// seen per export. The standing hold protects the read instead, so there is nothing to withhold,
/// and this test is the detector for a cap creeping back in.
#[mz_ore::test(tokio::test)]
async fn create_routes_to_interactive_and_compaction_is_not_capped() {
    let mut h = harness();
    let source = GlobalId::User(1);
    let export = GlobalId::Transient(7);
    let as_of = Antichain::from_elem(Timestamp::from(100u64));
    let beyond = Antichain::from_elem(Timestamp::from(200u64));

    h.mux
        .send(interactive_import_of(source, export, &as_of))
        .await
        .expect("send create");

    let seen = timeline(&h);
    assert_eq!(seen.len(), 1, "the create alone: {seen:?}");
    assert!(
        matches!(
            &seen[0],
            (Runtime::Interactive, ComputeCommand::CreateDataflow(_))
        ),
        "expected the create to interactive, got {:?}",
        seen[0]
    );

    // The controller now says its own readers are done with `source` beyond the `as_of`. Both
    // runtimes must see that frontier as issued: maintenance to compact, interactive to advance
    // its standing hold to the same place.
    h.mux
        .send(ComputeCommand::AllowCompaction {
            id: source,
            frontier: beyond.clone(),
        })
        .await
        .expect("send compaction");

    assert_eq!(
        compactions_for(&h, source),
        vec![beyond.clone()],
        "compaction must forward unmodified: the standing hold protects the read, not a cap"
    );
    assert_eq!(
        compactions_in(inter_commands(&h), source),
        vec![beyond],
        "and unmodified on the importing runtime's stream too"
    );
}

/// A `Hello` discards routing state, so a drop arriving in the new epoch is not routed by an
/// ownership record the new epoch has not re-established.
///
/// The controller replays its history after a `Hello`, which re-records ownership from the
/// replayed creates. Until it does, maintenance is the default, and routing a drop to interactive
/// on the strength of a stale record would drop a collection on the runtime that does not host it.
#[mz_ore::test(tokio::test)]
async fn hello_discards_routing_state() {
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

    assert_eq!(
        compactions_for(&h, export).len(),
        1,
        "with ownership discarded, the drop defaults to maintenance"
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
